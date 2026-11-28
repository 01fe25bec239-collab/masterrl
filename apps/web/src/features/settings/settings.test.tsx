import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it } from 'vitest';
import { resetSession, setDesktopBridge } from '../../app/session';
import { fakeCore } from '../../app/session/fake-core';
import { renderDesktop, seriousViolations } from '../../app/session/test-render';

afterEach(() => {
  setDesktopBridge(undefined);
  resetSession();
});

const done = (extra: Parameters<typeof fakeCore>[0] = {}) => fakeCore({ step: 'done', ...extra });

const section = async (name: string) =>
  (await screen.findByRole('heading', { level: 2, name })).closest('section') as HTMLElement;

describe('settings', () => {
  it('shows every desktop section with no serious a11y problems', async () => {
    const { container } = await renderDesktop('/settings', {
      core: done({
        signIn: 'active',
        syncSetUp: true,
        copies: [{ version: 2, at: '2026-11-27T09:00:00Z' }],
        backups: [
          {
            name: 'masterrl-20261127T093000123Z-daily.dump',
            reason: 'daily',
            size_bytes: 4096,
            created_at: '2026-11-27T09:30:00Z',
          },
        ],
      }),
    });
    for (const name of [
      'Sync your setup',
      'Data and backups',
      'Privacy and crash reports',
      'Updates',
      'This app',
    ]) {
      expect(await section(name)).toBeDefined();
    }
    expect(await screen.findByText('Sync is on.', { exact: false })).toBeDefined();
    expect(await screen.findByRole('list', { name: 'Backups' })).toBeDefined();
    expect(await screen.findByRole('list', { name: 'Crash reports' })).toBeDefined();
    expect(await screen.findByLabelText(/Closing the window keeps MasterRL running/)).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
  });

  it('turns on sync and shows the recovery key once', async () => {
    const user = userEvent.setup();
    const core = done({ signIn: 'active' });
    await renderDesktop('/settings', { core });
    const sync = await section('Sync your setup');
    await user.type(await within(sync).findByLabelText('Sync passphrase'), 'short');
    expect(within(sync).getByRole('button', { name: 'Turn on sync' })).toHaveProperty(
      'disabled',
      true,
    );
    await user.type(within(sync).getByLabelText('Sync passphrase'), ' but now long enough');
    await user.type(within(sync).getByLabelText('Type it again'), 'short but now long enough');
    await user.click(within(sync).getByRole('button', { name: 'Turn on sync' }));
    expect(await within(sync).findByText('RK-1111-2222-3333-4444')).toBeDefined();
    await user.click(within(sync).getByRole('button', { name: 'I saved it' }));
    expect(await within(sync).findByText(/Sync is on/)).toBeDefined();
    expect(within(sync).queryByText('RK-1111-2222-3333-4444')).toBeNull();

    // One operation id per save, so a retried save adds one copy.
    await user.click(within(sync).getByRole('button', { name: 'Save my setup now' }));
    await within(sync).findByText('Saved.');
    const push = core.state.calls.find((c) => c.call === 'POST /sync/push');
    expect((push?.body as { op_id?: string } | undefined)?.op_id).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('restores an older copy of the setup', async () => {
    const user = userEvent.setup();
    const core = done({
      signIn: 'active',
      syncSetUp: true,
      copies: [
        { version: 5, at: '2026-11-27T09:00:00Z' },
        { version: 4, at: '2026-11-20T09:00:00Z' },
      ],
    });
    await renderDesktop('/settings', { core });
    const copies = await screen.findByRole('list', { name: 'Saved copies' });
    const older = within(copies).getAllByRole('listitem')[1] as HTMLElement;
    await user.click(within(older).getByRole('button', { name: 'Restore this copy' }));
    await user.click(within(older).getByRole('button', { name: /use my recovery key/ }));
    await user.type(within(older).getByLabelText('Recovery key'), 'RK-1111');
    await user.click(within(older).getByRole('button', { name: 'Restore' }));
    expect(await within(older).findByText(/Your setup is back/)).toBeDefined();
    expect(core.state.calls.find((c) => c.call === 'POST /sync/restore')?.body).toEqual({
      recovery_key: 'RK-1111',
      version: 4,
    });
  });

  it('backs up now, and restores a backup after asking, then restarts the core', async () => {
    const user = userEvent.setup();
    const core = done({
      backups: [
        {
          name: 'masterrl-20261127T093000123Z-daily.dump',
          reason: 'daily',
          size_bytes: 4096,
          created_at: '2026-11-27T09:30:00Z',
        },
      ],
    });
    const { fake } = await renderDesktop('/settings', { core });
    const data = await section('Data and backups');
    expect(await within(data).findByText(/Application Support\/MasterRL/)).toBeDefined();
    await user.click(within(data).getByRole('button', { name: 'Back up now' }));
    expect(await within(data).findByText(/Backup made/)).toBeDefined();
    await waitFor(() => {
      expect(within(data).getAllByRole('button', { name: 'Restore' })).toHaveLength(2);
    });

    const daily = within(data).getByText(/Daily/).closest('li') as HTMLElement;
    await user.click(within(daily).getByRole('button', { name: 'Restore' }));
    const dialog = await screen.findByRole('dialog', { name: 'Restore this backup?' });
    await user.click(within(dialog).getByRole('button', { name: 'Restore' }));
    await waitFor(() => {
      expect(fake.bridge.core.restart).toHaveBeenCalledTimes(1);
    });
    expect(
      core.state.calls.some(
        (c) => c.call === 'POST /desktop/backups/masterrl-20261127T093000123Z-daily.dump/restore',
      ),
    ).toBe(true);
    await user.click(within(data).getByRole('button', { name: 'Show the data folder' }));
    expect(fake.bridge.app.showFolder).toHaveBeenCalledWith('data');
  });

  it('lists crash reports kept on this computer and deletes one', async () => {
    const user = userEvent.setup();
    const { fake } = await renderDesktop('/settings', { core: done() });
    const privacy = await section('Privacy and crash reports');
    expect(within(privacy).getByText(/MasterRL never uploads them/)).toBeDefined();
    const list = await within(privacy).findByRole('list', { name: 'Crash reports' });
    expect(within(list).getByText('The core service')).toBeDefined();
    await user.click(within(list).getByRole('button', { name: /Delete the report/ }));
    expect(fake.bridge.crash.delete).toHaveBeenCalledWith('mrl-server-1764320000000-42');
    expect(await within(privacy).findByText('No crash reports')).toBeDefined();
  });

  it('checks for updates and installs through the shell', async () => {
    const user = userEvent.setup();
    const { fake } = await renderDesktop('/settings', { core: done() });
    const updates = await section('Updates');
    await user.click(await within(updates).findByRole('button', { name: 'Check for updates' }));
    expect(await within(updates).findByText('Version 1.1.0 is available.')).toBeDefined();
    await user.click(within(updates).getByRole('button', { name: 'Install and restart' }));
    expect(fake.bridge.update.install).toHaveBeenCalledTimes(1);
    expect(
      await within(updates).findByText(/Saving checkpoints of running training/),
    ).toBeDefined();
  });

  it('keeps the app settings in the shell', async () => {
    const user = userEvent.setup();
    const { fake } = await renderDesktop('/settings', { core: done() });
    const app = await section('This app');
    await user.click(await within(app).findByLabelText(/Closing the window keeps MasterRL/));
    expect(fake.bridge.settings.set).toHaveBeenCalledWith({ closeToTray: false });
    await user.click(within(app).getByLabelText(/Keep this computer awake/));
    expect(fake.bridge.desktop.blockSleep).toHaveBeenCalledWith(true);
  });

  it('says desktop-only settings need the app in a plain browser', async () => {
    await renderDesktop('/settings', { desktop: false });
    const privacy = await section('Privacy and crash reports');
    expect(await within(privacy).findByText('Only in the desktop app')).toBeDefined();
  });
});

describe('crash report page', () => {
  it('shows the report with Copy and Open a GitHub issue, and nothing is sent', async () => {
    const user = userEvent.setup();
    const { fake, container } = await renderDesktop('/settings/crash/mrl-server-1764320000000-42', {
      core: done(),
    });
    expect(
      await screen.findByRole('heading', { level: 1, name: 'Something went wrong' }),
    ).toBeDefined();
    const report = await screen.findByRole('textbox', { name: 'Crash report' });
    expect((report as HTMLTextAreaElement).value).toContain('mrl-crash/v1');
    expect(await seriousViolations(container)).toEqual([]);
    await user.click(screen.getByRole('button', { name: 'Copy report' }));
    expect(fake.bridge.crash.copy).toHaveBeenCalledWith('mrl-server-1764320000000-42');
    expect(await screen.findByText(/Copied/)).toBeDefined();
    await user.click(screen.getByRole('button', { name: 'Open a GitHub issue' }));
    expect(fake.bridge.crash.openIssue).toHaveBeenCalledWith('mrl-server-1764320000000-42');
    expect(await screen.findByText(/Nothing is sent until you submit it there/)).toBeDefined();
  });
});
