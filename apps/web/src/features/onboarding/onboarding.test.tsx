import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { resetSession, setDesktopBridge } from '../../app/session';
import { fakeBridge } from '../../app/session/fake-bridge';
import { fakeCore } from '../../app/session/fake-core';
import { renderDesktop, seriousViolations } from '../../app/session/test-render';

afterEach(() => {
  setDesktopBridge(undefined);
  resetSession();
});

const steps = (core: ReturnType<typeof fakeCore>) =>
  core.state.calls.filter((c) => c.call === 'PUT /desktop/onboarding').map((c) => c.body);

describe('onboarding', () => {
  it('goes Welcome → Sign in → Restore my setup → Check this computer, saving each step', async () => {
    const user = userEvent.setup();
    const core = fakeCore({ copies: [{ version: 3, at: '2026-11-20T08:00:00Z' }] });
    const fake = fakeBridge();
    vi.mocked(fake.bridge.auth.signIn).mockImplementation(() => {
      core.state.signIn = 'active';
      return Promise.resolve({ status: 'signed_in' });
    });
    const { router, container } = await renderDesktop('/welcome', { core, bridge: fake });

    expect(await screen.findByText('Step 1 of 4: Welcome')).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
    await user.click(screen.getByRole('button', { name: 'Get started' }));

    expect(await screen.findByText('Step 2 of 4: Sign in')).toBeDefined();
    await user.click(await screen.findByRole('button', { name: 'Continue with GitHub' }));
    expect(fake.bridge.auth.signIn).toHaveBeenCalledWith('github');

    expect(await screen.findByText('Step 3 of 4: Restore my setup')).toBeDefined();
    expect(await screen.findByText(/We found a setup you saved/)).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
    await user.type(screen.getByLabelText('Sync passphrase'), 'wrong passphrase');
    await user.click(screen.getByRole('button', { name: 'Restore' }));
    expect((await screen.findByRole('alert')).textContent).toContain(
      'That passphrase does not open this setup.',
    );
    await user.clear(screen.getByLabelText('Sync passphrase'));
    await user.type(screen.getByLabelText('Sync passphrase'), 'correct horse battery');
    await user.click(screen.getByRole('button', { name: 'Restore' }));
    expect(await screen.findByText('Your setup is back: 1 machine and 0 teams.')).toBeDefined();
    await user.click(screen.getByRole('button', { name: 'Next' }));

    expect(await screen.findByText('Step 4 of 4: Check this computer')).toBeDefined();
    expect(await screen.findByText('Fine-tune small models', { exact: false })).toBeDefined();
    expect(screen.getByText(/needs an NVIDIA GPU/)).toBeDefined();
    expect(screen.getByText(/PyTorch/)).toBeDefined();
    expect(screen.getByText('Only 20 GB free.')).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
    await user.click(screen.getByRole('button', { name: 'Start using MasterRL' }));

    await waitFor(() => {
      expect(router.state.location.pathname).toBe('/');
    });
    expect(steps(core)).toEqual([
      { step: 'sign_in' },
      { step: 'restore' },
      { step: 'check' },
      { step: 'done' },
    ]);
  });

  it('comes back to the saved step after a restart', async () => {
    await renderDesktop('/welcome', { core: fakeCore({ step: 'check' }) });
    expect(await screen.findByText('Step 4 of 4: Check this computer')).toBeDefined();
  });

  it('explains a failed sign-in and lets the person skip it', async () => {
    const user = userEvent.setup();
    const fake = fakeBridge();
    vi.mocked(fake.bridge.auth.signIn).mockResolvedValue({
      status: 'failed',
      code: 'MRL-AUTH-005',
      detail: 'The sign-in link expired.',
    });
    const core = fakeCore({ step: 'sign_in' });
    await renderDesktop('/welcome', { core, bridge: fake });
    await user.click(await screen.findByRole('button', { name: 'Continue with Google' }));
    expect((await screen.findByRole('alert')).textContent).toContain('The sign-in link expired.');
    await user.click(screen.getByRole('button', { name: 'Skip for now' }));
    expect(await screen.findByText('Step 3 of 4: Restore my setup')).toBeDefined();
    expect(await screen.findByText(/Sign in to bring back a setup/)).toBeDefined();
  });

  it('says a cancelled sign-in plainly', async () => {
    const user = userEvent.setup();
    const fake = fakeBridge();
    vi.mocked(fake.bridge.auth.signIn).mockResolvedValue({ status: 'cancelled' });
    await renderDesktop('/welcome', { core: fakeCore({ step: 'sign_in' }), bridge: fake });
    await user.click(await screen.findByRole('button', { name: 'Continue with Google' }));
    expect(await screen.findByText('Sign-in was cancelled. You can try again.')).toBeDefined();
  });

  it('points a plain browser to the desktop app', async () => {
    await renderDesktop('/welcome', { desktop: false });
    expect(await screen.findByText('Setting up happens in the desktop app')).toBeDefined();
  });
});
