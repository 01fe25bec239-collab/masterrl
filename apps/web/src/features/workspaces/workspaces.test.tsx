import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it } from 'vitest';
import { resetSession, setDesktopBridge } from '../../app/session';
import { fakeCore } from '../../app/session/fake-core';
import { renderDesktop, seriousViolations } from '../../app/session/test-render';
import { workspaceSources } from './workspace-sources.registry';

afterEach(() => {
  setDesktopBridge(undefined);
  resetSession();
});

describe('workspace switcher', () => {
  it('has the Solo source first, with unique ids', () => {
    expect(workspaceSources[0]?.id).toBe('solo');
    const ids = workspaceSources.map((s) => s.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('lists this computer and the teams from the setup profile', async () => {
    const user = userEvent.setup();
    const core = fakeCore({
      step: 'done',
      teams: [{ name: 'Research lab', address: 'https://core.lab.example', role: 'admin' }],
    });
    const { container } = await renderDesktop('/', { core });
    await user.click(await screen.findByRole('button', { name: 'This computer' }));
    const dialog = await screen.findByRole('dialog', { name: 'Switch workspace' });
    expect(
      within(dialog)
        .getByRole('button', { name: /This computer/ })
        .getAttribute('aria-current'),
    ).toBe('true');
    const team = await within(dialog).findByRole('button', { name: /Research lab/ });
    expect(team.textContent).toContain('Your role: admin');
    expect(await seriousViolations(container)).toEqual([]);
    await user.click(team);
    expect(within(dialog).getByText('https://core.lab.example')).toBeDefined();
  });
});
