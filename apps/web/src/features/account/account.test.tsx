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

describe('account', () => {
  it('shows who is signed in and signs out after asking', async () => {
    const user = userEvent.setup();
    const core = fakeCore({ step: 'done', signIn: 'active' });
    const { container } = await renderDesktop('/account', { core });
    expect(await screen.findByText('Sam Example')).toBeDefined();
    expect(screen.getByText('Owner of this computer')).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
    await user.click(screen.getByRole('button', { name: 'Sign out' }));
    const dialog = await screen.findByRole('dialog', { name: 'Sign out?' });
    await user.click(within(dialog).getByRole('button', { name: 'Sign out' }));
    expect(await screen.findByText('Signed out')).toBeDefined();
    expect(screen.getByRole('button', { name: 'Continue with Google' })).toBeDefined();
  });

  it('makes a command-line token, shows it once, and removes it', async () => {
    const user = userEvent.setup();
    const core = fakeCore({ step: 'done', signIn: 'active' });
    await renderDesktop('/account', { core });
    await user.type(await screen.findByLabelText('Name'), 'Laptop scripts');
    await user.click(screen.getByRole('button', { name: 'Make a token' }));
    expect(await screen.findByText('mrl_pat_AbCdEfGhIjKlMnOp')).toBeDefined();
    expect(core.state.calls.find((c) => c.call === 'POST /auth/tokens')?.body).toEqual({
      name: 'Laptop scripts',
      days: 90,
      scopes: ['*'],
    });
    await user.click(screen.getByRole('button', { name: 'Done' }));
    expect(screen.queryByText('mrl_pat_AbCdEfGhIjKlMnOp')).toBeNull();

    const list = await screen.findByRole('list', { name: 'Your tokens' });
    await user.click(within(list).getByRole('button', { name: 'Remove Laptop scripts' }));
    const dialog = await screen.findByRole('dialog', { name: 'Remove this token?' });
    await user.click(within(dialog).getByRole('button', { name: 'Remove token' }));
    await waitFor(() => {
      expect(screen.getByText('No tokens')).toBeDefined();
    });
  });
});
