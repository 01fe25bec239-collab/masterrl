import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryHistory } from '@tanstack/react-router';
import { http, HttpResponse } from 'msw';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { API_URL } from '../../lib/api';
import { server } from '../../mocks/test-setup';
import { setDesktopBridge } from './bridge';
import { codeInReason } from './core-status';
import { followDeepLinks } from './deep-links';
import { fakeBridge } from './fake-bridge';
import { fakeCore } from './fake-core';
import { mrl, proxiedFetch, resetSession, startSession } from './session';
import { renderDesktop, seriousViolations } from './test-render';

afterEach(() => {
  setDesktopBridge(undefined);
  resetSession();
});

describe('desktop session', () => {
  it('sends calls to the proxy with the renderer token in a header, never in the URL', async () => {
    const fake = fakeBridge();
    vi.mocked(fake.bridge.core.connection).mockResolvedValue({
      proxyOrigin: API_URL,
      token: 'renderer-token-123',
    });
    const seen: { url: string; auth: string | null }[] = [];
    server.use(
      http.get(`${API_URL}/api/v1/desktop/state`, ({ request }) => {
        seen.push({ url: request.url, auth: request.headers.get('Authorization') });
        return HttpResponse.json({
          onboarding: { step: 'done' },
          bundled_database: true,
          version: '0.1.0',
        });
      }),
    );
    await startSession(fake.bridge);
    const state = await mrl().GET('/api/v1/desktop/state');
    expect(state.onboarding.step).toBe('done');
    expect(seen).toEqual([
      { url: `${API_URL}/api/v1/desktop/state`, auth: 'Bearer renderer-token-123' },
    ]);
    expect(seen[0]?.url).not.toContain('renderer-token');
  });

  it('rewrites only the origin of a call', async () => {
    const calls: string[] = [];
    vi.spyOn(globalThis, 'fetch').mockImplementation((input) => {
      calls.push(input instanceof Request ? input.url : input.toString());
      return Promise.resolve(new Response('{}'));
    });
    const f = proxiedFetch('app://app');
    await f(new URL('http://127.0.0.1:1/api/v1/desktop/activity?since=2026-11-28T10%3A00%3A00Z'));
    await f(new Request('http://127.0.0.1:1/api/v1/auth/me'));
    expect(calls).toEqual([
      'app://app/api/v1/desktop/activity?since=2026-11-28T10%3A00%3A00Z',
      'app://app/api/v1/auth/me',
    ]);
    vi.restoreAllMocks();
  });

  it('turns a problem+json answer into a plain error', async () => {
    server.use(
      http.get(`${API_URL}/api/v1/desktop/state`, () =>
        HttpResponse.json(
          { type: 'about:blank', title: 'Not here', status: 409, code: 'MRL-STATE-300' },
          { status: 409, headers: { 'Content-Type': 'application/problem+json' } },
        ),
      ),
    );
    await startSession(fakeBridge().bridge);
    await expect(mrl().GET('/api/v1/desktop/state')).rejects.toMatchObject({
      status: 409,
      code: 'MRL-STATE-300',
    });
  });

  it('finds the error code in a supervisor reason', () => {
    expect(codeInReason('The core stopped with exit code 1 (MRL-ENV-110).')).toBe('MRL-ENV-110');
    expect(codeInReason('The core stopped answering.')).toBeUndefined();
  });
});

describe('core status banner', () => {
  it('shows reconnecting, then the failure with Try again, then refetches when back', async () => {
    const user = userEvent.setup();
    const core = fakeCore({ step: 'done' });
    const { fake, container } = await renderDesktop('/', { core });
    await screen.findByRole('heading', { level: 1, name: 'Welcome to MasterRL' });
    expect(screen.queryByText(/Reconnecting to MasterRL core/)).toBeNull();

    act(() => {
      fake.setCore({
        phase: 'reconnecting',
        restarts: 1,
        reason: 'The core stopped answering.',
        retryAt: Date.now() + 4000,
      });
    });
    const banner = await screen.findByText(/Reconnecting to MasterRL core…/);
    expect(banner.closest('[role="status"]')?.textContent).toMatch(/Next try in \d s/);
    expect(await seriousViolations(container)).toEqual([]);

    act(() => {
      fake.setCore({
        phase: 'failed',
        reason:
          'The core stopped with exit code 1 (MRL-ENV-302). It stopped 7 times in a few minutes.',
      });
    });
    const alert = await screen.findByRole('alert');
    expect(alert.textContent).toContain('The app is reconnecting to its core.');
    expect(alert.textContent).toContain('It stopped 7 times');
    await user.click(within(alert).getByRole('button', { name: 'Try again' }));
    expect(fake.bridge.core.restart).toHaveBeenCalledTimes(1);

    const before = core.state.calls.filter((c) => c.call === 'GET /desktop/state').length;
    act(() => {
      fake.setCore({ phase: 'ready', reason: undefined });
    });
    await waitFor(() => {
      expect(screen.queryByText(/Reconnecting to MasterRL core/)).toBeNull();
      expect(core.state.calls.filter((c) => c.call === 'GET /desktop/state').length).toBe(
        before + 1,
      );
    });
  });

  it('is not shown in a plain browser', async () => {
    await renderDesktop('/', { desktop: false });
    await screen.findByRole('heading', { level: 1, name: 'Welcome to MasterRL' });
    expect(screen.queryByText(/MasterRL core/)).toBeNull();
    expect(screen.queryByRole('button', { name: /This computer/ })).toBeNull();
  });
});

describe('first run', () => {
  it('sends an unfinished onboarding to the welcome screens', async () => {
    const { router } = await renderDesktop('/', { core: fakeCore({ step: 'sign_in' }) });
    await waitFor(() => {
      expect(router.state.location.pathname).toBe('/welcome');
    });
    expect(await screen.findByText(/Step 2 of 4: Sign in/)).toBeDefined();
  });

  it('still opens a crash report before onboarding is done', async () => {
    const { router } = await renderDesktop('/settings/crash/mrl-server-1764320000000-42', {
      core: fakeCore({ step: 'welcome' }),
    });
    expect(
      await screen.findByRole('heading', { level: 1, name: 'Something went wrong' }),
    ).toBeDefined();
    expect(router.state.location.pathname).toBe('/settings/crash/mrl-server-1764320000000-42');
  });
});

describe('deep links', () => {
  it('shows the screens main asks for and ignores anything that is not a route', async () => {
    const fake = fakeBridge();
    const history = createMemoryHistory({ initialEntries: ['/'] });
    const stop = new AbortController();
    void followDeepLinks(fake.bridge, history, stop.signal);
    fake.pushRoute('/settings');
    await waitFor(() => {
      expect(history.location.pathname).toBe('/settings');
    });
    fake.pushRoute('https://evil.example/');
    fake.pushRoute('/../etc');
    fake.pushRoute('/account');
    await waitFor(() => {
      expect(history.location.pathname).toBe('/account');
    });
    expect(history.length).toBe(3);
    stop.abort();
    fake.pushRoute('/');
  });
});
