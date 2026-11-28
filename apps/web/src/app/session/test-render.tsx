// Test helper: the whole app as the desktop shell shows it. Imported by tests only.
import { createMemoryHistory } from '@tanstack/react-router';
import { act, render } from '@testing-library/react';
import axe from 'axe-core';
import { App } from '../../App';
import { createQueryClient } from '../../lib/api';
import { server } from '../../mocks/test-setup';
import { navRegistry } from '../nav.registry';
import { reloadPrefs } from '../prefs';
import { createAppRouter } from '../router';
import { setDesktopBridge } from './bridge';
import { fakeBridge, type FakeBridge } from './fake-bridge';
import { fakeCore } from './fake-core';
import { resetSession, startSession } from './session';

/** Renders the app at `path` inside a fake desktop shell, with the fake core's endpoints. */
export async function renderDesktop(
  path: string,
  options: { bridge?: FakeBridge; core?: ReturnType<typeof fakeCore>; desktop?: boolean } = {},
) {
  const fake = options.bridge ?? fakeBridge();
  const core = options.core ?? fakeCore();
  server.use(...core.handlers);
  resetSession();
  const desktop = options.desktop ?? true;
  setDesktopBridge(desktop ? fake.bridge : null);
  await startSession(desktop ? fake.bridge : null);
  reloadPrefs();
  const router = createAppRouter(navRegistry, createMemoryHistory({ initialEntries: [path] }));
  const view = render(<App router={router} queryClient={createQueryClient()} />);
  await act(() => router.load());
  return { router, fake, core, ...view };
}

/** Serious and critical axe findings, as `rule: targets`. */
export async function seriousViolations(container: HTMLElement): Promise<string[]> {
  const results = await axe.run(container);
  return results.violations
    .filter((v) => v.impact === 'serious' || v.impact === 'critical')
    .map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`);
}
