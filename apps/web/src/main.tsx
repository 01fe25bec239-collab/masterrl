import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { applyTheme } from '@mrl/ui';
import { App } from './App';
import { navRegistry, type NavEntry } from './app/nav.registry';
import { getPrefs } from './app/prefs';
import { createAppRouter } from './app/router';
import { followDeepLinks, startSession } from './app/session';
import { createQueryClient, mocksEnabled } from './lib/api';
import './app/styles.css';

const root = document.getElementById('root');
if (!root) throw new Error('The page is missing its #root element.');

applyTheme(getPrefs().theme);

let nav: readonly NavEntry[] = navRegistry;
// `import.meta.env.DEV` is replaced at build time, so production bundles drop the mocks entirely.
if (import.meta.env.DEV && mocksEnabled) {
  const { startMockWorker } = await import('./mocks/browser');
  try {
    await startMockWorker();
  } catch (err) {
    // Some embedded browsers block service workers. Show the app anyway; API calls then fail
    // visibly with the plain-language error states instead of a blank page.
    console.warn('Mock API is off: the browser refused the mock service worker.', err);
  }
  const { mockNav } = await import('./mocks/nav');
  nav = [...navRegistry, ...mockNav];
}

// In the desktop app: the core's address and this launch's token come over IPC, never in a URL.
const session = await startSession().catch((err: unknown) => {
  // Without the bridge nothing reaches the core; the pages then explain each failed call.
  console.error('MasterRL: the window could not connect to its core.', err);
  return startSession(null);
});
const router = createAppRouter(nav);
if (session.desktop)
  void followDeepLinks(session.desktop, router.history, new AbortController().signal);

createRoot(root).render(
  <StrictMode>
    <App router={router} queryClient={createQueryClient()} />
  </StrictMode>,
);
