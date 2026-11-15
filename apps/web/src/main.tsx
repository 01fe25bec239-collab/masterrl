import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { applyTheme } from '@mrl/ui';
import { App } from './App';
import { navRegistry, type NavEntry } from './app/nav.registry';
import { getPrefs } from './app/prefs';
import { createAppRouter } from './app/router';
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

createRoot(root).render(
  <StrictMode>
    <App router={createAppRouter(nav)} queryClient={createQueryClient()} />
  </StrictMode>,
);
