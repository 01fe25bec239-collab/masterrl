import { setupWorker } from 'msw/browser';
import { handlers } from './handlers';

/** Dev only: answers /api calls in the browser with mock data. */
export async function startMockWorker(): Promise<void> {
  await setupWorker(...handlers).start({ onUnhandledFrame: 'bypass', quiet: true });
}
