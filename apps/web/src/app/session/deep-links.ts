import type { RouterHistory } from '@tanstack/react-router';
import type { Bridge } from './bridge';

/** Same rule as Electron main's `routePattern`: letters, digits, `_`, `-` and `/`. */
const ROUTE = /^\/(?:[a-z0-9_-]+(?:\/[a-z0-9_-]+)*)?$/i;

/**
 * Shows the screens deep links ask for (`masterrl://open?screen=…`, a crash report…):
 * long-polls `nav:next` until `signal` aborts.
 */
export async function followDeepLinks(
  bridge: Bridge,
  history: RouterHistory,
  signal: AbortSignal,
): Promise<void> {
  // A function, so the check after each `await` reads the signal again.
  const stopped = () => signal.aborted;
  while (!stopped()) {
    let route: string;
    try {
      route = await bridge.nav.next();
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      continue;
    }
    if (!stopped() && ROUTE.test(route) && route !== history.location.pathname) {
      history.push(route);
    }
  }
}
