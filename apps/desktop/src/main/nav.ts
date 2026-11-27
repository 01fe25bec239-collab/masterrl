/**
 * Screens deep links and notifications ask for, waiting for the renderer to take them
 * (`nav:next` long-polls, since the bridge only has calls). Only the latest one is kept.
 */
export class NavQueue {
  private pending: string | null = null;
  private waiter: ((route: string) => void) | null = null;

  push(route: string): void {
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w(route);
    } else {
      this.pending = route;
    }
  }

  /** The next screen, or `''` after `timeoutMs`. One caller at a time; a new call replaces. */
  next(timeoutMs = 25_000): Promise<string> {
    if (this.pending !== null) {
      const r = this.pending;
      this.pending = null;
      return Promise.resolve(r);
    }
    this.waiter?.('');
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (this.waiter === done) this.waiter = null;
        resolve('');
      }, timeoutMs);
      const done = (route: string) => {
        clearTimeout(timer);
        resolve(route);
      };
      this.waiter = done;
    });
  }
}
