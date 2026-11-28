import { useSyncExternalStore } from 'react';
import type { Bridge, CoreStatus } from './bridge';
import { getSession } from './session';

/** Waits before asking again after the bridge itself failed (it should not). */
const RETRY_MS = 2000;

/**
 * The core's state, kept current with `core:watch` long-polls while anything listens.
 * Electron main answers a watch as soon as the state changes, or after 25 s unchanged.
 */
export class CoreStatusStore {
  private status: CoreStatus | null = null;
  private readonly listeners = new Set<() => void>();
  private polling = false;

  constructor(private readonly bridge: Bridge) {}

  readonly get = (): CoreStatus | null => this.status;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    if (!this.polling) void this.poll();
    return () => {
      this.listeners.delete(listener);
    };
  };

  private async poll(): Promise<void> {
    this.polling = true;
    while (this.listeners.size > 0) {
      try {
        const next = await this.bridge.core.watch(this.status?.version ?? -1);
        if (next.version !== this.status?.version) {
          this.status = next;
          for (const l of this.listeners) l();
        }
      } catch {
        await new Promise((resolve) => setTimeout(resolve, RETRY_MS));
      }
    }
    this.polling = false;
  }
}

const stores = new WeakMap<Bridge, CoreStatusStore>();

function storeFor(bridge: Bridge): CoreStatusStore {
  let store = stores.get(bridge);
  if (!store) {
    store = new CoreStatusStore(bridge);
    stores.set(bridge, store);
  }
  return store;
}

const none = {
  get: () => null,
  subscribe: () => () => undefined,
};

/** The core's state in the desktop app; null in a browser or before the first answer. */
export function useCoreStatus(): CoreStatus | null {
  const bridge = getSession().desktop;
  const store = bridge ? storeFor(bridge) : none;
  return useSyncExternalStore(store.subscribe, store.get);
}

/** The guidance code in a reason like "The core stopped with exit code 1 (MRL-ENV-110).". */
export function codeInReason(reason: string | undefined): string | undefined {
  return /MRL-[A-Z]+-\d{3}/.exec(reason ?? '')?.[0];
}
