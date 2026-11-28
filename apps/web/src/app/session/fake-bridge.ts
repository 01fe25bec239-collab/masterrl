// Test helper: a desktop bridge (`window.mrl`) without Electron. Imported by tests only.
import { vi } from 'vitest';
import { API_URL, MOCK_TOKEN } from '../../lib/api';
import type { Bridge, CoreStatus, CrashSummary, DesktopSettings, UpdateStatus } from './bridge';

export interface FakeBridge {
  bridge: Bridge;
  /** Changes the core's state; pending `core:watch` calls answer. */
  setCore: (patch: Partial<Omit<CoreStatus, 'version'>>) => void;
  /** The next screen a deep link asks for. */
  pushRoute: (route: string) => void;
  crashes: CrashSummary[];
}

/**
 * Calls answer like Electron main would; each is a `vi.fn` to check or override. The proxy
 * origin is the mock API's address and the token the mock token, so the MSW handlers apply.
 */
export function fakeBridge(): FakeBridge {
  let core: CoreStatus = { version: 1, phase: 'ready', restarts: 0 };
  const watchers = new Set<() => void>();
  const routes: string[] = [];
  const routeWaiters = new Set<() => void>();
  let settings: DesktopSettings = { closeToTray: true, checkForUpdates: true };
  const update: UpdateStatus = { phase: 'idle' };
  const sleep = { manual: false, training: false };
  const crashes: CrashSummary[] = [
    {
      id: 'mrl-server-1764320000000-42',
      program: 'mrl-server',
      time: '2026-11-28T09:00:00Z',
      message: 'index out of bounds',
    },
  ];

  const setCore = (patch: Partial<Omit<CoreStatus, 'version'>>) => {
    core = { ...core, ...patch, version: core.version + 1 };
    for (const w of [...watchers]) w();
  };
  const bridge: Bridge = {
    app: {
      getVersion: vi.fn(() => Promise.resolve('1.0.0')),
      getInfo: vi.fn(() =>
        Promise.resolve({ version: '1.0.0', platform: 'darwin', packaged: false }),
      ),
      showFolder: vi.fn(() => Promise.resolve(true)),
    },
    core: {
      connection: vi.fn(() => Promise.resolve({ proxyOrigin: API_URL, token: MOCK_TOKEN })),
      status: vi.fn(() => Promise.resolve(core)),
      watch: vi.fn(
        (seen: number) =>
          new Promise<CoreStatus>((resolve) => {
            if (core.version > seen) {
              resolve(core);
              return;
            }
            const done = () => {
              watchers.delete(done);
              resolve(core);
            };
            watchers.add(done);
          }),
      ),
      restart: vi.fn(() => {
        setCore({ phase: 'starting', reason: undefined });
        return Promise.resolve(core);
      }),
    },
    auth: {
      signIn: vi.fn(() => Promise.resolve({ status: 'signed_in' as const })),
      cancelSignIn: vi.fn(() => Promise.resolve(true)),
    },
    desktop: {
      blockSleep: vi.fn((on: boolean) => {
        sleep.manual = on;
        return Promise.resolve(on || sleep.training);
      }),
      sleepState: vi.fn(() => Promise.resolve({ ...sleep, awake: sleep.manual || sleep.training })),
    },
    nav: {
      next: vi.fn(
        () =>
          new Promise<string>((resolve) => {
            const take = () => {
              const r = routes.shift();
              if (r === undefined) return false;
              routeWaiters.delete(waiter);
              resolve(r);
              return true;
            };
            const waiter = () => {
              take();
            };
            if (!take()) routeWaiters.add(waiter);
          }),
      ),
    },
    crash: {
      list: vi.fn(() => Promise.resolve([...crashes])),
      read: vi.fn((id: string) =>
        Promise.resolve(`{"schema":"mrl-crash/v1","program":"mrl-server","id":"${id}"}`),
      ),
      copy: vi.fn(() => Promise.resolve(true)),
      delete: vi.fn((id: string) => {
        const at = crashes.findIndex((c) => c.id === id);
        if (at >= 0) crashes.splice(at, 1);
        return Promise.resolve(at >= 0);
      }),
      openIssue: vi.fn(() => Promise.resolve(true)),
    },
    update: {
      status: vi.fn(() => Promise.resolve(update)),
      check: vi.fn(() => Promise.resolve({ phase: 'available' as const, version: '1.1.0' })),
      install: vi.fn(() => Promise.resolve({ phase: 'preparing' as const })),
    },
    settings: {
      get: vi.fn(() => Promise.resolve(settings)),
      set: vi.fn((patch: Partial<DesktopSettings>) => {
        settings = { ...settings, ...patch };
        return Promise.resolve(settings);
      }),
    },
  };
  return {
    bridge,
    setCore,
    pushRoute: (route) => {
      routes.push(route);
      for (const w of [...routeWaiters]) w();
    },
    crashes,
  };
}
