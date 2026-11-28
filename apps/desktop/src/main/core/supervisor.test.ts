import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import type { CoreStatus } from '../ipc.registry';
import { CoreSupervisor, type SupervisorOptions } from './supervisor';

const fakeCore = path.join(import.meta.dirname, '../../../test/fake-core.ts');
const SECRET = 'test-launch-secret';
const running: CoreSupervisor[] = [];

function supervisor(mode: string, extra: Partial<SupervisorOptions> = {}) {
  const log = path.join(mkdtempSync(path.join(tmpdir(), 'mrl-core-')), 'log');
  const s = new CoreSupervisor({
    secret: SECRET,
    launch: () => ({
      program: process.execPath,
      args: [fakeCore, '--stop-on-stdin-close'],
      env: { ...process.env, MRL_LAUNCH_SECRET: SECRET, FAKE_CORE_MODE: mode, FAKE_CORE_LOG: log },
    }),
    backoffMs: () => 50,
    healthEveryMs: 50,
    healthTimeoutMs: 200,
    stopTimeoutMs: 2000,
    ...extra,
  });
  running.push(s);
  const events = () => readFileSync(log, 'utf8').trim().split('\n');
  return { s, events };
}

async function until(s: CoreSupervisor, ok: (st: CoreStatus) => boolean, ms = 15_000) {
  const end = Date.now() + ms;
  let st = s.status();
  while (!ok(st)) {
    if (Date.now() > end) throw new Error(`timed out in ${JSON.stringify(st)}`);
    st = await s.watch(st.version, 500);
  }
  return st;
}

afterEach(async () => {
  await Promise.all(running.splice(0).map((s) => s.stop()));
});

describe('core supervisor', () => {
  it('starts the core, reads its port and checks it answers with the secret', async () => {
    const ready: number[] = [];
    const { s } = supervisor('ok', { onReady: (ep) => ready.push(ep.port) });
    s.start();
    await until(s, (st) => st.phase === 'ready');
    const port = s.endpoint()?.port ?? 0;
    expect(ready).toEqual([port]);
    const r = await fetch(`http://127.0.0.1:${String(port)}/healthz`, {
      headers: { 'x-mrl-launch-secret': SECRET },
    });
    expect(r.status).toBe(200);
    expect(s.status().restarts).toBe(0);
  });

  it('reconnects on its own after a crash', async () => {
    const { s, events } = supervisor('crash-once');
    s.start();
    await until(s, (st) => st.phase === 'reconnecting');
    expect(s.status().reason).toMatch(/exit code 3/);
    expect(s.endpoint()).toBeNull();
    const back = await until(s, (st) => st.phase === 'ready' && st.restarts === 1);
    expect(back.reason).toBeUndefined();
    expect(events().filter((e) => e === 'start')).toHaveLength(2);
  });

  it('restarts a core that stops answering', async () => {
    const { s } = supervisor('hang');
    s.start();
    await until(s, (st) => st.phase === 'ready');
    const st = await until(s, (x) => x.phase === 'reconnecting');
    expect(st.reason).toBe('The core stopped answering.');
  });

  it('gives up after too many crashes, names the error code, and can try again', async () => {
    const { s, events } = supervisor('exit-now', { maxCrashes: 2 });
    s.start();
    const failed = await until(s, (st) => st.phase === 'failed');
    expect(failed.reason).toMatch(/MRL-ENV-110/);
    expect(events()).toHaveLength(3);
    await s.restart();
    await until(s, (st) => st.phase === 'reconnecting' || st.phase === 'starting');
    await until(s, (st) => st.phase === 'failed');
    expect(events()).toHaveLength(6);
  });

  it('restarts a core that never reports its port', async () => {
    const { s } = supervisor('never-listen', { startTimeoutMs: 300 });
    s.start();
    const st = await until(s, (x) => x.phase === 'reconnecting');
    expect(st.reason).toBe('The core took too long to start.');
  });

  it('stops the core cleanly, and kills one that does not stop', async () => {
    const { s, events } = supervisor('ok');
    s.start();
    await until(s, (st) => st.phase === 'ready');
    await s.stop();
    expect(s.status().phase).toBe('stopped');
    // Closing stdin is the stop request, on every OS.
    expect(events()).toEqual(['start', 'stdin-closed', 'stopped']);

    const slow = supervisor('slow-stop', { stopTimeoutMs: 300 });
    slow.s.start();
    await until(slow.s, (st) => st.phase === 'ready');
    const t = Date.now();
    await slow.s.stop();
    expect(Date.now() - t).toBeGreaterThanOrEqual(250);
    expect(slow.s.status().phase).toBe('stopped');
  });

  it.runIf(process.platform !== 'win32')(
    'falls back to SIGTERM when closing stdin does not stop the core',
    async () => {
      const { s, events } = supervisor('slow-stop', { termAfterMs: 100, stopTimeoutMs: 10_000 });
      s.start();
      await until(s, (st) => st.phase === 'ready');
      const t = Date.now();
      await s.stop();
      expect(Date.now() - t).toBeLessThan(5000);
      expect(events()).toEqual(['start', 'stdin-closed', 'stopped']);
    },
  );

  it('restarts a running core after a clean stop, but not once the app quits', async () => {
    const { s, events } = supervisor('ok');
    s.start();
    const first = await until(s, (st) => st.phase === 'ready');
    const firstPort = s.endpoint()?.port;
    await s.restart();
    await until(s, (st) => st.phase === 'ready' && st.version > first.version);
    expect(s.endpoint()?.port).not.toBe(firstPort);
    // A clean restart is not a crash.
    expect(s.status().restarts).toBe(0);
    expect(events()).toEqual(['start', 'stdin-closed', 'stopped', 'start']);

    // Quitting while a restart waits for the old core: nothing starts again.
    const again = s.restart();
    await s.stop();
    await again;
    expect(s.status().phase).toBe('stopped');
  });

  const posix = process.platform !== 'win32';
  const alive = (pid: number) => {
    try {
      process.kill(pid, 0);
      return true;
    } catch {
      return false;
    }
  };
  const dbPid = (lines: string[], n = 0) =>
    Number(lines.filter((l) => l.startsWith('db '))[n]?.slice(3));

  it('stopping a core that is still starting also stops its database', async () => {
    const { s, events } = supervisor('with-db');
    s.start();
    const lines = () => {
      try {
        return events();
      } catch {
        return [];
      }
    };
    await until(s, () => dbPid(lines()) > 0);
    const pid = dbPid(lines());
    // Still starting (it never listens): the stdin stop reaches it all the same, and it stops
    // its "db" first, like the real core with `--stop-on-stdin-close`.
    await s.stop();
    expect(s.status().phase).toBe('stopped');
    expect(events()).toContain('stdin-closed');
    await until(s, () => !alive(pid), 5000).catch(() => undefined);
    expect(alive(pid)).toBe(false);
  });

  it.runIf(posix)(
    'after a crash, the database is stopped before the next core starts',
    async () => {
      const { s, events } = supervisor('with-db-crash');
      s.start();
      await until(s, (st) => st.phase === 'ready' && st.restarts === 1);
      const lines = events().map((l) => l.replace(/^db \d+$/, 'db'));
      expect(lines.slice(0, 4)).toEqual(['start', 'db', 'db-stopped', 'start']);
      expect(alive(dbPid(events(), 0))).toBe(false);
    },
  );

  it('a stop while the launch is still being prepared starts nothing', async () => {
    let release: (() => void) | undefined;
    const spawned: string[] = [];
    const s = new CoreSupervisor({
      secret: SECRET,
      launch: () =>
        new Promise((resolve) => {
          release = () => {
            resolve({ program: process.execPath, args: [fakeCore], env: process.env });
          };
        }),
      spawn: (program: string) => {
        spawned.push(program);
        throw new Error('must not spawn');
      },
    });
    running.push(s);
    s.start();
    expect(s.status().phase).toBe('starting');
    await s.stop();
    release?.();
    await new Promise((r) => setTimeout(r, 50));
    expect(spawned).toEqual([]);
    expect(s.status().phase).toBe('stopped');
  });

  it('a watcher times out with the current state', async () => {
    const { s } = supervisor('ok');
    const st = await s.watch(s.status().version, 20);
    expect(st.phase).toBe('stopped');
  });
});
