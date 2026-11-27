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
      args: [fakeCore],
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
    s.restart();
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
    // Windows has no SIGTERM: the process is ended, so it can't note a clean stop.
    if (process.platform !== 'win32') expect(events()).toEqual(['start', 'stopped']);

    const slow = supervisor('slow-stop', { stopTimeoutMs: 300 });
    slow.s.start();
    await until(slow.s, (st) => st.phase === 'ready');
    const t = Date.now();
    await slow.s.stop();
    expect(Date.now() - t).toBeGreaterThanOrEqual(250);
    expect(slow.s.status().phase).toBe('stopped');
  });

  it('a watcher times out with the current state', async () => {
    const { s } = supervisor('ok');
    const st = await s.watch(s.status().version, 20);
    expect(st.phase).toBe('stopped');
  });
});
