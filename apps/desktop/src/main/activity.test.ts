import { describe, expect, it, vi } from 'vitest';
import { ActivityMonitor, describeChange, type JobChange } from './activity';
import type { CoreClient } from './core/client';
import { SleepGuard } from './power';

const change = (id: string, v: number, state = 'COMPLETED'): JobChange => ({
  job_id: id,
  name: `run ${id}`,
  state,
  state_version: v,
  at: '2026-11-27T10:00:00Z',
});

function fakeCore(answers: unknown[]) {
  const paths: string[] = [];
  const core = {
    call: vi.fn((_m: string, p: string) => {
      paths.push(p);
      const next = answers.shift();
      return Promise.resolve(
        next === undefined
          ? { ok: false, failure: { code: 'MRL-ENV-302', detail: '' } }
          : { ok: true, value: next },
      );
    }),
  } as unknown as CoreClient;
  return { core, paths };
}

describe('activity', () => {
  it('marks the first answer as seen, then reports each change once', async () => {
    const answers = [
      { cursor: 'c1', training_active: false, running: [], changes: [change('a', 3)] },
      {
        cursor: 'c2',
        training_active: true,
        running: [{ job_id: 'b', name: 'run b', state: 'RUNNING' }],
        changes: [change('a', 3), change('b', 1, 'FAILED')],
      },
      // A replay after a reconnect repeats changes: still once.
      { cursor: 'c3', training_active: false, running: [], changes: [change('b', 1, 'FAILED')] },
    ];
    const { core, paths } = fakeCore(answers);
    const seen: JobChange[] = [];
    const training: boolean[] = [];
    const running: number[] = [];
    const m = new ActivityMonitor({
      core,
      onChange: (c) => seen.push(c),
      onTraining: (t) => training.push(t),
      onRunning: (r) => running.push(r.length),
    });
    await m.tick();
    await m.tick();
    await m.tick();
    expect(seen.map((c) => c.job_id)).toEqual(['b']);
    expect(training).toEqual([true, false]);
    expect(running).toEqual([0, 1, 0]);
    expect(paths).toEqual([
      '/api/v1/desktop/activity',
      '/api/v1/desktop/activity?since=c1',
      '/api/v1/desktop/activity?since=c2',
    ]);
  });

  it('keeps the cursor when the core is away, and drops training when stopped', async () => {
    const { core, paths } = fakeCore([
      { cursor: 'c1', training_active: true, running: [], changes: [] },
    ]);
    const training: boolean[] = [];
    const m = new ActivityMonitor({
      core,
      onChange: () => undefined,
      onTraining: (t) => training.push(t),
      onRunning: () => undefined,
    });
    await m.tick();
    await m.tick();
    m.stop();
    expect(paths.at(-1)).toBe('/api/v1/desktop/activity?since=c1');
    expect(training).toEqual([true, false]);
  });

  it('describes changes plainly', () => {
    expect(describeChange(change('a', 1, 'FAILED')).title).toBe('Training stopped with an error');
    expect(describeChange(change('a', 1, 'PAUSED')).body).toBe('run a is paused.');
  });
});

describe('sleep guard', () => {
  it('holds one blocker while any reason is on', () => {
    const blocker = { start: vi.fn(() => 7), stop: vi.fn() };
    const g = new SleepGuard(blocker);
    expect(g.set('training', true)).toBe(true);
    g.set('manual', true);
    g.set('training', false);
    expect(blocker.start).toHaveBeenCalledTimes(1);
    expect(blocker.stop).not.toHaveBeenCalled();
    expect(g.set('manual', false)).toBe(false);
    expect(blocker.stop).toHaveBeenCalledWith(7);
  });
});
