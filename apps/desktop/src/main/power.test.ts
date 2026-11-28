import { describe, expect, it, vi } from 'vitest';
import { SleepGuard } from './power';

describe('sleep guard', () => {
  it('holds one blocker while any reason holds and reports each reason', () => {
    const blocker = { start: vi.fn(() => 7), stop: vi.fn() };
    const g = new SleepGuard(blocker);
    expect(g.state()).toEqual({ manual: false, training: false, awake: false });
    g.set('manual', true);
    g.set('training', true);
    expect(blocker.start).toHaveBeenCalledTimes(1);
    expect(g.state()).toEqual({ manual: true, training: true, awake: true });
    g.set('manual', false);
    expect(g.state()).toEqual({ manual: false, training: true, awake: true });
    expect(blocker.stop).not.toHaveBeenCalled();
    g.set('training', false);
    expect(blocker.stop).toHaveBeenCalledWith(7);
    expect(g.state().awake).toBe(false);
  });
});
