import { describe, expect, it } from 'vitest';
import { jobItems } from './tray';
import { NavQueue } from './nav';

describe('nav queue', () => {
  it('hands a waiting renderer the screen, or keeps the latest for later', async () => {
    const q = new NavQueue();
    const waiting = q.next(1000);
    q.push('/settings');
    expect(await waiting).toBe('/settings');
    q.push('/a');
    q.push('/b');
    expect(await q.next(10)).toBe('/b');
    expect(await q.next(10)).toBe('');
  });
});

describe('tray menu', () => {
  it('names the running jobs', () => {
    expect(jobItems([]).map((i) => i.label)).toEqual(['No jobs running']);
    expect(
      jobItems([{ job_id: 'j', name: 'sft-1', state: 'RUNNING' }]).map((i) => i.label),
    ).toEqual(['1 job running', 'sft-1 — running']);
  });
});
