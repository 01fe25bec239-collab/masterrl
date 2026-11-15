import { describe, expect, it, vi } from 'vitest';
import { cn } from './cn';
import { diffLines } from './diff';
import { minMaxDownsample } from './downsample';
import { formatAge, formatBytes, formatNumber, formatRange } from './format';
import { RingBuffer } from './ring-buffer';
import { applyTheme } from './theme';
import { FileUpload, type UploadTransport } from './uploader';

describe('RingBuffer', () => {
  it('keeps the newest items and counts evictions', () => {
    const r = new RingBuffer<number>(3);
    [1, 2, 3, 4, 5].forEach((n) => {
      r.push(n);
    });
    expect(r.toArray()).toEqual([3, 4, 5]);
    expect([r.size, r.evicted]).toEqual([3, 2]);
    r.clear();
    expect(r.toArray()).toEqual([]);
    expect(() => new RingBuffer(0)).toThrow(/capacity/);
  });
});

describe('minMaxDownsample', () => {
  it('keeps spikes and order while shrinking', () => {
    const x = Array.from({ length: 10_000 }, (_, i) => i);
    const y = x.map((i) => (i === 4321 ? 100 : i === 777 ? -50 : Math.sin(i)));
    const out = minMaxDownsample({ x, ys: [y] }, 100);
    expect(out.x.length).toBeLessThanOrEqual(302);
    expect(out.ys[0]).toContain(100);
    expect(out.ys[0]).toContain(-50);
    expect(out.x).toEqual([...out.x].sort((a, b) => a - b));
    expect(out.x.at(-1)).toBe(9999);
  });

  it('returns small inputs unchanged with gaps as null', () => {
    expect(minMaxDownsample({ x: [1, 2], ys: [[1, Number.NaN]] }, 10)).toEqual({
      x: [1, 2],
      ys: [[1, null]],
    });
  });
});

describe('diffLines', () => {
  it('marks added, removed and same lines with line numbers', () => {
    expect(diffLines('a\nb\nc', 'a\nc\nd')).toEqual([
      { kind: 'same', text: 'a', oldLine: 1, newLine: 1 },
      { kind: 'removed', text: 'b', oldLine: 2 },
      { kind: 'same', text: 'c', oldLine: 3, newLine: 2 },
      { kind: 'added', text: 'd', newLine: 3 },
    ]);
  });
});

describe('format', () => {
  it('writes numbers, ranges, ages and sizes plainly', () => {
    expect(formatNumber(1234.5)).toBe('1,234.5');
    expect(formatNumber(0.00002)).toBe('2.00e-5');
    expect(formatNumber(null)).toBe('no value');
    expect(formatRange({ min: 2.1, max: 3.4 }, 'GB')).toBe('2.1–3.4 GB');
    expect(formatRange({ min: 3, max: 3 }, 'GB')).toBe('3 GB');
    const now = new Date('2026-01-01T12:00:00Z');
    expect(formatAge(new Date('2026-01-01T11:55:00Z'), now)).toBe('5 minutes ago');
    expect(formatAge(new Date('2026-01-01T11:59:58Z'), now)).toBe('just now');
    expect(formatBytes(1536)).toBe('1.5 KB');
    expect(cn('a', false, 'b', undefined)).toBe('a b');
  });
});

describe('applyTheme', () => {
  it('sets token variables for a fixed theme and clears them for system', () => {
    const el = document.createElement('div');
    applyTheme('dark', el);
    expect(el.style.getPropertyValue('--mrl-color-bg')).toBe('#0b0f17');
    expect(el.dataset.theme).toBe('dark');
    applyTheme('system', el);
    expect(el.style.getPropertyValue('--mrl-color-bg')).toBe('');
    expect(el.dataset.theme).toBeUndefined();
  });
});

describe('FileUpload', () => {
  const file = new File([new Uint8Array(10)], 'data.jsonl', { lastModified: 1 });

  function transport(completed: number[] = []) {
    const put = vi.fn<UploadTransport['putPart']>((_t, n) =>
      Promise.resolve({ partNumber: n, etag: `e${n}` }),
    );
    const complete = vi.fn<UploadTransport['complete']>(() => Promise.resolve());
    const t: UploadTransport = {
      open: (info) =>
        Promise.resolve({
          uploadId: info.resumeKey,
          partSize: 4,
          completedParts: completed.map((n) => ({ partNumber: n, etag: `e${n}` })),
        }),
      putPart: put,
      complete,
    };
    return { t, put, complete };
  }

  it('sends each part, then completes with all parts in order', async () => {
    const { t, put, complete } = transport();
    const up = new FileUpload(file, t);
    await up.start();
    expect(put.mock.calls.map((c) => [c[1], c[2].size])).toEqual([
      [1, 4],
      [2, 4],
      [3, 2],
    ]);
    expect(complete.mock.calls[0]?.[1].map((p) => p.partNumber)).toEqual([1, 2, 3]);
    expect(up.state).toMatchObject({ status: 'done', sentBytes: 10 });
  });

  it('resumes: skips parts the server already has', async () => {
    const { t, put } = transport([1, 2]);
    const up = new FileUpload(file, t);
    await up.start();
    expect(put.mock.calls.map((c) => c[1])).toEqual([3]);
    expect(up.state.status).toBe('done');
  });

  it('retries a failed part, then reports a failure with its code', async () => {
    const { t, put } = transport();
    put.mockRejectedValue(
      Object.assign(new Error('Disk full.'), { code: 'INFRA_TRANSIENT/DISK_FULL' }),
    );
    const up = new FileUpload(file, t, 1);
    await up.start();
    expect(put).toHaveBeenCalledTimes(3);
    expect(up.state).toMatchObject({
      status: 'failed',
      errorCode: 'INFRA_TRANSIENT/DISK_FULL',
      errorDetail: 'Disk full.',
    });

    put.mockImplementation((_t, n) => Promise.resolve({ partNumber: n, etag: `e${n}` }));
    await up.start();
    expect(up.state.status).toBe('done');
  });

  it('stays cancelled when cancel happens while the server completes', async () => {
    const { t, complete } = transport();
    const abort = vi.fn<NonNullable<UploadTransport['abort']>>(() => Promise.resolve());
    t.abort = abort;
    let finish: () => void = () => undefined;
    complete.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
    const up = new FileUpload(file, t);
    const statuses: string[] = [];
    up.subscribe(() => statuses.push(up.state.status));
    const running = up.start();
    await vi.waitFor(() => {
      expect(complete).toHaveBeenCalled();
    });
    up.pause();
    expect(up.state.status).toBe('uploading');
    await up.cancel();
    finish();
    await running;
    expect(up.state.status).toBe('cancelled');
    expect(statuses).not.toContain('done');
    expect(abort).toHaveBeenCalledOnce();
  });

  it('stays cancelled when cancel happens while the ticket is opening', async () => {
    const { t, put } = transport();
    const open = t.open.bind(t);
    const holder: { up?: FileUpload } = {};
    t.open = async (info) => {
      await holder.up?.cancel();
      return open(info);
    };
    const up = new FileUpload(file, t);
    holder.up = up;
    await up.start();
    expect(up.state.status).toBe('cancelled');
    expect(put).not.toHaveBeenCalled();
  });

  it('pauses mid-upload and continues from the next part', async () => {
    const { t, put } = transport();
    const up = new FileUpload(file, t);
    put.mockImplementationOnce((_t, n) => {
      up.pause();
      return Promise.resolve({ partNumber: n, etag: `e${n}` });
    });
    await up.start();
    expect(up.state.status).toBe('paused');
    await up.start();
    expect(put.mock.calls.map((c) => c[1])).toEqual([1, 2, 3]);
    expect(up.state.status).toBe('done');
  });
});
