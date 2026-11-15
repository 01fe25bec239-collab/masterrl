import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { renderHook, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it } from 'vitest';
import { EventStreamHub, SseParser, type OpenStream, type StreamEvent } from './event-stream';
import { LiveMetricsStore } from './live-metrics';
import { LogBuffer } from './log-buffer';
import { EventStreamProvider, useEventStream } from './useEventStream';
import { guidanceFor } from './useGuidance';

describe('SseParser', () => {
  it('handles fields, multi-line data, comments, ids and split chunks', () => {
    const events: StreamEvent[] = [];
    const p = new SseParser((e) => events.push(e));
    p.push(': hello\nretry: 1500\nid: a:1\nevent: metrics\ndata: {"x"');
    p.push(':1}\n\ndata: line1\ndata: line2\n\n');
    expect(events).toEqual([
      { id: 'a:1', type: 'metrics', data: '{"x":1}', json: { x: 1 } },
      { id: 'a:1', type: 'message', data: 'line1\nline2', json: undefined },
    ]);
    expect(p.retry).toBe(1500);
  });
});

/** Mock SSE server: each connection gets `script(connection, lastEventId)` lines, then closes. */
function sseServer(
  script: (n: number, lastEventId: string | undefined) => { lines: string[]; status?: number },
) {
  const seen: { lastEventId: string | undefined; topics: string | null }[] = [];
  const server: Server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const lastEventId = req.headers['last-event-id'] as string | undefined;
    seen.push({
      lastEventId,
      topics: new URL(req.url ?? '/', 'http://x').searchParams.get('topics'),
    });
    const { lines, status = 200 } = script(seen.length, lastEventId);
    res.writeHead(status, {
      'Content-Type': status === 200 ? 'text/event-stream' : 'application/problem+json',
    });
    res.end(lines.join(''));
  });
  return new Promise<{ open: OpenStream; seen: typeof seen; close: () => void }>((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as AddressInfo;
      resolve({
        seen,
        close: () => server.close(),
        open: ({ topics, lastEventId, signal }) =>
          fetch(`http://127.0.0.1:${port}/api/v1/streams?topics=${topics.join(',')}`, {
            headers: lastEventId ? { 'Last-Event-ID': lastEventId } : {},
            signal,
          }),
      });
    });
  });
}

const event = (id: number, topic: string) =>
  `id: src:${id}\nevent: metrics\ndata: ${JSON.stringify({ topic, n: id })}\n\n`;

let cleanup: (() => void)[] = [];
afterEach(() => {
  cleanup.forEach((f) => {
    f();
  });
  cleanup = [];
});

describe('useEventStream', () => {
  it('resumes with Last-Event-ID after a drop, without losing or repeating events', async () => {
    const topic = 'run:run_1:metrics';
    const srv = await sseServer((n) =>
      n === 1
        ? { lines: [1, 2, 3].map((i) => event(i, topic)) }
        : n === 2
          ? { lines: [4, 5].map((i) => event(i, topic)) }
          : { lines: [] },
    );
    const hub = new EventStreamHub(srv.open, { minDelayMs: 10, maxDelayMs: 20, random: () => 1 });
    cleanup.push(srv.close, () => {
      hub.close();
    });
    const got: number[] = [];
    const statuses: string[] = [];
    hub.onStatus(() => statuses.push(hub.status));

    const wrapper = ({ children }: { children: ReactNode }) => (
      <EventStreamProvider value={hub}>{children}</EventStreamProvider>
    );
    renderHook(
      () =>
        useEventStream(topic, (e) => {
          got.push((e.json as { n: number }).n);
        }),
      { wrapper },
    );

    await waitFor(() => {
      expect(got).toEqual([1, 2, 3, 4, 5]);
    });
    expect(srv.seen[0]).toEqual({ lastEventId: undefined, topics: topic });
    expect(srv.seen[1]?.lastEventId).toBe('src:3');
    expect(statuses).toContain('reconnecting');
    expect(statuses.slice(0, 2)).toEqual(['connecting', 'live']);
  });

  it('shares one connection across topics and routes events by topic', async () => {
    const srv = await sseServer(() => ({
      lines: [event(1, 'a'), event(2, 'b'), 'event: resync\ndata: {}\n\n'],
    }));
    const hub = new EventStreamHub(srv.open, { minDelayMs: 5000 });
    cleanup.push(srv.close, () => {
      hub.close();
    });
    const a: string[] = [];
    const b: string[] = [];
    hub.subscribe('a', (e) => a.push(e.type));
    hub.subscribe('b', (e) => b.push(e.type));
    await waitFor(() => {
      expect(b).toEqual(['metrics', 'resync']);
    });
    expect(a).toEqual(['metrics', 'resync']);
    expect(srv.seen).toHaveLength(1);
    expect(srv.seen[0]?.topics).toBe('a,b');
  });

  it('stops retrying when the server refuses the topics', async () => {
    const srv = await sseServer(() => ({ status: 403, lines: ['{"code":"MRL-AUTH-003"}'] }));
    const hub = new EventStreamHub(srv.open, { minDelayMs: 1 });
    cleanup.push(srv.close, () => {
      hub.close();
    });
    hub.subscribe('secret', () => undefined);
    await waitFor(() => {
      expect(hub.status).toBe('failed');
    });
    expect(hub.failure).toEqual({ status: 403, code: 'MRL-AUTH-003' });
    await new Promise((r) => setTimeout(r, 30));
    expect(srv.seen).toHaveLength(1);
  });

  it('treats errors thrown by the API client (status + code) the same way', async () => {
    const hub = new EventStreamHub(() =>
      Promise.reject(Object.assign(new Error('no'), { status: 401, code: 'MRL-AUTH-001' })),
    );
    cleanup.push(() => {
      hub.close();
    });
    hub.subscribe('t', () => undefined);
    await waitFor(() => {
      expect(hub.failure).toEqual({ status: 401, code: 'MRL-AUTH-001' });
    });
  });

  it('backs off exponentially, with jitter, up to the cap', async () => {
    let calls = 0;
    const times: number[] = [];
    const hub = new EventStreamHub(
      () => {
        calls += 1;
        times.push(Date.now());
        return Promise.reject(new TypeError('fetch failed'));
      },
      { minDelayMs: 20, maxDelayMs: 60, random: () => 1 },
    );
    cleanup.push(() => {
      hub.close();
    });
    hub.subscribe('t', () => undefined);
    await waitFor(
      () => {
        expect(calls).toBeGreaterThanOrEqual(4);
      },
      { timeout: 2000 },
    );
    const gaps = times.slice(1).map((t, i) => t - (times[i] ?? 0));
    expect(gaps[0]).toBeGreaterThanOrEqual(15);
    expect(gaps[1]).toBeGreaterThanOrEqual(35);
    expect(gaps[2]).toBeLessThan(200);
    expect(hub.status).toBe('reconnecting');
  });

  it('throws a clear error without a provider', () => {
    expect(() => renderHook(() => useEventStream('x', () => undefined))).toThrow(
      /EventStreamProvider/,
    );
  });
});

describe('LiveMetricsStore', () => {
  it('appends, merges series by step, inserts out-of-order points and summarizes', () => {
    const s = new LiveMetricsStore(10);
    s.appendPoints('loss', [
      [1, 3],
      [2, 2],
      [4, 1],
    ]);
    s.appendPoints('lr', [
      [2, 0.1],
      [3, 0.2],
    ]);
    const snap = s.snapshot(['loss', 'lr']);
    expect(snap.x).toEqual([1, 2, 3, 4]);
    expect(snap.ys).toEqual([
      [3, 2, null, 1],
      [null, 0.1, 0.2, null],
    ]);
    expect(s.summary('loss')).toEqual({ latest: 1, min: 1, max: 3 });
    expect(s.summary('missing')).toEqual({ latest: null, min: null, max: null });
  });

  it('drops the oldest rows when full and notifies subscribers', () => {
    const s = new LiveMetricsStore(3);
    let n = 0;
    s.subscribe(() => (n += 1));
    s.appendPoints('y', [
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 4],
    ]);
    expect(s.snapshot(['y']).x).toEqual([2, 3, 4]);
    s.appendPoints('y', [[2.5, 9]]);
    expect(s.snapshot(['y']).x).toEqual([2.5, 3, 4]);
    s.appendPoints('y', [[0, 1]]);
    expect(s.snapshot(['y']).x).toEqual([2.5, 3, 4]);
    expect(n).toBe(3);
  });

  it('loads history and downsamples snapshots', () => {
    const s = new LiveMetricsStore(100_000);
    const x = Array.from({ length: 50_000 }, (_, i) => i);
    s.load(x, { y: x.map((i) => (i === 123 ? 99 : 0)) });
    const snap = s.snapshot(['y'], 1000);
    expect(snap.x.length).toBeLessThanOrEqual(1001);
    expect(snap.ys[0]).toContain(99);
  });
});

describe('LogBuffer', () => {
  it('rate-limits lines per second and counts what it skipped', () => {
    let t = 0;
    const b = new LogBuffer({ capacity: 100, maxLinesPerSecond: 5, now: () => t });
    b.append(Array.from({ length: 8 }, (_, i) => ({ text: `l${i}` })));
    expect(b.lines().map((l) => l.text)).toEqual(['l0', 'l1', 'l2', 'l3', 'l4']);
    expect(b.skipped).toBe(3);
    t = 1000;
    b.append([{ text: 'a\nb' }]);
    expect(
      b
        .lines()
        .map((l) => l.text)
        .slice(-2),
    ).toEqual(['a', 'b']);
    expect(b.lines()).toBe(b.lines());
  });
});

describe('useGuidance', () => {
  it('explains known codes and falls back for unknown ones', () => {
    const known = guidanceFor('OOM/OOM_GPU');
    expect(known.known).toBe(true);
    expect(known.title.length).toBeGreaterThan(0);
    expect(known.what_to_do.steps.length).toBeGreaterThan(0);
    const unknown = guidanceFor('MRL-NOPE-999');
    expect(unknown).toMatchObject({
      known: false,
      code: 'MRL-NOPE-999',
      title: 'Something went wrong.',
    });
  });
});
