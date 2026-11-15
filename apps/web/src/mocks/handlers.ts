import { http, HttpResponse } from 'msw';
import { API_URL, MOCK_TOKEN } from '../lib/api';
import { logLine, lossAt, runs, scopeTree } from './data';

const url = (path: string) => `${API_URL}${path}`;

const problem = (status: number, title: string, code?: string) =>
  HttpResponse.json(
    { type: 'about:blank', title, status, code },
    { status, headers: { 'Content-Type': 'application/problem+json' } },
  );

const authorized = (request: Request) =>
  request.headers.get('Authorization') === `Bearer ${MOCK_TOKEN}`;

/** Events per mock stream connection before the mock drops it, to show reconnect + resume. */
export const EVENTS_PER_CONNECTION = 40;

function sseStream(request: Request, intervalMs: number) {
  const topics = new URL(request.url).searchParams.get('topics')?.split(',') ?? [];
  const last = request.headers.get('Last-Event-ID');
  let n = last ? Number(last.split(':')[1] ?? '0') + 1 : 1;
  const stop = n + EVENTS_PER_CONNECTION;
  const encoder = new TextEncoder();
  let timer: ReturnType<typeof setInterval> | undefined;

  return new ReadableStream<Uint8Array>({
    start(controller) {
      const send = () => {
        for (const topic of topics) {
          const [, runId, kind] = topic.split(':');
          if (kind === 'metrics') {
            const step = n * 10;
            const data = { topic, series_id: 'loss', points: [[step, lossAt(step), Date.now()]] };
            controller.enqueue(
              encoder.encode(`id: m:${n}\nevent: metrics\ndata: ${JSON.stringify(data)}\n\n`),
            );
          } else if (kind === 'logs') {
            const data = {
              topic,
              attempt: 1,
              rank: 0,
              stream: 'stdout',
              offset: n,
              text: logLine(n),
            };
            controller.enqueue(
              encoder.encode(`id: m:${n}\nevent: log\ndata: ${JSON.stringify(data)}\n\n`),
            );
          } else if (kind === 'state' && runId) {
            controller.enqueue(encoder.encode(`id: m:${n}\nevent: heartbeat\ndata: {}\n\n`));
          }
        }
        n += 1;
        if (n >= stop) {
          clearInterval(timer);
          controller.close();
        }
      };
      timer = setInterval(send, intervalMs);
      request.signal.addEventListener('abort', () => {
        clearInterval(timer);
      });
    },
    cancel() {
      clearInterval(timer);
    },
  });
}

export const handlers = [
  http.get(url('/api/v1/me/scopes'), ({ request }) =>
    authorized(request) ? HttpResponse.json(scopeTree) : problem(401, 'Sign in again.'),
  ),

  // Doc 09 §6.1 cursor pages: `?limit=&cursor=` → `{ items, next_cursor }`.
  http.get(url('/api/v1/projects/:prj/runs'), ({ request }) => {
    if (!authorized(request)) return problem(401, 'Sign in again.');
    const params = new URL(request.url).searchParams;
    const limit = Math.min(200, Number(params.get('limit') ?? '50'));
    const start = Number(params.get('cursor') ?? '0');
    const items = runs.slice(start, start + limit);
    const next = start + limit < runs.length ? String(start + limit) : null;
    return HttpResponse.json({ items, next_cursor: next });
  }),

  http.get(url('/api/v1/streams'), ({ request }) => {
    if (!authorized(request)) return problem(401, 'Sign in again.');
    return new HttpResponse(sseStream(request, 500), {
      headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' },
    });
  }),
];
