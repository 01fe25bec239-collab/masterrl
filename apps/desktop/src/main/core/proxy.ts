import { timingSafeEqual } from 'node:crypto';
import { LAUNCH_SECRET_HEADER } from './supervisor';

/** Where the core answers right now, and its launch secret. */
export interface ProxyTarget {
  port: number;
  secret: string;
}

export interface ProxyOptions {
  /** This launch's renderer token; calls must send `Authorization: Bearer <token>`. */
  token: string;
  /** The core while it is ready, else null. */
  target: () => ProxyTarget | null;
  /** Dev only: the Vite page's origin, allowed by CORS. */
  allowOrigin?: string;
  fetch?: typeof globalThis.fetch;
}

/** Error code for "the core isn't answering right now" (guidance p30.yaml). */
export const CORE_UNAVAILABLE = 'MRL-ENV-302';

/** Request headers passed on to the core. Everything else (cookies, origin…) stays here. */
const FORWARD = [
  'accept',
  'content-type',
  'idempotency-key',
  'if-match',
  'if-none-match',
  'last-event-id',
  'traceparent',
  'x-csrf-token',
];
/** Response headers never passed back. */
const DROP = ['set-cookie', 'connection', 'keep-alive', 'transfer-encoding'];

function problem(status: number, code: string, title: string, detail: string): Response {
  return new Response(JSON.stringify({ type: 'about:blank', title, status, code, detail }), {
    status,
    headers: { 'content-type': 'application/problem+json' },
  });
}

function sameToken(header: string | null, token: string): boolean {
  const given = Buffer.from(header?.startsWith('Bearer ') ? header.slice(7) : '');
  const want = Buffer.from(token);
  return given.length === want.length && timingSafeEqual(given, want);
}

function cors(headers: Headers, request: Request, allowOrigin: string | undefined): void {
  const origin = request.headers.get('origin');
  if (allowOrigin && origin === allowOrigin) {
    headers.set('access-control-allow-origin', origin);
    headers.set('vary', 'origin');
  }
}

/**
 * Serves `app://app/api/…` by forwarding to the Solo core on `127.0.0.1:<port>`.
 *
 * The renderer never holds the core's launch secret: it sends this launch's renderer token,
 * which is checked here (constant time) and replaced by the secret. Only an allow-list of
 * headers goes through, so cookies and origins of the page never reach the core.
 */
export async function proxyToCore(request: Request, o: ProxyOptions): Promise<Response> {
  const url = new URL(request.url);
  if (request.method === 'OPTIONS') {
    const headers = new Headers({
      'access-control-allow-methods': 'GET, POST, PUT, PATCH, DELETE',
      'access-control-allow-headers': ['authorization', ...FORWARD].join(', '),
      'access-control-max-age': '600',
    });
    cors(headers, request, o.allowOrigin);
    return new Response(null, { status: 204, headers });
  }
  if (!sameToken(request.headers.get('authorization'), o.token)) {
    return problem(
      401,
      'MRL-AUTH-001',
      'You need to sign in.',
      'This request did not come from the app window.',
    );
  }
  const target = o.target();
  const unavailable = () =>
    problem(
      503,
      CORE_UNAVAILABLE,
      'The app is reconnecting to its core.',
      'MasterRL is restarting its core service. Training on your machines keeps running.',
    );
  if (!target) return unavailable();
  const headers = new Headers();
  for (const name of FORWARD) {
    const v = request.headers.get(name);
    if (v !== null) headers.set(name, v);
  }
  headers.set(LAUNCH_SECRET_HEADER, target.secret);
  const hasBody = !['GET', 'HEAD'].includes(request.method);
  let answer: Response;
  try {
    answer = await (o.fetch ?? globalThis.fetch)(
      `http://127.0.0.1:${String(target.port)}${url.pathname}${url.search}`,
      {
        method: request.method,
        headers,
        body: hasBody ? request.body : undefined,
        redirect: 'manual',
        signal: request.signal,
        // Node's fetch needs this to stream a request body.
        ...(hasBody ? { duplex: 'half' } : {}),
      },
    );
  } catch {
    return unavailable();
  }
  const out = new Headers();
  answer.headers.forEach((value, name) => {
    if (!DROP.includes(name)) out.set(name, value);
  });
  cors(out, request, o.allowOrigin);
  return new Response(answer.body, { status: answer.status, headers: out });
}
