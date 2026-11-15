import type { z } from 'zod';
import { ApiError, toProblemError, type ProblemError } from './problem';

export type HttpMethod = 'get' | 'post' | 'put' | 'patch' | 'delete';

// ---- Types over openapi-typescript's `paths` output -------------------------------------------

type SuccessStatus = 200 | 201 | 202 | 203 | 204 | 206;

/** Paths that define `method`. openapi-typescript marks the rest `method?: never`. */
export type PathsWith<Paths, M extends HttpMethod> = {
  [P in keyof Paths]: Paths[P] extends Record<M, infer Op>
    ? [Op] extends [never]
      ? never
      : P
    : never;
}[keyof Paths] &
  string;

export type OperationOf<Paths, P extends keyof Paths, M extends HttpMethod> =
  Paths[P] extends Record<M, infer Op> ? Op : never;

type Params<Op> = Op extends { parameters: infer P } ? P : Record<string, never>;

type PathPart<Op> =
  Params<Op> extends { path: infer A } ? ([A] extends [undefined] ? object : { path: A }) : object;
type QueryPart<Op> =
  Params<Op> extends { query: infer Q }
    ? { query: Q }
    : Params<Op> extends { query?: infer Q }
      ? [Q] extends [undefined]
        ? object
        : { query?: Q }
      : object;
type BodyPart<Op> = Op extends { requestBody: { content: { 'application/json': infer B } } }
  ? { body: B }
  : Op extends { requestBody?: { content: { 'application/json': infer B } } }
    ? { body?: B }
    : object;

/** Options for one call: `path`, `query` and `body` are typed from the spec. */
export type RequestOptions<Op> = PathPart<Op> &
  QueryPart<Op> &
  BodyPart<Op> & {
    headers?: Record<string, string>;
    signal?: AbortSignal;
  };

/** The JSON body of the 2xx responses (`undefined` for 204). */
export type ResponseOf<Op> = Op extends { responses: infer R }
  ? {
      [S in keyof R & SuccessStatus]: R[S] extends { content: { 'application/json': infer J } }
        ? J
        : undefined;
    }[keyof R & SuccessStatus]
  : never;

type Args<Op> = keyof RequestOptions<Op> extends 'headers' | 'signal'
  ? [options?: RequestOptions<Op>]
  : object extends PathPart<Op> & BodyPart<Op> & QueryPart<Op>
    ? [options?: RequestOptions<Op>]
    : [options: RequestOptions<Op>];

export interface TypedClient<Paths> {
  GET<P extends PathsWith<Paths, 'get'>>(
    path: P,
    ...args: Args<OperationOf<Paths, P, 'get'>>
  ): Promise<ResponseOf<OperationOf<Paths, P, 'get'>>>;
  POST<P extends PathsWith<Paths, 'post'>>(
    path: P,
    ...args: Args<OperationOf<Paths, P, 'post'>>
  ): Promise<ResponseOf<OperationOf<Paths, P, 'post'>>>;
  PUT<P extends PathsWith<Paths, 'put'>>(
    path: P,
    ...args: Args<OperationOf<Paths, P, 'put'>>
  ): Promise<ResponseOf<OperationOf<Paths, P, 'put'>>>;
  PATCH<P extends PathsWith<Paths, 'patch'>>(
    path: P,
    ...args: Args<OperationOf<Paths, P, 'patch'>>
  ): Promise<ResponseOf<OperationOf<Paths, P, 'patch'>>>;
  DELETE<P extends PathsWith<Paths, 'delete'>>(
    path: P,
    ...args: Args<OperationOf<Paths, P, 'delete'>>
  ): Promise<ResponseOf<OperationOf<Paths, P, 'delete'>>>;
  /** Untyped escape hatch with a zod check, for endpoints not yet in the spec. */
  get<T>(path: `/api/${string}`, schema: z.ZodType<T>): Promise<T>;
  /** Opens an SSE stream (`text/event-stream`) with auth and `Last-Event-ID`. */
  stream(
    path: `/api/${string}`,
    options: { query?: Record<string, string>; lastEventId?: string; signal: AbortSignal },
  ): Promise<Response>;
}

// ---- Runtime ------------------------------------------------------------------------------------

export interface ClientOptions {
  /** Core address, always `http://127.0.0.1:<port>`. */
  baseUrl: string;
  /**
   * Per-launch token, or a function returning the current one. Sent only in the
   * Authorization header, never in URLs.
   */
  token: string | (() => string | Promise<string>);
  fetch?: typeof globalThis.fetch;
  /** CSRF value for unsafe methods (POST/PUT/PATCH/DELETE), sent as `X-CSRF-Token`. */
  csrfToken?: () => string | undefined | Promise<string | undefined>;
  /**
   * Called on 401. Return true after refreshing the token to retry the call once
   * (e.g. ask the desktop shell for a new per-launch token).
   */
  onUnauthorized?: (error: ProblemError) => boolean | Promise<boolean>;
}

export const CSRF_HEADER = 'X-CSRF-Token';

/** Throws unless `baseUrl` is exactly `http://127.0.0.1:<port>` (P02 rule). */
export function checkBaseUrl(baseUrl: string): URL {
  const base = new URL(baseUrl);
  const extra = base.username || base.password || base.search || base.hash || base.pathname !== '/';
  if (base.protocol !== 'http:' || base.hostname !== '127.0.0.1' || extra) {
    throw new Error('The core API address must look like http://127.0.0.1:<port>.');
  }
  return base;
}

const scalar = (v: unknown): string =>
  typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean'
    ? String(v)
    : JSON.stringify(v);

function buildUrl(
  base: URL,
  template: string,
  path: Record<string, unknown> | undefined,
  query: unknown,
): URL {
  const filled = template.replace(/\{([^}]+)\}/g, (_, name: string) => {
    const v = path?.[name];
    if (v === undefined || v === null)
      throw new Error(`The call to ${template} is missing "${name}".`);
    return encodeURIComponent(scalar(v));
  });
  const url = new URL(filled, base);
  if (url.origin !== base.origin) throw new Error('API paths must stay on the core address.');
  if (query && typeof query === 'object') {
    for (const [key, raw] of Object.entries(query as Record<string, unknown>)) {
      for (const v of Array.isArray(raw) ? (raw as unknown[]) : [raw]) {
        if (v !== undefined && v !== null) url.searchParams.append(key, scalar(v));
      }
    }
  }
  return url;
}

interface RawOptions {
  path?: Record<string, unknown>;
  query?: unknown;
  body?: unknown;
  headers?: Record<string, string>;
  signal?: AbortSignal;
}

/** Thin typed fetch runtime over a generated `paths` type. */
export function createTypedClient<Paths>(options: ClientOptions): TypedClient<Paths> {
  const base = checkBaseUrl(options.baseUrl);
  // Looked up per call so test mocks and instrumentation that patch fetch later still apply.
  const doFetch: typeof globalThis.fetch = (input, init) =>
    (options.fetch ?? globalThis.fetch)(input, init);
  if (typeof options.token === 'string' && !options.token)
    throw new Error('The core API token is missing.');
  const token = async () => {
    const t = typeof options.token === 'function' ? await options.token() : options.token;
    if (!t) throw new Error('The core API token is missing.');
    return t;
  };

  async function send(
    method: HttpMethod,
    template: string,
    o: RawOptions = {},
    accept = 'application/json',
  ): Promise<Response> {
    const url = buildUrl(base, template, o.path, o.query);
    for (let attempt = 0; ; attempt += 1) {
      const headers = new Headers(o.headers);
      headers.set('Authorization', `Bearer ${await token()}`);
      headers.set('Accept', accept);
      if (o.body !== undefined) headers.set('Content-Type', 'application/json');
      if (method !== 'get') {
        const csrf = await options.csrfToken?.();
        if (csrf) headers.set(CSRF_HEADER, csrf);
      }
      const response = await doFetch(url, {
        method: method.toUpperCase(),
        headers,
        body: o.body === undefined ? undefined : JSON.stringify(o.body),
        signal: o.signal,
      });
      if (response.ok) return response;
      const error = await toProblemError(response);
      if (response.status === 401 && attempt === 0 && (await options.onUnauthorized?.(error)))
        continue;
      throw error;
    }
  }

  async function json(method: HttpMethod, template: string, o?: RawOptions): Promise<unknown> {
    const response = await send(method, template, o);
    if (response.status === 204) return undefined;
    const text = await response.text();
    return text ? (JSON.parse(text) as unknown) : undefined;
  }

  const call = (method: HttpMethod) => (path: string, o?: RawOptions) => json(method, path, o);

  return {
    GET: call('get'),
    POST: call('post'),
    PUT: call('put'),
    PATCH: call('patch'),
    DELETE: call('delete'),
    async get(path, schema) {
      const body = await json('get', path);
      const parsed = schema.safeParse(body);
      if (!parsed.success) {
        throw new ApiError(200, `The core API sent an unexpected reply for ${path}.`);
      }
      return parsed.data;
    },
    stream(path, { query, lastEventId, signal }) {
      return send(
        'get',
        path,
        { query, signal, headers: lastEventId ? { 'Last-Event-ID': lastEventId } : undefined },
        'text/event-stream',
      );
    },
  } as TypedClient<Paths>;
}
