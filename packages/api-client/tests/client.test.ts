import { describe, expect, it, vi } from 'vitest';
import { CSRF_HEADER, describeError, ProblemError, UNREACHABLE_DETAIL } from '../src/index';
import { createMrlClient } from './fixtures/fixture-api.gen';

const base = 'http://127.0.0.1:7801';
const problem = (status: number, body: Record<string, unknown>) =>
  new Response(JSON.stringify({ type: 'about:blank', title: 'Nope', status, ...body }), {
    status,
    headers: { 'Content-Type': 'application/problem+json' },
  });
const headersOf = (fetch: ReturnType<typeof vi.fn<typeof globalThis.fetch>>, call = 0) =>
  new Headers(fetch.mock.calls[call]?.[1]?.headers);

describe('typed runtime', () => {
  it('turns problem+json into ProblemError with code, trace id and field errors', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      problem(422, {
        code: 'MRL-VAL-001',
        detail: 'The name is too long.',
        trace_id: 'tr_1',
        errors: [{ path: '/name', code: 'too_long', message: 'Use at most 200 characters.' }],
      }),
    );
    const api = createMrlClient({ baseUrl: base, token: 't', fetch });
    const err = await api
      .POST('/api/v1/projects', { body: { name: 'x' } })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(ProblemError);
    const p = err as ProblemError;
    expect([p.status, p.code, p.traceId, p.message]).toEqual([
      422,
      'MRL-VAL-001',
      'tr_1',
      'The name is too long.',
    ]);
    expect(p.fieldErrors()).toEqual({ '/name': 'Use at most 200 characters.' });
    expect(describeError(p)).toEqual({
      code: 'MRL-VAL-001',
      detail: 'The name is too long.',
      traceId: 'tr_1',
    });
  });

  it('still gives a ProblemError when the body is not a problem document', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(new Response('oops', { status: 502 }));
    const api = createMrlClient({ baseUrl: base, token: 't', fetch });
    const err = (await api.GET('/api/v1/projects').catch((e: unknown) => e)) as ProblemError;
    expect(err).toBeInstanceOf(ProblemError);
    expect(err.status).toBe(502);
    expect(err.code).toBeUndefined();
  });

  it('explains network failures in plain words', () => {
    expect(describeError(new TypeError('fetch failed'))).toEqual({ detail: UNREACHABLE_DETAIL });
  });

  it('sends the CSRF header on unsafe methods only, and JSON bodies', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(Response.json({ id: 'prj_1', name: 'a' }, { status: 201 }))
      .mockResolvedValueOnce(Response.json({ items: [], next_cursor: null }));
    const api = createMrlClient({ baseUrl: base, token: 't', fetch, csrfToken: () => 'csrf-1' });

    await api.POST('/api/v1/projects', { body: { name: 'a' } });
    await api.GET('/api/v1/projects');

    expect(headersOf(fetch, 0).get(CSRF_HEADER)).toBe('csrf-1');
    expect(headersOf(fetch, 0).get('Content-Type')).toBe('application/json');
    expect(fetch.mock.calls[0]?.[1]?.body).toBe('{"name":"a"}');
    expect(headersOf(fetch, 1).get(CSRF_HEADER)).toBeNull();
  });

  it('keeps the token in the Authorization header, never the URL', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(Response.json({ id: 'p', name: 'n' }));
    const api = createMrlClient({ baseUrl: base, token: () => 'tok-42', fetch });
    await api.GET('/api/v1/projects/{prj}', { path: { prj: 'prj_1' } });
    expect(headersOf(fetch).get('Authorization')).toBe('Bearer tok-42');
    expect(String(fetch.mock.calls[0]?.[0] as URL)).not.toContain('tok-42');
  });

  it('retries once after onUnauthorized refreshes the token', async () => {
    let token = 'old';
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(problem(401, { code: 'MRL-AUTH-001' }))
      .mockResolvedValueOnce(Response.json({ items: [], next_cursor: null }));
    const onUnauthorized = vi.fn(() => {
      token = 'new';
      return true;
    });
    const api = createMrlClient({ baseUrl: base, token: () => token, fetch, onUnauthorized });

    await expect(api.GET('/api/v1/projects')).resolves.toEqual({ items: [], next_cursor: null });
    expect(onUnauthorized).toHaveBeenCalledOnce();
    expect(headersOf(fetch, 1).get('Authorization')).toBe('Bearer new');
  });

  it('gives up after one retry', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementation(() => Promise.resolve(problem(401, {})));
    const api = createMrlClient({ baseUrl: base, token: 't', fetch, onUnauthorized: () => true });
    await expect(api.GET('/api/v1/projects')).rejects.toBeInstanceOf(ProblemError);
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('rejects non-loopback addresses and paths that leave the core', async () => {
    expect(() => createMrlClient({ baseUrl: 'http://10.0.0.2:7801', token: 't' })).toThrow(
      /127\.0\.0\.1/,
    );
    const api = createMrlClient({
      baseUrl: base,
      token: 't',
      fetch: vi.fn<typeof globalThis.fetch>(),
    });
    await expect(api.GET('//evil.example/x' as '/api/v1/projects')).rejects.toThrow(/core address/);
  });

  it('opens SSE streams with Accept and Last-Event-ID', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(new Response('', { status: 200 }));
    const api = createMrlClient({ baseUrl: base, token: 't', fetch });
    await api.stream('/api/v1/streams', {
      query: { topics: 'run:run_1:metrics' },
      lastEventId: 'outbox:42',
      signal: new AbortController().signal,
    });
    const h = headersOf(fetch);
    expect(h.get('Accept')).toBe('text/event-stream');
    expect(h.get('Last-Event-ID')).toBe('outbox:42');
    expect(String(fetch.mock.calls[0]?.[0] as URL)).toBe(
      `${base}/api/v1/streams?topics=run%3Arun_1%3Ametrics`,
    );
  });
});
