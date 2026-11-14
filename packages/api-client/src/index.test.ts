import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { ApiError, createApiClient } from './index';

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });

describe('createApiClient', () => {
  it('sends the token in the Authorization header only', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(json({ ok: true }));
    const api = createApiClient({ baseUrl: 'http://127.0.0.1:7801', token: 's3cret', fetch });

    await expect(api.get('/api/health', z.object({ ok: z.boolean() }))).resolves.toEqual({
      ok: true,
    });

    const [url, init] = fetch.mock.calls[0] ?? [];
    expect(url).toEqual(new URL('http://127.0.0.1:7801/api/health'));
    expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer s3cret');
  });

  it.each([
    'http://localhost:7801',
    'https://127.0.0.1:7801',
    'http://127.0.0.1:7801/x',
    'http://127.0.0.1:7801/?token=t',
  ])('rejects non-loopback address %s', (baseUrl) => {
    expect(() => createApiClient({ baseUrl, token: 't' })).toThrow(/127\.0\.0\.1/);
  });

  it('rejects an empty token', () => {
    expect(() => createApiClient({ baseUrl: 'http://127.0.0.1:1', token: '' })).toThrow(/token/);
  });

  it('throws ApiError on an error status', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(json({}, 503));
    const api = createApiClient({ baseUrl: 'http://127.0.0.1:1', token: 't', fetch });

    await expect(api.get('/api/health', z.unknown())).rejects.toEqual(
      expect.objectContaining({ name: 'ApiError', status: 503 }),
    );
  });

  it('throws ApiError when the reply does not match the schema', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(json({ ok: 'yes' }));
    const api = createApiClient({ baseUrl: 'http://127.0.0.1:1', token: 't', fetch });

    await expect(api.get('/api/health', z.object({ ok: z.boolean() }))).rejects.toBeInstanceOf(
      ApiError,
    );
  });
});
