import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { proxyToCore, type ProxyOptions } from './proxy';

let core: Server;
let port = 0;
const seen: { headers: Record<string, unknown>; body: string; url: string }[] = [];

beforeAll(async () => {
  core = createServer((req, res) => {
    let body = '';
    req.on('data', (c: Buffer) => (body += c.toString()));
    req.on('end', () => {
      seen.push({ headers: req.headers, body, url: req.url ?? '' });
      res
        .writeHead(201, { 'content-type': 'application/json', 'set-cookie': 'mrl_session=x' })
        .end('{"ok":true}');
    });
  });
  await new Promise<void>((r) => core.listen(0, '127.0.0.1', r));
  port = (core.address() as AddressInfo).port;
});
afterAll(() => {
  core.close();
});

const opts = (over: Partial<ProxyOptions> = {}): ProxyOptions => ({
  token: 'renderer-token',
  target: () => ({ port, secret: 'launch-secret' }),
  ...over,
});

const call = (init: RequestInit & { url?: string } = {}) =>
  new Request(init.url ?? 'app://app/api/v1/desktop/state?x=1', init);

describe('core proxy', () => {
  it('swaps the renderer token for the launch secret and passes only safe headers', async () => {
    const r = await proxyToCore(
      call({
        method: 'PUT',
        headers: {
          authorization: 'Bearer renderer-token',
          'content-type': 'application/json',
          'idempotency-key': 'k1',
          cookie: 'other=1',
          origin: 'https://evil.example',
        },
        body: '{"step":"done"}',
      }),
      opts(),
    );
    expect(r.status).toBe(201);
    expect(await r.json()).toEqual({ ok: true });
    expect(r.headers.get('set-cookie')).toBeNull();
    const last = seen.at(-1);
    expect(last?.url).toBe('/api/v1/desktop/state?x=1');
    expect(last?.body).toBe('{"step":"done"}');
    expect(last?.headers['x-mrl-launch-secret']).toBe('launch-secret');
    expect(last?.headers['idempotency-key']).toBe('k1');
    for (const h of ['authorization', 'cookie', 'origin']) expect(last?.headers[h]).toBeUndefined();
  });

  it.each([undefined, 'Bearer wrong-token!', 'Bearer renderer-tokenX', 'renderer-token'])(
    'refuses a call without this launch token (%s)',
    async (authorization) => {
      const before = seen.length;
      const r = await proxyToCore(
        call({ headers: authorization ? { authorization } : {} }),
        opts(),
      );
      expect(r.status).toBe(401);
      expect(seen.length).toBe(before);
    },
  );

  it('answers a plain 503 while the core is restarting', async () => {
    const r = await proxyToCore(
      call({ headers: { authorization: 'Bearer renderer-token' } }),
      opts({ target: () => null }),
    );
    expect(r.status).toBe(503);
    expect(((await r.json()) as { code: string }).code).toBe('MRL-ENV-302');
    const gone = await proxyToCore(
      call({ headers: { authorization: 'Bearer renderer-token' } }),
      opts({ target: () => ({ port: 1, secret: 's' }) }),
    );
    expect(gone.status).toBe(503);
  });

  it('allows the dev page origin only', async () => {
    const pre = await proxyToCore(
      call({ method: 'OPTIONS', headers: { origin: 'http://localhost:5173' } }),
      opts({ allowOrigin: 'http://localhost:5173' }),
    );
    expect(pre.status).toBe(204);
    expect(pre.headers.get('access-control-allow-origin')).toBe('http://localhost:5173');
    const other = await proxyToCore(
      call({ method: 'OPTIONS', headers: { origin: 'https://evil.example' } }),
      opts({ allowOrigin: 'http://localhost:5173' }),
    );
    expect(other.headers.get('access-control-allow-origin')).toBeNull();
  });
});
