import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { isSignInPage, SignIn } from './auth';
import { CoreClient } from './core/client';

let core: Server;
let port = 0;
let page = 'https://auth.example/authorize?x=1';
const calls: { url: string; body: Record<string, unknown>; secret: unknown }[] = [];

beforeAll(async () => {
  core = createServer((req, res) => {
    let raw = '';
    req.on('data', (c: Buffer) => (raw += c.toString()));
    req.on('end', () => {
      const body = JSON.parse(raw || '{}') as Record<string, unknown>;
      calls.push({ url: req.url ?? '', body, secret: req.headers['x-mrl-launch-secret'] });
      res.setHeader('content-type', 'application/json');
      if (req.url === '/api/v1/auth/login') {
        res.end(JSON.stringify({ authorize_url: page, flow_id: 'flow1' }));
      } else if (body.code === 'good') {
        res.end('{}');
      } else {
        res.statusCode = 401;
        res.end(JSON.stringify({ code: 'MRL-AUTH-161', detail: 'The sign-in did not finish.' }));
      }
    });
  });
  await new Promise<void>((r) => core.listen(0, '127.0.0.1', r));
  port = (core.address() as AddressInfo).port;
});
afterAll(() => {
  core.close();
});

function broker(loopback: boolean, onOpen: (url: string, s: SignIn) => void) {
  const client = new CoreClient(() => ({ port }), 'secret');
  const s: SignIn = new SignIn({
    core: client,
    loopback,
    timeoutMs: 2000,
    openBrowser: (url) => {
      setTimeout(() => {
        onOpen(url, s);
      }, 10);
      return Promise.resolve();
    },
  });
  return s;
}

describe('sign-in', () => {
  it('finishes when the deep link comes back with the open flow', async () => {
    const opened: string[] = [];
    const s = broker(false, (url, b) => {
      opened.push(url);
      expect(b.complete('other-flow', 'good')).toBe(false);
      expect(b.complete('flow1', 'good')).toBe(true);
    });
    expect(await s.start('github')).toEqual({ status: 'signed_in' });
    expect(opened).toEqual([page]);
    const login = calls.find((c) => c.url === '/api/v1/auth/login');
    expect(login?.body).toEqual({ provider: 'github', redirect_uri: 'masterrl://auth/callback' });
    expect(login?.secret).toBe('secret');
    expect(calls.at(-1)?.body).toEqual({ flow_id: 'flow1', code: 'good' });
  });

  it('uses a loopback page when asked, and tells the browser tab it can close', async () => {
    let tab = '';
    const s = broker(true, () => {
      const redirect = String(calls.at(-1)?.body.redirect_uri);
      expect(redirect).toMatch(/^http:\/\/127\.0\.0\.1:\d+\/callback$/);
      void fetch(`${redirect}?flow=flow1&code=good`)
        .then((r) => r.text())
        .then((t) => (tab = t));
    });
    expect(await s.start('google')).toEqual({ status: 'signed_in' });
    expect(tab).toMatch(/close this tab/);
  });

  it('reports the core refusing the code in plain words', async () => {
    const s = broker(false, (_u, b) => b.complete('flow1', 'bad'));
    expect(await s.start('github')).toEqual({
      status: 'failed',
      code: 'MRL-AUTH-161',
      detail: 'The sign-in did not finish.',
    });
  });

  it('is cancelled by a second sign-in or a timeout', async () => {
    const s = broker(false, () => undefined);
    const first = s.start('github');
    await new Promise((r) => setTimeout(r, 50));
    const second = s.start('google');
    expect(await first).toEqual({ status: 'cancelled' });
    expect(await second).toEqual({ status: 'cancelled' });
  });

  it('refuses a sign-in page it does not trust', async () => {
    page = 'javascript:alert(1)';
    const s = broker(false, () => undefined);
    expect((await s.start('github')).status).toBe('failed');
    page = 'https://auth.example/authorize?x=1';
    expect(isSignInPage('http://127.0.0.1:54321/auth/v1/authorize')).toBe(true);
    expect(isSignInPage('http://evil.example/')).toBe(false);
    expect(isSignInPage('https://user:pw@auth.example/')).toBe(false);
  });
});
