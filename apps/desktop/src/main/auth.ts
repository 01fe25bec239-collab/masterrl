import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { CoreClient } from './core/client';
import type { SignInResult } from './ipc.registry';

/** Where the browser sends people back after sign-in (registered as a deep link). */
export const DEEP_LINK_REDIRECT = 'masterrl://auth/callback';
/** A started sign-in that never comes back ends after this long (P16 keeps flows 10 min). */
export const SIGN_IN_TIMEOUT_MS = 10 * 60_000;

export interface SignInOptions {
  core: CoreClient;
  /** Opens the sign-in page in the system browser. */
  openBrowser: (url: string) => Promise<void>;
  /** Use a loopback address instead of the deep link (Linux, where deep links may not work). */
  loopback: boolean;
  timeoutMs?: number;
}

interface Pending {
  flow: string;
  done: (r: { flow: string; code: string } | null) => void;
  server?: Server;
}

/** The sign-in page address must be the sign-in service's own: https, or a local mock. */
export function isSignInPage(url: string): boolean {
  try {
    const u = new URL(url);
    if (u.username || u.password) return false;
    const local = ['127.0.0.1', 'localhost', '[::1]'].includes(u.hostname);
    return u.protocol === 'https:' || (u.protocol === 'http:' && local);
  } catch {
    return false;
  }
}

const CLOSE_TAB = `<!doctype html><meta charset="utf-8"><title>MasterRL</title>
<p>You're signed in. You can close this tab and go back to MasterRL.</p>`;

/**
 * One sign-in at a time with Google or GitHub (P16's PKCE flow in the system browser):
 * ask the core for the sign-in page, open it, wait for the browser to come back with the code
 * (deep link `masterrl://auth/callback`, or a loopback page on Linux), then hand the code to the
 * core. Starting a second sign-in cancels the first.
 */
export class SignIn {
  private pending: Pending | null = null;

  constructor(private readonly o: SignInOptions) {}

  /** Called for the deep link and the loopback page. True when it finished the open sign-in. */
  complete(flow: string, code: string): boolean {
    const p = this.pending;
    if (!p || p.flow !== flow) return false;
    p.done({ flow, code });
    return true;
  }

  /** Ends the open sign-in, if any. */
  cancel(): boolean {
    const p = this.pending;
    if (!p) return false;
    p.done(null);
    return true;
  }

  async start(provider: 'google' | 'github'): Promise<SignInResult> {
    this.cancel();
    let server: Server | undefined;
    let redirect = DEEP_LINK_REDIRECT;
    if (this.o.loopback) {
      server = createServer();
      await new Promise<void>((r) => server?.listen(0, '127.0.0.1', r));
      redirect = `http://127.0.0.1:${String((server.address() as AddressInfo).port)}/callback`;
    }
    const close = () => server?.close();
    const started = await this.o.core.call<{ authorize_url: string; flow_id: string }>(
      'POST',
      '/api/v1/auth/login',
      { provider, redirect_uri: redirect },
    );
    if (!started.ok) {
      close();
      return { status: 'failed', ...started.failure };
    }
    const { authorize_url: page, flow_id: flow } = started.value;
    if (!isSignInPage(page)) {
      close();
      return {
        status: 'failed',
        code: 'MRL-AUTH-161',
        detail: 'The sign-in page address was not one the app trusts.',
      };
    }
    const back = await new Promise<{ flow: string; code: string } | null>((resolve) => {
      const timer = setTimeout(() => {
        finish(null);
      }, this.o.timeoutMs ?? SIGN_IN_TIMEOUT_MS);
      const finish = (r: { flow: string; code: string } | null) => {
        clearTimeout(timer);
        if (this.pending?.flow === flow) this.pending = null;
        close();
        resolve(r);
      };
      this.pending = { flow, done: finish, server };
      server?.on('request', (req, res) => {
        const url = new URL(req.url ?? '/', 'http://127.0.0.1');
        const f = url.searchParams.get('flow');
        const c = url.searchParams.get('code');
        if (req.method !== 'GET' || url.pathname !== '/callback' || !f || !c) {
          res.writeHead(404).end();
          return;
        }
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }).end(CLOSE_TAB);
        this.complete(f, c);
      });
      void this.o.openBrowser(page).catch(() => {
        finish(null);
      });
    });
    if (!back) return { status: 'cancelled' };
    const done = await this.o.core.call('POST', '/api/v1/auth/callback', {
      flow_id: back.flow,
      code: back.code,
    });
    return done.ok ? { status: 'signed_in' } : { status: 'failed', ...done.failure };
  }
}
