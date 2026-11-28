import { createApiClient, type ClientOptions } from '@mrl/api-client';
import {
  createMrlClient,
  type components,
  type MrlApiClient,
} from '@mrl/api-client/generated/mrl-api-v1';
import { API_URL, connectApi, MOCK_TOKEN, mocksEnabled } from '../../lib/api';
import { desktopBridge, type Bridge } from './bridge';

export type { MrlApiClient };
/** Schemas of the core API (`components['schemas']['Me']`…). */
export type Schemas = components['schemas'];

/** How this window talks to its core. */
export interface Session {
  /** Typed client generated from the core's OpenAPI document. */
  client: MrlApiClient;
  /** The desktop shell's bridge; null in a plain browser. */
  desktop: Bridge | null;
}

/**
 * The client checks that its base is `http://127.0.0.1:<port>`; in the desktop app every call
 * is then sent to the app's own address instead (see `proxiedFetch`).
 */
const PROXIED_BASE = 'http://127.0.0.1:1';

/**
 * Sends each call to `proxyOrigin` (`app://app`) with the same path and query. Electron main
 * checks the renderer token there and forwards the call to the core with its launch secret,
 * so neither the core's port nor its secret is known to the page.
 */
export function proxiedFetch(proxyOrigin: string): typeof globalThis.fetch {
  return (input, init) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    // Looked up per call so test mocks that patch fetch later still apply.
    return globalThis.fetch(`${proxyOrigin}${url.pathname}${url.search}`, init);
  };
}

let current: Session | null = null;

function browserSession(): Session {
  const options: ClientOptions = {
    baseUrl: API_URL,
    token: () => import.meta.env.VITE_MRL_API_TOKEN ?? (mocksEnabled ? MOCK_TOKEN : ''),
  };
  return { client: createMrlClient(options), desktop: null };
}

/**
 * Connects the app to its core before the first render. In the desktop app the renderer
 * token comes over IPC (never in a URL); in a browser the dev settings are used.
 */
export async function startSession(bridge: Bridge | null = desktopBridge()): Promise<Session> {
  if (!bridge) {
    current = browserSession();
    return current;
  }
  const { proxyOrigin, token } = await bridge.core.connection();
  const options: ClientOptions = { baseUrl: PROXIED_BASE, token, fetch: proxiedFetch(proxyOrigin) };
  connectApi(createApiClient(options));
  current = { client: createMrlClient(options), desktop: bridge };
  return current;
}

/** This window's session (the browser one until `startSession` ran). */
export function getSession(): Session {
  current ??= browserSession();
  return current;
}

/** The typed core client. */
export function mrl(): MrlApiClient {
  return getSession().client;
}

/** Tests: forget the session. */
export function resetSession(): void {
  current = null;
}
