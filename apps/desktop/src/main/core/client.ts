import { LAUNCH_SECRET_HEADER, type CoreEndpoint } from './supervisor';

/** A problem document's code and detail, or a plain reason when there was none. */
export interface CoreFailure {
  code: string;
  detail: string;
}

/** Calls the core from Electron main with the launch secret. */
export class CoreClient {
  constructor(
    private readonly endpoint: () => CoreEndpoint | null,
    private readonly secret: string,
    private readonly doFetch: typeof globalThis.fetch = (i, o) => globalThis.fetch(i, o),
  ) {}

  /** JSON answer of `method path`, or the failure (never throws). */
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- callers name the JSON shape they expect
  async call<T>(
    method: string,
    path: `/api/${string}`,
    body?: unknown,
    timeoutMs = 30_000,
  ): Promise<{ ok: true; value: T } | { ok: false; failure: CoreFailure }> {
    const ep = this.endpoint();
    if (!ep) {
      return {
        ok: false,
        failure: { code: 'MRL-ENV-302', detail: 'The app is reconnecting to its core.' },
      };
    }
    try {
      const r = await this.doFetch(`http://127.0.0.1:${String(ep.port)}${path}`, {
        method,
        headers: {
          [LAUNCH_SECRET_HEADER]: this.secret,
          ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        redirect: 'manual',
        signal: AbortSignal.timeout(timeoutMs),
      });
      const text = await r.text();
      const json: unknown = text ? JSON.parse(text) : undefined;
      if (r.ok) return { ok: true, value: json as T };
      const p = (json ?? {}) as { code?: unknown; detail?: unknown };
      return {
        ok: false,
        failure: {
          code: typeof p.code === 'string' ? p.code : `HTTP-${String(r.status)}`,
          detail:
            typeof p.detail === 'string' ? p.detail : `The core answered ${String(r.status)}.`,
        },
      };
    } catch {
      return {
        ok: false,
        failure: { code: 'MRL-ENV-302', detail: 'The core did not answer in time.' },
      };
    }
  }
}
