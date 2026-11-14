import type { z } from 'zod';

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export interface ApiClientOptions {
  /** Core address, always `http://127.0.0.1:<port>`. */
  baseUrl: string;
  /** Per-launch token. Sent only in the Authorization header, never in URLs. */
  token: string;
  fetch?: typeof globalThis.fetch;
}

export interface ApiClient {
  get<T>(path: `/api/${string}`, schema: z.ZodType<T>): Promise<T>;
}

export function createApiClient({
  baseUrl,
  token,
  fetch = globalThis.fetch,
}: ApiClientOptions): ApiClient {
  const base = new URL(baseUrl);
  const extra = base.username || base.password || base.search || base.hash || base.pathname !== '/';
  if (base.protocol !== 'http:' || base.hostname !== '127.0.0.1' || extra) {
    throw new Error('The core API address must look like http://127.0.0.1:<port>.');
  }
  if (!token) throw new Error('The core API token is missing.');

  return {
    async get(path, schema) {
      const response = await fetch(new URL(path, base), {
        headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
      });
      if (!response.ok) {
        throw new ApiError(
          response.status,
          `The core API answered ${response.status} for ${path}.`,
        );
      }
      const body: unknown = await response.json();
      const parsed = schema.safeParse(body);
      if (!parsed.success) {
        throw new ApiError(response.status, `The core API sent an unexpected reply for ${path}.`);
      }
      return parsed.data;
    },
  };
}
