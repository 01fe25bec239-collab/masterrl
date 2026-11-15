import { createTypedClient, type ClientOptions, type TypedClient } from './runtime/client';

export * from './runtime/client';
export * from './runtime/problem';

/** Options for the untyped client (no generated spec needed). */
export type ApiClientOptions = ClientOptions;

/** Client without a generated spec: zod-checked `get` and SSE `stream`. */
export type ApiClient = Pick<TypedClient<Record<string, never>>, 'get' | 'stream'>;

export function createApiClient(options: ApiClientOptions): ApiClient {
  return createTypedClient<Record<string, never>>(options);
}
