import { QueryClient } from '@tanstack/react-query';
import { ApiError, createApiClient, ProblemError, type ApiClient } from '@mrl/api-client';
import { EventStreamHub } from '@mrl/ui';

/** Dev server and tests use the mock API unless `VITE_MRL_MOCKS=off`. */
export const mocksEnabled = import.meta.env.DEV && import.meta.env.VITE_MRL_MOCKS !== 'off';

export const API_URL = import.meta.env.VITE_MRL_API_URL ?? 'http://127.0.0.1:7801';
/** Token the mock API accepts; never valid against a real core. */
export const MOCK_TOKEN = 'mock-session';

const token = import.meta.env.VITE_MRL_API_TOKEN ?? (mocksEnabled ? MOCK_TOKEN : undefined);

/** Null until the app has a core address and token; the desktop shell connects it at start. */
let api: ApiClient | null = token ? createApiClient({ baseUrl: API_URL, token }) : null;

/** Use this client from now on (the desktop session calls it before the first render). */
export function connectApi(client: ApiClient): void {
  api = client;
}

export function requireApi(): ApiClient {
  if (!api) throw new ApiError(0, 'The app is not connected to its core service yet.');
  return api;
}

/** The one SSE connection for this tab. */
export const streamHub = new EventStreamHub(({ topics, lastEventId, signal }) =>
  requireApi().stream('/api/v1/streams', {
    query: { topics: topics.join(',') },
    lastEventId,
    signal,
  }),
);

/** Retry only what retrying can fix: network trouble and server errors. */
export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: (failures, error) =>
          failures < 2 && (!(error instanceof ProblemError) || error.status >= 500),
        staleTime: 30_000,
      },
    },
  });
}
