import { QueryClientProvider, type QueryClient } from '@tanstack/react-query';
import { RouterProvider } from '@tanstack/react-router';
import { EventStreamProvider } from '@mrl/ui';
import { streamHub } from './lib/api';
import type { AppRouter } from './app/router';

export interface AppProps {
  router: AppRouter;
  queryClient: QueryClient;
}

/** Server state through TanStack Query, live data through one shared SSE hub, pages through the router. */
export function App({ router, queryClient }: AppProps) {
  return (
    <QueryClientProvider client={queryClient}>
      <EventStreamProvider value={streamHub}>
        <RouterProvider router={router} />
      </EventStreamProvider>
    </QueryClientProvider>
  );
}
