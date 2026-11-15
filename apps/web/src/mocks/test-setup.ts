import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll } from 'vitest';
import { handlers } from './handlers';

/** Same handlers as the dev server, for every web test. */
export const server = setupServer(...handlers);

beforeAll(() => {
  server.listen({ onUnhandledFrame: 'error' });
});
afterEach(() => {
  server.resetHandlers();
  // Some tests run in the node environment, which has no localStorage.
  if (typeof localStorage !== 'undefined') localStorage.clear();
});
afterAll(() => {
  server.close();
});
