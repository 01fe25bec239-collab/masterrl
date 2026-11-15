import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { msw } from 'msw/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  // Serves /mockServiceWorker.js in dev only (never copied into builds); src/mocks starts it.
  plugins: [react(), tailwindcss(), { ...msw({ mode: 'worker-only' }), apply: 'serve' }],
  server: { port: 5173, strictPort: true },
  worker: { format: 'es' },
  test: {
    environment: 'jsdom',
    globals: true,
    // lint-rules.test.ts starts typed ESLint on the whole app; its first call can pass 5 s on CI.
    testTimeout: 30_000,
    setupFiles: ['../../packages/ui/src/lib/test-env.ts', './src/mocks/test-setup.ts'],
  },
});
