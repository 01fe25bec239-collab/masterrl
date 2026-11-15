import { defineConfig } from 'vitest/config';

// Used by `pnpm test` (the package root has no config file of its own).
export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/lib/test-env.ts'],
  },
});
