import { defineConfig } from '@playwright/test';

/**
 * The desktop e2e suite (tests/e2e/desktop): the dev build of the app with the real core,
 * its own PostgreSQL and a mock Supabase. One app at a time; tests build on each other.
 */
export default defineConfig({
  testDir: '../../tests/e2e/desktop',
  testMatch: '*.e2e.ts',
  outputDir: 'release/e2e-results',
  workers: 1,
  fullyParallel: false,
  timeout: 240_000,
  expect: { timeout: 30_000 },
  reporter: process.env.CI ? [['github'], ['list']] : 'list',
});
