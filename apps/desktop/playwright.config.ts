import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: 'e2e',
  testMatch: '*.e2e.ts',
  outputDir: 'release/test-results',
  timeout: 60_000,
  reporter: process.env.CI ? 'github' : 'list',
});
