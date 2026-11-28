// Playwright for the desktop e2e tests in tests/e2e/desktop, which sit outside any package:
// they import it from here so it resolves from this package's dependencies.
export { _electron, expect, test } from '@playwright/test';
export type { ElectronApplication, Page } from '@playwright/test';
