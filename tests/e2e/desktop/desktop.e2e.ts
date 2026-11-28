// The desktop app end to end: the dev build of Electron, the real Solo core with its own
// PostgreSQL, and a mock Supabase. Tests run in order and build on each other (one "computer"
// is set up, used, crashed and restarted; a second one restores the first one's setup).
//
// Needs: `cargo build -p mrl-server`, `cargo test -p mrl-testkit --lib --no-run`,
// `pnpm --filter web build`, `pnpm --filter desktop build:main`, PostgreSQL programs on PATH or
// in MRL_PGBUNDLE__BIN_DIR. Run: `pnpm --filter desktop test:e2e`.
import { mkdirSync, writeFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import path from 'node:path';
import { expect, test } from '../../../apps/desktop/e2e/pw';
import type { Bridge } from '../../../apps/desktop/src/main/ipc.registry';
import { Computer, MockSupabase, until, type Desktop } from './harness';

test.describe.configure({ mode: 'serial' });

const PASSPHRASE = 'correct horse battery staple';
/** CI machines are slower than the owner's Mac; the limits there are scaled by this. */
const SCALE = Number(process.env.MRL_BUDGET_SCALE ?? '1');
const BUDGET = { shellMb: 350, coreMb: 300, coldStartMs: 4000 * SCALE };

let supabase: MockSupabase;
let first: Computer;
let open: Desktop | null = null;
const budgets: Record<string, number> = {};

async function launch(computer: Computer, env?: Record<string, string>): Promise<Desktop> {
  open = await computer.launch(env);
  return open;
}

async function ready(d: Desktop): Promise<void> {
  await d.coreReady();
  // The banner is gone once the core answers.
  await expect(d.page.getByText(/MasterRL core…/)).toHaveCount(0);
}

test.beforeAll(async () => {
  supabase = await MockSupabase.start();
  first = new Computer('first', supabase.url);
});

test.afterEach(async () => {
  await open?.quit();
  open = null;
});

test.afterAll(() => {
  supabase.stop();
  const dir = path.join(import.meta.dirname, '../../../apps/desktop/release');
  mkdirSync(dir, { recursive: true });
  writeFileSync(path.join(dir, 'desktop-budgets.json'), `${JSON.stringify(budgets, null, 2)}\n`);
  console.log(`desktop budgets: ${JSON.stringify(budgets)}`);
});

test('first run: welcome, sign in, check this computer', async () => {
  const d = await launch(first);
  const { page } = d;
  // The core starts (initdb on first run), then the app sends a first run to onboarding.
  await expect(page.getByText('Step 1 of 4: Welcome')).toBeVisible({ timeout: 180_000 });
  await page.getByRole('button', { name: 'Get started' }).click();

  await expect(page.getByText('Step 2 of 4: Sign in')).toBeVisible();
  await page.getByRole('button', { name: 'Continue with GitHub' }).click();
  await d.approveSignIn();

  await expect(page.getByText('Step 3 of 4: Restore my setup')).toBeVisible();
  await expect(page.getByText(/no saved setup for your account yet/)).toBeVisible();
  await page.getByRole('button', { name: 'Next' }).click();

  await expect(page.getByText('Step 4 of 4: Check this computer')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'This computer can' })).toBeVisible({
    timeout: 120_000,
  });
  await page.getByRole('button', { name: 'Start using MasterRL' }).click();
  await expect(page.getByRole('heading', { level: 1, name: 'Welcome to MasterRL' })).toBeVisible();
  expect(page.url()).not.toContain('token');
});

test('turn on sync and save the setup', async () => {
  const d = await launch(first);
  const { page } = d;
  await ready(d);
  await page.getByRole('link', { name: 'Settings' }).click();
  const sync = page.locator('section', {
    has: page.getByRole('heading', { name: 'Sync your setup' }),
  });
  await sync.getByLabel('Sync passphrase').fill(PASSPHRASE);
  await sync.getByLabel('Type it again').fill(PASSPHRASE);
  await sync.getByRole('button', { name: 'Turn on sync' }).click();
  await expect(sync.getByText('Your recovery key')).toBeVisible({ timeout: 60_000 });
  await sync.getByRole('button', { name: 'I saved it' }).click();
  await sync.getByRole('button', { name: 'Save my setup now' }).click();
  await expect(sync.getByText('Saved.')).toBeVisible();
  await expect(sync.getByRole('list', { name: 'Saved copies' }).getByRole('listitem')).toHaveCount(
    1,
  );
});

test('quit and start again keeps the state, within the cold-start budget', async () => {
  const d = await launch(first);
  const { page } = d;
  await expect(page.getByRole('heading', { level: 1, name: 'Welcome to MasterRL' })).toBeVisible();
  await ready(d);
  budgets.coldStartMs = Date.now() - d.startedAt;
  // Onboarding is done: no redirect to the welcome screens.
  expect(new URL(page.url()).hash).not.toContain('welcome');
  await page.getByRole('link', { name: 'Account' }).click();
  await expect(page.getByText('Signed in', { exact: true })).toBeVisible();
  expect(budgets.coldStartMs).toBeLessThanOrEqual(BUDGET.coldStartMs);
});

test('a second computer restores the saved setup', async () => {
  const second = new Computer('second', supabase.url);
  const d = await launch(second);
  const { page } = d;
  await expect(page.getByText('Step 1 of 4: Welcome')).toBeVisible({ timeout: 180_000 });
  await page.getByRole('button', { name: 'Get started' }).click();
  await page.getByRole('button', { name: 'Continue with Google' }).click();
  await d.approveSignIn();
  await expect(page.getByText(/We found a setup you saved/)).toBeVisible();
  await page.getByLabel('Sync passphrase').fill('not the passphrase');
  await page.getByRole('button', { name: 'Restore', exact: true }).click();
  await expect(page.getByRole('alert')).toBeVisible();
  await page.getByLabel('Sync passphrase').fill(PASSPHRASE);
  await page.getByRole('button', { name: 'Restore', exact: true }).click();
  await expect(page.getByText(/Your setup is back/)).toBeVisible({ timeout: 60_000 });
});

test('the core crashes: the app reconnects by itself and keeps working', async () => {
  const d = await launch(first);
  const { page } = d;
  await ready(d);
  const before = d.memory().corePid;
  expect(before).toBeDefined();
  process.kill(before ?? 0, 'SIGKILL');
  await expect(page.getByText(/Reconnecting to MasterRL core…/)).toBeVisible();
  await ready(d);
  const after = await until('a new core', () => {
    const pid = d.memory().corePid;
    return pid !== undefined && pid !== before ? pid : undefined;
  });
  expect(after).not.toBe(before);
  await page.getByRole('link', { name: 'Account' }).click();
  await expect(page.getByText('Signed in', { exact: true })).toBeVisible();
});

test('a deep link opens the right screen; others are refused', async () => {
  const d = await launch(first);
  const { page } = d;
  await ready(d);
  await d.deepLink('masterrl://open?screen=/settings');
  await expect(page.getByRole('heading', { level: 1, name: 'Settings' })).toBeVisible();
  await d.deepLink('masterrl://open?screen=https://evil.example');
  await d.deepLink('masterrl://nope?screen=/account');
  await d.deepLink('masterrl://open?screen=/account');
  await expect(page.getByRole('heading', { level: 1, name: 'Account' })).toBeVisible();
});

test('the dev build keeps the Electron security rules (Doc 21 §9)', async () => {
  const d = await launch(first);
  const { page, app } = d;
  await ready(d);
  // The settings themselves are checked by security.test.ts and check-security.ts; here, what
  // they do. Context isolation: the bridge is a frozen contextBridge copy. No Node in the page.
  expect(
    await page.evaluate(() => Object.isFrozen((window as unknown as { mrl: Bridge }).mrl)),
  ).toBe(true);
  expect(await page.evaluate(() => ['require', 'process'].filter((k) => k in globalThis))).toEqual(
    [],
  );
  // CSP: inline scripts do not run.
  expect(
    await page.evaluate(() => {
      const script = document.createElement('script');
      script.textContent = 'document.body.dataset.inline = "ran"';
      document.head.append(script);
      return document.body.dataset.inline === 'ran';
    }),
  ).toBe(false);
  // window.open and navigation away are refused; the app keeps one window on app://app.
  expect(await page.evaluate(() => window.open('https://example.com') === null)).toBe(true);
  await page.evaluate(() => {
    window.location.href = 'https://example.com/';
  });
  await page.waitForTimeout(500);
  expect(page.url()).toMatch(/^app:\/\/app\//);
  expect(app.windows()).toHaveLength(1);
  // The core is reached only with this launch's token, never in a URL or in storage.
  const anonymous = await page.evaluate(async () => {
    const r = await fetch('app://app/api/v1/desktop/state');
    return r.status;
  });
  expect(anonymous).toBe(401);
  const { token } = await page.evaluate(() =>
    (window as unknown as { mrl: Bridge }).mrl.core.connection(),
  );
  expect(page.url()).not.toContain(token);
  expect(await page.evaluate(() => JSON.stringify(Object.entries(localStorage)))).not.toContain(
    token,
  );
  expect(d.opened().join('\n')).not.toContain(token);
});

test('a newer version on the update feed is offered', async () => {
  const feed: Server = createServer((req, res) => {
    if (req.url?.startsWith('/latest')) {
      res.writeHead(200, { 'content-type': 'text/yaml' });
      res.end(
        [
          'version: 99.0.0',
          'files:',
          '  - url: MasterRL-99.0.0.zip',
          '    sha512: AAAA',
          '    size: 1',
          'path: MasterRL-99.0.0.zip',
          'sha512: AAAA',
          "releaseDate: '2026-11-28T00:00:00.000Z'",
          '',
        ].join('\n'),
      );
      return;
    }
    res.writeHead(404).end();
  });
  await new Promise<void>((r) => feed.listen(0, '127.0.0.1', r));
  const address = feed.address();
  const port = typeof address === 'object' && address ? address.port : 0;
  try {
    const d = await launch(first, { MRL_UPDATE_FEED_URL: `http://127.0.0.1:${String(port)}/` });
    const { page } = d;
    await ready(d);
    await d.deepLink('masterrl://open?screen=/settings');
    const updates = page.locator('section', {
      has: page.getByRole('heading', { name: 'Updates' }),
    });
    await updates.getByRole('button', { name: 'Check for updates' }).click();
    await expect(updates.getByText('Version 99.0.0 is available.')).toBeVisible();
  } finally {
    feed.close();
  }
});

test('idle memory stays within budget', async () => {
  const d = await launch(first);
  await ready(d);
  // Let start-up work (migrations, first backup, page load) settle.
  await d.page.waitForTimeout(10_000);
  const m = d.memory();
  budgets.shellMb = m.shellMb;
  budgets.coreMb = m.coreMb;
  budgets.rssSumMb = m.rssMb;
  expect(m.shellMb).toBeLessThanOrEqual(BUDGET.shellMb);
  expect(m.coreMb).toBeLessThanOrEqual(BUDGET.coreMb);
});

test('starts offline after the first sign-in', async () => {
  supabase.stop();
  const d = await launch(first);
  const { page } = d;
  await expect(page.getByRole('heading', { level: 1, name: 'Welcome to MasterRL' })).toBeVisible();
  await ready(d);
  await page.getByRole('link', { name: 'Account' }).click();
  await expect(page.getByRole('heading', { level: 2, name: 'Profile' })).toBeVisible();
  await expect(page.getByText(/Signed in|Sign-in expired/)).toBeVisible();
});
