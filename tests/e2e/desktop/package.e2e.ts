// The dev package (`pnpm --filter desktop package`): the packaged app with the release core
// and agent it bundles in resources/bin. PostgreSQL comes from PATH (or MRL_PGBUNDLE__BIN_DIR);
// shipping it is P50's. Cold start is measured here, on the second launch (the first one
// creates the database). Runs when the package exists; MRL_E2E_PACKAGE=1 makes it required.
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import path from 'node:path';
import { expect, test } from '../../../apps/desktop/e2e/pw';
import { Computer, packagedApp, packagedBin, until, type Desktop } from './harness';

const required = process.env.MRL_E2E_PACKAGE === '1';
const SCALE = Number(process.env.MRL_BUDGET_SCALE ?? '1');
const exe = (name: string) => (process.platform === 'win32' ? `${name}.exe` : name);

test.skip(
  !required && !existsSync(packagedApp()),
  'no dev package; build it with pnpm --filter desktop package',
);

let open: Desktop | null = null;
test.afterEach(async () => {
  await open?.quit();
  open = null;
});

async function coreReady(d: Desktop): Promise<void> {
  await d.coreReady();
  await expect(d.page.getByText(/MasterRL core…/)).toHaveCount(0);
}

test('the dev package starts the core it bundles, within the cold-start budget', async () => {
  for (const name of ['mrl-server', 'mrl-agent']) {
    expect(existsSync(path.join(packagedBin(), exe(name))), `${name} in resources/bin`).toBe(true);
  }
  // Sign-in is not set up here (no Supabase): the app works fully offline.
  const computer = new Computer('package', 'http://127.0.0.1:9');
  open = await computer.launch({}, { packaged: true });
  await coreReady(open);
  const args = open.memory().coreArgs ?? '';
  expect(args).toContain(path.join(packagedBin(), exe('mrl-server')));
  expect(args).toContain('--stop-on-stdin-close');
  const firstPort = await until('the gRPC port to be remembered', () => computer.grpcPort());
  await open.quit();
  open = null;
  expect(computer.postgres()).toEqual([]);

  // Cold start: from launching the app to a core that answers, with the database made.
  const d = await computer.launch({}, { packaged: true });
  open = d;
  await coreReady(d);
  const coldStartMs = Date.now() - d.startedAt;
  const dir = path.join(import.meta.dirname, '../../../apps/desktop/release');
  mkdirSync(dir, { recursive: true });
  writeFileSync(
    path.join(dir, 'desktop-package-budgets.json'),
    `${JSON.stringify({ packagedColdStartMs: coldStartMs }, null, 2)}\n`,
  );
  console.log(`dev package cold start: ${String(coldStartMs)} ms`);
  expect(coldStartMs).toBeLessThanOrEqual(4000 * SCALE);
  // The gRPC port is the same as last time, and the agent's settings point at it.
  expect(computer.grpcPort()).toBe(firstPort);
  expect(computer.agentSettings()).toContain(`127.0.0.1:${String(firstPort)}`);
  await d.quit();
  open = null;

  // Someone else took the port: the core gets another one, remembered and given to the agent.
  const squatter = createServer();
  await new Promise<void>((r) => squatter.listen(firstPort, '127.0.0.1', r));
  try {
    open = await computer.launch({}, { packaged: true });
    await coreReady(open);
    const moved = await until('a new gRPC port', () => {
      const p = computer.grpcPort();
      return p !== undefined && p !== firstPort ? p : undefined;
    });
    await until('the agent settings to follow', () =>
      computer.agentSettings().includes(`127.0.0.1:${String(moved)}`) ? true : undefined,
    );
  } finally {
    squatter.close();
  }
});
