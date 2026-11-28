// The pieces the desktop e2e tests drive: a mock Supabase, one install ("computer") with its
// own home folder, the running app, and the processes it started.
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { createWriteStream, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createInterface } from 'node:readline';
import { _electron, type ElectronApplication, type Page } from '../../../apps/desktop/e2e/pw';
import type { Bridge } from '../../../apps/desktop/src/main/ipc.registry';

export const repo = path.resolve(import.meta.dirname, '../../..');
const desktopDir = path.join(repo, 'apps/desktop');

/** A port nothing listens on right now. */
export async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address ? address.port : 0;
      server.close(() => {
        resolve(port);
      });
    });
  });
}

/** Waits for `ok` to hold, checking every 250 ms. */
export async function until<T>(
  what: string,
  check: () => Promise<T | undefined> | T | undefined,
  ms = 60_000,
): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value !== undefined) return value;
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 250));
  }
}

/**
 * The testkit's mock Supabase (`supabase::tests::serve_for_manual_demo`): PKCE sign-in for one
 * user and the setup-sync table. Built by `cargo test -p mrl-testkit --lib --no-run`.
 */
export class MockSupabase {
  private constructor(
    readonly url: string,
    private readonly child: ChildProcess,
  ) {}

  static async start(): Promise<MockSupabase> {
    const port = await freePort();
    const child = spawn(
      'cargo',
      [
        'test',
        '-q',
        '-p',
        'mrl-testkit',
        '--lib',
        'supabase::tests::serve_for_manual_demo',
        '--',
        '--ignored',
        '--exact',
        '--nocapture',
      ],
      {
        cwd: repo,
        env: { ...process.env, MRL_MOCK_SUPABASE_PORT: String(port) },
        stdio: ['ignore', 'pipe', 'inherit'],
        // Its own process group, so stopping it also stops the test binary cargo started.
        detached: true,
      },
    );
    const stdout = child.stdout;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error('the mock Supabase did not start in 5 minutes'));
      }, 300_000);
      child.once('exit', (code) => {
        clearTimeout(timer);
        reject(new Error(`the mock Supabase stopped (${String(code)})`));
      });
      createInterface({ input: stdout }).on('line', (line) => {
        if (line.includes('mock supabase:')) {
          clearTimeout(timer);
          resolve();
        }
      });
    });
    return new MockSupabase(`http://127.0.0.1:${String(port)}`, child);
  }

  /** Stops it: from now on the app is offline as far as sign-in and sync go. */
  stop(): void {
    if (this.child.pid !== undefined && this.child.exitCode === null) {
      try {
        process.kill(-this.child.pid, 'SIGKILL');
      } catch {
        // Already gone.
      }
    }
  }
}

/** One install of the app: its own home, app data, core settings and agent settings. */
export class Computer {
  readonly home: string;

  constructor(
    readonly name: string,
    supabaseUrl: string,
  ) {
    this.home = mkdtempSync(path.join(tmpdir(), `mrl-e2e-${name}-`));
    mkdirSync(path.join(this.home, 'user-data'), { recursive: true });
    writeFileSync(
      path.join(this.home, 'core.toml'),
      [
        '[auth]',
        `supabase_url = "${supabaseUrl}"`,
        'supabase_anon_key = "mock-anon-key"',
        `keychain_service = "masterrl-e2e-${path.basename(this.home)}"`,
        '',
        '[keychain]',
        'backend = "file"',
        '',
        '[pgbundle]',
        `data_dir = ${JSON.stringify(path.join(this.home, 'pg'))}`,
        '',
      ].join('\n'),
    );
  }

  /** Starts the app and waits for its window. */
  async launch(env: Record<string, string> = {}): Promise<Desktop> {
    const started = Date.now();
    const browserLog = path.join(this.home, `browser-${String(started)}.log`);
    // A host that is itself Electron may set this; the app must not run as plain Node.
    const inherited = Object.fromEntries(
      Object.entries(process.env).filter(
        (e): e is [string, string] => e[1] !== undefined && e[0] !== 'ELECTRON_RUN_AS_NODE',
      ),
    );
    const app = await _electron.launch({
      args: [desktopDir],
      cwd: desktopDir,
      timeout: 60_000,
      env: {
        ...inherited,
        HOME: this.home,
        XDG_DATA_HOME: path.join(this.home, '.local/share'),
        XDG_CONFIG_HOME: path.join(this.home, '.config'),
        MRL_USER_DATA: path.join(this.home, 'user-data'),
        MRL_AGENT_CONFIG: path.join(this.home, 'agent.toml'),
        MRL_CORE_CONFIG: path.join(this.home, 'core.toml'),
        MRL_CORE_BIN: path.join(
          repo,
          'target/debug',
          process.platform === 'win32' ? 'mrl-server.exe' : 'mrl-server',
        ),
        MRL_NO_PROTOCOL_REGISTER: '1',
        MRL_BROWSER_LOG: browserLog,
        ...env,
      },
    });
    // What main and the core print, for when a test fails.
    const log = createWriteStream(path.join(this.home, `app-${String(started)}.log`));
    app.process().stdout?.pipe(log);
    app.process().stderr?.pipe(log);
    const page = await app.firstWindow();
    return new Desktop(app, page, started, browserLog);
  }
}

interface Proc {
  pid: number;
  ppid: number;
  rssKb: number;
  args: string;
}

function processes(): Proc[] {
  const out = execFileSync('ps', ['-A', '-o', 'pid=,ppid=,rss=,args='], { encoding: 'utf8' });
  return out
    .split('\n')
    .map((line) => /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(.*)$/.exec(line))
    .filter((m): m is RegExpExecArray => m !== null)
    .map((m) => ({
      pid: Number(m[1]),
      ppid: Number(m[2]),
      rssKb: Number(m[3]),
      args: m[4] ?? '',
    }));
}

function subtree(all: Proc[], root: number): Proc[] {
  const out: Proc[] = [];
  const queue = [root];
  while (queue.length > 0) {
    const pid = queue.shift();
    for (const p of all) {
      if (p.pid === pid) out.push(p);
      if (p.ppid === pid) queue.push(p.pid);
    }
  }
  return out;
}

/**
 * Memory the processes use, in MB, the way the OS reports it to people: macOS "Memory" in
 * Activity Monitor (`footprint`), Linux PSS (shared pages split between their users). Summed
 * RSS would count Chromium's shared framework pages and PostgreSQL's shared buffers once per
 * process. Elsewhere RSS is the fallback.
 */
function memoryMb(procs: Proc[]): number {
  if (procs.length === 0) return 0;
  if (process.platform === 'darwin') {
    const out = execFileSync(
      'footprint',
      ['-f', 'bytes', ...procs.flatMap((p) => ['-p', String(p.pid)])],
      { encoding: 'utf8' },
    );
    const total =
      /Summary Footprint: (\d+) B/.exec(out)?.[1] ?? /Footprint: (\d+) B/.exec(out)?.[1];
    if (total) return Math.round(Number(total) / 2 ** 20);
  }
  if (process.platform === 'linux') {
    let kb = 0;
    for (const p of procs) {
      try {
        const rollup = readFileSync(`/proc/${String(p.pid)}/smaps_rollup`, 'utf8');
        kb += Number(/^Pss:\s+(\d+) kB/m.exec(rollup)?.[1] ?? p.rssKb);
      } catch {
        kb += p.rssKb;
      }
    }
    return Math.round(kb / 1024);
  }
  return Math.round(procs.reduce((n, p) => n + p.rssKb, 0) / 1024);
}

/** The running app. */
export class Desktop {
  constructor(
    readonly app: ElectronApplication,
    readonly page: Page,
    readonly startedAt: number,
    private readonly browserLog: string,
  ) {}

  /** Addresses the app opened in the "browser" (`MRL_BROWSER_LOG`). */
  opened(): string[] {
    try {
      return readFileSync(this.browserLog, 'utf8').split('\n').filter(Boolean);
    } catch {
      return [];
    }
  }

  /** Waits until the core answers (the supervisor says `ready`). */
  async coreReady(ms = 180_000): Promise<void> {
    await until(
      'the core',
      async () => {
        const phase = await this.page.evaluate(
          async () => (await (window as unknown as { mrl: Bridge }).mrl.core.status()).phase,
        );
        return phase === 'ready' ? true : undefined;
      },
      ms,
    );
  }

  /** Sends a `masterrl://` link to the app, as the OS does. */
  async deepLink(link: string): Promise<void> {
    await this.app.evaluate(({ app }, url) => {
      app.emit('open-url', { preventDefault: () => undefined }, url);
    }, link);
  }

  /**
   * Plays the person in the browser: opens the sign-in page the app asked for (the mock
   * approves at once) and follows it back to the app, by deep link or loopback page.
   */
  async approveSignIn(): Promise<void> {
    const before = this.opened().length;
    const authorize = await until('the sign-in page', () => {
      const urls = this.opened();
      return urls.length > before ? urls[urls.length - 1] : undefined;
    }).catch(async (e: unknown) => {
      // Say what the window shows instead (an error from the core, usually).
      const shown = await this.page.locator('main').innerText();
      throw new Error(`${String(e)}; the window shows: ${shown}`);
    });
    const answer = await fetch(authorize, { redirect: 'manual' });
    const back = answer.headers.get('location');
    if (!back) throw new Error(`sign-in did not redirect (${String(answer.status)})`);
    if (back.startsWith('masterrl://')) await this.deepLink(back);
    else {
      const page = await fetch(back);
      if (!page.ok) throw new Error(`the loopback page answered ${String(page.status)}`);
    }
  }

  /**
   * Memory of the app's own processes and of the core with its PostgreSQL, in MB, and the
   * plain RSS sum of all of them (reported, not budgeted).
   */
  memory(): { shellMb: number; coreMb: number; rssMb: number; corePid: number | undefined } {
    const all = processes();
    const pid = this.app.process().pid;
    const tree = pid === undefined ? [] : subtree(all, pid);
    const core = tree.find((p) => /mrl-server(\.exe)?\s+--mode\s+solo/.test(p.args));
    const coreTree = core ? subtree(all, core.pid) : [];
    const coreIds = new Set(coreTree.map((p) => p.pid));
    return {
      shellMb: memoryMb(tree.filter((p) => !coreIds.has(p.pid))),
      coreMb: memoryMb(coreTree),
      rssMb: Math.round(tree.reduce((n, p) => n + p.rssKb, 0) / 1024),
      corePid: core?.pid,
    };
  }

  /** Quits the app the way the person does (the core stops first, then PostgreSQL). */
  async quit(): Promise<void> {
    await this.app.close();
  }
}
