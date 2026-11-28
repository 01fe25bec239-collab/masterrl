import { spawn as nodeSpawn, type ChildProcess } from 'node:child_process';
import { createInterface } from 'node:readline';
import * as z from 'zod/mini';
import type { CoreStatus } from '../ipc.registry';

/** Header the Solo core checks on every request (P07). */
export const LAUNCH_SECRET_HEADER = 'x-mrl-launch-secret';

/** The program to start, with its arguments and environment. */
export interface CoreLaunch {
  program: string;
  args: string[];
  env: NodeJS.ProcessEnv;
}

/** Where a ready core listens. */
export interface CoreEndpoint {
  port: number;
  grpcPort?: number;
}

export interface SupervisorOptions {
  /** Called before every start, so settings changes apply on the next start. */
  launch: () => CoreLaunch;
  /** The per-launch secret (also in `launch().env.MRL_LAUNCH_SECRET`). */
  secret: string;
  spawn?: typeof nodeSpawn;
  fetch?: typeof globalThis.fetch;
  /** Wait before start number `n` after a crash (n ≥ 1). Default 1 s, doubling, at most 30 s. */
  backoffMs?: (n: number) => number;
  /** Health check period once ready. */
  healthEveryMs?: number;
  /** How long one health check may take. */
  healthTimeoutMs?: number;
  /** Failed health checks in a row before the core counts as hung and is restarted. */
  healthFailures?: number;
  /** How long a start may take until the core reports its port and answers. */
  startTimeoutMs?: number;
  /** How long a stop waits before the process is killed. */
  stopTimeoutMs?: number;
  /** More crashes than this within `crashWindowMs` stop the retries (`failed`). */
  maxCrashes?: number;
  crashWindowMs?: number;
  /** A start finished: the core answers on `endpoint`. */
  onReady?: (endpoint: CoreEndpoint) => void;
  /** The core went away (crash, hang or stop). */
  onLost?: () => void;
  log?: (message: string) => void;
}

/** Process groups: everything the core starts can be found and stopped (not on Windows). */
const GROUPS = process.platform !== 'win32';

function groupAlive(pid: number): boolean {
  try {
    process.kill(-pid, 0);
    return true;
  } catch {
    // ESRCH: the group is gone. EPERM: the id belongs to someone else now, not ours to stop.
    return false;
  }
}

function signalGroup(pid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(-pid, signal);
  } catch {
    // The group is gone already.
  }
}

const listeningLine = z.object({
  event: z.literal('listening'),
  port: z.number().check(z.int(), z.gte(1), z.lte(65535)),
  grpc_port: z.optional(z.number().check(z.int(), z.gte(1), z.lte(65535))),
});

/** First error code in the core's last words, like `MRL-ENV-110`. */
function codeIn(lines: readonly string[]): string | undefined {
  for (const line of [...lines].reverse()) {
    const m = /MRL-[A-Z]+-\d{3}/.exec(line);
    if (m) return m[0];
  }
  return undefined;
}

/**
 * Runs the bundled core (`mrl-server --mode solo`) and keeps it running.
 *
 * - Start: spawn with the launch secret, wait for `{"event":"listening","port":N}` on stdout,
 *   then for `/healthz` to answer. Then `ready`.
 * - Crash or hang (health fails `healthFailures` times in a row): `reconnecting`, start again
 *   after a backoff. More than `maxCrashes` within `crashWindowMs`: `failed` until `restart()`.
 * - Stop: SIGTERM; the core stops taking work, finishes requests, then stops its PostgreSQL.
 *   After `stopTimeoutMs` the process is killed. Agents and training are separate user
 *   services and keep running.
 * - Leftovers: on Linux and macOS the core leads its own process group. Whenever it exits
 *   (stop, crash, hang, or a SIGTERM that came before it could handle one), what is left in
 *   the group — its PostgreSQL — gets SIGINT (PostgreSQL's fast shutdown), then SIGKILL after
 *   `stopTimeoutMs`. The next core starts only after that, so it finds the database free.
 */
export class CoreSupervisor {
  private state: CoreStatus = { version: 0, phase: 'stopped', restarts: 0 };
  private child: ChildProcess | null = null;
  private generation = 0;
  private endpointNow: CoreEndpoint | null = null;
  private crashes: number[] = [];
  private timers = new Set<NodeJS.Timeout>();
  private waiters = new Set<() => void>();
  private stderrTail: string[] = [];
  /** `stop()` was asked for: nothing starts the core again but `start()`. */
  private halted = false;
  /** Leftovers of exited cores still being stopped (see the class docs). */
  private sweeps = new Set<Promise<void>>();
  private readonly o: Required<
    Omit<SupervisorOptions, 'onReady' | 'onLost' | 'log' | 'launch' | 'secret'>
  > &
    SupervisorOptions;

  constructor(options: SupervisorOptions) {
    this.o = {
      spawn: nodeSpawn,
      fetch: (input, init) => globalThis.fetch(input, init),
      backoffMs: (n) => Math.min(30_000, 1000 * 2 ** (n - 1)),
      healthEveryMs: 5000,
      healthTimeoutMs: 2000,
      healthFailures: 3,
      startTimeoutMs: 120_000,
      stopTimeoutMs: 45_000,
      maxCrashes: 6,
      crashWindowMs: 10 * 60_000,
      ...options,
    };
  }

  /** The current state. */
  status(): CoreStatus {
    return this.state;
  }

  /** Where the core listens, while ready. */
  endpoint(): CoreEndpoint | null {
    return this.state.phase === 'ready' ? this.endpointNow : null;
  }

  /** Resolves once the version passes `seen`, or after `timeoutMs` with the current state. */
  watch(seen: number, timeoutMs = 25_000): Promise<CoreStatus> {
    if (this.state.version > seen) return Promise.resolve(this.state);
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.waiters.delete(done);
        resolve(this.state);
      };
      const timer = setTimeout(done, timeoutMs);
      this.waiters.add(done);
    });
  }

  /** Starts the core unless it is starting or running. */
  start(): void {
    if (['starting', 'ready', 'stopping'].includes(this.state.phase)) return;
    this.halted = false;
    this.launchNow();
  }

  /**
   * Starts the core again and forgets earlier crashes: right away while it waits to reconnect
   * or after `failed`; after a clean stop while it runs (e.g. after a backup was restored).
   * Does nothing once `stop()` was asked for (the app is quitting).
   */
  async restart(): Promise<CoreStatus> {
    const quitting = () => this.halted || this.phase() === 'stopping';
    if (quitting()) return this.state;
    if (this.phase() === 'ready' || this.phase() === 'starting') await this.stopChild();
    await this.swept();
    // Read again: `stop()` may have been asked for while the old core stopped.
    if (quitting()) return this.state;
    this.crashes = [];
    this.launchNow();
    return this.state;
  }

  /** Asks the core to stop, then kills it if it has not exited in time. It stays stopped. */
  async stop(): Promise<void> {
    this.halted = true;
    await this.stopChild();
    await this.swept();
  }

  /** Resolves when the leftovers of every exited core are stopped. */
  private async swept(): Promise<void> {
    while (this.sweeps.size > 0) await Promise.all([...this.sweeps]);
  }

  /** Stops what is left in the process group of the core `pid`, which has exited. */
  private sweep(pid: number | undefined): void {
    if (!GROUPS || pid === undefined) return;
    const done = (async () => {
      signalGroup(pid, 'SIGINT');
      const end = Date.now() + this.o.stopTimeoutMs;
      while (groupAlive(pid)) {
        if (Date.now() > end) {
          this.o.log?.('what the core started did not stop in time; ending it');
          signalGroup(pid, 'SIGKILL');
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    })();
    this.sweeps.add(done);
    void done.finally(() => this.sweeps.delete(done));
  }

  private async stopChild(): Promise<void> {
    this.clearTimers();
    const child = this.child;
    if (!child || child.exitCode !== null || child.signalCode !== null) {
      this.child = null;
      this.set({ phase: 'stopped', reason: undefined, retryAt: undefined });
      return;
    }
    this.set({ phase: 'stopping', reason: undefined, retryAt: undefined });
    await new Promise<void>((resolve) => {
      const kill = setTimeout(() => {
        this.o.log?.('the core did not stop in time; ending it');
        child.kill('SIGKILL');
      }, this.o.stopTimeoutMs);
      child.once('exit', () => {
        clearTimeout(kill);
        resolve();
      });
      child.kill('SIGTERM');
    });
  }

  /** The phase now (a method, so checks after an `await` read it again). */
  private phase(): CoreStatus['phase'] {
    return this.state.phase;
  }

  private set(patch: Partial<Omit<CoreStatus, 'version'>>): void {
    this.state = { ...this.state, ...patch, version: this.state.version + 1 };
    for (const w of [...this.waiters]) w();
  }

  private later(ms: number, f: () => void): void {
    const t = setTimeout(() => {
      this.timers.delete(t);
      f();
    }, ms);
    this.timers.add(t);
  }

  private clearTimers(): void {
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
  }

  private launchNow(): void {
    this.clearTimers();
    const generation = ++this.generation;
    const current = () => generation === this.generation;
    this.endpointNow = null;
    this.stderrTail = [];
    this.set({ phase: 'starting', reason: undefined, retryAt: undefined });
    let child: ChildProcess;
    try {
      const { program, args, env } = this.o.launch();
      child = this.o.spawn(program, args, {
        env,
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: GROUPS,
      });
    } catch (e) {
      this.crashed(generation, `The core program could not start (${String(e)}).`);
      return;
    }
    this.child = child;
    child.on('error', (e) => {
      if (current()) this.crashed(generation, `The core program could not start (${e.message}).`);
    });
    child.on('exit', (code, signal) => {
      this.sweep(child.pid);
      if (!current()) return;
      this.child = null;
      if (this.state.phase === 'stopping') {
        this.set({ phase: 'stopped' });
        return;
      }
      const why = codeIn(this.stderrTail);
      const how = signal ? `was ended (${signal})` : `stopped with exit code ${String(code)}`;
      this.crashed(generation, `The core ${how}${why ? ` (${why})` : ''}.`);
    });
    if (child.stderr) {
      createInterface({ input: child.stderr }).on('line', (line) => {
        this.stderrTail = [...this.stderrTail.slice(-19), line];
      });
    }
    if (child.stdout) {
      createInterface({ input: child.stdout }).on('line', (line) => {
        if (!current() || this.endpointNow) return;
        let parsed: unknown;
        try {
          parsed = JSON.parse(line);
        } catch {
          return;
        }
        const r = listeningLine.safeParse(parsed);
        if (!r.success) return;
        this.endpointNow = { port: r.data.port, grpcPort: r.data.grpc_port };
        void this.awaitHealthy(generation);
      });
    }
    this.later(this.o.startTimeoutMs, () => {
      if (current() && this.state.phase === 'starting') {
        this.hung(generation, 'The core took too long to start.');
      }
    });
  }

  private async healthy(): Promise<boolean> {
    const ep = this.endpointNow;
    if (!ep) return false;
    try {
      const r = await this.o.fetch(`http://127.0.0.1:${String(ep.port)}/healthz`, {
        headers: { [LAUNCH_SECRET_HEADER]: this.o.secret },
        signal: AbortSignal.timeout(this.o.healthTimeoutMs),
      });
      await r.body?.cancel();
      return r.ok;
    } catch {
      return false;
    }
  }

  private async awaitHealthy(generation: number): Promise<void> {
    while (generation === this.generation && this.state.phase === 'starting') {
      if (await this.healthy()) {
        if (generation !== this.generation || this.phase() !== 'starting') return;
        this.set({ phase: 'ready' });
        const ep = this.endpointNow;
        if (ep) this.o.onReady?.(ep);
        this.monitor(generation, 0);
        return;
      }
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  private monitor(generation: number, failures: number): void {
    this.later(this.o.healthEveryMs, () => {
      if (generation !== this.generation || this.state.phase !== 'ready') return;
      void this.healthy().then((ok) => {
        if (generation !== this.generation || this.state.phase !== 'ready') return;
        const n = ok ? 0 : failures + 1;
        if (n >= this.o.healthFailures) {
          this.hung(generation, 'The core stopped answering.');
        } else {
          this.monitor(generation, n);
        }
      });
    });
  }

  /** A hung core is ended; its exit then counts as a crash with `reason`. */
  private hung(generation: number, reason: string): void {
    const child = this.child;
    this.child = null;
    child?.removeAllListeners('exit');
    child?.kill('SIGKILL');
    this.sweep(child?.pid);
    this.crashed(generation, reason);
  }

  private crashed(generation: number, reason: string): void {
    if (generation !== this.generation) return;
    this.generation++;
    this.clearTimers();
    this.endpointNow = null;
    this.o.onLost?.();
    this.o.log?.(reason);
    const now = Date.now();
    this.crashes = [...this.crashes.filter((t) => now - t < this.o.crashWindowMs), now];
    if (this.crashes.length > this.o.maxCrashes) {
      this.set({
        phase: 'failed',
        reason: `${reason} It stopped ${String(this.crashes.length)} times in a few minutes.`,
        retryAt: undefined,
      });
      return;
    }
    const wait = this.o.backoffMs(this.crashes.length);
    this.set({
      phase: 'reconnecting',
      reason,
      retryAt: now + wait,
      restarts: this.state.restarts + 1,
    });
    this.later(wait, () => {
      // The crashed core's database must be stopped before the next core opens it.
      void this.swept().then(() => {
        if (this.state.phase === 'reconnecting') this.launchNow();
      });
    });
  }
}
