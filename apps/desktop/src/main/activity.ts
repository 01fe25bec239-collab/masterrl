import type { CoreClient } from './core/client';

/** A job on or getting a machine (`GET /desktop/activity` → `running`). */
export interface RunningJob {
  job_id: string;
  run_id?: string | null;
  name: string;
  state: string;
}

/** A job that finished, failed, was cancelled or paused. */
export interface JobChange extends RunningJob {
  state_version: number;
  at: string;
}

interface Activity {
  cursor: string;
  training_active: boolean;
  running: RunningJob[];
  changes: JobChange[];
}

export interface ActivityOptions {
  core: CoreClient;
  everyMs?: number;
  /** The jobs to show in the tray. */
  onRunning: (jobs: RunningJob[]) => void;
  /** A change to tell the person about (each `(job, state_version)` once). */
  onChange: (change: JobChange) => void;
  /** Training on this core started or stopped. */
  onTraining: (active: boolean) => void;
}

/** How many shown changes are remembered (enough for the core's overlap window). */
const REMEMBER = 500;

/**
 * Polls the core's activity while it is ready. The first answer only marks recent changes as
 * seen; later ones report each change once, even when the core repeats it (overlap window,
 * replays after a reconnect).
 */
export class ActivityMonitor {
  private cursor: string | undefined;
  private seen = new Set<string>();
  private primed = false;
  private timer: NodeJS.Timeout | null = null;
  private busy = false;
  private training = false;

  constructor(private readonly o: ActivityOptions) {}

  start(): void {
    if (this.timer) return;
    void this.tick();
    this.timer = setInterval(() => void this.tick(), this.o.everyMs ?? 5000);
  }

  /** Stops polling (the core went away). Training counts as stopped until the next answer. */
  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.setTraining(false);
    this.o.onRunning([]);
  }

  private setTraining(active: boolean): void {
    if (active === this.training) return;
    this.training = active;
    this.o.onTraining(active);
  }

  /** One poll. Public for tests. */
  async tick(): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    try {
      const q = this.cursor ? `?since=${encodeURIComponent(this.cursor)}` : '';
      const r = await this.o.core.call<Activity>('GET', `/api/v1/desktop/activity${q}`);
      if (!r.ok) return;
      const a = r.value;
      this.cursor = a.cursor;
      this.setTraining(a.training_active);
      this.o.onRunning(a.running);
      for (const c of a.changes) {
        const key = `${c.job_id}:${String(c.state_version)}`;
        if (this.seen.has(key)) continue;
        this.seen.add(key);
        if (this.primed) this.o.onChange(c);
      }
      while (this.seen.size > REMEMBER) {
        const oldest = this.seen.values().next().value;
        if (oldest === undefined) break;
        this.seen.delete(oldest);
      }
      this.primed = true;
    } finally {
      this.busy = false;
    }
  }
}

/** Notification text for a change, in plain words. */
export function describeChange(c: JobChange): { title: string; body: string } {
  switch (c.state) {
    case 'COMPLETED':
      return { title: 'Training finished', body: `${c.name} finished.` };
    case 'FAILED':
      return {
        title: 'Training stopped with an error',
        body: `${c.name} failed. Open MasterRL to see why.`,
      };
    case 'PAUSED':
      return { title: 'Training paused', body: `${c.name} is paused.` };
    default:
      return { title: 'Training cancelled', body: `${c.name} was cancelled.` };
  }
}
