import type { CoreClient } from './core/client';
import type { UpdateStatus } from './ipc.registry';

/** The part of electron-updater's `autoUpdater` this uses. */
export interface Updater {
  autoDownload: boolean;
  autoInstallOnAppQuit: boolean;
  checkForUpdates(): Promise<{ updateInfo: { version: string } } | null>;
  downloadUpdate(): Promise<unknown>;
  quitAndInstall(): void;
  on(event: 'download-progress', listener: (p: { percent: number }) => void): unknown;
}

interface Readiness {
  ready: boolean;
  checkpoints: { name: string; outcome: string }[];
  backup_error?: string | null;
}

export interface UpdatesOptions {
  /** Loads electron-updater only when first needed (keeps app start light). */
  updater: () => Promise<Updater>;
  core: CoreClient;
  /** The running version. */
  current: string;
}

/**
 * Update plumbing over electron-updater (GitHub Releases; a local feed in tests). Nothing is
 * downloaded or installed without the person asking. Before installing, the core saves
 * emergency checkpoints of running jobs and backs up the database; only then does the app
 * restart into the new version (its quit stops the core first, then PostgreSQL).
 */
export class Updates {
  private state: UpdateStatus = { phase: 'idle' };
  private loaded: Promise<Updater> | null = null;

  constructor(private readonly o: UpdatesOptions) {}

  status(): UpdateStatus {
    return this.state;
  }

  private updater(): Promise<Updater> {
    this.loaded ??= this.o.updater().then((u) => {
      u.autoDownload = false;
      u.autoInstallOnAppQuit = false;
      u.on('download-progress', (p) => {
        this.state = { ...this.state, phase: 'downloading', percent: Math.round(p.percent) };
      });
      return u;
    });
    return this.loaded;
  }

  async check(): Promise<UpdateStatus> {
    if (['checking', 'downloading', 'preparing'].includes(this.state.phase)) return this.state;
    this.state = { phase: 'checking' };
    try {
      const u = await this.updater();
      const found = await u.checkForUpdates();
      const version = found?.updateInfo.version;
      this.state =
        version && version !== this.o.current
          ? { phase: 'available', version }
          : { phase: 'up_to_date' };
    } catch {
      this.state = {
        phase: 'error',
        detail: 'The app could not look for updates. Check your internet connection.',
      };
    }
    return this.state;
  }

  /** Downloads the update, prepares the core, then restarts into the new version. */
  async install(): Promise<UpdateStatus> {
    if (this.state.phase !== 'available' && this.state.phase !== 'ready') return this.state;
    const version = this.state.version;
    try {
      const u = await this.updater();
      if (this.state.phase === 'available') {
        this.state = { phase: 'downloading', version, percent: 0 };
        await u.downloadUpdate();
      }
      this.state = { phase: 'preparing', version };
      const r = await this.o.core.call<Readiness>(
        'POST',
        '/api/v1/desktop/update/prepare',
        undefined,
        15 * 60_000,
      );
      if (!r.ok) {
        this.state = { phase: 'ready', version, detail: r.failure.detail };
        return this.state;
      }
      if (!r.value.ready) {
        this.state = {
          phase: 'ready',
          version,
          detail: `The database backup failed, so the update waits: ${r.value.backup_error ?? 'unknown reason'}.`,
        };
        return this.state;
      }
      const late = r.value.checkpoints.filter((c) => c.outcome !== 'committed');
      this.state = {
        phase: 'ready',
        version,
        detail:
          late.length > 0
            ? `${String(late.length)} run(s) could not save a checkpoint now; they keep training and resume from their last one.`
            : undefined,
      };
      u.quitAndInstall();
    } catch {
      this.state = {
        phase: 'error',
        version,
        detail: 'The update could not be downloaded. Try again later.',
      };
    }
    return this.state;
  }
}
