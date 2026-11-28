/** The part of Electron's `powerSaveBlocker` this uses. */
export interface Blocker {
  start(type: 'prevent-app-suspension'): number;
  stop(id: number): void;
}

/** Why the computer is kept awake: training on it right now, or the renderer asked. */
export type SleepReason = 'training' | 'manual';

/**
 * Keeps the computer from sleeping while any reason holds (`desktop:blockSleep` and the core's
 * "local training active"). One blocker for all reasons; P37 adds the policy for when.
 */
export class SleepGuard {
  private reasons = new Set<SleepReason>();
  private id: number | null = null;

  constructor(private readonly blocker: Blocker) {}

  set(reason: SleepReason, on: boolean): boolean {
    if (on) this.reasons.add(reason);
    else this.reasons.delete(reason);
    if (this.reasons.size > 0 && this.id === null) {
      this.id = this.blocker.start('prevent-app-suspension');
    } else if (this.reasons.size === 0 && this.id !== null) {
      this.blocker.stop(this.id);
      this.id = null;
    }
    return this.blocking();
  }

  blocking(): boolean {
    return this.id !== null;
  }

  /** Which reasons hold now, for the renderer to show after a reload. */
  state(): { manual: boolean; training: boolean; awake: boolean } {
    return {
      manual: this.reasons.has('manual'),
      training: this.reasons.has('training'),
      awake: this.blocking(),
    };
  }
}
