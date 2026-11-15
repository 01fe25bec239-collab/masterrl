import { cn } from '../lib/cn';
import { Icon, type IconName } from './Icon';

type Tone = 'new' | 'wait' | 'progress' | 'ok' | 'paused' | 'stopped' | 'bad' | 'archived';

const tones: Record<Tone, { icon: IconName; className: string }> = {
  new: { icon: 'circle', className: 'text-fg-muted' },
  wait: { icon: 'clock', className: 'text-fg-muted' },
  progress: { icon: 'progress', className: 'text-accent' },
  ok: { icon: 'check', className: 'text-fg' },
  paused: { icon: 'pause', className: 'text-fg' },
  stopped: { icon: 'ban', className: 'text-fg-muted' },
  bad: { icon: 'alert', className: 'text-danger' },
  archived: { icon: 'archive', className: 'text-fg-muted' },
};

type Machine = Record<string, readonly [label: string, tone: Tone]>;

/**
 * Every state of every canonical state machine (baseline §6, Doc 09 §4, Doc 06 PolicyVersion),
 * with a plain-language label. The keys are the exact API values.
 */
export const stateMachines = {
  job: {
    CREATED: ['Created', 'new'],
    VALIDATING: ['Checking settings', 'progress'],
    REJECTED: ['Rejected', 'bad'],
    QUEUED: ['Waiting in queue', 'wait'],
    ALLOCATING: ['Reserving hardware', 'progress'],
    PREPARING: ['Getting ready', 'progress'],
    RUNNING: ['Running', 'progress'],
    FINALIZING: ['Finishing up', 'progress'],
    COMPLETED: ['Completed', 'ok'],
    PAUSING: ['Pausing', 'progress'],
    PAUSED: ['Paused', 'paused'],
    RESUMING: ['Resuming', 'progress'],
    RETRY_WAIT: ['Waiting to retry', 'wait'],
    CANCELLING: ['Cancelling', 'progress'],
    CANCELLED: ['Cancelled', 'stopped'],
    FAILED: ['Failed', 'bad'],
  },
  jobAttempt: {
    PENDING: ['Pending', 'wait'],
    LAUNCHING: ['Launching', 'progress'],
    RUNNING: ['Running', 'progress'],
    EXITED_OK: ['Finished', 'ok'],
    EXITED_ERROR: ['Exited with an error', 'bad'],
    KILLED: ['Stopped', 'stopped'],
    LOST: ['Lost contact', 'bad'],
  },
  checkpoint: {
    WRITING: ['Writing', 'progress'],
    UPLOADED: ['Uploaded', 'progress'],
    VERIFYING: ['Verifying', 'progress'],
    COMMITTED: ['Saved', 'ok'],
    CORRUPT: ['Damaged', 'bad'],
    TOMBSTONED: ['Removed', 'archived'],
  },
  node: {
    ENROLLING: ['Joining', 'progress'],
    ONLINE: ['Online', 'ok'],
    DEGRADED: ['Having problems', 'bad'],
    OFFLINE: ['Offline', 'stopped'],
    DRAINING: ['Finishing its work', 'progress'],
    DECOMMISSIONED: ['Retired', 'archived'],
  },
  deploymentRevision: {
    PENDING: ['Pending', 'wait'],
    STARTING: ['Starting', 'progress'],
    HEALTHY: ['Healthy', 'ok'],
    DEGRADED: ['Having problems', 'bad'],
    DRAINING: ['Finishing requests', 'progress'],
    STOPPED: ['Stopped', 'stopped'],
    FAILED: ['Failed', 'bad'],
    ROLLED_BACK: ['Rolled back', 'archived'],
  },
  policyVersion: {
    PROPOSED: ['Proposed', 'new'],
    TRANSFERRING: ['Sending weights', 'progress'],
    ACKED: ['Received', 'ok'],
    ACTIVE: ['In use', 'ok'],
    SUPERSEDED: ['Replaced', 'archived'],
    ABORTED: ['Aborted', 'bad'],
    ORPHANED: ['Discarded', 'archived'],
  },
  reservation: {
    HELD: ['Held', 'wait'],
    ACTIVE: ['In use', 'progress'],
    RELEASED: ['Released', 'stopped'],
    QUARANTINED: ['Quarantined', 'bad'],
  },
  modelVersionStage: {
    EXPERIMENTAL: ['Experimental', 'new'],
    CANDIDATE: ['Candidate', 'wait'],
    APPROVED: ['Approved', 'ok'],
    ARCHIVED: ['Archived', 'archived'],
  },
  datasetVersion: {
    INGESTING: ['Importing', 'progress'],
    VALIDATING: ['Checking', 'progress'],
    READY: ['Ready', 'ok'],
    INVALID: ['Has problems', 'bad'],
    ARCHIVED: ['Archived', 'archived'],
  },
  rewardVersion: {
    DRAFT: ['Draft', 'new'],
    TESTING: ['Testing', 'progress'],
    PUBLISHED: ['Published', 'ok'],
    FAILED_TESTS: ['Tests failed', 'bad'],
    DEPRECATED: ['Retired', 'archived'],
  },
  engineVersion: {
    PENDING_APPROVAL: ['Waiting for approval', 'wait'],
    PROBING: ['Testing on hardware', 'progress'],
    ENABLED: ['Enabled', 'ok'],
    DISABLED: ['Disabled', 'stopped'],
    REVOKED: ['Revoked', 'bad'],
  },
  feedbackItem: {
    PENDING: ['To do', 'wait'],
    IN_PROGRESS: ['In progress', 'progress'],
    DONE: ['Done', 'ok'],
    DISPUTED: ['Disputed', 'bad'],
    RESOLVED: ['Resolved', 'ok'],
  },
} as const satisfies Record<string, Machine>;

export type StateMachine = keyof typeof stateMachines;
export type StateOf<M extends StateMachine> = keyof (typeof stateMachines)[M] & string;

export interface StateChipProps<M extends StateMachine> {
  machine: M;
  state: StateOf<M>;
  className?: string;
}

/** Plain label for a state, e.g. `stateLabel('job', 'RETRY_WAIT')` → "Waiting to retry". */
export function stateLabel<M extends StateMachine>(machine: M, state: StateOf<M>): string {
  const table: Machine = stateMachines[machine];
  return table[state]?.[0] ?? state;
}

/** State shown with an icon and text, never color alone. */
export function StateChip<M extends StateMachine>({
  machine,
  state,
  className,
}: StateChipProps<M>) {
  const table: Machine = stateMachines[machine];
  const [label, tone] = table[state] ?? [state, 'new'];
  const style = tones[tone];
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 rounded-full border border-border bg-surface px-2 py-0.5 text-xs font-medium',
        style.className,
        className,
      )}
      data-state={state}
      title={state}
    >
      <Icon name={style.icon} className="size-3.5" />
      {label}
    </span>
  );
}
