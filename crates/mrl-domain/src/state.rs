//! Every lifecycle state machine, as data plus pure functions.
//!
//! Each machine is a unit struct implementing [`StateMachine`]: a states enum, an events
//! enum and a transition table. Each row of the table names the component that owns it.
//! Each event has a type name `<aggregate>.<past_tense_verb>`, used for outbox events.

use std::fmt;
use std::str::FromStr;

/// The component allowed to make a transition (the single writer for that step).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Owner {
    /// The jobs module: API handlers, the validator and agent-event handlers.
    Jobs,
    /// The scheduler (leader): placement, reservation leases, retry timers.
    Scheduler,
    /// The artifacts verifier; the jobs module applies its verdict.
    ArtifactsVerifier,
    /// The artifacts module (checkpoints and stored files).
    Artifacts,
    /// The node registry (enrollment, heartbeats, drain).
    Nodes,
    /// The deployments module.
    Deployments,
    /// The trainer-to-rollout weight sync coordinator.
    WeightSync,
    /// The models module (imports).
    Models,
    /// The promotions module (model stages).
    Promotions,
    /// The datasets module.
    Datasets,
    /// The rewards module.
    Rewards,
    /// The engine registry.
    Engines,
    /// The feedback module.
    Feedback,
}

impl Owner {
    /// Snake-case component name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Jobs => "jobs",
            Self::Scheduler => "scheduler",
            Self::ArtifactsVerifier => "artifacts_verifier",
            Self::Artifacts => "artifacts",
            Self::Nodes => "nodes",
            Self::Deployments => "deployments",
            Self::WeightSync => "weight_sync",
            Self::Models => "models",
            Self::Promotions => "promotions",
            Self::Datasets => "datasets",
            Self::Rewards => "rewards",
            Self::Engines => "engines",
            Self::Feedback => "feedback",
        }
    }
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One row of a transition table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition<S, E> {
    /// State before the event.
    pub from: S,
    /// The event.
    pub event: E,
    /// State after the event.
    pub to: S,
    /// The component allowed to make this step.
    pub owner: Owner,
}

/// Why a state change is not allowed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransitionError {
    /// The thing has already finished; its state never changes again.
    #[error("this {} is already {from} and can't change any more", aggregate.replace('_', " "))]
    Terminal {
        /// The aggregate, like `job`.
        aggregate: &'static str,
        /// The current (final) state.
        from: &'static str,
        /// The event type that was tried.
        event: &'static str,
    },
    /// The event does not apply in the current state.
    #[error("a {} that is {from} can't take the step `{event}`", aggregate.replace('_', " "))]
    NotAllowed {
        /// The aggregate, like `job`.
        aggregate: &'static str,
        /// The current state.
        from: &'static str,
        /// The event type that was tried.
        event: &'static str,
    },
}

/// A state name that no machine state uses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{name}` is not a {} state", aggregate.replace('_', " "))]
pub struct UnknownState {
    /// The aggregate, like `job`.
    pub aggregate: &'static str,
    /// The text that was given.
    pub name: String,
}

/// A lifecycle state machine described as data.
pub trait StateMachine {
    /// The states enum.
    type State: Copy + Eq + fmt::Debug + 'static;
    /// The events enum.
    type Event: Copy + Eq + fmt::Debug + 'static;

    /// Aggregate name used in event types, like `job`.
    const AGGREGATE: &'static str;
    /// Event type recorded when the thing is first created in [`Self::INITIAL`].
    const CREATED_EVENT: &'static str;
    /// The state a new thing starts in.
    const INITIAL: Self::State;
    /// Every state.
    const STATES: &'static [Self::State];
    /// Every event.
    const EVENTS: &'static [Self::Event];
    /// States that never change again.
    const TERMINAL: &'static [Self::State];
    /// Every allowed step.
    const TRANSITIONS: &'static [Transition<Self::State, Self::Event>];

    /// The wire name of a state, like `RUNNING`.
    fn state_name(state: Self::State) -> &'static str;

    /// The event type name, like `job.started`.
    fn event_type(event: Self::Event) -> &'static str;

    /// True if `state` never changes again.
    fn is_terminal(state: Self::State) -> bool {
        Self::TERMINAL.contains(&state)
    }

    /// The table row for `event` in state `from`, if the step is allowed.
    fn transition(
        from: Self::State,
        event: Self::Event,
    ) -> Option<&'static Transition<Self::State, Self::Event>> {
        Self::TRANSITIONS
            .iter()
            .find(|t| t.from == from && t.event == event)
    }

    /// The state after `event` in state `from`, or why the step is not allowed.
    fn can_transition(
        from: Self::State,
        event: Self::Event,
    ) -> Result<Self::State, TransitionError> {
        if let Some(t) = Self::transition(from, event) {
            return Ok(t.to);
        }
        let (aggregate, from_name, event) = (
            Self::AGGREGATE,
            Self::state_name(from),
            Self::event_type(event),
        );
        Err(if Self::is_terminal(from) {
            TransitionError::Terminal {
                aggregate,
                from: from_name,
                event,
            }
        } else {
            TransitionError::NotAllowed {
                aggregate,
                from: from_name,
                event,
            }
        })
    }
}

macro_rules! machine {
    (
        $(#[$meta:meta])*
        $machine:ident {
            aggregate: $aggregate:literal,
            created: $created:literal,
            initial: $initial:ident,
            state $state:ident { $($s:ident => $swire:literal),+ $(,)? }
            terminal [$($t:ident),* $(,)?]
            event $event:ident { $($e:ident => $etype:literal),+ $(,)? }
            transitions { $( [$($from:ident),+] + $ev:ident => $to:ident by $owner:ident ),+ $(,)? }
        }
    ) => {
        #[doc = concat!("States of a `", $aggregate, "` (see [`", stringify!($machine), "`]).")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub enum $state {
            $(
                #[doc = concat!("`", $swire, "`")]
                #[serde(rename = $swire)]
                $s,
            )+
        }

        impl $state {
            /// The wire name, like `RUNNING`.
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$s => $swire,)+ }
            }
        }

        impl fmt::Display for $state {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $state {
            type Err = UnknownState;

            fn from_str(s: &str) -> Result<Self, UnknownState> {
                match s {
                    $($swire => Ok(Self::$s),)+
                    _ => Err(UnknownState { aggregate: $aggregate, name: s.to_owned() }),
                }
            }
        }

        #[doc = concat!("Events of a `", $aggregate, "` (see [`", stringify!($machine), "`]).")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $event {
            $(
                #[doc = concat!("Recorded as `", $etype, "`.")]
                $e,
            )+
        }

        impl $event {
            /// The event type name, like `job.started`.
            pub const fn event_type(self) -> &'static str {
                match self { $(Self::$e => $etype,)+ }
            }
        }

        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $machine;

        impl StateMachine for $machine {
            type State = $state;
            type Event = $event;

            const AGGREGATE: &'static str = $aggregate;
            const CREATED_EVENT: &'static str = $created;
            const INITIAL: $state = $state::$initial;
            const STATES: &'static [$state] = &[$($state::$s),+];
            const EVENTS: &'static [$event] = &[$($event::$e),+];
            const TERMINAL: &'static [$state] = &[$($state::$t),*];
            const TRANSITIONS: &'static [Transition<$state, $event>] = &[
                $($(
                    Transition { from: $state::$from, event: $event::$ev, to: $state::$to, owner: Owner::$owner },
                )+)+
            ];

            fn state_name(state: $state) -> &'static str {
                state.as_str()
            }

            fn event_type(event: $event) -> &'static str {
                event.event_type()
            }
        }
    };
}

machine! {
    /// A scheduled job, from submission to a final state.
    Job {
        aggregate: "job",
        created: "job.created",
        initial: Created,
        state JobState {
            Created => "CREATED",
            Validating => "VALIDATING",
            Queued => "QUEUED",
            Allocating => "ALLOCATING",
            Preparing => "PREPARING",
            Running => "RUNNING",
            Finalizing => "FINALIZING",
            Pausing => "PAUSING",
            Paused => "PAUSED",
            Resuming => "RESUMING",
            RetryWait => "RETRY_WAIT",
            Cancelling => "CANCELLING",
            Completed => "COMPLETED",
            Failed => "FAILED",
            Cancelled => "CANCELLED",
            Rejected => "REJECTED",
        }
        terminal [Completed, Failed, Cancelled, Rejected]
        event JobEvent {
            Submit => "job.submitted",
            Accept => "job.queued",
            Reject => "job.rejected",
            Place => "job.placed",
            Launch => "job.launched",
            Requeue => "job.requeued",
            Start => "job.started",
            Finish => "job.finished",
            Complete => "job.completed",
            Interrupt => "job.retry_scheduled",
            Fail => "job.failed",
            RequestPause => "job.pause_requested",
            ConfirmPause => "job.paused",
            RefusePause => "job.pause_refused",
            Resume => "job.resumed",
            Retry => "job.retried",
            RequestCancel => "job.cancel_requested",
            ConfirmCancel => "job.cancelled",
        }
        transitions {
            [Created] + Submit => Validating by Jobs,
            [Validating] + Accept => Queued by Jobs,
            [Validating] + Reject => Rejected by Jobs,
            [Queued] + Place => Allocating by Scheduler,
            [Allocating] + Launch => Preparing by Scheduler,
            // Reservation lost before launch.
            [Allocating] + Requeue => Queued by Scheduler,
            [Preparing] + Start => Running by Jobs,
            [Running] + Finish => Finalizing by Jobs,
            [Finalizing] + Complete => Completed by ArtifactsVerifier,
            [Allocating, Preparing, Running, Finalizing] + Interrupt => RetryWait by Jobs,
            [Allocating, Preparing, Running, Finalizing, Pausing, RetryWait] + Fail => Failed by Jobs,
            [Running] + RequestPause => Pausing by Jobs,
            [Pausing] + ConfirmPause => Paused by Jobs,
            [Pausing] + RefusePause => Running by Jobs,
            [Paused] + Resume => Resuming by Jobs,
            [Resuming] + Requeue => Queued by Jobs,
            [RetryWait] + Retry => Queued by Scheduler,
            [Queued, Allocating, Preparing, Running, Pausing, Paused, Resuming, RetryWait, Finalizing]
                + RequestCancel => Cancelling by Jobs,
            [Cancelling] + ConfirmCancel => Cancelled by Jobs,
        }
    }
}

machine! {
    /// One attempt at running a job on its placement.
    JobAttempt {
        aggregate: "attempt",
        created: "attempt.created",
        initial: Pending,
        state JobAttemptState {
            Pending => "PENDING",
            Launching => "LAUNCHING",
            Running => "RUNNING",
            ExitedOk => "EXITED_OK",
            ExitedError => "EXITED_ERROR",
            Lost => "LOST",
            Killed => "KILLED",
        }
        terminal [ExitedOk, ExitedError, Lost, Killed]
        event JobAttemptEvent {
            Acknowledge => "attempt.acknowledged",
            Start => "attempt.started",
            ExitOk => "attempt.succeeded",
            ExitError => "attempt.errored",
            LoseLease => "attempt.lost",
            Kill => "attempt.killed",
        }
        transitions {
            [Pending] + Acknowledge => Launching by Jobs,
            [Launching] + Start => Running by Jobs,
            [Launching, Running] + ExitOk => ExitedOk by Jobs,
            [Launching, Running] + ExitError => ExitedError by Jobs,
            [Launching, Running] + LoseLease => Lost by Jobs,
            [Pending, Launching, Running] + Kill => Killed by Jobs,
        }
    }
}

machine! {
    /// A checkpoint, from staging to commit and deletion.
    Checkpoint {
        aggregate: "checkpoint",
        created: "checkpoint.staged",
        initial: Writing,
        state CheckpointState {
            Writing => "WRITING",
            Uploaded => "UPLOADED",
            Verifying => "VERIFYING",
            Committed => "COMMITTED",
            Corrupt => "CORRUPT",
            Tombstoned => "TOMBSTONED",
        }
        terminal [Tombstoned]
        event CheckpointEvent {
            Upload => "checkpoint.uploaded",
            RequestCommit => "checkpoint.commit_requested",
            Commit => "checkpoint.committed",
            MarkCorrupt => "checkpoint.corrupted",
            Tombstone => "checkpoint.tombstoned",
        }
        transitions {
            [Writing] + Upload => Uploaded by Artifacts,
            [Uploaded] + RequestCommit => Verifying by Artifacts,
            [Verifying] + Commit => Committed by Artifacts,
            [Writing, Uploaded, Verifying, Committed] + MarkCorrupt => Corrupt by Artifacts,
            [Committed, Corrupt] + Tombstone => Tombstoned by Artifacts,
        }
    }
}

machine! {
    /// A compute node, from enrollment to decommission.
    Node {
        aggregate: "node",
        created: "node.registered",
        initial: Enrolling,
        state NodeState {
            Enrolling => "ENROLLING",
            Online => "ONLINE",
            Degraded => "DEGRADED",
            Offline => "OFFLINE",
            Draining => "DRAINING",
            Decommissioned => "DECOMMISSIONED",
        }
        terminal [Decommissioned]
        event NodeEvent {
            Enroll => "node.enrolled",
            Degrade => "node.degraded",
            Recover => "node.recovered",
            LoseContact => "node.disconnected",
            Reconnect => "node.reconnected",
            Drain => "node.drain_started",
            Decommission => "node.decommissioned",
        }
        transitions {
            [Enrolling] + Enroll => Online by Nodes,
            [Online] + Degrade => Degraded by Nodes,
            [Degraded] + Recover => Online by Nodes,
            [Online, Degraded] + LoseContact => Offline by Nodes,
            [Offline] + Reconnect => Online by Nodes,
            [Enrolling, Online, Degraded, Offline] + Drain => Draining by Nodes,
            [Draining] + Decommission => Decommissioned by Nodes,
        }
    }
}

machine! {
    /// One revision of a deployment.
    DeploymentRevision {
        aggregate: "deployment_revision",
        created: "deployment_revision.created",
        initial: Pending,
        state DeploymentRevisionState {
            Pending => "PENDING",
            Starting => "STARTING",
            Healthy => "HEALTHY",
            Degraded => "DEGRADED",
            Draining => "DRAINING",
            Stopped => "STOPPED",
            Failed => "FAILED",
            RolledBack => "ROLLED_BACK",
        }
        terminal [Stopped, Failed, RolledBack]
        event DeploymentRevisionEvent {
            Start => "deployment_revision.started",
            PassHealthCheck => "deployment_revision.became_healthy",
            Fail => "deployment_revision.failed",
            Degrade => "deployment_revision.degraded",
            Drain => "deployment_revision.drain_started",
            RollBack => "deployment_revision.rolled_back",
            Stop => "deployment_revision.stopped",
        }
        transitions {
            [Pending] + Start => Starting by Deployments,
            [Starting, Degraded] + PassHealthCheck => Healthy by Deployments,
            [Starting] + Fail => Failed by Deployments,
            [Healthy] + Degrade => Degraded by Deployments,
            [Healthy, Degraded] + Drain => Draining by Deployments,
            [Healthy, Degraded] + RollBack => RolledBack by Deployments,
            [Draining] + Stop => Stopped by Deployments,
        }
    }
}

machine! {
    /// A policy version sent from the trainer to rollout workers.
    PolicyVersion {
        aggregate: "policy_version",
        created: "policy_version.proposed",
        initial: Proposed,
        state PolicyVersionState {
            Proposed => "PROPOSED",
            Transferring => "TRANSFERRING",
            Acked => "ACKED",
            Active => "ACTIVE",
            Superseded => "SUPERSEDED",
            Orphaned => "ORPHANED",
            Aborted => "ABORTED",
        }
        terminal [Orphaned, Aborted]
        event PolicyVersionEvent {
            Transfer => "policy_version.transfer_started",
            Acknowledge => "policy_version.acknowledged",
            Activate => "policy_version.activated",
            Supersede => "policy_version.superseded",
            Orphan => "policy_version.orphaned",
            Abort => "policy_version.aborted",
        }
        transitions {
            [Proposed] + Transfer => Transferring by WeightSync,
            [Transferring] + Acknowledge => Acked by WeightSync,
            [Acked] + Activate => Active by WeightSync,
            [Active] + Supersede => Superseded by WeightSync,
            [Active, Superseded] + Orphan => Orphaned by WeightSync,
            [Proposed, Transferring] + Abort => Aborted by WeightSync,
        }
    }
}

machine! {
    /// A resource reservation held for a job or a deployment. Fenced by its `epoch`.
    Reservation {
        aggregate: "reservation",
        created: "reservation.held",
        initial: Held,
        state ReservationState {
            Held => "HELD",
            Active => "ACTIVE",
            Releasing => "RELEASING",
            Released => "RELEASED",
            Expired => "EXPIRED",
            Quarantined => "QUARANTINED",
        }
        terminal [Released, Expired]
        event ReservationEvent {
            Activate => "reservation.activated",
            Release => "reservation.release_started",
            Expire => "reservation.expired",
            Quarantine => "reservation.quarantined",
            ConfirmRelease => "reservation.released",
        }
        transitions {
            [Held] + Activate => Active by Scheduler,
            [Held, Active] + Release => Releasing by Scheduler,
            [Held] + Expire => Expired by Scheduler,
            [Active] + Quarantine => Quarantined by Scheduler,
            [Releasing, Quarantined] + ConfirmRelease => Released by Scheduler,
        }
    }
}

machine! {
    /// The import state of a model version.
    ModelVersion {
        aggregate: "model_version",
        created: "model_version.created",
        initial: Importing,
        state ModelVersionState {
            Importing => "IMPORTING",
            Ready => "READY",
            Invalid => "INVALID",
            Archived => "ARCHIVED",
        }
        terminal [Invalid, Archived]
        event ModelVersionEvent {
            FinishImport => "model_version.imported",
            FailImport => "model_version.import_failed",
            Archive => "model_version.archived",
        }
        transitions {
            [Importing] + FinishImport => Ready by Models,
            [Importing] + FailImport => Invalid by Models,
            [Ready] + Archive => Archived by Models,
        }
    }
}

machine! {
    /// The promotion stage of a model version.
    ModelStage {
        aggregate: "model_version",
        created: "model_version.created",
        initial: Experimental,
        state ModelStageState {
            Experimental => "EXPERIMENTAL",
            Candidate => "CANDIDATE",
            Approved => "APPROVED",
            Archived => "ARCHIVED",
        }
        terminal [Archived]
        event ModelStageEvent {
            RequestPromotion => "model_version.promotion_requested",
            ApprovePromotion => "model_version.promotion_approved",
            ArchiveStage => "model_version.stage_archived",
        }
        transitions {
            [Experimental] + RequestPromotion => Candidate by Promotions,
            [Candidate] + ApprovePromotion => Approved by Promotions,
            [Experimental, Candidate, Approved] + ArchiveStage => Archived by Promotions,
        }
    }
}

machine! {
    /// One version of a dataset, from ingest to ready.
    DatasetVersion {
        aggregate: "dataset_version",
        created: "dataset_version.created",
        initial: Ingesting,
        state DatasetVersionState {
            Ingesting => "INGESTING",
            Validating => "VALIDATING",
            Ready => "READY",
            Invalid => "INVALID",
            Archived => "ARCHIVED",
        }
        terminal [Invalid, Archived]
        event DatasetVersionEvent {
            FinishIngest => "dataset_version.ingested",
            PassValidation => "dataset_version.validated",
            FailValidation => "dataset_version.validation_failed",
            Archive => "dataset_version.archived",
        }
        transitions {
            [Ingesting] + FinishIngest => Validating by Datasets,
            [Validating] + PassValidation => Ready by Datasets,
            [Ingesting, Validating] + FailValidation => Invalid by Datasets,
            [Ready] + Archive => Archived by Datasets,
        }
    }
}

machine! {
    /// One version of a reward, from draft to published.
    RewardVersion {
        aggregate: "reward_version",
        created: "reward_version.drafted",
        initial: Draft,
        state RewardVersionState {
            Draft => "DRAFT",
            Testing => "TESTING",
            Published => "PUBLISHED",
            FailedTests => "FAILED_TESTS",
            Deprecated => "DEPRECATED",
        }
        terminal [Deprecated]
        event RewardVersionEvent {
            StartTests => "reward_version.tests_started",
            Publish => "reward_version.published",
            FailTests => "reward_version.tests_failed",
            Deprecate => "reward_version.deprecated",
        }
        transitions {
            [Draft, FailedTests] + StartTests => Testing by Rewards,
            [Testing] + Publish => Published by Rewards,
            [Testing] + FailTests => FailedTests by Rewards,
            [Published] + Deprecate => Deprecated by Rewards,
        }
    }
}

machine! {
    /// One version of a training engine. Third-party engines wait for approval;
    /// first-party engines start at `PROBING` (see [`EngineVersion::initial_state`]).
    EngineVersion {
        aggregate: "engine_version",
        created: "engine_version.registered",
        initial: PendingApproval,
        state EngineVersionState {
            PendingApproval => "PENDING_APPROVAL",
            Probing => "PROBING",
            Enabled => "ENABLED",
            Disabled => "DISABLED",
            Revoked => "REVOKED",
        }
        terminal [Revoked]
        event EngineVersionEvent {
            Approve => "engine_version.approved",
            Enable => "engine_version.enabled",
            Disable => "engine_version.disabled",
            Revoke => "engine_version.revoked",
        }
        transitions {
            [PendingApproval] + Approve => Probing by Engines,
            [Probing, Disabled] + Enable => Enabled by Engines,
            [Probing, Enabled] + Disable => Disabled by Engines,
            [PendingApproval, Probing, Enabled, Disabled] + Revoke => Revoked by Engines,
        }
    }
}

impl EngineVersion {
    /// The first state of a new engine version: first-party engines skip approval.
    pub const fn initial_state(first_party: bool) -> EngineVersionState {
        if first_party {
            EngineVersionState::Probing
        } else {
            EngineVersionState::PendingApproval
        }
    }
}

machine! {
    /// One item in a feedback task.
    FeedbackItem {
        aggregate: "feedback_item",
        created: "feedback_item.created",
        initial: Pending,
        state FeedbackItemState {
            Pending => "PENDING",
            InProgress => "IN_PROGRESS",
            Done => "DONE",
            Disputed => "DISPUTED",
            Resolved => "RESOLVED",
        }
        terminal [Resolved]
        event FeedbackItemEvent {
            Assign => "feedback_item.assigned",
            ReleaseAssignment => "feedback_item.released",
            Complete => "feedback_item.completed",
            Dispute => "feedback_item.disputed",
            Resolve => "feedback_item.resolved",
        }
        transitions {
            [Pending] + Assign => InProgress by Feedback,
            [InProgress] + ReleaseAssignment => Pending by Feedback,
            [InProgress] + Complete => Done by Feedback,
            [Done] + Dispute => Disputed by Feedback,
            [Disputed] + Resolve => Resolved by Feedback,
        }
    }
}

#[cfg(test)]
mod tests;
