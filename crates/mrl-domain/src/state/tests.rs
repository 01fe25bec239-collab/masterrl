use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use proptest::prelude::*;

use super::*;

/// Check every (state, event) pair against an independent expected table of
/// `(from, event type, to)` rows. Pairs not in the table must be rejected, with
/// `Terminal` exactly when the state is terminal.
fn check_table<M: StateMachine>(expected: &[(&str, &str, &str)]) {
    let mut want = BTreeMap::new();
    for &(from, event, to) in expected {
        assert!(
            want.insert((from, event), to).is_none(),
            "duplicate row {from} {event}"
        );
    }
    let mut seen = BTreeSet::new();
    for &s in M::STATES {
        for &e in M::EVENTS {
            let key = (M::state_name(s), M::event_type(e));
            match (M::can_transition(s, e), want.get(&key)) {
                (Ok(to), Some(&w)) => {
                    assert_eq!(M::state_name(to), w, "{key:?}");
                    seen.insert(key);
                }
                (
                    Err(TransitionError::Terminal {
                        aggregate,
                        from,
                        event,
                    }),
                    None,
                ) => {
                    assert!(M::is_terminal(s), "{key:?} rejected as terminal");
                    assert_eq!((aggregate, from, event), (M::AGGREGATE, key.0, key.1));
                }
                (
                    Err(TransitionError::NotAllowed {
                        aggregate,
                        from,
                        event,
                    }),
                    None,
                ) => {
                    assert!(!M::is_terminal(s), "{key:?} rejected as not allowed");
                    assert_eq!((aggregate, from, event), (M::AGGREGATE, key.0, key.1));
                }
                (got, w) => panic!("{} {key:?}: got {got:?}, want {w:?}", M::AGGREGATE),
            }
        }
    }
    let missing: Vec<_> = want.keys().filter(|k| !seen.contains(*k)).collect();
    assert!(
        missing.is_empty(),
        "rows naming unknown states or events: {missing:?}"
    );
}

/// Structural rules every machine must follow.
fn check_shape<M: StateMachine>()
where
    M::State: Ord,
    M::Event: Ord,
{
    let states: BTreeSet<_> = M::STATES.iter().copied().collect();
    let events: BTreeSet<_> = M::EVENTS.iter().copied().collect();
    assert_eq!(states.len(), M::STATES.len(), "duplicate state");
    assert_eq!(events.len(), M::EVENTS.len(), "duplicate event");

    let names: BTreeSet<_> = M::STATES.iter().map(|&s| M::state_name(s)).collect();
    assert_eq!(names.len(), states.len(), "duplicate state name");
    for &s in M::STATES {
        let n = M::state_name(s);
        assert!(n.chars().all(|c| c.is_ascii_uppercase() || c == '_'), "{n}");
    }

    let types: BTreeSet<_> = M::EVENTS.iter().map(|&e| M::event_type(e)).collect();
    assert_eq!(types.len(), events.len(), "duplicate event type");
    for t in types.iter().chain([&M::CREATED_EVENT]) {
        let verb = t
            .strip_prefix(M::AGGREGATE)
            .and_then(|r| r.strip_prefix('.'));
        let verb = verb.unwrap_or_else(|| panic!("{t} must start with {}.", M::AGGREGATE));
        let past = |w: &str| w.ends_with("ed") || ["held", "lost", "became"].contains(&w);
        assert!(verb.split('_').any(past), "{t} must use a past-tense verb");
        assert!(
            verb.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "{t}"
        );
    }

    let mut rows = BTreeSet::new();
    for t in M::TRANSITIONS {
        assert!(
            rows.insert((t.from, t.event)),
            "two rows for {:?} + {:?}",
            t.from,
            t.event
        );
        assert!(
            !M::is_terminal(t.from),
            "terminal {:?} has an outgoing row",
            t.from
        );
        assert_ne!(t.from, t.to, "self loop {:?}", t.from);
    }
    let used: BTreeSet<_> = M::TRANSITIONS.iter().map(|t| t.event).collect();
    assert_eq!(used, events, "every event is used");
    for &s in M::STATES {
        let has_row = M::TRANSITIONS.iter().any(|t| t.from == s);
        assert_eq!(
            has_row,
            !M::is_terminal(s),
            "{:?}: non-terminal states need a way out",
            s
        );
    }

    // Every state is reachable from the initial state.
    let mut reached = BTreeSet::from([M::INITIAL]);
    let mut frontier = vec![M::INITIAL];
    while let Some(s) = frontier.pop() {
        for t in M::TRANSITIONS.iter().filter(|t| t.from == s) {
            if reached.insert(t.to) {
                frontier.push(t.to);
            }
        }
    }
    assert_eq!(reached, states, "{}: unreachable states", M::AGGREGATE);
}

macro_rules! machine_tests {
    ($($name:ident: $machine:ty => [$(($from:literal, $ev:literal, $to:literal)),+ $(,)?];)+) => {
        $(
            #[test]
            fn $name() {
                check_shape::<$machine>();
                check_table::<$machine>(&[$(($from, $ev, $to)),+]);
            }
        )+
    };
}

machine_tests! {
    job_table: Job => [
        ("CREATED", "job.submitted", "VALIDATING"),
        ("VALIDATING", "job.queued", "QUEUED"),
        ("VALIDATING", "job.rejected", "REJECTED"),
        ("QUEUED", "job.placed", "ALLOCATING"),
        ("ALLOCATING", "job.launched", "PREPARING"),
        ("ALLOCATING", "job.requeued", "QUEUED"),
        ("ALLOCATING", "job.retry_scheduled", "RETRY_WAIT"),
        ("ALLOCATING", "job.failed", "FAILED"),
        ("PREPARING", "job.started", "RUNNING"),
        ("PREPARING", "job.retry_scheduled", "RETRY_WAIT"),
        ("PREPARING", "job.failed", "FAILED"),
        ("RUNNING", "job.finished", "FINALIZING"),
        ("RUNNING", "job.pause_requested", "PAUSING"),
        ("RUNNING", "job.retry_scheduled", "RETRY_WAIT"),
        ("RUNNING", "job.failed", "FAILED"),
        ("FINALIZING", "job.completed", "COMPLETED"),
        ("FINALIZING", "job.failed", "FAILED"),
        ("FINALIZING", "job.retry_scheduled", "RETRY_WAIT"),
        ("PAUSING", "job.paused", "PAUSED"),
        ("PAUSING", "job.pause_refused", "RUNNING"),
        ("PAUSING", "job.failed", "FAILED"),
        ("PAUSED", "job.resumed", "RESUMING"),
        ("RESUMING", "job.requeued", "QUEUED"),
        ("RETRY_WAIT", "job.retried", "QUEUED"),
        ("RETRY_WAIT", "job.failed", "FAILED"),
        ("QUEUED", "job.cancel_requested", "CANCELLING"),
        ("ALLOCATING", "job.cancel_requested", "CANCELLING"),
        ("PREPARING", "job.cancel_requested", "CANCELLING"),
        ("RUNNING", "job.cancel_requested", "CANCELLING"),
        ("PAUSING", "job.cancel_requested", "CANCELLING"),
        ("PAUSED", "job.cancel_requested", "CANCELLING"),
        ("RESUMING", "job.cancel_requested", "CANCELLING"),
        ("RETRY_WAIT", "job.cancel_requested", "CANCELLING"),
        ("FINALIZING", "job.cancel_requested", "CANCELLING"),
        ("CANCELLING", "job.cancelled", "CANCELLED"),
    ];
    attempt_table: JobAttempt => [
        ("PENDING", "attempt.acknowledged", "LAUNCHING"),
        ("LAUNCHING", "attempt.started", "RUNNING"),
        ("LAUNCHING", "attempt.succeeded", "EXITED_OK"),
        ("RUNNING", "attempt.succeeded", "EXITED_OK"),
        ("LAUNCHING", "attempt.errored", "EXITED_ERROR"),
        ("RUNNING", "attempt.errored", "EXITED_ERROR"),
        ("LAUNCHING", "attempt.lost", "LOST"),
        ("RUNNING", "attempt.lost", "LOST"),
        ("PENDING", "attempt.killed", "KILLED"),
        ("LAUNCHING", "attempt.killed", "KILLED"),
        ("RUNNING", "attempt.killed", "KILLED"),
    ];
    checkpoint_table: Checkpoint => [
        ("WRITING", "checkpoint.uploaded", "UPLOADED"),
        ("UPLOADED", "checkpoint.commit_requested", "VERIFYING"),
        ("VERIFYING", "checkpoint.committed", "COMMITTED"),
        ("WRITING", "checkpoint.corrupted", "CORRUPT"),
        ("UPLOADED", "checkpoint.corrupted", "CORRUPT"),
        ("VERIFYING", "checkpoint.corrupted", "CORRUPT"),
        ("COMMITTED", "checkpoint.corrupted", "CORRUPT"),
        ("COMMITTED", "checkpoint.tombstoned", "TOMBSTONED"),
        ("CORRUPT", "checkpoint.tombstoned", "TOMBSTONED"),
    ];
    node_table: Node => [
        ("ENROLLING", "node.enrolled", "ONLINE"),
        ("ONLINE", "node.degraded", "DEGRADED"),
        ("DEGRADED", "node.recovered", "ONLINE"),
        ("ONLINE", "node.disconnected", "OFFLINE"),
        ("DEGRADED", "node.disconnected", "OFFLINE"),
        ("OFFLINE", "node.reconnected", "ONLINE"),
        ("ENROLLING", "node.drain_started", "DRAINING"),
        ("ONLINE", "node.drain_started", "DRAINING"),
        ("DEGRADED", "node.drain_started", "DRAINING"),
        ("OFFLINE", "node.drain_started", "DRAINING"),
        ("DRAINING", "node.decommissioned", "DECOMMISSIONED"),
    ];
    deployment_revision_table: DeploymentRevision => [
        ("PENDING", "deployment_revision.started", "STARTING"),
        ("STARTING", "deployment_revision.became_healthy", "HEALTHY"),
        ("STARTING", "deployment_revision.failed", "FAILED"),
        ("HEALTHY", "deployment_revision.degraded", "DEGRADED"),
        ("DEGRADED", "deployment_revision.became_healthy", "HEALTHY"),
        ("HEALTHY", "deployment_revision.drain_started", "DRAINING"),
        ("DEGRADED", "deployment_revision.drain_started", "DRAINING"),
        ("HEALTHY", "deployment_revision.rolled_back", "ROLLED_BACK"),
        ("DEGRADED", "deployment_revision.rolled_back", "ROLLED_BACK"),
        ("DRAINING", "deployment_revision.stopped", "STOPPED"),
    ];
    policy_version_table: PolicyVersion => [
        ("PROPOSED", "policy_version.transfer_started", "TRANSFERRING"),
        ("PROPOSED", "policy_version.aborted", "ABORTED"),
        ("TRANSFERRING", "policy_version.acknowledged", "ACKED"),
        ("TRANSFERRING", "policy_version.aborted", "ABORTED"),
        ("ACKED", "policy_version.activated", "ACTIVE"),
        ("ACTIVE", "policy_version.superseded", "SUPERSEDED"),
        ("ACTIVE", "policy_version.orphaned", "ORPHANED"),
        ("SUPERSEDED", "policy_version.orphaned", "ORPHANED"),
    ];
    reservation_table: Reservation => [
        ("HELD", "reservation.activated", "ACTIVE"),
        ("HELD", "reservation.release_started", "RELEASING"),
        ("HELD", "reservation.expired", "EXPIRED"),
        ("ACTIVE", "reservation.release_started", "RELEASING"),
        ("ACTIVE", "reservation.quarantined", "QUARANTINED"),
        ("RELEASING", "reservation.released", "RELEASED"),
        ("QUARANTINED", "reservation.released", "RELEASED"),
    ];
    model_version_table: ModelVersion => [
        ("IMPORTING", "model_version.imported", "READY"),
        ("IMPORTING", "model_version.import_failed", "INVALID"),
        ("READY", "model_version.archived", "ARCHIVED"),
    ];
    model_stage_table: ModelStage => [
        ("EXPERIMENTAL", "model_version.promotion_requested", "CANDIDATE"),
        ("CANDIDATE", "model_version.promotion_approved", "APPROVED"),
        ("EXPERIMENTAL", "model_version.stage_archived", "ARCHIVED"),
        ("CANDIDATE", "model_version.stage_archived", "ARCHIVED"),
        ("APPROVED", "model_version.stage_archived", "ARCHIVED"),
    ];
    dataset_version_table: DatasetVersion => [
        ("INGESTING", "dataset_version.ingested", "VALIDATING"),
        ("INGESTING", "dataset_version.validation_failed", "INVALID"),
        ("VALIDATING", "dataset_version.validated", "READY"),
        ("VALIDATING", "dataset_version.validation_failed", "INVALID"),
        ("READY", "dataset_version.archived", "ARCHIVED"),
    ];
    reward_version_table: RewardVersion => [
        ("DRAFT", "reward_version.tests_started", "TESTING"),
        ("FAILED_TESTS", "reward_version.tests_started", "TESTING"),
        ("TESTING", "reward_version.published", "PUBLISHED"),
        ("TESTING", "reward_version.tests_failed", "FAILED_TESTS"),
        ("PUBLISHED", "reward_version.deprecated", "DEPRECATED"),
    ];
    engine_version_table: EngineVersion => [
        ("PENDING_APPROVAL", "engine_version.approved", "PROBING"),
        ("PROBING", "engine_version.enabled", "ENABLED"),
        ("PROBING", "engine_version.disabled", "DISABLED"),
        ("ENABLED", "engine_version.disabled", "DISABLED"),
        ("DISABLED", "engine_version.enabled", "ENABLED"),
        ("PENDING_APPROVAL", "engine_version.revoked", "REVOKED"),
        ("PROBING", "engine_version.revoked", "REVOKED"),
        ("ENABLED", "engine_version.revoked", "REVOKED"),
        ("DISABLED", "engine_version.revoked", "REVOKED"),
    ];
    feedback_item_table: FeedbackItem => [
        ("PENDING", "feedback_item.assigned", "IN_PROGRESS"),
        ("IN_PROGRESS", "feedback_item.released", "PENDING"),
        ("IN_PROGRESS", "feedback_item.completed", "DONE"),
        ("DONE", "feedback_item.disputed", "DISPUTED"),
        ("DISPUTED", "feedback_item.resolved", "RESOLVED"),
    ];
}

#[test]
fn job_owners_match_the_spec() {
    let owned = |o: Owner| -> BTreeSet<(&str, &str)> {
        Job::TRANSITIONS
            .iter()
            .filter(|t| t.owner == o)
            .map(|t| (t.from.as_str(), t.to.as_str()))
            .collect()
    };
    assert_eq!(
        owned(Owner::Scheduler),
        BTreeSet::from([
            ("QUEUED", "ALLOCATING"),
            ("ALLOCATING", "PREPARING"),
            ("ALLOCATING", "QUEUED"),
            ("RETRY_WAIT", "QUEUED"),
        ])
    );
    assert_eq!(
        owned(Owner::ArtifactsVerifier),
        BTreeSet::from([("FINALIZING", "COMPLETED")])
    );
    let others = Job::TRANSITIONS
        .iter()
        .filter(|t| !matches!(t.owner, Owner::Scheduler | Owner::ArtifactsVerifier));
    assert!(others.clone().all(|t| t.owner == Owner::Jobs));
    assert_eq!(others.count(), Job::TRANSITIONS.len() - 5);
}

#[test]
fn engine_initial_state_depends_on_origin() {
    assert_eq!(
        EngineVersion::initial_state(true),
        EngineVersionState::Probing
    );
    assert_eq!(EngineVersion::initial_state(false), EngineVersion::INITIAL);
}

#[test]
fn states_parse_display_and_serialize_by_wire_name() {
    for &s in Job::STATES {
        assert_eq!(s.to_string().parse::<JobState>(), Ok(s));
        let json = serde_json::to_string(&s).map_err(|e| e.to_string());
        assert_eq!(json, Ok(format!("\"{}\"", s.as_str())));
    }
    let back: Result<CheckpointState, _> = serde_json::from_str("\"COMMITTED\"");
    assert_eq!(back.ok(), Some(CheckpointState::Committed));
    let err = "Running".parse::<JobState>().map_err(|e| e.to_string());
    assert_eq!(err, Err("`Running` is not a job state".into()));
    let err = "x"
        .parse::<DeploymentRevisionState>()
        .map_err(|e| e.to_string());
    assert_eq!(err, Err("`x` is not a deployment revision state".into()));
}

#[test]
fn errors_read_plainly() {
    let terminal =
        Job::can_transition(JobState::Completed, JobEvent::Start).map_err(|e| e.to_string());
    assert_eq!(
        terminal,
        Err("this job is already COMPLETED and can't change any more".into())
    );
    let wrong =
        Job::can_transition(JobState::Queued, JobEvent::Complete).map_err(|e| e.to_string());
    assert_eq!(
        wrong,
        Err("a job that is QUEUED can't take the step `job.completed`".into())
    );
    let multi =
        FeedbackItem::can_transition(FeedbackItemState::Pending, FeedbackItemEvent::Resolve)
            .map_err(|e| e.to_string());
    assert_eq!(
        multi,
        Err("a feedback item that is PENDING can't take the step `feedback_item.resolved`".into())
    );
}

#[test]
fn owner_names_are_snake_case_and_unique() {
    let all = [
        Owner::Jobs,
        Owner::Scheduler,
        Owner::ArtifactsVerifier,
        Owner::Artifacts,
        Owner::Nodes,
        Owner::Deployments,
        Owner::WeightSync,
        Owner::Models,
        Owner::Promotions,
        Owner::Datasets,
        Owner::Rewards,
        Owner::Engines,
        Owner::Feedback,
    ];
    let names: BTreeSet<String> = all.iter().map(Owner::to_string).collect();
    assert_eq!(names.len(), all.len());
    assert!(names
        .iter()
        .all(|n| n.chars().all(|c| c.is_ascii_lowercase() || c == '_')));
}

/// Render one machine as text: initial state, terminal states, then every row.
fn render<M: StateMachine>(out: &mut String) -> std::fmt::Result {
    writeln!(out, "## {} (created: {})", M::AGGREGATE, M::CREATED_EVENT)?;
    writeln!(out, "initial: {}", M::state_name(M::INITIAL))?;
    let terminal: Vec<_> = M::TERMINAL.iter().map(|&s| M::state_name(s)).collect();
    writeln!(out, "terminal: {}", terminal.join(" "))?;
    for t in M::TRANSITIONS {
        writeln!(
            out,
            "{:<16} --{}--> {:<16} by {}",
            M::state_name(t.from),
            M::event_type(t.event),
            M::state_name(t.to),
            t.owner
        )?;
    }
    writeln!(out)
}

#[test]
fn every_machine_snapshot() -> std::fmt::Result {
    let mut out = String::new();
    render::<Job>(&mut out)?;
    render::<JobAttempt>(&mut out)?;
    render::<Checkpoint>(&mut out)?;
    render::<Node>(&mut out)?;
    render::<DeploymentRevision>(&mut out)?;
    render::<PolicyVersion>(&mut out)?;
    render::<Reservation>(&mut out)?;
    render::<ModelVersion>(&mut out)?;
    render::<ModelStage>(&mut out)?;
    render::<DatasetVersion>(&mut out)?;
    render::<RewardVersion>(&mut out)?;
    render::<EngineVersion>(&mut out)?;
    render::<FeedbackItem>(&mut out)?;
    insta::assert_snapshot!(out);
    Ok(())
}

/// A random walk step: either take one of the allowed steps (picked by index), or try any event.
type Step = (bool, usize);

/// Each state a walk entered, with the state it came from.
type Entered<S> = Vec<(S, S)>;

/// Walk a machine from `start`, checking the generic rules at every step.
/// Returns every state visited and the state each one was entered from.
fn walk<M: StateMachine>(
    start: M::State,
    steps: &[Step],
) -> Result<Entered<M::State>, TestCaseError> {
    let mut state = start;
    let mut entered = Vec::new();
    for &(take_allowed, pick) in steps {
        let allowed: Vec<_> = M::TRANSITIONS.iter().filter(|t| t.from == state).collect();
        let event = if take_allowed && !allowed.is_empty() {
            allowed[pick % allowed.len()].event
        } else {
            M::EVENTS[pick % M::EVENTS.len()]
        };
        match M::can_transition(state, event) {
            Ok(next) => {
                prop_assert!(!M::is_terminal(state), "left terminal state {:?}", state);
                prop_assert!(M::transition(state, event).is_some_and(|t| t.to == next));
                entered.push((next, state));
                state = next;
            }
            Err(TransitionError::Terminal { .. }) => prop_assert!(M::is_terminal(state)),
            Err(TransitionError::NotAllowed { .. }) => {
                prop_assert!(!M::is_terminal(state));
                prop_assert!(M::transition(state, event).is_none());
            }
        }
    }
    Ok(entered)
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    proptest::collection::vec((any::<bool>(), any::<usize>()), 0..64)
}

#[test]
fn walks_reach_the_guarded_states() -> Result<(), TestCaseError> {
    // Always taking the first allowed step walks the happy path, so the proptests below
    // can reach COMPLETED and COMMITTED.
    let happy = [(true, 0); 16];
    let job = walk::<Job>(Job::INITIAL, &happy)?;
    assert_eq!(
        job.last(),
        Some(&(JobState::Completed, JobState::Finalizing))
    );
    let ckpt = walk::<Checkpoint>(Checkpoint::INITIAL, &happy)?;
    assert!(ckpt.contains(&(CheckpointState::Committed, CheckpointState::Verifying)));
    Ok(())
}

proptest! {
    #[test]
    fn job_completes_only_via_finalizing(s in steps()) {
        for (to, from) in walk::<Job>(Job::INITIAL, &s)? {
            if to == JobState::Completed {
                prop_assert_eq!(from, JobState::Finalizing);
            }
        }
    }

    #[test]
    fn checkpoint_commits_only_via_verifying(s in steps()) {
        for (to, from) in walk::<Checkpoint>(Checkpoint::INITIAL, &s)? {
            if to == CheckpointState::Committed {
                prop_assert_eq!(from, CheckpointState::Verifying);
            }
        }
    }

    #[test]
    fn terminal_states_absorb_in_every_machine(s in steps()) {
        walk::<Job>(Job::INITIAL, &s)?;
        walk::<JobAttempt>(JobAttempt::INITIAL, &s)?;
        walk::<Checkpoint>(Checkpoint::INITIAL, &s)?;
        walk::<Node>(Node::INITIAL, &s)?;
        walk::<DeploymentRevision>(DeploymentRevision::INITIAL, &s)?;
        walk::<PolicyVersion>(PolicyVersion::INITIAL, &s)?;
        walk::<Reservation>(Reservation::INITIAL, &s)?;
        walk::<ModelVersion>(ModelVersion::INITIAL, &s)?;
        walk::<ModelStage>(ModelStage::INITIAL, &s)?;
        walk::<DatasetVersion>(DatasetVersion::INITIAL, &s)?;
        walk::<RewardVersion>(RewardVersion::INITIAL, &s)?;
        walk::<EngineVersion>(EngineVersion::INITIAL, &s)?;
        walk::<EngineVersion>(EngineVersion::initial_state(true), &s)?;
        walk::<FeedbackItem>(FeedbackItem::INITIAL, &s)?;
    }
}
