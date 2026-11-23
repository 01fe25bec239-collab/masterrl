//! The CONTROL sink for `attempt_status` and `reconcile`, and the paths that end attempts.
//!
//! Both kinds are idempotent by content (A1 ruling, 2026-11-21): the agent re-sends facts
//! with new seqs (every reconcile lists ended attempts until purged), so a status for an
//! ended or older-epoch attempt is a no-op, never a second transition. Per attempt the
//! highest applied seq is kept (`scheduler_attempt_reports`); per machine the newest
//! reconcile (`scheduler_node_reconciles`), so a replayed or older report changes nothing.
//! A machine may only report attempts placed on it; anything else it runs gets a cancel.
//!
//! An attempt ends in one of these, each in one transaction that also settles its job and
//! reservation: it exited ([`on_status`]), its lease ran out or the machine lost it
//! ([`lose`]), or it never reported starting ([`end_unstarted`]).

use super::reservations;
use super::{Core, SchedError};
use crate::db::Tx;
use crate::modules::jobs::{AttemptChange, AttemptRow, JobChange, JobTransitions};
use crate::modules::nodes::commands::NewCommand;
use crate::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink, SinkError};
use mrl_domain::failure::{FailureClass, FailureCode};
use mrl_domain::ids::{AttemptId, NodeId};
use mrl_domain::state::{JobAttemptEvent, JobAttemptState, JobEvent, JobState, Owner};
use mrl_proto::agent::v1::agent_message::Body;
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{AttemptPhase, AttemptStatus, CancelAttempt, ReconcileReport};
use mrl_proto::common::v1::AttemptRef;
use mrl_proto::worker::v1::ExitKind;
use prost::Message;
use serde_json::json;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Work after the transaction commits.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum After {
    /// Tell the machine to stop an attempt it should not run.
    Cancel {
        node: Uuid,
        attempt: AttemptRef,
        reason: &'static str,
    },
    /// Verify a FINALIZING job.
    Finalize { org: Uuid, job: Uuid },
}

/// See the module docs.
pub struct ControlSink {
    core: Arc<Core>,
}

impl ControlSink {
    /// The sink.
    pub fn new(core: Arc<Core>) -> Self {
        Self { core }
    }
}

#[async_trait::async_trait]
impl LaneSink for ControlSink {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Control]
    }

    fn control_kinds(&self) -> &'static [&'static str] {
        &["attempt_status", "reconcile"]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        let core = &self.core;
        let seq = i64::try_from(item.seq).unwrap_or(i64::MAX);
        // A lost compare-and-set means another path moved the row on; decide again from the
        // fresh rows. Anything else that is not the database is a bug or bad input: logged and
        // dropped, so one bad message never blocks the machine's CONTROL lane.
        for _ in 0..5 {
            let r = match &item.message.body {
                Some(Body::AttemptStatus(s)) => on_status(core, item.node_id, seq, s).await,
                Some(Body::Reconcile(r)) => on_reconcile(core, item.node_id, seq, r).await,
                _ => Ok(()),
            };
            match r {
                Ok(()) => {
                    core.wake.notify_one();
                    return Ok(());
                }
                Err(e) if e.is_race() => continue,
                Err(e @ (SchedError::Db(_) | SchedError::Seam(_) | SchedError::Nodes(_))) => {
                    return Err(SinkError {
                        sink: "scheduler",
                        detail: e.to_string(),
                    })
                }
                Err(SchedError::Jobs(crate::modules::jobs::JobsError::Db(e))) => {
                    return Err(SinkError {
                        sink: "scheduler",
                        detail: e.to_string(),
                    })
                }
                Err(e) => {
                    tracing::error!(node = %item.node_id, seq, error = %e, "attempt report not applied");
                    return Ok(());
                }
            }
        }
        Err(SinkError {
            sink: "scheduler",
            detail: "the attempt kept changing while its report was applied".into(),
        })
    }
}

fn live_phase(phase: i32) -> bool {
    !matches!(
        AttemptPhase::try_from(phase),
        Ok(AttemptPhase::Exited | AttemptPhase::SelfFenced)
    )
}

/// The attempt's org and whether it was placed on `node`; `None` if no such attempt.
async fn whose(core: &Core, attempt: Uuid, node: Uuid) -> Result<Option<(Uuid, bool)>, SchedError> {
    Ok(sqlx::query_as(
        "SELECT a.org_id, EXISTS (SELECT 1 FROM attempt_placements p
                                   WHERE p.attempt_id = a.id AND p.node_id = $2)
         FROM job_attempts a WHERE a.id = $1",
    )
    .bind(attempt)
    .bind(node)
    .fetch_optional(core.db.pool())
    .await?)
}

/// Applies one attempt status (from its own message or inside a reconcile report at `seq`).
pub(crate) async fn on_status(
    core: &Core,
    node: Uuid,
    seq: i64,
    s: &AttemptStatus,
) -> Result<(), SchedError> {
    let Some(r) = &s.attempt else {
        tracing::warn!(node = %node, "attempt status without an attempt");
        return Ok(());
    };
    let Ok(id) = AttemptId::from_str(&r.attempt_id) else {
        tracing::warn!(node = %node, attempt = %r.attempt_id, "attempt status with a bad attempt id");
        return Ok(());
    };
    let id = id.as_uuid();
    let live = live_phase(s.phase);
    let Some((org, here)) = whose(core, id, node).await? else {
        if live {
            run(
                core,
                vec![After::Cancel {
                    node,
                    attempt: r.clone(),
                    reason: "ORPHAN",
                }],
            )
            .await?;
        }
        return Ok(());
    };
    if !here {
        tracing::warn!(node = %node, attempt = %r.attempt_id, "a machine reported an attempt placed elsewhere");
        if live {
            run(
                core,
                vec![After::Cancel {
                    node,
                    attempt: r.clone(),
                    reason: "ORPHAN",
                }],
            )
            .await?;
        }
        return Ok(());
    }
    let mut tx = core.db.begin().await?;
    let Some(attempt) = JobTransitions::lock_attempt(&mut tx, org, id).await? else {
        return Ok(());
    };
    let state = attempt.attempt_state()?;
    let stale = i64::try_from(r.fencing_epoch).ok() != Some(attempt.epoch);
    if (stale || is_ended(state)) && live {
        // Leave the inbox entry replayable until the cancel is saved. This branch does
        // not apply any attempt state, so it needs no status watermark.
        tx.rollback().await?;
        return cancel(core, node, r.clone(), "FENCED", Duration::ZERO).await;
    }
    if stale {
        return Ok(());
    }
    let fresh: Option<i32> = sqlx::query_scalar(
        "INSERT INTO scheduler_attempt_reports (attempt_id, node_id, seq) VALUES ($1, $2, $3)
         ON CONFLICT (attempt_id) DO UPDATE SET seq = EXCLUDED.seq
           WHERE scheduler_attempt_reports.seq < EXCLUDED.seq
         RETURNING 1",
    )
    .bind(id)
    .bind(node)
    .bind(seq)
    .fetch_optional(&mut *tx)
    .await?;
    if fresh.is_none() {
        return Ok(());
    }
    let mut after = Vec::new();
    if is_ended(state) {
        release_for_attempt(core, &mut tx, id, "the machine confirmed the attempt ended").await?;
        tx.commit().await?;
        return Ok(());
    }
    let previous_version = attempt.state_version;
    let job_id = attempt.job_id;
    apply_live_status(core, &mut tx, attempt, s, &mut after).await?;
    let current_version: i64 =
        sqlx::query_scalar("SELECT state_version FROM job_attempts WHERE id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    if current_version != previous_version {
        let job = lock_job(&mut tx, org, job_id).await?;
        core.audit_job(&mut tx, &job, "scheduler.attempt_status")
            .await?;
    }
    tx.commit().await?;
    run(core, after).await
}

fn is_ended(s: JobAttemptState) -> bool {
    matches!(
        s,
        JobAttemptState::ExitedOk
            | JobAttemptState::ExitedError
            | JobAttemptState::Lost
            | JobAttemptState::Killed
    )
}

/// PENDING → LAUNCHING, and the lease starts now (never earlier than the agent's).
async fn acknowledge(tx: &mut Tx, a: AttemptRow) -> Result<AttemptRow, SchedError> {
    if a.attempt_state()? != JobAttemptState::Pending {
        return Ok(a);
    }
    let a = JobTransitions::apply_attempt(
        tx,
        &a,
        JobAttemptEvent::Acknowledge,
        AttemptChange::default(),
    )
    .await?;
    let ttl: Option<f64> = sqlx::query_scalar(
        "SELECT extract(epoch from p.attempt_lease_ttl)::float8 FROM resource_reservations r
         JOIN compute_pools p ON p.id = r.pool_id WHERE r.attempt_id = $1 LIMIT 1",
    )
    .bind(a.id)
    .fetch_optional(&mut **tx)
    .await?;
    let until: time::OffsetDateTime = sqlx::query_scalar(
        "UPDATE job_attempts SET lease_expires_at = GREATEST(lease_expires_at, now() + make_interval(secs => $2))
         WHERE id = $1 RETURNING COALESCE(lease_expires_at, now() + make_interval(secs => $2))",
    )
    .bind(a.id)
    .bind(ttl.unwrap_or(90.0))
    .fetch_one(&mut **tx)
    .await?;
    reservations::renew_for_attempt(tx, a.id, until).await?;
    Ok(a)
}

/// Agent text as stored: PostgreSQL refuses NUL in text and jsonb, and a refused write
/// would be retried forever, blocking the machine's CONTROL lane. At most 4 KiB is kept.
pub(crate) fn clean(text: &str) -> String {
    let t: String = text.chars().filter(|c| *c != '\0').collect();
    match t.char_indices().nth(4096) {
        Some((i, _)) => t[..i].to_owned(),
        None => t,
    }
}

fn exit_report(s: &AttemptStatus) -> serde_json::Value {
    json!({
        "exit_kind": ExitKind::try_from(s.exit_kind).map(|k| k.as_str_name()).unwrap_or("EXIT_KIND_UNSPECIFIED"),
        "exit_code": s.exit_code,
        "last_step": s.last_step,
        "failure": s.failure.as_ref().map(|f| json!({
            "class": mrl_proto::common::v1::FailureClass::try_from(f.class).map(|c| c.as_str_name()).unwrap_or("FAILURE_CLASS_UNSPECIFIED"),
            "sub_code": clean(&f.sub_code),
            "message": clean(&f.message),
        })),
    })
}

/// The status's failure class and message (`fallback` when it has none).
fn failure_of(s: &AttemptStatus, fallback: FailureClass) -> (FailureCode, String) {
    let class = s
        .failure
        .as_ref()
        .and_then(|f| FailureClass::from_i32(f.class))
        .unwrap_or(fallback);
    let message = s
        .failure
        .as_ref()
        .map(|f| clean(&f.message))
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| format!("The attempt ended with {}.", class.as_str()));
    (FailureCode::class_only(class), message)
}

async fn apply_live_status(
    core: &Core,
    tx: &mut Tx,
    attempt: AttemptRow,
    s: &AttemptStatus,
    after: &mut Vec<After>,
) -> Result<(), SchedError> {
    let (org, job_id) = (attempt.org_id, attempt.job_id);
    let step = i64::try_from(s.last_step).ok().filter(|n| *n > 0);
    match AttemptPhase::try_from(s.phase).unwrap_or(AttemptPhase::Unspecified) {
        AttemptPhase::Unspecified => {}
        AttemptPhase::Staging
        | AttemptPhase::Preparing
        | AttemptPhase::Prepared
        | AttemptPhase::Stopping => {
            acknowledge(tx, attempt).await?;
        }
        AttemptPhase::Running => {
            let a = acknowledge(tx, attempt).await?;
            if a.attempt_state()? == JobAttemptState::Launching {
                let change = AttemptChange {
                    last_step: step,
                    ..AttemptChange::default()
                };
                JobTransitions::apply_attempt(tx, &a, JobAttemptEvent::Start, change).await?;
            }
            let job = lock_job(tx, org, job_id).await?;
            if job.job_state()? == JobState::Preparing {
                JobTransitions::apply(tx, &job, JobEvent::Start, Owner::Jobs, JobChange::default())
                    .await?;
            }
        }
        AttemptPhase::Exited => {
            let a = acknowledge(tx, attempt).await?;
            let job = lock_job(tx, org, job_id).await?;
            let kind = ExitKind::try_from(s.exit_kind).unwrap_or(ExitKind::Unspecified);
            let report = Some(exit_report(s));
            match kind {
                ExitKind::Completed | ExitKind::DryRunOk => {
                    let change = AttemptChange {
                        exit_report: report,
                        last_step: step,
                        failure: None,
                    };
                    JobTransitions::apply_attempt(tx, &a, JobAttemptEvent::ExitOk, change).await?;
                    let mut job = lock_job(tx, org, job_id).await?;
                    if job.job_state()? == JobState::Pausing {
                        job = refuse_pause(tx, &job, "The run finished before it could pause.")
                            .await?;
                    }
                    if job.job_state()? == JobState::Preparing {
                        job = JobTransitions::apply(
                            tx,
                            &job,
                            JobEvent::Start,
                            Owner::Jobs,
                            JobChange::default(),
                        )
                        .await?;
                    }
                    if job.job_state()? == JobState::Running {
                        JobTransitions::apply(
                            tx,
                            &job,
                            JobEvent::Finish,
                            Owner::Jobs,
                            JobChange::default(),
                        )
                        .await?;
                        after.push(After::Finalize { org, job: job_id });
                    }
                }
                ExitKind::Paused if job.job_state()? == JobState::Pausing => {
                    let change = AttemptChange {
                        exit_report: report,
                        last_step: step,
                        failure: None,
                    };
                    JobTransitions::apply_attempt(tx, &a, JobAttemptEvent::ExitOk, change).await?;
                    let job = lock_job(tx, org, job_id).await?;
                    JobTransitions::apply(
                        tx,
                        &job,
                        JobEvent::ConfirmPause,
                        Owner::Jobs,
                        JobChange::default(),
                    )
                    .await?;
                }
                ExitKind::Cancelled if job.job_state()? == JobState::Cancelling => {
                    let change = AttemptChange {
                        exit_report: report,
                        last_step: step,
                        failure: None,
                    };
                    JobTransitions::apply_attempt(tx, &a, JobAttemptEvent::Kill, change).await?;
                }
                other => {
                    // Paused or cancelled without being asked: the agent freed the machine.
                    let fallback = if matches!(other, ExitKind::Paused | ExitKind::Cancelled) {
                        FailureClass::Preempted
                    } else {
                        FailureClass::HarnessError
                    };
                    let (failure, message) = failure_of(s, fallback);
                    let change = AttemptChange {
                        exit_report: report,
                        last_step: step,
                        failure: Some(failure),
                    };
                    JobTransitions::apply_attempt(tx, &a, JobAttemptEvent::ExitError, change)
                        .await?;
                    fate(tx, org, job_id, failure, &message).await?;
                }
            }
            release_for_attempt(core, tx, a.id, "the attempt ended").await?;
        }
        AttemptPhase::SelfFenced => {
            let a = acknowledge(tx, attempt).await?;
            lose(
                tx,
                &a,
                "The machine stopped the attempt because its lease ran out.",
                true,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn lock_job(
    tx: &mut Tx,
    org: Uuid,
    job: Uuid,
) -> Result<crate::modules::jobs::JobRow, SchedError> {
    JobTransitions::lock(tx, org, job)
        .await?
        .ok_or_else(|| SchedError::internal(format!("job {job} of an attempt is missing")))
}

pub(crate) async fn refuse_pause(
    tx: &mut Tx,
    job: &crate::modules::jobs::JobRow,
    why: &str,
) -> Result<crate::modules::jobs::JobRow, SchedError> {
    let change = JobChange {
        reason: Some(json!({ "message": why })),
        failure: None,
    };
    Ok(JobTransitions::apply(tx, job, JobEvent::RefusePause, Owner::Jobs, change).await?)
}

/// After an attempt ended badly: the job retries or fails, as its rules say. A pausing job
/// stops pausing first; a cancelling job was already settled by the attempt ending.
pub(crate) async fn fate(
    tx: &mut Tx,
    org: Uuid,
    job: Uuid,
    failure: FailureCode,
    message: &str,
) -> Result<(), SchedError> {
    let mut row = lock_job(tx, org, job).await?;
    if row.job_state()? == JobState::Pausing {
        row = refuse_pause(tx, &row, message).await?;
    }
    if matches!(
        row.job_state()?,
        JobState::Preparing | JobState::Running | JobState::Finalizing
    ) {
        JobTransitions::fail_or_retry(tx, &row, failure, message).await?;
    }
    Ok(())
}

/// Gives back every live reservation `attempt` uses.
async fn release_for_attempt(
    core: &Core,
    tx: &mut Tx,
    attempt: Uuid,
    why: &str,
) -> Result<(), SchedError> {
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM resource_reservations WHERE attempt_id = $1
           AND state IN ('HELD','ACTIVE','RELEASING','QUARANTINED') ORDER BY id",
    )
    .bind(attempt)
    .fetch_all(&mut **tx)
    .await?;
    for id in ids {
        if let Some(r) = reservations::lock(tx, id).await? {
            let ended = reservations::release(tx, &r, why).await?;
            if ended.state_version != r.state_version {
                core.audit_reservation(tx, &ended, "scheduler.attempt_released")
                    .await?;
            }
        }
    }
    Ok(())
}

/// The attempt is lost (lease ran out, or its machine no longer knows it): LOST, the job's
/// epoch moves on, the job retries or fails (NODE_LOST). Its GPUs are released when the
/// machine said the processes are gone (`gone`), else quarantined until it does.
pub(crate) async fn lose(
    tx: &mut Tx,
    attempt: &AttemptRow,
    why: &str,
    gone: bool,
) -> Result<(), SchedError> {
    lose_with_failure(
        tx,
        attempt,
        FailureCode::class_only(FailureClass::NodeLost),
        why,
        gone,
    )
    .await
}

async fn lose_with_failure(
    tx: &mut Tx,
    attempt: &AttemptRow,
    failure: FailureCode,
    why: &str,
    gone: bool,
) -> Result<(), SchedError> {
    let change = AttemptChange {
        failure: Some(failure),
        ..AttemptChange::default()
    };
    JobTransitions::apply_attempt(tx, attempt, JobAttemptEvent::LoseLease, change).await?;
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM resource_reservations WHERE attempt_id = $1
           AND state IN ('HELD','ACTIVE','RELEASING') ORDER BY id",
    )
    .bind(attempt.id)
    .fetch_all(&mut **tx)
    .await?;
    for id in ids {
        if let Some(r) = reservations::lock(tx, id).await? {
            if gone {
                reservations::release(tx, &r, why).await?;
            } else {
                reservations::quarantine(tx, &r, why).await?;
            }
        }
    }
    fate(tx, attempt.org_id, attempt.job_id, failure, why).await
}

/// Ends a still-PENDING attempt. A possibly delivered launch is fenced and keeps its hold.
pub(crate) async fn end_unstarted(
    core: &Core,
    attempt: &AttemptRow,
    class: FailureClass,
    why: String,
) -> Result<(), SchedError> {
    let mut tx = core.db.begin().await?;
    let Some(a) = JobTransitions::lock_attempt(&mut tx, attempt.org_id, attempt.id).await? else {
        return Ok(());
    };
    if a.attempt_state()? != JobAttemptState::Pending {
        return Ok(());
    }
    // The gateway locks each command when it claims it. Withdraw unsent commands
    // and keep the hold if any launch may already have left the core.
    sqlx::query(
        "UPDATE agent_commands SET state = 'SUPERSEDED', updated_at = now()
         WHERE attempt_id = $1 AND epoch = $2 AND state = 'PENDING'
           AND kind IN ('LaunchAttempt','StageArtifacts','UpdateCredentials')",
    )
    .bind(a.id)
    .bind(a.epoch)
    .execute(&mut *tx)
    .await?;
    let may_have_started: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM agent_commands
         WHERE attempt_id = $1 AND epoch = $2 AND kind = 'LaunchAttempt'
           AND sent_at IS NOT NULL AND (
             state <> 'FAILED' OR COALESCE(result->'failure'->>'sub_code', '') NOT IN
               ('node_draining', 'unsupported_command', 'MRL-VAL-200', 'MRL-VAL-205', 'MRL-NODE-206', 'MRL-NODE-207', 'MRL-STATE-210')
           ))",
    )
    .bind(a.id)
    .bind(a.epoch)
    .fetch_one(&mut *tx)
    .await?;
    if may_have_started {
        let a = acknowledge(&mut tx, a).await?;
        lose_with_failure(&mut tx, &a, FailureCode::class_only(class), &why, false).await?;
        let job = lock_job(&mut tx, a.org_id, a.job_id).await?;
        core.audit_job(&mut tx, &job, "scheduler.launch_lost")
            .await?;
        tx.commit().await?;
        return Ok(());
    }
    let failure = FailureCode::class_only(class);
    let change = AttemptChange {
        failure: Some(failure),
        ..AttemptChange::default()
    };
    JobTransitions::apply_attempt(&mut tx, &a, JobAttemptEvent::Kill, change).await?;
    release_for_attempt(core, &mut tx, a.id, &why).await?;
    fate(&mut tx, a.org_id, a.job_id, failure, &why).await?;
    let job = lock_job(&mut tx, a.org_id, a.job_id).await?;
    core.audit_job(&mut tx, &job, "scheduler.launch_ended")
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Applies a reconcile report: each status, then the attempts the machine no longer has.
pub(crate) async fn on_reconcile(
    core: &Core,
    node: Uuid,
    seq: i64,
    report: &ReconcileReport,
) -> Result<(), SchedError> {
    let newest: Option<i64> =
        sqlx::query_scalar("SELECT seq FROM scheduler_node_reconciles WHERE node_id = $1")
            .bind(node)
            .fetch_optional(core.db.pool())
            .await?;
    if newest.is_some_and(|n| n >= seq) {
        return Ok(());
    }
    let mut listed = HashSet::new();
    let mut running = HashSet::new();
    for s in &report.attempts {
        if let Some(id) = s
            .attempt
            .as_ref()
            .and_then(|r| AttemptId::from_str(&r.attempt_id).ok())
        {
            listed.insert(id.as_uuid());
            if live_phase(s.phase) {
                running.insert(id.as_uuid());
            }
        }
        on_status(core, node, seq, s).await?;
    }
    // Live here, not in the report, and nothing newer heard: the machine lost it.
    let missing: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT a.id, a.org_id FROM job_attempts a
         JOIN attempt_placements p ON p.attempt_id = a.id AND p.node_id = $1
         LEFT JOIN scheduler_attempt_reports s ON s.attempt_id = a.id
         WHERE a.state IN ('LAUNCHING','RUNNING') AND COALESCE(s.seq, 0) < $2",
    )
    .bind(node)
    .bind(seq)
    .fetch_all(core.db.pool())
    .await?;
    for (id, org) in missing.into_iter().filter(|(id, _)| !listed.contains(id)) {
        let mut tx = core.db.begin().await?;
        if let Some(a) = JobTransitions::lock_attempt(&mut tx, org, id).await? {
            if matches!(
                a.attempt_state()?,
                JobAttemptState::Launching | JobAttemptState::Running
            ) {
                lose(
                    &mut tx,
                    &a,
                    "The machine restarted without this attempt.",
                    true,
                )
                .await?;
                let job = lock_job(&mut tx, org, a.job_id).await?;
                core.audit_job(&mut tx, &job, "scheduler.reconcile_lost")
                    .await?;
            }
        }
        tx.commit().await?;
    }
    // For a possibly delivered launch with no status, an empty startup report can predate
    // the launch. Keep its hold until a terminal report or decommission proves it stopped.
    let quarantined: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT r.id, r.attempt_id FROM resource_reservations r
         LEFT JOIN job_attempts a ON a.id = r.attempt_id
         LEFT JOIN scheduler_attempt_reports s ON s.attempt_id = r.attempt_id
         WHERE r.node_id = $1 AND s.seq <= $2
           AND (r.state = 'QUARANTINED' OR (r.state = 'RELEASING' AND a.state = 'LOST'))",
    )
    .bind(node)
    .bind(seq)
    .fetch_all(core.db.pool())
    .await?;
    for (id, attempt) in quarantined {
        if attempt.is_some_and(|a| running.contains(&a)) {
            continue;
        }
        let mut tx = core.db.begin().await?;
        if let Some(r) = reservations::lock(&mut tx, id).await? {
            let ended =
                reservations::release(&mut tx, &r, "the machine no longer runs the attempt")
                    .await?;
            if ended.state_version != r.state_version {
                core.audit_reservation(&mut tx, &ended, "scheduler.reconcile_released")
                    .await?;
            }
        }
        tx.commit().await?;
    }
    sqlx::query(
        "INSERT INTO scheduler_node_reconciles (node_id, seq) VALUES ($1, $2)
         ON CONFLICT (node_id) DO UPDATE SET seq = EXCLUDED.seq, applied_at = now()
           WHERE scheduler_node_reconciles.seq < EXCLUDED.seq",
    )
    .bind(node)
    .bind(seq)
    .execute(core.db.pool())
    .await?;
    Ok(())
}

/// Sends a cancel for a specific intent. Failed and expired commands can be redelivered.
pub(crate) async fn cancel(
    core: &Core,
    node: Uuid,
    attempt: AttemptRef,
    reason: &str,
    grace: Duration,
) -> Result<(), SchedError> {
    let Ok(id) = AttemptId::from_str(&attempt.attempt_id) else {
        return Ok(());
    };
    let epoch = i64::try_from(attempt.fencing_epoch).unwrap_or(i64::MAX);
    let prior: Option<(String, Vec<u8>, f64, i64)> = sqlx::query_as(
        "SELECT state, payload, extract(epoch from now() - updated_at)::float8,
                count(*) FILTER (WHERE state = 'FAILED') OVER ()
         FROM agent_commands WHERE node_id = $1 AND kind = 'CancelAttempt'
           AND attempt_id = $2 AND epoch = $3 ORDER BY seq DESC LIMIT 1",
    )
    .bind(node)
    .bind(id.as_uuid())
    .bind(epoch)
    .fetch_optional(core.db.pool())
    .await?;
    let epoch = attempt.fencing_epoch;
    let request_id = format!("{}-{}-{epoch}", reason.to_lowercase(), attempt.attempt_id);
    if prior.is_some_and(|(state, payload, age, failed)| {
        let same_request = matches!(
            mrl_proto::agent::v1::Command::decode(payload.as_slice()).map(|c| c.kind),
            Ok(Some(Kind::CancelAttempt(cmd))) if cmd.request_id == request_id
        );
        same_request
            && (matches!(
                state.as_str(),
                "PENDING" | "SENT" | "RECEIVED" | "COMPLETED"
            ) || (state == "FAILED" && age < refused_cancel_backoff(failed)))
    }) {
        return Ok(());
    }
    let k = Kind::CancelAttempt(CancelAttempt {
        request_id,
        attempt: Some(attempt),
        grace_s: u32::try_from(grace.as_secs()).unwrap_or(u32::MAX),
        reason: reason.to_owned(),
    });
    let cmd = NewCommand::new(k)
        .attempt(id, epoch)
        .deadline(Duration::from_secs(core.cfg.cancel_deadline_seconds));
    match core.sender.send(NodeId::from_uuid(node), cmd).await {
        Ok(_) => Ok(()),
        // A removed machine runs nothing any more.
        Err(crate::modules::nodes::NodesError::CommandRefused(why)) => {
            tracing::info!(node = %node, why, "cancel not sent");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// Seconds to wait before sending a cancel again after `failed` refusals: 5, 10, 20, 40,
/// then 60. A refusal can mean the launch is still being taken, so the cancel is retried; the
/// lease ends an attempt the machine never runs.
fn refused_cancel_backoff(failed: i64) -> f64 {
    let doublings = u32::try_from(failed.saturating_sub(1).clamp(0, 4)).unwrap_or(4);
    (5.0 * f64::from(1u32 << doublings)).min(60.0)
}

/// Runs the work queued after a commit.
pub(crate) async fn run(core: &Core, after: Vec<After>) -> Result<(), SchedError> {
    for a in after {
        match a {
            After::Cancel {
                node,
                attempt,
                reason,
            } => {
                cancel(core, node, attempt, reason, Duration::from_secs(0)).await?;
            }
            After::Finalize { org, job } => finalize(core, org, job).await?,
        }
    }
    Ok(())
}

/// Verifies a FINALIZING job (the verifiers run with no write transaction open).
pub(crate) async fn finalize(core: &Core, org: Uuid, job: Uuid) -> Result<(), SchedError> {
    let row: Option<crate::modules::jobs::JobRow> = {
        let mut tx = core.db.begin().await?;
        let r = JobTransitions::lock(&mut tx, org, job).await?;
        tx.rollback().await?;
        r
    };
    let Some(row) = row.filter(|r| r.state == "FINALIZING") else {
        return Ok(());
    };
    let verdict = core.chains.verify(&row).await?;
    let mut tx = core.db.begin().await?;
    let Some(locked) = JobTransitions::lock(&mut tx, org, job).await? else {
        return Ok(());
    };
    JobTransitions::verify_completion(&mut tx, &locked, verdict).await?;
    core.audit_job(&mut tx, &locked, "scheduler.finalize")
        .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::common::v1::Failure;

    #[test]
    fn refused_cancels_back_off_to_a_minute() {
        let waits: Vec<f64> = (1..=7).map(super::refused_cancel_backoff).collect();
        assert_eq!(waits, [5.0, 10.0, 20.0, 40.0, 60.0, 60.0, 60.0]);
        assert_eq!(super::refused_cancel_backoff(0), 5.0);
    }

    #[test]
    fn agent_text_is_stored_without_nul_and_bounded() {
        let s = AttemptStatus {
            failure: Some(Failure {
                class: FailureClass::Oom.as_i32(),
                sub_code: "OOM\0_GPU".into(),
                message: format!("bad\0text{}", "x".repeat(10_000)),
                ..Failure::default()
            }),
            ..AttemptStatus::default()
        };
        let r = exit_report(&s).to_string();
        assert!(!r.contains("\\u0000"), "{r}");
        let (code, msg) = failure_of(&s, FailureClass::HarnessError);
        assert_eq!(code.class(), FailureClass::Oom);
        assert!(msg.starts_with("badtext") && msg.chars().count() == 4096);
        let (code, msg) = failure_of(&AttemptStatus::default(), FailureClass::Preempted);
        assert_eq!(
            (code.class(), msg.as_str()),
            (FailureClass::Preempted, "The attempt ended with PREEMPTED.")
        );
    }

    #[test]
    fn only_exited_and_self_fenced_are_not_live() {
        for p in [
            AttemptPhase::Staging,
            AttemptPhase::Running,
            AttemptPhase::Stopping,
            AttemptPhase::Unspecified,
        ] {
            assert!(live_phase(p as i32));
        }
        for p in [AttemptPhase::Exited, AttemptPhase::SelfFenced] {
            assert!(!live_phase(p as i32));
        }
    }
}
