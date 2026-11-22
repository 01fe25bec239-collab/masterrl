//! The leader-elected scheduler loop. One tick, in order:
//!
//! 1. Gap check (D-04-09): if the last tick (any leader) is older than
//!    `outage_threshold_seconds`, every live lease is extended by one lease length first, so a
//!    control-plane outage never makes an attempt LOST.
//! 2. Retries due: RETRY_WAIT → QUEUED.
//! 3. Leases: mirror the gateway's grants (the launch row's last renewal + the pool's lease)
//!    onto attempts and reservations; past lease + grace → LOST (epoch bump, GPUs
//!    quarantined, job retries); unused HELD holds past their lease expire.
//! 4. Launches to send (or that never took).
//! 5. Controls: cancels for CANCELLING jobs, pauses for PAUSING jobs, fenced cancels for
//!    quarantined attempts, holds on removed machines released.
//! 6. FINALIZING jobs verified.
//! 7. The queue: admission, placement, hold + launch in one transaction.
//!
//! Placement rechecks the job and capacity under lock. The `one_holder_per_gpu` index
//! protects allocations even if leadership changes mid-tick (D-04-08).

use super::control::{self, cancel, end_unstarted, lock_job, lose, refuse_pause};
use super::launch::{self, attempt_ref, latest_command};
use super::pipeline::{Admission, Demand, Fit, Placement, PlacementHints, PoolInfo, QueuedJob};
use super::reservations::{self, NewReservation};
use super::{codes, pools, Core, SchedError, WAKE_CHANNEL};
use crate::db::LeaderElection;
use crate::modules::compute::NodeCapacity;
use crate::modules::jobs::{JobChange, JobRow, JobTransitions};
use crate::modules::nodes::commands::NewCommand;
use mrl_domain::failure::FailureClass;
use mrl_domain::ids::{AttemptId, JobId, NodeId};
use mrl_domain::resources::ReservationOwner;
use mrl_domain::state::{JobAttemptState, JobEvent, JobState, Owner, ReservationEvent};
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::PauseAttempt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Runs the loop while this replica leads `scheduler`.
pub async fn run(core: Arc<Core>, cancel: CancellationToken) {
    let election = LeaderElection::new(core.db.clone(), "scheduler", Duration::from_secs(10));
    election
        .run(cancel, |lead| {
            let core = core.clone();
            async move {
                let since = Instant::now();
                let every = Duration::from_secs(core.cfg.tick_seconds);
                loop {
                    once(&core, since.elapsed()).await;
                    tokio::select! {
                        () = lead.cancelled() => return Ok(()),
                        () = core.wake.notified() => {}
                        () = tokio::time::sleep(every) => {}
                    }
                }
            }
        })
        .await;
}

/// Wakes the loop when any replica notifies [`WAKE_CHANNEL`].
pub async fn listen(core: Arc<Core>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        let listener = async {
            let mut l = sqlx::postgres::PgListener::connect_with(core.db.pool()).await?;
            l.listen(WAKE_CHANNEL).await?;
            loop {
                l.recv().await?;
                core.wake.notify_one();
            }
            #[allow(unreachable_code)]
            Ok::<(), sqlx::Error>(())
        };
        tokio::select! {
            () = cancel.cancelled() => return,
            r = listener => if let Err(e) = r {
                tracing::debug!(error = %e, "scheduler wake listener stopped; the tick runs meanwhile");
            }
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

fn log(pass: &str, r: Result<(), SchedError>) {
    if let Err(e) = r {
        if e.is_race() {
            tracing::debug!(pass, error = %e, "scheduler step lost a race; it runs again next tick");
        } else {
            tracing::warn!(pass, error = %e, "scheduler step failed");
        }
    }
}

/// One tick (public for tests: drive the scheduler step by step).
pub async fn once(core: &Core, leader_for: Duration) {
    if let Err(e) = gap(core).await {
        log("gap", Err(e));
        return;
    }
    log("retries", retries(core).await);
    log("leases", leases(core).await);
    log("launches", launches(core, leader_for).await);
    log("controls", controls(core).await);
    log("finalize", finalize_all(core).await);
    log("queue", queue(core, leader_for).await);
}

async fn gap(core: &Core) -> Result<(), SchedError> {
    let mut tx = core.db.begin().await?;
    let gap: Option<f64> = sqlx::query_scalar(
        "SELECT extract(epoch from now() - last_tick_at)::float8 FROM scheduler_ticks WHERE id = 1 FOR UPDATE",
    )
    .fetch_optional(&mut *tx)
    .await?;
    let threshold = core.cfg.outage_threshold_seconds as f64;
    if gap.is_none_or(|g| g > threshold) {
        let n = sqlx::query(
            "UPDATE job_attempts a SET lease_expires_at = GREATEST(a.lease_expires_at, now() + p.attempt_lease_ttl)
             FROM resource_reservations r JOIN compute_pools p ON p.id = r.pool_id
             WHERE r.attempt_id = a.id AND a.state IN ('LAUNCHING','RUNNING')",
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();
        sqlx::query(
            "UPDATE resource_reservations r SET lease_expires_at = GREATEST(r.lease_expires_at, now() + p.attempt_lease_ttl)
             FROM compute_pools p WHERE p.id = r.pool_id AND r.state IN ('HELD','ACTIVE','RELEASING')",
        )
        .execute(&mut *tx)
        .await?;
        if let Some(elapsed) = gap {
            sqlx::query(
                "UPDATE agent_commands c SET deadline_at = deadline_at + make_interval(secs => $1),
                   state = CASE WHEN state = 'EXPIRED' THEN 'PENDING' ELSE state END
                 WHERE kind IN ('LaunchAttempt','StageArtifacts','UpdateCredentials','CancelAttempt','PauseAttempt')
                   AND EXISTS (SELECT 1 FROM attempt_placements p
                     JOIN job_attempts a ON a.id = p.attempt_id JOIN jobs j ON j.id = a.job_id
                     WHERE p.attempt_id = c.attempt_id AND p.node_id = c.node_id AND a.epoch = c.epoch
                       AND (c.kind = 'CancelAttempt' OR
                         (j.current_attempt_id = a.id AND j.epoch = a.epoch AND (
                           (c.kind = 'PauseAttempt' AND j.state = 'PAUSING') OR
                           (c.kind IN ('LaunchAttempt','StageArtifacts','UpdateCredentials')
                               AND j.state = 'PREPARING' AND a.state = 'PENDING')
                         )))
                   )
                   AND deadline_at BETWEEN now() - make_interval(secs => $1) AND now()
                   AND (state IN ('PENDING','SENT') OR (
                     state = 'EXPIRED' AND sent_at IS NULL
                       AND NOT EXISTS (SELECT 1 FROM agent_commands newer
                         WHERE newer.node_id = c.node_id AND newer.seq > c.seq AND newer.sent_at IS NOT NULL)
                   ))",
            )
            .bind(elapsed)
            .execute(&mut *tx)
            .await?;
            tracing::warn!(
                gap_seconds = gap,
                extended = n,
                "the scheduler was not running for a while; live leases were extended"
            );
        }
    }
    sqlx::query(
        "INSERT INTO scheduler_ticks (id, last_tick_at) VALUES (1, now())
         ON CONFLICT (id) DO UPDATE SET last_tick_at = now()",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn retries(core: &Core) -> Result<(), SchedError> {
    let due: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT org_id, id FROM jobs WHERE state = 'RETRY_WAIT' AND next_eligible_at <= now()
         ORDER BY next_eligible_at LIMIT 500",
    )
    .fetch_all(core.db.pool())
    .await?;
    for (org, id) in due {
        log(
            "retry",
            async {
                let mut tx = core.db.begin().await?;
                if let Some(row) = JobTransitions::lock(&mut tx, org, id).await? {
                    if row.job_state()? == JobState::RetryWait {
                        JobTransitions::apply(
                            &mut tx,
                            &row,
                            JobEvent::Retry,
                            Owner::Scheduler,
                            JobChange::default(),
                        )
                        .await?;
                        core.audit_job(&mut tx, &row, "scheduler.retry_ready")
                            .await?;
                    }
                }
                tx.commit().await?;
                Ok(())
            }
            .await,
        );
    }
    Ok(())
}

async fn leases(core: &Core) -> Result<(), SchedError> {
    // The gateway grants a lease on each heartbeat by touching the launch row; the server's
    // view starts at that touch, never before the agent's own deadline.
    sqlx::query(
        "UPDATE job_attempts a SET lease_expires_at = g.until FROM (
           SELECT c.attempt_id, c.epoch, max(c.updated_at + p.attempt_lease_ttl) AS until
           FROM agent_commands c JOIN compute_nodes n ON n.id = c.node_id
           JOIN compute_pools p ON p.id = n.pool_id
           WHERE c.kind = 'LaunchAttempt' AND c.state IN ('SENT','RECEIVED','COMPLETED')
             AND c.attempt_id IN (SELECT id FROM job_attempts WHERE state IN ('LAUNCHING','RUNNING'))
           GROUP BY c.attempt_id, c.epoch) g
         WHERE g.attempt_id = a.id AND g.epoch = a.epoch AND a.state IN ('LAUNCHING','RUNNING')
           AND (a.lease_expires_at IS NULL OR a.lease_expires_at < g.until)",
    )
    .execute(core.db.pool())
    .await?;
    sqlx::query(
        "UPDATE resource_reservations r SET lease_expires_at = a.lease_expires_at
         FROM job_attempts a WHERE r.attempt_id = a.id AND r.state IN ('ACTIVE','RELEASING')
           AND a.lease_expires_at > r.lease_expires_at",
    )
    .execute(core.db.pool())
    .await?;
    let grace = core.cfg.lease_grace_seconds as f64;
    let expired: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT org_id, id FROM job_attempts WHERE state IN ('LAUNCHING','RUNNING')
           AND lease_expires_at < now() - make_interval(secs => $1)
         ORDER BY lease_expires_at LIMIT 200",
    )
    .bind(grace)
    .fetch_all(core.db.pool())
    .await?;
    for (org, id) in expired {
        log("lease", async {
            let mut tx = core.db.begin().await?;
            if let Some(a) = JobTransitions::lock_attempt(&mut tx, org, id).await? {
                if !matches!(a.attempt_state()?, JobAttemptState::Launching | JobAttemptState::Running) {
                    return Ok(());
                }
                // A heartbeat can have renewed the command since the bulk mirror.
                let grants: Vec<time::OffsetDateTime> = sqlx::query_scalar(
                    "SELECT c.updated_at + p.attempt_lease_ttl FROM agent_commands c
                     JOIN compute_nodes n ON n.id = c.node_id
                     JOIN compute_pools p ON p.id = n.pool_id
                     WHERE c.kind = 'LaunchAttempt' AND c.attempt_id = $1 AND c.epoch = $2
                       AND c.state IN ('SENT','RECEIVED','COMPLETED')
                     ORDER BY c.id FOR UPDATE OF c",
                )
                .bind(id)
                .bind(a.epoch)
                .fetch_all(&mut *tx)
                .await?;
                let until = grants.into_iter().chain(a.lease_expires_at).max();
                let still: Option<bool> = sqlx::query_scalar(
                    "SELECT $1::timestamptz < clock_timestamp() - make_interval(secs => $2)",
                )
                .bind(until)
                .bind(grace)
                .fetch_one(&mut *tx)
                .await?;
                if still == Some(true) {
                    tracing::warn!(attempt = %AttemptId::from_uuid(id), "attempt lease ran out; it counts as lost");
                    lose(&mut tx, &a, "Contact with the machine was lost and the attempt's lease ran out.", false).await?;
                    let job = lock_job(&mut tx, org, a.job_id).await?;
                    core.audit_job(&mut tx, &job, "scheduler.lease_lost").await?;
                }
            }
            tx.commit().await?;
            Ok(())
        }
        .await);
    }
    let held: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM resource_reservations WHERE state = 'HELD' AND lease_expires_at < now() LIMIT 200",
    )
    .fetch_all(core.db.pool())
    .await?;
    for id in held {
        log(
            "hold",
            async {
                let mut tx = core.db.begin().await?;
                if let Some(r) = reservations::lock(&mut tx, id).await? {
                    let expired: bool =
                        sqlx::query_scalar("SELECT $1::timestamptz < clock_timestamp()")
                            .bind(r.lease_expires_at)
                            .fetch_one(&mut *tx)
                            .await?;
                    if r.state == "HELD" && expired {
                        let ended = reservations::settle(
                            &mut tx,
                            &r,
                            ReservationEvent::Expire,
                            "the hold was not used in time",
                        )
                        .await?;
                        core.audit_reservation(&mut tx, &ended, "scheduler.hold_expired")
                            .await?;
                    }
                }
                tx.commit().await?;
                Ok(())
            }
            .await,
        );
    }
    Ok(())
}

async fn launches(core: &Core, leader_for: Duration) -> Result<(), SchedError> {
    let pending: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT j.org_id, j.id FROM jobs j JOIN job_attempts a ON a.id = j.current_attempt_id
         WHERE j.state = 'PREPARING' AND a.state = 'PENDING'",
    )
    .fetch_all(core.db.pool())
    .await?;
    for (org, id) in pending {
        log(
            "launch",
            async {
                if let Some((job, attempt)) = launch::current(core, org, id).await? {
                    if attempt.attempt_state()? == JobAttemptState::Pending
                        && job.job_state()? == JobState::Preparing
                    {
                        launch::ensure_launched(core, &job, &attempt, leader_for).await?;
                    }
                }
                Ok(())
            }
            .await,
        );
    }
    Ok(())
}

/// The machine an attempt was placed on.
async fn node_of(core: &Core, attempt: Uuid) -> Result<Option<Uuid>, SchedError> {
    Ok(
        sqlx::query_scalar("SELECT node_id FROM attempt_placements WHERE attempt_id = $1 LIMIT 1")
            .bind(attempt)
            .fetch_optional(core.db.pool())
            .await?,
    )
}

async fn controls(core: &Core) -> Result<(), SchedError> {
    let busy: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT j.org_id, j.id FROM jobs j
         WHERE j.state IN ('CANCELLING','PAUSING') AND EXISTS (SELECT 1 FROM job_attempts a
           WHERE a.job_id = j.id AND a.state IN ('PENDING','LAUNCHING','RUNNING'))",
    )
    .fetch_all(core.db.pool())
    .await?;
    for (org, id) in busy {
        log(
            "control",
            async {
                let Some((job, attempt)) = launch::current(core, org, id).await? else {
                    return Ok(());
                };
                if !matches!(
                    attempt.attempt_state()?,
                    JobAttemptState::Pending
                        | JobAttemptState::Launching
                        | JobAttemptState::Running
                ) {
                    return Ok(());
                }
                let Some(node) = node_of(core, attempt.id).await? else {
                    return Ok(());
                };
                match job.job_state()? {
                    JobState::Cancelling => deliver_cancel(core, &attempt, node).await,
                    JobState::Pausing => deliver_pause(core, &job, &attempt, node).await,
                    _ => Ok(()),
                }
            }
            .await,
        );
    }
    // Quarantined attempts: the machine is told to stop them (sent when it reconnects).
    let quarantined: Vec<(Uuid, Uuid, Uuid)> = sqlx::query_as(
        "SELECT r.node_id, a.id, a.org_id FROM resource_reservations r
         JOIN job_attempts a ON a.id = r.attempt_id WHERE r.state = 'QUARANTINED'
            OR (r.state = 'RELEASING' AND a.state = 'LOST')",
    )
    .fetch_all(core.db.pool())
    .await?;
    for (node, attempt, org) in quarantined {
        log(
            "fence",
            async {
                let mut tx = core.db.begin().await?;
                let a = JobTransitions::lock_attempt(&mut tx, org, attempt).await?;
                tx.rollback().await?;
                if let Some(a) = a {
                    cancel(
                        core,
                        node,
                        attempt_ref(&a),
                        "FENCED",
                        Duration::from_secs(0),
                    )
                    .await?;
                }
                Ok(())
            }
            .await,
        );
    }
    // Holds on removed machines: nothing runs there any more.
    let gone: Vec<Uuid> = sqlx::query_scalar(
        "SELECT r.id FROM resource_reservations r JOIN compute_nodes n ON n.id = r.node_id
         WHERE n.status = 'DECOMMISSIONED' AND r.state IN ('HELD','ACTIVE','RELEASING','QUARANTINED') LIMIT 500",
    )
    .fetch_all(core.db.pool())
    .await?;
    for id in gone {
        log(
            "removed",
            async {
                let mut tx = core.db.begin().await?;
                if let Some(r) = reservations::lock(&mut tx, id).await? {
                    let ended =
                        reservations::release(&mut tx, &r, "the machine was removed").await?;
                    if ended.state_version != r.state_version {
                        core.audit_reservation(&mut tx, &ended, "scheduler.node_removed")
                            .await?;
                    }
                }
                tx.commit().await?;
                Ok(())
            }
            .await,
        );
    }
    Ok(())
}

async fn deliver_cancel(
    core: &Core,
    attempt: &crate::modules::jobs::AttemptRow,
    node: Uuid,
) -> Result<(), SchedError> {
    if attempt.attempt_state()? == JobAttemptState::Pending {
        let launch = latest_command(&core.db, "LaunchAttempt", attempt).await?;
        if launch.as_ref().is_none_or(|row| {
            !row.sent || (!row.accepted() && (!row.in_flight() || row.past_deadline))
        }) {
            let why = "Cancelled before the machine confirmed it started.".to_owned();
            return end_unstarted(core, attempt, FailureClass::CancelledByUser, why).await;
        }
    }
    let grace = Duration::from_secs(u64::from(core.cfg.cancel_grace_seconds));
    cancel(core, node, attempt_ref(attempt), "USER", grace).await?;
    let mut tx = core.db.begin().await?;
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM resource_reservations WHERE attempt_id = $1 AND state = 'ACTIVE'",
    )
    .bind(attempt.id)
    .fetch_all(&mut *tx)
    .await?;
    for id in ids {
        if let Some(r) = reservations::lock(&mut tx, id).await? {
            let releasing = reservations::start_release(&mut tx, &r, "cancel sent").await?;
            if releasing.state_version != r.state_version {
                core.audit_reservation(&mut tx, &releasing, "scheduler.cancel_sent")
                    .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

async fn deliver_pause(
    core: &Core,
    job: &JobRow,
    attempt: &crate::modules::jobs::AttemptRow,
    node: Uuid,
) -> Result<(), SchedError> {
    let refuse = |why: String| async move {
        let mut tx = core.db.begin().await?;
        let row = lock_job(&mut tx, job.org_id, job.id).await?;
        if row.job_state()? == JobState::Pausing && row.state_version == job.state_version {
            refuse_pause(&mut tx, &row, &why).await?;
            core.audit_job(&mut tx, &row, "scheduler.pause_refused")
                .await?;
        }
        tx.commit().await?;
        Ok::<(), SchedError>(())
    };
    if !core.pipeline.can_pause(job).await? {
        return refuse(
            "This training method can't pause, so the run keeps going. Cancel it to stop it."
                .into(),
        )
        .await;
    }
    let epoch = u64::try_from(attempt.epoch).unwrap_or(0);
    let request_id = format!(
        "pause-{}-{epoch}-{}",
        AttemptId::from_uuid(attempt.id),
        job.state_version
    );
    match latest_command(&core.db, "PauseAttempt", attempt)
        .await?
        .filter(|row| row.is_pause_request(&request_id))
    {
        None => {
            let k = Kind::PauseAttempt(PauseAttempt {
                attempt: Some(attempt_ref(attempt)),
                request_id,
                deadline_s: core.cfg.pause_deadline_seconds,
                reason: mrl_proto::worker::v1::PauseReason::User as i32,
            });
            let cmd = NewCommand::new(k)
                .attempt(AttemptId::from_uuid(attempt.id), epoch)
                .deadline(Duration::from_secs(core.cfg.cancel_deadline_seconds));
            core.sender.send(NodeId::from_uuid(node), cmd).await?;
            Ok(())
        }
        Some(row) if !row.in_flight() && !row.accepted() => refuse(row.failure_message()).await,
        Some(_) => Ok(()),
    }
}

async fn finalize_all(core: &Core) -> Result<(), SchedError> {
    let jobs: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT org_id, id FROM jobs WHERE state = 'FINALIZING'")
            .fetch_all(core.db.pool())
            .await?;
    for (org, id) in jobs {
        log("finalize", control::finalize(core, org, id).await);
    }
    Ok(())
}

/// Sets a QUEUED job's reason (no state change) when it differs.
async fn set_reason(
    core: &Core,
    job: &JobRow,
    code: &str,
    message: &str,
) -> Result<(), SchedError> {
    let reason = json!({ "code": code, "message": message });
    let mut tx = core.db.begin().await?;
    let changed = sqlx::query(
        "UPDATE jobs SET state_reason = $3, version = version + 1, updated_at = now()
         WHERE id = $1 AND org_id = $2 AND state = 'QUEUED' AND state_version = $4
           AND state_reason IS DISTINCT FROM $3",
    )
    .bind(job.id)
    .bind(job.org_id)
    .bind(&reason)
    .bind(job.state_version)
    .execute(&mut *tx)
    .await?;
    if changed.rows_affected() != 0 {
        core.audit_job(&mut tx, job, "scheduler.queue_reason")
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Machines earlier attempts of `job` failed on with a class that asks for another machine.
async fn avoid(core: &Core, job: Uuid) -> Result<Vec<Uuid>, SchedError> {
    Ok(sqlx::query_scalar(
        "SELECT DISTINCT p.node_id FROM job_attempts a JOIN attempt_placements p ON p.attempt_id = a.id
         WHERE a.job_id = $1 AND (a.failure_class LIKE 'NODE_LOST%' OR a.failure_class LIKE 'HARDWARE%')",
    )
    .bind(job)
    .fetch_all(core.db.pool())
    .await?)
}

/// Marks what a placement took in the tick's capacity snapshot.
fn take(nodes: &mut [NodeCapacity], p: &Placement) {
    let ids = p.gpu_ids();
    if let Some(n) = nodes.iter_mut().find(|n| n.node_id == p.node_id) {
        for g in &mut n.gpus {
            if ids.contains(&g.id) {
                g.free = false;
            }
        }
        n.cpu_millis_free = n.cpu_millis_free.saturating_sub(p.cpu_millis);
        n.mem_bytes_free = n.mem_bytes_free.saturating_sub(p.mem_bytes);
    }
}

async fn queue(core: &Core, leader_for: Duration) -> Result<(), SchedError> {
    let all = pools(&core.db, None).await?;
    let usable: Vec<PoolInfo> = all
        .into_iter()
        .filter(|p| {
            core.pipeline
                .providers
                .contains_key(p.provider_kind.as_str())
        })
        .collect();
    let mut capacity: HashMap<Uuid, Vec<NodeCapacity>> = HashMap::new();
    let mut ahead: HashMap<Uuid, u32> = HashMap::new();
    let limit = core.cfg.max_jobs_per_tick;
    let mut placed = 0;
    let (mut priority, mut queued_at, mut id) = (0i64, None, None);
    while placed < limit {
        let jobs: Vec<JobRow> = sqlx::query_as(concat!(
            "SELECT ",
            crate::modules::jobs::transitions::job_cols!(),
            " FROM jobs WHERE state = 'QUEUED'
               AND ($3::uuid IS NULL OR (-priority::bigint, COALESCE(queued_at, 'infinity'), id) >
                 (-$1::bigint, COALESCE($2::timestamptz, 'infinity'), $3))
             ORDER BY priority DESC, queued_at, id LIMIT $4"
        ))
        .bind(priority)
        .bind(queued_at)
        .bind(id)
        .bind(i64::from(limit))
        .fetch_all(core.db.pool())
        .await?;
        if jobs.is_empty() {
            break;
        }
        for job in jobs {
            (priority, queued_at, id) = (i64::from(job.priority), job.queued_at, Some(job.id));
            let n = ahead.entry(job.project_id).or_insert(0);
            let queued = QueuedJob {
                ahead_in_project: *n,
                job,
            };
            match place_one(core, &usable, &mut capacity, &queued, leader_for).await {
                Ok(true) => {
                    placed += 1;
                    if placed == limit {
                        break;
                    }
                }
                Ok(false) => *n = n.saturating_add(1),
                Err(e) => log("place", Err(e)),
            }
        }
    }
    Ok(())
}

async fn place_one(
    core: &Core,
    pools: &[PoolInfo],
    capacity: &mut HashMap<Uuid, Vec<NodeCapacity>>,
    q: &QueuedJob,
    leader_for: Duration,
) -> Result<bool, SchedError> {
    let job = &q.job;
    let Some(demand) = Demand::from_request(&job.resource_request) else {
        let msg = "The job's resource request can't be read, so it can't be placed. Submit it again with valid resources.";
        set_reason(core, job, &codes::NEVER_FITS.to_string(), msg).await?;
        return Ok(false);
    };
    let hints = PlacementHints {
        avoid_nodes: avoid(core, job.id).await?,
    };
    // The most useful reason across pools: busy beats never beats an admission hold.
    let mut why: Option<(u8, String, String)> = None;
    let mut keep = |rank: u8, code: String, msg: String| {
        if why.as_ref().is_none_or(|(r, _, _)| rank > *r) {
            why = Some((rank, code, msg));
        }
    };
    for pool in pools {
        match core.pipeline.admit(q, pool).await? {
            Admission::Admit => {}
            Admission::Hold { code, message } => {
                let rank = if code == codes::NO_POOL.to_string() {
                    0
                } else {
                    1
                };
                keep(rank, code, message);
                continue;
            }
        }
        let (Some(strategy), Some(provider)) = (
            core.pipeline.strategy(pool),
            core.pipeline.providers.get(pool.provider_kind.as_str()),
        ) else {
            continue;
        };
        if let std::collections::hash_map::Entry::Vacant(e) = capacity.entry(pool.id) {
            e.insert(provider.capacity(pool.id).await?);
        }
        let nodes = capacity
            .get(&pool.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        match strategy.place(&demand, nodes, &hints) {
            Fit::Placed(p) => match hold_and_launch(core, job, pool, &p).await? {
                Some((j, a)) => {
                    if let Some(nodes) = capacity.get_mut(&pool.id) {
                        take(nodes, &p);
                    }
                    tracing::info!(job = %JobId::from_uuid(j.id), node = %NodeId::from_uuid(p.node_id), "job placed");
                    log(
                        "launch",
                        launch::ensure_launched(core, &j, &a, leader_for).await,
                    );
                    return Ok(true);
                }
                None => {
                    // Taken meanwhile: read the pool again next time.
                    capacity.remove(&pool.id);
                    keep(
                        3,
                        codes::WAITING_FOR_MACHINE.to_string(),
                        "Waiting for a machine: the one chosen was taken meanwhile.".into(),
                    );
                }
            },
            Fit::Busy(msg) => keep(3, codes::WAITING_FOR_MACHINE.to_string(), msg),
            Fit::Never(msg) => keep(2, codes::NEVER_FITS.to_string(), msg),
        }
    }
    let (_, code, msg) = why.unwrap_or((
        0,
        codes::NO_POOL.to_string(),
        "No pool is set up to run jobs yet. Create a pool and add a machine to it.".into(),
    ));
    set_reason(core, job, &code, &msg).await?;
    Ok(false)
}

/// One transaction: QUEUED → ALLOCATING (pool set), the hold, ALLOCATING → PREPARING with a
/// new PENDING attempt at `epoch + 1`, the hold ACTIVE for it, and its placement rows.
/// `None`: the job or the machine changed meanwhile (nothing written).
async fn hold_and_launch(
    core: &Core,
    snapshot: &JobRow,
    pool: &PoolInfo,
    p: &Placement,
) -> Result<Option<(JobRow, crate::modules::jobs::AttemptRow)>, SchedError> {
    let Some(provider) = core.pipeline.providers.get(pool.provider_kind.as_str()) else {
        return Ok(None);
    };
    let mut tx = core.db.begin().await?;
    let Some(row) = JobTransitions::lock(&mut tx, snapshot.org_id, snapshot.id).await? else {
        return Ok(None);
    };
    if row.job_state()? != JobState::Queued || row.state_version != snapshot.state_version {
        return Ok(None);
    }
    let placed = JobTransitions::apply(
        &mut tx,
        &row,
        JobEvent::Place,
        Owner::Scheduler,
        JobChange::default(),
    )
    .await?;
    sqlx::query("UPDATE jobs SET pool_id = $3 WHERE id = $1 AND org_id = $2")
        .bind(placed.id)
        .bind(placed.org_id)
        .bind(pool.id)
        .execute(&mut *tx)
        .await?;
    let new = NewReservation {
        id: Uuid::now_v7(),
        org_id: placed.org_id,
        owner: ReservationOwner::Job(JobId::from_uuid(placed.id)),
        pool_id: pool.id,
        node_id: p.node_id,
        gpus: p.gpu_ids(),
        roles: launch::role_gpus(p),
        cpu_millis: p.cpu_millis,
        mem_bytes: p.mem_bytes,
        epoch: placed.epoch + 1,
        lease: pool.lease_ttl + Duration::from_secs(core.cfg.launch_deadline_seconds),
    };
    let held = match provider.place(&mut tx, &new).await {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(None),
        Err(e) if e.is_unique_violation("one_holder_per_gpu") => return Ok(None),
        Err(e) => return Err(e),
    };
    let resume: Option<Uuid> = match placed.run_id {
        Some(run) => sqlx::query_scalar(
            "SELECT latest_committed_checkpoint_id FROM runs WHERE id = $1 AND org_id = $2",
        )
        .bind(run)
        .bind(placed.org_id)
        .fetch_optional(&mut *tx)
        .await?
        .flatten(),
        None => None,
    };
    let (job, attempt) = JobTransitions::launch(&mut tx, &placed, resume).await?;
    reservations::activate(&mut tx, &held, attempt.id, attempt.epoch).await?;
    for (role, _) in &p.roles {
        sqlx::query(
            "INSERT INTO attempt_placements (attempt_id, node_id, org_id, node_rank, rank_lo, rank_hi, role)
             VALUES ($1, $2, $3, 0, 0, 0, $4)",
        )
        .bind(attempt.id)
        .bind(p.node_id)
        .bind(attempt.org_id)
        .bind(role)
        .execute(&mut *tx)
        .await?;
    }
    core.audit_job(&mut tx, &job, "scheduler.place").await?;
    tx.commit().await?;
    Ok(Some((job, attempt)))
}
