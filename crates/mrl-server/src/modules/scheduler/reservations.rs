//! The only writer of `resource_reservations.state` (and `reservation_devices.active`).
//!
//! HELD → ACTIVE → RELEASING → RELEASED, HELD → EXPIRED, ACTIVE → QUARANTINED → RELEASED
//! (P06's `Reservation` machine). Every step checks the machine, compare-and-sets on
//! `state_version`, keeps the GPUs held exactly while the state holds them (HELD, ACTIVE,
//! RELEASING, QUARANTINED) and writes a `reservation.*` outbox event, all in the caller's
//! transaction. Lock order everywhere: job, attempt, machine, reservation.

use super::SchedError;
use crate::db::Tx;
use crate::modules::events::{NewEvent, OutboxWriter};
use mrl_domain::ids::{JobId, NodeId, ReservationId};
use mrl_domain::resources::ReservationOwner;
use mrl_domain::state::{Reservation, ReservationEvent, ReservationState, StateMachine};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

/// Columns of [`ReservationRow`].
macro_rules! cols {
    () => {
        "id, org_id, owner_kind, job_id, deployment_revision_id, attempt_id, pool_id, node_id, \
         state, state_version, epoch, cpu_millis, mem_bytes, placement, lease_expires_at, reason, \
         created_at, released_at"
    };
}

/// A reservation as stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ReservationRow {
    /// Id (`rsv_…`).
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// `job` or `deployment`.
    pub owner_kind: String,
    /// The job, for a job.
    pub job_id: Option<Uuid>,
    /// The deployment revision, for a deployment.
    pub deployment_revision_id: Option<Uuid>,
    /// The attempt using it, once launched.
    pub attempt_id: Option<Uuid>,
    /// Pool.
    pub pool_id: Uuid,
    /// Machine.
    pub node_id: Uuid,
    /// HELD, ACTIVE, RELEASING, RELEASED, EXPIRED or QUARANTINED.
    pub state: String,
    /// Bumped by every step (compare-and-set).
    pub state_version: i64,
    /// Fencing token (the attempt's job epoch).
    pub epoch: i64,
    /// CPU held, thousandths of a core.
    pub cpu_millis: i64,
    /// Memory held, bytes.
    pub mem_bytes: i64,
    /// GPU indices per role, like `{"trainer": [0, 1]}`.
    pub placement: serde_json::Value,
    /// When the hold lapses unless renewed.
    pub lease_expires_at: time::OffsetDateTime,
    /// Why it last changed.
    pub reason: Option<String>,
    /// Created.
    pub created_at: time::OffsetDateTime,
    /// Released or expired.
    pub released_at: Option<time::OffsetDateTime>,
}

impl ReservationRow {
    /// Its state.
    pub fn state(&self) -> Result<ReservationState, SchedError> {
        self.state
            .parse()
            .map_err(|_| SchedError::internal(format!("unknown reservation state {}", self.state)))
    }

    /// The GPUs it holds while live.
    pub fn is_live(&self) -> bool {
        holds_devices(&self.state)
    }
}

fn holds_devices(state: &str) -> bool {
    matches!(state, "HELD" | "ACTIVE" | "RELEASING" | "QUARANTINED")
}

/// A reservation to hold.
#[derive(Debug, Clone, PartialEq)]
pub struct NewReservation {
    /// New id.
    pub id: Uuid,
    /// Owning org.
    pub org_id: Uuid,
    /// The job or deployment.
    pub owner: ReservationOwner,
    /// Pool.
    pub pool_id: Uuid,
    /// Machine.
    pub node_id: Uuid,
    /// `gpu_devices.id`s.
    pub gpus: Vec<Uuid>,
    /// GPU indices per role, like `{"trainer": [0, 1]}`.
    pub roles: serde_json::Value,
    /// CPU, thousandths of a core.
    pub cpu_millis: u64,
    /// Memory, bytes.
    pub mem_bytes: u64,
    /// Fencing epoch.
    pub epoch: i64,
    /// How long the hold lasts before it must be renewed or used.
    pub lease: Duration,
}

fn int(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Inserts a HELD reservation and its GPUs, and writes `reservation.held`.
/// A GPU already held by another live reservation fails on `one_holder_per_gpu`.
pub async fn hold(tx: &mut Tx, new: &NewReservation) -> Result<ReservationRow, SchedError> {
    let (kind, job, deployment) = match new.owner {
        ReservationOwner::Job(j) => ("job", Some(j.as_uuid()), None),
        ReservationOwner::Deployment(d) => ("deployment", None, Some(d.as_uuid())),
    };
    let row = sqlx::query_as::<_, ReservationRow>(concat!(
        "INSERT INTO resource_reservations (id, org_id, owner_kind, job_id, deployment_revision_id, \
           pool_id, node_id, state, epoch, cpu_millis, mem_bytes, placement, lease_expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'HELD', $8, $9, $10, $12, now() + make_interval(secs => $11)) \
         RETURNING ",
        cols!()
    ))
    .bind(new.id)
    .bind(new.org_id)
    .bind(kind)
    .bind(job)
    .bind(deployment)
    .bind(new.pool_id)
    .bind(new.node_id)
    .bind(new.epoch)
    .bind(int(new.cpu_millis))
    .bind(int(new.mem_bytes))
    .bind(new.lease.as_secs_f64())
    .bind(&new.roles)
    .fetch_one(&mut **tx)
    .await?;
    for gpu in &new.gpus {
        sqlx::query(
            "INSERT INTO reservation_devices (reservation_id, gpu_device_id) VALUES ($1, $2)",
        )
        .bind(row.id)
        .bind(gpu)
        .execute(&mut **tx)
        .await?;
    }
    let payload = json!({
        "to": "HELD",
        "owner_kind": kind,
        "job_id": job.map(|j| JobId::from_uuid(j).to_string()),
        "node_id": NodeId::from_uuid(row.node_id).to_string(),
        "gpus": new.gpus.len(),
        "epoch": row.epoch,
    });
    let e = NewEvent::new(
        Reservation::CREATED_EVENT,
        "reservation",
        row.id,
        0,
        payload,
    )
    .map_err(|e| SchedError::internal(e.to_string()))?
    .org(row.org_id);
    OutboxWriter::write(tx, &e).await?;
    Ok(row)
}

/// Reads and locks a reservation.
pub async fn lock(tx: &mut Tx, id: Uuid) -> Result<Option<ReservationRow>, SchedError> {
    Ok(sqlx::query_as::<_, ReservationRow>(concat!(
        "SELECT ",
        cols!(),
        " FROM resource_reservations WHERE id = $1 FOR UPDATE"
    ))
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?)
}

/// Locks the job's reservations that still hold GPUs, oldest first.
pub async fn live_for_job(tx: &mut Tx, job: Uuid) -> Result<Vec<ReservationRow>, SchedError> {
    Ok(sqlx::query_as::<_, ReservationRow>(concat!(
        "SELECT ",
        cols!(),
        " FROM resource_reservations WHERE job_id = $1 \
           AND state IN ('HELD','ACTIVE','RELEASING','QUARANTINED') ORDER BY created_at FOR UPDATE"
    ))
    .bind(job)
    .fetch_all(&mut **tx)
    .await?)
}

/// Applies `event` to the reservation `row` was read as (compare-and-set on `state_version`).
pub async fn settle(
    tx: &mut Tx,
    row: &ReservationRow,
    event: ReservationEvent,
    why: &str,
) -> Result<ReservationRow, SchedError> {
    step(tx, row, event, why, None).await
}

async fn step(
    tx: &mut Tx,
    row: &ReservationRow,
    event: ReservationEvent,
    why: &str,
    attempt: Option<(Uuid, i64)>,
) -> Result<ReservationRow, SchedError> {
    let from = row.state()?;
    let to = Reservation::can_transition(from, event)
        .map_err(|e| SchedError::internal(format!("reservation {}: {e}", row.id)))?;
    let (attempt_id, epoch) = attempt.unwrap_or((Uuid::nil(), row.epoch));
    let next = sqlx::query_as::<_, ReservationRow>(concat!(
        "UPDATE resource_reservations SET state = $2, state_version = state_version + 1, \
           reason = $3, updated_at = now(), \
           attempt_id = CASE WHEN $4 = '00000000-0000-0000-0000-000000000000'::uuid THEN attempt_id ELSE $4 END, \
           epoch = $5, \
           released_at = CASE WHEN $2 IN ('RELEASED','EXPIRED') THEN now() ELSE released_at END \
         WHERE id = $1 AND state_version = $6 RETURNING ",
        cols!()
    ))
    .bind(row.id)
    .bind(to.as_str())
    .bind(why)
    .bind(attempt_id)
    .bind(epoch)
    .bind(row.state_version)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| SchedError::changed("reservation"))?;
    if !holds_devices(&next.state) {
        sqlx::query("UPDATE reservation_devices SET active = false WHERE reservation_id = $1")
            .bind(next.id)
            .execute(&mut **tx)
            .await?;
    }
    let payload = json!({
        "from": from.as_str(),
        "to": next.state,
        "reason": why,
        "epoch": next.epoch,
        "node_id": NodeId::from_uuid(next.node_id).to_string(),
    });
    let e = NewEvent::new(
        event.event_type(),
        "reservation",
        next.id,
        next.state_version,
        payload,
    )
    .map_err(|e| SchedError::internal(e.to_string()))?
    .org(next.org_id);
    OutboxWriter::write(tx, &e).await?;
    Ok(next)
}

/// HELD → ACTIVE for `attempt` at `epoch` (the attempt now uses it).
pub async fn activate(
    tx: &mut Tx,
    row: &ReservationRow,
    attempt: Uuid,
    epoch: i64,
) -> Result<ReservationRow, SchedError> {
    step(
        tx,
        row,
        ReservationEvent::Activate,
        "attempt launched",
        Some((attempt, epoch)),
    )
    .await
}

/// Gives a reservation back, whatever live state it is in (→ RELEASED; a HELD one that was
/// never used → EXPIRED). An ended one is returned unchanged.
pub async fn release(
    tx: &mut Tx,
    row: &ReservationRow,
    why: &str,
) -> Result<ReservationRow, SchedError> {
    match row.state()? {
        ReservationState::Held => settle(tx, row, ReservationEvent::Expire, why).await,
        ReservationState::Active => {
            let r = settle(tx, row, ReservationEvent::Release, why).await?;
            settle(tx, &r, ReservationEvent::ConfirmRelease, why).await
        }
        ReservationState::Releasing | ReservationState::Quarantined => {
            settle(tx, row, ReservationEvent::ConfirmRelease, why).await
        }
        ReservationState::Released | ReservationState::Expired => Ok(row.clone()),
    }
}

/// ACTIVE → QUARANTINED: the GPUs stay held until the machine confirms the old processes are
/// gone (a HELD one that was never used just expires). Others are returned unchanged.
pub async fn quarantine(
    tx: &mut Tx,
    row: &ReservationRow,
    why: &str,
) -> Result<ReservationRow, SchedError> {
    match row.state()? {
        ReservationState::Active => settle(tx, row, ReservationEvent::Quarantine, why).await,
        ReservationState::Held => settle(tx, row, ReservationEvent::Expire, why).await,
        _ => Ok(row.clone()),
    }
}

/// ACTIVE → RELEASING (a cancel was sent; the GPUs stay held until the attempt ends).
pub async fn start_release(
    tx: &mut Tx,
    row: &ReservationRow,
    why: &str,
) -> Result<ReservationRow, SchedError> {
    match row.state()? {
        ReservationState::Active => settle(tx, row, ReservationEvent::Release, why).await,
        _ => Ok(row.clone()),
    }
}

/// Moves the leases of every reservation `attempt` uses to `until` (never backwards).
pub async fn renew_for_attempt(
    tx: &mut Tx,
    attempt: Uuid,
    until: time::OffsetDateTime,
) -> Result<(), SchedError> {
    sqlx::query(
        "UPDATE resource_reservations SET lease_expires_at = GREATEST(lease_expires_at, $2)
         WHERE attempt_id = $1 AND state IN ('ACTIVE','RELEASING')",
    )
    .bind(attempt)
    .bind(until)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The id as users see it.
pub fn public_id(id: Uuid) -> String {
    ReservationId::from_uuid(id).to_string()
}
