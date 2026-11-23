//! The scheduler (P21): queued jobs become placed, launched attempts.
//!
//! - A leader-elected loop ([`tick`]), woken by `job.*`/`node.*` events ([`Wake`]) and a
//!   periodic tick: retries due, lease mirror and reaper, launches to send, cancels and pauses
//!   to deliver, completions to verify, then placement of the queue.
//! - Placement through the [`pipeline::Pipeline`] (admission chain, placement strategies,
//!   compute providers); members are registered in `pipeline.registry.rs`.
//! - Reservations ([`reservations`]): HELD → ACTIVE → RELEASING → RELEASED, EXPIRED,
//!   QUARANTINED, GPUs guarded by the `one_holder_per_gpu` index.
//! - A CONTROL lane sink ([`control`]) for `attempt_status` and `reconcile`, idempotent by
//!   content: a status for an ended or older-epoch attempt never makes a second transition.
//! - [`Scheduler::reserve`] / [`Scheduler::release`] for other owners (deployments).
//! - `GET /queue?pool=` ([`api`]).

pub mod admission;
pub mod api;
pub mod control;
pub mod invariants;
pub mod launch;
pub mod pipeline;
pub mod placement;
pub mod reservations;
pub mod tick;

#[path = "pipeline.registry.rs"]
pub mod registry;

use crate::app::{ServerModule, Task};
use crate::config::Role;
use crate::db::{Db, DbError, Tx};
use crate::modules::events::{ConsumerError, Event, EventConsumer};
use crate::modules::jobs::{Chains, JobsError};
use crate::modules::nodes::commands::{CommandKind, CommandSender};
use crate::modules::nodes::lanes::LaneRouter;
use crate::modules::nodes::{Gateway, NodesError};
use crate::seams::SeamError;
use mrl_domain::errors::{ErrorCode as DomainCode, ErrorFamily};
use mrl_domain::resources::ReservationOwner;
use pipeline::{Demand, Fit, Pipeline, PlacementHints, PoolInfo};
use reservations::{NewReservation, ReservationRow};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Codes this part adds (guidance/fragments/errors/p21.yaml).
pub mod codes {
    use super::{DomainCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> DomainCode {
        match DomainCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// No machine in the pool could ever run the job as asked.
    pub const NEVER_FITS: DomainCode = code(ErrorFamily::Cap, 210);
    /// Machines that fit are busy; the job waits.
    pub const WAITING_FOR_MACHINE: DomainCode = code(ErrorFamily::Quota, 210);
    /// The pool takes no new work (flag `paused`).
    pub const POOL_PAUSED: DomainCode = code(ErrorFamily::Quota, 211);
    /// The project has too many jobs ahead in the queue.
    pub const PROJECT_QUEUE_FULL: DomainCode = code(ErrorFamily::Quota, 212);
    /// No pool is open to the job's org.
    pub const NO_POOL: DomainCode = code(ErrorFamily::Perm, 210);
    /// A reservation was changed from an older epoch.
    pub const STALE_RESERVATION: DomainCode = code(ErrorFamily::Conflict, 210);
}

/// What the scheduler fails with.
#[derive(Debug, thiserror::Error)]
pub enum SchedError {
    /// Jobs code (transitions, chains).
    #[error(transparent)]
    Jobs(#[from] JobsError),
    /// The gateway (commands).
    #[error(transparent)]
    Nodes(#[from] NodesError),
    /// The database.
    #[error(transparent)]
    Db(#[from] DbError),
    /// A seam (verifier, authorizer, resolver).
    #[error(transparent)]
    Seam(#[from] SeamError),
    /// A domain-coded refusal, like a stale reservation epoch.
    #[error("{code}: {detail}")]
    Refused {
        /// Registry code.
        code: DomainCode,
        /// Plain words.
        detail: String,
    },
    /// A bug or damaged row.
    #[error("{0}")]
    Internal(String),
}

impl From<sqlx::Error> for SchedError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(DbError::from(e))
    }
}

impl From<SchedError> for SeamError {
    fn from(e: SchedError) -> Self {
        SeamError::new("scheduler", e.to_string())
    }
}

impl SchedError {
    /// An internal problem.
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    /// The row changed since it was read (compare-and-set lost).
    pub fn changed(kind: &str) -> Self {
        Self::Jobs(JobsError::changed(kind))
    }

    /// True for a lost compare-and-set: someone else moved the row on, so the decision is
    /// taken again from fresh rows.
    pub fn is_race(&self) -> bool {
        match self {
            Self::Jobs(e) => e.code() == crate::modules::jobs::codes::JOB_CHANGED.to_string(),
            _ => false,
        }
    }

    /// True if the database refused the write with this unique constraint.
    pub fn is_unique_violation(&self, constraint: &str) -> bool {
        let db = match self {
            Self::Db(DbError::Query(sqlx::Error::Database(e))) => Some(e),
            Self::Jobs(JobsError::Db(DbError::Query(sqlx::Error::Database(e)))) => Some(e),
            _ => None,
        };
        db.is_some_and(|e| {
            e.code().as_deref() == Some("23505") && e.constraint() == Some(constraint)
        })
    }
}

/// `[scheduler]` settings.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SchedulerConfig {
    /// Seconds between ticks when nothing wakes the loop (1–60).
    pub tick_seconds: u64,
    /// A gap this long between two leader ticks is a control-plane outage: every live lease
    /// is extended by one lease length first (D-04-09).
    pub outage_threshold_seconds: u64,
    /// Extra seconds after a lease ends before its attempt counts as lost.
    pub lease_grace_seconds: u64,
    /// How long a launch (and its staging) may wait to be delivered to the machine.
    pub launch_deadline_seconds: u64,
    /// Seconds a cancelled attempt gets to stop cleanly.
    pub cancel_grace_seconds: u32,
    /// Seconds a paused attempt gets to save and stop.
    pub pause_deadline_seconds: u32,
    /// How long a cancel waits for an unreachable machine (it is sent when it reconnects).
    pub cancel_deadline_seconds: u64,
    /// Queued jobs of one project that may be placed; later ones wait.
    pub max_queued_per_project: u32,
    /// Maximum queued jobs considered per tick.
    pub max_jobs_per_tick: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_seconds: 2,
            outage_threshold_seconds: 30,
            lease_grace_seconds: 5,
            launch_deadline_seconds: 300,
            cancel_grace_seconds: 30,
            pause_deadline_seconds: 300,
            cancel_deadline_seconds: 86_400,
            max_queued_per_project: 100,
            max_jobs_per_tick: 200,
        }
    }
}

impl SchedulerConfig {
    /// Problems with the values, in plain words.
    pub fn problems(&self) -> Vec<String> {
        let mut p = Vec::new();
        let mut range = |name: &str, v: u64, lo: u64, hi: u64| {
            if !(lo..=hi).contains(&v) {
                p.push(format!("scheduler.{name}: {v} is outside {lo}–{hi}"));
            }
        };
        range("tick_seconds", self.tick_seconds, 1, 60);
        range(
            "outage_threshold_seconds",
            self.outage_threshold_seconds,
            5,
            3600,
        );
        range("lease_grace_seconds", self.lease_grace_seconds, 0, 600);
        range(
            "launch_deadline_seconds",
            self.launch_deadline_seconds,
            10,
            86_400,
        );
        range(
            "cancel_grace_seconds",
            u64::from(self.cancel_grace_seconds),
            0,
            3600,
        );
        range(
            "pause_deadline_seconds",
            u64::from(self.pause_deadline_seconds),
            1,
            3600,
        );
        range(
            "cancel_deadline_seconds",
            self.cancel_deadline_seconds,
            60,
            7 * 86_400,
        );
        range(
            "max_queued_per_project",
            u64::from(self.max_queued_per_project),
            1,
            100_000,
        );
        range(
            "max_jobs_per_tick",
            u64::from(self.max_jobs_per_tick),
            1,
            10_000,
        );
        p
    }
}

/// The command kinds this part sends.
pub static KINDS: &[CommandKind] = &[
    CommandKind::LaunchAttempt,
    CommandKind::StageArtifacts,
    CommandKind::UpdateCredentials,
    CommandKind::CancelAttempt,
    CommandKind::PauseAttempt,
];

/// `pg_notify` channel that wakes the scheduler leader on any replica.
pub const WAKE_CHANNEL: &str = "mrl_scheduler";

/// What every scheduler path shares.
pub struct Core {
    pub(crate) db: Db,
    pub(crate) cfg: SchedulerConfig,
    pub(crate) sender: CommandSender,
    pub(crate) chains: Chains,
    pub(crate) pipeline: Pipeline,
    pub(crate) transfer: launch::Transfer,
    pub(crate) wake: tokio::sync::Notify,
    pub(crate) queue_scan: tokio::sync::Mutex<tick::QueueScan>,
}

impl Core {
    async fn audit(
        &self,
        tx: &mut Tx,
        org: Uuid,
        project: Option<Uuid>,
        action: &str,
        target: String,
    ) -> Result<(), SchedError> {
        use crate::seams::{AuditEvent, AuditOutcome, AuditSink};
        crate::modules::audit::PgAuditSink::new(self.db.clone())
            .record_in(
                tx,
                AuditEvent {
                    at: time::OffsetDateTime::now_utc(),
                    actor: "system:scheduler".into(),
                    org: crate::modules::jobs::scope::org_text(org),
                    project: project
                        .map(|id| mrl_domain::ids::ProjectId::from_uuid(id).to_string()),
                    action: action.to_owned(),
                    target,
                    outcome: AuditOutcome::Success,
                    trace_id: Uuid::new_v4().simple().to_string(),
                },
            )
            .await?;
        Ok(())
    }

    async fn audit_job(
        &self,
        tx: &mut Tx,
        job: &crate::modules::jobs::JobRow,
        action: &str,
    ) -> Result<(), SchedError> {
        self.audit(
            tx,
            job.org_id,
            Some(job.project_id),
            action,
            mrl_domain::ids::JobId::from_uuid(job.id).to_string(),
        )
        .await
    }

    async fn audit_reservation(
        &self,
        tx: &mut Tx,
        row: &ReservationRow,
        action: &str,
    ) -> Result<(), SchedError> {
        let project = if let Some(job) = row.job_id {
            sqlx::query_scalar("SELECT project_id FROM jobs WHERE org_id = $1 AND id = $2")
                .bind(row.org_id)
                .bind(job)
                .fetch_optional(&mut **tx)
                .await?
        } else {
            None
        };
        self.audit(
            tx,
            row.org_id,
            project,
            action,
            reservations::public_id(row.id),
        )
        .await
    }
}

/// Holds and gives back resources for any owner (jobs and deployments).
#[derive(Clone)]
pub struct Scheduler {
    core: Arc<Core>,
}

/// What to hold.
#[derive(Debug, Clone, PartialEq)]
pub struct ReservationRequest {
    /// Stable id for this hold. Retry the same request with the same id.
    pub request_id: Uuid,
    /// Job or deployment.
    pub owner: ReservationOwner,
    /// Owning org.
    pub org_id: Uuid,
    /// The pool to hold it in.
    pub pool_id: Uuid,
    /// What to hold.
    pub demand: Demand,
    /// Fencing epoch the owner works under.
    pub epoch: i64,
    /// How long it lasts before it must be renewed (`None`: the pool's attempt lease).
    pub lease: Option<Duration>,
}

/// The answer to [`Scheduler::reserve`].
#[derive(Debug, Clone, PartialEq)]
pub enum Reserved {
    /// Held.
    Held(Box<ReservationRow>),
    /// This request already ended. A new hold requires a fresh request id.
    Ended(Box<ReservationRow>),
    /// Not now (plain reason); try again later.
    Busy(String),
    /// Never in this pool (plain reason).
    Never(String),
}

/// Reads a pool.
pub(crate) async fn pool_info(db: &Db, pool: Uuid) -> Result<Option<PoolInfo>, SchedError> {
    Ok(pools(db, Some(pool)).await?.into_iter().next())
}

/// Every pool (or just `only`).
pub(crate) async fn pools(db: &Db, only: Option<Uuid>) -> Result<Vec<PoolInfo>, SchedError> {
    #[derive(sqlx::FromRow)]
    struct Row {
        id: Uuid,
        org_id: Option<Uuid>,
        name: String,
        provider_kind: String,
        flags: Vec<String>,
        scheduling_policy: serde_json::Value,
        ttl: f64,
    }
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, org_id, name, provider_kind, flags, scheduling_policy,
                extract(epoch from attempt_lease_ttl)::float8 AS ttl
         FROM compute_pools WHERE $1::uuid IS NULL OR id = $1 ORDER BY created_at, id",
    )
    .bind(only)
    .fetch_all(db.pool())
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| PoolInfo {
            id: r.id,
            org_id: r.org_id,
            name: r.name,
            provider_kind: r.provider_kind,
            flags: r.flags,
            scheduling_policy: r.scheduling_policy,
            lease_ttl: Duration::from_secs_f64(r.ttl.max(1.0)),
        })
        .collect())
}

impl Scheduler {
    /// Places and holds `req` (its own transaction). A deployment owner renews the hold with
    /// [`Scheduler::renew`] and gives it back with [`Scheduler::release`].
    pub async fn reserve(&self, req: &ReservationRequest) -> Result<Reserved, SchedError> {
        let db = &self.core.db;
        let pool = pool_info(db, req.pool_id)
            .await?
            .filter(|pool| pool.org_id.is_none_or(|org| org == req.org_id))
            .ok_or_else(|| SchedError::Refused {
                code: codes::NO_POOL,
                detail: "No pool with this id exists.".into(),
            })?;
        let (provider, strategy) = match (
            self.core
                .pipeline
                .providers
                .get(pool.provider_kind.as_str()),
            self.core.pipeline.strategy(&pool),
        ) {
            (Some(p), Some(s)) => (p.clone(), s),
            _ => {
                return Ok(Reserved::Never(format!(
                    "This server can't place work on `{}` pools.",
                    pool.provider_kind
                )))
            }
        };
        // Capacity is read before the write transaction opens (no connection held twice);
        // `place` re-checks under the machine's lock.
        let paused = pool.flags.iter().any(|flag| flag == admission::PAUSED_FLAG);
        let fit = if paused {
            Fit::Busy("The pool is paused.".into())
        } else {
            let nodes = provider.capacity(pool.id).await?;
            strategy.place(&req.demand, &nodes, &PlacementHints::default())
        };
        let mut tx = db.begin().await?;
        // The row does not exist on the first call. Serialize replays before checking it.
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('p21-reserve:' || $1::uuid::text, 0))",
        )
        .bind(req.request_id)
        .execute(&mut *tx)
        .await?;
        let request = serde_json::to_value((
            req.owner,
            req.pool_id.to_string(),
            req.epoch,
            &req.demand,
            req.lease,
        ))
        .map_err(|e| SchedError::internal(e.to_string()))?;
        if let Some(row) = reservations::lock_for(&mut tx, req.request_id, Some(req.org_id)).await?
        {
            let (saved, live): (Option<serde_json::Value>, bool) = sqlx::query_as(
                "SELECT reserve_request, lease_expires_at > clock_timestamp()
                 FROM resource_reservations WHERE id = $1",
            )
            .bind(req.request_id)
            .fetch_one(&mut *tx)
            .await?;
            if saved.as_ref() == Some(&request) {
                return Ok(if matches!(row.state.as_str(), "HELD" | "ACTIVE") && live {
                    Reserved::Held(Box::new(row))
                } else {
                    Reserved::Ended(Box::new(row))
                });
            }
            return Err(SchedError::changed("reservation request"));
        }
        let placement = match fit {
            Fit::Placed(p) => p,
            Fit::Busy(w) => return Ok(Reserved::Busy(w)),
            Fit::Never(w) => return Ok(Reserved::Never(w)),
        };
        let new = NewReservation {
            id: req.request_id,
            org_id: req.org_id,
            owner: req.owner,
            pool_id: pool.id,
            node_id: placement.node_id,
            gpus: placement.gpu_ids(),
            roles: launch::role_gpus(&placement),
            cpu_millis: placement.cpu_millis,
            mem_bytes: placement.mem_bytes,
            epoch: req.epoch,
            lease: req.lease.unwrap_or(pool.lease_ttl),
        };
        let held = match provider.place(&mut tx, &new).await {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(Reserved::Busy("The machine was taken meanwhile.".into())),
            Err(e) if e.is_unique_violation("resource_reservations_pkey") => {
                return Err(SchedError::changed("reservation request"))
            }
            Err(e) if e.is_unique_violation("one_holder_per_gpu") => {
                return Ok(Reserved::Busy("The GPUs were taken meanwhile.".into()))
            }
            Err(e) => return Err(e),
        };
        sqlx::query("UPDATE resource_reservations SET reserve_request = $2 WHERE id = $1")
            .bind(held.id)
            .bind(request)
            .execute(&mut *tx)
            .await?;
        self.core
            .audit_reservation(&mut tx, &held, "scheduler.reserve")
            .await?;
        tx.commit().await?;
        Ok(Reserved::Held(Box::new(held)))
    }

    /// Extends a live hold to now + `lease`, if `epoch` is still its epoch.
    pub async fn renew(
        &self,
        org: Uuid,
        id: Uuid,
        epoch: i64,
        lease: Duration,
    ) -> Result<(), SchedError> {
        let mut tx = self.core.db.begin().await?;
        let row = fenced(&mut tx, org, id, epoch).await?;
        if row.is_live() {
            sqlx::query(
                "UPDATE resource_reservations SET lease_expires_at = GREATEST(lease_expires_at, clock_timestamp() + make_interval(secs => $2))
                 WHERE id = $1",
            )
            .bind(id)
            .bind(lease.as_secs_f64())
            .execute(&mut *tx)
            .await?;
            self.core
                .audit_reservation(&mut tx, &row, "scheduler.renew")
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Gives a hold back (→ RELEASED; never used → EXPIRED), if `epoch` is still its epoch.
    pub async fn release(
        &self,
        org: Uuid,
        id: Uuid,
        epoch: i64,
        why: &str,
    ) -> Result<ReservationRow, SchedError> {
        // The pool is read before the write transaction opens (no connection held twice).
        let pool: Option<Uuid> = sqlx::query_scalar(
            "SELECT pool_id FROM resource_reservations WHERE id = $1 AND org_id = $2",
        )
        .bind(id)
        .bind(org)
        .fetch_optional(self.core.db.pool())
        .await?;
        let pool = match pool {
            Some(p) => pool_info(&self.core.db, p).await?,
            None => None,
        };
        let mut tx = self.core.db.begin().await?;
        let row = fenced(&mut tx, org, id, epoch).await?;
        let out = match pool
            .as_ref()
            .and_then(|p| self.core.pipeline.providers.get(p.provider_kind.as_str()))
        {
            Some(p) => p.release(&mut tx, &row, why).await?,
            None => reservations::release(&mut tx, &row, why).await?,
        };
        if out.state_version != row.state_version {
            self.core
                .audit_reservation(&mut tx, &out, "scheduler.release")
                .await?;
        }
        tx.commit().await?;
        Ok(out)
    }
}

/// Locks reservation `id` and checks the caller's epoch.
async fn fenced(
    tx: &mut Tx,
    org: Uuid,
    id: Uuid,
    epoch: i64,
) -> Result<ReservationRow, SchedError> {
    let row = reservations::lock_for(tx, id, Some(org))
        .await?
        .ok_or_else(|| SchedError::Refused {
            code: codes::STALE_RESERVATION,
            detail: "No reservation with this id exists.".into(),
        })?;
    if row.epoch != epoch {
        return Err(SchedError::Refused {
            code: codes::STALE_RESERVATION,
            detail: format!(
                "This reservation moved on to epoch {}, so a change from epoch {epoch} doesn't count.",
                row.epoch
            ),
        });
    }
    Ok(row)
}

/// The `scheduler` module.
pub struct SchedulerModule {
    core: Arc<Core>,
}

impl SchedulerModule {
    /// The module for this build: commands through the gateway on this database, the CONTROL
    /// sink on [`LaneRouter::global`], P17's registered chains and this part's registry.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let gw = crate::modules::nodes::NodesModule::from_config(db.clone(), config).gateway();
        Self::with_parts(
            db.clone(),
            config.scheduler.clone(),
            &gw,
            &LaneRouter::global(),
            Chains::registered(&db),
            registry::pipeline(&db, &config.scheduler),
            launch::Transfer::from_config(&db, config),
        )
    }

    /// A module from explicit parts (tests, other builds).
    pub fn with_parts(
        db: Db,
        cfg: SchedulerConfig,
        gw: &Arc<Gateway>,
        lanes: &Arc<LaneRouter>,
        chains: Chains,
        pipeline: Pipeline,
        transfer: launch::Transfer,
    ) -> Self {
        let core = Arc::new(Core {
            db,
            cfg,
            sender: CommandSender::new(gw, KINDS),
            chains,
            pipeline,
            transfer,
            wake: tokio::sync::Notify::new(),
            queue_scan: tokio::sync::Mutex::default(),
        });
        lanes.register(Arc::new(control::ControlSink::new(core.clone())));
        Self { core }
    }

    /// Reserve and release for other owners.
    pub fn scheduler(&self) -> Scheduler {
        Scheduler {
            core: self.core.clone(),
        }
    }
}

impl ServerModule for SchedulerModule {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    fn routes(&self) -> axum::Router<crate::AppState> {
        api::routes(self.core.clone())
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        api::openapi()
    }

    fn background_tasks(&self) -> Vec<Task> {
        let (a, b) = (self.core.clone(), self.core.clone());
        vec![
            Task::new(
                "scheduler.loop",
                Role::Scheduler,
                move |cancel| async move {
                    tick::run(a, cancel).await;
                    Ok(())
                },
            ),
            Task::new(
                "scheduler.wake",
                Role::Scheduler,
                move |cancel| async move {
                    tick::listen(b, cancel).await;
                    Ok(())
                },
            ),
        ]
    }
}

/// Event consumer that wakes the scheduler leader (on any replica) when a job is queued,
/// asked to cancel or pause, a reservation changes or a machine changes.
pub struct Wake;

#[async_trait::async_trait]
impl EventConsumer for Wake {
    fn name(&self) -> &'static str {
        "scheduler-wake"
    }

    fn accepts(&self, t: &str) -> bool {
        matches!(
            t,
            "job.queued"
                | "job.retried"
                | "job.requeued"
                | "job.cancel_requested"
                | "job.pause_requested"
                | "job.finished"
        ) || t.starts_with("node.")
            || t.starts_with("reservation.")
    }

    async fn handle(&self, tx: &mut Tx, _event: &Event) -> Result<(), ConsumerError> {
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(WAKE_CHANNEL)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}
