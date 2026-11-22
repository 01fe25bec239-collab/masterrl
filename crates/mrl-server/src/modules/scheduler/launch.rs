//! Launching a placed attempt: build the `LaunchAttempt` (launch chain), send it, and settle
//! launches that never took.
//!
//! The attempt (PENDING) and its ACTIVE reservation are committed first; the commands go
//! out after. [`ensure_launched`] is the one path for both the first send and recovery after
//! a crash in between: it sends only when no launch exists for the attempt's epoch, and ends
//! the attempt when its launch failed, expired, or was accepted but never reported.

use super::control::end_unstarted;
use super::pipeline::{LaunchContributor, LaunchError, LaunchPlan, Placement};
use super::reservations::ReservationRow;
use super::{Core, SchedError};
use crate::db::Db;
use crate::modules::compute::GpuSlot;
use crate::modules::jobs::{AttemptRow, JobRow, JobTransitions};
use crate::modules::nodes::commands::NewCommand;
use mrl_domain::failure::FailureClass;
use mrl_domain::ids::{
    AttemptId, CheckpointId, DatasetVersionId, JobId, ModelVersionId, NodeId, RewardVersionId,
    RunId,
};
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{LaunchAttempt, RoleLaunch, StageArtifacts, UpdateCredentials};
use mrl_proto::common::v1::AttemptRef;
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::time::Duration;
use uuid::Uuid;

/// GPU indices per role, as stored on the reservation: `{"trainer": [0, 1]}`.
pub fn role_gpus(p: &Placement) -> Value {
    Value::Object(
        p.roles
            .iter()
            .map(|(r, g)| {
                (
                    r.clone(),
                    json!(g.iter().map(|g| g.index).collect::<Vec<_>>()),
                )
            })
            .collect(),
    )
}

/// The attempt as the agent names it.
pub fn attempt_ref(attempt: &AttemptRow) -> AttemptRef {
    AttemptRef {
        job_id: JobId::from_uuid(attempt.job_id).to_string(),
        attempt_id: AttemptId::from_uuid(attempt.id).to_string(),
        fencing_epoch: u64::try_from(attempt.epoch).unwrap_or(0),
    }
}

/// The default launch contributor: the job spec from the run (or the job's subject) and its
/// run config, and the run config's pinned runtime for every role.
pub struct RunConfigSpec {
    db: Db,
}

impl RunConfigSpec {
    /// Reads runs and run configs from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl LaunchContributor for RunConfigSpec {
    fn name(&self) -> &'static str {
        "run_config_spec"
    }

    async fn contribute(
        &self,
        job: &JobRow,
        attempt: &AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        let transient =
            |e: sqlx::Error| LaunchError::Transient(format!("the database did not answer: {e}"));
        let mut spec = Map::new();
        let mut runtime = None;
        if let Some(run) = job.run_id {
            let row = sqlx::query(
                "SELECT r.algorithm_id, r.base_model_version_id, r.reward_version_id, rc.resolved,
                        rc.runtime_ref, rc.seeds, rc.dataset_version_ids
                 FROM runs r JOIN run_configs rc ON rc.run_id = r.id
                 WHERE r.id = $1 AND r.org_id = $2",
            )
            .bind(run)
            .bind(job.org_id)
            .fetch_optional(self.db.pool())
            .await
            .map_err(transient)?
            .ok_or_else(|| {
                LaunchError::Fail(
                    FailureClass::HarnessError,
                    "The run's settings are missing.".into(),
                )
            })?;
            let get = |c: &str| -> Result<Value, LaunchError> {
                row.try_get::<Value, _>(c).map_err(transient)
            };
            if let Value::Object(m) = get("resolved")? {
                spec = m;
            }
            let mv: Uuid = row.try_get("base_model_version_id").map_err(transient)?;
            let rwd: Option<Uuid> = row.try_get("reward_version_id").map_err(transient)?;
            let dsv: Vec<Uuid> = row.try_get("dataset_version_ids").map_err(transient)?;
            runtime = row
                .try_get::<Option<String>, _>("runtime_ref")
                .map_err(transient)?;
            spec.insert("run_id".into(), json!(RunId::from_uuid(run).to_string()));
            spec.insert(
                "algorithm".into(),
                json!(row
                    .try_get::<String, _>("algorithm_id")
                    .map_err(transient)?),
            );
            spec.insert(
                "base_model_version_id".into(),
                json!(ModelVersionId::from_uuid(mv).to_string()),
            );
            spec.insert(
                "dataset_version_ids".into(),
                json!(dsv
                    .into_iter()
                    .map(|d| DatasetVersionId::from_uuid(d).to_string())
                    .collect::<Vec<_>>()),
            );
            spec.insert(
                "reward_version_id".into(),
                json!(rwd.map(|r| RewardVersionId::from_uuid(r).to_string())),
            );
            spec.insert("seeds".into(), get("seeds")?);
        } else {
            spec.insert(
                "subject".into(),
                job.subject_ref.clone().unwrap_or(Value::Null),
            );
        }
        spec.insert("job_id".into(), json!(JobId::from_uuid(job.id).to_string()));
        spec.insert("job_kind".into(), json!(job.kind));
        spec.insert(
            "resume_from_checkpoint_id".into(),
            json!(attempt
                .resume_from_checkpoint_id
                .map(|c| CheckpointId::from_uuid(c).to_string())),
        );
        plan.launch.job_spec_json = Value::Object(spec).to_string();
        if let Some(rt) = runtime {
            for r in &mut plan.launch.roles {
                if r.runtime_ref.is_empty() {
                    r.runtime_ref = rt.clone();
                }
            }
        }
        Ok(())
    }
}

/// The reservation's placement back as a [`Placement`] (roles in stored order).
async fn placement_of(db: &Db, res: &ReservationRow) -> Result<Placement, SchedError> {
    let rows = sqlx::query(
        "SELECT g.id, g.index_on_node, g.vendor, g.model, g.arch, g.vram_total_bytes, g.nvlink_peers
         FROM reservation_devices d JOIN gpu_devices g ON g.id = d.gpu_device_id
         WHERE d.reservation_id = $1",
    )
    .bind(res.id)
    .fetch_all(db.pool())
    .await?;
    let mut slots = Vec::with_capacity(rows.len());
    for r in rows {
        let peers: Vec<i32> = r.try_get("nvlink_peers")?;
        slots.push(GpuSlot {
            id: r.try_get("id")?,
            index: u32::try_from(r.try_get::<i32, _>("index_on_node")?).unwrap_or(0),
            vendor: r.try_get("vendor")?,
            model: r.try_get("model")?,
            arch: r.try_get("arch")?,
            vram_bytes: u64::try_from(r.try_get::<i64, _>("vram_total_bytes")?).unwrap_or(0),
            nvlink_peers: peers
                .into_iter()
                .filter_map(|p| u32::try_from(p).ok())
                .collect(),
            free: false,
        });
    }
    let roles = match &res.placement {
        Value::Object(m) => m
            .iter()
            .map(|(role, idx)| {
                let idx: Vec<u32> = serde_json::from_value(idx.clone()).unwrap_or_default();
                let gpus = slots
                    .iter()
                    .filter(|g| idx.contains(&g.index))
                    .cloned()
                    .collect();
                (role.clone(), gpus)
            })
            .collect(),
        _ => Vec::new(),
    };
    Ok(Placement {
        node_id: res.node_id,
        roles,
        cpu_millis: u64::try_from(res.cpu_millis).unwrap_or(0),
        mem_bytes: u64::try_from(res.mem_bytes).unwrap_or(0),
    })
}

/// Builds the launch for `attempt` on `res` through the launch chain.
pub(crate) async fn plan(
    core: &Core,
    job: &JobRow,
    attempt: &AttemptRow,
    res: &ReservationRow,
    lease: Duration,
) -> Result<Result<LaunchPlan, LaunchError>, SchedError> {
    let placement = placement_of(&core.db, res).await?;
    let mut all: Vec<u32> = placement.gpu_indices();
    all.dedup();
    let primary = placement
        .roles
        .iter()
        .position(|(r, _)| r == "trainer")
        .unwrap_or(0);
    let mut plan = LaunchPlan {
        launch: LaunchAttempt {
            attempt: Some(attempt_ref(attempt)),
            roles: placement
                .roles
                .iter()
                .enumerate()
                .map(|(i, (role, gpus))| RoleLaunch {
                    role_id: role.clone(),
                    kind: role.clone(),
                    primary: i == primary,
                    count: 1,
                    gpu_indices: gpus.iter().map(|g| g.index).collect(),
                    runtime_ref: String::new(),
                })
                .collect(),
            allocation: Some(mrl_proto::worker::v1::Allocation {
                gpu_indices: all,
                world_size: 1,
                node_rank: 0,
                role_endpoints: Default::default(),
            }),
            lease_duration_s: u32::try_from(lease.as_secs()).unwrap_or(u32::MAX).max(1),
            ..LaunchAttempt::default()
        },
        ..LaunchPlan::default()
    };
    for c in &core.pipeline.launch {
        if let Err(e) = c.contribute(job, attempt, &placement, &mut plan).await {
            tracing::info!(contributor = c.name(), error = %e, "a launch could not be built");
            return Ok(Err(e));
        }
    }
    if plan.launch.engine_name.is_empty()
        || plan.launch.roles.is_empty()
        || plan.launch.roles.iter().any(|r| r.runtime_ref.is_empty())
    {
        return Ok(Err(LaunchError::Fail(
            FailureClass::DependencyEnv,
            "No training engine or environment is chosen for this job yet, so it can't start. Submit it again once its method has an installed engine.".into(),
        )));
    }
    Ok(Ok(plan))
}

/// Sends stage, credentials and launch, in that order (one node, so they arrive in order).
async fn send(
    core: &Core,
    node: Uuid,
    attempt: &AttemptRow,
    plan: LaunchPlan,
) -> Result<(), SchedError> {
    let node = NodeId::from_uuid(node);
    let id = AttemptId::from_uuid(attempt.id);
    let epoch = u64::try_from(attempt.epoch).unwrap_or(0);
    let deadline = Duration::from_secs(core.cfg.launch_deadline_seconds);
    let r = plan.launch.attempt.clone();
    if !plan.stage.is_empty() {
        let k = Kind::StageArtifacts(StageArtifacts {
            attempt: r.clone(),
            items: plan.stage,
        });
        core.sender
            .send(
                node,
                NewCommand::new(k).attempt(id, epoch).deadline(deadline),
            )
            .await?;
    }
    if !plan.credentials.is_empty() {
        let k = Kind::UpdateCredentials(UpdateCredentials {
            attempt: r,
            credentials: plan.credentials,
        });
        core.sender
            .send(
                node,
                NewCommand::new(k).attempt(id, epoch).deadline(deadline),
            )
            .await?;
    }
    let k = Kind::LaunchAttempt(plan.launch);
    core.sender
        .send(
            node,
            NewCommand::new(k)
                .attempt(id, epoch)
                .deadline(deadline)
                .superseding(),
        )
        .await?;
    Ok(())
}

/// The newest `kind` command for the attempt's epoch: state, accepted, failure, age (s).
pub(crate) async fn latest_command(
    db: &Db,
    kind: &str,
    attempt: &AttemptRow,
) -> Result<Option<(String, bool, String, f64)>, SchedError> {
    let row: Option<(String, Option<Value>, f64)> = sqlx::query_as(
        "SELECT state, result, extract(epoch from now() - updated_at)::float8 FROM agent_commands
         WHERE kind = $1 AND attempt_id = $2 AND epoch = $3
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(kind)
    .bind(attempt.id)
    .bind(attempt.epoch)
    .fetch_optional(db.pool())
    .await?;
    Ok(row.map(|(state, result, age)| {
        let r = result.unwrap_or_default();
        let failure = match &r["failure"] {
            Value::String(s) => s.clone(),
            Value::Null => String::new(),
            other => other.to_string(),
        };
        (
            state,
            r["accepted"].as_bool().unwrap_or(false),
            failure,
            age,
        )
    }))
}

/// Sends the launch of `job`'s PENDING current attempt if none exists yet, or ends the
/// attempt when its launch can't succeed any more. `leader_for`: how long this replica has
/// led (an accepted launch is given up only after a full lease of leadership).
pub(crate) async fn ensure_launched(
    core: &Core,
    job: &JobRow,
    attempt: &AttemptRow,
    leader_for: Duration,
) -> Result<(), SchedError> {
    let mut tx = core.db.begin().await?;
    let res = super::reservations::live_for_job(&mut tx, job.id)
        .await?
        .into_iter()
        .find(|r| r.attempt_id == Some(attempt.id));
    tx.rollback().await?;
    let Some(res) = res else {
        let why = "The machine held for this attempt is no longer held.";
        return end_unstarted(core, attempt, FailureClass::InfraTransient, why.into()).await;
    };
    let pool = super::pool_info(&core.db, res.pool_id).await?;
    let lease = pool.map_or(Duration::from_secs(90), |p| p.lease_ttl);
    match latest_command(&core.db, "LaunchAttempt", attempt).await? {
        None => match plan(core, job, attempt, &res, lease).await? {
            Ok(p) => {
                if let Err(e) = send(core, res.node_id, attempt, p).await {
                    // A refused command (machine draining or removed) can't be sent again.
                    if matches!(
                        e,
                        SchedError::Nodes(crate::modules::nodes::NodesError::CommandRefused(_))
                    ) {
                        let why = format!("The machine did not take the launch: {e}");
                        return end_unstarted(core, attempt, FailureClass::InfraTransient, why)
                            .await;
                    }
                    return Err(e);
                }
                Ok(())
            }
            Err(LaunchError::Transient(why)) => {
                end_unstarted(core, attempt, FailureClass::InfraTransient, why).await
            }
            Err(LaunchError::Fail(class, why)) => end_unstarted(core, attempt, class, why).await,
        },
        Some((state, _, _, _)) if ["PENDING", "SENT", "RECEIVED"].contains(&state.as_str()) => {
            Ok(())
        }
        Some((state, true, _, age)) if state == "COMPLETED" => {
            let wait = lease + Duration::from_secs(core.cfg.lease_grace_seconds);
            if age > wait.as_secs_f64() && leader_for > wait {
                let why = "The machine took the launch but never reported the attempt starting.";
                end_unstarted(core, attempt, FailureClass::NodeLost, why.into()).await
            } else {
                Ok(())
            }
        }
        Some((state, _, failure, _)) => {
            let why = format!(
                "The machine did not take the launch ({}): {failure}",
                state.to_lowercase()
            );
            end_unstarted(core, attempt, FailureClass::InfraTransient, why).await
        }
    }
}

/// Reads the job and its current attempt (no lock), for passes that act outside a write tx.
pub(crate) async fn current(
    core: &Core,
    org: Uuid,
    job: Uuid,
) -> Result<Option<(JobRow, AttemptRow)>, SchedError> {
    let mut tx = core.db.begin().await?;
    let Some(row) = JobTransitions::lock(&mut tx, org, job).await? else {
        return Ok(None);
    };
    let Some(a) = row.current_attempt_id else {
        return Ok(None);
    };
    let attempt = JobTransitions::lock_attempt(&mut tx, org, a).await?;
    tx.rollback().await?;
    Ok(attempt.map(|a| (row, a)))
}
