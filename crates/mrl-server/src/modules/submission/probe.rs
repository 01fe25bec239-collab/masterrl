//! The L1 probe (Doc 20 item 10): a short CPU-only dry-run job (kind `TRAIN`, no run) that
//! the scheduler (P21) places on a connected machine. The worker checks the settings, the
//! staged inputs and the hardware, estimates the memory, answers `Prepared` and exits
//! `DRY_RUN_OK`.
//!
//! - [`start`]: the probe job (QUEUED at top priority, no retries) and its PENDING
//!   `resource_estimates` row, in one transaction.
//! - [`ProbeLaunch`] (launch chain): fills a probe's spec (resolved config, engine runtime)
//!   and sets `dry_run`; for runs, refuses a resume the engine can't do
//!   ([`super::policy::resume_problem`]).
//! - [`PreparedSink`] (CONTROL `worker.prepared`): stores the answer once, and only from
//!   the machine the probe's current attempt is placed on.
//! - [`ProbeVerifier`]: completes probe jobs (and only them).
//! - [`sweep`]: cancels probes nobody waits for any more (a server restarted mid-wait).

use super::policy;
use crate::db::Db;
use crate::modules::engines::launch::{environment, runtime_ref};
use crate::modules::jobs::{
    AttemptRow, CompletionVerifier, JobChange, JobRow, JobTransitions, JobsError, NewJob,
    Verification,
};
use crate::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink, SinkError};
use crate::modules::scheduler::pipeline::{LaunchContributor, LaunchError, LaunchPlan, Placement};
use crate::seams::SeamError;
use mrl_domain::failure::FailureClass;
use mrl_domain::ids::{AttemptId, JobId};
use mrl_domain::state::{JobEvent, Owner};
use mrl_proto::agent::v1::agent_message::Body;
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::Prepared;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use std::time::Duration;
use uuid::Uuid;

/// `jobs.subject_ref.kind` of a probe job.
pub const PROBE_KIND: &str = "run_probe";

/// What a probe launch needs, stored with its estimate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeSpec {
    /// Method.
    pub algorithm: String,
    /// `mv_…`.
    pub base_model_version_id: String,
    /// `dsv_…`.
    pub dataset_version_ids: Vec<String>,
    /// `rwdv_…`.
    pub reward_version_id: Option<String>,
    /// Seeds.
    pub seeds: Value,
    /// The resolved config.
    pub resolved: Value,
}

/// A new probe.
pub struct NewProbe<'a> {
    /// Org and project.
    pub org: Uuid,
    /// Project.
    pub project: Uuid,
    /// Who asked.
    pub created_by: &'a str,
    /// The launch's inputs.
    pub spec: &'a ProbeSpec,
    /// Engine version the probe runs.
    pub engine_version_id: Uuid,
    /// `<engine>@<version>`.
    pub estimator_version: String,
    /// Configuration class.
    pub calibration_key: String,
    /// Hash of `spec.resolved`.
    pub inputs_sha256: Vec<u8>,
    /// Give up after this long.
    pub max_runtime_seconds: i64,
}

/// Creates the probe job (QUEUED) and its PENDING estimate. Returns `(estimate, job)`.
pub async fn start(db: &Db, p: &NewProbe<'_>) -> Result<(Uuid, Uuid), JobsError> {
    let estimate = Uuid::now_v7();
    // One CPU core and 4 GiB on any machine; no retries (a failed probe is reported).
    let resources = json!({"roles": [{"role": "trainer", "replicas": 1, "gpu_count": 0,
        "cpu_millis": 1000, "mem_bytes": 4u64 << 30}]});
    let no_retries: serde_json::Map<String, Value> = FailureClass::ALL
        .iter()
        .map(|c| (c.as_str().to_owned(), json!(0)))
        .collect();
    let mut tx = db.begin().await?;
    let job = JobTransitions::create(
        &mut tx,
        &NewJob {
            id: Uuid::now_v7(),
            org_id: p.org,
            project_id: p.project,
            kind: "TRAIN".into(),
            run_id: None,
            subject_ref: Some(json!({"kind": PROBE_KIND, "estimate_id": estimate.to_string()})),
            priority: 1000,
            resource_request: resources,
            retry_policy: json!({"auto_tune": false, "budgets": no_retries}),
            max_runtime_seconds: Some(p.max_runtime_seconds),
            created_by: p.created_by.to_owned(),
        },
    )
    .await?;
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Submit,
        Owner::Jobs,
        JobChange::default(),
    )
    .await?;
    let job = JobTransitions::apply(
        &mut tx,
        &job,
        JobEvent::Accept,
        Owner::Jobs,
        JobChange::default(),
    )
    .await?;
    sqlx::query(
        "INSERT INTO resource_estimates (id, org_id, project_id, job_id, inputs_sha256,
           engine_version_id, algorithm_id, calibration_key, estimator_version, probe_spec, state)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'PENDING')",
    )
    .bind(estimate)
    .bind(p.org)
    .bind(p.project)
    .bind(job.id)
    .bind(&p.inputs_sha256)
    .bind(p.engine_version_id)
    .bind(&p.spec.algorithm)
    .bind(&p.calibration_key)
    .bind(&p.estimator_version)
    .bind(serde_json::to_value(p.spec).unwrap_or_else(|_| json!({})))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    // The scheduler places it on its next pass; wake it now.
    sqlx::query("SELECT pg_notify($1, '')")
        .bind(crate::modules::scheduler::WAKE_CHANNEL)
        .execute(db.pool())
        .await?;
    Ok((estimate, job.id))
}

/// How a probe ended for the caller.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The machine answered.
    Answered {
        /// The worker found no blocking problem.
        ok: bool,
        /// Its findings, as stored.
        issues: Vec<Value>,
    },
    /// The job ended without an answer (the reason, in plain words).
    Failed(String),
    /// No answer in time.
    TimedOut,
}

const TERMINAL: [&str; 4] = ["COMPLETED", "FAILED", "CANCELLED", "REJECTED"];

/// Waits up to `budget` for probe `estimate` (job `job`) to answer.
pub async fn wait(
    db: &Db,
    estimate: Uuid,
    job: Uuid,
    budget: Duration,
) -> Result<Outcome, JobsError> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let row = sqlx::query(
            "SELECT e.state, e.ok, e.issues, j.state AS job_state, j.state_reason
               FROM resource_estimates e JOIN jobs j ON j.id = e.job_id WHERE e.id = $1",
        )
        .bind(estimate)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| JobsError::not_found("estimate"))?;
        let state: String = row.try_get("state")?;
        if state == "READY" {
            let issues: Value = row.try_get("issues")?;
            return Ok(Outcome::Answered {
                ok: row.try_get::<Option<bool>, _>("ok")?.unwrap_or(false),
                issues: issues.as_array().cloned().unwrap_or_default(),
            });
        }
        let job_state: String = row.try_get("job_state")?;
        if TERMINAL.contains(&job_state.as_str()) {
            let reason: Option<Value> = row.try_get("state_reason")?;
            let why = reason
                .as_ref()
                .and_then(|r| r["message"].as_str())
                .unwrap_or("The check on the machine ended without an answer.")
                .to_owned();
            return Ok(Outcome::Failed(why));
        }
        if tokio::time::Instant::now() >= deadline {
            stop(db, job).await?;
            return Ok(Outcome::TimedOut);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Cancels probe job `job` unless it already ended.
pub async fn stop(db: &Db, job: Uuid) -> Result<(), JobsError> {
    let org: Option<Uuid> = sqlx::query_scalar("SELECT org_id FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_optional(db.pool())
        .await?;
    let Some(org) = org else {
        return Ok(());
    };
    let mut tx = db.begin().await?;
    let Some(row) = JobTransitions::lock(&mut tx, org, job).await? else {
        return Ok(());
    };
    if TERMINAL.contains(&row.state.as_str()) || row.state == "CANCELLING" {
        return Ok(());
    }
    JobTransitions::request_cancel(&mut tx, &row, "system:validate").await?;
    tx.commit().await?;
    Ok(())
}

/// Cancels probes still open `stale` after they were created (nobody waits for them).
pub async fn sweep(db: &Db, stale: Duration) -> Result<usize, JobsError> {
    let jobs: Vec<Uuid> = sqlx::query_scalar(
        "SELECT j.id FROM resource_estimates e JOIN jobs j ON j.id = e.job_id
          WHERE j.state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'REJECTED', 'CANCELLING')
            AND e.created_at < now() - make_interval(secs => $1)
          LIMIT 100",
    )
    .bind(stale.as_secs_f64())
    .fetch_all(db.pool())
    .await?;
    for j in &jobs {
        stop(db, *j).await?;
    }
    Ok(jobs.len())
}

fn probe_estimate(job: &JobRow) -> Option<Uuid> {
    let s = job.subject_ref.as_ref()?;
    (s["kind"] == PROBE_KIND)
        .then(|| s["estimate_id"].as_str()?.parse().ok())
        .flatten()
}

/// Launch contributor (`// <mrl:launch-contributors P28>`).
pub struct ProbeLaunch {
    db: Db,
}

impl ProbeLaunch {
    /// Reads estimates, runs and engines from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

fn transient(e: sqlx::Error) -> LaunchError {
    LaunchError::Transient(format!("the database did not answer: {e}"))
}

#[async_trait::async_trait]
impl LaunchContributor for ProbeLaunch {
    fn name(&self) -> &'static str {
        "run_probe"
    }

    async fn contribute(
        &self,
        job: &JobRow,
        attempt: &AttemptRow,
        placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        if let Some(run) = job.run_id {
            return self.check_resume(job, run, attempt).await;
        }
        let Some(estimate) = probe_estimate(job) else {
            return Ok(());
        };
        let row = sqlx::query(
            "SELECT e.probe_spec, te.name, ev.version, ev.manifest FROM resource_estimates e
               JOIN engine_versions ev ON ev.id = e.engine_version_id
               JOIN training_engines te ON te.id = ev.engine_id
              WHERE e.id = $1 AND e.job_id = $2",
        )
        .bind(estimate)
        .bind(job.id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(transient)?
        .ok_or_else(|| {
            LaunchError::Fail(
                FailureClass::HarnessError,
                "The settings check lost its request.".into(),
            )
        })?;
        let spec: ProbeSpec = serde_json::from_value(row.try_get("probe_spec").map_err(transient)?)
            .map_err(|e| {
                LaunchError::Fail(
                    FailureClass::HarnessError,
                    format!("The settings check's request does not read: {e}"),
                )
            })?;
        let (name, version): (String, String) = (
            row.try_get("name").map_err(transient)?,
            row.try_get("version").map_err(transient)?,
        );
        let manifest: Value = row.try_get("manifest").map_err(transient)?;
        let mut job_spec = match spec.resolved {
            Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        for (k, v) in [
            ("job_id", json!(JobId::from_uuid(job.id).to_string())),
            ("job_kind", json!("TRAIN")),
            ("algorithm", json!(spec.algorithm)),
            ("base_model_version_id", json!(spec.base_model_version_id)),
            ("dataset_version_ids", json!(spec.dataset_version_ids)),
            ("reward_version_id", json!(spec.reward_version_id)),
            ("seeds", spec.seeds),
        ] {
            job_spec.insert(k.to_owned(), v);
        }
        plan.launch.job_spec_json = Value::Object(job_spec).to_string();
        plan.launch.dry_run = true;
        let os = os_of(&self.db, placement.node_id)
            .await
            .map_err(transient)?;
        for role in &mut plan.launch.roles {
            let env = environment(&manifest["spec"], &role.kind, "cpu", os).ok_or_else(|| {
                LaunchError::Fail(
                    FailureClass::DependencyEnv,
                    format!("The engine {name} {version} has no processor-only runtime to check the settings with."),
                )
            })?;
            role.runtime_ref = runtime_ref(env).ok_or_else(|| {
                LaunchError::Fail(
                    FailureClass::DependencyEnv,
                    format!(
                        "The engine {name} {version} names an environment it does not describe."
                    ),
                )
            })?;
            if role.primary {
                plan.launch.environment_id = env["id"].as_str().unwrap_or_default().to_owned();
                plan.launch.environment_digest =
                    env["lockSha256"].as_str().unwrap_or_default().to_owned();
            }
        }
        plan.launch.engine_name = name;
        plan.launch.engine_version = version;
        Ok(())
    }
}

impl ProbeLaunch {
    async fn check_resume(
        &self,
        job: &JobRow,
        run: Uuid,
        attempt: &AttemptRow,
    ) -> Result<(), LaunchError> {
        let Some(checkpoint) = attempt.resume_from_checkpoint_id else {
            return Ok(());
        };
        let run_row: Option<(Option<Uuid>, String)> =
            sqlx::query_as("SELECT engine_version_id, algorithm_id FROM runs WHERE id = $1")
                .bind(run)
                .fetch_optional(self.db.pool())
                .await
                .map_err(transient)?;
        let Some((Some(engine), algorithm)) = run_row else {
            return Ok(());
        };
        match policy::resume_problem(&self.db, job.org_id, checkpoint, engine, &algorithm)
            .await
            .map_err(transient)?
        {
            Some(why) => Err(LaunchError::Fail(
                FailureClass::UserConfig,
                format!("{} ({})", why, super::codes::RESUME_INCOMPATIBLE),
            )),
            None => Ok(()),
        }
    }
}

/// The OS part of the machine's platform (`linux`, `darwin`, `windows`).
async fn os_of(db: &Db, node: Uuid) -> Result<Option<&'static str>, sqlx::Error> {
    let inventory: Option<Value> =
        sqlx::query_scalar("SELECT inventory FROM compute_nodes WHERE id = $1")
            .bind(node)
            .fetch_optional(db.pool())
            .await?
            .flatten();
    Ok(inventory.and_then(|i| match i["platform"].as_str()? {
        "PLATFORM_LINUX" | "PLATFORM_WSL2" => Some("linux"),
        "PLATFORM_MACOS" => Some("darwin"),
        "PLATFORM_WINDOWS" => Some("windows"),
        _ => None,
    }))
}

/// Completes probe jobs; abstains for every other job.
pub struct ProbeVerifier;

#[async_trait::async_trait]
impl CompletionVerifier for ProbeVerifier {
    fn name(&self) -> &'static str {
        "run-probe"
    }

    async fn verify(&self, job: &JobRow) -> Result<Verification, SeamError> {
        Ok(if job.run_id.is_none() && probe_estimate(job).is_some() {
            Verification::Verified
        } else {
            Verification::Abstain
        })
    }
}

/// Text PostgreSQL can store, cut to `max` characters.
fn clean(s: &str, max: usize) -> String {
    s.chars().filter(|c| *c != '\0').take(max).collect()
}

fn severity(s: i32) -> &'static str {
    use mrl_proto::common::v1::Severity;
    match Severity::try_from(s) {
        Ok(Severity::Error) => "error",
        Ok(Severity::Warning) => "warning",
        _ => "info",
    }
}

fn well_formed_code(c: &str) -> bool {
    c.parse::<mrl_domain::errors::ErrorCode>().is_ok()
}

/// The worker's answer as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    /// No blocking problem found.
    pub ok: bool,
    /// Issues (`[{severity, code, path, message, hint}]`).
    pub issues: Value,
    /// GPU memory per GPU, GiB.
    pub gpu: Option<(f64, f64)>,
    /// Planned optimizer steps.
    pub steps: Option<i64>,
    /// Wall time, seconds.
    pub wall: Option<(f64, f64)>,
    /// Assumptions (`[text]`).
    pub assumptions: Value,
}

/// `p` as stored: issues with checked codes and bounded text, ranges that are finite,
/// non-negative and ordered (otherwise dropped).
pub fn sanitize(p: &Prepared) -> Answer {
    let issues: Vec<Value> = p
        .issues
        .iter()
        .take(100)
        .map(|i| {
            let code = clean(&i.code, 32);
            json!({
                "severity": severity(i.severity),
                "code": if well_formed_code(&code) { code } else { "MRL-VAL-003".to_owned() },
                "path": (!i.path.is_empty()).then(|| clean(&i.path, 512)),
                "message": clean(&i.message, 2000),
                "hint": clean(&i.hint, 2000),
            })
        })
        .collect();
    let range = |lo: f64, hi: f64| {
        (lo.is_finite() && hi.is_finite() && lo >= 0.0 && lo <= hi && hi < 1.0e7 && hi > 0.0)
            .then_some((lo, hi))
    };
    let est = p.estimate.as_ref();
    let gpu = est.and_then(|e| range(e.gpu_mem_gib_lo, e.gpu_mem_gib_hi));
    let wall = est.and_then(|e| range(e.wall_seconds_lo, e.wall_seconds_hi));
    let steps = est
        .map(|e| e.total_steps)
        .filter(|s| *s > 0)
        .and_then(|s| i64::try_from(s).ok());
    let assumptions: Vec<String> = est
        .map(|e| {
            e.assumptions
                .iter()
                .take(50)
                .map(|a| clean(a, 500))
                .collect()
        })
        .unwrap_or_default();
    Answer {
        ok: p.ok,
        issues: Value::Array(issues),
        gpu,
        steps,
        wall,
        assumptions: json!(assumptions),
    }
}

/// CONTROL `worker.prepared`: a probe's answer.
pub struct PreparedSink {
    db: Db,
}

impl PreparedSink {
    /// Writes estimates to `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Stores `p` for the attempt `attempt` at `epoch`, sent by `node`. False when it changes
    /// nothing (not a probe, already answered, another machine or an old epoch).
    pub async fn store(
        &self,
        node: Uuid,
        attempt: Uuid,
        epoch: i64,
        p: &Prepared,
    ) -> Result<bool, sqlx::Error> {
        let Answer {
            ok,
            issues,
            gpu,
            steps,
            wall,
            assumptions,
        } = sanitize(p);
        let n = sqlx::query(
            "UPDATE resource_estimates e SET state = 'READY', node_id = $3, ok = $4, issues = $5,
                    gpu_mem_lo_gib = $6, gpu_mem_hi_gib = $7, total_steps = $8,
                    wall_lo_seconds = $9, wall_hi_seconds = $10, assumptions = $11,
                    decided_at = now()
               FROM job_attempts a
              WHERE a.id = $1 AND a.epoch = $2 AND e.job_id = a.job_id AND e.state = 'PENDING'
                AND EXISTS (SELECT 1 FROM attempt_placements p
                             WHERE p.attempt_id = a.id AND p.node_id = $3)",
        )
        .bind(attempt)
        .bind(epoch)
        .bind(node)
        .bind(ok)
        .bind(issues)
        .bind(gpu.map(|g| g.0))
        .bind(gpu.map(|g| g.1))
        .bind(steps)
        .bind(wall.map(|w| w.0))
        .bind(wall.map(|w| w.1))
        .bind(assumptions)
        .execute(self.db.pool())
        .await?
        .rows_affected();
        Ok(n == 1)
    }
}

#[async_trait::async_trait]
impl LaneSink for PreparedSink {
    fn name(&self) -> &'static str {
        "submission.prepared"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Control]
    }

    fn control_kinds(&self) -> &'static [&'static str] {
        &["worker.prepared"]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        let Some(Body::Worker(relay)) = &item.message.body else {
            return Ok(());
        };
        let Some(WorkerBody::Prepared(p)) = relay.message.as_ref().and_then(|m| m.body.as_ref())
        else {
            return Ok(());
        };
        let Some(r) = relay.attempt.as_ref() else {
            return Ok(());
        };
        // Bad input is dropped; only an unreachable database is retried.
        let (Ok(attempt), Ok(epoch)) = (
            r.attempt_id.parse::<AttemptId>(),
            i64::try_from(r.fencing_epoch),
        ) else {
            return Ok(());
        };
        self.store(item.node_id, attempt.as_uuid(), epoch, p)
            .await
            .map(|_| ())
            .map_err(|e| SinkError {
                sink: "submission.prepared",
                detail: e.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::common::v1::Issue;
    use mrl_proto::worker::v1::ResourceEstimate;

    #[test]
    fn hostile_answers_are_bounded() {
        let p = Prepared {
            ok: true,
            issues: (0..150)
                .map(|i| Issue {
                    code: if i == 0 {
                        "MRL-CAP-250".into()
                    } else {
                        "rm -rf /\0".into()
                    },
                    severity: 3,
                    path: "/engine\0/x".into(),
                    message: "m".repeat(5000),
                    hint: String::new(),
                })
                .collect(),
            estimate: Some(ResourceEstimate {
                gpu_mem_gib_lo: 9.0,
                gpu_mem_gib_hi: f64::NAN,
                total_steps: u64::MAX,
                wall_seconds_lo: 10.0,
                wall_seconds_hi: 20.0,
                assumptions: vec!["a\0b".into(); 80],
            }),
            effective_config_sha256: String::new(),
        };
        let Answer {
            ok,
            issues,
            gpu,
            steps,
            wall,
            assumptions,
        } = sanitize(&p);
        assert!(ok);
        let issues = issues.as_array().unwrap();
        assert_eq!(issues.len(), 100);
        assert_eq!(issues[0]["code"], "MRL-CAP-250");
        assert_eq!(issues[0]["severity"], "error");
        assert_eq!(issues[1]["code"], "MRL-VAL-003", "a bad code is replaced");
        assert_eq!(issues[1]["path"], "/engine/x");
        assert_eq!(issues[1]["message"].as_str().unwrap().len(), 2000);
        assert_eq!(gpu, None, "NaN drops the range");
        assert_eq!(steps, None, "too big for the column");
        assert_eq!(wall, Some((10.0, 20.0)));
        assert_eq!(assumptions.as_array().unwrap().len(), 50);
        assert_eq!(assumptions[0], "ab");
        let swapped = Prepared {
            estimate: Some(ResourceEstimate {
                gpu_mem_gib_lo: 9.0,
                gpu_mem_gib_hi: 3.0,
                ..ResourceEstimate::default()
            }),
            ..Prepared::default()
        };
        assert_eq!(sanitize(&swapped).gpu, None, "lo above hi");
    }
}
