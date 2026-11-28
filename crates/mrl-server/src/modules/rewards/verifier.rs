//! The REWARD_PREVIEW completion verifier: turns a finished job's report into the reward
//! records, in one transaction, and vouches only for jobs the rewards module started (every
//! other job: Abstain).
//!
//! The job uploads `report.json` (and, for previews, `rows.jsonl`) under its outputs. The
//! verifier does not trust the job's own pass/fail: it re-checks every case result against the
//! expectations stored with the version ([`case_passes`]), and requires every stored case
//! exactly once. Then:
//! - `test`: all cases pass (and deterministic components scored the same twice) → the
//!   version stays TESTING with a passing `test_summary` and the bundle hash the sandbox
//!   reported (`exec_sha256`); otherwise → FAILED_TESTS. Each case gets its `last_result`.
//! - `preview` / `compare`: the report row gets the server-computed statistics.
//! - `regression`: the version's `regression` is the comparison with the baseline.
//!
//! A report that can't be read marks the record INVALID (and a test run FAILED_TESTS) and
//! fails the job with VERIFICATION_FAILED. Replays after a crash find the record settled and
//! answer Verified again. Until INT-6 has registered the job's outputs the verifier answers an
//! error, which keeps the job FINALIZING for the next finalize pass (never RetryLater: that
//! would run the whole job again).
//!
//! [`EndedEarly`] settles the record of a job that ended (failed, cancelled, rejected) without
//! a report: INVALID, and a test run's version goes to FAILED_TESTS, so it never waits forever.

use super::policy::{Expect, Expected};
use super::stats::{self, Row};
use super::store::{self, CaseRow, ReportRow, VersionRow};
use super::RewardsError;
use crate::db::{Db, Tx};
use crate::modules::artifacts::{ArtifactStore, LOCAL_ORG};
use crate::modules::events::{ConsumerError, Event, EventConsumer};
use crate::modules::jobs::{scope, CompletionVerifier, JobRow, Rejection, Verification};
use crate::seams::{AuditOutcome, AuditSink, SeamError};
use async_trait::async_trait;
use mrl_domain::failure::{FailureClass, FailureCode};
use mrl_domain::ids::{JobId, ProjectId, RewardVersionId};
use mrl_domain::state::RewardVersionEvent;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

/// The job kind the rewards module runs.
pub const JOB_KIND: &str = "REWARD_PREVIEW";
/// The built-in task that serves it.
pub const TASK: &str = "reward_preview";
/// Report format tag.
pub const REPORT_FORMAT: &str = "mrl-reward-report/1";
/// Largest report or rows file read, in bytes.
pub const MAX_REPORT_BYTES: usize = 32 << 20;

/// The output files the verifier reads.
pub const OUTPUT_FILES: [&str; 2] = ["report.json", "rows.jsonl"];

/// A finished job's output files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFiles {
    /// The P19 artifact holding them.
    pub artifact_id: Uuid,
    /// `report.json` and, when present, `rows.jsonl`, checked against the artifact manifest.
    pub files: BTreeMap<String, Vec<u8>>,
    /// Why a listed file wasn't read (too large, changed in storage). Any makes the report
    /// unusable.
    pub problems: Vec<String>,
}

/// Where the verifier reads a job's outputs from.
#[async_trait]
pub trait ReportSource: Send + Sync {
    /// The job's output files, or `None` when it registered no outputs.
    async fn outputs(&self, job: &JobRow) -> Result<Option<JobFiles>, SeamError>;
}

/// One case's outcome in a report.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CaseResult {
    /// The case's position.
    pub position: i32,
    /// The total, or `null`.
    pub total: Option<f64>,
    /// `SCORED`, `MASKED` or `FALLBACK_APPLIED`.
    pub resolution: String,
    /// Raw component values.
    #[serde(default)]
    pub components: BTreeMap<String, Option<f64>>,
    /// Sample and component flags.
    #[serde(default)]
    pub flags: Vec<String>,
    /// Component statuses.
    #[serde(default)]
    pub statuses: BTreeMap<String, String>,
    /// Diagnostics per component (shown in the UI, never interpreted).
    #[serde(default)]
    pub diagnostics: BTreeMap<String, Value>,
    /// Deterministic components scored the same both times.
    pub repeatable: bool,
}

/// `report.json`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Report {
    /// [`REPORT_FORMAT`].
    pub format: String,
    /// `test`, `preview`, `regression` or `compare`.
    pub mode: String,
    /// `rwdv_…` of the version scored.
    pub reward_version_id: String,
    /// The bundle hash the sandbox reported (Describe).
    pub content_sha256: String,
    /// Test cases (test and regression).
    #[serde(default)]
    pub cases: Vec<CaseResult>,
    /// Baseline cases scored by the other version (regression).
    #[serde(default)]
    pub baseline_cases: Vec<CaseResult>,
    /// The other version's bundle hash (regression, compare).
    #[serde(default)]
    pub other_content_sha256: Option<String>,
    /// How the sandbox ran (profile, user code, …), kept for the record.
    #[serde(default)]
    pub sandbox: Value,
    /// Bounded preview examples available for the first publication snapshot.
    #[serde(default)]
    pub golden_samples: Vec<Value>,
}

fn matches(want: &Expected, got: f64) -> bool {
    match want {
        Expected::Exact(v) => (got - v).abs() <= 1e-9,
        Expected::Approx { approx, tol } => (got - approx).abs() <= *tol,
        Expected::Range { min, max } => {
            min.is_none_or(|m| got >= m) && max.is_none_or(|m| got <= m)
        }
    }
}

fn value_ok(want: Option<&Expected>, got: Option<f64>) -> bool {
    match (want, got) {
        (None, None) => true,
        (Some(w), Some(g)) => g.is_finite() && matches(w, g),
        _ => false,
    }
}

/// Why the result fails the expectation (empty: it passes).
pub fn case_passes(expect: &Expect, r: &CaseResult) -> Vec<String> {
    let mut why = Vec::new();
    if let Some(total) = &expect.total {
        if !value_ok(total.as_ref(), r.total) {
            why.push(format!("total was {}", show(r.total)));
        }
    }
    for (id, want) in &expect.components {
        let got = r.components.get(id).copied().flatten();
        if !value_ok(want.as_ref(), got) {
            why.push(format!("component {id} was {}", show(got)));
        }
    }
    for f in &expect.flags {
        if !r.flags.contains(f) {
            why.push(format!("flag {f} was missing"));
        }
    }
    for f in &expect.absent_flags {
        if r.flags.contains(f) {
            why.push(format!("flag {f} was set"));
        }
    }
    if let Some(res) = &expect.resolution {
        if &r.resolution != res {
            why.push(format!("resolution was {}", r.resolution));
        }
    }
    if !r.repeatable {
        why.push("a deterministic component scored differently the second time".into());
    }
    why
}

fn show(v: Option<f64>) -> String {
    v.map_or_else(|| "none".to_owned(), |x| x.to_string())
}

/// Every stored case exactly once; returns `(position, last_result, passed)` per case.
fn judge_cases(
    cases: &[CaseRow],
    results: &[CaseResult],
) -> Result<Vec<(i32, Value, bool)>, String> {
    let mut by_pos: BTreeMap<i32, &CaseResult> = BTreeMap::new();
    for r in results {
        if by_pos.insert(r.position, r).is_some() {
            return Err(format!("the report lists case {} twice", r.position));
        }
    }
    let known: BTreeSet<i32> = cases.iter().map(|c| c.position).collect();
    if let Some(extra) = by_pos.keys().find(|p| !known.contains(p)) {
        return Err(format!(
            "the report has a case {extra} the version doesn't have"
        ));
    }
    let mut out = Vec::new();
    for c in cases {
        let r = by_pos.get(&c.position).ok_or_else(|| {
            format!(
                "the report has no result for case {} ({})",
                c.position, c.name
            )
        })?;
        let expect: Expect = serde_json::from_value(c.expect.clone()).map_err(|e| {
            format!(
                "case {} has an expectation the server can't read: {e}",
                c.position
            )
        })?;
        let why = case_passes(&expect, r);
        let passed = why.is_empty();
        out.push((
            c.position,
            json!({"passed": passed, "failures": why, "total": r.total, "resolution": r.resolution,
                   "components": r.components, "statuses": r.statuses, "flags": r.flags,
                   "diagnostics": r.diagnostics}),
            passed,
        ));
    }
    Ok(out)
}

fn read_report(files: &JobFiles, report: &ReportRow) -> Result<Report, String> {
    let raw = files
        .files
        .get("report.json")
        .ok_or("the job wrote no report.json")?;
    if raw.len() > MAX_REPORT_BYTES {
        return Err("report.json is larger than 32 MiB".into());
    }
    let document: Value =
        serde_json::from_slice(raw).map_err(|e| format!("report.json can't be read: {e}"))?;
    scope::no_nul(&document).map_err(|_| "report.json contains a NUL character".to_owned())?;
    let r: Report =
        serde_json::from_value(document).map_err(|e| format!("report.json can't be read: {e}"))?;
    if r.format != REPORT_FORMAT {
        return Err(format!("report.json has format {}", r.format));
    }
    if r.mode != report.mode {
        return Err(format!(
            "the report is for mode {}, not {}",
            r.mode, report.mode
        ));
    }
    let id: Option<Uuid> = r
        .reward_version_id
        .parse::<RewardVersionId>()
        .ok()
        .map(|v| v.as_uuid());
    if id != Some(report.version_id) {
        return Err("the report is for another reward version".into());
    }
    if !is_sha(&r.content_sha256) {
        return Err("the report has no content hash from the sandbox".into());
    }
    Ok(r)
}

fn check_pins(
    report: &Report,
    version: &VersionRow,
    other: Option<&VersionRow>,
) -> Result<(), String> {
    if version
        .exec_sha256
        .as_ref()
        .is_some_and(|pin| pin != &report.content_sha256)
        || other.is_some_and(|v| {
            report.other_content_sha256.as_deref().is_none_or(|sha| {
                !is_sha(sha) || v.exec_sha256.as_deref().is_some_and(|pin| pin != sha)
            })
        })
    {
        return Err("the reward check scored a different bundle than the pinned version".into());
    }
    Ok(())
}

fn is_sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn read_rows(files: &JobFiles) -> Result<Vec<Row>, String> {
    let raw = files
        .files
        .get("rows.jsonl")
        .ok_or("the job wrote no rows.jsonl")?;
    if raw.len() > MAX_REPORT_BYTES {
        return Err("rows.jsonl is larger than 32 MiB".into());
    }
    std::str::from_utf8(raw)
        .map_err(|_| "rows.jsonl is not UTF-8 text".to_owned())?
        .lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(n, l)| {
            serde_json::from_str(l).map_err(|e| format!("rows.jsonl line {}: {e}", n + 1))
        })
        .collect()
}

/// What the verifier decided for one report.
#[derive(Serialize, Deserialize)]
enum Outcome {
    Test {
        cases: Vec<(i32, Value, bool)>,
        summary: Value,
        exec: String,
    },
    Results(Value),
    Preview {
        results: Value,
        golden_samples: Vec<Value>,
    },
    Regression(Value),
}

fn evaluate(
    report: &ReportRow,
    files: &JobFiles,
    cases: &[CaseRow],
    baseline_cases: &[CaseRow],
    warn_share: f64,
) -> Result<Outcome, String> {
    if !files.problems.is_empty() {
        return Err(files.problems.join("; "));
    }
    let r = read_report(files, report)?;
    match report.mode.as_str() {
        "test" => {
            if cases.is_empty() {
                return Err("the version has no test cases".into());
            }
            let judged = judge_cases(cases, &r.cases)?;
            let passed_n = judged.iter().filter(|c| c.2).count();
            let all = passed_n == judged.len();
            let summary = json!({"passed": all, "cases": judged.len(), "passed_cases": passed_n,
                "job_id": report.job_id.map(|j| mrl_domain::ids::JobId::from_uuid(j).to_string()),
                "content_sha256": r.content_sha256, "sandbox": r.sandbox});
            Ok(Outcome::Test {
                cases: judged,
                summary,
                exec: r.content_sha256,
            })
        }
        "preview" => {
            let rows = read_rows(files)?;
            let a: Vec<&Row> = rows.iter().collect();
            if r.golden_samples.len() > 16
                || serde_json::to_vec(&r.golden_samples)
                    .map_err(|e| e.to_string())?
                    .len()
                    > (128 << 10) + 64
                || r.golden_samples.iter().any(|sample| {
                    !sample["prompt"].is_string() || !sample["completion"].is_string()
                })
            {
                return Err("the frozen preview samples are invalid".into());
            }
            Ok(Outcome::Preview {
                results: json!({"summary": stats::summarize(&a, warn_share)}),
                golden_samples: r.golden_samples,
            })
        }
        "compare" => {
            let rows = read_rows(files)?;
            let a: Vec<&Row> = rows.iter().filter(|r| r.version != "b").collect();
            let b: Vec<&Row> = rows.iter().filter(|r| r.version == "b").collect();
            Ok(Outcome::Results(json!({
                "a": stats::summarize(&a, warn_share),
                "b": stats::summarize(&b, warn_share),
                "comparison": stats::compare(&rows),
            })))
        }
        "regression" => {
            let other = report
                .other_version_id
                .ok_or("a regression report needs a baseline version")?;
            // The new version and the baseline each scored the baseline's cases.
            let now = judge_cases(baseline_cases, &r.cases)?;
            let before = judge_cases(baseline_cases, &r.baseline_cases)?;
            let mut deltas = Vec::new();
            let mut newly_failing = Vec::new();
            let mut pairs = Vec::new();
            for (c, ((_, after_v, after_ok), (_, before_v, before_ok))) in
                baseline_cases.iter().zip(now.iter().zip(before.iter()))
            {
                let (a, b) = (before_v["total"].as_f64(), after_v["total"].as_f64());
                if let (Some(x), Some(y)) = (a, b) {
                    pairs.push((x, y));
                }
                if *before_ok && !*after_ok {
                    newly_failing.push(c.name.clone());
                }
                if deltas.len() < 200 {
                    deltas.push(json!({"name": c.name, "before": a, "after": b,
                        "delta": a.zip(b).map(|(x, y)| y - x)}));
                }
            }
            Ok(Outcome::Regression(json!({
                "against_version_id": RewardVersionId::from_uuid(other).to_string(),
                "cases": baseline_cases.len(),
                "newly_failing": newly_failing,
                "spearman": stats::spearman(&pairs),
                "deltas": deltas,
                "job_id": report.job_id.map(|j| mrl_domain::ids::JobId::from_uuid(j).to_string()),
            })))
        }
        other => Err(format!("unknown report mode {other}")),
    }
}

fn seam(e: impl std::fmt::Display) -> SeamError {
    SeamError::new("rewards", e.to_string())
}

/// Reads a REWARD_PREVIEW job's outputs from the artifact INT-6 registered for it
/// (`builtin::outputs`), with each file's size and hash checked against its manifest.
pub struct BuiltinOutputs {
    db: Db,
    store: Option<Arc<ArtifactStore>>,
}

impl BuiltinOutputs {
    /// With the store the rewards module registered for `db` ([`super::storage`]).
    pub fn registered(db: Db) -> Self {
        Self { db, store: None }
    }

    /// With an explicit store (tests).
    pub fn with_store(db: Db, store: Arc<ArtifactStore>) -> Self {
        Self {
            db,
            store: Some(store),
        }
    }
}

#[async_trait]
impl ReportSource for BuiltinOutputs {
    async fn outputs(&self, job: &JobRow) -> Result<Option<JobFiles>, SeamError> {
        let Some(out) = crate::modules::builtin::outputs(&self.db, job.org_id, job.id)
            .await
            .map_err(seam)?
        else {
            return Ok(None);
        };
        let store = self
            .store
            .clone()
            .or_else(|| super::storage::lookup(&self.db).map(|s| s.store))
            .ok_or_else(|| seam("the rewards module's storage isn't set up yet"))?;
        let org = scope::org_text(job.org_id).unwrap_or_else(|| LOCAL_ORG.to_owned());
        let artifact = store
            .get(&org, out.artifact_id)
            .await
            .map_err(seam)?
            .ok_or_else(|| seam("the job's outputs artifact is missing"))?;
        let mut found = JobFiles {
            artifact_id: out.artifact_id,
            files: BTreeMap::new(),
            problems: Vec::new(),
        };
        let listed = artifact.manifest["files"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for name in OUTPUT_FILES {
            let Some(entry) = listed.iter().find(|f| f["path"] == name) else {
                continue;
            };
            if entry["size"]
                .as_u64()
                .is_none_or(|n| n > MAX_REPORT_BYTES as u64)
            {
                found.problems.push(format!("{name} is larger than 32 MiB"));
                continue;
            }
            let (key, file) = store
                .download_source(&org, &artifact, name)
                .await
                .map_err(seam)?;
            let bytes = store
                .org_store(&org)
                .await
                .map_err(seam)?
                .get(&key)
                .await
                .map_err(seam)?;
            if mrl_storage::sha256_hex(&bytes) != file.sha256 {
                found
                    .problems
                    .push(format!("{name} changed in storage after it was saved"));
                continue;
            }
            found.files.insert(name.to_owned(), bytes.to_vec());
        }
        Ok(Some(found))
    }
}

/// The verifier (registered in `jobs::chains` under the P29 marker).
pub struct ReportVerifier {
    db: Db,
    source: Arc<dyn ReportSource>,
    warn_share: Option<f64>,
}

impl ReportVerifier {
    /// A verifier that reads outputs from `source`.
    pub fn new(db: Db, source: Arc<dyn ReportSource>, warn_share: f64) -> Self {
        Self {
            db,
            source,
            warn_share: Some(warn_share),
        }
    }

    /// The chain member: outputs from [`BuiltinOutputs`], the learnability threshold from the
    /// rewards module's settings for `db`.
    pub fn registered(db: Db) -> Self {
        Self {
            source: Arc::new(BuiltinOutputs::registered(db.clone())),
            db,
            warn_share: None,
        }
    }

    fn warn_share(&self) -> f64 {
        self.warn_share
            .or_else(|| super::storage::lookup(&self.db).map(|s| s.config.learnability_warn_share))
            .unwrap_or_else(|| super::RewardsConfig::default().learnability_warn_share)
    }

    async fn record(&self, job: &JobRow, report: ReportRow) -> Result<Verification, RewardsError> {
        // Not registered yet: wait (an error keeps the job FINALIZING). INT-6's verifier
        // fails a job whose outputs can't be registered.
        let Some(files) = self.source.outputs(job).await? else {
            return Err(seam("the reward check's results are still being saved").into());
        };
        let version = store::version(&self.db, report.version_id).await?;
        let cases = store::cases(&self.db, report.version_id).await?;
        let baseline = match report.other_version_id {
            Some(o) if report.mode == "regression" => store::regression_cases(&self.db, o).await?,
            _ => Vec::new(),
        };
        let other = match report.other_version_id {
            Some(id) => store::version(&self.db, id).await?,
            None => None,
        };
        let outcome = (|| {
            let version = version.as_ref().ok_or("the reward version is gone")?;
            let result = read_report(&files, &report)?;
            check_pins(&result, version, other.as_ref())?;
            evaluate(&report, &files, &cases, &baseline, self.warn_share())
        })();
        let mut tx = self.db.begin().await?;
        let Some(current) =
            crate::modules::jobs::JobTransitions::lock(&mut tx, job.org_id, job.id).await?
        else {
            return Ok(Verification::Abstain);
        };
        if current.state_version != job.state_version || current.state != "FINALIZING" {
            return Err(seam("the reward check's job changed during verification").into());
        }
        let Some(locked) = store::lock_report_for_job(&mut tx, job.id).await? else {
            return Ok(Verification::Abstain);
        };
        if locked.state != "PENDING" {
            return Ok(if locked.state == "READY" {
                Verification::Verified
            } else {
                Verification::Failed(invalid(locked.reason.as_deref().unwrap_or("")))
            });
        }
        let prepared = serde_json::to_value(&outcome).map_err(seam)?;
        sqlx::query(
            "UPDATE reward_reports SET prepared_result = $2, prepared_artifact_id = $3 WHERE id = $1",
        )
        .bind(report.id)
        .bind(prepared)
        .bind(files.artifact_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await.map_err(crate::db::DbError::from)?;
        Ok(match outcome {
            Ok(_) => Verification::Verified,
            Err(why) => Verification::Failed(invalid(&why)),
        })
    }
}

fn invalid(why: &str) -> Rejection {
    Rejection::failure(
        FailureCode::class_only(FailureClass::VerificationFailed),
        format!("The reward check's report can't be used: {why}."),
    )
}

/// Applies a test run to the locked version: only the run the version waits for counts.
pub async fn test_result(
    tx: &mut crate::db::Tx,
    v: &VersionRow,
    job: Uuid,
    cases: &[(i32, Value, bool)],
    summary: &Value,
    exec: &str,
) -> Result<(), RewardsError> {
    if v.state != "TESTING" || v.test_job_id != Some(job) {
        return Ok(());
    }
    let results: Vec<(i32, Value)> = cases.iter().map(|(p, r, _)| (*p, r.clone())).collect();
    store::record_case_results(tx, v.id, &results).await?;
    let passed = summary.get("passed").and_then(Value::as_bool) == Some(true);
    sqlx::query("UPDATE reward_versions SET test_summary = $2, exec_sha256 = $3 WHERE id = $1")
        .bind(v.id)
        .bind(summary)
        .bind(passed.then_some(exec))
        .execute(&mut **tx)
        .await?;
    let payload = json!({"job_id": mrl_domain::ids::JobId::from_uuid(job).to_string(),
                         "passed": passed});
    if passed {
        store::note(tx, v, "reward_version.tests_passed", payload).await?;
    } else {
        store::step(tx, v, RewardVersionEvent::FailTests, payload).await?;
    }
    Ok(())
}

#[async_trait]
impl CompletionVerifier for ReportVerifier {
    fn name(&self) -> &'static str {
        "rewards"
    }

    async fn verify(&self, job: &JobRow) -> Result<Verification, SeamError> {
        if job.kind != JOB_KIND {
            return Ok(Verification::Abstain);
        }
        let report = sqlx::query_as::<_, ReportRow>(concat!(
            "SELECT ",
            crate::reward_report_cols!(),
            " FROM reward_reports WHERE job_id = $1"
        ))
        .bind(job.id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|e| SeamError::new("rewards", e.to_string()))?;
        let Some(report) = report else {
            return Ok(Verification::Abstain);
        };
        // Errors (storage down, outputs not registered yet) keep the job FINALIZING.
        self.record(job, report).await.map_err(seam)
    }
}

/// Settles a reward report after the check has reached a terminal job state.
pub struct EndedEarly(Db);

impl EndedEarly {
    /// Uses the core's audit database.
    pub fn new(db: Db) -> Self {
        Self(db)
    }
}

#[async_trait]
impl EventConsumer for EndedEarly {
    fn name(&self) -> &'static str {
        "rewards-report-ended"
    }

    fn accepts(&self, t: &str) -> bool {
        matches!(
            t,
            "job.completed" | "job.failed" | "job.cancelled" | "job.rejected"
        )
    }

    async fn handle(&self, tx: &mut Tx, event: &Event) -> Result<(), ConsumerError> {
        let Some(report) = store::lock_report_for_job(tx, event.aggregate_id).await? else {
            return Ok(());
        };
        if report.state != "PENDING" {
            return Ok(());
        }
        let (prepared, artifact): (Option<Value>, Option<Uuid>) = sqlx::query_as(
            "SELECT prepared_result, prepared_artifact_id FROM reward_reports WHERE id = $1",
        )
        .bind(report.id)
        .fetch_one(&mut **tx)
        .await?;
        let staged: Option<Result<Outcome, String>> =
            prepared.map(serde_json::from_value).transpose()?;
        let (result, artifact) = match event.event_type.as_str() {
            "job.completed" => {
                let outcome =
                    staged.ok_or("completed reward check is missing verification evidence")??;
                let result = match outcome {
                    Outcome::Test {
                        cases,
                        summary,
                        exec,
                    } => {
                        if let Some(v) = store::lock_version(tx, report.version_id).await? {
                            test_result(tx, &v, event.aggregate_id, &cases, &summary, &exec)
                                .await?;
                        }
                        summary
                    }
                    Outcome::Results(results) => results,
                    Outcome::Preview {
                        results,
                        golden_samples,
                    } => {
                        sqlx::query("UPDATE reward_reports SET golden_samples = $2 WHERE id = $1")
                            .bind(report.id)
                            .bind(json!(golden_samples))
                            .execute(&mut **tx)
                            .await?;
                        results
                    }
                    Outcome::Regression(results) => {
                        let definition: Uuid = sqlx::query_scalar(
                            "SELECT reward_id FROM reward_versions WHERE id = $1",
                        )
                        .bind(report.version_id)
                        .fetch_one(&mut **tx)
                        .await?;
                        store::lock_definition(tx, definition).await?;
                        if let Some(v) = store::lock_version(tx, report.version_id).await? {
                            let baseline = store::latest_published(tx, v.reward_id, v.id).await?;
                            if matches!(v.state.as_str(), "DRAFT" | "TESTING" | "FAILED_TESTS")
                                && baseline.map(|b| b.id) == report.other_version_id
                            {
                                sqlx::query(
                                    "UPDATE reward_versions SET regression = $2 WHERE id = $1",
                                )
                                .bind(v.id)
                                .bind(&results)
                                .execute(&mut **tx)
                                .await?;
                            }
                        }
                        results
                    }
                };
                (Ok(result), artifact)
            }
            terminal => {
                let why = match (terminal, staged) {
                    ("job.cancelled", _) => "the check was cancelled before it finished".into(),
                    ("job.rejected", _) => "the check could not be started".into(),
                    (_, Some(Err(why))) => why,
                    _ => "the check stopped before it wrote its report".into(),
                };
                if report.mode == "test" {
                    if let Some(v) = store::lock_version(tx, report.version_id).await? {
                        let summary = json!({"passed": false, "error": why});
                        test_result(tx, &v, event.aggregate_id, &[], &summary, "").await?;
                    }
                }
                (Err(why), None)
            }
        };
        store::settle_report(tx, report.id, result, artifact).await?;
        let mut audit = crate::modules::audit::event(
            event.created_at,
            "system:rewards",
            scope::org_text(report.org_id).as_deref(),
            "rewards.report_settled",
            &JobId::from_uuid(event.aggregate_id).to_string(),
            AuditOutcome::Success,
            "",
        );
        audit.project = Some(ProjectId::from_uuid(report.project_id).to_string());
        crate::modules::audit::PgAuditSink::new(self.0.clone())
            .record_in(tx, audit)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect(v: Value) -> Expect {
        serde_json::from_value(v).unwrap()
    }

    fn result(total: Option<f64>) -> CaseResult {
        CaseResult {
            position: 0,
            total,
            resolution: if total.is_some() { "SCORED" } else { "MASKED" }.into(),
            components: BTreeMap::from([("c".to_owned(), Some(1.0))]),
            flags: vec!["gated".into()],
            statuses: BTreeMap::new(),
            diagnostics: BTreeMap::new(),
            repeatable: true,
        }
    }

    #[test]
    fn the_matcher_reads_every_expectation_kind() {
        assert!(case_passes(&expect(json!({"total": 1.0})), &result(Some(1.0))).is_empty());
        assert!(!case_passes(&expect(json!({"total": 1.0})), &result(Some(0.9))).is_empty());
        assert!(case_passes(
            &expect(json!({"total": {"approx": 1.0, "tol": 0.2}})),
            &result(Some(0.9))
        )
        .is_empty());
        assert!(
            case_passes(&expect(json!({"total": {"min": 0.5}})), &result(Some(0.9))).is_empty()
        );
        assert!(
            !case_passes(&expect(json!({"total": {"max": 0.5}})), &result(Some(0.9))).is_empty()
        );
        assert!(case_passes(
            &expect(json!({"total": null, "resolution": "MASKED"})),
            &result(None)
        )
        .is_empty());
        assert!(!case_passes(&expect(json!({"total": null})), &result(Some(0.0))).is_empty());
        assert!(case_passes(
            &expect(json!({"components": {"c": 1}, "flags": ["gated"]})),
            &result(Some(1.0))
        )
        .is_empty());
        let why = case_passes(
            &expect(json!({"absent_flags": ["gated"], "components": {"c": 0}})),
            &result(Some(1.0)),
        );
        assert_eq!(why.len(), 2, "{why:?}");
        let mut flaky = result(Some(1.0));
        flaky.repeatable = false;
        assert!(!case_passes(&expect(json!({"total": 1.0})), &flaky).is_empty());
    }

    fn case_row(position: i32, expect: Value) -> CaseRow {
        CaseRow {
            position,
            name: format!("c{position}"),
            sample: json!({}),
            expect,
            last_result: None,
        }
    }

    #[test]
    fn every_stored_case_must_be_reported_exactly_once() {
        let cases = [
            case_row(0, json!({"total": 1})),
            case_row(1, json!({"total": 1})),
        ];
        let mut r0 = result(Some(1.0));
        r0.position = 0;
        assert!(judge_cases(&cases, std::slice::from_ref(&r0))
            .unwrap_err()
            .contains("case 1"));
        assert!(judge_cases(&cases[..1], &[r0.clone(), r0.clone()])
            .unwrap_err()
            .contains("twice"));
        let mut r9 = r0.clone();
        r9.position = 9;
        assert!(judge_cases(&cases[..1], &[r0.clone(), r9])
            .unwrap_err()
            .contains("case 9"));
        let mut r1 = r0.clone();
        r1.position = 1;
        r1.total = Some(0.0);
        let judged = judge_cases(&cases, &[r0, r1]).unwrap();
        assert_eq!(
            judged.iter().map(|c| c.2).collect::<Vec<_>>(),
            [true, false]
        );
        assert_eq!(judged[1].1["failures"][0], "total was 0");
    }

    fn report_row(mode: &str) -> ReportRow {
        ReportRow {
            id: Uuid::nil(),
            org_id: Uuid::nil(),
            project_id: Uuid::nil(),
            version_id: Uuid::nil(),
            other_version_id: None,
            mode: mode.into(),
            job_id: None,
            state: "PENDING".into(),
            request: json!({}),
            results: None,
            reason: None,
            artifact_id: None,
            created_by: "t".into(),
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            finished_at: None,
        }
    }

    #[test]
    fn a_damaged_or_oversized_output_file_makes_the_report_unusable() {
        let files = JobFiles {
            artifact_id: Uuid::nil(),
            files: BTreeMap::new(),
            problems: vec!["report.json changed in storage after it was saved".into()],
        };
        let why = evaluate(&report_row("preview"), &files, &[], &[], 0.9)
            .err()
            .unwrap();
        assert!(why.contains("changed in storage"), "{why}");
    }
}
