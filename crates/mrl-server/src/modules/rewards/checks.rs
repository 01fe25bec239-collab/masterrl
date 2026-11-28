//! Starting reward checks: each one is a REWARD_PREVIEW built-in job (INT-6's
//! [`builtin::submit`]) running the `reward_preview` task in the `code_exec` sandbox, plus a
//! PENDING `reward_reports` row that [`super::verifier::ReportVerifier`] settles from the job's
//! report (or [`super::verifier::EndedEarly`], when the job ends without one).
//!
//! The task's inputs (see `mrl_runtime/builtin/reward_preview.py`):
//! `{mode, reward, other?, cases?, baseline_cases?, preview?}`, where `reward` and `other`
//! are the versions' job-spec entries ([`super::launch::spec_entry_at`]) with their code files
//! staged as `reward.code/<file>` and `reward_b.code/<file>`; previews and comparisons stage
//! the dataset (and the model, to write answers) as the folder inputs `dataset` and `model`.

use super::launch::spec_entry_at;
use super::store::{CaseRow, ReportRow, VersionRow};
use super::verifier::{JOB_KIND, TASK};
use super::RewardsError;
use crate::db::Tx;
use crate::modules::artifacts::{Idempotency, InputFile};
use crate::modules::builtin::{self, BuiltinJob};
use mrl_domain::ids::RewardVersionId;
use serde_json::{json, Value};
use uuid::Uuid;

/// Input names and staging folders of the first and second version's code files.
pub const CODE: [(&str, &str); 2] = [
    ("reward.code", "reward/code"),
    ("reward_b.code", "reward_b/code"),
];
/// Folder input names of a preview's dataset and model.
pub const DATASET: &str = "dataset";
/// See [`DATASET`].
pub const MODEL: &str = "model";

/// What a check does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Score the version's test cases twice (the publish gate).
    Test,
    /// Score dataset samples.
    Preview,
    /// Score the published baseline's test cases with both versions.
    Regression,
    /// Score dataset samples with two versions.
    Compare,
}

impl Mode {
    /// The `mode` the task and the report use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::Preview => "preview",
            Self::Regression => "regression",
            Self::Compare => "compare",
        }
    }
}

/// A check to start.
pub struct NewCheck<'a> {
    /// What it does.
    pub mode: Mode,
    /// The version checked (the report belongs to it).
    pub version: &'a VersionRow,
    /// The baseline (regression) or the second version (compare).
    pub other: Option<&'a VersionRow>,
    /// The version's test cases (test).
    pub cases: &'a [CaseRow],
    /// The baseline's test cases (regression).
    pub baseline_cases: &'a [CaseRow],
    /// Preview settings (preview, compare), with the dataset and model input names.
    pub preview: Option<Value>,
    /// Every file to stage: code files, dataset, model.
    pub files: Vec<InputFile>,
    /// What was asked, kept on the report.
    pub request: Value,
    /// The caller's principal id.
    pub created_by: String,
}

fn case_inputs(cases: &[CaseRow]) -> Value {
    Value::Array(
        cases
            .iter()
            .map(|c| {
                json!({"position": c.position, "name": c.name, "sample": c.sample,
                       "expect": c.expect})
            })
            .collect(),
    )
}

/// The task's inputs for `check`.
pub fn inputs(check: &NewCheck<'_>) -> Value {
    let mut inputs = json!({
        "mode": check.mode.as_str(),
        "reward": spec_entry_at(check.version, CODE[0].1),
    });
    if let Some(o) = check.other {
        inputs["other"] = spec_entry_at(o, CODE[1].1);
    }
    match check.mode {
        Mode::Test => inputs["cases"] = case_inputs(check.cases),
        Mode::Regression => inputs["baseline_cases"] = case_inputs(check.baseline_cases),
        Mode::Preview | Mode::Compare => {
            inputs["preview"] = check.preview.clone().unwrap_or_else(|| json!({}));
        }
    }
    inputs
}

/// Replays a stored check under the same transaction lock used for starting it.
pub(super) async fn replay(
    tx: &mut Tx,
    org: Uuid,
    principal: &str,
    idem: Option<&Idempotency>,
) -> Result<Option<ReportRow>, RewardsError> {
    let Some(idem) = idem else { return Ok(None) };
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("rewards:{org}:{principal}:{}", idem.key))
        .execute(&mut **tx)
        .await?;
    let row = sqlx::query_as::<_, (Uuid, Option<String>)>(
        "SELECT id, request_sha256 FROM reward_reports
         WHERE org_id = $1 AND created_by = $2 AND idempotency_key = $3",
    )
    .bind(org)
    .bind(principal)
    .bind(&idem.key)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((id, fingerprint)) = row else {
        return Ok(None);
    };
    if fingerprint.as_deref() != Some(&idem.request_sha256) {
        return Err(crate::error::ApiError::new(
            crate::error::ErrorCode::IdempotencyMismatch,
            "This key was already used for a different reward check.",
        )
        .into());
    }
    Ok(sqlx::query_as::<_, ReportRow>(concat!(
        "SELECT ",
        crate::reward_report_cols!(),
        " FROM reward_reports WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?)
}

/// Creates the check's job (QUEUED) and its PENDING report in `tx`.
pub(super) async fn start(
    tx: &mut Tx,
    check: NewCheck<'_>,
    idem: Option<&Idempotency>,
) -> Result<ReportRow, RewardsError> {
    let v = check.version;
    let job = builtin::submit(
        tx,
        BuiltinJob {
            org_id: v.org_id,
            project_id: v.project_id,
            kind: JOB_KIND.to_owned(),
            task: TASK.to_owned(),
            inputs: inputs(&check),
            files: check.files,
            subject: json!({"reward_version_id": RewardVersionId::from_uuid(v.id).to_string()}),
            created_by: check.created_by.clone(),
            grants: Vec::new(),
            node_id: None,
        },
    )
    .await?;
    Ok(sqlx::query_as::<_, ReportRow>(concat!(
        "INSERT INTO reward_reports
            (id, org_id, project_id, version_id, other_version_id, mode, job_id, state, request,
             created_by, idempotency_key, request_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8, $9, $10, $11) RETURNING ",
        crate::reward_report_cols!()
    ))
    .bind(Uuid::now_v7())
    .bind(v.org_id)
    .bind(v.project_id)
    .bind(v.id)
    .bind(check.other.map(|o| o.id))
    .bind(check.mode.as_str())
    .bind(job.id)
    .bind(&check.request)
    .bind(&check.created_by)
    .bind(idem.map(|i| &i.key))
    .bind(idem.map(|i| &i.request_sha256))
    .fetch_one(&mut **tx)
    .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    fn version(id: u128, code: bool) -> VersionRow {
        VersionRow {
            id: Uuid::from_u128(id),
            org_id: Uuid::nil(),
            project_id: Uuid::nil(),
            reward_id: Uuid::nil(),
            version_no: 1,
            state: "DRAFT".into(),
            state_version: 1,
            policy: json!({"kind": "RewardPolicy"}),
            content_sha256: "c".repeat(64),
            hash_inputs: json!({}),
            sandbox_tier: "standard".into(),
            code_artifact_id: code.then(Uuid::now_v7),
            code_bundle_sha256: None,
            code_files: json!([]),
            model_refs: Vec::new(),
            notes: String::new(),
            test_summary: None,
            regression: None,
            exec_sha256: None,
            test_job_id: None,
            created_by: "t".into(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            published_at: None,
            deprecated_at: None,
        }
    }

    fn case(position: i32) -> CaseRow {
        CaseRow {
            position,
            name: format!("c{position}"),
            sample: json!({"prompt": "p", "completion": "c"}),
            expect: json!({"total": 1.0}),
            last_result: Some(json!({"passed": true})),
        }
    }

    fn check<'a>(
        mode: Mode,
        v: &'a VersionRow,
        other: Option<&'a VersionRow>,
        cases: &'a [CaseRow],
    ) -> NewCheck<'a> {
        NewCheck {
            mode,
            version: v,
            other,
            cases,
            baseline_cases: cases,
            preview: Some(json!({"n": 3, "dataset": DATASET})),
            files: Vec::new(),
            request: json!({}),
            created_by: "t".into(),
        }
    }

    #[test]
    fn each_mode_gets_the_inputs_the_task_reads() {
        let (a, b) = (version(1, true), version(2, false));
        let cases = [case(0), case(1)];
        let t = inputs(&check(Mode::Test, &a, None, &cases));
        assert_eq!(t["mode"], "test");
        assert_eq!(t["reward"]["code_dir"], "reward/code");
        assert_eq!(t["cases"].as_array().unwrap().len(), 2);
        assert_eq!(
            t["cases"][1],
            json!({"position": 1, "name": "c1", "sample": {"prompt": "p", "completion": "c"},
                   "expect": {"total": 1.0}}),
            "the case without its last result"
        );
        assert!(t.get("other").is_none() && t.get("preview").is_none());

        let r = inputs(&check(Mode::Regression, &a, Some(&b), &cases));
        assert_eq!(
            r["other"]["version_id"],
            RewardVersionId::from_uuid(b.id).to_string()
        );
        assert_eq!(r["other"]["code_dir"], Value::Null, "no code bundle");
        assert_eq!(r["baseline_cases"].as_array().unwrap().len(), 2);
        assert!(r.get("cases").is_none());

        let c = inputs(&check(Mode::Compare, &b, Some(&a), &[]));
        assert_eq!(c["other"]["code_dir"], "reward_b/code");
        assert_eq!(c["preview"], json!({"n": 3, "dataset": "dataset"}));
        let p = inputs(&check(Mode::Preview, &a, None, &[]));
        assert_eq!(
            (p["mode"].clone(), p["preview"]["n"].clone()),
            (json!("preview"), json!(3))
        );
    }
}
