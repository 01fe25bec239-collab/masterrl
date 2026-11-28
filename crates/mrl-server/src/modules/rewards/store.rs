//! Reward storage: definitions, versions, test cases, reports and the component list.
//!
//! Lock order: definition → version → report. Every state step is a compare-and-set on
//! `state_version` and writes its outbox event in the same transaction.

use super::hash::HashInputs;
use super::policy::TestCase;
use super::{codes, Catalog, RewardsError};
use crate::db::{Db, Tx};
use crate::modules::events::{NewEvent, OutboxWriter};
use axum::http::StatusCode;
use mrl_domain::ids::RewardId;
use mrl_domain::state::{RewardVersion, RewardVersionEvent, RewardVersionState, StateMachine};
use serde_json::{json, Value};
use sqlx::Row;
use std::str::FromStr;
use time::OffsetDateTime;
use uuid::Uuid;

/// A reward definition.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct DefinitionRow {
    /// Id.
    pub id: Uuid,
    /// Org.
    pub org_id: Uuid,
    /// Project.
    pub project_id: Uuid,
    /// Name (unique in the project).
    pub name: String,
    /// Description.
    pub description: String,
    /// The next version number.
    pub next_version: i32,
    /// Who made it.
    pub created_by: String,
    /// When.
    pub created_at: OffsetDateTime,
}

/// A reward version.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct VersionRow {
    /// Id.
    pub id: Uuid,
    /// Org.
    pub org_id: Uuid,
    /// Project.
    pub project_id: Uuid,
    /// Its definition.
    pub reward_id: Uuid,
    /// 1, 2, …
    pub version_no: i32,
    /// Lifecycle state.
    pub state: String,
    /// Event sequence.
    pub state_version: i64,
    /// RewardPolicy v1.
    pub policy: Value,
    /// Content hash.
    pub content_sha256: String,
    /// What the hash covers.
    pub hash_inputs: Value,
    /// `standard` or `code_exec`.
    pub sandbox_tier: String,
    /// Code bundle artifact.
    pub code_artifact_id: Option<Uuid>,
    /// Code bundle hash.
    pub code_bundle_sha256: Option<String>,
    /// `[{path, sha256, size}]`.
    pub code_files: Value,
    /// Model versions read.
    pub model_refs: Vec<Uuid>,
    /// Notes.
    pub notes: String,
    /// Last test run's summary.
    pub test_summary: Option<Value>,
    /// Regression report against the previous published version.
    pub regression: Option<Value>,
    /// Bundle hash the sandbox reported.
    pub exec_sha256: Option<String>,
    /// The test job the version waits for.
    pub test_job_id: Option<Uuid>,
    /// Who made it.
    pub created_by: String,
    /// When.
    pub created_at: OffsetDateTime,
    /// When it was published.
    pub published_at: Option<OffsetDateTime>,
    /// When it was deprecated.
    pub deprecated_at: Option<OffsetDateTime>,
}

impl VersionRow {
    /// The parsed state.
    pub fn state(&self) -> Result<RewardVersionState, RewardsError> {
        RewardVersionState::from_str(&self.state).map_err(|e| {
            RewardsError::problem(codes::ILLEGAL_STEP, StatusCode::CONFLICT, e.to_string())
        })
    }
}

/// A test case as stored.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct CaseRow {
    /// 0-based order.
    pub position: i32,
    /// Name.
    pub name: String,
    /// The sample.
    pub sample: Value,
    /// The expectation.
    pub expect: Value,
    /// The latest result.
    pub last_result: Option<Value>,
}

/// A REWARD_PREVIEW job's record.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ReportRow {
    /// Id.
    pub id: Uuid,
    /// Org.
    pub org_id: Uuid,
    /// Project.
    pub project_id: Uuid,
    /// The version scored.
    pub version_id: Uuid,
    /// Baseline (regression) or second version (compare).
    pub other_version_id: Option<Uuid>,
    /// `test`, `preview`, `regression` or `compare`.
    pub mode: String,
    /// The job.
    pub job_id: Option<Uuid>,
    /// PENDING, READY or INVALID.
    pub state: String,
    /// What was asked.
    pub request: Value,
    /// Results once READY.
    pub results: Option<Value>,
    /// Why INVALID.
    pub reason: Option<String>,
    /// The report artifact.
    pub artifact_id: Option<Uuid>,
    /// Who asked.
    pub created_by: String,
    /// When.
    pub created_at: OffsetDateTime,
    /// When it settled.
    pub finished_at: Option<OffsetDateTime>,
}

/// Columns of [`VersionRow`] (a macro, so queries stay static strings).
#[macro_export]
#[doc(hidden)]
macro_rules! reward_version_cols {
    () => {
        "id, org_id, project_id, reward_id, version_no, state, state_version, policy, \
         content_sha256, hash_inputs, sandbox_tier, code_artifact_id, code_bundle_sha256, \
         code_files, model_refs, notes, test_summary, regression, exec_sha256, test_job_id, \
         created_by, created_at, published_at, deprecated_at"
    };
}

/// Columns of [`ReportRow`].
#[macro_export]
#[doc(hidden)]
macro_rules! reward_report_cols {
    () => {
        "id, org_id, project_id, version_id, other_version_id, mode, job_id, state, request, \
         results, reason, artifact_id, created_by, created_at, finished_at"
    };
}

fn event(
    version: &VersionRow,
    seq: i64,
    event_type: &str,
    payload: Value,
) -> Result<NewEvent, RewardsError> {
    NewEvent::new(
        event_type,
        RewardVersion::AGGREGATE,
        version.id,
        seq,
        payload,
    )
    .map(|e| e.org(version.org_id))
    .map_err(|e| RewardsError::from(sqlx::Error::Protocol(format!("event payload: {e}"))))
}

/// Upserts the component list; components no longer shipped are unlisted (kept for old
/// versions' records).
pub async fn seed_components(db: &Db, catalog: &Catalog) -> Result<(), RewardsError> {
    let mut tx = db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('mrl.rewards.components'))")
        .execute(&mut *tx)
        .await?;
    for c in &catalog.components {
        sqlx::query(
            "INSERT INTO reward_components (ref, version, kind, tier, shape, deterministic, stateful,
                beta, value_min, value_max, needs, description, param_schema, code_sha256, listed)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,true)
             ON CONFLICT (ref) DO UPDATE SET version = EXCLUDED.version, kind = EXCLUDED.kind,
                tier = EXCLUDED.tier, shape = EXCLUDED.shape, deterministic = EXCLUDED.deterministic,
                stateful = EXCLUDED.stateful, beta = EXCLUDED.beta, value_min = EXCLUDED.value_min,
                value_max = EXCLUDED.value_max, needs = EXCLUDED.needs,
                description = EXCLUDED.description, param_schema = EXCLUDED.param_schema,
                code_sha256 = EXCLUDED.code_sha256, listed = true, updated_at = now()
             WHERE (reward_components.version, reward_components.code_sha256, reward_components.listed,
                    reward_components.param_schema, reward_components.description)
                   IS DISTINCT FROM (EXCLUDED.version, EXCLUDED.code_sha256, true,
                    EXCLUDED.param_schema, EXCLUDED.description)",
        )
        .bind(&c.reference)
        .bind(&c.version)
        .bind(&c.kind)
        .bind(i16::from(c.tier))
        .bind(&c.shape)
        .bind(c.deterministic)
        .bind(c.stateful)
        .bind(c.beta)
        .bind(c.value_range[0])
        .bind(c.value_range[1])
        .bind(&c.needs)
        .bind(&c.description)
        .bind(&c.param_schema)
        .bind(&c.code_sha256)
        .execute(&mut *tx)
        .await?;
    }
    let shipped: Vec<String> = catalog
        .components
        .iter()
        .map(|c| c.reference.clone())
        .collect();
    sqlx::query("UPDATE reward_components SET listed = false, updated_at = now() WHERE listed AND NOT (ref = ANY($1))")
        .bind(&shipped)
        .execute(&mut *tx)
        .await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(())
}

/// Creates a definition; a taken name is MRL-CONFLICT-290.
pub async fn create_definition(
    tx: &mut Tx,
    org: Uuid,
    project: Uuid,
    name: &str,
    description: &str,
    actor: &str,
) -> Result<DefinitionRow, RewardsError> {
    let row = sqlx::query_as::<_, DefinitionRow>(
        "INSERT INTO reward_definitions (id, org_id, project_id, name, description, created_by)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT ON CONSTRAINT reward_definitions_name DO NOTHING
         RETURNING id, org_id, project_id, name, description, next_version, created_by, created_at",
    )
    .bind(Uuid::now_v7())
    .bind(org)
    .bind(project)
    .bind(name)
    .bind(description)
    .bind(actor)
    .fetch_optional(&mut **tx)
    .await?;
    row.ok_or_else(|| {
        RewardsError::problem(
            codes::NAME_TAKEN,
            StatusCode::CONFLICT,
            format!("A reward named `{name}` already exists in this project."),
        )
    })
}

/// The definition, locked for a new version.
pub async fn lock_definition(tx: &mut Tx, id: Uuid) -> Result<Option<DefinitionRow>, RewardsError> {
    Ok(sqlx::query_as::<_, DefinitionRow>(
        "SELECT id, org_id, project_id, name, description, next_version, created_by, created_at
         FROM reward_definitions WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?)
}

/// The definition.
pub async fn definition(db: &Db, id: Uuid) -> Result<Option<DefinitionRow>, RewardsError> {
    Ok(sqlx::query_as::<_, DefinitionRow>(
        "SELECT id, org_id, project_id, name, description, next_version, created_by, created_at
         FROM reward_definitions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(db.pool())
    .await?)
}

/// What a new version holds.
#[derive(Debug, Clone)]
pub struct NewVersion {
    /// The checked policy.
    pub policy: Value,
    /// Hash inputs.
    pub inputs: HashInputs,
    /// `standard` or `code_exec`.
    pub sandbox_tier: &'static str,
    /// Code bundle artifact and its hash and files.
    pub code: Option<(Uuid, String, Value)>,
    /// Model versions read.
    pub model_refs: Vec<Uuid>,
    /// Notes.
    pub notes: String,
    /// Test cases.
    pub cases: Vec<TestCase>,
}

/// Creates a version of the locked definition `def`. The same content as an existing
/// version returns that version (`false`: nothing was created).
pub async fn create_version(
    tx: &mut Tx,
    def: &DefinitionRow,
    new: NewVersion,
    actor: &str,
) -> Result<(VersionRow, bool), RewardsError> {
    let content = new.inputs.content_sha256();
    let existing = sqlx::query_as::<_, VersionRow>(concat!(
        "SELECT ",
        crate::reward_version_cols!(),
        " FROM reward_versions WHERE reward_id = $1 AND content_sha256 = $2"
    ))
    .bind(def.id)
    .bind(&content)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(v) = existing {
        return Ok((v, false));
    }
    let (artifact, bundle, files) = match new.code {
        Some((a, b, f)) => (Some(a), Some(b), f),
        None => (None, None, json!([])),
    };
    let row = sqlx::query_as::<_, VersionRow>(concat!(
        "INSERT INTO reward_versions (id, org_id, project_id, reward_id, version_no, state, policy,
            content_sha256, hash_inputs, sandbox_tier, code_artifact_id, code_bundle_sha256,
            code_files, model_refs, notes, created_by)
         VALUES ($1,$2,$3,$4,$5,'DRAFT',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
         RETURNING ",
        crate::reward_version_cols!()
    ))
    .bind(Uuid::now_v7())
    .bind(def.org_id)
    .bind(def.project_id)
    .bind(def.id)
    .bind(def.next_version)
    .bind(&new.policy)
    .bind(&content)
    .bind(new.inputs.to_value())
    .bind(new.sandbox_tier)
    .bind(artifact)
    .bind(bundle)
    .bind(files)
    .bind(&new.model_refs)
    .bind(&new.notes)
    .bind(actor)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query("UPDATE reward_definitions SET next_version = next_version + 1 WHERE id = $1")
        .bind(def.id)
        .execute(&mut **tx)
        .await?;
    for (i, c) in new.cases.iter().enumerate() {
        sqlx::query(
            "INSERT INTO reward_test_cases (version_id, org_id, position, name, sample, expect)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(row.id)
        .bind(row.org_id)
        .bind(i as i32)
        .bind(c.name.trim())
        .bind(serde_json::to_value(&c.sample).unwrap_or(Value::Null))
        .bind(serde_json::to_value(&c.expect).unwrap_or(Value::Null))
        .execute(&mut **tx)
        .await?;
    }
    let e = event(
        &row,
        1,
        RewardVersion::CREATED_EVENT,
        json!({"reward_id": RewardId::from_uuid(def.id).to_string(), "version_no": row.version_no, "content_sha256": content,
               "to": "DRAFT"}),
    )?;
    OutboxWriter::write(tx, &e).await?;
    Ok((row, true))
}

/// The version.
pub async fn version(db: &Db, id: Uuid) -> Result<Option<VersionRow>, RewardsError> {
    Ok(sqlx::query_as::<_, VersionRow>(concat!(
        "SELECT ",
        crate::reward_version_cols!(),
        " FROM reward_versions WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(db.pool())
    .await?)
}

/// The version, locked.
pub async fn lock_version(tx: &mut Tx, id: Uuid) -> Result<Option<VersionRow>, RewardsError> {
    Ok(sqlx::query_as::<_, VersionRow>(concat!(
        "SELECT ",
        crate::reward_version_cols!(),
        " FROM reward_versions WHERE id = $1 FOR UPDATE"
    ))
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?)
}

/// Versions of a definition, newest first.
pub async fn versions_of(db: &Db, reward: Uuid) -> Result<Vec<VersionRow>, RewardsError> {
    Ok(sqlx::query_as::<_, VersionRow>(concat!(
        "SELECT ",
        crate::reward_version_cols!(),
        " FROM reward_versions WHERE reward_id = $1 ORDER BY version_no DESC"
    ))
    .bind(reward)
    .fetch_all(db.pool())
    .await?)
}

/// The test cases of a version, in order.
pub async fn cases(db: &Db, version: Uuid) -> Result<Vec<CaseRow>, RewardsError> {
    Ok(sqlx::query_as::<_, CaseRow>(
        "SELECT position, name, sample, expect, last_result FROM reward_test_cases
         WHERE version_id = $1 ORDER BY position",
    )
    .bind(version)
    .fetch_all(db.pool())
    .await?)
}

/// Freezes the latest preview sample set only on the definition's first publication.
pub(super) async fn freeze_golden_samples(tx: &mut Tx, v: &VersionRow) -> Result<(), RewardsError> {
    sqlx::query(
        "UPDATE reward_definitions SET golden_samples = COALESCE((
             SELECT golden_samples FROM reward_reports WHERE version_id = $2
                   AND mode = 'preview' AND state = 'READY' AND golden_samples IS NOT NULL
             ORDER BY job_id DESC LIMIT 1
         ), '[]'::jsonb) WHERE id = $1 AND golden_samples IS NULL",
    )
    .bind(v.reward_id)
    .bind(v.id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The prior version's cases followed by the definition's frozen preview examples.
pub(super) async fn regression_cases(
    db: &Db,
    baseline: Uuid,
) -> Result<Vec<CaseRow>, RewardsError> {
    let mut cases = cases(db, baseline).await?;
    let frozen: Option<Value> = sqlx::query_scalar(
        "SELECT d.golden_samples FROM reward_definitions d
         JOIN reward_versions v ON v.reward_id = d.id WHERE v.id = $1",
    )
    .bind(baseline)
    .fetch_one(db.pool())
    .await?;
    let first = cases.last().map_or(0, |c| c.position + 1);
    if let Some(Value::Array(samples)) = frozen {
        cases.extend(
            samples
                .into_iter()
                .enumerate()
                .map(|(index, sample)| CaseRow {
                    position: first + index as i32,
                    name: format!("Preview sample {}", index + 1),
                    sample,
                    expect: json!({}),
                    last_result: None,
                }),
        );
    }
    Ok(cases)
}

/// The newest PUBLISHED version of a definition other than `except`.
pub async fn latest_published(
    tx: &mut Tx,
    reward: Uuid,
    except: Uuid,
) -> Result<Option<VersionRow>, RewardsError> {
    Ok(sqlx::query_as::<_, VersionRow>(concat!(
        "SELECT ",
        crate::reward_version_cols!(),
        " FROM reward_versions
         WHERE reward_id = $1 AND id <> $2 AND state = 'PUBLISHED'
         ORDER BY version_no DESC LIMIT 1"
    ))
    .bind(reward)
    .bind(except)
    .fetch_optional(&mut **tx)
    .await?)
}

/// Why `v` can't be published now (empty: it can). `previous` is the newest other
/// PUBLISHED version of the same reward.
pub fn publish_blockers(v: &VersionRow, previous: Option<&VersionRow>) -> Vec<String> {
    let mut out = Vec::new();
    if v.state != "TESTING" {
        out.push(format!(
            "The version is {}; only a version whose tests passed (TESTING) can be published.",
            v.state
        ));
    }
    let passed = v
        .test_summary
        .as_ref()
        .and_then(|s| s.get("passed"))
        .and_then(Value::as_bool)
        == Some(true);
    if !passed {
        out.push("Its test cases haven't passed in the reward sandbox yet.".into());
    }
    if v.exec_sha256.is_none() && passed {
        out.push("The test run didn't report what the sandbox loaded.".into());
    }
    if let Some(prev) = previous {
        let against = v
            .regression
            .as_ref()
            .and_then(|r| r.get("against_version_id"))
            .and_then(Value::as_str)
            .and_then(|s| mrl_domain::ids::RewardVersionId::from_str(s).ok())
            .map(|id| id.as_uuid());
        if against != Some(prev.id) {
            out.push(format!(
                "Version {} is published, so a regression report against it is needed first.",
                prev.version_no
            ));
        }
    }
    out
}

/// Takes `ev` on the locked `v` (compare-and-set) and writes its event with `payload`.
pub async fn step(
    tx: &mut Tx,
    v: &VersionRow,
    ev: RewardVersionEvent,
    payload: Value,
) -> Result<VersionRow, RewardsError> {
    let from = v.state()?;
    let to = RewardVersion::can_transition(from, ev).map_err(|e| {
        RewardsError::problem(codes::ILLEGAL_STEP, StatusCode::CONFLICT, {
            let t = e.to_string();
            let mut c = t.chars();
            c.next().map_or(t.clone(), |f| {
                f.to_uppercase().chain(c).collect::<String>() + "."
            })
        })
    })?;
    let row = sqlx::query_as::<_, VersionRow>(concat!(
        "UPDATE reward_versions SET state = $2, state_version = state_version + 1,
            published_at = CASE WHEN $2 = 'PUBLISHED' THEN now() ELSE published_at END,
            deprecated_at = CASE WHEN $2 = 'DEPRECATED' THEN now() ELSE deprecated_at END,
            test_summary = CASE WHEN $2 = 'TESTING' THEN NULL ELSE test_summary END,
            exec_sha256 = CASE WHEN $2 = 'TESTING' THEN NULL ELSE exec_sha256 END
         WHERE id = $1 AND state_version = $3 RETURNING ",
        crate::reward_version_cols!()
    ))
    .bind(v.id)
    .bind(to.as_str())
    .bind(v.state_version)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| {
        RewardsError::Jobs(crate::modules::jobs::JobsError::changed("reward version"))
    })?;
    let mut body = payload;
    body["from"] = json!(from.as_str());
    body["to"] = json!(to.as_str());
    let e = event(&row, row.state_version, ev.event_type(), body)?;
    OutboxWriter::write(tx, &e).await?;
    Ok(row)
}

/// Records an event that changes no state (like `reward_version.tests_passed`), bumping the
/// sequence (compare-and-set).
pub async fn note(
    tx: &mut Tx,
    v: &VersionRow,
    event_type: &str,
    payload: Value,
) -> Result<VersionRow, RewardsError> {
    let row = sqlx::query_as::<_, VersionRow>(concat!(
        "UPDATE reward_versions SET state_version = state_version + 1
         WHERE id = $1 AND state_version = $2 RETURNING ",
        crate::reward_version_cols!()
    ))
    .bind(v.id)
    .bind(v.state_version)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| {
        RewardsError::Jobs(crate::modules::jobs::JobsError::changed("reward version"))
    })?;
    let e = event(&row, row.state_version, event_type, payload)?;
    OutboxWriter::write(tx, &e).await?;
    Ok(row)
}

/// The report row of `job`, locked.
pub async fn lock_report_for_job(
    tx: &mut Tx,
    job: Uuid,
) -> Result<Option<ReportRow>, RewardsError> {
    Ok(sqlx::query_as::<_, ReportRow>(concat!(
        "SELECT ",
        crate::reward_report_cols!(),
        " FROM reward_reports WHERE job_id = $1 FOR UPDATE"
    ))
    .bind(job)
    .fetch_optional(&mut **tx)
    .await?)
}

/// The report.
pub async fn report(db: &Db, id: Uuid) -> Result<Option<ReportRow>, RewardsError> {
    Ok(sqlx::query_as::<_, ReportRow>(concat!(
        "SELECT ",
        crate::reward_report_cols!(),
        " FROM reward_reports WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(db.pool())
    .await?)
}

/// Settles a PENDING report: READY with `results`, or INVALID with `reason`.
pub async fn settle_report(
    tx: &mut Tx,
    report: Uuid,
    outcome: Result<Value, String>,
    artifact: Option<Uuid>,
) -> Result<bool, RewardsError> {
    let (state, results, reason) = match outcome {
        Ok(r) => ("READY", Some(r), None),
        Err(why) => ("INVALID", None, Some(why)),
    };
    let n = sqlx::query(
        "UPDATE reward_reports SET state = $2, results = $3, reason = $4, artifact_id = $5,
            finished_at = now(), prepared_result = NULL, prepared_artifact_id = NULL
         WHERE id = $1 AND state = 'PENDING'",
    )
    .bind(report)
    .bind(state)
    .bind(results)
    .bind(reason)
    .bind(artifact)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(n == 1)
}

/// Writes the outcome of each case of a test run.
pub async fn record_case_results(
    tx: &mut Tx,
    version: Uuid,
    results: &[(i32, Value)],
) -> Result<(), RewardsError> {
    for (position, result) in results {
        sqlx::query(
            "UPDATE reward_test_cases SET last_result = $3 WHERE version_id = $1 AND position = $2",
        )
        .bind(version)
        .bind(position)
        .bind(result)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// The listed components (for `GET /reward-components`).
pub async fn listed_components(db: &Db) -> Result<Vec<String>, RewardsError> {
    let rows = sqlx::query("SELECT ref FROM reward_components WHERE listed ORDER BY ref")
        .fetch_all(db.pool())
        .await?;
    rows.into_iter()
        .map(|r| r.try_get::<String, _>("ref").map_err(RewardsError::from))
        .collect()
}
