//! [`CheckpointService`]: staging, the commit, corruption, full verification, resume points,
//! emergency checkpoints and the fast background tick.
//!
//! Lock order, everywhere: job (and attempt), then run, then checkpoint. Storage is read before
//! a transaction opens; the transaction re-checks every guard under its locks.

use super::CheckpointsConfig;
use crate::db::{Db, DbError, Tx};
use crate::modules::artifacts::{ArtifactStore, LOCAL_ORG};
use crate::modules::events::{NewEvent, OutboxWriter};
use crate::modules::jobs::{scope, Chains, JobRow, JobTransitions, JobsError};
use crate::modules::nodes::commands::{CommandKind, CommandSender, NewCommand};
use crate::modules::nodes::Gateway;
use crate::seams::{Principal, RequestContext, RequestMeta, SeamError, Seams, SecretStore};
use mrl_domain::ids::{AttemptId, CheckpointId, JobId, NodeId, RunId};
use mrl_domain::state::{Checkpoint, CheckpointEvent, CheckpointState, StateMachine};
use mrl_proto::agent::v1::{command::Kind, CheckpointAttempt, UploadArtifacts, UploadItem};
use mrl_proto::common::v1::AttemptRef;
use mrl_proto::worker::v1::{CheckpointCommitRequest, CheckpointKind, CheckpointStaged};
use mrl_storage::{StorageError, Store, TicketMethod};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::OnceCell;
use uuid::Uuid;

/// Most files one checkpoint may list (keeps `UploadArtifacts` under the 4 MiB command limit).
pub const MAX_FILES: usize = 4096;
/// Largest manifest the commit reads.
const MAX_MANIFEST_BYTES: u64 = 16 << 20;
/// File roles dropped by optimizer stripping.
pub const STATE_ROLES: [&str; 4] = ["optimizer", "scheduler", "rng", "trainer_state"];

static KINDS: &[CommandKind] = &[CommandKind::UploadArtifacts, CommandKind::CheckpointAttempt];

macro_rules! ckpt_cols {
    () => {
        "id, org_id, storage_org, run_id, job_id, attempt_id, attempt_epoch, global_step, \
         policy_version, kind, layout, is_final, state, state_version, verification_level, \
         manifest_key, manifest_sha256, total_bytes, file_count, metrics_at_save, corrupt_reason, \
         commit_requested_at, upload_command_id, tickets_expire_at, node_id, created_at, \
         committed_at, verified_at"
    };
}
pub(crate) use ckpt_cols;

/// One `checkpoints` row.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct CheckpointRow {
    /// Checkpoint id.
    pub id: Uuid,
    /// The org when it was staged.
    pub org_id: Uuid,
    /// Storage org text (`orgs/{storage_org}/…`).
    pub storage_org: String,
    /// The run.
    pub run_id: Uuid,
    /// The run's job.
    pub job_id: Uuid,
    /// The attempt that wrote it.
    pub attempt_id: Uuid,
    /// That attempt's fencing epoch.
    pub attempt_epoch: i64,
    /// Training step.
    pub global_step: i64,
    /// Online RL policy version.
    pub policy_version: Option<i64>,
    /// `full_resume`, `weights_only` or `adapter_only`.
    pub kind: String,
    /// Format tag, like `hf-trainer-v1`.
    pub layout: String,
    /// The run's final checkpoint.
    pub is_final: bool,
    /// State (wire name).
    pub state: String,
    /// Bumped by every state change; the event sequence.
    pub state_version: i64,
    /// `manifest_head` or `full_rehash`, once verified.
    pub verification_level: Option<String>,
    /// Object key of the checkpoint manifest.
    pub manifest_key: Option<String>,
    /// sha256 of the manifest.
    pub manifest_sha256: Option<Vec<u8>>,
    /// Sum of file sizes.
    pub total_bytes: Option<i64>,
    /// Number of files.
    pub file_count: Option<i32>,
    /// Metrics the worker reported at this step.
    pub metrics_at_save: Option<Value>,
    /// Why it is CORRUPT.
    pub corrupt_reason: Option<String>,
    /// When the worker asked to commit.
    pub commit_requested_at: Option<OffsetDateTime>,
    /// The `UploadArtifacts` command with the current tickets.
    pub upload_command_id: Option<Uuid>,
    /// When those tickets end.
    pub tickets_expire_at: Option<OffsetDateTime>,
    /// The node that staged it.
    pub node_id: Option<Uuid>,
    /// Staged.
    pub created_at: OffsetDateTime,
    /// Committed.
    pub committed_at: Option<OffsetDateTime>,
    /// Last verified.
    pub verified_at: Option<OffsetDateTime>,
}

impl CheckpointRow {
    /// The state.
    pub fn state(&self) -> CheckpointState {
        CheckpointState::from_str(&self.state).unwrap_or(CheckpointState::Corrupt)
    }

    /// True while it may still commit.
    pub fn is_open(&self) -> bool {
        matches!(
            self.state(),
            CheckpointState::Writing | CheckpointState::Uploaded | CheckpointState::Verifying
        )
    }
}

/// One `checkpoint_files` row.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct FileRow {
    /// Path relative to the checkpoint.
    pub path: String,
    /// What it is for (`weights`, `optimizer`, `manifest`, …).
    pub role: String,
    /// Where it is stored.
    pub object_key: String,
    /// sha256.
    pub sha256: Vec<u8>,
    /// Size in bytes.
    pub size_bytes: i64,
    /// The object's entity tag at commit.
    pub storage_checksum: Option<String>,
}

/// How an emergency checkpoint request ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmergencyOutcome {
    /// A checkpoint staged after the request committed.
    Committed(Uuid),
    /// The agent refused the request or it was never sent.
    Refused(String),
    /// Nothing committed before the deadline.
    TimedOut,
}

/// Where the ticket signing key comes from: the server's keychain (production) or a store
/// handed in (tests). It is only opened when the first ticket is needed.
pub(crate) enum Secrets {
    Given(Arc<dyn SecretStore>),
    Keychain {
        service: String,
        dir: Result<std::path::PathBuf, String>,
        backend: crate::modules::keychain::KeychainBackend,
    },
}

impl Secrets {
    pub(crate) fn given(s: Arc<dyn SecretStore>) -> Self {
        Self::Given(s)
    }

    /// The same keychain `main` opens for the server's seams.
    // ponytail: a second handle on the server's keychain, because modules can't reach the
    // seams; A1 may add a module hook that hands modules `AppState`.
    pub(crate) fn keychain(config: &crate::Config) -> Self {
        Self::Keychain {
            service: config.auth.keychain_service.clone(),
            dir: crate::pgbundle::data_dir(&config.pgbundle)
                .map(|d| d.join("secrets"))
                .map_err(|e| e.to_string()),
            backend: config.keychain.backend,
        }
    }
}

/// See the module docs.
pub struct CheckpointService {
    db: Db,
    store: Arc<ArtifactStore>,
    secrets: Secrets,
    seams: OnceCell<Seams>,
    commands: CommandSender,
    config: CheckpointsConfig,
}

fn seam(what: &'static str, e: impl std::fmt::Display) -> JobsError {
    JobsError::Seam(SeamError::new(what, e.to_string()))
}

fn storage(e: StorageError) -> JobsError {
    seam("storage", e)
}

/// Lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 64 lowercase hex characters as 32 bytes.
pub(crate) fn sha_bytes(s: &str) -> Option<Vec<u8>> {
    if s.len() != 64 || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    (0..32)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

fn kind_name(k: i32) -> Option<&'static str> {
    match CheckpointKind::try_from(k).ok()? {
        CheckpointKind::FullResume => Some("full_resume"),
        CheckpointKind::WeightsOnly => Some("weights_only"),
        CheckpointKind::AdapterOnly => Some("adapter_only"),
        CheckpointKind::Unspecified => None,
    }
}

/// The storage org text for a job's org (P19's spelling: `local` for the solo tenant).
pub fn storage_org(org: Uuid) -> String {
    scope::org_text(org).unwrap_or_else(|| LOCAL_ORG.to_owned())
}

fn is_cancelled_or_over(job_state: &str) -> bool {
    matches!(
        job_state,
        "CANCELLING" | "CANCELLED" | "COMPLETED" | "FAILED" | "REJECTED"
    )
}

/// The checks a staged file list must pass; `Err` is why it can't be used.
fn check_files(s: &CheckpointStaged) -> Result<(), String> {
    if s.files.is_empty() || s.files.len() > MAX_FILES {
        return Err(format!(
            "it lists {} files; 1 to {MAX_FILES} are allowed",
            s.files.len()
        ));
    }
    let manifests: Vec<_> = s.files.iter().filter(|f| f.role == "manifest").collect();
    if manifests.len() != 1 || manifests[0].sha256 != s.manifest_sha256 {
        return Err("it needs exactly one manifest file with the manifest digest".into());
    }
    let mut seen = std::collections::HashSet::new();
    for f in &s.files {
        if sha_bytes(&f.sha256).is_none() {
            return Err(format!("{} has no valid sha256", f.path));
        }
        if i64::try_from(f.size).is_err() {
            return Err(format!("{} is too large", f.path));
        }
        if f.role.is_empty() || f.role.contains('\0') || f.path.contains('\0') {
            return Err(format!("{} has an empty role or a NUL character", f.path));
        }
        if !seen.insert(f.path.to_lowercase()) {
            return Err(format!("{} is listed twice", f.path));
        }
    }
    Ok(())
}

impl CheckpointService {
    pub(crate) fn new(
        db: Db,
        store: Arc<ArtifactStore>,
        secrets: Secrets,
        gateway: &Arc<Gateway>,
        config: CheckpointsConfig,
    ) -> Self {
        Self {
            db,
            store,
            secrets,
            seams: OnceCell::new(),
            commands: CommandSender::new(gateway, KINDS),
            config,
        }
    }

    /// The database.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// The settings.
    pub fn config(&self) -> &CheckpointsConfig {
        &self.config
    }

    pub(crate) async fn org_store(&self, org: &str) -> Result<Store, JobsError> {
        self.store
            .org_store(org)
            .await
            .map_err(|e| seam("storage", e))
    }

    /// Seams that carry only the ticket key's secret store (tickets read nothing else).
    async fn ticket_seams(&self) -> Result<&Seams, JobsError> {
        self.seams
            .get_or_try_init(|| async {
                let secrets: Arc<dyn SecretStore> = match &self.secrets {
                    Secrets::Given(s) => s.clone(),
                    Secrets::Keychain {
                        service,
                        dir,
                        backend,
                    } => {
                        let dir = dir.as_ref().map_err(|e| seam("secrets", e))?;
                        Arc::new(
                            crate::modules::keychain::KeychainStore::open(service, dir, *backend)
                                .await,
                        )
                    }
                };
                Seams::production(crate::config::Mode::Solo, "")
                    .secrets_arc(secrets)
                    .build(crate::config::Mode::Solo)
                    .map_err(|e| seam("secrets", e))
            })
            .await
    }

    /// Reads one checkpoint.
    pub async fn find(&self, id: Uuid) -> Result<Option<CheckpointRow>, JobsError> {
        Ok(sqlx::query_as::<_, CheckpointRow>(concat!(
            "SELECT ",
            ckpt_cols!(),
            " FROM checkpoints WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(self.db.pool())
        .await?)
    }

    /// A checkpoint's files.
    pub async fn files(&self, id: Uuid) -> Result<Vec<FileRow>, JobsError> {
        Ok(sqlx::query_as::<_, FileRow>(
            "SELECT path, role, object_key, sha256, size_bytes, storage_checksum \
             FROM checkpoint_files WHERE checkpoint_id = $1 ORDER BY path",
        )
        .bind(id)
        .fetch_all(self.db.pool())
        .await?)
    }

    // ---- staging ----

    /// A relayed `CheckpointStaged` from `node`. Creates the row (WRITING, or CORRUPT when the
    /// attempt is fenced off or the file list is unusable) and sends upload tickets. A repeat
    /// changes nothing, but sends tickets again if none were sent yet.
    pub async fn on_staged(
        &self,
        node: Uuid,
        at: &AttemptRef,
        s: &CheckpointStaged,
    ) -> Result<(), JobsError> {
        let (Ok(ckpt), Ok(attempt)) = (
            s.checkpoint_id.parse::<CheckpointId>(),
            at.attempt_id.parse::<AttemptId>(),
        ) else {
            tracing::warn!(checkpoint = %s.checkpoint_id, attempt = %at.attempt_id, "staged checkpoint with unreadable ids; ignored");
            return Ok(());
        };
        let (ckpt, attempt) = (ckpt.as_uuid(), attempt.as_uuid());
        let Some((org, job, run)) = self.attempt_owner(attempt).await? else {
            tracing::warn!(attempt = %at.attempt_id, "staged checkpoint for an unknown attempt; ignored");
            return Ok(());
        };
        let store_org = storage_org(org);
        let store = self.org_store(&store_org).await?;
        let prefix = store
            .keys()
            .checkpoint_staging(
                &RunId::from_uuid(run).to_string(),
                &CheckpointId::from_uuid(ckpt).to_string(),
                &AttemptId::from_uuid(attempt).to_string(),
            )
            .map_err(storage)?;
        let mut problem = check_files(s).err();
        let mut keys = Vec::with_capacity(s.files.len());
        for f in &s.files {
            match store.keys().join(&prefix, &f.path) {
                Ok(k) => keys.push(k),
                Err(_) => {
                    problem
                        .get_or_insert_with(|| format!("{} is not a safe relative path", f.path));
                    keys.push(String::new());
                }
            }
        }
        let kind = kind_name(s.kind);
        if kind.is_none() {
            problem.get_or_insert_with(|| "its kind is not set".to_owned());
        }
        let mut tx = self.db.begin().await?;
        let Some(job_row) = JobTransitions::lock(&mut tx, org, job).await? else {
            return Ok(());
        };
        let Some(att) = JobTransitions::lock_attempt(&mut tx, org, attempt).await? else {
            return Ok(());
        };
        let epoch = i64::try_from(at.fencing_epoch).unwrap_or(i64::MAX);
        if !launched_on(&mut tx, node, attempt, epoch).await? {
            tracing::warn!(node = %node, attempt = %at.attempt_id, epoch, "a node staged a checkpoint for an attempt it was not given; ignored");
            return Ok(());
        }
        if let Some(existing) = lock_row(&mut tx, ckpt).await? {
            tx.commit().await.map_err(DbError::from)?;
            if existing.attempt_id == attempt
                && existing.state() == CheckpointState::Writing
                && existing.upload_command_id.is_none()
            {
                self.send_tickets_or_later(&existing).await;
            }
            return Ok(());
        }
        if epoch != att.epoch || att.epoch != job_row.epoch {
            problem = Some(format!(
                "it comes from a replaced attempt (epoch {epoch}, current {})",
                job_row.epoch
            ));
        } else if is_cancelled_or_over(&job_row.state) {
            problem = Some(format!("the job is {}", job_row.state));
        }
        let total: i64 = s
            .files
            .iter()
            .map(|f| f.size as i64)
            .fold(0, i64::saturating_add);
        let manifest = s.files.iter().position(|f| f.role == "manifest");
        let metrics: serde_json::Map<String, Value> = s
            .metrics_at_step
            .iter()
            .filter(|(_, v)| v.is_finite())
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        let row = sqlx::query_as::<_, CheckpointRow>(concat!(
            "INSERT INTO checkpoints (id, org_id, storage_org, run_id, job_id, attempt_id, \
               attempt_epoch, global_step, policy_version, kind, layout, is_final, state, \
               manifest_key, manifest_sha256, total_bytes, file_count, metrics_at_save, node_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'WRITING',$13,$14,$15,$16,$17,$18) \
             RETURNING ",
            ckpt_cols!()
        ))
        .bind(ckpt)
        .bind(org)
        .bind(&store_org)
        .bind(run)
        .bind(job)
        .bind(attempt)
        .bind(epoch)
        .bind(i64::try_from(s.step).unwrap_or(i64::MAX))
        .bind((s.policy_version > 0).then(|| i64::try_from(s.policy_version).unwrap_or(i64::MAX)))
        .bind(kind.unwrap_or("full_resume"))
        .bind(s.format.replace('\0', ""))
        .bind(s.is_final)
        .bind(
            manifest
                .and_then(|i| keys.get(i).cloned())
                .filter(|k| !k.is_empty()),
        )
        .bind(sha_bytes(&s.manifest_sha256))
        .bind(total)
        .bind(i32::try_from(s.files.len()).unwrap_or(i32::MAX))
        .bind(Value::Object(metrics))
        .bind(node)
        .fetch_one(&mut *tx)
        .await?;
        if problem.is_none() {
            for (f, key) in s.files.iter().zip(&keys) {
                sqlx::query(
                    "INSERT INTO checkpoint_files (checkpoint_id, org_id, path, role, object_key, \
                       sha256, size_bytes) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                )
                .bind(ckpt)
                .bind(org)
                .bind(&f.path)
                .bind(&f.role)
                .bind(key)
                .bind(sha_bytes(&f.sha256).unwrap_or_default())
                .bind(f.size as i64)
                .execute(&mut *tx)
                .await?;
            }
        }
        event(&mut tx, &row, Checkpoint::CREATED_EVENT, 0).await?;
        let row = match &problem {
            Some(why) => {
                mark_corrupt(&mut tx, &row, &format!("checkpoint can't be used: {why}")).await?
            }
            None => row,
        };
        tx.commit().await.map_err(DbError::from)?;
        if problem.is_none() {
            self.send_tickets_or_later(&row).await;
        }
        Ok(())
    }

    /// Sends tickets now; on failure the tick sends them (the row is already saved, and the
    /// node's later CONTROL messages must not wait behind a storage problem).
    async fn send_tickets_or_later(&self, row: &CheckpointRow) {
        if let Err(e) = self.send_tickets(row).await {
            tracing::warn!(code = e.code(), error = %e, checkpoint = %CheckpointId::from_uuid(row.id), "upload tickets not sent yet; trying again later");
        }
    }

    /// The org, job and run of an attempt, if it is a run's attempt.
    async fn attempt_owner(&self, attempt: Uuid) -> Result<Option<(Uuid, Uuid, Uuid)>, JobsError> {
        Ok(sqlx::query_as::<_, (Uuid, Uuid, Uuid)>(
            "SELECT a.org_id, j.id, j.run_id FROM job_attempts a \
             JOIN jobs j ON j.id = a.job_id AND j.org_id = a.org_id \
             WHERE a.id = $1 AND j.run_id IS NOT NULL",
        )
        .bind(attempt)
        .fetch_optional(self.db.pool())
        .await?)
    }

    /// Issues a PUT ticket for every file and sends them in one `UploadArtifacts` (replacing
    /// any unsent earlier one).
    pub(crate) async fn send_tickets(&self, row: &CheckpointRow) -> Result<(), JobsError> {
        let Some(node) = row.node_id else {
            return Ok(());
        };
        let files = self.files(row.id).await?;
        let seams = self.ticket_seams().await?;
        let mut principal = Principal::local_owner();
        principal.id = "checkpoints".to_owned();
        principal.org = Some(row.storage_org.clone());
        let ctx = RequestContext::new(
            principal,
            &RequestMeta {
                trace_id: String::new(),
                request_id: String::new(),
            },
        );
        let mut items = Vec::with_capacity(files.len());
        let mut expires: Option<OffsetDateTime> = None;
        for f in &files {
            let size = u64::try_from(f.size_bytes).unwrap_or(0);
            let t = self
                .store
                .ticket(seams, &ctx, &f.object_key, TicketMethod::Put, size)
                .await
                .map_err(|e| seam("tickets", e))?;
            expires = Some(expires.map_or(t.expires_at, |e| e.min(t.expires_at)));
            items.push(UploadItem {
                local_path: f.path.clone(),
                url: t.url,
                sha256: hex(&f.sha256),
                size,
                expires_at: Some(prost_types::Timestamp {
                    seconds: t.expires_at.unix_timestamp(),
                    nanos: 0,
                }),
            });
        }
        let attempt = AttemptId::from_uuid(row.attempt_id);
        let epoch = u64::try_from(row.attempt_epoch).unwrap_or(0);
        let upload = UploadArtifacts {
            attempt: Some(AttemptRef {
                job_id: JobId::from_uuid(row.job_id).to_string(),
                attempt_id: attempt.to_string(),
                fencing_epoch: epoch,
            }),
            checkpoint_id: CheckpointId::from_uuid(row.id).to_string(),
            items,
        };
        let sent = self
            .commands
            .send(
                NodeId::from_uuid(node),
                NewCommand::new(Kind::UploadArtifacts(upload))
                    .attempt(attempt, epoch)
                    .deadline(crate::modules::artifacts::TICKET_TTL)
                    .superseding(),
            )
            .await
            .map_err(|e| seam("commands", e))?;
        sqlx::query(
            "UPDATE checkpoints SET upload_command_id = $2, tickets_expire_at = $3 \
             WHERE id = $1 AND state IN ('WRITING','UPLOADED','VERIFYING')",
        )
        .bind(row.id)
        .bind(sent.id)
        .bind(expires)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }

    // ---- commit ----

    /// A relayed `CheckpointCommitRequest` from `node`: notes the request and tries to commit.
    pub async fn on_commit_request(
        &self,
        node: Uuid,
        at: &AttemptRef,
        c: &CheckpointCommitRequest,
    ) -> Result<(), JobsError> {
        let Ok(ckpt) = c.checkpoint_id.parse::<CheckpointId>() else {
            return Ok(());
        };
        let Some(row) = self.find(ckpt.as_uuid()).await? else {
            tracing::warn!(checkpoint = %c.checkpoint_id, "commit for an unknown checkpoint; ignored");
            return Ok(());
        };
        let same_attempt = at
            .attempt_id
            .parse::<AttemptId>()
            .is_ok_and(|a| a.as_uuid() == row.attempt_id);
        if !same_attempt || row.node_id != Some(node) {
            tracing::warn!(checkpoint = %c.checkpoint_id, node = %node, "commit from another attempt or node; ignored");
            return Ok(());
        }
        if row.manifest_sha256.as_deref() != sha_bytes(&c.manifest_sha256).as_deref() {
            self.corrupt(row.id, "the manifest changed after staging")
                .await?;
            return Ok(());
        }
        sqlx::query(
            "UPDATE checkpoints SET commit_requested_at = now() \
             WHERE id = $1 AND commit_requested_at IS NULL",
        )
        .bind(row.id)
        .execute(self.db.pool())
        .await?;
        // Saved: a storage problem now is retried by the tick, not by redelivery.
        if let Err(e) = self.try_commit(row.id).await {
            tracing::warn!(code = e.code(), error = %e, checkpoint = %c.checkpoint_id, "commit not decided yet; trying again later");
        }
        Ok(())
    }

    /// Commits the checkpoint if its commit was requested and every file is stored and
    /// matches (`manifest_head`). Returns the state afterwards. A file not stored yet leaves
    /// it open until the staging TTL; anything else wrong makes it CORRUPT.
    pub async fn try_commit(&self, id: Uuid) -> Result<Option<CheckpointState>, JobsError> {
        let Some(row) = self.find(id).await? else {
            return Ok(None);
        };
        if !row.is_open() || row.commit_requested_at.is_none() {
            return Ok(Some(row.state()));
        }
        let files = self.files(id).await?;
        let checked = match self.head_check(&row, &files).await? {
            Head::Waiting(missing) => {
                let age = OffsetDateTime::now_utc() - row.created_at;
                if age.whole_seconds() >= self.config.staging_ttl_seconds as i64 {
                    let why = format!("files never arrived in storage: {}", missing.join(", "));
                    return self.corrupt(id, &why).await.map(Some);
                }
                return Ok(Some(row.state()));
            }
            Head::Bad(why) => return self.corrupt(id, &why).await.map(Some),
            Head::Ok(etags) => etags,
        };
        let mut tx = self.db.begin().await?;
        let Some(job) = lock_job(&mut tx, row.job_id).await? else {
            return Ok(None);
        };
        lock_run(&mut tx, row.run_id).await?;
        let Some(locked) = lock_row(&mut tx, id).await? else {
            return Ok(None);
        };
        if locked.state_version != row.state_version || !locked.is_open() {
            return Ok(Some(locked.state()));
        }
        if let Some(why) = fence(&job, &locked) {
            let done = mark_corrupt(&mut tx, &locked, &format!("rejected: {why}")).await?;
            tx.commit().await.map_err(DbError::from)?;
            return Ok(Some(done.state()));
        }
        let mut row = locked;
        for ev in [
            CheckpointEvent::Upload,
            CheckpointEvent::RequestCommit,
            CheckpointEvent::Commit,
        ] {
            if Checkpoint::can_transition(row.state(), ev).is_ok() {
                row = step(&mut tx, &row, ev).await?;
            }
        }
        for (path, etag) in &checked {
            sqlx::query(
                "UPDATE checkpoint_files SET storage_checksum = $3 WHERE checkpoint_id = $1 AND path = $2",
            )
            .bind(id)
            .bind(path)
            .bind(etag)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE checkpoints SET verification_level = 'manifest_head', committed_at = now(), \
               verified_at = now() WHERE id = $1",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        if row.is_final {
            // ponytail: one final per run; a newer final (a later attempt) replaces the older.
            sqlx::query(
                "UPDATE checkpoints SET is_final = false \
                 WHERE run_id = $1 AND is_final AND state = 'COMMITTED' AND id <> $2",
            )
            .bind(row.run_id)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE runs SET final_checkpoint_id = $2 WHERE id = $1")
                .bind(row.run_id)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(
            "UPDATE runs r SET latest_committed_checkpoint_id = $2 WHERE r.id = $1 AND \
               (r.latest_committed_checkpoint_id IS NULL OR \
                (SELECT c.global_step FROM checkpoints c WHERE c.id = r.latest_committed_checkpoint_id) < $3)",
        )
        .bind(row.run_id)
        .bind(id)
        .bind(row.global_step)
        .execute(&mut *tx)
        .await?;
        tx.commit().await.map_err(DbError::from)?;
        Ok(Some(CheckpointState::Committed))
    }

    /// `manifest_head`: every file is stored with its size, the manifest object hashes to the
    /// staged digest, and the manifest lists exactly the staged files.
    async fn head_check(&self, row: &CheckpointRow, files: &[FileRow]) -> Result<Head, JobsError> {
        let store = self.org_store(&row.storage_org).await?;
        let mut missing = Vec::new();
        let mut etags = Vec::new();
        for f in files {
            match store.head(&f.object_key).await {
                Ok(info) if info.size as i64 == f.size_bytes => {
                    etags.push((f.path.clone(), info.etag));
                }
                Ok(info) => {
                    return Ok(Head::Bad(format!(
                        "{} is {} bytes in storage, {} expected",
                        f.path, info.size, f.size_bytes
                    )))
                }
                Err(StorageError::NotFound { .. }) => missing.push(f.path.clone()),
                Err(e) => return Err(storage(e)),
            }
        }
        if !missing.is_empty() {
            return Ok(Head::Waiting(missing));
        }
        let Some(manifest) = files.iter().find(|f| f.role == "manifest") else {
            return Ok(Head::Bad("it has no manifest file".into()));
        };
        if manifest.size_bytes as u64 > MAX_MANIFEST_BYTES {
            return Ok(Head::Bad("its manifest is too large".into()));
        }
        let bytes = store.get(&manifest.object_key).await.map_err(storage)?;
        if Some(mrl_storage::sha256_hex(&bytes)) != row.manifest_sha256.as_deref().map(hex) {
            return Ok(Head::Bad(
                "the stored manifest does not match its digest".into(),
            ));
        }
        Ok(match manifest_problem(row, files, &bytes) {
            Some(why) => Head::Bad(why),
            None => Head::Ok(etags),
        })
    }

    // ---- corruption ----

    /// Marks the checkpoint CORRUPT (if it isn't already, or tombstoned) and points the run
    /// back at its previous COMMITTED checkpoint. Returns the state afterwards.
    pub async fn corrupt(&self, id: Uuid, why: &str) -> Result<CheckpointState, JobsError> {
        let Some(row) = self.find(id).await? else {
            return Ok(CheckpointState::Corrupt);
        };
        let mut tx = self.db.begin().await?;
        lock_run(&mut tx, row.run_id).await?;
        let Some(row) = lock_row(&mut tx, id).await? else {
            return Ok(CheckpointState::Corrupt);
        };
        if Checkpoint::can_transition(row.state(), CheckpointEvent::MarkCorrupt).is_err() {
            return Ok(row.state());
        }
        mark_corrupt(&mut tx, &row, why).await?;
        tx.commit().await.map_err(DbError::from)?;
        tracing::warn!(checkpoint = %CheckpointId::from_uuid(id), reason = why, "checkpoint is corrupt");
        Ok(CheckpointState::Corrupt)
    }

    // ---- full verification ----

    /// `full_rehash`: re-reads every stored byte. A mismatch makes the checkpoint CORRUPT
    /// (MRL-STORE-011); a match records the level. Only COMMITTED checkpoints (MRL-STATE-230).
    pub async fn rehash(&self, id: Uuid) -> Result<CheckpointRow, JobsError> {
        let row = self.committed(id).await?;
        let store = self.org_store(&row.storage_org).await?;
        for f in self.files(id).await? {
            let actual = match store.sha256(&f.object_key).await {
                Ok(s) => s,
                Err(StorageError::NotFound { .. }) => {
                    self.corrupt(id, &format!("{} is missing from storage", f.path))
                        .await?;
                    return Err(mismatch(&f.path));
                }
                Err(e) => return Err(storage(e)),
            };
            if actual != hex(&f.sha256) {
                self.corrupt(
                    id,
                    &format!("{} does not hash to its recorded sha256", f.path),
                )
                .await?;
                return Err(mismatch(&f.path));
            }
        }
        sqlx::query(
            "UPDATE checkpoints SET verification_level = 'full_rehash', verified_at = now() \
             WHERE id = $1 AND state = 'COMMITTED'",
        )
        .bind(id)
        .execute(self.db.pool())
        .await?;
        self.committed(id).await
    }

    /// Call before promoting or exporting a checkpoint: Ok once it is COMMITTED and verified
    /// at `full_rehash` level with its objects unchanged since (re-hashes when needed).
    pub async fn require_full_verification(&self, id: Uuid) -> Result<CheckpointRow, JobsError> {
        let row = self.committed(id).await?;
        if row.verification_level.as_deref() == Some("full_rehash") && self.unchanged(&row).await? {
            return Ok(row);
        }
        self.rehash(id).await
    }

    async fn committed(&self, id: Uuid) -> Result<CheckpointRow, JobsError> {
        match self.find(id).await? {
            Some(r) if r.state() == CheckpointState::Committed => Ok(r),
            Some(r) => Err(JobsError::domain(
                super::codes::NOT_COMMITTED,
                axum::http::StatusCode::CONFLICT,
                format!("This checkpoint is {}, not COMMITTED.", r.state),
            )),
            None => Err(JobsError::not_found("checkpoint")),
        }
    }

    /// True if every object still has the size and entity tag it had at commit.
    async fn unchanged(&self, row: &CheckpointRow) -> Result<bool, JobsError> {
        let store = self.org_store(&row.storage_org).await?;
        for f in self.files(row.id).await? {
            match store.head(&f.object_key).await {
                Ok(i)
                    if i.size as i64 == f.size_bytes
                        && (f.storage_checksum.is_none() || i.etag == f.storage_checksum) => {}
                Ok(_) | Err(StorageError::NotFound { .. }) => return Ok(false),
                Err(e) => return Err(storage(e)),
            }
        }
        Ok(true)
    }

    // ---- resume ----

    /// The checkpoint a new attempt of `run` resumes from: the latest COMMITTED one whose
    /// objects are unchanged since commit. A changed or missing one becomes CORRUPT and the
    /// previous COMMITTED one is tried.
    pub async fn resume_point(&self, run: Uuid) -> Result<Option<Uuid>, JobsError> {
        loop {
            let latest: Option<Uuid> =
                sqlx::query_scalar("SELECT latest_committed_checkpoint_id FROM runs WHERE id = $1")
                    .bind(run)
                    .fetch_optional(self.db.pool())
                    .await?
                    .flatten();
            let Some(id) = latest else {
                return Ok(None);
            };
            let Some(row) = self.find(id).await? else {
                return Ok(None);
            };
            if self.unchanged(&row).await? {
                return Ok(Some(id));
            }
            self.corrupt(id, "its files changed or went missing after commit")
                .await?;
        }
    }

    // ---- emergency checkpoints ----

    /// Asks the job's running attempt to checkpoint now and keep training (server → agent
    /// `CheckpointAttempt` → worker). Waits until a checkpoint staged after the request is
    /// COMMITTED, the agent refuses, or `deadline` passes.
    pub async fn emergency_checkpoint(
        &self,
        job: Uuid,
        reason: &str,
        deadline: Duration,
    ) -> Result<EmergencyOutcome, JobsError> {
        let since = OffsetDateTime::now_utc();
        let Some(sent) = self.request_checkpoint(job, reason, deadline).await? else {
            return Ok(EmergencyOutcome::Refused(
                "the job has no running attempt".into(),
            ));
        };
        let end = tokio::time::Instant::now() + deadline;
        loop {
            let done: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM checkpoints WHERE job_id = $1 AND state = 'COMMITTED' \
                   AND created_at >= $2 ORDER BY global_step DESC LIMIT 1",
            )
            .bind(job)
            .bind(since)
            .fetch_optional(self.db.pool())
            .await?;
            if let Some(id) = done {
                return Ok(EmergencyOutcome::Committed(id));
            }
            let status = self
                .commands
                .status(sent)
                .await
                .map_err(|e| seam("commands", e))?;
            if let Some(s) = status {
                let refused = s.result.as_ref().is_some_and(|r| r["accepted"] == false);
                if refused || matches!(s.state.as_str(), "FAILED" | "EXPIRED" | "SUPERSEDED") {
                    let why = s
                        .result
                        .as_ref()
                        .and_then(|r| r["failure"]["message"].as_str())
                        .unwrap_or("the agent did not take the request")
                        .to_owned();
                    return Ok(EmergencyOutcome::Refused(why));
                }
            }
            if tokio::time::Instant::now() >= end {
                return Ok(EmergencyOutcome::TimedOut);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Sends `CheckpointAttempt` to the node running the job's current attempt. `None` when
    /// no attempt of the job is running.
    async fn request_checkpoint(
        &self,
        job: Uuid,
        reason: &str,
        deadline: Duration,
    ) -> Result<Option<Uuid>, JobsError> {
        let target: Option<(Uuid, i64, Uuid)> = sqlx::query_as(
            "SELECT a.id, a.epoch, c.node_id FROM jobs j \
             JOIN job_attempts a ON a.id = j.current_attempt_id AND a.epoch = j.epoch \
               AND a.state = 'RUNNING' \
             JOIN agent_commands c ON c.attempt_id = a.id AND c.epoch = a.epoch \
               AND c.kind = 'LaunchAttempt' AND c.state IN ('SENT','RECEIVED','COMPLETED') \
             WHERE j.id = $1 AND j.state = 'RUNNING' ORDER BY c.created_at DESC LIMIT 1",
        )
        .bind(job)
        .fetch_optional(self.db.pool())
        .await?;
        let Some((attempt, epoch, node)) = target else {
            return Ok(None);
        };
        let attempt = AttemptId::from_uuid(attempt);
        let epoch = u64::try_from(epoch).unwrap_or(0);
        let cmd = CheckpointAttempt {
            attempt: Some(AttemptRef {
                job_id: JobId::from_uuid(job).to_string(),
                attempt_id: attempt.to_string(),
                fencing_epoch: epoch,
            }),
            request_id: Uuid::now_v7().simple().to_string(),
            deadline_s: u32::try_from(deadline.as_secs()).unwrap_or(u32::MAX),
            reason: reason.to_owned(),
        };
        let sent = self
            .commands
            .send(
                NodeId::from_uuid(node),
                NewCommand::new(Kind::CheckpointAttempt(cmd))
                    .attempt(attempt, epoch)
                    .deadline(deadline),
            )
            .await
            .map_err(|e| seam("commands", e))?;
        Ok(Some(sent.id))
    }

    // ---- the fast background tick ----

    /// One pass: pending commits, expired tickets, FINALIZING jobs and the time-based
    /// cadence. Errors are logged; the next tick tries again.
    pub async fn tick(&self, chains: &Chains) {
        let log = |what: &str, r: Result<(), JobsError>| {
            if let Err(e) = r {
                tracing::warn!(code = e.code(), error = %e, what, "checkpoint pass failed; trying again later");
            }
        };
        log("commits", self.pending_commits().await);
        log("tickets", self.refresh_tickets().await);
        log("finalize", super::verifier::finalize(self, chains).await);
        log("cadence", self.time_based_cadence().await);
    }

    async fn pending_commits(&self) -> Result<(), JobsError> {
        let ttl = self.config.staging_ttl_seconds as f64;
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM checkpoints WHERE state IN ('WRITING','UPLOADED','VERIFYING') \
               AND (commit_requested_at IS NOT NULL OR created_at < now() - make_interval(secs => $1)) \
             ORDER BY created_at LIMIT 100",
        )
        .bind(ttl)
        .fetch_all(self.db.pool())
        .await?;
        for id in ids {
            let Some(row) = self.find(id).await? else {
                continue;
            };
            if row.commit_requested_at.is_some() {
                self.try_commit(id).await?;
            } else {
                self.corrupt(id, "the worker never asked to commit it")
                    .await?;
            }
        }
        Ok(())
    }

    /// Upload tickets live 10 minutes: open checkpoints whose tickets ended get new ones.
    async fn refresh_tickets(&self) -> Result<(), JobsError> {
        let rows = sqlx::query_as::<_, CheckpointRow>(concat!(
            "SELECT ",
            ckpt_cols!(),
            " FROM checkpoints WHERE state IN ('WRITING','UPLOADED','VERIFYING') \
               AND (tickets_expire_at IS NULL OR tickets_expire_at < now() + interval '30 seconds') LIMIT 100"
        ))
        .fetch_all(self.db.pool())
        .await?;
        for row in rows {
            if self.live_attempt(&row).await? {
                self.send_tickets(&row).await?;
            }
        }
        Ok(())
    }

    async fn live_attempt(&self, row: &CheckpointRow) -> Result<bool, JobsError> {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT j.state FROM jobs j JOIN job_attempts a ON a.job_id = j.id \
             WHERE a.id = $1 AND a.epoch = j.epoch AND j.epoch = $2",
        )
        .bind(row.attempt_id)
        .bind(row.attempt_epoch)
        .fetch_optional(self.db.pool())
        .await?;
        Ok(state.is_some_and(|s| !is_cancelled_or_over(&s)))
    }

    /// Time-based cadence (Doc 24 §5): a RUNNING attempt whose newest checkpoint (or start) is
    /// older than the run's cadence gets a `CheckpointAttempt`, at most one per period.
    async fn time_based_cadence(&self) -> Result<(), JobsError> {
        let rows: Vec<(Uuid, Option<Value>)> = sqlx::query_as(
            "SELECT j.id, rc.resolved #> '{checkpoint,time_based}' \
             FROM jobs j JOIN job_attempts a ON a.id = j.current_attempt_id AND a.state = 'RUNNING' \
             LEFT JOIN run_configs rc ON rc.run_id = j.run_id \
             WHERE j.state = 'RUNNING' AND j.run_id IS NOT NULL LIMIT 500",
        )
        .fetch_all(self.db.pool())
        .await?;
        for (job, setting) in rows {
            let minutes = setting
                .as_ref()
                .and_then(Value::as_u64)
                .unwrap_or(self.config.time_based_minutes);
            if minutes == 0 {
                continue;
            }
            let secs = (minutes.saturating_mul(60)) as f64;
            let due: bool = sqlx::query_scalar(
                "SELECT COALESCE(GREATEST(a.started_at, a.created_at, \
                     (SELECT max(c.created_at) FROM checkpoints c WHERE c.attempt_id = a.id), \
                     (SELECT max(x.created_at) FROM agent_commands x WHERE x.attempt_id = a.id \
                        AND x.kind = 'CheckpointAttempt')) \
                   < now() - make_interval(secs => $2), false) \
                 FROM jobs j JOIN job_attempts a ON a.id = j.current_attempt_id WHERE j.id = $1",
            )
            .bind(job)
            .bind(secs)
            .fetch_optional(self.db.pool())
            .await?
            .unwrap_or(false);
            if due {
                let deadline = Duration::from_secs(self.config.emergency_deadline_seconds);
                self.request_checkpoint(job, "time_based", deadline).await?;
            }
        }
        Ok(())
    }
}

/// What the storage check found.
enum Head {
    /// Everything matches: (path, entity tag) per file.
    Ok(Vec<(String, Option<String>)>),
    /// These files are not stored yet.
    Waiting(Vec<String>),
    /// Something is wrong; the checkpoint is corrupt.
    Bad(String),
}

fn mismatch(path: &str) -> JobsError {
    JobsError::domain(
        super::codes::FILES_MISMATCH,
        axum::http::StatusCode::CONFLICT,
        format!("The stored file {path} doesn't match what was saved, so the checkpoint is now CORRUPT."),
    )
}

/// Why the stored manifest (a `checkpoint-manifest/v1` document) can't stand for this
/// checkpoint: it must name this checkpoint, step and final flag, and list exactly the other
/// staged files with the same sizes and hashes.
fn manifest_problem(row: &CheckpointRow, files: &[FileRow], bytes: &[u8]) -> Option<String> {
    let Ok(doc) = serde_json::from_slice::<Value>(bytes) else {
        return Some("its manifest is not JSON".into());
    };
    if doc["schema_version"] != "checkpoint-manifest/v1" {
        return Some("its manifest is not a checkpoint-manifest/v1 document".into());
    }
    if doc["checkpoint_id"] != CheckpointId::from_uuid(row.id).to_string() {
        return Some("its manifest names another checkpoint".into());
    }
    if doc["step"].as_i64() != Some(row.global_step)
        || doc["is_final"].as_bool() != Some(row.is_final)
    {
        return Some("its manifest has another step or final flag".into());
    }
    let staged: HashMap<&str, &FileRow> = files
        .iter()
        .filter(|f| f.role != "manifest")
        .map(|f| (f.path.as_str(), f))
        .collect();
    let listed = doc["files"].as_array().cloned().unwrap_or_default();
    if listed.len() != staged.len() {
        return Some(format!(
            "its manifest lists {} files, {} were staged",
            listed.len(),
            staged.len()
        ));
    }
    for f in &listed {
        let path = f["path"].as_str().unwrap_or_default();
        let Some(s) = staged.get(path) else {
            return Some(format!("its manifest lists {path}, which was not staged"));
        };
        if f["size"].as_i64() != Some(s.size_bytes) || f["sha256"].as_str() != Some(&hex(&s.sha256))
        {
            return Some(format!(
                "{path} differs between the manifest and the staged list"
            ));
        }
    }
    None
}

/// Why the job no longer accepts this checkpoint (stale epoch, cancelled, finished).
fn fence(job: &JobRow, row: &CheckpointRow) -> Option<String> {
    if row.attempt_epoch != job.epoch {
        return Some(format!(
            "it comes from a replaced attempt (epoch {}, current {})",
            row.attempt_epoch, job.epoch
        ));
    }
    is_cancelled_or_over(&job.state).then(|| format!("the job is {}", job.state))
}

/// True if `node` was given the attempt at `epoch` (a LaunchAttempt sent to it).
async fn launched_on(
    tx: &mut Tx,
    node: Uuid,
    attempt: Uuid,
    epoch: i64,
) -> Result<bool, JobsError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM agent_commands WHERE node_id = $1 AND attempt_id = $2 \
           AND epoch = $3 AND kind = 'LaunchAttempt' AND state IN ('SENT','RECEIVED','COMPLETED'))",
    )
    .bind(node)
    .bind(attempt)
    .bind(epoch)
    .fetch_one(&mut **tx)
    .await?)
}

/// Reads and locks a job by id (in whatever org it is filed under now).
pub(crate) async fn lock_job(tx: &mut Tx, job: Uuid) -> Result<Option<JobRow>, JobsError> {
    let org: Option<Uuid> = sqlx::query_scalar("SELECT org_id FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_optional(&mut **tx)
        .await?;
    match org {
        Some(org) => JobTransitions::lock(tx, org, job).await,
        None => Ok(None),
    }
}

/// Locks the run row: commits, corruption and deletions of one run go one at a time.
pub(crate) async fn lock_run(tx: &mut Tx, run: Uuid) -> Result<(), JobsError> {
    sqlx::query("SELECT 1 FROM runs WHERE id = $1 FOR UPDATE")
        .bind(run)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Reads and locks one checkpoint.
pub(crate) async fn lock_row(tx: &mut Tx, id: Uuid) -> Result<Option<CheckpointRow>, JobsError> {
    Ok(sqlx::query_as::<_, CheckpointRow>(concat!(
        "SELECT ",
        ckpt_cols!(),
        " FROM checkpoints WHERE id = $1 FOR UPDATE"
    ))
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?)
}

async fn event(tx: &mut Tx, row: &CheckpointRow, kind: &str, seq: i64) -> Result<(), JobsError> {
    let payload = json!({
        "checkpoint_id": CheckpointId::from_uuid(row.id).to_string(),
        "run_id": RunId::from_uuid(row.run_id).to_string(),
        "step": row.global_step,
        "state": row.state,
        "is_final": row.is_final,
    });
    let ev = NewEvent::new(kind, Checkpoint::AGGREGATE, row.id, seq, payload)
        .map_err(|e| seam("events", e))?
        .org(row.org_id);
    OutboxWriter::write(tx, &ev).await?;
    Ok(())
}

/// One state step of a locked row, with its outbox event.
pub(crate) async fn step(
    tx: &mut Tx,
    row: &CheckpointRow,
    ev: CheckpointEvent,
) -> Result<CheckpointRow, JobsError> {
    let to = Checkpoint::can_transition(row.state(), ev)?;
    let row = sqlx::query_as::<_, CheckpointRow>(concat!(
        "UPDATE checkpoints SET state = $2, state_version = state_version + 1, \
           tombstoned_at = CASE WHEN $2 = 'TOMBSTONED' THEN now() ELSE tombstoned_at END \
         WHERE id = $1 AND state_version = $3 RETURNING ",
        ckpt_cols!()
    ))
    .bind(row.id)
    .bind(Checkpoint::state_name(to))
    .bind(row.state_version)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| JobsError::changed("checkpoint"))?;
    event(tx, &row, Checkpoint::event_type(ev), row.state_version).await?;
    Ok(row)
}

/// Points the run past `id`: `latest_committed` to the newest other COMMITTED checkpoint,
/// and `final_checkpoint_id` cleared if it was `id`.
pub(crate) async fn repoint(tx: &mut Tx, run: Uuid, id: Uuid) -> Result<(), JobsError> {
    sqlx::query(
        "UPDATE runs SET latest_committed_checkpoint_id = (SELECT c.id FROM checkpoints c \
             WHERE c.run_id = $1 AND c.state = 'COMMITTED' AND c.id <> $2 \
             ORDER BY c.global_step DESC, c.committed_at DESC LIMIT 1) \
         WHERE id = $1 AND latest_committed_checkpoint_id = $2",
    )
    .bind(run)
    .bind(id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE runs SET final_checkpoint_id = NULL WHERE id = $1 AND final_checkpoint_id = $2",
    )
    .bind(run)
    .bind(id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Marks a locked row CORRUPT with `why` and repoints its run.
pub(crate) async fn mark_corrupt(
    tx: &mut Tx,
    row: &CheckpointRow,
    why: &str,
) -> Result<CheckpointRow, JobsError> {
    let row = step(tx, row, CheckpointEvent::MarkCorrupt).await?;
    let why: String = why.chars().filter(|c| *c != '\0').take(1000).collect();
    sqlx::query("UPDATE checkpoints SET corrupt_reason = $2, verified_at = now() WHERE id = $1")
        .bind(row.id)
        .bind(&why)
        .execute(&mut **tx)
        .await?;
    repoint(tx, row.run_id, row.id).await?;
    Ok(CheckpointRow {
        corrupt_reason: Some(why),
        ..row
    })
}

/// Run-config settings under `checkpoint` (absent keys fall back to the server's).
pub(crate) fn run_setting<'a>(resolved: &'a Value, key: &str) -> Option<&'a Value> {
    resolved.get("checkpoint").and_then(|c| c.get(key))
}

/// Metrics as a map, for retention and the verifier.
pub(crate) fn metrics(row: &CheckpointRow) -> BTreeMap<String, f64> {
    row.metrics_at_save
        .as_ref()
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_f64()?)))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::worker::v1::FileEntry;

    const SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn staged(files: Vec<FileEntry>) -> CheckpointStaged {
        CheckpointStaged {
            checkpoint_id: "ckpt_x".into(),
            manifest_sha256: SHA.into(),
            files,
            ..Default::default()
        }
    }

    fn file(path: &str, role: &str) -> FileEntry {
        FileEntry {
            path: path.into(),
            size: 0,
            sha256: SHA.into(),
            role: role.into(),
        }
    }

    #[test]
    fn hex_round_trips_and_refuses_bad_digests() {
        let b = sha_bytes(SHA).unwrap();
        assert_eq!(hex(&b), SHA);
        assert!(sha_bytes("E3B0").is_none());
        assert!(sha_bytes(&SHA.to_uppercase()).is_none());
    }

    #[test]
    fn file_lists_need_one_manifest_and_unique_paths() {
        assert!(check_files(&staged(vec![
            file("m.json", "manifest"),
            file("w", "weights")
        ]))
        .is_ok());
        assert!(check_files(&staged(vec![file("w", "weights")])).is_err());
        assert!(check_files(&staged(vec![
            file("m.json", "manifest"),
            file("W", "weights"),
            file("w", "weights")
        ]))
        .is_err());
        let mut bad = file("w", "weights");
        bad.sha256 = "nope".into();
        assert!(check_files(&staged(vec![file("m.json", "manifest"), bad])).is_err());
    }
}
