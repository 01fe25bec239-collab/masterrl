//! A built-in job's outputs: the CONTROL sink for `worker.outputs_staged` (PUT tickets and one
//! `UploadArtifacts`) and the pass that registers uploaded outputs as a P19 artifact.
//!
//! Every outputs message gets an `UploadArtifacts` answer, so the worker never waits for
//! nothing: outputs the core refuses (bad manifest, too many files) are recorded as the job's
//! failure and answered with an empty upload; [`super::OutputsVerifier`] then fails the job.

use crate::db::Db;
use crate::modules::artifacts::{
    ArtifactStore, ArtifactsModule, NewArtifact, LOCAL_ORG, TICKET_TTL,
};
use crate::modules::jobs::{scope, JobsError};
use crate::modules::nodes::commands::{envelope_len, CommandKind, CommandSender, NewCommand};
use crate::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink, SinkError};
use crate::modules::nodes::{Gateway, NodesModule};
use crate::modules::scheduler::launch::Transfer;
use crate::seams::{Principal, RequestContext, SeamError, Seams};
use mrl_domain::ids::{AttemptId, JobId, NodeId, ProjectId};
use mrl_proto::agent::v1::agent_message::Body;
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{Command, UploadArtifacts, UploadItem};
use mrl_proto::common::v1::AttemptRef;
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::OutputsStaged;
use mrl_storage::{ArtifactManifest, TicketMethod};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

static KINDS: &[CommandKind] = &[CommandKind::UploadArtifacts];

fn seam(e: impl std::fmt::Display) -> JobsError {
    JobsError::Seam(SeamError::new("builtin", e.to_string()))
}

/// Shared by the sink and the pass: storage, the seams for tickets and command sending.
pub struct Outputs {
    db: Db,
    store: Arc<ArtifactStore>,
    transfer: Transfer,
    commands: CommandSender,
}

impl Outputs {
    /// Its own handles on storage, the server's keychain and command sending.
    pub fn from_config(db: Db, config: &crate::Config) -> Self {
        let store = ArtifactsModule::new(db.clone(), config).store();
        let gateway = NodesModule::from_config(db.clone(), config).gateway();
        Self::new(
            db.clone(),
            store,
            Transfer::from_config(&db, config),
            &gateway,
        )
    }

    /// Explicit parts (tests): `transfer` must use the signing key `store`'s server serves.
    pub fn new(
        db: Db,
        store: Arc<ArtifactStore>,
        transfer: Transfer,
        gateway: &Arc<Gateway>,
    ) -> Self {
        Self {
            db,
            store,
            transfer,
            commands: CommandSender::new(gateway, KINDS),
        }
    }

    /// The database.
    pub fn db(&self) -> &Db {
        &self.db
    }

    async fn seams(&self) -> Result<&Seams, JobsError> {
        self.transfer.seams(&self.db).await.map_err(seam)
    }

    /// Takes a relayed `OutputsStaged` from the current, running attempt of a built-in job on
    /// `node`; anything else is ignored. Idempotent per attempt.
    pub async fn on_staged(
        &self,
        node: Uuid,
        at: &AttemptRef,
        staged: &OutputsStaged,
    ) -> Result<(), JobsError> {
        let (Ok(attempt), Ok(job), Ok(epoch)) = (
            at.attempt_id.parse::<AttemptId>(),
            at.job_id.parse::<JobId>(),
            i64::try_from(at.fencing_epoch),
        ) else {
            return Ok(());
        };
        let (attempt, job) = (attempt.as_uuid(), job.as_uuid());
        let owner: Option<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT j.org_id, j.project_id FROM jobs j
             JOIN builtin_jobs b ON b.job_id = j.id
             JOIN job_attempts a ON a.id = j.current_attempt_id
             WHERE j.id = $1 AND a.id = $2 AND a.epoch = $3 AND a.state IN ('LAUNCHING','RUNNING')
               AND EXISTS (SELECT 1 FROM attempt_placements p
                           WHERE p.attempt_id = a.id AND p.node_id = $4)",
        )
        .bind(job)
        .bind(attempt)
        .bind(epoch)
        .bind(node)
        .fetch_optional(self.db.pool())
        .await?;
        let Some((org, project)) = owner else {
            return Ok(());
        };
        let prefix = format!(
            "jobs/{}/outputs/attempt-{}/",
            JobId::from_uuid(job),
            AttemptId::from_uuid(attempt)
        );
        let manifest: Value = serde_json::from_str(&staged.manifest_json).unwrap_or(Value::Null);
        // Claims the upload for this attempt (a replay whose command went out does nothing).
        let claimed = sqlx::query(
            "UPDATE builtin_jobs SET upload_attempt_id = $2, upload_prefix = $3,
                    upload_manifest = $4, upload_command_id = NULL, artifact_id = NULL,
                    failure = NULL, updated_at = now()
             WHERE job_id = $1
               AND (upload_attempt_id IS DISTINCT FROM $2 OR upload_command_id IS NULL)",
        )
        .bind(job)
        .bind(attempt)
        .bind(&prefix)
        .bind(&manifest)
        .execute(self.db.pool())
        .await?;
        if claimed.rows_affected() == 0 {
            return Ok(());
        }
        let org_text = scope::org_text(org).unwrap_or_else(|| LOCAL_ORG.to_owned());
        let (items, refused) = match self
            .items(&org_text, project, &prefix, &staged.manifest_json)
            .await?
        {
            Ok(items) => (items, None),
            Err(why) => (Vec::new(), Some(why)),
        };
        let mut upload = UploadArtifacts {
            attempt: Some(at.clone()),
            checkpoint_id: String::new(),
            items,
        };
        let too_big = envelope_len(&Command {
            command_id: Uuid::nil().to_string(),
            kind: Some(Kind::UploadArtifacts(upload.clone())),
        }) > crate::grpc::MAX_MESSAGE_BYTES;
        let refused = refused.or_else(|| too_big.then(|| "it has too many files".to_owned()));
        if refused.is_some() {
            upload.items.clear();
        }
        let sent = self
            .commands
            .send(
                NodeId::from_uuid(node),
                NewCommand::new(Kind::UploadArtifacts(upload))
                    .attempt(AttemptId::from_uuid(attempt), at.fencing_epoch)
                    .deadline(TICKET_TTL),
            )
            .await
            .map_err(seam)?;
        sqlx::query(
            "UPDATE builtin_jobs SET upload_command_id = $3, failure = $4, updated_at = now()
             WHERE job_id = $1 AND upload_attempt_id = $2",
        )
        .bind(job)
        .bind(attempt)
        .bind(sent.id)
        .bind(refused)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }

    /// A PUT ticket per manifest file, under `prefix` in `org`'s storage. Inner `Err`: the
    /// outputs are refused (why); outer: try again later.
    async fn items(
        &self,
        org: &str,
        project: Uuid,
        prefix: &str,
        manifest_json: &str,
    ) -> Result<Result<Vec<UploadItem>, String>, JobsError> {
        let manifest = match ArtifactManifest::from_json(manifest_json.as_bytes()) {
            Ok(m) => m,
            Err(e) => return Ok(Err(format!("its file list is not valid: {e}"))),
        };
        let seams = self.seams().await?;
        let store = self.store.org_store(org).await.map_err(seam)?;
        let keys = store.keys();
        let folder = format!("{}{prefix}", keys.prefix());
        let ctx = system_ctx(org, project);
        let mut items = Vec::with_capacity(manifest.files.len());
        for f in &manifest.files {
            let key = match keys.join(&folder, &f.path) {
                Ok(k) => k,
                Err(e) => return Ok(Err(format!("{}: {e}", f.path))),
            };
            let t = self
                .store
                .ticket(seams, &ctx, &key, TicketMethod::Put, f.size)
                .await
                .map_err(seam)?;
            items.push(UploadItem {
                local_path: format!("outputs/{}", f.path),
                url: t.url,
                sha256: f.sha256.clone(),
                size: f.size,
                expires_at: Some(prost_types::Timestamp {
                    seconds: t.expires_at.unix_timestamp(),
                    nanos: 0,
                }),
            });
        }
        Ok(Ok(items))
    }

    /// Registers every finished outputs upload as one artifact, or records why it failed.
    /// A storage outage leaves the row for the next pass.
    async fn register_ready(&self) -> Result<(), JobsError> {
        #[allow(clippy::type_complexity)]
        let rows: Vec<(
            Uuid,
            Uuid,
            Uuid,
            Uuid,
            Uuid,
            String,
            Value,
            String,
            Option<Value>,
        )> = sqlx::query_as(
            "SELECT b.job_id, j.org_id, j.project_id, b.upload_attempt_id, b.upload_command_id,
                        b.upload_prefix, b.upload_manifest, c.state, c.outcome
                 FROM builtin_jobs b
                 JOIN jobs j ON j.id = b.job_id
                 JOIN agent_commands c ON c.id = b.upload_command_id
                 WHERE b.artifact_id IS NULL AND b.failure IS NULL
                   AND (c.state IN ('FAILED','EXPIRED')
                        OR (c.state = 'COMPLETED' AND c.outcome IS NOT NULL))
                 ORDER BY b.updated_at LIMIT 50",
        )
        .fetch_all(self.db.pool())
        .await?;
        for (job, org, project, attempt, command, prefix, manifest, state, outcome) in rows {
            let uploaded =
                state == "COMPLETED" && outcome.as_ref().is_some_and(|o| o["accepted"] == true);
            let result = if uploaded {
                self.register(org, project, job, attempt, prefix, manifest)
                    .await
            } else {
                let why = outcome
                    .as_ref()
                    .and_then(|o| o["failure"]["message"].as_str().or(o["failure"].as_str()))
                    .map_or_else(|| format!("the upload command is {state}"), str::to_owned);
                Err(why)
            };
            let (artifact, failure) = match result {
                Ok(Some(id)) => (Some(id), None),
                Ok(None) => continue, // storage is down: next pass
                Err(why) => (None, Some(why)),
            };
            sqlx::query(
                "UPDATE builtin_jobs SET artifact_id = $3, failure = $4, updated_at = now()
                 WHERE job_id = $1 AND upload_command_id = $2 AND artifact_id IS NULL",
            )
            .bind(job)
            .bind(command)
            .bind(artifact)
            .bind(failure)
            .execute(self.db.pool())
            .await?;
        }
        Ok(())
    }

    /// `Ok(None)`: try again later.
    async fn register(
        &self,
        org: Uuid,
        project: Uuid,
        job: Uuid,
        attempt: Uuid,
        prefix: String,
        manifest: Value,
    ) -> Result<Option<Uuid>, String> {
        let org_text = scope::org_text(org).unwrap_or_else(|| LOCAL_ORG.to_owned());
        let Ok(seams) = self.seams().await else {
            return Ok(None);
        };
        let new = NewArtifact {
            name: format!(
                "{}-{}",
                JobId::from_uuid(job),
                AttemptId::from_uuid(attempt)
            ),
            prefix: Some(prefix),
            manifest,
            project_id: Some(ProjectId::from_uuid(project).to_string()),
            links: Vec::new(),
            uploads: Default::default(),
        };
        match self
            .store
            .register(seams, &system_ctx(&org_text, project), new, None)
            .await
        {
            Ok(a) => Ok(Some(a.uuid())),
            Err(e) if e.status() == axum::http::StatusCode::SERVICE_UNAVAILABLE => Ok(None),
            Err(e) => Err(format!("{}: {e}", e.code())),
        }
    }
}

/// The core acting for a job in `org` / `project`: the local owner on the core's own solo
/// seams, under its own id (as P23 does for checkpoint tickets).
fn system_ctx(org: &str, project: Uuid) -> RequestContext {
    let mut principal = Principal::local_owner();
    principal.id = "system:builtin".into();
    principal.org = Some(org.to_owned());
    RequestContext {
        principal,
        org: Some(org.to_owned()),
        project: Some(ProjectId::from_uuid(project).to_string()),
        trace_id: String::new(),
        request_id: Uuid::now_v7().to_string(),
    }
}

/// One pass of the background task (errors are logged; the next pass retries).
pub async fn register_pass(outputs: &Outputs) {
    if let Err(e) = outputs.register_ready().await {
        tracing::warn!(code = e.code(), error = %e, "built-in outputs could not be registered");
    }
}

/// Takes `worker.outputs_staged`. Only a database, storage or command outage is an error
/// (the item is kept and retried).
pub struct OutputsSink {
    outputs: Arc<Outputs>,
}

impl OutputsSink {
    /// A sink feeding `outputs`.
    pub fn new(outputs: Arc<Outputs>) -> Self {
        Self { outputs }
    }
}

#[async_trait::async_trait]
impl LaneSink for OutputsSink {
    fn name(&self) -> &'static str {
        "builtin_outputs"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Control]
    }

    fn control_kinds(&self) -> &'static [&'static str] {
        &["worker.outputs_staged"]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        let Some(Body::Worker(relay)) = &item.message.body else {
            return Ok(());
        };
        let Some(WorkerBody::OutputsStaged(s)) =
            relay.message.as_ref().and_then(|m| m.body.as_ref())
        else {
            return Ok(());
        };
        let at = relay.attempt.clone().unwrap_or_default();
        match self.outputs.on_staged(item.node_id, &at, s).await {
            Ok(()) => Ok(()),
            Err(e @ (JobsError::Db(_) | JobsError::Seam(_))) => Err(SinkError {
                sink: "builtin_outputs",
                detail: e.to_string(),
            }),
            Err(e) => {
                tracing::warn!(code = e.code(), error = %e, node = %item.node_id, "job outputs refused");
                Ok(())
            }
        }
    }
}
