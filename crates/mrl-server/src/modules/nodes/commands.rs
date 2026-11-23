//! Durable commands to agents.
//!
//! [`CommandSender::send`] writes an `agent_commands` row (per-node `seq`, deadline) and
//! `pg_notify('mrl_agent_commands', <node uuid>)` in one transaction; it works on any replica.
//! The replica holding the node's CONTROL stream wakes up and sends it (every stream also polls
//! every 2 s). States: `PENDING → SENT → RECEIVED` (the agent journaled it, via `ack_seq`)
//! `→ COMPLETED | FAILED` (its `CommandResult`). `PENDING`/`SENT` past the deadline become
//! `EXPIRED`; a newer command with `supersede` turns older `PENDING` ones of the same kind and
//! attempt into `SUPERSEDED`.
//!
//! Each owning part builds its own sender with the kinds it owns, so the server never sends a
//! kind before the part that handles it exists:
//! ```no_run
//! # use mrl_server::modules::nodes::commands::{CommandKind, CommandSender, NewCommand};
//! # use mrl_proto::agent::v1::{command::Kind, CancelAttempt};
//! # async fn demo(gateway: std::sync::Arc<mrl_server::modules::nodes::Gateway>,
//! #     node: mrl_domain::ids::NodeId, attempt: mrl_domain::ids::AttemptId)
//! #     -> Result<(), mrl_server::modules::nodes::NodesError> {
//! static KINDS: &[CommandKind] = &[CommandKind::LaunchAttempt, CommandKind::CancelAttempt];
//! let sender = CommandSender::new(&gateway, KINDS);
//! let cancel = Kind::CancelAttempt(CancelAttempt::default());
//! let sent = sender.send(node, NewCommand::new(cancel).attempt(attempt, 3)).await?;
//! let status = sender.status(sent.id).await?;
//! # let _ = status; Ok(()) }
//! ```

use super::{Gateway, NodesError};
use crate::db::{Db, DbError, Tx};
use mrl_domain::ids::{AttemptId, NodeId};
use mrl_proto::agent::v1::{command::Kind, Command};
use prost::Message;
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// `pg_notify` channel for new commands.
pub const WAKE_CHANNEL: &str = "mrl_agent_commands";

/// The command kinds (`agent_commands.kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum CommandKind {
    LaunchAttempt,
    StartAttempt,
    CancelAttempt,
    PauseAttempt,
    CheckpointAttempt,
    CheckpointDecision,
    StageArtifacts,
    UploadArtifacts,
    Cleanup,
    Drain,
    UpdateCredentials,
    LaunchDeployment,
    StopDeployment,
    Probe,
}

impl CommandKind {
    /// The kind of a command body.
    pub fn of(kind: &Kind) -> Self {
        match kind {
            Kind::LaunchAttempt(_) => Self::LaunchAttempt,
            Kind::StartAttempt(_) => Self::StartAttempt,
            Kind::CancelAttempt(_) => Self::CancelAttempt,
            Kind::PauseAttempt(_) => Self::PauseAttempt,
            Kind::CheckpointAttempt(_) => Self::CheckpointAttempt,
            Kind::CheckpointDecision(_) => Self::CheckpointDecision,
            Kind::StageArtifacts(_) => Self::StageArtifacts,
            Kind::UploadArtifacts(_) => Self::UploadArtifacts,
            Kind::Cleanup(_) => Self::Cleanup,
            Kind::Drain(_) => Self::Drain,
            Kind::UpdateCredentials(_) => Self::UpdateCredentials,
            Kind::LaunchDeployment(_) => Self::LaunchDeployment,
            Kind::StopDeployment(_) => Self::StopDeployment,
            Kind::Probe(_) => Self::Probe,
        }
    }

    /// Stored name, like `LaunchAttempt`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LaunchAttempt => "LaunchAttempt",
            Self::StartAttempt => "StartAttempt",
            Self::CancelAttempt => "CancelAttempt",
            Self::PauseAttempt => "PauseAttempt",
            Self::CheckpointAttempt => "CheckpointAttempt",
            Self::CheckpointDecision => "CheckpointDecision",
            Self::StageArtifacts => "StageArtifacts",
            Self::UploadArtifacts => "UploadArtifacts",
            Self::Cleanup => "Cleanup",
            Self::Drain => "Drain",
            Self::UpdateCredentials => "UpdateCredentials",
            Self::LaunchDeployment => "LaunchDeployment",
            Self::StopDeployment => "StopDeployment",
            Self::Probe => "Probe",
        }
    }

    fn starts_work(self) -> bool {
        STARTS_WORK.contains(&self.as_str())
    }
}

/// Kinds that start new work: refused, and not sent, while a node drains.
pub const STARTS_WORK: [&str; 3] = ["LaunchAttempt", "StartAttempt", "LaunchDeployment"];

/// A command to write.
#[derive(Debug, Clone)]
pub struct NewCommand {
    kind: Kind,
    attempt: Option<(AttemptId, u64)>,
    deadline: Option<Duration>,
    not_after: Option<time::OffsetDateTime>,
    supersede: bool,
}

impl NewCommand {
    /// A command with the sender's default deadline.
    pub fn new(kind: Kind) -> Self {
        Self {
            kind,
            attempt: None,
            deadline: None,
            not_after: None,
            supersede: false,
        }
    }

    /// The attempt (and its fencing epoch) the command is about.
    pub fn attempt(mut self, attempt: AttemptId, epoch: u64) -> Self {
        self.attempt = Some((attempt, epoch));
        self
    }

    /// Fails (EXPIRED) unless sent within `d`.
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = Some(d);
        self
    }

    /// Keeps this command's delivery deadline no later than an absolute expiry.
    pub(crate) fn not_after(mut self, at: Option<time::OffsetDateTime>) -> Self {
        self.not_after = at;
        self
    }

    /// Older unsent commands of the same kind and attempt become SUPERSEDED.
    pub fn superseding(mut self) -> Self {
        self.supersede = true;
        self
    }
}

/// A written command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandRef {
    /// `agent_commands.id`; the agent echoes it as `command_id`.
    pub id: Uuid,
    /// Per-node sequence number.
    pub seq: u64,
    /// False when an identical live command already existed (nothing new was written).
    pub new: bool,
}

/// A command's state and result.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandStatus {
    /// `PENDING`, `SENT`, `RECEIVED`, `COMPLETED`, `FAILED`, `EXPIRED` or `SUPERSEDED`.
    pub state: String,
    /// `{accepted, failure}` once the agent answered.
    pub result: Option<serde_json::Value>,
}

/// Writes commands of the kinds its owner handles.
#[derive(Clone)]
pub struct CommandSender {
    db: Db,
    kinds: &'static [CommandKind],
    default_deadline: Duration,
}

impl CommandSender {
    /// A sender for `kinds` (the kinds the calling part owns).
    pub fn new(gw: &Arc<Gateway>, kinds: &'static [CommandKind]) -> Self {
        Self {
            db: gw.db.clone(),
            kinds,
            default_deadline: Duration::from_secs(gw.config.command_deadline_seconds),
        }
    }

    /// Writes the command for `node` (durable; delivered by whichever replica holds the stream).
    pub async fn send(&self, node: NodeId, cmd: NewCommand) -> Result<CommandRef, NodesError> {
        let w = self.prepare(node, cmd)?;
        let out = self
            .db
            .transaction(move |tx| {
                let w = w.clone();
                Box::pin(async move { w.write(tx).await })
            })
            .await?;
        out.map_err(NodesError::CommandRefused)
    }

    /// Writes the command inside the caller's transaction (INT-2), so it exists exactly when
    /// the caller's change commits: nothing reaches the agent from a rolled-back transaction.
    /// The wake-up `pg_notify` is delivered at commit.
    pub async fn send_in(
        &self,
        tx: &mut Tx,
        node: NodeId,
        cmd: NewCommand,
    ) -> Result<CommandRef, NodesError> {
        self.prepare(node, cmd)?
            .write(tx)
            .await?
            .map_err(NodesError::CommandRefused)
    }

    /// Withdraws the attempt's unsent commands of `kinds` at `epoch` in the caller's
    /// transaction: `PENDING` → `SUPERSEDED`, payloads emptied (a withdrawn credential never
    /// stays stored). Returns how many.
    pub async fn withdraw_in(
        tx: &mut Tx,
        attempt: Uuid,
        epoch: i64,
        kinds: &[CommandKind],
    ) -> Result<u64, NodesError> {
        let kinds: Vec<&str> = kinds.iter().map(|k| k.as_str()).collect();
        Ok(sqlx::query(
            "UPDATE agent_commands SET state = 'SUPERSEDED', payload = ''::bytea, updated_at = now()
             WHERE attempt_id = $1 AND epoch = $2 AND state = 'PENDING' AND kind = ANY($3)",
        )
        .bind(attempt)
        .bind(epoch)
        .bind(&kinds)
        .execute(&mut **tx)
        .await?
        .rows_affected())
    }

    fn prepare(&self, node: NodeId, cmd: NewCommand) -> Result<Write, NodesError> {
        let kind = CommandKind::of(&cmd.kind);
        if !self.kinds.contains(&kind) {
            return Err(NodesError::CommandRefused(format!(
                "this sender does not handle {} commands",
                kind.as_str()
            )));
        }
        let deadline = time::Duration::try_from(cmd.deadline.unwrap_or(self.default_deadline))
            .unwrap_or(time::Duration::DAY);
        let (attempt, epoch) = match cmd.attempt {
            Some((a, e)) => (
                Some(a.as_uuid()),
                Some(i64::try_from(e).unwrap_or(i64::MAX)),
            ),
            None => (None, None),
        };
        let id = Uuid::now_v7();
        let command = Command {
            command_id: id.to_string(),
            kind: Some(cmd.kind),
        };
        let size = envelope_len(&command);
        if size > crate::grpc::MAX_MESSAGE_BYTES {
            return Err(NodesError::CommandRefused(format!(
                "the command is {size} bytes on the wire; the limit is {} bytes",
                crate::grpc::MAX_MESSAGE_BYTES
            )));
        }
        Ok(Write {
            id,
            node: node.as_uuid(),
            kind,
            payload: command.encode_to_vec(),
            attempt,
            epoch,
            deadline,
            not_after: cmd.not_after,
            supersede: cmd.supersede,
        })
    }

    /// The command's state and result.
    pub async fn status(&self, id: Uuid) -> Result<Option<CommandStatus>, NodesError> {
        let row = sqlx::query("SELECT state, result FROM agent_commands WHERE id = $1")
            .bind(id)
            .fetch_optional(self.db.pool())
            .await?;
        row.map(|r| {
            Ok(CommandStatus {
                state: r.try_get("state")?,
                result: r.try_get("result")?,
            })
        })
        .transpose()
        .map_err(|e: sqlx::Error| e.into())
    }
}

/// One command ready to be written (see [`CommandSender::send`]).
#[derive(Clone)]
struct Write {
    id: Uuid,
    node: Uuid,
    kind: CommandKind,
    payload: Vec<u8>,
    attempt: Option<Uuid>,
    epoch: Option<i64>,
    deadline: time::Duration,
    not_after: Option<time::OffsetDateTime>,
    supersede: bool,
}

impl Write {
    /// `Err(reason)` when the machine refuses it. An attempt has one live command per kind
    /// and epoch ([`IDENTITY_KINDS`]): writing it again returns the live one.
    async fn write(&self, tx: &mut Tx) -> Result<Result<CommandRef, String>, DbError> {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM compute_nodes WHERE id = $1 FOR UPDATE")
                .bind(self.node)
                .fetch_optional(&mut **tx)
                .await?;
        let refused = match status.as_deref() {
            None => Some("no machine has this id".to_owned()),
            Some("DECOMMISSIONED") => Some("the machine was removed".to_owned()),
            Some("DRAINING") if self.kind.starts_work() => {
                Some("the machine is draining and takes no new work".to_owned())
            }
            _ => None,
        };
        if let Some(r) = refused {
            return Ok(Err(r));
        }
        if self.supersede {
            sqlx::query(
                "UPDATE agent_commands SET state = 'SUPERSEDED', payload = ''::bytea, updated_at = now()
                 WHERE node_id = $1 AND kind = $2 AND attempt_id IS NOT DISTINCT FROM $3
                   AND state = 'PENDING'",
            )
            .bind(self.node)
            .bind(self.kind.as_str())
            .bind(self.attempt)
            .execute(&mut **tx)
            .await?;
        }
        if self.attempt.is_some() && IDENTITY_KINDS.contains(&self.kind) {
            let live: Option<(Uuid, i64)> = sqlx::query_as(
                "SELECT id, seq FROM agent_commands
                 WHERE node_id = $1 AND kind = $2 AND attempt_id = $3 AND epoch IS NOT DISTINCT FROM $4
                   AND state IN ('PENDING','SENT','RECEIVED')
                 ORDER BY seq DESC LIMIT 1",
            )
            .bind(self.node)
            .bind(self.kind.as_str())
            .bind(self.attempt)
            .bind(self.epoch)
            .fetch_optional(&mut **tx)
            .await?;
            if let Some((id, seq)) = live {
                return Ok(Ok(CommandRef {
                    id,
                    seq: seq.max(0) as u64,
                    new: false,
                }));
            }
        }
        let seq: i64 = sqlx::query_scalar(
            "UPDATE compute_nodes SET command_seq = command_seq + 1 WHERE id = $1 RETURNING command_seq",
        )
        .bind(self.node)
        .fetch_one(&mut **tx)
        .await?;
        sqlx::query(
            "INSERT INTO agent_commands (id, node_id, seq, kind, payload, attempt_id, epoch, state, deadline_at, not_after)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', LEAST(now() + $8, $9), $9)",
        )
        .bind(self.id)
        .bind(self.node)
        .bind(seq)
        .bind(self.kind.as_str())
        .bind(&self.payload)
        .bind(self.attempt)
        .bind(self.epoch)
        .bind(self.deadline)
        .bind(self.not_after)
        .execute(&mut **tx)
        .await?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(WAKE_CHANNEL)
            .bind(self.node.to_string())
            .execute(&mut **tx)
            .await?;
        Ok(Ok(CommandRef {
            id: self.id,
            seq: seq.max(0) as u64,
            new: true,
        }))
    }
}

/// Kinds with one durable identity per (attempt, epoch): while one is live (`PENDING`, `SENT`,
/// `RECEIVED`) another is not written (INT-2; the `agent_commands_one_live` index).
pub const IDENTITY_KINDS: [CommandKind; 5] = [
    CommandKind::LaunchAttempt,
    CommandKind::StageArtifacts,
    CommandKind::UpdateCredentials,
    CommandKind::CancelAttempt,
    CommandKind::PauseAttempt,
];

/// The largest the command's `CoreMessage` can be when sent (every envelope field at its
/// widest), so a command that is stored can always be sent.
pub fn envelope_len(command: &Command) -> usize {
    mrl_proto::agent::v1::CoreMessage {
        seq: u64::MAX,
        ack_seq: u64::MAX,
        sent_at: Some(prost_types::Timestamp {
            seconds: i64::MIN,
            nanos: i32::MIN,
        }),
        body: Some(mrl_proto::agent::v1::core_message::Body::Command(
            command.clone(),
        )),
    }
    .encoded_len()
}

/// Wakes local CONTROL streams when any replica writes a command (LISTEN on [`WAKE_CHANNEL`]).
pub(crate) async fn listen_for_wakes(gw: Arc<Gateway>, cancel: CancellationToken) {
    while !cancel.is_cancelled() {
        let listener = async {
            let mut l = sqlx::postgres::PgListener::connect_with(gw.db.pool()).await?;
            l.listen(WAKE_CHANNEL).await?;
            loop {
                let n = l.recv().await?;
                if let Ok(node) = Uuid::parse_str(n.payload()) {
                    gw.sessions.wake(node);
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), sqlx::Error>(())
        };
        tokio::select! {
            () = cancel.cancelled() => return,
            r = listener => {
                if let Err(e) = r {
                    tracing::debug!(error = %e, "command wake listener stopped; streams poll meanwhile");
                }
            }
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

/// Reads a row's protobuf payload back into a command.
pub(crate) fn decode(payload: &[u8]) -> Result<Command, NodesError> {
    Command::decode(payload)
        .map_err(|e| NodesError::Protocol(format!("stored command is damaged: {e}")))
}
