//! The agent's link to the core: the gateway session client.
//!
//! [`GatewayService`] (registered in `registry.rs`) waits until the machine is enrolled, then
//! keeps one mutual-TLS connection to the core — dialing it, or waiting for it on
//! `gateway.listen` — and runs three streams on it (CONTROL, TELEMETRY, BULK). Reconnecting is
//! the same for every transport: 1 s → 60 s exponential backoff with full jitter.
//!
//! - CONTROL: hello → `CoreHello.resume_from_seq` → the [`crate::spool`] replays what the core has
//!   not acked; inventory; heartbeats; commands are written to the [`crate::journal`] *before* they
//!   are acked (RECEIVED), run once by their [`CommandHandler`], and answered with a
//!   `CommandResult` through the spool. A command with no handler is answered FAILED
//!   `unsupported_command`. The certificate is renewed at 2/3 of its lifetime.
//! - TELEMETRY: node telemetry from the hardware backends every `telemetry_interval_secs`
//!   (latest wins, never spooled) plus relayed worker metrics (bounded, decimated on overflow).
//! - BULK: relayed log chunks (bounded; on overflow a marker chunk says how many were dropped).
//!
//! Other agent services use the per-agent [`Link`]: `Link::of(&ctx)?.register("Drain", handler)`,
//! `send_control(body)`, `send_metrics(relay)`, `send_logs(relay)`, `leases()`, `set_running(..)`.

pub mod tls;

use crate::enroll::Identity;
use crate::journal::{Delivery, Journal};
use crate::spool::{Spool, SpoolFull};
use crate::transports::{self, BoxIo, Transport, TransportKind};
use crate::{AgentContext, AgentError};
use hyper_util::rt::TokioIo;
use mrl_proto::agent::v1::agent_endpoint_service_server::{
    AgentEndpointService, AgentEndpointServiceServer,
};
use mrl_proto::agent::v1::agent_gateway_service_client::AgentGatewayServiceClient;
use mrl_proto::agent::v1::{
    agent_message, command, core_message, AgentHello, AgentMessage, Command, CommandResult,
    CoreHello, CoreMessage, Heartbeat, Lane, LeaseGrant, ReconcileResult, RenewCertificateRequest,
    WorkerRelay,
};
use mrl_proto::common::v1::{AttemptRef, Failure, FailureClass};
use mrl_proto::worker::v1::{worker_message, LogChunk, LogStream, WorkerMessage};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{broadcast, mpsc, Notify};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

/// The protocol version this agent speaks.
pub const PROTOCOL_VERSION: &str = "mrl.agent.v1";
/// Largest message on a session.
pub const MAX_MESSAGE_BYTES: usize = 4 << 20;
const KEEPALIVE: Duration = Duration::from_secs(20);

/// `[gateway]` in `agent.toml`. Enrollment writes the server or listen address to the identity;
/// these settings override it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewaySettings {
    /// The core's `host:port` (or tunnel hostname for `cloudflare_tunnel`).
    pub server: Option<String>,
    /// How to reach the core: `direct`, `tailscale` or `cloudflare_tunnel`; `ssh` with `listen`.
    pub transport: TransportKind,
    /// Wait for the core on this address instead of dialing (LAN, SSH forward, tunnel).
    pub listen: Option<SocketAddr>,
    /// `tailscale` program.
    pub tailscale_program: PathBuf,
    /// `cloudflared` program.
    pub cloudflared_program: PathBuf,
    /// Seconds between telemetry samples (1–300).
    pub telemetry_interval_secs: u64,
    /// CONTROL messages kept while the core is away.
    pub spool_max_entries: usize,
    /// Bytes of CONTROL messages kept while the core is away.
    pub spool_max_bytes: u64,
    /// Worker metric messages buffered before decimating.
    pub metrics_buffer: usize,
    /// Log chunks buffered before dropping (with a marker).
    pub logs_buffer: usize,
    /// Where the machine's private key is kept: `auto` (OS keychain, else a private file),
    /// `keychain` or `file`.
    pub key_store: crate::enroll::keys::KeyStoreChoice,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            server: None,
            transport: TransportKind::Direct,
            listen: None,
            tailscale_program: PathBuf::from("tailscale"),
            cloudflared_program: PathBuf::from("cloudflared"),
            telemetry_interval_secs: 5,
            spool_max_entries: 100_000,
            spool_max_bytes: 256 << 20,
            metrics_buffer: 1024,
            logs_buffer: 1024,
            key_store: crate::enroll::keys::KeyStoreChoice::Auto,
        }
    }
}

/// Codes of link errors (guidance/fragments/errors/p18.yaml).
pub mod codes {
    /// Join token not accepted.
    pub const TOKEN: &str = "MRL-NODE-180";
    /// Certificate not accepted.
    pub const CERTIFICATE: &str = "MRL-NODE-181";
    /// Not enrolled.
    pub const NOT_ENROLLED: &str = "MRL-NODE-182";
    /// Core unreachable.
    pub const UNREACHABLE: &str = "MRL-LINK-184";
    /// Control spool full.
    pub const SPOOL_FULL: &str = "MRL-NODE-188";
    /// Protocol broken.
    pub const PROTOCOL: &str = "MRL-NODE-189";
}

/// Why the link to the core failed.
#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    /// The join token can't be used here.
    #[error("the join token can't be used: {0}")]
    Token(String),
    /// The core refused (with its own code).
    #[error("the app refused: {message}")]
    Refused {
        /// The core's code, like `MRL-NODE-180`.
        code: String,
        /// Its explanation.
        message: String,
    },
    /// A certificate problem.
    #[error("the certificate was not accepted: {0}")]
    Certificate(String),
    /// Not enrolled yet.
    #[error("this machine has not joined the app yet: {0}")]
    NotEnrolled(String),
    /// The core can't be reached.
    #[error("{0}")]
    Unreachable(String),
    /// The CONTROL spool is full.
    #[error(transparent)]
    SpoolFull(#[from] SpoolFull),
    /// The core broke the protocol.
    #[error("the connection broke the protocol: {0}")]
    Protocol(String),
    /// A CONTROL message is too large to send; it was not spooled.
    #[error(
        "the message is {bytes} bytes on the wire; the limit is {max} bytes, so it was not sent"
    )]
    TooLarge {
        /// Its size when sent.
        bytes: usize,
        /// The limit.
        max: usize,
    },
    /// A local agent error (files, settings).
    #[error(transparent)]
    Agent(#[from] AgentError),
}

impl LinkError {
    /// The catalogued code.
    pub fn code(&self) -> String {
        match self {
            Self::Token(_) => codes::TOKEN.into(),
            Self::Refused { code, .. } => code.clone(),
            Self::Certificate(_) => codes::CERTIFICATE.into(),
            Self::NotEnrolled(_) => codes::NOT_ENROLLED.into(),
            Self::Unreachable(_) => codes::UNREACHABLE.into(),
            Self::SpoolFull(_) => codes::SPOOL_FULL.into(),
            Self::Protocol(_) | Self::TooLarge { .. } => codes::PROTOCOL.into(),
            Self::Agent(e) => e.code().into(),
        }
    }

    /// Code and message.
    pub fn explain(&self) -> String {
        format!("{} {self}", self.code())
    }
}

/// Runs one command kind. Results are reported once; never runs a command twice.
#[async_trait::async_trait]
pub trait CommandHandler: Send + Sync {
    /// Runs `command`; `Err` reports it FAILED with that failure. A successful work start
    /// publishes its running attempt through [`Link::set_running`] before returning, so a
    /// subsequent drain acknowledgement and heartbeat account for the started work.
    async fn handle(&self, command: Command) -> Result<(), Failure>;
}

/// The `kind` name of a command, like `LaunchAttempt`.
pub fn kind_name(kind: Option<&command::Kind>) -> &'static str {
    use command::Kind as K;
    match kind {
        None => "None",
        Some(K::LaunchAttempt(_)) => "LaunchAttempt",
        Some(K::StartAttempt(_)) => "StartAttempt",
        Some(K::CancelAttempt(_)) => "CancelAttempt",
        Some(K::PauseAttempt(_)) => "PauseAttempt",
        Some(K::StageArtifacts(_)) => "StageArtifacts",
        Some(K::UploadArtifacts(_)) => "UploadArtifacts",
        Some(K::Cleanup(_)) => "Cleanup",
        Some(K::Drain(_)) => "Drain",
        Some(K::UpdateCredentials(_)) => "UpdateCredentials",
        Some(K::LaunchDeployment(_)) => "LaunchDeployment",
        Some(K::StopDeployment(_)) => "StopDeployment",
        Some(K::Probe(_)) => "Probe",
        Some(K::CheckpointAttempt(_)) => "CheckpointAttempt",
    }
}

/// A bounded in-memory lane (METRICS or LOGS).
struct Queue {
    items: VecDeque<AgentMessage>,
    cap: usize,
    /// METRICS: items decimated away.
    dropped: u64,
    decimate: bool,
    /// LOGS: chunks dropped, per relay (attempt and role), in the order they were first lost.
    lost: VecDeque<(Option<AttemptRef>, String, u64)>,
    unattributed: u64,
}

impl Queue {
    fn new(cap: usize, decimate: bool) -> Self {
        Self {
            items: VecDeque::new(),
            cap: cap.max(2),
            dropped: 0,
            decimate,
            lost: VecDeque::new(),
            unattributed: 0,
        }
    }

    fn push(&mut self, msg: AgentMessage) {
        self.flush_markers();
        if self.items.len() < self.cap {
            self.items.push_back(msg);
            return;
        }
        if self.decimate {
            // METRICS: keep every other queued item, so the series stays even but coarser.
            let before = self.items.len();
            let mut i = 0;
            self.items.retain(|_| {
                i += 1;
                i % 2 == 0
            });
            self.dropped += (before - self.items.len()) as u64;
            self.items.push_back(msg);
        } else {
            let (attempt, role) = match &msg.body {
                Some(agent_message::Body::Worker(w)) => (w.attempt.clone(), w.role_id.clone()),
                _ => (None, String::new()),
            };
            let has_room = self.lost.len() < self.cap;
            match self
                .lost
                .iter_mut()
                .find(|(a, r, _)| *a == attempt && *r == role)
            {
                Some((_, _, n)) => *n = n.saturating_add(1),
                None if has_room => self.lost.push_back((attempt, role, 1)),
                None => self.unattributed = self.unattributed.saturating_add(1),
            }
        }
    }

    /// LOGS: one marker per relay that lost chunks, in that relay's own log.
    fn flush_markers(&mut self) {
        while self.items.len() < self.cap {
            let (attempt, role_id, n) = if let Some(relay) = self.lost.pop_front() {
                relay
            } else if self.unattributed > 0 {
                (None, String::new(), std::mem::take(&mut self.unattributed))
            } else {
                break;
            };
            self.items.push_back(AgentMessage {
                body: Some(agent_message::Body::Worker(WorkerRelay {
                    attempt,
                    role_id,
                    message: Some(WorkerMessage {
                        body: Some(worker_message::Body::Log(LogChunk {
                            stream: LogStream::Harness as i32,
                            data: format!(
                                "[mrl-agent: {n} log chunk(s) dropped: the link was too slow]\n"
                            )
                            .into_bytes(),
                            ..LogChunk::default()
                        })),
                        ..WorkerMessage::default()
                    }),
                })),
                ..AgentMessage::default()
            });
        }
    }

    fn take(&mut self) -> Vec<AgentMessage> {
        self.flush_markers();
        self.items.drain(..).collect()
    }
}

/// A FAILED result for `command_id` whose real result (`bytes` long) was too large to send.
fn too_large_result(command_id: &str, bytes: usize) -> agent_message::Body {
    agent_message::Body::CommandResult(CommandResult {
        command_id: command_id.to_owned(),
        accepted: false,
        failure: Some(Failure {
            class: FailureClass::EngineError as i32,
            sub_code: "result_too_large".into(),
            message: format!(
                "the command's result was {bytes} bytes, over the {MAX_MESSAGE_BYTES}-byte limit; its details were dropped"
            ),
            ..Failure::default()
        }),
    })
}

/// The largest `body` can be when sent (every envelope field at its widest).
fn envelope_len(body: &agent_message::Body) -> usize {
    prost::Message::encoded_len(&AgentMessage {
        seq: u64::MAX,
        ack_seq: u64::MAX,
        sent_at: Some(prost_types::Timestamp {
            seconds: i64::MIN,
            nanos: i32::MIN,
        }),
        body: Some(body.clone()),
    })
}

// This seq-zero gateway Drain is exchanged on CONTROL without a registered handler.
const DRAIN_FENCE: &str = "gateway-drain-fence";

#[derive(Default)]
struct WorkStarts {
    draining: bool,
    active: usize,
}

struct WorkStart(Arc<Link>);

impl Drop for WorkStart {
    fn drop(&mut self) {
        if let Ok(mut starts) = self.0.work_starts.lock() {
            starts.active -= 1;
        }
    }
}

/// One agent's link state, shared by the session and other agent services.
pub struct Link {
    spool: Mutex<Spool>,
    journal: Mutex<Journal>,
    wake: Notify,
    metrics: Mutex<Queue>,
    logs: Mutex<Queue>,
    metrics_wake: Notify,
    logs_wake: Notify,
    handlers: RwLock<HashMap<&'static str, Arc<dyn CommandHandler>>>,
    leases: broadcast::Sender<LeaseGrant>,
    reconcile: broadcast::Sender<ReconcileResult>,
    running: Mutex<Vec<AttemptRef>>,
    work_starts: Mutex<WorkStarts>,
    reporting: Mutex<()>,
    data_seq: Mutex<DataSeq>,
    dir: PathBuf,
    limits: (usize, u64),
    owner: Mutex<Option<String>>,
    /// Bumped by every [`Link::bind`] that changes the node; an outcome from an earlier
    /// generation is dropped.
    generation: std::sync::atomic::AtomicU64,
}

/// File naming the node the durable link state (spool, journal, stream counters) belongs to.
const OWNER_FILE: &str = "owner";

/// Seq counters of the TELEMETRY and BULK streams. Blocks of [`DataSeq::BLOCK`] are reserved
/// on disk, so a restart continues above every seq already used (gaps are fine).
struct DataSeq {
    path: PathBuf,
    next: [u64; 2],
    reserved: [u64; 2],
}

impl DataSeq {
    const BLOCK: u64 = 10_000;

    fn open(path: &std::path::Path) -> Result<Self, AgentError> {
        let reserved: [u64; 2] = match std::fs::read(path) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| AgentError::Config {
                path: path.to_path_buf(),
                reason: format!("the stream counters are damaged ({e}); remove the file"),
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => [0, 0],
            Err(e) => return Err(AgentError::state(path, e)),
        };
        Ok(Self {
            path: path.to_path_buf(),
            next: [reserved[0] + 1, reserved[1] + 1],
            reserved,
        })
    }

    fn next(&mut self, stream: usize) -> Result<u64, AgentError> {
        let i = stream.min(1);
        if self.next[i] > self.reserved[i] {
            let mut r = self.reserved;
            r[i] = self.next[i] + Self::BLOCK;
            if let Some(d) = self.path.parent() {
                std::fs::create_dir_all(d).map_err(|e| AgentError::state(d, e))?;
            }
            let bytes =
                serde_json::to_vec(&r).map_err(|e| AgentError::exec("stream counters", e))?;
            crate::state::atomic_write(&self.path, &bytes)
                .map_err(|e| AgentError::state(&self.path, e))?;
            self.reserved = r;
        }
        let seq = self.next[i];
        self.next[i] += 1;
        Ok(seq)
    }
}

impl Link {
    /// The link of the agent that owns `ctx` (one per state folder).
    pub fn of(ctx: &AgentContext) -> Result<Arc<Link>, AgentError> {
        // ponytail: process-wide map by state folder, because AgentContext has no slot for it.
        static LINKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Link>>>> = OnceLock::new();
        let map = LINKS.get_or_init(Mutex::default);
        let root = ctx.state.root().to_path_buf();
        let mut map = map
            .lock()
            .map_err(|_| AgentError::exec("gateway link", "lock poisoned"))?;
        if let Some(l) = map.get(&root) {
            return Ok(l.clone());
        }
        let link = Arc::new(Self::open(&root, &ctx.config.gateway)?);
        map.insert(root, link.clone());
        Ok(link)
    }

    /// A link on the state folder `root` (use [`Link::of`]; this is for tests).
    pub fn open(root: &std::path::Path, g: &GatewaySettings) -> Result<Link, AgentError> {
        let dir = root.join("gateway");
        let data_seq = DataSeq::open(&dir.join("data_seq.json"))?;
        let owner = std::fs::read_to_string(dir.join(OWNER_FILE))
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());
        Ok(Link {
            dir: dir.clone(),
            limits: (g.spool_max_entries, g.spool_max_bytes),
            owner: Mutex::new(owner),
            spool: Mutex::new(Spool::open(
                &dir.join("spool"),
                g.spool_max_entries,
                g.spool_max_bytes,
            )?),
            journal: Mutex::new(Journal::open(&dir.join("journal.json"))?),
            wake: Notify::new(),
            metrics: Mutex::new(Queue::new(g.metrics_buffer, true)),
            logs: Mutex::new(Queue::new(g.logs_buffer, false)),
            metrics_wake: Notify::new(),
            logs_wake: Notify::new(),
            handlers: RwLock::default(),
            leases: broadcast::channel(64).0,
            reconcile: broadcast::channel(16).0,
            running: Mutex::default(),
            work_starts: Mutex::default(),
            reporting: Mutex::default(),
            data_seq: Mutex::new(data_seq),
            generation: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Binds the durable state to `node_id` (the enrolled identity). State that belongs to
    /// another node (the machine enrolled again) is discarded: its spooled messages, journaled
    /// commands and stream counters mean nothing to the new node or its core. State written
    /// before any enrollment is adopted. Returns true when the binding changed.
    pub fn bind(&self, node_id: &str) -> Result<bool, LinkError> {
        let poisoned = || LinkError::Protocol("link lock poisoned".into());
        let mut owner = self.owner.lock().map_err(|_| poisoned())?;
        if owner.as_deref() == Some(node_id) {
            return Ok(false);
        }
        let _one = self.reporting.lock().map_err(|_| poisoned())?;
        let mut spool = self.spool.lock().map_err(|_| poisoned())?;
        let mut journal = self.journal.lock().map_err(|_| poisoned())?;
        let mut starts = self.work_starts.lock().map_err(|_| poisoned())?;
        let mut data_seq = self.data_seq.lock().map_err(|_| poisoned())?;
        if owner.is_some() {
            let spool_dir = self.dir.join("spool");
            match std::fs::remove_dir_all(&spool_dir) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    return Err(AgentError::state(&spool_dir, e).into());
                }
                _ => {}
            }
            for f in ["journal.json", "data_seq.json"] {
                let p = self.dir.join(f);
                match std::fs::remove_file(&p) {
                    Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                        return Err(AgentError::state(&p, e).into());
                    }
                    _ => {}
                }
            }
            *spool = Spool::open(&spool_dir, self.limits.0, self.limits.1)?;
            *journal = Journal::open(&self.dir.join("journal.json"))?;
            *data_seq = DataSeq::open(&self.dir.join("data_seq.json"))?;
            // Metrics and logs queued for the old node are not the new node's.
            for q in [&self.metrics, &self.logs] {
                let mut q = q.lock().map_err(|_| poisoned())?;
                *q = Queue::new(q.cap, q.decimate);
            }
            // Handlers still running for the old node finish into nothing (see `deliver`).
            self.generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        std::fs::create_dir_all(&self.dir).map_err(|e| AgentError::state(&self.dir, e))?;
        let path = self.dir.join(OWNER_FILE);
        crate::state::atomic_write(&path, node_id.as_bytes())
            .map_err(|e| AgentError::state(&path, e))?;
        *owner = Some(node_id.to_owned());
        starts.draining = false;
        Ok(true)
    }

    /// The next seq of a TELEMETRY (`0`) or BULK (`1`) stream: unique across reconnects and
    /// restarts, so sinks can dedupe on `(node, stream, seq)`.
    fn next_data_seq(&self, stream: usize) -> Result<u64, LinkError> {
        self.data_seq
            .lock()
            .map_err(|_| LinkError::Protocol("seq lock poisoned".into()))?
            .next(stream)
            .map_err(LinkError::from)
    }

    /// Registers the handler for one command kind (like `"LaunchAttempt"`). Do it at the start
    /// of the service's `run`, before any `.await`.
    pub fn register(&self, kind: &'static str, handler: Arc<dyn CommandHandler>) {
        if let Ok(mut h) = self.handlers.write() {
            h.insert(kind, handler);
        }
    }

    /// Sends a CONTROL message durably (spooled until the core acks it). Returns its seq.
    /// A message that could not be sent within [`MAX_MESSAGE_BYTES`] is refused before it is
    /// spooled ([`LinkError::TooLarge`]), so it never blocks the messages after it.
    pub fn send_control(&self, body: agent_message::Body) -> Result<u64, LinkError> {
        let size = envelope_len(&body);
        if size > MAX_MESSAGE_BYTES {
            return Err(LinkError::TooLarge {
                bytes: size,
                max: MAX_MESSAGE_BYTES,
            });
        }
        let seq = self
            .spool
            .lock()
            .map_err(|_| LinkError::Protocol("spool lock poisoned".into()))?
            .push(body)??;
        self.wake.notify_one();
        Ok(seq)
    }

    /// Queues a worker metrics message (METRICS lane: bounded, decimated on overflow).
    pub fn send_metrics(&self, relay: WorkerRelay) {
        if let Ok(mut q) = self.metrics.lock() {
            q.push(AgentMessage {
                body: Some(agent_message::Body::Worker(relay)),
                ..AgentMessage::default()
            });
        }
        // notify_one keeps the wake-up for a stream that is not waiting right now.
        self.metrics_wake.notify_one();
    }

    /// Queues a worker log chunk (LOGS lane: bounded, truncated with a marker).
    pub fn send_logs(&self, relay: WorkerRelay) {
        if let Ok(mut q) = self.logs.lock() {
            q.push(AgentMessage {
                body: Some(agent_message::Body::Worker(relay)),
                ..AgentMessage::default()
            });
        }
        self.logs_wake.notify_one();
    }

    /// Lease grants from the core (answers to heartbeats).
    pub fn leases(&self) -> broadcast::Receiver<LeaseGrant> {
        self.leases.subscribe()
    }

    /// Reconciliation results from the core.
    pub fn reconcile(&self) -> broadcast::Receiver<ReconcileResult> {
        self.reconcile.subscribe()
    }

    /// The attempts reported in each heartbeat.
    pub fn set_running(&self, running: Vec<AttemptRef>) {
        if let Ok(mut r) = self.running.lock() {
            *r = running;
        }
    }

    /// Highest command seq journaled.
    pub fn journal_seq(&self) -> u64 {
        self.journal.lock().map_or(0, |j| j.last_seq())
    }

    /// CONTROL messages waiting for an ack.
    pub fn spooled(&self) -> usize {
        self.spool.lock().map_or(0, |s| s.len())
    }

    /// Commands a restart interrupted are reported FAILED `interrupted` (never run again).
    fn report_interrupted(&self) -> Result<(), LinkError> {
        let stuck = self
            .journal
            .lock()
            .map(|j| j.interrupted())
            .unwrap_or_default();
        for (seq, id) in stuck {
            let outcome = Err(Failure {
                class: FailureClass::InfraTransient as i32,
                sub_code: "interrupted".into(),
                message: "the agent restarted while running this command; it was not run again"
                    .into(),
                ..Failure::default()
            });
            self.journal
                .lock()
                .map_err(|_| LinkError::Protocol("journal lock poisoned".into()))?
                .complete(seq, &id, &outcome)?;
        }
        self.report_results()
    }

    /// Moves finished commands' results from the journal into the CONTROL spool, oldest
    /// first. A full spool stops here and leaves the rest for the next call (after acks, on
    /// every heartbeat); results are never dropped and handlers never rerun.
    pub fn report_results(&self) -> Result<(), LinkError> {
        let _one = self
            .reporting
            .lock()
            .map_err(|_| LinkError::Protocol("report lock poisoned".into()))?;
        let pending = self
            .journal
            .lock()
            .map(|j| j.unreported())
            .unwrap_or_default();
        for u in pending {
            let body = agent_message::Body::CommandResult(CommandResult {
                command_id: u.command_id.clone(),
                accepted: u.outcome.is_ok(),
                failure: u.outcome.err(),
            });
            let sent = match self.send_control(body) {
                // A failure too large to send is reported without its details.
                Err(LinkError::TooLarge { bytes, .. }) => {
                    self.send_control(too_large_result(&u.command_id, bytes))
                }
                other => other,
            };
            match sent {
                Ok(_) => {}
                Err(LinkError::SpoolFull(_)) => return Ok(()),
                Err(e) => return Err(e),
            }
            self.journal
                .lock()
                .map_err(|_| LinkError::Protocol("journal lock poisoned".into()))?
                .reported(u.seq)?;
        }
        Ok(())
    }

    /// The spooled `msgs` that can be sent. One too large to send (spooled before sizes were
    /// checked) would fail on every replay and block the rest: a command result is swapped for a
    /// small FAILED `result_too_large` one under the same seq (so the command still finishes);
    /// anything else is set aside. Either way the original is kept on disk for diagnosis.
    fn sendable(&self, msgs: Vec<AgentMessage>) -> Result<Vec<AgentMessage>, LinkError> {
        let mut out = Vec::with_capacity(msgs.len());
        for m in msgs {
            let Some(body) = m
                .body
                .as_ref()
                .filter(|b| envelope_len(b) > MAX_MESSAGE_BYTES)
            else {
                out.push(m);
                continue;
            };
            let size = envelope_len(body);
            let mut spool = self
                .spool
                .lock()
                .map_err(|_| LinkError::Protocol("spool lock poisoned".into()))?;
            let kept = match body {
                agent_message::Body::CommandResult(r) => {
                    let (replacement, kept) =
                        spool.swap(m.seq, too_large_result(&r.command_id, size))?;
                    out.push(replacement);
                    kept
                }
                _ => spool.set_aside(m.seq)?,
            };
            eprintln!(
                "mrl-agent: CONTROL message {} is {size} bytes, over the {MAX_MESSAGE_BYTES}-byte limit; it was not sent and is kept at {}",
                m.seq,
                kept.display()
            );
        }
        Ok(out)
    }

    /// Journals the outcome of `seq` unless the link was bound to another node since
    /// `generation` (then the outcome belongs to nobody and is dropped).
    fn save_outcome(
        &self,
        generation: u64,
        seq: u64,
        id: &str,
        outcome: &std::result::Result<(), Failure>,
    ) -> Result<(), LinkError> {
        let mut j = self
            .journal
            .lock()
            .map_err(|_| LinkError::Protocol("journal lock poisoned".into()))?;
        // `bind` swaps the journal under this lock, so the check and the write see one node.
        if self.generation.load(std::sync::atomic::Ordering::SeqCst) != generation {
            return Ok(());
        }
        j.complete(seq, id, outcome).map_err(LinkError::from)
    }

    /// Journals and runs a delivered command (once).
    fn deliver(self: &Arc<Self>, seq: u64, cmd: Command) -> Result<(), LinkError> {
        let kind = kind_name(cmd.kind.as_ref());
        let starts_work = matches!(kind, "LaunchAttempt" | "StartAttempt" | "LaunchDeployment");
        let mut journal = self
            .journal
            .lock()
            .map_err(|_| LinkError::Protocol("journal lock poisoned".into()))?;
        let mut starts = self
            .work_starts
            .lock()
            .map_err(|_| LinkError::Protocol("work start lock poisoned".into()))?;
        if journal.receive(seq, &cmd.command_id)? == Delivery::Repeat {
            return Ok(());
        }
        let refused = starts_work && starts.draining;
        let activity = if starts_work && !refused {
            starts.active += 1;
            Some(WorkStart(self.clone()))
        } else {
            None
        };
        let generation = self.generation.load(std::sync::atomic::Ordering::SeqCst);
        drop(starts);
        drop(journal);
        let handler = self.handlers.read().ok().and_then(|h| h.get(kind).cloned());
        let link = self.clone();
        tokio::spawn(async move {
            let _activity = activity;
            let id = cmd.command_id.clone();
            let outcome = match (refused, handler) {
                (true, _) => Err(Failure {
                    class: FailureClass::InfraTransient as i32,
                    sub_code: "node_draining".into(),
                    message: "the machine is draining and takes no new work".into(),
                    ..Failure::default()
                }),
                (false, Some(h)) => h.handle(cmd).await,
                (false, None) => Err(Failure {
                    class: FailureClass::DependencyEnv as i32,
                    sub_code: "unsupported_command".into(),
                    message: format!("this agent has no handler for {kind} commands yet"),
                    ..Failure::default()
                }),
            };
            // The outcome is kept here until the journal has it: a failed write is retried, the
            // handler never runs again.
            let mut failures = 0u32;
            while let Err(e) = link.save_outcome(generation, seq, &id, &outcome) {
                failures = failures.saturating_add(1);
                if failures == 1 || failures.is_multiple_of(20) {
                    eprintln!(
                        "mrl-agent: the result of command {id} is not saved yet (trying again): {}",
                        e.explain()
                    );
                }
                tokio::time::sleep(backoff(failures)).await;
            }
            if let Err(e) = link.report_results() {
                // Saved in the journal: the next heartbeat or ack spools it.
                eprintln!(
                    "mrl-agent: the result of command {id} is not sent yet: {}",
                    e.explain()
                );
            }
        });
        Ok(())
    }
}

/// The transport for dialing `server` with `kind`.
pub fn dial_transport(
    g: &GatewaySettings,
    kind: TransportKind,
    server: &str,
) -> Box<dyn Transport> {
    match kind {
        TransportKind::Tailscale => Box::new(transports::Tailscale::new(
            server,
            g.tailscale_program.clone(),
        )),
        TransportKind::CloudflareTunnel => Box::new(transports::CloudflareTunnel::new(
            server,
            g.cloudflared_program.clone(),
        )),
        TransportKind::Direct | TransportKind::Ssh => Box::new(transports::Direct::new(server)),
    }
}

/// The transport this agent uses with these settings and identity.
pub fn transport_for(g: &GatewaySettings, id: &Identity) -> Result<Box<dyn Transport>, LinkError> {
    if let Some(l) = g.listen.or(id.link.listen) {
        let kind = if g.listen.is_some() {
            g.transport
        } else {
            id.link.transport
        };
        return Ok(Box::new(transports::Listen::new(l, kind)));
    }
    let server = g
        .server
        .clone()
        .or_else(|| id.link.server.clone())
        .ok_or_else(|| {
            LinkError::NotEnrolled("no server address; set gateway.server or gateway.listen".into())
        })?;
    let kind = if g.server.is_some() {
        g.transport
    } else {
        id.link.transport
    };
    Ok(dial_transport(g, kind, &server))
}

/// Runs `step`, a wait on the peer, for at most `limit`; `stop` ends it at once. Every wait on
/// the peer goes through this (or has a limit of its own), so a peer that stops answering never
/// holds a connection, an enrollment or a shutdown. `what` names the step in the error.
pub async fn bounded<T>(
    what: &str,
    limit: Duration,
    stop: &CancellationToken,
    step: impl std::future::Future<Output = Result<T, LinkError>>,
) -> Result<T, LinkError> {
    tokio::select! {
        () = stop.cancelled() => Err(LinkError::Unreachable(format!("stopped during {what}"))),
        r = tokio::time::timeout(limit, step) => r.unwrap_or_else(|_| {
            Err(LinkError::Unreachable(format!(
                "the core did not finish {what} within {} s",
                limit.as_secs()
            )))
        }),
    }
}

/// An HTTP/2 channel over one open stream (no reconnects: the session loop does that), set up
/// within [`HANDSHAKE_TIMEOUT`]; `stop` ends the setup.
pub async fn channel_over<T>(
    io: T,
    stop: &CancellationToken,
) -> Result<tonic::transport::Channel, LinkError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let connector = OneShot(Arc::new(Mutex::new(Some(io))));
    let connect = async {
        tonic::transport::Endpoint::from_static("http://core.masterrl.internal")
            .http2_keep_alive_interval(KEEPALIVE)
            .keep_alive_while_idle(true)
            .connect_with_connector(connector)
            .await
            .map_err(|e| {
                // With TLS 1.3 the core's verdict on our certificate arrives after the handshake.
                let detail = format!("{e:?}");
                if detail.contains("CertificateRevoked") {
                    LinkError::Certificate(
                        "the core refused this machine's certificate: it was revoked".into(),
                    )
                } else {
                    LinkError::Unreachable(format!("HTTP/2 to the core failed: {e}"))
                }
            })
    };
    bounded("the HTTP/2 setup", HANDSHAKE_TIMEOUT, stop, connect).await
}

struct OneShot<T>(Arc<Mutex<Option<T>>>);

impl<T> Clone for OneShot<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + 'static> tonic::codegen::Service<tonic::codegen::http::Uri> for OneShot<T> {
    type Response = TokioIo<T>;
    type Error = std::io::Error;
    type Future = std::future::Ready<Result<TokioIo<T>, std::io::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: tonic::codegen::http::Uri) -> Self::Future {
        std::future::ready(
            self.0
                .lock()
                .ok()
                .and_then(|mut s| s.take())
                .map(TokioIo::new)
                .ok_or_else(|| std::io::Error::other("this connection was already used")),
        )
    }
}

/// One accepted connection for a tonic server.
pub struct OneConn<T>(T);

impl<T> tonic::transport::server::Connected for OneConn<T> {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}

impl<T: AsyncRead + Unpin> AsyncRead for OneConn<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for OneConn<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

/// Serves `AgentEndpointService` on one connection until `stop`.
pub async fn serve_one<T, S>(
    io: T,
    svc: AgentEndpointServiceServer<S>,
    stop: CancellationToken,
) -> Result<(), LinkError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: AgentEndpointService,
{
    let incoming =
        tokio_stream::once(Ok::<_, std::io::Error>(OneConn(io))).chain(tokio_stream::pending());
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE))
        .add_service(svc)
        .serve_with_incoming_shutdown(incoming, stop.cancelled())
        .await
        .map_err(|e| LinkError::Unreachable(format!("serving the core failed: {e}")))
}

/// Full-jitter backoff: 100 ms plus up to `min(60 s, 1 s × 2^failures)`.
pub fn backoff(failures: u32) -> Duration {
    let cap = (1000u64 << failures.min(6)).min(60_000);
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    let r = nanos
        .wrapping_mul(6_364_136_223_846_793_005)
        .rotate_left(17);
    Duration::from_millis(100 + r % cap)
}

/// How a connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Shut down on purpose.
    Stopped,
    /// The certificate was renewed; reconnect with the new one.
    Renewed,
    /// The core closed the connection.
    Closed,
}

type CoreIn = Pin<Box<dyn Stream<Item = Result<CoreMessage, tonic::Status>> + Send>>;

struct LaneCtx {
    ctx: Arc<AgentContext>,
    link: Arc<Link>,
    id: Identity,
    transport: TransportKind,
}

fn hello(l: &LaneCtx, lane: Lane, resume_from_seq: u64) -> AgentMessage {
    AgentMessage {
        body: Some(agent_message::Body::Hello(AgentHello {
            node_id: l.id.link.node_id.clone(),
            certificate_sha256: l.id.cert_sha256(),
            installation_id: l.id.link.installation_id.clone(),
            agent_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol_versions: vec![PROTOCOL_VERSION.to_owned()],
            lane: lane as i32,
            resume_from_seq,
            transport: l.transport.to_proto() as i32,
        })),
        ..AgentMessage::default()
    }
}

fn status_error(s: &tonic::Status) -> LinkError {
    // With TLS 1.3 the core's verdict on our certificate arrives after the handshake, as the
    // cause of a transport error.
    if format!("{s:?}").contains("CertificateRevoked") {
        return LinkError::Certificate(
            "the core refused this machine's certificate: it was revoked".into(),
        );
    }
    let msg = s.message().to_owned();
    match msg.split_whitespace().next() {
        Some(code) if code.starts_with("MRL-") => LinkError::Refused {
            code: code.to_owned(),
            message: msg[code.len()..].trim().to_owned(),
        },
        _ => LinkError::Unreachable(format!("the core closed the stream: {msg}")),
    }
}

async fn next(inbound: &mut CoreIn) -> Result<CoreMessage, LinkError> {
    match inbound.next().await {
        Some(Ok(m)) => Ok(m),
        Some(Err(s)) => Err(status_error(&s)),
        None => Err(LinkError::Unreachable("the core closed the stream".into())),
    }
}

async fn wait_hello(inbound: &mut CoreIn) -> Result<CoreHello, LinkError> {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(30), next(inbound))
            .await
            .map_err(|_| LinkError::Unreachable("the core sent no hello within 30 s".into()))??;
        if let Some(core_message::Body::Hello(h)) = m.body {
            return Ok(h);
        }
    }
}

/// Longest one message may wait for room on a stream (a core that stopped reading).
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

async fn put(out: &mpsc::Sender<AgentMessage>, m: AgentMessage) -> Result<(), LinkError> {
    match tokio::time::timeout(SEND_TIMEOUT, out.send(m)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(LinkError::Unreachable(
            "the connection to the core closed".into(),
        )),
        Err(_) => Err(LinkError::Unreachable(format!(
            "the core stopped reading for {} s",
            SEND_TIMEOUT.as_secs()
        ))),
    }
}

fn now_ts() -> Option<prost_types::Timestamp> {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?;
    Some(prost_types::Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    })
}

/// Sends spooled messages after `last` (and a volatile body with a fresh seq, if any).
async fn flush(
    l: &LaneCtx,
    out: &mpsc::Sender<AgentMessage>,
    last: &mut u64,
    volatile: Option<agent_message::Body>,
) -> Result<u64, LinkError> {
    let (msgs, seq) = l
        .link
        .spool
        .lock()
        .map_err(|_| LinkError::Protocol("spool lock poisoned".into()))?
        .pending(*last, volatile.is_some())?;
    let ack = l.link.journal_seq();
    for mut m in l.link.sendable(msgs)? {
        m.ack_seq = ack;
        m.sent_at = now_ts();
        *last = m.seq;
        put(out, m).await?;
    }
    if let (Some(body), Some(seq)) = (volatile, seq) {
        *last = seq;
        put(
            out,
            AgentMessage {
                seq,
                ack_seq: ack,
                sent_at: now_ts(),
                body: Some(body),
            },
        )
        .await?;
        return Ok(seq);
    }
    Ok(*last)
}

async fn control_lane(
    l: &LaneCtx,
    mut inbound: CoreIn,
    out: mpsc::Sender<AgentMessage>,
    stop: CancellationToken,
) -> Result<Outcome, LinkError> {
    put(&out, hello(l, Lane::Control, l.link.journal_seq() + 1)).await?;
    let core = wait_hello(&mut inbound).await?;
    let resume = core.resume_from_seq.max(1);
    l.link
        .spool
        .lock()
        .map_err(|_| LinkError::Protocol("spool lock poisoned".into()))?
        .resume_from(resume)?;
    let mut last = resume - 1;
    let ctx = l.ctx.clone();
    let inventory = tokio::task::spawn_blocking(move || {
        let inv = ctx.inventory();
        inv.to_proto(ctx.state.root())
    })
    .await
    .map_err(|e| LinkError::Protocol(e.to_string()))?;
    flush(
        l,
        &out,
        &mut last,
        Some(agent_message::Body::Inventory(inventory)),
    )
    .await?;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(u64::from(
        core.heartbeat_interval_s.max(1),
    )));
    let renew_in =
        l.id.renew_at()
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
    let renew = tokio::time::sleep(renew_in);
    tokio::pin!(renew);
    let mut pending_key: Option<rcgen::KeyPair> = None;
    let mut drain_pending = false;
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return Ok(Outcome::Stopped),
            m = next(&mut inbound) => {
                let m = m?;
                if m.ack_seq > 0 {
                    l.link.spool.lock().map_err(|_| LinkError::Protocol("spool lock poisoned".into()))?.ack(m.ack_seq)?;
                    // The ack freed spool space: results that waited for it go in now.
                    l.link.report_results()?;
                }
                match m.body {
                    Some(core_message::Body::Command(cmd)) if m.seq > 0 => {
                        l.link.deliver(m.seq, cmd)?;
                        put(&out, AgentMessage { ack_seq: l.link.journal_seq(), sent_at: now_ts(), ..AgentMessage::default() }).await?;
                    }
                    Some(core_message::Body::Command(cmd))
                        if m.seq == 0 && cmd.command_id == DRAIN_FENCE
                            && matches!(cmd.kind, Some(command::Kind::Drain(_))) =>
                    {
                        l.link.work_starts.lock()
                            .map_err(|_| LinkError::Protocol("work start lock poisoned".into()))?
                            .draining = true;
                        drain_pending = true;
                    }
                    Some(core_message::Body::Lease(g)) => {
                        let _ = l.link.leases.send(g);
                    }
                    Some(core_message::Body::ReconcileResult(r)) => {
                        let _ = l.link.reconcile.send(r);
                    }
                    Some(core_message::Body::RenewCertificateResult(r)) => {
                        match (r.ok, pending_key.take()) {
                            (true, Some(key)) => {
                                let next = l.id.renewed(&r.certificate_chain_der, &key, r.not_after.as_ref())?;
                                next.save(l.ctx.state.root())?;
                                return Ok(Outcome::Renewed);
                            }
                            _ => {
                                let why = r.failure.map(|f| f.message).unwrap_or_default();
                                eprintln!("mrl-agent: certificate renewal failed ({why}); trying again in an hour");
                                renew.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(3600));
                            }
                        }
                    }
                    _ => {}
                }
            }
            () = l.link.wake.notified() => { flush(l, &out, &mut last, None).await?; }
            _ = heartbeat.tick() => {
                l.link.report_results()?;
                if drain_pending && l.link.work_starts.lock()
                    .map_err(|_| LinkError::Protocol("work start lock poisoned".into()))?.active == 0
                {
                    let ack = CommandResult { command_id: DRAIN_FENCE.to_owned(), accepted: true, failure: None };
                    flush(l, &out, &mut last, Some(agent_message::Body::CommandResult(ack))).await?;
                    drain_pending = false;
                }
                let running = l.link.running.lock().map(|r| r.clone()).unwrap_or_default();
                flush(l, &out, &mut last, Some(agent_message::Body::Heartbeat(Heartbeat { time: now_ts(), running }))).await?;
            }
            () = &mut renew, if pending_key.is_none() => {
                let (key, csr) = crate::enroll::new_key()?;
                pending_key = Some(key);
                flush(l, &out, &mut last, Some(agent_message::Body::RenewCertificate(RenewCertificateRequest { csr_der: csr }))).await?;
                renew.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(3600));
            }
        }
    }
}

async fn data_lane(
    l: &LaneCtx,
    lane: Lane,
    mut inbound: CoreIn,
    out: mpsc::Sender<AgentMessage>,
    stop: CancellationToken,
) -> Result<Outcome, LinkError> {
    put(&out, hello(l, lane, 0)).await?;
    wait_hello(&mut inbound).await?;
    let stream = usize::from(lane == Lane::Bulk);
    let every = Duration::from_secs(l.ctx.config.gateway.telemetry_interval_secs.clamp(1, 300));
    let mut tick = tokio::time::interval(every);
    let inventory = {
        let ctx = l.ctx.clone();
        tokio::task::spawn_blocking(move || ctx.inventory())
            .await
            .map_err(|e| LinkError::Protocol(e.to_string()))?
    };
    let (queued, wake) = if lane == Lane::Telemetry {
        (&l.link.metrics, &l.link.metrics_wake)
    } else {
        (&l.link.logs, &l.link.logs_wake)
    };
    let mut batch: Vec<AgentMessage> = Vec::new();
    loop {
        // Drain first: items queued while disconnected go out right after the hello.
        batch.extend(
            queued
                .lock()
                .map(|mut q| {
                    let taken = q.take();
                    if !q.lost.is_empty() || q.unattributed > 0 {
                        wake.notify_one();
                    }
                    taken
                })
                .unwrap_or_default(),
        );
        for mut m in batch.drain(..) {
            m.seq = l.link.next_data_seq(stream)?;
            m.sent_at = now_ts();
            put(&out, m).await?;
        }
        tokio::select! {
            biased;
            () = stop.cancelled() => return Ok(Outcome::Stopped),
            m = next(&mut inbound) => { m?; }
            _ = tick.tick(), if lane == Lane::Telemetry => {
                let t = crate::hw::collect_telemetry(&l.ctx.hw, &inventory, l.ctx.state.root());
                batch.push(AgentMessage { body: Some(agent_message::Body::Telemetry(t.to_proto())), ..AgentMessage::default() });
            }
            () = wake.notified() => {}
        }
    }
}

async fn run_lane(
    l: Arc<LaneCtx>,
    lane: Lane,
    inbound: CoreIn,
    out: mpsc::Sender<AgentMessage>,
    stop: CancellationToken,
) -> Result<Outcome, LinkError> {
    let run = async {
        match lane {
            Lane::Control => control_lane(&l, inbound, out, stop.clone()).await,
            _ => data_lane(&l, lane, inbound, out, stop.clone()).await,
        }
    };
    // `stop` ends the lane even while it waits to send (state is durable: spool and journal).
    tokio::select! {
        r = run => r,
        () = stop.cancelled() => Ok(Outcome::Stopped),
    }
}

/// Dials the core and runs the three lanes until CONTROL ends.
async fn dialed(l: Arc<LaneCtx>, io: BoxIo, stop: CancellationToken) -> Result<Outcome, LinkError> {
    let config = tls::client(&l.id.ca, l.id.chain(), l.id.key())?;
    let Some(tls_io) = connect_tls(config, io, &stop).await? else {
        return Ok(Outcome::Stopped);
    };
    let channel = channel_over(tls_io, &stop).await?;
    let client = AgentGatewayServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let stop = stop.child_token();
    let lanes = start_lanes(&stop, |lane| {
        let (l, stop, mut c) = (l.clone(), stop.clone(), client.clone());
        async move {
            let (tx, rx) = mpsc::channel(256);
            let open = async {
                c.session(ReceiverStream::new(rx))
                    .await
                    .map_err(|s| status_error(&s))
            };
            let what = format!("opening the {lane:?} stream");
            let inbound: CoreIn = Box::pin(
                bounded(&what, HANDSHAKE_TIMEOUT, &stop, open)
                    .await?
                    .into_inner(),
            );
            Ok::<_, LinkError>(tokio::spawn(run_lane(l, lane, inbound, tx, stop)))
        }
    })
    .await?;
    supervise(lanes, &stop).await
}

/// Starts CONTROL, TELEMETRY and BULK with `start`, in that order. When one fails to start, the
/// lanes already running are stopped (`stop`) and joined before the error is returned, so no
/// lane task outlives its connection.
pub async fn start_lanes<T, E, F, Fut>(
    stop: &CancellationToken,
    mut start: F,
) -> Result<Vec<tokio::task::JoinHandle<T>>, E>
where
    F: FnMut(Lane) -> Fut,
    Fut: std::future::Future<Output = Result<tokio::task::JoinHandle<T>, E>>,
{
    let mut lanes = Vec::new();
    for lane in [Lane::Control, Lane::Telemetry, Lane::Bulk] {
        match start(lane).await {
            Ok(h) => lanes.push(h),
            Err(e) => {
                stop.cancel();
                for h in lanes {
                    let _ = h.await;
                }
                return Err(e);
            }
        }
    }
    Ok(lanes)
}

/// Waits for the first of the lane tasks (CONTROL first in `lanes`) to end, then stops the
/// others. All three streams are required: when TELEMETRY or BULK ends the connection ends too,
/// so the session loop reconnects every stream.
pub async fn supervise(
    lanes: Vec<tokio::task::JoinHandle<Result<Outcome, LinkError>>>,
    stop: &CancellationToken,
) -> Result<Outcome, LinkError> {
    if lanes.is_empty() {
        return Ok(Outcome::Closed);
    }
    let (first, index, rest) = futures::future::select_all(lanes).await;
    stop.cancel();
    for h in rest {
        let _ = h.await;
    }
    let first = first.map_err(|e| LinkError::Protocol(e.to_string()))?;
    match (index, first) {
        (0, r) => r,
        (_, Ok(_)) => Ok(Outcome::Closed),
        (_, Err(e)) => Err(e),
    }
}

/// The lane a core that dialed in offers on a new stream (its first message), within
/// [`HANDSHAKE_TIMEOUT`].
async fn lane_offer(inbound: &mut CoreIn, stop: &CancellationToken) -> Result<Lane, tonic::Status> {
    let offer = bounded("the lane offer", HANDSHAKE_TIMEOUT, stop, next(inbound))
        .await
        .map_err(|e| tonic::Status::cancelled(e.explain()))?;
    let lane = match offer.body {
        Some(core_message::Body::Hello(h)) => Lane::try_from(h.lane).unwrap_or(Lane::Unspecified),
        _ => Lane::Unspecified,
    };
    if lane == Lane::Unspecified {
        return Err(tonic::Status::invalid_argument(
            "the first message must offer a lane",
        ));
    }
    Ok(lane)
}

/// `AgentEndpointService` for a core that dialed in.
struct EndpointSvc {
    lane_ctx: Arc<LaneCtx>,
    stop: CancellationToken,
    /// The first stream to end reports here.
    control_done: mpsc::Sender<Result<Outcome, LinkError>>,
}

#[tonic::async_trait]
impl AgentEndpointService for EndpointSvc {
    type SessionStream = Pin<Box<dyn Stream<Item = Result<AgentMessage, tonic::Status>> + Send>>;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<CoreMessage>>,
    ) -> Result<tonic::Response<Self::SessionStream>, tonic::Status> {
        let mut inbound: CoreIn = Box::pin(request.into_inner());
        let lane = lane_offer(&mut inbound, &self.stop).await?;
        let (tx, rx) = mpsc::channel(256);
        let (l, stop, done) = (
            self.lane_ctx.clone(),
            self.stop.clone(),
            self.control_done.clone(),
        );
        tokio::spawn(async move {
            // Any stream ending ends the connection (the first report wins), so the core
            // reconnects all three; a data stream that ends reports `Closed`.
            let r = run_lane(l, lane, inbound, tx, stop).await;
            let r = match (lane, r) {
                (Lane::Control, r) => r,
                (_, Ok(Outcome::Stopped)) => Ok(Outcome::Stopped),
                (_, Ok(_)) => Ok(Outcome::Closed),
                (_, Err(e)) => Err(e),
            };
            let _ = done.send(r).await;
        });
        Ok(tonic::Response::new(Box::pin(
            ReceiverStream::new(rx).map(Ok),
        )))
    }
}

/// Serves a core that dialed in, until its CONTROL lane ends.
/// Longest a TLS handshake may take, on every path (listen, dial, enroll).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// True when the TLS peer presented the core's certificate.
pub fn peer_is_core(conn: &tokio_rustls::rustls::ServerConnection) -> bool {
    conn.peer_certificates()
        .and_then(|c| c.first())
        .is_some_and(tls::is_core)
}

/// Accepts connections from `listen` until one finishes the TLS handshake and passes `check`.
/// Handshakes run side by side, each limited to [`HANDSHAKE_TIMEOUT`], so a peer that connects
/// and stays silent never blocks the core; `stop` ends the wait at once (`Ok(None)`).
pub async fn accept_tls(
    listen: &dyn Transport,
    acceptor: tokio_rustls::TlsAcceptor,
    check: fn(&tokio_rustls::rustls::ServerConnection) -> bool,
    stop: &CancellationToken,
) -> Result<Option<tokio_rustls::server::TlsStream<BoxIo>>, LinkError> {
    let mut handshakes = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            () = stop.cancelled() => return Ok(None),
            io = listen.connect() => {
                // Can't listen (port taken, say): back to the caller's reconnect backoff.
                let io = io?;
                let acceptor = acceptor.clone();
                handshakes.spawn(async move {
                    tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(io))
                        .await
                        .map_err(|_| LinkError::Unreachable("the peer did not finish the TLS handshake in time".into()))?
                        .map_err(|e| LinkError::Certificate(format!("TLS with the connecting peer failed: {e}")))
                });
            }
            Some(done) = handshakes.join_next(), if !handshakes.is_empty() => {
                match done {
                    Ok(Ok(tls_io)) if check(tls_io.get_ref().1) => return Ok(Some(tls_io)),
                    Ok(Ok(_)) => eprintln!("mrl-agent: dropped a connection that is not from the MasterRL core"),
                    Ok(Err(e)) => eprintln!("mrl-agent: {}", e.explain()),
                    Err(_) => {}
                }
            }
        }
    }
}

/// TLS client handshake with a time limit and `stop`.
pub async fn connect_tls(
    config: tokio_rustls::rustls::ClientConfig,
    io: BoxIo,
    stop: &CancellationToken,
) -> Result<Option<tokio_rustls::client::TlsStream<BoxIo>>, LinkError> {
    let name =
        ServerName::try_from(tls::CORE_NAME).map_err(|e| LinkError::Protocol(e.to_string()))?;
    let connect = tokio_rustls::TlsConnector::from(Arc::new(config)).connect(name, io);
    tokio::select! {
        () = stop.cancelled() => Ok(None),
        r = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect) => match r {
            Err(_) => Err(LinkError::Unreachable("the core did not finish the TLS handshake in time".into())),
            Ok(Err(e)) => Err(LinkError::Certificate(format!("TLS with the core failed: {e}"))),
            Ok(Ok(t)) => Ok(Some(t)),
        },
    }
}

async fn accepted(
    l: Arc<LaneCtx>,
    tls_io: tokio_rustls::server::TlsStream<BoxIo>,
    stop: CancellationToken,
) -> Result<Outcome, LinkError> {
    let stop = stop.child_token();
    let (done_tx, mut done_rx) = mpsc::channel(1);
    let svc = AgentEndpointServiceServer::new(EndpointSvc {
        lane_ctx: l,
        stop: stop.clone(),
        control_done: done_tx,
    })
    .max_decoding_message_size(MAX_MESSAGE_BYTES)
    .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let serve = serve_one(tls_io, svc, stop.clone());
    tokio::pin!(serve);
    let outcome = tokio::select! {
        r = &mut serve => r.map(|()| Outcome::Closed),
        r = done_rx.recv() => r.unwrap_or(Ok(Outcome::Closed)),
    };
    stop.cancel();
    outcome
}

/// One connection: open the transport, run the lanes, report how it ended.
pub async fn connect_once(
    ctx: Arc<AgentContext>,
    link: Arc<Link>,
    id: Identity,
    transport: &dyn Transport,
    stop: CancellationToken,
) -> Result<Outcome, LinkError> {
    // Never send one node's spooled data or journal positions as another node's.
    link.bind(&id.link.node_id)?;
    if transport.waits_for_core() {
        let config = tls::server(&id.ca, id.chain(), id.key())?;
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let Some(tls_io) = accept_tls(transport, acceptor, peer_is_core, &stop).await? else {
            return Ok(Outcome::Stopped);
        };
        let l = Arc::new(LaneCtx {
            ctx,
            link,
            id,
            transport: transport.kind(),
        });
        return accepted(l, tls_io, stop).await;
    }
    let io = tokio::select! {
        () = stop.cancelled() => return Ok(Outcome::Stopped),
        io = transport.connect() => io?,
    };
    let l = Arc::new(LaneCtx {
        ctx,
        link,
        id,
        transport: transport.kind(),
    });
    dialed(l, io, stop).await
}

/// The gateway link as an agent service.
#[derive(Debug, Default)]
pub struct GatewayService;

impl GatewayService {
    /// The service.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl crate::registry::AgentService for GatewayService {
    fn name(&self) -> &'static str {
        "gateway"
    }

    async fn run(&self, ctx: Arc<AgentContext>, shutdown: CancellationToken) -> crate::Result<()> {
        let link = Link::of(&ctx)?;
        let mut reported = false;
        let mut failures = 0u32;
        let mut warned = false;
        let mut listen: Option<(SocketAddr, Arc<dyn Transport>)> = None;
        while !shutdown.is_cancelled() {
            let id = match Identity::load_in(ctx.state.root(), ctx.config.gateway.key_store) {
                Ok(Some(id)) => id,
                Ok(None) => {
                    if !warned {
                        eprintln!(
                            "mrl-agent: {} this machine has not joined the app yet; run `mrl-agent enroll --server <host:port> --token <token>`",
                            codes::NOT_ENROLLED
                        );
                        warned = true;
                    }
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(Duration::from_secs(5)) => continue,
                    }
                }
                Err(e) => {
                    return Err(AgentError::Config {
                        path: crate::enroll::identity_dir(ctx.state.root()),
                        reason: e.explain(),
                    });
                }
            };
            if let Err(e) = link.bind(&id.link.node_id) {
                return Err(AgentError::Config {
                    path: crate::enroll::identity_dir(ctx.state.root()),
                    reason: e.explain(),
                });
            }
            // Before the first connection (no command runs yet): commands a restart cut off are
            // reported. Until that succeeds the agent does not connect, so a running command is
            // never mistaken for an interrupted one.
            if !reported {
                if let Err(e) = link.report_interrupted() {
                    eprintln!("mrl-agent: {}", e.explain());
                    failures = failures.saturating_add(1);
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(backoff(failures)) => continue,
                    }
                }
                reported = true;
            }
            let transport: Arc<dyn Transport> = match transport_for(&ctx.config.gateway, &id) {
                Ok(t) => {
                    // Keep one listener across reconnects.
                    match (ctx.config.gateway.listen.or(id.link.listen), &listen) {
                        (Some(a), Some((b, t))) if a == *b => t.clone(),
                        (Some(a), _) => {
                            let t: Arc<dyn Transport> = Arc::from(t);
                            listen = Some((a, t.clone()));
                            t
                        }
                        (None, _) => Arc::from(t),
                    }
                }
                Err(e) => {
                    return Err(AgentError::Config {
                        path: crate::enroll::identity_dir(ctx.state.root()),
                        reason: e.explain(),
                    });
                }
            };
            let started = tokio::time::Instant::now();
            match connect_once(
                ctx.clone(),
                link.clone(),
                id,
                transport.as_ref(),
                shutdown.clone(),
            )
            .await
            {
                Ok(Outcome::Stopped) => break,
                Ok(Outcome::Renewed) => {
                    failures = 0;
                    continue;
                }
                Ok(Outcome::Closed) => {}
                Err(e) => eprintln!("mrl-agent: link to the core: {}", e.explain()),
            }
            failures = if started.elapsed() > Duration::from_secs(30) {
                0
            } else {
                failures.saturating_add(1)
            };
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(backoff(failures)) => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay(n: u8) -> AgentMessage {
        AgentMessage {
            seq: u64::from(n),
            body: Some(agent_message::Body::Worker(WorkerRelay {
                role_id: "trainer".into(),
                ..WorkerRelay::default()
            })),
            ..AgentMessage::default()
        }
    }

    #[test]
    fn metrics_overflow_decimates_and_logs_overflow_leaves_a_marker() {
        let mut m = Queue::new(4, true);
        for i in 1..=5 {
            m.push(relay(i));
        }
        let kept: Vec<u64> = m.take().iter().map(|x| x.seq).collect();
        assert_eq!(kept, [2, 4, 5], "every other old item kept, newest kept");
        assert_eq!(m.dropped, 2);

        let mut l = Queue::new(2, false);
        for i in 1..=5 {
            l.push(relay(i));
        }
        let out = l.take();
        assert_eq!(out.iter().map(|x| x.seq).collect::<Vec<_>>(), [1, 2]);
        let mut l2 = Queue::new(2, false);
        for i in 1..=4 {
            l2.push(relay(i));
        }
        l2.items.pop_front();
        let out = l2.take();
        let marker = out.last().unwrap();
        let (role, text) = log_of(marker);
        assert!(text.contains("2 log chunk(s) dropped"), "{text}");
        assert_eq!(role, "trainer");
    }

    fn log_of(m: &AgentMessage) -> (String, String) {
        let Some(agent_message::Body::Worker(w)) = &m.body else {
            panic!("marker")
        };
        let Some(worker_message::Body::Log(c)) = w.message.as_ref().and_then(|m| m.body.as_ref())
        else {
            panic!("log")
        };
        (
            w.role_id.clone(),
            String::from_utf8_lossy(&c.data).into_owned(),
        )
    }

    #[test]
    fn each_relay_gets_its_own_loss_marker() {
        let log = |role: &str| AgentMessage {
            body: Some(agent_message::Body::Worker(WorkerRelay {
                role_id: role.into(),
                message: Some(WorkerMessage {
                    body: Some(worker_message::Body::Log(LogChunk::default())),
                    ..WorkerMessage::default()
                }),
                ..WorkerRelay::default()
            })),
            ..AgentMessage::default()
        };
        let mut q = Queue::new(2, false);
        for role in ["a", "b", "trainer", "trainer", "rollout"] {
            q.push(log(role));
        }
        q.items.clear(); // sent
        let markers: Vec<(String, String)> = q.take().iter().map(log_of).collect();
        assert_eq!(markers.len(), 2);
        assert_eq!(markers[0].0, "trainer");
        assert!(markers[0].1.contains("2 log chunk(s)"));
        assert_eq!(markers[1].0, "rollout");
        assert!(markers[1].1.contains("1 log chunk(s)"));
    }

    #[test]
    fn log_loss_bookkeeping_and_batches_are_bounded() {
        let mut queue = Queue::new(2, false);
        for i in 0..10 {
            queue.push(AgentMessage {
                body: Some(agent_message::Body::Worker(WorkerRelay {
                    role_id: i.to_string(),
                    ..WorkerRelay::default()
                })),
                ..AgentMessage::default()
            });
        }
        assert_eq!(queue.lost.len(), 2);
        assert_eq!(queue.unattributed, 6);
        assert_eq!(queue.take().len(), 2);
        assert_eq!(queue.take().len(), 2);
        let rest = queue.take();
        assert_eq!(rest.len(), 1);
        let (role, msg) = log_of(&rest[0]);
        assert!(role.is_empty());
        assert!(msg.contains("6 log chunk(s)"));
        assert!(queue.take().is_empty());
    }

    struct Count(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl CommandHandler for Count {
        async fn handle(&self, _: Command) -> Result<(), Failure> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_result_waits_for_spool_space_and_the_command_runs_once() {
        let dir = tempfile::tempdir().unwrap();
        let g = GatewaySettings {
            spool_max_entries: 1,
            ..GatewaySettings::default()
        };
        let link = Arc::new(Link::open(dir.path(), &g).unwrap());
        let count = Arc::new(Count(Default::default()));
        link.register("Probe", count.clone());
        let first = link
            .send_control(agent_message::Body::AttemptStatus(Default::default()))
            .unwrap();
        let cmd = Command {
            command_id: "c1".into(),
            kind: Some(command::Kind::Probe(Default::default())),
        };
        link.deliver(1, cmd.clone()).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(count.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            link.journal.lock().unwrap().unreported().len(),
            1,
            "kept, not dropped"
        );
        assert_eq!(link.spooled(), 1, "the spool is full");

        link.spool.lock().unwrap().ack(first).unwrap();
        link.report_results().unwrap();
        assert!(link.journal.lock().unwrap().unreported().is_empty());
        let (msgs, _) = link.spool.lock().unwrap().pending(0, false).unwrap();
        assert!(
            matches!(&msgs[0].body, Some(agent_message::Body::CommandResult(r)) if r.command_id == "c1" && r.accepted)
        );

        // The result survives a restart of the link, and a redelivery never reruns it.
        drop(link);
        let link = Arc::new(Link::open(dir.path(), &g).unwrap());
        link.register("Probe", count.clone());
        link.deliver(1, cmd).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(count.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(link.spooled(), 1);
    }

    /// A CA, and a server config for `localhost` plus a client config that trusts it.
    fn tls_pair() -> (
        tokio_rustls::rustls::ServerConfig,
        tokio_rustls::rustls::ClientConfig,
    ) {
        use tokio_rustls::rustls;
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca, ca_key);
        let key = rcgen::KeyPair::generate().unwrap();
        let leaf = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
            .unwrap()
            .signed_by(&key, &issuer)
            .unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            )
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca_cert.der().clone()).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        (server, client)
    }

    #[tokio::test]
    async fn a_silent_peer_never_blocks_the_core_and_stop_ends_the_wait() {
        let (server, client) = tls_pair();
        let listen = transports::Listen::new("127.0.0.1:0".parse().unwrap(), TransportKind::Direct);
        let addr = listen.bind().await.unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        // Connects and never speaks.
        let _silent = tokio::net::TcpStream::connect(addr).await.unwrap();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
        let good = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let name = ServerName::try_from("localhost").unwrap();
            let tls = connector.connect(name, tcp).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(tls);
        });
        let started = std::time::Instant::now();
        let stop = CancellationToken::new();
        let got = accept_tls(&listen, acceptor.clone(), |_| true, &stop)
            .await
            .unwrap();
        assert!(got.is_some());
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the silent peer did not hold the accept"
        );
        good.await.unwrap();

        // Only a silent peer: stopping ends the wait at once.
        let _silent2 = tokio::net::TcpStream::connect(addr).await.unwrap();
        let s2 = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            s2.cancel();
        });
        let started = std::time::Instant::now();
        assert!(accept_tls(&listen, acceptor, |_| true, &stop)
            .await
            .unwrap()
            .is_none());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn http2_setup_with_a_peer_that_never_reads_is_bounded_and_stop_ends_it() {
        // The peer end stays open and never reads: the HTTP/2 preface can't be written.
        let (io, _peer) = tokio::io::duplex(1);
        let stop = CancellationToken::new();
        let started = tokio::time::Instant::now();
        let r = tokio::time::timeout(HANDSHAKE_TIMEOUT * 2, channel_over(io, &stop))
            .await
            .expect("the HTTP/2 setup has a time limit");
        assert!(matches!(r, Err(LinkError::Unreachable(_))), "{r:?}");
        assert!(started.elapsed() <= HANDSHAKE_TIMEOUT + Duration::from_secs(2));
        let (io, _peer) = tokio::io::duplex(1);
        let s = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            s.cancel();
        });
        let started = tokio::time::Instant::now();
        let r = tokio::time::timeout(HANDSHAKE_TIMEOUT * 2, channel_over(io, &stop)).await;
        assert!(matches!(r, Ok(Err(_))), "{r:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "stop ended the wait"
        );
    }

    #[tokio::test]
    async fn a_core_that_never_offers_a_lane_is_dropped_in_time() {
        let mut silent: CoreIn = Box::pin(futures::stream::pending());
        let stop = CancellationToken::new();
        let started = std::time::Instant::now();
        let r = tokio::time::timeout(HANDSHAKE_TIMEOUT * 2, lane_offer(&mut silent, &stop))
            .await
            .expect("the lane offer has a time limit");
        assert_eq!(r.unwrap_err().code(), tonic::Code::Cancelled);
        assert!(started.elapsed() <= HANDSHAKE_TIMEOUT + Duration::from_secs(2));
        stop.cancel();
        assert!(
            lane_offer(&mut silent, &stop).await.is_err(),
            "stop ends the wait"
        );
    }

    #[test]
    fn enrolling_again_starts_with_fresh_link_state() {
        let dir = tempfile::tempdir().unwrap();
        let g = GatewaySettings::default();
        let link = Link::open(dir.path(), &g).unwrap();
        link.send_control(agent_message::Body::AttemptStatus(Default::default()))
            .unwrap();
        assert!(
            link.bind("node_a").unwrap(),
            "state written before enrollment is adopted"
        );
        assert_eq!(link.spooled(), 1);
        link.journal.lock().unwrap().receive(7, "c7").unwrap();
        assert!(link.next_data_seq(0).unwrap() >= 1);
        assert!(!link.bind("node_a").unwrap());
        // Re-enrolled as another node: nothing of node_a is sent or skipped.
        assert!(link.bind("node_b").unwrap());
        assert_eq!(link.spooled(), 0);
        assert_eq!(link.journal_seq(), 0);
        assert_eq!(link.next_data_seq(0).unwrap(), 1);
        // The binding survives a restart.
        drop(link);
        let link = Link::open(dir.path(), &g).unwrap();
        assert!(!link.bind("node_b").unwrap());
    }

    #[test]
    fn a_message_too_large_to_send_is_never_spooled_and_the_next_one_goes() {
        let dir = tempfile::tempdir().unwrap();
        let link = Link::open(dir.path(), &GatewaySettings::default()).unwrap();
        let huge = agent_message::Body::CommandResult(CommandResult {
            command_id: "big".into(),
            accepted: false,
            failure: Some(Failure {
                message: "x".repeat(MAX_MESSAGE_BYTES),
                ..Failure::default()
            }),
        });
        assert!(matches!(
            link.send_control(huge),
            Err(LinkError::TooLarge { .. })
        ));
        assert_eq!(link.spooled(), 0);
        link.send_control(agent_message::Body::AttemptStatus(Default::default()))
            .unwrap();
        assert_eq!(link.spooled(), 1, "the next message is not blocked");

        // A result whose failure is too large is reported without its details.
        link.journal.lock().unwrap().receive(1, "c1").unwrap();
        let big = Failure {
            message: "y".repeat(MAX_MESSAGE_BYTES),
            ..Failure::default()
        };
        link.journal
            .lock()
            .unwrap()
            .complete(1, "c1", &Err(big))
            .unwrap();
        link.report_results().unwrap();
        let (msgs, _) = link.spool.lock().unwrap().pending(0, false).unwrap();
        assert!(matches!(
            &msgs[1].body,
            Some(agent_message::Body::CommandResult(r))
                if r.command_id == "c1" && r.failure.as_ref().is_some_and(|f| f.sub_code == "result_too_large")
        ));
    }

    #[test]
    fn a_result_spooled_too_large_is_swapped_durably_and_the_next_one_goes() {
        let dir = tempfile::tempdir().unwrap();
        let link = Link::open(dir.path(), &GatewaySettings::default()).unwrap();
        // Spooled by an older agent, before sizes were checked.
        let big = link
            .spool
            .lock()
            .unwrap()
            .push(agent_message::Body::CommandResult(CommandResult {
                command_id: "big".into(),
                accepted: false,
                failure: Some(Failure {
                    message: "x".repeat(MAX_MESSAGE_BYTES),
                    ..Failure::default()
                }),
            }))
            .unwrap()
            .unwrap();
        link.send_control(agent_message::Body::AttemptStatus(Default::default()))
            .unwrap();
        let (msgs, _) = link.spool.lock().unwrap().pending(0, false).unwrap();
        let sent = link.sendable(msgs).unwrap();
        // The result is swapped in place (same seq): the command still finishes, nothing waits
        // for spool space, and the next message follows.
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].seq, big);
        assert!(matches!(&sent[0].body,
            Some(agent_message::Body::CommandResult(r)) if r.command_id == "big"
                && r.failure.as_ref().is_some_and(|f| f.sub_code == "result_too_large")));
        assert!(matches!(
            sent[1].body,
            Some(agent_message::Body::AttemptStatus(_))
        ));
        let kept = dir
            .path()
            .join(format!("gateway/spool/oversized/{big:020}.pb"));
        assert!(kept.exists(), "kept for diagnosis");
        // Durable: after a restart the spool holds the small result, not the original.
        drop(link);
        let link = Link::open(dir.path(), &GatewaySettings::default()).unwrap();
        let (msgs, _) = link.spool.lock().unwrap().pending(0, false).unwrap();
        assert_eq!(
            link.sendable(msgs.clone()).unwrap(),
            msgs,
            "nothing left to swap"
        );
        assert_eq!(msgs.len(), 2);
    }

    /// Runs until `go` is notified.
    struct Gate(tokio::sync::Notify);

    #[async_trait::async_trait]
    impl CommandHandler for Gate {
        async fn handle(&self, _: Command) -> Result<(), Failure> {
            self.0.notified().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_failed_journal_write_keeps_the_outcome_and_retries_it() {
        let dir = tempfile::tempdir().unwrap();
        let link = Arc::new(Link::open(dir.path(), &GatewaySettings::default()).unwrap());
        let gate = Arc::new(Gate(tokio::sync::Notify::new()));
        link.register("Probe", gate.clone());
        link.deliver(
            1,
            Command {
                command_id: "c1".into(),
                kind: Some(command::Kind::Probe(Default::default())),
            },
        )
        .unwrap();
        // The state folder goes away while the command runs: saving its outcome fails.
        let state = dir.path().join("gateway");
        std::fs::remove_dir_all(&state).unwrap();
        gate.0.notify_one();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            link.journal.lock().unwrap().interrupted(),
            vec![(1, "c1".to_owned())],
            "not saved yet"
        );
        std::fs::create_dir_all(state.join("spool")).unwrap();
        for _ in 0..100 {
            if link.spooled() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (msgs, _) = link.spool.lock().unwrap().pending(0, false).unwrap();
        assert!(
            matches!(
                &msgs[..],
                [AgentMessage { body: Some(agent_message::Body::CommandResult(r)), .. }] if r.command_id == "c1" && r.accepted
            ),
            "the kept outcome is saved and reported once the write works"
        );
        assert!(link.journal.lock().unwrap().interrupted().is_empty());
    }

    #[tokio::test]
    async fn an_old_nodes_handler_never_reports_into_the_new_nodes_journal() {
        let dir = tempfile::tempdir().unwrap();
        let link = Arc::new(Link::open(dir.path(), &GatewaySettings::default()).unwrap());
        let gate = Arc::new(Gate(tokio::sync::Notify::new()));
        link.register("Probe", gate.clone());
        link.bind("node_a").unwrap();
        let probe = |id: &str| Command {
            command_id: id.into(),
            kind: Some(command::Kind::Probe(Default::default())),
        };
        link.deliver(1, probe("old")).unwrap();
        link.send_logs(WorkerRelay::default());
        tokio::time::sleep(Duration::from_millis(50)).await;

        link.bind("node_b").unwrap();
        assert!(
            link.logs.lock().unwrap().take().is_empty(),
            "old logs are gone"
        );
        link.deliver(1, probe("new")).unwrap();
        gate.0.notify_one(); // the old (or new) handler finishes
        tokio::time::sleep(Duration::from_millis(100)).await;
        gate.0.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (msgs, _) = link.spool.lock().unwrap().pending(0, false).unwrap();
        let ids: Vec<String> = msgs
            .iter()
            .filter_map(|m| match &m.body {
                Some(agent_message::Body::CommandResult(r)) => Some(r.command_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            ids,
            vec!["new".to_owned()],
            "only the new node's command reports"
        );
    }

    #[tokio::test]
    async fn a_data_stream_ending_ends_the_connection() {
        let stop = CancellationToken::new();
        let s = stop.clone();
        let control = tokio::spawn(async move {
            s.cancelled().await;
            Ok(Outcome::Stopped)
        });
        let telemetry = tokio::spawn(async { Err(LinkError::Unreachable("stream reset".into())) });
        let s = stop.clone();
        let bulk = tokio::spawn(async move {
            s.cancelled().await;
            Ok(Outcome::Stopped)
        });
        let r = tokio::time::timeout(
            Duration::from_secs(2),
            supervise(vec![control, telemetry, bulk], &stop),
        )
        .await
        .expect("supervise returns when TELEMETRY ends");
        assert!(matches!(r, Err(LinkError::Unreachable(_))));
        assert!(stop.is_cancelled(), "the other streams were stopped");
    }

    #[tokio::test]
    async fn a_port_that_cannot_be_listened_on_returns_to_the_backoff() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listen =
            crate::transports::Listen::new(taken.local_addr().unwrap(), TransportKind::Direct);
        let (server, _) = tls_pair();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let r = tokio::time::timeout(
            Duration::from_secs(5),
            accept_tls(&listen, acceptor, |_| true, &CancellationToken::new()),
        )
        .await
        .expect("returns at once instead of retrying");
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn a_lane_that_fails_to_open_stops_the_lanes_already_running() {
        let stop = CancellationToken::new();
        let ended = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r = start_lanes(&stop, |lane| {
            let (stop, ended) = (stop.clone(), ended.clone());
            async move {
                if lane == Lane::Bulk {
                    return Err("refused");
                }
                Ok(tokio::spawn(async move {
                    stop.cancelled().await;
                    ended.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }))
            }
        })
        .await;
        assert_eq!(r.unwrap_err(), "refused");
        assert!(stop.is_cancelled());
        assert_eq!(
            ended.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "CONTROL and TELEMETRY ended before the error came back"
        );
    }

    #[test]
    fn stream_seqs_never_repeat_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g").join("data_seq.json");
        let mut s = DataSeq::open(&path).unwrap();
        assert_eq!(
            (s.next(0).unwrap(), s.next(0).unwrap(), s.next(1).unwrap()),
            (1, 2, 1)
        );
        let mut again = DataSeq::open(&path).unwrap();
        assert!(again.next(0).unwrap() > 2 && again.next(1).unwrap() > 1);
    }

    #[test]
    fn backoff_stays_in_range() {
        for f in 0..20 {
            let d = backoff(f);
            assert!(d >= Duration::from_millis(100) && d <= Duration::from_millis(60_100));
        }
    }

    #[test]
    fn server_errors_keep_their_code() {
        let e = status_error(&tonic::Status::permission_denied(
            "MRL-NODE-181 the machine's certificate was not accepted: revoked",
        ));
        assert_eq!(e.code(), "MRL-NODE-181");
        assert_eq!(
            status_error(&tonic::Status::unavailable("x")).code(),
            codes::UNREACHABLE
        );
    }
}
