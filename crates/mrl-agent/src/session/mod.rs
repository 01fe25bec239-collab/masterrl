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
            Self::Protocol(_) => codes::PROTOCOL.into(),
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
    /// Runs `command`; `Err` reports it FAILED with that failure.
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
    dropped: u64,
    decimate: bool,
    last_relay: Option<(Option<AttemptRef>, String)>,
}

impl Queue {
    fn new(cap: usize, decimate: bool) -> Self {
        Self {
            items: VecDeque::new(),
            cap: cap.max(2),
            dropped: 0,
            decimate,
            last_relay: None,
        }
    }

    fn push(&mut self, msg: AgentMessage) {
        if self.items.len() < self.cap {
            if !self.decimate && self.dropped > 0 {
                let marker = self.marker();
                self.items.push_back(marker);
                self.dropped = 0;
            }
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
            if let Some(agent_message::Body::Worker(w)) = &msg.body {
                self.last_relay = Some((w.attempt.clone(), w.role_id.clone()));
            }
            self.dropped += 1;
        }
    }

    fn marker(&self) -> AgentMessage {
        let (attempt, role_id) = self.last_relay.clone().unwrap_or_default();
        AgentMessage {
            body: Some(agent_message::Body::Worker(WorkerRelay {
                attempt,
                role_id,
                message: Some(WorkerMessage {
                    body: Some(worker_message::Body::Log(LogChunk {
                        stream: LogStream::Harness as i32,
                        data: format!(
                            "[mrl-agent: {} log chunk(s) dropped: the link was too slow]\n",
                            self.dropped
                        )
                        .into_bytes(),
                        ..LogChunk::default()
                    })),
                    ..WorkerMessage::default()
                }),
            })),
            ..AgentMessage::default()
        }
    }

    fn take(&mut self) -> Vec<AgentMessage> {
        if !self.decimate && self.dropped > 0 && self.items.len() < self.cap {
            let m = self.marker();
            self.items.push_back(m);
            self.dropped = 0;
        }
        self.items.drain(..).collect()
    }
}

/// One agent's link state, shared by the session and other agent services.
pub struct Link {
    spool: Mutex<Spool>,
    journal: Mutex<Journal>,
    wake: Notify,
    metrics: Mutex<Queue>,
    logs: Mutex<Queue>,
    data_wake: Notify,
    handlers: RwLock<HashMap<&'static str, Arc<dyn CommandHandler>>>,
    leases: broadcast::Sender<LeaseGrant>,
    reconcile: broadcast::Sender<ReconcileResult>,
    running: Mutex<Vec<AttemptRef>>,
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
        let g = &ctx.config.gateway;
        let dir = root.join("gateway");
        let link = Arc::new(Link {
            spool: Mutex::new(Spool::open(
                &dir.join("spool"),
                g.spool_max_entries,
                g.spool_max_bytes,
            )?),
            journal: Mutex::new(Journal::open(&dir.join("journal.json"))?),
            wake: Notify::new(),
            metrics: Mutex::new(Queue::new(g.metrics_buffer, true)),
            logs: Mutex::new(Queue::new(g.logs_buffer, false)),
            data_wake: Notify::new(),
            handlers: RwLock::default(),
            leases: broadcast::channel(64).0,
            reconcile: broadcast::channel(16).0,
            running: Mutex::default(),
        });
        map.insert(root, link.clone());
        Ok(link)
    }

    /// Registers the handler for one command kind (like `"LaunchAttempt"`). Do it at the start
    /// of the service's `run`, before any `.await`.
    pub fn register(&self, kind: &'static str, handler: Arc<dyn CommandHandler>) {
        if let Ok(mut h) = self.handlers.write() {
            h.insert(kind, handler);
        }
    }

    /// Sends a CONTROL message durably (spooled until the core acks it). Returns its seq.
    pub fn send_control(&self, body: agent_message::Body) -> Result<u64, LinkError> {
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
        self.data_wake.notify_waiters();
    }

    /// Queues a worker log chunk (LOGS lane: bounded, truncated with a marker).
    pub fn send_logs(&self, relay: WorkerRelay) {
        if let Ok(mut q) = self.logs.lock() {
            q.push(AgentMessage {
                body: Some(agent_message::Body::Worker(relay)),
                ..AgentMessage::default()
            });
        }
        self.data_wake.notify_waiters();
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

    fn report_interrupted(&self) -> Result<(), LinkError> {
        let stuck = self
            .journal
            .lock()
            .map(|j| j.interrupted())
            .unwrap_or_default();
        for (seq, id) in stuck {
            self.result(
                &id,
                Err(Failure {
                    class: FailureClass::InfraTransient as i32,
                    sub_code: "interrupted".into(),
                    message: "the agent restarted while running this command; it was not run again"
                        .into(),
                    ..Failure::default()
                }),
            )?;
            if let Ok(mut j) = self.journal.lock() {
                j.finish(seq)?;
            }
        }
        Ok(())
    }

    fn result(&self, command_id: &str, outcome: Result<(), Failure>) -> Result<(), LinkError> {
        self.send_control(agent_message::Body::CommandResult(CommandResult {
            command_id: command_id.to_owned(),
            accepted: outcome.is_ok(),
            failure: outcome.err(),
        }))
        .map(|_| ())
    }

    /// Journals and runs a delivered command (once).
    fn deliver(self: &Arc<Self>, seq: u64, cmd: Command) -> Result<(), LinkError> {
        let fresh = self
            .journal
            .lock()
            .map_err(|_| LinkError::Protocol("journal lock poisoned".into()))?
            .receive(seq, &cmd.command_id)?;
        if fresh == Delivery::Repeat {
            return Ok(());
        }
        let kind = kind_name(cmd.kind.as_ref());
        let handler = self.handlers.read().ok().and_then(|h| h.get(kind).cloned());
        let link = self.clone();
        tokio::spawn(async move {
            let id = cmd.command_id.clone();
            let outcome = match handler {
                Some(h) => h.handle(cmd).await,
                None => Err(Failure {
                    class: FailureClass::DependencyEnv as i32,
                    sub_code: "unsupported_command".into(),
                    message: format!("this agent has no handler for {kind} commands yet"),
                    ..Failure::default()
                }),
            };
            if let Err(e) = link.result(&id, outcome) {
                eprintln!(
                    "mrl-agent: could not record the result of command {id}: {}",
                    e.explain()
                );
                return;
            }
            if let Ok(mut j) = link.journal.lock() {
                let _ = j.finish(seq);
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

/// An HTTP/2 channel over one open stream (no reconnects: the session loop does that).
pub async fn channel_over<T>(io: T) -> Result<tonic::transport::Channel, LinkError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let connector = OneShot(Arc::new(Mutex::new(Some(io))));
    tonic::transport::Endpoint::from_static("http://core.masterrl.internal")
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_while_idle(true)
        .connect_with_connector(connector)
        .await
        .map_err(|e| LinkError::Unreachable(format!("HTTP/2 to the core failed: {e}")))
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

async fn put(out: &mpsc::Sender<AgentMessage>, m: AgentMessage) -> Result<(), LinkError> {
    out.send(m)
        .await
        .map_err(|_| LinkError::Unreachable("the connection to the core closed".into()))
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
    for mut m in msgs {
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
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return Ok(Outcome::Stopped),
            m = next(&mut inbound) => {
                let m = m?;
                if m.ack_seq > 0 {
                    l.link.spool.lock().map_err(|_| LinkError::Protocol("spool lock poisoned".into()))?.ack(m.ack_seq)?;
                }
                match m.body {
                    Some(core_message::Body::Command(cmd)) if m.seq > 0 => {
                        l.link.deliver(m.seq, cmd)?;
                        put(&out, AgentMessage { ack_seq: l.link.journal_seq(), sent_at: now_ts(), ..AgentMessage::default() }).await?;
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
    let mut seq = 0u64;
    let every = Duration::from_secs(l.ctx.config.gateway.telemetry_interval_secs.clamp(1, 300));
    let mut tick = tokio::time::interval(every);
    let inventory = {
        let ctx = l.ctx.clone();
        tokio::task::spawn_blocking(move || ctx.inventory())
            .await
            .map_err(|e| LinkError::Protocol(e.to_string()))?
    };
    loop {
        let mut batch = Vec::new();
        tokio::select! {
            biased;
            () = stop.cancelled() => return Ok(Outcome::Stopped),
            m = next(&mut inbound) => { m?; }
            _ = tick.tick(), if lane == Lane::Telemetry => {
                let t = crate::hw::collect_telemetry(&l.ctx.hw, &inventory, l.ctx.state.root());
                batch.push(AgentMessage { body: Some(agent_message::Body::Telemetry(t.to_proto())), ..AgentMessage::default() });
            }
            () = l.link.data_wake.notified() => {}
        }
        let queued = if lane == Lane::Telemetry {
            &l.link.metrics
        } else {
            &l.link.logs
        };
        batch.extend(queued.lock().map(|mut q| q.take()).unwrap_or_default());
        for mut m in batch {
            seq += 1;
            m.seq = seq;
            m.sent_at = now_ts();
            put(&out, m).await?;
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
    match lane {
        Lane::Control => control_lane(&l, inbound, out, stop).await,
        _ => data_lane(&l, lane, inbound, out, stop).await,
    }
}

/// Dials the core and runs the three lanes until CONTROL ends.
async fn dialed(l: Arc<LaneCtx>, io: BoxIo, stop: CancellationToken) -> Result<Outcome, LinkError> {
    let config = tls::client(&l.id.ca, l.id.chain(), l.id.key())?;
    let name =
        ServerName::try_from(tls::CORE_NAME).map_err(|e| LinkError::Protocol(e.to_string()))?;
    let tls_io = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, io)
        .await
        .map_err(|e| LinkError::Certificate(format!("TLS with the core failed: {e}")))?;
    let channel = channel_over(tls_io).await?;
    let client = AgentGatewayServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let stop = stop.child_token();
    let mut lanes = Vec::new();
    for lane in [Lane::Control, Lane::Telemetry, Lane::Bulk] {
        let (tx, rx) = mpsc::channel(256);
        let mut c = client.clone();
        let inbound: CoreIn = Box::pin(
            c.session(ReceiverStream::new(rx))
                .await
                .map_err(|s| status_error(&s))?
                .into_inner(),
        );
        lanes.push(tokio::spawn(run_lane(
            l.clone(),
            lane,
            inbound,
            tx,
            stop.clone(),
        )));
    }
    let mut it = lanes.into_iter();
    let control = match it.next() {
        Some(h) => h.await.map_err(|e| LinkError::Protocol(e.to_string()))?,
        None => Ok(Outcome::Closed),
    };
    stop.cancel();
    for h in it {
        let _ = h.await;
    }
    control
}

/// `AgentEndpointService` for a core that dialed in.
struct EndpointSvc {
    lane_ctx: Arc<LaneCtx>,
    stop: CancellationToken,
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
        let offer = match inbound.next().await {
            Some(Ok(m)) => m,
            _ => return Err(tonic::Status::cancelled("closed before the lane offer")),
        };
        let lane = match offer.body {
            Some(core_message::Body::Hello(h)) => {
                Lane::try_from(h.lane).unwrap_or(Lane::Unspecified)
            }
            _ => Lane::Unspecified,
        };
        if lane == Lane::Unspecified {
            return Err(tonic::Status::invalid_argument(
                "the first message must offer a lane",
            ));
        }
        let (tx, rx) = mpsc::channel(256);
        let (l, stop, done) = (
            self.lane_ctx.clone(),
            self.stop.clone(),
            self.control_done.clone(),
        );
        tokio::spawn(async move {
            let r = run_lane(l, lane, inbound, tx, stop).await;
            if lane == Lane::Control {
                let _ = done.send(r).await;
            }
        });
        Ok(tonic::Response::new(Box::pin(
            ReceiverStream::new(rx).map(Ok),
        )))
    }
}

/// Serves a core that dialed in, until its CONTROL lane ends.
async fn accepted(
    l: Arc<LaneCtx>,
    io: BoxIo,
    stop: CancellationToken,
) -> Result<Outcome, LinkError> {
    let config = tls::server(&l.id.ca, l.id.chain(), l.id.key())?;
    let tls_io = tokio_rustls::TlsAcceptor::from(Arc::new(config))
        .accept(io)
        .await
        .map_err(|e| LinkError::Certificate(format!("TLS with the connecting peer failed: {e}")))?;
    let is_core = tls_io
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first())
        .is_some_and(tls::is_core);
    if !is_core {
        return Err(LinkError::Certificate(
            "the connecting peer is not the MasterRL core".into(),
        ));
    }
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
    if transport.waits_for_core() {
        accepted(l, io, stop).await
    } else {
        dialed(l, io, stop).await
    }
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
        if let Err(e) = link.report_interrupted() {
            eprintln!("mrl-agent: {}", e.explain());
        }
        let mut failures = 0u32;
        let mut warned = false;
        let mut listen: Option<(SocketAddr, Arc<dyn Transport>)> = None;
        while !shutdown.is_cancelled() {
            let id = match Identity::load(ctx.state.root()) {
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
                    })
                }
            };
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
                    })
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
        let Some(agent_message::Body::Worker(w)) = &marker.body else {
            panic!("marker")
        };
        let Some(worker_message::Body::Log(c)) = w.message.as_ref().and_then(|m| m.body.as_ref())
        else {
            panic!("log")
        };
        assert!(String::from_utf8_lossy(&c.data).contains("2 log chunk(s) dropped"));
        assert_eq!(w.role_id, "trainer");
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
