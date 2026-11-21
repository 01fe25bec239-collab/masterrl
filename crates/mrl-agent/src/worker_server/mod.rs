//! The agent side of the Worker Protocol (`worker.v1.WorkerControlService`), one server per
//! attempt.
//!
//! - Transport: a Unix socket (mode 0600) where the OS has them; `127.0.0.1:<port>` elsewhere.
//! - Auth: the first message must be `Hello` with this attempt's id and per-attempt nonce
//!   (compared in constant time); anything else closes the stream.
//! - Versions: the highest version both sides offer; with none in common the agent answers
//!   `HelloAck` with its own version and closes, and the worker exits.
//! - Delivery: data messages (seq ≥ 1) are de-duplicated per `(attempt_id, role_id, seq)`.
//!   A new message is handed to the attempt runner and acknowledged (`ack_seq`) only after the
//!   runner has handled it; the accepted seq is saved, so after an agent restart `HelloAck`
//!   asks the worker to replay from the first message the agent did not finish. A gap in the
//!   numbering ends the stream so the worker reconnects and replays. `Hello` and `Heartbeat`
//!   (seq 0) are never acknowledged or de-duplicated.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use mrl_proto::worker::v1::agent_message::Body as AgentBody;
use mrl_proto::worker::v1::worker_control_service_server::{
    WorkerControlService, WorkerControlServiceServer,
};
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::{AgentMessage, Hello, HelloAck, WorkerMessage};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

use crate::attempts::spec::Secret;
use crate::attempts::AttemptError;
use crate::lanes::{Dedup, Verdict};
use crate::AgentError;

/// Worker Protocol versions this agent speaks.
pub const PROTOCOL_VERSIONS: &[&str] = &["1.0"];
/// Heartbeat interval asked of workers, seconds.
pub const HEARTBEAT_INTERVAL_S: u32 = 5;
/// Largest message the agent accepts, bytes: the gateway's limit
/// ([`crate::session::MAX_MESSAGE_BYTES`]) minus room for the relay around it, so every worker
/// message the agent takes can also be sent on to the core.
pub const MAX_MESSAGE_BYTES: u32 = (4 << 20) - (64 << 10);
/// Longest the agent waits to hand one message to a worker whose queue is full. A worker that
/// stops reading for this long is disconnected, so a send never holds up lease expiry or a
/// cancel for longer.
pub const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Queues `msg` for the worker, giving up when the session is fenced.
async fn push(tx: &Outbox, fence: &CancellationToken, msg: Result<AgentMessage, Status>) -> bool {
    tokio::select! {
        () = fence.cancelled() => false,
        r = tx.send(msg) => r.is_ok(),
    }
}

/// What the server hands the attempt runner.
#[derive(Debug)]
pub enum Inbound {
    /// A worker connected (or reconnected) and passed the checks.
    Hello {
        /// Role id from the Hello.
        role_id: String,
        /// The Hello itself.
        hello: Hello,
    },
    /// A new sequenced message. Send `()` on `done` once it is handled; only then is it acked.
    Message {
        /// Role id.
        role_id: String,
        /// The message.
        msg: WorkerMessage,
        /// Completion signal.
        done: oneshot::Sender<()>,
    },
    /// An unsequenced message other than Hello (Heartbeat).
    Live {
        /// Role id.
        role_id: String,
        /// The message.
        msg: WorkerMessage,
    },
    /// The worker's stream ended.
    Disconnected {
        /// Role id.
        role_id: String,
    },
    /// A worker was refused, with the reason.
    Refused {
        /// Role id, if the Hello had one.
        role_id: String,
        /// Why, in plain words.
        reason: String,
        /// True when the refusal means the environment is wrong (no common protocol version).
        env_mismatch: bool,
    },
}

type Outbox = mpsc::Sender<Result<AgentMessage, Status>>;

struct Shared {
    attempt_id: String,
    nonce: Secret,
    /// The worker ids of this attempt: no other role gets a session.
    roles: Vec<String>,
    inbound: mpsc::Sender<Inbound>,
    dedup: Mutex<Dedup>,
    seq_file: PathBuf,
    /// The current session per role: id, outbox, and the token that fences it.
    outboxes: Mutex<HashMap<String, (u64, Outbox, CancellationToken)>>,
    /// One message at a time per role, across overlapping sessions.
    role_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// One seq-file write at a time, in order.
    save_lock: tokio::sync::Mutex<()>,
    sessions: AtomicU64,
    stop: CancellationToken,
}

impl Shared {
    /// The highest seq saved for `role` (the durable watermark).
    fn durable(&self, role: &str) -> u64 {
        lock(&self.dedup).accepted(&self.attempt_id, role)
    }

    fn role_lock(&self, role: &str) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(lock(&self.role_locks).entry(role.to_owned()).or_default())
    }

    fn is_current(&self, role: &str, session: u64) -> bool {
        lock(&self.outboxes)
            .get(role)
            .is_some_and(|(s, _, _)| *s == session)
    }

    /// Saves `seq` as accepted, then publishes it. False (nothing published) if the save failed.
    async fn persist(&self, role: &str, seq: u64) -> bool {
        let _one_writer = self.save_lock.lock().await;
        let snapshot = {
            let mut d = lock(&self.dedup).clone();
            d.record(&self.attempt_id, role, seq);
            serde_json::to_vec(&d)
        };
        let Ok(bytes) = snapshot else {
            return false;
        };
        let file = self.seq_file.clone();
        let saved =
            tokio::task::spawn_blocking(move || crate::state::atomic_write(&file, &bytes)).await;
        if !matches!(saved, Ok(Ok(()))) {
            return false;
        }
        lock(&self.dedup).record(&self.attempt_id, role, seq);
        true
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Constant-time comparison, so the nonce can't be guessed byte by byte.
fn same(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// The highest version both sides offer.
pub fn negotiate(offered: &[String]) -> Option<&'static str> {
    let num = |v: &str| {
        let (a, b) = v.split_once('.').unwrap_or((v, "0"));
        (a.parse::<u32>().unwrap_or(0), b.parse::<u32>().unwrap_or(0))
    };
    PROTOCOL_VERSIONS
        .iter()
        .copied()
        .filter(|v| offered.iter().any(|o| o == v))
        .max_by_key(|v| num(v))
}

#[derive(Clone)]
struct Svc(Arc<Shared>);

#[tonic::async_trait]
impl WorkerControlService for Svc {
    type SessionStream = ReceiverStream<Result<AgentMessage, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let shared = Arc::clone(&self.0);
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("the stream ended before Hello"))?;
        let Some(WorkerBody::Hello(hello)) = first.body else {
            return Err(Status::invalid_argument("the first message must be Hello"));
        };
        let role_id = hello.role_id.clone();
        if hello.attempt_id != shared.attempt_id || !same(&hello.nonce, shared.nonce.expose()) {
            let _ = shared
                .inbound
                .send(Inbound::Refused {
                    role_id,
                    reason: "a worker connected with the wrong attempt id or nonce".into(),
                    env_mismatch: false,
                })
                .await;
            return Err(Status::permission_denied("wrong attempt or nonce"));
        }
        if !shared.roles.contains(&role_id) {
            let _ = shared
                .inbound
                .send(Inbound::Refused {
                    role_id: role_id.clone(),
                    reason: format!(
                        "a worker connected as {role_id}, which this attempt does not run"
                    ),
                    env_mismatch: false,
                })
                .await;
            return Err(Status::permission_denied("unknown role"));
        }
        let (tx, rx) = mpsc::channel(64);
        let durable = shared.durable(&role_id);
        let chosen = negotiate(&hello.protocol_versions);
        let ack = AgentMessage {
            ack_seq: durable,
            body: Some(AgentBody::HelloAck(HelloAck {
                protocol_version: chosen.unwrap_or(PROTOCOL_VERSIONS[0]).to_owned(),
                heartbeat_interval_s: HEARTBEAT_INTERVAL_S,
                max_message_bytes: MAX_MESSAGE_BYTES,
                resume_from_seq: durable + 1,
            })),
        };
        let _ = tx.send(Ok(ack)).await;
        if chosen.is_none() {
            let _ = shared
                .inbound
                .send(Inbound::Refused {
                    role_id,
                    reason: format!(
                        "the worker speaks protocol {:?}; this agent speaks {:?}",
                        hello.protocol_versions, PROTOCOL_VERSIONS
                    ),
                    env_mismatch: true,
                })
                .await;
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        let session = shared.sessions.fetch_add(1, Ordering::Relaxed);
        let fence = shared.stop.child_token();
        // A reconnect replaces the role's session: the old reader stops before it reads more.
        if let Some((_, _, old)) =
            lock(&shared.outboxes).insert(role_id.clone(), (session, tx.clone(), fence.clone()))
        {
            old.cancel();
        }
        if shared
            .inbound
            .send(Inbound::Hello {
                role_id: role_id.clone(),
                hello,
            })
            .await
            .is_err()
        {
            return Err(Status::unavailable("the attempt is shutting down"));
        }
        tokio::spawn(read_loop(shared, role_id, session, fence, stream, tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn read_loop(
    shared: Arc<Shared>,
    role_id: String,
    session: u64,
    fence: CancellationToken,
    mut stream: Streaming<WorkerMessage>,
    tx: Outbox,
) {
    let role_lock = shared.role_lock(&role_id);
    loop {
        let msg = tokio::select! {
            () = fence.cancelled() => break,
            m = stream.message() => match m {
                Ok(Some(m)) => m,
                _ => break,
            },
        };
        // Check, hand over, save and ack one message at a time per role; a replaced session
        // handles nothing more.
        let _one_at_a_time = role_lock.lock().await;
        if !shared.is_current(&role_id, session) {
            break;
        }
        let verdict = lock(&shared.dedup).check(&shared.attempt_id, &role_id, msg.seq);
        match verdict {
            Verdict::Unsequenced => {
                if matches!(msg.body, Some(WorkerBody::Hello(_))) {
                    let twice = Status::failed_precondition("Hello sent twice on one stream");
                    let _ = push(&tx, &fence, Err(twice)).await;
                    break;
                }
                // Only heartbeats go without a seq: anything else must be numbered, saved and
                // deduplicated.
                if !matches!(msg.body, Some(WorkerBody::Heartbeat(_))) {
                    let unnumbered =
                        Status::failed_precondition("only Heartbeat may be sent with seq 0");
                    let _ = push(&tx, &fence, Err(unnumbered)).await;
                    break;
                }
                let live = Inbound::Live {
                    role_id: role_id.clone(),
                    msg,
                };
                if shared.inbound.send(live).await.is_err() {
                    break;
                }
            }
            Verdict::Duplicate => {
                let ack = AgentMessage {
                    ack_seq: shared.durable(&role_id),
                    body: None,
                };
                if !push(&tx, &fence, Ok(ack)).await {
                    break;
                }
            }
            Verdict::Gap { expected } => {
                let gap = Status::out_of_range(format!(
                    "expected seq {expected}, got {}; reconnect and replay",
                    msg.seq
                ));
                let _ = push(&tx, &fence, Err(gap)).await;
                break;
            }
            Verdict::New => {
                let seq = msg.seq;
                let (done, handled) = oneshot::channel();
                let m = Inbound::Message {
                    role_id: role_id.clone(),
                    msg,
                    done,
                };
                // The runner drops `done` when it could not save what the message changed.
                if shared.inbound.send(m).await.is_err() || handled.await.is_err() {
                    break;
                }
                // Not saved: no ack, so the worker keeps the message and replays it.
                if !shared.persist(&role_id, seq).await {
                    break;
                }
                let ack = AgentMessage {
                    ack_seq: seq,
                    body: None,
                };
                if !push(&tx, &fence, Ok(ack)).await {
                    break;
                }
            }
        }
    }
    let current = {
        let mut boxes = lock(&shared.outboxes);
        let is_current = boxes.get(&role_id).is_some_and(|(s, _, _)| *s == session);
        if is_current {
            boxes.remove(&role_id);
        }
        is_current
    };
    if current && !shared.stop.is_cancelled() {
        let _ = shared.inbound.send(Inbound::Disconnected { role_id }).await;
    }
}

/// A running WorkerControl server for one attempt.
pub struct WorkerServer {
    shared: Arc<Shared>,
    target: String,
    socket: Option<PathBuf>,
    task: tokio::task::JoinHandle<()>,
}

impl WorkerServer {
    /// Starts serving at `socket` (Unix) or on a free localhost port (elsewhere). `seq_file`
    /// keeps the accepted seqs; an existing one is loaded (re-adoption).
    pub async fn start(
        socket: &Path,
        attempt_id: &str,
        nonce: Secret,
        roles: Vec<String>,
        seq_file: PathBuf,
        inbound: mpsc::Sender<Inbound>,
    ) -> Result<Self, AttemptError> {
        let dedup = match std::fs::read(&seq_file) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| AttemptError::Socket {
                path: seq_file.clone(),
                reason: format!("the saved message numbers can't be read ({e})"),
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Dedup::default(),
            Err(e) => return Err(AgentError::state(&seq_file, e).into()),
        };
        let shared = Arc::new(Shared {
            attempt_id: attempt_id.to_owned(),
            nonce,
            roles,
            inbound,
            dedup: Mutex::new(dedup),
            seq_file,
            outboxes: Mutex::default(),
            role_locks: Mutex::default(),
            save_lock: tokio::sync::Mutex::new(()),
            sessions: AtomicU64::new(0),
            stop: CancellationToken::new(),
        });
        let svc = WorkerControlServiceServer::new(Svc(Arc::clone(&shared)))
            .max_decoding_message_size(MAX_MESSAGE_BYTES as usize)
            .max_encoding_message_size(MAX_MESSAGE_BYTES as usize);
        let router = tonic::transport::Server::builder().add_service(svc);
        let sock_err = |reason: String| AttemptError::Socket {
            path: socket.to_path_buf(),
            reason,
        };
        #[cfg(unix)]
        let (target, socket, task) = {
            if let Some(dir) = socket.parent() {
                use std::os::unix::fs::PermissionsExt;
                std::fs::create_dir_all(dir).map_err(|e| sock_err(e.to_string()))?;
                // The attempt's own folder: owner only, so no one else reaches its socket.
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                    .map_err(|e| sock_err(e.to_string()))?;
            }
            match std::fs::remove_file(socket) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(sock_err(e.to_string())),
            }
            let listener =
                tokio::net::UnixListener::bind(socket).map_err(|e| sock_err(e.to_string()))?;
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
                    .map_err(|e| sock_err(e.to_string()))?;
            }
            let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
            let task = tokio::spawn(async move {
                let _ = router.serve_with_incoming(incoming).await;
            });
            (
                socket.display().to_string(),
                Some(socket.to_path_buf()),
                task,
            )
        };
        #[cfg(not(unix))]
        let (target, socket, task) = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| sock_err(e.to_string()))?;
            let addr = listener.local_addr().map_err(|e| sock_err(e.to_string()))?;
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let task = tokio::spawn(async move {
                let _ = router.serve_with_incoming(incoming).await;
            });
            (addr.to_string(), None, task)
        };
        Ok(Self {
            shared,
            target,
            socket,
            task,
        })
    }

    /// What the worker passes as `--socket`.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// The highest seq accepted from `role_id`.
    pub fn durable(&self, role_id: &str) -> u64 {
        self.shared.durable(role_id)
    }

    /// Sends a message to the connected worker of `role_id`. False if it is not connected, or
    /// if its queue stayed full for [`SEND_TIMEOUT`]: that session is then dropped (the worker
    /// reconnects and gets everything pending again).
    pub async fn send(&self, role_id: &str, body: AgentBody) -> bool {
        let outbox = lock(&self.shared.outboxes)
            .get(role_id)
            .map(|(session, tx, fence)| (*session, tx.clone(), fence.clone()));
        let Some((session, tx, fence)) = outbox else {
            return false;
        };
        let msg = AgentMessage {
            ack_seq: self.durable(role_id),
            body: Some(body),
        };
        match tokio::time::timeout(SEND_TIMEOUT, push(&tx, &fence, Ok(msg))).await {
            Ok(sent) => sent,
            Err(_) => {
                let mut boxes = lock(&self.shared.outboxes);
                if boxes.get(role_id).is_some_and(|(s, _, _)| *s == session) {
                    boxes.remove(role_id);
                }
                fence.cancel();
                false
            }
        }
    }

    /// Stops serving and removes the socket file.
    pub async fn stop(self) {
        self.shared.stop.cancel();
        lock(&self.shared.outboxes).clear();
        self.task.abort();
        let _ = self.task.await;
        if let Some(s) = &self.socket {
            let _ = std::fs::remove_file(s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_worker_message_always_fits_a_gateway_message() {
        use mrl_proto::agent::v1::{agent_message, AgentMessage, WorkerRelay};
        // The relay around an empty worker message, with generous ids and the largest header.
        let around = prost::Message::encoded_len(&AgentMessage {
            seq: u64::MAX,
            ack_seq: u64::MAX,
            sent_at: Some(prost_types::Timestamp {
                seconds: i64::MIN,
                nanos: i32::MIN,
            }),
            body: Some(agent_message::Body::Worker(WorkerRelay {
                attempt: Some(mrl_proto::common::v1::AttemptRef {
                    job_id: "j".repeat(1024),
                    attempt_id: "a".repeat(1024),
                    fencing_epoch: u64::MAX,
                }),
                role_id: "r".repeat(1024),
                message: Some(Default::default()),
            })),
        });
        // + the worker message itself and the longer length prefixes it needs.
        let worst = around + MAX_MESSAGE_BYTES as usize + 2 * 10;
        assert!(worst <= crate::session::MAX_MESSAGE_BYTES, "{worst}");
    }

    #[test]
    fn version_negotiation() {
        assert_eq!(negotiate(&["1.0".into(), "0.9".into()]), Some("1.0"));
        assert_eq!(negotiate(&["2.0".into()]), None);
        assert!(same("abc", "abc") && !same("abc", "abd") && !same("abc", "ab"));
    }
}
