//! Test helpers for the agent attempt runtime (P20).
//!
//! - [`FakeWorker`]: a Worker Protocol client in Rust (no Python), to drive an agent's
//!   [`WorkerServer`] message by message: Hello, sequenced messages, replays, gaps.
//! - [`ServerFixture`]: a [`WorkerServer`] on a temporary socket whose inbound messages are
//!   recorded and handled at once, as a runner would.
//!
//! ```no_run
//! # async fn demo() {
//! use mrl_testkit::agent::attempts::{hello, FakeWorker, ServerFixture};
//! use mrl_proto::worker::v1::{worker_message::Body, Progress};
//! let fx = ServerFixture::start("att_1", "nonce").await;
//! let (mut w, ack) = FakeWorker::connect(fx.target(), hello("att_1", "nonce", "trainer")).await.unwrap();
//! assert_eq!(ack.resume_from_seq, 1);
//! let seq = w.send(Body::Progress(Progress { step: 1, ..Default::default() })).await;
//! w.wait_ack(seq).await.unwrap();
//! # }
//! ```

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mrl_agent::attempts::spec::Secret;
use mrl_agent::worker_server::{Inbound, WorkerServer};
use mrl_proto::worker::v1::agent_message::Body as AgentBody;
use mrl_proto::worker::v1::worker_control_service_client::WorkerControlServiceClient;
use mrl_proto::worker::v1::worker_message::Body;
use mrl_proto::worker::v1::{AgentMessage, Hello, HelloAck, WorkerMessage};
use tokio::sync::mpsc;
use tonic::Status;

/// How long [`FakeWorker`] waits for an agent message.
pub const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// A Hello for `attempt_id` speaking protocol 1.0.
pub fn hello(attempt_id: &str, nonce: &str, role_id: &str) -> Hello {
    Hello {
        attempt_id: attempt_id.to_owned(),
        nonce: nonce.to_owned(),
        role_id: role_id.to_owned(),
        primary: true,
        protocol_versions: vec!["1.0".into()],
        ..Hello::default()
    }
}

/// A Worker Protocol client.
pub struct FakeWorker {
    tx: mpsc::Sender<WorkerMessage>,
    rx: tonic::Streaming<AgentMessage>,
    next_seq: u64,
}

async fn channel(target: &str) -> Result<tonic::transport::Channel, Status> {
    let unavailable = |e: tonic::transport::Error| Status::unavailable(e.to_string());
    let endpoint = tonic::transport::Endpoint::from_static("http://localhost");
    #[cfg(unix)]
    if !target.contains(':') {
        let path = PathBuf::from(target);
        return endpoint
            .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                let path = path.clone();
                async move {
                    let s = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
                }
            }))
            .await
            .map_err(unavailable);
    }
    tonic::transport::Endpoint::from_shared(format!("http://{target}"))
        .map_err(unavailable)?
        .connect()
        .await
        .map_err(unavailable)
}

impl FakeWorker {
    /// Connects to `target` (a socket path, or `host:port`), sends `hello` and returns the
    /// agent's HelloAck. Data messages then start at `HelloAck.resume_from_seq`.
    pub async fn connect(target: &str, hello: Hello) -> Result<(Self, HelloAck), Status> {
        let mut client = WorkerControlServiceClient::new(channel(target).await?);
        let (tx, rx) = mpsc::channel(64);
        tx.send(WorkerMessage {
            seq: 0,
            sent_at: None,
            body: Some(Body::Hello(hello)),
        })
        .await
        .map_err(|_| Status::internal("the request stream closed"))?;
        let mut rx = client
            .session(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await?
            .into_inner();
        let first = tokio::time::timeout(RECV_TIMEOUT, rx.message())
            .await
            .map_err(|_| Status::deadline_exceeded("no HelloAck"))??
            .ok_or_else(|| Status::unavailable("the stream ended before HelloAck"))?;
        let Some(AgentBody::HelloAck(ack)) = first.body else {
            return Err(Status::internal("the first agent message was not HelloAck"));
        };
        let next_seq = ack.resume_from_seq.max(1);
        Ok((Self { tx, rx, next_seq }, ack))
    }

    /// Sends `body` with the next seq and returns that seq.
    pub async fn send(&mut self, body: Body) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.send_raw(WorkerMessage {
            seq,
            sent_at: None,
            body: Some(body),
        })
        .await;
        seq
    }

    /// Sends a message exactly as given (replays, gaps, unsequenced heartbeats).
    pub async fn send_raw(&mut self, msg: WorkerMessage) {
        let _ = self.tx.send(msg).await;
    }

    /// The next agent message; `Ok(None)` when the stream ended, `Err` on a stream error.
    pub async fn recv(&mut self) -> Result<Option<AgentMessage>, Status> {
        tokio::time::timeout(RECV_TIMEOUT, self.rx.message())
            .await
            .map_err(|_| Status::deadline_exceeded("no agent message"))?
    }

    /// Reads until an ack covers `seq`; returns the messages with a body seen meanwhile.
    pub async fn wait_ack(&mut self, seq: u64) -> Result<Vec<AgentMessage>, Status> {
        let mut other = Vec::new();
        loop {
            let m = self
                .recv()
                .await?
                .ok_or_else(|| Status::unavailable("the stream ended"))?;
            let acked = m.ack_seq >= seq;
            if m.body.is_some() {
                other.push(m);
            }
            if acked {
                return Ok(other);
            }
        }
    }
}

/// What a [`ServerFixture`] received, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Received {
    /// A worker passed the Hello checks.
    Hello(String),
    /// A new sequenced message (role, seq).
    Message(String, u64),
    /// An unsequenced message (role).
    Live(String),
    /// A stream ended (role).
    Disconnected(String),
    /// A worker was refused (reason, environment mismatch).
    Refused(String, bool),
}

/// A [`WorkerServer`] on a temporary socket that handles every message at once.
pub struct ServerFixture {
    /// The server.
    pub server: Option<WorkerServer>,
    /// What it received.
    pub received: Arc<Mutex<Vec<Received>>>,
    dir: tempfile::TempDir,
    attempt_id: String,
    nonce: String,
}

impl ServerFixture {
    /// Starts a server for `attempt_id` with `nonce`.
    pub async fn start(attempt_id: &str, nonce: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("mrl")
            .tempdir_in(short_tmp())
            .unwrap_or_else(|e| panic!("no temporary folder: {e}"));
        let mut fx = Self {
            server: None,
            received: Arc::default(),
            dir,
            attempt_id: attempt_id.to_owned(),
            nonce: nonce.to_owned(),
        };
        fx.restart().await;
        fx
    }

    /// Stops the server (if running) and starts a new one on the same socket and seq file, as
    /// a restarted agent would.
    pub async fn restart(&mut self) {
        if let Some(s) = self.server.take() {
            s.stop().await;
        }
        let (tx, mut rx) = mpsc::channel(64);
        let received = Arc::clone(&self.received);
        tokio::spawn(async move {
            while let Some(inb) = rx.recv().await {
                let r = match inb {
                    Inbound::Hello { role_id, .. } => Received::Hello(role_id),
                    Inbound::Message { role_id, msg, done } => {
                        let _ = done.send(());
                        Received::Message(role_id, msg.seq)
                    }
                    Inbound::Live { role_id, .. } => Received::Live(role_id),
                    Inbound::Disconnected { role_id } => Received::Disconnected(role_id),
                    Inbound::Refused {
                        reason,
                        env_mismatch,
                        ..
                    } => Received::Refused(reason, env_mismatch),
                };
                received.lock().unwrap_or_else(|p| p.into_inner()).push(r);
            }
        });
        let server = WorkerServer::start(
            &self.dir.path().join("w.sock"),
            &self.attempt_id,
            Secret::new(self.nonce.clone()),
            self.dir.path().join("seq.json"),
            tx,
        )
        .await
        .unwrap_or_else(|e| panic!("the worker server did not start: {}", e.explain()));
        self.server = Some(server);
    }

    /// What workers pass as `--socket`.
    pub fn target(&self) -> &str {
        self.server.as_ref().map_or("", |s| s.target())
    }

    /// A copy of what was received so far.
    pub fn received(&self) -> Vec<Received> {
        self.received
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// A short temporary folder (Unix socket paths are limited to about 100 bytes).
fn short_tmp() -> PathBuf {
    if cfg!(unix) {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::worker::v1::{Heartbeat, Progress, Start};

    fn progress(step: u64) -> Body {
        Body::Progress(Progress {
            step,
            ..Progress::default()
        })
    }

    #[tokio::test]
    async fn wrong_nonce_or_attempt_is_refused() {
        let fx = ServerFixture::start("att_1", "secret").await;
        let e = FakeWorker::connect(fx.target(), hello("att_1", "wrong", "trainer"))
            .await
            .err()
            .expect("refused");
        assert_eq!(e.code(), tonic::Code::PermissionDenied);
        assert!(
            FakeWorker::connect(fx.target(), hello("att_2", "secret", "trainer"))
                .await
                .is_err()
        );
        assert!(matches!(fx.received()[0], Received::Refused(_, false)));
    }

    #[tokio::test]
    async fn no_common_version_answers_then_closes() {
        let fx = ServerFixture::start("att_1", "n").await;
        let mut h = hello("att_1", "n", "trainer");
        h.protocol_versions = vec!["2.0".into()];
        let (mut w, ack) = FakeWorker::connect(fx.target(), h).await.unwrap();
        assert_eq!(ack.protocol_version, "1.0");
        assert!(w.recv().await.unwrap().is_none(), "the stream closes");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(fx.received()[0], Received::Refused(_, true)));
    }

    #[tokio::test]
    async fn acks_dedups_and_resumes_after_restart() {
        let mut fx = ServerFixture::start("att_1", "n").await;
        let (mut w, ack) = FakeWorker::connect(fx.target(), hello("att_1", "n", "trainer"))
            .await
            .unwrap();
        assert_eq!(
            (ack.protocol_version.as_str(), ack.resume_from_seq),
            ("1.0", 1)
        );
        for step in 1..=3 {
            let seq = w.send(progress(step)).await;
            w.wait_ack(seq).await.unwrap();
        }
        // Heartbeats are unsequenced: delivered, never acked.
        w.send_raw(WorkerMessage {
            seq: 0,
            sent_at: None,
            body: Some(Body::Heartbeat(Heartbeat::default())),
        })
        .await;
        // A replay of seq 2 is acked again but not delivered twice.
        w.send_raw(WorkerMessage {
            seq: 2,
            sent_at: None,
            body: Some(progress(2)),
        })
        .await;
        let m = w.recv().await.unwrap().unwrap();
        assert_eq!(m.ack_seq, 3);
        // The agent can talk to the connected worker.
        let server = fx.server.as_ref().unwrap();
        assert!(server.send("trainer", AgentBody::Start(Start {})).await);
        assert!(!server.send("nobody", AgentBody::Start(Start {})).await);
        let m = w.recv().await.unwrap().unwrap();
        assert!(matches!(m.body, Some(AgentBody::Start(_))));
        let delivered: Vec<u64> = fx
            .received()
            .iter()
            .filter_map(|r| match r {
                Received::Message(_, s) => Some(*s),
                _ => None,
            })
            .collect();
        assert_eq!(delivered, [1, 2, 3]);
        assert!(fx.received().contains(&Received::Live("trainer".into())));

        // A restarted agent asks for a replay from the first message it did not finish.
        fx.restart().await;
        let (mut w2, ack) = FakeWorker::connect(fx.target(), hello("att_1", "n", "trainer"))
            .await
            .unwrap();
        assert_eq!(ack.resume_from_seq, 4);
        // A gap (seq 6 before 4 and 5) ends the stream so the worker replays.
        w2.send_raw(WorkerMessage {
            seq: 6,
            sent_at: None,
            body: Some(progress(6)),
        })
        .await;
        let e = w2.recv().await.expect_err("stream error");
        assert_eq!(e.code(), tonic::Code::OutOfRange);
    }
}
