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
    #[cfg(unix)]
    if !target.contains(':') {
        let path = PathBuf::from(target);
        return tonic::transport::Endpoint::from_static("http://localhost")
            // HTTP/2's standard windows: a worker that stops reading backs up the agent at
            // once instead of buffering megabytes.
            .initial_stream_window_size(Some(65_535))
            .initial_connection_window_size(Some(65_535))
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
        .initial_stream_window_size(Some(65_535))
        .initial_connection_window_size(Some(65_535))
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

    /// The temporary folder holding the socket and `seq.json`.
    pub fn dir(&self) -> &std::path::Path {
        self.dir.path()
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

/// An attempt runner re-adopting a saved record, with sleeping stand-in processes as its
/// workers (Unix): connect [`FakeWorker`]s to drive it as a restarted agent would see them.
#[cfg(unix)]
pub struct RunnerFixture {
    /// The fake agent (its state folder holds the record).
    pub agent: super::FakeAgent,
    /// Events the runner sends.
    pub events: mrl_agent::attempts::AttemptEvents,
    /// Commands to the runner.
    pub control: mpsc::Sender<mrl_agent::attempts::runner::Control>,
    /// The runner task; its result is the attempt's outcome (`None`: detached).
    pub task: tokio::task::JoinHandle<Option<mrl_agent::attempts::exit::ExitDecision>>,
    /// The record as saved before re-adoption.
    pub record: mrl_agent::attempts::record::AttemptRecord,
    stand_ins: Vec<std::process::Child>,
}

#[cfg(unix)]
impl RunnerFixture {
    /// Saves a record for `spec` (one stand-in process per worker, phase RUNNING), lets `edit`
    /// change it, then re-adopts it.
    pub async fn readopt(
        spec: serde_json::Value,
        edit: impl FnOnce(&mut mrl_agent::attempts::record::AttemptRecord),
    ) -> Self {
        Self::readopt_with(spec, 256, edit).await
    }

    /// [`Self::readopt`] with an event channel of `capacity` (to test a slow reader).
    pub async fn readopt_with(
        spec: serde_json::Value,
        capacity: usize,
        edit: impl FnOnce(&mut mrl_agent::attempts::record::AttemptRecord),
    ) -> Self {
        use mrl_agent::attempts::record::{AttemptRecord, ProcIdentity, WorkerProc};
        use mrl_agent::attempts::runner::{socket_path, Runner, Shared};
        let agent = super::FakeAgent::start(0).unwrap_or_else(|e| panic!("{}", e.explain()));
        let spec = mrl_agent::attempts::AttemptSpec::from_json(&spec.to_string())
            .unwrap_or_else(|e| panic!("{}", e.explain()));
        let id = spec.attempt.attempt_id.clone();
        let mut stand_ins = Vec::new();
        let mut workers = Vec::new();
        for (role_id, _, primary) in spec.workers() {
            let child = std::process::Command::new("sleep")
                .arg("300")
                .spawn()
                .unwrap_or_else(|e| panic!("sleep: {e}"));
            let identity =
                ProcIdentity::of_pid(child.id()).unwrap_or_else(|| panic!("no identity"));
            stand_ins.push(child);
            workers.push(WorkerProc {
                role_id,
                primary,
                identity,
                cgroup: None,
            });
        }
        let mut record = AttemptRecord {
            socket: socket_path(agent.ctx.state.root(), &id),
            nonce: "n0nce".into(),
            spec,
            phase: mrl_proto::agent::v1::AttemptPhase::Running as i32,
            workers,
            prepared: Vec::new(),
            started: Vec::new(),
            last_step: 0,
            checkpoints: Default::default(),
            cancel_requested: false,
            exit: None,
            staged_paths: Default::default(),
            lease_deadline_unix_ms: None,
            stop_deadline_unix_ms: None,
            exit_report: None,
            requests: Vec::new(),
            uploads: Default::default(),
            results: Default::default(),
            adhoc_uploads: Default::default(),
        };
        edit(&mut record);
        let job = agent.ctx.state.job_dir(&id);
        std::fs::create_dir_all(job.join("staging")).unwrap_or_else(|e| panic!("{e}"));
        record
            .save(&job)
            .unwrap_or_else(|e| panic!("{}", e.explain()));
        let (tx, events) = mrl_agent::attempts::attempt_events(capacity);
        let envs =
            mrl_agent::envs::Envs::open(agent.ctx.state.root(), Arc::clone(&agent.ctx.cache))
                .unwrap_or_else(|e| panic!("{}", e.explain()));
        let shared = Arc::new(Shared {
            ctx: Arc::clone(&agent.ctx),
            envs: Arc::new(envs),
            events: tx,
            progress: None,
        });
        let (control, rx) = mpsc::channel(16);
        let runner = Runner::new(shared, record.clone(), Vec::new());
        let task = tokio::spawn(runner.readopt(rx));
        Self {
            agent,
            events,
            control,
            task,
            record,
            stand_ins,
        }
    }

    /// An agent crash and restart: the runner task is dropped mid-flight and a new runner
    /// re-adopts the record as saved on disk (same agent, stand-ins still running).
    pub async fn restart(&mut self) {
        use mrl_agent::attempts::runner::{Runner, Shared};
        self.task.abort();
        let _ = (&mut self.task).await;
        let record = self.saved();
        let (tx, events) = mrl_agent::attempts::attempt_events(256);
        let envs = mrl_agent::envs::Envs::open(
            self.agent.ctx.state.root(),
            Arc::clone(&self.agent.ctx.cache),
        )
        .unwrap_or_else(|e| panic!("{}", e.explain()));
        let shared = Arc::new(Shared {
            ctx: Arc::clone(&self.agent.ctx),
            envs: Arc::new(envs),
            events: tx,
            progress: None,
        });
        let (control, rx) = mpsc::channel(16);
        let runner = Runner::new(shared, record.clone(), Vec::new());
        self.task = tokio::spawn(runner.readopt(rx));
        self.events = events;
        self.control = control;
        self.record = record;
    }

    /// Connects a worker for `role_id` (retrying until the runner's server is up).
    pub async fn worker(&self, role_id: &str) -> (FakeWorker, HelloAck) {
        let target = self.record.socket.display().to_string();
        let mut last = None;
        for _ in 0..100 {
            let h = hello(
                &self.record.spec.attempt.attempt_id,
                &self.record.nonce,
                role_id,
            );
            match FakeWorker::connect(&target, h).await {
                Ok(w) => return w,
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the worker could not connect: {last:?}")
    }

    /// The attempt's staging folder.
    pub fn staging(&self) -> PathBuf {
        self.agent
            .ctx
            .state
            .job_dir(&self.record.spec.attempt.attempt_id)
            .join("staging")
    }

    /// The record as saved on disk now.
    pub fn saved(&self) -> mrl_agent::attempts::record::AttemptRecord {
        let job = self
            .agent
            .ctx
            .state
            .job_dir(&self.record.spec.attempt.attempt_id);
        mrl_agent::attempts::record::AttemptRecord::load(&job)
            .ok()
            .flatten()
            .unwrap_or_else(|| panic!("no saved record"))
    }

    /// Stops the stand-in processes (the "workers" exit).
    pub fn end_workers(&mut self) {
        for c in &mut self.stand_ins {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// The next event the runner sends that `pick` accepts.
    pub async fn event<T>(
        &mut self,
        mut pick: impl FnMut(mrl_agent::attempts::AttemptEvent) -> Option<T>,
    ) -> T {
        let found = tokio::time::timeout(RECV_TIMEOUT, async {
            while let Some(e) = self.events.recv().await {
                if let Some(t) = pick(e) {
                    return Some(t);
                }
            }
            None
        })
        .await;
        match found {
            Ok(Some(t)) => t,
            _ => panic!("the expected event did not come"),
        }
    }
}

#[cfg(unix)]
impl Drop for RunnerFixture {
    fn drop(&mut self) {
        self.end_workers();
    }
}

/// Agent messages with a body, read until `until` matches one (included).
pub async fn read_until(
    w: &mut FakeWorker,
    until: impl Fn(&AgentBody) -> bool,
) -> Result<Vec<AgentBody>, Status> {
    let mut out = Vec::new();
    loop {
        let m = w
            .recv()
            .await?
            .ok_or_else(|| Status::unavailable("the stream ended"))?;
        if let Some(b) = m.body {
            let done = until(&b);
            out.push(b);
            if done {
                return Ok(out);
            }
        }
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

    /// A worker that stops reading can't hold up the agent: once its queue is full, a send
    /// gives up within `SEND_TIMEOUT`, the session is dropped and later sends fail at once.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_worker_that_stops_reading_is_disconnected() {
        use mrl_agent::worker_server::SEND_TIMEOUT;
        let fx = ServerFixture::start("att_full", "n0nce").await;
        let (_w, _) = FakeWorker::connect(fx.target(), hello("att_full", "n0nce", "trainer"))
            .await
            .unwrap();
        let server = fx.server.as_ref().unwrap();
        let big = || {
            AgentBody::Prepare(mrl_proto::worker::v1::Prepare {
                job_spec_json: "x".repeat(256 << 10),
                ..Default::default()
            })
        };
        let started = std::time::Instant::now();
        let mut refused = false;
        for _ in 0..400 {
            if !server.send("trainer", big()).await {
                refused = true;
                break;
            }
        }
        assert!(refused, "the full queue was noticed");
        assert!(
            started.elapsed() < SEND_TIMEOUT * 3,
            "{:?}",
            started.elapsed()
        );
        let at = std::time::Instant::now();
        assert!(
            !server.send("trainer", big()).await,
            "the session was dropped"
        );
        assert!(at.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn a_replaced_session_handles_nothing_more() {
        let fx = ServerFixture::start("att_1", "n").await;
        let (mut old, _) = FakeWorker::connect(fx.target(), hello("att_1", "n", "trainer"))
            .await
            .unwrap();
        let (mut new, _) = FakeWorker::connect(fx.target(), hello("att_1", "n", "trainer"))
            .await
            .unwrap();
        // Both send seq 1: only the current session's copy is handled.
        old.send(progress(1)).await;
        let seq = new.send(progress(1)).await;
        new.wait_ack(seq).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let ones = fx
            .received()
            .iter()
            .filter(|r| matches!(r, Received::Message(_, 1)))
            .count();
        assert_eq!(ones, 1);
        assert!(
            old.recv().await.unwrap_or(None).is_none(),
            "the old stream ends"
        );
    }

    async fn twenty_steps(w: &mut FakeWorker) {
        for step in 1..=20 {
            let seq = w.send(progress(step)).await;
            w.wait_ack(seq).await.unwrap();
        }
    }

    #[tokio::test]
    async fn an_unsaved_seq_is_not_acked_or_skipped() {
        let mut fx = ServerFixture::start("att_1", "n").await;
        // A folder where seq.json should go makes every save fail.
        std::fs::create_dir(fx.dir().join("seq.json")).unwrap();
        let (mut w, _) = FakeWorker::connect(fx.target(), hello("att_1", "n", "trainer"))
            .await
            .unwrap();
        w.send(progress(1)).await;
        assert!(
            w.recv().await.unwrap_or(None).is_none(),
            "no ack; the stream ends"
        );
        std::fs::remove_dir(fx.dir().join("seq.json")).unwrap();
        let (_, ack) = FakeWorker::connect(fx.target(), hello("att_1", "n", "trainer"))
            .await
            .unwrap();
        assert_eq!(ack.resume_from_seq, 1, "the unsaved seq is asked for again");
        // Saves from two roles at once all reach the file.
        let mut ws = Vec::new();
        for role in ["a", "b"] {
            ws.push(
                FakeWorker::connect(fx.target(), hello("att_1", "n", role))
                    .await
                    .unwrap()
                    .0,
            );
        }
        let (mut a, mut b) = (ws.remove(0), ws.remove(0));
        tokio::join!(twenty_steps(&mut a), twenty_steps(&mut b));
        fx.restart().await;
        for role in ["a", "b"] {
            let (_, ack) = FakeWorker::connect(fx.target(), hello("att_1", "n", role))
                .await
                .unwrap();
            assert_eq!(ack.resume_from_seq, 21, "{role}");
        }
    }
}

#[cfg(all(test, unix))]
mod runner_tests {
    use super::*;
    use mrl_agent::attempts::record::{StagedCheckpoint, StagedFile, UploadPlan, UploadTarget};
    use mrl_agent::attempts::runner::Control;
    use mrl_agent::attempts::AttemptEvent;
    use mrl_proto::agent::v1::AttemptPhase;
    use mrl_proto::common::v1::FailureClass;
    use mrl_proto::worker::v1::{CheckpointRequest, CommitState, ExitKind, ExitReport, Prepared};
    use prost::Message;

    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn spec(id: &str, extra: serde_json::Value) -> serde_json::Value {
        let rt = format!("uv:sha256:{SHA}");
        let mut v = serde_json::json!({
            "attempt": {"job_id": "job_r", "attempt_id": id, "fencing_epoch": 1},
            "engine": "fake",
            "job_spec": {"job_kind": "EXPORT"},
            "runtimes": [{"runtime_ref": rt, "project": "/nowhere"}],
            "roles": [{"role_id": "trainer", "primary": true, "runtime_ref": rt}]
        });
        if let (Some(b), Some(m)) = (v.as_object_mut(), extra.as_object()) {
            b.extend(m.clone());
        }
        v
    }

    fn sha_hex(b: &[u8]) -> String {
        use sha2::Digest;
        sha2::Sha256::digest(b)
            .iter()
            .map(|x| format!("{x:02x}"))
            .collect()
    }

    fn now_ms() -> i64 {
        (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
    }

    fn staged(path: &str, sha: &str, size: u64, state: Option<i32>) -> StagedCheckpoint {
        StagedCheckpoint {
            step: 4,
            is_final: true,
            manifest_sha256: SHA.into(),
            files: vec![StagedFile {
                path: path.into(),
                size,
                sha256: sha.into(),
                role: "weights".into(),
            }],
            commit_requested: true,
            state,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_replays_start_acks_and_requests() {
        let request = mrl_proto::worker::v1::AgentMessage {
            ack_seq: 0,
            body: Some(AgentBody::CheckpointRequest(CheckpointRequest {
                request_id: "r1".into(),
                deadline_s: 30,
                reason: "guard".into(),
            })),
        };
        let fx = RunnerFixture::readopt(spec("att_rp", serde_json::json!({})), |r| {
            r.prepared = vec!["trainer".into()];
            r.started = vec!["trainer".into()];
            r.checkpoints.insert(
                "ckpt_1".into(),
                staged(
                    "checkpoints/ckpt_1/w.bin",
                    SHA,
                    1,
                    Some(CommitState::Committed as i32),
                ),
            );
            r.requests.push(request.encode_to_vec());
        })
        .await;
        for _ in 0..2 {
            let (mut w, _) = fx.worker("trainer").await;
            let got = read_until(&mut w, |b| matches!(b, AgentBody::CheckpointCommitAck(_)))
                .await
                .unwrap();
            assert!(got.iter().any(|b| matches!(b, AgentBody::Prepare(_))));
            assert!(got.iter().any(|b| matches!(b, AgentBody::Start(_))));
            assert!(got
                .iter()
                .any(|b| matches!(b, AgentBody::CheckpointRequest(r) if r.request_id == "r1")));
            assert!(got.iter().any(|b| matches!(b,
                AgentBody::CheckpointCommitAck(a) if a.checkpoint_id == "ckpt_1" && a.state == CommitState::Committed as i32)));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_keeps_the_lease_deadline() {
        let deadline = now_ms() + 3_600_000;
        let fx = RunnerFixture::readopt(spec("att_ls", serde_json::json!({"lease_s": 30})), |r| {
            r.lease_deadline_unix_ms = Some(deadline);
        })
        .await;
        let (mut w, _) = fx.worker("trainer").await;
        let got = read_until(&mut w, |b| matches!(b, AgentBody::LeaseDeadline(_)))
            .await
            .unwrap();
        let Some(AgentBody::LeaseDeadline(l)) = got.last() else {
            panic!("lease deadline")
        };
        assert_eq!(
            l.deadline.as_ref().unwrap().seconds,
            deadline.div_euclid(1000)
        );
        drop(fx);

        // Expired while the agent was away: fenced at once, never extended.
        let fx = RunnerFixture::readopt(spec("att_lx", serde_json::json!({"lease_s": 30})), |r| {
            r.lease_deadline_unix_ms = Some(now_ms() - 1000);
        })
        .await;
        let mut fx = fx;
        let d = tokio::time::timeout(Duration::from_secs(30), &mut fx.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(d.phase, AttemptPhase::SelfFenced as i32);
        assert_eq!(d.class(), Some(FailureClass::NodeLost));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_exit_report_is_saved_before_its_ack() {
        let mut fx = RunnerFixture::readopt(spec("att_ex", serde_json::json!({})), |_| {}).await;
        let (mut w, ack) = fx.worker("trainer").await;
        assert_eq!(ack.resume_from_seq, 1);
        let seq = w
            .send(Body::Exit(ExitReport {
                kind: ExitKind::Completed as i32,
                ..ExitReport::default()
            }))
            .await;
        w.wait_ack(seq).await.unwrap();
        assert!(fx.saved().exit_report.is_some(), "saved before the ack");
        // The agent goes away right after the ack, then the worker exits.
        let (tx, rx) = tokio::sync::oneshot::channel();
        fx.control.send(Control::Detach(tx)).await.unwrap();
        rx.await.unwrap();
        fx.end_workers();
        let (etx, _erx) = mrl_agent::attempts::attempt_events(64);
        let next =
            mrl_agent::attempts::Attempts::new(Arc::clone(&fx.agent.ctx), etx, None).unwrap();
        let statuses = next.readopt().await;
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].failure.is_none(), "{:?}", statuses[0]);
        assert_eq!(statuses[0].exit_kind, ExitKind::Completed as i32);
    }

    async fn sink() -> (String, Arc<Mutex<Vec<Vec<u8>>>>) {
        use axum::routing::put;
        let got: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let g = Arc::clone(&got);
        let app = axum::Router::new().route(
            "/up/{*p}",
            put(move |body: axum::body::Bytes| {
                let g = Arc::clone(&g);
                async move {
                    g.lock().unwrap().push(body.to_vec());
                    axum::http::StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/up"), got)
    }

    fn target(base: &str, path: &str, sha: &str, size: u64) -> UploadTarget {
        UploadTarget {
            local_path: path.into(),
            url: format!("{base}/{path}"),
            size,
            sha256: sha.into(),
            expires_unix: time::OffsetDateTime::now_utc().unix_timestamp() + 600,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn saved_tickets_finish_the_upload_after_a_restart() {
        let (base, got) = sink().await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_up", serde_json::json!({})), |r| {
            r.checkpoints
                .insert("ckpt_1".into(), staged(path, &sha, 7, None));
            r.uploads.insert(
                "ckpt_1".into(),
                UploadPlan {
                    command_id: "up-1".into(),
                    answered: false,
                    targets: vec![target(&base, path, &sha, 7)],
                },
            );
        })
        .await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"weights").unwrap();
        let (mut w, _) = fx.worker("trainer").await;
        let got_ack = read_until(&mut w, |b| matches!(b, AgentBody::CheckpointCommitAck(_)))
            .await
            .unwrap();
        assert!(matches!(got_ack.last(),
            Some(AgentBody::CheckpointCommitAck(a)) if a.state == CommitState::Committed as i32));
        let done = fx
            .event(|e| match e {
                AttemptEvent::CommandDone(r) => Some(r),
                _ => None,
            })
            .await;
        assert_eq!(done.command_id, "up-1");
        assert!(done.failure.is_none());
        assert_eq!(got.lock().unwrap().as_slice(), [b"weights".to_vec()]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tickets_for_other_content_are_rejected() {
        let (base, got) = sink().await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_hm", serde_json::json!({})), |r| {
            r.checkpoints
                .insert("ckpt_1".into(), staged(path, &sha, 7, None));
        })
        .await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"weights").unwrap();
        let (mut w, _) = fx.worker("trainer").await;
        fx.control
            .send(Control::Upload {
                command_id: "up-2".into(),
                checkpoint_id: "ckpt_1".into(),
                targets: vec![target(&base, path, SHA, 7)],
            })
            .await
            .unwrap();
        let acks = read_until(&mut w, |b| matches!(b, AgentBody::CheckpointCommitAck(_)))
            .await
            .unwrap();
        assert!(matches!(acks.last(),
            Some(AgentBody::CheckpointCommitAck(a)) if a.state == CommitState::Rejected as i32));
        let done = fx
            .event(|e| match e {
                AttemptEvent::CommandDone(r) => Some(r),
                _ => None,
            })
            .await;
        assert!(done.failure.is_some());
        assert!(got.lock().unwrap().is_empty(), "nothing was uploaded");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_barrier_waits_for_every_good_prepared() {
        let rt = format!("uv:sha256:{SHA}");
        let extra = serde_json::json!({
            "barrier": true,
            "roles": [
                {"role_id": "trainer", "primary": true, "runtime_ref": rt},
                {"role_id": "rollout", "runtime_ref": rt}
            ]
        });
        let mut fx = RunnerFixture::readopt(spec("att_bar", extra), |r| {
            r.phase = AttemptPhase::Preparing as i32;
        })
        .await;
        let (mut trainer, _) = fx.worker("trainer").await;
        let (mut rollout, _) = fx.worker("rollout").await;
        let prepared = || {
            Body::Prepared(Prepared {
                ok: true,
                ..Prepared::default()
            })
        };
        let seq = trainer.send(prepared()).await;
        trainer.wait_ack(seq).await.unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        fx.control.send(Control::Start(tx)).await.unwrap();
        assert!(
            rx.await.unwrap().is_err(),
            "one Prepared does not open the barrier"
        );
        let seq = rollout.send(prepared()).await;
        rollout.wait_ack(seq).await.unwrap();
        fx.event(|e| match e {
            AttemptEvent::Status(s) if s.phase == AttemptPhase::Prepared as i32 => Some(()),
            _ => None,
        })
        .await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        fx.control.send(Control::Start(tx)).await.unwrap();
        assert!(rx.await.unwrap().is_ok());
        for w in [&mut trainer, &mut rollout] {
            read_until(w, |b| matches!(b, AgentBody::Start(_)))
                .await
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_closed_event_channel_leaves_messages_unacked() {
        let mut fx = RunnerFixture::readopt(spec("att_cl", serde_json::json!({})), |_| {}).await;
        let (_, closed) = mrl_agent::attempts::attempt_events(1);
        drop(std::mem::replace(&mut fx.events, closed));
        let (mut w, _) = fx.worker("trainer").await;
        w.send(Body::Progress(mrl_proto::worker::v1::Progress {
            step: 1,
            ..Default::default()
        }))
        .await;
        // Not relayed, so not acked: the stream ends and the worker keeps the message.
        let mut acked = false;
        while let Ok(Some(m)) = w.recv().await {
            acked |= m.ack_seq >= 1;
        }
        assert!(!acked);
        let (_, ack) = fx.worker("trainer").await;
        assert_eq!(ack.resume_from_seq, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_event_channel_does_not_outlive_the_lease() {
        let fx = RunnerFixture::readopt_with(
            spec("att_full", serde_json::json!({"lease_s": 30})),
            1,
            |r| r.lease_deadline_unix_ms = Some(now_ms() + 2000),
        )
        .await;
        let (mut w, _) = fx.worker("trainer").await;
        for step in 1..=5 {
            w.send(Body::Progress(mrl_proto::worker::v1::Progress {
                step,
                ..Default::default()
            }))
            .await;
        }
        // Nobody reads the events; the lease still fences the attempt on time.
        let mut fx = fx;
        let d = tokio::time::timeout(Duration::from_secs(30), &mut fx.task)
            .await
            .expect("the runner was not stuck")
            .unwrap()
            .unwrap();
        assert_eq!(d.phase, AttemptPhase::SelfFenced as i32);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_tickets_retry_a_rejected_upload() {
        let (base, got) = sink().await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_rt", serde_json::json!({})), |r| {
            r.checkpoints
                .insert("ckpt_1".into(), staged(path, &sha, 7, None));
        })
        .await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"weights").unwrap();
        let (mut w, _) = fx.worker("trainer").await;
        let mut expired = target(&base, path, &sha, 7);
        expired.expires_unix = time::OffsetDateTime::now_utc().unix_timestamp() - 60;
        fx.control
            .send(Control::Upload {
                command_id: "up-old".into(),
                checkpoint_id: "ckpt_1".into(),
                targets: vec![expired],
            })
            .await
            .unwrap();
        let first = fx
            .event(|e| match e {
                AttemptEvent::CommandDone(r) => Some(r),
                _ => None,
            })
            .await;
        assert_eq!(first.command_id, "up-old");
        assert!(first.failure.is_some(), "the expired ticket fails");
        fx.control
            .send(Control::Upload {
                command_id: "up-new".into(),
                checkpoint_id: "ckpt_1".into(),
                targets: vec![target(&base, path, &sha, 7)],
            })
            .await
            .unwrap();
        let second = fx
            .event(|e| match e {
                AttemptEvent::CommandDone(r) => Some(r),
                _ => None,
            })
            .await;
        assert_eq!(second.command_id, "up-new");
        assert!(second.failure.is_none(), "{second:?}");
        assert_eq!(got.lock().unwrap().as_slice(), [b"weights".to_vec()]);
        let acks = read_until(&mut w, |b| {
            matches!(b, AgentBody::CheckpointCommitAck(a) if a.state == CommitState::Committed as i32)
        })
        .await
        .unwrap();
        assert!(!acks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancel_deadline_survives_a_restart() {
        // The cancel's grace ran out while the agent was away; the worker ignored it.
        let mut fx = RunnerFixture::readopt(spec("att_cd", serde_json::json!({})), |r| {
            r.cancel_requested = true;
            r.stop_deadline_unix_ms = Some(now_ms() - 1000);
        })
        .await;
        let d = tokio::time::timeout(Duration::from_secs(30), &mut fx.task)
            .await
            .expect("stopped at the saved deadline")
            .unwrap()
            .unwrap();
        assert_eq!(d.exit_kind, ExitKind::Cancelled as i32);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replicas_of_a_dotted_role_are_found() {
        let rt = format!("uv:sha256:{SHA}");
        let roles = serde_json::json!({"roles": [
            {"role_id": "train.main", "primary": true, "count": 2, "runtime_ref": rt}
        ]});
        let fx = RunnerFixture::readopt(spec("att_dot", roles), |_| {}).await;
        for id in ["train.main.0", "train.main.1"] {
            let (mut w, _) = fx.worker(id).await;
            read_until(&mut w, |b| matches!(b, AgentBody::Prepare(_)))
                .await
                .unwrap_or_else(|e| panic!("{id}: {e}"));
        }
    }

    /// Two connected workers that stop reading can't hold the attempt past its lease: sends
    /// are bounded by the deadline and go to both workers at once.
    #[tokio::test(flavor = "multi_thread")]
    async fn blocked_workers_do_not_delay_the_lease() {
        let rt = format!("uv:sha256:{SHA}");
        let extra = serde_json::json!({
            "roles": [{"role_id": "trainer", "primary": true, "count": 2, "runtime_ref": rt}],
            // A big Prepare fills each worker's connection at once.
            "job_spec": {"job_kind": "EXPORT", "blob": "x".repeat(300 << 10)},
        });
        let mut fx = RunnerFixture::readopt(spec("att_bl", extra), |r| {
            r.lease_deadline_unix_ms = Some(now_ms() + 60_000);
        })
        .await;
        let (_a, _) = fx.worker("trainer.0").await;
        let (_b, _) = fx.worker("trainer.1").await;
        let started = std::time::Instant::now();
        // Each renewal goes to both workers and sets the lease to 3 s from now; the queues
        // fill after a few dozen.
        let control = fx.control.clone();
        tokio::spawn(async move {
            for _ in 0..200 {
                let lease = Control::Lease {
                    duration_s: 3,
                    kill_budget_s: 0,
                };
                if control.send(lease).await.is_err() {
                    return;
                }
            }
            let _ = control
                .send(Control::Cancel {
                    request_id: "c".into(),
                    grace_s: 60,
                    reason: "test".into(),
                })
                .await;
        });
        let d = tokio::time::timeout(Duration::from_secs(60), &mut fx.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(d.phase, AttemptPhase::SelfFenced as i32, "{d:?}");
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "fenced late: {:?}",
            started.elapsed()
        );
    }

    /// A slow checkpoint upload still running when the lease expires is stopped before the
    /// attempt is cleaned up: the PUT never completes afterwards, and its command fails once.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_upload_crossing_lease_expiry_is_stopped() {
        let (base, got) = slow_sink(Duration::from_secs(6)).await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_lx", serde_json::json!({})), |r| {
            r.lease_deadline_unix_ms = Some(now_ms() + 2500);
            r.checkpoints
                .insert("ckpt_1".into(), staged(path, &sha, 7, None));
            r.uploads.insert(
                "ckpt_1".into(),
                UploadPlan {
                    command_id: "up-lx".into(),
                    answered: false,
                    targets: vec![target(&base, path, &sha, 7)],
                },
            );
        })
        .await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"weights").unwrap();
        let (_w, _) = fx.worker("trainer").await;
        let d = tokio::time::timeout(Duration::from_secs(60), &mut fx.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(d.phase, AttemptPhase::SelfFenced as i32);
        let r = done(&mut fx).await;
        assert_eq!(r.command_id, "up-lx");
        assert!(r.failure.is_some());
        // Past the sink's 6 s: the stopped PUT never lands.
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert!(
            got.lock().unwrap().is_empty(),
            "no PUT outlived the attempt"
        );
    }

    /// No lease and nobody reading events: a cancel still ends the attempt at its deadline
    /// (publishing STOPPING waits only until then).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancel_ends_on_time_with_a_full_event_channel() {
        let mut fx =
            RunnerFixture::readopt_with(spec("att_fe", serde_json::json!({})), 1, |_| {}).await;
        let (mut w, _) = fx.worker("trainer").await;
        // One relayed message fills the channel (capacity 1); its ack says it was relayed.
        let seq = w
            .send(Body::Progress(mrl_proto::worker::v1::Progress {
                step: 1,
                ..Default::default()
            }))
            .await;
        w.wait_ack(seq).await.unwrap();
        let started = std::time::Instant::now();
        fx.control
            .send(Control::Cancel {
                request_id: "c".into(),
                grace_s: 1,
                reason: "test".into(),
            })
            .await
            .unwrap();
        let d = tokio::time::timeout(Duration::from_secs(60), &mut fx.task)
            .await
            .expect("the cancel deadline was enforced")
            .unwrap();
        assert!(d.is_some());
        // Grace 1 s + 5 s margin, then the stand-in gets the kill grace (it can't be reaped
        // here, so it stays a zombie until then).
        let bound = Duration::from_secs(6 + fx.agent.ctx.config.exec.kill_grace_secs + 8);
        assert!(started.elapsed() < bound, "{:?}", started.elapsed());
    }

    /// A PUT sink that answers after `delay`.
    async fn slow_sink(delay: Duration) -> (String, Arc<Mutex<Vec<Vec<u8>>>>) {
        use axum::routing::put;
        let got: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let g = Arc::clone(&got);
        let app = axum::Router::new().route(
            "/up/{*p}",
            put(move |body: axum::body::Bytes| {
                let g = Arc::clone(&g);
                async move {
                    tokio::time::sleep(delay).await;
                    g.lock().unwrap().push(body.to_vec());
                    axum::http::StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/up"), got)
    }

    async fn done(fx: &mut RunnerFixture) -> mrl_proto::agent::v1::CommandResult {
        fx.event(|e| match e {
            AttemptEvent::CommandDone(r) => Some(r),
            _ => None,
        })
        .await
    }

    fn upload(id: &str, t: UploadTarget) -> Control {
        Control::Upload {
            command_id: id.into(),
            checkpoint_id: "ckpt_1".into(),
            targets: vec![t],
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_upload_command_gets_one_result() {
        let (base, got) = slow_sink(Duration::from_millis(1500)).await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_ov", serde_json::json!({})), |r| {
            r.checkpoints
                .insert("ckpt_1".into(), staged(path, &sha, 7, None));
        })
        .await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"weights").unwrap();
        let (_w, _) = fx.worker("trainer").await;
        fx.control
            .send(upload("up-a", target(&base, path, &sha, 7)))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        // While A's PUT is open: B is refused, a replay of A waits for A's result.
        fx.control
            .send(upload("up-b", target(&base, path, &sha, 7)))
            .await
            .unwrap();
        fx.control
            .send(upload("up-a", target(&base, path, &sha, 7)))
            .await
            .unwrap();
        let first = done(&mut fx).await;
        assert_eq!(first.command_id, "up-b");
        assert!(first.failure.is_some(), "refused while A runs");
        let second = done(&mut fx).await;
        assert_eq!(second.command_id, "up-a");
        assert!(second.failure.is_none());
        // After the commit, another command gets the saved outcome without a second upload.
        fx.control
            .send(upload("up-c", target(&base, path, &sha, 7)))
            .await
            .unwrap();
        let third = done(&mut fx).await;
        assert_eq!(third.command_id, "up-c");
        assert!(third.failure.is_none());
        assert_eq!(got.lock().unwrap().len(), 1, "uploaded once");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_commit_is_acknowledged_only_once_saved() {
        let (base, got) = sink().await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_du", serde_json::json!({})), |r| {
            r.checkpoints
                .insert("ckpt_1".into(), staged(path, &sha, 7, None));
            r.uploads.insert(
                "ckpt_1".into(),
                UploadPlan {
                    command_id: "up-1".into(),
                    answered: false,
                    // Expires while the outcome can't be saved.
                    targets: vec![UploadTarget {
                        expires_unix: time::OffsetDateTime::now_utc().unix_timestamp() + 2,
                        ..target(&base, path, &sha, 7)
                    }],
                },
            );
        })
        .await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"weights").unwrap();
        // Every save fails: a folder sits where the record goes.
        let job = fx.staging().parent().unwrap().to_path_buf();
        let record = job.join("attempt.json");
        std::fs::remove_file(&record).unwrap();
        std::fs::create_dir_all(record.join("blocker")).unwrap();
        let (mut w, _) = fx.worker("trainer").await;
        let early = tokio::time::timeout(
            Duration::from_millis(2500),
            read_until(&mut w, |b| matches!(b, AgentBody::CheckpointCommitAck(_))),
        )
        .await;
        assert!(early.is_err(), "no ack while the outcome can't be saved");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(got.lock().unwrap().len(), 1, "the PUT succeeded once");
        // Saving works again (the ticket has expired): the kept result is saved and only then
        // acknowledged, with no second upload.
        std::fs::remove_dir_all(&record).unwrap();
        fx.record.save(&job).unwrap();
        let acks = read_until(&mut w, |b| matches!(b, AgentBody::CheckpointCommitAck(_)))
            .await
            .unwrap();
        assert!(matches!(acks.last(),
            Some(AgentBody::CheckpointCommitAck(a)) if a.state == CommitState::Committed as i32));
        assert_eq!(
            fx.saved().checkpoints["ckpt_1"].state,
            Some(CommitState::Committed as i32)
        );
        let r = done(&mut fx).await;
        assert_eq!(r.command_id, "up-1");
        assert!(r.failure.is_none());
        assert_eq!(got.lock().unwrap().len(), 1, "uploaded exactly once");
    }

    /// An accepted upload command always ends with one result: when the worker commits a
    /// changed manifest (CORRUPT), and when the attempt ends before the upload.
    #[tokio::test(flavor = "multi_thread")]
    async fn accepted_upload_commands_end_with_one_result() {
        let (base, got) = sink().await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_cr", serde_json::json!({})), |r| {
            let mut ck = staged(path, &sha, 7, None);
            ck.commit_requested = false;
            r.checkpoints.insert("ckpt_1".into(), ck);
            let mut other = staged("checkpoints/ckpt_2/w.bin", &sha, 7, None);
            other.commit_requested = false;
            other.is_final = false;
            r.checkpoints.insert("ckpt_2".into(), other);
        })
        .await;
        let (mut w, _) = fx.worker("trainer").await;
        fx.control
            .send(upload("up-corrupt", target(&base, path, &sha, 7)))
            .await
            .unwrap();
        fx.control
            .send(Control::Upload {
                command_id: "up-late".into(),
                checkpoint_id: "ckpt_2".into(),
                targets: vec![target(&base, "checkpoints/ckpt_2/w.bin", &sha, 7)],
            })
            .await
            .unwrap();
        w.send(Body::CheckpointCommit(
            mrl_proto::worker::v1::CheckpointCommitRequest {
                checkpoint_id: "ckpt_1".into(),
                manifest_sha256: "f".repeat(64),
            },
        ))
        .await;
        let first = done(&mut fx).await;
        assert_eq!(first.command_id, "up-corrupt");
        assert!(first.failure.is_some(), "the corrupt commit is reported");
        // The worker exits without asking to commit ckpt_2: its command still gets a result.
        fx.end_workers();
        let second = done(&mut fx).await;
        assert_eq!(second.command_id, "up-late");
        assert!(second.failure.is_some());
        let rec = fx.saved();
        assert!(
            rec.uploads.values().all(|p| p.answered),
            "saved as answered"
        );
        assert!(got.lock().unwrap().is_empty(), "nothing was uploaded");
    }

    /// A result saved but not sent before a crash is sent after the restart: by the re-adopted
    /// runner, and for an attempt that had ended, by the next manager.
    #[tokio::test(flavor = "multi_thread")]
    async fn saved_results_are_sent_after_a_restart() {
        let mut fx = RunnerFixture::readopt(spec("att_rs", serde_json::json!({})), |r| {
            r.results.insert("up-9".into(), None);
        })
        .await;
        let r = done(&mut fx).await;
        assert_eq!(r.command_id, "up-9");
        assert!(r.failure.is_none());
        fx.end_workers();
        let _ = tokio::time::timeout(Duration::from_secs(30), &mut fx.task).await;
        let mut ended = fx.saved();
        assert!(ended.results.is_empty(), "removed once sent");
        // Ended, with a result still unsent (the agent died right after saving it).
        ended.results.insert("up-10".into(), Some("late".into()));
        let job = fx.staging().parent().unwrap().to_path_buf();
        ended.save(&job).unwrap();
        let (etx, mut erx) = mrl_agent::attempts::attempt_events(16);
        let next =
            mrl_agent::attempts::Attempts::new(Arc::clone(&fx.agent.ctx), etx, None).unwrap();
        next.readopt().await;
        let mut got = None;
        while let Ok(e) = erx.try_recv() {
            if let AttemptEvent::CommandDone(r) = e {
                got = Some(r);
            }
        }
        assert_eq!(got.unwrap().command_id, "up-10");
        assert!(fx.saved().results.is_empty());
    }

    /// The agent stopped right after saving a committed upload (outcome and result in one
    /// save, before either was sent): after the restart the worker gets COMMITTED and the
    /// command gets success, and teardown never reports that upload as failed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restart_after_a_commit_tells_both_sides_success() {
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_bd", serde_json::json!({})), |r| {
            r.checkpoints.insert(
                "ckpt_1".into(),
                staged(path, &sha, 7, Some(CommitState::Committed as i32)),
            );
            r.uploads.insert(
                "ckpt_1".into(),
                UploadPlan {
                    command_id: "up-1".into(),
                    answered: true,
                    targets: Vec::new(),
                },
            );
            r.results.insert("up-1".into(), None);
        })
        .await;
        let (mut w, _) = fx.worker("trainer").await;
        let acks = read_until(&mut w, |b| matches!(b, AgentBody::CheckpointCommitAck(_)))
            .await
            .unwrap();
        assert!(matches!(acks.last(),
            Some(AgentBody::CheckpointCommitAck(a)) if a.state == CommitState::Committed as i32));
        let r = done(&mut fx).await;
        assert_eq!(r.command_id, "up-1");
        assert!(r.failure.is_none());
        fx.end_workers();
        let _ = tokio::time::timeout(Duration::from_secs(30), &mut fx.task).await;
        while let Ok(e) = fx.events.try_recv() {
            if let AttemptEvent::CommandDone(r) = e {
                panic!("a second result: {r:?}");
            }
        }
    }

    /// An upload of other files is saved before its PUT starts; after an agent crash during
    /// the PUT, the restarted runner sends it again and the command gets its result.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_upload_survives_an_agent_crash() {
        let (base, got) = slow_sink(Duration::from_millis(1500)).await;
        let path = "logs/run.txt";
        let sha = sha_hex(b"log lines");
        let mut fx = RunnerFixture::readopt(spec("att_ac", serde_json::json!({})), |_| {}).await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"log lines").unwrap();
        fx.control
            .send(Control::Upload {
                command_id: "up-log".into(),
                checkpoint_id: String::new(),
                targets: vec![target(&base, path, &sha, 9)],
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            fx.saved().adhoc_uploads.contains_key("up-log"),
            "saved before the PUT"
        );
        // The crash, mid-PUT: the next runner has only what was saved.
        fx.restart().await;
        let r = done(&mut fx).await;
        assert_eq!(r.command_id, "up-log");
        assert!(r.failure.is_none(), "{r:?}");
        assert!(fx.saved().adhoc_uploads.is_empty());
        assert!(!got.lock().unwrap().is_empty());
    }

    /// The worker exits while an upload of other files (no checkpoint) is still sending: the
    /// attempt waits for it and its command gets exactly one result.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_upload_running_at_exit_gets_one_result() {
        let (base, got) = slow_sink(Duration::from_millis(2500)).await;
        let path = "logs/run.txt";
        let sha = sha_hex(b"log lines");
        let mut fx = RunnerFixture::readopt(spec("att_ad", serde_json::json!({})), |_| {}).await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"log lines").unwrap();
        let (_w, _) = fx.worker("trainer").await;
        fx.control
            .send(Control::Upload {
                command_id: "up-log".into(),
                checkpoint_id: String::new(),
                targets: vec![target(&base, path, &sha, 9)],
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        fx.end_workers();
        let r = done(&mut fx).await;
        assert_eq!(r.command_id, "up-log");
        assert!(r.failure.is_none(), "{r:?}");
        let _ = tokio::time::timeout(Duration::from_secs(30), &mut fx.task).await;
        let mut more = 0;
        while let Ok(e) = fx.events.try_recv() {
            more += usize::from(matches!(e, AttemptEvent::CommandDone(_)));
        }
        assert_eq!(more, 0, "one result only");
        assert_eq!(got.lock().unwrap().len(), 1);
    }

    /// Two commands for one checkpoint before the worker asks to commit: the first is answered
    /// as replaced, the second owns the upload and gets its result.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_waiting_upload_command_is_answered_when_replaced() {
        let (base, got) = sink().await;
        let path = "checkpoints/ckpt_1/w.bin";
        let sha = sha_hex(b"weights");
        let mut fx = RunnerFixture::readopt(spec("att_rp", serde_json::json!({})), |r| {
            let mut ck = staged(path, &sha, 7, None);
            ck.commit_requested = false;
            r.checkpoints.insert("ckpt_1".into(), ck);
        })
        .await;
        let file = fx.staging().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"weights").unwrap();
        let (mut w, _) = fx.worker("trainer").await;
        for id in ["up-a", "up-b"] {
            fx.control
                .send(upload(id, target(&base, path, &sha, 7)))
                .await
                .unwrap();
        }
        let first = done(&mut fx).await;
        assert_eq!(first.command_id, "up-a");
        assert!(first.failure.is_some(), "A was replaced by B");
        w.send(Body::CheckpointCommit(
            mrl_proto::worker::v1::CheckpointCommitRequest {
                checkpoint_id: "ckpt_1".into(),
                manifest_sha256: SHA.into(),
            },
        ))
        .await;
        let second = done(&mut fx).await;
        assert_eq!(second.command_id, "up-b");
        assert!(second.failure.is_none(), "{second:?}");
        assert_eq!(got.lock().unwrap().len(), 1);
    }
}
