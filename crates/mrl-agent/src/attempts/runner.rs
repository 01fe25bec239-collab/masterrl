//! One attempt from start to end: stage inputs, materialize environments, launch the workers
//! under the `worker` sandbox profile, serve their WorkerControl socket, relay their messages
//! into lanes, commit (upload) staged checkpoints, then decide how it ended and release its
//! pins. A runner started with [`Runner::readopt`] picks up live workers after an agent restart.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use mrl_proto::agent::v1::{AttemptPhase, AttemptStatus, CommandResult, WorkerRelay};
use mrl_proto::worker::v1::agent_message::Body as AgentBody;
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::{
    Allocation, CancelRequest, CheckpointCommitAck, CheckpointRequest, CommitState, ExitReport,
    LeaseDeadline, PauseRequest, Prepare, Start, UploadGrant, WorkerMessage,
};
use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::exit::{decide, ExitDecision, ExitEvidence};
use super::files::{self, Destination};
use super::record::{
    AttemptRecord, ProcIdentity, StagedCheckpoint, StagedFile, UploadPlan, UploadTarget, WorkerProc,
};
use super::spec::{safe_relative, RoleSpec, Secret};
use super::{AttemptError, AttemptEvent, AttemptEventSender};
use crate::envs::{Envs, Materialized, ProgressFn, WorkerCommand};
use crate::exec::{ExecSpec, Execution};
use crate::hw::{HwEvent, HwEventKind};
use crate::worker_server::{Inbound, WorkerServer, HEARTBEAT_INTERVAL_S};
use crate::{AgentContext, AgentError};

/// No message at all from the workers for this long (12 heartbeats) means they stalled.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(12 * HEARTBEAT_INTERVAL_S as u64);
/// Extra time after a cancel or pause deadline before the agent stops the workers itself.
const DEADLINE_MARGIN: Duration = Duration::from_secs(5);

/// A command for a running attempt.
#[derive(Debug)]
pub enum Control {
    /// Release the barrier.
    Start(oneshot::Sender<Result<(), AttemptError>>),
    /// Cancel with a grace period.
    Cancel {
        /// Request id the worker echoes.
        request_id: String,
        /// Seconds the worker gets.
        grace_s: u32,
        /// Why.
        reason: String,
    },
    /// Checkpoint, then exit PAUSED.
    Pause {
        /// Request id.
        request_id: String,
        /// Seconds the worker gets.
        deadline_s: u32,
        /// `PauseReason` number.
        reason: i32,
    },
    /// Emergency checkpoint, keep running.
    Checkpoint {
        /// Request id.
        request_id: String,
        /// Seconds the worker gets.
        deadline_s: u32,
        /// Why.
        reason: String,
    },
    /// Upload staged files (a checkpoint, or other outputs when `checkpoint_id` is empty).
    Upload {
        /// The command to report on when done.
        command_id: String,
        /// Checkpoint id, or empty.
        checkpoint_id: String,
        /// One authorized target per file.
        targets: Vec<UploadTarget>,
    },
    /// Lease renewed by a `LeaseGrant`: it runs `duration_s` from when the acknowledged
    /// heartbeat was sent, and the attempt is stopped `kill_budget_s` before it ends.
    Lease {
        /// The acknowledged heartbeat's seq; grants for older heartbeats are ignored.
        heartbeat_seq: u64,
        /// When that heartbeat was sent (this agent's monotonic clock).
        heartbeat_sent: std::time::Instant,
        /// Lease length.
        duration_s: u32,
        /// Safety margin.
        kill_budget_s: u32,
    },
    /// GPU events from the node.
    Hw(Vec<HwEvent>),
    /// The agent is shutting down: leave the workers running for the next agent.
    Detach(oneshot::Sender<()>),
    /// `inner`, then `saved` gets whether the attempt record holding its effect was saved: a
    /// command is answered as accepted only once it survives a restart.
    Acked {
        /// The control.
        inner: Box<Control>,
        /// Told after the save.
        saved: oneshot::Sender<bool>,
    },
}

/// Unwraps [`Control::Acked`].
fn split_ack(c: Option<Control>) -> (Option<Control>, Option<oneshot::Sender<bool>>) {
    match c {
        Some(Control::Acked { inner, saved }) => (Some(*inner), Some(saved)),
        c => (c, None),
    }
}

fn ack_with(ack: Option<oneshot::Sender<bool>>, saved: bool) {
    if let Some(a) = ack {
        let _ = a.send(saved);
    }
}

/// Why a launch stopped before its workers ran.
enum Early {
    Failed(AttemptError),
    Cancelled,
    /// Paused before any worker started (its deadline passed while staging).
    Paused,
    Fenced,
    Detached,
}

/// What happened to an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Sent,
    Closed,
    /// The lease or the stop deadline came first.
    DeadlinePassed,
}

/// The monotonic instant for a wall-clock time (Unix ms); now if it has passed.
fn instant_at(deadline_ms: i64) -> Instant {
    let left = u64::try_from(deadline_ms - now_ms()).unwrap_or(0);
    Instant::now() + Duration::from_millis(left)
}

/// Downloads the inputs and materializes every runtime the roles use.
async fn prepare(
    sh: Arc<Shared>,
    spec: super::AttemptSpec,
    job_dir: PathBuf,
) -> Result<(BTreeMap<String, String>, HashMap<String, Materialized>), AttemptError> {
    let id = spec.attempt.attempt_id.clone();
    let staged = files::stage_inputs(&sh.ctx, &spec, &inputs_dir(&job_dir), &id).await?;
    let log = job_dir.join("logs").join("environment.log");
    let progress = sh.progress.clone();
    let say = move |p: crate::envs::EnvProgress| {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
        {
            let _ = writeln!(f, "[{}] {}: {}", p.runtime_ref, p.stage, p.detail);
        }
        if let Some(cb) = &progress {
            cb(p);
        }
    };
    let mut envs = HashMap::new();
    for rt in &spec.runtimes {
        if spec.roles.iter().any(|r| r.runtime_ref == rt.runtime_ref) {
            let m = sh.envs.materialize(rt, &id, &say).await?;
            envs.insert(rt.runtime_ref.clone(), m);
        }
    }
    Ok((staged, envs))
}

enum StopMsg {
    Stop(Instant),
    Kill,
    Detach,
}

struct WorkerExit {
    role_id: String,
    code: Option<i32>,
    signal: Option<i32>,
    oom: bool,
    readopted: bool,
}

struct WorkerHandle {
    primary: bool,
    stop: mpsc::Sender<StopMsg>,
}

struct UploadDone {
    checkpoint_id: Option<String>,
    command_id: Option<String>,
    /// `Err((CommitState, reason))`.
    result: Result<(), (CommitState, String)>,
}

/// Everything a runner shares with the attempt manager.
pub struct Shared {
    /// Agent context.
    pub ctx: Arc<AgentContext>,
    /// Environments.
    pub envs: Arc<Envs>,
    /// Outgoing events.
    pub events: AttemptEventSender,
    /// Environment progress lines (local runs print them).
    pub progress: Option<Arc<ProgressFn>>,
}

/// One attempt's task state.
pub struct Runner {
    sh: Arc<Shared>,
    job_dir: PathBuf,
    rec: AttemptRecord,
    secrets: Vec<(String, Secret)>,
    server: Option<WorkerServer>,
    inbound_tx: mpsc::Sender<Inbound>,
    inbound_rx: mpsc::Receiver<Inbound>,
    exits_tx: mpsc::Sender<WorkerExit>,
    exits_rx: mpsc::Receiver<WorkerExit>,
    uploads_tx: mpsc::Sender<UploadDone>,
    uploads_rx: mpsc::Receiver<UploadDone>,
    workers: HashMap<String, WorkerHandle>,
    gpu_uuids: HashSet<String>,
    report: Option<ExitReport>,
    primary_exit: Option<WorkerExit>,
    secondary_oom: bool,
    env_mismatch: Option<String>,
    protocol_error: Option<String>,
    stalled: bool,
    self_fenced: bool,
    agent_stopped: bool,
    pause_requested: bool,
    lease: Option<Instant>,
    stop_at: Option<Instant>,
    /// When a stop deadline fired: from then on nothing waits (sends and events give up at
    /// once), so stopping and finishing can't hang on a stuck reader.
    stop_passed: Option<Instant>,
    last_seen: Instant,
    /// Checkpoints uploading now, or uploaded with an outcome not saved yet.
    in_flight: HashSet<String>,
    /// Finished uploads whose outcome could not be saved: saved again on the retry tick,
    /// never uploaded twice.
    unsaved: HashMap<String, UploadDone>,
    /// Uploads of files outside a checkpoint (`UploadArtifacts` without a checkpoint id),
    /// running now, by command id.
    adhoc: HashMap<String, tokio::task::JoinHandle<()>>,
    /// Checkpoint uploads running now, by checkpoint id.
    ck_tasks: HashMap<String, tokio::task::JoinHandle<()>>,
    /// Results sent and waiting for the gateway's receipt.
    in_transit: HashSet<String>,
    receipts_tx: mpsc::Sender<(String, bool)>,
    receipts_rx: mpsc::Receiver<(String, bool)>,
}

/// Folders of an attempt.
pub fn inputs_dir(job_dir: &Path) -> PathBuf {
    job_dir.join("inputs")
}
/// The writable staging folder.
pub fn staging_dir(job_dir: &Path) -> PathBuf {
    job_dir.join("staging")
}

/// Name of the link to the worker socket inside the worker's working folder.
pub const WORKER_SOCKET_LINK: &str = "w.sock";

/// Longest socket path used (Unix sockets allow 104 bytes on macOS, 108 on Linux).
#[cfg(unix)]
const MAX_SOCKET_PATH: usize = 100;

/// Environment variable every worker (and everything it starts) carries, so the janitor can
/// find this launch's processes even after they changed folder and session. Its value is
/// [`attempt_token`]: unique per launch (derived from the nonce), so two agents never match
/// each other's processes, even for the same attempt id.
pub const TOKEN_ENV: &str = "MRL_ATTEMPT_TOKEN";

/// The launch token for a nonce (not the nonce itself, which stays secret).
pub fn attempt_token(nonce: &str) -> String {
    let h = mrl_storage::sha256_hex(format!("mrl-attempt-token\0{nonce}").as_bytes());
    h[..32].to_owned()
}

/// Keeps an isolating sandbox's launcher (bubblewrap, which uses `--die-with-parent`) alive
/// when the agent dies, so the worker can be re-adopted. Forwards SIGTERM and exits with the
/// launcher's status.
#[cfg(unix)]
const KEEPER: &str = r#""$@" & c=$!
trap 'kill -TERM "$c" 2>/dev/null' TERM
while :; do wait "$c"; s=$?; kill -0 "$c" 2>/dev/null || exit "$s"; done"#;

/// Now, Unix milliseconds.
pub fn now_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

/// Socket path: `<state>/sock/<hash>/w.sock`, or `/tmp/mrl-<uid>/<hash>/w.sock` when the
/// state folder is too deep for a Unix socket path. One folder per attempt, so a container
/// gets only its own socket's folder (read-only) and no other attempt's.
pub fn socket_path(state_root: &Path, attempt_id: &str) -> PathBuf {
    let key = format!("{}\0{attempt_id}", state_root.display());
    let name = &mrl_storage::sha256_hex(key.as_bytes())[..16];
    let p = state_root.join("sock").join(name).join("w.sock");
    #[cfg(unix)]
    if p.as_os_str().len() > MAX_SOCKET_PATH {
        return PathBuf::from(format!("/tmp/mrl-{}", nix::unistd::getuid()))
            .join(name)
            .join("w.sock");
    }
    p
}

/// A new random nonce (32 bytes, hex).
pub fn new_nonce() -> Result<String, AttemptError> {
    let mut b = [0u8; 32];
    getrandom::fill(&mut b).map_err(|e| AttemptError::Socket {
        path: PathBuf::new(),
        reason: format!("no random numbers for the worker nonce ({e})"),
    })?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

fn now_ts() -> prost_types::Timestamp {
    let now = time::OffsetDateTime::now_utc();
    prost_types::Timestamp {
        seconds: now.unix_timestamp(),
        nanos: i32::try_from(now.nanosecond()).unwrap_or(0),
    }
}

impl Runner {
    /// A runner for a new attempt.
    pub fn new(sh: Arc<Shared>, rec: AttemptRecord, secrets: Vec<(String, Secret)>) -> Self {
        let job_dir = sh.ctx.state.job_dir(&rec.spec.attempt.attempt_id);
        let (inbound_tx, inbound_rx) = mpsc::channel(256);
        let (exits_tx, exits_rx) = mpsc::channel(16);
        let (uploads_tx, uploads_rx) = mpsc::channel(16);
        let (receipts_tx, receipts_rx) = mpsc::channel(64);
        Self {
            sh,
            job_dir,
            rec,
            secrets,
            server: None,
            inbound_tx,
            inbound_rx,
            exits_tx,
            exits_rx,
            uploads_tx,
            uploads_rx,
            workers: HashMap::new(),
            gpu_uuids: HashSet::new(),
            report: None,
            primary_exit: None,
            secondary_oom: false,
            env_mismatch: None,
            protocol_error: None,
            stalled: false,
            self_fenced: false,
            agent_stopped: false,
            pause_requested: false,
            lease: None,
            stop_at: None,
            stop_passed: None,
            last_seen: Instant::now(),
            in_flight: HashSet::new(),
            unsaved: HashMap::new(),
            adhoc: HashMap::new(),
            ck_tasks: HashMap::new(),
            in_transit: HashSet::new(),
            receipts_tx,
            receipts_rx,
        }
    }

    fn id(&self) -> &str {
        &self.rec.spec.attempt.attempt_id
    }

    fn staging(&self) -> PathBuf {
        staging_dir(&self.job_dir)
    }

    /// Runs a new attempt to its end. `None` means it was detached (agent shutdown).
    pub async fn run(mut self, mut control: mpsc::Receiver<Control>) -> Option<ExitDecision> {
        self.gpu_uuids = self.lookup_gpus();
        // The lease was fixed when the launch was accepted; staging counts against it.
        if let Some(at) = self.rec.lease_deadline_unix_ms {
            self.arm_lease(at);
        }
        match self.launch(&mut control).await {
            Ok(()) => self.main_loop(control).await,
            Err(early) => {
                for w in self.workers.values() {
                    let _ = w.stop.send(StopMsg::Kill).await;
                }
                match early {
                    Early::Failed(e) => Some(self.fail_early(e, &mut control).await),
                    Early::Detached => {
                        self.save();
                        None
                    }
                    Early::Paused => {
                        let d = ExitDecision {
                            phase: AttemptPhase::Exited as i32,
                            exit_kind: mrl_proto::worker::v1::ExitKind::Paused as i32,
                            exit_code: -1,
                            failure: None,
                        };
                        Some(self.finish_with(d, &mut control).await)
                    }
                    Early::Cancelled | Early::Fenced => {
                        let ev = ExitEvidence {
                            self_fenced: self.self_fenced,
                            cancel_requested: self.rec.cancel_requested,
                            agent_stopped: true,
                            ..ExitEvidence::default()
                        };
                        Some(self.finish_with(decide(&ev), &mut control).await)
                    }
                }
            }
        }
    }

    /// Re-attaches to live workers after an agent restart.
    pub async fn readopt(mut self, mut control: mpsc::Receiver<Control>) -> Option<ExitDecision> {
        self.gpu_uuids = self.lookup_gpus();
        if let Err(e) = self.start_server().await {
            return Some(self.fail_early(e, &mut control).await);
        }
        for w in self.rec.workers.clone() {
            let handle = spawn_watch(w.role_id.clone(), w.identity.clone(), self.exits_tx.clone());
            self.workers.insert(
                w.role_id.clone(),
                WorkerHandle {
                    primary: w.primary,
                    stop: handle,
                },
            );
        }
        self.report = self
            .rec
            .exit_report
            .as_deref()
            .and_then(|b| ExitReport::decode(b).ok());
        // The saved deadline, never a fresh one: an expired lease fences at once.
        if let Some(at) = self.rec.lease_deadline_unix_ms {
            self.arm_lease(at);
        }
        // A cancel or pause deadline set before the restart still holds.
        if let Some(at) = self.rec.stop_deadline_unix_ms {
            self.stop_at = Some(instant_at(at));
        }
        // Uploads accepted before the restart start again (a PUT can be repeated).
        for (command_id, targets) in self.rec.adhoc_uploads.clone() {
            self.start_adhoc(command_id, targets);
        }
        self.main_loop(control).await
    }

    /// Arms the lease timer for a wall-clock deadline (Unix ms).
    fn arm_lease(&mut self, deadline_ms: i64) {
        self.lease = Some(instant_at(deadline_ms));
    }

    /// True once the lease deadline has passed.
    fn lease_over(&self) -> bool {
        self.lease.is_some_and(|at| Instant::now() >= at)
    }

    /// Queues an event. Waits while the channel is full, but never past the next deadline
    /// (lease, cancel/pause stop), so a slow reader can't keep the attempt running past either.
    /// `Ok`: queued, with the receipt that resolves once the gateway saved it.
    async fn queue(&self, ev: AttemptEvent) -> Result<oneshot::Receiver<()>, Delivery> {
        let send = super::send_event(&self.sh.events, ev);
        let queued = match self.next_deadline() {
            // Nothing to enforce: wait for the reader.
            None => send.await,
            Some(at) => tokio::select! {
                biased;
                r = send => r,
                () = tokio::time::sleep_until(at) => return Err(Delivery::DeadlinePassed),
            },
        };
        queued.ok_or(Delivery::Closed)
    }

    /// Queues an event whose loss costs nothing (statuses: the next reconcile repeats them).
    async fn emit(&self, ev: AttemptEvent) -> Delivery {
        match self.queue(ev).await {
            Ok(_) => Delivery::Sent,
            Err(d) => d,
        }
    }

    /// Sets the absolute stop deadline after a cancel or pause request, saved for restarts.
    fn set_stop_deadline(&mut self, after: Duration) {
        let at = now_ms() + i64::try_from(after.as_millis()).unwrap_or(i64::MAX);
        self.rec.stop_deadline_unix_ms = Some(at);
        self.stop_at = Some(instant_at(at));
        self.save();
    }

    /// Records and arms a new lease deadline.
    /// Applies a lease grant: the deadline counts from the acknowledged heartbeat's send
    /// time (as the core does), never from when the grant arrived. A grant for a heartbeat
    /// older than the last applied one is ignored. True when applied.
    fn apply_lease(
        &mut self,
        heartbeat_seq: u64,
        heartbeat_sent: std::time::Instant,
        duration_s: u32,
        kill_budget_s: u32,
    ) -> bool {
        if self.self_fenced || heartbeat_seq <= self.rec.lease_heartbeat_seq {
            return false;
        }
        let ago = std::time::Instant::now().saturating_duration_since(heartbeat_sent);
        let ago = i64::try_from(ago.as_millis()).unwrap_or(i64::MAX);
        let sent_ms = now_ms().saturating_sub(ago);
        let hard = sent_ms + i64::from(duration_s) * 1000;
        self.rec.lease_heartbeat_seq = heartbeat_seq;
        self.rec.lease_hard_deadline_unix_ms = Some(hard);
        self.set_lease(hard - i64::from(kill_budget_s.min(duration_s)) * 1000);
        true
    }

    fn set_lease(&mut self, deadline_ms: i64) {
        self.rec.lease_deadline_unix_ms = Some(deadline_ms);
        self.arm_lease(deadline_ms);
        self.save();
    }

    /// Keeps a GPU event as exit evidence (saved with the record) when it is on one of this
    /// attempt's GPUs and new: one per GPU and kind, so a repeating event can't grow the
    /// record. True when kept.
    fn keep_hw(&mut self, e: HwEvent) -> bool {
        let new = self.gpu_uuids.contains(&e.uuid)
            && !self
                .rec
                .hw_events
                .iter()
                .any(|h| h.uuid == e.uuid && h.kind == e.kind);
        if new {
            self.rec.hw_events.push(e);
        }
        new
    }

    fn lookup_gpus(&self) -> HashSet<String> {
        let wanted: HashSet<u32> = self
            .rec
            .spec
            .roles
            .iter()
            .flat_map(|r| r.gpus.iter().copied())
            .collect();
        if wanted.is_empty() {
            return HashSet::new();
        }
        self.sh
            .ctx
            .inventory()
            .gpus
            .into_iter()
            .filter(|g| wanted.contains(&g.index))
            .map(|g| g.uuid)
            .collect()
    }

    async fn status(&self, decision: Option<&ExitDecision>) {
        let status = AttemptStatus {
            attempt: Some(self.rec.spec.attempt.to_proto()),
            phase: self.rec.phase,
            last_step: self.rec.last_step,
            exit_kind: decision.map_or(0, |d| d.exit_kind),
            exit_code: decision.map_or(0, |d| d.exit_code),
            failure: decision.and_then(|d| d.failure.clone()),
            time: Some(now_ts()),
        };
        let ev = AttemptEvent::Status(status);
        if decision.is_some() {
            // An outage can fill the queue until after the attempt's deadline. Keep the
            // terminal status waiting for room without holding up runner cleanup.
            let (receipt, _) = oneshot::channel();
            let envelope = super::Envelope { event: ev, receipt };
            if let Err(mpsc::error::TrySendError::Full(envelope)) =
                self.sh.events.try_send(envelope)
            {
                super::post(&self.sh.events, vec![(envelope.event, None)]);
            }
        } else {
            let _ = self.emit(ev).await;
        }
    }

    async fn set_phase(&mut self, phase: AttemptPhase) {
        if self.rec.phase != phase as i32 {
            self.rec.phase = phase as i32;
            self.save();
            self.status(None).await;
        }
    }

    /// Saves the record. False when it could not be saved: callers that acknowledge a worker
    /// message must then not acknowledge it, so the worker replays it.
    fn save(&mut self) -> bool {
        match self.rec.save(&self.job_dir) {
            Ok(()) => true,
            Err(_) => false,
        }
    }

    /// Stages inputs and environments (cancellable, bounded by the lease), then starts the
    /// server and the workers.
    async fn launch(&mut self, control: &mut mpsc::Receiver<Control>) -> Result<(), Early> {
        self.set_phase(AttemptPhase::Staging).await;
        std::fs::create_dir_all(self.job_dir.join("logs"))
            .map_err(|e| Early::Failed(AgentError::state(self.job_dir.join("logs"), e).into()))?;
        let work = prepare(
            Arc::clone(&self.sh),
            self.rec.spec.clone(),
            self.job_dir.clone(),
        );
        tokio::pin!(work);
        let far = || Instant::now() + Duration::from_secs(365 * 24 * 3600);
        let (staged, envs) = loop {
            let lease_at = self.lease.unwrap_or_else(far);
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(lease_at), if self.lease.is_some() => {
                    self.self_fenced = true;
                    return Err(Early::Fenced);
                }
                () = tokio::time::sleep_until(self.stop_at.unwrap_or_else(far)), if self.stop_at.is_some() => {
                    return Err(if self.pause_requested && !self.rec.cancel_requested { Early::Paused } else { Early::Cancelled });
                }
                c = control.recv() => { let (c, ack) = split_ack(c); match c {
                    Some(Control::Cancel { .. }) | None => {
                        self.rec.cancel_requested = true;
                        let saved = self.save();
                        ack_with(ack, saved);
                        return Err(Early::Cancelled);
                    }
                    Some(Control::Detach(done)) => {
                        ack_with(ack, true);
                        let _ = done.send(());
                        return Err(Early::Detached);
                    }
                    Some(Control::Start(reply)) => {
                        let _ = reply.send(Err(AttemptError::state(self.id(), "is still staging its inputs")));
                    }
                    Some(Control::Lease { heartbeat_seq, heartbeat_sent, duration_s, kill_budget_s }) => {
                        self.apply_lease(heartbeat_seq, heartbeat_sent, duration_s, kill_budget_s);
                    }
                    Some(Control::Pause { request_id, deadline_s, reason }) => {
                        // Sent to the workers when they connect (they pause at once); its
                        // deadline holds from now, as after the launch.
                        if self.remember(&AgentBody::Pause(PauseRequest { request_id, deadline_s, reason })).is_err() {
                            ack_with(ack, false);
                            continue;
                        }
                        self.pause_requested = true;
                        self.set_stop_deadline(Duration::from_secs(deadline_s.into()) + DEADLINE_MARGIN);
                    }
                    Some(Control::Checkpoint { request_id, deadline_s, reason }) => {
                        if self.remember(&AgentBody::CheckpointRequest(CheckpointRequest {
                            request_id, deadline_s, reason,
                        })).is_err() {
                            ack_with(ack, false);
                            continue;
                        }
                    }
                    Some(Control::Upload { command_id, checkpoint_id, targets }) => {
                        self.accept_plan(checkpoint_id, command_id, targets).await;
                    }
                    Some(Control::Hw(events)) => {
                        for e in events {
                            self.keep_hw(e);
                        }
                    }
                    Some(Control::Acked { .. }) => {}
                }
                let saved = self.save();
                ack_with(ack, saved); },
                r = &mut work => break r.map_err(Early::Failed)?,
            }
        };
        if self.lease_over() {
            self.self_fenced = true;
            return Err(Early::Fenced);
        }
        self.rec.staged_paths = staged;
        self.save();
        self.set_phase(AttemptPhase::Preparing).await;
        // Publishing the phase may have waited for a slow reader up to the lease.
        if self.lease_over() {
            self.self_fenced = true;
            return Err(Early::Fenced);
        }
        self.start_server().await.map_err(Early::Failed)?;
        let spec = self.rec.spec.clone();
        for (role_id, role, primary) in spec.workers() {
            if self.lease_over() {
                self.self_fenced = true;
                return Err(Early::Fenced);
            }
            let mat = envs.get(&role.runtime_ref).ok_or_else(|| {
                Early::Failed(AttemptError::Spec(format!(
                    "runtime {} was not prepared",
                    role.runtime_ref
                )))
            })?;
            self.start_worker(&role_id, role, primary, mat)
                .await
                .map_err(Early::Failed)?;
        }
        Ok(())
    }

    async fn start_server(&mut self) -> Result<(), AttemptError> {
        let server = WorkerServer::start(
            &self.rec.socket,
            self.id(),
            Secret::new(self.rec.nonce.clone()),
            self.rec
                .spec
                .workers()
                .into_iter()
                .map(|(id, _, _)| id)
                .collect(),
            self.job_dir.join(super::record::SEQ_FILE),
            self.inbound_tx.clone(),
        )
        .await?;
        self.server = Some(server);
        Ok(())
    }

    /// What the worker passes as `--socket`. On Unix this is `w.sock` in the worker's working
    /// folder, a link to the real socket: gRPC turns a socket path into the HTTP/2 `:authority`
    /// by percent-encoding its slashes, which the agent's HTTP/2 server rejects as malformed; a
    /// bare file name needs no encoding.
    fn worker_target(&self) -> Result<String, AttemptError> {
        #[cfg(unix)]
        {
            let work = self.job_dir.join("work");
            std::fs::create_dir_all(&work).map_err(|e| AgentError::state(&work, e))?;
            let link = work.join(WORKER_SOCKET_LINK);
            let _ = std::fs::remove_file(&link);
            std::os::unix::fs::symlink(&self.rec.socket, &link)
                .map_err(|e| AgentError::state(&link, e))?;
            Ok(WORKER_SOCKET_LINK.to_owned())
        }
        #[cfg(not(unix))]
        Ok(self
            .server
            .as_ref()
            .map(|s| s.target().to_owned())
            .unwrap_or_default())
    }

    /// Names the worker's cgroup (`job-<name>`) and container (`mrl-<name>`): derived from the
    /// launch token, so no other launch (even of the same attempt id) can share them.
    fn exec_name(&self, role_id: &str) -> String {
        format!("{}-{role_id}", &attempt_token(&self.rec.nonce)[..16])
    }

    async fn start_worker(
        &mut self,
        role_id: &str,
        role: &RoleSpec,
        primary: bool,
        mat: &Materialized,
    ) -> Result<(), AttemptError> {
        let target = self.worker_target()?;
        let mut worker_args: Vec<String> = [
            "--engine",
            &self.rec.spec.engine,
            "--socket",
            &target,
            "--attempt-id",
            self.id(),
            "--role-id",
            role_id,
        ]
        .map(String::from)
        .into();
        if !primary {
            worker_args.push("--secondary".into());
        }
        let mut env = BTreeMap::new();
        env.insert("MRL_WORKER_NONCE".to_owned(), self.rec.nonce.clone());
        env.insert(
            "MRL_ENVIRONMENT_DIGEST".to_owned(),
            role.runtime_ref.clone(),
        );
        env.insert("MRL_ROLE_ID".to_owned(), role_id.to_owned());
        env.insert(TOKEN_ENV.to_owned(), attempt_token(&self.rec.nonce));
        for (k, v) in &self.secrets {
            env.insert(k.clone(), v.expose().to_owned());
        }
        let cfg = &self.sh.ctx.config;
        let (spec, container) = match &mat.worker {
            WorkerCommand::Process { program } => {
                let program = program.display().to_string();
                // Output goes straight to files in staging, not through the agent, so the
                // worker keeps running (and can be re-adopted) if the agent goes away.
                #[cfg(unix)]
                let (prog, args) = {
                    let mut a = vec![
                        "-c".to_owned(),
                        r#"mkdir -p "$MRL_STAGING_DIR/logs" && exec "$0" "$@" >>"$MRL_STAGING_DIR/logs/$MRL_ROLE_ID.stdout.log" 2>>"$MRL_STAGING_DIR/logs/$MRL_ROLE_ID.stderr.log""#.to_owned(),
                        program,
                    ];
                    a.extend(worker_args);
                    ("/bin/sh".to_owned(), a)
                };
                #[cfg(not(unix))]
                let (prog, args) = (program, worker_args);
                let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
                let mut spec =
                    ExecSpec::new(&self.exec_name(role_id), &prog, &arg_refs, &self.job_dir);
                spec.log_dir = self.job_dir.join("logs").join(role_id);
                spec.env = env;
                spec.gpus = role.gpus.iter().map(u32::to_string).collect();
                spec.limits = self.rec.spec.limits;
                let mut allowed = cfg.allowed_paths.clone();
                allowed.extend(mat.read_paths.iter().cloned());
                allowed.push(inputs_dir(&self.job_dir));
                if let Some(dir) = self.rec.socket.parent() {
                    allowed.push(dir.to_path_buf());
                }
                let backend = crate::sandbox::select(&cfg.sandbox)?;
                let profile = crate::sandbox::profile("worker")?;
                let wrapped = backend.wrap(&profile, spec, &allowed)?;
                #[cfg(unix)]
                let wrapped = if backend.isolates() {
                    keep_alive(wrapped)
                } else {
                    wrapped
                };
                (wrapped, None)
            }
            WorkerCommand::Container { image } => {
                let name = format!("mrl-{}", self.exec_name(role_id));
                let args = docker_run_args(
                    &name,
                    image,
                    &self.job_dir,
                    self.rec.socket.parent(),
                    role,
                    &self.rec.spec.limits,
                    env.keys().map(String::as_str),
                    &worker_args,
                );
                let docker = crate::envs::which("docker")
                    .ok_or_else(|| AttemptError::DockerMissing("no `docker` on PATH".into()))?;
                let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
                let mut spec = ExecSpec::new(
                    &self.exec_name(role_id),
                    &docker.display().to_string(),
                    &arg_refs,
                    &self.job_dir,
                );
                spec.log_dir = self.job_dir.join("logs").join(role_id);
                spec.env = env;
                (spec, Some(name))
            }
        };
        std::fs::create_dir_all(inputs_dir(&self.job_dir))
            .map_err(|e| AgentError::state(inputs_dir(&self.job_dir), e))?;
        // A container is recorded (by name, carrying the launch token) before it starts: if
        // the agent dies right after `docker run`, the next one still finds and stops it.
        if let Some(name) = &container {
            self.rec.workers.push(WorkerProc {
                role_id: role_id.to_owned(),
                primary,
                identity: ProcIdentity::Container {
                    name: name.clone(),
                    id: None,
                },
                cgroup: None,
            });
            if !self.save() {
                return Err(AttemptError::state(
                    self.id(),
                    "could not save the worker before starting it",
                ));
            }
        }
        let exec = self.sh.ctx.executor.spawn(spec).await?;
        if container.is_none() {
            if let Some(identity) = exec.pid().and_then(ProcIdentity::of_pid) {
                self.rec.workers.push(WorkerProc {
                    role_id: role_id.to_owned(),
                    primary,
                    identity,
                    cgroup: None,
                });
            }
        }
        let watched = container.map(|name| (name, attempt_token(&self.rec.nonce)));
        let stop = spawn_exec(role_id.to_owned(), exec, watched, self.exits_tx.clone());
        self.workers
            .insert(role_id.to_owned(), WorkerHandle { primary, stop });
        self.save();
        Ok(())
    }

    async fn fail_early(
        &mut self,
        e: AttemptError,
        control: &mut mpsc::Receiver<Control>,
    ) -> ExitDecision {
        let decision = ExitDecision {
            phase: AttemptPhase::Exited as i32,
            exit_kind: mrl_proto::worker::v1::ExitKind::Failed as i32,
            exit_code: -1,
            failure: Some(e.to_failure()),
        };
        self.finish_with(decision, control).await
    }

    async fn main_loop(&mut self, mut control: mpsc::Receiver<Control>) -> Option<ExitDecision> {
        let mut stall_tick = tokio::time::interval(Duration::from_secs(1));
        self.last_seen = Instant::now();
        let far = || Instant::now() + Duration::from_secs(365 * 24 * 3600);
        loop {
            let lease_at = self.lease.unwrap_or_else(far);
            let stop_at = self.stop_at.unwrap_or_else(far);
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(lease_at), if self.lease.is_some() && !self.self_fenced => {
                    self.self_fenced = true;
                    self.abort_transfers().await;
                    self.stop_all().await;
                }
                () = tokio::time::sleep_until(stop_at), if self.stop_at.is_some() => {
                    self.stop_passed = self.stop_at.take();
                    self.stop_all().await;
                }
                Some(inb) = self.inbound_rx.recv() => self.on_inbound(inb).await,
                Some(exit) = self.exits_rx.recv() => {
                    if self.on_exit(exit).await {
                        break;
                    }
                }
                c = control.recv() => match c {
                    Some(Control::Detach(done)) => {
                        self.detach().await;
                        let _ = done.send(());
                        return None;
                    }
                    Some(c) => {
                        let (c, ack) = split_ack(Some(c));
                        let handled = match c {
                            Some(c) => self.on_control(c).await,
                            None => true,
                        };
                        let saved = handled && self.save();
                        ack_with(ack, saved);
                    }
                    // The manager is gone without a detach: stop the attempt.
                    None => self.stop_all().await,
                },
                Some(u) = self.uploads_rx.recv() => self.on_upload(u).await,
                Some((id, saved)) = self.receipts_rx.recv() => self.on_receipt(&id, saved),
                _ = stall_tick.tick() => {
                    self.retry_commits().await;
                    if !self.stalled
                        && self.rec.phase == AttemptPhase::Running as i32
                        && self.last_seen.elapsed() > STALL_TIMEOUT
                    {
                        self.stalled = true;
                        self.stop_all().await;
                    }
                }
            }
        }
        while let Ok(inb) = self.inbound_rx.try_recv() {
            self.on_inbound(inb).await;
        }
        Some(self.finish(&mut control).await)
    }

    async fn stop_all(&mut self) {
        self.agent_stopped = true;
        // Never past the lease's hard end: by then the core may run the attempt elsewhere.
        let mut grace = Duration::from_secs(self.sh.ctx.config.exec.kill_grace_secs);
        if let Some(hard) = self.rec.lease_hard_deadline_unix_ms {
            let left = u64::try_from(hard.saturating_sub(now_ms())).unwrap_or(0);
            grace = grace.min(Duration::from_millis(left));
        }
        for w in self.workers.values() {
            let _ = w.stop.send(StopMsg::Stop(Instant::now() + grace)).await;
        }
        self.set_phase(AttemptPhase::Stopping).await;
    }

    async fn detach(&mut self) {
        // The next runner resumes these uploads from the saved record.
        self.abort_transfers().await;
        for w in self.workers.values() {
            let _ = w.stop.send(StopMsg::Detach).await;
        }
        if let Some(s) = self.server.take() {
            s.stop().await;
        }
        self.save();
    }

    /// The earliest deadline in force (lease, cancel/pause stop, or a stop deadline that has
    /// already passed), if any.
    fn next_deadline(&self) -> Option<Instant> {
        [self.lease, self.stop_at, self.stop_passed]
            .into_iter()
            .flatten()
            .min()
    }

    /// Sends to one worker. Never waits past the next deadline (nor the server's own
    /// `SEND_TIMEOUT`), so the main loop gets back to lease expiry and stop deadlines on time.
    async fn send(&self, role_id: &str, body: AgentBody) -> bool {
        let Some(s) = &self.server else {
            return false;
        };
        match self.next_deadline() {
            Some(at) => tokio::time::timeout_at(at, s.send(role_id, body))
                .await
                .unwrap_or(false),
            None => s.send(role_id, body).await,
        }
    }

    /// Sends to every worker at once, so blocked workers cost one wait, not one each.
    async fn send_all(&self, body: AgentBody) {
        let sends = self
            .workers
            .keys()
            .map(|role| self.send(role, body.clone()));
        futures::future::join_all(sends).await;
    }

    fn primary_role(&self) -> Option<String> {
        self.workers
            .iter()
            .find(|(_, w)| w.primary)
            .map(|(r, _)| r.clone())
    }

    fn lease_deadline(&self) -> Option<AgentBody> {
        let ms = self.rec.lease_deadline_unix_ms?;
        Some(AgentBody::LeaseDeadline(LeaseDeadline {
            deadline: Some(prost_types::Timestamp {
                seconds: ms.div_euclid(1000),
                nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).unwrap_or(0),
            }),
            fencing_epoch: self.rec.spec.attempt.fencing_epoch,
        }))
    }

    fn prepare_msg(&self, role: &RoleSpec) -> AgentBody {
        let spec = &self.rec.spec;
        AgentBody::Prepare(Prepare {
            job_spec_json: spec.job_spec.to_string(),
            staged_paths: self
                .rec
                .staged_paths
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            allocation: Some(Allocation {
                gpu_indices: role.gpus.clone(),
                world_size: spec.placement.world_size,
                node_rank: spec.placement.node_rank,
                role_endpoints: spec.placement.role_endpoints.clone().into_iter().collect(),
            }),
            dry_run: spec.dry_run,
            smoke_steps: spec.smoke_steps,
            reward_socket: String::new(),
            staging_dir: self.staging().display().to_string(),
        })
    }

    /// Relays a worker message. `Ok`: queued, with the receipt that resolves once the
    /// gateway saved it (a message with no lane needs none and resolves at once).
    async fn relay(
        &self,
        role_id: &str,
        msg: WorkerMessage,
    ) -> Result<oneshot::Receiver<()>, Delivery> {
        let Some(lane) = crate::lanes::lane_for(&msg) else {
            let (done, saved) = oneshot::channel();
            let _ = done.send(());
            return Ok(saved);
        };
        let relay = WorkerRelay {
            attempt: Some(self.rec.spec.attempt.to_proto()),
            role_id: role_id.to_owned(),
            message: Some(msg),
        };
        self.queue(AttemptEvent::Relay { lane, relay }).await
    }

    /// The role of a worker id, from the exact worker list the launch expanded.
    fn role_spec(&self, worker_id: &str) -> Option<RoleSpec> {
        self.rec
            .spec
            .workers()
            .into_iter()
            .find(|(id, _, _)| id == worker_id)
            .map(|(_, r, _)| r.clone())
    }

    async fn on_inbound(&mut self, inb: Inbound) {
        // Only traffic from admitted workers counts as a sign of life.
        if matches!(
            inb,
            Inbound::Hello { .. } | Inbound::Message { .. } | Inbound::Live { .. }
        ) {
            self.last_seen = Instant::now();
        }
        match inb {
            Inbound::Hello { role_id, hello } => {
                let Some(role) = self.role_spec(&role_id) else {
                    self.protocol_error =
                        Some(format!("a worker connected as unknown role {role_id}"));
                    return;
                };
                if !hello.environment_digest.is_empty()
                    && hello.environment_digest != role.runtime_ref
                {
                    self.env_mismatch = Some(format!(
                        "The worker runs in environment {} but the attempt needs {}.",
                        hello.environment_digest, role.runtime_ref
                    ));
                    self.stop_all().await;
                    return;
                }
                self.note_cgroup(&role_id).await;
                // Everything the worker may be waiting for is sent again; it ignores repeats.
                self.send(&role_id, self.prepare_msg(&role)).await;
                if self.rec.started.contains(&role_id) {
                    self.send(&role_id, AgentBody::Start(Start {})).await;
                }
                self.resend_pending(&role_id).await;
                if let Some(l) = self.lease_deadline() {
                    self.send(&role_id, l).await;
                }
            }
            Inbound::Message { role_id, msg, done } => {
                // Not saved: `done` is dropped, so the message is not acked and comes again.
                // Acked only once the gateway saved the relay too: until then (or if it never
                // does) the worker keeps the message and replays it after a reconnect. The
                // wait runs beside the runner, which goes on with its loop.
                if self.on_message(&role_id, &msg).await {
                    if let Ok(saved) = self.relay(&role_id, msg).await {
                        tokio::spawn(async move {
                            if saved.await.is_ok() {
                                let _ = done.send(());
                            }
                        });
                    }
                }
            }
            Inbound::Live { role_id, msg } => {
                if let Some(WorkerBody::Heartbeat(hb)) = &msg.body {
                    self.rec.last_step = self.rec.last_step.max(hb.last_step);
                }
                let _ = self.relay(&role_id, msg).await;
            }
            Inbound::Disconnected { .. } => {}
            Inbound::Refused {
                reason,
                env_mismatch,
                ..
            } => {
                if env_mismatch {
                    self.env_mismatch = Some(reason);
                    self.stop_all().await;
                }
            }
        }
    }

    async fn resend_pending(&mut self, role_id: &str) {
        for raw in self.rec.requests.clone() {
            if let Some(body) = mrl_proto::worker::v1::AgentMessage::decode(raw.as_slice())
                .ok()
                .and_then(|m| m.body)
            {
                self.send(role_id, body).await;
            }
        }
        let is_primary = self.workers.get(role_id).is_some_and(|w| w.primary);
        if !is_primary {
            return;
        }
        for (id, ck) in self.rec.checkpoints.clone() {
            match ck.state {
                None => {
                    self.send(role_id, grant(&id, &ck)).await;
                    self.try_commit(&id).await;
                }
                Some(state) if ck.commit_requested => {
                    let state = CommitState::try_from(state).unwrap_or(CommitState::Rejected);
                    self.send(role_id, commit_ack(&id, state, "")).await;
                }
                Some(_) => {}
            }
        }
    }

    /// Linux: remembers the worker's own cgroup once it is in it (at its first Hello).
    /// Once a worker has connected: notes its job cgroup (Linux), or binds its container to
    /// the container id after checking the launch token, so later signals never reach a
    /// container that took over the name.
    async fn note_cgroup(&mut self, role_id: &str) {
        let token = attempt_token(&self.rec.nonce);
        let Some(w) = self.rec.workers.iter_mut().find(|w| w.role_id == role_id) else {
            return;
        };
        match &w.identity {
            ProcIdentity::Pid { pid, .. } if w.cgroup.is_none() => {
                w.cgroup = crate::janitor::job_cgroup_of(*pid);
                if w.cgroup.is_some() {
                    self.save();
                }
            }
            ProcIdentity::Container { id: None, .. } => {
                let identity = w.identity.clone();
                let bound = identity.verified(&token).await;
                if let Ok(Some(bound)) = bound {
                    if let Some(w) = self.rec.workers.iter_mut().find(|w| w.role_id == role_id) {
                        w.identity = bound;
                    }
                    self.save();
                }
            }
            _ => {}
        }
    }

    /// Takes the tickets of an `UploadArtifacts` command for a checkpoint that is not uploading.
    /// Every command gets exactly one result: an earlier command still waiting for this
    /// checkpoint (no outcome yet) is answered now as replaced, and the new one owns the upload.
    async fn accept_plan(
        &mut self,
        checkpoint_id: String,
        command_id: String,
        targets: Vec<UploadTarget>,
    ) {
        let undecided = self
            .rec
            .checkpoints
            .get(&checkpoint_id)
            .is_none_or(|c| c.state.is_none());
        let replaced = self
            .rec
            .uploads
            .get(&checkpoint_id)
            .map(|p| p.command_id.clone())
            .filter(|old| undecided && *old != command_id);
        self.store_plan(checkpoint_id.clone(), command_id.clone(), targets);
        if let Some(old) = replaced {
            let why =
                format!("replaced by upload command {command_id} for checkpoint {checkpoint_id}");
            self.command_done(old, Some(why)).await;
        }
    }

    /// Saves the tickets for a checkpoint, so a restarted agent still has them and the command
    /// to answer.
    fn store_plan(
        &mut self,
        checkpoint_id: String,
        command_id: String,
        targets: Vec<UploadTarget>,
    ) {
        let plan = self
            .rec
            .uploads
            .entry(checkpoint_id)
            .or_insert_with(|| UploadPlan {
                command_id: command_id.clone(),
                targets: Vec::new(),
                answered: false,
            });
        if plan.command_id != command_id {
            plan.answered = false;
        }
        plan.command_id = command_id;
        for t in targets {
            plan.targets.retain(|x| x.local_path != t.local_path);
            plan.targets.push(t);
        }
        self.save();
    }

    /// Keeps a control request so a reconnecting worker gets it again.
    /// Returns whether the request was saved and is new to this worker session.
    fn remember(&mut self, body: &AgentBody) -> Result<bool, ()> {
        let id = request_id(body);
        if id.is_some()
            && self.kept_requests().any(|b| {
                std::mem::discriminant(&b) == std::mem::discriminant(body) && request_id(&b) == id
            })
        {
            return Ok(false);
        }
        let msg = mrl_proto::worker::v1::AgentMessage {
            ack_seq: 0,
            body: Some(body.clone()),
        };
        self.rec.requests.push(msg.encode_to_vec());
        if self.save() {
            Ok(true)
        } else {
            self.rec.requests.pop();
            Err(())
        }
    }

    /// The control requests kept for reconnecting workers.
    fn kept_requests(&self) -> impl Iterator<Item = AgentBody> + '_ {
        self.rec.requests.iter().filter_map(|raw| {
            mrl_proto::worker::v1::AgentMessage::decode(raw.as_slice())
                .ok()
                .and_then(|m| m.body)
        })
    }

    /// Handles one sequenced worker message. False when what it changed could not be saved.
    async fn on_message(&mut self, role_id: &str, msg: &WorkerMessage) -> bool {
        let primary = self.workers.get(role_id).is_some_and(|w| w.primary);
        match &msg.body {
            Some(WorkerBody::Prepared(p)) => {
                if !p.ok || self.rec.spec.dry_run {
                    return true;
                }
                if !self.rec.prepared.iter().any(|r| r == role_id) {
                    self.rec.prepared.push(role_id.to_owned());
                }
                if self.rec.spec.barrier && !self.rec.started.iter().any(|r| r == role_id) {
                    // The barrier opens only when every worker reported a good Prepared.
                    if self.rec.prepared.len() == self.rec.spec.workers().len() {
                        self.set_phase(AttemptPhase::Prepared).await;
                    }
                    return self.save();
                }
                if !self.rec.started.iter().any(|r| r == role_id) {
                    self.rec.started.push(role_id.to_owned());
                }
                if !self.save() {
                    return false;
                }
                self.send(role_id, AgentBody::Start(Start {})).await;
                self.set_phase(AttemptPhase::Running).await;
                true
            }
            Some(WorkerBody::Progress(p)) => {
                self.rec.last_step = self.rec.last_step.max(p.step);
                true
            }
            Some(WorkerBody::CheckpointStaged(s)) if primary => {
                if let Some(bad) = s.files.iter().find(|f| safe_relative(&f.path).is_err()) {
                    self.protocol_error = Some(format!(
                        "the worker staged a file outside its staging folder: {}",
                        bad.path
                    ));
                    self.stop_all().await;
                    return true;
                }
                // A replay of a checkpoint already known keeps its saved state: the grant (or
                // the decided outcome) is sent again, nothing is replaced.
                if let Some(known) = self.rec.checkpoints.get(&s.checkpoint_id).cloned() {
                    match known.state.and_then(|st| CommitState::try_from(st).ok()) {
                        Some(st) => {
                            self.send(role_id, commit_ack(&s.checkpoint_id, st, ""))
                                .await;
                        }
                        None => {
                            self.send(role_id, grant(&s.checkpoint_id, &known)).await;
                        }
                    }
                    return true;
                }
                // The checkpoint is bound to one manifest file whose content hash is the
                // digest the commit names; the upload then verifies that file like any other.
                let manifests: Vec<_> = s.files.iter().filter(|f| f.role == "manifest").collect();
                let invalid = if manifests.len() != 1 {
                    Some(format!(
                        "it lists {} manifest files; exactly one is needed",
                        manifests.len()
                    ))
                } else if !super::spec::is_sha256(&s.manifest_sha256)
                    || manifests[0].sha256 != s.manifest_sha256
                {
                    Some("its manifest file does not have the manifest digest".to_owned())
                } else {
                    None
                };
                if let Some(why) = invalid {
                    let ck = StagedCheckpoint {
                        step: s.step,
                        is_final: s.is_final,
                        manifest_sha256: s.manifest_sha256.clone(),
                        files: Vec::new(),
                        commit_requested: false,
                        state: Some(CommitState::Corrupt as i32),
                    };
                    self.rec.checkpoints.insert(s.checkpoint_id.clone(), ck);
                    if !self.save() {
                        return false;
                    }
                    let reason = format!("checkpoint {} can't be used: {why}", s.checkpoint_id);
                    self.send(
                        role_id,
                        commit_ack(&s.checkpoint_id, CommitState::Corrupt, &reason),
                    )
                    .await;
                    self.answer_plan(&s.checkpoint_id, Some(reason)).await;
                    return true;
                }
                let ck = StagedCheckpoint {
                    step: s.step,
                    is_final: s.is_final,
                    manifest_sha256: s.manifest_sha256.clone(),
                    files: s
                        .files
                        .iter()
                        .map(|f| StagedFile {
                            path: f.path.clone(),
                            size: f.size,
                            sha256: f.sha256.clone(),
                            role: f.role.clone(),
                        })
                        .collect(),
                    commit_requested: false,
                    state: None,
                };
                self.rec
                    .checkpoints
                    .insert(s.checkpoint_id.clone(), ck.clone());
                if !self.save() {
                    return false;
                }
                self.send(role_id, grant(&s.checkpoint_id, &ck)).await;
                true
            }
            Some(WorkerBody::CheckpointCommit(c)) if primary => {
                let Some(ck) = self.rec.checkpoints.get_mut(&c.checkpoint_id) else {
                    let ack = commit_ack(
                        &c.checkpoint_id,
                        CommitState::Rejected,
                        "unknown checkpoint",
                    );
                    self.send(role_id, ack).await;
                    return true;
                };
                ck.commit_requested = true;
                let corrupt = ck.manifest_sha256 != c.manifest_sha256;
                if corrupt {
                    ck.state = Some(CommitState::Corrupt as i32);
                }
                if !self.save() {
                    return false;
                }
                if corrupt {
                    let why = "the manifest changed after staging";
                    let ack = commit_ack(&c.checkpoint_id, CommitState::Corrupt, why);
                    self.send(role_id, ack).await;
                    self.answer_plan(&c.checkpoint_id, Some(why.to_owned()))
                        .await;
                } else {
                    self.try_commit(&c.checkpoint_id).await;
                }
                true
            }
            Some(WorkerBody::Exit(r)) if primary => {
                // Saved before the ack, so an agent restart still knows how the worker ended.
                self.rec.exit_report = Some(r.encode_to_vec());
                self.rec.requests.retain(|raw| {
                    !mrl_proto::worker::v1::AgentMessage::decode(raw.as_slice())
                        .ok()
                        .and_then(|m| m.body)
                        .is_some_and(|body| {
                            matches!(body, AgentBody::CheckpointRequest(req)
                            if r.acknowledged_requests.contains(&req.request_id))
                        })
                });
                self.report = Some(r.clone());
                self.save()
            }
            _ => true,
        }
    }

    /// Starts uploading a checkpoint once the worker asked to commit it and every file has an
    /// authorized target whose size and sha256 match what the worker staged.
    async fn try_commit(&mut self, id: &str) {
        if self.self_fenced || self.lease_over() {
            return;
        }
        let Some(ck) = self.rec.checkpoints.get(id).cloned() else {
            return;
        };
        if !ck.commit_requested || ck.state.is_some() || self.in_flight.contains(id) {
            return;
        }
        let dests: Vec<(StagedFile, Destination)> = match &self.rec.spec.upload_dir {
            Some(dir) => ck
                .files
                .iter()
                .map(|f| (f.clone(), Destination::Dir(dir.clone())))
                .collect(),
            None => {
                let Some(plan) = self.rec.uploads.get(id).cloned() else {
                    return; // waiting for UploadArtifacts
                };
                let mut out = Vec::new();
                for f in &ck.files {
                    let Some(t) = plan.targets.iter().find(|t| t.local_path == f.path) else {
                        return; // waiting for a ticket for every file
                    };
                    let refused = if t.sha256 != f.sha256 || t.size != f.size {
                        Some(format!(
                            "the core expects different content for {} than the worker staged",
                            f.path
                        ))
                    } else {
                        t.ticket().err().map(|e| e.explain())
                    };
                    if let Some(reason) = refused {
                        let done = UploadDone {
                            checkpoint_id: Some(id.to_owned()),
                            command_id: Some(plan.command_id.clone()),
                            result: Err((CommitState::Rejected, reason)),
                        };
                        self.on_upload(done).await;
                        return;
                    }
                    if let Ok(ticket) = t.ticket() {
                        out.push((f.clone(), Destination::Ticket(ticket)));
                    }
                }
                out
            }
        };
        self.in_flight.insert(id.to_owned());
        let ctx = Arc::clone(&self.sh.ctx);
        let staging = self.staging();
        let tx = self.uploads_tx.clone();
        let done = UploadDone {
            checkpoint_id: Some(id.to_owned()),
            command_id: self.rec.uploads.get(id).map(|p| p.command_id.clone()),
            result: Ok(()),
        };
        let task = tokio::spawn(async move {
            let mut result = Ok(());
            for (f, d) in dests {
                if let Err(e) = files::send(&ctx, &staging, &f.path, f.size, &f.sha256, &d).await {
                    let state = match &e {
                        AttemptError::Agent(AgentError::Storage(
                            mrl_storage::StorageError::ChecksumMismatch { .. },
                        )) => CommitState::Corrupt,
                        _ => CommitState::Rejected,
                    };
                    result = Err((state, e.explain()));
                    break;
                }
            }
            let _ = tx.send(UploadDone { result, ..done }).await;
        });
        self.ck_tasks.insert(id.to_owned(), task);
    }

    async fn on_upload(&mut self, u: UploadDone) {
        let failure = u.result.as_ref().err().map(|(_, reason)| reason.clone());
        if let Some(id) = &u.checkpoint_id {
            self.in_flight.remove(id);
            self.ck_tasks.remove(id);
            let (state, reason) = match &u.result {
                Ok(()) => (CommitState::Committed, String::new()),
                Err((s, r)) => (*s, r.clone()),
            };
            let prev = self.rec.checkpoints.get_mut(id).map(|ck| {
                let prev = ck.state;
                ck.state = Some(state as i32);
                prev
            });
            // The checkpoint's outcome and its command's result go into one save: after a
            // crash both say the same, never COMMITTED with a failed or missing result.
            let answered = self.record_answer(id, failure);
            if !self.save() {
                // Not durable yet: nobody hears the outcome. The result is kept and the retry
                // tick saves it again (no second upload), then reports it.
                if let (Some(ck), Some(prev)) = (self.rec.checkpoints.get_mut(id), prev) {
                    ck.state = prev;
                }
                if let Some(command_id) = answered {
                    self.rec.results.remove(&command_id);
                    if let Some(plan) = self.rec.uploads.get_mut(id) {
                        plan.answered = false;
                    }
                }
                self.in_flight.insert(id.clone());
                self.unsaved.insert(id.clone(), u);
                return;
            }
            if let Some(role) = self.primary_role() {
                self.send(&role, commit_ack(id, state, &reason)).await;
            }
            self.deliver_results().await;
            return;
        }
        if let Some(command_id) = u.command_id {
            self.adhoc.remove(&command_id);
            // Saved together with its result (in `command_done`).
            self.rec.adhoc_uploads.remove(&command_id);
            self.command_done(command_id, failure).await;
        }
    }

    /// Notes the result of the checkpoint's upload command in the record (not saved yet),
    /// unless it already has one. Returns the command id when it was noted now.
    fn record_answer(&mut self, checkpoint_id: &str, failure: Option<String>) -> Option<String> {
        let plan = self.rec.uploads.get_mut(checkpoint_id)?;
        if plan.answered {
            return None;
        }
        plan.answered = true;
        let command_id = plan.command_id.clone();
        self.rec.results.insert(command_id.clone(), failure);
        Some(command_id)
    }

    /// Gives the checkpoint's upload command its one result, unless it already has one. Saved
    /// first, so a restart neither repeats nor loses it.
    async fn answer_plan(&mut self, checkpoint_id: &str, failure: Option<String>) {
        if self.record_answer(checkpoint_id, failure).is_some() {
            self.save();
            self.deliver_results().await;
        }
    }

    /// Starts sending files outside a checkpoint for `command_id` (in the background).
    fn start_adhoc(&mut self, command_id: String, targets: Vec<UploadTarget>) {
        if self.self_fenced || self.lease_over() {
            return;
        }
        let ctx = Arc::clone(&self.sh.ctx);
        let staging = self.staging();
        let tx = self.uploads_tx.clone();
        let id = command_id.clone();
        let task = tokio::spawn(async move {
            let mut result = Ok(());
            for t in targets {
                let sent = match t.ticket() {
                    Ok(ticket) => {
                        files::send(
                            &ctx,
                            &staging,
                            &t.local_path,
                            t.size,
                            &t.sha256,
                            &Destination::Ticket(ticket),
                        )
                        .await
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = sent {
                    result = Err((CommitState::Rejected, e.explain()));
                    break;
                }
            }
            let _ = tx
                .send(UploadDone {
                    checkpoint_id: None,
                    command_id: Some(id),
                    result,
                })
                .await;
        });
        self.adhoc.insert(command_id, task);
    }

    /// Reports a finished upload command (`failure`: why it failed): saved with the record
    /// first (in `results`), then sent; see [`Self::deliver_results`].
    async fn command_done(&mut self, command_id: String, failure: Option<String>) {
        self.rec.results.insert(command_id, failure);
        self.save();
        self.deliver_results().await;
    }

    /// Sends every saved upload result not sent yet, removing each once the gateway's channel
    /// took it. Stops at the first that can't be sent (closed channel, lease over); the retry
    /// tick, a restart or the next agent sends it again.
    async fn deliver_results(&mut self) {
        let pending: Vec<(String, Option<String>)> = self
            .rec
            .results
            .iter()
            .filter(|(k, _)| !self.in_transit.contains(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (command_id, failure) in pending {
            let ev = AttemptEvent::CommandDone(upload_result(command_id.clone(), failure));
            let Ok(saved) = self.queue(ev).await else {
                break;
            };
            // Forgotten when the gateway's receipt comes back (in the main loop); sent again
            // by the retry tick if it never does.
            self.in_transit.insert(command_id.clone());
            let back = self.receipts_tx.clone();
            tokio::spawn(async move {
                let _ = back.send((command_id, saved.await.is_ok())).await;
            });
        }
    }

    /// The gateway saved (or dropped) the result of `command_id`.
    fn on_receipt(&mut self, command_id: &str, saved: bool) {
        self.in_transit.remove(command_id);
        if saved && self.rec.results.remove(command_id).is_some() {
            self.save();
        }
    }

    /// Retries commits whose outcome could not be saved (and any that now have tickets), and
    /// results not sent yet.
    async fn retry_commits(&mut self) {
        if !self.rec.results.is_empty() {
            self.deliver_results().await;
        }
        for (_, done) in std::mem::take(&mut self.unsaved) {
            self.on_upload(done).await;
        }
        let waiting: Vec<String> = self
            .rec
            .checkpoints
            .iter()
            .filter(|(id, c)| {
                c.commit_requested && c.state.is_none() && !self.in_flight.contains(*id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in waiting {
            self.try_commit(&id).await;
        }
    }

    async fn on_control(&mut self, c: Control) -> bool {
        match c {
            Control::Start(reply) => {
                let all_prepared = self.rec.prepared.len() == self.rec.spec.workers().len();
                let r = if self.rec.spec.barrier
                    && self.rec.phase == AttemptPhase::Prepared as i32
                    && all_prepared
                {
                    let waiting: Vec<String> = self
                        .rec
                        .prepared
                        .iter()
                        .filter(|r| !self.rec.started.contains(r))
                        .cloned()
                        .collect();
                    let old_len = self.rec.started.len();
                    self.rec.started.extend(waiting.iter().cloned());
                    self.rec.phase = AttemptPhase::Running as i32;
                    if self.save() {
                        for role in waiting {
                            self.send(&role, AgentBody::Start(Start {})).await;
                        }
                        self.status(None).await;
                        Ok(())
                    } else {
                        self.rec.started.truncate(old_len);
                        self.rec.phase = AttemptPhase::Prepared as i32;
                        Err(AttemptError::state(self.id(), "could not save the start"))
                    }
                } else {
                    Err(AttemptError::state(
                        self.id(),
                        "is not waiting at the start barrier",
                    ))
                };
                let _ = reply.send(r);
            }
            Control::Cancel {
                request_id,
                grace_s,
                reason,
            } => {
                self.rec.cancel_requested = true;
                let body = AgentBody::Cancel(CancelRequest {
                    request_id,
                    grace_s,
                    reason,
                });
                if self.remember(&body).is_err() {
                    return false;
                }
                // Armed on receipt: the grace period runs from now, however long the workers
                // take to accept the request.
                self.set_stop_deadline(Duration::from_secs(grace_s.into()) + DEADLINE_MARGIN);
                self.send_all(body).await;
                self.set_phase(AttemptPhase::Stopping).await;
            }
            Control::Pause {
                request_id,
                deadline_s,
                reason,
            } => {
                let body = AgentBody::Pause(PauseRequest {
                    request_id,
                    deadline_s,
                    reason,
                });
                if self.remember(&body).is_err() {
                    return false;
                }
                // Armed on receipt, before the (bounded) send.
                self.set_stop_deadline(Duration::from_secs(deadline_s.into()) + DEADLINE_MARGIN);
                if let Some(role) = self.primary_role() {
                    self.send(&role, body).await;
                }
            }
            Control::Checkpoint {
                request_id,
                deadline_s,
                reason,
            } => {
                let body = AgentBody::CheckpointRequest(CheckpointRequest {
                    request_id,
                    deadline_s,
                    reason,
                });
                match self.remember(&body) {
                    Ok(true) => {
                        if let Some(role) = self.primary_role() {
                            self.send(&role, body).await;
                        }
                    }
                    Ok(false) => {}
                    Err(()) => return false,
                }
            }
            Control::Upload {
                command_id,
                checkpoint_id,
                targets,
            } => {
                if self.self_fenced || self.lease_over() {
                    self.command_done(command_id, Some("the attempt's lease expired".into()))
                        .await;
                    return true;
                }
                if checkpoint_id.is_empty() {
                    if self.adhoc.contains_key(&command_id) {
                        return true; // a replay of a running upload: answered when it ends
                    }
                    // Saved before it starts, so a restarted agent knows about it.
                    self.rec
                        .adhoc_uploads
                        .insert(command_id.clone(), targets.clone());
                    self.save();
                    self.start_adhoc(command_id, targets);
                } else {
                    let active = self
                        .rec
                        .uploads
                        .get(&checkpoint_id)
                        .map(|p| p.command_id.clone());
                    if self.in_flight.contains(&checkpoint_id) {
                        // One upload per checkpoint at a time (including one whose outcome is
                        // still being saved); a replay of the running command gets its result
                        // when it ends.
                        if active.as_deref() != Some(command_id.as_str()) {
                            let why = format!(
                                "checkpoint {checkpoint_id} is already uploading (command {})",
                                active.unwrap_or_default()
                            );
                            self.command_done(command_id, Some(why)).await;
                        }
                        return true;
                    }
                    let state = self
                        .rec
                        .checkpoints
                        .get(&checkpoint_id)
                        .and_then(|c| c.state)
                        .and_then(|s| CommitState::try_from(s).ok());
                    match state {
                        // Already decided: the saved outcome, no second upload.
                        Some(CommitState::Committed) => self.command_done(command_id, None).await,
                        Some(CommitState::Corrupt) => {
                            let why = format!("checkpoint {checkpoint_id} is corrupt");
                            self.command_done(command_id, Some(why)).await;
                        }
                        _ => {
                            self.accept_plan(checkpoint_id.clone(), command_id, targets)
                                .await;
                            // New tickets retry a rejected upload.
                            if let Some(ck) = self.rec.checkpoints.get_mut(&checkpoint_id) {
                                if ck.state == Some(CommitState::Rejected as i32) {
                                    ck.state = None;
                                    // The retry is a new upload: its command gets a result.
                                    if let Some(p) = self.rec.uploads.get_mut(&checkpoint_id) {
                                        p.answered = false;
                                    }
                                }
                            }
                            self.save();
                            self.try_commit(&checkpoint_id).await;
                        }
                    }
                }
            }
            Control::Lease {
                heartbeat_seq,
                heartbeat_sent,
                duration_s,
                kill_budget_s,
            } => {
                if self.apply_lease(heartbeat_seq, heartbeat_sent, duration_s, kill_budget_s) {
                    if let Some(l) = self.lease_deadline() {
                        self.send_all(l).await;
                    }
                }
            }
            Control::Hw(events) => {
                for e in events {
                    let fatal = matches!(e.kind, HwEventKind::DeviceLost);
                    if self.keep_hw(e) && fatal {
                        self.stop_all().await;
                    }
                }
            }
            Control::Detach(done) => {
                self.detach().await;
                let _ = done.send(());
            }
            // Unwrapped by the caller (`split_ack`); never nested.
            Control::Acked { saved, .. } => {
                let _ = saved.send(false);
            }
        }
        true
    }

    /// True when the attempt is over (every worker ended).
    async fn on_exit(&mut self, exit: WorkerExit) -> bool {
        if let Some(w) = self.workers.remove(&exit.role_id) {
            if w.primary {
                // The primary decides the attempt; the others are stopped now.
                let grace = Duration::from_secs(self.sh.ctx.config.exec.kill_grace_secs);
                for other in self.workers.values() {
                    let _ = other.stop.send(StopMsg::Stop(Instant::now() + grace)).await;
                }
                self.primary_exit = Some(exit);
            } else {
                self.secondary_oom |= exit.oom;
            }
        }
        self.workers.is_empty()
    }

    async fn finish(&mut self, control: &mut mpsc::Receiver<Control>) -> ExitDecision {
        let p = self.primary_exit.take();
        let ev = ExitEvidence {
            report: self.report.clone(),
            exit_code: p.as_ref().and_then(|p| p.code),
            signal: p.as_ref().and_then(|p| p.signal),
            oom_killed: self.secondary_oom || p.as_ref().is_some_and(|p| p.oom),
            hw_events: self.rec.hw_events.clone(),
            self_fenced: self.self_fenced,
            stalled: self.stalled,
            env_mismatch: self.env_mismatch.clone(),
            cancel_requested: self.rec.cancel_requested,
            protocol_error: self.protocol_error.clone(),
            final_committed: self.rec.final_committed(),
            needs_final_checkpoint: self.rec.needs_final_checkpoint(),
            readopted: p.as_ref().is_some_and(|p| p.readopted),
            agent_stopped: self.agent_stopped,
        };
        self.finish_with(decide(&ev), control).await
    }

    async fn abort_transfers(&mut self) {
        for task in self.ck_tasks.values().chain(self.adhoc.values()) {
            task.abort();
        }
        for (_, task) in self.ck_tasks.drain() {
            let _ = task.await;
        }
        for (_, task) in self.adhoc.drain() {
            let _ = task.await;
        }
    }

    async fn finish_with(
        &mut self,
        decision: ExitDecision,
        control: &mut mpsc::Receiver<Control>,
    ) -> ExitDecision {
        // Uploads still running get to finish (not after a self-fence), so their commands get
        // their real result; every command still without one is then answered as failed.
        let drain_limit = Instant::now() + UPLOAD_DRAIN;
        let until = drain_limit.min(self.lease.unwrap_or(drain_limit));
        let mut detached = None;
        while !(self.in_flight.is_empty() && self.adhoc.is_empty()) && !self.self_fenced {
            for (_, done) in std::mem::take(&mut self.unsaved) {
                self.on_upload(done).await;
            }
            if self.in_flight.is_empty() && self.adhoc.is_empty() {
                break;
            }
            tokio::select! {
                () = tokio::time::sleep_until(until) => break,
                Some(command) = control.recv() => {
                    if let Control::Detach(done) = command {
                        self.stop_passed = Some(Instant::now());
                        detached = Some(done);
                        break;
                    }
                }
                Some(upload) = self.uploads_rx.recv() => self.on_upload(upload).await,
            }
        }
        // Transfers still running are stopped and joined before anything is reported or
        // cleaned up: no PUT outlives the attempt.
        self.abort_transfers().await;
        // Results that made it before the stop keep their real outcome (and an unsaved one
        // gets one more save).
        while let Ok(u) = self.uploads_rx.try_recv() {
            self.on_upload(u).await;
        }
        for (_, done) in std::mem::take(&mut self.unsaved) {
            self.on_upload(done).await;
        }
        self.in_flight.clear();
        let waiting: Vec<String> = self
            .rec
            .uploads
            .iter()
            .filter(|(_, p)| !p.answered)
            .map(|(id, _)| id.clone())
            .collect();
        for id in waiting {
            let why = format!("the attempt ended before checkpoint {id} was uploaded");
            self.answer_plan(&id, Some(why)).await;
        }
        // Uploads of other files still running are stopped, so no transfer outlives the
        // attempt, and their commands fail.
        for (command_id, _) in std::mem::take(&mut self.rec.adhoc_uploads) {
            let why = "the attempt ended before the upload finished".to_owned();
            self.command_done(command_id, Some(why)).await;
        }
        if let Some(s) = self.server.take() {
            s.stop().await;
        }
        self.rec.phase = decision.phase;
        self.rec.exit = Some(decision.clone());
        self.save();
        self.status(Some(&decision)).await;
        if let Some(done) = detached {
            let _ = done.send(());
        }
        let report =
            crate::janitor::release(&self.sh.ctx, &self.sh.envs, &self.rec, &self.gpu_uuids).await;
        let line = serde_json::to_string(&report).unwrap_or_default();
        let _ = std::fs::write(self.job_dir.join("cleanup.json"), line);
        decision
    }
}

/// The request id of a cancel, pause or checkpoint request.
fn request_id(body: &AgentBody) -> Option<&str> {
    match body {
        AgentBody::Cancel(c) => Some(&c.request_id),
        AgentBody::Pause(p) => Some(&p.request_id),
        AgentBody::CheckpointRequest(c) => Some(&c.request_id),
        _ => None,
    }
}

/// The `CommandResult` for a finished upload command (`failure`: why it failed).
pub fn upload_result(command_id: String, failure: Option<String>) -> CommandResult {
    CommandResult {
        command_id,
        accepted: true,
        failure: failure.map(|reason| mrl_proto::common::v1::Failure {
            class: mrl_proto::common::v1::FailureClass::InfraTransient as i32,
            sub_code: "UPLOAD_FAILED".into(),
            message: reason,
            retryable_hint: true,
            classified_by: mrl_proto::common::v1::ClassifiedBy::Agent as i32,
            ..Default::default()
        }),
    }
}

/// How long an ending attempt waits for uploads still running.
const UPLOAD_DRAIN: Duration = Duration::from_secs(60);

/// Runs `spec` under [`KEEPER`] (see there).
#[cfg(unix)]
fn keep_alive(spec: ExecSpec) -> ExecSpec {
    let mut args = vec![
        "-c".to_owned(),
        KEEPER.to_owned(),
        "mrl-keeper".to_owned(),
        spec.program.clone(),
    ];
    args.extend(spec.args.iter().cloned());
    ExecSpec {
        program: "/bin/sh".to_owned(),
        args,
        ..spec
    }
}

fn grant(id: &str, ck: &StagedCheckpoint) -> AgentBody {
    AgentBody::UploadGrant(UploadGrant {
        checkpoint_id: id.to_owned(),
        // The agent uploads from where the worker staged; nothing moves.
        targets: ck
            .files
            .iter()
            .map(|f| (f.path.clone(), f.path.clone()))
            .collect(),
        expires_at: None,
    })
}

fn commit_ack(id: &str, state: CommitState, reason: &str) -> AgentBody {
    AgentBody::CheckpointCommitAck(CheckpointCommitAck {
        checkpoint_id: id.to_owned(),
        state: state as i32,
        reason: reason.to_owned(),
    })
}

/// `docker run` arguments for a container worker. Secrets are passed by name only (`-e NAME`),
/// so their values never appear on a command line.
#[allow(clippy::too_many_arguments)]
pub fn docker_run_args<'a>(
    name: &str,
    image: &str,
    job_dir: &Path,
    socket_dir: Option<&Path>,
    role: &RoleSpec,
    limits: &crate::exec::Limits,
    env_names: impl Iterator<Item = &'a str>,
    worker_args: &[String],
) -> Vec<String> {
    let staging = staging_dir(job_dir).display().to_string();
    let inputs = inputs_dir(job_dir).display().to_string();
    let mut a: Vec<String> = ["run", "--name", name, "--network", "none", "--init"]
        .map(String::from)
        .into();
    #[cfg(unix)]
    {
        let (uid, gid) = (nix::unistd::getuid(), nix::unistd::getgid());
        a.extend(["--user".into(), format!("{uid}:{gid}")]);
    }
    let work = job_dir.join("work").display().to_string();
    for v in [
        format!("{staging}:{staging}"),
        format!("{inputs}:{inputs}:ro"),
        format!("{work}:{work}:ro"),
    ] {
        a.extend(["-v".into(), v]);
    }
    if let Some(dir) = socket_dir {
        // Connecting needs no write access; read-only keeps the worker from removing or
        // replacing the socket.
        let d = dir.display().to_string();
        a.extend(["-v".into(), format!("{d}:{d}:ro")]);
    }
    a.extend(["-w".into(), work]);
    for n in env_names.chain(["MRL_STAGING_DIR", "TMPDIR"]) {
        a.extend(["-e".into(), n.to_owned()]);
    }
    if !role.gpus.is_empty() {
        let ids: Vec<String> = role.gpus.iter().map(u32::to_string).collect();
        a.extend(["--gpus".into(), format!("\"device={}\"", ids.join(","))]);
    }
    if let Some(m) = limits.memory_bytes {
        a.extend([
            "--memory".into(),
            m.to_string(),
            "--memory-swap".into(),
            m.to_string(),
        ]);
    }
    if let Some(c) = limits.cpu_millis {
        a.extend(["--cpus".into(), format!("{:.3}", c as f64 / 1000.0)]);
    }
    if let Some(p) = limits.pids {
        a.extend(["--pids-limit".into(), p.to_string()]);
    }
    a.push(image.to_owned());
    a.push("mrl-worker".to_owned());
    a.extend(worker_args.iter().cloned());
    a
}

/// Container exit code and OOM flag (`docker inspect`).
pub(crate) async fn container_exit(name: &str) -> (Option<i32>, bool) {
    use super::record::{docker_output, DOCKER_TIMEOUT};
    let Some(out) = docker_output(
        &[
            "container",
            "inspect",
            "--format",
            "{{.State.ExitCode}} {{.State.OOMKilled}}",
            name,
        ],
        DOCKER_TIMEOUT,
    )
    .await
    .filter(|out| out.status.success()) else {
        return (None, false);
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.split_whitespace();
    let code = parts.next().and_then(|c| c.parse().ok());
    (code, parts.next() == Some("true"))
}

async fn docker_stop(id: &str, grace: Duration) {
    use super::record::{docker_output, DOCKER_TIMEOUT};
    if !grace.is_zero()
        && docker_output(&["stop", "-t", &grace.as_secs().to_string(), id], grace)
            .await
            .is_some_and(|out| out.status.success())
    {
        return;
    }
    let _ = docker_output(&["kill", id], DOCKER_TIMEOUT).await;
}

/// Watches a worker the agent started.
///
/// A container (`container`: its name and this launch's token) is stopped, killed and
/// inspected only by its id, once `docker inspect` shows it carries the token; a container
/// that took over the name is never touched. Until then, stopping the `docker run` client
/// (which forwards signals) is all that is done.
fn spawn_exec(
    role_id: String,
    mut exec: Box<dyn Execution>,
    container: Option<(String, String)>,
    exits: mpsc::Sender<WorkerExit>,
) -> mpsc::Sender<StopMsg> {
    let (tx, mut rx) = mpsc::channel(4);
    tokio::spawn(async move {
        let mut id: Option<String> = None;
        let resolve = |id: &mut Option<String>| {
            let pending = container.clone().filter(|_| id.is_none());
            async move {
                let (name, token) = pending?;
                super::record::owned_container(&name, &token)
                    .await
                    .ok()
                    .flatten()
            }
        };
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let report = loop {
            tokio::select! {
                r = exec.wait() => break r,
                _ = tick.tick(), if container.is_some() && id.is_none() => {
                    if let Some(found) = resolve(&mut id).await {
                        id = Some(found);
                    }
                }
                msg = rx.recv() => match msg {
                    Some(StopMsg::Stop(until)) => {
                        if let Some(found) = resolve(&mut id).await {
                            id = Some(found);
                        }
                        if let Some(c) = &id {
                            docker_stop(c, until.saturating_duration_since(Instant::now())).await;
                        }
                        break exec.stop(until.saturating_duration_since(Instant::now())).await;
                    }
                    Some(StopMsg::Kill) => {
                        if let Some(found) = resolve(&mut id).await {
                            id = Some(found);
                        }
                        if let Some(c) = &id {
                            docker_stop(c, Duration::ZERO).await;
                        }
                        let _ = exec.kill().await;
                    }
                    Some(StopMsg::Detach) | None => {
                        // ponytail: the agent is going away and the next one re-adopts this
                        // worker, so its handle is dropped without the kill its Drop would do.
                        std::mem::forget(exec);
                        return;
                    }
                },
            }
        };
        let (mut code, mut signal, mut oom) = match report {
            Ok(r) => (r.code, r.signal, r.oom_killed),
            Err(_) => (None, None, false),
        };
        if let Some(found) = resolve(&mut id).await {
            id = Some(found);
        }
        if let Some(c) = &id {
            let (c_code, c_oom) = container_exit(c).await;
            code = c_code.or(code);
            oom |= c_oom;
            signal = None;
        }
        let _ = exits
            .send(WorkerExit {
                role_id,
                code,
                signal,
                oom,
                readopted: false,
            })
            .await;
    });
    tx
}

/// Signals a process by identity (its process group too: workers lead their own). A process
/// is signalled only while it is still the same process (pid and start time), so a reused pid
/// or process group is never hit.
pub fn signal_identity(identity: &ProcIdentity, force: bool) {
    match identity {
        ProcIdentity::Pid { .. } if !identity.alive() => {}
        ProcIdentity::Pid { pid, .. } => {
            #[cfg(unix)]
            {
                use nix::sys::signal::{kill, killpg, Signal};
                use nix::unistd::Pid;
                let sig = if force {
                    Signal::SIGKILL
                } else {
                    Signal::SIGTERM
                };
                if let Ok(p) = i32::try_from(*pid) {
                    let _ = killpg(Pid::from_raw(p), sig);
                    let _ = kill(Pid::from_raw(p), sig);
                }
            }
            #[cfg(windows)]
            {
                let mut c = std::process::Command::new("taskkill");
                c.args(["/T", "/PID", &pid.to_string()]);
                if force {
                    c.arg("/F");
                }
                let _ = c
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
        ProcIdentity::Container { .. } => {}
    }
}

/// Watches a re-adopted worker (not the agent's child: no exit status, only liveness).
fn spawn_watch(
    role_id: String,
    identity: ProcIdentity,
    exits: mpsc::Sender<WorkerExit>,
) -> mpsc::Sender<StopMsg> {
    let (tx, mut rx) = mpsc::channel(4);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(500)) => {
                    if !super::record::running(&identity).await {
                        break;
                    }
                }
                msg = rx.recv() => match msg {
                    Some(StopMsg::Stop(until)) => {
                        if let ProcIdentity::Container { id: Some(id), .. } = &identity {
                            docker_stop(id, until.saturating_duration_since(Instant::now())).await;
                            break;
                        }
                        signal_identity(&identity, false);
                        while Instant::now() < until && super::record::running(&identity).await {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        // Escalate only if the original process is still there.
                        if super::record::running(&identity).await {
                            signal_identity(&identity, true);
                        }
                        break;
                    }
                    Some(StopMsg::Kill) => {
                        if let ProcIdentity::Container { id: Some(id), .. } = &identity {
                            docker_stop(id, Duration::ZERO).await;
                        } else {
                            signal_identity(&identity, true);
                        }
                    },
                    Some(StopMsg::Detach) | None => return,
                },
            }
        }
        let (code, oom) = match &identity {
            ProcIdentity::Container { name, id } => {
                container_exit(id.as_deref().unwrap_or(name)).await
            }
            ProcIdentity::Pid { .. } => (None, false),
        };
        let _ = exits
            .send(WorkerExit {
                role_id,
                code,
                signal: None,
                oom,
                readopted: true,
            })
            .await;
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A started program that never ends on its own (stands in for `docker run`).
    #[cfg(unix)]
    struct Forever;

    #[cfg(unix)]
    #[async_trait::async_trait]
    impl Execution for Forever {
        fn pid(&self) -> Option<u32> {
            None
        }
        async fn wait(&mut self) -> crate::Result<crate::exec::ExitReport> {
            std::future::pending().await
        }
        async fn kill(&mut self) -> crate::Result<()> {
            Ok(())
        }
        async fn stop(&mut self, _: Duration) -> crate::Result<crate::exec::ExitReport> {
            Ok(crate::exec::ExitReport {
                code: Some(0),
                signal: None,
                oom_killed: false,
                limits: crate::exec::LimitSupport::default(),
                runtime_ms: 0,
            })
        }
    }

    /// The live watcher stops a container only after checking its launch token: one that
    /// took over the name is left running; this launch's own is stopped by id.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs Docker; runs in ci-attempts"]
    async fn docker_watcher_stops_only_its_own_container() {
        let Some(docker) = crate::envs::which("docker") else {
            assert!(
                std::env::var("MRL_TEST_DOCKER_REQUIRED").is_err(),
                "Docker is required"
            );
            return;
        };
        let name = format!("mrl-test-w{}", std::process::id());
        let start = |token: &str| {
            let _ = std::process::Command::new(&docker)
                .args(["rm", "-f", &name])
                .output();
            let ok = std::process::Command::new(&docker)
                .args(["run", "-d", "--name", &name, "-e"])
                .arg(format!("{TOKEN_ENV}={token}"))
                .args(["busybox:1.36", "sleep", "300"])
                .stdout(std::process::Stdio::null())
                .status()
                .unwrap();
            assert!(ok.success());
        };
        let running = || crate::attempts::record::container_running(&name);
        let watch = |token: String| {
            let name = name.clone();
            async move {
                let (etx, mut erx) = mpsc::channel(1);
                let stop = spawn_exec(
                    "trainer".into(),
                    Box::new(Forever),
                    Some((name, token)),
                    etx,
                );
                stop.send(StopMsg::Stop(Instant::now() + Duration::from_secs(1)))
                    .await
                    .unwrap();
                erx.recv().await.unwrap()
            }
        };
        start(&attempt_token("newer"));
        watch(attempt_token("older")).await;
        assert!(running(), "another launch's container is left alone");
        start(&attempt_token("mine"));
        watch(attempt_token("mine")).await;
        assert!(!running(), "its own container is stopped");
        let _ = std::process::Command::new(&docker)
            .args(["rm", "-f", &name])
            .output();
    }

    #[test]
    fn docker_args_keep_secrets_off_the_command_line() {
        let role = RoleSpec {
            role_id: "trainer".into(),
            kind: "trainer".into(),
            primary: true,
            count: 1,
            gpus: vec![0, 1],
            runtime_ref: "oci:x@sha256:aa".into(),
        };
        let limits = crate::exec::Limits {
            memory_bytes: Some(1 << 30),
            cpu_millis: Some(1500),
            pids: None,
        };
        let job = Path::new("/s").join("jobs").join("att");
        let a = docker_run_args(
            "mrl-att-trainer",
            "x@sha256:aa",
            &job,
            Some(&Path::new("/s").join("sock")),
            &role,
            &limits,
            ["MRL_WORKER_NONCE", "MRL_CREDENTIAL_HF"].into_iter(),
            &["--engine".into(), "fake".into()],
        );
        let s = a.join(" ");
        let inputs = job.join("inputs").display().to_string();
        assert!(s.contains("--network none"));
        assert!(s.contains("-e MRL_WORKER_NONCE -e MRL_CREDENTIAL_HF"));
        assert!(s.contains(&format!("-v {inputs}:{inputs}:ro")));
        let sock = Path::new("/s").join("sock").display().to_string();
        assert!(
            s.contains(&format!("-v {sock}:{sock}:ro")),
            "the socket folder is read-only"
        );
        assert!(s.contains("--gpus \"device=0,1\""));
        assert!(s.contains("--cpus 1.500"));
        assert!(s.ends_with("x@sha256:aa mrl-worker --engine fake"));
        assert!(!s.contains("secret"), "only names are passed");
    }

    #[test]
    fn sockets_are_short_and_nonces_random() {
        let p = socket_path(Path::new("/s"), "att_0123456789abcdef0123456789abcdef");
        assert!(p.as_os_str().len() < 40);
        assert_ne!(
            p,
            socket_path(Path::new("/other"), "att_0123456789abcdef0123456789abcdef")
        );
        // Two attempts never share a socket folder (the one a container gets mounted).
        assert_ne!(
            p.parent(),
            socket_path(Path::new("/s"), "att_other").parent()
        );
        let (a, b) = (new_nonce().unwrap(), new_nonce().unwrap());
        assert_eq!(a.len(), 64);
        assert_ne!(a, b);
    }

    #[cfg(unix)]
    #[test]
    fn deep_state_folders_use_a_short_socket_path() {
        let deep = Path::new("/very").join("deep".repeat(30));
        let q = socket_path(&deep, "att_1");
        assert!(q.as_os_str().len() <= MAX_SOCKET_PATH);
        assert!(q.starts_with("/tmp"));
    }

    #[cfg(unix)]
    #[test]
    fn signals_reach_only_the_same_process() {
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let ProcIdentity::Pid { pid, start_time } = ProcIdentity::of_pid(child.id()).unwrap()
        else {
            panic!("pid identity")
        };
        // Same pid, other start time: a reused pid is left alone.
        signal_identity(
            &ProcIdentity::Pid {
                pid,
                start_time: start_time + 1,
            },
            true,
        );
        std::thread::sleep(Duration::from_millis(200));
        assert!(child.try_wait().unwrap().is_none(), "still running");
        signal_identity(&ProcIdentity::Pid { pid, start_time }, true);
        assert!(child.wait().unwrap().code().is_none(), "killed");
    }

    #[cfg(unix)]
    #[test]
    fn keeper_wraps_the_sandbox_launcher() {
        let spec = ExecSpec::new(
            "j",
            "/usr/bin/bwrap",
            &["--die-with-parent", "--", "w"],
            Path::new("/s"),
        );
        let k = keep_alive(spec);
        assert_eq!(k.program, "/bin/sh");
        assert_eq!(k.args[2], "mrl-keeper");
        assert_eq!(
            &k.args[3..],
            ["/usr/bin/bwrap", "--die-with-parent", "--", "w"]
        );
    }
}
