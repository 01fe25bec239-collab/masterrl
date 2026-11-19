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
    /// Lease renewed: `duration_s` from now, stop `kill_budget_s` before it ends.
    Lease {
        /// Lease length.
        duration_s: u32,
        /// Safety margin.
        kill_budget_s: u32,
    },
    /// GPU events from the node.
    Hw(Vec<HwEvent>),
    /// The agent is shutting down: leave the workers running for the next agent.
    Detach(oneshot::Sender<()>),
}

enum StopMsg {
    Stop(Duration),
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
    hw_events: Vec<HwEvent>,
    env_mismatch: Option<String>,
    protocol_error: Option<String>,
    stalled: bool,
    self_fenced: bool,
    agent_stopped: bool,
    lease: Option<Instant>,
    stop_at: Option<Instant>,
    last_seen: Instant,
    in_flight: HashSet<String>,
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
/// find the attempt's processes even after they changed folder and session.
pub const ATTEMPT_ENV: &str = "MRL_ATTEMPT_ID";

/// Keeps an isolating sandbox's launcher (bubblewrap, which uses `--die-with-parent`) alive
/// when the agent dies, so the worker can be re-adopted. Forwards SIGTERM and exits with the
/// launcher's status.
#[cfg(unix)]
const KEEPER: &str = r#""$@" & c=$!
trap 'kill -TERM "$c" 2>/dev/null' TERM
while :; do wait "$c"; s=$?; kill -0 "$c" 2>/dev/null || exit "$s"; done"#;

fn now_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

/// Socket path: `<state>/sock/<hash>.sock`, or `/tmp/mrl-<uid>/<hash>.sock` when the state
/// folder is too deep for a Unix socket path.
pub fn socket_path(state_root: &Path, attempt_id: &str) -> PathBuf {
    let key = format!("{}\0{attempt_id}", state_root.display());
    let name = format!("{}.sock", &mrl_storage::sha256_hex(key.as_bytes())[..16]);
    let p = state_root.join("sock").join(&name);
    #[cfg(unix)]
    if p.as_os_str().len() > MAX_SOCKET_PATH {
        return PathBuf::from(format!("/tmp/mrl-{}", nix::unistd::getuid())).join(name);
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
            hw_events: Vec::new(),
            env_mismatch: None,
            protocol_error: None,
            stalled: false,
            self_fenced: false,
            agent_stopped: false,
            lease: None,
            stop_at: None,
            last_seen: Instant::now(),
            in_flight: HashSet::new(),
        }
    }

    fn id(&self) -> &str {
        &self.rec.spec.attempt.attempt_id
    }

    fn staging(&self) -> PathBuf {
        staging_dir(&self.job_dir)
    }

    /// Runs a new attempt to its end. `None` means it was detached (agent shutdown).
    pub async fn run(mut self, control: mpsc::Receiver<Control>) -> Option<ExitDecision> {
        self.gpu_uuids = self.lookup_gpus();
        if let Err(e) = self.launch().await {
            for w in self.workers.values() {
                let _ = w.stop.send(StopMsg::Kill).await;
            }
            return Some(self.fail_early(e).await);
        }
        self.main_loop(control).await
    }

    /// Re-attaches to live workers after an agent restart.
    pub async fn readopt(mut self, control: mpsc::Receiver<Control>) -> Option<ExitDecision> {
        self.gpu_uuids = self.lookup_gpus();
        if let Err(e) = self.start_server().await {
            return Some(self.fail_early(e).await);
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
        self.main_loop(control).await
    }

    /// Arms the lease timer for a wall-clock deadline (Unix ms).
    fn arm_lease(&mut self, deadline_ms: i64) {
        let left = u64::try_from(deadline_ms - now_ms()).unwrap_or(0);
        self.lease = Some(Instant::now() + Duration::from_millis(left));
    }

    /// Records and arms a new lease deadline.
    fn set_lease(&mut self, deadline_ms: i64) {
        self.rec.lease_deadline_unix_ms = Some(deadline_ms);
        self.arm_lease(deadline_ms);
        self.save();
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
        let _ = self.sh.events.send(AttemptEvent::Status(status)).await;
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

    async fn launch(&mut self) -> Result<(), AttemptError> {
        self.set_phase(AttemptPhase::Staging).await;
        let id = self.id().to_owned();
        self.rec.staged_paths = files::stage_inputs(
            &self.sh.ctx,
            &self.rec.spec,
            &inputs_dir(&self.job_dir),
            &id,
        )
        .await?;
        let log = self.job_dir.join("logs").join("environment.log");
        let progress = self.sh.progress.clone();
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
        std::fs::create_dir_all(self.job_dir.join("logs"))
            .map_err(|e| AgentError::state(self.job_dir.join("logs"), e))?;
        let mut envs: HashMap<String, Materialized> = HashMap::new();
        for rt in self.rec.spec.runtimes.clone() {
            if self
                .rec
                .spec
                .roles
                .iter()
                .any(|r| r.runtime_ref == rt.runtime_ref)
            {
                let m = self.sh.envs.materialize(&rt, &id, &say).await?;
                envs.insert(rt.runtime_ref.clone(), m);
            }
        }
        self.set_phase(AttemptPhase::Preparing).await;
        self.start_server().await?;
        let spec = self.rec.spec.clone();
        for (role_id, role, primary) in spec.workers() {
            let mat = envs.get(&role.runtime_ref).ok_or_else(|| {
                AttemptError::Spec(format!("runtime {} was not prepared", role.runtime_ref))
            })?;
            self.start_worker(&role_id, role, primary, mat).await?;
        }
        if spec.lease_s > 0 {
            self.set_lease(now_ms() + i64::from(spec.lease_s) * 1000);
        }
        Ok(())
    }

    async fn start_server(&mut self) -> Result<(), AttemptError> {
        let server = WorkerServer::start(
            &self.rec.socket,
            self.id(),
            Secret::new(self.rec.nonce.clone()),
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

    fn exec_name(&self, role_id: &str) -> String {
        format!("{}-{role_id}", self.id())
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
        env.insert(ATTEMPT_ENV.to_owned(), self.id().to_owned());
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
        let exec = self.sh.ctx.executor.spawn(spec).await?;
        let identity = match &container {
            Some(name) => Some(ProcIdentity::Container { name: name.clone() }),
            None => exec.pid().and_then(ProcIdentity::of_pid),
        };
        if let Some(identity) = identity {
            self.rec.workers.push(WorkerProc {
                role_id: role_id.to_owned(),
                primary,
                identity,
                cgroup: None,
            });
        }
        let stop = spawn_exec(role_id.to_owned(), exec, container, self.exits_tx.clone());
        self.workers
            .insert(role_id.to_owned(), WorkerHandle { primary, stop });
        self.save();
        Ok(())
    }

    async fn fail_early(&mut self, e: AttemptError) -> ExitDecision {
        let decision = ExitDecision {
            phase: AttemptPhase::Exited as i32,
            exit_kind: mrl_proto::worker::v1::ExitKind::Failed as i32,
            exit_code: -1,
            failure: Some(e.to_failure()),
        };
        self.finish_with(decision).await
    }

    async fn main_loop(&mut self, mut control: mpsc::Receiver<Control>) -> Option<ExitDecision> {
        let mut stall_tick = tokio::time::interval(Duration::from_secs(1));
        self.last_seen = Instant::now();
        let far = || Instant::now() + Duration::from_secs(365 * 24 * 3600);
        loop {
            let lease_at = self.lease.unwrap_or_else(far);
            let stop_at = self.stop_at.unwrap_or_else(far);
            tokio::select! {
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
                    Some(c) => self.on_control(c).await,
                    // The manager is gone without a detach: stop the attempt.
                    None => self.stop_all().await,
                },
                Some(u) = self.uploads_rx.recv() => self.on_upload(u).await,
                () = tokio::time::sleep_until(lease_at), if self.lease.is_some() && !self.self_fenced => {
                    self.self_fenced = true;
                    self.stop_all().await;
                }
                () = tokio::time::sleep_until(stop_at), if self.stop_at.is_some() => {
                    self.stop_at = None;
                    self.stop_all().await;
                }
                _ = stall_tick.tick() => {
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
        Some(self.finish().await)
    }

    async fn stop_all(&mut self) {
        self.agent_stopped = true;
        let grace = Duration::from_secs(self.sh.ctx.config.exec.kill_grace_secs);
        for w in self.workers.values() {
            let _ = w.stop.send(StopMsg::Stop(grace)).await;
        }
        self.set_phase(AttemptPhase::Stopping).await;
    }

    async fn detach(&mut self) {
        for w in self.workers.values() {
            let _ = w.stop.send(StopMsg::Detach).await;
        }
        if let Some(s) = self.server.take() {
            s.stop().await;
        }
        self.save();
    }

    async fn send(&self, role_id: &str, body: AgentBody) -> bool {
        match &self.server {
            Some(s) => s.send(role_id, body).await,
            None => false,
        }
    }

    async fn send_all(&self, body: AgentBody) {
        for role in self.workers.keys() {
            self.send(role, body.clone()).await;
        }
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

    async fn relay(&self, role_id: &str, msg: WorkerMessage) {
        if let Some(lane) = crate::lanes::lane_for(&msg) {
            let relay = WorkerRelay {
                attempt: Some(self.rec.spec.attempt.to_proto()),
                role_id: role_id.to_owned(),
                message: Some(msg),
            };
            let _ = self
                .sh
                .events
                .send(AttemptEvent::Relay { lane, relay })
                .await;
        }
    }

    fn role_spec(&self, role_id: &str) -> Option<RoleSpec> {
        let base = role_id.split_once('.').map_or(role_id, |(b, _)| b);
        self.rec
            .spec
            .roles
            .iter()
            .find(|r| r.role_id == role_id || r.role_id == base)
            .cloned()
    }

    async fn on_inbound(&mut self, inb: Inbound) {
        self.last_seen = Instant::now();
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
                self.note_cgroup(&role_id);
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
                if self.on_message(&role_id, &msg).await {
                    self.relay(&role_id, msg).await;
                    let _ = done.send(());
                }
            }
            Inbound::Live { role_id, msg } => {
                if let Some(WorkerBody::Heartbeat(hb)) = &msg.body {
                    self.rec.last_step = self.rec.last_step.max(hb.last_step);
                }
                self.relay(&role_id, msg).await;
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
    fn note_cgroup(&mut self, role_id: &str) {
        let Some(w) = self
            .rec
            .workers
            .iter_mut()
            .find(|w| w.role_id == role_id && w.cgroup.is_none())
        else {
            return;
        };
        if let ProcIdentity::Pid { pid, .. } = w.identity {
            w.cgroup = crate::janitor::job_cgroup_of(pid);
            if w.cgroup.is_some() {
                self.save();
            }
        }
    }

    /// Keeps a control request so a reconnecting worker gets it again.
    fn remember(&mut self, body: &AgentBody) {
        let msg = mrl_proto::worker::v1::AgentMessage {
            ack_seq: 0,
            body: Some(body.clone()),
        };
        self.rec.requests.push(msg.encode_to_vec());
        self.save();
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
                    let ack = commit_ack(
                        &c.checkpoint_id,
                        CommitState::Corrupt,
                        "the manifest changed after staging",
                    );
                    self.send(role_id, ack).await;
                } else {
                    self.try_commit(&c.checkpoint_id).await;
                }
                true
            }
            Some(WorkerBody::Exit(r)) if primary => {
                // Saved before the ack, so an agent restart still knows how the worker ended.
                self.rec.exit_report = Some(r.encode_to_vec());
                self.report = Some(r.clone());
                self.save()
            }
            _ => true,
        }
    }

    /// Starts uploading a checkpoint once the worker asked to commit it and every file has an
    /// authorized target whose size and sha256 match what the worker staged.
    async fn try_commit(&mut self, id: &str) {
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
        tokio::spawn(async move {
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
    }

    async fn on_upload(&mut self, u: UploadDone) {
        if let Some(id) = &u.checkpoint_id {
            self.in_flight.remove(id);
            let (state, reason) = match &u.result {
                Ok(()) => (CommitState::Committed, String::new()),
                Err((s, r)) => (*s, r.clone()),
            };
            if let Some(ck) = self.rec.checkpoints.get_mut(id) {
                ck.state = Some(state as i32);
            }
            self.save();
            if let Some(role) = self.primary_role() {
                self.send(&role, commit_ack(id, state, &reason)).await;
            }
        }
        if let Some(command_id) = u.command_id {
            let result = CommandResult {
                command_id,
                accepted: true,
                failure: u
                    .result
                    .err()
                    .map(|(_, reason)| mrl_proto::common::v1::Failure {
                        class: mrl_proto::common::v1::FailureClass::InfraTransient as i32,
                        sub_code: "UPLOAD_FAILED".into(),
                        message: reason,
                        retryable_hint: true,
                        classified_by: mrl_proto::common::v1::ClassifiedBy::Agent as i32,
                        ..Default::default()
                    }),
            };
            let _ = self.sh.events.send(AttemptEvent::CommandDone(result)).await;
        }
    }

    async fn on_control(&mut self, c: Control) {
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
                    for role in waiting {
                        self.rec.started.push(role.clone());
                        self.save();
                        self.send(&role, AgentBody::Start(Start {})).await;
                    }
                    self.set_phase(AttemptPhase::Running).await;
                    Ok(())
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
                self.remember(&body);
                self.send_all(body).await;
                self.stop_at =
                    Some(Instant::now() + Duration::from_secs(grace_s.into()) + DEADLINE_MARGIN);
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
                self.remember(&body);
                if let Some(role) = self.primary_role() {
                    self.send(&role, body).await;
                }
                self.stop_at =
                    Some(Instant::now() + Duration::from_secs(deadline_s.into()) + DEADLINE_MARGIN);
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
                self.remember(&body);
                if let Some(role) = self.primary_role() {
                    self.send(&role, body).await;
                }
            }
            Control::Upload {
                command_id,
                checkpoint_id,
                targets,
            } => {
                if checkpoint_id.is_empty() {
                    let ctx = Arc::clone(&self.sh.ctx);
                    let staging = self.staging();
                    let tx = self.uploads_tx.clone();
                    tokio::spawn(async move {
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
                                command_id: Some(command_id),
                                result,
                            })
                            .await;
                    });
                } else {
                    // Saved, so a restarted agent still has the tickets and the command to answer.
                    let plan = self
                        .rec
                        .uploads
                        .entry(checkpoint_id.clone())
                        .or_insert_with(|| UploadPlan {
                            command_id: command_id.clone(),
                            targets: Vec::new(),
                        });
                    plan.command_id = command_id;
                    for t in targets {
                        plan.targets.retain(|x| x.local_path != t.local_path);
                        plan.targets.push(t);
                    }
                    self.save();
                    self.try_commit(&checkpoint_id).await;
                }
            }
            Control::Lease {
                duration_s,
                kill_budget_s,
            } => {
                let margin = i64::from(duration_s.saturating_sub(kill_budget_s)) * 1000;
                self.set_lease(now_ms() + margin);
                if let Some(l) = self.lease_deadline() {
                    self.send_all(l).await;
                }
            }
            Control::Hw(events) => {
                let mine: Vec<HwEvent> = events
                    .into_iter()
                    .filter(|e| self.gpu_uuids.contains(&e.uuid))
                    .collect();
                for e in mine {
                    let fatal = matches!(e.kind, HwEventKind::DeviceLost);
                    self.hw_events.push(e);
                    if fatal {
                        self.stop_all().await;
                    }
                }
            }
            Control::Detach(done) => {
                self.detach().await;
                let _ = done.send(());
            }
        }
    }

    /// True when the attempt is over (every worker ended).
    async fn on_exit(&mut self, exit: WorkerExit) -> bool {
        if let Some(w) = self.workers.remove(&exit.role_id) {
            if w.primary {
                // The primary decides the attempt; the others are stopped now.
                let grace = Duration::from_secs(self.sh.ctx.config.exec.kill_grace_secs);
                for other in self.workers.values() {
                    let _ = other.stop.send(StopMsg::Stop(grace)).await;
                }
                self.primary_exit = Some(exit);
            } else {
                self.secondary_oom |= exit.oom;
            }
        }
        self.workers.is_empty()
    }

    async fn finish(&mut self) -> ExitDecision {
        let p = self.primary_exit.take();
        let ev = ExitEvidence {
            report: self.report.clone(),
            exit_code: p.as_ref().and_then(|p| p.code),
            signal: p.as_ref().and_then(|p| p.signal),
            oom_killed: self.secondary_oom || p.as_ref().is_some_and(|p| p.oom),
            hw_events: self.hw_events.clone(),
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
        self.finish_with(decide(&ev)).await
    }

    async fn finish_with(&mut self, decision: ExitDecision) -> ExitDecision {
        if let Some(s) = self.server.take() {
            s.stop().await;
        }
        self.rec.phase = decision.phase;
        self.rec.exit = Some(decision.clone());
        self.save();
        self.status(Some(&decision)).await;
        let report =
            crate::janitor::release(&self.sh.ctx, &self.sh.envs, &self.rec, &self.gpu_uuids).await;
        let line = serde_json::to_string(&report).unwrap_or_default();
        let _ = std::fs::write(self.job_dir.join("cleanup.json"), line);
        decision
    }
}

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
        let d = dir.display().to_string();
        a.extend(["-v".into(), format!("{d}:{d}")]);
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
async fn container_exit(name: &str) -> (Option<i32>, bool) {
    let Some(docker) = crate::envs::which("docker") else {
        return (None, false);
    };
    let out = tokio::process::Command::new(docker)
        .args([
            "inspect",
            "--format",
            "{{.State.ExitCode}} {{.State.OOMKilled}}",
            name,
        ])
        .stderr(std::process::Stdio::null())
        .output()
        .await;
    let Ok(out) = out else {
        return (None, false);
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.split_whitespace();
    let code = parts.next().and_then(|c| c.parse().ok());
    (code, parts.next() == Some("true"))
}

async fn docker(args: &[&str]) {
    if let Some(d) = crate::envs::which("docker") {
        let _ = tokio::process::Command::new(d)
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
    }
}

/// Watches a worker the agent started.
fn spawn_exec(
    role_id: String,
    mut exec: Box<dyn Execution>,
    container: Option<String>,
    exits: mpsc::Sender<WorkerExit>,
) -> mpsc::Sender<StopMsg> {
    let (tx, mut rx) = mpsc::channel(4);
    tokio::spawn(async move {
        let report = loop {
            tokio::select! {
                r = exec.wait() => break r,
                msg = rx.recv() => match msg {
                    Some(StopMsg::Stop(grace)) => {
                        if let Some(c) = &container {
                            docker(&["stop", "-t", &grace.as_secs().to_string(), c]).await;
                        }
                        break exec.stop(grace).await;
                    }
                    Some(StopMsg::Kill) => {
                        if let Some(c) = &container {
                            docker(&["kill", c]).await;
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
        if let Some(c) = &container {
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

/// Signals a process by identity (its process group too: workers lead their own).
pub fn signal_identity(identity: &ProcIdentity, force: bool) {
    match identity {
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
        ProcIdentity::Container { name } => {
            let name = name.clone();
            let _ = std::process::Command::new(
                crate::envs::which("docker").unwrap_or_else(|| "docker".into()),
            )
            .args([if force { "kill" } else { "stop" }, &name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        }
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
        let alive = |id: &ProcIdentity| {
            let id = id.clone();
            async move {
                tokio::task::spawn_blocking(move || id.alive())
                    .await
                    .unwrap_or(false)
            }
        };
        loop {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(500)) => {
                    if !alive(&identity).await {
                        break;
                    }
                }
                msg = rx.recv() => match msg {
                    Some(StopMsg::Stop(grace)) => {
                        signal_identity(&identity, false);
                        let until = Instant::now() + grace;
                        while Instant::now() < until && alive(&identity).await {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        signal_identity(&identity, true);
                        break;
                    }
                    Some(StopMsg::Kill) => signal_identity(&identity, true),
                    Some(StopMsg::Detach) | None => return,
                },
            }
        }
        let (code, oom) = match &identity {
            ProcIdentity::Container { name } => container_exit(name).await,
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
