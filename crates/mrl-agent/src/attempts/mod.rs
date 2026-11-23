//! The agent attempt runtime: runs attempts end to end on this node.
//!
//! [`Attempts`] takes the attempt commands of `agent.v1.Command` (launch, stage, start, cancel,
//! pause, checkpoint, upload, cleanup, drain, credentials, probe) and runs each attempt in its
//! own task ([`runner`]): stage inputs → materialize environments ([`crate::envs`]) → launch
//! the workers under the `worker` sandbox profile → serve WorkerControl
//! ([`crate::worker_server`]) → relay worker messages into lanes ([`crate::lanes`]) → upload
//! committed checkpoints with tickets → decide how it ended ([`exit`]) → release
//! ([`crate::janitor`]). Everything meant for the core comes out of one [`AttemptEvents`]
//! channel; the gateway (P18) forwards each [`AttemptEvent`] on its lane.
//!
//! Attempt state is saved in `<state>/jobs/<attempt>/attempt.json` ([`record`]), so after an
//! agent restart [`Attempts::readopt`] re-attaches to live workers (pid + start time, or
//! container name, plus the nonce) or reports them lost.

pub mod error;
pub mod exit;
pub mod files;
pub mod local;
pub mod record;
pub mod runner;
pub mod service;
pub mod spec;

pub use error::AttemptError;
pub use spec::AttemptSpec;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{
    agent_message, AttemptPhase, AttemptStatus, Command, CommandResult, Lane, ProbeResult,
    StageItem, WorkerRelay,
};
use mrl_proto::common::v1::{AttemptRef, ClassifiedBy, Failure, FailureClass};
use mrl_proto::worker::v1::ExitReport;
use tokio::sync::{mpsc, oneshot, watch};

use self::exit::ExitDecision;
use self::record::{AttemptRecord, UploadTarget};
use self::runner::{Control, Runner, Shared};
use self::spec::Secret;
use crate::envs::{Envs, ProgressFn};
use crate::hw::HwEvent;
use crate::AgentContext;

/// Something the attempt runtime tells the core.
#[derive(Debug, Clone, PartialEq)]
pub enum AttemptEvent {
    /// A worker message, unchanged, on the lane [`crate::lanes::lane_for`] picked.
    Relay {
        /// Lane.
        lane: Lane,
        /// The relayed message.
        relay: WorkerRelay,
    },
    /// An attempt changed phase or ended (CONTROL lane).
    Status(AttemptStatus),
    /// A command that runs in the background finished (uploads). `failure` set means it failed.
    CommandDone(CommandResult),
    /// Environment Doctor results for a `Probe` command (CONTROL lane).
    Probe(ProbeResult),
}

impl AttemptEvent {
    /// The gateway lane for this event.
    pub fn lane(&self) -> Lane {
        match self {
            Self::Relay { lane, .. } => *lane,
            _ => Lane::Control,
        }
    }

    /// The `agent.v1.AgentMessage` body that carries it.
    pub fn into_body(self) -> agent_message::Body {
        match self {
            Self::Relay { relay, .. } => agent_message::Body::Worker(relay),
            Self::Status(s) => agent_message::Body::AttemptStatus(s),
            Self::CommandDone(r) => agent_message::Body::CommandResult(r),
            Self::Probe(p) => agent_message::Body::ProbeResult(p),
        }
    }
}

/// One event for the gateway and the receipt it signs once the event is saved (journaled).
/// A worker message is acknowledged to the worker, and a saved upload result forgotten, only
/// after [`Envelope::saved`]. An envelope dropped unsaved leaves the event to come again (the
/// worker replays the message; the result is sent again).
#[derive(Debug)]
pub struct Envelope {
    event: AttemptEvent,
    receipt: oneshot::Sender<()>,
}

impl Envelope {
    /// The event.
    pub fn event(&self) -> &AttemptEvent {
        &self.event
    }

    /// Takes the event and tells the runtime it is saved. Call it once the event is durable
    /// on the gateway's side (written to its journal), not before.
    pub fn saved(self) -> AttemptEvent {
        let _ = self.receipt.send(());
        self.event
    }

    /// The event and its receipt, for a gateway that saves later.
    pub fn into_parts(self) -> (AttemptEvent, Receipt) {
        (self.event, Receipt(self.receipt))
    }
}

/// The receipt of an [`Envelope`] taken apart.
#[derive(Debug)]
pub struct Receipt(oneshot::Sender<()>);

impl Receipt {
    /// The event is saved.
    pub fn saved(self) {
        let _ = self.0.send(());
    }
}

/// The receiving end of the attempt runtime's events (the gateway drains it).
pub type AttemptEvents = mpsc::Receiver<Envelope>;
/// The sending end.
pub type AttemptEventSender = mpsc::Sender<Envelope>;

/// A bounded event channel. When the receiver falls behind, workers are acked later and spool
/// on their side, so nothing is lost.
pub fn attempt_events(capacity: usize) -> (AttemptEventSender, AttemptEvents) {
    mpsc::channel(capacity)
}

/// Queues `event`; the returned receiver resolves when the consumer saved it (an error when
/// it was dropped unsaved). `None`: the channel is closed.
pub(crate) async fn send_event(
    tx: &AttemptEventSender,
    event: AttemptEvent,
) -> Option<oneshot::Receiver<()>> {
    let (receipt, saved) = oneshot::channel();
    tx.send(Envelope { event, receipt }).await.ok()?;
    Some(saved)
}

/// What runs once the gateway saved an event the manager posted (see [`post`]).
type OnSaved = Box<dyn FnOnce() + Send>;

/// How the manager (not a runner) hands an event to the gateway: from a background task, so
/// the caller never waits for room in the channel. The gateway may start draining only after
/// [`Attempts::readopt`] or [`Attempts::handle`] returned, while re-adopted runners already
/// fill the channel. `on_saved` runs once the gateway saved the event (never if it was
/// dropped unsaved). Events are delivered in the order of `events`, and their `on_saved`
/// calls run one after another in that order (they may rewrite the same record).
fn post(tx: &AttemptEventSender, events: Vec<(AttemptEvent, Option<OnSaved>)>) {
    let tx = tx.clone();
    tokio::spawn(async move {
        let mut receipts = Vec::with_capacity(events.len());
        for (event, on_saved) in events {
            let Some(receipt) = send_event(&tx, event).await else {
                break;
            };
            receipts.push((receipt, on_saved));
        }
        for (receipt, on_saved) in receipts {
            if let (Ok(()), Some(f)) = (receipt.await, on_saved) {
                f();
            }
        }
    });
}

/// Marks every event saved as it arrives, for local tools and tests that keep no journal
/// (`attempt run`, `attempt stop-all`). The gateway must not use it.
pub fn auto_saved(mut rx: AttemptEvents) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(e) = rx.recv().await {
            e.saved();
        }
    })
}

struct Live {
    attempt: AttemptRef,
    control: mpsc::Sender<Control>,
}

/// Runs and tracks every attempt on this node.
pub struct Attempts {
    shared: Arc<Shared>,
    live: Mutex<HashMap<String, Live>>,
    done: Mutex<HashMap<String, watch::Receiver<Option<ExitDecision>>>>,
    pending: Mutex<HashMap<String, Pending>>,
    epochs: Mutex<HashMap<String, u64>>,
    draining: AtomicBool,
    launching: Mutex<()>,
}

/// What the core sent for an attempt before launching it (`StageArtifacts`,
/// `UpdateCredentials`), for the job and epoch it named. Used by the launch with that same job
/// and epoch, then dropped.
#[derive(Default, Clone)]
struct Pending {
    /// The attempt as those commands named it (`None`: local runs, [`Attempts::set_credential`]).
    attempt: Option<AttemptRef>,
    items: Vec<StageItem>,
    creds: BTreeMap<String, Secret>,
    required_creds: std::collections::BTreeSet<String>,
}

// Keep metadata and staged inputs in one atomic write while keeping secret values in memory.
#[derive(serde::Serialize, serde::Deserialize)]
struct SavedPending {
    staged: Vec<u8>,
    required_creds: std::collections::BTreeSet<String>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl Attempts {
    /// A manager sending its events to `events`. `progress` receives environment install lines.
    pub fn new(
        ctx: Arc<AgentContext>,
        events: AttemptEventSender,
        progress: Option<Arc<ProgressFn>>,
    ) -> Result<Arc<Self>, AttemptError> {
        let envs = Arc::new(Envs::open(ctx.state.root(), Arc::clone(&ctx.cache))?);
        let me = Self {
            shared: Arc::new(Shared {
                ctx,
                envs,
                events,
                progress,
            }),
            live: Mutex::default(),
            done: Mutex::default(),
            pending: Mutex::default(),
            epochs: Mutex::default(),
            draining: AtomicBool::new(false),
            launching: Mutex::new(()),
        };
        // Inputs staged before a restart, for launches still to come.
        if let Ok(rd) = std::fs::read_dir(me.shared.ctx.state.root().join("pending")) {
            for f in rd.flatten() {
                if f.path().extension().and_then(|ext| ext.to_str()) != Some("stage") {
                    continue;
                }
                use prost::Message;
                let bytes =
                    std::fs::read(f.path()).map_err(|e| crate::AgentError::state(f.path(), e))?;
                let saved = serde_json::from_slice::<SavedPending>(&bytes).unwrap_or_else(|_| {
                    SavedPending {
                        staged: bytes,
                        required_creds: Default::default(),
                    }
                });
                let staged = mrl_proto::agent::v1::StageArtifacts::decode(saved.staged.as_slice())
                    .map_err(|_| AttemptError::state("", "could not read saved staging data"))?;
                if let Some(attempt) = staged.attempt {
                    lock(&me.pending).insert(
                        attempt.attempt_id.clone(),
                        Pending {
                            attempt: Some(attempt),
                            items: staged.items,
                            required_creds: saved.required_creds,
                            creds: BTreeMap::new(),
                        },
                    );
                }
            }
        }
        // Every job's highest epoch is known before the first launch is accepted.
        if let Ok(rd) = std::fs::read_dir(me.shared.ctx.state.root().join("jobs")) {
            for dir in rd.flatten() {
                if let Ok(Some(rec)) = AttemptRecord::load(&dir.path()) {
                    me.note_epoch(&rec);
                }
            }
        }
        Ok(Arc::new(me))
    }

    /// The environments folder.
    pub fn envs(&self) -> &Arc<Envs> {
        &self.shared.envs
    }

    /// Starts an attempt. Returns once it is accepted; it runs in the background.
    pub fn launch(self: &Arc<Self>, spec: AttemptSpec) -> Result<(), AttemptError> {
        let _one_launch = lock(&self.launching);
        self.launch_locked(spec)
    }

    /// [`Self::launch`] with `launching` held: one launch (or stage) at a time, so the
    /// "already exists" check and the saved record that claims the id happen together.
    fn launch_locked(self: &Arc<Self>, spec: AttemptSpec) -> Result<(), AttemptError> {
        spec.validate()?;
        let id = spec.attempt.attempt_id.clone();
        if self.draining.load(Ordering::SeqCst) {
            return Err(AttemptError::Draining("the machine is draining".into()));
        }
        let seen = lock(&self.epochs)
            .get(&spec.attempt.job_id)
            .copied()
            .unwrap_or(0);
        if spec.attempt.fencing_epoch < seen {
            return Err(AttemptError::state(
                &id,
                format!(
                    "fencing epoch {} is older than epoch {seen} already seen for this job",
                    spec.attempt.fencing_epoch
                ),
            ));
        }
        let job_dir = self.shared.ctx.state.job_dir(&id);
        if lock(&self.live).contains_key(&id) {
            return Err(AttemptError::state(&id, "already exists on this machine"));
        }
        // Claimed first, so the "no record yet" decision can't race another manager.
        let claim = self.claim(&id)?;
        if AttemptRecord::load(&job_dir)?.is_some() {
            return Err(AttemptError::state(&id, "already exists on this machine"));
        }
        if let Some(a) = lock(&self.pending).get(&id).and_then(|p| p.attempt.clone()) {
            if a.job_id != spec.attempt.job_id || a.fencing_epoch != spec.attempt.fencing_epoch {
                return Err(AttemptError::state(
                    &id,
                    format!(
                        "its inputs or credentials were sent for job {} epoch {}, not job {} epoch {}",
                        a.job_id, a.fencing_epoch, spec.attempt.job_id, spec.attempt.fencing_epoch
                    ),
                ));
            }
        }
        let secrets = self.secrets_for(&spec)?;
        let rec_lease_s = spec.lease_s;
        let rec = AttemptRecord {
            socket: runner::socket_path(self.shared.ctx.state.root(), &id),
            nonce: runner::new_nonce()?,
            spec,
            phase: AttemptPhase::Unspecified as i32,
            workers: Vec::new(),
            prepared: Vec::new(),
            started: Vec::new(),
            last_step: 0,
            checkpoints: BTreeMap::new(),
            cancel_requested: false,
            exit: None,
            staged_paths: BTreeMap::new(),
            // Fixed now: staging and environment installs count against the lease.
            lease_deadline_unix_ms: (rec_lease_s > 0)
                .then(|| runner::now_ms() + i64::from(rec_lease_s) * 1000),
            lease_hard_deadline_unix_ms: (rec_lease_s > 0)
                .then(|| runner::now_ms() + i64::from(rec_lease_s) * 1000),
            lease_heartbeat_seq: 0,
            stop_deadline_unix_ms: None,
            exit_report: None,
            requests: Vec::new(),
            uploads: BTreeMap::new(),
            results: BTreeMap::new(),
            adhoc_uploads: BTreeMap::new(),
            hw_events: Vec::new(),
        };
        record::private_dir(&job_dir).map_err(|e| crate::AgentError::state(&job_dir, e))?;
        rec.save(&job_dir)?;
        // Accepted (its record is saved): from now on this job's older epochs are refused.
        self.note_epoch(&rec);
        // Used: the staged inputs are in the spec, the credentials in `secrets`.
        lock(&self.pending).remove(&id);
        let _ = std::fs::remove_file(self.pending_path(&id));
        let runner = Runner::new(Arc::clone(&self.shared), rec.clone(), secrets);
        self.spawn(&rec, false, runner, claim);
        Ok(())
    }

    /// Claims `attempt_id` for this manager: an exclusive lock on `<state>/locks/<id>.lock`,
    /// held while the attempt runs here. Another agent (or `attempt run`) on the same state
    /// folder then leaves it alone; the lock goes when the runner ends, is detached, or the
    /// process dies.
    fn claim(&self, attempt_id: &str) -> Result<std::fs::File, AttemptError> {
        let dir = self.shared.ctx.state.root().join("locks");
        std::fs::create_dir_all(&dir).map_err(|e| crate::AgentError::state(&dir, e))?;
        let path = dir.join(format!("{attempt_id}.lock"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| crate::AgentError::state(&path, e))?;
        match file.try_lock() {
            Ok(()) => Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => Err(AttemptError::state(
                attempt_id,
                "is run by another mrl-agent on this machine",
            )),
            Err(std::fs::TryLockError::Error(e)) => Err(crate::AgentError::state(&path, e).into()),
        }
    }

    fn spawn(
        self: &Arc<Self>,
        rec: &AttemptRecord,
        readopt: bool,
        runner: Runner,
        claim: std::fs::File,
    ) {
        let id = rec.spec.attempt.attempt_id.clone();
        let (control, rx) = mpsc::channel(32);
        let (tx_done, rx_done) = watch::channel(None);
        lock(&self.live).insert(
            id.clone(),
            Live {
                attempt: rec.spec.attempt.to_proto(),
                control,
            },
        );
        lock(&self.done).insert(id.clone(), rx_done);
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let decision = if readopt {
                runner.readopt(rx).await
            } else {
                runner.run(rx).await
            };
            lock(&me.live).remove(&id);
            drop(claim);
            let _ = tx_done.send(decision);
        });
    }

    /// The worker's secret environment: the spec's grants, plus every credential the core sent
    /// for this attempt with `UpdateCredentials` (as `MRL_CREDENTIAL_<NAME>`).
    pub fn secrets_for(&self, spec: &AttemptSpec) -> Result<Vec<(String, Secret)>, AttemptError> {
        let pending = lock(&self.pending);
        if let Some(p) = pending.get(&spec.attempt.attempt_id) {
            if p.required_creds
                .iter()
                .any(|name| !p.creds.contains_key(name))
            {
                return Err(AttemptError::CredentialsLost);
            }
        }
        let given = pending.get(&spec.attempt.attempt_id).map(|p| &p.creds);
        let granted: Vec<String> = spec.secrets.iter().map(|g| g.name.clone()).collect();
        let sent = given
            .into_iter()
            .flatten()
            .filter(|(name, _)| !granted.contains(name))
            .map(|(name, v)| Ok((spec::credential_env(name), v.clone())));
        spec.secrets
            .iter()
            .map(|g| {
                let env = g.env_var();
                let value = given
                    .and_then(|c| c.get(&g.name).cloned())
                    // Local runs: the value comes from the agent's own environment.
                    .or_else(|| std::env::var(&env).ok().map(Secret::new))
                    .ok_or_else(|| {
                        AttemptError::Spec(format!("the secret {} was not provided", g.name))
                    })?;
                Ok((env, value))
            })
            .chain(sent)
            .collect()
    }

    /// Waits until the attempt ends. `None` if it is unknown here or was detached.
    pub async fn wait(&self, attempt_id: &str) -> Option<ExitDecision> {
        let mut rx = lock(&self.done).get(attempt_id)?.clone();
        let decision = rx.wait_for(Option::is_some).await.ok()?.clone();
        decision
    }

    /// Attempts running now.
    pub fn running(&self) -> Vec<AttemptRef> {
        lock(&self.live)
            .values()
            .map(|l| l.attempt.clone())
            .collect()
    }

    /// Hands a command to its attempt's runner and returns once the runner saved its effect.
    /// The command must name the attempt's job and its fencing epoch; with `newer_ok` (a
    /// cancel), a newer epoch of the same job may also stop it.
    async fn control(
        &self,
        attempt: Option<&AttemptRef>,
        c: Control,
        newer_ok: bool,
    ) -> Result<(), AttemptError> {
        let r = attempt.ok_or_else(|| AttemptError::Spec("the command names no attempt".into()))?;
        let tx = {
            let live = lock(&self.live);
            let l = live.get(&r.attempt_id).ok_or_else(|| {
                AttemptError::state(&r.attempt_id, "is not running on this machine")
            })?;
            if r.job_id != l.attempt.job_id {
                return Err(AttemptError::state(
                    &r.attempt_id,
                    format!("belongs to job {}, not {}", l.attempt.job_id, r.job_id),
                ));
            }
            let epoch_ok = if newer_ok {
                r.fencing_epoch >= l.attempt.fencing_epoch
            } else {
                r.fencing_epoch == l.attempt.fencing_epoch
            };
            if !epoch_ok {
                return Err(AttemptError::state(
                    &r.attempt_id,
                    format!(
                        "runs under fencing epoch {}, not the command's {}",
                        l.attempt.fencing_epoch, r.fencing_epoch
                    ),
                ));
            }
            l.control.clone()
        };
        let ended = || AttemptError::state(&r.attempt_id, "has just ended");
        // Start answers on its own channel once the barrier opened.
        if matches!(c, Control::Start(_)) {
            return tx.send(c).await.map_err(|_| ended());
        }
        let (saved, done) = oneshot::channel();
        tx.send(Control::Acked {
            inner: Box::new(c),
            saved,
        })
        .await
        .map_err(|_| ended())?;
        match done.await {
            Ok(true) => Ok(()),
            Ok(false) => Err(AttemptError::state(
                &r.attempt_id,
                "could not save the command; send it again",
            )),
            Err(_) => Err(ended()),
        }
    }

    /// Renews a lease from the gateway's `LeaseGrant`: `heartbeat_seq` is the grant's
    /// `heartbeat_seq` and `heartbeat_sent` when the gateway sent that heartbeat (its
    /// monotonic clock). The deadline runs from that send time; older grants are ignored.
    pub async fn renew_lease(
        &self,
        attempt: &AttemptRef,
        heartbeat_seq: u64,
        heartbeat_sent: std::time::Instant,
        duration_s: u32,
        kill_budget_s: u32,
    ) -> Result<(), AttemptError> {
        self.control(
            Some(attempt),
            Control::Lease {
                heartbeat_seq,
                heartbeat_sent,
                duration_s,
                kill_budget_s,
            },
            false,
        )
        .await
    }

    /// Passes GPU events (from the telemetry reader) to the attempts using those GPUs. They
    /// are exit evidence, so none is dropped: a runner whose control queue is full gets them
    /// once it has room (called outside a tokio runtime, only if it has room now).
    pub fn hw_events(&self, events: &[HwEvent]) {
        if events.is_empty() {
            return;
        }
        let rt = tokio::runtime::Handle::try_current().ok();
        for l in lock(&self.live).values() {
            let (control, events) = (l.control.clone(), events.to_vec());
            match &rt {
                Some(rt) => {
                    rt.spawn(async move {
                        let _ = control.send(Control::Hw(events)).await;
                    });
                }
                None => {
                    let _ = control.try_send(Control::Hw(events));
                }
            }
        }
    }

    /// Leaves every worker running and stops managing them (agent shutdown); the next agent
    /// re-adopts them.
    pub async fn detach_all(&self) {
        let controls: Vec<mpsc::Sender<Control>> = lock(&self.live)
            .values()
            .map(|l| l.control.clone())
            .collect();
        futures::future::join_all(controls.into_iter().map(|c| async move {
            let (tx, rx) = oneshot::channel();
            if c.send(Control::Detach(tx)).await.is_ok() {
                let _ = rx.await;
            }
        }))
        .await;
    }

    /// Remembers the highest fencing epoch seen for the record's job.
    fn note_epoch(&self, rec: &AttemptRecord) {
        lock(&self.epochs)
            .entry(rec.spec.attempt.job_id.clone())
            .and_modify(|e| *e = (*e).max(rec.spec.attempt.fencing_epoch))
            .or_insert(rec.spec.attempt.fencing_epoch);
    }

    /// After an agent restart: re-attaches to attempts whose workers still run, reports the
    /// others lost, and reports attempts that ended earlier with their saved outcome. Returns
    /// the status of each (for the reconcile report).
    pub async fn readopt(self: &Arc<Self>) -> Vec<AttemptStatus> {
        self.readopt_all(true).await
    }

    async fn readopt_for_session(self: &Arc<Self>) -> Vec<AttemptStatus> {
        self.readopt_all(false).await
    }

    async fn readopt_all(self: &Arc<Self>, emit_snapshot: bool) -> Vec<AttemptStatus> {
        let statuses = self.readopt_records(|_| true, emit_snapshot).await;
        if let Ok(entries) = std::fs::read_dir(self.probe_dir()) {
            for entry in entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "probe"))
            {
                use prost::Message;
                if let Ok(bytes) = std::fs::read(entry.path()) {
                    if let Ok(probe) = mrl_proto::agent::v1::ProbeResult::decode(bytes.as_slice()) {
                        self.post_probe(entry.path(), probe);
                    }
                }
            }
        }
        statuses
    }

    fn probe_dir(&self) -> std::path::PathBuf {
        self.shared.ctx.state.root().join("probes")
    }

    fn post_probe(&self, path: std::path::PathBuf, probe: mrl_proto::agent::v1::ProbeResult) {
        post(
            &self.shared.events,
            vec![(
                AttemptEvent::Probe(probe),
                Some(Box::new(move || {
                    let _ = std::fs::remove_file(path);
                })),
            )],
        );
    }

    fn save_probe(
        &self,
        command_id: &str,
        probe: mrl_proto::agent::v1::ProbeResult,
    ) -> Result<(), AttemptError> {
        let dir = self.probe_dir();
        let path = dir.join(format!(
            "{}.probe",
            mrl_storage::sha256_hex(command_id.as_bytes())
        ));
        record::private_dir(&dir)
            .and_then(|()| record::write_private(&path, &prost::Message::encode_to_vec(&probe)))
            .map_err(|e| crate::AgentError::state(&path, e))?;
        self.post_probe(path, probe);
        Ok(())
    }

    /// [`Self::readopt`] for the attempts whose id `wanted` accepts (a local `attempt run`
    /// takes only its own). Attempts another manager runs (claimed) are skipped.
    pub async fn readopt_where(
        self: &Arc<Self>,
        wanted: impl Fn(&str) -> bool,
    ) -> Vec<AttemptStatus> {
        self.readopt_records(wanted, true).await
    }

    async fn readopt_records(
        self: &Arc<Self>,
        wanted: impl Fn(&str) -> bool,
        emit_snapshot: bool,
    ) -> Vec<AttemptStatus> {
        let jobs = self.shared.ctx.state.root().join("jobs");
        let Ok(rd) = std::fs::read_dir(&jobs) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for dir in rd.flatten().map(|e| e.path()) {
            let Ok(Some(mut rec)) = AttemptRecord::load(&dir) else {
                continue;
            };
            let id = rec.spec.attempt.attempt_id.clone();
            self.note_epoch(&rec);
            if !wanted(&id) || lock(&self.live).contains_key(&id) {
                continue;
            }
            if let Some(d) = rec.exit.clone() {
                // Ended before the restart: reported again, in case its status never got out.
                out.push(status_of(&rec, Some(&d)));
                // Only once nobody else holds it (its runner may still be finishing).
                if !rec.results.is_empty()
                    || !rec.adhoc_uploads.is_empty()
                    || rec.uploads.values().any(|plan| !plan.answered)
                {
                    if let Ok(_claim) = self.claim(&id) {
                        if let Ok(Some(mut fresh)) = AttemptRecord::load(&dir) {
                            self.replay_results(&dir, &mut fresh);
                        }
                    }
                }
                continue;
            }
            // Still managed by another agent process: not ours to adopt or declare lost.
            let Ok(claim) = self.claim(&id) else {
                continue;
            };
            // Decided from the record as it is now that it is ours: the owner may have
            // finished in between.
            let Ok(Some(fresh)) = AttemptRecord::load(&dir) else {
                continue;
            };
            rec = fresh;
            if let Some(d) = rec.exit.clone() {
                out.push(status_of(&rec, Some(&d)));
                self.replay_results(&dir, &mut rec);
                drop(claim);
                continue;
            }
            // Keep identities that could not be verified so the janitor can retry cleanup.
            let token = runner::attempt_token(&rec.nonce);
            let mut alive = HashSet::new();
            let mut unknown = false;
            for worker in &mut rec.workers {
                match worker.identity.verified(&token).await {
                    Ok(Some(identity)) => {
                        if record::running(&identity).await {
                            alive.insert(worker.role_id.clone());
                        }
                        worker.identity = identity;
                    }
                    Ok(None) => {}
                    Err(()) => unknown = true,
                }
            }
            let _ = rec.save(&dir);
            let primary_alive = rec
                .workers
                .iter()
                .any(|w| w.primary && alive.contains(&w.role_id));
            let complete = rec
                .spec
                .workers()
                .iter()
                .all(|(id, _, _)| alive.contains(id));
            // A restarted Windows agent cannot take back the same localhost endpoint.
            let readoptable = primary_alive && complete && cfg!(unix);
            let status = if readoptable {
                let runner = Runner::new(Arc::clone(&self.shared), rec.clone(), Vec::new());
                self.spawn(&rec, true, runner, claim);
                status_of(&rec, None)
            } else {
                let why = if unknown {
                    "The agent could not verify the container after restart. Cleanup will check it again."
                } else if !complete && primary_alive {
                    "Some training workers did not survive the restart, so the attempt cannot continue."
                } else if primary_alive {
                    "The agent restarted; on Windows it can't reconnect to a running training worker, so it stopped it."
                } else {
                    "The agent restarted and the training worker was no longer running."
                };
                let st = self.recover_ended(&dir, &mut rec, why).await;
                drop(claim);
                st
            };
            // The session sends this snapshot in its reconcile report. A separate status
            // could land after a newer transition from the re-adopted runner.
            if emit_snapshot {
                post(
                    &self.shared.events,
                    vec![(AttemptEvent::Status(status.clone()), None)],
                );
            }
            out.push(status);
        }
        out
    }

    /// The one way an attempt that ended (or can't go on) while no agent ran it is closed:
    /// the exit evidence that survived (the saved ExitReport, a container's exit code and OOM
    /// flag, a job cgroup's OOM count) decides; without any, it is WORKER_LOST with `lost_why`.
    /// Every upload command still waiting is failed, the outcome saved, the workers stopped
    /// and cleaned up, and the saved results sent.
    async fn recover_ended(
        &self,
        dir: &std::path::Path,
        rec: &mut AttemptRecord,
        lost_why: &str,
    ) -> AttemptStatus {
        let (mut exit_code, mut oom_killed) = (None, false);
        let token = runner::attempt_token(&rec.nonce);
        for w in &rec.workers {
            if matches!(w.identity, record::ProcIdentity::Container { .. }) {
                if let Ok(Some(record::ProcIdentity::Container { id: Some(cid), .. })) =
                    w.identity.verified(&token).await
                {
                    let (code, oom) = runner::container_exit(&cid).await;
                    if w.primary {
                        exit_code = code;
                    }
                    oom_killed |= oom;
                }
            }
            oom_killed |= w.cgroup.as_deref().is_some_and(cgroup_oom_killed);
        }
        let report = rec
            .exit_report
            .as_deref()
            .and_then(|b| <ExitReport as prost::Message>::decode(b).ok());
        let evidence = exit::ExitEvidence {
            report,
            exit_code,
            oom_killed,
            hw_events: rec.hw_events.clone(),
            readopted: true,
            cancel_requested: rec.cancel_requested,
            final_committed: rec.final_committed(),
            needs_final_checkpoint: rec.needs_final_checkpoint(),
            ..exit::ExitEvidence::default()
        };
        let d = if evidence.report.is_some() || exit::evidence_failure(&evidence).is_some() {
            exit::decide(&evidence)
        } else {
            ExitDecision {
                phase: AttemptPhase::Exited as i32,
                exit_kind: mrl_proto::worker::v1::ExitKind::Failed as i32,
                exit_code: -1,
                failure: Some(Failure {
                    class: FailureClass::NodeLost as i32,
                    sub_code: "WORKER_LOST".into(),
                    message: lost_why.to_owned(),
                    retryable_hint: true,
                    classified_by: ClassifiedBy::Agent as i32,
                    ..Failure::default()
                }),
            }
        };
        rec.phase = d.phase;
        rec.exit = Some(d.clone());
        let _ = rec.save(dir);
        let _ = crate::janitor::release(&self.shared.ctx, &self.shared.envs, rec, &HashSet::new())
            .await;
        self.replay_results(dir, rec);
        status_of(rec, Some(&d))
    }

    /// Sends the upload results an ended attempt saved but never got out (its agent stopped
    /// in between), then forgets them.
    fn replay_results(&self, dir: &std::path::Path, rec: &mut AttemptRecord) {
        // Uploads still saved as running when it ended can't be resumed any more: they fail.
        for (command_id, _) in std::mem::take(&mut rec.adhoc_uploads) {
            rec.results.insert(
                command_id,
                Some("the agent stopped during the upload".into()),
            );
        }
        for plan in rec.uploads.values_mut().filter(|p| !p.answered) {
            plan.answered = true;
            rec.results.insert(
                plan.command_id.clone(),
                Some("the attempt ended before its checkpoint was uploaded".into()),
            );
        }
        let _ = rec.save(dir);
        // Handed over without waiting (see `post`); each is removed from the record once the
        // gateway saved it.
        let events = rec
            .results
            .clone()
            .into_iter()
            .map(|(command_id, failure)| {
                let r = runner::upload_result(command_id.clone(), failure);
                let dir = dir.to_path_buf();
                let forget: OnSaved = Box::new(move || {
                    if let Ok(Some(mut saved)) = AttemptRecord::load(&dir) {
                        if saved.results.remove(&command_id).is_some() {
                            let _ = saved.save(&dir);
                        }
                    }
                });
                (AttemptEvent::CommandDone(r), Some(forget))
            })
            .collect();
        post(&self.shared.events, events);
    }

    /// Handles an attempt command. `None` for commands this runtime does not own (deployments).
    pub async fn handle(self: &Arc<Self>, cmd: &Command) -> Option<CommandResult> {
        let result = match cmd.kind.as_ref()? {
            Kind::LaunchAttempt(l) => {
                let id = l
                    .attempt
                    .as_ref()
                    .map(|a| a.attempt_id.clone())
                    .unwrap_or_default();
                // Staged inputs stay until the launch is accepted, so a retry still has them.
                // Staging waits on the same lock, so every accepted stage is in the spec.
                let _one_launch = lock(&self.launching);
                let staged = lock(&self.pending)
                    .get(&id)
                    .map(|p| p.items.clone())
                    .unwrap_or_default();
                AttemptSpec::from_launch(l, &staged, self.shared.ctx.state.root())
                    .and_then(|spec| self.launch_locked(spec))
            }
            Kind::StageArtifacts(s) => {
                // Under the launch lock: a stage either lands before a launch copies the list,
                // or sees the launch's record and is refused.
                let _one_launch = lock(&self.launching);
                // A repeat of the same input (by name) replaces it: retries are harmless.
                self.add_pending(s.attempt.as_ref(), |p| {
                    p.items
                        .retain(|have| !s.items.iter().any(|new| new.name == have.name));
                    p.items.extend(s.items.iter().cloned());
                    Ok(())
                })
            }
            Kind::StartAttempt(s) => {
                let (tx, rx) = oneshot::channel();
                match self
                    .control(s.attempt.as_ref(), Control::Start(tx), false)
                    .await
                {
                    Ok(()) => rx
                        .await
                        .unwrap_or_else(|_| Err(AttemptError::state("", "ended"))),
                    Err(e) => Err(e),
                }
            }
            Kind::CancelAttempt(c) => {
                self.control(
                    c.attempt.as_ref(),
                    Control::Cancel {
                        request_id: c.request_id.clone(),
                        grace_s: c.grace_s,
                        reason: c.reason.clone(),
                    },
                    true,
                )
                .await
            }
            Kind::PauseAttempt(p) => {
                self.control(
                    p.attempt.as_ref(),
                    Control::Pause {
                        request_id: p.request_id.clone(),
                        deadline_s: p.deadline_s,
                        reason: p.reason,
                    },
                    false,
                )
                .await
            }
            Kind::CheckpointAttempt(c) => {
                self.control(
                    c.attempt.as_ref(),
                    Control::Checkpoint {
                        request_id: c.request_id.clone(),
                        deadline_s: c.deadline_s,
                        reason: c.reason.clone(),
                    },
                    false,
                )
                .await
            }
            Kind::UploadArtifacts(u) => {
                match u
                    .items
                    .iter()
                    .map(|i| spec::safe_relative(&i.local_path).map(|_| UploadTarget::from_item(i)))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(targets) => {
                        self.control(
                            u.attempt.as_ref(),
                            Control::Upload {
                                command_id: cmd.command_id.clone(),
                                checkpoint_id: u.checkpoint_id.clone(),
                                targets,
                            },
                            false,
                        )
                        .await
                    }
                    Err(e) => Err(e),
                }
            }
            Kind::Cleanup(c) => self.cleanup(c).await,
            Kind::Drain(d) => {
                self.draining.store(true, Ordering::SeqCst);
                if d.cancel_running {
                    let running = self.running();
                    for r in running {
                        let _ = self
                            .control(
                                Some(&r),
                                Control::Cancel {
                                    request_id: cmd.command_id.clone(),
                                    grace_s: u32::try_from(
                                        self.shared.ctx.config.exec.kill_grace_secs,
                                    )
                                    .unwrap_or(10),
                                    reason: d.reason.clone(),
                                },
                                true,
                            )
                            .await;
                    }
                }
                Ok(())
            }
            Kind::UpdateCredentials(u) => {
                let _one_launch = lock(&self.launching);
                self.add_pending(u.attempt.as_ref(), |p| {
                    for c in &u.credentials {
                        let v = String::from_utf8(c.value.clone()).map_err(|_| {
                            AttemptError::Spec(format!("credential {} is not text", c.name))
                        })?;
                        p.required_creds.insert(c.name.clone());
                        p.creds.insert(c.name.clone(), Secret::new(v));
                    }
                    Ok(())
                })
            }
            Kind::Probe(p) => {
                let facts = crate::doctor::Facts::gather(&self.shared.ctx, &self.shared.envs).await;
                let report = crate::doctor::run(&facts);
                let result = report.to_proto(
                    &p.request_id,
                    &p.checks,
                    &facts.inventory,
                    self.shared.ctx.state.root(),
                );
                self.save_probe(&cmd.command_id, result)
            }
            Kind::LaunchDeployment(_) | Kind::StopDeployment(_) => return None,
        };
        Some(CommandResult {
            command_id: cmd.command_id.clone(),
            accepted: result.is_ok(),
            failure: result.err().map(|e| e.to_failure()),
        })
    }

    async fn cleanup(&self, c: &mrl_proto::agent::v1::Cleanup) -> Result<(), AttemptError> {
        let ctx = &self.shared.ctx;
        // Node-wide only when no attempt is named at all.
        let id = c
            .attempt
            .as_ref()
            .map(|a| a.attempt_id.clone())
            .unwrap_or_default();
        if c.attempt.is_some() && id.is_empty() {
            return Err(AttemptError::Spec(
                "the cleanup names an attempt without an id; leave the attempt out for a node-wide cleanup".into(),
            ));
        }
        // The id names a folder: anything but a plain name (like `../x`) is refused.
        if !id.is_empty() && !spec::safe_name(&id) {
            return Err(AttemptError::Spec(format!(
                "attempt id {id:?} may use only letters, digits, '.', '_' and '-'"
            )));
        }
        if id.is_empty() {
            let live: HashSet<String> = lock(&self.live).keys().cloned().collect();
            crate::janitor::sweep(ctx, &self.shared.envs, &live).await?;
            if c.purge_scratch {
                // Asked for explicitly: every ended attempt's scratch now, not only old ones.
                // Running attempts (and ones waiting to be re-adopted) are left alone.
                let jobs = ctx.state.root().join("jobs");
                for dir in std::fs::read_dir(&jobs)
                    .map_err(|e| crate::AgentError::state(&jobs, e))?
                    .flatten()
                    .map(|e| e.path())
                {
                    let name = dir
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned();
                    let ended = AttemptRecord::load(&dir)
                        .ok()
                        .flatten()
                        .is_some_and(|r| r.exit.is_some());
                    // Only attempts nobody runs (claimable) are purged.
                    if ended && !live.contains(&name) {
                        if let Ok(_claim) = self.claim(&name) {
                            crate::janitor::purge(ctx.state.root(), &dir)?;
                        }
                    }
                }
            }
        } else {
            if lock(&self.live).contains_key(&id) {
                return Err(AttemptError::state(
                    &id,
                    "is still running; cancel it first",
                ));
            }
            // Held for the whole cleanup: an attempt another manager runs is refused, and the
            // record is read only once it can no longer change.
            let _claim = self.claim(&id).map_err(|_| {
                AttemptError::state(
                    &id,
                    "is still running (in another mrl-agent); cancel it first",
                )
            })?;
            let dir = ctx.state.job_dir(&id);
            let rec = AttemptRecord::load(&dir)?;
            if let (Some(rec), Some(r)) = (&rec, c.attempt.as_ref()) {
                // The whole AttemptRef must fit what is saved: the same job, no older epoch.
                let saved = &rec.spec.attempt;
                if r.job_id != saved.job_id || r.fencing_epoch < saved.fencing_epoch {
                    return Err(AttemptError::state(
                        &id,
                        format!(
                            "is job {} epoch {}, not job {} epoch {}",
                            saved.job_id, saved.fencing_epoch, r.job_id, r.fencing_epoch
                        ),
                    ));
                }
            }
            if let Some(rec) = &rec {
                crate::janitor::release(ctx, &self.shared.envs, rec, &HashSet::new()).await;
            }
            if c.purge_scratch {
                crate::janitor::purge(ctx.state.root(), &dir)?;
            }
        }
        if c.cache_limit_bytes > 0 {
            // Node cache plus environments, down to the limit; pinned data stays.
            self.shared.envs.prune_to(c.cache_limit_bytes)?;
        }
        Ok(())
    }

    /// Stores a credential for an attempt that has not launched yet (local tools and tests).
    pub fn set_credential(&self, attempt_id: &str, name: &str, value: Secret) {
        lock(&self.pending)
            .entry(attempt_id.to_owned())
            .or_default()
            .creds
            .insert(name.to_owned(), value);
    }

    /// Where an attempt's staged inputs wait for its launch.
    fn pending_path(&self, attempt_id: &str) -> std::path::PathBuf {
        self.shared
            .ctx
            .state
            .root()
            .join("pending")
            .join(format!("{attempt_id}.stage"))
    }

    /// True once `attempt_id` was launched here (running, or its record exists).
    fn launched(&self, attempt_id: &str) -> bool {
        lock(&self.live).contains_key(attempt_id)
            || AttemptRecord::load(&self.shared.ctx.state.job_dir(attempt_id))
                .ok()
                .flatten()
                .is_some()
    }

    /// Keeps data sent ahead of a launch for `r` (call with `launching` held). The attempt must
    /// not be launched yet, and `r` must agree with what earlier commands for it said: the same
    /// job, an epoch no older (a newer epoch drops what the older one sent) and no older than
    /// the job's highest epoch seen here.
    fn add_pending(
        &self,
        r: Option<&AttemptRef>,
        add: impl FnOnce(&mut Pending) -> Result<(), AttemptError>,
    ) -> Result<(), AttemptError> {
        let r = r.ok_or_else(|| AttemptError::Spec("the command names no attempt".into()))?;
        let id = r.attempt_id.as_str();
        if !spec::safe_name(id) || r.job_id.is_empty() {
            return Err(AttemptError::Spec(format!(
                "the command names an invalid attempt {id:?} of job {:?}",
                r.job_id
            )));
        }
        if self.launched(id) {
            return Err(AttemptError::state(
                id,
                "is already launched; this applies only before the launch",
            ));
        }
        let seen = lock(&self.epochs).get(&r.job_id).copied().unwrap_or(0);
        let stale = |had: u64| {
            AttemptError::state(
                id,
                format!(
                    "the command's fencing epoch {} is older than {had}",
                    r.fencing_epoch
                ),
            )
        };
        if r.fencing_epoch < seen {
            return Err(stale(seen));
        }
        let mut pending = lock(&self.pending);
        let current = pending.get(id).cloned().unwrap_or_default();
        // The change is made on a copy and kept only if all of it is valid: a rejected
        // command leaves what was there (and its epoch) untouched.
        let mut next = match &current.attempt {
            Some(a) if a.job_id != r.job_id => {
                return Err(AttemptError::state(
                    id,
                    format!("was announced for job {}, not {}", a.job_id, r.job_id),
                ));
            }
            Some(a) if r.fencing_epoch < a.fencing_epoch => return Err(stale(a.fencing_epoch)),
            Some(a) if r.fencing_epoch > a.fencing_epoch => Pending::default(),
            _ => current,
        };
        next.attempt = Some(r.clone());
        add(&mut next)?;
        // Staged inputs are saved before the command is accepted, so a restarted agent still
        // has them for the launch. Credentials are never written down: after a restart the
        // core sends them again.
        let staged = mrl_proto::agent::v1::StageArtifacts {
            attempt: next.attempt.clone(),
            items: next.items.clone(),
        };
        let saved = serde_json::to_vec(&SavedPending {
            staged: prost::Message::encode_to_vec(&staged),
            required_creds: next.required_creds.clone(),
        })
        .map_err(|_| AttemptError::state(id, "could not encode the staged inputs"))?;
        let path = self.pending_path(id);
        record::private_dir(path.parent().unwrap_or(&path))
            .and_then(|()| record::write_private(&path, &saved))
            .map_err(|e| crate::AgentError::state(&path, e))?;
        pending.insert(id.to_owned(), next);
        Ok(())
    }
}

/// True when the kernel OOM-killed something in the job cgroup at `cg` (Linux
/// `memory.events`); false when it can't tell.
fn cgroup_oom_killed(cg: &std::path::Path) -> bool {
    std::fs::read_to_string(cg.join("memory.events")).is_ok_and(|t| {
        t.lines().any(|l| {
            l.strip_prefix("oom_kill ")
                .and_then(|n| n.trim().parse::<u64>().ok())
                .is_some_and(|n| n > 0)
        })
    })
}

fn status_of(rec: &AttemptRecord, d: Option<&ExitDecision>) -> AttemptStatus {
    AttemptStatus {
        attempt: Some(rec.spec.attempt.to_proto()),
        phase: rec.phase,
        last_step: rec.last_step,
        exit_kind: d.map_or(0, |d| d.exit_kind),
        exit_code: d.map_or(0, |d| d.exit_code),
        failure: d.and_then(|d| d.failure.clone()),
        time: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_map_to_lanes_and_bodies() {
        let e = AttemptEvent::Relay {
            lane: Lane::Bulk,
            relay: WorkerRelay::default(),
        };
        assert_eq!(e.lane(), Lane::Bulk);
        assert!(matches!(e.into_body(), agent_message::Body::Worker(_)));
        let s = AttemptEvent::Status(AttemptStatus::default());
        assert_eq!(s.lane(), Lane::Control);
        assert!(matches!(
            s.into_body(),
            agent_message::Body::AttemptStatus(_)
        ));
    }
}
