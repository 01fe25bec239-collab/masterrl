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

/// The receiving end of the attempt runtime's events (the gateway drains it).
pub type AttemptEvents = mpsc::Receiver<AttemptEvent>;
/// The sending end.
pub type AttemptEventSender = mpsc::Sender<AttemptEvent>;

/// A bounded event channel. When the receiver falls behind, workers are acked later and spool
/// on their side, so nothing is lost.
pub fn attempt_events(capacity: usize) -> (AttemptEventSender, AttemptEvents) {
    mpsc::channel(capacity)
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
#[derive(Default)]
struct Pending {
    /// The attempt as those commands named it (`None`: local runs, [`Attempts::set_credential`]).
    attempt: Option<AttemptRef>,
    items: Vec<StageItem>,
    creds: BTreeMap<String, Secret>,
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
        if lock(&self.live).contains_key(&id) || AttemptRecord::load(&job_dir)?.is_some() {
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
        let claim = self.claim(&id)?;
        // Every check passed: from now on this job's older epochs are refused.
        lock(&self.epochs)
            .entry(spec.attempt.job_id.clone())
            .and_modify(|e| *e = (*e).max(spec.attempt.fencing_epoch))
            .or_insert(spec.attempt.fencing_epoch);
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
            stop_deadline_unix_ms: None,
            exit_report: None,
            requests: Vec::new(),
            uploads: BTreeMap::new(),
        };
        record::private_dir(&job_dir).map_err(|e| crate::AgentError::state(&job_dir, e))?;
        rec.save(&job_dir)?;
        // Used: the staged inputs are in the spec, the credentials in `secrets`.
        lock(&self.pending).remove(&id);
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

    async fn control(&self, attempt: Option<&AttemptRef>, c: Control) -> Result<(), AttemptError> {
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
            if r.fencing_epoch < l.attempt.fencing_epoch {
                return Err(AttemptError::state(
                    &r.attempt_id,
                    format!("the command's fencing epoch {} is stale", r.fencing_epoch),
                ));
            }
            l.control.clone()
        };
        tx.send(c)
            .await
            .map_err(|_| AttemptError::state(&r.attempt_id, "has just ended"))
    }

    /// Renews a lease (from the gateway's `LeaseGrant`).
    pub async fn renew_lease(
        &self,
        attempt: &AttemptRef,
        duration_s: u32,
        kill_budget_s: u32,
    ) -> Result<(), AttemptError> {
        self.control(
            Some(attempt),
            Control::Lease {
                duration_s,
                kill_budget_s,
            },
        )
        .await
    }

    /// Passes GPU events (from the telemetry reader) to the attempts using those GPUs.
    pub fn hw_events(&self, events: &[HwEvent]) {
        if events.is_empty() {
            return;
        }
        for l in lock(&self.live).values() {
            let _ = l.control.try_send(Control::Hw(events.to_vec()));
        }
    }

    /// Leaves every worker running and stops managing them (agent shutdown); the next agent
    /// re-adopts them.
    pub async fn detach_all(&self) {
        let controls: Vec<mpsc::Sender<Control>> = lock(&self.live)
            .values()
            .map(|l| l.control.clone())
            .collect();
        for c in controls {
            let (tx, rx) = oneshot::channel();
            if c.send(Control::Detach(tx)).await.is_ok() {
                let _ = rx.await;
            }
        }
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
        self.readopt_where(|_| true).await
    }

    /// [`Self::readopt`] for the attempts whose id `wanted` accepts (a local `attempt run`
    /// takes only its own). Attempts another manager runs (claimed) are skipped.
    pub async fn readopt_where(
        self: &Arc<Self>,
        wanted: impl Fn(&str) -> bool,
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
                continue;
            }
            // Still managed by another agent process: not ours to adopt or declare lost.
            let Ok(claim) = self.claim(&id) else {
                continue;
            };
            // A container is adopted only when it carries this launch's token, and from then on
            // by its id; one that is gone or took over the name is dropped from the record, so
            // nothing here ever stops it.
            let token = runner::attempt_token(&rec.nonce);
            let mut checked = Vec::with_capacity(rec.workers.len());
            for mut w in std::mem::take(&mut rec.workers) {
                if matches!(w.identity, record::ProcIdentity::Container { .. }) {
                    let (i, t) = (w.identity.clone(), token.clone());
                    match tokio::task::spawn_blocking(move || i.verified(&t))
                        .await
                        .ok()
                        .flatten()
                    {
                        Some(v) => w.identity = v,
                        None => continue,
                    }
                }
                checked.push(w);
            }
            rec.workers = checked;
            let _ = rec.save(&dir);
            let primary_alive = rec
                .workers
                .iter()
                .find(|w| w.primary)
                .is_some_and(|w| w.identity.alive());
            // Windows: the worker server is TCP on a port picked at start, which a restarted
            // agent can't get back, so a surviving worker could never reconnect. It is stopped
            // (by the release below) and reported lost instead of re-adopted.
            let readoptable = primary_alive && cfg!(unix);
            let status = if readoptable {
                let runner = Runner::new(Arc::clone(&self.shared), rec.clone(), Vec::new());
                self.spawn(&rec, true, runner, claim);
                status_of(&rec, None)
            } else if let Some(report) = rec
                .exit_report
                .as_deref()
                .and_then(|b| <ExitReport as prost::Message>::decode(b).ok())
            {
                // The worker ended after its report was saved: the report decides.
                let d = exit::decide(&exit::ExitEvidence {
                    report: Some(report),
                    readopted: true,
                    cancel_requested: rec.cancel_requested,
                    final_committed: rec.final_committed(),
                    needs_final_checkpoint: rec.needs_final_checkpoint(),
                    ..exit::ExitEvidence::default()
                });
                rec.phase = d.phase;
                rec.exit = Some(d.clone());
                let _ = rec.save(&dir);
                let _ = crate::janitor::release(
                    &self.shared.ctx,
                    &self.shared.envs,
                    &rec,
                    &HashSet::new(),
                )
                .await;
                status_of(&rec, Some(&d))
            } else {
                let d = ExitDecision {
                    phase: AttemptPhase::Exited as i32,
                    exit_kind: mrl_proto::worker::v1::ExitKind::Failed as i32,
                    exit_code: -1,
                    failure: Some(Failure {
                        class: FailureClass::NodeLost as i32,
                        sub_code: "WORKER_LOST".into(),
                        message: if primary_alive {
                            "The agent restarted; on Windows it can't reconnect to a running training worker, so it stopped it.".into()
                        } else {
                            "The agent restarted and the training worker was no longer running."
                                .into()
                        },
                        retryable_hint: true,
                        classified_by: ClassifiedBy::Agent as i32,
                        ..Failure::default()
                    }),
                };
                rec.phase = d.phase;
                rec.exit = Some(d.clone());
                let _ = rec.save(&dir);
                let _ = crate::janitor::release(
                    &self.shared.ctx,
                    &self.shared.envs,
                    &rec,
                    &HashSet::new(),
                )
                .await;
                status_of(&rec, Some(&d))
            };
            let _ = self
                .shared
                .events
                .send(AttemptEvent::Status(status.clone()))
                .await;
            out.push(status);
        }
        out
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
                self.add_pending(s.attempt.as_ref(), |p| {
                    p.items.extend(s.items.iter().cloned());
                    Ok(())
                })
            }
            Kind::StartAttempt(s) => {
                let (tx, rx) = oneshot::channel();
                match self.control(s.attempt.as_ref(), Control::Start(tx)).await {
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
                let _ = self.shared.events.send(AttemptEvent::Probe(result)).await;
                Ok(())
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
        let id = c
            .attempt
            .as_ref()
            .map(|a| a.attempt_id.clone())
            .unwrap_or_default();
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
                    if ended && !live.contains(&name) {
                        crate::janitor::purge(ctx.state.root(), &dir)?;
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
            let dir = ctx.state.job_dir(&id);
            if let Some(rec) = AttemptRecord::load(&dir)? {
                crate::janitor::release(ctx, &self.shared.envs, &rec, &HashSet::new()).await;
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
        let p = pending.entry(id.to_owned()).or_default();
        match &p.attempt {
            Some(a) if a.job_id != r.job_id => {
                return Err(AttemptError::state(
                    id,
                    format!("was announced for job {}, not {}", a.job_id, r.job_id),
                ));
            }
            Some(a) if r.fencing_epoch < a.fencing_epoch => return Err(stale(a.fencing_epoch)),
            Some(a) if r.fencing_epoch > a.fencing_epoch => *p = Pending::default(),
            _ => {}
        }
        p.attempt = Some(r.clone());
        add(p)
    }
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
