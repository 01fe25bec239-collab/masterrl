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
use tokio::sync::{mpsc, oneshot, watch};

use self::exit::ExitDecision;
use self::files::Destination;
use self::record::AttemptRecord;
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
    epoch: u64,
    control: mpsc::Sender<Control>,
}

/// Runs and tracks every attempt on this node.
pub struct Attempts {
    shared: Arc<Shared>,
    live: Mutex<HashMap<String, Live>>,
    done: Mutex<HashMap<String, watch::Receiver<Option<ExitDecision>>>>,
    staged: Mutex<HashMap<String, Vec<StageItem>>>,
    credentials: Mutex<HashMap<String, BTreeMap<String, Secret>>>,
    epochs: Mutex<HashMap<String, u64>>,
    draining: AtomicBool,
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
        Ok(Arc::new(Self {
            shared: Arc::new(Shared {
                ctx,
                envs,
                events,
                progress,
            }),
            live: Mutex::default(),
            done: Mutex::default(),
            staged: Mutex::default(),
            credentials: Mutex::default(),
            epochs: Mutex::default(),
            draining: AtomicBool::new(false),
        }))
    }

    /// The environments folder.
    pub fn envs(&self) -> &Arc<Envs> {
        &self.shared.envs
    }

    /// Starts an attempt. Returns once it is accepted; it runs in the background.
    pub fn launch(self: &Arc<Self>, spec: AttemptSpec) -> Result<(), AttemptError> {
        spec.validate()?;
        let id = spec.attempt.attempt_id.clone();
        if self.draining.load(Ordering::SeqCst) {
            return Err(AttemptError::Draining("the machine is draining".into()));
        }
        {
            let mut epochs = lock(&self.epochs);
            let seen = epochs.entry(spec.attempt.job_id.clone()).or_insert(0);
            if spec.attempt.fencing_epoch < *seen {
                return Err(AttemptError::state(
                    &id,
                    format!(
                        "fencing epoch {} is older than epoch {} already seen for this job",
                        spec.attempt.fencing_epoch, *seen
                    ),
                ));
            }
            *seen = spec.attempt.fencing_epoch;
        }
        let job_dir = self.shared.ctx.state.job_dir(&id);
        if lock(&self.live).contains_key(&id) || AttemptRecord::load(&job_dir)?.is_some() {
            return Err(AttemptError::state(&id, "already exists on this machine"));
        }
        let secrets = self.secrets_for(&spec)?;
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
        };
        std::fs::create_dir_all(&job_dir).map_err(|e| crate::AgentError::state(&job_dir, e))?;
        rec.save(&job_dir)?;
        let runner = Runner::new(Arc::clone(&self.shared), rec.clone(), secrets);
        self.spawn(&rec, false, runner);
        Ok(())
    }

    fn spawn(self: &Arc<Self>, rec: &AttemptRecord, readopt: bool, runner: Runner) {
        let id = rec.spec.attempt.attempt_id.clone();
        let (control, rx) = mpsc::channel(32);
        let (tx_done, rx_done) = watch::channel(None);
        lock(&self.live).insert(
            id.clone(),
            Live {
                epoch: rec.spec.attempt.fencing_epoch,
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
            let _ = tx_done.send(decision);
        });
    }

    fn secrets_for(&self, spec: &AttemptSpec) -> Result<Vec<(String, Secret)>, AttemptError> {
        let creds = lock(&self.credentials);
        let given = creds.get(&spec.attempt.attempt_id);
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
        let live = lock(&self.live);
        live.iter()
            .map(|(id, l)| AttemptRef {
                job_id: String::new(),
                attempt_id: id.clone(),
                fencing_epoch: l.epoch,
            })
            .collect()
    }

    async fn control(&self, attempt: Option<&AttemptRef>, c: Control) -> Result<(), AttemptError> {
        let r = attempt.ok_or_else(|| AttemptError::Spec("the command names no attempt".into()))?;
        let tx = {
            let live = lock(&self.live);
            let l = live.get(&r.attempt_id).ok_or_else(|| {
                AttemptError::state(&r.attempt_id, "is not running on this machine")
            })?;
            if r.fencing_epoch < l.epoch {
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

    /// After an agent restart: re-attaches to attempts whose workers still run and reports the
    /// others lost. Returns the status of each (for the reconcile report).
    pub async fn readopt(self: &Arc<Self>) -> Vec<AttemptStatus> {
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
            if rec.exit.is_some() || lock(&self.live).contains_key(&id) {
                continue;
            }
            lock(&self.epochs)
                .entry(rec.spec.attempt.job_id.clone())
                .and_modify(|e| *e = (*e).max(rec.spec.attempt.fencing_epoch))
                .or_insert(rec.spec.attempt.fencing_epoch);
            let primary_alive = rec
                .workers
                .iter()
                .find(|w| w.primary)
                .is_some_and(|w| w.identity.alive());
            let status = if primary_alive {
                let runner = Runner::new(Arc::clone(&self.shared), rec.clone(), Vec::new());
                self.spawn(&rec, true, runner);
                status_of(&rec, None)
            } else {
                let d = ExitDecision {
                    phase: AttemptPhase::Exited as i32,
                    exit_kind: mrl_proto::worker::v1::ExitKind::Failed as i32,
                    exit_code: -1,
                    failure: Some(Failure {
                        class: FailureClass::NodeLost as i32,
                        sub_code: "WORKER_LOST".into(),
                        message:
                            "The agent restarted and the training worker was no longer running."
                                .into(),
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
                let staged = lock(&self.staged).remove(&id).unwrap_or_default();
                AttemptSpec::from_launch(l, &staged, self.shared.ctx.state.root())
                    .and_then(|spec| self.launch(spec))
            }
            Kind::StageArtifacts(s) => {
                let id = s
                    .attempt
                    .as_ref()
                    .map(|a| a.attempt_id.clone())
                    .unwrap_or_default();
                if lock(&self.live).contains_key(&id) {
                    Err(AttemptError::state(
                        &id,
                        "is already launched; stage its inputs before the launch",
                    ))
                } else {
                    lock(&self.staged)
                        .entry(id)
                        .or_default()
                        .extend(s.items.iter().cloned());
                    Ok(())
                }
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
                let items: Result<Vec<_>, AttemptError> = u
                    .items
                    .iter()
                    .map(|i| {
                        spec::safe_relative(&i.local_path)?;
                        Ok((
                            i.local_path.clone(),
                            Destination::Ticket(spec::upload_ticket(i)?),
                            i.size,
                            i.sha256.clone(),
                        ))
                    })
                    .collect();
                match items {
                    Ok(items) => {
                        self.control(
                            u.attempt.as_ref(),
                            Control::Upload {
                                command_id: cmd.command_id.clone(),
                                checkpoint_id: u.checkpoint_id.clone(),
                                items,
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
                let id = u
                    .attempt
                    .as_ref()
                    .map(|a| a.attempt_id.clone())
                    .unwrap_or_default();
                let mut creds = lock(&self.credentials);
                let slot = creds.entry(id).or_default();
                u.credentials.iter().try_for_each(|c| {
                    let v = String::from_utf8(c.value.clone()).map_err(|_| {
                        AttemptError::Spec(format!("credential {} is not text", c.name))
                    })?;
                    slot.insert(c.name.clone(), Secret::new(v));
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
        if id.is_empty() {
            let live: HashSet<String> = lock(&self.live).keys().cloned().collect();
            crate::janitor::sweep(ctx, &self.shared.envs, &live).await?;
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
            let budget = ctx.cache.budget().max_bytes;
            ctx.cache
                .reserve(budget.saturating_sub(c.cache_limit_bytes))?;
        }
        Ok(())
    }

    /// Stores a credential for an attempt that has not launched yet (local tools and tests).
    pub fn set_credential(&self, attempt_id: &str, name: &str, value: Secret) {
        lock(&self.credentials)
            .entry(attempt_id.to_owned())
            .or_default()
            .insert(name.to_owned(), value);
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
