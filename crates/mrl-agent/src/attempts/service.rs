//! The attempt runtime inside `mrl-agent run`: [`AttemptsService`] wires [`Attempts`] into
//! the gateway [`Link`].
//!
//! - Start: handlers for every attempt command kind are registered and the attempts saved on
//!   disk without an outcome are published as running, all before the first `.await`. Then
//!   [`Attempts::readopt`] re-attaches live workers, [`Link::set_running`] gets what really
//!   runs, and the `ReconcileReport` is spooled. Commands that arrived meanwhile wait at the
//!   gate and run only after that.
//! - Commands: every result comes from [`Attempts::handle`].
//! - Events: CONTROL events are spooled ([`Link::send_control`]) and only then marked saved; a
//!   failed spool write keeps the event and tries it again, in order. Worker METRICS/LOGS go to
//!   the lossy lanes ([`Link::send_metrics`] / [`Link::send_logs`]).
//! - Leases: each `LeaseGrant` renews its attempts from the send time of the heartbeat it
//!   answers ([`Link::heartbeat_sent`]).
//! - Stop: no new commands, running ones get [`STOP_WAIT`], then [`Attempts::detach_all`]
//!   leaves the workers running for the next agent (systemd `KillMode=process`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use mrl_proto::agent::v1::{agent_message, Command, Lane, LeaseGrant, ReconcileReport};
use mrl_proto::common::v1::{AttemptRef, Failure, FailureClass};
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

use super::record::AttemptRecord;
use super::{attempt_events, AttemptError, AttemptEvent, AttemptEvents, Attempts, Envelope};
use crate::session::{backoff, CommandHandler, Link, LinkError};
use crate::AgentContext;

/// Every command kind [`Attempts::handle`] answers (all but deployments).
pub const KINDS: &[&str] = &[
    "LaunchAttempt",
    "StartAttempt",
    "CancelAttempt",
    "PauseAttempt",
    "StageArtifacts",
    "UploadArtifacts",
    "Cleanup",
    "Drain",
    "UpdateCredentials",
    "Probe",
    "CheckpointAttempt",
];

/// How long a stopping agent waits for commands already running before it detaches.
pub const STOP_WAIT: Duration = Duration::from_secs(10);
/// A single shutdown budget, below the installed service's 30-second timeout.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(25);
/// How often the running list in heartbeats is refreshed between commands.
const PUBLISH_EVERY: Duration = Duration::from_secs(1);
/// Bounds the queued work from grants a runner can't accept promptly.
const LEASE_WAIT: Duration = Duration::from_secs(1);

/// The attempt runtime as an agent service (registered in `registry.rs`).
#[derive(Debug, Default)]
pub struct AttemptsService;

impl AttemptsService {
    /// The service.
    pub fn new() -> Self {
        Self
    }
}

/// What the command handlers and the service share.
struct Wiring {
    attempts: Arc<Attempts>,
    link: Arc<Link>,
    /// Held for writing until start-up is done, and again while stopping; each command holds
    /// it for reading while it runs.
    gate: Arc<RwLock<()>>,
    shutdown: CancellationToken,
    publishing: Mutex<()>,
}

impl Wiring {
    /// Publishes the attempts running now. Serialized, so an older list never replaces a newer.
    fn publish(&self) {
        let _one = self.publishing.lock().unwrap_or_else(|p| p.into_inner());
        self.link.set_running(self.attempts.running());
    }
}

struct Handler(Arc<Wiring>);

#[async_trait::async_trait]
impl CommandHandler for Handler {
    async fn handle(&self, command: Command) -> Result<(), Failure> {
        let w = &self.0;
        let _running = w.gate.read().await;
        if w.shutdown.is_cancelled() {
            return Err(AttemptError::Stopping.to_failure());
        }
        let result = w.attempts.handle(&command).await;
        // A started attempt is in the next heartbeat (and the drain acknowledgement).
        w.publish();
        match result {
            Some(r) if r.accepted => Ok(()),
            Some(r) => Err(r.failure.unwrap_or_else(|| Failure {
                class: FailureClass::EngineError as i32,
                sub_code: "rejected".into(),
                message: "the attempt runtime refused the command".into(),
                ..Failure::default()
            })),
            None => Err(Failure {
                class: FailureClass::DependencyEnv as i32,
                sub_code: "unsupported_command".into(),
                message: "the attempt runtime does not run this command".into(),
                ..Failure::default()
            }),
        }
    }
}

/// Attempts saved on disk without an outcome: running, or waiting to be re-adopted.
fn unfinished(ctx: &AgentContext) -> Vec<AttemptRef> {
    let Ok(rd) = std::fs::read_dir(ctx.state.root().join("jobs")) else {
        return Vec::new();
    };
    rd.flatten()
        .filter_map(|e| AttemptRecord::load(&e.path()).ok().flatten())
        .filter(|r| r.exit.is_none())
        .map(|r| AttemptRef {
            job_id: r.spec.attempt.job_id,
            attempt_id: r.spec.attempt.attempt_id,
            fencing_epoch: r.spec.attempt.fencing_epoch,
        })
        .collect()
}

/// Spools `body` (CONTROL), trying again after a failure until it is in or `stop` fires.
/// False when it was not spooled.
async fn spool(
    link: &Link,
    body: &agent_message::Body,
    what: &str,
    stop: &CancellationToken,
) -> bool {
    let mut failures = 0u32;
    loop {
        match link.send_control(body.clone()) {
            Ok(_) => return true,
            // It can never fit; trying again would hold up everything after it.
            Err(e @ LinkError::TooLarge { .. }) => {
                eprintln!("mrl-agent: {what} was not sent: {}", e.explain());
                return false;
            }
            Err(e) => {
                failures = failures.saturating_add(1);
                if failures == 1 || failures.is_multiple_of(20) {
                    eprintln!(
                        "mrl-agent: {what} is not saved yet (trying again): {}",
                        e.explain()
                    );
                }
                tokio::select! {
                    () = stop.cancelled() => return false,
                    () = tokio::time::sleep(backoff(failures)) => {}
                }
            }
        }
    }
}

/// Hands one event to the link. A CONTROL event is marked saved only once it is spooled; an
/// event that could not be spooled is dropped unsaved (the runtime sends it again).
async fn forward(link: &Link, envelope: Envelope, stop: &CancellationToken) {
    match envelope.event().lane() {
        Lane::Telemetry | Lane::Bulk => {
            let lane = envelope.event().lane();
            // Lossy lanes by design: queued is as saved as these get.
            let (event, receipt) = envelope.into_parts();
            if let AttemptEvent::Relay { relay, .. } = event {
                if lane == Lane::Telemetry {
                    link.send_metrics(relay);
                } else {
                    link.send_logs(relay);
                }
                receipt.saved();
            }
        }
        Lane::Control | Lane::Unspecified => {
            let body = envelope.event().clone().into_body();
            if spool(link, &body, "an attempt event", stop).await {
                envelope.saved();
            }
        }
    }
}

async fn forward_events(link: &Link, mut events: AttemptEvents, stop: CancellationToken) {
    loop {
        let envelope = tokio::select! {
            () = stop.cancelled() => return,
            e = events.recv() => match e { Some(e) => e, None => return },
        };
        forward(link, envelope, &stop).await;
    }
}

/// Renews the leases of one grant, counted from the send time of the heartbeat it answers.
async fn renew(w: &Wiring, grant: LeaseGrant, sent: std::time::Instant) {
    futures::future::join_all(grant.leases.into_iter().map(|lease| async move {
        if let Some(attempt) = lease.attempt.as_ref() {
            // A runner finishing or blocked on its own work must not hold up other grants.
            let _ = w
                .attempts
                .renew_lease(
                    attempt,
                    grant.heartbeat_seq,
                    sent,
                    lease.duration_s,
                    lease.kill_budget_s,
                )
                .await;
        }
    }))
    .await;
}

async fn renew_leases(
    w: &Wiring,
    mut grants: broadcast::Receiver<LeaseGrant>,
    stop: CancellationToken,
) {
    let mut publish = tokio::time::interval(PUBLISH_EVERY);
    let mut pending = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = publish.tick() => w.publish(),
            Some(_) = pending.next() => {}
            g = grants.recv() => match g {
                Ok(g) => {
                    if let Some(sent) = w.link.heartbeat_sent(g.heartbeat_seq) {
                        pending.push(tokio::time::timeout(LEASE_WAIT, renew(w, g, sent)));
                    }
                }
                // Missed grants are older; the next one renews from a newer heartbeat.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}

#[async_trait::async_trait]
impl crate::registry::AgentService for AttemptsService {
    fn name(&self) -> &'static str {
        "attempts"
    }

    fn command_kinds(&self) -> &'static [&'static str] {
        KINDS
    }

    async fn run(&self, ctx: Arc<AgentContext>, shutdown: CancellationToken) -> crate::Result<()> {
        // Register every handler before re-adoption yields.
        let link = Link::of(&ctx)?;
        let (tx, events) = attempt_events(1024);
        let attempts = Attempts::new(Arc::clone(&ctx), tx, None)
            .map_err(|e| crate::AgentError::exec("the attempt runtime", e.explain()))?;
        let gate = Arc::new(RwLock::new(()));
        let starting = Arc::clone(&gate)
            .try_write_owned()
            .map_err(|_| crate::AgentError::exec("the attempt runtime", "start gate busy"))?;
        let w = Arc::new(Wiring {
            attempts: Arc::clone(&attempts),
            link: Arc::clone(&link),
            gate,
            shutdown: shutdown.clone(),
            publishing: Mutex::new(()),
        });
        for kind in KINDS {
            link.register(kind, Arc::new(Handler(Arc::clone(&w))));
        }
        let grants = link.leases();
        // Until re-adoption says otherwise, every attempt without an outcome counts as running,
        // so an early heartbeat never reports this machine idle.
        link.set_running(unfinished(&ctx));

        let stop = CancellationToken::new();
        let statuses = tokio::select! {
            biased;
            () = shutdown.cancelled() => None,
            statuses = attempts.readopt_for_session() => Some(statuses),
        };
        w.publish();

        let mut leases = tokio::spawn({
            let stop = stop.clone();
            let w = w.clone();
            async move { renew_leases(&w, grants, stop).await }
        });
        if let Some(statuses) = statuses {
            // Read while the start gate is held: no command runs between re-adoption and here,
            // so every command up to this seq shows in `statuses` (INT-4).
            let report = agent_message::Body::Reconcile(ReconcileReport {
                attempts: statuses,
                deployments: Vec::new(),
                commands_through: link.commands_through(),
            });
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {}
                _ = spool(&link, &report, "the reconcile report", &stop) => {}
            }
        }
        drop(starting);
        let mut forwarding = tokio::spawn({
            let stop = stop.clone();
            async move { forward_events(&link, events, stop).await }
        });

        shutdown.cancelled().await;
        let deadline = tokio::time::Instant::now() + SHUTDOWN_WAIT;
        // Commands still running get a moment to finish; later ones are refused.
        let _ = tokio::time::timeout(STOP_WAIT, w.gate.write()).await;
        // Events keep flowing while the attempts detach; workers keep running.
        if tokio::time::timeout_at(deadline, attempts.detach_all())
            .await
            .is_err()
        {
            eprintln!(
                "mrl-agent: some attempts did not detach in time; the next agent re-adopts them"
            );
        }
        stop.cancel();
        if tokio::time::timeout_at(deadline, async {
            let _ = tokio::join!(&mut leases, &mut forwarding);
        })
        .await
        .is_err()
        {
            leases.abort();
            forwarding.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::agent::v1::command::Kind;
    use mrl_proto::agent::v1::AttemptLease;

    #[tokio::test]
    async fn one_stuck_runner_does_not_delay_another_lease() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::config::AgentConfig {
            state_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let ctx = Arc::new(AgentContext::with_hw(config, Vec::new()).unwrap());
        let (events, _reader) = attempt_events(1);
        let w = Wiring {
            link: Link::of(&ctx).unwrap(),
            attempts: Attempts::new(ctx, events, None).unwrap(),
            gate: Arc::new(RwLock::new(())),
            shutdown: CancellationToken::new(),
            publishing: Mutex::new(()),
        };
        let runner = |id: &str| {
            let (control, rx) = tokio::sync::mpsc::channel(1);
            let attempt = AttemptRef {
                job_id: "job_test".into(),
                attempt_id: id.into(),
                fencing_epoch: 1,
            };
            super::super::lock(&w.attempts.live).insert(
                id.into(),
                super::super::Live {
                    attempt: attempt.clone(),
                    control,
                },
            );
            (
                AttemptLease {
                    attempt: Some(attempt),
                    duration_s: 90,
                    kill_budget_s: 22,
                },
                rx,
            )
        };
        let (stuck, _unread) = runner("att_stuck");
        let (healthy, mut inbox) = runner("att_healthy");
        let sent = std::time::Instant::now() - Duration::from_secs(5);
        let grant = LeaseGrant {
            heartbeat_seq: 7,
            leases: vec![stuck, healthy],
        };
        tokio::select! {
            () = renew(&w, grant, sent) => panic!("stuck runner answered"),
            msg = inbox.recv() => {
                let Some(super::super::runner::Control::Acked { inner, .. }) = msg else {
                    panic!("the healthy runner did not get a renewal");
                };
                assert!(matches!(
                    *inner,
                    super::super::runner::Control::Lease { heartbeat_seq: 7, heartbeat_sent, .. }
                        if heartbeat_sent == sent
                ));
            }
            () = tokio::time::sleep(Duration::from_secs(2)) => panic!("renewal waited on the stuck runner"),
        }
    }

    #[test]
    fn every_attempt_command_kind_is_registered() {
        let all = [
            Kind::LaunchAttempt(Default::default()),
            Kind::StartAttempt(Default::default()),
            Kind::CancelAttempt(Default::default()),
            Kind::PauseAttempt(Default::default()),
            Kind::StageArtifacts(Default::default()),
            Kind::UploadArtifacts(Default::default()),
            Kind::Cleanup(Default::default()),
            Kind::Drain(Default::default()),
            Kind::UpdateCredentials(Default::default()),
            Kind::LaunchDeployment(Default::default()),
            Kind::StopDeployment(Default::default()),
            Kind::Probe(Default::default()),
            Kind::CheckpointAttempt(Default::default()),
        ];
        let deployments = ["LaunchDeployment", "StopDeployment"];
        let mut served: Vec<&str> = all
            .iter()
            .map(|k| crate::session::kind_name(Some(k)))
            .filter(|n| !deployments.contains(n))
            .collect();
        let mut kinds = KINDS.to_vec();
        served.sort_unstable();
        kinds.sort_unstable();
        assert_eq!(kinds, served);
    }
}
