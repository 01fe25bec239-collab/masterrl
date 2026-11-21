//! The four logical lanes of D-04-05 and where their messages go.
//!
//! An agent opens one gRPC stream per protobuf lane (CONTROL, TELEMETRY, BULK). Each message
//! also belongs to one logical lane, which decides its loss rule:
//!
//! | Lane | Messages | On overflow | Acked |
//! |---|---|---|---|
//! | CONTROL | hello, heartbeat, inventory, command results, attempt/deployment status, reconcile, probe results, guard events, other worker messages | never dropped: the stream waits | after it is handled and recorded |
//! | METRICS | worker metrics, samples, rollout and reward stats | decimated: every other queued item dropped; kept items after a gap count it (`dropped_before`) | on receipt |
//! | LOGS | worker log chunks | new chunks dropped; the first chunk after the gap counts them (truncation marker) | on receipt |
//! | TELEMETRY | node telemetry | latest wins, never queued | on receipt |
//!
//! Later parts consume lanes through a [`LaneSink`] registered on a [`LaneRouter`]. With no
//! sink, CONTROL messages are journaled to `node_events` (`control_unrouted`, warning metric)
//! and METRICS/LOGS are acked and discarded (counter). TELEMETRY always updates
//! `node_telemetry_latest` here, and also reaches a sink that takes it.

use mrl_proto::agent::v1::{agent_message::Body, AgentMessage};
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use prometheus::{IntCounterVec, Opts};
use std::sync::{Arc, OnceLock, RwLock};
use uuid::Uuid;

/// A logical lane (D-04-05).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LaneKind {
    /// Never dropped.
    Control,
    /// Bounded, decimated on overflow.
    Metrics,
    /// Bounded, truncated with a marker on overflow.
    Logs,
    /// Latest wins, no spool.
    Telemetry,
}

impl LaneKind {
    /// Metric label, like `metrics`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Telemetry => "telemetry",
        }
    }

    /// The logical lane of an agent message.
    pub fn of(msg: &AgentMessage) -> LaneKind {
        match &msg.body {
            Some(Body::Telemetry(_)) => Self::Telemetry,
            Some(Body::Worker(w)) => match w.message.as_ref().and_then(|m| m.body.as_ref()) {
                Some(WorkerBody::Log(_)) => Self::Logs,
                Some(
                    WorkerBody::Metrics(_)
                    | WorkerBody::Samples(_)
                    | WorkerBody::RolloutBatch(_)
                    | WorkerBody::RewardBatchStats(_),
                ) => Self::Metrics,
                _ => Self::Control,
            },
            _ => Self::Control,
        }
    }
}

/// One agent message handed to a sink.
#[derive(Debug, Clone)]
pub struct LaneItem {
    /// The node (uuid of its `node_…` id).
    pub node_id: Uuid,
    /// The node's session epoch when the message arrived.
    pub session_epoch: i64,
    /// The logical lane.
    pub lane: LaneKind,
    /// The protobuf stream it came on (CONTROL, TELEMETRY or BULK); `seq` counts per stream.
    pub stream: mrl_proto::agent::v1::Lane,
    /// `AgentMessage.seq`: unique per `(node, stream)` across reconnects and agent restarts.
    /// Delivery is at-least-once: dedupe on `(node_id, stream, seq)`.
    pub seq: u64,
    /// METRICS/LOGS: how many items of this lane were dropped right before this one (the gap).
    pub dropped_before: u64,
    /// The message.
    pub message: AgentMessage,
}

/// Why a sink could not take an item.
#[derive(Debug, thiserror::Error)]
#[error("{sink}: {detail}")]
pub struct SinkError {
    /// The sink's name.
    pub sink: &'static str,
    /// What went wrong.
    pub detail: String,
}

/// Where a later part takes the messages of some lanes.
///
/// CONTROL items are kept in the database (only from the node's current session) before the
/// agent gets its ack, then delivered with no lock held, so a sink may send commands to the
/// same node. A sink error keeps the item; it is retried in order (at-least-once). METRICS and
/// LOGS items come from a bounded queue; TELEMETRY items are the latest reading.
#[async_trait::async_trait]
pub trait LaneSink: Send + Sync {
    /// Short name for logs and metrics.
    fn name(&self) -> &'static str;
    /// The lanes this sink takes (the first registered sink for a lane wins).
    fn lanes(&self) -> &'static [LaneKind];
    /// Handles one item. Must be idempotent per `(node_id, stream, seq)`. Lossy delivery
    /// holds the node session fence. Keep lossy callbacks short and finish their writes before
    /// returning. A lossy callback cannot change session ownership for its own node.
    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError>;
}

/// The registered sinks.
#[derive(Default)]
pub struct LaneRouter {
    sinks: RwLock<Vec<Arc<dyn LaneSink>>>,
}

impl LaneRouter {
    /// An empty router (tests, or a module built with its own router).
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The process-wide router `NodesModule::from_config` uses. Later parts register their sink
    /// here when their module is built: `LaneRouter::global().register(Arc::new(MySink))`.
    pub fn global() -> Arc<Self> {
        // ponytail: one router per process; production runs one server per process.
        static GLOBAL: OnceLock<Arc<LaneRouter>> = OnceLock::new();
        GLOBAL.get_or_init(LaneRouter::new).clone()
    }

    /// Adds a sink.
    pub fn register(&self, sink: Arc<dyn LaneSink>) {
        if let Ok(mut s) = self.sinks.write() {
            s.push(sink);
        }
    }

    /// The sink for `lane`, if any.
    pub fn sink_for(&self, lane: LaneKind) -> Option<Arc<dyn LaneSink>> {
        self.sinks
            .read()
            .ok()?
            .iter()
            .find(|s| s.lanes().contains(&lane))
            .cloned()
    }
}

/// Lane counters in the process-wide Prometheus registry (all no-ops if they can't be built).
pub struct LaneMetrics(Option<[IntCounterVec; 3]>);

impl LaneMetrics {
    /// The process-wide instance.
    pub fn get() -> &'static LaneMetrics {
        static M: OnceLock<LaneMetrics> = OnceLock::new();
        M.get_or_init(|| {
            let make = |name: &str, help: &str, label: &str| {
                let c = IntCounterVec::new(Opts::new(name, help), &[label]).ok()?;
                let _ = prometheus::default_registry().register(Box::new(c.clone()));
                Some(c)
            };
            let all = (|| {
                Some([
                    make(
                        "mrl_gateway_lane_dropped_total",
                        "Agent lane items dropped because the lane buffer was full.",
                        "lane",
                    )?,
                    make(
                        "mrl_gateway_lane_discarded_total",
                        "Agent lane items acked and discarded because no sink takes the lane yet.",
                        "lane",
                    )?,
                    make(
                        "mrl_gateway_control_unrouted_total",
                        "CONTROL messages journaled to node_events because no sink takes them yet.",
                        "body",
                    )?,
                ])
            })();
            LaneMetrics(all)
        })
    }

    fn inc(&self, which: usize, label: &str) {
        if let Some(m) = &self.0 {
            m[which].with_label_values(&[label]).inc();
        }
    }

    fn value(&self, which: usize, label: &str) -> u64 {
        self.0
            .as_ref()
            .map_or(0, |m| m[which].with_label_values(&[label]).get())
    }

    /// `mrl_gateway_lane_dropped_total{lane}` += 1.
    pub fn dropped(&self, lane: LaneKind) {
        self.inc(0, lane.as_str());
    }

    /// `mrl_gateway_lane_discarded_total{lane}` += 1.
    pub fn discarded(&self, lane: LaneKind) {
        self.inc(1, lane.as_str());
    }

    /// `mrl_gateway_control_unrouted_total{body}` += 1.
    pub fn unrouted(&self, body: &str) {
        self.inc(2, body);
    }

    /// Current `discarded` count for `lane`.
    pub fn discarded_count(&self, lane: LaneKind) -> u64 {
        self.value(1, lane.as_str())
    }

    /// Current `unrouted` count for `body`.
    pub fn unrouted_count(&self, body: &str) -> u64 {
        self.value(2, body)
    }
}

/// The session a lane's items belong to: node and session epoch.
pub(crate) struct Owner {
    /// Where the current epoch is read.
    pub db: crate::db::Db,
    /// The node.
    pub node: Uuid,
    /// The session epoch the items came under.
    pub epoch: i64,
}

impl Owner {
    /// A lossy item is delivered under the session fence. Use a dedicated connection so
    /// sinks can acquire a pooled connection, even when the pool has only one.
    pub async fn deliver(
        &self,
        session: &tokio_util::sync::CancellationToken,
        sink: &dyn LaneSink,
        item: LaneItem,
    ) {
        use sqlx::Connection;
        let delivery = async {
            let mut conn = self.db.dedicated().await?;
            let mut tx = conn.begin().await.map_err(crate::db::DbError::from)?;
            super::store::lock_lane_epoch(&mut tx, self.node, true).await?;
            if !super::store::holds_lane(&mut tx, self.node, self.epoch).await? {
                session.cancel();
                return Ok(());
            }
            if !session.is_cancelled() {
                if let Err(e) = sink.deliver(item).await {
                    tracing::warn!(error = %e, "lossy lane sink failed");
                }
            }
            Ok::<_, super::NodesError>(())
        };
        tokio::select! {
            biased;
            () = session.cancelled() => {}
            result = tokio::time::timeout(std::time::Duration::from_secs(5), delivery) => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::debug!(error = %e, "lossy lane fence unavailable"),
                    Err(_) => tracing::debug!("lossy lane delivery timed out"),
                }
            }
        }
    }
}

/// A bounded METRICS or LOGS queue for one stream, drained into the sink by its own task.
///
/// - METRICS, when full: every other queued item is dropped (oldest first), so the series stays
///   even but coarser; the new item is kept.
/// - LOGS, when full: the new item is dropped.
///
/// Each kept item that follows dropped ones carries their count in `dropped_before`, so the
/// marker sits exactly at the gap.
pub(crate) struct LossyLane {
    lane: LaneKind,
    cap: usize,
    shared: Option<Arc<Shared>>,
    session: tokio_util::sync::CancellationToken,
}

struct Shared {
    q: std::sync::Mutex<Queue>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct Queue {
    items: std::collections::VecDeque<LaneItem>,
    gap: u64,
    closed: bool,
}

impl Queue {
    /// Drops every other item (oldest first); kept items carry the count dropped before them.
    fn decimate(&mut self) -> u64 {
        let mut kept = std::collections::VecDeque::with_capacity(self.items.len());
        let (mut run, mut dropped) = (self.gap, 0);
        for (i, mut item) in self.items.drain(..).enumerate() {
            if i % 2 == 0 {
                // The dropped item's own gap carries over to the next kept one.
                run += 1 + item.dropped_before;
                dropped += 1;
            } else {
                item.dropped_before += run;
                run = 0;
                kept.push_back(item);
            }
        }
        self.items = kept;
        self.gap = run;
        dropped
    }
}

impl LossyLane {
    /// A queue of `capacity` items feeding `router`'s sink for `lane` (none: discard), for one
    /// CONTROL session: once `session` is cancelled (it ended or was taken over) nothing more
    /// is queued or delivered.
    /// With `owner`, each item is delivered only while that session is still the node's in the
    /// database (a takeover on another replica stops delivery before this stream hears of it).
    // ponytail: items still queued when their session ends are dropped (lossy lanes, D-04-05);
    // shortcut: one fenced connection per item, batch delivery when lane rates require it.
    pub fn start(
        router: &LaneRouter,
        lane: LaneKind,
        capacity: usize,
        session: tokio_util::sync::CancellationToken,
        owner: Option<Owner>,
    ) -> Self {
        let shared = router.sink_for(lane).map(|sink| {
            let shared = Arc::new(Shared {
                q: std::sync::Mutex::new(Queue::default()),
                notify: tokio::sync::Notify::new(),
            });
            let s = shared.clone();
            let session = session.clone();
            tokio::spawn(async move {
                loop {
                    let next = match s.q.lock() {
                        Ok(_) if session.is_cancelled() => return,
                        Ok(mut q) => match q.items.pop_front() {
                            Some(i) => Some(i),
                            None if q.closed => return,
                            None => None,
                        },
                        Err(_) => return,
                    };
                    match next {
                        Some(item) => {
                            if let Some(o) = &owner {
                                o.deliver(&session, sink.as_ref(), item).await;
                            } else if let Err(e) = sink.deliver(item).await {
                                tracing::warn!(lane = lane.as_str(), error = %e, "lane sink failed; item dropped");
                            }
                        }
                        None => tokio::select! {
                            () = s.notify.notified() => {}
                            () = session.cancelled() => return,
                        },
                    }
                }
            });
            shared
        });
        Self {
            lane,
            cap: capacity.max(2),
            shared,
            session,
        }
    }

    /// Queues `item` under the lane's overflow rule. Never waits.
    pub fn offer(&self, mut item: LaneItem) {
        let m = LaneMetrics::get();
        if self.session.is_cancelled() {
            m.discarded(self.lane);
            return;
        }
        let Some(shared) = &self.shared else {
            m.discarded(self.lane);
            return;
        };
        let Ok(mut q) = shared.q.lock() else { return };
        if q.items.len() >= self.cap {
            if self.lane == LaneKind::Metrics {
                for _ in 0..q.decimate() {
                    m.dropped(self.lane);
                }
            } else {
                q.gap += 1;
                m.dropped(self.lane);
                return;
            }
        }
        item.dropped_before += q.gap;
        q.gap = 0;
        q.items.push_back(item);
        drop(q);
        shared.notify.notify_one();
    }
}

impl Drop for LossyLane {
    fn drop(&mut self) {
        if let Some(s) = &self.shared {
            if let Ok(mut q) = s.q.lock() {
                q.closed = true;
            }
            s.notify.notify_one();
        }
    }
}

/// The body name of a message, for metrics and the journal.
pub(crate) fn body_name(msg: &AgentMessage) -> &'static str {
    match &msg.body {
        None => "none",
        Some(Body::Hello(_)) => "hello",
        Some(Body::Heartbeat(_)) => "heartbeat",
        Some(Body::Reconcile(_)) => "reconcile",
        Some(Body::CommandResult(_)) => "command_result",
        Some(Body::AttemptStatus(_)) => "attempt_status",
        Some(Body::Worker(_)) => "worker",
        Some(Body::Inventory(_)) => "inventory",
        Some(Body::Telemetry(_)) => "telemetry",
        Some(Body::Guard(_)) => "guard",
        Some(Body::ProbeResult(_)) => "probe_result",
        Some(Body::DeploymentStatus(_)) => "deployment_status",
        Some(Body::Enroll(_)) => "enroll",
        Some(Body::RenewCertificate(_)) => "renew_certificate",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::agent::v1::{NodeTelemetry, WorkerRelay};
    use mrl_proto::worker::v1::{LogChunk, MetricsBatch, WorkerMessage};
    use std::sync::Mutex;
    use tokio::sync::Notify;

    fn worker(body: WorkerBody) -> AgentMessage {
        AgentMessage {
            body: Some(Body::Worker(WorkerRelay {
                message: Some(WorkerMessage {
                    body: Some(body),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn messages_map_to_lanes() {
        assert_eq!(LaneKind::of(&AgentMessage::default()), LaneKind::Control);
        let t = AgentMessage {
            body: Some(Body::Telemetry(NodeTelemetry::default())),
            ..Default::default()
        };
        assert_eq!(LaneKind::of(&t), LaneKind::Telemetry);
        assert_eq!(
            LaneKind::of(&worker(WorkerBody::Log(LogChunk::default()))),
            LaneKind::Logs
        );
        assert_eq!(
            LaneKind::of(&worker(WorkerBody::Metrics(MetricsBatch::default()))),
            LaneKind::Metrics
        );
        assert_eq!(
            LaneKind::of(&worker(WorkerBody::Heartbeat(Default::default()))),
            LaneKind::Control
        );
    }

    /// A sink that blocks until released and records what it got.
    struct Gate {
        open: Notify,
        got: Mutex<Vec<(u64, u64)>>,
    }

    #[async_trait::async_trait]
    impl LaneSink for Gate {
        fn name(&self) -> &'static str {
            "gate"
        }
        fn lanes(&self) -> &'static [LaneKind] {
            &[LaneKind::Metrics, LaneKind::Logs]
        }
        async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
            self.open.notified().await;
            self.got
                .lock()
                .unwrap()
                .push((item.seq, item.dropped_before));
            Ok(())
        }
    }

    fn item(seq: u64, lane: LaneKind) -> LaneItem {
        LaneItem {
            node_id: Uuid::nil(),
            session_epoch: 1,
            lane,
            stream: mrl_proto::agent::v1::Lane::Telemetry,
            seq,
            dropped_before: 0,
            message: AgentMessage::default(),
        }
    }

    #[test]
    fn a_second_decimation_keeps_the_gaps_of_the_items_it_drops() {
        let mut q = Queue::default();
        q.items.extend((1..=4).map(|s| item(s, LaneKind::Metrics)));
        assert_eq!(q.decimate(), 2); // drops 1, 3
        q.items.extend((5..=6).map(|s| item(s, LaneKind::Metrics)));
        assert_eq!(q.decimate(), 2); // drops 2 (gap 1), 5
        let kept: Vec<(u64, u64)> = q.items.iter().map(|i| (i.seq, i.dropped_before)).collect();
        assert_eq!(kept, [(4, 3), (6, 1)], "1, 2, 3 before 4; 5 before 6");
        assert_eq!(q.gap, 0);
    }

    async fn drain(gate: &Gate, n: usize) -> Vec<(u64, u64)> {
        for _ in 0..n {
            gate.open.notify_one();
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        gate.got.lock().unwrap().clone()
    }

    fn gate() -> (Arc<LaneRouter>, Arc<Gate>) {
        let router = LaneRouter::new();
        let gate = Arc::new(Gate {
            open: Notify::new(),
            got: Mutex::new(Vec::new()),
        });
        router.register(gate.clone());
        (router, gate)
    }

    #[tokio::test]
    async fn metrics_overflow_decimates_and_marks_the_gaps() {
        let (router, gate) = gate();
        let lane = LossyLane::start(
            &router,
            LaneKind::Metrics,
            4,
            tokio_util::sync::CancellationToken::new(),
            None,
        );
        lane.offer(item(1, LaneKind::Metrics)); // taken by the sink task, held at the gate
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        for s in 2..=7 {
            lane.offer(item(s, LaneKind::Metrics));
        }
        // Queue [2,3,4,5] was full at #6: 2 and 4 dropped, 3 and 5 each mark one lost item.
        assert_eq!(
            drain(&gate, 5).await,
            vec![(1, 0), (3, 1), (5, 1), (6, 0), (7, 0)]
        );
    }

    #[tokio::test]
    async fn logs_overflow_drops_new_chunks_and_marks_the_first_after_the_gap() {
        let (router, gate) = gate();
        let lane = LossyLane::start(
            &router,
            LaneKind::Logs,
            2,
            tokio_util::sync::CancellationToken::new(),
            None,
        );
        lane.offer(item(1, LaneKind::Logs));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        for s in 2..=5 {
            lane.offer(item(s, LaneKind::Logs)); // 2, 3 queued; 4, 5 dropped
        }
        let first = drain(&gate, 2).await; // delivers 1 and 2, frees space
        assert_eq!(first, vec![(1, 0), (2, 0)]);
        lane.offer(item(6, LaneKind::Logs));
        assert_eq!(drain(&gate, 2).await, vec![(1, 0), (2, 0), (3, 0), (6, 2)]);
    }

    #[tokio::test]
    async fn without_a_sink_items_are_discarded_and_counted() {
        let router = LaneRouter::new();
        let lane = LossyLane::start(
            &router,
            LaneKind::Logs,
            4,
            tokio_util::sync::CancellationToken::new(),
            None,
        );
        let before = LaneMetrics::get().discarded_count(LaneKind::Logs);
        lane.offer(item(1, LaneKind::Logs));
        lane.offer(item(2, LaneKind::Logs));
        let after = LaneMetrics::get().discarded_count(LaneKind::Logs);
        assert_eq!(after - before, 2);
        assert!(router.sink_for(LaneKind::Control).is_none());
    }
}
