//! [`MetricObserver`]: other modules hear about newly stored metric points, per run.
//!
//! ```no_run
//! use mrl_server::modules::telemetry::{IngestedBatch, MetricObserver, MetricObservers};
//! use std::sync::Arc;
//! struct Watch;
//! #[async_trait::async_trait]
//! impl MetricObserver for Watch {
//!     fn name(&self) -> &'static str { "watch" }
//!     async fn observe(&self, batch: Arc<IngestedBatch>) { let _ = batch.points.len(); }
//! }
//! // In your module's constructor:
//! MetricObservers::global().register(Arc::new(Watch));
//! ```
//!
//! Each observer gets every batch once committed, in commit order of this process, from a task
//! of its own (a slow observer never slows ingest). Its queue holds 1024 batches; when it is
//! full, new batches are dropped for that observer (`mrl_telemetry_observer_dropped_total`).
//! Batches are not kept across restarts: an observer that must see every point reads
//! `metric_points` (by `batch_id`) itself.

use prometheus::{IntCounterVec, Opts};
use std::sync::{Arc, OnceLock, RwLock};
use time::OffsetDateTime;
use tokio::sync::mpsc;

/// Batches queued per observer.
pub const OBSERVER_QUEUE: usize = 1024;

/// One stored metric point.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedPoint {
    /// `metric_series.id`.
    pub series_id: i64,
    /// Metric name, like `train/loss`.
    pub name: String,
    /// Split, like `train` (may be empty).
    pub split: String,
    /// Global step.
    pub step: i64,
    /// Value.
    pub value: f64,
    /// When the worker measured it.
    pub ts: OffsetDateTime,
}

/// The points one worker message added to a run.
#[derive(Debug, Clone, PartialEq)]
pub struct IngestedBatch {
    /// `run_…`.
    pub run_id: String,
    /// `att_…`.
    pub attempt_id: String,
    /// `metric_batches.id`.
    pub batch_id: i64,
    /// The points stored.
    pub points: Vec<ObservedPoint>,
}

/// Hears about newly stored points.
#[async_trait::async_trait]
pub trait MetricObserver: Send + Sync {
    /// Short unique name, for logs and metrics.
    fn name(&self) -> &'static str;
    /// Handles one batch. Runs on the observer's own task, after the batch committed.
    async fn observe(&self, batch: Arc<IngestedBatch>);
}

/// One observer's queue, by name.
type Queue = (&'static str, mpsc::Sender<Arc<IngestedBatch>>);

/// The registered observers.
#[derive(Default)]
pub struct MetricObservers {
    queues: RwLock<Vec<Queue>>,
}

impl MetricObservers {
    /// An empty set (tests, or a module with its own set).
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The process-wide set `TelemetryModule::new` publishes to.
    pub fn global() -> Arc<Self> {
        // ponytail: one set per process, like `LaneRouter::global`.
        static GLOBAL: OnceLock<Arc<MetricObservers>> = OnceLock::new();
        GLOBAL.get_or_init(MetricObservers::new).clone()
    }

    /// Adds an observer and starts its task. Call it from inside the Tokio runtime.
    pub fn register(&self, observer: Arc<dyn MetricObserver>) {
        let (tx, mut rx) = mpsc::channel::<Arc<IngestedBatch>>(OBSERVER_QUEUE);
        let name = observer.name();
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                observer.observe(batch).await;
            }
        });
        if let Ok(mut q) = self.queues.write() {
            q.push((name, tx));
        }
    }

    /// Hands `batch` to every observer; a full queue drops it for that observer.
    pub fn publish(&self, batch: IngestedBatch) {
        let Ok(queues) = self.queues.read() else {
            return;
        };
        if queues.is_empty() {
            return;
        }
        let batch = Arc::new(batch);
        for (name, q) in queues.iter() {
            if q.try_send(batch.clone()).is_err() {
                observer_dropped(name);
            }
        }
    }
}

fn observer_dropped(name: &str) {
    static C: OnceLock<Option<IntCounterVec>> = OnceLock::new();
    let c = C.get_or_init(|| {
        let c = IntCounterVec::new(
            Opts::new(
                "mrl_telemetry_observer_dropped_total",
                "Metric batches a MetricObserver did not get because its queue was full.",
            ),
            &["observer"],
        )
        .ok()?;
        let _ = prometheus::default_registry().register(Box::new(c.clone()));
        Some(c)
    });
    if let Some(c) = c {
        c.with_label_values(&[name]).inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Collect(Mutex<Vec<i64>>, tokio::sync::Notify);

    #[async_trait::async_trait]
    impl MetricObserver for Collect {
        fn name(&self) -> &'static str {
            "collect"
        }
        async fn observe(&self, batch: Arc<IngestedBatch>) {
            self.0.lock().unwrap().push(batch.batch_id);
            self.1.notify_one();
        }
    }

    #[tokio::test]
    async fn observers_get_batches_in_order() {
        let set = MetricObservers::new();
        let c = Arc::new(Collect(Mutex::default(), tokio::sync::Notify::new()));
        set.register(c.clone());
        for id in 1..=3 {
            set.publish(IngestedBatch {
                run_id: "run_x".into(),
                attempt_id: "att_x".into(),
                batch_id: id,
                points: Vec::new(),
            });
        }
        for _ in 0..50 {
            if c.0.lock().unwrap().len() == 3 {
                break;
            }
            let _ =
                tokio::time::timeout(std::time::Duration::from_millis(100), c.1.notified()).await;
        }
        assert_eq!(*c.0.lock().unwrap(), vec![1, 2, 3]);
    }
}
