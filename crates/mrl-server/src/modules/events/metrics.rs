//! Outbox metrics, registered once in the process-wide Prometheus registry.

use prometheus::{Gauge, IntCounterVec, Opts};
use std::sync::OnceLock;

/// `mrl_outbox_*` metrics. Empty (all calls no-ops) if they could not be built.
pub struct EventMetrics(Option<Inner>);

struct Inner {
    lag_seconds: Gauge,
    delivered: IntCounterVec,
    quarantined: IntCounterVec,
}

impl EventMetrics {
    /// The process-wide instance.
    pub fn get() -> &'static EventMetrics {
        static METRICS: OnceLock<EventMetrics> = OnceLock::new();
        METRICS.get_or_init(|| match build() {
            Ok(inner) => EventMetrics(Some(inner)),
            Err(e) => {
                tracing::error!(error = %e, "outbox metrics could not be built");
                EventMetrics(None)
            }
        })
    }

    /// Sets `mrl_outbox_lag_seconds`.
    pub fn set_lag(&self, seconds: f64) {
        if let Some(m) = &self.0 {
            m.lag_seconds.set(seconds);
        }
    }

    /// The current `mrl_outbox_lag_seconds`.
    pub fn lag(&self) -> Option<f64> {
        self.0.as_ref().map(|m| m.lag_seconds.get())
    }

    /// Counts one handled event in `mrl_outbox_delivered_total{consumer}`.
    pub fn delivered(&self, consumer: &str) {
        if let Some(m) = &self.0 {
            m.delivered.with_label_values(&[consumer]).inc();
        }
    }

    /// Events handled by `consumer` so far.
    pub fn delivered_count(&self, consumer: &str) -> u64 {
        self.0
            .as_ref()
            .map_or(0, |m| m.delivered.with_label_values(&[consumer]).get())
    }

    /// Counts one event in `mrl_outbox_quarantined_total{consumer}`.
    pub fn quarantined(&self, consumer: &str) {
        if let Some(m) = &self.0 {
            m.quarantined.with_label_values(&[consumer]).inc();
        }
    }
}

// The default registry: `/metrics` renders it next to the server's own (`Metrics::render`).
fn build() -> Result<Inner, prometheus::Error> {
    let inner = Inner {
        lag_seconds: Gauge::with_opts(Opts::new(
            "mrl_outbox_lag_seconds",
            "Age of the oldest outbox event some consumer has not handled.",
        ))?,
        delivered: IntCounterVec::new(
            Opts::new(
                "mrl_outbox_delivered_total",
                "Outbox events handled, by consumer.",
            ),
            &["consumer"],
        )?,
        quarantined: IntCounterVec::new(
            Opts::new(
                "mrl_outbox_quarantined_total",
                "Outbox events set aside after too many failures, by consumer.",
            ),
            &["consumer"],
        )?,
    };
    let r = prometheus::default_registry();
    r.register(Box::new(inner.lag_seconds.clone()))?;
    r.register(Box::new(inner.delivered.clone()))?;
    r.register(Box::new(inner.quarantined.clone()))?;
    Ok(inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_are_exported_by_name() {
        let m = EventMetrics::get();
        m.delivered("unit-test");
        assert!(m.delivered_count("unit-test") >= 1);
        let names: Vec<String> = prometheus::gather()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();
        for want in ["mrl_outbox_lag_seconds", "mrl_outbox_delivered_total"] {
            assert!(names.iter().any(|n| n == want), "{want} in {names:?}");
        }
    }
}
