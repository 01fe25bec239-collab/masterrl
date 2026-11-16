//! Telemetry: JSON logs on stderr, optional OTLP trace export, Prometheus metrics and
//! the `/healthz` and `/readyz` handlers.
//!
//! Log rules: lowercase messages, structured fields, never secrets or tokens.
//! Metric names: `mrl_<component>_<thing>_<unit>`.

use crate::app::AppState;
use crate::config::TelemetryConfig;
use crate::error::{ErrorCode, ServerError};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry,
};
use serde::Serialize;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// How long one readiness check may take.
pub const READINESS_TIMEOUT: Duration = Duration::from_secs(2);

/// Keeps the trace exporter alive; call `shutdown` to flush on exit.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
}

impl Telemetry {
    /// Flushes and stops trace export.
    pub fn shutdown(self) {
        if let Some(p) = self.provider {
            if let Err(e) = p.shutdown() {
                tracing::warn!(error = %e, "trace exporter did not shut down cleanly");
            }
        }
    }
}

/// Sets up the global log subscriber (and OTLP export when configured).
/// If a subscriber is already installed (tests), the existing one is kept.
pub fn init(config: &TelemetryConfig) -> Result<Telemetry, ServerError> {
    // The level was checked by `Config::validate`; fall back to `info` defensively.
    let filter = EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_writer(std::io::stderr);
    let provider = match &config.otlp_endpoint {
        None => None,
        Some(endpoint) => {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .map_err(|e| ServerError::new(ErrorCode::Telemetry, format!("{endpoint}: {e}")))?;
            let resource = opentelemetry_sdk::Resource::builder()
                .with_service_name("mrl-server")
                .build();
            Some(
                SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(resource)
                    .build(),
            )
        }
    };
    let otel = provider
        .as_ref()
        .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("mrl-server")));
    // Err only means a subscriber exists already (tests start many servers in one process).
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel)
        .try_init();
    Ok(Telemetry { provider })
}

/// Prometheus registry and the server's own metrics. Modules register theirs on `registry`.
pub struct Metrics {
    /// Shared registry, rendered at `/metrics`.
    pub registry: Registry,
    http_requests: IntCounterVec,
    http_duration: HistogramVec,
}

impl Metrics {
    /// Creates the registry and core metrics.
    pub fn new() -> Result<Self, ServerError> {
        let err = |e: prometheus::Error| ServerError::new(ErrorCode::Telemetry, e.to_string());
        let registry = Registry::new();
        let http_requests = IntCounterVec::new(
            Opts::new(
                "mrl_http_requests_total",
                "HTTP requests by method, route and status.",
            ),
            &["method", "route", "status"],
        )
        .map_err(err)?;
        let http_duration = HistogramVec::new(
            HistogramOpts::new("mrl_http_request_duration_seconds", "HTTP request latency."),
            &["method", "route"],
        )
        .map_err(err)?;
        let build = IntGaugeVec::new(
            Opts::new(
                "mrl_server_build_info",
                "Always 1; labels carry the version.",
            ),
            &["version"],
        )
        .map_err(err)?;
        build.with_label_values(&[env!("CARGO_PKG_VERSION")]).set(1);
        registry
            .register(Box::new(http_requests.clone()))
            .map_err(err)?;
        registry
            .register(Box::new(http_duration.clone()))
            .map_err(err)?;
        registry.register(Box::new(build)).map_err(err)?;
        Ok(Self {
            registry,
            http_requests,
            http_duration,
        })
    }

    /// Records one finished request.
    pub fn observe_http(&self, method: &str, route: &str, status: u16, took: Duration) {
        self.http_requests
            .with_label_values(&[method, route, &status.to_string()])
            .inc();
        self.http_duration
            .with_label_values(&[method, route])
            .observe(took.as_secs_f64());
    }

    /// Text exposition format: the server registry plus the process-wide default registry,
    /// where modules register their own metrics.
    pub fn render(&self) -> String {
        let mut families = self.registry.gather();
        families.extend(prometheus::gather());
        families.sort_by(|a, b| a.name().cmp(b.name()));
        let mut buf = Vec::new();
        if let Err(e) = prometheus::TextEncoder::new().encode(&families, &mut buf) {
            tracing::error!(error = %e, "could not render metrics");
        }
        String::from_utf8_lossy(&buf).into_owned()
    }
}

/// `/healthz` body.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct Health {
    /// `ok`.
    pub status: &'static str,
}

/// One readiness check result.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CheckResult {
    /// Check name.
    pub name: String,
    /// `ready` or `not_ready`.
    pub status: &'static str,
    /// Why it is not ready.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// `/readyz` body.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct Readiness {
    /// `ready`, `not_ready` or `draining`.
    pub status: &'static str,
    /// Module checks.
    pub checks: Vec<CheckResult>,
}

/// Liveness: the process is up.
#[utoipa::path(get, path = "/healthz", tag = "health",
    responses((status = 200, description = "The process is up.", body = Health)))]
pub async fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}

/// Readiness: every module check passes and the server is not shutting down.
#[utoipa::path(get, path = "/readyz", tag = "health",
    responses(
        (status = 200, description = "Ready for traffic.", body = Readiness),
        (status = 503, description = "Not ready, or shutting down.", body = Readiness)))]
pub async fn readyz(State(state): State<AppState>) -> Response {
    if state.is_draining() {
        let body = Readiness {
            status: "draining",
            checks: Vec::new(),
        };
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    let mut set = tokio::task::JoinSet::new();
    for (i, check) in state.readiness_checks().iter().cloned().enumerate() {
        set.spawn(async move {
            let name = check.name().to_owned();
            let outcome = match tokio::time::timeout(READINESS_TIMEOUT, check.check()).await {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(e.to_string()),
                Err(_) => Some(format!(
                    "no answer within {} s",
                    READINESS_TIMEOUT.as_secs()
                )),
            };
            (i, name, outcome)
        });
    }
    let mut results = Vec::new();
    while let Some(joined) = set.join_next().await {
        results.push(match joined {
            Ok(r) => r,
            Err(e) => (
                usize::MAX,
                "unknown".to_owned(),
                Some(format!("check stopped: {e}")),
            ),
        });
    }
    results.sort_by_key(|(i, _, _)| *i);
    let ready = results.iter().all(|(_, _, o)| o.is_none());
    let checks = results
        .into_iter()
        .map(|(_, name, detail)| CheckResult {
            name,
            status: if detail.is_none() {
                "ready"
            } else {
                "not_ready"
            },
            detail,
        })
        .collect();
    let (code, status) = if ready {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not_ready")
    };
    (code, Json(Readiness { status, checks })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_names_follow_the_convention() {
        let m = Metrics::new().unwrap();
        m.observe_http("GET", "/healthz", 200, Duration::from_millis(3));
        let text = m.render();
        for name in [
            "mrl_http_requests_total",
            "mrl_http_request_duration_seconds",
            "mrl_server_build_info",
        ] {
            assert!(text.contains(name), "{name} missing");
        }
        for family in m.registry.gather() {
            assert!(family.name().starts_with("mrl_"), "{}", family.name());
        }
    }

    #[test]
    fn render_includes_metrics_from_the_default_registry() {
        let c = prometheus::register_int_counter!(
            "mrl_test_default_registry_total",
            "Counter on the default registry."
        )
        .unwrap();
        c.inc();
        let text = Metrics::new().unwrap().render();
        assert!(text.contains("mrl_test_default_registry_total 1"), "{text}");
        assert!(text.contains("mrl_server_build_info"));
    }

    #[test]
    fn init_without_otlp_is_ok_twice() {
        init(&TelemetryConfig::default()).unwrap().shutdown();
        init(&TelemetryConfig::default()).unwrap().shutdown();
    }
}
