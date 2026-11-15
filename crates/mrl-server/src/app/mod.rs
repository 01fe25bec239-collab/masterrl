//! Bootstrap and lifecycle.
//!
//! Order: load config → init telemetry → build seams (in `main`) → register modules →
//! run startup steps → bind and start listeners → run tasks → on cancel, drain (30 s default).
//! Every spawned task is owned by one `TaskTracker` and stopped through one `CancellationToken`.

mod module;

pub use module::{ServerModule, Task};

use crate::config::{Config, Mode, Role};
use crate::error::{ApiError, ErrorCode, ServerError};
use crate::http::rate_limit::RateLimiter;
use crate::seams::{Decision, ReadinessCheck, RequestContext, Resource, Seams};
use crate::telemetry::Metrics;
use axum::Router;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Shared state every handler can extract with `State<AppState>`.
#[derive(Clone)]
pub struct AppState {
    /// The seams of this process.
    pub seams: Arc<Seams>,
    /// Checked settings.
    pub config: Arc<Config>,
    /// Prometheus registry; modules register their metrics here.
    pub metrics: Arc<Metrics>,
    inner: Arc<Inner>,
}

struct Inner {
    readiness: Vec<Arc<dyn ReadinessCheck>>,
    draining: AtomicBool,
    limiter: RateLimiter,
    openapi_json: String,
    tasks: TaskTracker,
}

impl AppState {
    /// Builds state for these modules (collects readiness checks and the OpenAPI document).
    pub fn new(
        config: Config,
        seams: Seams,
        modules: &[Box<dyn ServerModule>],
    ) -> Result<Self, ServerError> {
        let openapi_json = crate::openapi::document(modules)
            .to_pretty_json()
            .map_err(|e| ServerError::new(ErrorCode::Internal, format!("openapi document: {e}")))?;
        Ok(Self {
            metrics: Arc::new(Metrics::new()?),
            inner: Arc::new(Inner {
                readiness: modules.iter().flat_map(|m| m.readiness_checks()).collect(),
                draining: AtomicBool::new(false),
                limiter: RateLimiter::new(config.server.rate_limit.clone()),
                openapi_json,
                tasks: TaskTracker::new(),
            }),
            seams: Arc::new(seams),
            config: Arc::new(config),
        })
    }

    /// Asks the `Authorizer`: `Deny` → 403 MRL-PERM-001, `Hide` → 404 MRL-RES-001.
    pub async fn authorize(
        &self,
        ctx: &RequestContext,
        action: &str,
        resource: &Resource,
    ) -> Result<(), ApiError> {
        match self
            .seams
            .authorizer
            .authorize(ctx, action, resource)
            .await?
        {
            Decision::Allow => Ok(()),
            Decision::Deny => Err(ApiError::new(
                ErrorCode::Forbidden,
                format!("Your account cannot do {action} on this {}.", resource.kind),
            )),
            Decision::Hide => Err(ApiError::new(
                ErrorCode::NotFound,
                format!("No {} with this id exists.", resource.kind),
            )),
        }
    }

    /// The task tracker that owns every task this server spawns.
    pub fn tasks(&self) -> &TaskTracker {
        &self.inner.tasks
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.inner.draining.load(Ordering::Relaxed)
    }

    pub(crate) fn readiness_checks(&self) -> &[Arc<dyn ReadinessCheck>] {
        &self.inner.readiness
    }

    pub(crate) fn limiter(&self) -> &RateLimiter {
        &self.inner.limiter
    }

    pub(crate) fn openapi_json(&self) -> String {
        self.inner.openapi_json.clone()
    }
}

/// A server ready to start.
pub struct App {
    config: Config,
    seams: Seams,
    modules: Vec<Box<dyn ServerModule>>,
}

impl App {
    /// A server with checked config, built seams and registered modules.
    pub fn new(config: Config, seams: Seams, modules: Vec<Box<dyn ServerModule>>) -> Self {
        Self {
            config,
            seams,
            modules,
        }
    }

    /// Runs startup steps, binds listeners and starts tasks.
    pub async fn start(self) -> Result<Running, ServerError> {
        for m in &self.modules {
            for step in m.startup_steps() {
                step.run().await.map_err(|e| {
                    ServerError::new(
                        ErrorCode::StartupStep,
                        format!("{}/{}: {e}", m.name(), step.name()),
                    )
                })?;
                tracing::info!(module = m.name(), step = step.name(), "startup step done");
            }
        }
        let solo = self.config.server.mode == Mode::Solo;
        let listeners = self.config.server.listeners.clone();
        let grace = Duration::from_secs(self.config.server.shutdown_grace_seconds);
        let has = |r| self.config.has_role(r);
        let (api_on, gateway_on) = (has(Role::Api), has(Role::Gateway));
        let mut grpc = tonic::service::RoutesBuilder::default();
        for m in &self.modules {
            m.grpc_services(&mut grpc);
        }
        let grpc = grpc.routes().into_axum_router();
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let pick = |configured| if solo { loopback } else { configured };

        // Bind everything first, so a port clash fails startup before anything runs.
        let api = if api_on {
            Some(bind(pick(listeners.api)).await?)
        } else {
            None
        };
        let grpc_listener = if gateway_on && grpc.has_routes() {
            Some(bind(pick(listeners.grpc)).await?)
        } else {
            None
        };
        let internal = if solo {
            None
        } else {
            Some(bind(listeners.internal).await?)
        };

        let state = AppState::new(self.config, self.seams, &self.modules)?;
        let token = CancellationToken::new();
        let failure: Arc<Mutex<Option<ServerError>>> = Arc::default();
        let tracker = state.tasks().clone();
        let mut addrs = Addrs::default();
        let serve = |listener: TcpListener,
                     router: Router,
                     name: &'static str|
         -> Option<SocketAddr> {
            let addr = listener.local_addr().ok();
            let (token, failure) = (token.clone(), failure.clone());
            tracker.spawn(async move {
                let shutdown = token.clone().cancelled_owned();
                if let Err(e) = axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown)
                    .await
                {
                    fail(
                        &failure,
                        &token,
                        ServerError::new(ErrorCode::Bind, format!("{name} listener: {e}")),
                    );
                }
            });
            tracing::info!(listener = name, addr = %addr.map(|a| a.to_string()).unwrap_or_default(), "listening");
            addr
        };
        if let Some(l) = api {
            addrs.api = serve(
                l,
                crate::http::api_router(state.clone(), &self.modules, solo),
                "api",
            );
        }
        if let Some(l) = grpc_listener {
            addrs.grpc = serve(
                l,
                crate::http::grpc_router(grpc, state.clone(), solo),
                "grpc",
            );
        }
        if let Some(l) = internal {
            addrs.internal = serve(l, crate::http::internal_router(state.clone()), "internal");
        }
        for m in &self.modules {
            for task in m.background_tasks().into_iter().chain(m.event_consumers()) {
                if !state.config.has_role(task.role) {
                    continue;
                }
                let (token, failure, name) = (token.clone(), failure.clone(), task.name.clone());
                let fut = task.start(token.child_token());
                tracker.spawn(async move {
                    match fut.await {
                        Ok(()) => tracing::info!(task = %name, "task stopped"),
                        Err(e) => fail(
                            &failure,
                            &token,
                            ServerError::new(ErrorCode::TaskFailed, format!("{name}: {e}")),
                        ),
                    }
                });
            }
        }
        Ok(Running {
            addrs,
            token,
            tracker,
            state,
            failure,
            grace,
        })
    }
}

fn fail(slot: &Mutex<Option<ServerError>>, token: &CancellationToken, err: ServerError) {
    tracing::error!(code = %err.code, detail = %err.detail, "server stopping after a failure");
    if let Ok(mut s) = slot.lock() {
        s.get_or_insert(err);
    }
    token.cancel();
}

async fn bind(addr: SocketAddr) -> Result<TcpListener, ServerError> {
    TcpListener::bind(addr)
        .await
        .map_err(|e| ServerError::new(ErrorCode::Bind, format!("{addr}: {e}")))
}

#[derive(Debug, Default, Clone, Copy)]
struct Addrs {
    api: Option<SocketAddr>,
    grpc: Option<SocketAddr>,
    internal: Option<SocketAddr>,
}

/// A started server.
pub struct Running {
    addrs: Addrs,
    token: CancellationToken,
    tracker: TaskTracker,
    state: AppState,
    failure: Arc<Mutex<Option<ServerError>>>,
    grace: Duration,
}

impl Running {
    /// HTTP API address (when the `api` role is on).
    pub fn api_addr(&self) -> Option<SocketAddr> {
        self.addrs.api
    }

    /// gRPC address (when a module registered a gRPC service and `gateway` is on).
    pub fn grpc_addr(&self) -> Option<SocketAddr> {
        self.addrs.grpc
    }

    /// Internal address (team mode only).
    pub fn internal_addr(&self) -> Option<SocketAddr> {
        self.addrs.internal
    }

    /// Cancel this to start a graceful shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Waits until shutdown starts, then drains for up to the grace period.
    pub async fn wait(self) -> Result<(), ServerError> {
        self.token.cancelled().await;
        self.state.inner.draining.store(true, Ordering::Relaxed);
        tracing::info!(grace_seconds = self.grace.as_secs(), "shutting down");
        self.tracker.close();
        if tokio::time::timeout(self.grace, self.tracker.wait())
            .await
            .is_err()
        {
            return Err(ServerError::new(
                ErrorCode::DrainTimeout,
                format!(
                    "{} task(s) still running after {} s",
                    self.tracker.len(),
                    self.grace.as_secs()
                ),
            ));
        }
        let failed = self.failure.lock().ok().and_then(|mut s| s.take());
        match failed {
            Some(e) => Err(e),
            None => {
                tracing::info!("shutdown complete");
                Ok(())
            }
        }
    }

    /// Starts a graceful shutdown and waits for it.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        self.token.cancel();
        self.wait().await
    }
}

/// Resolves on Ctrl-C, or SIGTERM on Unix.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "could not listen for ctrl-c");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "could not listen for sigterm");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
}
