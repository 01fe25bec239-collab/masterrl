//! Start a real server on loopback and send HTTP requests to it.
//!
//! ```text
//! let srv = TestServer::solo(vec![Box::new(TestModule::default())]).await;
//! let res = srv.get_authed("/api/v1/ping").await;
//! assert_eq!(res.status, 200);
//! srv.shutdown().await.unwrap();
//! ```

#[cfg(test)]
mod tests;

use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use mrl_server::config::{Config, Mode, Role};
use mrl_server::error::ServerError;
use mrl_server::seams::{ReadinessCheck, SeamError, Seams, StartupStep, LAUNCH_SECRET_HEADER};
use mrl_server::{App, AppState, Running, ServerModule, Task};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The secret `TestServer::solo` uses.
pub const TEST_SECRET: &str = "testkit-launch-secret";

/// What the test module's background task does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TaskBehaviour {
    /// No task.
    #[default]
    None,
    /// Runs until cancelled, then returns `Ok`.
    UntilCancelled,
    /// Returns an error right away.
    Fails,
    /// Ignores cancellation and sleeps for an hour.
    IgnoresCancel,
}

/// Counters a test can read after the server ran.
#[derive(Debug, Default)]
pub struct Probe {
    /// The task started.
    pub task_started: AtomicBool,
    /// The task saw cancellation and returned.
    pub task_stopped: AtomicBool,
    /// Startup steps run.
    pub steps_run: AtomicUsize,
}

/// A module with routes, docs, a readiness check, an optional task, gRPC and startup step.
///
/// Routes (under `/api/v1`): `GET /ping`, `POST /items` (201, echoes JSON),
/// `GET /slow` (sleeps 300 ms), public `GET /hello`.
#[derive(Clone, Default)]
pub struct TestModule {
    /// Readiness answer; `false` makes `/readyz` 503.
    pub not_ready: bool,
    /// Background task behaviour.
    pub task: TaskBehaviour,
    /// Registers a gRPC service.
    pub grpc: bool,
    /// Adds a startup step that fails.
    pub failing_step: bool,
    /// Shared counters.
    pub probe: Arc<Probe>,
}

impl ServerModule for TestModule {
    fn name(&self) -> &'static str {
        "testkit"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/ping", get(|| async { Json(serde_json::json!({ "pong": true })) }))
            .route(
                "/items",
                post(|Json(v): Json<serde_json::Value>| async move { (StatusCode::CREATED, Json(v)) }),
            )
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    "done"
                }),
            )
    }

    fn public_routes(&self) -> Router<AppState> {
        Router::new().route("/hello", get(|| async { "hello" }))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        use utoipa::openapi::{path::Operation, HttpMethod, PathItem, PathsBuilder};
        utoipa::openapi::OpenApiBuilder::new()
            .paths(
                PathsBuilder::new().path("/ping", PathItem::new(HttpMethod::Get, Operation::new())),
            )
            .build()
    }

    fn readiness_checks(&self) -> Vec<Arc<dyn ReadinessCheck>> {
        vec![Arc::new(Check(!self.not_ready))]
    }

    fn background_tasks(&self) -> Vec<Task> {
        let probe = self.probe.clone();
        match self.task {
            TaskBehaviour::None => Vec::new(),
            TaskBehaviour::UntilCancelled => vec![Task::new(
                "waiter",
                Role::Background,
                move |cancel| async move {
                    probe.task_started.store(true, Ordering::SeqCst);
                    cancel.cancelled().await;
                    probe.task_stopped.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )],
            TaskBehaviour::Fails => vec![Task::new("breaker", Role::Background, |_| async {
                Err(SeamError::new("testkit", "task failed on purpose"))
            })],
            TaskBehaviour::IgnoresCancel => {
                vec![Task::new("stubborn", Role::Background, |_| async {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(())
                })]
            }
        }
    }

    fn grpc_services(&self, routes: &mut tonic::service::RoutesBuilder) {
        if self.grpc {
            routes.add_service(PingGrpc);
        }
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(Step {
            fail: self.failing_step,
            probe: self.probe.clone(),
        })]
    }
}

struct Check(bool);

#[async_trait]
impl ReadinessCheck for Check {
    fn name(&self) -> &str {
        "testkit"
    }
    async fn check(&self) -> Result<(), SeamError> {
        if self.0 {
            Ok(())
        } else {
            Err(SeamError::new("testkit", "told to be not ready"))
        }
    }
}

struct Step {
    fail: bool,
    probe: Arc<Probe>,
}

#[async_trait]
impl StartupStep for Step {
    fn name(&self) -> &str {
        "prepare"
    }
    async fn run(&self) -> Result<(), SeamError> {
        self.probe.steps_run.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Err(SeamError::new("testkit", "step failed on purpose"))
        } else {
            Ok(())
        }
    }
}

/// A gRPC service that answers every call with status OK and no message.
#[derive(Clone)]
pub struct PingGrpc;

impl tonic::server::NamedService for PingGrpc {
    const NAME: &'static str = "mrl.testkit.Ping";
}

impl tower::Service<Request<tonic::body::Body>> for PingGrpc {
    type Response = axum::response::Response;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Infallible>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Infallible>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Request<tonic::body::Body>) -> Self::Future {
        let mut res = axum::response::Response::new(Body::empty());
        res.headers_mut().insert(
            "content-type",
            axum::http::HeaderValue::from_static("application/grpc"),
        );
        res.headers_mut()
            .insert("grpc-status", axum::http::HeaderValue::from_static("0"));
        std::future::ready(Ok(res))
    }
}

/// A response read in full.
#[derive(Debug)]
pub struct TestResponse {
    /// Status code.
    pub status: u16,
    /// Headers.
    pub headers: HeaderMap,
    /// Body.
    pub body: Bytes,
}

impl TestResponse {
    /// The body as JSON (`Null` when it is not JSON).
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    /// The body as text.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// A running server for tests.
pub struct TestServer {
    running: Running,
    /// Secret for solo-mode requests.
    pub secret: String,
}

impl TestServer {
    /// Starts a server with these parts.
    pub async fn start(
        config: Config,
        seams: Seams,
        modules: Vec<Box<dyn ServerModule>>,
    ) -> Result<Self, ServerError> {
        let running = App::new(config, seams, modules).start().await?;
        Ok(Self {
            running,
            secret: TEST_SECRET.to_owned(),
        })
    }

    /// Solo mode with test seams. Panics if the server does not start.
    pub async fn solo(modules: Vec<Box<dyn ServerModule>>) -> Self {
        let seams = crate::seams::build(crate::seams::solo(TEST_SECRET), Mode::Solo);
        match Self::start(Config::default(), seams, modules).await {
            Ok(s) => s,
            Err(e) => panic!("test server did not start: {}", e.explain()),
        }
    }

    /// The running server.
    pub fn running(&self) -> &Running {
        &self.running
    }

    /// API address. Panics when the `api` role is off.
    pub fn api(&self) -> SocketAddr {
        match self.running.api_addr() {
            Some(a) => a,
            None => panic!("this test server has no api listener"),
        }
    }

    /// Sends a request to `addr`.
    pub async fn request_to(
        addr: SocketAddr,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: impl Into<Body>,
    ) -> TestResponse {
        let stream = match tokio::net::TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) => panic!("connect {addr}: {e}"),
        };
        let (mut sender, conn) =
            match hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await
            {
                Ok(x) => x,
                Err(e) => panic!("handshake {addr}: {e}"),
            };
        tokio::spawn(conn);
        let mut req = Request::builder()
            .method(method)
            .uri(path)
            .header("host", addr.to_string());
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let req = match req.body(body.into()) {
            Ok(r) => r,
            Err(e) => panic!("bad test request: {e}"),
        };
        let res = match sender.send_request(req).await {
            Ok(r) => r,
            Err(e) => panic!("request {path}: {e}"),
        };
        let (parts, body) = res.into_parts();
        let body = match axum::body::to_bytes(Body::new(body), usize::MAX).await {
            Ok(b) => b,
            Err(e) => panic!("read body {path}: {e}"),
        };
        TestResponse {
            status: parts.status.as_u16(),
            headers: parts.headers,
            body,
        }
    }

    /// Sends a request to the API listener.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: impl Into<Body>,
    ) -> TestResponse {
        Self::request_to(self.api(), method, path, headers, body).await
    }

    /// `GET` without credentials.
    pub async fn get(&self, path: &str) -> TestResponse {
        self.request(Method::GET, path, &[], Body::empty()).await
    }

    /// `GET` with the solo launch secret.
    pub async fn get_authed(&self, path: &str) -> TestResponse {
        let secret = self.secret.clone();
        self.request(
            Method::GET,
            path,
            &[(LAUNCH_SECRET_HEADER, &secret)],
            Body::empty(),
        )
        .await
    }

    /// Graceful shutdown.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        self.running.shutdown().await
    }
}
