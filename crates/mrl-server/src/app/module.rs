//! `ServerModule`: how a part plugs routes, docs, tasks and checks into the server.

use super::AppState;
use crate::config::Role;
use crate::seams::{ReadinessCheck, SeamError, StartupStep};
use axum::Router;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

type TaskFn = Box<
    dyn FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = Result<(), SeamError>> + Send>>
        + Send,
>;

/// A long-running task (background job or event consumer) owned by the server.
///
/// It must return soon after `cancel` fires. `Err` shuts the server down (MRL-SRV-007).
pub struct Task {
    /// Name for logs and errors.
    pub name: String,
    /// The role a process needs to run this task.
    pub role: Role,
    run: TaskFn,
}

impl Task {
    /// A task that runs `f(cancel)` when this process has `role`.
    pub fn new<F, Fut>(name: impl Into<String>, role: Role, f: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), SeamError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            role,
            run: Box::new(move |cancel| Box::pin(f(cancel))),
        }
    }

    pub(crate) fn start(
        self,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), SeamError>> + Send>> {
        (self.run)(cancel)
    }
}

/// One feature area of the server. Every method has an empty default.
///
/// Register a module by adding one line inside `// <mrl:register>` in `registry.rs`,
/// and its `mod` line inside `// <mrl:modules>` in `modules/mod.rs`.
pub trait ServerModule: Send + Sync {
    /// Short unique name, like `runs`.
    fn name(&self) -> &'static str;

    /// Signed-in routes, mounted under `/api/v1`.
    fn routes(&self) -> Router<AppState> {
        Router::new()
    }

    /// Routes that need no sign-in (like a login endpoint), mounted under `/api/v1`.
    fn public_routes(&self) -> Router<AppState> {
        Router::new()
    }

    /// OpenAPI fragment for `routes` and `public_routes`, with paths relative to `/api/v1`.
    fn openapi(&self) -> utoipa::openapi::OpenApi {
        utoipa::openapi::OpenApiBuilder::new().build()
    }

    /// Background tasks.
    fn background_tasks(&self) -> Vec<Task> {
        Vec::new()
    }

    /// Checks behind `/readyz`.
    fn readiness_checks(&self) -> Vec<Arc<dyn ReadinessCheck>> {
        Vec::new()
    }

    /// Event consumers; they run like background tasks.
    fn event_consumers(&self) -> Vec<Task> {
        Vec::new()
    }

    /// gRPC services. Adding any makes the server bind the gRPC listener.
    fn grpc_services(&self, routes: &mut tonic::service::RoutesBuilder) {
        let _ = routes;
    }

    /// Steps run in order before listeners open, like database migrations.
    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        Vec::new()
    }
}
