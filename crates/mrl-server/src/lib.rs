//! The MasterRL server runtime: config and modes, listeners, HTTP framework, errors,
//! telemetry, health, OpenAPI and the seams every module plugs into; PostgreSQL access,
//! durable events and the bundled PostgreSQL manager.

pub mod app;
pub mod config;
pub mod db;
pub mod error;
pub mod http;
pub mod modules;
pub mod openapi;
pub mod pgbundle;
pub mod registry;
pub mod seams;
pub mod telemetry;

pub use app::{App, AppState, Running, ServerModule, Task};
pub use config::{Config, Mode, Role};
pub use db::{Db, DbError, Tx};
pub use error::{ApiError, ErrorCode, FieldError, ServerError};
pub use seams::{RequestContext, Seams};
