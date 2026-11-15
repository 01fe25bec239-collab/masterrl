//! The MasterRL server runtime: config and modes, listeners, HTTP framework, errors,
//! telemetry, health, OpenAPI and the seams every module plugs into.

pub mod app;
pub mod config;
pub mod error;
pub mod http;
pub mod modules;
pub mod openapi;
pub mod registry;
pub mod seams;
pub mod telemetry;

pub use app::{App, AppState, Running, ServerModule, Task};
pub use config::{Config, Mode, Role};
pub use error::{ApiError, ErrorCode, FieldError, ServerError};
pub use seams::{RequestContext, Seams};
