//! `JobsError`: what runs and jobs code returns, and how it becomes a problem document.
//!
//! P07's `ApiError` carries only the server's own codes. Domain codes from the registry
//! (`MRL-STATE-170`, `MRL-CONFLICT-170`, …) travel as [`DomainProblem`]; the [`render`]
//! layer turns them into the same RFC 9457 document `ApiError` produces.

use crate::db::DbError;
use crate::error::{ApiError, ErrorCode, Problem, PROBLEM_JSON};
use crate::seams::{RequestMeta, SeamError};
use axum::body::Body;
use axum::extract::{OriginalUri, Request};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use mrl_domain::errors::ErrorCode as DomainCode;
use mrl_domain::state::TransitionError;
use std::sync::Arc;

// The generated catalog is plain static data with no dependencies. It is generated, so rustfmt skips it.
#[allow(clippy::all, dead_code)]
#[rustfmt::skip]
#[path = "../../../../../guidance/build/catalog.rs"]
mod catalog;

/// Codes this part adds (guidance/fragments/errors/p17.yaml).
pub mod codes {
    use mrl_domain::errors::{ErrorCode, ErrorFamily};

    const fn code(family: ErrorFamily, n: u16) -> ErrorCode {
        match ErrorCode::new(family, n) {
            Some(c) => c,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// The job can't take that step in its current state.
    pub const ILLEGAL_TRANSITION: ErrorCode = code(ErrorFamily::State, 170);
    /// The job changed between reading and writing it (compare-and-set lost).
    pub const JOB_CHANGED: ErrorCode = code(ErrorFamily::Conflict, 170);
    /// An experiment with this name already exists in the project.
    pub const EXPERIMENT_NAME_TAKEN: ErrorCode = code(ErrorFamily::Conflict, 171);
    /// `POST …/runs` came without an `Idempotency-Key`.
    pub const SUBMIT_KEY_MISSING: ErrorCode = code(ErrorFamily::Idem, 170);
    /// The CLI has no token (used by `mrl`).
    pub const CLI_NO_TOKEN: ErrorCode = code(ErrorFamily::Auth, 170);
    /// The CLI could not reach the server (used by `mrl`).
    pub const CLI_UNREACHABLE: ErrorCode = code(ErrorFamily::Env, 170);
    /// No engine is registered for the method (reused from P06).
    pub const NO_ENGINE: ErrorCode = code(ErrorFamily::Cap, 1);
    /// A required field is missing (reused from the base catalog).
    pub const FIELD_MISSING: ErrorCode = code(ErrorFamily::Val, 2);
    /// A field value is not accepted (reused from the base catalog).
    pub const FIELD_VALUE: ErrorCode = code(ErrorFamily::Val, 3);
}

/// The catalog's "what to do" steps for `code`, joined into one hint.
pub fn hint_for(code: &str) -> String {
    let steps = catalog::explain_error(code)
        .get("what_to_do")
        .and_then(|w| w.get("steps"))
        .map(|s| s.as_list())
        .unwrap_or_default();
    steps
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

/// A problem with a domain registry code, rendered by [`render`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainProblem {
    /// The registry code, like `MRL-STATE-170`.
    pub code: DomainCode,
    /// HTTP status.
    pub status: StatusCode,
    /// What happened, in plain words.
    pub detail: String,
}

/// Everything runs and jobs code can fail with.
#[derive(Debug, thiserror::Error)]
pub enum JobsError {
    /// A server error code (404, 403, 422, …).
    #[error(transparent)]
    Api(#[from] ApiError),
    /// A domain registry code (state, conflict, submit key).
    #[error("{}: {}", .0.code, .0.detail)]
    Domain(DomainProblem),
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// A chain member (validator, verifier, resolver) or another seam failed.
    #[error(transparent)]
    Seam(#[from] SeamError),
}

impl From<sqlx::Error> for JobsError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(DbError::from(e))
    }
}

impl From<TransitionError> for JobsError {
    fn from(e: TransitionError) -> Self {
        Self::domain(
            codes::ILLEGAL_TRANSITION,
            StatusCode::CONFLICT,
            capitalize(&e.to_string()),
        )
    }
}

impl JobsError {
    /// A domain-coded error.
    pub fn domain(code: DomainCode, status: StatusCode, detail: impl Into<String>) -> Self {
        Self::Domain(DomainProblem {
            code,
            status,
            detail: detail.into(),
        })
    }

    /// 404 for a missing (or hidden) `kind`.
    pub fn not_found(kind: &str) -> Self {
        Self::Api(ApiError::new(
            ErrorCode::NotFound,
            format!("No {kind} with this id exists."),
        ))
    }

    /// 409 MRL-CONFLICT-170: the row changed since it was read.
    pub fn changed(kind: &str) -> Self {
        Self::domain(
            codes::JOB_CHANGED,
            StatusCode::CONFLICT,
            format!("The {kind} changed since it was read. Nothing was changed."),
        )
    }

    /// 409 MRL-STATE-170 with a custom detail.
    pub fn illegal(detail: impl Into<String>) -> Self {
        Self::domain(codes::ILLEGAL_TRANSITION, StatusCode::CONFLICT, detail)
    }

    /// The code string, for tests and logs.
    pub fn code(&self) -> String {
        match self {
            Self::Api(e) => e.code.as_str().to_owned(),
            Self::Domain(p) => p.code.to_string(),
            Self::Db(e) => e.code().to_owned(),
            Self::Seam(_) => ErrorCode::Unavailable.as_str().to_owned(),
        }
    }

    /// True if the database rejected a write with this unique constraint.
    pub fn is_unique_violation(&self, constraint: &str) -> bool {
        match self {
            Self::Db(DbError::Query(sqlx::Error::Database(e))) => {
                e.code().as_deref() == Some("23505") && e.constraint() == Some(constraint)
            }
            _ => false,
        }
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(first) => first.to_uppercase().chain(c).collect::<String>() + ".",
        None => String::new(),
    }
}

impl IntoResponse for JobsError {
    fn into_response(self) -> Response {
        match self {
            Self::Api(e) => e.into_response(),
            Self::Seam(e) => ApiError::from(e).into_response(),
            Self::Db(e) => {
                tracing::error!(error = %e, code = e.code(), "runs/jobs database call failed");
                let api = match e {
                    DbError::Unavailable(_) => {
                        ApiError::new(ErrorCode::Unavailable, "The database did not answer.")
                    }
                    _ => ApiError::new(ErrorCode::Internal, "The server hit an internal problem."),
                };
                api.into_response()
            }
            Self::Domain(p) => {
                let mut res = p.status.into_response();
                res.extensions_mut().insert(Arc::new(p));
                res
            }
        }
    }
}

/// Route layer: renders [`DomainProblem`]s as `application/problem+json`.
pub async fn render(req: Request, next: Next) -> Response {
    let instance = req
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| req.uri().path().to_owned(), |u| u.path().to_owned());
    let trace_id = req
        .extensions()
        .get::<RequestMeta>()
        .map(|m| m.trace_id.clone())
        .unwrap_or_default();
    let res = next.run(req).await;
    let Some(p) = res.extensions().get::<Arc<DomainProblem>>().cloned() else {
        return res;
    };
    let code = p.code.to_string();
    let problem = Problem {
        r#type: format!("{}{code}", crate::error::ERROR_TYPE_BASE),
        title: p.code.title().unwrap_or("Something went wrong.").to_owned(),
        status: p.status.as_u16(),
        detail: p.detail.clone(),
        instance,
        code,
        errors: Vec::new(),
        trace_id,
    };
    let (mut parts, _) = res.into_parts();
    parts.extensions.remove::<Arc<DomainProblem>>();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts
        .headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
    let body = serde_json::to_vec(&problem).unwrap_or_else(|_| b"{}".to_vec());
    Response::from_parts(parts, Body::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_is_registered_and_catalogued() {
        for c in [
            codes::ILLEGAL_TRANSITION,
            codes::JOB_CHANGED,
            codes::EXPERIMENT_NAME_TAKEN,
            codes::SUBMIT_KEY_MISSING,
            codes::CLI_NO_TOKEN,
            codes::CLI_UNREACHABLE,
            codes::NO_ENGINE,
            codes::FIELD_MISSING,
            codes::FIELD_VALUE,
        ] {
            assert!(c.title().is_some(), "{c} missing from mrl-domain");
            assert!(
                catalog::error(&c.to_string()).is_some(),
                "{c} missing from the guidance catalog"
            );
            assert!(!hint_for(&c.to_string()).is_empty(), "{c} has no steps");
        }
    }

    #[test]
    fn transition_errors_map_to_state_170() {
        use mrl_domain::state::{Job, JobEvent, JobState, StateMachine};
        let err = Job::can_transition(JobState::Completed, JobEvent::RequestCancel).unwrap_err();
        let e = JobsError::from(err);
        assert_eq!(e.code(), "MRL-STATE-170");
        assert!(e.to_string().contains("already COMPLETED"), "{e}");
    }

    #[tokio::test]
    async fn domain_problems_render_as_problem_json() {
        use axum::routing::get;
        use tower::ServiceExt;
        let app = axum::Router::new()
            .route(
                "/x",
                get(|| async { JobsError::illegal("The job is finished.") }),
            )
            .layer(axum::middleware::from_fn(render));
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/x")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        assert_eq!(res.headers()[header::CONTENT_TYPE], PROBLEM_JSON);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "MRL-STATE-170");
        assert_eq!(json["status"], 409);
        assert_eq!(json["instance"], "/x");
        assert_eq!(json["title"], "This job can't take that step right now.");
        assert_eq!(
            json["type"],
            "https://docs.masterrl.dev/errors/MRL-STATE-170"
        );
    }
}
