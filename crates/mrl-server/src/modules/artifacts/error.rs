//! Errors of the P19 endpoints and their problem documents.
//!
//! `ApiError` carries only P07's codes, so storage and P19 codes (`MRL-STORE-…`,
//! `MRL-CONFLICT-190`, …) are rendered here: a handler returns [`Error::Coded`], and the
//! [`render`] layer turns it into `application/problem+json` with `instance` and `trace_id`.
//! P07's renderer leaves JSON bodies alone.

use crate::db::DbError;
use crate::error::{ApiError, Problem, ERROR_TYPE_BASE, PROBLEM_JSON};
use crate::seams::{RequestMeta, SeamError};
use axum::extract::{OriginalUri, Request};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use mrl_storage::StorageError;
use std::sync::Arc;

/// Codes this part adds (guidance/fragments/errors/p19.yaml).
pub mod codes {
    /// The name is taken by another artifact or secret.
    pub const NAME_TAKEN: &str = "MRL-CONFLICT-190";
    /// The upload is completed, aborted or expired.
    pub const UPLOAD_CLOSED: &str = "MRL-CONFLICT-191";
    /// The secret is being deleted.
    pub const SECRET_DELETING: &str = "MRL-CONFLICT-192";
    /// Parts are missing or have the wrong size.
    pub const PARTS_INCOMPLETE: &str = "MRL-STORE-190";
    /// The secret does not allow this use.
    pub const SECRET_USE: &str = "MRL-PERM-190";
    /// The secret has no value (never saved, or its environment variable is unset).
    pub const SECRET_EMPTY: &str = "MRL-RES-190";
    /// The server has no `SecretStore`.
    pub const NO_SECRET_STORE: &str = "MRL-ENV-190";
}

/// A P19 request error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A P07 error (validation, authorization, not found, …).
    #[error(transparent)]
    Api(#[from] ApiError),
    /// A catalogued code outside P07's list.
    #[error("{code}: {detail}")]
    Coded {
        /// The code, like `MRL-STORE-010`.
        code: &'static str,
        /// HTTP status.
        status: StatusCode,
        /// What happened, in plain words. Never contains secrets.
        detail: String,
    },
}

impl Error {
    /// An error with a catalogued code.
    pub fn coded(code: &'static str, status: StatusCode, detail: impl Into<String>) -> Self {
        Self::Coded {
            code,
            status,
            detail: detail.into(),
        }
    }

    /// The code string.
    pub fn code(&self) -> &str {
        match self {
            Self::Api(e) => e.code.as_str(),
            Self::Coded { code, .. } => code,
        }
    }

    /// The HTTP status.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Api(e) => e.code.status(),
            Self::Coded { status, .. } => *status,
        }
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        let status = match e.code() {
            "MRL-STORE-003" => StatusCode::NOT_FOUND,
            "MRL-STORE-007" | "MRL-STORE-008" | "MRL-STORE-009" => StatusCode::FORBIDDEN,
            "MRL-STORE-013" => StatusCode::CONFLICT,
            "MRL-STORE-001" | "MRL-STORE-002" | "MRL-STORE-010" | "MRL-STORE-011"
            | "MRL-STORE-012" => StatusCode::UNPROCESSABLE_ENTITY,
            "MRL-STORE-004" | "MRL-STORE-006" => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        // Service and local-file errors may name hosts or paths; keep them in the log only.
        let detail = if status.is_server_error() {
            tracing::error!(error = %e, code = e.code(), "storage call failed");
            "The storage service did not answer as expected.".to_owned()
        } else {
            e.to_string()
        };
        Self::coded(e.code(), status, detail)
    }
}

impl From<DbError> for Error {
    fn from(e: DbError) -> Self {
        tracing::error!(error = %e, code = e.code(), "database call failed");
        let status = match e {
            DbError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            DbError::Contended { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::coded(
            e.code(),
            status,
            "The database could not finish this request.",
        )
    }
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        DbError::from(e).into()
    }
}

impl From<SeamError> for Error {
    fn from(e: SeamError) -> Self {
        Self::Api(ApiError::from(e))
    }
}

/// Stored in the response until [`render`] writes the problem body.
#[derive(Debug, Clone)]
struct Pending {
    code: &'static str,
    detail: String,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Self::Api(e) => e.into_response(),
            Self::Coded {
                code,
                status,
                detail,
            } => {
                let mut res = status.into_response();
                res.extensions_mut()
                    .insert(Arc::new(Pending { code, detail }));
                res
            }
        }
    }
}

/// Layer on every P19 route: renders [`Error::Coded`] responses as problem documents and
/// marks every response not replayable.
pub async fn render(req: Request, next: Next) -> Response {
    let trace_id = req
        .extensions()
        .get::<RequestMeta>()
        .map(|m| m.trace_id.clone())
        .unwrap_or_default();
    let instance = req.extensions().get::<OriginalUri>().map_or_else(
        || crate::http::shown_path(req.uri().path()).to_owned(),
        |u| crate::http::shown_path(u.path()).to_owned(),
    );
    let mut res = next.run(req).await;
    // P19 answers are never replayed by the generic Idempotency-Key middleware: tickets go
    // stale and every answer depends on current permissions. Creation endpoints dedupe
    // retries themselves (see `crate::modules::artifacts::Idempotency`).
    res.extensions_mut()
        .insert(crate::http::idempotency::NotReplayable);
    let Some(p) = res.extensions().get::<Arc<Pending>>().cloned() else {
        return res;
    };
    let status = res.status();
    let title = p
        .code
        .parse::<mrl_domain::errors::ErrorCode>()
        .ok()
        .and_then(|c| c.title())
        .unwrap_or("Something went wrong.");
    let problem = Problem {
        r#type: format!("{ERROR_TYPE_BASE}{}", p.code),
        title: title.to_owned(),
        status: status.as_u16(),
        detail: p.detail.clone(),
        instance,
        code: p.code.to_owned(),
        errors: Vec::new(),
        trace_id,
    };
    let body = serde_json::to_vec(&problem).unwrap_or_default();
    let (mut parts, _) = res.into_parts();
    parts.extensions.remove::<Arc<Pending>>();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts
        .headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
    Response::from_parts(parts, axum::body::Body::from(body))
}
