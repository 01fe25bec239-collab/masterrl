//! Error codes, startup errors and the RFC 9457 `ApiError` that every handler returns.
//!
//! Every code has a plain-language entry in the guidance catalog (`guidance/fragments/errors/p07.yaml`).
//! Titles come from that catalog, so the API and the desktop app say the same thing.

use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use std::sync::Arc;

// The generated catalog is plain static data with no dependencies. It is generated, so rustfmt skips it.
#[allow(clippy::all)]
#[rustfmt::skip]
#[path = "../../../../guidance/build/catalog.rs"]
mod catalog;

/// Base URL of the error reference; `type` is this plus the code.
pub const ERROR_TYPE_BASE: &str = "https://docs.masterrl.dev/errors/";

macro_rules! codes {
    ($($(#[$doc:meta])* $name:ident = $code:literal, $status:ident;)+) => {
        /// Every error code this crate can produce.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorCode {
            $($(#[$doc])* $name,)+
        }

        impl ErrorCode {
            /// All codes, for catalog checks.
            pub const ALL: &'static [ErrorCode] = &[$(ErrorCode::$name,)+];

            /// The code string, like `MRL-AUTH-001`.
            pub fn as_str(self) -> &'static str {
                match self { $(ErrorCode::$name => $code,)+ }
            }

            /// The HTTP status a request error maps to (500 for startup errors).
            pub fn status(self) -> StatusCode {
                match self { $(ErrorCode::$name => StatusCode::$status,)+ }
            }
        }
    };
}

codes! {
    /// Settings file could not be read.
    ConfigUnreadable = "MRL-CFG-001", INTERNAL_SERVER_ERROR;
    /// Settings file could not be parsed, or has an unknown key.
    ConfigParse = "MRL-CFG-002", INTERNAL_SERVER_ERROR;
    /// A setting has a value the server cannot use.
    ConfigValue = "MRL-CFG-003", INTERNAL_SERVER_ERROR;
    /// An `MRL_*` environment override could not be used.
    ConfigEnv = "MRL-CFG-004", INTERNAL_SERVER_ERROR;
    /// A seam this mode needs has no implementation.
    SeamMissing = "MRL-SRV-001", INTERNAL_SERVER_ERROR;
    /// Team mode has no `IdentityProvider`.
    IdentityProviderMissing = "MRL-SRV-002", INTERNAL_SERVER_ERROR;
    /// A listener could not bind its address.
    Bind = "MRL-SRV-003", INTERNAL_SERVER_ERROR;
    /// A `StartupStep` failed.
    StartupStep = "MRL-SRV-004", INTERNAL_SERVER_ERROR;
    /// Telemetry (OTLP export) could not be set up.
    Telemetry = "MRL-SRV-005", INTERNAL_SERVER_ERROR;
    /// Shutdown did not finish within the grace period.
    DrainTimeout = "MRL-SRV-006", INTERNAL_SERVER_ERROR;
    /// A background task or event consumer failed.
    TaskFailed = "MRL-SRV-007", INTERNAL_SERVER_ERROR;
    /// The request is not authenticated.
    Unauthenticated = "MRL-AUTH-001", UNAUTHORIZED;
    /// The principal may not do this.
    Forbidden = "MRL-PERM-001", FORBIDDEN;
    /// The resource does not exist (or is hidden from this principal).
    NotFound = "MRL-RES-001", NOT_FOUND;
    /// The body is not readable (bad JSON or content type).
    BadBody = "MRL-VAL-001", BAD_REQUEST;
    /// Field error: a required field is missing.
    FieldMissing = "MRL-VAL-002", UNPROCESSABLE_ENTITY;
    /// Field error: a field value is not accepted.
    FieldValue = "MRL-VAL-003", UNPROCESSABLE_ENTITY;
    /// Some fields need a change; see `errors[]`.
    Validation = "MRL-VAL-004", UNPROCESSABLE_ENTITY;
    /// `limit` or `cursor` could not be used.
    Pagination = "MRL-VAL-005", BAD_REQUEST;
    /// `filter` could not be parsed or names an unknown field.
    Filter = "MRL-VAL-006", BAD_REQUEST;
    /// The body is over the size limit.
    BodyTooLarge = "MRL-VAL-007", PAYLOAD_TOO_LARGE;
    /// `If-Match` does not match the current `ETag`.
    PreconditionFailed = "MRL-CONFLICT-001", PRECONDITION_FAILED;
    /// `If-Match` is required but missing.
    PreconditionRequired = "MRL-CONFLICT-002", PRECONDITION_REQUIRED;
    /// A request with this idempotency key is still running.
    IdempotencyInProgress = "MRL-IDEM-001", CONFLICT;
    /// The idempotency key was used with a different body.
    IdempotencyMismatch = "MRL-IDEM-002", UNPROCESSABLE_ENTITY;
    /// The idempotency key is empty, too long or not visible ASCII.
    IdempotencyKey = "MRL-IDEM-003", BAD_REQUEST;
    /// Rate limit hit.
    RateLimited = "MRL-QUOTA-001", TOO_MANY_REQUESTS;
    /// No route at this path.
    RouteNotFound = "MRL-ROUTE-001", NOT_FOUND;
    /// The route exists but not for this method.
    MethodNotAllowed = "MRL-ROUTE-002", METHOD_NOT_ALLOWED;
    /// Unexpected server error.
    Internal = "MRL-INTERNAL-001", INTERNAL_SERVER_ERROR;
    /// A backing service (a seam) failed.
    Unavailable = "MRL-INTERNAL-002", SERVICE_UNAVAILABLE;
}

impl ErrorCode {
    /// The plain-language title from the guidance catalog.
    pub fn title(self) -> &'static str {
        catalog::explain_error(self.as_str())
            .str("title")
            .unwrap_or("Something went wrong.")
    }

    /// The "what to do" steps from the guidance catalog.
    pub fn steps(self) -> Vec<&'static str> {
        catalog::explain_error(self.as_str())
            .get("what_to_do")
            .and_then(|w| w.get("steps"))
            .map(|s| s.as_list().iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default()
    }

    /// The `type` URI for problem documents.
    pub fn type_uri(self) -> String {
        format!("{ERROR_TYPE_BASE}{}", self.as_str())
    }

    /// The code that fits a bare error status (one with no problem body yet).
    pub fn for_status(status: StatusCode) -> ErrorCode {
        match status {
            StatusCode::NOT_FOUND => ErrorCode::RouteNotFound,
            StatusCode::METHOD_NOT_ALLOWED => ErrorCode::MethodNotAllowed,
            StatusCode::PAYLOAD_TOO_LARGE => ErrorCode::BodyTooLarge,
            StatusCode::UNAUTHORIZED => ErrorCode::Unauthenticated,
            StatusCode::FORBIDDEN => ErrorCode::Forbidden,
            StatusCode::TOO_MANY_REQUESTS => ErrorCode::RateLimited,
            StatusCode::SERVICE_UNAVAILABLE => ErrorCode::Unavailable,
            StatusCode::UNSUPPORTED_MEDIA_TYPE | StatusCode::BAD_REQUEST => ErrorCode::BadBody,
            _ => ErrorCode::Internal,
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A startup or runtime error of the server process (not a request error).
#[derive(Debug, thiserror::Error)]
#[error("{code}: {detail}")]
pub struct ServerError {
    /// The catalogued code.
    pub code: ErrorCode,
    /// What exactly went wrong, in plain words. Never contains secrets.
    pub detail: String,
}

impl ServerError {
    /// A new error with a code and detail.
    pub fn new(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    /// The full plain-language explanation: code, title, detail and steps.
    pub fn explain(&self) -> String {
        let mut out = format!(
            "{} — {}\nDetails: {}\nWhat to do:",
            self.code,
            self.code.title(),
            self.detail
        );
        for step in self.code.steps() {
            out.push_str("\n  - ");
            out.push_str(step);
        }
        out
    }
}

/// One field problem inside a 422 response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct FieldError {
    /// JSON Pointer to the field, like `/config/peft/r`.
    pub path: String,
    /// Field-level code, like `MRL-VAL-003`.
    pub code: String,
    /// What is wrong with the field.
    pub message: String,
    /// How to fix it.
    pub hint: String,
}

impl FieldError {
    /// A field error with its code.
    pub fn new(
        path: impl Into<String>,
        code: ErrorCode,
        message: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            code: code.as_str().to_owned(),
            message: message.into(),
            hint: hint.into(),
        }
    }
}

/// A request error. Handlers return `Result<_, ApiError>`.
///
/// `into_response` stores the error in the response; the HTTP layer renders it as
/// `application/problem+json` with the request's `trace_id` and path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// The catalogued code; decides status, title and type.
    pub code: ErrorCode,
    /// What happened in this case, in plain words.
    pub detail: String,
    /// Field problems (422).
    pub errors: Vec<FieldError>,
    /// Seconds for `Retry-After` (409 in progress, 429).
    pub retry_after: Option<u64>,
}

impl ApiError {
    /// An error with a code and detail.
    pub fn new(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
            errors: Vec::new(),
            retry_after: None,
        }
    }

    /// A 422 with field errors.
    pub fn validation(errors: Vec<FieldError>) -> Self {
        let n = errors.len();
        Self {
            errors,
            ..Self::new(
                ErrorCode::Validation,
                format!("{n} field(s) need a change."),
            )
        }
    }

    /// Adds a `Retry-After` value.
    pub fn retry_after(mut self, seconds: u64) -> Self {
        self.retry_after = Some(seconds);
        self
    }

    /// The problem document for this error.
    pub fn to_problem(&self, instance: &str, trace_id: &str) -> Problem {
        Problem {
            r#type: self.code.type_uri(),
            title: self.code.title().to_owned(),
            status: self.code.status().as_u16(),
            detail: self.detail.clone(),
            instance: instance.to_owned(),
            code: self.code.as_str().to_owned(),
            errors: self.errors.clone(),
            trace_id: trace_id.to_owned(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for ApiError {}

impl From<crate::seams::SeamError> for ApiError {
    fn from(err: crate::seams::SeamError) -> Self {
        // The detail stays generic: seam errors may name internal hosts.
        tracing::error!(error = %err, "seam call failed");
        ApiError::new(
            ErrorCode::Unavailable,
            "A service behind the server did not answer.",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut res = self.code.status().into_response();
        if let Some(secs) = self.retry_after {
            res.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, HeaderValue::from(secs));
        }
        res.extensions_mut().insert(Arc::new(self));
        res
    }
}

/// RFC 9457 problem document.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct Problem {
    /// `https://docs.masterrl.dev/errors/<code>`.
    pub r#type: String,
    /// Plain-language title from the guidance catalog.
    pub title: String,
    /// HTTP status.
    pub status: u16,
    /// What happened in this case.
    pub detail: String,
    /// The request path.
    pub instance: String,
    /// The error code, like `MRL-AUTH-001`.
    pub code: String,
    /// Field problems, present on 422.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<FieldError>,
    /// Trace id to quote in reports.
    pub trace_id: String,
}

/// The media type of problem documents.
pub const PROBLEM_JSON: &str = "application/problem+json";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_is_in_the_catalog() {
        for code in ErrorCode::ALL {
            assert!(
                catalog::error(code.as_str()).is_some(),
                "{code} has no guidance entry"
            );
            assert!(!code.steps().is_empty(), "{code} has no steps");
        }
    }

    #[test]
    fn problem_has_all_rfc9457_fields() {
        let err = ApiError::validation(vec![FieldError::new(
            "/config/peft/r",
            ErrorCode::FieldValue,
            "r must be a power of two",
            "use 8, 16 or 32",
        )]);
        let json = serde_json::to_value(err.to_problem("/api/v1/runs", "abc")).unwrap();
        assert_eq!(json["type"], "https://docs.masterrl.dev/errors/MRL-VAL-004");
        assert_eq!(json["status"], 422);
        assert_eq!(json["code"], "MRL-VAL-004");
        assert_eq!(json["instance"], "/api/v1/runs");
        assert_eq!(json["trace_id"], "abc");
        assert_eq!(json["title"], ErrorCode::Validation.title());
        assert_eq!(json["errors"][0]["path"], "/config/peft/r");
        assert_eq!(json["errors"][0]["code"], "MRL-VAL-003");
        assert_eq!(json["errors"][0]["hint"], "use 8, 16 or 32");
    }

    #[test]
    fn problem_omits_empty_field_errors() {
        let json = serde_json::to_value(
            ApiError::new(ErrorCode::NotFound, "no run").to_problem("/x", "t"),
        )
        .unwrap();
        assert!(json.get("errors").is_none());
        assert_eq!(json["status"], 404);
    }

    #[test]
    fn into_response_sets_status_and_retry_after() {
        let res = ApiError::new(ErrorCode::RateLimited, "slow down")
            .retry_after(3)
            .into_response();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.headers()[axum::http::header::RETRY_AFTER], "3");
        assert!(res.extensions().get::<Arc<ApiError>>().is_some());
    }

    #[test]
    fn status_mapping_for_bare_responses() {
        assert_eq!(
            ErrorCode::for_status(StatusCode::NOT_FOUND),
            ErrorCode::RouteNotFound
        );
        assert_eq!(
            ErrorCode::for_status(StatusCode::PAYLOAD_TOO_LARGE),
            ErrorCode::BodyTooLarge
        );
        assert_eq!(
            ErrorCode::for_status(StatusCode::IM_A_TEAPOT),
            ErrorCode::Internal
        );
    }

    #[test]
    fn explain_includes_title_and_steps() {
        let text = ServerError::new(ErrorCode::IdentityProviderMissing, "team mode").explain();
        assert!(text.starts_with("MRL-SRV-002 — "));
        assert!(text.contains("--mode solo"));
    }
}
