//! Errors with P16's own codes (MRL-*-16x) as real `code`/`title` fields.
//!
//! P07's `ApiError` takes only P07's code enum. P16 handlers return [`Fail`]: either a plain
//! `ApiError` or a [`Coded`] error, which [`render`] (a layer on P16's routers) turns into
//! an RFC 9457 problem document with the request path and trace id. P07's renderer passes
//! finished problem documents through unchanged.

use crate::error::{ApiError, ErrorCode, FieldError, Problem, ERROR_TYPE_BASE, PROBLEM_JSON};
use crate::seams::{RequestMeta, SeamError};
use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::str::FromStr;
use std::sync::Arc;

/// An error with a P16 code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coded {
    /// The catalogued code, like `MRL-AUTH-160`.
    pub code: &'static str,
    /// HTTP status.
    pub status: StatusCode,
    /// What happened in this case.
    pub detail: String,
    /// Field problems (422).
    pub errors: Vec<FieldError>,
}

impl Coded {
    /// A coded error.
    pub fn new(code: &'static str, status: StatusCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            status,
            detail: detail.into(),
            errors: Vec::new(),
        }
    }

    /// A 422 whose one field problem carries the same code.
    pub fn field(code: &'static str, path: &str, message: impl Into<String>, hint: &str) -> Self {
        let message = message.into();
        Self {
            errors: vec![FieldError {
                path: path.to_owned(),
                code: code.to_owned(),
                message: message.clone(),
                hint: hint.to_owned(),
            }],
            ..Self::new(code, StatusCode::UNPROCESSABLE_ENTITY, message)
        }
    }

    /// The title from the code registry (the same text as the guidance catalog).
    pub fn title(&self) -> &'static str {
        mrl_domain::errors::ErrorCode::from_str(self.code)
            .ok()
            .and_then(|c| c.title())
            .unwrap_or("Something went wrong.")
    }
}

/// What a P16 handler fails with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fail {
    /// A P07 error.
    Api(ApiError),
    /// A P16 error.
    Coded(Coded),
}

impl From<ApiError> for Fail {
    fn from(e: ApiError) -> Self {
        Fail::Api(e)
    }
}

impl From<Coded> for Fail {
    fn from(e: Coded) -> Self {
        Fail::Coded(e)
    }
}

impl From<SeamError> for Fail {
    fn from(e: SeamError) -> Self {
        Fail::Api(ApiError::from(e))
    }
}

impl From<super::signin::SignInError> for Fail {
    fn from(e: super::signin::SignInError) -> Self {
        use super::signin::{SignInError as E, EXPIRED, NOT_SET_UP};
        match e {
            E::NotConfigured => Coded::new(
                NOT_SET_UP,
                StatusCode::SERVICE_UNAVAILABLE,
                "This app has no sign-in settings.",
            )
            .into(),
            E::SignedOut => ApiError::new(
                ErrorCode::Unauthenticated,
                "You are not signed in to your MasterRL account on this computer.",
            )
            .into(),
            E::Expired => Coded::new(
                EXPIRED,
                StatusCode::UNAUTHORIZED,
                "The sign-in service no longer accepts this session; sign in again.",
            )
            .into(),
            E::Offline(d) => {
                Coded::new(super::supabase::OFFLINE, StatusCode::SERVICE_UNAVAILABLE, d).into()
            }
            E::Store(d) => Coded::new(
                crate::modules::keychain::KEYCHAIN_FAILED,
                StatusCode::SERVICE_UNAVAILABLE,
                d,
            )
            .into(),
        }
    }
}

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        match self {
            Fail::Api(e) => e.into_response(),
            Fail::Coded(c) => {
                let mut res = c.status.into_response();
                res.extensions_mut().insert(Arc::new(c));
                res
            }
        }
    }
}

/// Layer for P16's routers: renders [`Coded`] errors as problem documents.
pub async fn render(req: Request, next: Next) -> Response {
    // Under `/api/v1` the router strips the prefix; the original path keeps it.
    let instance = req
        .extensions()
        .get::<axum::extract::OriginalUri>()
        .map_or_else(|| req.uri().path().to_owned(), |u| u.path().to_owned());
    let trace_id = req
        .extensions()
        .get::<RequestMeta>()
        .map(|m| m.trace_id.clone())
        .unwrap_or_default();
    let res = next.run(req).await;
    let Some(c) = res.extensions().get::<Arc<Coded>>().cloned() else {
        return res;
    };
    let problem = Problem {
        r#type: format!("{ERROR_TYPE_BASE}{}", c.code),
        title: c.title().to_owned(),
        status: c.status.as_u16(),
        detail: c.detail.clone(),
        instance,
        code: c.code.to_owned(),
        errors: c.errors.clone(),
        trace_id,
    };
    let body = serde_json::to_vec(&problem).unwrap_or_default();
    (c.status, [(header::CONTENT_TYPE, PROBLEM_JSON)], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_p16_code_has_a_registered_title() {
        for code in [
            "MRL-AUTH-160",
            "MRL-AUTH-161",
            "MRL-AUTH-162",
            "MRL-AUTH-163",
            "MRL-LINK-160",
            "MRL-VAL-160",
            "MRL-STATE-160",
            "MRL-RES-160",
            "MRL-ENV-160",
            "MRL-ENV-161",
            "MRL-ENV-162",
            "MRL-CONFLICT-160",
        ] {
            let c = Coded::new(code, StatusCode::CONFLICT, "x");
            assert_ne!(c.title(), "Something went wrong.", "{code}");
        }
    }
}
