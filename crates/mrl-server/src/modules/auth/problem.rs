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
        match keychain_coded(&e.detail) {
            Some(c) => Fail::Coded(c),
            None => Fail::Api(ApiError::from(e)),
        }
    }
}

/// A keychain failure (`MRL-ENV-161: …`, `MRL-ENV-162: …`) as its own code, so clients
/// show the right guidance (a cancelled prompt is not a locked keychain).
fn keychain_coded(detail: &str) -> Option<Coded> {
    use crate::modules::keychain::{KEYCHAIN_CANCELLED, KEYCHAIN_FAILED};
    [KEYCHAIN_CANCELLED, KEYCHAIN_FAILED]
        .into_iter()
        .find_map(|code| {
            let rest = detail.strip_prefix(code)?.strip_prefix(": ")?;
            Some(Coded::new(code, StatusCode::SERVICE_UNAVAILABLE, rest))
        })
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
            E::Store(d) => keychain_coded(&d)
                .unwrap_or_else(|| {
                    Coded::new(
                        crate::modules::keychain::KEYCHAIN_FAILED,
                        StatusCode::SERVICE_UNAVAILABLE,
                        d,
                    )
                })
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

/// A JSON request body, like `axum::Json`, whose rejection says only what kind of problem
/// the body has. axum's own text can quote parts of the body (a passphrase, a token, a key
/// shaped like one), and P07's renderer would put it in the problem's `detail`.
pub struct Body<T>(pub T);

impl<T, S> axum::extract::FromRequest<S> for Body<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Fail;

    async fn from_request(req: Request, state: &S) -> Result<Self, Fail> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(axum::Json(v)) => Ok(Body(v)),
            Err(r) => {
                let status = r.status();
                let (code, detail) = match status {
                    StatusCode::UNPROCESSABLE_ENTITY => (
                        ErrorCode::Validation,
                        "The request body has a field that is missing, not allowed here, or of the wrong type; check it against the API reference.",
                    ),
                    StatusCode::UNSUPPORTED_MEDIA_TYPE => (
                        ErrorCode::for_status(status),
                        "Send the body as JSON, with Content-Type: application/json.",
                    ),
                    StatusCode::PAYLOAD_TOO_LARGE => {
                        (ErrorCode::BodyTooLarge, "The request body is too large.")
                    }
                    _ => (ErrorCode::for_status(status), "The request body is not valid JSON."),
                };
                Err(ApiError::new(code, detail).into())
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
    use tower::ServiceExt;

    /// The problem document a handler failing with `fail` answers.
    async fn problem_for(fail: Fail) -> serde_json::Value {
        let app = axum::Router::new()
            .route(
                "/x",
                axum::routing::get(move || {
                    let fail = fail.clone();
                    async move { Err::<(), Fail>(fail) }
                }),
            )
            .layer(axum::middleware::from_fn(render));
        let res = app
            .oneshot(
                axum::http::Request::get("/x")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(res.into_body(), 1 << 16)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn body_rejections_never_repeat_the_body() {
        #[derive(serde::Deserialize, Debug)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Wanted {
            name: String,
        }
        let app = axum::Router::new()
            .route(
                "/x",
                axum::routing::post(|Body(_): Body<Wanted>| async { StatusCode::NO_CONTENT }),
            )
            .layer(axum::middleware::from_fn(render));
        let canary = "hf_CANARYbody8f3e1c9a77";
        for body in [
            format!(r#"{{"name": "a", "{canary}": 1}}"#),
            format!(r#"{{"name": 7, "x": "{canary}"}}"#),
            format!(r#"{{"name": "{canary}"#),
        ] {
            let res = app
                .clone()
                .oneshot(
                    axum::http::Request::post("/x")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(res.status().is_client_error());
            let text = axum::body::to_bytes(res.into_body(), 1 << 16)
                .await
                .unwrap();
            let text = String::from_utf8_lossy(&text);
            assert!(!text.contains(canary), "{text}");
        }
    }

    #[tokio::test]
    async fn a_cancelled_keychain_prompt_keeps_its_code_over_http() {
        use crate::modules::auth::signin::SignInError;
        let cancelled = format!(
            "{}: the keychain request was cancelled",
            crate::modules::keychain::KEYCHAIN_CANCELLED
        );
        // Sign-in's storage error, and a plain seam error from the secret store.
        for fail in [
            Fail::from(SignInError::Store(cancelled.clone())),
            Fail::from(SeamError::new("secrets", cancelled.clone())),
        ] {
            let p = problem_for(fail).await;
            assert_eq!(p["code"], "MRL-ENV-162", "{p}");
            assert_eq!(p["title"], "The keychain request was cancelled.");
            assert_eq!(p["detail"], "the keychain request was cancelled");
        }
        let locked = Fail::from(SignInError::Store(
            "MRL-ENV-161: could not read: locked".into(),
        ));
        assert_eq!(problem_for(locked).await["code"], "MRL-ENV-161");
        let other = Fail::from(SignInError::Store("the vault is damaged".into()));
        assert_eq!(problem_for(other).await["code"], "MRL-ENV-161");
    }

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
            "MRL-STATE-161",
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
