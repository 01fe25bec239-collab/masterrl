//! `Idempotency-Key` middleware over the `IdempotencyStore` seam (POST only).
//!
//! Same key + same body → the stored response is replayed (`idempotent-replayed: true`).
//! Same key + different body → 422 MRL-IDEM-002. Key still running → 409 MRL-IDEM-001.
//! 5xx responses are not stored, so the client can retry with the same key. Keys are scoped
//! per principal and org. A response carrying [`NotReplayable`] is not stored either: the
//! key is released, and a retry runs the handler again (with fresh authorization). Modules
//! whose answers go stale (tickets) or depend on current permissions use it and dedupe
//! retries themselves.

use super::BODY_LIMIT;
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode};
use crate::seams::{IdempotencyBegin, RequestContext, StoredResponse};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

/// The request header.
pub const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
/// Set on replayed responses.
pub const REPLAYED: HeaderName = HeaderName::from_static("idempotent-replayed");
/// Largest response body kept for replay.
const MAX_STORED_BODY: usize = 8 * 1024 * 1024;
const KEPT_HEADERS: [HeaderName; 3] = [header::CONTENT_TYPE, header::LOCATION, header::ETAG];

/// Response extension: never store this response for replay (see the module docs).
#[derive(Debug, Clone, Copy)]
pub struct NotReplayable;

/// The middleware. Runs after authentication (keys are scoped per principal and org).
pub async fn middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if req.method() != Method::POST {
        return next.run(req).await;
    }
    let Some(raw) = req.headers().get(&IDEMPOTENCY_KEY) else {
        return next.run(req).await;
    };
    let key = match raw.to_str() {
        Ok(k) if valid_key(k) => k.to_owned(),
        _ => {
            return ApiError::new(
                ErrorCode::IdempotencyKey,
                "Idempotency-Key must be 1 to 255 visible characters.",
            )
            .into_response()
        }
    };
    let scope = match req.extensions().get::<RequestContext>() {
        // The unit separator can't appear in ids, so scopes never collide.
        Some(ctx) => format!(
            "{}\u{1f}{}",
            ctx.principal.id,
            ctx.org.as_deref().unwrap_or_default()
        ),
        None => return ApiError::new(ErrorCode::Unauthenticated, "Sign in first.").into_response(),
    };
    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, BODY_LIMIT).await {
        Ok(b) => b,
        Err(_) => {
            return ApiError::new(ErrorCode::BodyTooLarge, "The body is over 2 MiB.")
                .into_response()
        }
    };
    let fp = fingerprint(
        &parts.method,
        parts.uri.path_and_query().map_or("", |p| p.as_str()),
        &bytes,
    );
    let store = state.seams.idempotency.clone();
    match store.begin(&scope, &key, fp).await {
        Err(e) => ApiError::from(e).into_response(),
        Ok(IdempotencyBegin::Mismatch) => ApiError::new(
            ErrorCode::IdempotencyMismatch,
            "This key was already used with a different request body.",
        )
        .into_response(),
        Ok(IdempotencyBegin::InProgress) => ApiError::new(
            ErrorCode::IdempotencyInProgress,
            "A request with this key is still running.",
        )
        .retry_after(1)
        .into_response(),
        Ok(IdempotencyBegin::Replay(stored)) => replay(stored),
        Ok(IdempotencyBegin::Started) => {
            // Releases the key if this future is dropped (client gone) or the handler fails.
            let mut guard = Release {
                state: state.clone(),
                scope: scope.clone(),
                key: key.clone(),
                armed: true,
            };
            let res = next
                .run(Request::from_parts(parts, Body::from(bytes)))
                .await;
            if res.status().is_server_error() || res.extensions().get::<NotReplayable>().is_some() {
                return res;
            }
            let (res_parts, res_body) = res.into_parts();
            let Ok(body) = axum::body::to_bytes(res_body, MAX_STORED_BODY).await else {
                return ApiError::new(ErrorCode::Internal, "The response was too large to keep.")
                    .into_response();
            };
            let stored = StoredResponse {
                status: res_parts.status.as_u16(),
                headers: KEPT_HEADERS
                    .iter()
                    .filter_map(|h| {
                        let v = res_parts.headers.get(h)?.to_str().ok()?;
                        Some((h.as_str().to_owned(), v.to_owned()))
                    })
                    .collect(),
                body: body.to_vec(),
            };
            match store.complete(&scope, &key, stored).await {
                Ok(()) => guard.armed = false,
                Err(e) => tracing::error!(error = %e, "could not store idempotent response"),
            }
            Response::from_parts(res_parts, Body::from(body))
        }
    }
}

struct Release {
    state: AppState,
    scope: String,
    key: String,
    armed: bool,
}

impl Drop for Release {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let store = self.state.seams.idempotency.clone();
        let (scope, key) = (
            std::mem::take(&mut self.scope),
            std::mem::take(&mut self.key),
        );
        self.state.tasks().spawn(async move {
            if let Err(e) = store.release(&scope, &key).await {
                tracing::error!(error = %e, "could not release idempotency key");
            }
        });
    }
}

fn valid_key(k: &str) -> bool {
    (1..=255).contains(&k.len()) && k.bytes().all(|b| b.is_ascii_graphic())
}

/// SHA-256 over method, path and body, length-prefixed so fields cannot run together.
fn fingerprint(method: &Method, path: &str, body: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    for part in [method.as_str().as_bytes(), path.as_bytes(), body] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part);
    }
    h.finalize().into()
}

fn replay(stored: StoredResponse) -> Response {
    let mut res = Response::new(Body::from(stored.body));
    *res.status_mut() = StatusCode::from_u16(stored.status).unwrap_or(StatusCode::OK);
    for (name, value) in stored.headers {
        if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            res.headers_mut().insert(n, v);
        }
    }
    res.headers_mut()
        .insert(REPLAYED, HeaderValue::from_static("true"));
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::http::tests::{app, req, send};

    fn post(key: &str, body: &str) -> axum::http::Request<Body> {
        req(Method::POST, "/api/v1/echo")
            .header(&IDEMPOTENCY_KEY, key)
            .body(Body::from(body.to_owned()))
            .unwrap()
    }

    #[tokio::test]
    async fn same_key_same_body_replays() {
        let app = app(Config::default());
        let (s1, h1, _, b1) = send(&app, post("k1", "hello")).await;
        assert_eq!(s1, StatusCode::CREATED);
        assert!(!h1.contains_key(&REPLAYED));
        let (s2, h2, _, b2) = send(&app, post("k1", "hello")).await;
        assert_eq!((s2, b2), (s1, b1));
        assert_eq!(h2[&REPLAYED], "true");
    }

    #[tokio::test]
    async fn same_key_different_body_is_422() {
        let app = app(Config::default());
        send(&app, post("k2", "one")).await;
        let (status, _, json, _) = send(&app, post("k2", "two")).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["code"], "MRL-IDEM-002");
    }

    #[tokio::test]
    async fn bad_keys_are_400() {
        let app = app(Config::default());
        for key in ["", &"k".repeat(256)] {
            let (status, _, json, _) = send(&app, post(key, "x")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{key:?}");
            assert_eq!(json["code"], "MRL-IDEM-003");
        }
    }

    #[tokio::test]
    async fn without_key_nothing_is_stored() {
        let app = app(Config::default());
        let r = || {
            req(Method::POST, "/api/v1/echo")
                .body(Body::from("x"))
                .unwrap()
        };
        let (_, h, _, _) = send(&app, r()).await;
        assert!(!h.contains_key(&REPLAYED));
        let (_, h, _, _) = send(&app, r()).await;
        assert!(!h.contains_key(&REPLAYED));
    }

    #[test]
    fn fingerprint_separates_fields() {
        let a = fingerprint(&Method::POST, "/ab", b"c");
        let b = fingerprint(&Method::POST, "/a", b"bc");
        assert_ne!(a, b);
    }
}
