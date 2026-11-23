//! HTTP framework: router composition from modules and the middleware every request passes.
//!
//! Order, outermost first: request ids and trace → metrics → compression → problem
//! renderer → (solo: launch-secret check on every path but ticket transfers) → body limit (2 MiB; ticket PUTs are
//! exempt and capped by their ticket) → panic
//! catcher → route. Module routes under `/api/v1` add, outermost first: authentication
//! (team; solo already checked) → rate limit → idempotency → problem renderer.

pub mod etag;
pub mod filter;
pub mod idempotency;
pub mod pagination;
pub mod rate_limit;
pub mod tls;

use crate::app::{AppState, ServerModule};
use crate::error::{ApiError, ErrorCode, Problem, PROBLEM_JSON};
use crate::seams::{RequestContext, RequestMeta};
use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{from_fn, from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Largest request body accepted (2 MiB). Larger content goes through uploads.
pub const BODY_LIMIT: usize = 2 * 1024 * 1024;
/// `PUT` bodies under this path skip [`BODY_LIMIT`]: the transfer ticket carries its own size
/// limit, and the endpoint enforces it (P19).
pub const UNLIMITED_PUT_PREFIX: &str = "/api/v1/transfer/";
/// How a transfer path appears in spans, logs and problem `instance`s: the path itself
/// carries the ticket, which is a credential.
pub const TRANSFER_ROUTE: &str = "/api/v1/transfer/{ticket}";
/// Request id header (echoed on every response).
pub const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
/// W3C trace context header.
pub const TRACEPARENT: HeaderName = HeaderName::from_static("traceparent");
/// Prefix for module routes.
pub const API_PREFIX: &str = "/api/v1";

/// Builds the API router: health, OpenAPI, module routes and (solo) `/metrics`.
pub fn api_router(state: AppState, modules: &[Box<dyn ServerModule>], solo: bool) -> Router {
    let mut protected = Router::new();
    for m in modules {
        protected = protected.merge(m.routes());
    }
    if protected.has_routes() {
        let owned_paths: std::collections::HashSet<&'static str> = modules
            .iter()
            .flat_map(|module| module.handler_idempotency_paths().iter().copied())
            .collect();
        protected = protected
            .route_layer(from_fn(render_problems))
            .route_layer(from_fn_with_state(state.clone(), idempotency::middleware))
            .route_layer(axum::Extension(idempotency::HandlerOwned(Arc::new(
                owned_paths,
            ))))
            .route_layer(from_fn_with_state(state.clone(), rate_limit::middleware));
        if !solo {
            protected = protected.route_layer(from_fn_with_state(state.clone(), authenticate));
        }
    }
    let mut api = protected;
    for m in modules {
        api = api.merge(m.public_routes());
    }
    let mut root = health_routes().route("/api-docs/openapi.json", get(openapi_json));
    if api.has_routes() {
        root = root.nest(API_PREFIX, api);
    }
    if solo {
        // Solo binds one loopback port; metrics share it. `finish` gates every path.
        root = root.route("/metrics", get(metrics));
    }
    finish(root, state, solo)
}

/// Builds the internal router (team mode, `:9090`): metrics and health, no auth.
pub fn internal_router(state: AppState) -> Router {
    finish(
        health_routes().route("/metrics", get(metrics)),
        state,
        false,
    )
}

/// Wraps a gRPC router with the shared layers (and auth in solo mode).
pub fn grpc_router(grpc: Router, state: AppState, solo: bool) -> Router {
    let grpc = if solo {
        grpc.layer(from_fn_with_state(state.clone(), authenticate))
    } else {
        grpc
    };
    grpc.layer(from_fn_with_state(state.clone(), track_request))
        .layer(from_fn_with_state(state, request_meta))
}

fn health_routes() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(crate::telemetry::healthz))
        .route("/readyz", get(crate::telemetry::readyz))
}

/// Adds fallbacks and the shared layers. With `gate`, every path (health, docs, public
/// routes, unknown paths) needs the authenticator to accept the request: solo mode's
/// per-launch secret.
fn finish(router: Router<AppState>, state: AppState, gate: bool) -> Router {
    let router = router
        .fallback(|| async {
            ApiError::new(ErrorCode::RouteNotFound, "No endpoint has this path.")
        })
        .method_not_allowed_fallback(|| async {
            ApiError::new(
                ErrorCode::MethodNotAllowed,
                "This endpoint does not take this method.",
            )
        })
        .layer(tower_http::catch_panic::CatchPanicLayer::custom(|_| {
            ApiError::new(ErrorCode::Internal, "The server hit an internal problem.")
                .into_response()
        }))
        .layer(from_fn(limit_body));
    let router = if gate {
        router.layer(from_fn_with_state(state.clone(), solo_gate))
    } else {
        router
    };
    router
        // Inside compression, so it can read the plain body of bare errors (like a 413).
        .layer(from_fn(render_problems))
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(from_fn_with_state(state.clone(), track_request))
        .layer(from_fn_with_state(state.clone(), request_meta))
        .with_state(state)
}

async fn openapi_json(State(state): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        state.openapi_json(),
    )
        .into_response()
}

async fn metrics(State(state): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
        .into_response()
}

/// Sets `RequestMeta` (request id, trace id), opens the request span (its `path` is the
/// matched route template, never a raw transfer path) and links the
/// incoming W3C trace context so OTLP traces join the caller's trace.
async fn request_meta(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get(&REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 128 && v.bytes().all(|b| b.is_ascii_graphic()))
        .map(str::to_owned)
        .unwrap_or_else(|| state.seams.ids.new_uuid().simple().to_string());
    let incoming = req
        .headers()
        .get(&TRACEPARENT)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_traceparent);
    let (trace_id, traceparent) = match incoming {
        Some((trace_id, full)) => (trace_id, full),
        None => {
            let trace_id = state.seams.ids.new_uuid().simple().to_string();
            let span_id = &state.seams.ids.new_uuid().simple().to_string()[16..];
            let full = format!("00-{trace_id}-{span_id}-01");
            (trace_id, full)
        }
    };
    let span = tracing::info_span!(
        "http.request",
        method = %req.method(),
        path = %req
            .extensions()
            .get::<MatchedPath>()
            .map_or_else(|| shown_path(req.uri().path()), |m| m.as_str()),
        trace_id = %trace_id,
        request_id = %request_id,
    );
    let carrier = HashMap::from([("traceparent".to_owned(), traceparent)]);
    // Without an OTLP layer there is no OpenTelemetry span to link; that is fine.
    let _ = span.set_parent(TraceContextPropagator::new().extract(&carrier));
    req.extensions_mut().insert(RequestMeta {
        trace_id,
        request_id: request_id.clone(),
    });
    let mut res = next.run(req).instrument(span).await;
    if let Ok(v) = HeaderValue::from_str(&request_id) {
        res.headers_mut().insert(REQUEST_ID, v);
    }
    res
}

/// `path` as spans, logs and problem `instance`s may show it: any transfer path (matched
/// or not) becomes [`TRANSFER_ROUTE`], so its ticket is never recorded.
pub fn shown_path(path: &str) -> &str {
    if path.contains("/transfer/") {
        TRANSFER_ROUTE
    } else {
        path
    }
}

/// `00-<32 hex>-<16 hex>-<2 hex>` → (trace id, header). All-zero ids are not valid.
fn parse_traceparent(v: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = v.trim().split('-').collect();
    let hex = |s: &str, n: usize| {
        s.len() == n
            && s.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    };
    match parts.as_slice() {
        [ver, trace, span, flags]
            if hex(ver, 2)
                && *ver != "ff"
                && hex(trace, 32)
                && hex(span, 16)
                && hex(flags, 2)
                && trace.bytes().any(|b| b != b'0')
                && span.bytes().any(|b| b != b'0') =>
        {
            Some(((*trace).to_owned(), v.trim().to_owned()))
        }
        _ => None,
    }
}

/// Counts requests and records latency by matched route.
async fn track_request(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let method = req.method().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |p| p.as_str().to_owned());
    let started = Instant::now();
    let res = next.run(req).await;
    state
        .metrics
        .observe_http(&method, &route, res.status().as_u16(), started.elapsed());
    if res.status().is_server_error() {
        tracing::error!(status = res.status().as_u16(), route = %route, "request failed");
    }
    res
}

/// Solo mode's gate on every path, except `/api/v1/transfer/{ticket}`: there the ticket is
/// the only credential, as in team mode, so agents never hold the owner's launch secret.
async fn solo_gate(state: State<AppState>, req: Request, next: Next) -> Response {
    if is_ticket_path(req.uri().path()) {
        return next.run(req).await;
    }
    authenticate(state, req, next).await
}

/// `/api/v1/transfer/<one segment>`, the segment made of ticket characters only.
fn is_ticket_path(path: &str) -> bool {
    path.strip_prefix(API_PREFIX)
        .and_then(|p| p.strip_prefix("/transfer/"))
        .is_some_and(|t| {
            !t.is_empty()
                && t != "."
                && t != ".."
                && t.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
}

/// Authenticates through the `Authenticator` seam and sets `RequestContext`.
async fn authenticate(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let meta = meta_of(&req);
    match state.seams.authenticator.authenticate(req.headers()).await {
        Ok(Some(principal)) => {
            req.extensions_mut()
                .insert(RequestContext::new(principal, &meta));
            next.run(req).await
        }
        Ok(None) => ApiError::new(
            ErrorCode::Unauthenticated,
            "The request has no valid sign-in details.",
        )
        .into_response(),
        Err(e) => ApiError::from(e).into_response(),
    }
}

fn meta_of(req: &Request) -> RequestMeta {
    req.extensions()
        .get::<RequestMeta>()
        .cloned()
        .unwrap_or_else(|| RequestMeta {
            trace_id: String::new(),
            request_id: String::new(),
        })
}

/// [`BODY_LIMIT`] on every request except ticket uploads (`PUT` under [`UNLIMITED_PUT_PREFIX`]).
async fn limit_body(req: Request, next: Next) -> Response {
    use tower::ServiceExt;
    if req.method() == axum::http::Method::PUT && req.uri().path().starts_with(UNLIMITED_PUT_PREFIX)
    {
        return next.run(req).await;
    }
    let inner = tower::util::MapRequest::new(next, |r: axum::http::Request<_>| r.map(Body::new));
    match tower_http::limit::RequestBodyLimit::new(inner, BODY_LIMIT)
        .oneshot(req)
        .await
    {
        Ok(res) => res.map(Body::new),
        Err(never) => match never {},
    }
}

/// Renders `ApiError`s and bare error responses (axum rejections, body limit) as
/// `application/problem+json` with `instance` and `trace_id`.
async fn render_problems(req: Request, next: Next) -> Response {
    let meta = meta_of(&req);
    let instance = shown_path(req.uri().path()).to_owned();
    let res = next.run(req).await;
    let status = res.status();
    if let Some(err) = res.extensions().get::<Arc<ApiError>>().cloned() {
        return problem_response(res, err.to_problem(&instance, &meta.trace_id));
    }
    // JSON bodies (problems, `/readyz`) are already structured; leave them alone.
    let is_json = res.headers().get(header::CONTENT_TYPE).is_some_and(|v| {
        let v = v.as_bytes();
        v.starts_with(PROBLEM_JSON.as_bytes()) || v.starts_with(b"application/json")
    });
    if !(status.is_client_error() || status.is_server_error()) || is_json {
        return res;
    }
    let code = match status {
        StatusCode::UNPROCESSABLE_ENTITY => ErrorCode::Validation,
        s => ErrorCode::for_status(s),
    };
    let (parts, body) = res.into_parts();
    // Client errors keep the framework's reason (like a JSON parse position); 5xx text may leak internals.
    let detail = match axum::body::to_bytes(body, 4096).await {
        Ok(b) if status.is_client_error() && !b.is_empty() => {
            String::from_utf8_lossy(&b).into_owned()
        }
        _ => status
            .canonical_reason()
            .unwrap_or("Request failed.")
            .to_owned(),
    };
    let mut problem = ApiError::new(code, detail).to_problem(&instance, &meta.trace_id);
    problem.status = status.as_u16();
    problem_response(Response::from_parts(parts, Body::empty()), problem)
}

fn problem_response(res: Response, problem: Problem) -> Response {
    let (mut parts, _) = res.into_parts();
    parts.extensions.remove::<Arc<ApiError>>();
    let body = match serde_json::to_vec(&problem) {
        Ok(b) => b,
        // Problem has only strings, numbers and lists; serialization cannot fail.
        Err(_) => b"{\"code\":\"MRL-INTERNAL-001\"}".to_vec(),
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    parts
        .headers
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
    Response::from_parts(parts, Body::from(body))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::app::{AppState, ServerModule};
    use crate::config::{Config, Mode};
    use crate::seams::{Seams, LAUNCH_SECRET_HEADER};
    use axum::http::Method;
    use axum::routing::post;
    use tower::ServiceExt;

    pub const SECRET: &str = "unit-test-secret";

    struct Echo;
    impl ServerModule for Echo {
        fn name(&self) -> &'static str {
            "echo"
        }
        fn routes(&self) -> Router<AppState> {
            Router::new()
                .route(
                    "/echo",
                    post(|body: String| async move { (StatusCode::CREATED, body) }),
                )
                .route(
                    "/json",
                    post(
                        |axum::Json(v): axum::Json<serde_json::Value>| async move { axum::Json(v) },
                    ),
                )
                .route("/boom", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
        }
        fn public_routes(&self) -> Router<AppState> {
            Router::new().route("/hello", get(|| async { "hello" }))
        }
    }

    pub fn app(config: Config) -> Router {
        let seams = Seams::production(Mode::Solo, SECRET)
            .build(Mode::Solo)
            .unwrap();
        let modules: Vec<Box<dyn ServerModule>> = vec![Box::new(Echo)];
        let state = AppState::new(config, seams, &modules).unwrap();
        api_router(state, &modules, true)
    }

    pub async fn send(
        app: &Router,
        req: axum::http::Request<Body>,
    ) -> (
        StatusCode,
        axum::http::HeaderMap,
        serde_json::Value,
        Vec<u8>,
    ) {
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let headers = res.headers().clone();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, headers, json, bytes)
    }

    pub fn req(method: Method, uri: &str) -> axum::http::request::Builder {
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header(LAUNCH_SECRET_HEADER, SECRET)
    }

    #[tokio::test]
    async fn unknown_route_is_problem_json() {
        let app = app(Config::default());
        let r = req(Method::GET, "/nope").body(Body::empty()).unwrap();
        let (status, headers, json, _) = send(&app, r).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers[header::CONTENT_TYPE], PROBLEM_JSON);
        assert_eq!(json["code"], "MRL-RES-002");
        assert_eq!(json["instance"], "/nope");
        assert_eq!(json["trace_id"].as_str().unwrap().len(), 32);
        assert!(headers.contains_key(&REQUEST_ID));
    }

    #[tokio::test]
    async fn wrong_method_is_problem_json() {
        let app = app(Config::default());
        let r = req(Method::GET, "/api/v1/echo")
            .body(Body::empty())
            .unwrap();
        let (status, _, json, _) = send(&app, r).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(json["code"], "MRL-VAL-013");
    }

    #[tokio::test]
    async fn missing_or_wrong_secret_is_401() {
        let app = app(Config::default());
        for secret in [None, Some("wrong")] {
            let mut b = axum::http::Request::post("/api/v1/echo");
            if let Some(s) = secret {
                b = b.header(LAUNCH_SECRET_HEADER, s);
            }
            let (status, _, json, _) = send(&app, b.body(Body::from("x")).unwrap()).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(json["code"], "MRL-AUTH-001");
        }
        let r = req(Method::POST, "/api/v1/echo")
            .body(Body::from("hi"))
            .unwrap();
        let (status, _, _, body) = send(&app, r).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body, b"hi");
    }

    #[tokio::test]
    async fn metrics_need_the_secret_in_solo() {
        let app = app(Config::default());
        let r = axum::http::Request::get("/metrics")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(&app, r).await.0, StatusCode::UNAUTHORIZED);
        let r = req(Method::GET, "/metrics").body(Body::empty()).unwrap();
        let (status, _, _, body) = send(&app, r).await;
        assert_eq!(status, StatusCode::OK);
        assert!(String::from_utf8(body)
            .unwrap()
            .contains("mrl_http_requests_total"));
    }

    #[tokio::test]
    async fn bad_json_is_problem_with_reason() {
        let app = app(Config::default());
        let r = req(Method::POST, "/api/v1/json")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{nope"))
            .unwrap();
        let (status, headers, json, _) = send(&app, r).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(headers[header::CONTENT_TYPE], PROBLEM_JSON);
        assert_eq!(json["code"], "MRL-VAL-001");
        assert!(json["detail"].as_str().unwrap().contains("JSON"));
    }

    #[tokio::test]
    async fn bare_5xx_does_not_leak_body() {
        let app = app(Config::default());
        let r = req(Method::GET, "/api/v1/boom")
            .body(Body::empty())
            .unwrap();
        let (status, _, json, _) = send(&app, r).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["code"], "MRL-INTERNAL-001");
    }

    #[tokio::test]
    async fn body_over_2_mib_is_413() {
        let app = app(Config::default());
        let big = "x".repeat(BODY_LIMIT + 1);
        let r = req(Method::POST, "/api/v1/echo")
            .body(Body::from(big))
            .unwrap();
        let (status, _, json, _) = send(&app, r).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json["code"], "MRL-VAL-007");
    }

    #[tokio::test]
    async fn traceparent_and_request_id_are_kept() {
        let app = app(Config::default());
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let r = req(Method::GET, "/nope")
            .header(TRACEPARENT, tp)
            .header(&REQUEST_ID, "req-1")
            .body(Body::empty())
            .unwrap();
        let (_, headers, json, _) = send(&app, r).await;
        assert_eq!(json["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(headers[&REQUEST_ID], "req-1");
    }

    #[test]
    fn traceparent_parsing() {
        assert!(
            parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").is_some()
        );
        for bad in [
            "",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f-00f067aa0ba902b7-01",
        ] {
            assert!(parse_traceparent(bad).is_none(), "{bad}");
        }
    }

    #[tokio::test]
    async fn solo_gates_every_path_on_the_launch_secret() {
        let app = app(Config::default());
        let paths = [
            ("/healthz", StatusCode::OK),
            ("/readyz", StatusCode::OK),
            ("/api-docs/openapi.json", StatusCode::OK),
            ("/api/v1/hello", StatusCode::OK),
            ("/nope", StatusCode::NOT_FOUND),
        ];
        for (path, with_secret) in paths {
            for wrong in [None, Some("wrong")] {
                let mut b = axum::http::Request::get(path);
                if let Some(w) = wrong {
                    b = b.header(LAUNCH_SECRET_HEADER, w);
                }
                let (status, _, json, _) = send(&app, b.body(Body::empty()).unwrap()).await;
                assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} {wrong:?}");
                assert_eq!(json["code"], "MRL-AUTH-001", "{path}");
            }
            let r = req(Method::GET, path).body(Body::empty()).unwrap();
            assert_eq!(send(&app, r).await.0, with_secret, "{path}");
        }
    }

    #[tokio::test]
    async fn solo_lets_only_ticket_paths_through_without_the_secret() {
        let app = app(Config::default());
        // No such route in this app: 404 means the gate let it through.
        let r = axum::http::Request::get("/api/v1/transfer/tkt_a-b.c")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(&app, r).await.0, StatusCode::NOT_FOUND);
        for path in [
            "/api/v1/transfer/",
            "/api/v1/transfer/..",
            "/api/v1/transfer/a/b",
            "/api/v1/transfer/a%2Fb",
            "/api/v1/transferx/a",
            "/transfer/a",
        ] {
            let r = axum::http::Request::get(path).body(Body::empty()).unwrap();
            assert_eq!(send(&app, r).await.0, StatusCode::UNAUTHORIZED, "{path}");
        }
    }

    #[tokio::test]
    async fn team_health_and_public_routes_need_no_sign_in() {
        let seams = Seams::production(Mode::Team, "")
            .identity(NoUsers)
            .authorizer(crate::seams::LocalOwnerAuthorizer)
            .build(Mode::Team)
            .unwrap();
        let modules: Vec<Box<dyn ServerModule>> = vec![Box::new(Echo)];
        let state = AppState::new(Config::default(), seams, &modules).unwrap();
        let app = api_router(state, &modules, false);
        for path in ["/healthz", "/api-docs/openapi.json", "/api/v1/hello"] {
            let r = axum::http::Request::get(path).body(Body::empty()).unwrap();
            assert_eq!(send(&app, r).await.0, StatusCode::OK, "{path}");
        }
        let r = axum::http::Request::post("/api/v1/echo")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(&app, r).await.0, StatusCode::UNAUTHORIZED);
    }

    struct NoUsers;
    #[async_trait::async_trait]
    impl crate::seams::IdentityProvider for NoUsers {
        async fn verify_bearer(
            &self,
            _: &str,
        ) -> Result<Option<crate::seams::Principal>, crate::seams::SeamError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn declared_length_over_2_mib_is_problem_413() {
        let app = app(Config::default());
        // A declared Content-Length over the limit is refused before the body is read.
        let r = req(Method::POST, "/api/v1/echo")
            .header(header::CONTENT_LENGTH, BODY_LIMIT + 1)
            .body(Body::from("x".repeat(BODY_LIMIT + 1)))
            .unwrap();
        let (status, headers, json, _) = send(&app, r).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(headers[header::CONTENT_TYPE], PROBLEM_JSON);
        assert_eq!(json["code"], "MRL-VAL-007");
        assert_eq!(json["trace_id"].as_str().unwrap().len(), 32);
    }

    #[test]
    fn transfer_paths_never_show_their_ticket() {
        for p in [
            "/api/v1/transfer/eyJrZXkiOi.sig",
            "/api/v1/transfer/eyJrZXkiOi.sig/extra",
            "/api/v1//transfer/eyJrZXkiOi.sig",
        ] {
            assert_eq!(shown_path(p), TRANSFER_ROUTE, "{p}");
        }
        assert_eq!(
            shown_path("/api/v1/artifacts/art_1"),
            "/api/v1/artifacts/art_1"
        );
    }
}
