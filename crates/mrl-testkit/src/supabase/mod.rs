//! A mock Supabase project on loopback: Auth (PKCE authorize, token, refresh, logout, JWKS
//! with an ES256 key) and the one REST table setup sync uses, with row-level security on
//! the user id. All P16 tests use it; the real project is set up by the owner later.
//!
//! ```text
//! let sb = MockSupabase::start().await;
//! let config = sb.auth_config();                   // [auth] settings for the server
//! let redirect = sb.approve(&authorize_url)?;      // "the user clicked Allow"
//! ```

pub mod core;
#[cfg(test)]
mod tests_core;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use mrl_server::modules::auth::AuthConfig;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

/// The anon key the mock expects in `apikey`.
pub const ANON_KEY: &str = "mock-anon-key";
const KID: &str = "mock-es256-1";

/// The person who signs in at `/authorize`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockUser {
    /// Supabase user id.
    pub id: Uuid,
    /// Email.
    pub email: String,
    /// Full name.
    pub name: String,
}

impl MockUser {
    /// A user with a fresh id.
    pub fn new(email: &str, name: &str) -> Self {
        Self {
            id: Uuid::now_v7(),
            email: email.to_owned(),
            name: name.to_owned(),
        }
    }
}

struct Mock {
    issuer: String,
    signing: EncodingKey,
    verifying: DecodingKey,
    jwk: Value,
    user: Mutex<MockUser>,
    codes: Mutex<HashMap<String, (String, MockUser)>>,
    refresh: Mutex<HashMap<String, MockUser>>,
    rows: Mutex<HashMap<String, Value>>,
    versions: Mutex<HashMap<String, i64>>,
    offline: AtomicBool,
    access_ttl: AtomicI64,
    refresh_limited: AtomicU32,
    lost_push_replies: AtomicU32,
    jwks_requests: AtomicU32,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// A running mock project. Stops when dropped.
pub struct MockSupabase {
    addr: SocketAddr,
    mock: Arc<Mock>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockSupabase {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockSupabase {
    /// Starts the mock on `127.0.0.1:<random>`. Panics if it cannot.
    pub async fn start() -> Self {
        Self::start_on(0).await
    }

    /// Starts the mock on `127.0.0.1:port` (0 picks a free port). Panics if it cannot.
    pub async fn start_on(port: u16) -> Self {
        let listener = match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
            Ok(l) => l,
            Err(e) => panic!("mock supabase bind: {e}"),
        };
        let addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => panic!("mock supabase addr: {e}"),
        };
        let key = match rcgen::KeyPair::generate() {
            Ok(k) => k,
            Err(e) => panic!("mock supabase key: {e}"),
        };
        let raw = key.public_key_raw();
        let (x, y) = (&raw[1..33], &raw[33..65]);
        let jwk = json!({
            "kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig", "kid": KID,
            "x": URL_SAFE_NO_PAD.encode(x), "y": URL_SAFE_NO_PAD.encode(y),
        });
        let verifying = match serde_json::from_value::<jsonwebtoken::jwk::Jwk>(jwk.clone())
            .map_err(|e| e.to_string())
            .and_then(|j| DecodingKey::from_jwk(&j).map_err(|e| e.to_string()))
        {
            Ok(k) => k,
            Err(e) => panic!("mock supabase jwk: {e}"),
        };
        let mock = Arc::new(Mock {
            issuer: format!("http://{addr}/auth/v1"),
            signing: EncodingKey::from_ec_der(&key.serialize_der()),
            verifying,
            jwk,
            user: Mutex::new(MockUser::new("ada@example.com", "Ada Lovelace")),
            codes: Mutex::default(),
            refresh: Mutex::default(),
            rows: Mutex::default(),
            versions: Mutex::default(),
            offline: AtomicBool::new(false),
            access_ttl: AtomicI64::new(3600),
            refresh_limited: AtomicU32::new(0),
            lost_push_replies: AtomicU32::new(0),
            jwks_requests: AtomicU32::new(0),
        });
        let app = Router::new()
            .route("/auth/v1/authorize", get(authorize))
            .route("/auth/v1/token", post(token))
            .route("/auth/v1/logout", post(logout))
            .route("/auth/v1/.well-known/jwks.json", get(jwks))
            .route("/rest/v1/setup_profiles", get(read_rows))
            .route("/rest/v1/rpc/push_setup_copy", post(push_copy))
            .with_state(mock.clone());
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                eprintln!("mock supabase stopped: {e}");
            }
        });
        Self { addr, mock, task }
    }

    /// The project address, like `http://127.0.0.1:5432`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// `[auth]` settings pointing at this mock.
    pub fn auth_config(&self) -> AuthConfig {
        AuthConfig {
            supabase_url: Some(self.url()),
            supabase_anon_key: Some(ANON_KEY.to_owned()),
            ..AuthConfig::default()
        }
    }

    /// Who signs in next.
    pub fn set_user(&self, user: MockUser) {
        *lock(&self.mock.user) = user;
    }

    /// The user who signs in next.
    pub fn user(&self) -> MockUser {
        lock(&self.mock.user).clone()
    }

    /// While true, every endpoint answers 503 (as if the service were unreachable).
    pub fn set_offline(&self, offline: bool) {
        self.mock.offline.store(offline, Ordering::SeqCst);
    }

    /// The next `n` refreshes answer 429 (rate limited) and leave the refresh token valid.
    pub fn rate_limit_refreshes(&self, n: u32) {
        self.mock.refresh_limited.store(n, Ordering::SeqCst);
    }

    /// How many times the JWKS was asked for (answered or not).
    pub fn jwks_requests(&self) -> u32 {
        self.mock.jwks_requests.load(Ordering::SeqCst)
    }

    /// The next `n` pushes are saved but answer 503, as if the reply were lost on the way.
    pub fn lose_push_replies(&self, n: u32) {
        self.mock.lost_push_replies.store(n, Ordering::SeqCst);
    }

    /// Lifetime of new access tokens, in seconds (negative: already expired).
    pub fn set_access_ttl(&self, seconds: i64) {
        self.mock.access_ttl.store(seconds, Ordering::SeqCst);
    }

    /// Revokes every refresh token (as when the user signs out everywhere).
    pub fn revoke_refresh_tokens(&self) {
        lock(&self.mock.refresh).clear();
    }

    /// Every stored sync row, as JSON (to check that no secret is in it).
    pub fn stored_rows(&self) -> Value {
        json!(*lock(&self.mock.rows))
    }

    /// A JWT with exactly these claims, signed with the project key (for claim checks).
    pub fn sign(&self, claims: &Value) -> String {
        self.sign_as(KID, claims)
    }

    /// Like [`MockSupabase::sign`], with header `kid` set to `kid` (the key is the same).
    pub fn sign_as(&self, kid: &str, claims: &Value) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(kid.to_owned());
        match jsonwebtoken::encode(&header, claims, &self.mock.signing) {
            Ok(t) => t,
            Err(e) => panic!("mock supabase sign: {e}"),
        }
    }

    /// The `iss` of this project's tokens.
    pub fn issuer(&self) -> String {
        self.mock.issuer.clone()
    }

    /// A signed access token for `user`, valid for `ttl` seconds.
    pub fn access_token(&self, user: &MockUser, ttl: i64) -> String {
        self.mock.access_token(user, ttl)
    }

    /// "The user clicked Allow": the address Supabase would send the browser back to,
    /// with `code` added, for an authorize URL the server made.
    pub fn approve(&self, authorize_url: &str) -> Result<String, String> {
        let url = reqwest::Url::parse(authorize_url).map_err(|e| e.to_string())?;
        let q: HashMap<String, String> = url.query_pairs().into_owned().collect();
        self.mock.approve(&q)
    }
}

impl Mock {
    fn access_token(&self, user: &MockUser, ttl: i64) -> String {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let claims = json!({
            "iss": self.issuer, "aud": "authenticated", "sub": user.id.to_string(), "role": "authenticated",
            "email": user.email, "iat": now, "exp": now + ttl,
            "user_metadata": { "full_name": user.name, "avatar_url": "https://example.com/a.png" },
        });
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(KID.to_owned());
        match jsonwebtoken::encode(&header, &claims, &self.signing) {
            Ok(t) => t,
            Err(e) => panic!("mock supabase sign: {e}"),
        }
    }

    fn approve(&self, q: &HashMap<String, String>) -> Result<String, String> {
        let redirect = q.get("redirect_to").ok_or("redirect_to is missing")?;
        let challenge = q.get("code_challenge").ok_or("code_challenge is missing")?;
        if q.get("code_challenge_method").map(String::as_str) != Some("s256") {
            return Err("code_challenge_method must be s256".into());
        }
        if !matches!(
            q.get("provider").map(String::as_str),
            Some("google" | "github")
        ) {
            return Err("unknown provider".into());
        }
        let code = Uuid::now_v7().simple().to_string();
        lock(&self.codes).insert(code.clone(), (challenge.clone(), lock(&self.user).clone()));
        let sep = if redirect.contains('?') { '&' } else { '?' };
        Ok(format!("{redirect}{sep}code={code}"))
    }

    fn session(&self, user: MockUser) -> Value {
        let ttl = self.access_ttl.load(Ordering::SeqCst);
        let refresh = format!("mock-refresh-{}", Uuid::now_v7().simple());
        let access = self.access_token(&user, ttl);
        lock(&self.refresh).insert(refresh.clone(), user.clone());
        json!({
            "access_token": access, "token_type": "bearer", "expires_in": ttl,
            "expires_at": time::OffsetDateTime::now_utc().unix_timestamp() + ttl,
            "refresh_token": refresh,
            "user": { "id": user.id.to_string(), "email": user.email,
                      "user_metadata": { "full_name": user.name, "avatar_url": "https://example.com/a.png" } },
        })
    }

    /// The `sub` of a valid bearer token.
    fn bearer(&self, h: &HeaderMap) -> Option<String> {
        let token = h
            .get("authorization")?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")?;
        let mut v = Validation::new(Algorithm::ES256);
        v.set_issuer(&[&self.issuer]);
        v.set_audience(&["authenticated"]);
        let data = jsonwebtoken::decode::<Value>(token, &self.verifying, &v).ok()?;
        data.claims.get("sub")?.as_str().map(str::to_owned)
    }
}

fn gate(m: &Mock, h: &HeaderMap) -> Option<Response> {
    if m.offline.load(Ordering::SeqCst) {
        return Some((StatusCode::SERVICE_UNAVAILABLE, "offline").into_response());
    }
    if h.get("apikey").and_then(|v| v.to_str().ok()) != Some(ANON_KEY) {
        return Some(error(
            StatusCode::UNAUTHORIZED,
            "no_api_key",
            "No API key found in request",
        ));
    }
    None
}

fn error(status: StatusCode, code: &str, msg: &str) -> Response {
    (
        status,
        Json(json!({ "error": code, "error_description": msg })),
    )
        .into_response()
}

async fn authorize(
    State(m): State<Arc<Mock>>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if m.offline.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "offline").into_response();
    }
    match m.approve(&q) {
        Ok(to) => Redirect::to(&to).into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, "validation_failed", &e),
    }
}

async fn token(
    State(m): State<Arc<Mock>>,
    h: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
    Json(body): Json<Value>,
) -> Response {
    if let Some(r) = gate(&m, &h) {
        return r;
    }
    let field = |k: &str| {
        body.get(k)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    match q.get("grant_type").map(String::as_str) {
        Some("pkce") => {
            let found = lock(&m.codes).remove(&field("auth_code"));
            let Some((challenge, user)) = found else {
                return error(
                    StatusCode::NOT_FOUND,
                    "flow_state_not_found",
                    "invalid flow state, no valid flow state found",
                );
            };
            let got = URL_SAFE_NO_PAD.encode(Sha256::digest(field("code_verifier").as_bytes()));
            if got != challenge {
                return error(
                    StatusCode::BAD_REQUEST,
                    "bad_code_verifier",
                    "code challenge does not match previously saved code verifier",
                );
            }
            Json(m.session(user)).into_response()
        }
        Some("refresh_token") => {
            let limited = m
                .refresh_limited
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if limited {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({ "code": 429, "error_code": "over_request_rate_limit", "msg": "Request rate limit reached" })),
                )
                    .into_response();
            }
            let found = lock(&m.refresh).remove(&field("refresh_token"));
            match found {
                Some(user) => Json(m.session(user)).into_response(),
                None => error(
                    StatusCode::BAD_REQUEST,
                    "refresh_token_not_found",
                    "Invalid Refresh Token: Refresh Token Not Found",
                ),
            }
        }
        _ => error(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
            "unsupported grant type",
        ),
    }
}

async fn logout(State(m): State<Arc<Mock>>, h: HeaderMap) -> Response {
    if let Some(r) = gate(&m, &h) {
        return r;
    }
    let Some(sub) = m.bearer(&h) else {
        return error(StatusCode::UNAUTHORIZED, "bad_jwt", "invalid JWT");
    };
    lock(&m.refresh).retain(|_, u| u.id.to_string() != sub);
    StatusCode::NO_CONTENT.into_response()
}

async fn jwks(State(m): State<Arc<Mock>>, h: HeaderMap) -> Response {
    m.jwks_requests.fetch_add(1, Ordering::SeqCst);
    if let Some(r) = gate(&m, &h) {
        return r;
    }
    Json(json!({ "keys": [m.jwk] })).into_response()
}

async fn read_rows(
    State(m): State<Arc<Mock>>,
    h: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Some(r) = gate(&m, &h) {
        return r;
    }
    let Some(sub) = m.bearer(&h) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "PGRST301",
            "JWT expired or invalid",
        );
    };
    // RLS: `auth.uid() = user_id`; the filter can only narrow that further.
    let wanted = q
        .get("user_id")
        .and_then(|f| f.strip_prefix("eq."))
        .unwrap_or(&sub)
        .to_owned();
    let rows = lock(&m.rows);
    let out: Vec<Value> = rows
        .get(&sub)
        .filter(|_| wanted == sub)
        .map(|copies| json!({ "user_id": sub, "copies": copies }))
        .into_iter()
        .collect();
    Json(out).into_response()
}

/// `public.push_setup_copy(p_blob, p_op)`: like the SQL function in the P16 handoff, one lock
/// per call allocates the next version, prepends the copy and keeps the 5 newest. A copy
/// already saved with `p_op` is answered again instead of adding another.
async fn push_copy(State(m): State<Arc<Mock>>, h: HeaderMap, Json(body): Json<Value>) -> Response {
    if let Some(r) = gate(&m, &h) {
        return r;
    }
    let Some(sub) = m.bearer(&h) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "PGRST301",
            "JWT expired or invalid",
        );
    };
    let Some(blob) = body.get("p_blob").cloned() else {
        return error(StatusCode::BAD_REQUEST, "PGRST202", "function needs p_blob");
    };
    let op = body.get("p_op").cloned().unwrap_or(Value::Null);
    let mut rows = lock(&m.rows);
    let saved = rows
        .get(&sub)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if let Some(hit) = saved.iter().find(|c| !op.is_null() && c["op"] == op) {
        return Json(json!({ "version": hit["version"], "at": hit["at"], "copies": saved.len() }))
            .into_response();
    }
    let mut versions = lock(&m.versions);
    let version = versions.get(&sub).copied().unwrap_or(0) + 1;
    versions.insert(sub.clone(), version);
    let at = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    let mut copies = rows
        .get(&sub)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    copies.insert(
        0,
        json!({ "version": version, "at": at, "blob": blob, "op": op }),
    );
    copies.truncate(5);
    let kept = copies.len();
    rows.insert(sub, Value::Array(copies));
    let lost = m
        .lost_push_replies
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok();
    if lost {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "PGRST000",
            "connection lost",
        );
    }
    Json(json!({ "version": version, "at": at, "copies": kept })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serves the mock until stopped, for trying `mrl login` and `mrl sync` by hand (see the
    /// P16 handoff). Does nothing unless `MRL_MOCK_SUPABASE_PORT` is set.
    #[tokio::test]
    #[ignore = "manual demo: set MRL_MOCK_SUPABASE_PORT"]
    async fn serve_for_manual_demo() {
        let Some(port) = std::env::var("MRL_MOCK_SUPABASE_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
        else {
            return;
        };
        let sb = MockSupabase::start_on(port).await;
        println!(
            "mock supabase: {} (anon key {ANON_KEY}); signs in {:?}",
            sb.url(),
            sb.user().email
        );
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("could not wait for ctrl-c: {e}");
        }
    }

    #[tokio::test]
    async fn pkce_refresh_and_rls() {
        let sb = MockSupabase::start().await;
        let http = reqwest::Client::new();
        let verifier = "v".repeat(43);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let back = sb
            .approve(&format!(
                "{}/auth/v1/authorize?provider=github&redirect_to=http%3A%2F%2F127.0.0.1%3A9%2Fcb%3Fflow%3Df&code_challenge={challenge}&code_challenge_method=s256",
                sb.url()
            ))
            .unwrap();
        let code = back.split("code=").nth(1).unwrap().to_owned();
        assert!(back.starts_with("http://127.0.0.1:9/cb?flow=f&code="));
        let post = |path: &str, body: Value| {
            http.post(format!("{}{path}", sb.url()))
                .header("apikey", ANON_KEY)
                .json(&body)
                .send()
        };
        let bad = post(
            "/auth/v1/token?grant_type=pkce",
            json!({"auth_code": code, "code_verifier": "wrong"}),
        )
        .await
        .unwrap();
        assert_eq!(bad.status(), 400);
        let spent = post(
            "/auth/v1/token?grant_type=pkce",
            json!({"auth_code": code, "code_verifier": verifier}),
        )
        .await
        .unwrap();
        assert_eq!(
            spent.status(),
            404,
            "the code is spent even after a wrong verifier"
        );
        let back = sb.approve(&format!("{}/auth/v1/authorize?provider=github&redirect_to=x&code_challenge={challenge}&code_challenge_method=s256", sb.url())).unwrap();
        let code = back.split("code=").nth(1).unwrap().to_owned();
        let s: Value = post(
            "/auth/v1/token?grant_type=pkce",
            json!({"auth_code": code, "code_verifier": verifier}),
        )
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        let refresh = s["refresh_token"].as_str().unwrap().to_owned();
        let again = post(
            "/auth/v1/token?grant_type=refresh_token",
            json!({"refresh_token": refresh}),
        )
        .await
        .unwrap();
        assert_eq!(again.status(), 200);
        let reused = post(
            "/auth/v1/token?grant_type=refresh_token",
            json!({"refresh_token": refresh}),
        )
        .await
        .unwrap();
        assert_eq!(reused.status(), 400, "refresh tokens rotate");
        let access = s["access_token"].as_str().unwrap();
        let other = MockUser::new("eve@example.com", "Eve");
        let push = |token: String| {
            http.post(format!("{}/rest/v1/rpc/push_setup_copy", sb.url()))
                .header("apikey", ANON_KEY)
                .bearer_auth(token)
                .json(&json!({ "p_blob": {"n": 1} }))
                .send()
        };
        for want in 1..=6 {
            let r: Value = push(access.to_owned()).await.unwrap().json().await.unwrap();
            assert_eq!(r["version"], want);
            assert_eq!(r["copies"], want.min(5));
        }
        let eve = sb.access_token(&other, 60);
        let rows: Value = http
            .get(format!(
                "{}/rest/v1/setup_profiles?user_id=eq.{}",
                sb.url(),
                sb.user().id
            ))
            .header("apikey", ANON_KEY)
            .bearer_auth(eve)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(rows, json!([]), "RLS hides other users' rows");
    }
}
