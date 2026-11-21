//! Sign-in and credentials: Google/GitHub through Supabase Auth (PKCE in the system browser),
//! personal access tokens for the CLI and SDK, and renderer session cookies.
//!
//! Endpoints (under `/api/v1`):
//! - `POST /auth/login`, `POST /auth/callback` (no sign-in needed in team mode; solo mode
//!   gates every path with the launch secret)
//! - `GET /auth/me`, `POST /auth/logout`, `POST|DELETE /auth/session`,
//!   `GET|POST /auth/tokens`, `DELETE /auth/tokens/{id}`

pub mod credentials;
pub mod problem;
pub mod signin;
pub mod supabase;

use crate::app::{AppState, Task};
use crate::config::{Mode, Role as ProcessRole};
use crate::db::{Db, DbError};
use crate::error::{ApiError, ErrorCode};
use crate::http::pagination::Page;
use crate::modules::audit;
use crate::modules::tenancy;
use crate::seams::{AuditOutcome, PrincipalKind, RequestContext, RequestMeta, Resource, SeamError};
use crate::ServerModule;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use mrl_domain::ids::{OrgId, ProjectId, TokenId, UserId};
use problem::{Coded, Fail};
use serde::{Deserialize, Serialize};
use signin::{Flows, SignIn, SignInState};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use supabase::{JwtVerifier, Supabase};
use time::OffsetDateTime;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

/// `[auth]` settings. The Supabase values are public (the anon key is meant for clients).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Supabase project address, like `https://abcd.supabase.co`. Unset: sign-in is off
    /// (solo mode still works fully offline).
    pub supabase_url: Option<String>,
    /// The project's anon (public) key.
    pub supabase_anon_key: Option<String>,
    /// Lifetime of new access tokens when the request names none (1–365 days).
    pub token_days: u32,
    /// OS keychain service for this install's secrets. Change it only to keep a development
    /// core's secrets apart from the installed app's.
    pub keychain_service: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            supabase_url: None,
            supabase_anon_key: None,
            token_days: 90,
            keychain_service: "masterrl".to_owned(),
        }
    }
}

/// True for `https://host[:port][/path]`, or `http://` to a loopback host with a port.
fn supabase_url_ok(url: &str) -> bool {
    let Ok(u) = reqwest::Url::parse(url) else {
        return false;
    };
    let loopback = matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
    (u.scheme() == "https" || (u.scheme() == "http" && loopback && u.port().is_some()))
        && u.host_str().is_some()
        && u.username().is_empty()
        && u.password().is_none()
        && u.query().is_none()
        && u.fragment().is_none()
}

impl AuthConfig {
    /// Problems with these settings, one line each (team mode also needs Supabase, checked at startup).
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        match (&self.supabase_url, &self.supabase_anon_key) {
            (Some(url), Some(_)) => {
                // The value is not echoed: a mistyped one may hold a password.
                if !supabase_url_ok(url) {
                    out.push(
                        "auth.supabase_url must be an https:// address with no user name, password, query or fragment, like https://abcd.supabase.co (http:// only for a local test server on 127.0.0.1, localhost or [::1])"
                            .to_owned(),
                    );
                }
            }
            (None, None) => {}
            _ => out.push(
                "auth.supabase_url and auth.supabase_anon_key: set both or neither".to_owned(),
            ),
        }
        let service_ok = (1..=64).contains(&self.keychain_service.len())
            && self
                .keychain_service
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
        if !service_ok {
            out.push(format!(
                "auth.keychain_service: {:?} must be 1–64 letters, digits, '.', '_' or '-'",
                self.keychain_service
            ));
        }
        if !(1..=credentials::MAX_TOKEN_DAYS).contains(&self.token_days) {
            out.push(format!(
                "auth.token_days: {} is outside 1–365",
                self.token_days
            ));
        }
        out
    }
}

struct Shared {
    db: Db,
    config: AuthConfig,
    mode: Mode,
    secure_cookie: bool,
    supabase: Option<Supabase>,
    verifier: Option<JwtVerifier>,
    flows: Flows,
    pepper: tokio::sync::OnceCell<[u8; 32]>,
    /// Bounds sign-in work open to anyone (login and callback need no credentials in team
    /// mode, and each callback calls Supabase).
    anonymous: tokio::sync::Semaphore,
}

/// Login and callback requests served at once; more get 429 with Retry-After.
const ANONYMOUS_AT_ONCE: usize = 32;

fn busy() -> ApiError {
    ApiError::new(
        ErrorCode::RateLimited,
        "The server is busy with other sign-ins; try again in a moment.",
    )
    .retry_after(1)
}

impl Shared {
    fn sign_in(&self, state: &AppState) -> SignIn {
        SignIn::new(
            self.db.clone(),
            self.supabase.clone(),
            state.seams.secrets.clone(),
        )
    }

    async fn pepper(&self, state: &AppState) -> Result<[u8; 32], ApiError> {
        let store = state.seams.secrets.clone().ok_or_else(no_secret_store)?;
        Ok(*self
            .pepper
            .get_or_try_init(|| async move { credentials::load_pepper(store.as_ref()).await })
            .await?)
    }
}

fn no_secret_store() -> ApiError {
    ApiError::new(ErrorCode::Unavailable, "This server has no secret store.")
}

/// Refuses an `Idempotency-Key` on an endpoint that takes or returns a secret (a passphrase, a
/// recovery key, a token, a session cookie). These endpoints are mounted outside P07's
/// idempotency layer (see [`Caller`]), so nothing about the request is stored.
pub fn no_replay(headers: &HeaderMap) -> Result<(), ApiError> {
    if headers.contains_key(crate::http::idempotency::IDEMPOTENCY_KEY) {
        return Err(ApiError::new(
            ErrorCode::IdempotencyKey,
            "This endpoint returns a secret only once, so it does not take an Idempotency-Key; send the request without it.",
        ));
    }
    Ok(())
}

/// The signed-in caller on an endpoint that takes or returns a secret.
///
/// P07's idempotency layer fingerprints the body (a fast check of a passphrase) and keeps the
/// response for replay before any handler runs, so these endpoints are mounted with the
/// public routes, and this extractor does what the signed-in stack does: refuses an
/// `Idempotency-Key`, signs the caller in (solo mode's launch-secret gate already has) and
/// applies the rate limit.
pub struct Caller(pub RequestContext);

impl axum::extract::FromRequestParts<AppState> for Caller {
    type Rejection = Fail;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Fail> {
        no_replay(&parts.headers)?;
        let ctx = match parts.extensions.get::<RequestContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                let meta = parts
                    .extensions
                    .get::<RequestMeta>()
                    .cloned()
                    .unwrap_or(RequestMeta {
                        trace_id: String::new(),
                        request_id: String::new(),
                    });
                let principal = state
                    .seams
                    .authenticator
                    .authenticate(&parts.headers)
                    .await?
                    .ok_or_else(|| {
                        ApiError::new(
                            ErrorCode::Unauthenticated,
                            "The request has no valid sign-in details.",
                        )
                    })?;
                RequestContext::new(principal, &meta)
            }
        };
        let now = std::time::Instant::now();
        if let Err(secs) = state
            .limiter()
            .check(&ctx.principal.id, ctx.org.as_deref(), now)
        {
            return Err(ApiError::new(
                ErrorCode::RateLimited,
                "Too many requests for this account or org.",
            )
            .retry_after(secs)
            .into());
        }
        Ok(Caller(ctx))
    }
}

/// SHA-384 that `3791ab7`'s migration 16007 (which cleared every replay record) left in
/// `_sqlx_migrations`. Its text was narrowed later; those databases already ran the wider
/// cleanup, so only the recorded checksum is out of date.
const OLD_16007_CHECKSUM: &str = "1d60f7824cb6fd11da14c37b45e470fdc7c76a701ac3046ad6e89fc51b60523d5bdda95b26d1e1e37e94ccd72103ba0c";

/// Brings a database that ran the earlier 16007 in line with this build, so later migrations
/// pass SQLx's checksum check. Changes nothing else, and nothing on any other database. Run
/// it before migrations.
pub async fn repair_migration_checksums(db: &Db) -> Result<(), DbError> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(db.pool())
        .await?;
    let current = crate::db::migrate::migrator()
        .iter()
        .find(|m| m.version == 16007)
        .map(|m| m.checksum.to_vec());
    let (true, Some(current)) = (exists, current) else {
        return Ok(());
    };
    let n = sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 16007 AND checksum = decode($2, 'hex')",
    )
    .bind(current)
    .bind(OLD_16007_CHECKSUM)
    .execute(db.pool())
    .await?
    .rows_affected();
    if n > 0 {
        tracing::info!("updated the recorded checksum of database update 16007");
    }
    Ok(())
}

/// A database error as an API error (503, details only in the log).
pub fn db_error(e: DbError) -> ApiError {
    ApiError::from(SeamError::from(e))
}

/// The production seams for `mode` over `db` (P16): `CoreAuthenticator`, `Rbac`,
/// `PgAuditSink`, `SupabaseIdentity`, `PgIdempotencyStore` and `secrets` (the OS keychain in
/// `main`). Returns the builder so tests can still swap the clock or ids.
pub async fn production_seams(
    config: &crate::Config,
    db: &Db,
    secrets: Arc<dyn crate::seams::SecretStore>,
    launch_secret: &str,
) -> Result<crate::seams::SeamsBuilder, crate::ServerError> {
    let mode = config.server.mode;
    if mode == Mode::Team && Supabase::from_config(&config.auth).is_none() {
        return Err(crate::ServerError::new(
            ErrorCode::IdentityProviderMissing,
            "team mode signs people in through Supabase; set auth.supabase_url and auth.supabase_anon_key",
        ));
    }
    let pepper = credentials::load_pepper(secrets.as_ref())
        .await
        .map_err(|e| {
            crate::ServerError::new(ErrorCode::StartupStep, format!("access token key: {e}"))
        })?;
    let identity: Arc<dyn crate::seams::IdentityProvider> = Arc::new(
        supabase::SupabaseIdentity::new(&config.auth, db.clone(), mode),
    );
    let solo = (mode == Mode::Solo).then_some(launch_secret);
    Ok(crate::Seams::production(mode, launch_secret)
        .idempotency(crate::db::PgIdempotencyStore::new(db.clone()))
        .authenticator(credentials::CoreAuthenticator::new(
            db.clone(),
            pepper,
            identity.clone(),
            solo,
        ))
        .authorizer(tenancy::Rbac::new(db.clone(), mode))
        .audit(audit::PgAuditSink::new(db.clone()))
        .identity_arc(identity)
        .secrets_arc(secrets))
}

/// Sign-in, tokens and sessions.
pub struct AuthModule {
    shared: Arc<Shared>,
}

impl AuthModule {
    /// The module for these settings.
    pub fn new(db: Db, config: &crate::Config) -> Self {
        let supabase = Supabase::from_config(&config.auth);
        Self {
            shared: Arc::new(Shared {
                verifier: supabase.clone().map(|s| JwtVerifier::new(s, db.clone())),
                supabase,
                db,
                config: config.auth.clone(),
                mode: config.server.mode,
                secure_cookie: config.server.mode == Mode::Team && config.server.tls.is_some(),
                flows: Flows::default(),
                pepper: tokio::sync::OnceCell::new(),
                anonymous: tokio::sync::Semaphore::new(ANONYMOUS_AT_ONCE),
            }),
        }
    }
}

impl ServerModule for AuthModule {
    fn name(&self) -> &'static str {
        "auth"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/auth/me", get(me))
            .route("/auth/logout", post(logout))
            .route("/auth/tokens/{id}", delete(revoke_token))
            .layer(Extension(self.shared.clone()))
            .layer(axum::middleware::from_fn(
                crate::modules::auth::problem::render,
            ))
    }

    /// Also the routes that hand out a secret; their handlers sign the caller in through
    /// [`Caller`], outside P07's idempotency layer.
    fn public_routes(&self) -> Router<AppState> {
        Router::new()
            .route("/auth/login", post(login))
            .route("/auth/callback", post(callback))
            .route("/auth/session", post(start_session).delete(end_session))
            .route("/auth/tokens", get(list_tokens).post(create_token))
            .layer(Extension(self.shared.clone()))
            .layer(axum::middleware::from_fn(
                crate::modules::auth::problem::render,
            ))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        AuthDoc::openapi()
    }

    fn background_tasks(&self) -> Vec<Task> {
        let db = self.shared.db.clone();
        vec![Task::new(
            "auth.sweep",
            ProcessRole::Background,
            move |cancel| async move {
                loop {
                    if let Err(e) = credentials::sweep(&db).await {
                        tracing::warn!(error = %e, "could not delete ended sessions and tokens");
                    }
                    tokio::select! {
                        () = cancel.cancelled() => return Ok(()),
                        () = tokio::time::sleep(Duration::from_secs(3600)) => {}
                    }
                }
            },
        )]
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(login, callback, me, logout, start_session, end_session, list_tokens, create_token, revoke_token),
    components(schemas(LoginRequest, LoginStarted, CallbackRequest, SignedIn, UserInfo, NewToken, Me, RoleInfo, SessionStarted, TokenInfo, CreateToken, SignInState)),
    tags((name = "auth", description = "Sign-in, access tokens and sessions."))
)]
struct AuthDoc;

/// Start a sign-in.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    /// `google` or `github`.
    pub provider: String,
    /// `masterrl://auth/callback`, or a loopback address like `http://127.0.0.1:53111/callback`.
    pub redirect_uri: String,
}

/// Open `authorize_url` in the system browser.
#[derive(Debug, Serialize, ToSchema)]
pub struct LoginStarted {
    /// The address to open.
    pub authorize_url: String,
    /// Pass back with the code; also in the callback query as `flow`.
    pub flow_id: String,
}

/// Finish a sign-in with the code from the callback.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CallbackRequest {
    /// `flow` from the callback query.
    pub flow_id: String,
    /// `code` from the callback query.
    pub code: String,
    /// Also make an access token (for the CLI).
    #[serde(default)]
    pub issue_token: bool,
    /// Name for that token.
    #[serde(default)]
    pub token_name: Option<String>,
}

/// A person.
#[derive(Debug, Serialize, ToSchema)]
pub struct UserInfo {
    /// `usr_…`.
    pub id: String,
    /// Email.
    pub email: Option<String>,
    /// Name.
    pub display_name: String,
    /// Avatar.
    pub avatar_url: Option<String>,
}

/// A token, shown once.
#[derive(Debug, Serialize, ToSchema)]
pub struct NewToken {
    /// `tok_…`.
    pub id: String,
    /// `mrl_pat_…`. Store it now; it is never shown again.
    pub token: String,
    /// Scopes.
    pub scopes: Vec<String>,
    /// Expiry.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: OffsetDateTime,
}

/// The sign-in finished.
#[derive(Debug, Serialize, ToSchema)]
pub struct SignedIn {
    /// Who signed in.
    pub user: UserInfo,
    /// The new token, when asked for.
    pub token: Option<NewToken>,
}

fn sign_in_failed(detail: impl std::fmt::Display) -> Fail {
    Coded::new(
        signin::NOT_FINISHED,
        StatusCode::UNAUTHORIZED,
        format!("{detail}."),
    )
    .into()
}

fn not_set_up() -> Fail {
    Fail::from(signin::SignInError::NotConfigured)
}

fn field(path: &str, message: impl Into<String>) -> ApiError {
    ApiError::validation(vec![crate::error::FieldError::new(
        path,
        ErrorCode::FieldValue,
        message,
        "See the API reference for accepted values.",
    )])
}

#[utoipa::path(post, path = "/auth/login", tag = "auth", request_body = LoginRequest,
    responses((status = 200, body = LoginStarted), (status = 422, body = crate::error::Problem), (status = 503, body = crate::error::Problem)))]
async fn login(
    Extension(shared): Extension<Arc<Shared>>,
    ctx: Option<Extension<RequestContext>>,
    crate::modules::auth::problem::Body(req): crate::modules::auth::problem::Body<LoginRequest>,
) -> Result<Json<LoginStarted>, Fail> {
    reject_scoped_session(&shared, ctx.as_ref().map(|c| &c.0)).await?;
    let _permit = shared.anonymous.try_acquire().map_err(|_| busy())?;
    let supabase = shared.supabase.as_ref().ok_or_else(not_set_up)?;
    if !matches!(req.provider.as_str(), "google" | "github") {
        return Err(field("/provider", "provider must be google or github").into());
    }
    if !signin::redirect_allowed(&req.redirect_uri) {
        return Err(field(
            "/redirect_uri",
            "redirect_uri must be masterrl://auth/callback or an http://127.0.0.1:<port>/… address",
        )
        .into());
    }
    let (flow_id, challenge) = shared.flows.start()?;
    let redirect_to = signin::with_flow(&req.redirect_uri, &flow_id);
    Ok(Json(LoginStarted {
        authorize_url: supabase.authorize_url(&req.provider, &redirect_to, &challenge),
        flow_id,
    }))
}

#[utoipa::path(post, path = "/auth/callback", tag = "auth", request_body = CallbackRequest,
    responses((status = 200, body = SignedIn), (status = 401, body = crate::error::Problem), (status = 503, body = crate::error::Problem)))]
async fn callback(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    Extension(meta): Extension<RequestMeta>,
    ctx: Option<Extension<RequestContext>>,
    headers: HeaderMap,
    crate::modules::auth::problem::Body(req): crate::modules::auth::problem::Body<CallbackRequest>,
) -> Result<Json<SignedIn>, Fail> {
    no_replay(&headers)?;
    reject_scoped_session(&shared, ctx.as_ref().map(|c| &c.0)).await?;
    let _permit = shared.anonymous.try_acquire().map_err(|_| busy())?;
    let (supabase, verifier) = match (&shared.supabase, &shared.verifier) {
        (Some(s), Some(v)) => (s, v),
        _ => return Err(not_set_up()),
    };
    let flow = shared
        .flows
        .take(&req.flow_id)
        .ok_or_else(|| sign_in_failed("this sign-in expired or was already used; start again"))?;
    let tokens = supabase
        .exchange_code(&req.code, &flow.verifier)
        .await
        .map_err(|e| match e {
            supabase::SupabaseError::Offline(_) => Coded::new(
                supabase::OFFLINE,
                StatusCode::SERVICE_UNAVAILABLE,
                format!("{e}."),
            )
            .into(),
            other => sign_in_failed(other),
        })?;
    let claims = verifier
        .verify(&tokens.access_token)
        .await?
        .filter(|c| c.sub == tokens.user.id)
        .ok_or_else(|| {
            sign_in_failed("the sign-in service sent a token the app could not check")
        })?;
    let profile = claims.profile();
    let db = &shared.db;
    // Solo: one sign-in at a time from the account check to the keychain write, so two
    // accounts finishing at once cannot both pass the check.
    let held = match shared.mode {
        Mode::Solo => Some(signin::hold_session().await),
        Mode::Team => None,
    };
    let sign_in = shared.sign_in(&state);
    let solo_user = if let Some(h) = &held {
        let owner = tenancy::ensure_local_owner(db).await.map_err(db_error)?;
        let linked = tenancy::local_owner(db)
            .await
            .map_err(db_error)?
            .and_then(|o| o.supabase_user_id);
        let active = sign_in.state_held(h).await? == SignInState::Active;
        if linked.is_some_and(|l| l != profile.supabase_user_id) && active {
            return Err(Coded::new(
                signin::OTHER_ACCOUNT,
                StatusCode::CONFLICT,
                "This app is signed in to another account; sign out first.",
            )
            .into());
        }
        Some(owner)
    } else {
        None
    };
    let mut tx = db.begin().await.map_err(db_error)?;
    let (user, org) = if let Some(owner) = solo_user {
        (owner, None)
    } else {
        let user = tenancy::upsert_supabase_user_in(&mut tx, &profile)
            .await
            .map_err(db_error)?;
        let org = tenancy::primary_org_with(&mut *tx, user)
            .await
            .map_err(db_error)?;
        (user, org)
    };
    let pepper = if req.issue_token {
        Some(shared.pepper(&state).await?)
    } else {
        None
    };
    let token_req = credentials::TokenRequest {
        user,
        org,
        project: None,
        name: req
            .token_name
            .clone()
            .unwrap_or_else(|| "mrl CLI".to_owned())
            .chars()
            .take(100)
            .collect(),
        scopes: vec!["*".to_owned()],
        days: shared.config.token_days,
        not_after: None,
    };
    let actor = user.to_string();
    let at = state.seams.clock.now();
    if shared.mode == Mode::Solo {
        tenancy::link_local_owner(&mut tx, user, &profile)
            .await
            .map_err(db_error)?;
    }
    let token = match &pepper {
        Some(p) => Some(credentials::issue_token(&mut tx, p, &token_req).await?),
        None => None,
    };
    let org_text = org.map(|o| o.to_string());
    let event = audit::event(
        at,
        &actor,
        org_text.as_deref(),
        "auth.session",
        &actor,
        AuditOutcome::Success,
        &meta.trace_id,
    );
    let details = serde_json::json!({ "step": "signed_in", "token": token.as_ref().map(|t| t.id.to_string()) });
    audit::write(&mut tx, &event, &details)
        .await
        .map_err(db_error)?;
    if let Some(h) = &held {
        let operation = uuid::Uuid::now_v7();
        sqlx::query("UPDATE users SET signin_operation = $2 WHERE id = $1")
            .bind(user.as_uuid())
            .bind(operation)
            .execute(&mut *tx)
            .await
            .map_err(|e| db_error(e.into()))?;
        sign_in.stage(h, operation, &tokens).await?;
        let result = commit(tx).await;
        if !sign_in.reconcile(h).await? {
            result?;
            return Err(ApiError::new(
                ErrorCode::Unavailable,
                "The sign-in could not be confirmed.",
            )
            .into());
        }
    } else {
        commit(tx).await?;
    }
    drop(held);
    Ok(Json(SignedIn {
        user: UserInfo {
            id: user.to_string(),
            email: profile.email.clone(),
            display_name: profile.display_name.clone(),
            avatar_url: profile.avatar_url.clone(),
        },
        token: token.map(|t| NewToken {
            id: t.id.to_string(),
            token: t.token.expose().to_owned(),
            scopes: token_req.scopes.clone(),
            expires_at: t.expires_at,
        }),
    }))
}

/// Commits `tx`, as an API error on failure.
pub async fn commit(tx: crate::db::Tx) -> Result<(), ApiError> {
    tx.commit().await.map_err(|e| db_error(e.into()))
}

/// One role a person holds.
#[derive(Debug, Serialize, ToSchema)]
pub struct RoleInfo {
    /// `org_…`.
    pub org: String,
    /// Role name.
    pub role: String,
    /// `org`, `workspace` or `project`.
    pub scope_kind: String,
    /// The scope's id.
    pub scope_id: String,
}

/// The caller.
#[derive(Debug, Serialize, ToSchema)]
pub struct Me {
    /// The person (absent for an unknown principal).
    pub user: Option<UserInfo>,
    /// `local_owner`, `user` or `service`.
    pub principal_kind: String,
    /// `tok_…` when the caller uses an access token.
    pub token_id: Option<String>,
    /// This install's Supabase sign-in (solo mode).
    pub sign_in: SignInState,
    /// Roles.
    pub roles: Vec<RoleInfo>,
}

#[utoipa::path(get, path = "/auth/me", tag = "auth", responses((status = 200, body = Me), (status = 401, body = crate::error::Problem)))]
async fn me(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    ctx: RequestContext,
) -> Result<Json<Me>, Fail> {
    state
        .authorize(&ctx, "auth.session", &Resource::kind("account"))
        .await?;
    let db = &shared.db;
    let user = tenancy::acting_user(db, &ctx.principal)
        .await
        .map_err(db_error)?;
    let (info, roles) = match user {
        None => (None, Vec::new()),
        Some(u) => {
            let row: Option<(Option<String>, String, Option<String>)> =
                sqlx::query_as("SELECT email, display_name, avatar_url FROM users WHERE id = $1")
                    .bind(u.as_uuid())
                    .fetch_optional(db.pool())
                    .await
                    .map_err(|e| db_error(e.into()))?;
            // A token sees only the bindings inside its org and project.
            let (org, project) = token_bounds(&shared, &ctx).await?;
            let roles: Vec<(Uuid, String, String, Uuid)> = sqlx::query_as(
                "SELECT org_id, role, scope_kind, scope_id FROM role_bindings \
                 WHERE principal_kind = 'user' AND principal_id = $1 \
                   AND ($2::uuid IS NULL OR org_id = $2) \
                   AND ($3::uuid IS NULL OR scope_kind = 'org' OR scope_id = $3 \
                        OR scope_id = (SELECT workspace_id FROM projects WHERE id = $3)) \
                 ORDER BY created_at",
            )
            .bind(u.as_uuid())
            .bind(org.map(|o| o.as_uuid()))
            .bind(project.map(|p| p.as_uuid()))
            .fetch_all(db.pool())
            .await
            .map_err(|e| db_error(e.into()))?;
            (
                row.map(|(email, display_name, avatar_url)| UserInfo {
                    id: u.to_string(),
                    email,
                    display_name,
                    avatar_url,
                }),
                roles
                    .into_iter()
                    .map(|(org, role, scope_kind, scope_id)| RoleInfo {
                        org: OrgId::from_uuid(org).to_string(),
                        scope_id: match scope_kind.as_str() {
                            "project" => ProjectId::from_uuid(scope_id).to_string(),
                            "workspace" => {
                                mrl_domain::ids::WorkspaceId::from_uuid(scope_id).to_string()
                            }
                            _ => OrgId::from_uuid(scope_id).to_string(),
                        },
                        role,
                        scope_kind,
                    })
                    .collect(),
            )
        }
    };
    let sign_in = match shared.mode {
        Mode::Solo => shared.sign_in(&state).state().await?,
        Mode::Team => SignInState::Active,
    };
    Ok(Json(Me {
        user: info,
        principal_kind: kind_name(ctx.principal.kind).to_owned(),
        token_id: (ctx.principal.kind == PrincipalKind::Service).then(|| ctx.principal.id.clone()),
        sign_in,
        roles,
    }))
}

fn kind_name(k: PrincipalKind) -> &'static str {
    match k {
        PrincipalKind::LocalOwner => "local_owner",
        PrincipalKind::User => "user",
        PrincipalKind::Service => "service",
    }
}

/// Writes an `auth.session` audit row in `tx`, with the change it records.
async fn record(
    state: &AppState,
    tx: &mut crate::db::Tx,
    ctx: &RequestContext,
    target: &str,
    details: serde_json::Value,
) -> Result<(), ApiError> {
    let event = audit::event(
        state.seams.clock.now(),
        &ctx.principal.id,
        ctx.org.as_deref(),
        "auth.session",
        target,
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    audit::write(tx, &event, &details).await.map_err(db_error)
}

#[utoipa::path(post, path = "/auth/logout", tag = "auth", responses((status = 204, description = "Signed out.")))]
async fn logout(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    ctx: RequestContext,
    headers: HeaderMap,
) -> Result<StatusCode, Fail> {
    state
        .authorize(&ctx, "auth.session", &Resource::kind("account"))
        .await?;
    let db = &shared.db;
    // The access token the caller sent is revoked even when another credential (like the
    // solo launch secret, which the CLI also sends) signed the request in.
    let mut revoke = Vec::new();
    if ctx.principal.kind == PrincipalKind::Service {
        revoke.extend(TokenId::from_str(&ctx.principal.id).ok());
    }
    if let Some(sent) =
        credentials::bearer(&headers).filter(|t| t.starts_with(credentials::PAT_PREFIX))
    {
        let pepper = shared.pepper(&state).await?;
        if let Some(p) = credentials::verify_token(db, &pepper, sent).await? {
            revoke.extend(TokenId::from_str(&p.id).ok());
        }
    }
    let (org, project) = token_bounds(&shared, &ctx).await?;
    let scoped = org.is_some() || project.is_some();
    if shared.mode == Mode::Solo && !scoped {
        shared.sign_in(&state).sign_out().await?;
    }
    let mut tx = db.begin().await.map_err(db_error)?;
    for id in &revoke {
        sqlx::query(
            "UPDATE api_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL",
        )
        .bind(id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e.into()))?;
    }
    if !scoped {
        credentials::end_session(&mut tx, &headers)
            .await
            .map_err(db_error)?;
    }
    let revoked: Vec<String> = revoke.iter().map(ToString::to_string).collect();
    record(
        &state,
        &mut tx,
        &ctx,
        &ctx.principal.id,
        serde_json::json!({ "step": "signed_out", "tokens_revoked": revoked }),
    )
    .await?;
    commit(tx).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// A renderer session started; the cookie is in `Set-Cookie`.
#[derive(Debug, Serialize, ToSchema)]
pub struct SessionStarted {
    /// Send as `x-csrf-token` on requests from another origin.
    pub csrf_token: String,
    /// When the session ends at the latest.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: OffsetDateTime,
}

fn cookie(value: &str, max_age: i64, secure: bool) -> String {
    format!(
        "{}={value}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age}{}",
        credentials::SESSION_COOKIE,
        if secure { "; Secure" } else { "" }
    )
}

#[utoipa::path(post, path = "/auth/session", tag = "auth", responses((status = 201, body = SessionStarted)))]
async fn start_session(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    Caller(ctx): Caller,
    headers: HeaderMap,
) -> Result<impl IntoResponse, Fail> {
    if ctx.principal.kind == PrincipalKind::Service {
        return Err(ApiError::new(
            ErrorCode::Forbidden,
            "Access tokens cannot start browser sessions.",
        )
        .into());
    }
    let user = authorize_self_shared(&state, &shared, &ctx, "auth.session").await?;
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok());
    let mut tx = shared.db.begin().await.map_err(db_error)?;
    let s = credentials::create_session(&mut tx, user, ua).await?;
    record(
        &state,
        &mut tx,
        &ctx,
        &user.to_string(),
        serde_json::json!({ "step": "session_started" }),
    )
    .await?;
    commit(tx).await?;
    let max_age = i64::from(credentials::SESSION_MAX_DAYS) * 86_400;
    Ok((
        StatusCode::CREATED,
        [(
            header::SET_COOKIE,
            cookie(s.cookie.expose(), max_age, shared.secure_cookie),
        )],
        Json(SessionStarted {
            csrf_token: s.csrf.expose().to_owned(),
            expires_at: s.expires_at,
        }),
    ))
}

#[utoipa::path(delete, path = "/auth/session", tag = "auth", responses((status = 204, description = "Session ended.")))]
async fn end_session(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    Caller(ctx): Caller,
    headers: HeaderMap,
) -> Result<impl IntoResponse, Fail> {
    reject_scoped_session(&shared, Some(&ctx)).await?;
    state
        .authorize(&ctx, "auth.session", &Resource::kind("account"))
        .await?;
    let mut tx = shared.db.begin().await.map_err(db_error)?;
    credentials::end_session(&mut tx, &headers)
        .await
        .map_err(db_error)?;
    record(
        &state,
        &mut tx,
        &ctx,
        &ctx.principal.id,
        serde_json::json!({ "step": "session_ended" }),
    )
    .await?;
    commit(tx).await?;
    Ok((
        StatusCode::NO_CONTENT,
        [(header::SET_COOKIE, cookie("", 0, shared.secure_cookie))],
    ))
}

async fn authorize_self_shared(
    state: &AppState,
    shared: &Shared,
    ctx: &RequestContext,
    action: &str,
) -> Result<UserId, ApiError> {
    state
        .authorize(ctx, action, &Resource::kind("account"))
        .await?;
    acting(shared, ctx).await
}

/// The org and project a token caller is held to; `(None, None)` for people. Account-wide
/// lists and changes stay inside these bounds.
async fn token_bounds(
    shared: &Shared,
    ctx: &RequestContext,
) -> Result<(Option<OrgId>, Option<ProjectId>), ApiError> {
    if ctx.principal.kind != PrincipalKind::Service {
        return Ok((None, None));
    }
    let grant = credentials::token_grant(&shared.db, &ctx.principal.id)
        .await
        .map_err(db_error)?
        .ok_or_else(|| {
            ApiError::new(
                ErrorCode::Unauthenticated,
                "This access token no longer works.",
            )
        })?;
    Ok((grant.org, grant.project))
}

async fn reject_scoped_session(
    shared: &Shared,
    ctx: Option<&RequestContext>,
) -> Result<(), ApiError> {
    if let Some(ctx) = ctx {
        let (org, project) = token_bounds(shared, ctx).await?;
        if org.is_some() || project.is_some() {
            return Err(ApiError::new(
                ErrorCode::Forbidden,
                "A scoped access token cannot change the app's sign-in or browser session.",
            ));
        }
    }
    Ok(())
}

fn token_resource(org: Option<OrgId>, project: Option<ProjectId>) -> Resource {
    Resource {
        org: org.map(|id| id.to_string()),
        project: project.map(|id| id.to_string()),
        ..Resource::kind("account")
    }
}

/// The person behind `ctx`, with no permission check.
async fn acting(shared: &Shared, ctx: &RequestContext) -> Result<UserId, ApiError> {
    tenancy::acting_user(&shared.db, &ctx.principal)
        .await
        .map_err(db_error)?
        .ok_or_else(|| {
            ApiError::new(
                ErrorCode::Unauthenticated,
                "This sign-in has no account behind it.",
            )
        })
}

type TokenRow = (
    Uuid,
    String,
    String,
    Vec<String>,
    OffsetDateTime,
    Option<OffsetDateTime>,
);

/// A token as listed (never the token itself).
#[derive(Debug, Serialize, ToSchema)]
pub struct TokenInfo {
    /// `tok_…`.
    pub id: String,
    /// Name.
    pub name: String,
    /// First characters, like `mrl_pat_AbCd`.
    pub prefix: String,
    /// Scopes.
    pub scopes: Vec<String>,
    /// Expiry.
    #[serde(with = "time::serde::rfc3339")]
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: OffsetDateTime,
    /// Last use (to the minute).
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub last_used_at: Option<OffsetDateTime>,
}

#[utoipa::path(get, path = "/auth/tokens", tag = "auth", params(crate::http::pagination::PageQuery),
    responses((status = 200, description = "Your live tokens, newest first, in cursor pages.", body = Page<TokenInfo>)))]
async fn list_tokens(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    Caller(ctx): Caller,
    axum::extract::Query(q): axum::extract::Query<crate::http::pagination::PageQuery>,
) -> Result<Json<Page<TokenInfo>>, Fail> {
    let (bound_org, bound_project) = token_bounds(&shared, &ctx).await?;
    let resource = Resource {
        kind: tenancy::TOKEN_LIST.into(),
        ..token_resource(bound_org, bound_project)
    };
    state
        .authorize(&ctx, "auth.manage_tokens", &resource)
        .await?;
    let user = acting(&shared, &ctx).await?;
    let page = q.resolve()?;
    let bad_cursor = || {
        ApiError::new(
            ErrorCode::Pagination,
            "cursor must be the next_cursor value from an earlier page, unchanged.",
        )
    };
    let after = match page.after.as_deref() {
        Some(a) => Some(Uuid::parse_str(a).map_err(|_| bad_cursor())?),
        None => None,
    };
    // Token ids are UUIDv7, so id order is creation order.
    let rows: Vec<TokenRow> = sqlx::query_as(
        "SELECT t.id, t.name, t.token_prefix, t.scopes, t.expires_at, t.last_used_at \
         FROM api_tokens t LEFT JOIN projects p ON p.id = t.project_id \
         WHERE t.user_id = $1 AND t.revoked_at IS NULL AND t.expires_at > now() \
           AND ($2::uuid IS NULL OR t.id < $2) AND ($4::uuid IS NULL OR t.org_id = $4) \
           AND ($5::uuid IS NULL OR t.project_id = $5) AND p.deleted_at IS NULL \
           AND ($6 OR EXISTS (SELECT 1 FROM role_bindings r \
             WHERE r.principal_kind = 'user' AND r.principal_id = t.user_id \
               AND r.org_id = COALESCE(t.org_id, $8) AND r.role = ANY($7) \
               AND (r.scope_kind = 'org' \
                 OR (r.scope_kind = 'project' AND r.scope_id = t.project_id) \
                 OR (r.scope_kind = 'workspace' AND r.scope_id = p.workspace_id)))) \
         ORDER BY t.id DESC LIMIT $3",
    )
    .bind(user.as_uuid())
    .bind(after)
    .bind(i64::try_from(page.fetch()).unwrap_or(201))
    .bind(bound_org.map(|o| o.as_uuid()))
    .bind(bound_project.map(|p| p.as_uuid()))
    .bind(
        shared.mode == Mode::Solo
            && tenancy::local_owner(&shared.db)
                .await
                .map_err(db_error)?
                .is_some_and(|o| o.id == user),
    )
    .bind(tenancy::roles_allowing(
        mrl_domain::rbac::Action::AuthManageTokens,
    ))
    .bind(
        ctx.org
            .as_deref()
            .and_then(|id| OrgId::from_str(id).ok())
            .map(|id| id.as_uuid()),
    )
    .fetch_all(shared.db.pool())
    .await
    .map_err(|e| db_error(e.into()))?;
    let page = Page::from_overfetch(rows, &page, |r| r.0.to_string());
    Ok(Json(Page {
        items: page
            .items
            .into_iter()
            .map(
                |(id, name, prefix, scopes, expires_at, last_used_at)| TokenInfo {
                    id: TokenId::from_uuid(id).to_string(),
                    name,
                    prefix,
                    scopes,
                    expires_at,
                    last_used_at,
                },
            )
            .collect(),
        next_cursor: page.next_cursor,
    }))
}

/// Make an access token.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateToken {
    /// A name you recognize, 1–100 characters.
    pub name: String,
    /// `["*"]`, or areas like `["runs:write", "datasets:read"]`.
    pub scopes: Vec<String>,
    /// Lifetime, 1–365 days (default from `auth.token_days`).
    #[serde(default)]
    pub days: Option<u32>,
    /// Limit the token to one project (`prj_…`).
    #[serde(default)]
    pub project_id: Option<String>,
}

#[utoipa::path(post, path = "/auth/tokens", tag = "auth", request_body = CreateToken,
    responses((status = 201, body = NewToken), (status = 422, body = crate::error::Problem)))]
async fn create_token(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    Caller(ctx): Caller,
    crate::modules::auth::problem::Body(req): crate::modules::auth::problem::Body<CreateToken>,
) -> Result<(StatusCode, Json<NewToken>), Fail> {
    // Authorized below: on the project when one is named (a project-only researcher may
    // make tokens for it), else account-wide.
    let user = acting(&shared, &ctx).await?;
    let name = req.name.trim();
    if name.is_empty() || name.chars().count() > 100 {
        return Err(field("/name", "name must have 1–100 characters").into());
    }
    credentials::check_scopes(&req.scopes).map_err(|_| {
        field(
            "/scopes",
            "Use one or more known scopes, such as runs:read, or *.",
        )
    })?;
    let days = req.days.unwrap_or(shared.config.token_days);
    if !(1..=credentials::MAX_TOKEN_DAYS).contains(&days) {
        return Err(field("/days", "days must be 1–365").into());
    }
    let mut project = req
        .project_id
        .as_deref()
        .map(|p| ProjectId::from_str(p).map_err(|e| field("/project_id", e.to_string())))
        .transpose()?;
    let db = &shared.db;
    let mut org = ctx.org.as_deref().and_then(|o| OrgId::from_str(o).ok());
    let mut not_after = None;
    if ctx.principal.kind == PrincipalKind::Service {
        let grant = credentials::token_grant(db, &ctx.principal.id)
            .await
            .map_err(db_error)?;
        let Some(grant) = grant else {
            return Err(ApiError::new(
                ErrorCode::Unauthenticated,
                "This access token no longer works.",
            )
            .into());
        };
        if !credentials::scopes_cover(&grant.scopes, &req.scopes) {
            return Err(ApiError::new(
                ErrorCode::Forbidden,
                "A token can only make tokens with the same or fewer scopes.",
            )
            .into());
        }
        // A one-project token only makes tokens for that same project.
        match (grant.project, project) {
            (Some(bound), Some(asked)) if bound != asked => {
                return Err(ApiError::new(
                    ErrorCode::Forbidden,
                    "This token is limited to one project, so the tokens it makes are too.",
                )
                .into())
            }
            (Some(bound), _) => project = Some(bound),
            (None, _) => {}
        }
        org = grant.org;
        // A child never outlives the token that made it.
        not_after = Some(grant.expires_at);
    } else if org.is_none() && shared.mode == Mode::Team {
        org = tenancy::primary_org(db, user).await.map_err(db_error)?;
    }
    if let Some(p) = project {
        // The token's org is the project's; the caller must be able to act there.
        let owner: Option<Uuid> =
            sqlx::query_scalar("SELECT org_id FROM projects WHERE id = $1 AND deleted_at IS NULL")
                .bind(p.as_uuid())
                .fetch_optional(db.pool())
                .await
                .map_err(|e| db_error(e.into()))?;
        let Some(project_org) = owner.map(OrgId::from_uuid) else {
            return Err(
                ApiError::new(ErrorCode::NotFound, "No project with this id exists.").into(),
            );
        };
        if org.is_some_and(|o| o != project_org) && ctx.principal.kind == PrincipalKind::Service {
            return Err(
                ApiError::new(ErrorCode::NotFound, "No project with this id exists.").into(),
            );
        }
        org = Some(project_org);
        let on_project = Resource {
            org: Some(project_org.to_string()),
            project: Some(p.to_string()),
            ..Resource::kind("project")
        };
        state
            .authorize(&ctx, "auth.manage_tokens", &on_project)
            .await?;
    } else {
        state
            .authorize(&ctx, "auth.manage_tokens", &Resource::kind("account"))
            .await?;
    }
    let pepper = shared.pepper(&state).await?;
    let token_req = credentials::TokenRequest {
        user,
        org,
        project,
        name: name.to_owned(),
        scopes: req.scopes.clone(),
        days,
        not_after,
    };
    let at = state.seams.clock.now();
    let mut tx = db.begin().await.map_err(db_error)?;
    let issued = credentials::issue_token(&mut tx, &pepper, &token_req).await?;
    let event = audit::event(
        at,
        &ctx.principal.id,
        ctx.org.as_deref(),
        "auth.manage_tokens",
        &issued.id.to_string(),
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    let details = serde_json::json!({ "step": "token_created", "scopes": token_req.scopes });
    audit::write(&mut tx, &event, &details)
        .await
        .map_err(db_error)?;
    commit(tx).await?;
    Ok((
        StatusCode::CREATED,
        Json(NewToken {
            id: issued.id.to_string(),
            token: issued.token.expose().to_owned(),
            scopes: req.scopes,
            expires_at: issued.expires_at,
        }),
    ))
}

#[utoipa::path(delete, path = "/auth/tokens/{id}", tag = "auth", params(("id" = String, Path, description = "`tok_…`")),
    responses((status = 204, description = "Revoked."), (status = 404, body = crate::error::Problem)))]
async fn revoke_token(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    ctx: RequestContext,
    Path(id): Path<String>,
) -> Result<StatusCode, Fail> {
    let missing = || ApiError::new(ErrorCode::NotFound, "No token with this id exists.");
    let token = TokenId::from_str(&id).map_err(|_| missing())?;
    let user = acting(&shared, &ctx).await?;
    // Self-revocation works even if the account lost its role or the token has no auth scope.
    let (bound_org, bound_project) = if ctx.principal.kind == PrincipalKind::Service
        && ctx.principal.id == id
    {
        (None, None)
    } else {
        let (bound_org, bound_project) = token_bounds(&shared, &ctx).await?;
        let target: Option<(Option<uuid::Uuid>, Option<uuid::Uuid>)> = sqlx::query_as(
            "SELECT org_id, project_id FROM api_tokens WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL",
        )
        .bind(token.as_uuid())
        .bind(user.as_uuid())
        .fetch_optional(shared.db.pool())
        .await
        .map_err(|e| db_error(e.into()))?;
        let Some((org, project)) = target else {
            state
                .authorize(
                    &ctx,
                    "auth.manage_tokens",
                    &token_resource(bound_org, bound_project),
                )
                .await?;
            return Err(missing().into());
        };
        let org = org.map(OrgId::from_uuid);
        let project = project.map(ProjectId::from_uuid);
        if bound_org.is_some_and(|id| org != Some(id))
            || bound_project.is_some_and(|id| project != Some(id))
        {
            return Err(missing().into());
        }
        state
            .authorize(&ctx, "auth.manage_tokens", &token_resource(org, project))
            .await?;
        (bound_org, bound_project)
    };
    let at = state.seams.clock.now();
    let mut tx = shared.db.begin().await.map_err(db_error)?;
    let n = sqlx::query(
        "UPDATE api_tokens SET revoked_at = now() WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL \
           AND ($3::uuid IS NULL OR org_id = $3) AND ($4::uuid IS NULL OR project_id = $4)",
    )
        .bind(token.as_uuid())
        .bind(user.as_uuid())
        .bind(bound_org.map(|o| o.as_uuid()))
        .bind(bound_project.map(|p| p.as_uuid()))
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e.into()))?
        .rows_affected();
    if n != 1 {
        return Err(missing().into());
    }
    let event = audit::event(
        at,
        &ctx.principal.id,
        ctx.org.as_deref(),
        "auth.manage_tokens",
        &id,
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    audit::write(
        &mut tx,
        &event,
        &serde_json::json!({ "step": "token_revoked" }),
    )
    .await
    .map_err(db_error)?;
    commit(tx).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supabase_url_is_checked_parsed_and_never_echoed() {
        for ok in [
            "https://abcd.supabase.co",
            "http://127.0.0.1:54321",
            "http://localhost:54321",
            "http://[::1]:54321",
        ] {
            assert!(supabase_url_ok(ok), "{ok}");
        }
        for bad in [
            "http://127.0.0.1:@evil.example",
            "http://localhost:x@evil.example:80",
            "http://evil.example",
            "http://127.0.0.1",
            "https://user:hunter2@abcd.supabase.co",
            "https://abcd.supabase.co/?apikey=1",
            "https://abcd.supabase.co/#x",
            "ftp://abcd.supabase.co",
            "abcd.supabase.co",
        ] {
            assert!(!supabase_url_ok(bad), "{bad}");
        }
        let config = AuthConfig {
            supabase_url: Some("https://user:hunter2@abcd.supabase.co".into()),
            supabase_anon_key: Some("k".into()),
            ..AuthConfig::default()
        };
        let problems = config.problems();
        assert_eq!(problems.len(), 1);
        assert!(!problems[0].contains("hunter2"), "{}", problems[0]);
    }
}
