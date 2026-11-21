//! Setup sync (Doc 22 §5): this install's setup (`setup-profile/v1`: machines, preferences,
//! team memberships, UI settings; never secrets) encrypted on the device and kept as one row
//! per user in Supabase, protected by row-level security.
//!
//! The key comes from Argon2id over a sync passphrase; its hex form is the recovery key,
//! shown once. The row keeps the 5 newest copies; the newest push wins.
//!
//! Endpoints (under `/api/v1`): `GET|PUT /sync/profile`, `POST /sync/setup`,
//! `POST /sync/push`, `POST /sync/restore`, `GET /sync/status`.

pub mod crypto;

use crate::app::AppState;
use crate::config::Mode;
use crate::db::Db;
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::modules::audit;
use crate::modules::auth::problem::{Coded, Fail};
use crate::modules::auth::signin::{Access, SignIn, SignInState};
use crate::modules::auth::supabase::{self, Supabase, SupabaseError};
use crate::modules::auth::{commit, db_error, AuthConfig};
use crate::modules::tenancy;
use crate::seams::{AuditOutcome, RequestContext, Resource, Secret, SecretStore};
use crate::ServerModule;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use crypto::KdfParams;
use mrl_domain::ids::UserId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;
use time::OffsetDateTime;
use utoipa::{OpenApi, ToSchema};

/// The profile format.
pub const PROFILE_SCHEMA: &str = "setup-profile/v1";
/// The envelope format of one encrypted copy.
pub const ENVELOPE_FORMAT: &str = "mrl-sync/v1";
/// `SecretStore` name prefix of sync keys (`sync.key.<id>`; bare `sync.key` before 16005).
pub const KEY_SECRET: &str = "sync.key";
/// Supabase table (see the P16 handoff for its schema).
pub const TABLE: &str = "setup_profiles";
/// Copies kept remotely.
pub const KEEP: usize = 5;
/// Code: the passphrase or recovery key does not open the copy.
pub const WRONG_PASSPHRASE: &str = "MRL-VAL-160";
/// Code: setup sync is not set up on this install.
pub const NOT_SET_UP: &str = "MRL-STATE-160";
/// Code: setup sync is personal, so a team (shared) core refuses it.
pub const ON_TEAM_CORE: &str = "MRL-STATE-161";
/// Code: there is no saved copy to restore.
pub const NOTHING_SAVED: &str = "MRL-RES-160";
/// Shortest passphrase.
pub const MIN_PASSPHRASE: usize = 12;

/// How the app reaches a machine. Only these typed settings: keys and passwords stay in the
/// keychain, named by `key_ref`; free-form options are refused (unknown fields).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Transport {
    /// `lan`, `ssh`, `tailscale` or `cloudflare_tunnel`.
    pub kind: String,
    /// Port, when not the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Login name (SSH).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Name of the keychain secret that holds the key (never the key itself).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_ref: Option<String>,
}

/// One connected machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Machine {
    /// Name shown in the app.
    pub name: String,
    /// Host name or address.
    pub address: String,
    /// How to reach it.
    pub transport: Transport,
}

/// A team this person belongs to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TeamMembership {
    /// Team name.
    pub name: String,
    /// Address of the team core.
    pub address: String,
    /// The person's role there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

/// `setup-profile/v1`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SetupProfile {
    /// Always `setup-profile/v1`.
    pub schema: String,
    /// Connected machines.
    #[serde(default)]
    pub machines: Vec<Machine>,
    /// App preferences.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub preferences: Map<String, Value>,
    /// Teams.
    #[serde(default)]
    pub team_memberships: Vec<TeamMembership>,
    /// UI settings.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub ui: Map<String, Value>,
}

impl Default for SetupProfile {
    fn default() -> Self {
        Self {
            schema: PROFILE_SCHEMA.to_owned(),
            machines: Vec::new(),
            preferences: Map::new(),
            team_memberships: Vec::new(),
            ui: Map::new(),
        }
    }
}

const SECRET_WORDS: [&str; 13] = [
    "authorization",
    "cookie",
    "credential",
    "bearer",
    "password",
    "passwd",
    "secret",
    "token",
    "private_key",
    "privatekey",
    "api_key",
    "apikey",
    "passphrase",
];
const SECRET_PREFIXES: [&str; 6] = [
    "-----BEGIN",
    "mrl_pat_",
    "hf_",
    "sk-",
    "ghp_",
    "github_pat_",
];

/// True for text that holds something shaped like a credential, anywhere in it: a word with
/// a known token prefix (`hf_…`, `mrl_pat_…`), a PEM block, an `Authorization` value
/// (`Bearer …`, `Basic …`), or an address with a user name or password in it.
fn secret_shaped(s: &str) -> bool {
    let t = s.trim_start();
    let lower = t.to_ascii_lowercase();
    let token_word = t
        .split(|c: char| !(c.is_ascii_alphanumeric() || "_-.".contains(c)))
        // Real tokens run long after the prefix; names like `hf_token_ref` do not.
        .any(|w| {
            SECRET_PREFIXES
                .iter()
                .any(|p| w.starts_with(p) && w.len() >= p.len() + 12)
        });
    let words: Vec<&str> = lower
        .split(|c: char| c.is_ascii_whitespace() || matches!(c, ':' | '\'' | '"'))
        .filter(|w| !w.is_empty())
        .collect();
    let authorization = words
        .windows(2)
        .any(|w| matches!(w[0], "bearer" | "basic" | "token" | "digest") && w[1].len() >= 8);
    token_word
        || t.contains("-----BEGIN")
        || authorization
        || ["bearer ", "basic ", "token ", "digest "]
            .iter()
            .any(|p| lower.starts_with(p))
        || t.split_whitespace().any(|w| {
            reqwest::Url::parse(w).is_ok_and(|u| !u.username().is_empty() || u.password().is_some())
        })
        || (t.contains('@') && t.contains(':') && t.contains("//"))
}

/// A JSON pointer segment for key `k` that never repeats a credential: keys shaped like one
/// (or unusual ones) show as `*`.
fn safe_segment(k: &str) -> String {
    let plain = (1..=64).contains(&k.len())
        && k.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
        && !secret_shaped(k);
    if plain {
        k.to_owned()
    } else {
        "*".to_owned()
    }
}

/// The JSON pointer of the first thing in `v` that looks like a secret: a key named like one
/// (`password`, `api_key`, …) or a value shaped like one (a PEM key, `hf_…`, `mrl_pat_…`).
pub fn find_secret(v: &Value, path: &str) -> Option<String> {
    match v {
        Value::Object(m) => m.iter().find_map(|(k, v)| {
            let p = format!("{path}/{}", safe_segment(k));
            let lower = k.to_ascii_lowercase();
            let named = SECRET_WORDS.iter().any(|w| lower.contains(w)) && !lower.ends_with("_ref");
            // A key shaped like a credential is refused; so is any non-empty value under a
            // secret-like name, whatever its type.
            if secret_shaped(k) || (named && !v.is_null()) {
                Some(p)
            } else {
                find_secret(v, &p)
            }
        }),
        Value::Array(a) => a
            .iter()
            .enumerate()
            .find_map(|(i, v)| find_secret(v, &format!("{path}/{i}"))),
        Value::String(s) => secret_shaped(s).then(|| path.to_owned()),
        _ => None,
    }
}

fn check(profile: &SetupProfile) -> Result<(), ApiError> {
    let bad = |path: &str, message: &str, hint: &str| {
        ApiError::validation(vec![FieldError::new(
            path,
            ErrorCode::FieldValue,
            message,
            hint,
        )])
    };
    if profile.schema != PROFILE_SCHEMA {
        return Err(bad(
            "/schema",
            "schema must be setup-profile/v1",
            "Send \"schema\": \"setup-profile/v1\".",
        ));
    }
    let name_ok = |n: &str| (1..=100).contains(&n.trim().chars().count());
    let word_ok = |w: &str, max: usize| {
        (1..=max).contains(&w.len())
            && w.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    };
    for (i, m) in profile.machines.iter().enumerate() {
        let at = |f: &str| format!("/machines/{i}/{f}");
        if !name_ok(&m.name) {
            return Err(bad(
                &at("name"),
                "a machine name needs 1–100 characters",
                "Use a short name, like gpu-box.",
            ));
        }
        if !host_ok(&m.address) {
            return Err(bad(
                &at("address"),
                "the address must be a host name or IP address only, with no user name, password, port or path",
                "Put the port and login name in the transport settings, and the key in the keychain.",
            ));
        }
        if m.transport.user.as_deref().is_some_and(|u| !word_ok(u, 64)) {
            return Err(bad(
                &at("transport/user"),
                "the login name may use only letters, digits, '.', '_' and '-'",
                "Use the plain login name, like ada.",
            ));
        }
        if m.transport
            .key_ref
            .as_deref()
            .is_some_and(|r| !word_ok(r, 128))
        {
            return Err(bad(
                &at("transport/key_ref"),
                "key_ref must be a keychain name: letters, digits, '.', '_' and '-'",
                "Use the name of the keychain entry, like ssh.gpu-box.",
            ));
        }
        if !matches!(
            m.transport.kind.as_str(),
            "lan" | "ssh" | "tailscale" | "cloudflare_tunnel"
        ) {
            return Err(bad(
                &format!("/machines/{i}/transport/kind"),
                "transport kind must be lan, ssh, tailscale or cloudflare_tunnel",
                "Pick one of the four transports.",
            ));
        }
    }
    for (i, t) in profile.team_memberships.iter().enumerate() {
        if !name_ok(&t.name) {
            return Err(bad(
                &format!("/team_memberships/{i}/name"),
                "a team name needs 1–100 characters",
                "Use the team's name.",
            ));
        }
        if !team_address_ok(&t.address) {
            return Err(bad(
                &format!("/team_memberships/{i}/address"),
                "the team address must be an https:// address with no user name, password, path or query",
                "Use the address the team owner gave you, like https://lab.example:8443.",
            ));
        }
    }
    let value = serde_json::to_value(profile)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    if let Some(path) = find_secret(&value, "") {
        return Err(bad(
            &path,
            "this looks like a secret, and setup sync never stores secrets",
            "Keep the secret in the keychain and put its name in a `key_ref` field.",
        ));
    }
    Ok(())
}

/// A host name or IP address (IPv6 in brackets or bare), nothing else.
fn host_ok(a: &str) -> bool {
    let bare = a
        .strip_prefix('[')
        .and_then(|x| x.strip_suffix(']'))
        .unwrap_or(a);
    if bare.parse::<std::net::IpAddr>().is_ok() {
        return true;
    }
    (1..=253).contains(&a.len())
        && a.split('.').all(|label| {
            (1..=63).contains(&label.len())
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
}

/// `https://host[:port]` (or `http://` on loopback), with no credentials, path or query.
fn team_address_ok(a: &str) -> bool {
    let Ok(u) = reqwest::Url::parse(a) else {
        return false;
    };
    let loopback = matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
    (u.scheme() == "https" || (u.scheme() == "http" && loopback))
        && u.host_str().is_some()
        && u.username().is_empty()
        && u.password().is_none()
        && matches!(u.path(), "" | "/")
        && u.query().is_none()
        && u.fragment().is_none()
}

/// One encrypted copy as stored in Supabase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Copy {
    /// 1, 2, 3, … per user.
    pub version: i64,
    /// When it was pushed (RFC 3339).
    pub at: String,
    /// The envelope.
    pub blob: Envelope,
}

/// `mrl-sync/v1`: Argon2id settings, and nonce + ciphertext of the profile JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    /// Always `mrl-sync/v1`.
    pub format: String,
    /// How the key was made from the passphrase.
    pub kdf: KdfParams,
    /// Base64 of nonce, ciphertext and tag.
    pub sealed: String,
}

/// The ciphertext is bound to the format and the Supabase user, so a copy cannot be moved
/// to another account unnoticed.
fn aad(supabase_user: uuid::Uuid) -> Vec<u8> {
    format!("{ENVELOPE_FORMAT}:{supabase_user}").into_bytes()
}

#[derive(Debug, Deserialize)]
struct RemoteRow {
    #[serde(default)]
    copies: Vec<Copy>,
}

async fn remote_copies(sb: &Supabase, access: &Access) -> Result<Vec<Copy>, SupabaseError> {
    let rows: Vec<RemoteRow> = supabase::json(
        sb.request(reqwest::Method::GET, &format!("/rest/v1/{TABLE}"))
            .bearer_auth(access.token.expose())
            .query(&[
                ("select", "copies".to_owned()),
                ("user_id", format!("eq.{}", access.user.id)),
            ]),
    )
    .await?;
    Ok(rows
        .into_iter()
        .next()
        .map(|r| r.copies)
        .unwrap_or_default())
}

/// Supabase SQL function that adds one copy atomically (see the P16 handoff).
pub const PUSH_FUNCTION: &str = "push_setup_copy";

/// What [`PUSH_FUNCTION`] answers.
#[derive(Debug, Deserialize)]
struct PushAnswer {
    version: i64,
    at: String,
    copies: usize,
}

/// Adds `blob` as the newest copy. Supabase allocates the version and trims to 5 under one
/// row lock, so two devices pushing at once both keep their copy with different versions.
async fn push_copy(
    sb: &Supabase,
    access: &Access,
    blob: &Envelope,
    op: uuid::Uuid,
) -> Result<PushAnswer, SupabaseError> {
    supabase::json(
        sb.request(
            reqwest::Method::POST,
            &format!("/rest/v1/rpc/{PUSH_FUNCTION}"),
        )
        .bearer_auth(access.token.expose())
        .json(&serde_json::json!({ "p_blob": blob, "p_op": op.to_string() })),
    )
    .await
}

/// A sync-service failure as a fixed message: the service's own text (which may repeat what
/// was sent) goes only to the log as its status and code.
fn remote_error(e: SupabaseError) -> Fail {
    let offline = || {
        Coded::new(
            crate::modules::auth::supabase::OFFLINE,
            StatusCode::SERVICE_UNAVAILABLE,
            "The sync service did not answer; try again in a moment.",
        )
        .into()
    };
    match e {
        SupabaseError::Offline(_) => offline(),
        SupabaseError::Refused {
            status: 401 | 403, ..
        } => Coded::new(
            crate::modules::auth::signin::EXPIRED,
            StatusCode::UNAUTHORIZED,
            "The sign-in service refused this session; sign in again.",
        )
        .into(),
        // A 5xx may have saved the copy before failing: the client retries with its op_id.
        SupabaseError::Refused { status, .. } if status >= 500 => offline(),
        SupabaseError::Refused { status, .. } => {
            tracing::warn!(status, "the sync service refused a request");
            ApiError::new(
                ErrorCode::Unavailable,
                format!("The sync service refused the request (HTTP {status})."),
            )
            .into()
        }
        _ => ApiError::new(
            ErrorCode::Unavailable,
            "The sync service sent an answer the app could not read.",
        )
        .into(),
    }
}

fn fail_text(f: Fail) -> String {
    match f {
        Fail::Api(e) => e.detail,
        Fail::Coded(c) => format!("{}: {}", c.code, c.detail),
    }
}

struct Shared {
    db: Db,
    supabase: Option<Supabase>,
}

/// Setup sync.
pub struct SyncModule {
    shared: Arc<Shared>,
}

impl SyncModule {
    /// The module for these settings.
    pub fn new(db: Db, config: &AuthConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                db,
                supabase: Supabase::from_config(config),
            }),
        }
    }
}

impl ServerModule for SyncModule {
    fn name(&self) -> &'static str {
        "sync"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/sync/profile", get(get_profile).put(put_profile))
            .route("/sync/push", post(push))
            .route("/sync/status", get(status))
            .layer(Extension(self.shared.clone()))
            .layer(axum::middleware::from_fn(
                crate::modules::auth::problem::render,
            ))
    }

    /// Setup and restore take the passphrase or recovery key: their handlers sign the caller
    /// in through `auth::Caller`, outside P07's idempotency layer.
    fn public_routes(&self) -> Router<AppState> {
        Router::new()
            .route("/sync/setup", post(setup))
            .route("/sync/restore", post(restore))
            .layer(Extension(self.shared.clone()))
            .layer(axum::middleware::from_fn(
                crate::modules::auth::problem::render,
            ))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        SyncDoc::openapi()
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(get_profile, put_profile, setup, push, restore, status),
    components(schemas(SetupProfile, Machine, Transport, TeamMembership, SetupRequest, SetupDone, PushRequest, Pushed, RestoreRequest, Restored, SyncStatus, CopyInfo)),
    tags((name = "sync", description = "Encrypted setup sync."))
)]
struct SyncDoc;

struct Caller {
    user: UserId,
    secrets: Arc<dyn SecretStore>,
}

async fn caller(state: &AppState, shared: &Shared, ctx: &RequestContext) -> Result<Caller, Fail> {
    // Setup sync is per device: the member's own (solo) core holds their sign-in and keys.
    // A shared team core refuses it before any key material is made.
    if state.config.server.mode == Mode::Team {
        return Err(Coded::new(
            ON_TEAM_CORE,
            StatusCode::CONFLICT,
            "Setup sync runs in the MasterRL app on your own computer, not on a team server.",
        )
        .into());
    }
    state
        .authorize(ctx, "auth.session", &Resource::kind(tenancy::SETUP_PROFILE))
        .await?;
    let user = tenancy::acting_user(&shared.db, &ctx.principal)
        .await
        .map_err(db_error)?
        .ok_or_else(|| {
            ApiError::new(
                ErrorCode::Unauthenticated,
                "This sign-in has no account behind it.",
            )
        })?;
    let secrets =
        state.seams.secrets.clone().ok_or_else(|| {
            ApiError::new(ErrorCode::Unavailable, "This server has no secret store.")
        })?;
    Ok(Caller { user, secrets })
}

fn sign_in(state: &AppState, shared: &Shared) -> SignIn {
    SignIn::new(
        shared.db.clone(),
        shared.supabase.clone(),
        state.seams.secrets.clone(),
    )
}

/// The stored sign-in, only when it is the caller's own account: the Supabase user linked to
/// the caller, or in solo mode the not-yet-linked local owner (restore links them). A team
/// core opened on a former solo keychain never lends that owner's session to anyone.
async fn own_access(state: &AppState, shared: &Shared, c: &Caller) -> Result<Access, Fail> {
    let access = sign_in(state, shared).access().await?;
    let row: Option<(Option<uuid::Uuid>, bool)> =
        sqlx::query_as("SELECT supabase_user_id, is_local_owner FROM users WHERE id = $1")
            .bind(c.user.as_uuid())
            .fetch_optional(shared.db.pool())
            .await
            .map_err(|e| db_error(e.into()))?;
    let solo = state.config.server.mode == Mode::Solo;
    let mine = match row {
        Some((Some(linked), _)) => linked == access.user.id,
        Some((None, owner)) => solo && owner,
        None => false,
    };
    if !mine {
        return Err(Coded::new(
            crate::modules::auth::signin::OTHER_ACCOUNT,
            StatusCode::CONFLICT,
            "The MasterRL account signed in on this computer is not yours, so setup sync cannot use it.",
        )
        .into());
    }
    Ok(access)
}

fn online(shared: &Shared) -> Result<&Supabase, Fail> {
    shared
        .supabase
        .as_ref()
        .ok_or_else(|| Fail::from(crate::modules::auth::signin::SignInError::NotConfigured))
}

/// A saved profile (the local copy or a decrypted backup). Revisions before the typed
/// transport wrote `transport.options` on every machine; those free-form settings are no
/// longer kept, so they are dropped here instead of failing the read. The API still refuses
/// them.
fn read_saved(mut v: Value) -> Result<SetupProfile, serde_json::Error> {
    if let Some(machines) = v.get_mut("machines").and_then(Value::as_array_mut) {
        for m in machines {
            if let Some(t) = m.get_mut("transport").and_then(Value::as_object_mut) {
                t.remove("options");
            }
        }
    }
    serde_json::from_value(v)
}

async fn load_profile(db: &Db, user: UserId) -> Result<SetupProfile, ApiError> {
    let raw: Option<Value> =
        sqlx::query_scalar("SELECT profile FROM setup_profiles WHERE user_id = $1")
            .bind(user.as_uuid())
            .fetch_optional(db.pool())
            .await
            .map_err(|e| db_error(e.into()))?;
    profile_from_row(raw)
}

fn profile_from_row(raw: Option<Value>) -> Result<SetupProfile, ApiError> {
    match raw {
        None => Ok(SetupProfile::default()),
        Some(v) => {
            let profile = read_saved(v).map_err(|_| {
                ApiError::new(
                    ErrorCode::Internal,
                    "The saved setup could not be read; save it again with PUT /sync/profile.",
                )
            })?;
            // A profile saved under older rules is checked again before it is shown or pushed.
            check(&profile)?;
            Ok(profile)
        }
    }
}

#[utoipa::path(get, path = "/sync/profile", tag = "sync", responses((status = 200, body = SetupProfile)))]
async fn get_profile(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    ctx: RequestContext,
) -> Result<Json<SetupProfile>, Fail> {
    let c = caller(&state, &shared, &ctx).await?;
    Ok(Json(load_profile(&shared.db, c.user).await?))
}

#[utoipa::path(put, path = "/sync/profile", tag = "sync", request_body = SetupProfile,
    responses((status = 200, body = SetupProfile), (status = 422, body = crate::error::Problem)))]
async fn put_profile(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    ctx: RequestContext,
    crate::modules::auth::problem::Body(profile): crate::modules::auth::problem::Body<SetupProfile>,
) -> Result<Json<SetupProfile>, Fail> {
    let c = caller(&state, &shared, &ctx).await?;
    check(&profile)?;
    let value = serde_json::to_value(&profile)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let event = audit::event(
        state.seams.clock.now(),
        &ctx.principal.id,
        ctx.org.as_deref(),
        "auth.session",
        &c.user.to_string(),
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    let mut tx = shared.db.begin().await.map_err(db_error)?;
    sqlx::query(
        "INSERT INTO setup_profiles (user_id, profile) VALUES ($1, $2) \
         ON CONFLICT (user_id) DO UPDATE SET profile = EXCLUDED.profile, updated_at = now()",
    )
    .bind(c.user.as_uuid())
    .bind(value)
    .execute(&mut *tx)
    .await
    .map_err(|e| db_error(e.into()))?;
    audit::write(
        &mut tx,
        &event,
        &serde_json::json!({ "step": "profile_saved" }),
    )
    .await
    .map_err(db_error)?;
    commit(tx).await?;
    Ok(Json(profile))
}

/// Turn on setup sync.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SetupRequest {
    /// At least 12 characters. A forgotten passphrase (and recovery key) means the setup cannot be restored.
    pub passphrase: String,
}

/// Setup sync is on.
#[derive(Debug, Serialize, ToSchema)]
pub struct SetupDone {
    /// Shown once. It opens your saved setup if you forget the passphrase.
    pub recovery_key: String,
}

async fn derive(kdf: KdfParams, passphrase: String) -> Result<[u8; crypto::KEY_LEN], ApiError> {
    tokio::task::spawn_blocking(move || kdf.derive(&passphrase))
        .await
        .map_err(|e| ApiError::new(ErrorCode::Internal, format!("key derivation stopped: {e}")))?
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))
}

#[utoipa::path(post, path = "/sync/setup", tag = "sync", request_body = SetupRequest,
    responses((status = 200, body = SetupDone), (status = 422, body = crate::error::Problem)))]
async fn setup(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    crate::modules::auth::Caller(ctx): crate::modules::auth::Caller,
    crate::modules::auth::problem::Body(req): crate::modules::auth::problem::Body<SetupRequest>,
) -> Result<Json<SetupDone>, Fail> {
    let c = caller(&state, &shared, &ctx).await?;
    if req.passphrase.chars().count() < MIN_PASSPHRASE {
        return Err(ApiError::validation(vec![FieldError::new(
            "/passphrase",
            ErrorCode::FieldValue,
            "the passphrase needs at least 12 characters",
            "Use a few unrelated words. Write it down somewhere safe.",
        )])
        .into());
    }
    let kdf =
        KdfParams::generate().map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let key = derive(kdf.clone(), req.passphrase).await?;
    let previous = active_key(&shared.db, c.user).await?.map(|(r, _)| r);
    let staged = stage_key(c.secrets.as_ref(), &key).await?;
    let kdf_json = serde_json::to_value(&kdf)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let event = audit::event(
        state.seams.clock.now(),
        &ctx.principal.id,
        ctx.org.as_deref(),
        "auth.session",
        &c.user.to_string(),
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    let committed = async {
        let mut tx = shared.db.begin().await.map_err(db_error)?;
        sqlx::query(
            "INSERT INTO sync_state (user_id, kdf, key_ref) VALUES ($1, $2, $3) \
             ON CONFLICT (user_id) DO UPDATE SET kdf = EXCLUDED.kdf, key_ref = EXCLUDED.key_ref, set_up_at = now()",
        )
        .bind(c.user.as_uuid())
        .bind(&kdf_json)
        .bind(&staged)
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error(e.into()))?;
        audit::write(&mut tx, &event, &serde_json::json!({ "step": "sync_set_up" }))
            .await
            .map_err(db_error)?;
        commit(tx).await
    }
    .await;
    settle_key(
        &shared.db,
        c.secrets.as_ref(),
        c.user,
        committed,
        &staged,
        previous,
    )
    .await?;
    Ok(Json(SetupDone {
        recovery_key: crypto::recovery_key(&key),
    }))
}

/// A copy was saved.
#[derive(Debug, Serialize, ToSchema)]
pub struct Pushed {
    /// Its version.
    pub version: i64,
    /// When.
    pub at: String,
    /// Copies kept now (at most 5).
    pub copies: usize,
}

fn not_set_up() -> Fail {
    Coded::new(
        NOT_SET_UP,
        StatusCode::CONFLICT,
        "Setup sync is not turned on yet; run `mrl sync setup` first.",
    )
    .into()
}

/// The keychain name of the active sync key and its KDF settings, if sync is set up.
async fn active_key(db: &Db, user: UserId) -> Result<Option<(String, Value)>, ApiError> {
    let row: Option<(Option<String>, Value)> =
        sqlx::query_as("SELECT key_ref, kdf FROM sync_state WHERE user_id = $1")
            .bind(user.as_uuid())
            .fetch_optional(db.pool())
            .await
            .map_err(|e| db_error(e.into()))?;
    Ok(row.map(|(r, kdf)| (r.unwrap_or_else(|| KEY_SECRET.to_owned()), kdf)))
}

/// Saves `key` under a fresh keychain name. It is not used until a `sync_state` row names it.
async fn stage_key(
    secrets: &dyn SecretStore,
    key: &[u8; crypto::KEY_LEN],
) -> Result<String, ApiError> {
    let name = format!("{KEY_SECRET}.{}", uuid::Uuid::now_v7().simple());
    secrets.set(&name, Secret::new(crypto::to_hex(key))).await?;
    Ok(name)
}

/// After the transaction that names `staged`: on success the previous key goes, on failure
/// the staged one does, so the active key always matches the stored KDF settings.
/// Finishes a key change after its transaction: on success the previous key goes; on a
/// failure the staged key goes, but only once the database confirms it is not the active
/// one. A commit can succeed even when its reply is lost, so an error alone proves nothing;
/// when the database cannot be asked, the staged key is kept (an unused key is harmless, a
/// deleted active key loses sync).
pub async fn settle_key(
    db: &Db,
    secrets: &dyn SecretStore,
    user: UserId,
    committed: Result<(), ApiError>,
    staged: &str,
    previous: Option<String>,
) -> Result<(), ApiError> {
    let drop_quietly = |name: String| async move {
        if let Err(e) = secrets.delete(&name).await {
            tracing::warn!(error = %e, "could not delete an unused sync key from the keychain");
        }
    };
    let drop_previous = |previous: Option<String>| async move {
        if let Some(old) = previous.filter(|o| o != staged) {
            drop_quietly(old).await;
        }
    };
    let Err(e) = committed else {
        drop_previous(previous).await;
        return Ok(());
    };
    match active_key(db, user).await {
        Ok(Some((active, _))) if active == staged => {
            tracing::info!("the key change was saved although its reply was lost");
            drop_previous(previous).await;
            Ok(())
        }
        Ok(_) => {
            drop_quietly(staged.to_owned()).await;
            Err(e)
        }
        Err(_) => Err(e),
    }
}

/// Get the key reference, KDF and profile from one database snapshot. If a later
/// rotation removed that key before we could load it, repeat the whole read.
async fn current_setup(
    db: &Db,
    secrets: &dyn SecretStore,
    user: UserId,
) -> Result<Option<([u8; crypto::KEY_LEN], Value, Option<Value>)>, ApiError> {
    loop {
        let row: Option<(Option<String>, Value, Option<Value>)> = sqlx::query_as(
            "SELECT s.key_ref, s.kdf, p.profile FROM sync_state s \
             LEFT JOIN setup_profiles p ON p.user_id = s.user_id WHERE s.user_id = $1",
        )
        .bind(user.as_uuid())
        .fetch_optional(db.pool())
        .await
        .map_err(|e| db_error(e.into()))?;
        let Some((key_ref, kdf, profile)) = row else {
            return Ok(None);
        };
        let name = key_ref.as_deref().unwrap_or(KEY_SECRET);
        if let Some(key) = secrets
            .get(name)
            .await?
            .and_then(|k| crypto::key_from_hex(k.expose()))
        {
            return Ok(Some((key, kdf, profile)));
        }
        if active_key(db, user)
            .await?
            .is_some_and(|(active, _)| active == name)
        {
            return Ok(None);
        }
    }
}

/// `POST /sync/push` body (optional).
#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PushRequest {
    /// A UUID the client keeps for one push and sends again on every retry. The saved
    /// history gains one copy per operation, however often it is retried.
    #[serde(default)]
    pub op_id: Option<String>,
}

#[utoipa::path(post, path = "/sync/push", tag = "sync", request_body(content = Option<PushRequest>, description = "Optional: `{\"op_id\": \"<uuid>\"}`, the same on retries."),
    responses((status = 200, body = Pushed), (status = 401, body = crate::error::Problem), (status = 503, body = crate::error::Problem)))]
async fn push(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    ctx: RequestContext,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Pushed>, Fail> {
    let c = caller(&state, &shared, &ctx).await?;
    let req: PushRequest = if body.iter().all(u8::is_ascii_whitespace) {
        PushRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|_| {
            Fail::from(ApiError::validation(vec![FieldError::new(
                "/op_id",
                ErrorCode::FieldValue,
                "the body may hold only op_id, a UUID",
                "Send {} or {\"op_id\": \"<uuid>\"}.",
            )]))
        })?
    };
    let op = match req.op_id.as_deref() {
        Some(o) => uuid::Uuid::parse_str(o).map_err(|_| {
            Fail::from(ApiError::validation(vec![FieldError::new(
                "/op_id",
                ErrorCode::FieldValue,
                "op_id must be a UUID",
                "Make one UUID per push and send it again on retries.",
            )]))
        })?,
        None => headers
            .get(crate::http::idempotency::IDEMPOTENCY_KEY)
            .map_or_else(uuid::Uuid::now_v7, |key| {
                use sha2::{Digest, Sha256};
                let hash = Sha256::new()
                    .chain_update(b"mrl-sync-push/v1:")
                    .chain_update(ctx.principal.id.as_bytes())
                    .chain_update([0])
                    .chain_update(key.as_bytes())
                    .finalize();
                let mut bytes = [0; 16];
                bytes.copy_from_slice(&hash[..16]);
                uuid::Builder::from_custom_bytes(bytes).into_uuid()
            }),
    };
    let sb = online(&shared)?;
    let Some((key, kdf, profile)) = current_setup(&shared.db, c.secrets.as_ref(), c.user).await?
    else {
        return Err(not_set_up());
    };
    let kdf: KdfParams = serde_json::from_value(kdf)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let profile = profile_from_row(profile)?;
    let access = own_access(&state, &shared, &c).await?;
    let plain = serde_json::to_vec(&profile)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let sealed = crypto::seal(&key, &aad(access.user.id), &plain)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let blob = Envelope {
        format: ENVELOPE_FORMAT.to_owned(),
        kdf,
        sealed: STANDARD.encode(sealed),
    };
    let pushed = push_copy(sb, &access, &blob, op)
        .await
        .map_err(|error| match error {
            SupabaseError::BadAnswer(_) => {
                remote_error(SupabaseError::Offline("unconfirmed push".into()))
            }
            other => remote_error(other),
        })?;
    let (version, now) = (pushed.version, state.seams.clock.now());
    let event = audit::event(
        now,
        &ctx.principal.id,
        ctx.org.as_deref(),
        "auth.session",
        &c.user.to_string(),
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    let mut tx = shared.db.begin().await.map_err(db_error)?;
    sqlx::query(
        // Overlapping pushes may answer out of order: never move back to an older copy.
        "UPDATE sync_state SET last_push_at = GREATEST(last_push_at, now()), \
           last_pushed_version = GREATEST(last_pushed_version, $2) WHERE user_id = $1",
    )
    .bind(c.user.as_uuid())
    .bind(version)
    .execute(&mut *tx)
    .await
    .map_err(|e| db_error(e.into()))?;
    audit::write(
        &mut tx,
        &event,
        &serde_json::json!({ "step": "sync_pushed", "version": version }),
    )
    .await
    .map_err(db_error)?;
    commit(tx).await?;
    Ok(Json(Pushed {
        version,
        at: pushed.at,
        copies: pushed.copies,
    }))
}

/// Restore a saved setup. Give the passphrase or the recovery key.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RestoreRequest {
    /// The sync passphrase.
    #[serde(default)]
    pub passphrase: Option<String>,
    /// The recovery key shown at setup.
    #[serde(default)]
    pub recovery_key: Option<String>,
    /// An older copy; the newest when absent.
    #[serde(default)]
    pub version: Option<i64>,
}

/// The setup was restored on this install.
#[derive(Debug, Serialize, ToSchema)]
pub struct Restored {
    /// The copy used.
    pub version: i64,
    /// When it was pushed.
    pub at: String,
    /// The setup now on this install.
    pub profile: SetupProfile,
}

fn wrong_key() -> Fail {
    Coded::field(
        WRONG_PASSPHRASE,
        "/passphrase",
        "This passphrase or recovery key does not open the saved setup.",
        "Check for typing mistakes, or use the recovery key you saved at setup.",
    )
    .into()
}

#[utoipa::path(post, path = "/sync/restore", tag = "sync", request_body = RestoreRequest,
    responses((status = 200, body = Restored), (status = 404, body = crate::error::Problem), (status = 422, body = crate::error::Problem)))]
async fn restore(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    crate::modules::auth::Caller(ctx): crate::modules::auth::Caller,
    crate::modules::auth::problem::Body(req): crate::modules::auth::problem::Body<RestoreRequest>,
) -> Result<Json<Restored>, Fail> {
    let c = caller(&state, &shared, &ctx).await?;
    let sb = online(&shared)?;
    let access = own_access(&state, &shared, &c).await?;
    let copies = remote_copies(sb, &access).await.map_err(remote_error)?;
    let copy = match req.version {
        Some(v) => copies.into_iter().find(|c| c.version == v),
        None => copies.into_iter().next(),
    }
    .ok_or_else(|| {
        Coded::new(
            NOTHING_SAVED,
            StatusCode::NOT_FOUND,
            format!(
                "There is no saved setup{} for this account.",
                req.version
                    .map(|v| format!(" with version {v}"))
                    .unwrap_or_default()
            ),
        )
    })?;
    if copy.blob.format != ENVELOPE_FORMAT {
        return Err(ApiError::new(
            ErrorCode::FieldValue,
            "This saved setup was made by a newer version of the app.",
        )
        .into());
    }
    let key = match (&req.recovery_key, req.passphrase) {
        (Some(r), _) => crypto::key_from_hex(r).ok_or_else(wrong_key)?,
        (None, Some(p)) => derive(copy.blob.kdf.clone(), p).await?,
        (None, None) => {
            return Err(ApiError::validation(vec![FieldError::new(
                "/passphrase",
                ErrorCode::FieldMissing,
                "give the sync passphrase or the recovery key",
                "Send passphrase or recovery_key.",
            )])
            .into())
        }
    };
    let sealed = STANDARD
        .decode(&copy.blob.sealed)
        .map_err(|_| wrong_key())?;
    let plain = crypto::open(&key, &aad(access.user.id), &sealed).map_err(|_| wrong_key())?;
    let profile = serde_json::from_slice(&plain)
        .and_then(read_saved)
        .map_err(|_| {
            ApiError::new(
                ErrorCode::FieldValue,
                "The saved copy could not be read as a setup profile.",
            )
        })?;
    check(&profile)?;
    let previous = active_key(&shared.db, c.user).await?.map(|(r, _)| r);
    let staged = stage_key(c.secrets.as_ref(), &key).await?;
    let (profile_json, kdf_json) = (
        serde_json::to_value(&profile)
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?,
        serde_json::to_value(&copy.blob.kdf)
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?,
    );
    let link = access.user.profile();
    let solo = state.config.server.mode == Mode::Solo;
    let event = audit::event(
        state.seams.clock.now(),
        &ctx.principal.id,
        ctx.org.as_deref(),
        "auth.session",
        &c.user.to_string(),
        AuditOutcome::Success,
        &ctx.trace_id,
    );
    let version = copy.version;
    let committed = async {
        let mut tx = shared.db.begin().await.map_err(db_error)?;
        let q = |e: sqlx::Error| db_error(e.into());
        // After a wiped data folder the local owner is new: link it again.
        if solo {
            sqlx::query(
                "UPDATE users SET supabase_user_id = $2 WHERE id = $1 AND supabase_user_id IS NULL",
            )
            .bind(c.user.as_uuid())
            .bind(link.supabase_user_id)
            .execute(&mut *tx)
            .await
            .map_err(q)?;
        }
        sqlx::query(
            "INSERT INTO setup_profiles (user_id, profile) VALUES ($1, $2) \
             ON CONFLICT (user_id) DO UPDATE SET profile = EXCLUDED.profile, updated_at = now()",
        )
        .bind(c.user.as_uuid())
        .bind(&profile_json)
        .execute(&mut *tx)
        .await
        .map_err(q)?;
        sqlx::query(
            "INSERT INTO sync_state (user_id, kdf, key_ref, last_restore_at) VALUES ($1, $2, $3, now()) \
             ON CONFLICT (user_id) DO UPDATE SET kdf = EXCLUDED.kdf, key_ref = EXCLUDED.key_ref, last_restore_at = now()",
        )
        .bind(c.user.as_uuid())
        .bind(&kdf_json)
        .bind(&staged)
        .execute(&mut *tx)
        .await
        .map_err(q)?;
        audit::write(
            &mut tx,
            &event,
            &serde_json::json!({ "step": "sync_restored", "version": version }),
        )
        .await
        .map_err(db_error)?;
        commit(tx).await
    }
    .await;
    settle_key(
        &shared.db,
        c.secrets.as_ref(),
        c.user,
        committed,
        &staged,
        previous,
    )
    .await?;
    Ok(Json(Restored {
        version: copy.version,
        at: copy.at,
        profile,
    }))
}

/// A saved copy (metadata only).
#[derive(Debug, Serialize, ToSchema)]
pub struct CopyInfo {
    /// Version.
    pub version: i64,
    /// When it was pushed.
    pub at: String,
}

/// Where setup sync stands.
#[derive(Debug, Serialize, ToSchema)]
pub struct SyncStatus {
    /// A sync passphrase is set on this install.
    pub set_up: bool,
    /// This install's sign-in.
    pub sign_in: SignInState,
    /// Last push.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub last_push_at: Option<OffsetDateTime>,
    /// Version of the last push.
    pub last_pushed_version: Option<i64>,
    /// Last restore.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub last_restore_at: Option<OffsetDateTime>,
    /// Saved copies, newest first (when signed in and online).
    pub copies: Option<Vec<CopyInfo>>,
    /// Why `copies` is missing.
    pub copies_unavailable: Option<String>,
}

#[utoipa::path(get, path = "/sync/status", tag = "sync", responses((status = 200, body = SyncStatus)))]
async fn status(
    State(state): State<AppState>,
    Extension(shared): Extension<Arc<Shared>>,
    ctx: RequestContext,
) -> Result<Json<SyncStatus>, Fail> {
    let c = caller(&state, &shared, &ctx).await?;
    let row: Option<(Option<OffsetDateTime>, Option<i64>, Option<OffsetDateTime>)> =
        sqlx::query_as("SELECT last_push_at, last_pushed_version, last_restore_at FROM sync_state WHERE user_id = $1")
            .bind(c.user.as_uuid())
            .fetch_optional(shared.db.pool())
            .await
            .map_err(|e| db_error(e.into()))?;
    let set_up = current_setup(&shared.db, c.secrets.as_ref(), c.user)
        .await?
        .is_some();
    let si = sign_in(&state, &shared);
    let sign_in_state = si.state().await?;
    let (copies, copies_unavailable) = match (&shared.supabase, sign_in_state) {
        (Some(sb), SignInState::Active) => match own_access(&state, &shared, &c).await {
            Ok(access) => match remote_copies(sb, &access).await {
                Ok(cs) => (
                    Some(
                        cs.into_iter()
                            .map(|c| CopyInfo {
                                version: c.version,
                                at: c.at,
                            })
                            .collect(),
                    ),
                    None,
                ),
                Err(e) => (None, Some(fail_text(remote_error(e)))),
            },
            Err(e) => (None, Some(fail_text(e))),
        },
        _ => (None, Some("Sign in to see your saved copies.".to_owned())),
    };
    let (last_push_at, last_pushed_version, last_restore_at) = row.unwrap_or_default();
    Ok(Json(SyncStatus {
        set_up,
        sign_in: si.state().await?,
        last_push_at,
        last_pushed_version,
        last_restore_at,
        copies,
        copies_unavailable,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_spotted_by_name_and_shape() {
        let v = serde_json::json!({
            "machines": [{"name": "gpu", "transport": {"key_ref": "ssh.gpu"}}],
            "preferences": {"theme": "dark", "hf_token_ref": "hf"}
        });
        assert_eq!(find_secret(&v, ""), None);
        for (bad, path) in [
            (
                serde_json::json!({"preferences": {"hf_token": "x"}}),
                "/preferences/hf_token",
            ),
            (serde_json::json!({"ui": {"Password": "x"}}), "/ui/Password"),
            (
                serde_json::json!({"ui": {"note": "-----BEGIN OPENSSH PRIVATE KEY-----"}}),
                "/ui/note",
            ),
            (
                serde_json::json!({"m": [{"o": "mrl_pat_abcdefghijklmnop"}]}),
                "/m/0/o",
            ),
            (
                serde_json::json!({"ui": {"header": "Bearer abc.def"}}),
                "/ui/header",
            ),
            (
                serde_json::json!({"ui": {"h": "basic dXNlcjpwYXNz"}}),
                "/ui/h",
            ),
            (
                serde_json::json!({"ui": {"proxy": "http://u:p@proxy:8080"}}),
                "/ui/proxy",
            ),
            (
                serde_json::json!({"ui": {"Authorization": "x"}}),
                "/ui/Authorization",
            ),
            (
                serde_json::json!({"ui": {"password": ["x"]}}),
                "/ui/password",
            ),
            (
                serde_json::json!({"preferences": {"note": "my hub token is hf_AbCdEf0123456789xyzQW, keep it"}}),
                "/preferences/note",
            ),
            (
                serde_json::json!({"ui": {"cmd": format!("curl -H 'Authorization: {} {}' x", "Bearer", "e".repeat(12))}}),
                "/ui/cmd",
            ),
            (
                serde_json::json!({"ui": {"cmd": format!("example {} {} argument", "Basic", "e".repeat(12))}}),
                "/ui/cmd",
            ),
            (
                serde_json::json!({"ui": {"hf_AbCdEf0123456789xyzQW": true}}),
                "/ui/*",
            ),
        ] {
            assert_eq!(find_secret(&bad, "").as_deref(), Some(path));
        }
    }

    #[test]
    fn addresses_carry_no_credentials() {
        for ok in [
            "gpu-box",
            "10.0.0.7",
            "laptop.tail1234.ts.net",
            "[::1]",
            "fe80::1",
        ] {
            assert!(host_ok(ok), "{ok}");
        }
        for bad in [
            "ada:pw@host",
            "ada@host",
            "host:22",
            "host/path",
            "http://host",
            "",
            "-x.y",
            "a b",
        ] {
            assert!(!host_ok(bad), "{bad}");
        }
        assert!(team_address_ok("https://lab.example:8443"));
        assert!(team_address_ok("http://127.0.0.1:8443"));
        for bad in [
            "https://u:p@lab.example",
            "https://u@lab.example",
            "http://lab.example",
            "https://lab.example/x?t=1",
            "lab.example",
        ] {
            assert!(!team_address_ok(bad), "{bad}");
        }
    }

    #[test]
    fn profile_checks() {
        assert!(check(&SetupProfile::default()).is_ok());
        assert!(check(&SetupProfile {
            schema: "other".into(),
            ..SetupProfile::default()
        })
        .is_err());
        let mut p = SetupProfile::default();
        p.machines.push(Machine {
            name: "box".into(),
            address: "10.0.0.2".into(),
            transport: Transport {
                kind: "carrier-pigeon".into(),
                port: None,
                user: None,
                key_ref: None,
            },
        });
        assert!(check(&p).is_err());
        let unknown: Result<SetupProfile, _> =
            serde_json::from_value(serde_json::json!({"schema": PROFILE_SCHEMA, "ssh_key": "x"}));
        assert!(unknown.is_err(), "unknown fields are refused");
    }

    #[test]
    fn backups_from_the_revision_with_transport_options_still_open() {
        // What 10766ef's serializer wrote: `options` on every machine, empty or not.
        let legacy = serde_json::json!({
            "schema": PROFILE_SCHEMA,
            "machines": [
                {"name": "gpu-box", "address": "10.0.0.7", "transport": {"kind": "ssh", "user": "ada", "key_ref": "ssh.gpu-box", "options": {}}},
                {"name": "laptop", "address": "laptop.tail1234.ts.net", "transport": {"kind": "tailscale", "options": {"exit_node": "lab"}}}
            ],
            "preferences": {}, "team_memberships": [], "ui": {}
        });
        let key = [7u8; crypto::KEY_LEN];
        let user = uuid::Uuid::now_v7();
        let sealed = crypto::seal(&key, &aad(user), &serde_json::to_vec(&legacy).unwrap()).unwrap();
        let plain = crypto::open(&key, &aad(user), &sealed).unwrap();
        let profile = serde_json::from_slice(&plain).and_then(read_saved).unwrap();
        check(&profile).unwrap();
        assert_eq!(profile.machines.len(), 2);
        assert_eq!(profile.machines[0].transport.user.as_deref(), Some("ada"));
        let again = serde_json::to_value(&profile).unwrap();
        assert!(!again.to_string().contains("options"), "{again}");
        // The API itself still refuses free-form options.
        let posted: Result<SetupProfile, _> = serde_json::from_value(legacy);
        assert!(posted.is_err());
    }
}
