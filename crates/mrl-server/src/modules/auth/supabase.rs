//! Supabase Auth: PKCE sign-in, token refresh, and checking access tokens (JWTs) against the
//! project's signing keys (JWKS), cached in the database for offline use.

use super::AuthConfig;
use crate::config::Mode;
use crate::db::{Db, DbError};
use crate::modules::tenancy::{self, Profile};
use crate::seams::{IdentityProvider, Principal, SeamError};
use async_trait::async_trait;
use jsonwebtoken::jwk::Jwk;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

/// The audience Supabase puts in signed-in users' access tokens.
pub const AUDIENCE: &str = "authenticated";
/// Code: the sign-in service could not be reached.
pub const OFFLINE: &str = "MRL-LINK-160";
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// Keys are fetched again after this long, when online.
const JWKS_FRESH: time::Duration = time::Duration::minutes(10);
/// While offline, cached keys stay trusted this long after the last fetch.
const JWKS_GRACE: time::Duration = time::Duration::days(7);
/// At most one fetch for an unknown key id per this long.
const JWKS_RETRY: time::Duration = time::Duration::seconds(30);

/// A call to Supabase failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SupabaseError {
    /// No network, DNS, timeout or a 5xx.
    #[error("{OFFLINE}: the app could not reach the sign-in service ({0})")]
    Offline(String),
    /// Supabase answered no (bad code, revoked refresh token, …).
    #[error("the sign-in service refused the request ({status}): {message}")]
    Refused {
        /// HTTP status.
        status: u16,
        /// Supabase's reason.
        message: String,
    },
    /// The answer was not what Supabase sends.
    #[error("the sign-in service sent an answer the app could not read: {0}")]
    BadAnswer(String),
}

/// UUIDs as text in JSON (this build's `uuid` has no serde support).
mod uuid_text {
    use serde::Deserialize;
    use uuid::Uuid;

    pub fn serialize<S: serde::Serializer>(u: &Uuid, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(u)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        Uuid::parse_str(&text).map_err(serde::de::Error::custom)
    }
}

/// A Supabase user as the token endpoint reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, serde::Serialize)]
pub struct SupabaseUser {
    /// User id (the JWT `sub`).
    #[serde(with = "uuid_text")]
    pub id: Uuid,
    /// Email.
    #[serde(default)]
    pub email: Option<String>,
    /// Name and avatar from Google or GitHub.
    #[serde(default)]
    pub user_metadata: Value,
}

impl SupabaseUser {
    /// The person's profile for the `users` table.
    pub fn profile(&self) -> Profile {
        profile_from(self.id, self.email.clone(), &self.user_metadata)
    }
}

fn profile_from(id: Uuid, email: Option<String>, meta: &Value) -> Profile {
    let text = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| meta.get(*k).and_then(Value::as_str))
            .map(|s| s.chars().take(200).collect::<String>())
    };
    Profile {
        supabase_user_id: id,
        display_name: text(&["full_name", "name", "user_name"])
            .or_else(|| email.clone())
            .unwrap_or_default(),
        email,
        avatar_url: text(&["avatar_url", "picture"]),
    }
}

/// Tokens from a sign-in or a refresh.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenSet {
    /// Short-lived JWT.
    pub access_token: String,
    /// Long-lived refresh token (kept in the keychain).
    pub refresh_token: String,
    /// Lifetime in seconds.
    #[serde(default)]
    pub expires_in: Option<i64>,
    /// Expiry as a Unix time.
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// Who signed in.
    pub user: SupabaseUser,
}

impl TokenSet {
    /// Expiry as a Unix time.
    pub fn expiry(&self) -> i64 {
        self.expires_at.unwrap_or_else(|| {
            OffsetDateTime::now_utc().unix_timestamp() + self.expires_in.unwrap_or(3600)
        })
    }
}

/// A Supabase project: its address and public (anon) key.
#[derive(Clone)]
pub struct Supabase {
    http: reqwest::Client,
    url: String,
    anon_key: String,
}

impl Supabase {
    /// The project in the settings, or `None` when sign-in is not set up.
    pub fn from_config(c: &AuthConfig) -> Option<Self> {
        let (url, key) = (c.supabase_url.as_deref()?, c.supabase_anon_key.as_deref()?);
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| tracing::error!(error = %e, "could not build the HTTP client"))
            .ok()?;
        Some(Self {
            http,
            url: url.trim_end_matches('/').to_owned(),
            anon_key: key.to_owned(),
        })
    }

    /// The `iss` of this project's tokens.
    pub fn issuer(&self) -> String {
        format!("{}/auth/v1", self.url)
    }

    /// The system-browser address that starts sign-in with `provider` (PKCE, S256).
    pub fn authorize_url(&self, provider: &str, redirect_to: &str, challenge: &str) -> String {
        let mut u = match reqwest::Url::parse(&format!("{}/authorize", self.issuer())) {
            Ok(u) => u,
            Err(_) => return String::new(),
        };
        u.query_pairs_mut()
            .append_pair("provider", provider)
            .append_pair("redirect_to", redirect_to)
            .append_pair("code_challenge", challenge)
            .append_pair("code_challenge_method", "s256");
        u.into()
    }

    /// A request to `path` under the project with the anon key set.
    pub fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.url))
            .header("apikey", &self.anon_key)
    }

    /// Exchanges the sign-in code for tokens.
    pub async fn exchange_code(
        &self,
        code: &str,
        verifier: &str,
    ) -> Result<TokenSet, SupabaseError> {
        let req = self
            .request(reqwest::Method::POST, "/auth/v1/token")
            .query(&[("grant_type", "pkce")])
            .json(&serde_json::json!({ "auth_code": code, "code_verifier": verifier }));
        json(req).await
    }

    /// New tokens from a refresh token.
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenSet, SupabaseError> {
        let req = self
            .request(reqwest::Method::POST, "/auth/v1/token")
            .query(&[("grant_type", "refresh_token")])
            .json(&serde_json::json!({ "refresh_token": refresh_token }));
        json(req).await
    }

    /// Ends the Supabase session of `access_token`.
    pub async fn logout(&self, access_token: &str) -> Result<(), SupabaseError> {
        let req = self
            .request(reqwest::Method::POST, "/auth/v1/logout")
            .bearer_auth(access_token);
        send(req).await.map(drop)
    }

    async fn jwks(&self) -> Result<Value, SupabaseError> {
        json(self.request(reqwest::Method::GET, "/auth/v1/.well-known/jwks.json")).await
    }
}

/// Sends `req`; 5xx and network failures are `Offline`, other non-2xx are `Refused`.
pub async fn send(req: reqwest::RequestBuilder) -> Result<reqwest::Response, SupabaseError> {
    let res = req
        .send()
        .await
        .map_err(|e| SupabaseError::Offline(without_url(&e)))?;
    let status = res.status();
    if status.is_success() {
        return Ok(res);
    }
    if status.is_server_error() {
        return Err(SupabaseError::Offline(format!(
            "status {}",
            status.as_u16()
        )));
    }
    let body: Value = res.json().await.unwrap_or(Value::Null);
    let message = ["error_description", "msg", "message", "error"]
        .iter()
        .find_map(|k| body.get(*k).and_then(Value::as_str))
        .unwrap_or("no reason given")
        .chars()
        .take(200)
        .collect();
    Err(SupabaseError::Refused {
        status: status.as_u16(),
        message,
    })
}

/// Sends `req` and reads a JSON answer.
pub async fn json<T: serde::de::DeserializeOwned>(
    req: reqwest::RequestBuilder,
) -> Result<T, SupabaseError> {
    send(req)
        .await?
        .json()
        .await
        .map_err(|e| SupabaseError::BadAnswer(without_url(&e)))
}

// reqwest errors print the URL, which can carry the sign-in code; keep only the kind.
fn without_url(e: &reqwest::Error) -> String {
    let kind = if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "could not connect"
    } else if e.is_decode() {
        "unreadable answer"
    } else {
        "request failed"
    };
    kind.to_owned()
}

/// Claims the core reads from a Supabase access token.
#[derive(Debug, Clone, Deserialize)]
pub struct Claims {
    /// Supabase user id.
    #[serde(with = "uuid_text")]
    pub sub: Uuid,
    /// Email.
    #[serde(default)]
    pub email: Option<String>,
    /// Name and avatar.
    #[serde(default)]
    pub user_metadata: Value,
}

impl Claims {
    /// The person's profile for the `users` table.
    pub fn profile(&self) -> Profile {
        profile_from(self.sub, self.email.clone(), &self.user_metadata)
    }
}

struct Cached {
    keys: Vec<Jwk>,
    fetched_at: OffsetDateTime,
    tried_at: OffsetDateTime,
}

/// Checks Supabase access tokens: signature from the project's JWKS, `iss`, `aud = authenticated`, `exp`.
pub struct JwtVerifier {
    supabase: Supabase,
    db: Db,
    cache: tokio::sync::Mutex<Option<Cached>>,
}

impl JwtVerifier {
    /// A verifier for this project, caching keys in `db`.
    pub fn new(supabase: Supabase, db: Db) -> Self {
        Self {
            supabase,
            db,
            cache: tokio::sync::Mutex::new(None),
        }
    }

    /// The token's claims, or `None` when it is not a live token of this project.
    pub async fn verify(&self, token: &str) -> Result<Option<Claims>, SeamError> {
        let Ok(header) = jsonwebtoken::decode_header(token) else {
            return Ok(None);
        };
        if !matches!(
            header.alg,
            Algorithm::ES256 | Algorithm::RS256 | Algorithm::EdDSA
        ) {
            return Ok(None);
        }
        let Some(jwk) = self.key(header.kid.as_deref()).await? else {
            return Ok(None);
        };
        let Ok(key) = DecodingKey::from_jwk(&jwk) else {
            return Ok(None);
        };
        let mut v = Validation::new(header.alg);
        v.set_issuer(&[self.supabase.issuer()]);
        v.set_audience(&[AUDIENCE]);
        // `set_issuer`/`set_audience` check these claims only when present; require them.
        v.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        v.leeway = 30;
        Ok(jsonwebtoken::decode::<Claims>(token, &key, &v)
            .ok()
            .map(|d| d.claims))
    }

    async fn key(&self, kid: Option<&str>) -> Result<Option<Jwk>, SeamError> {
        let now = OffsetDateTime::now_utc();
        let mut cache = self.cache.lock().await;
        if cache.is_none() {
            *cache = self.load().await?;
        }
        let find = |c: &Option<Cached>| {
            c.as_ref().and_then(|c| {
                c.keys
                    .iter()
                    .find(|k| k.common.key_id.as_deref() == kid)
                    .cloned()
            })
        };
        let fresh = cache
            .as_ref()
            .is_some_and(|c| now - c.fetched_at < JWKS_FRESH);
        let found = find(&cache);
        let may_try = cache
            .as_ref()
            .is_none_or(|c| now - c.tried_at >= JWKS_RETRY);
        if (found.is_none() || !fresh) && may_try {
            match self.supabase.jwks().await {
                Ok(set) => {
                    let keys = parse_keys(&set);
                    self.save(&set, now).await?;
                    *cache = Some(Cached {
                        keys,
                        fetched_at: now,
                        tried_at: now,
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "could not fetch the sign-in keys; using the cached ones");
                    if let Some(c) = cache.as_mut() {
                        c.tried_at = now;
                    }
                }
            }
        }
        if cache
            .as_ref()
            .is_some_and(|c| now - c.fetched_at > JWKS_GRACE)
        {
            return Ok(None);
        }
        Ok(find(&cache))
    }

    async fn load(&self) -> Result<Option<Cached>, DbError> {
        let row: Option<(Value, OffsetDateTime)> =
            sqlx::query_as("SELECT jwks, fetched_at FROM auth_jwks WHERE issuer = $1")
                .bind(self.supabase.issuer())
                .fetch_optional(self.db.pool())
                .await?;
        Ok(row.map(|(set, fetched_at)| Cached {
            keys: parse_keys(&set),
            fetched_at,
            tried_at: OffsetDateTime::UNIX_EPOCH,
        }))
    }

    async fn save(&self, set: &Value, at: OffsetDateTime) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO auth_jwks (issuer, jwks, fetched_at) VALUES ($1, $2, $3) \
             ON CONFLICT (issuer) DO UPDATE SET jwks = EXCLUDED.jwks, fetched_at = EXCLUDED.fetched_at",
        )
        .bind(self.supabase.issuer())
        .bind(set)
        .bind(at)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }
}

// Keys the library cannot read (a future key type) are skipped, not fatal.
fn parse_keys(set: &Value) -> Vec<Jwk> {
    set.get("keys")
        .and_then(Value::as_array)
        .map(|ks| {
            ks.iter()
                .filter_map(|k| serde_json::from_value(k.clone()).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// The production `IdentityProvider`: Supabase access tokens.
///
/// Solo: only the account linked to the local owner signs in, as the local owner. Team: any
/// account of the project signs in as its own user (made on first sight, with no roles).
pub struct SupabaseIdentity {
    verifier: Option<JwtVerifier>,
    db: Db,
    mode: Mode,
}

impl SupabaseIdentity {
    /// The identity provider for these settings (accepts nothing when sign-in is not set up).
    pub fn new(config: &AuthConfig, db: Db, mode: Mode) -> Self {
        Self {
            verifier: Supabase::from_config(config).map(|s| JwtVerifier::new(s, db.clone())),
            db,
            mode,
        }
    }
}

#[async_trait]
impl IdentityProvider for SupabaseIdentity {
    async fn verify_bearer(&self, token: &str) -> Result<Option<Principal>, SeamError> {
        let Some(verifier) = &self.verifier else {
            return Ok(None);
        };
        let Some(claims) = verifier.verify(token).await? else {
            return Ok(None);
        };
        match self.mode {
            Mode::Solo => {
                let linked = tenancy::local_owner(&self.db)
                    .await?
                    .and_then(|o| o.supabase_user_id);
                Ok((linked == Some(claims.sub)).then(Principal::local_owner))
            }
            Mode::Team => {
                let user = tenancy::upsert_supabase_user(&self.db, &claims.profile()).await?;
                if !tenancy::is_active(&self.db, user).await? {
                    return Ok(None);
                }
                Ok(Some(tenancy::principal_for(&self.db, user).await?))
            }
        }
    }
}
