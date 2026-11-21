//! Credentials the core issues itself: personal access tokens (CLI and SDK) and renderer
//! session cookies, plus the `Authenticator` that accepts every kind of sign-in.

use crate::db::{Db, DbError, Tx};
use crate::modules::sync::crypto;
use crate::modules::tenancy;
use crate::seams::{
    Authenticator, IdentityProvider, Principal, PrincipalKind, SeamError, Secret, SecretStore,
    SoloSecretAuthenticator,
};
use async_trait::async_trait;
use axum::http::{header, HeaderMap};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, KeyInit, Mac};
use mrl_domain::ids::{OrgId, ProjectId, TokenId, UserId};
use mrl_domain::rbac::Action;
use sha2::{Digest, Sha256};
use std::str::FromStr;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use uuid::Uuid;

/// Every personal access token starts with this.
pub const PAT_PREFIX: &str = "mrl_pat_";
/// `SecretStore` name of the HMAC key for token hashes.
pub const PEPPER_SECRET: &str = "auth.token_pepper";
/// The renderer session cookie.
pub const SESSION_COOKIE: &str = "mrl_session";
/// Header that must carry the session's CSRF token on cookie requests from other origins.
pub const CSRF_HEADER: &str = "x-csrf-token";
/// Sessions end after this long without use.
pub const SESSION_IDLE_HOURS: i32 = 12;
/// Sessions end this long after sign-in, used or not.
pub const SESSION_MAX_DAYS: i32 = 7;
/// Longest token lifetime.
pub const MAX_TOKEN_DAYS: u32 = 365;

/// The token pepper from the keychain, made on first use.
pub async fn load_pepper(store: &dyn SecretStore) -> Result<[u8; 32], SeamError> {
    if let Some(hex) = store.get(PEPPER_SECRET).await? {
        return crypto::key_from_hex(hex.expose())
            .ok_or_else(|| SeamError::new("secrets", "the stored token key is damaged"));
    }
    let pepper = crypto::random::<32>().map_err(|e| SeamError::new("secrets", e.to_string()))?;
    store
        .set(PEPPER_SECRET, Secret::new(crypto::to_hex(&pepper)))
        .await?;
    Ok(pepper)
}

fn token_hash(pepper: &[u8; 32], token: &str) -> Result<Vec<u8>, SeamError> {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(pepper)
        .map_err(|_| SeamError::new("auth", "token key has the wrong length"))?;
    mac.update(token.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}

fn sha256(text: &str) -> Vec<u8> {
    Sha256::digest(text.as_bytes()).to_vec()
}

fn random_text() -> Result<String, SeamError> {
    let bytes = crypto::random::<32>().map_err(|e| SeamError::new("auth", e.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// What a live token lets its holder do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenGrant {
    /// Token id.
    pub id: TokenId,
    /// The person the token acts for.
    pub user: UserId,
    /// The org it is bound to.
    pub org: Option<OrgId>,
    /// Its scopes.
    pub scopes: Vec<String>,
    /// The one project it is limited to, if any.
    pub project: Option<ProjectId>,
    /// When it stops working.
    pub expires_at: OffsetDateTime,
}

/// user, org, scopes, project.
type GrantRow = (
    Uuid,
    Option<Uuid>,
    Vec<String>,
    Option<Uuid>,
    OffsetDateTime,
);

/// The grant behind a token principal id (`tok_…`), if that token is still live.
pub async fn token_grant(db: &Db, principal_id: &str) -> Result<Option<TokenGrant>, DbError> {
    let Ok(id) = TokenId::from_str(principal_id) else {
        return Ok(None);
    };
    let row: Option<GrantRow> = sqlx::query_as(
        "SELECT user_id, org_id, scopes, project_id, expires_at FROM api_tokens \
         WHERE id = $1 AND revoked_at IS NULL AND expires_at > now()",
    )
    .bind(id.as_uuid())
    .fetch_optional(db.pool())
    .await?;
    Ok(
        row.map(|(user, org, scopes, project, expires_at)| TokenGrant {
            id,
            user: UserId::from_uuid(user),
            org: org.map(OrgId::from_uuid),
            scopes,
            project: project.map(ProjectId::from_uuid),
            expires_at,
        }),
    )
}

/// A token just made. `token` is shown to the caller once and never stored.
#[derive(Debug, Clone)]
pub struct IssuedToken {
    /// Token id.
    pub id: TokenId,
    /// The token itself.
    pub token: Secret,
    /// First characters, safe to show in lists.
    pub prefix: String,
    /// When it stops working.
    pub expires_at: OffsetDateTime,
}

/// What a new token is for.
#[derive(Debug, Clone)]
pub struct TokenRequest {
    /// Owner.
    pub user: UserId,
    /// Bound org.
    pub org: Option<OrgId>,
    /// Bound project.
    pub project: Option<ProjectId>,
    /// A name the user recognizes, like `mrl CLI`.
    pub name: String,
    /// Scopes (checked by [`check_scopes`]).
    pub scopes: Vec<String>,
    /// Lifetime, 1–365 days.
    pub days: u32,
    /// Never later than this (a child token ends with its parent).
    pub not_after: Option<OffsetDateTime>,
}

/// Makes a token in `tx`.
pub async fn issue_token(
    tx: &mut Tx,
    pepper: &[u8; 32],
    req: &TokenRequest,
) -> Result<IssuedToken, SeamError> {
    let token = format!("{PAT_PREFIX}{}", random_text()?);
    let prefix: String = token.chars().take(PAT_PREFIX.len() + 4).collect();
    let id = TokenId::new();
    let expires_at: OffsetDateTime = sqlx::query_scalar(
        "INSERT INTO api_tokens (id, user_id, org_id, project_id, name, token_prefix, token_hash, scopes, expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, \
           LEAST(now() + make_interval(days => $9), COALESCE($10, 'infinity'::timestamptz))) RETURNING expires_at",
    )
    .bind(id.as_uuid())
    .bind(req.user.as_uuid())
    .bind(req.org.map(|o| o.as_uuid()))
    .bind(req.project.map(|p| p.as_uuid()))
    .bind(&req.name)
    .bind(&prefix)
    .bind(token_hash(pepper, &token)?)
    .bind(&req.scopes)
    .bind(i32::try_from(req.days.clamp(1, MAX_TOKEN_DAYS)).unwrap_or(1))
    .bind(req.not_after)
    .fetch_one(&mut **tx)
    .await
    .map_err(DbError::from)?;
    Ok(IssuedToken {
        id,
        token: Secret::new(token),
        prefix,
        expires_at,
    })
}

/// The principal for a token: a `Service` principal named by the token id.
pub async fn verify_token(
    db: &Db,
    pepper: &[u8; 32],
    token: &str,
) -> Result<Option<Principal>, SeamError> {
    let row: Option<(Uuid, Option<Uuid>, bool)> = sqlx::query_as(
        "SELECT id, org_id, (last_used_at IS NULL OR last_used_at < now() - interval '1 minute') \
         FROM api_tokens WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > now()",
    )
    .bind(token_hash(pepper, token)?)
    .fetch_optional(db.pool())
    .await
    .map_err(DbError::from)?;
    let Some((id, org, stale)) = row else {
        return Ok(None);
    };
    if stale {
        sqlx::query("UPDATE api_tokens SET last_used_at = now() WHERE id = $1")
            .bind(id)
            .execute(db.pool())
            .await
            .map_err(DbError::from)?;
    }
    Ok(Some(Principal {
        id: TokenId::from_uuid(id).to_string(),
        kind: PrincipalKind::Service,
        org: org.map(|o| OrgId::from_uuid(o).to_string()),
    }))
}

fn action_family(action: Action) -> &'static str {
    action.as_str().split('.').next().unwrap_or("")
}

fn is_read(action: Action) -> bool {
    action
        .as_str()
        .split('.')
        .nth(1)
        .is_some_and(|v| v.starts_with("read"))
}

/// Token scopes: `*`, or `<family>:read` / `<family>:write` where family is the part of an
/// action before the dot (`runs:write`, `datasets:read`). `write` includes `read`.
pub fn scopes_allow(scopes: &[String], action: Action) -> bool {
    let family = action_family(action);
    scopes.iter().any(|s| {
        s == "*"
            || s.split_once(':').is_some_and(|(f, level)| {
                f == family && (level == "write" || (level == "read" && is_read(action)))
            })
    })
}

/// Checks requested scopes. `Err` says which scope is unknown.
pub fn check_scopes(scopes: &[String]) -> Result<(), String> {
    if scopes.is_empty() {
        return Err("a token needs at least one scope".into());
    }
    for s in scopes {
        let known = s == "*"
            || s.split_once(':').is_some_and(|(f, level)| {
                matches!(level, "read" | "write")
                    && Action::ALL
                        .iter()
                        .any(|&a| !a.is_machine() && action_family(a) == f)
            });
        if !known {
            return Err(format!(
                "{s:?} is not a scope; use \"*\" or <area>:read / <area>:write, like runs:write"
            ));
        }
    }
    Ok(())
}

/// True when every scope in `want` is within `have`.
pub fn scopes_cover(have: &[String], want: &[String]) -> bool {
    want.iter().all(|w| {
        have.iter().any(|h| {
            h == "*"
                || h == w
                || (w.split_once(':').zip(h.split_once(':')))
                    .is_some_and(|((wf, wl), (hf, hl))| wf == hf && hl == "write" && wl == "read")
        })
    })
}

/// A new renderer session. Both values go to the caller once.
#[derive(Debug, Clone)]
pub struct NewSession {
    /// The cookie value.
    pub cookie: Secret,
    /// The CSRF token for `x-csrf-token`.
    pub csrf: Secret,
    /// When the session ends at the latest.
    pub expires_at: OffsetDateTime,
}

/// Starts a session for `user` in `tx`.
pub async fn create_session(
    tx: &mut Tx,
    user: UserId,
    user_agent: Option<&str>,
) -> Result<NewSession, SeamError> {
    let (cookie, csrf) = (random_text()?, random_text()?);
    let expires_at: OffsetDateTime = sqlx::query_scalar(
        "INSERT INTO sessions (id_hash, user_id, csrf_hash, idle_expires_at, absolute_expires_at, user_agent) \
         VALUES ($1, $2, $3, now() + make_interval(hours => $4), now() + make_interval(days => $5), $6) \
         RETURNING absolute_expires_at",
    )
    .bind(sha256(&cookie))
    .bind(user.as_uuid())
    .bind(sha256(&csrf))
    .bind(SESSION_IDLE_HOURS)
    .bind(SESSION_MAX_DAYS)
    .bind(user_agent.map(|u| u.chars().take(200).collect::<String>()))
    .fetch_one(&mut **tx)
    .await
    .map_err(DbError::from)?;
    Ok(NewSession {
        cookie: Secret::new(cookie),
        csrf: Secret::new(csrf),
        expires_at,
    })
}

/// The `Authorization: Bearer` token in `headers`.
pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

/// The session cookie value in `headers`.
pub fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == SESSION_COOKIE)
        .map(|(_, v)| v)
        .filter(|v| !v.is_empty())
}

/// The user of a live session. Cookie requests need the CSRF header unless the browser says
/// they come from the core's own origin (`Sec-Fetch-Site: same-origin`).
pub async fn verify_session(db: &Db, headers: &HeaderMap) -> Result<Option<UserId>, DbError> {
    let Some(cookie) = session_cookie(headers) else {
        return Ok(None);
    };
    let id_hash = sha256(cookie);
    let row: Option<(Uuid, Vec<u8>, bool)> = sqlx::query_as(
        "SELECT user_id, csrf_hash, idle_expires_at < now() + make_interval(hours => $2) - interval '1 minute' \
         FROM sessions WHERE id_hash = $1 AND idle_expires_at > now() AND absolute_expires_at > now()",
    )
    .bind(&id_hash)
    .bind(SESSION_IDLE_HOURS)
    .fetch_optional(db.pool())
    .await?;
    let Some((user, csrf_hash, stale)) = row else {
        return Ok(None);
    };
    let same_origin = headers
        .get("sec-fetch-site")
        .is_some_and(|v| v.as_bytes() == b"same-origin");
    let csrf_ok = headers
        .get(CSRF_HEADER)
        .is_some_and(|v| bool::from(Sha256::digest(v.as_bytes()).as_slice().ct_eq(&csrf_hash)));
    if !(same_origin || csrf_ok) {
        return Ok(None);
    }
    if stale {
        sqlx::query(
            "UPDATE sessions SET idle_expires_at = LEAST(now() + make_interval(hours => $2), absolute_expires_at) \
             WHERE id_hash = $1",
        )
        .bind(&id_hash)
        .bind(SESSION_IDLE_HOURS)
        .execute(db.pool())
        .await?;
    }
    Ok(Some(UserId::from_uuid(user)))
}

/// Ends the session named by the cookie in `headers`, if any, in `tx`.
pub async fn end_session(tx: &mut Tx, headers: &HeaderMap) -> Result<(), DbError> {
    if let Some(cookie) = session_cookie(headers) {
        sqlx::query("DELETE FROM sessions WHERE id_hash = $1")
            .bind(sha256(cookie))
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Deletes ended sessions and tokens that expired or were revoked over 90 days ago.
pub async fn sweep(db: &Db) -> Result<(), DbError> {
    sqlx::query(
        "DELETE FROM sessions WHERE idle_expires_at < now() OR absolute_expires_at < now()",
    )
    .execute(db.pool())
    .await?;
    sqlx::query(
        "DELETE FROM api_tokens WHERE COALESCE(revoked_at, expires_at) < now() - interval '90 days'",
    )
    .execute(db.pool())
    .await?;
    Ok(())
}

/// The production `Authenticator`, for both modes. Tried in order: the solo launch secret,
/// `Authorization: Bearer mrl_pat_…`, a Supabase access token (via the identity provider),
/// then the session cookie.
pub struct CoreAuthenticator {
    solo: Option<SoloSecretAuthenticator>,
    db: Db,
    pepper: [u8; 32],
    identity: Arc<dyn IdentityProvider>,
}

impl CoreAuthenticator {
    /// `launch_secret` is set in solo mode only.
    pub fn new(
        db: Db,
        pepper: [u8; 32],
        identity: Arc<dyn IdentityProvider>,
        launch_secret: Option<&str>,
    ) -> Self {
        Self {
            solo: launch_secret.map(SoloSecretAuthenticator::new),
            db,
            pepper,
            identity,
        }
    }
}

#[async_trait]
impl Authenticator for CoreAuthenticator {
    async fn authenticate(&self, headers: &HeaderMap) -> Result<Option<Principal>, SeamError> {
        if let Some(solo) = &self.solo {
            if let Some(p) = solo.authenticate(headers).await? {
                return Ok(Some(p));
            }
        }
        if let Some(token) = bearer(headers) {
            return if token.starts_with(PAT_PREFIX) {
                verify_token(&self.db, &self.pepper, token).await
            } else {
                self.identity.verify_bearer(token).await
            };
        }
        match verify_session(&self.db, headers).await? {
            Some(user) => {
                let mode = if self.solo.is_some() {
                    crate::config::Mode::Solo
                } else {
                    crate::config::Mode::Team
                };
                Ok(Some(tenancy::principal_for(&self.db, mode, user).await?))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_owned()).collect()
    }

    #[test]
    fn scopes_follow_action_families() {
        assert!(scopes_allow(&s(&["*"]), Action::RunsSubmit));
        assert!(scopes_allow(&s(&["runs:write"]), Action::RunsSubmit));
        assert!(scopes_allow(&s(&["runs:write"]), Action::RunsRead));
        assert!(scopes_allow(&s(&["runs:read"]), Action::RunsRead));
        assert!(!scopes_allow(&s(&["runs:read"]), Action::RunsSubmit));
        assert!(!scopes_allow(&s(&["datasets:write"]), Action::RunsRead));
        assert!(scopes_allow(
            &s(&["feedback:read"]),
            Action::FeedbackReadAssigned
        ));
        assert!(!scopes_allow(&[], Action::AuthSession));
    }

    #[test]
    fn scope_names_are_checked() {
        assert!(check_scopes(&s(&["*", "runs:read", "datasets:write"])).is_ok());
        for bad in [
            &[][..],
            &["runs"],
            &["runs:admin"],
            &["agent:write"],
            &["nope:read"],
        ] {
            assert!(check_scopes(&s(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_token_can_only_hand_out_what_it_has() {
        assert!(scopes_cover(&s(&["*"]), &s(&["*"])));
        assert!(scopes_cover(
            &s(&["runs:write"]),
            &s(&["runs:read", "runs:write"])
        ));
        assert!(!scopes_cover(&s(&["runs:read"]), &s(&["runs:write"])));
        assert!(!scopes_cover(&s(&["runs:write"]), &s(&["*"])));
    }

    #[test]
    fn hashes_depend_on_the_pepper() {
        let a = token_hash(&[1; 32], "mrl_pat_x").unwrap();
        assert_eq!(a, token_hash(&[1; 32], "mrl_pat_x").unwrap());
        assert_ne!(a, token_hash(&[2; 32], "mrl_pat_x").unwrap());
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn session_cookie_is_found_among_others() {
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            "a=1; mrl_session=abc ; b=2".parse().unwrap(),
        );
        assert_eq!(session_cookie(&h), Some("abc"));
        h.insert(header::COOKIE, "mrl_session=".parse().unwrap());
        assert_eq!(session_cookie(&h), None);
    }
}
