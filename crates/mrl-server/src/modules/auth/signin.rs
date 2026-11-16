//! This install's own Supabase sign-in (solo mode): PKCE flows, and the session kept in the
//! OS keychain. Local work never needs it; it is needed for setup sync and joining a team.

use super::supabase::{Supabase, SupabaseError, SupabaseUser, TokenSet};
use crate::error::{ApiError, ErrorCode};
use crate::modules::sync::crypto;
use crate::seams::{SeamError, Secret, SecretStore};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

/// `SecretStore` name of the Supabase session (access and refresh token).
pub const SESSION_SECRET: &str = "auth.supabase_session";
/// Code: the sign-in has expired and the user must sign in again.
pub const EXPIRED: &str = "MRL-AUTH-160";
/// Code: a sign-in did not finish (expired, used twice, or refused).
pub const NOT_FINISHED: &str = "MRL-AUTH-161";
/// Code: this build has no sign-in settings.
pub const NOT_SET_UP: &str = "MRL-AUTH-162";
/// Code: this install is linked to another account.
pub const OTHER_ACCOUNT: &str = "MRL-AUTH-163";
/// How long a started sign-in stays open.
const FLOW_TTL: Duration = Duration::from_secs(600);
/// Refresh when the access token has less than this left.
const REFRESH_MARGIN: i64 = 60;

/// The session as stored in the keychain.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Stored {
    access_token: String,
    refresh_token: String,
    expires_at: i64,
    user: SupabaseUser,
    /// Supabase refused the refresh token: the user must sign in again.
    #[serde(default)]
    expired: bool,
}

/// Where this install's sign-in stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignInState {
    /// This build has no Supabase settings.
    NotConfigured,
    /// Never signed in, or signed out.
    SignedOut,
    /// Signed in (the token is refreshed when online).
    Active,
    /// Supabase no longer accepts the session; sign in again.
    Expired,
}

/// Why an online action could not get a token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignInError {
    /// No Supabase settings.
    #[error("{NOT_SET_UP}: sign-in is not set up for this app")]
    NotConfigured,
    /// Not signed in.
    #[error("you are not signed in to your MasterRL account on this computer")]
    SignedOut,
    /// Supabase refused the refresh token.
    #[error("{EXPIRED}: your sign-in has expired, so sign in again")]
    Expired,
    /// Supabase could not be reached.
    #[error("{0}")]
    Offline(String),
    /// The keychain failed.
    #[error("{0}")]
    Store(String),
}

impl From<SignInError> for ApiError {
    fn from(e: SignInError) -> Self {
        let code = match &e {
            SignInError::SignedOut | SignInError::Expired => ErrorCode::Unauthenticated,
            _ => ErrorCode::Unavailable,
        };
        ApiError::new(code, format!("{e}."))
    }
}

/// A live access token and who it belongs to.
#[derive(Debug, Clone)]
pub struct Access {
    /// The JWT, for calls to Supabase.
    pub token: Secret,
    /// The user.
    pub user: SupabaseUser,
}

/// This install's sign-in session, kept in `secrets`.
pub struct SignIn {
    supabase: Option<Supabase>,
    secrets: Option<Arc<dyn SecretStore>>,
}

fn store_err(e: SeamError) -> SignInError {
    SignInError::Store(e.to_string())
}

impl SignIn {
    /// The session for this project, kept in `secrets`.
    pub fn new(supabase: Option<Supabase>, secrets: Option<Arc<dyn SecretStore>>) -> Self {
        Self { supabase, secrets }
    }

    fn parts(&self) -> Result<(&Supabase, &dyn SecretStore), SignInError> {
        match (&self.supabase, &self.secrets) {
            (Some(s), Some(k)) => Ok((s, k.as_ref())),
            (None, _) => Err(SignInError::NotConfigured),
            (_, None) => Err(SignInError::Store("this server has no secret store".into())),
        }
    }

    async fn load(&self) -> Result<Option<Stored>, SignInError> {
        let (_, store) = self.parts()?;
        let Some(raw) = store.get(SESSION_SECRET).await.map_err(store_err)? else {
            return Ok(None);
        };
        // A damaged entry is treated as signed out rather than blocking forever.
        Ok(serde_json::from_str(raw.expose()).ok())
    }

    async fn save(&self, s: &Stored) -> Result<(), SignInError> {
        let (_, store) = self.parts()?;
        let raw = serde_json::to_string(s).map_err(|e| SignInError::Store(e.to_string()))?;
        store
            .set(SESSION_SECRET, Secret::new(raw))
            .await
            .map_err(store_err)
    }

    /// Keeps the tokens of a finished sign-in.
    pub async fn keep(&self, t: &TokenSet) -> Result<(), SignInError> {
        self.save(&Stored {
            access_token: t.access_token.clone(),
            refresh_token: t.refresh_token.clone(),
            expires_at: t.expiry(),
            user: t.user.clone(),
            expired: false,
        })
        .await
    }

    /// The state, without going online.
    pub async fn state(&self) -> Result<SignInState, SignInError> {
        if self.supabase.is_none() {
            return Ok(SignInState::NotConfigured);
        }
        Ok(match self.load().await? {
            None => SignInState::SignedOut,
            Some(s) if s.expired => SignInState::Expired,
            Some(_) => SignInState::Active,
        })
    }

    /// The signed-in Supabase user, without going online.
    pub async fn user(&self) -> Result<Option<SupabaseUser>, SignInError> {
        if self.supabase.is_none() {
            return Ok(None);
        }
        Ok(self.load().await?.map(|s| s.user))
    }

    /// A live access token, refreshed first when it is about to expire.
    pub async fn access(&self) -> Result<Access, SignInError> {
        let (supabase, _) = self.parts()?;
        let mut s = self.load().await?.ok_or(SignInError::SignedOut)?;
        if s.expired {
            return Err(SignInError::Expired);
        }
        if s.expires_at - REFRESH_MARGIN <= OffsetDateTime::now_utc().unix_timestamp() {
            match supabase.refresh(&s.refresh_token).await {
                Ok(t) => {
                    self.keep(&t).await?;
                    s = self.load().await?.ok_or(SignInError::SignedOut)?;
                }
                Err(SupabaseError::Refused { status, .. }) if status < 500 => {
                    s.expired = true;
                    self.save(&s).await?;
                    return Err(SignInError::Expired);
                }
                Err(e) => return Err(SignInError::Offline(e.to_string())),
            }
        }
        Ok(Access {
            token: Secret::new(s.access_token),
            user: s.user,
        })
    }

    /// Forgets the session, and ends it at Supabase when online (best effort).
    pub async fn sign_out(&self) -> Result<(), SignInError> {
        let (supabase, store) = match self.parts() {
            Ok(p) => p,
            Err(SignInError::NotConfigured) => return Ok(()),
            Err(e) => return Err(e),
        };
        if let Some(s) = self.load().await? {
            if let Err(e) = supabase.logout(&s.access_token).await {
                tracing::info!(error = %e, "could not end the session at the sign-in service");
            }
        }
        store.delete(SESSION_SECRET).await.map_err(store_err)
    }
}

/// A started sign-in waiting for its callback.
#[derive(Debug, Clone)]
pub struct Flow {
    /// PKCE verifier.
    pub verifier: String,
    started: Instant,
}

/// Sign-ins started but not finished. One-shot: `take` removes the flow.
///
/// ponytail: in memory, so a restart mid-sign-in means signing in again; fine for one
/// process per install.
#[derive(Default)]
pub struct Flows(Mutex<HashMap<String, Flow>>);

impl Flows {
    /// A new flow: its id and the PKCE challenge for the authorize URL.
    pub fn start(&self) -> Result<(String, String), SeamError> {
        let rnd = |n| -> Result<String, SeamError> {
            let b = crypto::random::<32>().map_err(|e| SeamError::new("auth", e.to_string()))?;
            Ok(URL_SAFE_NO_PAD.encode(&b[..n]))
        };
        let (id, verifier) = (rnd(16)?, rnd(32)?);
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let mut map = self
            .0
            .lock()
            .map_err(|_| SeamError::new("auth", "flow lock poisoned"))?;
        map.retain(|_, f| f.started.elapsed() < FLOW_TTL);
        map.insert(
            id.clone(),
            Flow {
                verifier,
                started: Instant::now(),
            },
        );
        Ok((id, challenge))
    }

    /// The flow `id`, removed; `None` if unknown or older than 10 minutes.
    pub fn take(&self, id: &str) -> Option<Flow> {
        let flow = self.0.lock().ok()?.remove(id)?;
        (flow.started.elapsed() < FLOW_TTL).then_some(flow)
    }
}

/// Where Supabase may send the browser back: the desktop deep link, or a loopback address
/// (the CLI, and Linux desktops without scheme handling).
pub fn redirect_allowed(uri: &str) -> bool {
    if uri == "masterrl://auth/callback" {
        return true;
    }
    let Ok(u) = reqwest::Url::parse(uri) else {
        return false;
    };
    u.scheme() == "http"
        && matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))
        && u.port().is_some()
        && u.username().is_empty()
        && u.password().is_none()
        && u.fragment().is_none()
}

/// `uri` with `flow=<id>` added to its query.
pub fn with_flow(uri: &str, flow: &str) -> String {
    let sep = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{sep}flow={flow}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_deep_link_and_loopback_may_receive_codes() {
        for ok in [
            "masterrl://auth/callback",
            "http://127.0.0.1:53111/callback",
            "http://localhost:8080/x",
            "http://[::1]:9000/cb",
        ] {
            assert!(redirect_allowed(ok), "{ok}");
        }
        for bad in [
            "masterrl://auth/other",
            "https://evil.example/callback",
            "http://127.0.0.1/callback",
            "http://127.0.0.1.evil.example:80/",
            "http://user@127.0.0.1:5/",
            "http://127.0.0.1:5/#frag",
            "javascript:alert(1)",
        ] {
            assert!(!redirect_allowed(bad), "{bad}");
        }
    }

    #[test]
    fn flows_are_one_shot_and_pkce_shaped() {
        let flows = Flows::default();
        let (id, challenge) = flows.start().unwrap();
        assert_eq!(challenge.len(), 43);
        let flow = flows.take(&id).unwrap();
        assert_eq!(flow.verifier.len(), 43);
        assert_eq!(
            URL_SAFE_NO_PAD.encode(Sha256::digest(flow.verifier.as_bytes())),
            challenge
        );
        assert!(flows.take(&id).is_none(), "second use");
        assert!(flows.take("unknown").is_none());
        assert_eq!(
            with_flow("http://127.0.0.1:5/cb", "f1"),
            "http://127.0.0.1:5/cb?flow=f1"
        );
        assert_eq!(
            with_flow("masterrl://auth/callback?a=1", "f1"),
            "masterrl://auth/callback?a=1&flow=f1"
        );
    }
}
