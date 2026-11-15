//! Built-in seam implementations used in production wiring.

use super::{
    AuditEvent, AuditSink, Authenticator, Authorizer, Clock, Decision, IdGenerator,
    IdempotencyBegin, IdempotencyStore, IdentityProvider, Principal, PrincipalKind, RequestContext,
    Resource, SeamError, StoredResponse,
};
use async_trait::async_trait;
use axum::http::{header, HeaderMap};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use uuid::Uuid;

/// Environment variable the desktop app sets with the per-launch secret.
pub const LAUNCH_SECRET_ENV: &str = "MRL_LAUNCH_SECRET";
/// Header that carries the per-launch secret in solo mode.
pub const LAUNCH_SECRET_HEADER: &str = "x-mrl-launch-secret";
/// How long idempotency keys are kept.
pub const IDEMPOTENCY_TTL: time::Duration = time::Duration::hours(24);

/// System time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// Time-ordered UUID v7 ids.
#[derive(Debug, Clone, Copy, Default)]
pub struct UuidV7Ids;

impl IdGenerator for UuidV7Ids {
    fn new_uuid(&self) -> Uuid {
        Uuid::now_v7()
    }
}

/// Solo mode: the request must carry the per-launch secret the desktop app chose.
pub struct SoloSecretAuthenticator {
    digest: [u8; 32],
}

impl SoloSecretAuthenticator {
    /// Accepts requests whose `x-mrl-launch-secret` equals `secret`.
    pub fn new(secret: &str) -> Self {
        Self {
            digest: Sha256::digest(secret.as_bytes()).into(),
        }
    }

    /// Constant-time check. Hashing first makes the compare length-independent.
    pub fn matches(&self, candidate: &[u8]) -> bool {
        let got: [u8; 32] = Sha256::digest(candidate).into();
        got.ct_eq(&self.digest).into()
    }
}

#[async_trait]
impl Authenticator for SoloSecretAuthenticator {
    async fn authenticate(&self, headers: &HeaderMap) -> Result<Option<Principal>, SeamError> {
        Ok(headers
            .get(LAUNCH_SECRET_HEADER)
            .filter(|v| self.matches(v.as_bytes()))
            .map(|_| Principal::local_owner()))
    }
}

/// Team mode default: `Authorization: Bearer <token>` checked by the identity provider.
pub struct BearerAuthenticator {
    idp: Arc<dyn IdentityProvider>,
}

impl BearerAuthenticator {
    /// Wraps an identity provider.
    pub fn new(idp: Arc<dyn IdentityProvider>) -> Self {
        Self { idp }
    }
}

#[async_trait]
impl Authenticator for BearerAuthenticator {
    async fn authenticate(&self, headers: &HeaderMap) -> Result<Option<Principal>, SeamError> {
        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|t| !t.is_empty());
        match token {
            Some(t) => self.idp.verify_bearer(t).await,
            None => Ok(None),
        }
    }
}

/// Solo mode: the local owner may do everything; nobody else may do anything.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalOwnerAuthorizer;

#[async_trait]
impl Authorizer for LocalOwnerAuthorizer {
    async fn authorize(
        &self,
        ctx: &RequestContext,
        _action: &str,
        _resource: &Resource,
    ) -> Result<Decision, SeamError> {
        Ok(match ctx.principal.kind {
            PrincipalKind::LocalOwner => Decision::Allow,
            PrincipalKind::User | PrincipalKind::Service => Decision::Deny,
        })
    }
}

/// Writes audit events to the structured log (target `mrl_audit`).
#[derive(Debug, Clone, Copy, Default)]
pub struct TracingAuditSink;

#[async_trait]
impl AuditSink for TracingAuditSink {
    async fn record(&self, e: AuditEvent) -> Result<(), SeamError> {
        tracing::info!(
            target: "mrl_audit",
            actor = %e.actor,
            org = e.org.as_deref(),
            project = e.project.as_deref(),
            action = %e.action,
            resource = %e.target,
            outcome = ?e.outcome,
            trace_id = %e.trace_id,
            "audit event"
        );
        Ok(())
    }
}

type IdemMap = HashMap<(String, String), IdemEntry>;

struct IdemEntry {
    fingerprint: [u8; 32],
    created: OffsetDateTime,
    response: Option<StoredResponse>,
}

/// In-memory idempotency keys with a 24 h TTL. Keys do not survive a restart.
pub struct MemoryIdempotencyStore {
    clock: Arc<dyn Clock>,
    entries: Mutex<IdemMap>,
}

impl MemoryIdempotencyStore {
    /// A store that expires keys by `clock`.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, IdemMap>, SeamError> {
        self.entries
            .lock()
            .map_err(|_| SeamError::new("idempotency", "store lock poisoned"))
    }
}

#[async_trait]
impl IdempotencyStore for MemoryIdempotencyStore {
    async fn begin(
        &self,
        scope: &str,
        key: &str,
        fingerprint: [u8; 32],
    ) -> Result<IdempotencyBegin, SeamError> {
        let now = self.clock.now();
        let mut map = self.lock()?;
        // ponytail: O(n) sweep per write; switch to a time-ordered queue if keys reach ~100k.
        map.retain(|_, e| now - e.created < IDEMPOTENCY_TTL);
        let id = (scope.to_owned(), key.to_owned());
        Ok(match map.get(&id) {
            Some(e) if e.fingerprint != fingerprint => IdempotencyBegin::Mismatch,
            Some(IdemEntry {
                response: Some(r), ..
            }) => IdempotencyBegin::Replay(r.clone()),
            Some(_) => IdempotencyBegin::InProgress,
            None => {
                map.insert(
                    id,
                    IdemEntry {
                        fingerprint,
                        created: now,
                        response: None,
                    },
                );
                IdempotencyBegin::Started
            }
        })
    }

    async fn complete(
        &self,
        scope: &str,
        key: &str,
        response: StoredResponse,
    ) -> Result<(), SeamError> {
        if let Some(e) = self.lock()?.get_mut(&(scope.to_owned(), key.to_owned())) {
            e.response = Some(response);
        }
        Ok(())
    }

    async fn release(&self, scope: &str, key: &str) -> Result<(), SeamError> {
        self.lock()?.remove(&(scope.to_owned(), key.to_owned()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    struct At(Mutex<OffsetDateTime>);
    impl Clock for At {
        fn now(&self) -> OffsetDateTime {
            *self.0.lock().unwrap()
        }
    }

    #[tokio::test]
    async fn solo_secret_accepts_only_the_exact_secret() {
        let auth = SoloSecretAuthenticator::new("s3cret-launch-value");
        let mut h = HeaderMap::new();
        assert_eq!(auth.authenticate(&h).await.unwrap(), None, "missing header");
        for wrong in [
            "",
            "s3cret",
            "s3cret-launch-valuE",
            "s3cret-launch-value-and-more",
        ] {
            h.insert(LAUNCH_SECRET_HEADER, HeaderValue::from_str(wrong).unwrap());
            assert_eq!(auth.authenticate(&h).await.unwrap(), None, "{wrong:?}");
        }
        h.insert(
            LAUNCH_SECRET_HEADER,
            HeaderValue::from_static("s3cret-launch-value"),
        );
        assert_eq!(
            auth.authenticate(&h).await.unwrap(),
            Some(Principal::local_owner())
        );
    }

    #[tokio::test]
    async fn idempotency_lifecycle_and_ttl() {
        let start = OffsetDateTime::UNIX_EPOCH;
        let clock = Arc::new(At(Mutex::new(start)));
        let store = MemoryIdempotencyStore::new(clock.clone());
        let fp = [1u8; 32];
        assert_eq!(
            store.begin("p", "k", fp).await.unwrap(),
            IdempotencyBegin::Started
        );
        assert_eq!(
            store.begin("p", "k", fp).await.unwrap(),
            IdempotencyBegin::InProgress
        );
        assert_eq!(
            store.begin("p", "k", [2; 32]).await.unwrap(),
            IdempotencyBegin::Mismatch
        );
        assert_eq!(
            store.begin("other", "k", [2; 32]).await.unwrap(),
            IdempotencyBegin::Started
        );
        let resp = StoredResponse {
            status: 201,
            headers: vec![],
            body: b"{}".to_vec(),
        };
        store.complete("p", "k", resp.clone()).await.unwrap();
        assert_eq!(
            store.begin("p", "k", fp).await.unwrap(),
            IdempotencyBegin::Replay(resp)
        );
        *clock.0.lock().unwrap() = start + IDEMPOTENCY_TTL;
        assert_eq!(
            store.begin("p", "k", [9; 32]).await.unwrap(),
            IdempotencyBegin::Started
        );
        store.release("p", "k").await.unwrap();
        assert_eq!(
            store.begin("p", "k", fp).await.unwrap(),
            IdempotencyBegin::Started
        );
    }
}
