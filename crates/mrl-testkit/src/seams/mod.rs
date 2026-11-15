//! Test implementations of the server seams: in memory and deterministic.
//!
//! ```text
//! let seams = mrl_testkit::seams::solo("secret").build(Mode::Solo)?;
//! let seams = mrl_testkit::seams::team(&[("tok", user("usr_1", "org_1"))]).build(Mode::Team)?;
//! ```

use async_trait::async_trait;
use mrl_server::config::Mode;
use mrl_server::seams::{
    AuditEvent, AuditSink, Authorizer, Clock, Decision, IdGenerator, IdentityProvider,
    LocalOwnerAuthorizer, MemoryIdempotencyStore, Principal, PrincipalKind, RequestContext,
    Resource, SeamError, Seams, SeamsBuilder, Secret, SecretStore, SoloSecretAuthenticator,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use uuid::Uuid;

/// A clock that only moves when told to. Starts at 2026-01-01T00:00:00Z.
pub struct FixedClock(Mutex<OffsetDateTime>);

impl Default for FixedClock {
    fn default() -> Self {
        Self(Mutex::new(
            OffsetDateTime::UNIX_EPOCH + time::Duration::days(20_454),
        ))
    }
}

impl FixedClock {
    /// Moves the clock forward.
    pub fn advance(&self, by: time::Duration) {
        let mut now = self.0.lock().unwrap_or_else(|p| p.into_inner());
        *now += by;
    }
}

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        *self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Ids 1, 2, 3, … as UUIDs.
#[derive(Default)]
pub struct SequentialIds(AtomicU64);

impl IdGenerator for SequentialIds {
    fn new_uuid(&self) -> Uuid {
        Uuid::from_u128(u128::from(self.0.fetch_add(1, Ordering::Relaxed) + 1))
    }
}

/// Secrets in a map.
#[derive(Default)]
pub struct InMemorySecretStore(Mutex<HashMap<String, Secret>>);

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .cloned())
    }
    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError> {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(name.to_owned(), value);
        Ok(())
    }
    async fn delete(&self, name: &str) -> Result<(), SeamError> {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name);
        Ok(())
    }
}

/// Bearer tokens mapped to principals.
#[derive(Default)]
pub struct StaticIdentityProvider(HashMap<String, Principal>);

impl StaticIdentityProvider {
    /// Accepts exactly these tokens.
    pub fn new(tokens: &[(&str, Principal)]) -> Self {
        Self(
            tokens
                .iter()
                .map(|(t, p)| ((*t).to_owned(), p.clone()))
                .collect(),
        )
    }
}

#[async_trait]
impl IdentityProvider for StaticIdentityProvider {
    async fn verify_bearer(&self, token: &str) -> Result<Option<Principal>, SeamError> {
        Ok(self.0.get(token).cloned())
    }
}

/// Always gives the same decision.
pub struct StaticAuthorizer(pub Decision);

#[async_trait]
impl Authorizer for StaticAuthorizer {
    async fn authorize(
        &self,
        _: &RequestContext,
        _: &str,
        _: &Resource,
    ) -> Result<Decision, SeamError> {
        Ok(self.0)
    }
}

/// Keeps audit events in memory.
#[derive(Default)]
pub struct RecordingAuditSink(Mutex<Vec<AuditEvent>>);

impl RecordingAuditSink {
    /// Events so far.
    pub fn events(&self) -> Vec<AuditEvent> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

#[async_trait]
impl AuditSink for RecordingAuditSink {
    async fn record(&self, event: AuditEvent) -> Result<(), SeamError> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).push(event);
        Ok(())
    }
}

/// A team-mode user in `org`.
pub fn user(id: &str, org: &str) -> Principal {
    Principal {
        id: id.to_owned(),
        kind: PrincipalKind::User,
        org: Some(org.to_owned()),
    }
}

fn common() -> SeamsBuilder {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock::default());
    Seams::builder()
        .idempotency(MemoryIdempotencyStore::new(clock.clone()))
        .clock_arc(clock)
        .ids(SequentialIds::default())
        .audit(RecordingAuditSink::default())
        .secrets(InMemorySecretStore::default())
}

/// Solo seams: requests need `secret` in `x-mrl-launch-secret`.
pub fn solo(secret: &str) -> SeamsBuilder {
    common()
        .authenticator(SoloSecretAuthenticator::new(secret))
        .authorizer(LocalOwnerAuthorizer)
}

/// Team seams: these bearer tokens sign in; every action is allowed.
pub fn team(tokens: &[(&str, Principal)]) -> SeamsBuilder {
    common()
        .identity(StaticIdentityProvider::new(tokens))
        .authorizer(StaticAuthorizer(Decision::Allow))
}

/// Builds `builder` for `mode`; panics with the catalogued error if a seam is missing.
pub fn build(builder: SeamsBuilder, mode: Mode) -> Seams {
    match builder.build(mode) {
        Ok(s) => s,
        Err(e) => panic!("test seams are incomplete: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn secret_store_round_trips() {
        let s = InMemorySecretStore::default();
        assert_eq!(s.get("k").await.unwrap(), None);
        s.set("k", Secret::new("v")).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap().expose(), "v");
        assert_eq!(
            format!("{:?}", s.get("k").await.unwrap().unwrap()),
            "Secret(<redacted>)"
        );
        s.delete("k").await.unwrap();
        assert_eq!(s.get("k").await.unwrap(), None);
    }

    #[test]
    fn clock_and_ids_are_deterministic() {
        let c = FixedClock::default();
        let t0 = c.now();
        assert_eq!(t0.year(), 2026);
        c.advance(time::Duration::seconds(5));
        assert_eq!(c.now() - t0, time::Duration::seconds(5));
        let ids = SequentialIds::default();
        assert_eq!(ids.new_uuid(), Uuid::from_u128(1));
        assert_eq!(ids.new_uuid(), Uuid::from_u128(2));
    }

    #[test]
    fn builders_are_complete_for_their_mode() {
        assert!(solo("s").build(Mode::Solo).is_ok());
        assert!(team(&[]).build(Mode::Team).is_ok());
    }
}
