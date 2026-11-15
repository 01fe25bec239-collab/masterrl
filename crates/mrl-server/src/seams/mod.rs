//! Seams: the traits other parts implement to plug into the server (ADR-020).
//!
//! `Seams::builder()` collects implementations; `build(mode)` fails fast with a catalogued
//! error when a seam this mode needs is missing. Test implementations live in `mrl-testkit`.

mod builtin;

pub use builtin::{
    BearerAuthenticator, LocalOwnerAuthorizer, MemoryIdempotencyStore, SoloSecretAuthenticator,
    SystemClock, TracingAuditSink, UuidV7Ids, IDEMPOTENCY_TTL, LAUNCH_SECRET_ENV,
    LAUNCH_SECRET_HEADER,
};

use crate::config::Mode;
use crate::error::{ApiError, ErrorCode, ServerError};
use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use serde::Serialize;
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

/// A seam implementation failed (its backend is down, timed out, …).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{seam}: {detail}")]
pub struct SeamError {
    /// Which seam, like `idempotency`.
    pub seam: &'static str,
    /// What went wrong. Must not contain secrets.
    pub detail: String,
}

impl SeamError {
    /// A new seam error.
    pub fn new(seam: &'static str, detail: impl Into<String>) -> Self {
        Self {
            seam,
            detail: detail.into(),
        }
    }
}

/// What kind of caller a principal is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// The one person using a solo desktop server.
    LocalOwner,
    /// A signed-in person (team mode).
    User,
    /// A machine caller, like an agent or a token.
    Service,
}

/// Who is calling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Principal {
    /// Stable id, like `usr_…`.
    pub id: String,
    /// Kind of caller.
    pub kind: PrincipalKind,
    /// The org the principal acts in, if any.
    pub org: Option<String>,
}

impl Principal {
    /// The solo-mode owner.
    pub fn local_owner() -> Self {
        Self {
            id: "local-owner".to_owned(),
            kind: PrincipalKind::LocalOwner,
            org: None,
        }
    }
}

/// Ids every request carries, set for all requests (signed in or not).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestMeta {
    /// W3C trace id (32 hex chars), from `traceparent` or new.
    pub trace_id: String,
    /// `x-request-id`, from the request or new.
    pub request_id: String,
}

/// Who is calling and in which scope. Extract it in handlers: `ctx: RequestContext`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    /// The authenticated caller.
    pub principal: Principal,
    /// Org scope (defaults to the principal's org).
    pub org: Option<String>,
    /// Project scope, set by handlers that act inside a project.
    pub project: Option<String>,
    /// W3C trace id.
    pub trace_id: String,
    /// Request id.
    pub request_id: String,
}

impl RequestContext {
    /// Context for `principal` on a request with `meta`.
    pub fn new(principal: Principal, meta: &RequestMeta) -> Self {
        Self {
            org: principal.org.clone(),
            principal,
            project: None,
            trace_id: meta.trace_id.clone(),
            request_id: meta.request_id.clone(),
        }
    }

    /// The same context scoped to a project.
    pub fn with_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RequestContext {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<RequestContext>()
            .cloned()
            .ok_or_else(|| {
                ApiError::new(
                    ErrorCode::Unauthenticated,
                    "This endpoint needs a signed-in request.",
                )
            })
    }
}

/// Turns request headers into a principal. `Ok(None)` means "not signed in" (401).
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Checks the credentials in `headers`.
    async fn authenticate(&self, headers: &HeaderMap) -> Result<Option<Principal>, SeamError>;
}

/// The thing an action applies to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resource {
    /// Resource kind, like `run`.
    pub kind: String,
    /// Resource id, if it exists already.
    pub id: Option<String>,
    /// Owning org.
    pub org: Option<String>,
    /// Owning project.
    pub project: Option<String>,
}

impl Resource {
    /// A resource of `kind`, no id or scope yet.
    pub fn kind(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: None,
            org: None,
            project: None,
        }
    }
}

/// An authorization answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Go ahead.
    Allow,
    /// Visible but not allowed: 403.
    Deny,
    /// In another org: answer 404 so its existence does not leak.
    Hide,
}

/// Decides whether a principal may do an action on a resource.
#[async_trait]
pub trait Authorizer: Send + Sync {
    /// Decides `action` (like `runs.cancel`) on `resource`.
    async fn authorize(
        &self,
        ctx: &RequestContext,
        action: &str,
        resource: &Resource,
    ) -> Result<Decision, SeamError>;
}

/// How an audited action ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    /// Done.
    Success,
    /// Refused by authorization.
    Denied,
    /// Tried and failed.
    Failed,
}

/// One audit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEvent {
    /// When it happened.
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    /// Principal id.
    pub actor: String,
    /// Org scope.
    pub org: Option<String>,
    /// Project scope.
    pub project: Option<String>,
    /// Action, like `runs.cancel`.
    pub action: String,
    /// Target, like `run_0193…`.
    pub target: String,
    /// Outcome.
    pub outcome: AuditOutcome,
    /// Trace id.
    pub trace_id: String,
}

/// Where audit records go.
#[async_trait]
pub trait AuditSink: Send + Sync {
    /// Stores one record.
    async fn record(&self, event: AuditEvent) -> Result<(), SeamError>;
}

/// A response kept for idempotent replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredResponse {
    /// HTTP status.
    pub status: u16,
    /// Replayed headers (content type, location, etag).
    pub headers: Vec<(String, String)>,
    /// Body bytes.
    pub body: Vec<u8>,
}

/// What `IdempotencyStore::begin` found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyBegin {
    /// New key: run the request, then `complete` or `release`.
    Started,
    /// Same key and body already finished: replay this.
    Replay(StoredResponse),
    /// Same key still running.
    InProgress,
    /// Same key, different body.
    Mismatch,
}

/// Remembers idempotency keys (24 h) and their responses.
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Claims `key` in `scope` for a request with `fingerprint`.
    async fn begin(
        &self,
        scope: &str,
        key: &str,
        fingerprint: [u8; 32],
    ) -> Result<IdempotencyBegin, SeamError>;
    /// Stores the finished response for replay.
    async fn complete(
        &self,
        scope: &str,
        key: &str,
        response: StoredResponse,
    ) -> Result<(), SeamError>;
    /// Forgets the key so the client can retry (used after 5xx).
    async fn release(&self, scope: &str, key: &str) -> Result<(), SeamError>;
}

/// One check behind `/readyz`.
#[async_trait]
pub trait ReadinessCheck: Send + Sync {
    /// Short name shown in `/readyz`, like `database`.
    fn name(&self) -> &str;
    /// `Ok` when ready.
    async fn check(&self) -> Result<(), SeamError>;
}

/// Work that must finish before listeners open, like database migrations.
#[async_trait]
pub trait StartupStep: Send + Sync {
    /// Short name for logs and errors, like `migrations`.
    fn name(&self) -> &str;
    /// Runs the step. An error stops startup (MRL-SRV-004).
    async fn run(&self) -> Result<(), SeamError>;
}

/// Signs people in for team mode (P16 implements it).
#[async_trait]
pub trait IdentityProvider: Send + Sync {
    /// Checks a bearer token. `Ok(None)` means unknown or expired.
    async fn verify_bearer(&self, token: &str) -> Result<Option<Principal>, SeamError>;
}

/// A secret value. `Debug` never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw value. Never log it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Stores secrets, backed by the OS keychain in the desktop app.
#[async_trait]
pub trait SecretStore: Send + Sync {
    /// Reads `name`.
    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError>;
    /// Writes `name`.
    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError>;
    /// Deletes `name` (no error if absent).
    async fn delete(&self, name: &str) -> Result<(), SeamError>;
}

/// Wall-clock time. Tests use a fixed clock.
pub trait Clock: Send + Sync {
    /// Now, in UTC.
    fn now(&self) -> OffsetDateTime;
}

/// New ids. Tests use a sequence.
pub trait IdGenerator: Send + Sync {
    /// A new UUID.
    fn new_uuid(&self) -> Uuid;
}

/// The seams a running server uses.
#[derive(Clone)]
pub struct Seams {
    /// Signs requests in.
    pub authenticator: Arc<dyn Authenticator>,
    /// Allows or refuses actions.
    pub authorizer: Arc<dyn Authorizer>,
    /// Records audit events.
    pub audit: Arc<dyn AuditSink>,
    /// Backs the idempotency middleware.
    pub idempotency: Arc<dyn IdempotencyStore>,
    /// Time.
    pub clock: Arc<dyn Clock>,
    /// Ids.
    pub ids: Arc<dyn IdGenerator>,
    /// Team-mode sign-in (always set in team mode).
    pub identity: Option<Arc<dyn IdentityProvider>>,
    /// Secret storage, when this build has one.
    pub secrets: Option<Arc<dyn SecretStore>>,
}

impl Seams {
    /// Starts an empty builder.
    pub fn builder() -> SeamsBuilder {
        SeamsBuilder::default()
    }

    /// The seams production uses for `mode`. Team mode has no identity provider yet (P16),
    /// so `build(Mode::Team)` on this fails with MRL-SRV-002.
    pub fn production(mode: Mode, launch_secret: &str) -> SeamsBuilder {
        let ids: Arc<dyn IdGenerator> = Arc::new(UuidV7Ids);
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let b = Seams::builder()
            .audit(TracingAuditSink)
            .idempotency(MemoryIdempotencyStore::new(clock.clone()))
            .clock_arc(clock)
            .ids_arc(ids);
        match mode {
            Mode::Solo => b
                .authenticator(SoloSecretAuthenticator::new(launch_secret))
                .authorizer(LocalOwnerAuthorizer),
            Mode::Team => b,
        }
    }
}

/// Collects seam implementations, then checks them for a mode.
#[derive(Default, Clone)]
pub struct SeamsBuilder {
    authenticator: Option<Arc<dyn Authenticator>>,
    authorizer: Option<Arc<dyn Authorizer>>,
    audit: Option<Arc<dyn AuditSink>>,
    idempotency: Option<Arc<dyn IdempotencyStore>>,
    clock: Option<Arc<dyn Clock>>,
    ids: Option<Arc<dyn IdGenerator>>,
    identity: Option<Arc<dyn IdentityProvider>>,
    secrets: Option<Arc<dyn SecretStore>>,
}

impl SeamsBuilder {
    /// Sets the authenticator. In team mode it defaults to bearer tokens via the identity provider.
    pub fn authenticator(mut self, v: impl Authenticator + 'static) -> Self {
        self.authenticator = Some(Arc::new(v));
        self
    }
    /// Sets the authorizer.
    pub fn authorizer(mut self, v: impl Authorizer + 'static) -> Self {
        self.authorizer = Some(Arc::new(v));
        self
    }
    /// Sets the audit sink.
    pub fn audit(mut self, v: impl AuditSink + 'static) -> Self {
        self.audit = Some(Arc::new(v));
        self
    }
    /// Sets the audit sink from a shared handle (tests keep one to read records).
    pub fn audit_arc(mut self, v: Arc<dyn AuditSink>) -> Self {
        self.audit = Some(v);
        self
    }
    /// Sets the idempotency store.
    pub fn idempotency(mut self, v: impl IdempotencyStore + 'static) -> Self {
        self.idempotency = Some(Arc::new(v));
        self
    }
    /// Sets the clock (default: system clock).
    pub fn clock(mut self, v: impl Clock + 'static) -> Self {
        self.clock = Some(Arc::new(v));
        self
    }
    /// Sets the clock from a shared handle.
    pub fn clock_arc(mut self, v: Arc<dyn Clock>) -> Self {
        self.clock = Some(v);
        self
    }
    /// Sets the id generator (default: UUID v7).
    pub fn ids(mut self, v: impl IdGenerator + 'static) -> Self {
        self.ids = Some(Arc::new(v));
        self
    }
    /// Sets the id generator from a shared handle.
    pub fn ids_arc(mut self, v: Arc<dyn IdGenerator>) -> Self {
        self.ids = Some(v);
        self
    }
    /// Sets the identity provider (required in team mode).
    pub fn identity(mut self, v: impl IdentityProvider + 'static) -> Self {
        self.identity = Some(Arc::new(v));
        self
    }
    /// Sets the secret store.
    pub fn secrets(mut self, v: impl SecretStore + 'static) -> Self {
        self.secrets = Some(Arc::new(v));
        self
    }

    /// Checks that every seam `mode` needs is present.
    pub fn build(self, mode: Mode) -> Result<Seams, ServerError> {
        let authenticator = match mode {
            Mode::Solo => self.authenticator,
            Mode::Team => {
                let Some(idp) = &self.identity else {
                    return Err(ServerError::new(
                        ErrorCode::IdentityProviderMissing,
                        "team mode needs an identity provider seam; this build has none",
                    ));
                };
                let bearer = || -> Arc<dyn Authenticator> {
                    Arc::new(BearerAuthenticator::new(idp.clone()))
                };
                Some(self.authenticator.unwrap_or_else(bearer))
            }
        };
        let mut missing = Vec::new();
        if authenticator.is_none() {
            missing.push("authenticator");
        }
        if self.authorizer.is_none() {
            missing.push("authorizer");
        }
        if self.audit.is_none() {
            missing.push("audit sink");
        }
        if self.idempotency.is_none() {
            missing.push("idempotency store");
        }
        match (authenticator, self.authorizer, self.audit, self.idempotency) {
            (Some(authenticator), Some(authorizer), Some(audit), Some(idempotency)) => Ok(Seams {
                authenticator,
                authorizer,
                audit,
                idempotency,
                clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
                ids: self.ids.unwrap_or_else(|| Arc::new(UuidV7Ids)),
                identity: self.identity,
                secrets: self.secrets,
            }),
            _ => Err(ServerError::new(
                ErrorCode::SeamMissing,
                format!(
                    "{} mode needs these seams, which have no implementation: {}",
                    match mode {
                        Mode::Solo => "solo",
                        Mode::Team => "team",
                    },
                    missing.join(", ")
                ),
            )),
        }
    }
}
