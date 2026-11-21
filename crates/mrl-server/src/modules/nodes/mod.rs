//! Machines: pools, enrollment, node and GPU records, node states, durable agent commands,
//! and the agent gateway that carries them (`crate::grpc`).
//!
//! - [`ca`]: the internal CA (key in the `SecretStore`).
//! - [`store`]: SQL for pools, nodes, GPUs, tokens, certificates, telemetry and events.
//! - [`commands`]: [`commands::CommandSender`] — durable commands, any replica writes.
//! - [`lanes`]: the four lanes of D-04-05 and the [`lanes::LaneSink`] seam.
//! - [`health`]: ONLINE/DEGRADED/OFFLINE detection, command deadlines, retention.
//! - [`api`]: `/pools`, `/nodes` REST endpoints.

pub mod api;
pub mod ca;
pub mod commands;
pub mod health;
pub mod lanes;
pub mod secrets;
pub mod store;

use crate::app::{ServerModule, Task};
use crate::config::{Config, Role};
use crate::db::{Db, DbError};
use crate::seams::{ReadinessCheck, SeamError, SecretStore, StartupStep};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// `[gateway]`: the agent gateway and node health.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct GatewayConfig {
    /// Secret-store name of the internal CA (key and certificate).
    pub ca_secret: String,
    /// Where the CA secret lives: `database` (sealed rows every replica shares; needs the
    /// key-encryption key in `kek_env`), `file` (this host's secret folder), or `auto`
    /// (`database` when `kek_env` is set, else `file`). Several team hosts need `database`.
    pub ca_store: CaStore,
    /// Environment variable holding the 64-hex-character key-encryption key.
    pub kek_env: String,
    /// Folder of the file secret store that holds the CA until the OS keychain store exists.
    /// Default: `<pgbundle data folder>/secrets`.
    pub secrets_dir: Option<PathBuf>,
    /// The core's own SSH `known_hosts` (host keys confirmed for the SSH transport).
    /// Default: `known_hosts` next to `secrets_dir`.
    pub known_hosts: Option<PathBuf>,
    /// SSH client config read for host aliases. Default: `~/.ssh/config`.
    pub ssh_config: Option<PathBuf>,
    /// `tailscale` program.
    pub tailscale_program: PathBuf,
    /// `cloudflared` program.
    pub cloudflared_program: PathBuf,
    /// How often agents send a heartbeat (2–120 s).
    pub heartbeat_interval_seconds: u32,
    /// Heartbeat age that makes an ONLINE node DEGRADED.
    pub degraded_after_seconds: u64,
    /// Heartbeat age that makes a node OFFLINE (`node_offline_after`).
    pub offline_after_seconds: u64,
    /// How often node health and command deadlines are checked (1–60 s).
    pub health_check_seconds: u64,
    /// How often an open stream re-checks its certificate against the revocation list.
    pub revocation_check_seconds: u64,
    /// Lifetime of a STATIC node's certificate (1–90 days).
    pub static_cert_days: u32,
    /// Lifetime of an EPHEMERAL node's certificate (1–24 hours).
    pub ephemeral_cert_hours: u32,
    /// Default deadline of a command (10 s – 1 day).
    pub command_deadline_seconds: u64,
    /// METRICS and LOGS items buffered per stream before overflow rules apply.
    pub lane_buffer: usize,
    /// Agent clock offset (from heartbeats) that makes a node DEGRADED (`clock_skew`).
    pub max_clock_skew_seconds: u64,
    /// Free disk below which a node is DEGRADED (`disk_low`).
    pub min_disk_free_bytes: u64,
    /// GPU temperature at which the GPU, and its node, are DEGRADED (`gpu_hot`).
    pub gpu_hot_celsius: u32,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            ca_secret: "gateway_ca".to_owned(),
            ca_store: CaStore::Auto,
            kek_env: "MRL_GATEWAY_KEK".to_owned(),
            secrets_dir: None,
            known_hosts: None,
            ssh_config: None,
            tailscale_program: PathBuf::from("tailscale"),
            cloudflared_program: PathBuf::from("cloudflared"),
            heartbeat_interval_seconds: 10,
            degraded_after_seconds: 30,
            offline_after_seconds: 60,
            health_check_seconds: 5,
            revocation_check_seconds: 15,
            static_cert_days: 30,
            ephemeral_cert_hours: 24,
            command_deadline_seconds: 600,
            lane_buffer: 1024,
            max_clock_skew_seconds: 30,
            min_disk_free_bytes: 5 << 30,
            gpu_hot_celsius: 90,
        }
    }
}

impl GatewayConfig {
    /// Problems with the values (pushed into `Config::validate`). Agents use the shared gRPC
    /// listener (`server.listeners.grpc`), so nothing here can clash with it.
    pub fn problems(&self, _server: &crate::config::ServerConfig) -> Vec<String> {
        let mut p = Vec::new();
        let mut range = |name: &str, v: u64, lo: u64, hi: u64| {
            if !(lo..=hi).contains(&v) {
                p.push(format!("gateway.{name}: {v} is outside {lo}–{hi}"));
            }
        };
        range(
            "heartbeat_interval_seconds",
            self.heartbeat_interval_seconds.into(),
            1,
            120,
        );
        range(
            "degraded_after_seconds",
            self.degraded_after_seconds,
            1,
            3600,
        );
        range(
            "offline_after_seconds",
            self.offline_after_seconds,
            1,
            86_400,
        );
        range("health_check_seconds", self.health_check_seconds, 1, 60);
        range(
            "revocation_check_seconds",
            self.revocation_check_seconds,
            1,
            300,
        );
        range("static_cert_days", self.static_cert_days.into(), 1, 90);
        range(
            "ephemeral_cert_hours",
            self.ephemeral_cert_hours.into(),
            1,
            24,
        );
        range(
            "command_deadline_seconds",
            self.command_deadline_seconds,
            10,
            86_400,
        );
        range("lane_buffer", self.lane_buffer as u64, 1, 1_000_000);
        range(
            "max_clock_skew_seconds",
            self.max_clock_skew_seconds,
            1,
            3600,
        );
        range("gpu_hot_celsius", self.gpu_hot_celsius.into(), 50, 120);
        if self.offline_after_seconds < self.degraded_after_seconds {
            p.push(format!(
                "gateway.offline_after_seconds: {} is less than gateway.degraded_after_seconds ({})",
                self.offline_after_seconds, self.degraded_after_seconds
            ));
        }
        if self.ca_secret.trim().is_empty() {
            p.push("gateway.ca_secret: give the secret-store name of the CA".to_owned());
        }
        p
    }
}

/// `gateway.ca_store`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaStore {
    /// `database` when the key-encryption key is set, else `file`.
    #[default]
    Auto,
    /// Sealed rows in `gateway_secrets`, shared by every replica.
    Database,
    /// This host's file secret store (`secrets_dir`).
    File,
}

/// Error codes this part adds (guidance/fragments/errors/p18.yaml).
pub mod codes {
    /// The join token was not accepted.
    pub const TOKEN_REJECTED: &str = "MRL-NODE-180";
    /// The machine's certificate was not accepted.
    pub const CERT_REJECTED: &str = "MRL-NODE-181";
    /// The agent has not joined the app yet.
    pub const NOT_ENROLLED: &str = "MRL-NODE-182";
    /// The internal CA could not be loaded or used.
    pub const CA_UNAVAILABLE: &str = "MRL-NODE-183";
    /// A command was refused before sending.
    pub const COMMAND_REFUSED: &str = "MRL-NODE-187";
    /// The control spool is full.
    pub const SPOOL_FULL: &str = "MRL-NODE-188";
    /// The other side broke the gateway protocol.
    pub const PROTOCOL: &str = "MRL-NODE-189";
}

/// Something in this module went wrong.
#[derive(Debug, thiserror::Error)]
pub enum NodesError {
    /// The join token was not accepted.
    #[error("the join token can't be used: {0}")]
    Token(String),
    /// The certificate (or certificate request) was not accepted.
    #[error("the machine's certificate was not accepted: {0}")]
    Certificate(String),
    /// The internal CA could not be loaded or used.
    #[error("the internal certificate authority can't be used: {0}")]
    Ca(String),
    /// A command was refused before sending.
    #[error("the command was not sent: {0}")]
    CommandRefused(String),
    /// The other side broke the protocol.
    #[error("the agent connection broke the protocol: {0}")]
    Protocol(String),
    /// A transport failed.
    #[error(transparent)]
    Transport(#[from] crate::transports::TransportError),
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
}

impl NodesError {
    /// The catalogued code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Token(_) => codes::TOKEN_REJECTED,
            Self::Certificate(_) => codes::CERT_REJECTED,
            Self::Ca(_) => codes::CA_UNAVAILABLE,
            Self::CommandRefused(_) => codes::COMMAND_REFUSED,
            Self::Protocol(_) => codes::PROTOCOL,
            Self::Transport(e) => e.code(),
            Self::Db(e) => e.code(),
        }
    }

    /// Code and message, for logs and stream errors.
    pub fn explain(&self) -> String {
        format!("{} {self}", self.code())
    }
}

impl From<sqlx::Error> for NodesError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e.into())
    }
}

impl From<NodesError> for SeamError {
    fn from(e: NodesError) -> Self {
        SeamError::new("nodes", e.explain())
    }
}

/// The CA plus the TLS settings made from it.
pub struct Pki {
    /// The CA.
    pub ca: ca::Ca,
    /// Client-certificate check for the shared gRPC listener: optional (a machine without one may
    /// only enroll); one that is sent must chain to the CA and not be revoked.
    pub client_verifier: Arc<dyn tokio_rustls::rustls::server::danger::ClientCertVerifier>,
    /// The core certificate (and CA) with its key, served to agents on the gRPC listener.
    pub core_key: Arc<tokio_rustls::rustls::sign::CertifiedKey>,
    /// TLS for dialing an enrolled agent (core certificate as client certificate).
    pub dial_tls: Arc<tokio_rustls::rustls::ClientConfig>,
    /// TLS for dialing an agent that waits to enroll (its temporary certificate is not checked;
    /// the join token proves who it is).
    pub enroll_tls: Arc<tokio_rustls::rustls::ClientConfig>,
}

/// One node's live streams on this replica.
pub(crate) struct NodeSessions {
    /// Epoch of the CONTROL session this replica holds.
    pub epoch: i64,
    /// The connection that CONTROL session came on ([`crate::grpc::PeerInfo::connection`]).
    pub connection: u64,
    /// Cancelled when a newer CONTROL session for the node starts here.
    pub control: CancellationToken,
    /// Cancelled when the node's certificates are revoked (all streams close).
    pub revoked: CancellationToken,
    /// Woken when a command for the node was written.
    pub wake: Arc<Notify>,
}

/// Live sessions on this replica, by node.
#[derive(Default)]
pub(crate) struct Sessions(Mutex<HashMap<Uuid, NodeSessions>>);

impl Sessions {
    /// Registers a new CONTROL session; an older one on this replica is told to stop.
    pub fn open_control(
        &self,
        node: Uuid,
        epoch: i64,
        connection: u64,
    ) -> (CancellationToken, CancellationToken, Arc<Notify>) {
        let Ok(mut map) = self.0.lock() else {
            let t = CancellationToken::new();
            t.cancel();
            return (t.clone(), t, Arc::new(Notify::new()));
        };
        let entry = map.entry(node).or_insert_with(|| NodeSessions {
            epoch: 0,
            connection: 0,
            control: CancellationToken::new(),
            revoked: CancellationToken::new(),
            wake: Arc::new(Notify::new()),
        });
        if entry.epoch > epoch {
            // A newer session registered first (its open committed later but ran faster): this
            // one is already stale and must not cancel it.
            let stale = CancellationToken::new();
            stale.cancel();
            return (stale, entry.revoked.clone(), entry.wake.clone());
        }
        if entry.revoked.is_cancelled() {
            entry.revoked = CancellationToken::new();
        }
        entry.control.cancel();
        entry.control = CancellationToken::new();
        entry.epoch = epoch;
        entry.connection = connection;
        (
            entry.control.clone(),
            entry.revoked.clone(),
            entry.wake.clone(),
        )
    }

    /// The revocation token for a data stream of `node`.
    pub fn revoked_token(&self, node: Uuid) -> CancellationToken {
        let Ok(mut map) = self.0.lock() else {
            return CancellationToken::new();
        };
        map.entry(node)
            .or_insert_with(|| NodeSessions {
                epoch: 0,
                connection: 0,
                control: CancellationToken::new(),
                revoked: CancellationToken::new(),
                wake: Arc::new(Notify::new()),
            })
            .revoked
            .clone()
    }

    /// Ends the CONTROL session `epoch` (when it is still the current one): its token is
    /// cancelled, so the data streams bound to it close.
    pub fn close_control(&self, node: Uuid, epoch: i64) {
        if let Ok(mut map) = self.0.lock() {
            if map.get(&node).is_some_and(|e| e.epoch == epoch) {
                if let Some(e) = map.remove(&node) {
                    e.control.cancel();
                }
            }
        }
    }

    /// The CONTROL session this replica holds for `node` when it came on `connection`: its epoch
    /// and the token cancelled when it ends or is taken over.
    pub fn control_of(&self, node: Uuid, connection: u64) -> Option<(i64, CancellationToken)> {
        let map = self.0.lock().ok()?;
        map.get(&node)
            .filter(|e| {
                e.epoch > 0
                    && connection != 0
                    && e.connection == connection
                    && !e.control.is_cancelled()
            })
            .map(|e| (e.epoch, e.control.clone()))
    }

    /// True when this replica holds a CONTROL session for `node`.
    pub fn has_control(&self, node: Uuid) -> bool {
        self.0
            .lock()
            .map(|m| {
                m.get(&node)
                    .is_some_and(|e| e.epoch > 0 && !e.control.is_cancelled())
            })
            .unwrap_or(false)
    }

    /// Wakes `node`'s CONTROL session to send new commands.
    pub fn wake(&self, node: Uuid) {
        if let Ok(map) = self.0.lock() {
            if let Some(e) = map.get(&node) {
                e.wake.notify_one();
            }
        }
    }

    /// Closes every stream of `node` (its certificates were revoked).
    pub fn revoke(&self, node: Uuid) {
        if let Ok(map) = self.0.lock() {
            if let Some(e) = map.get(&node) {
                e.revoked.cancel();
                e.control.cancel();
            }
        }
    }
}

/// Everything the gateway, the dialer, the health task and the API share.
pub struct Gateway {
    pub(crate) db: Db,
    pub(crate) config: GatewayConfig,
    /// `server.listeners.grpc`'s port (for the enroll command before the listener is bound).
    pub(crate) grpc_port: u16,
    /// Cancelled when the server shuts down: every gateway stream ends.
    pub(crate) stop: CancellationToken,
    pub(crate) core_id: String,
    pub(crate) secrets: Arc<dyn SecretStore>,
    pub(crate) secrets_problem: Option<String>,
    pub(crate) pki: RwLock<Option<Arc<Pki>>>,
    pub(crate) sessions: Sessions,
    pub(crate) lanes: Arc<lanes::LaneRouter>,
    pub(crate) addr: OnceLock<SocketAddr>,
    pub(crate) known_hosts: PathBuf,
    /// Revoked, unexpired certificate fingerprints; the TLS verifiers refuse them.
    pub(crate) revoked: Arc<RwLock<std::collections::HashSet<String>>>,
    /// Held while the revocation list is reloaded (one reload at a time).
    pub(crate) revocation_refresh: tokio::sync::Mutex<()>,
    /// Revocations made on this replica, with their generation (see [`revocation_list`]).
    pub(crate) local_revocations: Mutex<Vec<(u64, String)>>,
    pub(crate) revocation_gen: std::sync::atomic::AtomicU64,
}

impl Gateway {
    /// The CA and TLS settings (loaded by the `gateway.pki` startup step).
    pub fn pki(&self) -> Result<Arc<Pki>, NodesError> {
        self.pki
            .read()
            .ok()
            .and_then(|p| p.clone())
            .ok_or_else(|| NodesError::Ca("the internal CA is not loaded yet".into()))
    }

    /// The bound gRPC listener address agents dial (after startup).
    pub fn addr(&self) -> Option<SocketAddr> {
        self.addr.get().copied()
    }

    /// Hex SHA-256 of the CA certificate, once loaded.
    pub fn ca_fingerprint(&self) -> Option<String> {
        self.pki().ok().map(|p| p.ca.fingerprint().to_owned())
    }

    /// The database.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// The settings.
    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    /// What transports need from the core.
    pub fn transport_env(&self) -> crate::transports::TransportEnv {
        crate::transports::TransportEnv {
            known_hosts: self.known_hosts.clone(),
            ssh_config: self.config.ssh_config.clone().or_else(|| {
                std::env::var_os("HOME")
                    .or_else(|| std::env::var_os("USERPROFILE"))
                    .map(|h| PathBuf::from(h).join(".ssh").join("config"))
            }),
            secrets: self.secrets.clone(),
            tailscale_program: self.config.tailscale_program.clone(),
            cloudflared_program: self.config.cloudflared_program.clone(),
        }
    }

    /// Closes every stream of `node` on this replica (after its certificates were revoked).
    pub fn kick(&self, node: Uuid) {
        self.sessions.revoke(node);
    }

    /// Reloads the revocation list the TLS verifiers use (revoked and not yet expired).
    /// Refreshes run one at a time, so an older snapshot never replaces a newer one, and a
    /// certificate revoked here while the snapshot was read is kept (see [`revocation_list`]).
    pub async fn refresh_revocations(&self) -> Result<(), NodesError> {
        let _one = self.revocation_refresh.lock().await;
        let mark = self
            .revocation_gen
            .load(std::sync::atomic::Ordering::SeqCst);
        let snapshot: Vec<String> = sqlx::query_scalar(
            "SELECT sha256 FROM node_certificates WHERE revoked_at IS NOT NULL AND not_after > now()",
        )
        .fetch_all(self.db.pool())
        .await?;
        if let (Ok(mut set), Ok(mut local)) = (self.revoked.write(), self.local_revocations.lock())
        {
            *set = revocation_list(snapshot, &mut local, mark);
        }
        Ok(())
    }

    /// Adds certificates this replica just revoked (after the revocation committed), so
    /// handshakes refuse them at once.
    pub(crate) fn revoked_locally(&self, shas: &[String]) {
        if let (Ok(mut set), Ok(mut local)) = (self.revoked.write(), self.local_revocations.lock())
        {
            for sha in shas {
                let g = self
                    .revocation_gen
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                local.push((g, sha.clone()));
                set.insert(sha.clone());
            }
        }
    }
}

/// The revocation list from a database `snapshot` read after generation `mark`: local
/// revocations up to `mark` committed before the read (they are in it) and are forgotten;
/// later ones may be missing from it and are kept.
fn revocation_list(
    snapshot: Vec<String>,
    local: &mut Vec<(u64, String)>,
    mark: u64,
) -> std::collections::HashSet<String> {
    local.retain(|(g, _)| *g > mark);
    snapshot
        .into_iter()
        .chain(local.iter().map(|(_, s)| s.clone()))
        .collect()
}

/// The `nodes` module.
pub struct NodesModule {
    gw: Arc<Gateway>,
}

impl NodesModule {
    /// The module for this build: the CA lives in a file secret store under
    /// `gateway.secrets_dir` (until P16's keychain store), lanes go to [`lanes::LaneRouter::global`].
    pub fn from_config(db: Db, config: &Config) -> Self {
        let kek = std::env::var(&config.gateway.kek_env)
            .ok()
            .filter(|v| !v.is_empty());
        Self::from_config_with_kek(db, config, kek.as_deref())
    }

    /// Like [`NodesModule::from_config`] with the key-encryption key given (tests, embedders).
    pub fn from_config_with_kek(db: Db, config: &Config, kek: Option<&str>) -> Self {
        let g = &config.gateway;
        let use_db = match g.ca_store {
            CaStore::Database => true,
            CaStore::File => false,
            CaStore::Auto => kek.is_some(),
        };
        let fallback = || -> Arc<dyn SecretStore> {
            Arc::new(crate::pgbundle::FileSecretStore::new(
                std::env::temp_dir().join("mrl-unused"),
            ))
        };
        let (secrets, problem): (Arc<dyn SecretStore>, Option<String>) = if use_db {
            match kek.map(secrets::parse_kek) {
                Some(Ok(k)) => match secrets::PgSecretStore::new(db.clone(), &k) {
                    Ok(s) => (Arc::new(s), None),
                    Err(e) => (fallback(), Some(e.to_string())),
                },
                Some(Err(e)) => (fallback(), Some(format!("{}: {e}", g.kek_env))),
                None => (
                    fallback(),
                    Some(format!(
                        "gateway.ca_store is database, but {} is not set",
                        g.kek_env
                    )),
                ),
            }
        } else {
            let dir = match &g.secrets_dir {
                Some(d) => Ok(d.clone()),
                None => crate::pgbundle::data_dir(&config.pgbundle).map(|d| d.join("secrets")),
            };
            // ponytail: one host's file store; several team hosts set the KEK (database store).
            match dir {
                Ok(d) => (Arc::new(crate::pgbundle::FileSecretStore::new(d)), None),
                Err(e) => (
                    fallback(),
                    Some(format!(
                        "no folder for the CA secret: {e}; set gateway.secrets_dir"
                    )),
                ),
            }
        };
        let mut m = Self::new(db, config, secrets, lanes::LaneRouter::global());
        if let Some(gw) = Arc::get_mut(&mut m.gw) {
            gw.secrets_problem = problem;
        }
        m
    }

    /// A module with an explicit secret store and lane router (tests, other builds).
    pub fn new(
        db: Db,
        config: &Config,
        secrets: Arc<dyn SecretStore>,
        lanes: Arc<lanes::LaneRouter>,
    ) -> Self {
        let g = &config.gateway;
        let known_hosts = g.known_hosts.clone().unwrap_or_else(|| {
            g.secrets_dir
                .clone()
                .or_else(|| {
                    crate::pgbundle::data_dir(&config.pgbundle)
                        .ok()
                        .map(|d| d.join("secrets"))
                })
                .and_then(|d| d.parent().map(|p| p.join("known_hosts")))
                .unwrap_or_else(|| PathBuf::from("known_hosts"))
        });
        Self {
            gw: Arc::new(Gateway {
                db,
                config: g.clone(),
                grpc_port: config.server.listeners.grpc.port(),
                stop: CancellationToken::new(),
                core_id: format!("core-{}", Uuid::new_v4().simple()),
                secrets,
                secrets_problem: None,
                pki: RwLock::new(None),
                sessions: Sessions::default(),
                lanes,
                addr: OnceLock::new(),
                known_hosts,
                revoked: Arc::default(),
                revocation_refresh: tokio::sync::Mutex::new(()),
                local_revocations: Mutex::default(),
                revocation_gen: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }

    /// The shared gateway state (address, CA fingerprint, command sending).
    pub fn gateway(&self) -> Arc<Gateway> {
        self.gw.clone()
    }
}

/// Loads (or creates) the CA, issues the core certificate and builds the TLS settings, before
/// the server asks for [`ServerModule::grpc_tls`].
struct PkiStep {
    gw: Arc<Gateway>,
}

#[async_trait::async_trait]
impl StartupStep for PkiStep {
    fn name(&self) -> &'static str {
        "gateway.pki"
    }

    async fn run(&self) -> Result<(), SeamError> {
        if let Some(p) = &self.gw.secrets_problem {
            return Err(NodesError::Ca(p.clone()).into());
        }
        let ca = ca::Ca::load_or_create(
            self.gw.secrets.as_ref(),
            &self.gw.config.ca_secret,
            &self.gw.db,
        )
        .await?;
        self.gw.refresh_revocations().await?;
        let pki = crate::grpc::tls::build(ca, self.gw.revoked.clone())?;
        if let Ok(mut slot) = self.gw.pki.write() {
            *slot = Some(Arc::new(pki));
        }
        Ok(())
    }
}

/// Ready once the CA is loaded.
struct PkiReady(Arc<Gateway>);

#[async_trait::async_trait]
impl ReadinessCheck for PkiReady {
    fn name(&self) -> &'static str {
        "gateway"
    }

    async fn check(&self) -> Result<(), SeamError> {
        self.0.pki().map(|_| ()).map_err(SeamError::from)
    }
}

impl ServerModule for NodesModule {
    fn name(&self) -> &'static str {
        "nodes"
    }

    fn routes(&self) -> Router<crate::AppState> {
        api::routes(self.gw.clone())
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        api::openapi()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![Arc::new(PkiStep {
            gw: self.gw.clone(),
        })]
    }

    fn grpc_services(&self, routes: &mut tonic::service::RoutesBuilder) {
        routes.add_service(crate::grpc::service(self.gw.clone()));
    }

    fn grpc_tls(&self) -> Option<crate::app::GrpcTls> {
        let pki = match self.gw.pki() {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(code = e.code(), error = %e, "the gateway has no TLS settings; agents can't connect");
                return None;
            }
        };
        let gw = self.gw.clone();
        Some(crate::app::GrpcTls {
            client_verifier: pki.client_verifier.clone(),
            certified_key: pki.core_key.clone(),
            server_names: vec![ca::CORE_NAME.to_owned()],
            open_services: vec![crate::grpc::SERVICE_NAME],
            on_listen: Box::new(move |a| {
                tracing::info!(listener = "grpc", scheme = "mtls", addr = %a, "agent gateway on the gRPC listener");
                let _ = gw.addr.set(a);
            }),
        })
    }

    fn readiness_checks(&self) -> Vec<Arc<dyn ReadinessCheck>> {
        vec![Arc::new(PkiReady(self.gw.clone()))]
    }

    fn background_tasks(&self) -> Vec<Task> {
        let (a, b, c, d, e) = (
            self.gw.clone(),
            self.gw.clone(),
            self.gw.clone(),
            self.gw.clone(),
            self.gw.clone(),
        );
        vec![
            // The service itself runs on the shared gRPC listener; this ends its streams at
            // shutdown so the listener can drain.
            Task::new("nodes.gateway", Role::Gateway, move |cancel| async move {
                cancel.cancelled().await;
                a.stop.cancel();
                Ok(())
            }),
            Task::new(
                "nodes.command_wake",
                Role::Gateway,
                move |cancel| async move {
                    commands::listen_for_wakes(b, cancel).await;
                    Ok(())
                },
            ),
            Task::new("nodes.dialer", Role::Gateway, move |cancel| async move {
                crate::grpc::dialer::run(c, cancel).await;
                Ok(())
            }),
            Task::new(
                "nodes.revocations",
                Role::Gateway,
                move |cancel| async move {
                    let every = Duration::from_secs(e.config.revocation_check_seconds);
                    loop {
                        tokio::select! {
                            () = cancel.cancelled() => return Ok(()),
                            () = tokio::time::sleep(every) => {}
                        }
                        if let Err(err) = e.refresh_revocations().await {
                            tracing::warn!(code = err.code(), error = %err, "could not reload the revocation list");
                        }
                    }
                },
            ),
            Task::new("nodes.health", Role::Background, move |cancel| async move {
                health::run(d, cancel).await;
                Ok(())
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_ranges_checked() {
        let server = crate::config::ServerConfig::default();
        assert!(GatewayConfig::default().problems(&server).is_empty());
        let bad = GatewayConfig {
            heartbeat_interval_seconds: 0,
            offline_after_seconds: 5,
            degraded_after_seconds: 10,
            ca_secret: " ".into(),
            ..GatewayConfig::default()
        };
        let p = bad.problems(&server);
        assert_eq!(p.len(), 3, "{p:?}");
    }

    #[test]
    fn a_reload_keeps_revocations_made_while_it_read() {
        let mut local = vec![(1, "old".to_owned()), (3, "new".to_owned())];
        // The reload started at generation 2: "old" was committed before its read, "new" after.
        let set = revocation_list(vec!["old".into(), "other".into()], &mut local, 2);
        assert!(set.contains("new"), "revoked here during the read: kept");
        assert!(set.contains("old") && set.contains("other"));
        assert_eq!(local, vec![(3, "new".to_owned())]);
        let set = revocation_list(vec!["new".into()], &mut local, 3);
        assert_eq!(set.len(), 1);
        assert!(local.is_empty());
    }

    #[test]
    fn sessions_supersede_and_revoke() {
        let s = Sessions::default();
        let node = Uuid::now_v7();
        let (c1, r1, _) = s.open_control(node, 1, 7);
        let (c2, _, _) = s.open_control(node, 2, 8);
        assert!(c1.is_cancelled() && !c2.is_cancelled());
        // An older open that registers late is stale at once and leaves the newer one alone.
        let (late, _, _) = s.open_control(node, 1, 9);
        assert!(late.is_cancelled() && !c2.is_cancelled());
        assert!(s.has_control(node));
        assert_eq!(s.control_of(node, 8).map(|c| c.0), Some(2));
        assert!(
            s.control_of(node, 7).is_none(),
            "another connection never borrows it"
        );
        assert!(s.control_of(node, 0).is_none());
        s.close_control(node, 1); // stale epoch: ignored
        assert!(s.has_control(node));
        s.revoke(node);
        assert!(r1.is_cancelled() && c2.is_cancelled());
        s.close_control(node, 2);
        assert!(!s.has_control(node));
    }
}
