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
use crate::config::{Config, Mode, Role};
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
    /// mTLS gRPC address agents dial. Default: `0.0.0.0:9444` in team mode, `127.0.0.1:<random>`
    /// in solo mode (set it to let other machines dial in). Never `server.listeners.grpc`: that
    /// listener belongs to the shared gRPC services, which can't check client certificates.
    pub listen: Option<SocketAddr>,
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
            listen: None,
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
    /// The gateway address for `mode` (see [`GatewayConfig::listen`]).
    pub fn listen_addr(&self, mode: Mode) -> SocketAddr {
        self.listen.unwrap_or(match mode {
            Mode::Team => SocketAddr::from(([0, 0, 0, 0], DEFAULT_TEAM_PORT)),
            Mode::Solo => SocketAddr::from(([127, 0, 0, 1], 0)),
        })
    }

    /// Problems with the values (pushed into `Config::validate`).
    pub fn problems(&self, server: &crate::config::ServerConfig) -> Vec<String> {
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
        let listen = self.listen_addr(server.mode);
        if server.mode == Mode::Team
            && listen.port() != 0
            && [
                server.listeners.grpc,
                server.listeners.api,
                server.listeners.internal,
            ]
            .iter()
            .any(|a| {
                a.port() == listen.port()
                    && (a.ip() == listen.ip()
                        || a.ip().is_unspecified()
                        || listen.ip().is_unspecified())
            })
        {
            p.push(format!(
                "gateway.listen: {listen} clashes with a server.listeners address; the gateway needs its own port"
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

/// Default gateway port in team mode (next to the shared gRPC port 9443).
pub const DEFAULT_TEAM_PORT: u16 = 9444;

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
    /// TLS for the gateway listener (agents dial in; client certificate optional for enrollment).
    pub server_tls: Arc<tokio_rustls::rustls::ServerConfig>,
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
    pub(crate) listen: SocketAddr,
    pub(crate) core_id: String,
    pub(crate) secrets: Arc<dyn SecretStore>,
    pub(crate) secrets_problem: Option<String>,
    pub(crate) pki: RwLock<Option<Arc<Pki>>>,
    pub(crate) sessions: Sessions,
    pub(crate) lanes: Arc<lanes::LaneRouter>,
    pub(crate) listener: Mutex<Option<std::net::TcpListener>>,
    pub(crate) addr: OnceLock<SocketAddr>,
    pub(crate) known_hosts: PathBuf,
    /// Revoked, unexpired certificate fingerprints; the TLS verifiers refuse them.
    pub(crate) revoked: Arc<RwLock<std::collections::HashSet<String>>>,
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

    /// The bound gateway address (after startup).
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
    pub async fn refresh_revocations(&self) -> Result<(), NodesError> {
        let list: Vec<String> = sqlx::query_scalar(
            "SELECT sha256 FROM node_certificates WHERE revoked_at IS NOT NULL AND not_after > now()",
        )
        .fetch_all(self.db.pool())
        .await?;
        if let Ok(mut set) = self.revoked.write() {
            *set = list.into_iter().collect();
        }
        Ok(())
    }

    /// Adds certificates this replica just revoked, so handshakes refuse them at once.
    pub(crate) fn revoked_locally(&self, shas: &[String]) {
        if let Ok(mut set) = self.revoked.write() {
            set.extend(shas.iter().cloned());
        }
    }
}

/// The `nodes` module.
pub struct NodesModule {
    gw: Arc<Gateway>,
    roles: Vec<Role>,
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
        let listen = g.listen_addr(config.server.mode);
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
                listen,
                core_id: format!("core-{}", Uuid::new_v4().simple()),
                secrets,
                secrets_problem: None,
                pki: RwLock::new(None),
                sessions: Sessions::default(),
                lanes,
                listener: Mutex::new(None),
                addr: OnceLock::new(),
                known_hosts,
                revoked: Arc::default(),
            }),
            roles: config.server.roles.clone(),
        }
    }

    /// The shared gateway state (address, CA fingerprint, command sending).
    pub fn gateway(&self) -> Arc<Gateway> {
        self.gw.clone()
    }
}

/// Loads (or creates) the CA, issues the core certificate, builds the TLS settings and binds
/// the gateway listener, so a port clash fails startup.
struct PkiStep {
    gw: Arc<Gateway>,
    bind: bool,
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
        if self.bind {
            let l = std::net::TcpListener::bind(self.gw.listen).map_err(|e| {
                SeamError::new(
                    "gateway",
                    format!("could not bind the gateway on {}: {e}", self.gw.listen),
                )
            })?;
            l.set_nonblocking(true)
                .map_err(|e| SeamError::new("gateway", e.to_string()))?;
            if let Ok(a) = l.local_addr() {
                let _ = self.gw.addr.set(a);
            }
            if let Ok(mut slot) = self.gw.listener.lock() {
                *slot = Some(l);
            }
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
            bind: self.roles.contains(&Role::Gateway),
        })]
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
            Task::new("nodes.gateway", Role::Gateway, move |cancel| async move {
                crate::grpc::serve(a, cancel).await.map_err(SeamError::from)
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
        // In team mode the gateway never takes the shared gRPC port.
        let team = crate::config::ServerConfig {
            mode: Mode::Team,
            ..crate::config::ServerConfig::default()
        };
        let g = GatewayConfig::default();
        assert_eq!(g.listen_addr(Mode::Team).port(), DEFAULT_TEAM_PORT);
        assert_ne!(g.listen_addr(Mode::Team), team.listeners.grpc);
        assert!(g.problems(&team).is_empty());
        let clash = GatewayConfig {
            listen: Some(team.listeners.grpc),
            ..GatewayConfig::default()
        };
        assert!(
            clash.problems(&team)[0].contains("clashes"),
            "{:?}",
            clash.problems(&team)
        );
    }

    #[test]
    fn sessions_supersede_and_revoke() {
        let s = Sessions::default();
        let node = Uuid::now_v7();
        let (c1, r1, _) = s.open_control(node, 1, 7);
        let (c2, _, _) = s.open_control(node, 2, 8);
        assert!(c1.is_cancelled() && !c2.is_cancelled());
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
