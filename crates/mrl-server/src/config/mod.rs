//! Server settings: a TOML file, then `MRL_<SECTION>__<KEY>` environment overrides,
//! then command-line flags. Every key is checked at startup; unknown keys are errors.

use crate::error::{ErrorCode, ServerError};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Environment variables with this prefix and a `__` override settings.
pub const ENV_PREFIX: &str = "MRL_";

/// Top-level settings sections: read from [`Config`] itself, so every part's section
/// (added below its `<mrl:config>` marker) can be set from the environment too.
fn sections() -> Vec<String> {
    toml::Table::try_from(Config::default())
        .map(|t| t.keys().cloned().collect())
        .unwrap_or_default()
}

/// How the server runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// One person on this computer: loopback only, launched by the desktop app.
    #[default]
    Solo,
    /// A shared server: configured addresses, people sign in through an identity provider.
    Team,
}

/// Work a server process takes on. A process can take several.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    clap::ValueEnum,
)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Serves the HTTP API.
    Api,
    /// Serves gRPC for agents (bound only when a module registers a gRPC service).
    Gateway,
    /// Runs scheduling tasks.
    Scheduler,
    /// Runs background tasks and event consumers.
    Background,
}

impl Role {
    /// Every role; the default.
    pub const ALL: [Role; 4] = [Role::Api, Role::Gateway, Role::Scheduler, Role::Background];
}

/// All settings.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Process, listeners and limits.
    pub server: ServerConfig,
    /// Database connection.
    pub database: DatabaseConfig,
    /// The PostgreSQL the server runs itself (solo mode without `database.url`).
    pub pgbundle: PgBundleConfig,
    /// Object storage for artifacts.
    pub object_store: ObjectStoreConfig,
    /// Logs and traces.
    pub telemetry: TelemetryConfig,
    // <mrl:config P16> P16 settings fields go right below this line.

    // <mrl:config P17> P17 settings fields go right below this line.

    // <mrl:config P18> P18 settings fields go right below this line.

    // <mrl:config P19> P19 settings fields go right below this line.
}

/// `[server]`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// `solo` (default) or `team`.
    pub mode: Mode,
    /// Roles this process takes on (default: all).
    pub roles: Vec<Role>,
    /// Addresses for team mode. Solo mode ignores them and binds `127.0.0.1:<random>`.
    pub listeners: ListenersConfig,
    /// How long shutdown waits for work to finish (1–300 s).
    pub shutdown_grace_seconds: u64,
    /// Request rate limits.
    pub rate_limit: RateLimitConfig,
    /// HTTPS for the team-mode api and grpc listeners. Unset: plain HTTP.
    /// Solo mode ignores it (loopback only).
    pub tls: Option<TlsConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: Mode::Solo,
            roles: Role::ALL.to_vec(),
            listeners: ListenersConfig::default(),
            shutdown_grace_seconds: 30,
            rate_limit: RateLimitConfig::default(),
            tls: None,
        }
    }
}

/// `[server.tls]`: PEM files for HTTPS.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Certificate chain, leaf first.
    pub cert_path: PathBuf,
    /// Private key (PKCS#8, PKCS#1 or SEC1).
    pub key_path: PathBuf,
}

/// `[server.listeners]`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ListenersConfig {
    /// HTTP API.
    pub api: SocketAddr,
    /// gRPC, bound only when a module registers a gRPC service.
    pub grpc: SocketAddr,
    /// `/metrics`, `/healthz`, `/readyz` for operators.
    pub internal: SocketAddr,
}

impl Default for ListenersConfig {
    fn default() -> Self {
        Self {
            api: SocketAddr::from(([0, 0, 0, 0], 8443)),
            grpc: SocketAddr::from(([0, 0, 0, 0], 9443)),
            internal: SocketAddr::from(([127, 0, 0, 1], 9090)),
        }
    }
}

/// `[server.rate_limit]`: token buckets per principal and per org.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct RateLimitConfig {
    /// Bucket per signed-in principal.
    pub principal: BucketConfig,
    /// Bucket per org.
    pub org: BucketConfig,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            principal: BucketConfig {
                per_second: 20,
                burst: 40,
            },
            org: BucketConfig {
                per_second: 100,
                burst: 200,
            },
        }
    }
}

/// One token bucket.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfig {
    /// Tokens added per second (1–100000).
    pub per_second: u32,
    /// Bucket size (1–100000).
    pub burst: u32,
}

/// `[database]`.
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct DatabaseConfig {
    /// `postgres://…` URL. May hold a password, so it never appears in logs.
    /// Unset in solo mode: the server runs its own PostgreSQL (`[pgbundle]`).
    pub url: Option<String>,
    /// Most pooled connections per process (1–1000).
    pub max_connections: u32,
    /// Connections kept open when idle (0 to `max_connections`).
    pub min_connections: u32,
    /// How long a request waits for a free connection (1–300 s).
    pub acquire_timeout_seconds: u64,
    /// Longest a single statement may run (1–3600 s). Migrations have no limit.
    pub statement_timeout_seconds: u64,
    /// Apply pending migrations at startup. When false, the server refuses to start
    /// until `mrl-server migrate` has run.
    pub auto_migrate: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: None,
            max_connections: 10,
            min_connections: 0,
            acquire_timeout_seconds: 10,
            statement_timeout_seconds: 30,
            auto_migrate: true,
        }
    }
}

impl std::fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("url", &self.url.as_ref().map(|_| "<redacted>"))
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("acquire_timeout_seconds", &self.acquire_timeout_seconds)
            .field("statement_timeout_seconds", &self.statement_timeout_seconds)
            .field("auto_migrate", &self.auto_migrate)
            .finish()
    }
}

/// `[pgbundle]`: the PostgreSQL the server starts itself in solo mode.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PgBundleConfig {
    /// Folder with `postgres`, `initdb`, `pg_ctl`, `pg_dump`, `pg_restore` and `pg_upgrade`.
    /// Unset: next to the server program (`postgres/bin`), then `PATH`.
    pub bin_dir: Option<PathBuf>,
    /// Binaries of the previous major version, used once to upgrade an older data folder.
    pub previous_bin_dir: Option<PathBuf>,
    /// Where the database lives. Unset: the app data folder for this account.
    pub data_dir: Option<PathBuf>,
    /// Backups to keep (1–100).
    pub backups_keep: u32,
}

impl Default for PgBundleConfig {
    fn default() -> Self {
        Self {
            bin_dir: None,
            previous_bin_dir: None,
            data_dir: None,
            backups_keep: 7,
        }
    }
}

/// `[object_store]`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObjectStoreConfig {
    /// `s3://bucket/prefix` or `file:///absolute/path`.
    pub url: Option<String>,
    /// Endpoint for S3-compatible stores, like `http://127.0.0.1:8333`.
    pub endpoint: Option<String>,
}

/// `[telemetry]`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct TelemetryConfig {
    /// Log filter, like `info` or `info,mrl_server=debug`.
    pub log_level: String,
    /// OTLP gRPC endpoint for traces, like `http://127.0.0.1:4317`. Unset: no export.
    pub otlp_endpoint: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: "info".to_owned(),
            otlp_endpoint: None,
        }
    }
}

/// Command-line values that override the file and environment.
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    /// `--mode`.
    pub mode: Option<Mode>,
    /// `--roles`.
    pub roles: Option<Vec<Role>>,
}

impl Config {
    /// Loads the file (if any), applies `MRL_*` overrides from `env`, then `cli`, then validates.
    pub fn load(
        path: Option<&Path>,
        env: impl IntoIterator<Item = (String, String)>,
        cli: &Overrides,
    ) -> Result<Config, ServerError> {
        let text = match path {
            Some(p) => std::fs::read_to_string(p).map_err(|e| {
                ServerError::new(ErrorCode::ConfigUnreadable, format!("{}: {e}", p.display()))
            })?,
            None => String::new(),
        };
        let mut config = Config::from_toml_with_env(&text, env)?;
        if let Some(mode) = cli.mode {
            config.server.mode = mode;
        }
        if let Some(roles) = &cli.roles {
            config.server.roles = roles.clone();
        }
        config.validate()?;
        Ok(config)
    }

    /// Parses TOML text and applies environment overrides. Does not validate.
    pub fn from_toml_with_env(
        text: &str,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Config, ServerError> {
        // Never `e.to_string()`: it quotes the source line, which may hold a password.
        let parse_err =
            |e: toml::de::Error| ServerError::new(ErrorCode::ConfigParse, describe(&e, text));
        // Parse once as typed config so file mistakes get file line numbers.
        toml::from_str::<Config>(text).map_err(parse_err)?;
        let mut table: toml::Table = toml::from_str(text).map_err(parse_err)?;
        let mut applied = Vec::new();
        for (name, raw) in env {
            let Some(rest) = name.strip_prefix(ENV_PREFIX) else {
                continue;
            };
            if !rest.contains("__") {
                continue; // like MRL_LAUNCH_SECRET: not a setting
            }
            set_path(&mut table, &name, rest, &raw)?;
            applied.push(name);
        }
        toml::Value::Table(table)
            .try_into()
            .map_err(|e: toml::de::Error| {
                ServerError::new(
                    ErrorCode::ConfigEnv,
                    format!("{} ({})", hide_values(e.message()), applied.join(", ")),
                )
            })
    }

    /// Checks every value. Returns one error listing all problems.
    pub fn validate(&self) -> Result<(), ServerError> {
        let mut problems = Vec::new();
        let s = &self.server;
        if s.roles.is_empty() {
            problems.push(
                "server.roles: list at least one of api, gateway, scheduler, background".to_owned(),
            );
        }
        if !(1..=300).contains(&s.shutdown_grace_seconds) {
            problems.push(format!(
                "server.shutdown_grace_seconds: {} is outside 1–300",
                s.shutdown_grace_seconds
            ));
        }
        if s.mode == Mode::Team {
            let l = &s.listeners;
            // Port 0 asks the OS for a free port, so equal `:0` addresses never clash.
            let clash = |a: SocketAddr, b: SocketAddr| a == b && a.port() != 0;
            if clash(l.api, l.grpc) || clash(l.api, l.internal) || clash(l.grpc, l.internal) {
                problems.push(
                    "server.listeners: api, grpc and internal need different addresses".to_owned(),
                );
            }
        }
        if let Some(tls) = &s.tls {
            for (key, path) in [("cert_path", &tls.cert_path), ("key_path", &tls.key_path)] {
                if !path.is_file() {
                    problems.push(format!(
                        "server.tls.{key}: {} is not a file the server can find",
                        path.display()
                    ));
                }
            }
        }
        for (name, b) in [
            ("principal", s.rate_limit.principal),
            ("org", s.rate_limit.org),
        ] {
            for (field, v) in [("per_second", b.per_second), ("burst", b.burst)] {
                if !(1..=100_000).contains(&v) {
                    problems.push(format!(
                        "server.rate_limit.{name}.{field}: {v} is outside 1–100000"
                    ));
                }
            }
        }
        if let Some(url) = &self.database.url {
            if !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
                // Never echo the value: it may hold a password.
                problems
                    .push("database.url: must start with postgres:// or postgresql://".to_owned());
            }
        }
        let d = &self.database;
        if !(1..=1000).contains(&d.max_connections) {
            problems.push(format!(
                "database.max_connections: {} is outside 1–1000",
                d.max_connections
            ));
        }
        if d.min_connections > d.max_connections {
            problems.push(format!(
                "database.min_connections: {} is more than database.max_connections ({})",
                d.min_connections, d.max_connections
            ));
        }
        if !(1..=300).contains(&d.acquire_timeout_seconds) {
            problems.push(format!(
                "database.acquire_timeout_seconds: {} is outside 1–300",
                d.acquire_timeout_seconds
            ));
        }
        if !(1..=3600).contains(&d.statement_timeout_seconds) {
            problems.push(format!(
                "database.statement_timeout_seconds: {} is outside 1–3600",
                d.statement_timeout_seconds
            ));
        }
        if !(1..=100).contains(&self.pgbundle.backups_keep) {
            problems.push(format!(
                "pgbundle.backups_keep: {} is outside 1–100",
                self.pgbundle.backups_keep
            ));
        }
        if let Some(url) = &self.object_store.url {
            if !(url.starts_with("s3://") || url.starts_with("file:///")) {
                problems.push(format!(
                    "object_store.url: {url:?} must start with s3:// or file:///"
                ));
            }
        }
        if let Some(ep) = &self.object_store.endpoint {
            if !is_http_url(ep) {
                problems.push(format!(
                    "object_store.endpoint: {ep:?} must start with http:// or https://"
                ));
            }
        }
        if let Err(e) = tracing_subscriber::EnvFilter::try_new(&self.telemetry.log_level) {
            problems.push(format!(
                "telemetry.log_level: {:?} is not a log filter the server reads, because {e}",
                self.telemetry.log_level
            ));
        }
        if let Some(ep) = &self.telemetry.otlp_endpoint {
            if !is_http_url(ep) {
                problems.push(format!(
                    "telemetry.otlp_endpoint: {ep:?} must start with http:// or https://"
                ));
            }
        }
        // <mrl:validate P16> P16 checks (push to `problems`) go right below this line.

        // <mrl:validate P17> P17 checks (push to `problems`) go right below this line.

        // <mrl:validate P18> P18 checks (push to `problems`) go right below this line.

        // <mrl:validate P19> P19 checks (push to `problems`) go right below this line.
        if problems.is_empty() {
            Ok(())
        } else {
            Err(ServerError::new(
                ErrorCode::ConfigValue,
                problems.join("; "),
            ))
        }
    }

    /// True when this process takes on `role`.
    pub fn has_role(&self, role: Role) -> bool {
        self.server.roles.contains(&role)
    }
}

/// `line L, column C: <message>` with quoted values hidden.
fn describe(e: &toml::de::Error, text: &str) -> String {
    let at = e
        .span()
        .and_then(|r| text.get(..r.start))
        .map(|before| {
            let line = before.matches('\n').count() + 1;
            let col = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
            format!("line {line}, column {col}: ")
        })
        .unwrap_or_default();
    format!("{at}{}", hide_values(e.message()))
}

/// Drops every value from a parser message, keeping only setting names, grammar and
/// the expected type.
///
/// serde puts values in its "invalid …" and "unknown variant" messages, as `"…"` with
/// escapes or in backticks. Splitting on quotes cannot be made safe, so those messages
/// keep only their final `, expected …` part, which comes from our types, never the file.
/// Any other message that still has a quote is replaced whole.
fn hide_values(msg: &str) -> String {
    const HIDDEN: &str = "<value hidden>";
    for prefix in [
        "invalid type: ",
        "invalid value: ",
        "invalid length ",
        "unknown variant ",
    ] {
        if let Some(rest) = msg.strip_prefix(prefix) {
            // `rfind`: a value may itself contain ", expected ", but the real clause is last.
            let expected = rest.rfind(", expected ").map_or("", |i| &rest[i..]);
            return format!("{prefix}{HIDDEN}{expected}");
        }
    }
    let without_tokens = msg.replace("`\"`", "").replace("`'`", "");
    if without_tokens.contains(['"', '\'']) {
        "the file has a typing mistake here".to_owned()
    } else {
        msg.to_owned()
    }
}

fn is_http_url(s: &str) -> bool {
    let rest = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"));
    rest.is_some_and(|r| !r.is_empty())
}

/// Sets `section.key[.sub…]` from `MRL_SECTION__KEY[__SUB…]`.
fn set_path(table: &mut toml::Table, name: &str, rest: &str, raw: &str) -> Result<(), ServerError> {
    let env_err = |why: &str| ServerError::new(ErrorCode::ConfigEnv, format!("{name}: {why}"));
    let parts: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
    if parts.iter().any(String::is_empty) || parts.len() < 2 {
        return Err(env_err(
            "use MRL_SECTION__KEY, with two underscores between parts",
        ));
    }
    let sections = sections();
    if !sections.contains(&parts[0]) {
        return Err(env_err(&format!(
            "{:?} is not a settings section; use one of {}",
            parts[0],
            sections.join(", ")
        )));
    }
    let (last, path) = parts.split_last().ok_or_else(|| env_err("empty name"))?;
    let mut node = table;
    for key in path {
        let entry = node
            .entry(key.clone())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        node = entry
            .as_table_mut()
            .ok_or_else(|| env_err(&format!("{key:?} is a value, not a section")))?;
    }
    node.insert(last.clone(), parse_env_value(raw));
    Ok(())
}

/// TOML literal if it parses (`8443`, `true`, `["api"]`), otherwise a plain string.
fn parse_env_value(raw: &str) -> toml::Value {
    toml::from_str::<toml::Table>(&format!("v = {raw}"))
        .ok()
        .and_then(|mut t| t.remove("v"))
        .unwrap_or_else(|| toml::Value::String(raw.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn parse(text: &str) -> Result<Config, ServerError> {
        let c = Config::from_toml_with_env(text, Vec::new())?;
        c.validate()?;
        Ok(c)
    }

    fn code(r: Result<Config, ServerError>) -> ErrorCode {
        r.expect_err("expected an error").code
    }

    #[test]
    fn defaults_are_solo_all_roles() {
        let c = parse("").unwrap();
        assert_eq!(c.server.mode, Mode::Solo);
        assert_eq!(c.server.roles, Role::ALL.to_vec());
        assert_eq!(c.server.listeners.api.port(), 8443);
        assert_eq!(c.server.listeners.grpc.port(), 9443);
        assert_eq!(c.server.listeners.internal.port(), 9090);
        assert_eq!(c.server.shutdown_grace_seconds, 30);
    }

    #[test]
    fn dev_sample_is_valid() {
        let text = include_str!("../../config/server.dev.toml");
        parse(text).unwrap();
    }

    #[test]
    fn unknown_keys_are_rejected_everywhere() {
        for text in [
            "bogus = 1",
            "[server]\nbogus = 1",
            "[server.listeners]\nbogus = \"1.2.3.4:1\"",
            "[server.rate_limit.org]\nper_second = 1\nburst = 1\nbogus = 1",
            "[database]\nbogus = 1",
            "[pgbundle]\nbogus = 1",
            "[object_store]\nbogus = 1",
            "[telemetry]\nbogus = 1",
        ] {
            assert_eq!(code(parse(text)), ErrorCode::ConfigParse, "{text}");
        }
    }

    #[test]
    fn mode_values() {
        assert_eq!(
            parse("[server]\nmode = \"team\"").unwrap().server.mode,
            Mode::Team
        );
        assert_eq!(
            parse("[server]\nmode = \"solo\"").unwrap().server.mode,
            Mode::Solo
        );
        for old in ["local", "private", "managed"] {
            assert_eq!(
                code(parse(&format!("[server]\nmode = \"{old}\""))),
                ErrorCode::ConfigParse
            );
        }
    }

    #[test]
    fn roles_values() {
        let c = parse("[server]\nroles = [\"api\", \"background\"]").unwrap();
        assert!(c.has_role(Role::Api) && c.has_role(Role::Background));
        assert!(!c.has_role(Role::Gateway));
        assert_eq!(code(parse("[server]\nroles = []")), ErrorCode::ConfigValue);
        assert_eq!(
            code(parse("[server]\nroles = [\"web\"]")),
            ErrorCode::ConfigParse
        );
    }

    #[test]
    fn listeners_values() {
        let ok = "[server]\nmode = \"team\"\n[server.listeners]\napi = \"10.0.0.1:8443\"";
        assert_eq!(
            parse(ok).unwrap().server.listeners.api.to_string(),
            "10.0.0.1:8443"
        );
        assert_eq!(
            code(parse("[server.listeners]\napi = \"nope\"")),
            ErrorCode::ConfigParse
        );
        let clash = "[server]\nmode = \"team\"\n[server.listeners]\napi = \"0.0.0.0:9443\"";
        assert_eq!(code(parse(clash)), ErrorCode::ConfigValue);
    }

    #[test]
    fn tls_values() {
        let file = std::env::temp_dir().join(format!("mrl-tls-cfg-{}.pem", std::process::id()));
        std::fs::write(&file, "x").unwrap();
        let ok = format!(
            "[server.tls]\ncert_path = {:?}\nkey_path = {:?}",
            file, file
        );
        assert!(parse(&ok).unwrap().server.tls.is_some());
        let missing = "[server.tls]\ncert_path = \"/nope/cert.pem\"\nkey_path = \"/nope/key.pem\"";
        let err = parse(missing).unwrap_err();
        assert_eq!(err.code, ErrorCode::ConfigValue);
        assert!(
            err.detail.contains("server.tls.cert_path")
                && err.detail.contains("server.tls.key_path")
        );
        assert_eq!(
            code(parse("[server.tls]\ncert_path = \"/a\"")),
            ErrorCode::ConfigParse
        );
        assert_eq!(
            code(parse(
                "[server.tls]\ncert_path = \"/a\"\nkey_path = \"/b\"\nca = 1"
            )),
            ErrorCode::ConfigParse
        );
        std::fs::remove_file(&file).unwrap();
    }

    #[test]
    fn shutdown_grace_values() {
        assert!(parse("[server]\nshutdown_grace_seconds = 1").is_ok());
        assert!(parse("[server]\nshutdown_grace_seconds = 300").is_ok());
        assert_eq!(
            code(parse("[server]\nshutdown_grace_seconds = 0")),
            ErrorCode::ConfigValue
        );
        assert_eq!(
            code(parse("[server]\nshutdown_grace_seconds = 301")),
            ErrorCode::ConfigValue
        );
    }

    #[test]
    fn rate_limit_values() {
        let ok = "[server.rate_limit.principal]\nper_second = 5\nburst = 10";
        assert_eq!(parse(ok).unwrap().server.rate_limit.principal.burst, 10);
        let zero = "[server.rate_limit.org]\nper_second = 0\nburst = 10";
        assert_eq!(code(parse(zero)), ErrorCode::ConfigValue);
        let missing = "[server.rate_limit.org]\nper_second = 5";
        assert_eq!(code(parse(missing)), ErrorCode::ConfigParse);
    }

    #[test]
    fn database_pool_values() {
        let c = parse("").unwrap();
        assert_eq!(c.database.max_connections, 10);
        assert!(c.database.auto_migrate);
        assert_eq!(c.pgbundle.backups_keep, 7);
        for bad in [
            "[database]\nmax_connections = 0",
            "[database]\nmax_connections = 2\nmin_connections = 3",
            "[database]\nacquire_timeout_seconds = 0",
            "[database]\nstatement_timeout_seconds = 3601",
            "[pgbundle]\nbackups_keep = 0",
        ] {
            assert_eq!(code(parse(bad)), ErrorCode::ConfigValue, "{bad}");
        }
        let c = Config::from_toml_with_env(
            "",
            env(&[
                ("MRL_DATABASE__AUTO_MIGRATE", "false"),
                ("MRL_PGBUNDLE__DATA_DIR", "/data/mrl"),
            ]),
        )
        .unwrap();
        assert!(!c.database.auto_migrate);
        assert_eq!(c.pgbundle.data_dir, Some(PathBuf::from("/data/mrl")));
    }

    #[test]
    fn database_url_values() {
        assert!(parse("[database]\nurl = \"postgres://u:p@h/db\"").is_ok());
        let err = parse("[database]\nurl = \"mysql://u:secretpw@h/db\"").unwrap_err();
        assert_eq!(err.code, ErrorCode::ConfigValue);
        assert!(
            !err.detail.contains("secretpw"),
            "password leaked: {}",
            err.detail
        );
    }

    #[test]
    fn parse_errors_never_echo_values() {
        for text in [
            "[database]\nurl = \"postgres://u:hunter2@h/db",
            "[database]\nurl = postgres://u:hunter2@h/db",
            "[server]\nshutdown_grace_seconds = \"hunter2\"",
            "[database]\nurl = [\"postgres://u:hunter2@h/db\"]",
        ] {
            let err = parse(text).unwrap_err();
            assert_eq!(err.code, ErrorCode::ConfigParse, "{text}");
            assert!(!err.detail.contains("hunter2"), "leaked: {}", err.detail);
            assert!(err.detail.starts_with("line 2, column "), "{}", err.detail);
        }
        let env_err = Config::from_toml_with_env(
            "",
            env(&[("MRL_SERVER__SHUTDOWN_GRACE_SECONDS", "hunter2")]),
        )
        .unwrap_err();
        assert!(
            !env_err.detail.contains("hunter2"),
            "leaked: {}",
            env_err.detail
        );
        let unknown = parse("[server]\nbogus = 1").unwrap_err();
        assert!(
            unknown.detail.contains("bogus"),
            "names stay: {}",
            unknown.detail
        );
    }

    #[test]
    fn hide_values_drops_every_value() {
        let cases = [
            (
                r#"invalid type: string "pw", expected u64"#,
                "invalid type: <value hidden>, expected u64",
            ),
            (
                r#"invalid type: string "pre\"pw", expected u64"#,
                "invalid type: <value hidden>, expected u64",
            ),
            (
                r#"invalid type: string "pw, expected u64", expected u64"#,
                "invalid type: <value hidden>, expected u64",
            ),
            (
                "invalid type: floating point `1.5`, expected u64",
                "invalid type: <value hidden>, expected u64",
            ),
            (
                "invalid value: integer `7`, expected a port",
                "invalid value: <value hidden>, expected a port",
            ),
            (
                "unknown variant `pw`, expected `solo` or `team`",
                "unknown variant <value hidden>, expected `solo` or `team`",
            ),
            (
                "unknown field `bogus`, expected `mode`",
                "unknown field `bogus`, expected `mode`",
            ),
            ("missing field `burst`", "missing field `burst`"),
            (
                "invalid basic string, expected `\"`",
                "invalid basic string, expected `\"`",
            ),
            (
                r#"something "pw" else"#,
                "the file has a typing mistake here",
            ),
            ("odd 'pw", "the file has a typing mistake here"),
        ];
        for (raw, want) in cases {
            assert_eq!(hide_values(raw), want, "{raw}");
        }
    }

    #[test]
    fn embedded_quotes_and_backticked_values_never_leak() {
        const PW: &str = "review-password";
        for text in [
            "[server]\nshutdown_grace_seconds = 'prefix\"review-password'",
            "[server]\nshutdown_grace_seconds = \"a\\\"review-password\"",
            "[server]\nshutdown_grace_seconds = \"review-password, expected u64\"",
            "[server]\nmode = \"review-password\"",
            "[server]\nmode = 'x\"review-password'",
            "[server]\nroles = ['review-password']",
            "[server.listeners]\napi = \"review-password\"",
            "[database]\nurl = 'review-password",
            "[database]\nurl = [\"review-password\"]",
        ] {
            let err = parse(text).unwrap_err();
            assert!(!err.detail.contains(PW), "leaked: {text} → {}", err.detail);
            assert!(err.detail.starts_with("line 2, column "), "{}", err.detail);
        }
        let err = parse("[server]\nmode = \"review-password\"").unwrap_err();
        assert!(
            err.detail.contains("expected `solo` or `team`"),
            "{}",
            err.detail
        );
        for raw in ["x\"review-password", "'review-password", "review-password"] {
            let env_err =
                Config::from_toml_with_env("", env(&[("MRL_SERVER__MODE", raw)])).unwrap_err();
            assert!(
                !env_err.detail.contains(PW),
                "leaked: {raw} → {}",
                env_err.detail
            );
        }
    }

    #[test]
    fn database_url_is_redacted_in_debug() {
        let c = parse("[database]\nurl = \"postgres://u:secretpw@h/db\"").unwrap();
        assert!(!format!("{c:?}").contains("secretpw"));
    }

    #[test]
    fn object_store_values() {
        assert!(
            parse("[object_store]\nurl = \"s3://b/p\"\nendpoint = \"http://127.0.0.1:8333\"")
                .is_ok()
        );
        assert!(parse("[object_store]\nurl = \"file:///var/mrl\"").is_ok());
        assert_eq!(
            code(parse("[object_store]\nurl = \"ftp://x\"")),
            ErrorCode::ConfigValue
        );
        assert_eq!(
            code(parse("[object_store]\nendpoint = \"127.0.0.1\"")),
            ErrorCode::ConfigValue
        );
    }

    #[test]
    fn telemetry_values() {
        assert!(parse("[telemetry]\nlog_level = \"info,mrl_server=debug\"").is_ok());
        assert!(parse("[telemetry]\notlp_endpoint = \"http://127.0.0.1:4317\"").is_ok());
        assert_eq!(
            code(parse("[telemetry]\nlog_level = \"info,=[\"")),
            ErrorCode::ConfigValue
        );
        assert_eq!(
            code(parse("[telemetry]\notlp_endpoint = \"udp://x\"")),
            ErrorCode::ConfigValue
        );
    }

    #[test]
    fn env_overrides_apply_and_type() {
        let c = Config::from_toml_with_env(
            "[server]\nmode = \"solo\"",
            env(&[
                ("MRL_SERVER__MODE", "team"),
                ("MRL_SERVER__SHUTDOWN_GRACE_SECONDS", "5"),
                ("MRL_SERVER__ROLES", "[\"api\"]"),
                ("MRL_SERVER__LISTENERS__API", "127.0.0.1:1234"),
                ("MRL_DATABASE__URL", "postgres://x/y"),
                ("MRL_LAUNCH_SECRET", "not-a-setting"),
                ("PATH", "/bin"),
            ]),
        )
        .unwrap();
        assert_eq!(c.server.mode, Mode::Team);
        assert_eq!(c.server.shutdown_grace_seconds, 5);
        assert_eq!(c.server.roles, vec![Role::Api]);
        assert_eq!(c.server.listeners.api.port(), 1234);
        assert_eq!(c.database.url.as_deref(), Some("postgres://x/y"));
    }

    #[test]
    fn env_overrides_reject_unknown_names() {
        for (k, v) in [
            ("MRL_NOPE__KEY", "1"),
            ("MRL_SERVER__BOGUS", "1"),
            ("MRL_SERVER__", "1"),
            ("MRL_SERVER__MODE__X", "1"),
            ("MRL_SERVER__MODE", "managed"),
        ] {
            let r = Config::from_toml_with_env("[server]\nmode = \"solo\"", env(&[(k, v)]));
            assert_eq!(code(r), ErrorCode::ConfigEnv, "{k}");
        }
    }

    #[test]
    fn every_settings_section_takes_env_overrides() {
        // Parts add sections below their markers, so check a superset.
        let found = sections();
        for s in [
            "server",
            "database",
            "pgbundle",
            "object_store",
            "telemetry",
        ] {
            assert!(found.iter().any(|f| f == s), "{s} missing from {found:?}");
        }
        let c = Config::from_toml_with_env("", env(&[("MRL_TELEMETRY__LOG_LEVEL", "debug")]));
        assert!(c.is_ok(), "{c:?}");
    }

    #[test]
    fn cli_overrides_win() {
        let dir = std::env::temp_dir().join(format!("mrl-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.toml");
        std::fs::write(&path, "[server]\nmode = \"solo\"").unwrap();
        let cli = Overrides {
            mode: Some(Mode::Team),
            roles: Some(vec![Role::Scheduler]),
        };
        let c = Config::load(Some(&path), env(&[("MRL_SERVER__MODE", "solo")]), &cli).unwrap();
        assert_eq!(c.server.mode, Mode::Team);
        assert_eq!(c.server.roles, vec![Role::Scheduler]);
        let missing = Config::load(Some(&dir.join("none.toml")), Vec::new(), &cli);
        assert_eq!(code(missing), ErrorCode::ConfigUnreadable);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
