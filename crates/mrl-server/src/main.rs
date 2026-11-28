//! The MasterRL core: API, scheduler, registries and events. Run by the desktop app.

use clap::{Parser, Subcommand};
use mrl_server::config::{Config, DatabaseConfig, Mode, Overrides, Role};
use mrl_server::db::{self, Db, DbError};
use mrl_server::error::{ErrorCode, ServerError};
use mrl_server::modules::auth::production_seams;
use mrl_server::modules::keychain::KeychainStore;
use mrl_server::pgbundle::{
    self, BundleError, BundleOptions, FileSecretStore, PgBinaries, PgBundle, PASSWORD_SECRET,
};
use mrl_server::seams::{SecretStore, LAUNCH_SECRET_ENV};
use mrl_server::{app, openapi, registry, telemetry, App};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgConnectOptions;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const NAME: &str = env!("CARGO_BIN_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
// `--exclude=*` skips all tags, so `--always` yields the commit SHA.
const GIT_SHA: &str = git_version::git_version!(
    args = ["--always", "--abbrev=12", "--dirty=-dirty", "--exclude=*"],
    fallback = "unknown"
);

/// The MasterRL server.
#[derive(Debug, Parser)]
#[command(name = "mrl-server", disable_version_flag = true)]
struct Cli {
    /// Show the version and git commit.
    #[arg(short = 'V', long)]
    version: bool,
    /// Settings file (TOML). Without it, defaults and MRL_* variables apply.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    /// Run mode; overrides `server.mode`.
    #[arg(long, value_enum)]
    mode: Option<Mode>,
    /// Comma-separated roles; overrides `server.roles`.
    #[arg(long, value_enum, value_delimiter = ',')]
    roles: Option<Vec<Role>>,
    /// Team mode without the desktop app: no startup line on stdout.
    #[arg(long)]
    headless: bool,
    /// Also stop gracefully when stdin closes (how the desktop app stops the core on every OS).
    #[arg(long)]
    stop_on_stdin_close: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the OpenAPI document as YAML.
    Openapi,
    /// Apply pending database migrations (backing up the bundled database first), then exit.
    Migrate {
        /// Only list the pending migrations.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Why the process stopped with an error.
#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Bundle(#[from] BundleError),
}

impl Failure {
    fn explain(&self) -> String {
        match self {
            Self::Server(e) => e.explain(),
            Self::Db(e) => e.explain(),
            Self::Bundle(e) => e.explain(),
        }
    }
}

/// The dev fallback when no PostgreSQL programs are found (the `deploy/local` stack).
const DEV_DATABASE_URL_ENV: &str = "DATABASE_URL";

fn version_line() -> String {
    format!("{NAME} {VERSION} ({GIT_SHA})")
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.version {
        println!("{}", version_line());
        return ExitCode::SUCCESS;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{NAME}: could not start the async runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{NAME}: {}", e.explain());
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Failure> {
    let stop = stop_requests(cli.stop_on_stdin_close);
    if let Some(Command::Openapi) = cli.command {
        // The document needs no database; this pool never connects.
        let db = Db::connect_lazy(&DatabaseConfig::default(), PgConnectOptions::new());
        let yaml = openapi::document(&registry::modules(&db, &Config::default()))
            .to_yaml()
            .map_err(|e| ServerError::new(ErrorCode::Internal, format!("openapi document: {e}")))?;
        print!("{yaml}");
        return Ok(());
    }
    // Non-UTF-8 variables cannot be settings; skip them instead of panicking.
    let env: Vec<(String, String)> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let overrides = Overrides {
        mode: cli.mode,
        roles: cli.roles,
    };
    let config = Config::load(cli.config.as_deref(), env.iter().cloned(), &overrides)?;
    let mode = config.server.mode;
    if cli.headless && mode == Mode::Solo {
        return Err(ServerError::new(
            ErrorCode::ConfigValue,
            "--headless works only with --mode team; solo mode is started by the desktop app",
        )
        .into());
    }
    let telemetry = telemetry::init(&config.telemetry)?;
    let secrets_dir = pgbundle::data_dir(&config.pgbundle)?.join("secrets");
    let (db, bundle) = open_database(&config, &env, &secrets_dir).await?;
    // Before any migration runs, so SQLx's checksum check passes (see the function); only on
    // runs that may write migration metadata.
    let repaired = if writes_migrations(cli.command.as_ref(), db.auto_migrate()) {
        mrl_server::modules::auth::repair_migration_checksums(&db)
            .await
            .map_err(Failure::from)
    } else {
        Ok(())
    };
    let result = match (repaired, cli.command) {
        (Err(e), _) => Err(e),
        (Ok(()), Some(Command::Migrate { dry_run })) => migrate(&db, dry_run).await,
        (Ok(()), _) => {
            let service = &config.auth.keychain_service;
            let secrets: Arc<dyn SecretStore> =
                Arc::new(KeychainStore::open(service, &secrets_dir, config.keychain.backend).await);
            serve(
                config,
                &env,
                cli.headless,
                db,
                bundle.as_ref(),
                secrets,
                stop,
            )
            .await
        }
    };
    let stopped = match bundle {
        Some(b) => b.stop().await.map_err(Failure::from),
        None => Ok(()),
    };
    telemetry.shutdown();
    result.and(stopped)
}

async fn serve(
    config: Config,
    env: &[(String, String)],
    headless: bool,
    db: Db,
    bundle: Option<&PgBundle>,
    secrets: Arc<dyn SecretStore>,
    stop: CancellationToken,
) -> Result<(), Failure> {
    let mode = config.server.mode;
    let (secret, generated) = launch_secret(env);
    let seams = production_seams(&config, &db, secrets, &secret)
        .await?
        .build(mode)?;
    let modules = registry::modules(&db, &config);
    let running = App::new(config, seams, modules).start().await?;
    if !headless {
        println!(
            "{}",
            listening_line(&running, generated.then_some(secret.as_str()))
        );
    }
    let token = running.shutdown_token();
    let db_failed = bundle.map(PgBundle::failed).unwrap_or_default();
    tokio::select! {
        () = stop.cancelled() => token.cancel(),
        () = token.cancelled() => {},
        () = db_failed.cancelled() => token.cancel(),
    }
    running.wait().await?;
    Ok(())
}

/// Cancelled on Ctrl-C, SIGTERM or (with `stdin`) stdin closing. Listening starts before the
/// database opens, so a stop during start-up still stops PostgreSQL.
fn stop_requests(stdin: bool) -> CancellationToken {
    let stop = CancellationToken::new();
    let on_signal = stop.clone();
    tokio::spawn(async move {
        app::shutdown_signal().await;
        on_signal.cancel();
    });
    if stdin {
        let on_close = stop.clone();
        // A plain thread: a tokio stdin read can't be cancelled and would hold up exit.
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
            on_close.cancel();
        });
    }
    stop
}

/// `migrate [--dry-run]`.
async fn migrate(db: &Db, dry_run: bool) -> Result<(), Failure> {
    let pending = db::migrate::pending(db, &db::migrator()).await?;
    if pending.is_empty() {
        println!("The database is up to date.");
        return Ok(());
    }
    for m in &pending {
        println!("{} {}", m.version, m.description);
    }
    if dry_run {
        println!("{} database update(s) would be applied.", pending.len());
        return Ok(());
    }
    let applied = db::migrate::apply(db).await?;
    println!("Applied {} database update(s).", applied.len());
    Ok(())
}

/// `database.url` if set (team mode needs it). In solo mode without it, the bundled
/// PostgreSQL; when its programs are not found and `pgbundle.bin_dir` is unset, the
/// `DATABASE_URL` of the dev stack (`just dev`).
async fn open_database(
    config: &Config,
    env: &[(String, String)],
    secrets_dir: &std::path::Path,
) -> Result<(Db, Option<PgBundle>), Failure> {
    let lazy = |url: &str| -> Result<Db, Failure> {
        let options: PgConnectOptions = url.parse().map_err(|_| {
            // Never echo the value: it may hold a password.
            ServerError::new(
                ErrorCode::ConfigValue,
                "database.url: not a PostgreSQL address the server can read",
            )
        })?;
        Ok(Db::connect_lazy(&config.database, options))
    };
    if let Some(url) = &config.database.url {
        return Ok((lazy(url)?, None));
    }
    if config.server.mode == Mode::Team {
        return Err(ServerError::new(
            ErrorCode::ConfigValue,
            "database.url: team mode needs the address of its PostgreSQL database",
        )
        .into());
    }
    let p = &config.pgbundle;
    let binaries = match PgBinaries::locate(p.bin_dir.as_deref()).await {
        Ok(b) => b,
        Err(e) if p.bin_dir.is_none() => {
            match env
                .iter()
                .find(|(k, v)| k == DEV_DATABASE_URL_ENV && !v.is_empty())
            {
                Some((_, url)) => {
                    tracing::warn!("no PostgreSQL programs found; using DATABASE_URL (dev stack)");
                    return Ok((lazy(url)?, None));
                }
                None => return Err(e.into()),
            }
        }
        Err(e) => return Err(e.into()),
    };
    let secrets = bundle_secrets(
        &config.auth.keychain_service,
        config.keychain.backend,
        &pgbundle::data_dir(p)?,
        secrets_dir,
    )
    .await?;
    let options = BundleOptions::from_config(p, binaries, secrets).await?;
    let bundle = PgBundle::start(options).await?;
    let db =
        Db::connect_lazy(&config.database, bundle.connect_options()).with_backups(bundle.backups());
    Ok((db, Some(bundle)))
}

/// True when this run may write migration metadata: `migrate` without `--dry-run`, or
/// serving with `database.auto_migrate`.
fn writes_migrations(command: Option<&Command>, auto_migrate: bool) -> bool {
    match command {
        Some(Command::Migrate { dry_run }) => !dry_run,
        _ => auto_migrate,
    }
}

/// The bundled database's password lives in the keychain under a service named after the
/// data folder, so two data folders never share one. A password an older build kept in
/// `<data>/secrets/` moves to the keychain once. After a switch to `keychain.backend = "file"`
/// the password is copied once from the OS keychain, so the existing database still opens.
async fn bundle_secrets(
    service: &str,
    backend: mrl_server::modules::keychain::KeychainBackend,
    data_dir: &std::path::Path,
    secrets_dir: &std::path::Path,
) -> Result<Arc<dyn SecretStore>, Failure> {
    let digest = Sha256::digest(data_dir.to_string_lossy().as_bytes());
    let tag: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    let name = format!("{service}-db-{tag}");
    let store = KeychainStore::open(&name, secrets_dir, backend).await;
    let fail = |e: mrl_server::seams::SeamError| {
        ServerError::new(ErrorCode::StartupStep, format!("database password: {e}"))
    };
    let old = FileSecretStore::new(secrets_dir);
    let os = KeychainStore::os(&name);
    let mut from: Vec<(&str, &dyn SecretStore)> = vec![("old secrets folder", &old)];
    if backend == mrl_server::modules::keychain::KeychainBackend::File {
        from.push(("OS keychain", &os));
    }
    let carried = mrl_server::modules::keychain::carry_secret(PASSWORD_SECRET, &store, &from)
        .await
        .map_err(fail)?;
    match carried.as_deref() {
        Some("old secrets folder") => {
            old.delete(PASSWORD_SECRET).await.map_err(fail)?;
            tracing::info!("moved the database password into the keychain");
        }
        Some(source) => tracing::info!(source, "copied the database password to the chosen store"),
        None => {}
    }
    Ok(Arc::new(store))
}

/// The desktop app passes the secret in `MRL_LAUNCH_SECRET`. Started by hand (development),
/// the server makes a random one and prints it once in the startup line, never in logs.
fn launch_secret(env: &[(String, String)]) -> (String, bool) {
    match env.iter().find(|(k, _)| k == LAUNCH_SECRET_ENV) {
        Some((_, v)) if !v.is_empty() => (v.clone(), false),
        _ => {
            let a = uuid::Uuid::new_v4().simple().to_string();
            let b = uuid::Uuid::new_v4().simple().to_string();
            (format!("{a}{b}"), true)
        }
    }
}

/// One JSON line on stdout for the desktop app: `{"event":"listening","port":N}`.
fn listening_line(running: &mrl_server::Running, secret: Option<&str>) -> String {
    let mut line = serde_json::json!({ "event": "listening" });
    if let Some(a) = running.api_addr() {
        line["port"] = a.port().into();
    }
    if let Some(a) = running.grpc_addr() {
        line["grpc_port"] = a.port().into();
    }
    if let Some(a) = running.internal_addr() {
        line["internal_port"] = a.port().into();
    }
    if let Some(s) = secret {
        line["launch_secret"] = s.into();
    }
    line.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_runs_that_migrate_repair_migration_metadata() {
        let dry = Command::Migrate { dry_run: true };
        let real = Command::Migrate { dry_run: false };
        assert!(
            !writes_migrations(Some(&dry), true),
            "a dry run never writes"
        );
        assert!(
            writes_migrations(Some(&real), false),
            "an explicit migrate does"
        );
        assert!(writes_migrations(None, true));
        assert!(
            !writes_migrations(None, false),
            "no auto_migrate, no writes at startup"
        );
    }

    #[test]
    fn version_shows_name_version_and_sha() {
        assert_eq!(version_line(), format!("mrl-server {VERSION} ({GIT_SHA})"));
        let sha = GIT_SHA.trim_end_matches("-dirty");
        assert!(
            sha == "unknown" || (sha.len() == 12 && sha.bytes().all(|b| b.is_ascii_hexdigit()))
        );
        assert!(
            Cli::try_parse_from(["mrl-server", "--version"])
                .unwrap()
                .version
        );
        assert!(Cli::try_parse_from(["mrl-server", "-V"]).unwrap().version);
    }

    #[test]
    fn flags_parse() {
        let cli = Cli::try_parse_from([
            "mrl-server",
            "--config",
            "a.toml",
            "--mode",
            "team",
            "--roles",
            "api,background",
            "--headless",
            "--stop-on-stdin-close",
        ])
        .unwrap();
        assert!(cli.stop_on_stdin_close);
        assert_eq!(cli.mode, Some(Mode::Team));
        assert_eq!(cli.roles, Some(vec![Role::Api, Role::Background]));
        assert!(cli.headless);
        assert!(Cli::try_parse_from(["mrl-server", "--mode", "local"]).is_err());
        assert!(Cli::try_parse_from(["mrl-server", "--bogus"]).is_err());
        assert!(matches!(
            Cli::try_parse_from(["mrl-server", "openapi"])
                .unwrap()
                .command,
            Some(Command::Openapi)
        ));
        assert!(matches!(
            Cli::try_parse_from(["mrl-server", "migrate", "--dry-run"])
                .unwrap()
                .command,
            Some(Command::Migrate { dry_run: true })
        ));
        assert!(matches!(
            Cli::try_parse_from(["mrl-server", "migrate"])
                .unwrap()
                .command,
            Some(Command::Migrate { dry_run: false })
        ));
    }

    #[test]
    fn launch_secret_from_env_or_generated() {
        let env = vec![(LAUNCH_SECRET_ENV.to_owned(), "abc".to_owned())];
        assert_eq!(launch_secret(&env), ("abc".to_owned(), false));
        let (s, generated) = launch_secret(&[]);
        assert!(generated && s.len() == 64);
        assert_ne!(launch_secret(&[]).0, s);
    }
}
