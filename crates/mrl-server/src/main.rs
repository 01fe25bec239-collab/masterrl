//! The MasterRL core: API, scheduler, registries and events. Run by the desktop app.

use clap::{Parser, Subcommand};
use mrl_server::config::{Config, Mode, Overrides, Role};
use mrl_server::error::{ErrorCode, ServerError};
use mrl_server::seams::{Seams, LAUNCH_SECRET_ENV};
use mrl_server::{app, openapi, registry, telemetry, App};
use std::path::PathBuf;
use std::process::ExitCode;

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
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the OpenAPI document as YAML.
    Openapi,
}

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

async fn run(cli: Cli) -> Result<(), ServerError> {
    if let Some(Command::Openapi) = cli.command {
        let yaml = openapi::document(&registry::modules())
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
        ));
    }
    let telemetry = telemetry::init(&config.telemetry)?;
    let (secret, generated) = launch_secret(&env);
    let seams = Seams::production(mode, &secret).build(mode)?;
    let running = App::new(config, seams, registry::modules()).start().await?;
    if !cli.headless {
        println!(
            "{}",
            listening_line(&running, generated.then_some(secret.as_str()))
        );
    }
    let token = running.shutdown_token();
    tokio::select! {
        () = app::shutdown_signal() => token.cancel(),
        () = token.cancelled() => {},
    }
    let result = running.wait().await;
    telemetry.shutdown();
    result
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
        ])
        .unwrap();
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
