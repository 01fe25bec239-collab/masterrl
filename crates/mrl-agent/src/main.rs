//! The MasterRL machine agent: runs training jobs on this computer or a remote one.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use mrl_agent::config::{AgentConfig, SandboxChoice};
use mrl_agent::exec::{ExecSpec, Executor, Limits};
use mrl_agent::{hw, registry, sandbox, service, AgentContext, AgentError};
use tokio_util::sync::CancellationToken;

const NAME: &str = env!("CARGO_BIN_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
// `--exclude=*` skips all tags, so `--always` yields the commit SHA.
const GIT_SHA: &str = git_version::git_version!(
    args = ["--always", "--abbrev=12", "--dirty=-dirty", "--exclude=*"],
    fallback = "unknown"
);

/// The MasterRL machine agent.
#[derive(Debug, Parser)]
#[command(name = "mrl-agent", disable_version_flag = true)]
struct Cli {
    /// Show the version and git commit.
    #[arg(short = 'V', long)]
    version: bool,
    /// Settings file (TOML). Default: $MRL_AGENT_CONFIG, then the OS settings folder.
    #[arg(long, value_name = "PATH", global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print this computer's hardware inventory.
    Inventory {
        /// Print JSON instead of a summary.
        #[arg(long)]
        json: bool,
        /// Print the raw NVML readings as JSON (for recording test fixtures).
        #[arg(long, conflicts_with = "json")]
        nvml_snapshot: bool,
    },
    /// Run the agent in the foreground (the background service runs this).
    Run,
    /// Debug: run one command here under a sandbox profile and limits.
    RunLocal {
        /// Sandbox profile.
        #[arg(long, default_value = "data_prep")]
        profile: String,
        /// Sandbox backend; overrides `sandbox.backend`.
        #[arg(long, value_enum)]
        sandbox: Option<SandboxChoice>,
        /// Memory limit, like `512M` or `2G`.
        #[arg(long)]
        memory: Option<String>,
        /// CPU limit in cores, like `1.5`.
        #[arg(long)]
        cpus: Option<f64>,
        /// Process and thread limit.
        #[arg(long)]
        pids: Option<u64>,
        /// GPUs to expose (comma-separated indexes or uuids).
        #[arg(long, value_delimiter = ',')]
        gpus: Vec<String>,
        /// Job folder (work/, staging/, logs/). Default: a new one in the state folder.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// The command, after `--`.
        #[arg(last = true, required = true)]
        cmd: Vec<String>,
    },
    /// Install, remove or check the background service.
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    // <mrl:cli-commands P18> P18 subcommands go right below this line.
    /// Join this machine to the app with a one-time token (dial the app, or wait for it).
    Enroll {
        /// The app's gateway `host:port` (or tunnel hostname); the agent dials it.
        #[arg(long, conflicts_with = "listen", required_unless_present = "listen")]
        server: Option<String>,
        /// Wait for the app to connect on this address instead (SSH, LAN, tunnel).
        #[arg(long)]
        listen: Option<std::net::SocketAddr>,
        /// The `mrlj_…` join token from the app.
        #[arg(long)]
        token: String,
        /// How to reach the app: direct, tailscale, cloudflare-tunnel (or ssh with --listen).
        #[arg(long, value_enum, default_value = "direct")]
        transport: mrl_agent::transports::TransportKind,
        /// Replace an existing identity.
        #[arg(long)]
        force: bool,
    },
    /// Check, step by step, that this machine can reach the app.
    ConnectionTest,
    // <mrl:cli-commands P20> P20 subcommands go right below this line.
}

#[derive(Debug, Subcommand)]
enum ServiceAction {
    /// Start the agent with the computer (user-level, no admin rights).
    Install,
    /// Stop the agent and remove the service.
    Uninstall,
    /// Show whether the service is installed and running.
    Status,
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
    let Some(command) = cli.command else {
        eprintln!("{NAME}: choose a command; run `{NAME} --help` to see them");
        return ExitCode::from(2);
    };
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
    match runtime.block_on(dispatch(cli.config, command)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("{NAME}: {}", e.explain());
            ExitCode::FAILURE
        }
    }
}

fn json<T: serde::Serialize>(value: &T) -> Result<String, AgentError> {
    serde_json::to_string_pretty(value).map_err(|e| AgentError::Exec {
        program: "json output".into(),
        reason: e.to_string(),
    })
}

async fn dispatch(config: Option<PathBuf>, command: Command) -> Result<ExitCode, AgentError> {
    match command {
        Command::Inventory {
            json: as_json,
            nvml_snapshot,
        } => {
            if nvml_snapshot {
                let snap = hw::nvml::NvmlBackend::new().raw_snapshot()?;
                println!("{}", json(&snap)?);
                return Ok(ExitCode::SUCCESS);
            }
            let inv = hw::collect_inventory(&hw::registry::backends());
            if as_json {
                println!("{}", json(&inv)?);
            } else {
                print!("{}", summary(&inv));
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Run => {
            let (cfg, _) = AgentConfig::load(config.as_deref())?;
            let ctx = Arc::new(AgentContext::from_config(cfg)?);
            let shutdown = CancellationToken::new();
            let stop = shutdown.clone();
            tokio::spawn(async move {
                wait_for_signal().await;
                stop.cancel();
            });
            mrl_agent::run_agent(ctx, registry::services(), shutdown).await?;
            Ok(ExitCode::SUCCESS)
        }
        Command::RunLocal {
            profile,
            sandbox: choice,
            memory,
            cpus,
            pids,
            gpus,
            dir,
            cmd,
        } => {
            let (mut cfg, _) = AgentConfig::load(config.as_deref())?;
            if let Some(c) = choice {
                cfg.sandbox.backend = c;
            }
            let limits = Limits {
                cpu_millis: cpus.map(|c| (c * 1000.0).round() as u64),
                memory_bytes: memory
                    .as_deref()
                    .map(|m| {
                        hw::wsl::parse_size(m).ok_or_else(|| AgentError::UnknownName {
                            kind: "memory size",
                            name: m.to_owned(),
                            known: "a number with K, M, G or T, like 512M".into(),
                        })
                    })
                    .transpose()?,
                pids,
            };
            run_local(cfg, &profile, limits, gpus, dir, cmd).await
        }
        // <mrl:cli-dispatch P18> P18 match arms go right below this line.
        Command::Enroll {
            server,
            listen,
            token,
            transport,
            force,
        } => {
            let (cfg, _) = AgentConfig::load(config.as_deref())?;
            let ctx = AgentContext::from_config(cfg)?;
            let kind = transport;
            let enrolled = match (server, listen) {
                (Some(s), _) => mrl_agent::enroll::enroll_dial(&ctx, &s, &token, kind, force).await,
                (None, Some(l)) => {
                    let stop = CancellationToken::new();
                    let s2 = stop.clone();
                    tokio::spawn(async move {
                        wait_for_signal().await;
                        s2.cancel();
                    });
                    mrl_agent::enroll::enroll_listen(&ctx, l, &token, kind, force, stop).await
                }
                (None, None) => Err(mrl_agent::session::LinkError::Token(
                    "give --server or --listen".into(),
                )),
            };
            match enrolled {
                Ok(id) => {
                    println!(
                        "This machine joined the app as {}. Start the agent with `{NAME} run`.",
                        id.link.node_id
                    );
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => {
                    eprintln!("{NAME}: {}", e.explain());
                    Ok(ExitCode::FAILURE)
                }
            }
        }
        Command::ConnectionTest => {
            let (cfg, _) = AgentConfig::load(config.as_deref())?;
            let state = mrl_agent::state::StateDir::open(&cfg.state_dir)?;
            let id = match mrl_agent::enroll::Identity::load(state.root()) {
                Ok(Some(id)) => id,
                Ok(None) => {
                    eprintln!("{NAME}: {} this machine has not joined the app yet; run `{NAME} enroll` first", mrl_agent::session::codes::NOT_ENROLLED);
                    return Ok(ExitCode::FAILURE);
                }
                Err(e) => {
                    eprintln!("{NAME}: {}", e.explain());
                    return Ok(ExitCode::FAILURE);
                }
            };
            let report = match mrl_agent::session::transport_for(&cfg.gateway, &id) {
                Ok(t) => t.connection_test().await,
                Err(e) => {
                    eprintln!("{NAME}: {}", e.explain());
                    return Ok(ExitCode::FAILURE);
                }
            };
            for s in &report.steps {
                println!(
                    "{} {}: {}",
                    if s.ok { "ok  " } else { "FAIL" },
                    s.name,
                    s.detail
                );
            }
            Ok(if report.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }

        // <mrl:cli-dispatch P20> P20 match arms go right below this line.
        Command::Service { action } => {
            let (cfg, file) = AgentConfig::load(config.as_deref())?;
            let state = mrl_agent::state::StateDir::open(&cfg.state_dir)?;
            let manager = service::detect()?;
            let target = service::Target::current(&file, &state.logs_dir())?;
            match action {
                ServiceAction::Install => {
                    service::ensure_config_file(&file)?;
                    let plan = service::install_plan(manager, &target)?;
                    for w in service::apply(&plan)? {
                        eprintln!("{NAME}: note: {w}");
                    }
                    println!("Installed the {manager:?} service; it starts with your computer.");
                }
                ServiceAction::Uninstall => {
                    for w in service::apply(&service::uninstall_plan(manager, &target))? {
                        eprintln!("{NAME}: note: {w}");
                    }
                    println!("Removed the {manager:?} service.");
                }
                ServiceAction::Status => {
                    let alive = state
                        .read(mrl_agent::PID_FILE)?
                        .and_then(|b| String::from_utf8(b).ok())
                        .and_then(|s| s.trim().parse::<u32>().ok())
                        .is_some_and(pid_alive);
                    println!("{}", json(&service::status(manager, &target, alive))?);
                }
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        i32::try_from(pid)
            .is_ok_and(|p| nix::sys::signal::kill(nix::unistd::Pid::from_raw(p), None).is_ok())
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run_local(
    cfg: AgentConfig,
    profile: &str,
    limits: Limits,
    gpus: Vec<String>,
    dir: Option<PathBuf>,
    cmd: Vec<String>,
) -> Result<ExitCode, AgentError> {
    let state = mrl_agent::state::StateDir::open(&cfg.state_dir)?;
    let profile = sandbox::profile(profile)?;
    let backend = sandbox::select(&cfg.sandbox)?;
    let executor = mrl_agent::exec::process::ProcessExecutor::new(&cfg.exec)?;
    let name = format!("local-{}", std::process::id());
    let dir = dir.unwrap_or_else(|| state.job_dir(&name));
    let (program, args) = cmd.split_first().ok_or_else(|| AgentError::Exec {
        program: String::new(),
        reason: "no command was given after --".into(),
    })?;
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let mut spec = ExecSpec::new(&name, program, &args, &dir);
    spec.limits = limits;
    spec.gpus = gpus;
    let spec = backend.wrap(&profile, spec, &cfg.allowed_paths)?;
    let log_dir = spec.log_dir.clone();

    let support = executor.limit_support();
    eprintln!(
        "{NAME}: profile {} with sandbox {}{}",
        profile.name,
        backend.name(),
        if backend.isolates() {
            ""
        } else {
            " (no isolation)"
        }
    );
    if !limits.is_empty() || !support.contained {
        if let Some(note) = &support.note {
            eprintln!("{NAME}: note: {note}");
        }
    }
    let mut run = executor.spawn(spec).await?;
    let report = tokio::select! {
        r = run.wait() => r?,
        () = wait_for_signal() => run.stop(Duration::from_secs(cfg.exec.kill_grace_secs)).await?,
    };
    for stream in ["stdout", "stderr"] {
        if let Ok(text) = std::fs::read(log_dir.join(format!("{stream}.log"))) {
            if stream == "stdout" {
                print!("{}", String::from_utf8_lossy(&text));
            } else {
                eprint!("{}", String::from_utf8_lossy(&text));
            }
        }
    }
    eprintln!("{NAME}: {}", json(&report)?);
    let code = report.code.and_then(|c| u8::try_from(c).ok()).unwrap_or(1);
    Ok(ExitCode::from(code))
}

fn gib(b: u64) -> String {
    format!("{:.1} GiB", b as f64 / (1u64 << 30) as f64)
}

fn summary(inv: &hw::Inventory) -> String {
    let mut s = format!(
        "{} ({:?}, {} {} {})\nCPU: {} — {} cores ({} threads)\nRAM: {} ({} available)\n",
        inv.hostname,
        inv.platform,
        inv.os.name,
        inv.os.version,
        inv.os.arch,
        inv.cpu.model,
        inv.cpu.physical_cores.map_or("?".into(), |c| c.to_string()),
        inv.cpu.logical_cores,
        gib(inv.memory.total_bytes),
        gib(inv.memory.available_bytes),
    );
    for d in &inv.disks {
        s += &format!(
            "Disk {}: {} free of {} ({})\n",
            d.mount_point,
            gib(d.free_bytes),
            gib(d.total_bytes),
            d.file_system
        );
    }
    for g in &inv.gpus {
        s += &format!(
            "GPU {}: {} ({}, {}){}\n",
            g.index,
            g.model,
            g.backend,
            gib(g.memory_total_bytes),
            if g.experimental {
                " [experimental]"
            } else {
                ""
            }
        );
    }
    for b in &inv.backends {
        if let Some(m) = &b.message {
            s += &format!("Backend {}: {m}\n", b.name);
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_shows_name_version_and_sha() {
        assert_eq!(version_line(), format!("{NAME} {VERSION} ({GIT_SHA})"));
        assert!(!GIT_SHA.is_empty());
        assert!(Cli::try_parse_from(["mrl-agent", "-V"]).unwrap().version);
    }

    #[test]
    fn commands_parse() {
        let cli = Cli::try_parse_from([
            "mrl-agent",
            "run-local",
            "--profile",
            "worker",
            "--memory",
            "2G",
            "--cpus",
            "1.5",
            "--gpus",
            "0,1",
            "--",
            "python3",
            "-c",
            "print(1)",
        ])
        .unwrap();
        let Some(Command::RunLocal {
            profile,
            memory,
            cpus,
            gpus,
            cmd,
            ..
        }) = cli.command
        else {
            panic!("run-local parses")
        };
        assert_eq!(profile, "worker");
        assert_eq!(memory.as_deref(), Some("2G"));
        assert_eq!(cpus, Some(1.5));
        assert_eq!(gpus, ["0", "1"]);
        assert_eq!(cmd, ["python3", "-c", "print(1)"]);
        assert!(
            Cli::try_parse_from(["mrl-agent", "run-local"]).is_err(),
            "needs a command"
        );
        assert!(Cli::try_parse_from(["mrl-agent", "inventory", "--json"]).is_ok());
        assert!(Cli::try_parse_from(["mrl-agent", "service", "status"]).is_ok());
        assert!(Cli::try_parse_from(["mrl-agent", "--bogus"]).is_err());
    }

    #[test]
    fn summary_lists_real_hardware() {
        let inv = hw::collect_inventory(&[]);
        let s = summary(&inv);
        assert!(s.contains("CPU: ") && s.contains("RAM: "));
    }
}
