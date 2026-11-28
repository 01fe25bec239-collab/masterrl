//! The `command` profile: a user's own command (P39's bring-your-own-AI custom command).
//!
//! The command sees the system folders read-only, the folders its caller names read-only
//! (never a MasterRL data folder: the agent state folder or a node data path is refused),
//! its own input folder read-only, its output folder writable and a 256 MiB in-memory
//! `/tmp`. Its environment is exactly what the caller sets (credentials of the MasterRL
//! job never pass). It has no network unless the user enabled it for that provider
//! (`command_net`: the same profile with the network and any socket family). Limits:
//! 1 CPU, 2 GiB, 128 processes by default; [`run`] stops it at its time limit (60 s by
//! default) or when it has printed more than its output limit (1 MiB by default):
//! MRL-RWD-298.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{classify, Ending, Filter, Hardening, ProfileError, COMMAND_LIMIT};
use crate::exec::{ExecSpec, Executor, ExitReport, Limits};
use crate::sandbox::{SandboxBackend, SandboxProfile};

/// Profile name (no network).
pub const NAME: &str = "command";
/// Profile name with the network.
pub const NAME_NET: &str = "command_net";
/// Default time limit.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
/// Default output limit (stdout + stderr).
pub const DEFAULT_OUTPUT_CAP: u64 = 1 << 20;
/// How often the output size is checked.
const POLL: Duration = Duration::from_millis(100);

/// No network.
pub fn profile() -> SandboxProfile {
    SandboxProfile {
        name: NAME,
        description:
            "a user's own command: no network, no MasterRL data folders, time and output limits",
        network: false,
        gpu_devices: false,
    }
}

/// With the network (only when the user enabled it for the provider).
pub fn profile_net() -> SandboxProfile {
    SandboxProfile {
        name: NAME_NET,
        description: "a user's own command with the network the user allowed: no MasterRL data folders, time and output limits",
        network: true,
        gpu_devices: false,
    }
}

/// Limits and filter of either variant.
pub fn hardening(network: bool) -> Hardening {
    Hardening {
        filter: if network {
            Filter::StrictNetwork
        } else {
            Filter::Strict
        },
        tmpfs_bytes: 256 << 20,
        default_limits: Limits {
            cpu_millis: Some(1000),
            memory_bytes: Some(2 << 30),
            pids: Some(128),
        },
        max_limits: Limits {
            cpu_millis: Some(4000),
            memory_bytes: Some(8 << 30),
            pids: Some(512),
        },
        env: &[],
        keep_spec_env: true,
        wall_clock_secs: Some(DEFAULT_TIMEOUT.as_secs()),
        output_cap_bytes: Some(DEFAULT_OUTPUT_CAP),
    }
}

/// One command to run.
#[derive(Debug, Clone)]
pub struct CommandSpec {
    /// Execution name.
    pub name: String,
    /// Folder for `work/` (its input, read-only), `staging/` (its output) and `logs/`.
    pub dir: PathBuf,
    /// Program.
    pub program: String,
    /// Arguments.
    pub args: Vec<String>,
    /// Its whole environment.
    pub env: Vec<(String, String)>,
    /// Extra folders it may read (like the provider CLI's install folder).
    pub read_paths: Vec<PathBuf>,
    /// Folders it must never see: the agent state folder and the node data paths.
    pub forbidden: Vec<PathBuf>,
    /// The user enabled the network for this provider.
    pub network: bool,
    /// Time limit.
    pub timeout: Duration,
    /// Output limit, bytes.
    pub output_cap: u64,
    /// Limits asked for (lowered to the caps).
    pub limits: Limits,
}

/// What the command did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    /// The exit report.
    pub report: ExitReport,
    /// How it ended.
    pub ending: Ending,
    /// Bytes it printed.
    pub output_bytes: u64,
}

fn inside(path: &Path, root: &Path) -> bool {
    let canon = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    let (p, r) = (canon(path), canon(root));
    p.starts_with(&r) || r.starts_with(&p)
}

fn output_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

/// The sandboxed command (checks the folders it may read).
pub fn spec(backend: &dyn SandboxBackend, c: &CommandSpec) -> Result<ExecSpec, ProfileError> {
    let mut reads = c.read_paths.clone();
    reads.push(c.dir.join("work"));
    for p in &reads {
        if let Some(root) = c.forbidden.iter().find(|r| inside(p, r)) {
            return Err(ProfileError::UserCodeRefused(format!(
                "a command can't read {} (MasterRL data under {})",
                p.display(),
                root.display()
            )));
        }
    }
    let args: Vec<&str> = c.args.iter().map(String::as_str).collect();
    let mut spec = ExecSpec::new(&c.name, &c.program, &args, &c.dir);
    spec.env = c.env.iter().cloned().collect();
    spec.limits = c.limits;
    let profile = if c.network { profile_net() } else { profile() };
    Ok(super::wrap(backend, &profile, spec, &c.read_paths)?)
}

/// Runs `c` to its end, its time limit, its output limit or `cancel`.
pub async fn run(
    executor: &dyn Executor,
    backend: &dyn SandboxBackend,
    c: &CommandSpec,
    cancel: CancellationToken,
) -> Result<CommandResult, ProfileError> {
    let spec = spec(backend, c)?;
    let logs = spec.log_dir.clone();
    let mut run = executor.spawn(spec).await?;
    let deadline = tokio::time::Instant::now() + c.timeout;
    let report = loop {
        tokio::select! {
            r = run.wait() => break r?,
            () = tokio::time::sleep_until(deadline) => {
                run.kill().await?;
                let _ = run.wait().await;
                return Err(ProfileError::CommandLimit(format!(
                    "it ran longer than {} s", c.timeout.as_secs_f64()
                )));
            }
            () = cancel.cancelled() => {
                let report = run.stop(Duration::from_secs(2)).await?;
                let ending = classify(&report);
                return Ok(CommandResult { report, ending, output_bytes: output_bytes(&logs) });
            }
            () = tokio::time::sleep(POLL) => {
                let n = output_bytes(&logs);
                if n > c.output_cap {
                    run.kill().await?;
                    let _ = run.wait().await;
                    return Err(ProfileError::CommandLimit(format!(
                        "it printed more than {} bytes", c.output_cap
                    )));
                }
            }
        }
    };
    let n = output_bytes(&logs);
    if n > c.output_cap {
        return Err(ProfileError::CommandLimit(format!(
            "it printed more than {} bytes",
            c.output_cap
        )));
    }
    let ending = classify(&report);
    if let Ending::Violation(why) = &ending {
        return Err(ProfileError::Violation(why.clone()));
    }
    Ok(CommandResult {
        report,
        ending,
        output_bytes: n,
    })
}

/// The code for a command stopped at a limit.
pub const fn limit_code() -> &'static str {
    COMMAND_LIMIT
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::sandbox::NoSandbox;

    fn cmd(dir: &Path, script: &str) -> CommandSpec {
        CommandSpec {
            name: "cmd-1".into(),
            dir: dir.to_path_buf(),
            program: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            env: vec![("PROVIDER_TOKEN".into(), "t".into())],
            read_paths: vec![],
            forbidden: vec![dir.join("state")],
            network: false,
            timeout: Duration::from_secs(2),
            output_cap: 4096,
            limits: Limits::default(),
        }
    }

    fn executor() -> crate::exec::process::ProcessExecutor {
        crate::exec::process::ProcessExecutor::new(&crate::config::ExecConfig::default()).unwrap()
    }

    #[test]
    fn data_folders_are_refused_and_the_network_is_off_unless_enabled() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("state/cache")).unwrap();
        let mut c = cmd(dir.path(), "true");
        c.read_paths = vec![dir.path().join("state/cache")];
        assert_eq!(spec(&NoSandbox, &c).unwrap_err().code(), "MRL-RWD-295");
        let bw = crate::sandbox::bwrap::Bubblewrap::new(Some("/usr/bin/bwrap".into()));
        let c = cmd(dir.path(), "true");
        let s = spec(&bw, &c).unwrap();
        assert!(!s.args.iter().any(|a| a == "--share-net"));
        let mut net = c.clone();
        net.network = true;
        let s = spec(&bw, &net).unwrap();
        assert!(s.args.iter().any(|a| a == "--share-net"));
        assert_eq!(s.limits, hardening(true).default_limits);
    }

    #[tokio::test]
    async fn time_and_output_limits_stop_the_command() {
        let dir = tempfile::tempdir().unwrap();
        let ex = executor();
        let ok = run(
            &ex,
            &NoSandbox,
            &cmd(dir.path(), "echo hi"),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(ok.ending, Ending::Ok);
        let slow = run(
            &ex,
            &NoSandbox,
            &cmd(dir.path(), "sleep 30"),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(slow.unwrap_err().code(), "MRL-RWD-298");
        let loud = run(
            &ex,
            &NoSandbox,
            &cmd(dir.path(), "yes flood"),
            CancellationToken::new(),
        )
        .await;
        let e = loud.unwrap_err();
        assert_eq!(e.code(), limit_code());
        assert!(e.to_string().contains("printed more"), "{e}");
    }
}
