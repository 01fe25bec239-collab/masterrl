//! Install the agent as a user-level background service (no admin rights):
//!
//! - Linux: a systemd user unit (`~/.config/systemd/user/mrl-agent.service`) with
//!   `Delegate=yes`, so the agent can create job cgroups; lingering is turned on when allowed,
//!   so it starts at boot without a login.
//! - macOS: a launchd agent (`~/Library/LaunchAgents/dev.masterrl.agent.plist`).
//! - WSL2: a Windows scheduled task that starts the agent in this distro at Windows sign-in.
//!
//! Each command first builds a [`Plan`] (files and commands, easy to test and to print), then
//! [`apply`] carries it out.

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{AgentError, Result};

/// systemd unit name.
pub const SYSTEMD_UNIT: &str = "mrl-agent.service";
/// launchd label.
pub const LAUNCHD_LABEL: &str = "dev.masterrl.agent";
/// Windows scheduled task name.
pub const TASK_NAME: &str = "MasterRL Agent";

/// The service manager used on this computer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Manager {
    /// systemd user instance (Linux).
    SystemdUser,
    /// launchd per-user agent (macOS).
    Launchd,
    /// Windows Task Scheduler, from inside WSL2.
    WslTask,
}

/// Picks the manager for this computer.
pub fn detect() -> Result<Manager> {
    if cfg!(target_os = "macos") {
        Ok(Manager::Launchd)
    } else if cfg!(windows) {
        Err(AgentError::Service(
            "on Windows, install the agent inside WSL2 instead".into(),
        ))
    } else if crate::hw::wsl::detect().is_some() {
        Ok(Manager::WslTask)
    } else if crate::sandbox::bwrap::which("systemctl").is_some() {
        Ok(Manager::SystemdUser)
    } else {
        Err(AgentError::Service(
            "this Linux has no systemd, so the agent can't install itself as a service".into(),
        ))
    }
}

/// Who and what the service runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The `mrl-agent` program.
    pub exe: PathBuf,
    /// The settings file passed with `--config`.
    pub config: PathBuf,
    /// The user's home folder.
    pub home: PathBuf,
    /// The user's login name.
    pub user: String,
    /// The user id (launchd domain `gui/<uid>`).
    pub uid: u32,
    /// The WSL2 distro.
    pub distro: Option<String>,
    /// Where launchd writes the agent's stdout/stderr.
    pub log_dir: PathBuf,
}

impl Target {
    /// This program, run by this user.
    pub fn current(config: &Path, log_dir: &Path) -> Result<Self> {
        let exe = std::env::current_exe().map_err(|e| {
            AgentError::Service(format!("the agent can't find its own program ({e})"))
        })?;
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| AgentError::Service("HOME is not set".into()))?;
        #[cfg(unix)]
        let uid = nix::unistd::getuid().as_raw();
        #[cfg(not(unix))]
        let uid = 0;
        Ok(Self {
            exe,
            config: config.to_path_buf(),
            home,
            user: std::env::var("USER").unwrap_or_default(),
            uid,
            distro: std::env::var("WSL_DISTRO_NAME").ok(),
            log_dir: log_dir.to_path_buf(),
        })
    }

    fn systemd_unit_path(&self) -> PathBuf {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .unwrap_or_else(|| self.home.join(".config"))
            .join("systemd/user")
            .join(SYSTEMD_UNIT)
    }

    fn plist_path(&self) -> PathBuf {
        self.home
            .join("Library/LaunchAgents")
            .join(format!("{LAUNCHD_LABEL}.plist"))
    }
}

/// One step of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    /// Write a file (atomically, creating folders).
    Write {
        /// File.
        path: PathBuf,
        /// Content.
        content: String,
    },
    /// Remove a file if it exists.
    Remove {
        /// File.
        path: PathBuf,
    },
    /// Run a command. A failed optional command is reported, not fatal.
    Run {
        /// Program and arguments.
        argv: Vec<String>,
        /// Fail the plan if this fails.
        required: bool,
    },
}

/// What install or uninstall will do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Plan {
    /// The service manager.
    pub manager: Manager,
    /// Steps, in order.
    pub actions: Vec<Action>,
}

fn run(argv: &[&str], required: bool) -> Action {
    Action::Run {
        argv: argv.iter().map(|s| (*s).to_owned()).collect(),
        required,
    }
}

/// Quotes one systemd `ExecStart` word.
fn systemd_quote(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%");
    format!("\"{escaped}\"")
}

fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// The systemd user unit.
pub fn systemd_unit(t: &Target) -> String {
    let exec = [
        t.exe.display().to_string(),
        "run".into(),
        "--config".into(),
        t.config.display().to_string(),
    ]
    .iter()
    .map(|w| systemd_quote(w))
    .collect::<Vec<_>>()
    .join(" ");
    format!(
        "[Unit]\n\
         Description=MasterRL agent\n\
         After=network-online.target\n\
         \n\
         [Service]\n\
         ExecStart={exec}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         # Lets the agent create job cgroups under its own (cgroup v2).\n\
         Delegate=yes\n\
         # Stop the agent first, then anything left in its cgroup.\n\
         KillMode=mixed\n\
         TimeoutStopSec=30\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

/// The launchd agent plist.
pub fn launchd_plist(t: &Target) -> String {
    let args = [
        t.exe.display().to_string(),
        "run".into(),
        "--config".into(),
        t.config.display().to_string(),
    ]
    .iter()
    .map(|a| format!("    <string>{}</string>\n", xml(a)))
    .collect::<String>();
    let out = xml(&t.log_dir.join("agent.out.log").display().to_string());
    let err = xml(&t.log_dir.join("agent.err.log").display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
{args}  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ProcessType</key>
  <string>Background</string>
  <key>StandardOutPath</key>
  <string>{out}</string>
  <key>StandardErrorPath</key>
  <string>{err}</string>
</dict>
</plist>
"#
    )
}

fn wsl_command(t: &Target) -> Result<String> {
    let distro = t
        .distro
        .as_deref()
        .ok_or_else(|| AgentError::Service("WSL_DISTRO_NAME is not set".into()))?;
    let words = [
        distro.to_owned(),
        t.exe.display().to_string(),
        t.config.display().to_string(),
    ];
    if let Some(w) = words
        .iter()
        .find(|w| w.is_empty() || w.contains([' ', '"', '\'']))
    {
        return Err(AgentError::Service(format!(
            "{w:?} has a space or quote, which the Windows task can't pass on; move the agent or its settings to a path without one"
        )));
    }
    Ok(format!(
        "wsl.exe -d {} --exec {} run --config {}",
        words[0], words[1], words[2]
    ))
}

/// What `install` does with `manager`.
pub fn install_plan(manager: Manager, t: &Target) -> Result<Plan> {
    let actions = match manager {
        Manager::SystemdUser => vec![
            Action::Write {
                path: t.systemd_unit_path(),
                content: systemd_unit(t),
            },
            run(&["systemctl", "--user", "daemon-reload"], true),
            run(
                &["systemctl", "--user", "enable", "--now", SYSTEMD_UNIT],
                true,
            ),
            // Start at boot without a login; some systems only allow this for admins.
            run(&["loginctl", "enable-linger", &t.user], false),
        ],
        Manager::Launchd => {
            let domain = format!("gui/{}", t.uid);
            let plist = t.plist_path();
            vec![
                Action::Write {
                    path: plist.clone(),
                    content: launchd_plist(t),
                },
                run(
                    &["launchctl", "bootout", &format!("{domain}/{LAUNCHD_LABEL}")],
                    false,
                ),
                run(
                    &[
                        "launchctl",
                        "bootstrap",
                        &domain,
                        &plist.display().to_string(),
                    ],
                    true,
                ),
            ]
        }
        Manager::WslTask => {
            let tr = wsl_command(t)?;
            vec![run(
                &[
                    "schtasks.exe",
                    "/Create",
                    "/F",
                    "/TN",
                    TASK_NAME,
                    "/SC",
                    "ONLOGON",
                    "/RL",
                    "LIMITED",
                    "/TR",
                    &tr,
                ],
                true,
            )]
        }
    };
    Ok(Plan { manager, actions })
}

/// What `uninstall` does with `manager`.
pub fn uninstall_plan(manager: Manager, t: &Target) -> Plan {
    let actions = match manager {
        Manager::SystemdUser => vec![
            run(
                &["systemctl", "--user", "disable", "--now", SYSTEMD_UNIT],
                false,
            ),
            Action::Remove {
                path: t.systemd_unit_path(),
            },
            run(&["systemctl", "--user", "daemon-reload"], false),
        ],
        Manager::Launchd => vec![
            run(
                &[
                    "launchctl",
                    "bootout",
                    &format!("gui/{}/{LAUNCHD_LABEL}", t.uid),
                ],
                false,
            ),
            Action::Remove {
                path: t.plist_path(),
            },
        ],
        Manager::WslTask => vec![run(
            &["schtasks.exe", "/Delete", "/F", "/TN", TASK_NAME],
            true,
        )],
    };
    Plan { manager, actions }
}

/// Creates the settings file the service points at (with its folders) if it is missing, so
/// the service's `--config` always names a file. Returns true when it was created.
pub fn ensure_config_file(path: &Path) -> Result<bool> {
    if path.exists() {
        return Ok(false);
    }
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).map_err(|e| AgentError::state(dir, e))?;
    }
    let text = "# MasterRL agent settings. Every field is optional.\n";
    crate::state::atomic_write(path, text.as_bytes()).map_err(|e| AgentError::state(path, e))?;
    Ok(true)
}

fn output(argv: &[&str]) -> Option<(bool, String)> {
    let (program, args) = argv.split_first()?;
    let out = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    Some((out.status.success(), text.trim().to_owned()))
}

/// Carries out a plan. Returns warnings from optional steps that failed.
pub fn apply(plan: &Plan) -> Result<Vec<String>> {
    let mut warnings = Vec::new();
    for action in &plan.actions {
        match action {
            Action::Write { path, content } => {
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir).map_err(|e| AgentError::state(dir, e))?;
                }
                crate::state::atomic_write(path, content.as_bytes())
                    .map_err(|e| AgentError::state(path, e))?;
            }
            Action::Remove { path } => match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(AgentError::state(path, e)),
            },
            Action::Run { argv, required } => {
                let words: Vec<&str> = argv.iter().map(String::as_str).collect();
                let (ok, text) = output(&words)
                    .unwrap_or_else(|| (false, format!("{} could not start", argv[0])));
                if !ok {
                    let msg = format!("`{}` failed: {text}", argv.join(" "));
                    if *required {
                        return Err(AgentError::Service(msg));
                    }
                    warnings.push(msg);
                }
            }
        }
    }
    Ok(warnings)
}

/// Whether the service is installed and running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Status {
    /// The service manager.
    pub manager: Manager,
    /// Its definition exists.
    pub installed: bool,
    /// The agent process is running.
    pub running: bool,
    /// What the manager said.
    pub detail: String,
}

/// Asks the manager about the service. `pid_alive` tells whether the agent's pid file names
/// a live process (used where the manager can't say).
pub fn status(manager: Manager, t: &Target, pid_alive: bool) -> Status {
    let (installed, running, detail) = match manager {
        Manager::SystemdUser => {
            let installed = t.systemd_unit_path().is_file();
            let (active, text) =
                output(&["systemctl", "--user", "is-active", SYSTEMD_UNIT]).unwrap_or_default();
            (installed, active, text)
        }
        Manager::Launchd => {
            let installed = t.plist_path().is_file();
            let (loaded, text) = output(&[
                "launchctl",
                "print",
                &format!("gui/{}/{LAUNCHD_LABEL}", t.uid),
            ])
            .unwrap_or_default();
            let running = loaded && text.contains("state = running");
            let first = text
                .lines()
                .find(|l| l.trim_start().starts_with("state ="))
                .unwrap_or(if loaded { "loaded" } else { "not loaded" })
                .trim()
                .to_owned();
            (installed, running, first)
        }
        Manager::WslTask => {
            let (installed, text) =
                output(&["schtasks.exe", "/Query", "/TN", TASK_NAME]).unwrap_or_default();
            (
                installed,
                pid_alive,
                text.lines().last().unwrap_or("").trim().to_owned(),
            )
        }
    };
    Status {
        manager,
        installed,
        running,
        detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> Target {
        Target {
            exe: "/home/me/bin/mrl-agent".into(),
            config: "/home/me/.config/masterrl/my agent.toml".into(),
            home: "/home/me".into(),
            user: "me".into(),
            uid: 501,
            distro: Some("Ubuntu".into()),
            log_dir: "/home/me/state/logs".into(),
        }
    }

    #[test]
    fn systemd_plan_writes_a_delegating_unit() {
        let plan = install_plan(Manager::SystemdUser, &target()).unwrap();
        let Action::Write { path, content } = &plan.actions[0] else {
            panic!("first step writes the unit")
        };
        assert!(path.ends_with("systemd/user/mrl-agent.service"));
        assert!(content.contains(
            "ExecStart=\"/home/me/bin/mrl-agent\" \"run\" \"--config\" \"/home/me/.config/masterrl/my agent.toml\""
        ));
        assert!(content.contains("Delegate=yes") && content.contains("WantedBy=default.target"));
        assert_eq!(
            plan.actions[2],
            run(
                &["systemctl", "--user", "enable", "--now", SYSTEMD_UNIT],
                true
            )
        );
        assert!(matches!(
            &plan.actions[3],
            Action::Run {
                required: false,
                ..
            }
        ));
        assert_eq!(systemd_quote("50%\"x"), "\"50%%\\\"x\"");

        let un = uninstall_plan(Manager::SystemdUser, &target());
        assert!(matches!(un.actions[1], Action::Remove { .. }));
    }

    #[test]
    fn launchd_plan_bootstraps_an_escaped_plist() {
        let mut t = target();
        t.exe = "/Users/me/A&B/mrl-agent".into();
        let plan = install_plan(Manager::Launchd, &t).unwrap();
        let Action::Write { path, content } = &plan.actions[0] else {
            panic!("first step writes the plist")
        };
        assert!(path.ends_with("Library/LaunchAgents/dev.masterrl.agent.plist"));
        assert!(content.contains("<string>/Users/me/A&amp;B/mrl-agent</string>"));
        assert!(content.contains("<string>dev.masterrl.agent</string>"));
        assert!(content.contains("/home/me/state/logs/agent.err.log"));
        assert_eq!(
            plan.actions[2],
            run(
                &[
                    "launchctl",
                    "bootstrap",
                    "gui/501",
                    &path.display().to_string()
                ],
                true
            )
        );
    }

    #[test]
    fn wsl_plan_creates_a_logon_task() {
        let mut t = target();
        t.config = "/home/me/.config/masterrl/agent.toml".into();
        let plan = install_plan(Manager::WslTask, &t).unwrap();
        let Action::Run {
            argv,
            required: true,
        } = &plan.actions[0]
        else {
            panic!("one required schtasks step")
        };
        assert_eq!(argv[..4], ["schtasks.exe", "/Create", "/F", "/TN"]);
        assert_eq!(
            argv.last().unwrap(),
            "wsl.exe -d Ubuntu --exec /home/me/bin/mrl-agent run --config /home/me/.config/masterrl/agent.toml"
        );
        // Spaces can't be passed through the task's command line.
        let e = install_plan(Manager::WslTask, &target()).unwrap_err();
        assert_eq!(e.code(), "MRL-NODE-125");
    }

    #[test]
    fn install_creates_the_settings_file_in_a_fresh_home() {
        let home = tempfile::tempdir().unwrap();
        // A brand-new account: not even ~/.config exists yet.
        let file = home.path().join(".config/masterrl/agent.toml");
        assert!(ensure_config_file(&file).unwrap());
        let (cfg, used) = crate::config::AgentConfig::load(Some(&file)).unwrap();
        assert_eq!(used, file);
        assert_eq!(cfg.pool, crate::config::PoolHints::default());
        std::fs::write(&file, "allowed_paths = []\n").unwrap();
        assert!(
            !ensure_config_file(&file).unwrap(),
            "an existing file is kept"
        );
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            "allowed_paths = []\n"
        );
    }

    #[test]
    fn apply_writes_removes_and_reports_optional_failures() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a/b.txt");
        let plan = Plan {
            manager: Manager::SystemdUser,
            actions: vec![
                Action::Write {
                    path: file.clone(),
                    content: "x".into(),
                },
                run(&["/no/such/tool"], false),
            ],
        };
        let warnings = apply(&plan).unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "x");
        assert_eq!(warnings.len(), 1);
        let plan = Plan {
            manager: Manager::SystemdUser,
            actions: vec![
                Action::Remove { path: file.clone() },
                run(&["/no/such/tool"], true),
            ],
        };
        assert_eq!(apply(&plan).unwrap_err().code(), "MRL-NODE-125");
        assert!(!file.exists());
    }
}
