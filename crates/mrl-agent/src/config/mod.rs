//! The agent settings file, `agent.toml`. Every field is optional; defaults fit one desktop.
//!
//! ```toml
//! state_dir = "/home/me/.local/state/masterrl/agent"
//! allowed_paths = ["/data/datasets"]      # node paths jobs may read
//!
//! [pool]
//! pool = "lab-a100"                        # hint: the pool this node should join
//! labels = { rack = "r1" }
//!
//! [cache]
//! max_bytes = 500_000_000_000              # default: 70% of the cache disk
//! min_free_bytes = 21_474_836_480          # default: 20 GiB
//!
//! [sandbox]
//! backend = "auto"                         # auto | bubblewrap | none
//!
//! [exec]
//! cgroup_root = "/sys/fs/cgroup/user.slice/.../mrl-agent.service"   # default: own cgroup
//!
//! [transfer]
//! server_url = "https://core.example:8443"
//! bandwidth_bytes_per_sec = 50_000_000
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{AgentError, Result};

/// Environment variable that names the settings file.
pub const CONFIG_ENV: &str = "MRL_AGENT_CONFIG";
const GIB: u64 = 1 << 30;

/// All agent settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentConfig {
    /// The agent's state folder (cache, logs, job folders). Must be absolute.
    pub state_dir: PathBuf,
    /// Node paths jobs may read (bound read-only into sandboxes). Must be absolute.
    pub allowed_paths: Vec<PathBuf>,
    /// Hints for which pool this node joins.
    pub pool: PoolHints,
    /// Node cache budget.
    pub cache: CacheConfig,
    /// Sandbox backend selection.
    pub sandbox: SandboxConfig,
    /// Process executor settings.
    pub exec: ExecConfig,
    /// Transfer client settings.
    pub transfer: TransferConfig,
    // <mrl:config P18> P18 settings fields go right below this line.

    // <mrl:config P20> P20 settings fields go right below this line.
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            state_dir: default_state_dir(),
            allowed_paths: Vec::new(),
            pool: PoolHints::default(),
            cache: CacheConfig::default(),
            sandbox: SandboxConfig::default(),
            exec: ExecConfig::default(),
            transfer: TransferConfig::default(),
            // <mrl:config-default P18> P18 defaults go right below this line.

            // <mrl:config-default P20> P20 defaults go right below this line.
        }
    }
}

/// Hints the server uses to place this node in a pool.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolHints {
    /// The pool this node asks to join.
    pub pool: Option<String>,
    /// Free-form labels, like `rack = "r1"`.
    pub labels: BTreeMap<String, String>,
}

/// Node cache budget (Doc 20 item 16).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Largest total size of cache entries. Default: 70% of the cache disk.
    pub max_bytes: Option<u64>,
    /// Free space the cache always leaves on its disk. Default: 20 GiB.
    pub min_free_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: None,
            min_free_bytes: 20 * GIB,
        }
    }
}

impl CacheConfig {
    /// The budget for a cache stored in `dir`.
    pub fn budget_for(&self, dir: &Path) -> crate::state::cache::CacheBudget {
        let max_bytes = self.max_bytes.unwrap_or_else(|| {
            crate::hw::cpu::volume_of(dir).map_or(0, |(total, _)| total / 10 * 7)
        });
        crate::state::cache::CacheBudget {
            max_bytes,
            min_free_bytes: self.min_free_bytes,
        }
    }
}

/// Which sandbox backend runs sandboxed programs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum SandboxChoice {
    /// Bubblewrap on Linux; no sandbox on other systems (reported as such).
    #[default]
    Auto,
    /// Bubblewrap; fails if it can't run.
    Bubblewrap,
    /// No sandbox (development only).
    None,
}

/// Sandbox settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SandboxConfig {
    /// Backend selection.
    pub backend: SandboxChoice,
    /// The `bwrap` program. Default: found on `PATH`.
    pub bwrap_path: Option<PathBuf>,
}

/// Process executor settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecConfig {
    /// cgroup v2 folder that job cgroups go under (Linux). Default: the agent's own cgroup,
    /// which the systemd user unit delegates.
    pub cgroup_root: Option<PathBuf>,
    /// Environment variables passed from the agent to every job.
    pub env_allow: Vec<String>,
    /// Size at which a job log file rotates.
    pub log_max_bytes: u64,
    /// Rotated log files kept per stream.
    pub log_keep: u32,
    /// Seconds between the stop signal and the forced kill.
    pub kill_grace_secs: u64,
}

impl Default for ExecConfig {
    fn default() -> Self {
        let mut env_allow: Vec<String> = ["PATH", "HOME", "USER", "LANG", "LC_ALL", "TZ", "TERM"]
            .map(String::from)
            .into();
        if cfg!(windows) {
            env_allow.extend(
                [
                    "SYSTEMROOT",
                    "WINDIR",
                    "TEMP",
                    "TMP",
                    "USERPROFILE",
                    "PATHEXT",
                    "COMSPEC",
                ]
                .map(String::from),
            );
        }
        Self {
            cgroup_root: None,
            env_allow,
            log_max_bytes: 64 << 20,
            log_keep: 3,
            kill_grace_secs: 10,
        }
    }
}

/// Transfer client settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransferConfig {
    /// Base URL for server-proxied tickets (`/api/v1/transfer/...`).
    pub server_url: Option<String>,
    /// Node-wide transfer speed limit. Default: none.
    pub bandwidth_bytes_per_sec: Option<u64>,
    /// Tries per transfer after the first one fails.
    pub retries: u32,
    /// Seconds to wait for a connection.
    pub connect_timeout_secs: u64,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            server_url: None,
            bandwidth_bytes_per_sec: None,
            retries: 5,
            connect_timeout_secs: 30,
        }
    }
}

impl AgentConfig {
    /// Loads settings from `path`, else from `$MRL_AGENT_CONFIG`, else from the default file.
    /// A missing default file means all defaults; a missing named file is an error.
    /// Returns the settings and the file they came from.
    pub fn load(path: Option<&Path>) -> Result<(Self, PathBuf)> {
        let named = path
            .map(Path::to_path_buf)
            .or_else(|| std::env::var_os(CONFIG_ENV).map(PathBuf::from));
        let file = named.clone().unwrap_or_else(default_config_path);
        let config = match std::fs::read_to_string(&file) {
            Ok(text) => Self::parse(&text, &file)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && named.is_none() => {
                Self::default()
            }
            Err(e) => {
                return Err(AgentError::Config {
                    path: file,
                    reason: format!("the file could not be read ({e})"),
                })
            }
        };
        config.validate(&file)?;
        Ok((config, file))
    }

    /// Parses settings from TOML text. `file` is only used in error messages.
    pub fn parse(text: &str, file: &Path) -> Result<Self> {
        toml::from_str(text).map_err(|e| AgentError::Config {
            path: file.to_path_buf(),
            reason: e.message().to_owned(),
        })
    }

    /// Checks values the parser can't.
    pub fn validate(&self, file: &Path) -> Result<()> {
        let bad = |reason: String| AgentError::Config {
            path: file.to_path_buf(),
            reason,
        };
        if !self.state_dir.is_absolute() {
            return Err(bad(format!(
                "state_dir must be a full path, not {:?}; set it, or set HOME",
                self.state_dir
            )));
        }
        if let Some(p) = self.allowed_paths.iter().find(|p| !p.is_absolute()) {
            return Err(bad(format!(
                "allowed_paths entries must be full paths, not {p:?}"
            )));
        }
        if self.pool.labels.keys().any(|k| k.trim().is_empty()) {
            return Err(bad("pool label names can't be empty".into()));
        }
        if self.transfer.bandwidth_bytes_per_sec == Some(0) {
            return Err(bad(
                "transfer.bandwidth_bytes_per_sec must be above 0; leave it out for no limit"
                    .into(),
            ));
        }
        if self.exec.log_max_bytes < 4096 {
            return Err(bad("exec.log_max_bytes must be at least 4096".into()));
        }
        if let Some(root) = &self.exec.cgroup_root {
            if !root.is_absolute() {
                return Err(bad(format!(
                    "exec.cgroup_root must be a full path, not {root:?}"
                )));
            }
        }
        Ok(())
    }

    /// True when `path` is inside one of [`Self::allowed_paths`] (after resolving links).
    pub fn path_allowed(&self, path: &Path) -> bool {
        let Ok(path) = path.canonicalize() else {
            return false;
        };
        self.allowed_paths
            .iter()
            .filter_map(|p| p.canonicalize().ok())
            .any(|allowed| path.starts_with(allowed))
    }
}

fn home() -> Option<PathBuf> {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
}

fn env_dir(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
}

/// The default state folder for this OS (empty when the home folder is unknown).
pub fn default_state_dir() -> PathBuf {
    let base = if cfg!(windows) {
        env_dir("LOCALAPPDATA").map(|d| d.join("MasterRL"))
    } else if cfg!(target_os = "macos") {
        home().map(|h| h.join("Library/Application Support/MasterRL"))
    } else {
        env_dir("XDG_STATE_HOME")
            .or_else(|| home().map(|h| h.join(".local/state")))
            .map(|d| d.join("masterrl"))
    };
    base.map(|b| b.join("agent")).unwrap_or_default()
}

/// The default settings file for this OS.
pub fn default_config_path() -> PathBuf {
    let base = if cfg!(windows) {
        env_dir("APPDATA").map(|d| d.join("MasterRL"))
    } else if cfg!(target_os = "macos") {
        home().map(|h| h.join("Library/Application Support/MasterRL"))
    } else {
        env_dir("XDG_CONFIG_HOME")
            .or_else(|| home().map(|h| h.join(".config")))
            .map(|d| d.join("masterrl"))
    };
    base.unwrap_or_default().join("agent.toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_file_parses() {
        let root = if cfg!(windows) { "C:/srv" } else { "/srv" };
        let text = format!(
            r#"
            state_dir = "{root}/state"
            allowed_paths = ["{root}/data"]
            [pool]
            pool = "lab"
            labels = {{ rack = "r1" }}
            [cache]
            max_bytes = 1000
            min_free_bytes = 10
            [sandbox]
            backend = "bubblewrap"
            [exec]
            log_keep = 5
            [transfer]
            server_url = "https://core:8443"
            bandwidth_bytes_per_sec = 100
            "#
        );
        let c = AgentConfig::parse(&text, Path::new("agent.toml")).unwrap();
        c.validate(Path::new("agent.toml")).unwrap();
        assert_eq!(c.pool.pool.as_deref(), Some("lab"));
        assert_eq!(c.pool.labels["rack"], "r1");
        assert_eq!(c.cache.max_bytes, Some(1000));
        assert_eq!(c.sandbox.backend, SandboxChoice::Bubblewrap);
        assert_eq!(c.exec.log_keep, 5);
        assert_eq!(c.exec.kill_grace_secs, 10, "unset fields keep defaults");
        assert_eq!(c.transfer.bandwidth_bytes_per_sec, Some(100));
        assert_eq!(
            c.cache.budget_for(Path::new(root)).max_bytes,
            1000,
            "explicit budget wins"
        );
    }

    #[test]
    fn unknown_and_bad_values_are_refused() {
        let file = Path::new("agent.toml");
        let e = AgentConfig::parse("colour = 1", file).unwrap_err();
        assert_eq!(e.code(), "MRL-VAL-120");
        assert!(e.to_string().contains("colour"), "{e}");

        let c = AgentConfig {
            state_dir: "relative".into(),
            ..AgentConfig::default()
        };
        assert!(c
            .validate(file)
            .unwrap_err()
            .to_string()
            .contains("state_dir"));
        let mut c = AgentConfig {
            state_dir: std::env::temp_dir(),
            allowed_paths: vec!["data".into()],
            ..AgentConfig::default()
        };
        assert!(c.validate(file).is_err());
        c.allowed_paths.clear();
        c.transfer.bandwidth_bytes_per_sec = Some(0);
        assert!(c.validate(file).is_err());
    }

    #[test]
    fn missing_named_file_is_an_error() {
        let e = AgentConfig::load(Some(Path::new("/no/such/agent.toml"))).unwrap_err();
        assert_eq!(e.code(), "MRL-VAL-120");
    }

    #[test]
    fn allowed_paths_resolve_links_and_parents() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        std::fs::create_dir_all(data.join("set")).unwrap();
        let c = AgentConfig {
            allowed_paths: vec![data.clone()],
            ..AgentConfig::default()
        };
        assert!(c.path_allowed(&data.join("set")));
        assert!(!c.path_allowed(dir.path()));
        assert!(!c.path_allowed(&data.join("set/../..")));
        assert!(!c.path_allowed(&data.join("missing")));
    }
}
