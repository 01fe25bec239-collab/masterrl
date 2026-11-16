//! The CLI's own settings file: server address, personal access token, default project.
//!
//! Location: `MRL_CONFIG` if set; else `%APPDATA%\MasterRL\cli.toml` on Windows and
//! `$XDG_CONFIG_HOME/masterrl/cli.toml` (or `~/.config/masterrl/cli.toml`) elsewhere.
//! `MRL_SERVER`, `MRL_TOKEN` and `MRL_PROJECT` override the file for one terminal.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Saved settings. Every field is optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliConfig {
    /// Server address, like `https://mrl.example.com` or `http://127.0.0.1:8443`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    /// Personal access token (`mrl_pat_…`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Project used when a command needs one and `--project` is not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// The settings file's path.
pub fn path() -> Result<PathBuf, String> {
    if let Some(p) = std::env::var_os("MRL_CONFIG") {
        return Ok(PathBuf::from(p));
    }
    let base = if cfg!(windows) {
        std::env::var_os("APPDATA").map(|d| PathBuf::from(d).join("MasterRL"))
    } else {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
            .map(|d| d.join("masterrl"))
    };
    base.map(|d| d.join("cli.toml"))
        .ok_or_else(|| "can't find a folder for settings; set MRL_CONFIG to a file path".to_owned())
}

impl CliConfig {
    /// Reads the file (missing = empty settings).
    pub fn load() -> Result<Self, String> {
        let path = path()?;
        match std::fs::read_to_string(&path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| format!("can't read the settings in {}: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(format!("can't open {}: {e}", path.display())),
        }
    }

    /// Writes the file, readable only by you on Unix (it holds the token).
    pub fn save(&self) -> Result<PathBuf, String> {
        let path = path()?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|e| format!("can't create {}: {e}", dir.display()))?;
        }
        let text = toml::to_string(self).map_err(|e| format!("can't write settings: {e}"))?;
        write_private(&path, &text).map_err(|e| format!("can't write {}: {e}", path.display()))?;
        Ok(path)
    }

    /// The server address after `MRL_SERVER`.
    pub fn server(&self) -> Option<String> {
        env("MRL_SERVER").or_else(|| self.server.clone())
    }

    /// The token after `MRL_TOKEN`.
    pub fn token(&self) -> Option<String> {
        env("MRL_TOKEN").or_else(|| self.token.clone())
    }

    /// The default project after `MRL_PROJECT`.
    pub fn project(&self) -> Option<String> {
        env("MRL_PROJECT").or_else(|| self.project.clone())
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

#[cfg(unix)]
fn write_private(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    // An older file may have wider permissions; narrow them too.
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    f.write_all(text.as_bytes())
}

#[cfg(not(unix))]
fn write_private(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    // ponytail: on Windows the file inherits the user profile's ACL; move the token to the OS keychain (P16) for more.
    std::fs::write(path, text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_refuses_unknown_keys() {
        let c = CliConfig {
            server: Some("http://127.0.0.1:1".into()),
            token: Some("mrl_pat_x".into()),
            project: None,
        };
        let text = toml::to_string(&c).unwrap();
        assert!(!text.contains("project"));
        assert_eq!(toml::from_str::<CliConfig>(&text).unwrap(), c);
        assert!(toml::from_str::<CliConfig>("bogus = 1").is_err());
    }
}
