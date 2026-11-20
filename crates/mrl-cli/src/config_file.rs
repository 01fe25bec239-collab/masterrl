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
        Self::load_from(&path()?)
    }

    fn load_from(path: &std::path::Path) -> Result<Self, String> {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| format!("can't read the settings in {}: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(format!("can't open {}: {e}", path.display())),
        }
    }

    /// Changes the saved settings with `change` and returns the file's path. Two `mrl`
    /// commands can't lose each other's changes: the file is locked (`cli.toml.lock`),
    /// read again under the lock, changed, written to a private temporary file next to it
    /// and renamed over it, so a crash leaves the old file or the new one, never half.
    pub fn update(change: impl FnOnce(&mut Self)) -> Result<PathBuf, String> {
        Self::update_at(&path()?, change)
    }

    fn update_at(
        path: &std::path::Path,
        change: impl FnOnce(&mut Self),
    ) -> Result<PathBuf, String> {
        let shown = path.display();
        let dir = path.parent().unwrap_or(std::path::Path::new("."));
        std::fs::create_dir_all(dir).map_err(|e| format!("can't create {}: {e}", dir.display()))?;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(format!("{name}.lock")))
            .map_err(|e| format!("can't lock {shown}: {e}"))?;
        // Held until `lock` is dropped at the end of this function.
        lock.lock()
            .map_err(|e| format!("can't lock {shown}: {e}"))?;
        let mut cfg = Self::load_from(path)?;
        change(&mut cfg);
        let text = toml::to_string(&cfg).map_err(|e| format!("can't write settings: {e}"))?;
        let tmp = dir.join(format!("{name}.{}.tmp", std::process::id()));
        let written = write_private(&tmp, &text).and_then(|()| std::fs::rename(&tmp, path));
        if let Err(e) = written {
            let _ = std::fs::remove_file(&tmp);
            return Err(format!("can't write {shown}: {e}"));
        }
        Ok(path.to_path_buf())
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
    // A leftover file may have wider permissions; narrow them too.
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    f.write_all(text.as_bytes())?;
    f.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    // ponytail: on Windows the file inherits the user profile's ACL; move the token to the OS keychain (P16) for more.
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    f.write_all(text.as_bytes())?;
    f.sync_all()
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

    #[test]
    fn concurrent_updates_keep_every_change() {
        let dir = std::env::temp_dir().join(format!("mrl-cli-cfg-{}", std::process::id()));
        let path = dir.join("cli.toml");
        let _ = std::fs::remove_dir_all(&dir);
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let path = path.clone();
                std::thread::spawn(move || {
                    CliConfig::update_at(&path, |c| match i % 3 {
                        0 => c.token = Some(format!("tok{i}")),
                        1 => c.server = Some(format!("http://h{i}")),
                        _ => c.project = Some(format!("prj{i}")),
                    })
                    .unwrap();
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let saved = CliConfig::load_from(&path).unwrap();
        assert!(
            saved.token.is_some() && saved.server.is_some() && saved.project.is_some(),
            "{saved:?}"
        );
        // No temporary files are left behind.
        let left: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
