//! Finding the PostgreSQL programs and their major version.

use super::BundleError;
use std::path::{Path, PathBuf};
use std::process::Stdio;

/// Programs the manager runs.
const TOOLS: [&str; 6] = [
    "postgres",
    "initdb",
    "pg_ctl",
    "pg_dump",
    "pg_restore",
    "pg_upgrade",
];

/// A folder with the PostgreSQL programs of one major version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgBinaries {
    dir: PathBuf,
    major: u32,
}

impl PgBinaries {
    /// The programs in `dir`. Fails when one is missing or `postgres -V` is unreadable.
    pub async fn in_dir(dir: &Path) -> Result<Self, BundleError> {
        for tool in TOOLS {
            let path = dir.join(exe(tool));
            if !path.is_file() {
                return Err(BundleError::BinariesMissing(format!(
                    "{} is missing",
                    path.display()
                )));
            }
        }
        let out = tokio::process::Command::new(dir.join(exe("postgres")))
            .arg("-V")
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| {
                BundleError::BinariesMissing(format!("{} could not run: {e}", dir.display()))
            })?;
        let text = String::from_utf8_lossy(&out.stdout);
        let major = parse_major(&text).ok_or_else(|| {
            BundleError::BinariesMissing(format!(
                "could not read the version from `postgres -V` in {}: {text:?}",
                dir.display()
            ))
        })?;
        Ok(Self {
            dir: dir.to_path_buf(),
            major,
        })
    }

    /// `configured` if set; otherwise `postgres/bin` next to this program, then `PATH`, then
    /// the folders Linux packages install to (`/usr/lib/postgresql/<N>/bin`, newest first).
    pub async fn locate(configured: Option<&Path>) -> Result<Self, BundleError> {
        if let Some(dir) = configured {
            return Self::in_dir(dir).await;
        }
        let mut tried = Vec::new();
        for dir in candidates() {
            match Self::in_dir(&dir).await {
                Ok(b) => return Ok(b),
                Err(_) => tried.push(dir.display().to_string()),
            }
        }
        Err(BundleError::BinariesMissing(format!(
            "no folder had all of {}; looked in: {}",
            TOOLS.join(", "),
            if tried.is_empty() {
                "nothing (no PATH)".to_owned()
            } else {
                tried.join(", ")
            }
        )))
    }

    /// The folder.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The major version, like 17.
    pub fn major(&self) -> u32 {
        self.major
    }

    /// The full path of one program.
    pub fn tool(&self, name: &str) -> PathBuf {
        self.dir.join(exe(name))
    }
}

fn candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(dir) = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(Path::to_path_buf))
    {
        out.push(dir.join("postgres").join("bin"));
    }
    if let Some(path) = std::env::var_os("PATH") {
        out.extend(std::env::split_paths(&path).filter(|d| d.join(exe("initdb")).is_file()));
    }
    if cfg!(target_os = "linux") {
        if let Ok(entries) = std::fs::read_dir("/usr/lib/postgresql") {
            let mut versions: Vec<(u32, PathBuf)> = entries
                .filter_map(Result::ok)
                .filter_map(|e| {
                    let v = e.file_name().to_str()?.parse().ok()?;
                    Some((v, e.path().join("bin")))
                })
                .collect();
            versions.sort_by_key(|v| std::cmp::Reverse(v.0));
            out.extend(versions.into_iter().map(|(_, p)| p));
        }
    }
    out
}

fn exe(name: &str) -> String {
    format!("{name}{}", std::env::consts::EXE_SUFFIX)
}

/// `postgres (PostgreSQL) 17.6 (Homebrew)` → 17.
fn parse_major(version: &str) -> Option<u32> {
    let after = version.split(')').nth(1)?;
    let number = after.split_whitespace().next()?;
    number.split(['.', 'b', 'r']).next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn major_versions_parse() {
        assert_eq!(
            parse_major("postgres (PostgreSQL) 17.6 (Homebrew)\n"),
            Some(17)
        );
        assert_eq!(parse_major("postgres (PostgreSQL) 16.10"), Some(16));
        assert_eq!(parse_major("postgres (PostgreSQL) 18beta1"), Some(18));
        assert_eq!(parse_major("postgres (PostgreSQL) 18rc1"), Some(18));
        assert_eq!(parse_major("garbage"), None);
    }

    #[tokio::test]
    async fn a_folder_without_programs_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let err = PgBinaries::in_dir(dir.path()).await.unwrap_err();
        assert_eq!(err.code(), crate::db::codes::BINARIES_MISSING);
        let err = PgBinaries::locate(Some(dir.path())).await.unwrap_err();
        assert!(err.to_string().contains("is missing"), "{err}");
    }
}
