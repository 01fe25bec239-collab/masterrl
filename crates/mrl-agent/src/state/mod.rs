//! The agent state folder and crash-safe writes.
//!
//! Layout: `cache/` (the [`cache::NodeCache`]), `jobs/` (per-job work, staging and logs),
//! `logs/` (the agent's own logs) and small JSON files written with [`atomic_write`].

pub mod cache;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{AgentError, Result};

/// The agent's state folder.
#[derive(Debug, Clone)]
pub struct StateDir {
    root: PathBuf,
}

impl StateDir {
    /// Opens (and creates) the state folder and its sub-folders.
    pub fn open(root: &Path) -> Result<Self> {
        for sub in ["", "cache", "jobs", "logs"] {
            let dir = root.join(sub);
            std::fs::create_dir_all(&dir).map_err(|e| AgentError::state(&dir, e))?;
        }
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// The folder itself.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The node cache folder.
    pub fn cache_dir(&self) -> PathBuf {
        self.root.join("cache")
    }

    /// The per-job folder for `job` (not created).
    pub fn job_dir(&self, job: &str) -> PathBuf {
        self.root.join("jobs").join(job)
    }

    /// The agent's own log folder.
    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    /// Writes `bytes` to `rel` (relative to the state folder) atomically.
    pub fn write_atomic(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        let path = self.root.join(rel);
        atomic_write(&path, bytes).map_err(|e| AgentError::state(path, e))
    }

    /// Reads `rel`, or `None` when it does not exist.
    pub fn read(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        let path = self.root.join(rel);
        match std::fs::read(&path) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(AgentError::state(path, e)),
        }
    }
}

/// Writes a file so readers see either the old or the new content, never a mix, even after a
/// crash: write a temporary file next to it, fsync it, rename it over the target, then fsync
/// the folder (Unix).
pub fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("the path has no file name"))?
        .to_string_lossy();
    let tmp_name = format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let tmp = dir.map_or_else(|| PathBuf::from(&tmp_name), |d| d.join(&tmp_name));
    let result = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)?;
        #[cfg(unix)]
        if let Some(d) = dir {
            std::fs::File::open(d)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_and_leaves_no_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateDir::open(&dir.path().join("s")).unwrap();
        assert!(state.cache_dir().is_dir() && state.logs_dir().is_dir());
        assert_eq!(state.read("a.json").unwrap(), None);
        state.write_atomic("a.json", b"one").unwrap();
        state.write_atomic("a.json", b"two").unwrap();
        assert_eq!(state.read("a.json").unwrap().as_deref(), Some(&b"two"[..]));
        let names: Vec<_> = std::fs::read_dir(state.root())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(
            names.iter().all(|n| !n.contains(".tmp-")),
            "temp file left: {names:?}"
        );
    }

    #[test]
    fn failed_write_reports_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateDir::open(dir.path()).unwrap();
        let e = state.write_atomic("missing/x.json", b"x").unwrap_err();
        assert_eq!(e.code(), "MRL-NODE-121");
        assert!(e.to_string().contains("x.json"));
    }
}
