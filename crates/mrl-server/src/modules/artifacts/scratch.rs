//! Private scratch folders for request data: an upload received through a ticket, the
//! parts of an upload being joined, an artifact file being copied. Each operation gets its
//! own folder, created atomically with a random name and (on Unix) mode `0700`, holding the
//! file and the multipart ledger `upload_file` writes next to it.
//!
//! The folder is a [`TempDir`]: dropping it deletes it, also when the request is cancelled
//! (its future is dropped). Callers close their files first, which Windows needs. Folders a
//! killed process left behind are deleted by [`sweep`] once they are a day old.

use super::Error;
use mrl_storage::StorageError;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

/// Name prefix of every scratch folder.
pub const PREFIX: &str = "mrl-p19-";
/// Age after which [`sweep`] deletes a leftover folder.
pub const MAX_AGE: Duration = Duration::from_secs(24 * 3600);

fn io(path: &Path, e: std::io::Error) -> Error {
    StorageError::Io {
        path: path.to_owned(),
        source: e,
    }
    .into()
}

/// A new private folder under `root`.
pub fn dir(root: &Path) -> Result<TempDir, Error> {
    std::fs::create_dir_all(root).map_err(|e| io(root, e))?;
    let mut b = tempfile::Builder::new();
    b.prefix(PREFIX);
    #[cfg(unix)]
    let private = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::Permissions::from_mode(0o700)
    };
    #[cfg(unix)]
    b.permissions(private);
    b.tempdir_in(root).map_err(|e| io(root, e))
}

/// Deletes a folder now, reporting failures (dropping it ignores them).
pub fn close(dir: TempDir) -> Result<(), Error> {
    let path = dir.path().to_owned();
    dir.close().map_err(|e| io(&path, e))
}

/// Deletes scratch folders under `root` older than [`MAX_AGE`]; returns how many. Entries
/// that are not folders with [`PREFIX`] are left alone.
pub fn sweep(root: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let cutoff = SystemTime::now()
        .checked_sub(MAX_AGE)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    for entry in entries.flatten() {
        let ours = entry.file_name().to_string_lossy().starts_with(PREFIX);
        // `symlink_metadata`: a link is never followed.
        let old_dir = entry
            .path()
            .symlink_metadata()
            .ok()
            .filter(|m| m.is_dir())
            .and_then(|m| m.modified().ok())
            .is_some_and(|t| t < cutoff);
        if ours && old_dir && std::fs::remove_dir_all(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

/// The default root: the system temporary folder.
pub fn default_root() -> PathBuf {
    std::env::temp_dir()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folders_are_private_and_swept_when_old() {
        let root = tempfile::tempdir().unwrap();
        let d = dir(root.path()).unwrap();
        std::fs::write(d.path().join("file"), b"x").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(d.path()).unwrap().permissions().mode();
            assert_eq!(mode & 0o077, 0, "group and others have no access: {mode:o}");
        }
        let kept = root.path().join("other");
        std::fs::create_dir(&kept).unwrap();
        assert_eq!(sweep(root.path()), 0, "fresh folders stay");
        // Ageing a folder needs opening it as a file, which only Unix allows.
        #[cfg(unix)]
        {
            let old = SystemTime::now() - MAX_AGE - Duration::from_secs(60);
            for p in [d.path(), kept.as_path()] {
                std::fs::File::open(p).unwrap().set_modified(old).unwrap();
            }
            assert_eq!(sweep(root.path()), 1, "only our old folder goes");
            assert!(!d.path().exists() && kept.exists());
        }
        let d2 = dir(root.path()).unwrap();
        let p = d2.path().to_owned();
        drop(d2);
        assert!(!p.exists(), "dropping deletes it");
    }
}
