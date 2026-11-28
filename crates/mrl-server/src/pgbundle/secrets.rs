//! A file-backed `SecretStore`: one file per secret in a private folder.

use crate::seams::{SeamError, Secret, SecretStore};
use async_trait::async_trait;
use std::path::PathBuf;

/// Secrets as files (mode 0600 in a 0700 folder on macOS and Linux; on Windows the
/// per-user app data folder's permissions apply).
///
/// ponytail: used only for the bundled PostgreSQL password until P16's keychain store is in
/// `Seams`; then `main` passes that instead.
#[derive(Debug, Clone)]
pub struct FileSecretStore {
    dir: PathBuf,
}

impl FileSecretStore {
    /// A store in `dir` (created on first write).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn path(&self, name: &str) -> Result<PathBuf, SeamError> {
        let ok = !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            && !name.starts_with('.');
        if !ok {
            return Err(SeamError::new(
                "secrets",
                "secret names use only letters, digits, '.', '_' and '-'",
            ));
        }
        Ok(self.dir.join(name))
    }
}

fn io(what: &str, e: std::io::Error) -> SeamError {
    SeamError::new("secrets", format!("{what}: {e}"))
}

#[async_trait]
impl SecretStore for FileSecretStore {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError> {
        match tokio::fs::read_to_string(self.path(name)?).await {
            Ok(v) => Ok(Some(Secret::new(v))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io("could not read a secret", e)),
        }
    }

    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError> {
        let path = self.path(name)?;
        super::create_private_dir(&self.dir)
            .map_err(|e| io("could not create the secret folder", e))?;
        // One temp file per write: writers of the same secret (several servers sharing a
        // data folder, like test cores) never move each other's file away.
        let tmp = self
            .dir
            .join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
        super::write_private(&tmp, value.expose().as_bytes())
            .map_err(|e| io("could not write a secret", e))?;
        std::fs::rename(&tmp, &path).map_err(|e| io("could not save a secret", e))
    }

    async fn delete(&self, name: &str) -> Result<(), SeamError> {
        match tokio::fs::remove_file(self.path(name)?).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io("could not delete a secret", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_writers_of_one_secret_all_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(FileSecretStore::new(dir.path().join("s")));
        let writes: Vec<_> = (0..16)
            .map(|i| {
                let s = s.clone();
                tokio::spawn(async move { s.set("ca", Secret::new(format!("v{i}"))).await })
            })
            .collect();
        for w in writes {
            w.await.unwrap().unwrap();
        }
        assert!(s
            .get("ca")
            .await
            .unwrap()
            .unwrap()
            .expose()
            .starts_with('v'));
        let left: Vec<_> = std::fs::read_dir(dir.path().join("s")).unwrap().collect();
        assert_eq!(left.len(), 1, "temp files left behind");
    }

    #[tokio::test]
    async fn set_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let s = FileSecretStore::new(dir.path().join("s"));
        assert!(s.get("pw").await.unwrap().is_none());
        s.set("pw", Secret::new("hunter2")).await.unwrap();
        assert_eq!(s.get("pw").await.unwrap().unwrap().expose(), "hunter2");
        s.set("pw", Secret::new("again")).await.unwrap();
        assert_eq!(s.get("pw").await.unwrap().unwrap().expose(), "again");
        s.delete("pw").await.unwrap();
        s.delete("pw").await.unwrap();
        assert!(s.get("pw").await.unwrap().is_none());
        for bad in ["", "../x", ".hidden", "a/b"] {
            assert!(s.get(bad).await.is_err(), "{bad}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn files_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let s = FileSecretStore::new(dir.path().join("s"));
        s.set("pw", Secret::new("x")).await.unwrap();
        let mode = |p: PathBuf| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(dir.path().join("s")), 0o700);
        assert_eq!(mode(dir.path().join("s").join("pw")), 0o600);
    }
}
