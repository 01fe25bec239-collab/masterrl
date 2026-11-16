//! `SecretStore` backed by the OS keychain: macOS Keychain, Windows Credential Manager,
//! Linux Secret Service. Without a keychain service (a headless Linux box), secrets go to an
//! encrypted file after a warning (MRL-ENV-160).
//!
//! Secrets are found by name (a reference such as `auth.supabase_session`). Values never go
//! to the database or logs; errors never include them.

use crate::modules::sync::crypto;
use crate::seams::{SeamError, Secret, SecretStore};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Code logged when the encrypted-file fallback is in use.
pub const NO_KEYCHAIN: &str = "MRL-ENV-160";
/// Code for a keychain that exists but refused a read or write.
pub const KEYCHAIN_FAILED: &str = "MRL-ENV-161";

/// Secrets in the OS keychain under one service name, or in the fallback file.
pub struct KeychainStore {
    service: String,
    backend: Backend,
}

enum Backend {
    Os,
    File(FileVault),
}

impl KeychainStore {
    /// The OS keychain when this computer has one; otherwise the encrypted file
    /// `<fallback_dir>/<service>.vault`, after logging a warning.
    pub async fn open(service: &str, fallback_dir: &Path) -> Self {
        if os_keychain_works(service).await {
            return Self::os(service);
        }
        tracing::warn!(
            code = NO_KEYCHAIN,
            folder = %fallback_dir.display(),
            "this computer has no keychain service; secrets are kept in an encrypted file \
             that only this folder's permissions protect"
        );
        Self::file(service, fallback_dir)
    }

    /// The OS keychain, with no fallback.
    pub fn os(service: &str) -> Self {
        Self {
            service: service.to_owned(),
            backend: Backend::Os,
        }
    }

    /// The encrypted file in `dir`, even when a keychain exists.
    pub fn file(service: &str, dir: &Path) -> Self {
        Self {
            service: service.to_owned(),
            backend: Backend::File(FileVault {
                path: dir.join(format!("{service}.vault")),
                key_path: dir.join(format!("{service}.vault-key")),
                dir: dir.to_owned(),
                lock: tokio::sync::Mutex::new(()),
            }),
        }
    }

    /// True when secrets go to the encrypted file instead of the OS keychain.
    pub fn uses_fallback(&self) -> bool {
        matches!(self.backend, Backend::File(_))
    }
}

fn check_name(name: &str) -> Result<(), SeamError> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    if ok {
        Ok(())
    } else {
        Err(SeamError::new(
            "secrets",
            "secret names use 1–128 letters, digits, '.', '_' and '-'",
        ))
    }
}

fn keychain_error(what: &str, e: &keyring::Error) -> SeamError {
    // keyring errors name the platform failure, never the value.
    SeamError::new(
        "secrets",
        format!("{KEYCHAIN_FAILED}: could not {what}: {e}"),
    )
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, SeamError> + Send + 'static,
) -> Result<T, SeamError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| SeamError::new("secrets", format!("keychain call stopped: {e}")))?
}

/// True when the OS keychain answers a read (a missing entry counts as an answer).
async fn os_keychain_works(service: &str) -> bool {
    let service = service.to_owned();
    let probe = blocking(move || {
        let entry = keyring::Entry::new(&service, "keychain-probe")
            .map_err(|e| keychain_error("open the keychain", &e))?;
        match entry.get_password() {
            Ok(_) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(keychain_error("read the keychain", &e)),
        }
    })
    .await;
    match probe {
        Ok(()) => true,
        Err(e) => {
            tracing::debug!(error = %e, "keychain probe failed");
            false
        }
    }
}

#[async_trait]
impl SecretStore for KeychainStore {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError> {
        check_name(name)?;
        match &self.backend {
            Backend::File(v) => v.get(name).await,
            Backend::Os => {
                let (service, name) = (self.service.clone(), name.to_owned());
                blocking(move || {
                    let entry = keyring::Entry::new(&service, &name)
                        .map_err(|e| keychain_error("open the keychain", &e))?;
                    match entry.get_password() {
                        Ok(v) => Ok(Some(Secret::new(v))),
                        Err(keyring::Error::NoEntry) => Ok(None),
                        Err(e) => Err(keychain_error("read a secret", &e)),
                    }
                })
                .await
            }
        }
    }

    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError> {
        check_name(name)?;
        match &self.backend {
            Backend::File(v) => v.set(name, Some(value)).await,
            Backend::Os => {
                let (service, name) = (self.service.clone(), name.to_owned());
                blocking(move || {
                    keyring::Entry::new(&service, &name)
                        .and_then(|e| e.set_password(value.expose()))
                        .map_err(|e| keychain_error("save a secret", &e))
                })
                .await
            }
        }
    }

    async fn delete(&self, name: &str) -> Result<(), SeamError> {
        check_name(name)?;
        match &self.backend {
            Backend::File(v) => v.set(name, None).await,
            Backend::Os => {
                let (service, name) = (self.service.clone(), name.to_owned());
                blocking(move || {
                    let entry = keyring::Entry::new(&service, &name)
                        .map_err(|e| keychain_error("open the keychain", &e))?;
                    match entry.delete_credential() {
                        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
                        Err(e) => Err(keychain_error("delete a secret", &e)),
                    }
                })
                .await
            }
        }
    }
}

/// The fallback: every secret of a service in one file, sealed with a key kept in a second
/// private file next to it.
///
/// ponytail: the key sits beside the data, so this protects against copied backups of the
/// vault alone, not against someone who can read the folder; ask for a passphrase at startup
/// if headless installs need more.
struct FileVault {
    dir: PathBuf,
    path: PathBuf,
    key_path: PathBuf,
    lock: tokio::sync::Mutex<()>,
}

const VAULT_AAD: &[u8] = b"mrl-keychain-vault/v1";

fn io(what: &str, e: &std::io::Error) -> SeamError {
    SeamError::new("secrets", format!("could not {what}: {e}"))
}

impl FileVault {
    fn key(&self, create: bool) -> Result<Option<[u8; crypto::KEY_LEN]>, SeamError> {
        match std::fs::read(&self.key_path) {
            Ok(bytes) => bytes
                .try_into()
                .map(Some)
                .map_err(|_| SeamError::new("secrets", "the secret file's key is damaged")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && create => {
                let key = crypto::random::<{ crypto::KEY_LEN }>()
                    .map_err(|e| SeamError::new("secrets", e.to_string()))?;
                crate::pgbundle::create_private_dir(&self.dir)
                    .map_err(|e| io("create the secret folder", &e))?;
                crate::pgbundle::write_private(&self.key_path, &key)
                    .map_err(|e| io("write the secret file's key", &e))?;
                Ok(Some(key))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io("read the secret file's key", &e)),
        }
    }

    fn load(&self, key: &[u8; crypto::KEY_LEN]) -> Result<BTreeMap<String, String>, SeamError> {
        let sealed = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(io("read the secret file", &e)),
        };
        let plain = crypto::open(key, VAULT_AAD, &sealed)
            .map_err(|_| SeamError::new("secrets", "the secret file is damaged"))?;
        serde_json::from_slice(&plain)
            .map_err(|_| SeamError::new("secrets", "the secret file is damaged"))
    }

    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError> {
        let _guard = self.lock.lock().await;
        let Some(key) = self.key(false)? else {
            return Ok(None);
        };
        Ok(self.load(&key)?.remove(name).map(Secret::new))
    }

    async fn set(&self, name: &str, value: Option<Secret>) -> Result<(), SeamError> {
        let _guard = self.lock.lock().await;
        let Some(key) = self.key(value.is_some())? else {
            return Ok(());
        };
        let mut map = self.load(&key)?;
        match value {
            Some(v) => map.insert(name.to_owned(), v.expose().to_owned()),
            None => map.remove(name),
        };
        let plain = serde_json::to_vec(&map)
            .map_err(|e| SeamError::new("secrets", format!("could not encode secrets: {e}")))?;
        let sealed = crypto::seal(&key, VAULT_AAD, &plain)
            .map_err(|e| SeamError::new("secrets", e.to_string()))?;
        let tmp = self.path.with_extension("vault.tmp");
        crate::pgbundle::write_private(&tmp, &sealed)
            .map_err(|e| io("write the secret file", &e))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| io("save the secret file", &e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn round_trip(store: &KeychainStore) {
        let name = format!("test.{}", uuid::Uuid::new_v4().simple());
        assert_eq!(store.get(&name).await.unwrap(), None);
        store.set(&name, Secret::new("first value")).await.unwrap();
        assert_eq!(
            store.get(&name).await.unwrap().unwrap().expose(),
            "first value"
        );
        store.set(&name, Secret::new("second value")).await.unwrap();
        assert_eq!(
            store.get(&name).await.unwrap().unwrap().expose(),
            "second value"
        );
        store.delete(&name).await.unwrap();
        store.delete(&name).await.unwrap();
        assert_eq!(store.get(&name).await.unwrap(), None);
        assert!(store.get("bad/name").await.is_err());
        assert!(store.set("", Secret::new("x")).await.is_err());
    }

    #[tokio::test]
    async fn file_fallback_round_trips_and_never_stores_plain_text() {
        let dir = tempfile::tempdir().unwrap();
        let store = KeychainStore::file("mrl-test", dir.path());
        assert!(store.uses_fallback());
        round_trip(&store).await;
        store
            .set("canary", Secret::new("canary-value-8d1f"))
            .await
            .unwrap();
        let raw = std::fs::read(dir.path().join("mrl-test.vault")).unwrap();
        assert!(!String::from_utf8_lossy(&raw).contains("canary-value-8d1f"));
        // A second store on the same folder reads what the first wrote.
        let again = KeychainStore::file("mrl-test", dir.path());
        assert_eq!(
            again.get("canary").await.unwrap().unwrap().expose(),
            "canary-value-8d1f"
        );
        // Another service in the same folder is separate.
        let other = KeychainStore::file("mrl-other", dir.path());
        assert_eq!(other.get("canary").await.unwrap(), None);
    }

    #[tokio::test]
    async fn damaged_vault_is_an_error_not_an_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = KeychainStore::file("mrl-test", dir.path());
        store.set("a", Secret::new("b")).await.unwrap();
        std::fs::write(dir.path().join("mrl-test.vault"), b"garbage").unwrap();
        let err = store.get("a").await.unwrap_err();
        assert!(err.detail.contains("damaged"), "{err}");
    }

    /// The real OS keychain, or the fallback where there is none. Run by `ci-keychain` on
    /// macOS, Windows, Linux with Secret Service, and Linux without it.
    #[tokio::test]
    #[ignore = "needs an OS keychain; run by ci-keychain"]
    async fn os_keychain_round_trips() {
        let service = format!("masterrl-test-{}", uuid::Uuid::new_v4().simple());
        let dir = tempfile::tempdir().unwrap();
        let store = KeychainStore::open(&service, dir.path()).await;
        if std::env::var_os("MRL_EXPECT_KEYCHAIN").is_some() {
            assert!(
                !store.uses_fallback(),
                "this runner should have a keychain service"
            );
        }
        if std::env::var_os("MRL_EXPECT_NO_KEYCHAIN").is_some() {
            assert!(
                store.uses_fallback(),
                "this runner has no keychain, so the file must be used"
            );
        }
        round_trip(&store).await;
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none() || store.uses_fallback(),
            "the OS backend must not write the fallback file"
        );
    }
}
