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
/// Code for a keychain prompt that was cancelled.
pub const KEYCHAIN_CANCELLED: &str = "MRL-ENV-162";

/// Where secrets are kept.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeychainBackend {
    /// The OS keychain; the encrypted file only where there is no keychain service.
    #[default]
    Auto,
    /// Always the encrypted file (development machines, CI).
    File,
}

/// `[keychain]` settings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct KeychainConfig {
    /// `auto` (default) or `file`. Secrets do not move between the two: choose before
    /// first use, or sign in and set up sync again after a change.
    pub backend: KeychainBackend,
}

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
    /// The store this install used before; on first use, the OS keychain, or the encrypted
    /// file `<fallback_dir>/<service>.vault` (after a warning) only when there is no keychain
    /// service at all (a Linux box without Secret Service).
    ///
    /// The choice sticks: a keychain that is locked or fails for a while is reported
    /// (MRL-ENV-161) and keeps being used, so saved secrets never seem to vanish and new ones
    /// never go to a second store.
    ///
    /// With [`KeychainBackend::File`], always the encrypted file.
    pub async fn open(service: &str, fallback_dir: &Path, backend: KeychainBackend) -> Self {
        let marker = fallback_dir.join(format!("{service}.os-keychain"));
        let file = Self::file(service, fallback_dir);
        if backend == KeychainBackend::File {
            tracing::info!(
                folder = %fallback_dir.display(),
                "secrets are kept in an encrypted file (keychain.backend = \"file\")"
            );
            return file;
        }
        if marker.exists() {
            return Self::os(service);
        }
        if let Backend::File(v) = &file.backend {
            if v.path.exists() || v.key_path.exists() {
                return file;
            }
        }
        Self::after_probe(service, fallback_dir, file, probe(service).await)
    }

    /// The store for a first use, from the probe's answer.
    fn after_probe(
        service: &str,
        fallback_dir: &Path,
        file: Self,
        probe: Result<(), keyring::Error>,
    ) -> Self {
        let marker = fallback_dir.join(format!("{service}.os-keychain"));
        match probe {
            Ok(()) => {
                let written = std::fs::create_dir_all(fallback_dir)
                    .and_then(|()| std::fs::write(&marker, b"os keychain\n"));
                if let Err(e) = written {
                    tracing::warn!(error = %e, "could not note that this install uses the keychain");
                }
                Self::os(service)
            }
            Err(e) if no_keychain_service(&e) => {
                tracing::warn!(
                    code = NO_KEYCHAIN,
                    folder = %fallback_dir.display(),
                    "this computer has no keychain service; secrets are kept in an encrypted file \
                     that only this folder's permissions protect"
                );
                file
            }
            Err(e) => {
                tracing::error!(
                    code = KEYCHAIN_FAILED,
                    error = %keychain_error("read the keychain", &e),
                    "the keychain did not answer; secrets stay in it, so unlock it and try again"
                );
                Self::os(service)
            }
        }
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

/// Copies secret `name` into `to` from the first of `from` that has it, when `to` lacks it.
/// Returns where it came from. A cancelled keychain prompt on a source is returned (the
/// secret is still there; allowing access once moves it); other source failures are logged
/// and that source skipped.
///
/// Used for the bundled database's password when `keychain.backend` changes: without it the
/// existing database could not be opened.
pub async fn carry_secret(
    name: &str,
    to: &dyn SecretStore,
    from: &[(&str, &dyn SecretStore)],
) -> Result<Option<String>, SeamError> {
    if to.get(name).await?.is_some() {
        return Ok(None);
    }
    for (label, source) in from {
        match source.get(name).await {
            Ok(Some(v)) => {
                to.set(name, v).await?;
                return Ok(Some((*label).to_owned()));
            }
            Ok(None) => {}
            Err(e) if e.detail.starts_with(KEYCHAIN_CANCELLED) => return Err(e),
            Err(e) => {
                tracing::warn!(source = label, error = %e, "could not look for a secret there")
            }
        }
    }
    Ok(None)
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
    let text = e.to_string();
    let lower = text.to_ascii_lowercase();
    if lower.contains("cancel") || lower.contains("dismissed") {
        return SeamError::new(
            "secrets",
            format!(
                "{KEYCHAIN_CANCELLED}: the keychain request was cancelled, so the app could not {what}. \
                 Start it again and choose Allow when the keychain asks, or set backend = \"file\" \
                 under [keychain] in the settings file to keep secrets in an encrypted file. After \
                 that switch the bundled database's password is copied from the keychain once, so \
                 allow that one read; sign-in and sync keys are not copied (sign in again)"
            ),
        );
    }
    SeamError::new(
        "secrets",
        format!("{KEYCHAIN_FAILED}: could not {what}: {text}"),
    )
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, SeamError> + Send + 'static,
) -> Result<T, SeamError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| SeamError::new("secrets", format!("keychain call stopped: {e}")))?
}

/// Reads a probe entry from the OS keychain (a missing entry counts as an answer).
async fn probe(service: &str) -> Result<(), keyring::Error> {
    let service = service.to_owned();
    let read = tokio::task::spawn_blocking(move || {
        match keyring::Entry::new(&service, "keychain-probe")?.get_password() {
            Ok(_) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(e),
        }
    })
    .await;
    read.unwrap_or_else(|e| Err(keyring::Error::NoStorageAccess(Box::new(e))))
}

/// What the Linux Secret Service driver reports when there is no provider or no D-Bus
/// session (`secret_service::Error::Unavailable`, and D-Bus finding no provider to start).
const NO_SECRET_SERVICE: [&str; 2] = [
    "no secret service provider or dbus session found",
    "org.freedesktop.DBus.Error.ServiceUnknown",
];

/// True only for a confirmed absence: no store for this platform, or (Linux and other Unix)
/// Secret Service saying there is no provider or session. A locked store, a timeout or any
/// other failure is not absence.
fn no_keychain_service(e: &keyring::Error) -> bool {
    match e {
        keyring::Error::NoDefaultStore => true,
        keyring::Error::PlatformFailure(p) => {
            let text = p.to_string();
            cfg!(not(any(target_os = "macos", windows)))
                && NO_SECRET_SERVICE.iter().any(|m| text.contains(m))
        }
        _ => false,
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
                blocking(move || Ok(os_get(&service, &name)?.map(Secret::new))).await
            }
        }
    }

    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError> {
        check_name(name)?;
        match &self.backend {
            Backend::File(v) => v.set(name, Some(value)).await,
            Backend::Os => {
                let (service, name) = (self.service.clone(), name.to_owned());
                blocking(move || os_put(&service, &name, value.expose())).await
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
                    for i in 0..os_parts(&service, &name)? {
                        os_remove(&service, &part_name(&name, i))?;
                    }
                    os_remove(&service, &name)
                })
                .await
            }
        }
    }
}

/// Longest value kept in one OS keychain entry. Windows Credential Manager holds at most
/// 2,560 bytes (UTF-16) per credential; 600 characters stay under that even when each takes
/// two UTF-16 units. Longer values (a Supabase session is several KiB) are split.
const ENTRY_CHARS: usize = 600;
/// What the main entry of a split value holds, followed by the number of parts.
const SPLIT: &str = "mrl-split/v1:";

fn part_name(name: &str, i: usize) -> String {
    format!("{name}.part{i}")
}

fn os_read(service: &str, name: &str) -> Result<Option<String>, SeamError> {
    let entry =
        keyring::Entry::new(service, name).map_err(|e| keychain_error("open the keychain", &e))?;
    match entry.get_password() {
        Ok(v) => Ok(Some(v)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(keychain_error("read a secret", &e)),
    }
}

fn os_write(service: &str, name: &str, value: &str) -> Result<(), SeamError> {
    keyring::Entry::new(service, name)
        .and_then(|e| e.set_password(value))
        .map_err(|e| keychain_error("save a secret", &e))
}

fn os_remove(service: &str, name: &str) -> Result<(), SeamError> {
    let entry =
        keyring::Entry::new(service, name).map_err(|e| keychain_error("open the keychain", &e))?;
    match entry.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(keychain_error("delete a secret", &e)),
    }
}

/// How many parts the value under `name` is split into (0: one entry, or none).
fn os_parts(service: &str, name: &str) -> Result<usize, SeamError> {
    Ok(os_read(service, name)?
        .and_then(|head| head.strip_prefix(SPLIT)?.parse().ok())
        .unwrap_or(0))
}

fn os_get(service: &str, name: &str) -> Result<Option<String>, SeamError> {
    let Some(head) = os_read(service, name)? else {
        return Ok(None);
    };
    let Some(count) = head.strip_prefix(SPLIT) else {
        return Ok(Some(head));
    };
    let damaged = || {
        SeamError::new(
            "secrets",
            format!("{KEYCHAIN_FAILED}: a saved secret is damaged (a part is missing)"),
        )
    };
    let count: usize = count.parse().map_err(|_| damaged())?;
    let mut value = String::new();
    for i in 0..count {
        value.push_str(&os_read(service, &part_name(name, i))?.ok_or_else(damaged)?);
    }
    Ok(Some(value))
}

/// Saves `value`, split into parts when it is long. Parts are written before the entry that
/// names them, and parts a longer earlier value left are removed after.
fn os_put(service: &str, name: &str, value: &str) -> Result<(), SeamError> {
    let before = os_parts(service, name)?;
    let chars: Vec<char> = value.chars().collect();
    let parts: Vec<String> = if chars.len() <= ENTRY_CHARS && !value.starts_with(SPLIT) {
        os_write(service, name, value)?;
        Vec::new()
    } else {
        let parts: Vec<String> = chars
            .chunks(ENTRY_CHARS)
            .map(|c| c.iter().collect())
            .collect();
        for (i, part) in parts.iter().enumerate() {
            os_write(service, &part_name(name, i), part)?;
        }
        os_write(service, name, &format!("{SPLIT}{}", parts.len()))?;
        parts
    };
    for i in parts.len()..before {
        os_remove(service, &part_name(name, i))?;
    }
    Ok(())
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

    /// A store switch on an existing install: the bundled database still opens and keeps its
    /// data. The "before" store stands in for the OS keychain.
    #[tokio::test]
    #[ignore = "needs PostgreSQL programs"]
    async fn the_database_survives_a_switch_to_the_file_backend() {
        use crate::pgbundle::{BundleOptions, PgBinaries, PgBundle, PASSWORD_SECRET};
        use std::sync::Arc;
        let configured = std::env::var_os("MRL_TEST_PG_BIN").map(std::path::PathBuf::from);
        let bins = match PgBinaries::locate(configured.as_deref()).await {
            Ok(b) => b,
            Err(e) if std::env::var_os("CI").is_some() => {
                panic!("CI needs PostgreSQL programs: {e}")
            }
            Err(e) => return eprintln!("skipped: {e}"),
        };
        let dir = tempfile::tempdir().unwrap();
        let options = |secrets: Arc<dyn SecretStore>| BundleOptions {
            data_dir: dir.path().join("data"),
            binaries: bins.clone(),
            previous: None,
            backups_keep: 2,
            secrets,
        };
        let connect = |b: &PgBundle| {
            crate::db::Db::connect_lazy(
                &crate::config::DatabaseConfig::default(),
                b.connect_options(),
            )
        };
        let before: Arc<dyn SecretStore> =
            Arc::new(KeychainStore::file("mrl-db", &dir.path().join("os")));
        let b = PgBundle::start(options(before.clone())).await.unwrap();
        let db = connect(&b);
        sqlx::raw_sql("CREATE TABLE kept (v int); INSERT INTO kept VALUES (42);")
            .execute(db.pool())
            .await
            .unwrap();
        db.pool().close().await;
        b.stop().await.unwrap();

        let after: Arc<dyn SecretStore> =
            Arc::new(KeychainStore::file("mrl-db", &dir.path().join("file")));
        assert!(
            PgBundle::start(options(after.clone())).await.is_err(),
            "no password, no database"
        );
        let from: [(&str, &dyn SecretStore); 1] = [("OS keychain", before.as_ref())];
        let carried = carry_secret(PASSWORD_SECRET, after.as_ref(), &from)
            .await
            .unwrap();
        assert_eq!(carried.as_deref(), Some("OS keychain"));
        let b = PgBundle::start(options(after)).await.unwrap();
        let db = connect(&b);
        let v: i32 = sqlx::query_scalar("SELECT v FROM kept")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(v, 42);
        db.pool().close().await;
        b.stop().await.unwrap();
    }

    #[tokio::test]
    async fn carrying_a_secret_copies_once_and_stops_at_a_cancelled_prompt() {
        let dir = tempfile::tempdir().unwrap();
        let to = KeychainStore::file("mrl-to", dir.path());
        let from = KeychainStore::file("mrl-from", dir.path());
        from.set("pw", Secret::new("p1")).await.unwrap();
        let sources: [(&str, &dyn SecretStore); 1] = [("from", &from)];
        assert_eq!(
            carry_secret("pw", &to, &sources).await.unwrap().as_deref(),
            Some("from")
        );
        assert_eq!(to.get("pw").await.unwrap().unwrap().expose(), "p1");
        from.set("pw", Secret::new("p2")).await.unwrap();
        assert_eq!(
            carry_secret("pw", &to, &sources).await.unwrap(),
            None,
            "kept, not overwritten"
        );
        struct Cancelled;
        #[async_trait]
        impl SecretStore for Cancelled {
            async fn get(&self, _: &str) -> Result<Option<Secret>, SeamError> {
                Err(keychain_error(
                    "read the keychain",
                    &keyring::Error::PlatformFailure("User canceled the operation.".into()),
                ))
            }
            async fn set(&self, _: &str, _: Secret) -> Result<(), SeamError> {
                Ok(())
            }
            async fn delete(&self, _: &str) -> Result<(), SeamError> {
                Ok(())
            }
        }
        let empty = KeychainStore::file("mrl-empty", dir.path());
        let cancelled: [(&str, &dyn SecretStore); 1] = [("os", &Cancelled)];
        let e = carry_secret("pw", &empty, &cancelled).await.unwrap_err();
        assert!(e.detail.starts_with(KEYCHAIN_CANCELLED), "{e}");
    }

    #[test]
    fn a_cancelled_prompt_is_a_plain_error_with_a_way_out() {
        let cancelled = keyring::Error::PlatformFailure("User canceled the operation.".into());
        let e = keychain_error("read the keychain", &cancelled);
        assert!(e.detail.starts_with(KEYCHAIN_CANCELLED), "{e}");
        assert!(e.detail.contains("backend = \"file\""), "{e}");
        let locked = keyring::Error::NoStorageAccess("locked".into());
        assert!(keychain_error("read", &locked)
            .detail
            .starts_with(KEYCHAIN_FAILED));
        let settings: KeychainConfig = toml::from_str("backend = \"file\"").unwrap();
        assert_eq!(settings.backend, KeychainBackend::File);
        assert!(toml::from_str::<KeychainConfig>("backend = \"cloud\"").is_err());
    }

    #[tokio::test]
    async fn the_file_backend_is_used_when_chosen() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mrl-pick.os-keychain"), b"").unwrap();
        let store = KeychainStore::open("mrl-pick", dir.path(), KeychainBackend::File).await;
        assert!(store.uses_fallback());
        round_trip(&store).await;
    }

    #[test]
    fn only_a_missing_keychain_service_selects_the_file() {
        let platform =
            || -> Box<dyn std::error::Error + Send + Sync> { "dbus: no such service".into() };
        assert!(no_keychain_service(&keyring::Error::NoDefaultStore));
        assert!(!no_keychain_service(&keyring::Error::NoStorageAccess(
            platform()
        )));
        let absent = || -> Box<dyn std::error::Error + Send + Sync> {
            "no secret service provider or dbus session found".into()
        };
        assert_eq!(
            no_keychain_service(&keyring::Error::PlatformFailure(absent())),
            cfg!(not(any(target_os = "macos", windows)))
        );
        // A transient failure (like a D-Bus timeout) is never absence.
        assert!(!no_keychain_service(&keyring::Error::PlatformFailure(
            platform()
        )));
    }

    #[test]
    fn a_transient_failure_on_a_new_install_keeps_the_keychain() {
        let dir = tempfile::tempdir().unwrap();
        let file = KeychainStore::file("mrl-new", dir.path());
        let timeout = keyring::Error::PlatformFailure("D-Bus method call timed out".into());
        let store = KeychainStore::after_probe("mrl-new", dir.path(), file, Err(timeout));
        assert!(!store.uses_fallback(), "reported, not swapped for the file");
        assert!(!dir.path().join("mrl-new.vault").exists());
    }

    #[tokio::test]
    async fn the_store_chosen_first_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        // A vault from earlier stays in use even where a keychain now answers.
        let first = KeychainStore::file("mrl-sticky", dir.path());
        first.set("x", Secret::new("v")).await.unwrap();
        let again = KeychainStore::open("mrl-sticky", dir.path(), KeychainBackend::Auto).await;
        assert!(again.uses_fallback());
        assert_eq!(again.get("x").await.unwrap().unwrap().expose(), "v");
        // An install noted as using the keychain never moves to a file, whatever the probe says.
        let other = tempfile::tempdir().unwrap();
        std::fs::write(other.path().join("mrl-sticky.os-keychain"), b"").unwrap();
        assert!(
            !KeychainStore::open("mrl-sticky", other.path(), KeychainBackend::Auto)
                .await
                .uses_fallback()
        );
    }

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
        let store = KeychainStore::open(&service, dir.path(), KeychainBackend::Auto).await;
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
        // A realistic Supabase session (several KiB, with non-ASCII names) round-trips too,
        // and shrinking it leaves no stale parts behind.
        let session = serde_json::json!({
            "access_token": format!("eyJ{}", "a".repeat(1400)),
            "refresh_token": "r".repeat(40),
            "expires_at": 1_900_000_000,
            "user": {"id": uuid::Uuid::new_v4().to_string(), "email": "zoë@example.com",
                     "display_name": "Zoë Ñúñez 🧪".repeat(40), "avatar_url": format!("https://example.com/{}", "p".repeat(300))},
            "issuer": "https://abcd.supabase.co/auth/v1",
        })
        .to_string();
        assert!(session.len() > 2_560);
        store
            .set("auth.supabase_session", Secret::new(session.clone()))
            .await
            .unwrap();
        assert_eq!(
            store
                .get("auth.supabase_session")
                .await
                .unwrap()
                .unwrap()
                .expose(),
            session
        );
        store
            .set("auth.supabase_session", Secret::new("short"))
            .await
            .unwrap();
        assert_eq!(
            store
                .get("auth.supabase_session")
                .await
                .unwrap()
                .unwrap()
                .expose(),
            "short"
        );
        store
            .set("auth.supabase_session", Secret::new(session.clone()))
            .await
            .unwrap();
        store.delete("auth.supabase_session").await.unwrap();
        assert!(store.get("auth.supabase_session").await.unwrap().is_none());
        let vault = dir.path().join(format!("{service}.vault"));
        let key = dir.path().join(format!("{service}.vault-key"));
        assert!(
            store.uses_fallback() || !(vault.exists() || key.exists()),
            "the OS backend must not write the fallback file"
        );
        // The choice is kept: opening again picks the same store.
        let again = KeychainStore::open(&service, dir.path(), KeychainBackend::Auto).await;
        assert_eq!(again.uses_fallback(), store.uses_fallback());
    }
}
