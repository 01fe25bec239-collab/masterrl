//! Where the node's private key lives: the OS keychain (Keychain on macOS, Credential Manager
//! on Windows, Secret Service on Linux) or, when that is unavailable (a headless server), a
//! key file readable only by this account. `identity.json` holds only a [`KeyRef`].

use crate::session::LinkError;
use crate::state::atomic_write;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Keychain service name of agent keys.
pub const SERVICE: &str = "dev.masterrl.agent";

/// `gateway.key_store`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStoreChoice {
    /// The OS keychain when it works, else a key file.
    #[default]
    Auto,
    /// The OS keychain only.
    Keychain,
    /// A key file in the identity folder (mode 0600 on Unix).
    File,
}

/// Where one key is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "store", rename_all = "snake_case")]
pub enum KeyRef {
    /// An OS keychain entry.
    Keychain {
        /// Entry account name.
        account: String,
    },
    /// A file relative to the identity folder.
    File {
        /// Like `keys/<name>.key`.
        file: String,
    },
}

impl KeyRef {
    /// The store this key is in.
    pub fn store(&self) -> KeyStoreChoice {
        match self {
            Self::Keychain { .. } => KeyStoreChoice::Keychain,
            Self::File { .. } => KeyStoreChoice::File,
        }
    }
}

fn keychain_err(e: impl std::fmt::Display) -> LinkError {
    LinkError::Certificate(format!(
        "the OS keychain could not store the machine key: {e}"
    ))
}

fn io_err(path: &Path, e: std::io::Error) -> LinkError {
    LinkError::Agent(crate::AgentError::state(path, e))
}

/// Stores `der` (named after its certificate) and returns where it went.
pub fn put(
    identity_dir: &Path,
    choice: KeyStoreChoice,
    name: &str,
    der: &[u8],
) -> Result<KeyRef, LinkError> {
    let keychain = || -> Result<KeyRef, LinkError> {
        // One account per state folder and certificate, so several agents never collide.
        let account = format!(
            "{}:{name}",
            &crate::session::tls::sha256_hex(identity_dir.to_string_lossy().as_bytes())[..12]
        );
        let entry = keyring::Entry::new(SERVICE, &account).map_err(keychain_err)?;
        entry.set_secret(der).map_err(keychain_err)?;
        if entry.get_secret().map_err(keychain_err)? != der {
            return Err(keychain_err("the stored key reads back differently"));
        }
        Ok(KeyRef::Keychain { account })
    };
    match choice {
        KeyStoreChoice::Keychain => keychain(),
        KeyStoreChoice::File => put_file(identity_dir, name, der),
        KeyStoreChoice::Auto => keychain().or_else(|e| {
            eprintln!(
                "mrl-agent: {}; keeping the key in a private file instead",
                e.explain()
            );
            put_file(identity_dir, name, der)
        }),
    }
}

fn put_file(identity_dir: &Path, name: &str, der: &[u8]) -> Result<KeyRef, LinkError> {
    let keys = identity_dir.join("keys");
    std::fs::create_dir_all(&keys).map_err(|e| io_err(&keys, e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&keys, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| io_err(&keys, e))?;
    }
    let file = format!("keys/{name}.key");
    let path = identity_dir.join(&file);
    atomic_write(&path, der).map_err(|e| io_err(&path, e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| io_err(&path, e))?;
    }
    Ok(KeyRef::File { file })
}

/// Reads the key `r` points to.
pub fn get(identity_dir: &Path, r: &KeyRef) -> Result<Vec<u8>, LinkError> {
    match r {
        KeyRef::Keychain { account } => keyring::Entry::new(SERVICE, account)
            .and_then(|e| e.get_secret())
            .map_err(|e| {
                LinkError::NotEnrolled(format!(
                    "the machine key is missing from the OS keychain ({e}); enroll again"
                ))
            }),
        KeyRef::File { file } => {
            let path = identity_dir.join(file);
            std::fs::read(&path).map_err(|e| io_err(&path, e))
        }
    }
}

/// Removes the key `r` points to (best effort: a leftover old key is harmless).
pub fn delete(identity_dir: &Path, r: &KeyRef) {
    match r {
        KeyRef::Keychain { account } => {
            if let Ok(e) = keyring::Entry::new(SERVICE, account) {
                let _ = e.delete_credential();
            }
        }
        KeyRef::File { file } => {
            let _ = std::fs::remove_file(identity_dir.join(file));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_keys_are_private_and_removable() {
        let dir = tempfile::tempdir().unwrap();
        let r = put(dir.path(), KeyStoreChoice::File, "abc", b"secret-der").unwrap();
        assert_eq!(
            r,
            KeyRef::File {
                file: "keys/abc.key".into()
            }
        );
        assert_eq!(get(dir.path(), &r).unwrap(), b"secret-der");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m = std::fs::metadata(dir.path().join("keys/abc.key"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(m & 0o777, 0o600);
        }
        delete(dir.path(), &r);
        assert!(get(dir.path(), &r).is_err());
    }

    /// The real OS keychain; set `MRL_TEST_KEYCHAIN=1` on a machine with an unlocked keychain.
    #[test]
    #[ignore = "uses the OS keychain"]
    fn keychain_round_trip() {
        if std::env::var("MRL_TEST_KEYCHAIN").is_err() {
            eprintln!("SKIPPED: set MRL_TEST_KEYCHAIN=1");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let r = put(dir.path(), KeyStoreChoice::Keychain, "rt", b"k").unwrap();
        assert_eq!(get(dir.path(), &r).unwrap(), b"k");
        delete(dir.path(), &r);
    }
}
