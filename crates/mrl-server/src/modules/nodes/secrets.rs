//! A `SecretStore` every replica shares: rows in `gateway_secrets`, each sealed with
//! XChaCha20-Poly1305 under a 256-bit key-encryption key (KEK) that comes from the environment
//! (`gateway.kek_env`, default `MRL_GATEWAY_KEK`, 64 hex characters) and never reaches the
//! database. The row name is bound into each seal, so a sealed value can't be moved to another
//! name. Team mode on several hosts sets the same KEK on each host.

use crate::db::Db;
use crate::seams::{SeamError, Secret, SecretStore};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use uuid::Uuid;

/// The database-backed, sealed secret store.
pub struct PgSecretStore {
    db: Db,
    cipher: XChaCha20Poly1305,
}

fn err(detail: impl Into<String>) -> SeamError {
    SeamError::new("gateway_secrets", detail)
}

/// Reads a KEK given as 64 hex characters.
pub fn parse_kek(hex: &str) -> Result<[u8; 32], String> {
    let hex = hex.trim();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("the key-encryption key must be 64 hex characters (256 bits); make one with `openssl rand -hex 32`".into());
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

impl PgSecretStore {
    /// A store sealing with `kek`.
    pub fn new(db: Db, kek: &[u8; 32]) -> Result<Self, SeamError> {
        let cipher = XChaCha20Poly1305::new_from_slice(kek).map_err(|e| err(e.to_string()))?;
        Ok(Self { db, cipher })
    }

    fn nonce() -> [u8; 24] {
        let mut n = [0u8; 24];
        n[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        n[16..].copy_from_slice(&Uuid::new_v4().as_bytes()[..8]);
        n
    }
}

#[async_trait::async_trait]
impl SecretStore for PgSecretStore {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SeamError> {
        let row: Option<(Vec<u8>, Vec<u8>)> =
            sqlx::query_as("SELECT nonce, sealed FROM gateway_secrets WHERE name = $1")
                .bind(name)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|e| err(e.to_string()))?;
        let Some((nonce, sealed)) = row else {
            return Ok(None);
        };
        let nonce =
            XNonce::try_from(nonce.as_slice()).map_err(|_| err("a stored nonce is damaged"))?;
        let plain = self
            .cipher
            .decrypt(&nonce, Payload { msg: &sealed, aad: name.as_bytes() })
            .map_err(|_| err("the secret could not be opened: this server's key-encryption key differs from the one that sealed it"))?;
        let text = String::from_utf8(plain).map_err(|_| err("a stored secret is not text"))?;
        Ok(Some(Secret::new(text)))
    }

    async fn set(&self, name: &str, value: Secret) -> Result<(), SeamError> {
        let nonce_bytes = Self::nonce();
        let nonce = XNonce::try_from(&nonce_bytes[..]).map_err(|_| err("nonce"))?;
        let sealed = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: value.expose().as_bytes(),
                    aad: name.as_bytes(),
                },
            )
            .map_err(|_| err("the secret could not be sealed"))?;
        sqlx::query(
            "INSERT INTO gateway_secrets (name, nonce, sealed) VALUES ($1, $2, $3)
             ON CONFLICT (name) DO UPDATE SET nonce = EXCLUDED.nonce, sealed = EXCLUDED.sealed, updated_at = now()",
        )
        .bind(name)
        .bind(&nonce_bytes[..])
        .bind(sealed)
        .execute(self.db.pool())
        .await
        .map_err(|e| err(e.to_string()))?;
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<(), SeamError> {
        sqlx::query("DELETE FROM gateway_secrets WHERE name = $1")
            .bind(name)
            .execute(self.db.pool())
            .await
            .map_err(|e| err(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keks_are_64_hex_characters() {
        assert_eq!(parse_kek(&"ab".repeat(32)).unwrap(), [0xab; 32]);
        assert!(parse_kek("short").unwrap_err().contains("64 hex"));
        assert!(parse_kek(&"zz".repeat(32)).is_err());
    }

    #[test]
    fn a_seal_opens_only_with_its_key_and_name() {
        let a = XChaCha20Poly1305::new_from_slice(&[1u8; 32]).unwrap();
        let b = XChaCha20Poly1305::new_from_slice(&[2u8; 32]).unwrap();
        let n = PgSecretStore::nonce();
        let nonce = XNonce::try_from(&n[..]).unwrap();
        let sealed = a
            .encrypt(
                &nonce,
                Payload {
                    msg: b"ca key",
                    aad: b"gateway_ca",
                },
            )
            .unwrap();
        assert_eq!(
            a.decrypt(
                &nonce,
                Payload {
                    msg: &sealed,
                    aad: b"gateway_ca"
                }
            )
            .unwrap(),
            b"ca key"
        );
        assert!(b
            .decrypt(
                &nonce,
                Payload {
                    msg: &sealed,
                    aad: b"gateway_ca"
                }
            )
            .is_err());
        assert!(a
            .decrypt(
                &nonce,
                Payload {
                    msg: &sealed,
                    aad: b"other"
                }
            )
            .is_err());
        assert_ne!(PgSecretStore::nonce(), n);
    }
}
