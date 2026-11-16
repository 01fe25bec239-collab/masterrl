//! Encryption for setup sync and the keychain fallback: XChaCha20-Poly1305 with a random
//! 24-byte nonce, and 32-byte keys from Argon2id.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};

/// Key length in bytes.
pub const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;
const SALT_LEN: usize = 16;

/// Something went wrong while encrypting or decrypting. Never carries key material.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CryptoError {
    /// The operating system gave no random bytes.
    #[error("this computer did not provide random numbers")]
    Random,
    /// The key-derivation settings could not be used.
    #[error("the key settings could not be used, because {0}")]
    Kdf(String),
    /// The data is damaged or the key is wrong.
    #[error("the data could not be decrypted with this key")]
    Open,
}

/// `N` random bytes from the operating system.
pub fn random<const N: usize>() -> Result<[u8; N], CryptoError> {
    let mut buf = [0u8; N];
    getrandom::fill(&mut buf).map_err(|_| CryptoError::Random)?;
    Ok(buf)
}

/// Encrypts `plaintext`, bound to `aad`. Output: nonce followed by ciphertext and tag.
pub fn seal(key: &[u8; KEY_LEN], aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let nonce: [u8; NONCE_LEN] = random()?;
    let cipher = XChaCha20Poly1305::new(&(*key).into());
    let ct = cipher
        .encrypt(
            &XNonce::from(nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| CryptoError::Open)?;
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Decrypts what [`seal`] made with the same key and `aad`.
pub fn open(key: &[u8; KEY_LEN], aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let (nonce, ct) = sealed
        .split_first_chunk::<NONCE_LEN>()
        .ok_or(CryptoError::Open)?;
    XChaCha20Poly1305::new(&(*key).into())
        .decrypt(&XNonce::from(*nonce), Payload { msg: ct, aad })
        .map_err(|_| CryptoError::Open)
}

/// Argon2id settings and salt, stored next to the encrypted data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KdfParams {
    /// Always `argon2id`.
    pub alg: String,
    /// Memory in KiB.
    pub m_kib: u32,
    /// Passes.
    pub t: u32,
    /// Lanes.
    pub p: u32,
    /// Salt, base64.
    pub salt: String,
}

impl KdfParams {
    /// New settings with a fresh salt: 64 MiB, 3 passes, 1 lane (OWASP's Argon2id baseline).
    pub fn generate() -> Result<Self, CryptoError> {
        Ok(Self {
            alg: "argon2id".to_owned(),
            m_kib: 64 * 1024,
            t: 3,
            p: 1,
            salt: STANDARD.encode(random::<SALT_LEN>()?),
        })
    }

    /// The key for `passphrase`. Takes about a second; call it off the async threads.
    pub fn derive(&self, passphrase: &str) -> Result<[u8; KEY_LEN], CryptoError> {
        if self.alg != "argon2id" {
            return Err(CryptoError::Kdf(format!("{:?} is not argon2id", self.alg)));
        }
        // Refuse settings so weak or so large that a damaged copy could hurt.
        if !(8 * 1024..=1024 * 1024).contains(&self.m_kib)
            || !(1..=10).contains(&self.t)
            || !(1..=8).contains(&self.p)
        {
            return Err(CryptoError::Kdf(
                "its cost settings are out of range".into(),
            ));
        }
        let salt = STANDARD
            .decode(&self.salt)
            .map_err(|_| CryptoError::Kdf("the salt is not base64".into()))?;
        let params = argon2::Params::new(self.m_kib, self.t, self.p, Some(KEY_LEN))
            .map_err(|e| CryptoError::Kdf(e.to_string()))?;
        let mut key = [0u8; KEY_LEN];
        argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params)
            .hash_password_into(passphrase.as_bytes(), &salt, &mut key)
            .map_err(|e| CryptoError::Kdf(e.to_string()))?;
        Ok(key)
    }
}

/// Lower-case hex.
pub fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A 32-byte key from hex; spaces and dashes are ignored, any case.
pub fn key_from_hex(text: &str) -> Option<[u8; KEY_LEN]> {
    let hex: Vec<u8> = text.bytes().filter(|b| !matches!(b, b'-' | b' ')).collect();
    if hex.len() != KEY_LEN * 2 || !hex.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let mut key = [0u8; KEY_LEN];
    for (k, pair) in key.iter_mut().zip(hex.chunks(2)) {
        *k = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(key)
}

/// The recovery-key form of a key: 64 hex digits in 16 groups of 4.
pub fn recovery_key(key: &[u8; KEY_LEN]) -> String {
    to_hex(key)
        .as_bytes()
        .chunks(4)
        .map(|c| String::from_utf8_lossy(c).into_owned())
        .collect::<Vec<_>>()
        .join("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast() -> KdfParams {
        KdfParams {
            m_kib: 8 * 1024,
            t: 1,
            ..KdfParams::generate().unwrap()
        }
    }

    #[test]
    fn seal_and_open_round_trip() {
        let key = random::<KEY_LEN>().unwrap();
        let sealed = seal(&key, b"aad", b"hello").unwrap();
        assert_eq!(open(&key, b"aad", &sealed).unwrap(), b"hello");
        assert_ne!(
            seal(&key, b"aad", b"hello").unwrap(),
            sealed,
            "nonce is fresh"
        );
        assert_eq!(open(&key, b"other", &sealed), Err(CryptoError::Open));
        let mut damaged = sealed.clone();
        damaged[30] ^= 1;
        assert_eq!(open(&key, b"aad", &damaged), Err(CryptoError::Open));
        assert_eq!(open(&key, b"aad", &sealed[..10]), Err(CryptoError::Open));
    }

    #[test]
    fn passphrase_round_trip_and_wrong_passphrase_fails() {
        let kdf = fast();
        let key = kdf.derive("correct horse battery").unwrap();
        assert_eq!(kdf.derive("correct horse battery").unwrap(), key);
        let sealed = seal(&key, b"u1", br#"{"machines":[]}"#).unwrap();
        let wrong = kdf.derive("correct horse battery!").unwrap();
        assert_eq!(open(&wrong, b"u1", &sealed), Err(CryptoError::Open));
        let other_salt = KdfParams {
            salt: fast().salt,
            ..kdf.clone()
        };
        assert_ne!(other_salt.derive("correct horse battery").unwrap(), key);
    }

    #[test]
    fn kdf_refuses_unusable_settings() {
        for bad in [
            KdfParams {
                alg: "scrypt".into(),
                ..fast()
            },
            KdfParams { m_kib: 1, ..fast() },
            KdfParams { t: 0, ..fast() },
            KdfParams {
                salt: "!!".into(),
                ..fast()
            },
        ] {
            assert!(
                matches!(bad.derive("x"), Err(CryptoError::Kdf(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn recovery_key_round_trips() {
        let key = random::<KEY_LEN>().unwrap();
        let text = recovery_key(&key);
        assert_eq!(text.len(), 64 + 15);
        assert_eq!(key_from_hex(&text), Some(key));
        assert_eq!(
            key_from_hex(&text.to_uppercase().replace('-', " ")),
            Some(key)
        );
        assert_eq!(key_from_hex(&text[..70]), None);
        assert_eq!(
            key_from_hex(&text.replace('a', "g")).is_some(),
            !text.contains('a')
        );
    }
}
