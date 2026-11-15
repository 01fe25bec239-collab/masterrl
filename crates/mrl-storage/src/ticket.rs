//! Transfer tickets: short-lived, HMAC-SHA256 signed permission to GET or PUT one key.
//!
//! Token format: `v1.<payload>.<signature>`, where `payload` is unpadded base64url of the
//! claims JSON and `signature` is unpadded base64url of HMAC-SHA256(key, "v1.<payload>").
//! The server-proxied endpoint is `/api/v1/transfer/{token}`.

use std::fmt;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{KeyInit, Mac};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Result, StorageError};
use crate::keys::{org_of, OrgScope};

type HmacSha256 = hmac::Hmac<sha2::Sha256>;

/// Longest lifetime a ticket can have.
pub const MAX_TICKET_TTL: Duration = Duration::from_secs(15 * 60);
/// Shortest ticket signing key, in bytes.
pub const MIN_KEY_LEN: usize = 32;
/// Path of the server-proxied transfer endpoint; the token is appended.
pub const PROXY_PATH: &str = "/api/v1/transfer/";
const VERSION: &str = "v1";

/// The secret that signs tickets. Comes from server config; at least 32 bytes.
#[derive(Clone)]
pub struct TicketKey(Vec<u8>);

impl TicketKey {
    /// Wraps raw key bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.len() < MIN_KEY_LEN {
            return Err(StorageError::Config(format!(
                "the ticket key must be at least {MIN_KEY_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self(bytes))
    }

    fn mac(&self) -> Result<HmacSha256> {
        HmacSha256::new_from_slice(&self.0)
            .map_err(|_| StorageError::Config("the ticket key was rejected".into()))
    }
}

impl fmt::Debug for TicketKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TicketKey(..)")
    }
}

/// The one method a ticket allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TicketMethod {
    /// Download.
    Get,
    /// Upload.
    Put,
}

/// What a ticket allows. Times are Unix seconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TicketClaims {
    /// Full object key, `orgs/{org}/...`.
    pub key: String,
    /// Allowed method.
    pub method: TicketMethod,
    /// Largest allowed body for PUT, or object size limit for GET, in bytes.
    pub max_size: u64,
    /// When the ticket was issued.
    pub issued_at: i64,
    /// When the ticket stops working.
    pub expires_at: i64,
}

impl TicketClaims {
    /// The org the key belongs to.
    pub fn org_id(&self) -> &str {
        org_of(&self.key).unwrap_or_default()
    }

    /// Fails if `size` bytes is more than the ticket allows.
    pub fn check_size(&self, size: u64) -> Result<()> {
        if size > self.max_size {
            return Err(StorageError::TicketScope(format!(
                "{size} bytes is more than the {} bytes this ticket allows",
                self.max_size
            )));
        }
        Ok(())
    }
}

/// A ticket handed to a client: either a presigned URL (object stores that sign) or a
/// path on the server-proxied endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransferTicket {
    /// Allowed method.
    pub method: TicketMethod,
    /// Full object key.
    pub key: String,
    /// Size limit in bytes. Presigned PUT URLs cannot enforce it; the server checks the
    /// object size before it accepts the upload.
    pub max_size: u64,
    /// When the ticket stops working.
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    /// Presigned URL, or `/api/v1/transfer/{token}` for the proxied endpoint.
    pub url: String,
    /// True when `url` is a presigned object store URL.
    pub presigned: bool,
}

/// Checks a requested lifetime and returns the expiry.
pub(crate) fn expiry(now: OffsetDateTime, ttl: Duration) -> Result<OffsetDateTime> {
    if ttl.is_zero() || ttl > MAX_TICKET_TTL {
        return Err(StorageError::TicketScope(format!(
            "a ticket must last between 1 second and 15 minutes, not {} seconds",
            ttl.as_secs()
        )));
    }
    Ok(now + ttl)
}

/// Signs a ticket token for the proxied endpoint.
pub fn sign_ticket(key: &TicketKey, claims: &TicketClaims) -> Result<String> {
    let json = serde_json::to_vec(claims)
        .map_err(|e| StorageError::Config(format!("ticket claims: {e}")))?;
    let signed = format!("{VERSION}.{}", B64.encode(json));
    let mut mac = key.mac()?;
    mac.update(signed.as_bytes());
    Ok(format!(
        "{signed}.{}",
        B64.encode(mac.finalize().into_bytes())
    ))
}

/// Verifies a token for the proxied endpoint and returns its claims.
///
/// Checks the signature (constant time), then the method, the lifetime and the key shape.
pub fn verify_ticket(
    key: &TicketKey,
    token: &str,
    method: TicketMethod,
    now: OffsetDateTime,
) -> Result<TicketClaims> {
    let (signed, sig) = token
        .rsplit_once('.')
        .ok_or(StorageError::TicketInvalid("it is not a ticket"))?;
    let payload = signed
        .strip_prefix(VERSION)
        .and_then(|p| p.strip_prefix('.'))
        .ok_or(StorageError::TicketInvalid("it has an unknown version"))?;
    let sig = B64
        .decode(sig)
        .map_err(|_| StorageError::TicketInvalid("the signature is not base64url"))?;
    let mut mac = key.mac()?;
    mac.update(signed.as_bytes());
    mac.verify_slice(&sig)
        .map_err(|_| StorageError::TicketInvalid("the signature does not match"))?;

    let json = B64
        .decode(payload)
        .map_err(|_| StorageError::TicketInvalid("the payload is not base64url"))?;
    let claims: TicketClaims = serde_json::from_slice(&json)
        .map_err(|_| StorageError::TicketInvalid("the payload is not valid claims"))?;
    if claims.method != method {
        return Err(StorageError::TicketScope(format!(
            "it allows {:?}, not {method:?}",
            claims.method
        )));
    }
    let lifetime = claims.expires_at.saturating_sub(claims.issued_at);
    if lifetime <= 0 || lifetime > MAX_TICKET_TTL.as_secs() as i64 {
        return Err(StorageError::TicketInvalid(
            "its lifetime is outside 1 second to 15 minutes",
        ));
    }
    if now.unix_timestamp() >= claims.expires_at {
        return Err(StorageError::TicketExpired);
    }
    let org =
        org_of(&claims.key).ok_or(StorageError::TicketInvalid("its key is not under orgs/"))?;
    OrgScope::new(org)?.check(&claims.key)?;
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> TicketKey {
        TicketKey::new([7u8; 32]).unwrap()
    }

    fn claims(now: OffsetDateTime) -> TicketClaims {
        TicketClaims {
            key: "orgs/o1/runs/r/config/resolved.yaml".into(),
            method: TicketMethod::Put,
            max_size: 100,
            issued_at: now.unix_timestamp(),
            expires_at: now.unix_timestamp() + 600,
        }
    }

    #[test]
    fn round_trip() {
        let now = OffsetDateTime::now_utc();
        let token = sign_ticket(&key(), &claims(now)).unwrap();
        let got = verify_ticket(&key(), &token, TicketMethod::Put, now).unwrap();
        assert_eq!(got, claims(now));
        assert_eq!(got.org_id(), "o1");
        assert!(got.check_size(100).is_ok());
        assert_eq!(got.check_size(101).unwrap_err().code(), "MRL-STORE-009");
    }

    #[test]
    fn tamper_is_rejected() {
        let now = OffsetDateTime::now_utc();
        let token = sign_ticket(&key(), &claims(now)).unwrap();
        let (signed, sig) = token.rsplit_once('.').unwrap();

        // Changed payload: bigger size, other org.
        for forged in [
            TicketClaims {
                max_size: u64::MAX,
                ..claims(now)
            },
            TicketClaims {
                key: "orgs/o2/runs/r/config/resolved.yaml".into(),
                ..claims(now)
            },
        ] {
            let payload = B64.encode(serde_json::to_vec(&forged).unwrap());
            let bad = format!("v1.{payload}.{sig}");
            assert_eq!(
                verify_ticket(&key(), &bad, TicketMethod::Put, now)
                    .unwrap_err()
                    .code(),
                "MRL-STORE-007"
            );
        }
        // Flipped signature byte, wrong key, truncated, unknown version.
        let mut raw = B64.decode(sig).unwrap();
        raw[0] ^= 1;
        let flipped = format!("{signed}.{}", B64.encode(raw));
        let other = TicketKey::new([8u8; 32]).unwrap();
        for (k, t) in [
            (key(), flipped.as_str()),
            (other, token.as_str()),
            (key(), signed),
            (key(), &token[1..]),
            (key(), ""),
        ] {
            assert_eq!(
                verify_ticket(&k, t, TicketMethod::Put, now)
                    .unwrap_err()
                    .code(),
                "MRL-STORE-007",
                "{t}"
            );
        }
        // Right signature, wrong method.
        assert_eq!(
            verify_ticket(&key(), &token, TicketMethod::Get, now)
                .unwrap_err()
                .code(),
            "MRL-STORE-009"
        );
    }

    #[test]
    fn expiry_is_enforced() {
        let now = OffsetDateTime::now_utc();
        let token = sign_ticket(&key(), &claims(now)).unwrap();
        let later = now + Duration::from_secs(600);
        assert!(matches!(
            verify_ticket(&key(), &token, TicketMethod::Put, later),
            Err(StorageError::TicketExpired)
        ));
        assert!(verify_ticket(
            &key(),
            &token,
            TicketMethod::Put,
            later - Duration::from_secs(1)
        )
        .is_ok());

        // A correctly signed ticket that claims too long a life is still refused.
        let long = TicketClaims {
            expires_at: now.unix_timestamp() + 16 * 60,
            ..claims(now)
        };
        let token = sign_ticket(&key(), &long).unwrap();
        assert_eq!(
            verify_ticket(&key(), &token, TicketMethod::Put, now)
                .unwrap_err()
                .code(),
            "MRL-STORE-007"
        );

        assert!(expiry(now, Duration::from_secs(16 * 60)).is_err());
        assert!(expiry(now, Duration::ZERO).is_err());
        assert!(expiry(now, MAX_TICKET_TTL).is_ok());
    }

    #[test]
    fn short_keys_are_refused() {
        assert_eq!(
            TicketKey::new([1u8; 31]).unwrap_err().code(),
            "MRL-STORE-006"
        );
    }

    #[test]
    fn escaping_key_is_refused_even_if_signed() {
        let now = OffsetDateTime::now_utc();
        let bad = TicketClaims {
            key: "orgs/o1/../o2/x".into(),
            ..claims(now)
        };
        let token = sign_ticket(&key(), &bad).unwrap();
        assert_eq!(
            verify_ticket(&key(), &token, TicketMethod::Put, now)
                .unwrap_err()
                .code(),
            "MRL-STORE-001"
        );
    }
}
