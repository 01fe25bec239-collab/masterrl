//! `IdempotencyStore` on Postgres, shared by every replica (Doc 09 §3.10, §6.1).
//!
//! Same key + same body → replay; different body → `Mismatch` (422); still running →
//! `InProgress` (409). Finished keys live 24 hours.
//!
//! A running key is never taken over because of its age. Each server process holds an
//! advisory lock `(70110, owner)` on its own connection for as long as it lives, and writes
//! `owner` on the keys it starts. Only a key whose owner's lock is gone (the process died or
//! lost its database session) can be claimed again.
//!
//! Every claim gets a new `claim` token, kept by this store until the request ends.
//! `complete` and `release` change a key only while it still carries that token, so a request
//! that outlived its claim cannot finish or delete the next one. A store never reclaims a key
//! it still has a running request for, so one store holds at most one claim per key.

use super::{Db, DbError};
use crate::seams::{IdempotencyBegin, IdempotencyStore, SeamError, StoredResponse};
use async_trait::async_trait;
use sqlx::{Connection, PgConnection};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Finished keys expire after this long (matches the in-memory store).
const TTL: &str = "24 hours";
/// Advisory-lock namespace of server-process owner locks.
const OWNER_LOCKS: i32 = 70110;

/// `true` while the process that owns row `k` is alive (holds its owner lock).
macro_rules! owner_alive {
    ($k:literal) => {
        concat!(
            "EXISTS (SELECT 1 FROM pg_locks l WHERE l.locktype = 'advisory' AND l.granted
               AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND l.classid = 70110::oid AND l.objsubid = 2 AND l.objid = ",
            $k,
            ".owner::oid)"
        )
    };
}

/// `(scope, key)` → claim token of a running request.
type Claims = HashMap<(String, String), Uuid>;

/// The Postgres-backed store. Plug it in with `Seams::builder().idempotency(..)`.
#[derive(Clone)]
pub struct PgIdempotencyStore {
    db: Db,
    presence: Arc<Mutex<Option<(i32, PgConnection)>>>,
    /// The claim token of every key this store has a running request for.
    claims: Arc<std::sync::Mutex<Claims>>,
}

impl PgIdempotencyStore {
    /// A store on this database. Its owner lock is taken on first use.
    pub fn new(db: Db) -> Self {
        Self {
            db,
            presence: Arc::default(),
            claims: Arc::default(),
        }
    }

    fn claims(&self) -> Result<std::sync::MutexGuard<'_, Claims>, SeamError> {
        self.claims
            .lock()
            .map_err(|_| SeamError::new("idempotency", "claim table lock poisoned"))
    }

    fn claim_of(&self, scope: &str, key: &str) -> Result<Option<Uuid>, SeamError> {
        Ok(self
            .claims()?
            .get(&(scope.to_owned(), key.to_owned()))
            .copied())
    }

    /// This process's owner id, (re)taking its lock when the lock connection is gone.
    async fn owner(&self) -> Result<i32, DbError> {
        let mut presence = self.presence.lock().await;
        if let Some((id, conn)) = presence.as_mut() {
            if conn.ping().await.is_ok() {
                return Ok(*id);
            }
            tracing::warn!("idempotency owner lock connection was lost; taking it again");
        }
        let previous = presence.take().map(|(id, _)| id);
        let mut conn = self.db.dedicated().await?;
        // Keep the old id when it is free, so keys this process started stay its own.
        let candidates = previous
            .into_iter()
            .chain(std::iter::repeat_with(random_owner));
        for id in candidates.take(20) {
            let won: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
                .bind(OWNER_LOCKS)
                .bind(id)
                .fetch_one(&mut conn)
                .await?;
            if won {
                *presence = Some((id, conn));
                return Ok(id);
            }
        }
        Err(DbError::Contended { attempts: 20 })
    }
}

/// A random non-negative owner id.
fn random_owner() -> i32 {
    let b = uuid::Uuid::new_v4().into_bytes();
    i32::from_be_bytes([b[0] & 0x7f, b[1], b[2], b[3]])
}

#[async_trait]
impl IdempotencyStore for PgIdempotencyStore {
    async fn begin(
        &self,
        scope: &str,
        key: &str,
        fingerprint: [u8; 32],
    ) -> Result<IdempotencyBegin, SeamError> {
        let owner = self.owner().await?;
        // A request of ours still runs with this key (even if its row was swept): never
        // claim it twice, so the token below stays unique per key in this store.
        let ours = self.claim_of(scope, key)?.is_some();
        // A row can vanish between the two statements (release, sweep); then claim again.
        for _ in 0..3 {
            let token = Uuid::new_v4();
            let claimed = if ours {
                0
            } else {
                sqlx::query(concat!(
                "INSERT INTO idempotency_keys (scope, key, request_sha256, state, owner, claim, expires_at)
                 VALUES ($1, $2, $3, 'IN_PROGRESS', $4, $5, now() + $6::interval)
                 ON CONFLICT (scope, key) DO UPDATE SET
                   request_sha256 = EXCLUDED.request_sha256, state = 'IN_PROGRESS',
                   owner = EXCLUDED.owner, claim = EXCLUDED.claim,
                   response_status = NULL, response_headers = NULL,
                   response_body = NULL, created_at = now(), expires_at = EXCLUDED.expires_at
                 WHERE (idempotency_keys.state = 'DONE' AND idempotency_keys.expires_at <= now())
                    OR (idempotency_keys.state = 'IN_PROGRESS' AND NOT ",
                owner_alive!("idempotency_keys"),
                ")"
            ))
            .bind(scope)
            .bind(key)
            .bind(&fingerprint[..])
            .bind(owner)
            .bind(token)
            .bind(TTL)
            .execute(self.db.pool())
            .await
            .map_err(DbError::from)?
            .rows_affected()
            };
            if claimed == 1 {
                self.claims()?
                    .insert((scope.to_owned(), key.to_owned()), token);
                return Ok(IdempotencyBegin::Started);
            }
            let row: Option<Row> = sqlx::query_as(
                "SELECT request_sha256, state, response_status, response_headers, response_body
                 FROM idempotency_keys WHERE scope = $1 AND key = $2",
            )
            .bind(scope)
            .bind(key)
            .fetch_optional(self.db.pool())
            .await
            .map_err(DbError::from)?;
            let Some(row) = row else {
                if ours {
                    return Ok(IdempotencyBegin::InProgress);
                }
                continue;
            };
            if row.0 != fingerprint {
                return Ok(IdempotencyBegin::Mismatch);
            }
            return Ok(match (row.1.as_str(), row.2, row.4) {
                ("DONE", Some(status), Some(body)) => IdempotencyBegin::Replay(StoredResponse {
                    status: u16::try_from(status).unwrap_or(500),
                    headers: row.3.map(|h| h.0).unwrap_or_default(),
                    body,
                }),
                _ => IdempotencyBegin::InProgress,
            });
        }
        Err(SeamError::new(
            "idempotency",
            "the key kept changing while the server read it",
        ))
    }

    async fn complete(
        &self,
        scope: &str,
        key: &str,
        response: StoredResponse,
    ) -> Result<(), SeamError> {
        let Some(token) = self.claim_of(scope, key)? else {
            tracing::warn!("complete for an idempotency key this server has no claim on");
            return Ok(());
        };
        let done = sqlx::query(
            "UPDATE idempotency_keys
             SET state = 'DONE', owner = NULL, claim = NULL, response_status = $4,
                 response_headers = $5, response_body = $6
             WHERE scope = $1 AND key = $2 AND state = 'IN_PROGRESS' AND claim = $3",
        )
        .bind(scope)
        .bind(key)
        .bind(token)
        .bind(i32::from(response.status))
        .bind(sqlx::types::Json(&response.headers))
        .bind(&response.body)
        .execute(self.db.pool())
        .await
        .map_err(DbError::from)?;
        // Stored (or lost to a newer claim): this request's claim is over either way.
        self.claims()?.remove(&(scope.to_owned(), key.to_owned()));
        if done.rows_affected() == 0 {
            tracing::warn!(
                "an idempotency key was claimed again or swept; its response was not stored"
            );
        }
        Ok(())
    }

    async fn release(&self, scope: &str, key: &str) -> Result<(), SeamError> {
        let Some(token) = self.claims()?.remove(&(scope.to_owned(), key.to_owned())) else {
            return Ok(());
        };
        sqlx::query(
            "DELETE FROM idempotency_keys
             WHERE scope = $1 AND key = $2 AND state = 'IN_PROGRESS' AND claim = $3",
        )
        .bind(scope)
        .bind(key)
        .bind(token)
        .execute(self.db.pool())
        .await
        .map_err(DbError::from)?;
        Ok(())
    }
}

type Row = (
    Vec<u8>,
    String,
    Option<i32>,
    Option<sqlx::types::Json<Vec<(String, String)>>>,
    Option<Vec<u8>>,
);

/// Deletes expired finished keys and keys whose owner process is gone, 10 000 per
/// statement. Keys of running requests stay. Returns how many went.
pub async fn sweep_idempotency_keys(db: &Db) -> Result<u64, DbError> {
    let mut total = 0;
    loop {
        let n = sqlx::query(concat!(
            "DELETE FROM idempotency_keys WHERE ctid IN (
               SELECT ctid FROM idempotency_keys k
               WHERE k.expires_at <= now() AND (k.state = 'DONE' OR NOT ",
            owner_alive!("k"),
            ") LIMIT 10000)"
        ))
        .execute(db.pool())
        .await?
        .rows_affected();
        total += n;
        if n < 10_000 {
            return Ok(total);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_ids_are_non_negative() {
        for _ in 0..1000 {
            assert!(random_owner() >= 0);
        }
        assert_eq!(OWNER_LOCKS, 70110, "matches the SQL namespace");
    }
}
