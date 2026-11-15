//! `IdempotencyStore` on Postgres, shared by every replica (Doc 09 §3.10, §6.1).
//!
//! Same key + same body → replay; different body → `Mismatch` (422); still running →
//! `InProgress` (409). Keys live 24 hours. A key left `IN_PROGRESS` by a crashed server is
//! taken over after 10 minutes, so the client can retry.

use super::{Db, DbError};
use crate::seams::{IdempotencyBegin, IdempotencyStore, SeamError, StoredResponse};
use async_trait::async_trait;

/// Keys expire after this long (matches the in-memory store).
const TTL: &str = "24 hours";
/// An `IN_PROGRESS` key older than this belongs to a request that will never finish.
const STALE: &str = "10 minutes";

/// The Postgres-backed store. Plug it in with `Seams::builder().idempotency(..)`.
#[derive(Clone)]
pub struct PgIdempotencyStore {
    db: Db,
}

impl PgIdempotencyStore {
    /// A store on this database.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait]
impl IdempotencyStore for PgIdempotencyStore {
    async fn begin(
        &self,
        scope: &str,
        key: &str,
        fingerprint: [u8; 32],
    ) -> Result<IdempotencyBegin, SeamError> {
        // A row can vanish between the two statements (release, sweep); then claim again.
        for _ in 0..3 {
            let claimed = sqlx::query(
                "INSERT INTO idempotency_keys (scope, key, request_sha256, state, expires_at)
                 VALUES ($1, $2, $3, 'IN_PROGRESS', now() + $4::interval)
                 ON CONFLICT (scope, key) DO UPDATE SET
                   request_sha256 = EXCLUDED.request_sha256, state = 'IN_PROGRESS',
                   response_status = NULL, response_headers = NULL, response_body = NULL,
                   created_at = now(), expires_at = EXCLUDED.expires_at
                 WHERE idempotency_keys.expires_at <= now()
                    OR (idempotency_keys.state = 'IN_PROGRESS'
                        AND idempotency_keys.created_at < now() - $5::interval)",
            )
            .bind(scope)
            .bind(key)
            .bind(&fingerprint[..])
            .bind(TTL)
            .bind(STALE)
            .execute(self.db.pool())
            .await
            .map_err(DbError::from)?;
            if claimed.rows_affected() == 1 {
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
            let Some(row) = row else { continue };
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
        sqlx::query(
            "UPDATE idempotency_keys
             SET state = 'DONE', response_status = $3, response_headers = $4, response_body = $5
             WHERE scope = $1 AND key = $2",
        )
        .bind(scope)
        .bind(key)
        .bind(i32::from(response.status))
        .bind(sqlx::types::Json(&response.headers))
        .bind(&response.body)
        .execute(self.db.pool())
        .await
        .map_err(DbError::from)?;
        Ok(())
    }

    async fn release(&self, scope: &str, key: &str) -> Result<(), SeamError> {
        sqlx::query(
            "DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2 AND state = 'IN_PROGRESS'",
        )
        .bind(scope)
        .bind(key)
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

/// Deletes expired keys, 10 000 per statement. Returns how many went.
pub async fn sweep_idempotency_keys(db: &Db) -> Result<u64, DbError> {
    let mut total = 0;
    loop {
        let n = sqlx::query(
            "DELETE FROM idempotency_keys WHERE ctid IN (
               SELECT ctid FROM idempotency_keys WHERE expires_at <= now() LIMIT 10000)",
        )
        .execute(db.pool())
        .await?
        .rows_affected();
        total += n;
        if n < 10_000 {
            return Ok(total);
        }
    }
}
