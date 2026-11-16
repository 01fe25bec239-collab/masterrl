//! Leader election for singleton loops (D-04-08): a session advisory lock on a dedicated
//! connection. The lock gives liveness; callers still guard writes with constraints.
//!
//! The lease: the leader pings its lock connection every `lease / 3` and steps down when a
//! ping fails. The server ends that session after `lease` without traffic
//! (`idle_session_timeout`), so a frozen leader loses the lock within the lease, and
//! followers retry every `lease / 3`.

use super::{Db, DbError};
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection};
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Runs work in exactly one process at a time per name.
#[derive(Clone)]
pub struct LeaderElection {
    db: Db,
    name: String,
    key: i64,
    lease: Duration,
}

impl LeaderElection {
    /// An election for `name` (like `events-dispatcher`) with this lease (at least 3 s).
    pub fn new(db: Db, name: impl Into<String>, lease: Duration) -> Self {
        let name = name.into();
        Self {
            key: lock_key(&name),
            db,
            name,
            lease: lease.max(Duration::from_secs(3)),
        }
    }

    /// The advisory-lock key for this name.
    pub fn key(&self) -> i64 {
        self.key
    }

    /// Runs `work` whenever this process leads, until `cancel` fires.
    ///
    /// `work` gets a token that is cancelled when leadership is lost; it must return soon
    /// after. When `work` returns (or fails), this process steps down and campaigns again.
    /// Database outages are logged and retried, never returned.
    pub async fn run<F, Fut>(&self, cancel: CancellationToken, mut work: F)
    where
        F: FnMut(CancellationToken) -> Fut,
        Fut: Future<Output = Result<(), DbError>>,
    {
        let retry = self.lease / 3;
        while !cancel.is_cancelled() {
            match self.campaign().await {
                Ok(Some(mut conn)) => {
                    tracing::info!(election = %self.name, "took leadership");
                    let lead = cancel.child_token();
                    let job = work(lead.clone());
                    tokio::pin!(job);
                    tokio::select! {
                        result = &mut job => {
                            lead.cancel();
                            if let Err(e) = result {
                                tracing::error!(election = %self.name, code = e.code(), error = %e, "leader work failed; stepping down");
                            }
                        }
                        () = self.hold(&mut conn, &lead) => {
                            lead.cancel();
                            if !cancel.is_cancelled() {
                                tracing::warn!(election = %self.name, "lost leadership");
                            }
                            if let Err(e) = job.await {
                                tracing::error!(election = %self.name, code = e.code(), error = %e, "leader work failed");
                            }
                        }
                    }
                    // Closing the session releases the lock; a failed close drops it anyway.
                    if let Err(e) = conn.close().await {
                        tracing::debug!(election = %self.name, error = %e, "lock connection closed with an error");
                    }
                    if !cancel.is_cancelled() {
                        // Give another replica a fair chance before campaigning again.
                        sleep_or_cancel(retry, &cancel).await;
                    }
                }
                Ok(None) => sleep_or_cancel(retry, &cancel).await,
                Err(e) => {
                    tracing::warn!(election = %self.name, code = e.code(), error = %e, "could not campaign for leadership");
                    sleep_or_cancel(retry, &cancel).await;
                }
            }
        }
    }

    /// Tries the lock once on a fresh connection. `None`: another process leads.
    async fn campaign(&self) -> Result<Option<PgConnection>, DbError> {
        let mut conn = self.db.dedicated().await?;
        let won: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(self.key)
            .fetch_one(&mut conn)
            .await?;
        if !won {
            conn.close().await?;
            return Ok(None);
        }
        let ms = self.lease.as_millis().to_string();
        sqlx::query("SELECT set_config('idle_session_timeout', $1, false)")
            .bind(ms)
            .execute(&mut conn)
            .await?;
        Ok(Some(conn))
    }

    /// Returns when the lock connection stops answering, or when `lead` is cancelled.
    async fn hold(&self, conn: &mut PgConnection, lead: &CancellationToken) {
        let every = self.lease / 3;
        loop {
            tokio::select! {
                () = lead.cancelled() => return,
                () = tokio::time::sleep(every) => {}
            }
            let ping = tokio::time::timeout(every, sqlx::query("SELECT 1").execute(&mut *conn));
            match ping.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!(election = %self.name, error = %e, "lock connection failed");
                    return;
                }
                Err(_) => {
                    tracing::warn!(election = %self.name, "lock connection did not answer in time");
                    return;
                }
            }
        }
    }
}

/// First 8 bytes of SHA-256 of the name, so names map to stable lock keys.
fn lock_key(name: &str) -> i64 {
    let digest = Sha256::digest(format!("mrl-leader:{name}").as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) {
    tokio::select! {
        () = cancel.cancelled() => {}
        () = tokio::time::sleep(d) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_stable_and_distinct() {
        assert_eq!(lock_key("events-dispatcher"), lock_key("events-dispatcher"));
        assert_ne!(lock_key("events-dispatcher"), lock_key("scheduler"));
    }
}
