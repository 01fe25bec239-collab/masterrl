//! PostgreSQL access: the pool, transactions with retry, the org setting row-level security
//! reads, migrations, leader election and the Postgres idempotency store.
//!
//! ```text
//! let db = Db::connect_lazy(&config.database, url.parse()?);
//! let id = db.transaction(|tx| Box::pin(async move {
//!     db::set_org(tx, org).await?;
//!     sqlx::query("UPDATE jobs SET …").execute(&mut **tx).await?;
//!     OutboxWriter::write(tx, &event).await
//! })).await?;
//! ```

mod error;
mod idempotency;
mod leader;
pub mod migrate;

pub use error::{codes, explain, DbError};
pub use idempotency::{sweep_idempotency_keys, PgIdempotencyStore};
pub use leader::LeaderElection;
pub use migrate::{migrator, DbReady, Migrations};

use crate::config::DatabaseConfig;
use crate::pgbundle::Backups;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{Connection, PgConnection};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// A transaction on the pool. Run statements with `.execute(&mut **tx)`.
pub type Tx = sqlx::Transaction<'static, sqlx::Postgres>;

/// What a transaction body returns; see [`Db::transaction`].
pub type TxFuture<'t, T> = Pin<Box<dyn Future<Output = Result<T, DbError>> + Send + 't>>;

/// Tries a transaction makes before giving up on serialization failures and deadlocks.
const TX_ATTEMPTS: u32 = 5;

/// How a transaction isolates itself from others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// PostgreSQL's default.
    ReadCommitted,
    /// Transactions behave as if they ran one after another.
    Serializable,
}

/// The database handle every module shares. Cheap to clone.
#[derive(Clone)]
pub struct Db {
    pool: PgPool,
    auto_migrate: bool,
    backups: Option<Arc<Backups>>,
}

impl Db {
    /// A pool that connects on first use, with the limits from `[database]`.
    /// Every pooled connection gets `statement_timeout`.
    pub fn connect_lazy(config: &DatabaseConfig, options: PgConnectOptions) -> Self {
        let timeout_ms = config
            .statement_timeout_seconds
            .saturating_mul(1000)
            .to_string();
        let options = options
            .application_name("mrl-server")
            .options([("statement_timeout", timeout_ms.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_seconds))
            .connect_lazy_with(options);
        Self {
            pool,
            auto_migrate: config.auto_migrate,
            backups: None,
        }
    }

    /// Backs up the database before every migration and restores it if one fails.
    pub fn with_backups(mut self, backups: Arc<Backups>) -> Self {
        self.backups = Some(backups);
        self
    }

    /// The pool, for statements outside a transaction.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Whether startup may apply pending migrations.
    pub fn auto_migrate(&self) -> bool {
        self.auto_migrate
    }

    /// The backups taken before migrations, when the server runs its own PostgreSQL.
    pub fn backups(&self) -> Option<&Arc<Backups>> {
        self.backups.as_ref()
    }

    /// A new connection outside the pool. Session state (advisory locks, `LISTEN`) dies
    /// with it, so it is the right place for leader locks.
    pub async fn dedicated(&self) -> Result<PgConnection, DbError> {
        Ok(PgConnection::connect_with(&self.pool.connect_options()).await?)
    }

    /// Checks the database answers.
    pub async fn ping(&self) -> Result<(), DbError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Starts a transaction. It rolls back when dropped without `commit`.
    pub async fn begin(&self) -> Result<Tx, DbError> {
        Ok(self.pool.begin().await?)
    }

    /// Runs `body` in a READ COMMITTED transaction and commits it. Retries the whole body
    /// on serialization failures and deadlocks, so `body` must be safe to run again.
    pub async fn transaction<T, F>(&self, body: F) -> Result<T, DbError>
    where
        F: for<'t> FnMut(&'t mut Tx) -> TxFuture<'t, T>,
    {
        self.transaction_with(Isolation::ReadCommitted, body).await
    }

    /// Like [`Db::transaction`] with a chosen isolation level.
    pub async fn transaction_with<T, F>(
        &self,
        isolation: Isolation,
        mut body: F,
    ) -> Result<T, DbError>
    where
        F: for<'t> FnMut(&'t mut Tx) -> TxFuture<'t, T>,
    {
        for attempt in 1..=TX_ATTEMPTS {
            let result = async {
                let mut tx = self.begin().await?;
                if isolation == Isolation::Serializable {
                    sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                        .execute(&mut *tx)
                        .await?;
                }
                let value = body(&mut tx).await?;
                tx.commit().await?;
                Ok(value)
            }
            .await;
            match result {
                Err(e) if is_retryable(&e) && attempt < TX_ATTEMPTS => {
                    // 10, 20, 40, 80 ms plus up to 10 ms of jitter, so retries spread out.
                    let jitter = u64::from(Uuid::new_v4().as_bytes()[0] % 10);
                    let wait = 10 * (1u64 << (attempt - 1)) + jitter;
                    tracing::debug!(attempt, sqlstate = ?e.sqlstate(), "retrying transaction");
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                }
                Err(e) if is_retryable(&e) => {
                    return Err(DbError::Contended {
                        attempts: TX_ATTEMPTS,
                    })
                }
                other => return other,
            }
        }
        Err(DbError::Contended {
            attempts: TX_ATTEMPTS,
        })
    }
}

/// `SET LOCAL mrl.org_id`: row-level security policies read it with
/// `current_setting('mrl.org_id', true)`. It ends with the transaction.
pub async fn set_org(tx: &mut Tx, org_id: Uuid) -> Result<(), DbError> {
    sqlx::query("SELECT set_config('mrl.org_id', $1, true)")
        .bind(org_id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Serialization failure (40001) or deadlock (40P01).
fn is_retryable(e: &DbError) -> bool {
    matches!(e.sqlstate().as_deref(), Some("40001" | "40P01"))
}
