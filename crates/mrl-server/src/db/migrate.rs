//! Forward-only migrations in `crates/mrl-server/migrations/{PP}{NNN}_{name}.sql`
//! (part number, then a 3-digit sequence: P11's are `11001…`).
//!
//! Migrations are additive, so an older server keeps working on a newer database:
//! versions this build does not know are ignored.

use super::{Db, DbError};
use crate::seams::{ReadinessCheck, SeamError, StartupStep};
use async_trait::async_trait;
use sqlx::migrate::{Migration, Migrator};
use std::collections::HashSet;

/// Every migration this build ships, ignoring newer ones already applied.
pub fn migrator() -> Migrator {
    let mut m = sqlx::migrate!("./migrations");
    m.set_ignore_missing(true);
    m
}

/// The migrations of `m` not yet applied to `db`, in order.
pub async fn pending(db: &Db, m: &Migrator) -> Result<Vec<Migration>, DbError> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(db.pool())
        .await?;
    let rows: Vec<(i64, bool)> = if exists {
        sqlx::query_as("SELECT version, success FROM _sqlx_migrations")
            .fetch_all(db.pool())
            .await?
    } else {
        Vec::new()
    };
    if let Some((version, _)) = rows.iter().find(|(_, ok)| !ok) {
        return Err(DbError::MigrationFailed {
            detail: format!("database update {version} was left half-done by an earlier run"),
            restored: None,
        });
    }
    let applied: HashSet<i64> = rows.into_iter().map(|(v, _)| v).collect();
    Ok(m.iter()
        .filter(|x| !x.migration_type.is_down_migration() && !applied.contains(&x.version))
        .cloned()
        .collect())
}

/// Applies every pending migration of this build. See [`apply_with`].
pub async fn apply(db: &Db) -> Result<Vec<i64>, DbError> {
    apply_with(db, &migrator()).await
}

/// Applies the pending migrations of `m` and returns their versions.
///
/// When the server runs its own PostgreSQL, it backs up first and restores that backup if a
/// migration fails. Migrations run on their own connection without a statement timeout.
pub async fn apply_with(db: &Db, m: &Migrator) -> Result<Vec<i64>, DbError> {
    let todo: Vec<i64> = pending(db, m).await?.iter().map(|x| x.version).collect();
    if todo.is_empty() {
        return Ok(todo);
    }
    let backup = match db.backups() {
        Some(b) => Some(b.create("before-update").await?),
        None => None,
    };
    let run = async {
        let mut conn = db.dedicated().await?;
        sqlx::query("SET statement_timeout = 0")
            .execute(&mut conn)
            .await?;
        // `run(&mut conn)` makes this future non-`Send` (sqlx's `Acquire` lifetime issue);
        // `run_direct` is the same code without it.
        m.run_direct(None, &mut conn, false)
            .await
            .map_err(|e| DbError::MigrationFailed {
                detail: format!("database update failed: {e}"),
                restored: None,
            })
    };
    match run.await {
        Ok(()) => {
            tracing::info!(versions = ?todo, "database updated");
            Ok(todo)
        }
        Err(e) => {
            let detail = e.to_string();
            let restored = match (db.backups(), backup) {
                (Some(b), Some(path)) => match b.restore(&path).await {
                    Ok(()) => Some(path),
                    Err(re) => {
                        return Err(DbError::MigrationFailed {
                            detail: format!("{detail}; putting back the backup also failed: {re}"),
                            restored: None,
                        })
                    }
                },
                _ => None,
            };
            Err(DbError::MigrationFailed { detail, restored })
        }
    }
}

/// Startup step: applies pending migrations, or refuses to start when
/// `database.auto_migrate` is false and any are pending.
pub struct Migrations(pub Db);

#[async_trait]
impl StartupStep for Migrations {
    fn name(&self) -> &str {
        "migrations"
    }

    async fn run(&self) -> Result<(), SeamError> {
        if self.0.auto_migrate() {
            apply(&self.0).await?;
            return Ok(());
        }
        let n = pending(&self.0, &migrator()).await?.len();
        if n > 0 {
            return Err(DbError::MigrationsPending { pending: n }.into());
        }
        Ok(())
    }
}

/// Readiness: the database answers and every migration of this build is applied.
pub struct DbReady(pub Db);

#[async_trait]
impl ReadinessCheck for DbReady {
    fn name(&self) -> &str {
        "database"
    }

    async fn check(&self) -> Result<(), SeamError> {
        let n = pending(&self.0, &migrator()).await?.len();
        if n > 0 {
            return Err(SeamError::new(
                "database",
                format!("{n} database update(s) are not applied yet"),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_follow_the_numbering() {
        let m = migrator();
        let versions: Vec<i64> = m.iter().map(|x| x.version).collect();
        assert!(!versions.is_empty());
        let mut sorted = versions.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted, versions, "unique and in order");
        for x in m.iter() {
            // {PP}{NNN}: part 01–99, sequence 001–999.
            assert!((1001..=99_999).contains(&x.version), "{}", x.version);
            assert_ne!(x.version % 1000, 0, "{}", x.version);
            assert!(!x.migration_type.is_down_migration(), "forward-only");
        }
        assert!(versions.contains(&11001));
    }
}
