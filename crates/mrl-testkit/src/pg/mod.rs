//! A fresh PostgreSQL database per test, cloned from a template that has every migration.
//!
//! The server is `DATABASE_URL` (the `just dev` stack by default). Its user needs `CREATEDB`.
//! ```text
//! let t = TestDb::new().await;               // migrated, empty tables
//! t.seed("INSERT INTO …").await;              // fixture SQL
//! let ids = t.write_events(&[event("job", id, 1)]).await;
//! assert_eq!(t.count("outbox_events").await, 1);
//! ```
//! The database is dropped when the `TestDb` is.

#[cfg(test)]
mod dispatcher_tests;
#[cfg(test)]
mod tests;

use mrl_server::config::DatabaseConfig;
use mrl_server::db::{self, Db};
use mrl_server::modules::events::{NewEvent, OutboxWriter};
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgConnectOptions, PgPool};
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use std::fmt::Display;
use std::time::Duration;
use tokio::sync::OnceCell;
use uuid::Uuid;

/// The dev stack's database (`deploy/local/compose.yaml`), used when `DATABASE_URL` is unset.
pub const DEV_DATABASE_URL: &str = "postgres://mrl:mrl-dev-only@127.0.0.1:55432/masterrl";

/// Pool settings for test databases: small, so many tests fit in one server.
pub fn test_config() -> DatabaseConfig {
    DatabaseConfig {
        max_connections: 5,
        acquire_timeout_seconds: 30,
        ..DatabaseConfig::default()
    }
}

/// Options for the server in `DATABASE_URL` (or [`DEV_DATABASE_URL`]).
pub fn server_options() -> PgConnectOptions {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEV_DATABASE_URL.to_owned());
    ok(url.parse(), "DATABASE_URL is not a PostgreSQL URL")
}

/// One test's database.
pub struct TestDb {
    db: Db,
    name: String,
    options: PgConnectOptions,
}

impl TestDb {
    /// A new database with every migration of this build applied.
    pub async fn new() -> TestDb {
        let template = ok(template().await, "could not prepare the template database");
        Self::create(Some(template)).await
    }

    /// A new database with no tables at all.
    pub async fn empty() -> TestDb {
        Self::create(None).await
    }

    async fn create(template: Option<&str>) -> TestDb {
        let name = format!("mrl_test_{}", Uuid::new_v4().simple());
        let sql = match template {
            Some(t) => format!("CREATE DATABASE {name} TEMPLATE {t}"),
            None => format!("CREATE DATABASE {name}"),
        };
        let mut admin = ok(
            PgConnection::connect_with(&server_options()).await,
            "could not reach the test PostgreSQL (run `just dev` or set DATABASE_URL)",
        );
        // Cloning fails while another session is still leaving the template; try again.
        for attempt in 1.. {
            match sqlx::raw_sql(AssertSqlSafe(sql.clone()))
                .execute(&mut admin)
                .await
            {
                Ok(_) => break,
                Err(e) if attempt < 50 && sqlstate(&e).as_deref() == Some("55006") => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("could not create {name}: {e}"),
            }
        }
        let _ = admin.close().await;
        let options = server_options().database(&name);
        TestDb {
            db: Db::connect_lazy(&test_config(), options.clone()),
            name,
            options,
        }
    }

    /// The database handle (no backups, `auto_migrate` on).
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// The pool.
    pub fn pool(&self) -> &PgPool {
        self.db.pool()
    }

    /// The database name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Another pool on the same database, like a second server replica.
    pub fn replica(&self) -> Db {
        Db::connect_lazy(&test_config(), self.options.clone())
    }

    /// Another pool on the same database with these pool settings.
    pub fn replica_with(&self, config: &DatabaseConfig) -> Db {
        Db::connect_lazy(config, self.options.clone())
    }

    /// Runs fixture SQL (several statements are fine).
    pub async fn seed(&self, sql: &str) {
        ok(
            sqlx::raw_sql(AssertSqlSafe(sql.to_owned()))
                .execute(self.pool())
                .await,
            "fixture SQL failed",
        );
    }

    /// Rows in `table` (a trusted, test-chosen name).
    pub async fn count(&self, table: &str) -> i64 {
        ok(
            sqlx::query_scalar(AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                .fetch_one(self.pool())
                .await,
            "count failed",
        )
    }

    /// Writes `events` in one transaction and returns their ids.
    pub async fn write_events(&self, events: &[NewEvent]) -> Vec<Uuid> {
        let mut tx = ok(self.db.begin().await, "begin failed");
        let mut ids = Vec::with_capacity(events.len());
        for e in events {
            ids.push(ok(
                OutboxWriter::write(&mut tx, e).await,
                "writing an event failed",
            ));
        }
        ok(tx.commit().await, "commit failed");
        ids
    }
}

/// An event fixture: type `<kind>.changed` with payload `{"seq": seq}`.
pub fn event(kind: &str, aggregate_id: Uuid, seq: i64) -> NewEvent {
    ok(
        NewEvent::new(
            format!("{kind}.changed"),
            kind,
            aggregate_id,
            seq,
            serde_json::json!({ "seq": seq }),
        ),
        "event fixture",
    )
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let sql = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        // Drop runs outside async; use a short-lived runtime on its own thread.
        let dropped = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            rt.block_on(async move {
                let mut admin = PgConnection::connect_with(&server_options())
                    .await
                    .map_err(|e| e.to_string())?;
                sqlx::raw_sql(AssertSqlSafe(sql))
                    .execute(&mut admin)
                    .await
                    .map_err(|e| e.to_string())?;
                let _ = admin.close().await;
                Ok::<(), String>(())
            })
        })
        .join();
        if let Ok(Err(e)) = dropped {
            eprintln!("testkit: could not drop {}: {e}", self.name);
        }
    }
}

/// The template's name: a hash of every migration, so new migrations make a new template.
fn template_name() -> String {
    let mut h = Sha256::new();
    for m in db::migrator().iter() {
        h.update(m.version.to_be_bytes());
        h.update(&m.checksum);
    }
    let hex: String = h.finalize()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("mrl_tpl_{hex}")
}

/// Makes the template once per server; an advisory lock keeps parallel test processes apart.
async fn template() -> Result<&'static str, String> {
    static NAME: OnceCell<String> = OnceCell::const_new();
    NAME.get_or_try_init(|| async {
        let name = template_name();
        let err = |e: sqlx::Error| e.to_string();
        let mut admin = PgConnection::connect_with(&server_options())
            .await
            .map_err(err)?;
        sqlx::query("SELECT pg_advisory_lock(7011001)")
            .execute(&mut admin)
            .await
            .map_err(err)?;
        let exists: Option<i32> =
            sqlx::query_scalar("SELECT 1 FROM pg_database WHERE datname = $1")
                .bind(&name)
                .fetch_optional(&mut admin)
                .await
                .map_err(err)?;
        if exists.is_none() {
            // Build under a temporary name and rename, so a crash never leaves a half template.
            let building = format!("{name}_build");
            for sql in [
                format!("DROP DATABASE IF EXISTS {building} WITH (FORCE)"),
                format!("CREATE DATABASE {building}"),
            ] {
                sqlx::raw_sql(AssertSqlSafe(sql))
                    .execute(&mut admin)
                    .await
                    .map_err(err)?;
            }
            let db = Db::connect_lazy(&test_config(), server_options().database(&building));
            db::migrate::apply(&db).await.map_err(|e| e.to_string())?;
            db.pool().close().await;
            sqlx::raw_sql(AssertSqlSafe(format!(
                "ALTER DATABASE {building} RENAME TO {name}"
            )))
            .execute(&mut admin)
            .await
            .map_err(err)?;
        }
        sqlx::query("SELECT pg_advisory_unlock(7011001)")
            .execute(&mut admin)
            .await
            .map_err(err)?;
        let _ = admin.close().await;
        Ok(name)
    })
    .await
    .map(String::as_str)
}

fn sqlstate(e: &sqlx::Error) -> Option<String> {
    match e {
        sqlx::Error::Database(d) => d.code().map(|c| c.into_owned()),
        _ => None,
    }
}

/// Unwraps with a readable panic; test helpers fail the test on setup errors.
fn ok<T, E: Display>(r: Result<T, E>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("testkit: {what}: {e}"),
    }
}
