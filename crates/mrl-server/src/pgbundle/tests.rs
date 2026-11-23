//! Integration tests with real PostgreSQL programs (`just test-integration`).
//!
//! Programs come from `MRL_TEST_PG_BIN`, else [`PgBinaries::locate`]. Outside CI the tests
//! skip with a message when none are found; in CI (`CI` set) they fail instead.
//! The upgrade test needs `MRL_TEST_PG_OLD_BIN` and `MRL_TEST_PG_NEW_BIN` (two majors);
//! in CI it fails without them.

use super::*;
use crate::config::DatabaseConfig;
use crate::db::{self, Db};
use sqlx::migrate::{Migration, MigrationType, Migrator};
use sqlx::SqlSafeStr;
use std::borrow::Cow;

async fn binaries() -> Option<PgBinaries> {
    let configured = std::env::var_os("MRL_TEST_PG_BIN").map(PathBuf::from);
    match PgBinaries::locate(configured.as_deref()).await {
        Ok(b) => Some(b),
        Err(e) if std::env::var_os("CI").is_some() => panic!("CI needs PostgreSQL programs: {e}"),
        Err(e) => {
            eprintln!("skipped: {e}");
            None
        }
    }
}

#[tokio::test]
async fn a_lock_released_a_moment_later_is_taken_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    let held = lock_data_dir(dir.path()).await.unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(held);
    });
    lock_data_dir(dir.path()).await.unwrap();
    release.await.unwrap();
}

#[tokio::test]
async fn a_lock_held_for_good_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let _held = lock_data_dir(dir.path()).await.unwrap();
    let Err(err) = lock_data_dir(dir.path()).await else {
        panic!("a held folder must be refused")
    };
    assert_eq!(err.code(), codes::DATA_DIR_LOCKED, "{err}");
}

fn options(dir: &Path, bins: PgBinaries, keep: usize) -> BundleOptions {
    BundleOptions {
        data_dir: dir.join("data"),
        binaries: bins,
        previous: None,
        backups_keep: keep,
        secrets: Arc::new(FileSecretStore::new(dir.join("secrets"))),
    }
}

fn db(b: &PgBundle) -> Db {
    Db::connect_lazy(&DatabaseConfig::default(), b.connect_options()).with_backups(b.backups())
}

async fn count(db: &Db, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap()
}

#[tokio::test]
#[ignore = "needs PostgreSQL programs"]
async fn init_start_stop_and_start_again_keeps_data() {
    let Some(bins) = binaries().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let b = PgBundle::start(options(dir.path(), bins.clone(), 7))
        .await
        .unwrap();
    let d = db(&b);
    sqlx::raw_sql("CREATE TABLE kept (v int); INSERT INTO kept VALUES (42);")
        .execute(d.pool())
        .await
        .unwrap();
    d.pool().close().await;
    b.stop().await.unwrap();
    let secret = FileSecretStore::new(dir.path().join("secrets"));
    assert!(secret.get(PASSWORD_SECRET).await.unwrap().is_some());

    let b = PgBundle::start(options(dir.path(), bins, 7)).await.unwrap();
    let d = db(&b);
    assert_eq!(count(&d, "SELECT v::bigint FROM kept").await, 42);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir.path().join("data"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }
    d.pool().close().await;
    b.stop().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL programs"]
async fn a_second_server_cannot_use_the_same_folder() {
    let Some(bins) = binaries().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let first = PgBundle::start(options(dir.path(), bins.clone(), 7))
        .await
        .unwrap();
    let Err(err) = PgBundle::start(options(dir.path(), bins.clone(), 7)).await else {
        panic!("second start must fail")
    };
    assert_eq!(err.code(), codes::DATA_DIR_LOCKED, "{err}");
    first.stop().await.unwrap();
    let again = PgBundle::start(options(dir.path(), bins, 7)).await.unwrap();
    again.stop().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL programs"]
async fn backups_restore_and_keep_the_newest() {
    let Some(bins) = binaries().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let b = PgBundle::start(options(dir.path(), bins, 2)).await.unwrap();
    let backups = b.backups();
    // The supervisor makes the first daily backup right after start.
    let started = Instant::now();
    while backups.list().unwrap().is_empty() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "no daily backup"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let d = db(&b);
    sqlx::raw_sql("CREATE TABLE notes (v text); INSERT INTO notes VALUES ('a'), ('b');")
        .execute(d.pool())
        .await
        .unwrap();
    let file = backups.create("test").await.unwrap();
    sqlx::raw_sql("DELETE FROM notes")
        .execute(d.pool())
        .await
        .unwrap();
    backups.restore(&file).await.unwrap();
    assert_eq!(count(&d, "SELECT count(*) FROM notes").await, 2);
    backups.create("test").await.unwrap();
    backups.create("test").await.unwrap();
    let list = backups.list().unwrap();
    assert_eq!(list.len(), 2, "{list:?}");
    assert!(!list.contains(&file), "oldest went first");
    d.pool().close().await;
    b.stop().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL programs"]
async fn a_failed_migration_restores_the_backup() {
    let Some(bins) = binaries().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let b = PgBundle::start(options(dir.path(), bins, 7)).await.unwrap();
    let d = db(&b);
    assert!(!db::migrate::apply(&d).await.unwrap().is_empty());
    sqlx::raw_sql("CREATE TABLE marker (v int); INSERT INTO marker VALUES (1);")
        .execute(d.pool())
        .await
        .unwrap();
    let extra = |v: i64, sql: &'static str| {
        Migration::new(
            v,
            Cow::Borrowed("test"),
            MigrationType::Simple,
            sql.into_sql_str(),
            false,
        )
    };
    let mut all: Vec<Migration> = db::migrator().iter().cloned().collect();
    all.push(extra(99_001, "CREATE TABLE half_done (v int)"));
    all.push(extra(99_002, "SELECT 1 / 0"));
    let bad = Migrator::with_migrations(all);
    let before = backups_len(&b);
    let err = db::migrate::apply_with(&d, &bad).await.unwrap_err();
    assert_eq!(err.code(), codes::MIGRATION_FAILED, "{err}");
    assert!(
        matches!(
            err,
            crate::db::DbError::MigrationFailed {
                restored: Some(_),
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(backups_len(&b), before + 1, "backed up first");
    assert_eq!(count(&d, "SELECT count(*) FROM marker").await, 1);
    let half: bool = sqlx::query_scalar("SELECT to_regclass('half_done') IS NOT NULL")
        .fetch_one(d.pool())
        .await
        .unwrap();
    assert!(!half, "the restore undid the first new migration");
    assert!(db::migrate::pending(&d, &db::migrator())
        .await
        .unwrap()
        .is_empty());
    d.pool().close().await;
    b.stop().await.unwrap();
}

fn backups_len(b: &PgBundle) -> usize {
    b.backups().list().unwrap().len()
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "needs PostgreSQL programs"]
async fn a_crashed_server_is_restarted() {
    let Some(bins) = binaries().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let b = PgBundle::start(options(dir.path(), bins, 7)).await.unwrap();
    let pid = b.server_pid().unwrap();
    let killed = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let d = db(&b);
    let started = Instant::now();
    loop {
        if b.server_pid().is_some_and(|p| p != pid) && d.ping().await.is_ok() {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(60), "not restarted");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(!b.failed().is_cancelled());
    d.pool().close().await;
    b.stop().await.unwrap();
}

#[tokio::test]
#[ignore = "needs PostgreSQL programs"]
async fn a_newer_data_folder_is_refused() {
    let Some(bins) = binaries().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let pgdata = dir.path().join("data").join("pgdata");
    std::fs::create_dir_all(&pgdata).unwrap();
    std::fs::write(pgdata.join("PG_VERSION"), "999\n").unwrap();
    FileSecretStore::new(dir.path().join("secrets"))
        .set(PASSWORD_SECRET, Secret::new("x"))
        .await
        .unwrap();
    let Err(err) = PgBundle::start(options(dir.path(), bins, 7)).await else {
        panic!("must refuse")
    };
    assert_eq!(err.code(), codes::DATA_DIR_NEWER, "{err}");
}

#[tokio::test]
#[ignore = "needs two PostgreSQL versions"]
async fn pg_upgrade_moves_the_data_to_the_new_major() {
    let (Some(old), Some(new)) = (
        std::env::var_os("MRL_TEST_PG_OLD_BIN"),
        std::env::var_os("MRL_TEST_PG_NEW_BIN"),
    ) else {
        let why = "set MRL_TEST_PG_OLD_BIN and MRL_TEST_PG_NEW_BIN to two PostgreSQL bin folders";
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must run the upgrade test: {why}"
        );
        eprintln!("skipped: {why}");
        return;
    };
    let old = PgBinaries::in_dir(Path::new(&old)).await.unwrap();
    let new = PgBinaries::in_dir(Path::new(&new)).await.unwrap();
    assert!(
        old.major() < new.major(),
        "old {} new {}",
        old.major(),
        new.major()
    );
    let dir = tempfile::tempdir().unwrap();

    let b = PgBundle::start(options(dir.path(), old.clone(), 7))
        .await
        .unwrap();
    let d = db(&b);
    db::migrate::apply(&d).await.unwrap();
    sqlx::raw_sql("CREATE TABLE survives (v int); INSERT INTO survives VALUES (7);")
        .execute(d.pool())
        .await
        .unwrap();
    d.pool().close().await;
    b.stop().await.unwrap();

    let Err(err) = PgBundle::start(options(dir.path(), new.clone(), 7)).await else {
        panic!("needs the previous binaries")
    };
    assert_eq!(err.code(), codes::UPGRADE_FAILED, "{err}");

    let mut o = options(dir.path(), new.clone(), 7);
    o.previous = Some(old.clone());
    let b = PgBundle::start(o).await.unwrap();
    let d = db(&b);
    assert_eq!(count(&d, "SELECT v::bigint FROM survives").await, 7);
    assert!(db::migrate::pending(&d, &db::migrator())
        .await
        .unwrap()
        .is_empty());
    let version = std::fs::read_to_string(dir.path().join("data/pgdata/PG_VERSION")).unwrap();
    assert_eq!(version.trim(), new.major().to_string());
    assert!(dir
        .path()
        .join(format!("data/pgdata-{}.old", old.major()))
        .is_dir());
    d.pool().close().await;
    b.stop().await.unwrap();
}
