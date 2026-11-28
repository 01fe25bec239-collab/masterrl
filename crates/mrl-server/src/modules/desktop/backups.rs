//! `GET|POST /desktop/backups` and `POST /desktop/backups/{name}/restore`, over P11's
//! [`Backups`] of the bundled database.
//!
//! A restore never loses the current data: the chosen file is copied aside first (making the
//! safety backup may rotate the oldest file away), then a `before-restore` backup is made, then
//! the copy is restored. The app restarts the core afterwards, so migrations run on the
//! restored data. Backups are named only by files in the backup folder, so a name can't point
//! anywhere else.

use super::{api, codes, Desktop};
use crate::app::AppState;
use crate::modules::jobs::JobsError;
use crate::pgbundle::Backups;
use crate::seams::RequestContext;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use time::OffsetDateTime;

/// One backup file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopBackup {
    /// File name, like `masterrl-20261127T093000123Z-daily.dump`.
    pub name: String,
    /// Why it was made: `daily`, `manual`, `before-update`, `before-restore`, a migration…
    pub reason: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// When it was made.
    #[serde(with = "time::serde::rfc3339::option")]
    #[schema(value_type = Option<String>, format = DateTime)]
    pub created_at: Option<OffsetDateTime>,
}

/// The backups of the bundled database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopBackupList {
    /// False when the core uses another database (nothing to back up here).
    pub available: bool,
    /// The folder they are kept in.
    pub folder: Option<String>,
    /// Newest first.
    pub items: Vec<DesktopBackup>,
}

/// A finished restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct DesktopRestored {
    /// The backup now in use.
    pub restored: String,
    /// The backup of the data from just before, to undo the restore.
    pub safety_backup: String,
    /// Always true: restart the core so it updates the restored data.
    pub restart_required: bool,
}

fn file_name(p: &std::path::Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// `masterrl-<stamp>-<reason>.dump` → (stamp, reason).
fn parts(name: &str) -> Option<(&str, &str)> {
    let middle = name.strip_prefix("masterrl-")?.strip_suffix(".dump")?;
    middle.split_once('-')
}

/// `20261115T093000123Z` → that time.
fn parse_stamp(s: &str) -> Option<OffsetDateTime> {
    let b = s.as_bytes();
    if b.len() != 19 || b[8] != b'T' || b[18] != b'Z' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<u32>().ok();
    let month = time::Month::try_from(u8::try_from(n(4..6)?).ok()?).ok()?;
    let date = time::Date::from_calendar_date(i32::try_from(n(0..4)?).ok()?, month, n(6..8)? as u8)
        .ok()?;
    let t = time::Time::from_hms_milli(
        n(9..11)? as u8,
        n(11..13)? as u8,
        n(13..15)? as u8,
        n(15..18)? as u16,
    )
    .ok()?;
    Some(date.with_time(t).assume_utc())
}

/// Describes one backup file.
pub fn describe(path: &std::path::Path) -> DesktopBackup {
    let name = file_name(path);
    let (stamp, reason) = parts(&name).unwrap_or(("", ""));
    DesktopBackup {
        reason: reason.to_owned(),
        created_at: parse_stamp(stamp),
        size_bytes: std::fs::metadata(path).map(|m| m.len()).unwrap_or(0),
        name,
    }
}

fn backups(desktop: &Desktop) -> Result<&Arc<Backups>, JobsError> {
    desktop.db.backups().ok_or_else(|| {
        JobsError::domain(
            codes::NO_BUNDLED_DATABASE,
            StatusCode::CONFLICT,
            "This core uses a database it doesn't run itself, so it can't back it up here.",
        )
    })
}

fn failed(what: &str, e: impl std::fmt::Display) -> JobsError {
    JobsError::domain(
        codes::BACKUP_FAILED,
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("{what}: {e}"),
    )
}

/// Holds the one-at-a-time lock, or answers MRL-CONFLICT-300.
pub fn exclusive(desktop: &Desktop) -> Result<tokio::sync::MutexGuard<'_, ()>, JobsError> {
    desktop.busy.try_lock().map_err(|_| {
        JobsError::domain(
            codes::BUSY,
            StatusCode::CONFLICT,
            "A backup, restore or update check is already running. Try again when it ends.",
        )
    })
}

#[utoipa::path(get, path = "/desktop/backups", tag = "desktop", operation_id = "desktop_list_backups",
    responses((status = 200, body = DesktopBackupList),
        (status = 409, description = "A team core (MRL-STATE-300)")))]
/// The bundled database's backups, newest first.
pub async fn list_backups(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
) -> Result<Json<DesktopBackupList>, JobsError> {
    api::guard(&state, &ctx).await?;
    let Some(b) = desktop.db.backups() else {
        return Ok(Json(DesktopBackupList {
            available: false,
            folder: None,
            items: Vec::new(),
        }));
    };
    let files = b
        .list()
        .map_err(|e| failed("The backups could not be listed", e))?;
    Ok(Json(DesktopBackupList {
        available: true,
        folder: Some(b.dir().to_string_lossy().into_owned()),
        items: files.iter().map(|f| describe(f)).collect(),
    }))
}

#[utoipa::path(post, path = "/desktop/backups", tag = "desktop", operation_id = "desktop_create_backup",
    responses((status = 201, body = DesktopBackup),
        (status = 409, description = "No bundled database (MRL-ENV-300), busy (MRL-CONFLICT-300) or a team core (MRL-STATE-300)"),
        (status = 500, description = "The backup failed (MRL-INTERNAL-300)")))]
/// Backs the database up now.
pub async fn create_backup(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
) -> Result<(StatusCode, Json<DesktopBackup>), JobsError> {
    api::guard(&state, &ctx).await?;
    let b = backups(&desktop)?;
    let _one = exclusive(&desktop)?;
    let file = b
        .create("manual")
        .await
        .map_err(|e| failed("The backup failed", e))?;
    let made = describe(&file);
    let mut tx = desktop.db.begin().await?;
    api::audit(&mut tx, &state, &ctx, "desktop.backup", &made.name).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok((StatusCode::CREATED, Json(made)))
}

/// Restores backup `name` of `b`, keeping a `before-restore` backup of the current data.
/// Returns that safety backup's file name.
pub async fn restore_named(b: &Backups, name: &str) -> Result<String, JobsError> {
    let chosen = b
        .list()
        .map_err(|e| failed("The backups could not be listed", e))?
        .into_iter()
        .find(|f| file_name(f) == name)
        .ok_or_else(|| {
            JobsError::domain(
                codes::NO_SUCH_BACKUP,
                StatusCode::NOT_FOUND,
                "There is no backup with this name. Refresh the list and pick one again.",
            )
        })?;
    // A name the backup listing never shows, so a crash here leaves nothing that looks like one.
    let copy: PathBuf = b
        .dir()
        .join(format!(".restoring-{}.tmp", uuid::Uuid::new_v4().simple()));
    std::fs::copy(&chosen, &copy).map_err(|e| failed("The backup could not be read", e))?;
    let result = async {
        let safety = b
            .create("before-restore")
            .await
            .map_err(|e| failed("The current data could not be backed up first", e))?;
        b.restore(&copy)
            .await
            .map_err(|e| failed("The restore failed", e))?;
        Ok(file_name(&safety))
    }
    .await;
    if let Err(e) = std::fs::remove_file(&copy) {
        tracing::warn!(error = %e, "could not remove the restore copy");
    }
    result
}

#[utoipa::path(post, path = "/desktop/backups/{name}/restore", tag = "desktop", operation_id = "desktop_restore_backup",
    params(("name" = String, Path, description = "A backup's file name from the list")),
    responses((status = 200, body = DesktopRestored),
        (status = 404, description = "No such backup (MRL-RES-300)"),
        (status = 409, description = "No bundled database (MRL-ENV-300), busy (MRL-CONFLICT-300) or a team core (MRL-STATE-300)"),
        (status = 500, description = "The restore failed (MRL-INTERNAL-300)")))]
/// Replaces the app's data with a backup (after backing up the current data). Restart the core
/// afterwards.
pub async fn restore_backup(
    State(state): State<AppState>,
    Extension(desktop): Extension<Arc<Desktop>>,
    ctx: RequestContext,
    Path(name): Path<String>,
) -> Result<Json<DesktopRestored>, JobsError> {
    api::guard(&state, &ctx).await?;
    let b = backups(&desktop)?;
    let _one = exclusive(&desktop)?;
    let safety_backup = restore_named(b, &name).await?;
    // The restored database gets the record (the one it replaced is in the safety backup).
    let mut tx = desktop.db.begin().await?;
    api::audit(&mut tx, &state, &ctx, "desktop.restore", &name).await?;
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(Json(DesktopRestored {
        restored: name,
        safety_backup,
        restart_required: true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_tell_the_time_and_reason() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let file = dir
            .path()
            .join("masterrl-20261127T093000123Z-before-update.dump");
        std::fs::write(&file, b"x").unwrap_or_else(|e| panic!("{e}"));
        let d = describe(&file);
        assert_eq!(d.reason, "before-update");
        assert_eq!(d.size_bytes, 1);
        let at = d.created_at.unwrap_or_else(|| panic!("no time"));
        assert_eq!(
            (
                at.year(),
                u8::from(at.month()),
                at.day(),
                at.hour(),
                at.millisecond()
            ),
            (2026, 11, 27, 9, 123)
        );
        assert_eq!(parse_stamp("2026-11-27"), None);
        assert_eq!(parts("other.dump"), None);
    }

    #[tokio::test]
    #[ignore = "needs PostgreSQL programs"]
    async fn restoring_the_oldest_backup_keeps_it_through_the_safety_backup() {
        use crate::pgbundle::{BundleOptions, FileSecretStore, PgBinaries, PgBundle};
        let configured = std::env::var_os("MRL_TEST_PG_BIN").map(PathBuf::from);
        let bins = match PgBinaries::locate(configured.as_deref()).await {
            Ok(b) => b,
            Err(e) if std::env::var_os("CI").is_some() => panic!("CI needs PostgreSQL: {e}"),
            Err(e) => {
                eprintln!("skipped: {e}");
                return;
            }
        };
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let bundle = PgBundle::start(BundleOptions {
            data_dir: dir.path().join("data"),
            binaries: bins,
            previous: None,
            backups_keep: 2,
            secrets: Arc::new(FileSecretStore::new(dir.path().join("secrets"))),
        })
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        let b = bundle.backups();
        // The supervisor makes its first daily backup right after start; wait for it.
        let started = std::time::Instant::now();
        while b.list().map(|l| l.is_empty()).unwrap_or(true) {
            assert!(started.elapsed().as_secs() < 30, "no daily backup");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let db = crate::db::Db::connect_lazy(
            &crate::config::DatabaseConfig::default(),
            bundle.connect_options(),
        );
        let count = |db: crate::db::Db| async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notes")
                .fetch_one(db.pool())
                .await
                .unwrap_or_else(|e| panic!("{e}"))
        };
        sqlx::raw_sql("CREATE TABLE notes (v text); INSERT INTO notes VALUES ('a'), ('b');")
            .execute(db.pool())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let chosen = b.create("manual").await.unwrap_or_else(|e| panic!("{e}"));
        sqlx::raw_sql("DELETE FROM notes")
            .execute(db.pool())
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        b.create("manual").await.unwrap_or_else(|e| panic!("{e}"));
        let list = b.list().unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            list.last(),
            Some(&chosen),
            "the chosen backup is now the oldest"
        );
        // keep = 2: the safety backup rotates the chosen file out; the copy is restored anyway.
        let safety = restore_named(&b, &file_name(&chosen))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(safety.contains("before-restore"));
        assert_eq!(
            count(db.clone()).await,
            2,
            "the chosen backup's data is back"
        );
        let names: Vec<String> = b
            .list()
            .unwrap_or_else(|e| panic!("{e}"))
            .iter()
            .map(|f| file_name(f))
            .collect();
        assert!(names.contains(&safety), "{names:?}");
        assert!(
            std::fs::read_dir(b.dir())
                .unwrap_or_else(|e| panic!("{e}"))
                .filter_map(Result::ok)
                .all(|e| !e.file_name().to_string_lossy().starts_with(".restoring-")),
            "the restore copy is removed"
        );
        let missing = restore_named(&b, "masterrl-19990101T000000000Z-x.dump").await;
        assert!(missing.is_err_and(|e| e.code() == "MRL-RES-300"));
        db.pool().close().await;
        bundle.stop().await.unwrap_or_else(|e| panic!("{e}"));
    }
}
