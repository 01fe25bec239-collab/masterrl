//! `pg_dump` backups of the app database (Doc 22 §10): daily and before every migration,
//! newest `keep` kept, restorable in one step.

use super::cluster::{run, Target};
use super::{BundleError, PgBinaries, DATABASE};
use sqlx::{Connection, PgConnection};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;

/// Backups of one bundled database.
#[derive(Debug)]
pub struct Backups {
    bins: PgBinaries,
    target: Target,
    dir: PathBuf,
    keep: usize,
    busy: tokio::sync::Mutex<()>,
}

impl Backups {
    pub(crate) fn new(bins: PgBinaries, target: Target, dir: PathBuf, keep: usize) -> Self {
        Self {
            bins,
            target,
            dir,
            keep: keep.max(1),
            busy: tokio::sync::Mutex::new(()),
        }
    }

    /// The folder backups go to.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Dumps the database to `masterrl-<UTC time>-<reason>.dump`, then keeps only the
    /// newest `keep`. Returns the new file.
    pub async fn create(&self, reason: &str) -> Result<PathBuf, BundleError> {
        let _one_at_a_time = self.busy.lock().await;
        super::create_private_dir(&self.dir).map_err(|e| {
            BundleError::Backup(format!("could not create {}: {e}", self.dir.display()))
        })?;
        let reason: String = reason
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let path = self.dir.join(format!(
            "masterrl-{}-{reason}.dump",
            stamp(OffsetDateTime::now_utc())
        ));
        let partial = path.with_extension("partial");
        let mut cmd = self.target.client(self.bins.tool("pg_dump"));
        cmd.arg("-d")
            .arg(DATABASE)
            .args(["-Fc", "-f"])
            .arg(&partial);
        if let Err(e) = run(cmd, "pg_dump").await {
            let _ = std::fs::remove_file(&partial);
            return Err(BundleError::Backup(e));
        }
        std::fs::rename(&partial, &path)
            .map_err(|e| BundleError::Backup(format!("could not save {}: {e}", path.display())))?;
        for old in self.list()?.into_iter().skip(self.keep) {
            if let Err(e) = std::fs::remove_file(&old) {
                tracing::warn!(file = %old.display(), error = %e, "could not delete an old backup");
            }
        }
        tracing::info!(file = %path.display(), "database backed up");
        Ok(path)
    }

    /// Replaces the app database with `file`. Open connections to it are ended first.
    pub async fn restore(&self, file: &Path) -> Result<(), BundleError> {
        let _one_at_a_time = self.busy.lock().await;
        let fail =
            |e: sqlx::Error| BundleError::Backup(format!("could not reset the database: {e}"));
        let mut conn = PgConnection::connect_with(&self.target.options("postgres"))
            .await
            .map_err(fail)?;
        sqlx::raw_sql("DROP DATABASE IF EXISTS masterrl WITH (FORCE)")
            .execute(&mut conn)
            .await
            .map_err(fail)?;
        sqlx::raw_sql("CREATE DATABASE masterrl")
            .execute(&mut conn)
            .await
            .map_err(fail)?;
        let _ = conn.close().await;
        let mut cmd = self.target.client(self.bins.tool("pg_restore"));
        cmd.arg("-d")
            .arg(DATABASE)
            .args(["--no-owner", "--exit-on-error", "--single-transaction"])
            .arg(file);
        run(cmd, "pg_restore").await.map_err(BundleError::Backup)?;
        tracing::warn!(file = %file.display(), "database restored from backup");
        Ok(())
    }

    /// Backup files, newest first.
    pub fn list(&self) -> Result<Vec<PathBuf>, BundleError> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(BundleError::Backup(format!(
                    "could not list {}: {e}",
                    self.dir.display()
                )))
            }
        };
        let mut files: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("masterrl-") && n.ends_with(".dump"))
            })
            .collect();
        files.sort_by(|a, b| b.cmp(a));
        Ok(files)
    }

    /// True when the newest backup is older than `every`, or there is none.
    pub fn due(&self, every: Duration) -> bool {
        let newest = self.list().ok().and_then(|l| l.into_iter().next());
        let age = newest
            .and_then(|p| std::fs::metadata(p).ok()?.modified().ok())
            .and_then(|m| SystemTime::now().duration_since(m).ok());
        age.is_none_or(|a| a >= every)
    }
}

/// `20261115T093000123Z`: sorts by time as text.
fn stamp(t: OffsetDateTime) -> String {
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}{:03}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.millisecond()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_sort_by_time() {
        let a = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let b = a + time::Duration::milliseconds(5);
        assert_eq!(stamp(a), "20231114T221320000Z");
        assert!(stamp(a) < stamp(b));
    }
}
