//! Moving a data folder to a new PostgreSQL major version with `pg_upgrade`.
//!
//! The app ships the new binaries plus the previous major's (`pgbundle.previous_bin_dir`).
//! On the first start with an older `pgdata`, the manager:
//! 1. runs `initdb` for the new version into `pgdata.upgrading` (same superuser and password),
//! 2. runs `pg_upgrade` (copy mode: the old folder is never changed),
//! 3. renames `pgdata` to `pgdata-<old>.old` and `pgdata.upgrading` to `pgdata`.
//!
//! If a step fails, `pgdata` is untouched and the new folder is removed. The kept
//! `pgdata-<old>.old` lets support roll back by hand; delete it once the new version works.

use super::cluster::{initdb, run, tool};
use super::{BundleError, PgBinaries, SUPERUSER};
use crate::seams::Secret;
use std::path::Path;

/// Upgrades `data_dir/pgdata` from `old` to `new`.
pub async fn upgrade(
    old: &PgBinaries,
    new: &PgBinaries,
    data_dir: &Path,
    password: &Secret,
) -> Result<(), BundleError> {
    let pgdata = data_dir.join("pgdata");
    let next = data_dir.join("pgdata.upgrading");
    let kept = data_dir.join(format!("pgdata-{}.old", old.major()));
    let fail = |m: String| BundleError::Upgrade(m);
    if next.exists() {
        std::fs::remove_dir_all(&next)
            .map_err(|e| fail(format!("could not remove an unfinished upgrade: {e}")))?;
    }
    tracing::info!(
        from = old.major(),
        to = new.major(),
        "upgrading the database"
    );
    initdb(new, &next, password)
        .await
        .map_err(|e| fail(format!("preparing the new version: {e}")))?;
    let work = data_dir.join("upgrade-work");
    super::create_private_dir(&work)
        .map_err(|e| fail(format!("could not create {}: {e}", work.display())))?;
    let mut cmd = tool(new.tool("pg_upgrade"));
    cmd.current_dir(&work)
        .arg("--old-bindir")
        .arg(old.dir())
        .arg("--new-bindir")
        .arg(new.dir())
        .arg("--old-datadir")
        .arg(&pgdata)
        .arg("--new-datadir")
        .arg(&next)
        .arg("--username")
        .arg(SUPERUSER)
        .env("PGPASSWORD", password.expose());
    let (old_port, new_port) = (super::free_port()?, super::free_port()?);
    cmd.arg("--old-port")
        .arg(old_port.to_string())
        .arg("--new-port")
        .arg(new_port.to_string());
    #[cfg(unix)]
    {
        let sock = super::socket_dir(data_dir)?;
        cmd.arg("--socketdir").arg(&sock);
        let result = run(cmd, "pg_upgrade").await;
        let _ = std::fs::remove_dir_all(&sock);
        finish(result, &pgdata, &next, &kept, &work)
    }
    #[cfg(not(unix))]
    {
        let result = run(cmd, "pg_upgrade").await;
        finish(result, &pgdata, &next, &kept, &work)
    }
}

fn finish(
    result: Result<(), String>,
    pgdata: &Path,
    next: &Path,
    kept: &Path,
    work: &Path,
) -> Result<(), BundleError> {
    if let Err(e) = result {
        let _ = std::fs::remove_dir_all(next);
        return Err(BundleError::Upgrade(format!(
            "{e}; the old data folder was not changed (pg_upgrade logs are in {})",
            work.display()
        )));
    }
    if kept.exists() {
        std::fs::remove_dir_all(kept).map_err(|e| {
            BundleError::Upgrade(format!("could not remove {}: {e}", kept.display()))
        })?;
    }
    std::fs::rename(pgdata, kept)
        .map_err(|e| BundleError::Upgrade(format!("could not keep the old data folder: {e}")))?;
    if let Err(e) = std::fs::rename(next, pgdata) {
        // Put the old folder back so the next start finds the data where it was.
        let back = std::fs::rename(kept, pgdata);
        return Err(BundleError::Upgrade(format!(
            "could not put the new data folder in place: {e}{}",
            back.err()
                .map(|b| format!(
                    "; moving the old one back also failed ({b}), it is in {}",
                    kept.display()
                ))
                .unwrap_or_default()
        )));
    }
    let _ = std::fs::remove_dir_all(work);
    tracing::info!(kept = %kept.display(), "database upgrade done");
    Ok(())
}
