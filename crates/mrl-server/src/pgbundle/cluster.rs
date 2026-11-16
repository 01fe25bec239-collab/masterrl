//! Running the PostgreSQL programs: `initdb`, the server process, `pg_ctl stop`.

use super::{BundleError, PgBinaries, DATABASE, SUPERUSER};
use crate::seams::Secret;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

/// How long a starting server may take to accept connections.
const START_TIMEOUT: Duration = Duration::from_secs(60);

/// Libpq settings from the environment that would point tools elsewhere.
const CLEARED_ENV: [&str; 9] = [
    "PGHOST",
    "PGHOSTADDR",
    "PGPORT",
    "PGUSER",
    "PGDATABASE",
    "PGPASSFILE",
    "PGSERVICE",
    "PGSERVICEFILE",
    "PGOPTIONS",
];

/// Where and how to reach the running server.
#[derive(Debug, Clone)]
pub struct Target {
    /// Unix socket folder (macOS, Linux) or `127.0.0.1` (Windows).
    pub host: String,
    /// Port; on Unix it only names the socket file.
    pub port: u16,
    /// The superuser password.
    pub password: Secret,
}

impl Target {
    /// sqlx options for `database`.
    pub fn options(&self, database: &str) -> PgConnectOptions {
        let o = PgConnectOptions::new()
            .port(self.port)
            .username(SUPERUSER)
            .password(self.password.expose())
            .database(database);
        if Path::new(&self.host).is_absolute() {
            o.socket(&self.host)
        } else {
            o.host(&self.host)
        }
    }

    /// A client tool (`pg_dump`, `pg_restore`) pointed at this server, password in its env.
    pub fn client(&self, program: PathBuf) -> Command {
        let mut cmd = tool(program);
        cmd.arg("-h")
            .arg(&self.host)
            .arg("-p")
            .arg(self.port.to_string())
            .arg("-U")
            .arg(SUPERUSER)
            .env("PGPASSWORD", self.password.expose());
        cmd
    }
}

/// A command with libpq variables cleared, the C locale and no stdin.
pub fn tool(program: PathBuf) -> Command {
    let mut cmd = Command::new(program);
    for v in CLEARED_ENV {
        cmd.env_remove(v);
    }
    // Without a valid locale, macOS's postmaster fails with "became multithreaded during
    // startup". The cluster is made with `--no-locale`, so C fits.
    cmd.env("LC_ALL", "C");
    cmd.stdin(Stdio::null()).kill_on_drop(true);
    cmd
}

/// Runs a tool to completion; the error carries the end of its output.
pub async fn run(mut cmd: Command, what: &str) -> Result<(), String> {
    let out = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| format!("{what} could not start: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    let mut text = String::from_utf8_lossy(&out.stderr).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stdout));
    Err(format!(
        "{what} failed ({}): {}",
        out.status,
        tail(&text, 15)
    ))
}

/// The last `n` non-empty lines.
pub fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    lines[lines.len().saturating_sub(n)..].join(" | ")
}

/// `initdb` with the superuser, its password, UTF-8 and no locale (portable across OSes).
pub async fn initdb(
    bins: &PgBinaries,
    pgdata: &Path,
    password: &Secret,
) -> Result<(), BundleError> {
    let parent = pgdata.parent().unwrap_or(Path::new("."));
    let pwfile = parent.join(".initdb-pw");
    super::write_private(&pwfile, password.expose().as_bytes())
        .map_err(|e| BundleError::Failed(format!("could not write a temporary file: {e}")))?;
    let mut cmd = tool(bins.tool("initdb"));
    cmd.arg("-D")
        .arg(pgdata)
        .arg("-U")
        .arg(SUPERUSER)
        .arg(pw_arg(&pwfile))
        .args(["--auth=scram-sha-256", "--encoding=UTF8", "--no-locale"]);
    let result = run(cmd, "initdb").await;
    let _ = std::fs::remove_file(&pwfile);
    result.map_err(BundleError::Failed)
}

fn pw_arg(pwfile: &Path) -> OsString {
    let mut a = OsString::from("--pwfile=");
    a.push(pwfile);
    a
}

/// Stops a server still running on `pgdata` (left by a process that crashed). The data
/// folder lock guarantees no live server of ours owns it.
pub async fn stop_leftover(bins: &PgBinaries, pgdata: &Path) {
    let mut status = tool(bins.tool("pg_ctl"));
    status.arg("status").arg("-D").arg(pgdata);
    if run(status, "pg_ctl status").await.is_ok() {
        tracing::warn!("stopping a database server left running by an earlier run");
        if let Err(e) = stop(bins, pgdata).await {
            tracing::warn!(error = %e, "could not stop the leftover database server");
        }
    }
}

/// `pg_ctl stop -m fast`: ends sessions, checkpoints, exits.
pub async fn stop(bins: &PgBinaries, pgdata: &Path) -> Result<(), BundleError> {
    let mut cmd = tool(bins.tool("pg_ctl"));
    cmd.arg("stop")
        .arg("-D")
        .arg(pgdata)
        .args(["-m", "fast", "-w", "-t", "60"]);
    run(cmd, "pg_ctl stop").await.map_err(BundleError::Failed)
}

/// Starts the server, waits until it accepts connections and makes sure the app database
/// exists. Its log goes to `log` (the previous one is kept as `<log>.1`).
pub async fn launch(
    bins: &PgBinaries,
    pgdata: &Path,
    target: &Target,
    log: &Path,
) -> Result<Child, BundleError> {
    let failed = |what: &str, e: std::io::Error| BundleError::Failed(format!("{what}: {e}"));
    if log.exists() {
        let mut old = log.as_os_str().to_owned();
        old.push(".1");
        std::fs::rename(log, PathBuf::from(old))
            .map_err(|e| failed("could not rotate the database log", e))?;
    }
    let file =
        std::fs::File::create(log).map_err(|e| failed("could not open the database log", e))?;
    let file2 = file
        .try_clone()
        .map_err(|e| failed("could not open the database log", e))?;
    let mut cmd = tool(bins.tool("postgres"));
    cmd.arg("-D")
        .arg(pgdata)
        .arg("-p")
        .arg(target.port.to_string());
    if Path::new(&target.host).is_absolute() {
        cmd.arg("-k")
            .arg(&target.host)
            .args(["-c", "listen_addresses="]);
    } else {
        cmd.args(["-c", &format!("listen_addresses={}", target.host)]);
    }
    // The supervisor stops it with `pg_ctl stop`; dropping the handle must not kill it.
    cmd.stdout(file).stderr(file2).kill_on_drop(false);
    let mut child = cmd
        .spawn()
        .map_err(|e| failed("could not start the database server", e))?;
    let started = Instant::now();
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            let text = std::fs::read_to_string(log).unwrap_or_default();
            return Err(BundleError::Failed(format!(
                "the database server stopped while starting ({status}): {}",
                tail(&text, 10)
            )));
        }
        if let Ok(mut conn) = PgConnection::connect_with(&target.options("postgres")).await {
            let exists: Option<i32> =
                sqlx::query_scalar("SELECT 1 FROM pg_database WHERE datname = $1")
                    .bind(DATABASE)
                    .fetch_optional(&mut conn)
                    .await
                    .map_err(|e| {
                        BundleError::Failed(format!("could not read the database list: {e}"))
                    })?;
            if exists.is_none() {
                sqlx::raw_sql(concat!("CREATE DATABASE ", "masterrl"))
                    .execute(&mut conn)
                    .await
                    .map_err(|e| {
                        BundleError::Failed(format!("could not create the app database: {e}"))
                    })?;
            }
            let _ = conn.close().await;
            return Ok(child);
        }
        if started.elapsed() > START_TIMEOUT {
            let _ = stop(bins, pgdata).await;
            return Err(BundleError::Failed(format!(
                "the database server did not accept connections within {} s",
                START_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_keeps_the_last_lines() {
        assert_eq!(tail("a\n\nb\nc\n", 2), "b | c");
        assert_eq!(tail("", 3), "");
    }

    #[test]
    fn password_stays_out_of_debug() {
        let t = Target {
            host: "127.0.0.1".into(),
            port: 1,
            password: Secret::new("hunter2"),
        };
        assert!(!format!("{t:?}").contains("hunter2"));
    }
}
