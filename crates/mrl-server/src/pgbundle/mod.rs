//! The bundled PostgreSQL manager (Doc 22 §4, §10): the server runs its own database in
//! solo mode, invisible to the user.
//!
//! Layout of the data folder:
//! - `mrl-server.lock`: held while a server uses the folder (one server per folder).
//! - `pgdata/`: the cluster. `initdb` makes it on first start with a random superuser
//!   password kept in the `SecretStore`.
//! - `backups/`: `pg_dump` files, daily and before every migration, newest 7 kept.
//! - `postgres.log` (and `.1`): the database server's log.
//!
//! The server listens only on a private Unix socket folder (macOS, Linux) or on
//! `127.0.0.1` with a random port (Windows). A supervisor restarts it after a crash with
//! backoff, and gives up after 5 crashes in 10 minutes.

mod backup;
mod binaries;
mod cluster;
mod secrets;
mod upgrade;

#[cfg(test)]
mod tests;

pub use backup::Backups;
pub use binaries::PgBinaries;
pub use secrets::FileSecretStore;

use crate::config::PgBundleConfig;
use crate::db::codes;
use crate::seams::{Secret, SecretStore};
use cluster::Target;
use sqlx::postgres::PgConnectOptions;
use std::collections::VecDeque;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::Child;
use tokio_util::sync::CancellationToken;

/// The superuser `initdb` creates.
pub const SUPERUSER: &str = "mrl";
/// The app database.
pub const DATABASE: &str = "masterrl";
/// The `SecretStore` name of the superuser password.
pub const PASSWORD_SECRET: &str = "pgbundle.superuser_password";

const DAILY: Duration = Duration::from_secs(24 * 3600);
const CRASH_WINDOW: Duration = Duration::from_secs(600);
const MAX_CRASHES: usize = 5;
/// How long a held folder lock is retried before the folder counts as in use.
const LOCK_WAIT: Duration = Duration::from_secs(2);

/// The bundled database could not be set up or run.
#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    /// The PostgreSQL programs were not found.
    #[error("the database programs were not found: {0}")]
    BinariesMissing(String),
    /// Another server holds the data folder.
    #[error("{} is already used by another MasterRL server", .0.display())]
    Locked(PathBuf),
    /// The data folder is from a newer major version.
    #[error("the data folder was made by PostgreSQL {found}, and this app has PostgreSQL {have}")]
    Newer {
        /// Version in `PG_VERSION`.
        found: u32,
        /// Version of the binaries.
        have: u32,
    },
    /// Setting up, starting or keeping the server running failed.
    #[error("{0}")]
    Failed(String),
    /// A backup or restore failed.
    #[error("{0}")]
    Backup(String),
    /// The major-version upgrade failed.
    #[error("{0}")]
    Upgrade(String),
}

impl BundleError {
    /// The catalogued code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::BinariesMissing(_) => codes::BINARIES_MISSING,
            Self::Locked(_) => codes::DATA_DIR_LOCKED,
            Self::Newer { .. } => codes::DATA_DIR_NEWER,
            Self::Failed(_) => codes::BUNDLE_FAILED,
            Self::Backup(_) => codes::BACKUP_FAILED,
            Self::Upgrade(_) => codes::UPGRADE_FAILED,
        }
    }

    /// Code, title, details and steps, for people reading a terminal.
    pub fn explain(&self) -> String {
        crate::db::explain(self.code(), &self.to_string())
    }
}

/// What [`PgBundle::start`] needs.
pub struct BundleOptions {
    /// The data folder.
    pub data_dir: PathBuf,
    /// Programs of the version to run.
    pub binaries: PgBinaries,
    /// Programs of the previous major version, for a one-time upgrade.
    pub previous: Option<PgBinaries>,
    /// Backups to keep.
    pub backups_keep: usize,
    /// Where the superuser password lives.
    pub secrets: Arc<dyn SecretStore>,
}

impl BundleOptions {
    /// Options from `[pgbundle]` with already located `binaries`.
    pub async fn from_config(
        config: &PgBundleConfig,
        binaries: PgBinaries,
        secrets: Arc<dyn SecretStore>,
    ) -> Result<Self, BundleError> {
        let previous = match &config.previous_bin_dir {
            Some(dir) => Some(PgBinaries::in_dir(dir).await?),
            None => None,
        };
        Ok(Self {
            data_dir: data_dir(config)?,
            binaries,
            previous,
            backups_keep: usize::try_from(config.backups_keep).unwrap_or(7),
            secrets,
        })
    }
}

/// A running bundled PostgreSQL. Call [`PgBundle::stop`] on shutdown.
pub struct PgBundle {
    target: Target,
    backups: Arc<Backups>,
    data_dir: PathBuf,
    stop: CancellationToken,
    failed: CancellationToken,
    supervisor: tokio::task::JoinHandle<()>,
    _lock: File,
}

impl PgBundle {
    /// Locks the data folder, creates or upgrades the cluster, starts the server and its
    /// supervisor (crash restarts, daily backups).
    pub async fn start(o: BundleOptions) -> Result<PgBundle, BundleError> {
        create_private_dir(&o.data_dir).map_err(|e| {
            BundleError::Failed(format!("could not create {}: {e}", o.data_dir.display()))
        })?;
        let lock = lock_data_dir(&o.data_dir).await?;
        let pgdata = o.data_dir.join("pgdata");
        let version = cluster_version(&pgdata)?;
        let password = password(o.secrets.as_ref(), version.is_some()).await?;
        match version {
            None => cluster::initdb(&o.binaries, &pgdata, &password).await?,
            Some(v) if v == o.binaries.major() => {}
            Some(v) if v > o.binaries.major() => {
                return Err(BundleError::Newer {
                    found: v,
                    have: o.binaries.major(),
                })
            }
            Some(v) => match &o.previous {
                Some(old) if old.major() == v => {
                    upgrade::upgrade(old, &o.binaries, &o.data_dir, &password).await?
                }
                _ => {
                    return Err(BundleError::Upgrade(format!(
                        "the data folder is PostgreSQL {v} and this app runs {}; \
                         set pgbundle.previous_bin_dir to the PostgreSQL {v} programs",
                        o.binaries.major()
                    )))
                }
            },
        }
        cluster::stop_leftover(&o.binaries, &pgdata).await;
        let target = new_target(&o.data_dir, password)?;
        let log = o.data_dir.join("postgres.log");
        let child = cluster::launch(&o.binaries, &pgdata, &target, &log).await?;
        let backups = Arc::new(Backups::new(
            o.binaries.clone(),
            target.clone(),
            o.data_dir.join("backups"),
            o.backups_keep,
        ));
        let (stop, failed) = (CancellationToken::new(), CancellationToken::new());
        let supervisor = tokio::spawn(supervise(
            Supervised {
                bins: o.binaries,
                pgdata,
                target: target.clone(),
                log,
                backups: backups.clone(),
                stop: stop.clone(),
                failed: failed.clone(),
            },
            child,
        ));
        tracing::info!(data_dir = %o.data_dir.display(), "bundled database running");
        Ok(PgBundle {
            target,
            backups,
            data_dir: o.data_dir,
            stop,
            failed,
            supervisor,
            _lock: lock,
        })
    }

    /// sqlx options for the app database.
    pub fn connect_options(&self) -> PgConnectOptions {
        self.target.options(DATABASE)
    }

    /// The backups of this database.
    pub fn backups(&self) -> Arc<Backups> {
        self.backups.clone()
    }

    /// The data folder.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Cancelled when the server crashed too often and the supervisor gave up.
    pub fn failed(&self) -> CancellationToken {
        self.failed.clone()
    }

    /// The postmaster's process id, from `pgdata/postmaster.pid`.
    pub fn server_pid(&self) -> Option<u32> {
        let text =
            std::fs::read_to_string(self.data_dir.join("pgdata").join("postmaster.pid")).ok()?;
        text.lines().next()?.trim().parse().ok()
    }

    /// Stops the server cleanly and waits for the supervisor.
    pub async fn stop(self) -> Result<(), BundleError> {
        self.stop.cancel();
        self.supervisor
            .await
            .map_err(|e| BundleError::Failed(format!("the database supervisor failed: {e}")))?;
        if self.failed.is_cancelled() {
            return Err(BundleError::Failed(
                "the database server kept stopping and was not restarted".to_owned(),
            ));
        }
        Ok(())
    }
}

struct Supervised {
    bins: PgBinaries,
    pgdata: PathBuf,
    target: Target,
    log: PathBuf,
    backups: Arc<Backups>,
    stop: CancellationToken,
    failed: CancellationToken,
}

async fn supervise(s: Supervised, mut child: Child) {
    let mut crashes: VecDeque<Instant> = VecDeque::new();
    let mut tick = tokio::time::interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            () = s.stop.cancelled() => {
                if let Err(e) = cluster::stop(&s.bins, &s.pgdata).await {
                    tracing::error!(error = %e, "could not stop the database cleanly");
                    let _ = child.start_kill();
                }
                let _ = child.wait().await;
                remove_socket_dir(&s.target);
                tracing::info!("bundled database stopped");
                return;
            }
            status = child.wait() => {
                let text = std::fs::read_to_string(&s.log).unwrap_or_default();
                tracing::error!(code = codes::BUNDLE_FAILED, status = ?status.ok(), log = %cluster::tail(&text, 5),
                    "the bundled database stopped unexpectedly");
                match restart(&s, &mut crashes).await {
                    Some(c) => child = c,
                    None => {
                        s.failed.cancel();
                        remove_socket_dir(&s.target);
                        return;
                    }
                }
            }
            _ = tick.tick() => {
                if s.backups.due(DAILY) {
                    if let Err(e) = s.backups.create("daily").await {
                        tracing::error!(code = e.code(), error = %e, "daily database backup failed");
                    }
                }
            }
        }
    }
}

/// Restarts with 1, 2, 4… 30 s waits. `None` after too many crashes or when stopping.
async fn restart(s: &Supervised, crashes: &mut VecDeque<Instant>) -> Option<Child> {
    let mut delay = Duration::from_secs(1);
    loop {
        let now = Instant::now();
        crashes.push_back(now);
        while crashes
            .front()
            .is_some_and(|t| now.duration_since(*t) > CRASH_WINDOW)
        {
            crashes.pop_front();
        }
        if crashes.len() > MAX_CRASHES {
            tracing::error!(
                code = codes::BUNDLE_FAILED,
                "the bundled database kept stopping; giving up"
            );
            return None;
        }
        tokio::select! {
            () = s.stop.cancelled() => return None,
            () = tokio::time::sleep(delay) => {}
        }
        cluster::stop_leftover(&s.bins, &s.pgdata).await;
        match cluster::launch(&s.bins, &s.pgdata, &s.target, &s.log).await {
            Ok(c) => {
                tracing::warn!("bundled database restarted");
                return Some(c);
            }
            Err(e) => {
                tracing::error!(code = e.code(), error = %e, "could not restart the bundled database");
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// `pgbundle.data_dir`, or [`default_data_dir`].
pub fn data_dir(config: &PgBundleConfig) -> Result<PathBuf, BundleError> {
    match &config.data_dir {
        Some(d) => Ok(d.clone()),
        None => default_data_dir().ok_or_else(|| {
            BundleError::Failed(
                "found no app data folder for this account; set pgbundle.data_dir".to_owned(),
            )
        }),
    }
}

/// The default data folder: the per-account app data folder plus `postgres`.
pub fn default_data_dir() -> Option<PathBuf> {
    let env = |k: &str| {
        std::env::var_os(k)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    let base = if cfg!(target_os = "macos") {
        env("HOME").map(|h| {
            h.join("Library")
                .join("Application Support")
                .join("MasterRL")
        })
    } else if cfg!(windows) {
        env("LOCALAPPDATA").map(|d| d.join("MasterRL"))
    } else {
        env("XDG_DATA_HOME")
            .or_else(|| env("HOME").map(|h| h.join(".local").join("share")))
            .map(|d| d.join("masterrl"))
    };
    base.map(|b| b.join("postgres"))
}

/// Holds an exclusive lock on `<dir>/mrl-server.lock` for as long as the file stays open.
///
/// A lock that was just released can stay held for a moment: a process that another thread
/// is starting keeps a copy of every open file until it runs its program. So a held lock is
/// retried for [`LOCK_WAIT`] before the folder counts as in use.
async fn lock_data_dir(dir: &Path) -> Result<File, BundleError> {
    let path = dir.join("mrl-server.lock");
    let file = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| BundleError::Failed(format!("could not open {}: {e}", path.display())))?;
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(BundleError::Locked(dir.to_path_buf()))
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(BundleError::Failed(format!(
                    "could not lock {}: {e}",
                    path.display()
                )))
            }
        }
    }
}

/// The major version in `pgdata/PG_VERSION`, or `None` when there is no cluster yet.
fn cluster_version(pgdata: &Path) -> Result<Option<u32>, BundleError> {
    match std::fs::read_to_string(pgdata.join("PG_VERSION")) {
        Ok(v) => v.trim().parse().map(Some).map_err(|_| {
            BundleError::Failed(format!(
                "{} has an unreadable PG_VERSION file",
                pgdata.display()
            ))
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if pgdata.exists() {
                // A half-made cluster from an interrupted first start: make it again.
                std::fs::remove_dir_all(pgdata).map_err(|e| {
                    BundleError::Failed(format!(
                        "could not remove an unfinished {}: {e}",
                        pgdata.display()
                    ))
                })?;
            }
            Ok(None)
        }
        Err(e) => Err(BundleError::Failed(format!(
            "could not read {}: {e}",
            pgdata.display()
        ))),
    }
}

/// The stored password, or a new random one when the cluster does not exist yet.
async fn password(store: &dyn SecretStore, cluster_exists: bool) -> Result<Secret, BundleError> {
    let fail = |e: crate::seams::SeamError| BundleError::Failed(format!("secret store: {e}"));
    if let Some(p) = store.get(PASSWORD_SECRET).await.map_err(fail)? {
        return Ok(p);
    }
    if cluster_exists {
        return Err(BundleError::Failed(
            "the database password is missing from the secret store, so the server cannot \
             open the existing database"
                .to_owned(),
        ));
    }
    let p = Secret::new(format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    ));
    store.set(PASSWORD_SECRET, p.clone()).await.map_err(fail)?;
    Ok(p)
}

#[cfg(unix)]
fn new_target(data_dir: &Path, password: Secret) -> Result<Target, BundleError> {
    Ok(Target {
        host: socket_dir(data_dir)?.to_string_lossy().into_owned(),
        port: 5432,
        password,
    })
}

#[cfg(not(unix))]
fn new_target(_data_dir: &Path, password: Secret) -> Result<Target, BundleError> {
    Ok(Target {
        host: "127.0.0.1".to_owned(),
        port: free_port()?,
        password,
    })
}

/// A new private socket folder: `<data>/run-<random>`, or under the temp folder when that
/// path is too long for a Unix socket (about 100 bytes).
#[cfg(unix)]
fn socket_dir(data_dir: &Path) -> Result<PathBuf, BundleError> {
    let name = format!(
        "mrl-pg-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let mut dir = data_dir.join(&name);
    // `.s.PGSQL.65535` plus the separator.
    if dir.as_os_str().len() + 15 > 100 {
        dir = std::env::temp_dir().join(&name);
    }
    // `create_dir` fails if the folder exists, so nobody else can have prepared it.
    std::fs::create_dir(&dir)
        .and_then(|()| set_private(&dir, 0o700))
        .map_err(|e| BundleError::Failed(format!("could not create {}: {e}", dir.display())))?;
    Ok(dir)
}

fn remove_socket_dir(t: &Target) {
    if Path::new(&t.host).is_absolute() {
        let _ = std::fs::remove_dir_all(&t.host);
    }
}

/// A free loopback port.
fn free_port() -> Result<u16, BundleError> {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .map_err(|e| BundleError::Failed(format!("found no free local port: {e}")))
}

/// Creates `dir` (and parents); the last folder gets mode 0700 on Unix.
pub(crate) fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    set_private(dir, 0o700)
}

/// Writes `bytes` to `path`, readable only by this account on Unix.
pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    let mut f = o.open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    // An older file keeps its mode on open; tighten it.
    set_private(path, 0o600)
}

#[cfg(unix)]
fn set_private(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_private(_path: &Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
}
