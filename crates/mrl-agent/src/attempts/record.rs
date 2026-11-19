//! What the agent keeps on disk about each attempt (`<state>/jobs/<attempt>/attempt.json`), so a
//! restarted agent can re-adopt live workers: the spec, the nonce, the socket, each worker's
//! process identity (pid + start time, or container name), staged checkpoints and the outcome.
//! The file holds the nonce and short-lived links, so it is readable by the owner only.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::exit::ExitDecision;
use super::spec::AttemptSpec;
use super::AttemptError;
use crate::AgentError;

/// File name inside the job folder.
pub const RECORD_FILE: &str = "attempt.json";
/// Accepted worker message numbers, kept by the worker server.
pub const SEQ_FILE: &str = "seq.json";

/// One attempt's saved state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptRecord {
    /// The spec it was launched with.
    pub spec: AttemptSpec,
    /// Per-attempt secret the workers present in Hello.
    pub nonce: String,
    /// The worker socket.
    pub socket: PathBuf,
    /// `AttemptPhase` number.
    pub phase: i32,
    /// The started worker processes.
    pub workers: Vec<WorkerProc>,
    /// Roles whose `Prepared` came back ok.
    pub prepared: Vec<String>,
    /// Roles that were sent Start.
    pub started: Vec<String>,
    /// Last step seen.
    pub last_step: u64,
    /// Staged checkpoints by id.
    pub checkpoints: BTreeMap<String, StagedCheckpoint>,
    /// The agent was asked to cancel.
    pub cancel_requested: bool,
    /// How it ended, once it has.
    pub exit: Option<ExitDecision>,
    /// Input role -> path, as sent in `Prepare`.
    #[serde(default)]
    pub staged_paths: BTreeMap<String, String>,
    /// When the agent must stop the attempt unless the lease is renewed (Unix ms, wall clock,
    /// kill budget already taken off). Kept across restarts so a restart never extends a lease.
    #[serde(default)]
    pub lease_deadline_unix_ms: Option<i64>,
    /// The primary worker's ExitReport (protobuf bytes), saved before it is acknowledged.
    #[serde(default)]
    pub exit_report: Option<Vec<u8>>,
    /// Control requests (cancel, pause, checkpoint) sent to the workers, as protobuf
    /// `AgentMessage` bytes; sent again when a worker reconnects (workers ignore repeats).
    #[serde(default)]
    pub requests: Vec<Vec<u8>>,
    /// After a cancel or pause: when the agent stops the workers itself (Unix ms, wall clock).
    #[serde(default)]
    pub stop_deadline_unix_ms: Option<i64>,
    /// Upload tickets per checkpoint id, from `UploadArtifacts`.
    #[serde(default)]
    pub uploads: BTreeMap<String, UploadPlan>,
}

/// The tickets for one checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadPlan {
    /// The `UploadArtifacts` command to report on.
    pub command_id: String,
    /// One target per file.
    pub targets: Vec<UploadTarget>,
}

/// One authorized upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadTarget {
    /// Path relative to staging.
    pub local_path: String,
    /// Upload link.
    pub url: String,
    /// Size the core expects.
    pub size: u64,
    /// sha256 the core expects.
    pub sha256: String,
    /// Link expiry, Unix seconds.
    pub expires_unix: i64,
}

impl UploadTarget {
    /// From an `UploadItem`.
    pub fn from_item(item: &mrl_proto::agent::v1::UploadItem) -> Self {
        Self {
            local_path: item.local_path.clone(),
            url: item.url.clone(),
            size: item.size,
            sha256: item.sha256.clone(),
            expires_unix: item.expires_at.as_ref().map_or(0, |t| t.seconds),
        }
    }

    /// The PUT ticket.
    pub fn ticket(&self) -> Result<mrl_storage::TransferTicket, AttemptError> {
        let expires_at = time::OffsetDateTime::from_unix_timestamp(self.expires_unix)
            .ok()
            .filter(|_| self.expires_unix > 0)
            .ok_or_else(|| {
                AttemptError::Spec(format!(
                    "the link for {} has no expiry time",
                    self.local_path
                ))
            })?;
        Ok(mrl_storage::TransferTicket {
            method: mrl_storage::TicketMethod::Put,
            key: self.local_path.clone(),
            max_size: self.size,
            expires_at,
            url: self.url.clone(),
            presigned: self.url.starts_with("http://") || self.url.starts_with("https://"),
            headers: BTreeMap::new(),
        })
    }
}

/// One worker process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerProc {
    /// Role id.
    pub role_id: String,
    /// The primary worker.
    pub primary: bool,
    /// How to find it again.
    pub identity: ProcIdentity,
    /// Linux: the worker's own cgroup (`/sys/fs/cgroup/.../job-…`), so a later agent can stop
    /// everything in it.
    #[serde(default)]
    pub cgroup: Option<PathBuf>,
}

/// How a worker is recognised after an agent restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcIdentity {
    /// An OS process: pid plus start time, so a reused pid is not mistaken for it.
    Pid {
        /// Process id.
        pid: u32,
        /// Start time, seconds since the Unix epoch.
        start_time: u64,
    },
    /// A container, by name.
    Container {
        /// Container name.
        name: String,
    },
}

impl ProcIdentity {
    /// The identity of a running process, if it exists.
    pub fn of_pid(pid: u32) -> Option<Self> {
        process_start(pid).map(|start_time| Self::Pid { pid, start_time })
    }

    /// True while the same process (or container) is still running.
    pub fn alive(&self) -> bool {
        match self {
            Self::Pid { pid, start_time } => process_start(*pid) == Some(*start_time),
            Self::Container { name } => container_running(name),
        }
    }
}

/// Start time of `pid` (seconds since the epoch), if the process exists and is not a zombie.
pub fn process_start(pid: u32) -> Option<u64> {
    use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};
    let Ok(mut sys) = System::new() else {
        return Default::default();
    };
    let p = Pid::from_u32(pid);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[p]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let proc_ = sys.process(p)?;
    (!matches!(proc_.status(), ProcessStatus::Zombie | ProcessStatus::Dead))
        .then(|| proc_.start_time())
}

/// True when a container called `name` is running.
pub fn container_running(name: &str) -> bool {
    crate::envs::which("docker").is_some_and(|docker| {
        std::process::Command::new(docker)
            .args(["inspect", "--format", "{{.State.Running}}", name])
            .stderr(std::process::Stdio::null())
            .output()
            .is_ok_and(|o| o.status.success() && o.stdout.starts_with(b"true"))
    })
}

/// A checkpoint the worker staged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedCheckpoint {
    /// Training step.
    pub step: u64,
    /// The last checkpoint of the run.
    pub is_final: bool,
    /// sha256 of `checkpoint-manifest.json`.
    pub manifest_sha256: String,
    /// The files, relative to staging.
    pub files: Vec<StagedFile>,
    /// The worker asked to commit it.
    pub commit_requested: bool,
    /// `CommitState` number once decided.
    pub state: Option<i32>,
}

/// One staged file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedFile {
    /// Path relative to staging.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// sha256.
    pub sha256: String,
    /// `manifest`, `weights`, ...
    pub role: String,
}

impl AttemptRecord {
    /// A final checkpoint was committed.
    pub fn final_committed(&self) -> bool {
        self.checkpoints.values().any(|c| {
            c.is_final && c.state == Some(mrl_proto::worker::v1::CommitState::Committed as i32)
        })
    }

    /// The attempt must commit a final checkpoint when it completes (TRAIN, not smoke or dry).
    pub fn needs_final_checkpoint(&self) -> bool {
        self.spec.trains() && self.spec.smoke_steps == 0 && !self.spec.dry_run
    }

    /// Saves the record (owner-only permissions on Unix).
    pub fn save(&self, job_dir: &Path) -> Result<(), AttemptError> {
        let path = job_dir.join(RECORD_FILE);
        let json =
            serde_json::to_vec_pretty(self).map_err(|e| AttemptError::Spec(e.to_string()))?;
        write_private(&path, &json).map_err(|e| AgentError::state(&path, e))?;
        Ok(())
    }

    /// Loads the record of a job folder, or `None` when it has none.
    pub fn load(job_dir: &Path) -> Result<Option<Self>, AttemptError> {
        let path = job_dir.join(RECORD_FILE);
        match std::fs::read(&path) {
            Ok(b) => serde_json::from_slice(&b).map(Some).map_err(|e| {
                AttemptError::state(
                    &job_dir.file_name().unwrap_or_default().to_string_lossy(),
                    format!("its saved state in {} can't be read ({e})", path.display()),
                )
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(AgentError::state(path, e).into()),
        }
    }
}

/// Like [`crate::state::atomic_write`], but the file is owner-only (0600 on Unix) from the
/// moment it exists: the temporary file is created with that mode before any byte is written,
/// and the rename keeps it.
pub fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("the path has no file name"))?
        .to_string_lossy();
    let tmp = dir.join(format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut open = std::fs::OpenOptions::new();
        open.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut open, 0o600);
        let mut f = open.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)?;
        #[cfg(unix)]
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Creates a folder only the owner can open (0700 on Unix).
pub fn private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn private_files_are_never_readable_by_others() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("attempt.json");
        super::write_private(&p, b"{}").unwrap();
        super::write_private(&p, b"{\"a\":1}").unwrap();
        let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(std::fs::read(&p).unwrap(), b"{\"a\":1}");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "no temporary file left"
        );
    }

    use super::*;

    #[test]
    fn identity_tracks_this_process() {
        let me = ProcIdentity::of_pid(std::process::id()).unwrap();
        assert!(me.alive());
        let ProcIdentity::Pid { pid, start_time } = me else {
            panic!("pid identity")
        };
        // Same pid, other start time: a reused pid is not the same process.
        assert!(!ProcIdentity::Pid {
            pid,
            start_time: start_time + 1
        }
        .alive());
        assert!(ProcIdentity::of_pid(u32::MAX - 7).is_none());
    }
}
