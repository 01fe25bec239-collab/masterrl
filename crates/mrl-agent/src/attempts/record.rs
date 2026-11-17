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
    /// Roles that were sent Prepare.
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
    /// Saves the record (owner-only permissions on Unix).
    pub fn save(&self, job_dir: &Path) -> Result<(), AttemptError> {
        let path = job_dir.join(RECORD_FILE);
        let json =
            serde_json::to_vec_pretty(self).map_err(|e| AttemptError::Spec(e.to_string()))?;
        crate::state::atomic_write(&path, &json).map_err(|e| AgentError::state(&path, e))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| AgentError::state(&path, e))?;
        }
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

#[cfg(test)]
mod tests {
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
