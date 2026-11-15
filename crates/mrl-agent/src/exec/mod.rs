//! Running programs for jobs.
//!
//! An [`Executor`] starts an [`ExecSpec`] and returns an [`Execution`] to watch and stop it.
//! The [`process::ProcessExecutor`] runs plain OS processes: on Linux each one gets its own
//! cgroup v2 (CPU, memory and process-count limits, OOM detection, whole-tree kill); on macOS
//! and Windows it has the same interface and reports limits as unsupported. Sandboxing is a
//! separate step: [`crate::sandbox::SandboxBackend::wrap`] rewrites the spec first.

#[cfg(target_os = "linux")]
mod cgroup;
pub mod logs;
pub mod process;
mod tree;

pub use tree::TOKEN_ENV;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::Result;

/// What to run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecSpec {
    /// Job name: letters, digits, `.`, `_` and `-` only. Names the cgroup and logs.
    pub name: String,
    /// Program to run (a path, or a name looked up on the job's `PATH`).
    pub program: String,
    /// Arguments.
    pub args: Vec<String>,
    /// Extra environment, on top of the agent's allow-list.
    pub env: BTreeMap<String, String>,
    /// GPUs the job may use (uuids or indexes); sets `CUDA_VISIBLE_DEVICES`. Empty means none.
    pub gpus: Vec<String>,
    /// Working folder (created if missing).
    pub work_dir: PathBuf,
    /// The job's writable staging folder (created; also `MRL_STAGING_DIR`, and `TMPDIR` is its
    /// `tmp/`).
    pub staging_dir: PathBuf,
    /// Where `stdout.log` and `stderr.log` go (created).
    pub log_dir: PathBuf,
    /// Resource limits.
    pub limits: Limits,
}

impl ExecSpec {
    /// A spec with the program, folders under `dir` (`work/`, `staging/`, `logs/`) and no
    /// limits.
    pub fn new(name: &str, program: &str, args: &[&str], dir: &std::path::Path) -> Self {
        Self {
            name: name.to_owned(),
            program: program.to_owned(),
            args: args.iter().map(|a| (*a).to_owned()).collect(),
            env: BTreeMap::new(),
            gpus: Vec::new(),
            work_dir: dir.join("work"),
            staging_dir: dir.join("staging"),
            log_dir: dir.join("logs"),
            limits: Limits::default(),
        }
    }
}

/// Resource limits. `None` means no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// CPU time, in thousandths of a core (1500 = 1.5 cores).
    pub cpu_millis: Option<u64>,
    /// Memory (RAM, no swap), bytes.
    pub memory_bytes: Option<u64>,
    /// Number of processes and threads.
    pub pids: Option<u64>,
}

impl Limits {
    /// True when no limit is set.
    pub fn is_empty(&self) -> bool {
        self.cpu_millis.is_none() && self.memory_bytes.is_none() && self.pids.is_none()
    }
}

/// Which limits an executor can enforce here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitSupport {
    /// CPU limit.
    pub cpu: bool,
    /// Memory limit.
    pub memory: bool,
    /// Process-count limit.
    pub pids: bool,
    /// Out-of-memory kills are detected.
    pub oom_detection: bool,
    /// Stopping a job reaches every process it started, even ones that detached (`setsid`)
    /// and lost their parent. True on Linux (cgroup, or the job token in `/proc`); false on
    /// macOS, which offers no unprivileged way to find such a process.
    pub contained: bool,
    /// Why some are missing, in plain words.
    pub note: Option<String>,
}

/// How a program ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitReport {
    /// Exit code, if it exited normally.
    pub code: Option<i32>,
    /// Signal number, if a signal ended it (Unix).
    pub signal: Option<i32>,
    /// The kernel killed it for using more memory than its limit.
    pub oom_killed: bool,
    /// The limits that were enforced for this run.
    pub limits: LimitSupport,
    /// Wall time, milliseconds.
    pub runtime_ms: u64,
}

impl ExitReport {
    /// Exit code 0.
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }
}

/// Starts programs.
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    /// The limits this executor enforces on this computer.
    fn limit_support(&self) -> LimitSupport;
    /// Starts `spec`.
    async fn spawn(&self, spec: ExecSpec) -> Result<Box<dyn Execution>>;
}

/// A started program.
#[async_trait::async_trait]
pub trait Execution: Send {
    /// OS process id of the main process.
    fn pid(&self) -> Option<u32>;
    /// Waits for the main process to end, then kills anything it left behind.
    async fn wait(&mut self) -> Result<ExitReport>;
    /// Kills the whole process tree now. Call [`Self::wait`] afterwards for the report.
    async fn kill(&mut self) -> Result<()>;
    /// Asks the tree to stop (SIGTERM), waits up to `grace`, then kills it.
    async fn stop(&mut self, grace: Duration) -> Result<ExitReport>;
}
