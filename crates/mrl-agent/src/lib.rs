//! The MasterRL machine agent, minus its network link to the server.
//!
//! - [`config`]: the agent settings file (`agent.toml`).
//! - [`state`]: the local state folder, atomic writes and the content-addressed [`state::cache`].
//! - [`hw`]: hardware discovery through the [`hw::HwBackend`] registry, plus CPU, disk, network,
//!   Apple and WSL2 inventory.
//! - [`exec`]: the [`exec::Executor`] trait and the process executor (cgroup v2 limits on Linux).
//! - [`sandbox`]: sandbox profiles and backends (bubblewrap on Linux).
//! - [`transfer`]: ticket-based downloads and uploads.
//! - [`service`]: install the agent as a user-level background service.
//! - [`registry`]: the agent services the main loop runs.

pub mod config;
pub mod exec;
pub mod hw;
pub mod registry;
pub mod sandbox;
pub mod service;
pub mod state;
pub mod transfer;

// <mrl:lib P18> P18 `pub mod` lines go right below this line.
pub mod enroll;
pub mod journal;
pub mod session;
pub mod spool;
pub mod transports;

// <mrl:lib P20> P20 `pub mod` lines go right below this line.
pub mod attempts;
pub mod doctor;
pub mod envs;
pub mod janitor;
pub mod lanes;
pub mod worker_server;

use std::path::PathBuf;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

/// Result alias for this crate.
pub type Result<T, E = AgentError> = std::result::Result<T, E>;

/// Everything that can go wrong in the agent. Every variant has a stable code (see [`Self::code`])
/// explained in `guidance/fragments/errors/p12.yaml` (or the part that owns a reused code).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AgentError {
    /// The settings file could not be read or has a value the agent can't use.
    #[error("the agent settings in {path} can't be used: {reason}")]
    Config {
        /// The settings file, or `<defaults>`.
        path: PathBuf,
        /// What is wrong, in plain words.
        reason: String,
    },
    /// A file or folder in the agent's state could not be read or written.
    #[error("the agent could not use {path}: {source}")]
    State {
        /// The file or folder.
        path: PathBuf,
        /// The cause.
        #[source]
        source: std::io::Error,
    },
    /// The selected sandbox can't run on this computer.
    #[error("the sandbox can't run here: {0}")]
    SandboxUnavailable(String),
    /// A program could not be started, watched or stopped.
    #[error("the agent could not run {program}: {reason}")]
    Exec {
        /// The program.
        program: String,
        /// What went wrong.
        reason: String,
    },
    /// A hardware backend failed while reading the hardware.
    #[error("the agent could not read the hardware through {backend}: {reason}")]
    Hardware {
        /// The backend name, like `nvml`.
        backend: &'static str,
        /// What went wrong.
        reason: String,
    },
    /// Installing, removing or checking the background service failed.
    #[error("the agent could not set up its background service: {0}")]
    Service(String),
    /// A profile, backend or other name is not one the agent knows.
    #[error("{kind} {name:?} is not known here; known: {known}")]
    UnknownName {
        /// What kind of name, like `sandbox profile`.
        kind: &'static str,
        /// The name given.
        name: String,
        /// The names that are known, comma separated.
        known: String,
    },
    /// The node cache can't make room: pinned entries alone use the budget.
    #[error(
        "the node cache needs {needed} bytes, but pinned files use {pinned} of its {budget}-byte budget and the disk keeps {min_free} bytes free"
    )]
    CacheFull {
        /// Bytes the new entry needs.
        needed: u64,
        /// Bytes held by pinned entries.
        pinned: u64,
        /// The cache budget in bytes.
        budget: u64,
        /// Bytes that must stay free on the disk.
        min_free: u64,
    },
    /// A transfer failed after its retries (network or HTTP error).
    #[error("the transfer of {key} failed: {reason}")]
    Transfer {
        /// Object key from the ticket.
        key: String,
        /// What went wrong.
        reason: String,
    },
    /// The storage side refused the ticket (HTTP 401/403).
    #[error("storage refused the transfer ticket for {key} (HTTP {status})")]
    TicketRefused {
        /// Object key from the ticket.
        key: String,
        /// HTTP status.
        status: u16,
    },
    /// A storage-crate error (hashing, ticket, size, checksum, local file).
    #[error(transparent)]
    Storage(#[from] mrl_storage::StorageError),
}

impl AgentError {
    /// The stable error code, like `MRL-NODE-123`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Config { .. } => "MRL-VAL-120",
            Self::State { .. } => "MRL-NODE-121",
            Self::SandboxUnavailable(_) => "MRL-ENV-122",
            Self::Exec { .. } => "MRL-NODE-123",
            Self::Hardware { .. } => "MRL-NODE-124",
            Self::Service(_) => "MRL-NODE-125",
            Self::UnknownName { .. } => "MRL-VAL-126",
            Self::CacheFull { .. } => "MRL-ENV-004",
            Self::Transfer { .. } => "MRL-STORE-004",
            Self::TicketRefused { .. } => "MRL-STORE-007",
            Self::Storage(e) => e.code(),
        }
    }

    /// The code and the message, for logs and the command line.
    pub fn explain(&self) -> String {
        format!("{} {self}", self.code())
    }

    pub(crate) fn state(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::State {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn exec(program: impl Into<String>, reason: impl ToString) -> Self {
        Self::Exec {
            program: program.into(),
            reason: reason.to_string(),
        }
    }
}

/// Everything a running agent shares with its services.
pub struct AgentContext {
    /// The settings in force.
    pub config: config::AgentConfig,
    /// The state folder.
    pub state: state::StateDir,
    /// Hardware backends, from [`hw::registry`] (or a fake in tests).
    pub hw: Vec<Arc<dyn hw::HwBackend>>,
    /// Runs programs.
    pub executor: Arc<dyn exec::Executor>,
    /// The content-addressed node cache.
    pub cache: Arc<state::cache::NodeCache>,
    /// Downloads and uploads by ticket.
    pub transfer: transfer::TransferClient,
}

impl AgentContext {
    /// Builds the context from settings, with the registered hardware backends.
    pub fn from_config(config: config::AgentConfig) -> Result<Self> {
        let hw = hw::registry::backends();
        Self::with_hw(config, hw)
    }

    /// Builds the context from settings with the given hardware backends.
    pub fn with_hw(config: config::AgentConfig, hw: Vec<Arc<dyn hw::HwBackend>>) -> Result<Self> {
        let state = state::StateDir::open(&config.state_dir)?;
        let budget = config.cache.budget_for(&state.cache_dir());
        let cache = Arc::new(state::cache::NodeCache::open(state.cache_dir(), budget)?);
        let executor = Arc::new(exec::process::ProcessExecutor::new(&config.exec)?);
        let transfer = transfer::TransferClient::new(&config.transfer)?;
        Ok(Self {
            config,
            state,
            hw,
            executor,
            cache,
            transfer,
        })
    }

    /// A full inventory of this computer.
    pub fn inventory(&self) -> hw::Inventory {
        hw::collect_inventory(&self.hw)
    }
}

/// The agent main loop: saves the inventory to the state folder, runs every service until
/// `shutdown` is cancelled or one service fails, then stops the others.
pub async fn run_agent(
    ctx: Arc<AgentContext>,
    services: Vec<Box<dyn registry::AgentService>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let inventory = ctx.inventory();
    let json = serde_json::to_vec_pretty(&inventory).map_err(|e| AgentError::Exec {
        program: "inventory".into(),
        reason: e.to_string(),
    })?;
    ctx.state.write_atomic("inventory.json", &json)?;
    ctx.state
        .write_atomic(PID_FILE, std::process::id().to_string().as_bytes())?;

    let mut tasks = tokio::task::JoinSet::new();
    for service in services {
        let ctx = Arc::clone(&ctx);
        let stop = shutdown.child_token();
        tasks.spawn(async move {
            let name = service.name();
            (name, service.run(ctx, stop).await)
        });
    }
    let mut first_error = None;
    while let Some(joined) = tasks.join_next().await {
        let failed = match joined {
            Ok((_, Ok(()))) => None,
            Ok((_, Err(e))) => Some(e),
            Err(e) => Some(AgentError::exec("an agent service", e)),
        };
        if let Some(e) = failed {
            shutdown.cancel();
            first_error.get_or_insert(e);
        }
    }
    let _ = std::fs::remove_file(ctx.state.root().join(PID_FILE));
    first_error.map_or(Ok(()), Err)
}

/// File in the state folder that holds the running agent's process id.
pub const PID_FILE: &str = "agent.pid";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_stable() {
        let e = AgentError::CacheFull {
            needed: 1,
            pinned: 2,
            budget: 3,
            min_free: 0,
        };
        assert_eq!(e.code(), "MRL-ENV-004");
        assert!(e.explain().starts_with("MRL-ENV-004 the node cache"));
        let e = AgentError::Storage(mrl_storage::StorageError::TicketExpired);
        assert_eq!(e.code(), "MRL-STORE-008");
    }
}
