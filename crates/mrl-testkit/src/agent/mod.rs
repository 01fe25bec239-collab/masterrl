//! An in-process `mrl-agent` with fake GPUs, for integration tests.
//!
//! ```no_run
//! # async fn demo() -> Result<(), mrl_agent::AgentError> {
//! use mrl_agent::hw::fake::Fault;
//! let agent = mrl_testkit::agent::FakeAgent::start(2)?;
//! assert_eq!(agent.inventory().gpus.len(), 2);
//! agent.inject(Fault::Xid { index: 1, code: 79 });
//! let running = agent.serve(Vec::new());
//! running.stop().await?;
//! # Ok(()) }
//! ```

use std::path::Path;
use std::sync::Arc;

use mrl_agent::config::{AgentConfig, CacheConfig, SandboxChoice};
use mrl_agent::exec::ExecSpec;
use mrl_agent::hw::fake::{FakeHw, Fault};
use mrl_agent::hw::{HwBackend, Inventory, Telemetry};
use mrl_agent::registry::AgentService;
use mrl_agent::{AgentContext, AgentError};
use tokio_util::sync::CancellationToken;

/// GPU model of [`FakeAgent::start`] devices.
pub const FAKE_GPU_MODEL: &str = "Fake A100";
/// Memory of each [`FakeAgent::start`] device: 80 GiB.
pub const FAKE_GPU_MEMORY: u64 = 80 << 30;

/// A fake agent: real state folder (temporary), real cache and process executor, fake GPUs.
pub struct FakeAgent {
    /// The fake GPUs; inject faults here or with [`Self::inject`].
    pub hw: Arc<FakeHw>,
    /// The agent context, as services see it.
    pub ctx: Arc<AgentContext>,
    dir: tempfile::TempDir,
}

impl FakeAgent {
    /// An agent with `gpus` idle fake GPUs.
    pub fn start(gpus: u32) -> Result<Self, AgentError> {
        Self::with_hw(FakeHw::with_gpus(gpus, FAKE_GPU_MODEL, FAKE_GPU_MEMORY))
    }

    /// An agent with the given fake hardware.
    pub fn with_hw(hw: FakeHw) -> Result<Self, AgentError> {
        let dir = tempfile::tempdir().map_err(|e| AgentError::State {
            path: std::env::temp_dir(),
            source: e,
        })?;
        let config = AgentConfig {
            state_dir: dir.path().join("state"),
            cache: CacheConfig {
                max_bytes: Some(1 << 30),
                min_free_bytes: 0,
            },
            sandbox: mrl_agent::config::SandboxConfig {
                backend: SandboxChoice::None,
                bwrap_path: None,
            },
            ..AgentConfig::default()
        };
        let hw = Arc::new(hw);
        let backends: Vec<Arc<dyn HwBackend>> = vec![hw.clone()];
        let ctx = Arc::new(AgentContext::with_hw(config, backends)?);
        Ok(Self { hw, ctx, dir })
    }

    /// The agent's inventory (fake GPUs, real CPU/memory/disks).
    pub fn inventory(&self) -> Inventory {
        self.ctx.inventory()
    }

    /// Live readings, including events since the last call.
    pub fn telemetry(&self) -> Telemetry {
        mrl_agent::hw::collect_telemetry(&self.ctx.hw, &self.inventory(), self.ctx.state.root())
    }

    /// Injects a fault into the fake GPUs.
    pub fn inject(&self, fault: Fault) {
        self.hw.inject(fault);
    }

    /// The temporary folder that holds the state folder; removed on drop.
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// A spec for `program` with its folders under this agent's job folder `name`.
    pub fn spec(&self, name: &str, program: &str, args: &[&str]) -> ExecSpec {
        ExecSpec::new(name, program, args, &self.ctx.state.job_dir(name))
    }

    /// Runs the main loop with `services` on the current runtime until [`RunningAgent::stop`].
    pub fn serve(&self, services: Vec<Box<dyn AgentService>>) -> RunningAgent {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(mrl_agent::run_agent(
            Arc::clone(&self.ctx),
            services,
            shutdown.clone(),
        ));
        RunningAgent { shutdown, task }
    }
}

/// A main loop started by [`FakeAgent::serve`].
pub struct RunningAgent {
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), AgentError>>,
}

impl RunningAgent {
    /// Cancels the main loop and waits for every service to end.
    pub async fn stop(self) -> Result<(), AgentError> {
        self.shutdown.cancel();
        self.join().await
    }

    /// Waits for the main loop to end on its own (a service failed or cancelled it).
    pub async fn join(self) -> Result<(), AgentError> {
        self.task.await.map_err(|e| AgentError::Exec {
            program: "agent main loop".into(),
            reason: e.to_string(),
        })?
    }
}

// <mrl:testkit-agent P18> P18 `pub mod` lines go right below this line.

// <mrl:testkit-agent P20> P20 `pub mod` lines go right below this line.

#[cfg(test)]
mod tests;
