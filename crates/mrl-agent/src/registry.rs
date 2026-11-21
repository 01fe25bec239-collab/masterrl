//! The agent services this build registers. Each part adds one line inside the block, like
//! `Box::new(crate::gateway::GatewayService::new()),` (P18). The main loop
//! ([`crate::run_agent`]) runs every one until shutdown. An empty list is valid: no networked
//! service exists yet.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::{AgentContext, Result};

/// A long-running part of the agent (gateway link, telemetry, janitor, ...).
#[async_trait::async_trait]
pub trait AgentService: Send + Sync {
    /// Short name for logs, like `gateway`.
    fn name(&self) -> &'static str;
    /// Runs until `shutdown` is cancelled. Returning an error stops the whole agent.
    async fn run(&self, ctx: Arc<AgentContext>, shutdown: CancellationToken) -> Result<()>;
}

/// Every registered service, in start order.
pub fn services() -> Vec<Box<dyn AgentService>> {
    vec![
        // <mrl:register>
        // <mrl:register P18> P18 services go right below this line.
        Box::new(crate::session::GatewayService::new()),
        // <mrl:register P20> P20 services go right below this line.
        Box::new(crate::janitor::JanitorService),
        Box::new(crate::doctor::DoctorService),
        // </mrl:register>
    ]
}
