//! The CONTROL sink for relayed checkpoint messages.

use super::service::CheckpointService;
use crate::modules::jobs::JobsError;
use crate::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink, SinkError};
use mrl_proto::agent::v1::agent_message::Body;
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use std::sync::Arc;

/// Takes `worker.checkpoint_staged` and `worker.checkpoint_commit`. Both handlers are
/// idempotent per checkpoint id, so at-least-once delivery is safe. Only a database or
/// storage outage is an error (the item is kept and retried); bad input is logged and dropped.
pub struct CheckpointSink {
    service: Arc<CheckpointService>,
}

impl CheckpointSink {
    /// A sink feeding `service`.
    pub fn new(service: Arc<CheckpointService>) -> Self {
        Self { service }
    }
}

#[async_trait::async_trait]
impl LaneSink for CheckpointSink {
    fn name(&self) -> &'static str {
        "checkpoints"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Control]
    }

    fn control_kinds(&self) -> &'static [&'static str] {
        &["worker.checkpoint_staged", "worker.checkpoint_commit"]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        let Some(Body::Worker(relay)) = &item.message.body else {
            return Ok(());
        };
        let at = relay.attempt.clone().unwrap_or_default();
        let result = match relay.message.as_ref().and_then(|m| m.body.as_ref()) {
            Some(WorkerBody::CheckpointStaged(s)) => {
                self.service.on_staged(item.node_id, &at, s).await
            }
            Some(WorkerBody::CheckpointCommit(c)) => {
                self.service.on_commit_request(item.node_id, &at, c).await
            }
            _ => Ok(()),
        };
        match result {
            Ok(()) => Ok(()),
            Err(e @ (JobsError::Db(_) | JobsError::Seam(_))) => Err(SinkError {
                sink: "checkpoints",
                detail: e.to_string(),
            }),
            Err(e) => {
                tracing::warn!(code = e.code(), error = %e, node = %item.node_id, "checkpoint message refused");
                Ok(())
            }
        }
    }
}
