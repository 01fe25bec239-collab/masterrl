//! Which gateway lane carries each worker message, and de-duplication of worker messages.
//!
//! Lanes follow Doc 09 §7 with the `agent.v1.Lane` names: progress, heartbeats, prepared,
//! checkpoints, exit reports and online-RL events go on `CONTROL`; metrics on `TELEMETRY`;
//! logs and sample batches on `BULK`. `Hello` is never relayed (the agent answers it).
//!
//! Delivery from the worker is at-least-once, so [`Dedup`] keeps the highest accepted `seq` per
//! `(attempt_id, role_id)`. Unsequenced messages (`seq = 0`: `Hello`, `Heartbeat`) are never
//! de-duplicated.

use std::collections::BTreeMap;

use mrl_proto::agent::v1::Lane;
use mrl_proto::worker::v1::worker_message::Body;
use mrl_proto::worker::v1::WorkerMessage;
use serde::{Deserialize, Serialize};

/// The lane for a worker message, or `None` for messages the agent keeps to itself (`Hello`,
/// an empty body).
pub fn lane_for(msg: &WorkerMessage) -> Option<Lane> {
    Some(match msg.body.as_ref()? {
        Body::Hello(_) => return None,
        Body::Metrics(_) => Lane::Telemetry,
        Body::Log(_) | Body::Samples(_) => Lane::Bulk,
        Body::Prepared(_)
        | Body::Progress(_)
        | Body::CheckpointStaged(_)
        | Body::CheckpointCommit(_)
        | Body::Heartbeat(_)
        | Body::Exit(_)
        | Body::PolicyVersion(_)
        | Body::RolloutBatch(_)
        | Body::RewardBatchStats(_)
        | Body::OutputsStaged(_) => Lane::Control,
    })
}

/// What [`Dedup::accept`] decided about one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The next message in order: process it.
    New,
    /// Already accepted (a replay): ack again and drop it.
    Duplicate,
    /// Unsequenced (`seq = 0`): process it, never ack or de-duplicate it.
    Unsequenced,
    /// A message is missing before this one; the sender must replay from `expected`.
    Gap {
        /// The seq that should have come next.
        expected: u64,
    },
}

/// Highest accepted `seq` per `(attempt_id, role_id)`. Serializes to JSON so it survives an
/// agent restart (keys are `attempt_id/role_id`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Dedup {
    accepted: BTreeMap<String, u64>,
}

impl Dedup {
    fn key(attempt_id: &str, role_id: &str) -> String {
        format!("{attempt_id}/{role_id}")
    }

    /// The highest contiguous seq accepted for this worker (0 before the first one).
    pub fn accepted(&self, attempt_id: &str, role_id: &str) -> u64 {
        self.accepted
            .get(&Self::key(attempt_id, role_id))
            .copied()
            .unwrap_or(0)
    }

    /// Checks `seq` without recording it.
    pub fn check(&self, attempt_id: &str, role_id: &str, seq: u64) -> Verdict {
        if seq == 0 {
            return Verdict::Unsequenced;
        }
        let last = self.accepted(attempt_id, role_id);
        if seq <= last {
            Verdict::Duplicate
        } else if seq == last + 1 {
            Verdict::New
        } else {
            Verdict::Gap { expected: last + 1 }
        }
    }

    /// Records `seq` as accepted. Call only after [`Self::check`] returned [`Verdict::New`]
    /// and the message was handled.
    pub fn record(&mut self, attempt_id: &str, role_id: &str, seq: u64) {
        let slot = self
            .accepted
            .entry(Self::key(attempt_id, role_id))
            .or_insert(0);
        *slot = (*slot).max(seq);
    }

    /// [`Self::check`], then [`Self::record`] when new.
    pub fn accept(&mut self, attempt_id: &str, role_id: &str, seq: u64) -> Verdict {
        let v = self.check(attempt_id, role_id, seq);
        if v == Verdict::New {
            self.record(attempt_id, role_id, seq);
        }
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::worker::v1::{Hello, LogChunk, MetricsBatch, Progress};

    fn msg(body: Body) -> WorkerMessage {
        WorkerMessage {
            seq: 1,
            sent_at: None,
            body: Some(body),
        }
    }

    #[test]
    fn lanes_follow_doc_09() {
        assert_eq!(lane_for(&msg(Body::Hello(Hello::default()))), None);
        assert_eq!(
            lane_for(&msg(Body::Progress(Progress::default()))),
            Some(Lane::Control)
        );
        assert_eq!(
            lane_for(&msg(Body::Metrics(MetricsBatch::default()))),
            Some(Lane::Telemetry)
        );
        assert_eq!(
            lane_for(&msg(Body::Log(LogChunk::default()))),
            Some(Lane::Bulk)
        );
        assert_eq!(lane_for(&WorkerMessage::default()), None);
    }

    #[test]
    fn dedup_per_attempt_and_role() {
        let mut d = Dedup::default();
        assert_eq!(d.accept("a", "trainer", 0), Verdict::Unsequenced);
        assert_eq!(d.accept("a", "trainer", 1), Verdict::New);
        assert_eq!(d.accept("a", "trainer", 1), Verdict::Duplicate);
        assert_eq!(d.accept("a", "trainer", 3), Verdict::Gap { expected: 2 });
        assert_eq!(d.accept("a", "trainer", 2), Verdict::New);
        // Another role and another attempt count on their own.
        assert_eq!(d.accept("a", "rollout", 1), Verdict::New);
        assert_eq!(d.accept("b", "trainer", 1), Verdict::New);
        let json = serde_json::to_string(&d).unwrap();
        let back: Dedup = serde_json::from_str(&json).unwrap();
        assert_eq!(back.accepted("a", "trainer"), 2);
        assert_eq!(back.check("a", "trainer", 2), Verdict::Duplicate);
    }
}
