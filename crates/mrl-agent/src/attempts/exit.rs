//! How an attempt ended: the worker's `ExitReport` combined with the agent's own evidence.
//!
//! Precedence (Doc 05 §14), highest first: agent out-of-band evidence (lease ran out, cgroup
//! OOM kill, GPU XID / out-of-memory / device lost, environment mismatch, stall) → the worker's
//! report (adapter, then harness classification) → agent defaults (no report: `HARNESS_ERROR`).
//! A worker's own claim never overrides contradicting evidence: a worker that reports
//! `COMPLETED` but was OOM-killed is `OOM`.

use mrl_proto::agent::v1::AttemptPhase;
use mrl_proto::common::v1::{ClassifiedBy, Failure, FailureClass};
use mrl_proto::worker::v1::{ExitKind, ExitReport};
use serde::{Deserialize, Serialize};

use crate::hw::{HwEvent, HwEventKind};

/// XID codes that mean the GPU itself failed (HARDWARE), as P12's NVML backend flags them.
const FATAL_XIDS: [u64; 6] = [48, 63, 64, 74, 79, 95];

/// Everything known when the attempt's workers are gone.
#[derive(Debug, Clone, Default)]
pub struct ExitEvidence {
    /// The primary worker's report, if it sent one.
    pub report: Option<ExitReport>,
    /// The primary worker's process exit code (`None`: killed by a signal, or re-adopted).
    pub exit_code: Option<i32>,
    /// Signal that ended the primary worker (Unix).
    pub signal: Option<i32>,
    /// The kernel killed a worker for going over its memory limit (cgroup evidence).
    pub oom_killed: bool,
    /// GPU events seen on the attempt's GPUs while it ran.
    pub hw_events: Vec<HwEvent>,
    /// The agent stopped the attempt because its lease ran out.
    pub self_fenced: bool,
    /// The agent stopped the attempt because the worker went silent.
    pub stalled: bool,
    /// The worker's environment did not match the attempt (digest or protocol version).
    pub env_mismatch: Option<String>,
    /// The agent was asked to cancel.
    pub cancel_requested: bool,
    /// The worker broke the protocol.
    pub protocol_error: Option<String>,
    /// A final checkpoint was committed.
    pub final_committed: bool,
    /// The job must commit a final checkpoint when it completes (TRAIN, not a smoke run).
    pub needs_final_checkpoint: bool,
    /// The worker process was re-adopted after an agent restart (no exit status).
    pub readopted: bool,
    /// The agent itself stopped the workers (cancel, pause deadline, lease, stall, mismatch),
    /// so a signal or non-zero status is its own doing, not the worker's.
    pub agent_stopped: bool,
}

/// The agent's verdict.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExitDecision {
    /// `EXITED` or `SELF_FENCED` (as the proto number).
    pub phase: i32,
    /// `ExitKind` number.
    pub exit_kind: i32,
    /// Process exit code (-1 when unknown).
    pub exit_code: i32,
    /// Set if and only if `exit_kind` is FAILED.
    #[serde(with = "failure_json")]
    pub failure: Option<Failure>,
}

impl ExitDecision {
    /// The failure class, if failed.
    pub fn class(&self) -> Option<FailureClass> {
        self.failure
            .as_ref()
            .and_then(|f| FailureClass::try_from(f.class).ok())
    }
}

fn agent_failure(
    class: FailureClass,
    sub: &str,
    message: impl Into<String>,
    retry: bool,
) -> Failure {
    Failure {
        class: class as i32,
        sub_code: sub.to_owned(),
        message: message.into(),
        detail_ref: String::new(),
        retryable_hint: retry,
        suggested_mutations: Vec::new(),
        classified_by: ClassifiedBy::Agent as i32,
    }
}

/// Decides how the attempt ended.
pub fn decide(ev: &ExitEvidence) -> ExitDecision {
    let exit_code = ev.exit_code.unwrap_or(-1);
    let failed = |f: Failure| ExitDecision {
        phase: if ev.self_fenced {
            AttemptPhase::SelfFenced as i32
        } else {
            AttemptPhase::Exited as i32
        },
        exit_kind: ExitKind::Failed as i32,
        exit_code,
        failure: Some(f),
    };
    if let Some(f) = evidence_failure(ev) {
        return failed(f);
    }
    let Some(report) = &ev.report else {
        if ev.cancel_requested {
            return ExitDecision {
                phase: AttemptPhase::Exited as i32,
                exit_kind: ExitKind::Cancelled as i32,
                exit_code,
                failure: None,
            };
        }
        let how = match (ev.exit_code, ev.signal, ev.readopted) {
            (_, _, true) => "after the agent restarted".to_owned(),
            (_, Some(s), _) => format!("after signal {s}"),
            (Some(c), _, _) => format!("with exit code {c}"),
            _ => "for an unknown reason".to_owned(),
        };
        return failed(agent_failure(
            FailureClass::HarnessError,
            "NO_EXIT_REPORT",
            format!("The training worker stopped {how} without reporting how it ended."),
            true,
        ));
    };
    let kind = ExitKind::try_from(report.kind).unwrap_or(ExitKind::Unspecified);
    match kind {
        ExitKind::Failed => failed(report.failure.clone().unwrap_or_else(|| {
            agent_failure(
                FailureClass::HarnessError,
                "FAILED_WITHOUT_CLASS",
                "The training worker reported a failure without saying what kind.",
                true,
            )
        })),
        ExitKind::Completed if ev.needs_final_checkpoint && !ev.final_committed => {
            failed(agent_failure(
                FailureClass::HarnessError,
                "NO_FINAL_CHECKPOINT",
                "The training worker finished without a saved final checkpoint.",
                true,
            ))
        }
        _ if abnormal_status(ev) => failed(agent_failure(
            FailureClass::HarnessError,
            "EXIT_STATUS_MISMATCH",
            format!(
                "The training worker reported {} but its process ended {}.",
                kind.as_str_name()
                    .trim_start_matches("EXIT_KIND_")
                    .to_lowercase(),
                match (ev.exit_code, ev.signal) {
                    (_, Some(s)) => format!("after signal {s}"),
                    (Some(c), _) => format!("with exit code {c}"),
                    _ => "abnormally".to_owned(),
                }
            ),
            true,
        )),
        ExitKind::Unspecified => failed(agent_failure(
            FailureClass::HarnessError,
            "EXIT_KIND_MISSING",
            "The training worker's exit report did not say how it ended.",
            true,
        )),
        _ => ExitDecision {
            phase: AttemptPhase::Exited as i32,
            exit_kind: report.kind,
            exit_code,
            failure: None,
        },
    }
}

/// The process ended badly although the worker reported success (and the agent did not stop
/// it itself). A re-adopted worker has no status to compare.
fn abnormal_status(ev: &ExitEvidence) -> bool {
    !ev.readopted
        && !ev.agent_stopped
        && (ev.signal.is_some() || ev.exit_code.is_some_and(|c| c != 0))
}

/// Out-of-band evidence, strongest first.
fn evidence_failure(ev: &ExitEvidence) -> Option<Failure> {
    if ev.self_fenced {
        return Some(agent_failure(
            FailureClass::NodeLost,
            "LEASE_EXPIRED",
            "The agent stopped the attempt because the server stopped renewing its lease.",
            true,
        ));
    }
    if ev.oom_killed {
        return Some(agent_failure(
            FailureClass::Oom,
            "OOM_HOST",
            "The training worker used more memory than it was allowed and was stopped.",
            false,
        ));
    }
    for e in &ev.hw_events {
        let hw = match e.kind {
            HwEventKind::Xid { code } if FATAL_XIDS.contains(&code) => {
                format!("GPU {} failed with XID error {code}.", e.uuid)
            }
            HwEventKind::DeviceLost => format!("GPU {} stopped responding.", e.uuid),
            _ => continue,
        };
        return Some(agent_failure(FailureClass::Hardware, "GPU_FAULT", hw, true));
    }
    if ev
        .hw_events
        .iter()
        .any(|e| e.kind == HwEventKind::OutOfMemory)
    {
        // The worker's own OOM report (with its suggested mutations) is better when it has one.
        if let Some(f) = ev.report.as_ref().and_then(|r| r.failure.as_ref()) {
            if f.class == FailureClass::Oom as i32 {
                return Some(f.clone());
            }
        }
        return Some(agent_failure(
            FailureClass::Oom,
            "OOM_GPU",
            "A GPU ran out of memory during the attempt.",
            false,
        ));
    }
    if let Some(why) = &ev.env_mismatch {
        return Some(agent_failure(
            FailureClass::DependencyEnv,
            "ENVIRONMENT_MISMATCH",
            why.clone(),
            true,
        ));
    }
    if ev.stalled {
        return Some(agent_failure(
            FailureClass::Stalled,
            "WORKER_SILENT",
            "The training worker stopped sending updates, so the agent stopped it.",
            true,
        ));
    }
    if let Some(why) = &ev.protocol_error {
        return Some(agent_failure(
            FailureClass::HarnessError,
            "PROTOCOL_VIOLATION",
            why.clone(),
            true,
        ));
    }
    None
}

/// `Failure` is a prost message without serde; store it as base64-free JSON of its fields.
mod failure_json {
    use mrl_proto::common::v1::Failure;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct F {
        class: i32,
        sub_code: String,
        message: String,
        detail_ref: String,
        retryable_hint: bool,
        classified_by: i32,
    }

    pub fn serialize<S: Serializer>(f: &Option<Failure>, s: S) -> Result<S::Ok, S::Error> {
        f.as_ref()
            .map(|f| F {
                class: f.class,
                sub_code: f.sub_code.clone(),
                message: f.message.clone(),
                detail_ref: f.detail_ref.clone(),
                retryable_hint: f.retryable_hint,
                classified_by: f.classified_by,
            })
            .serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Failure>, D::Error> {
        Ok(Option::<F>::deserialize(d)?.map(|f| Failure {
            class: f.class,
            sub_code: f.sub_code,
            message: f.message,
            detail_ref: f.detail_ref,
            retryable_hint: f.retryable_hint,
            suggested_mutations: Vec::new(),
            classified_by: f.classified_by,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(kind: ExitKind, failure: Option<Failure>) -> Option<ExitReport> {
        Some(ExitReport {
            kind: kind as i32,
            failure,
            ..ExitReport::default()
        })
    }

    fn harness_oom() -> Failure {
        Failure {
            class: FailureClass::Oom as i32,
            sub_code: "OOM_GPU".into(),
            message: "CUDA out of memory".into(),
            classified_by: ClassifiedBy::Harness as i32,
            ..Failure::default()
        }
    }

    #[test]
    fn completed_with_final_checkpoint_is_clean() {
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Completed, None),
            exit_code: Some(0),
            final_committed: true,
            needs_final_checkpoint: true,
            ..ExitEvidence::default()
        });
        assert_eq!(d.exit_kind, ExitKind::Completed as i32);
        assert_eq!(d.phase, AttemptPhase::Exited as i32);
        assert!(d.failure.is_none());
    }

    #[test]
    fn oom_kill_overrides_a_completed_report() {
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Completed, None),
            oom_killed: true,
            ..ExitEvidence::default()
        });
        assert_eq!(d.class(), Some(FailureClass::Oom));
        assert_eq!(d.failure.unwrap().sub_code, "OOM_HOST");
    }

    #[test]
    fn fake_gpu_oom_is_oom() {
        let ev = HwEvent::now("GPU-0", HwEventKind::OutOfMemory);
        // No report: the agent's evidence alone.
        let d = decide(&ExitEvidence {
            hw_events: vec![ev.clone()],
            exit_code: Some(1),
            ..ExitEvidence::default()
        });
        assert_eq!(d.class(), Some(FailureClass::Oom));
        assert_eq!(
            d.failure.as_ref().unwrap().classified_by,
            ClassifiedBy::Agent as i32
        );
        // With the harness's OOM report, its classification (and hints) are kept.
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Failed, Some(harness_oom())),
            hw_events: vec![ev],
            ..ExitEvidence::default()
        });
        assert_eq!(
            d.failure.unwrap().classified_by,
            ClassifiedBy::Harness as i32
        );
        // Harness OOM report without agent evidence.
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Failed, Some(harness_oom())),
            ..ExitEvidence::default()
        });
        assert_eq!(d.class(), Some(FailureClass::Oom));
    }

    #[test]
    fn xid_is_hardware_and_lease_is_self_fenced() {
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Failed, Some(harness_oom())),
            hw_events: vec![HwEvent::now("GPU-1", HwEventKind::Xid { code: 79 })],
            ..ExitEvidence::default()
        });
        assert_eq!(d.class(), Some(FailureClass::Hardware));
        // A harmless XID is ignored.
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Completed, None),
            hw_events: vec![HwEvent::now("GPU-1", HwEventKind::Xid { code: 13 })],
            ..ExitEvidence::default()
        });
        assert!(d.failure.is_none());
        let d = decide(&ExitEvidence {
            self_fenced: true,
            oom_killed: true,
            ..ExitEvidence::default()
        });
        assert_eq!(d.phase, AttemptPhase::SelfFenced as i32);
        assert_eq!(d.class(), Some(FailureClass::NodeLost));
    }

    #[test]
    fn missing_report_is_harness_error_unless_cancelled() {
        let d = decide(&ExitEvidence {
            signal: Some(9),
            ..ExitEvidence::default()
        });
        assert_eq!(d.class(), Some(FailureClass::HarnessError));
        assert!(d.failure.unwrap().message.contains("signal 9"));
        let d = decide(&ExitEvidence {
            cancel_requested: true,
            ..ExitEvidence::default()
        });
        assert_eq!(d.exit_kind, ExitKind::Cancelled as i32);
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Completed, None),
            needs_final_checkpoint: true,
            ..ExitEvidence::default()
        });
        assert_eq!(d.failure.unwrap().sub_code, "NO_FINAL_CHECKPOINT");
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Completed, None),
            env_mismatch: Some("digest differs".into()),
            ..ExitEvidence::default()
        });
        assert_eq!(d.class(), Some(FailureClass::DependencyEnv));
    }

    #[test]
    fn a_bad_exit_status_contradicts_a_completed_report() {
        let completed = ExitEvidence {
            report: report(ExitKind::Completed, None),
            exit_code: Some(1),
            ..ExitEvidence::default()
        };
        let d = decide(&completed);
        assert_eq!(d.class(), Some(FailureClass::HarnessError));
        assert_eq!(d.failure.unwrap().sub_code, "EXIT_STATUS_MISMATCH");
        let d = decide(&ExitEvidence {
            signal: Some(9),
            exit_code: None,
            ..completed.clone()
        });
        assert!(d.failure.unwrap().message.contains("signal 9"));
        // The agent stopped it (cancel): the report stands.
        let d = decide(&ExitEvidence {
            report: report(ExitKind::Cancelled, None),
            signal: Some(15),
            agent_stopped: true,
            ..ExitEvidence::default()
        });
        assert_eq!(d.exit_kind, ExitKind::Cancelled as i32);
        // Stronger evidence still wins over the status check.
        let d = decide(&ExitEvidence {
            oom_killed: true,
            ..completed.clone()
        });
        assert_eq!(d.class(), Some(FailureClass::Oom));
        // Re-adopted: no status to compare, the report decides.
        let d = decide(&ExitEvidence {
            readopted: true,
            exit_code: None,
            ..completed
        });
        assert!(d.failure.is_none());
    }

    #[test]
    fn decision_round_trips_json() {
        let d = decide(&ExitEvidence::default());
        let back: ExitDecision = serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
        assert_eq!(back, d);
    }
}
