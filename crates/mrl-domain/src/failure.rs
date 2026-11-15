//! Failure classes, sub-codes and the default retry rules.
//!
//! The numbers match the protobuf `FailureClass` enum. Number 0 is `UNSPECIFIED` there
//! and has no variant here: every failure has a class.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// What kind of problem ended an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum FailureClass {
    /// `USER_CONFIG`: settings that don't work together.
    UserConfig = 1,
    /// `DATA`: a problem in the dataset.
    Data = 2,
    /// `OOM`: memory ran out.
    Oom = 3,
    /// `NODE_LOST`: contact with a machine was lost.
    NodeLost = 4,
    /// `HARDWARE`: a GPU or another part failed.
    Hardware = 5,
    /// `INFRA_TRANSIENT`: a short network or storage problem.
    InfraTransient = 6,
    /// `ENGINE_ERROR`: the training engine stopped with an error.
    EngineError = 7,
    /// `SANDBOX_VIOLATION`: the sandbox stopped reward code.
    SandboxViolation = 8,
    /// `PREEMPTED`: the app freed the machine for other work.
    Preempted = 9,
    /// `CANCELLED_BY_USER`: someone stopped the run.
    CancelledByUser = 10,
    /// `VERIFICATION_FAILED`: the result did not pass its final checks.
    VerificationFailed = 11,
    /// `REWARD_INFRA`: the reward service is not working.
    RewardInfra = 12,
    /// `DEPENDENCY_ENV`: needed software is missing or out of date.
    DependencyEnv = 13,
    /// `MODEL_INCOMPATIBLE`: the model can't be used with this method.
    ModelIncompatible = 14,
    /// `NUMERIC_DIVERGENCE`: training numbers went out of range.
    NumericDivergence = 15,
    /// `STALLED`: the run stopped making progress.
    Stalled = 16,
    /// `HARNESS_ERROR`: an internal problem in the app's job harness.
    HarnessError = 17,
    /// `SAFETY_HALT`: a safety rule stopped the run.
    SafetyHalt = 18,
}

impl FailureClass {
    /// Every class, in protobuf order.
    pub const ALL: [Self; 18] = [
        Self::UserConfig,
        Self::Data,
        Self::Oom,
        Self::NodeLost,
        Self::Hardware,
        Self::InfraTransient,
        Self::EngineError,
        Self::SandboxViolation,
        Self::Preempted,
        Self::CancelledByUser,
        Self::VerificationFailed,
        Self::RewardInfra,
        Self::DependencyEnv,
        Self::ModelIncompatible,
        Self::NumericDivergence,
        Self::Stalled,
        Self::HarnessError,
        Self::SafetyHalt,
    ];

    /// The protobuf number.
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    /// The class for a protobuf number; `None` for 0 (`UNSPECIFIED`) and unknown numbers.
    pub fn from_i32(n: i32) -> Option<Self> {
        Self::ALL.iter().copied().find(|c| c.as_i32() == n)
    }

    /// The wire name, like `OOM`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserConfig => "USER_CONFIG",
            Self::Data => "DATA",
            Self::Oom => "OOM",
            Self::NodeLost => "NODE_LOST",
            Self::Hardware => "HARDWARE",
            Self::InfraTransient => "INFRA_TRANSIENT",
            Self::EngineError => "ENGINE_ERROR",
            Self::SandboxViolation => "SANDBOX_VIOLATION",
            Self::Preempted => "PREEMPTED",
            Self::CancelledByUser => "CANCELLED_BY_USER",
            Self::VerificationFailed => "VERIFICATION_FAILED",
            Self::RewardInfra => "REWARD_INFRA",
            Self::DependencyEnv => "DEPENDENCY_ENV",
            Self::ModelIncompatible => "MODEL_INCOMPATIBLE",
            Self::NumericDivergence => "NUMERIC_DIVERGENCE",
            Self::Stalled => "STALLED",
            Self::HarnessError => "HARNESS_ERROR",
            Self::SafetyHalt => "SAFETY_HALT",
        }
    }

    /// The default retry rule for this class (a job may override it).
    pub const fn retry_policy_default(self) -> RetryPolicy {
        use Backoff::{Exponential, Fixed};
        const fn secs(s: u64) -> Duration {
            Duration::from_secs(s)
        }
        match self {
            Self::UserConfig
            | Self::Data
            | Self::SandboxViolation
            | Self::VerificationFailed
            | Self::DependencyEnv
            | Self::ModelIncompatible
            | Self::NumericDivergence
            | Self::SafetyHalt
            | Self::CancelledByUser => RetryPolicy::NEVER,
            Self::Oom => RetryPolicy {
                requires_auto_tune: true,
                ..RetryPolicy::limited(2, Backoff::None)
            },
            Self::NodeLost => RetryPolicy {
                exclude_node: true,
                ..RetryPolicy::limited(
                    3,
                    Exponential {
                        base: secs(30),
                        cap: None,
                    },
                )
            },
            Self::Hardware => RetryPolicy {
                exclude_node: true,
                ..RetryPolicy::limited(2, Fixed(secs(60)))
            },
            Self::InfraTransient => RetryPolicy::limited(
                5,
                Exponential {
                    base: secs(10),
                    cap: Some(secs(600)),
                },
            ),
            Self::EngineError => RetryPolicy::limited(1, Fixed(secs(60))),
            Self::RewardInfra => RetryPolicy::limited(2, Fixed(secs(30))),
            Self::Preempted => RetryPolicy {
                budget: RetryBudget::Unlimited,
                backoff: Backoff::None,
                exclude_node: false,
                requires_auto_tune: false,
                consumes_budget: false,
            },
            Self::Stalled | Self::HarnessError => RetryPolicy::limited(1, Backoff::None),
        }
    }

    /// True if a job may retry after this class by default. `OOM` retries only with `auto_tune`.
    pub const fn is_retryable(self, auto_tune: bool) -> bool {
        self.retry_policy_default().is_retryable(auto_tune)
    }

    /// True if a retry after this class counts against the job's retry budget.
    pub const fn consumes_budget(self) -> bool {
        self.retry_policy_default().consumes_budget
    }
}

impl fmt::Display for FailureClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for FailureClass {
    type Err = FailureCodeError;

    fn from_str(s: &str) -> Result<Self, FailureCodeError> {
        Self::ALL
            .iter()
            .copied()
            .find(|c| c.as_str() == s)
            .ok_or_else(|| FailureCodeError::UnknownClass(s.to_owned()))
    }
}

/// How many retries a class allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryBudget {
    /// At most this many retries (0 = never retry).
    Limited(u32),
    /// No limit.
    Unlimited,
}

/// How long to wait before a retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    /// Retry at once.
    None,
    /// Always wait this long.
    Fixed(Duration),
    /// Wait `base × 2ⁿ` before retry number n (counting from 0), up to `cap` if set.
    Exponential {
        /// Wait before the first retry.
        base: Duration,
        /// Longest wait.
        cap: Option<Duration>,
    },
}

impl Backoff {
    /// The wait before retry number `n` (0 = first retry). Saturates instead of overflowing.
    pub fn delay(self, n: u32) -> Duration {
        match self {
            Self::None => Duration::ZERO,
            Self::Fixed(d) => d,
            Self::Exponential { base, cap } => {
                let raw = base.saturating_mul(1u32.checked_shl(n).unwrap_or(u32::MAX));
                cap.map_or(raw, |c| raw.min(c))
            }
        }
    }
}

/// The retry rule for one failure class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// How many retries are allowed.
    pub budget: RetryBudget,
    /// Wait before each retry.
    pub backoff: Backoff,
    /// Place the retry on a different node than the one that failed.
    pub exclude_node: bool,
    /// Retry only when the job allows automatic setting changes (`auto_tune`).
    pub requires_auto_tune: bool,
    /// The retry counts against the job's retry budget.
    pub consumes_budget: bool,
}

impl RetryPolicy {
    /// Never retry.
    pub const NEVER: Self = Self::limited(0, Backoff::None);

    /// Up to `n` retries with `backoff`, counted against the budget.
    pub const fn limited(n: u32, backoff: Backoff) -> Self {
        Self {
            budget: RetryBudget::Limited(n),
            backoff,
            exclude_node: false,
            requires_auto_tune: false,
            consumes_budget: true,
        }
    }

    /// True if this rule allows any retry.
    pub const fn is_retryable(&self, auto_tune: bool) -> bool {
        let has_budget = !matches!(self.budget, RetryBudget::Limited(0));
        has_budget && (auto_tune || !self.requires_auto_tune)
    }

    /// The wait before the next retry, or `None` if no retry is allowed.
    /// `retries_used` counts earlier retries of this class that consumed budget.
    pub fn next_delay(&self, retries_used: u32, auto_tune: bool) -> Option<Duration> {
        if !self.is_retryable(auto_tune) {
            return None;
        }
        match self.budget {
            RetryBudget::Limited(max) if retries_used >= max => None,
            _ => Some(self.backoff.delay(retries_used)),
        }
    }
}

/// A finer reason inside a failure class, written `CLASS/SUB`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SubCode {
    /// `OOM/OOM_GPU`: the GPU ran out of memory.
    OomGpu,
    /// `OOM/OOM_HOST`: the computer ran out of memory.
    OomHost,
    /// `INFRA_TRANSIENT/SHM_EXHAUSTED`: shared memory (`/dev/shm`) ran out.
    ShmExhausted,
    /// `INFRA_TRANSIENT/DISK_FULL`: the scratch disk is full.
    DiskFull,
    /// `HARDWARE/GPU_CONTENDED`: another program is using the GPU.
    GpuContended,
    /// `NODE_LOST/NODE_REBOOTED`: the machine restarted.
    NodeRebooted,
}

impl SubCode {
    /// Every sub-code.
    pub const ALL: [Self; 6] = [
        Self::OomGpu,
        Self::OomHost,
        Self::ShmExhausted,
        Self::DiskFull,
        Self::GpuContended,
        Self::NodeRebooted,
    ];

    /// The class this sub-code belongs to.
    pub const fn class(self) -> FailureClass {
        match self {
            Self::OomGpu | Self::OomHost => FailureClass::Oom,
            Self::ShmExhausted | Self::DiskFull => FailureClass::InfraTransient,
            Self::GpuContended => FailureClass::Hardware,
            Self::NodeRebooted => FailureClass::NodeLost,
        }
    }

    /// The wire name without the class, like `OOM_GPU`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OomGpu => "OOM_GPU",
            Self::OomHost => "OOM_HOST",
            Self::ShmExhausted => "SHM_EXHAUSTED",
            Self::DiskFull => "DISK_FULL",
            Self::GpuContended => "GPU_CONTENDED",
            Self::NodeRebooted => "NODE_REBOOTED",
        }
    }
}

/// Why a failure code could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FailureCodeError {
    /// No class has this name.
    #[error("`{0}` is not a failure class the app knows")]
    UnknownClass(String),
    /// No sub-code has this name.
    #[error("`{0}` is not a failure sub-code the app knows")]
    UnknownSubCode(String),
    /// The sub-code belongs to a different class.
    #[error("the sub-code `{sub}` belongs to `{expected}`, not `{found}`")]
    WrongClass {
        /// The sub-code name.
        sub: &'static str,
        /// The class it belongs to.
        expected: FailureClass,
        /// The class that was given.
        found: FailureClass,
    },
}

/// A failure class with an optional sub-code: `OOM` or `OOM/OOM_GPU`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FailureCode {
    class: FailureClass,
    sub: Option<SubCode>,
}

impl FailureCode {
    /// A code with only a class.
    pub const fn class_only(class: FailureClass) -> Self {
        Self { class, sub: None }
    }

    /// A code from a sub-code; the class comes from the sub-code.
    pub const fn from_sub(sub: SubCode) -> Self {
        Self {
            class: sub.class(),
            sub: Some(sub),
        }
    }

    /// The failure class.
    pub const fn class(&self) -> FailureClass {
        self.class
    }

    /// The sub-code, if any.
    pub const fn sub(&self) -> Option<SubCode> {
        self.sub
    }
}

impl From<FailureClass> for FailureCode {
    fn from(class: FailureClass) -> Self {
        Self::class_only(class)
    }
}

impl From<SubCode> for FailureCode {
    fn from(sub: SubCode) -> Self {
        Self::from_sub(sub)
    }
}

impl fmt::Display for FailureCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.sub {
            Some(sub) => write!(f, "{}/{}", self.class, sub.as_str()),
            None => write!(f, "{}", self.class),
        }
    }
}

impl FromStr for FailureCode {
    type Err = FailureCodeError;

    fn from_str(s: &str) -> Result<Self, FailureCodeError> {
        let Some((class, sub)) = s.split_once('/') else {
            return s.parse().map(Self::class_only);
        };
        let class: FailureClass = class.parse()?;
        let sub = SubCode::ALL
            .iter()
            .copied()
            .find(|c| c.as_str() == sub)
            .ok_or_else(|| FailureCodeError::UnknownSubCode(sub.to_owned()))?;
        if sub.class() != class {
            return Err(FailureCodeError::WrongClass {
                sub: sub.as_str(),
                expected: sub.class(),
                found: class,
            });
        }
        Ok(Self::from_sub(sub))
    }
}

impl serde::Serialize for FailureCode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for FailureCode {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn numbers_match_the_protobuf() {
        let numbered: Vec<(i32, &str)> = FailureClass::ALL
            .iter()
            .map(|c| (c.as_i32(), c.as_str()))
            .collect();
        let want = [
            "USER_CONFIG",
            "DATA",
            "OOM",
            "NODE_LOST",
            "HARDWARE",
            "INFRA_TRANSIENT",
            "ENGINE_ERROR",
            "SANDBOX_VIOLATION",
            "PREEMPTED",
            "CANCELLED_BY_USER",
            "VERIFICATION_FAILED",
            "REWARD_INFRA",
            "DEPENDENCY_ENV",
            "MODEL_INCOMPATIBLE",
            "NUMERIC_DIVERGENCE",
            "STALLED",
            "HARNESS_ERROR",
            "SAFETY_HALT",
        ];
        let want: Vec<(i32, &str)> = (1..).zip(want).collect();
        assert_eq!(numbered, want);
        assert_eq!(FailureClass::from_i32(0), None);
        assert_eq!(FailureClass::from_i32(19), None);
        assert_eq!(FailureClass::from_i32(18), Some(FailureClass::SafetyHalt));
    }

    #[test]
    fn retry_table() {
        use FailureClass::*;
        let s = Duration::from_secs;
        for c in [
            UserConfig,
            Data,
            SandboxViolation,
            VerificationFailed,
            DependencyEnv,
            ModelIncompatible,
            NumericDivergence,
            SafetyHalt,
            CancelledByUser,
        ] {
            assert!(!c.is_retryable(true), "{c}");
            assert_eq!(c.retry_policy_default().next_delay(0, true), None, "{c}");
        }

        assert!(!Oom.is_retryable(false));
        assert!(Oom.is_retryable(true));
        let oom = Oom.retry_policy_default();
        assert_eq!(
            [0, 1, 2].map(|n| oom.next_delay(n, true)),
            [Some(s(0)), Some(s(0)), None]
        );

        let lost = NodeLost.retry_policy_default();
        assert!(lost.exclude_node);
        assert_eq!(
            [0, 1, 2, 3].map(|n| lost.next_delay(n, false)),
            [Some(s(30)), Some(s(60)), Some(s(120)), None]
        );

        let hw = Hardware.retry_policy_default();
        assert!(hw.exclude_node);
        assert_eq!(
            [0, 1, 2].map(|n| hw.next_delay(n, false)),
            [Some(s(60)), Some(s(60)), None]
        );

        let infra = InfraTransient.retry_policy_default();
        assert!(!infra.exclude_node);
        assert_eq!(
            [0, 1, 4, 5].map(|n| infra.next_delay(n, false)),
            [Some(s(10)), Some(s(20)), Some(s(160)), None]
        );
        assert_eq!(infra.backoff.delay(6), s(600), "capped at 10 minutes");

        assert_eq!(
            EngineError.retry_policy_default().next_delay(0, false),
            Some(s(60))
        );
        assert_eq!(
            EngineError.retry_policy_default().next_delay(1, false),
            None
        );
        assert_eq!(
            RewardInfra.retry_policy_default().next_delay(1, false),
            Some(s(30))
        );
        assert_eq!(
            RewardInfra.retry_policy_default().next_delay(2, false),
            None
        );
        for c in [Stalled, HarnessError] {
            assert_eq!(c.retry_policy_default().next_delay(0, false), Some(s(0)));
            assert_eq!(c.retry_policy_default().next_delay(1, false), None);
        }

        let pre = Preempted.retry_policy_default();
        assert_eq!(pre.next_delay(u32::MAX, false), Some(s(0)));
        assert!(!Preempted.consumes_budget());
        assert!(FailureClass::ALL
            .iter()
            .filter(|&&c| c != Preempted)
            .all(|c| c.consumes_budget()));
    }

    #[test]
    fn exponential_backoff_saturates() {
        let b = Backoff::Exponential {
            base: Duration::from_secs(30),
            cap: None,
        };
        assert_eq!(
            b.delay(40),
            Duration::from_secs(30).saturating_mul(u32::MAX)
        );
        assert_eq!(
            Backoff::Exponential {
                base: Duration::MAX,
                cap: None
            }
            .delay(3),
            Duration::MAX
        );
    }

    #[test]
    fn codes_parse_and_display() {
        assert_eq!(
            "OOM".parse(),
            Ok(FailureCode::class_only(FailureClass::Oom))
        );
        assert_eq!(
            "OOM/OOM_GPU".parse(),
            Ok(FailureCode::from_sub(SubCode::OomGpu))
        );
        assert_eq!(
            FailureCode::from(SubCode::DiskFull).to_string(),
            "INFRA_TRANSIENT/DISK_FULL"
        );
        assert_eq!(
            FailureCode::from(FailureClass::Stalled).to_string(),
            "STALLED"
        );
        assert_eq!(
            FailureCode::from(SubCode::NodeRebooted).class(),
            FailureClass::NodeLost
        );
        assert_eq!(
            FailureCode::from(SubCode::GpuContended).sub(),
            Some(SubCode::GpuContended)
        );
        assert_eq!(
            "OOM/OOM_GPU".parse::<FailureCode>().map(|c| c.to_string()),
            Ok("OOM/OOM_GPU".into())
        );
        assert_eq!(
            "NOPE".parse::<FailureCode>(),
            Err(FailureCodeError::UnknownClass("NOPE".into()))
        );
        assert_eq!(
            "OOM/NOPE".parse::<FailureCode>(),
            Err(FailureCodeError::UnknownSubCode("NOPE".into()))
        );
        let wrong = "HARDWARE/OOM_GPU"
            .parse::<FailureCode>()
            .map_err(|e| e.to_string());
        assert_eq!(
            wrong,
            Err("the sub-code `OOM_GPU` belongs to `OOM`, not `HARDWARE`".into())
        );
    }

    #[test]
    fn shm_exhausted_is_infra_transient() {
        let shm = FailureCode::from(SubCode::ShmExhausted);
        assert_eq!(shm.to_string(), "INFRA_TRANSIENT/SHM_EXHAUSTED");
        assert_eq!("INFRA_TRANSIENT/SHM_EXHAUSTED".parse(), Ok(shm));
        let old = "OOM/SHM_EXHAUSTED".parse::<FailureCode>();
        assert_eq!(
            old,
            Err(FailureCodeError::WrongClass {
                sub: "SHM_EXHAUSTED",
                expected: FailureClass::InfraTransient,
                found: FailureClass::Oom
            })
        );
        // Retries like other short system problems: no auto_tune needed, 5 tries, backoff from 10 s.
        let policy = shm.class().retry_policy_default();
        assert!(shm.class().is_retryable(false));
        assert_eq!(policy.next_delay(0, false), Some(Duration::from_secs(10)));
        assert_eq!(policy.next_delay(5, false), None);
    }

    #[test]
    fn codes_serialize_as_text() {
        let code = FailureCode::from(SubCode::OomHost);
        assert_eq!(
            serde_json::to_string(&code).ok(),
            Some("\"OOM/OOM_HOST\"".into())
        );
        let back: Result<FailureCode, _> = serde_json::from_str("\"OOM/OOM_HOST\"");
        assert_eq!(back.ok(), Some(code));
        assert!(serde_json::from_str::<FailureCode>("\"OOM/DISK_FULL\"").is_err());
    }

    proptest! {
        #[test]
        fn every_code_round_trips(i in 0usize..FailureClass::ALL.len(), j in 0usize..=SubCode::ALL.len()) {
            let code = SubCode::ALL.get(j).map_or(FailureCode::from(FailureClass::ALL[i]), |&s| FailureCode::from(s));
            prop_assert_eq!(code.to_string().parse::<FailureCode>(), Ok(code));
            prop_assert_eq!(FailureClass::from_i32(FailureClass::ALL[i].as_i32()), Some(FailureClass::ALL[i]));
        }

        #[test]
        fn limited_budgets_stop(c in 0usize..FailureClass::ALL.len(), used in 0u32..20, auto in any::<bool>()) {
            let p = FailureClass::ALL[c].retry_policy_default();
            if let RetryBudget::Limited(max) = p.budget {
                if used >= max {
                    prop_assert_eq!(p.next_delay(used, auto), None);
                }
            }
            prop_assert_eq!(p.next_delay(0, auto).is_some(), p.is_retryable(auto));
        }
    }
}
