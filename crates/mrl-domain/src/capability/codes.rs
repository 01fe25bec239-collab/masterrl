//! Capability rejection codes (`MRL-CAP-001` to `MRL-CAP-023`).
//!
//! A rejection means the chosen engine, method, model, dataset and hardware don't work together.

use crate::errors::{ErrorCode, ErrorFamily};

/// Why a run setup was rejected before it was queued. The number is the `MRL-CAP-NNN` code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u16)]
pub enum CapabilityRejection {
    /// No engine here supports this method.
    NoEngineForMethod = 1,
    /// PPO is not available with the TRL engine.
    PpoUnavailableOnTrl = 2,
    /// DPO needs preference pairs, but the dataset has conversations.
    DpoNeedsPreferencePairs = 3,
    /// GRPO needs a reward, but the run has none.
    GrpoNeedsReward = 4,
    /// KTO needs a batch size of at least 2.
    KtoBatchTooSmall = 5,
    /// DPO with LoRA can't also use reference syncing.
    DpoLoraWithRefSync = 6,
    /// The output head is trained fully instead of with LoRA.
    OutputHeadTrainedFully = 7,
    /// GRPO server mode needs a second GPU.
    GrpoServerNeedsSecondGpu = 8,
    /// The GRPO batch size must divide evenly by samples per prompt.
    GrpoBatchNotDivisible = 9,
    /// This GRPO setup needs colocated mode.
    GrpoNeedsColocated = 10,
    /// DAPO dynamic sampling is not available with TRL.
    DapoDynamicSamplingOnTrl = 11,
    /// The field can't change right now.
    FieldLocked = 12,
    /// Async mode works only with FSDP2 and text data.
    AsyncNeedsFsdp2Text = 13,
    /// The method can't train an encoder-only model.
    EncoderOnlyModel = 14,
    /// Workspace rules block this model.
    ModelBlockedByPolicy = 15,
    /// The model has no chat template for conversation data.
    NoChatTemplate = 16,
    /// A streaming dataset needs a step limit.
    StreamingNeedsStepLimit = 17,
    /// No engine setup matches this hardware.
    NoEngineForHardware = 18,
    /// The method runs on one machine only.
    SingleMachineOnly = 19,
    /// The checkpoint came from a different engine or format.
    CheckpointFormatMismatch = 20,
    /// The engine can't pause.
    PauseUnsupported = 21,
    /// The method is experimental and turned off.
    ExperimentalDisabled = 22,
    /// The reward needs token log-probabilities the rollout engine doesn't return.
    RewardNeedsLogprobs = 23,
}

/// `MRL-CAP-NNN` for each rejection, in `ALL` order. Built at compile time, so a bad
/// number stops the build.
const CODES: [ErrorCode; 23] = {
    let mut codes = [ErrorCode::known(ErrorFamily::Cap, 1); 23];
    let mut i = 0;
    while i < codes.len() {
        codes[i] = ErrorCode::known(ErrorFamily::Cap, CapabilityRejection::ALL[i] as u16);
        i += 1;
    }
    codes
};

impl CapabilityRejection {
    /// Every rejection, in code order.
    pub const ALL: [Self; 23] = [
        Self::NoEngineForMethod,
        Self::PpoUnavailableOnTrl,
        Self::DpoNeedsPreferencePairs,
        Self::GrpoNeedsReward,
        Self::KtoBatchTooSmall,
        Self::DpoLoraWithRefSync,
        Self::OutputHeadTrainedFully,
        Self::GrpoServerNeedsSecondGpu,
        Self::GrpoBatchNotDivisible,
        Self::GrpoNeedsColocated,
        Self::DapoDynamicSamplingOnTrl,
        Self::FieldLocked,
        Self::AsyncNeedsFsdp2Text,
        Self::EncoderOnlyModel,
        Self::ModelBlockedByPolicy,
        Self::NoChatTemplate,
        Self::StreamingNeedsStepLimit,
        Self::NoEngineForHardware,
        Self::SingleMachineOnly,
        Self::CheckpointFormatMismatch,
        Self::PauseUnsupported,
        Self::ExperimentalDisabled,
        Self::RewardNeedsLogprobs,
    ];

    /// The error code, like `MRL-CAP-004`.
    pub const fn code(self) -> ErrorCode {
        // ponytail: discriminants run 1..=23 in `ALL` order, so the index is always in range.
        CODES[self as usize - 1]
    }

    /// The rejection for a code, if it is an `MRL-CAP` code.
    pub fn from_code(code: ErrorCode) -> Option<Self> {
        Self::ALL.iter().copied().find(|r| r.code() == code)
    }

    /// The plain-language title shown to the user (same as the guidance catalog).
    pub const fn title(self) -> &'static str {
        match self {
            Self::NoEngineForMethod => "No engine here supports this method.",
            Self::PpoUnavailableOnTrl => "PPO is not available with the TRL engine.",
            Self::DpoNeedsPreferencePairs => "DPO needs preference pairs, but this dataset has conversations.",
            Self::GrpoNeedsReward => "GRPO needs a reward, but this run has none.",
            Self::KtoBatchTooSmall => "KTO needs a batch size of at least 2.",
            Self::DpoLoraWithRefSync => "DPO with LoRA can't also use reference syncing.",
            Self::OutputHeadTrainedFully => "The app trains the output head fully instead of with LoRA.",
            Self::GrpoServerNeedsSecondGpu => "GRPO server mode needs a second GPU.",
            Self::GrpoBatchNotDivisible => "The GRPO batch size must divide evenly by samples per prompt.",
            Self::GrpoNeedsColocated => "This GRPO setup needs colocated mode.",
            Self::DapoDynamicSamplingOnTrl => "DAPO dynamic sampling is not available with TRL.",
            Self::FieldLocked => "You can't change this field right now.",
            Self::AsyncNeedsFsdp2Text => "Async mode works only with FSDP2 and text data.",
            Self::EncoderOnlyModel => "This method can't train an encoder-only model.",
            Self::ModelBlockedByPolicy => "Your workspace rules block this model.",
            Self::NoChatTemplate => "This model has no chat template for conversation data.",
            Self::StreamingNeedsStepLimit => "A streaming dataset needs a step limit.",
            Self::NoEngineForHardware => "No engine setup here matches this hardware.",
            Self::SingleMachineOnly => "This method runs on one machine only.",
            Self::CheckpointFormatMismatch => "This checkpoint came from a different engine or format.",
            Self::PauseUnsupported => "This engine can't pause.",
            Self::ExperimentalDisabled => "This method is experimental and turned off.",
            Self::RewardNeedsLogprobs => {
                "This reward needs token log-probabilities, but the rollout engine doesn't return them."
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_one_to_twenty_three_in_order() {
        for (i, r) in CapabilityRejection::ALL.iter().enumerate() {
            assert_eq!(r.code().to_string(), format!("MRL-CAP-{:03}", i + 1));
            assert_eq!(CapabilityRejection::from_code(r.code()), Some(*r));
        }
        assert_eq!(
            ErrorCode::new(ErrorFamily::Cap, 24).and_then(CapabilityRejection::from_code),
            None
        );
        assert_eq!(
            ErrorCode::new(ErrorFamily::Val, 1).and_then(CapabilityRejection::from_code),
            None
        );
    }
}
