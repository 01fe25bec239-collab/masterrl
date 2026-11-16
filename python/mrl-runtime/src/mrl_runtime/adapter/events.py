"""Events an adapter's run() yields (Doc 05 §11.5). The harness maps them to Worker Protocol."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

CheckpointKind = Literal["full_resume", "weights_only", "adapter_only"]


@dataclass(frozen=True)
class Progress:
    step: int
    total_steps: int | None = None
    epoch: float | None = None
    phase: str = "train"
    tokens_seen: int | None = None


@dataclass(frozen=True)
class Metrics:
    """Engine-side names; the harness maps them to canonical names from the manifest."""

    step: int
    values: Mapping[str, float]
    split: str = "train"


@dataclass(frozen=True)
class Log:
    message: str
    level: Literal["debug", "info", "warning", "error"] = "info"


@dataclass(frozen=True)
class EngineWarning:
    message: str


@dataclass(frozen=True)
class RolloutSample:
    sample_id: str
    prompt: str
    completion: str
    group_id: str = ""
    reward_total: float = 0.0
    reward_components: Mapping[str, float] = field(default_factory=dict[str, float])
    truncated: bool = False
    completion_tokens: int = 0
    policy_version: int = 0


@dataclass(frozen=True)
class Samples:
    """Sampled and size-capped by the adapter; the harness redacts and overflows to files."""

    step: int
    items: tuple[RolloutSample, ...]


@dataclass(frozen=True)
class AdapterInfo:
    """LoRA-style adapter details, required for adapter_only checkpoints."""

    method: Literal["lora", "qlora", "dora"]
    rank: int
    alpha: float
    target_modules: tuple[str, ...]


@dataclass(frozen=True)
class CheckpointReady:
    """The engine finished writing a checkpoint directory. The harness stages and commits it."""

    step: int
    path: Path
    kind: CheckpointKind = "full_resume"
    is_final: bool = False
    metrics: Mapping[str, float] = field(default_factory=dict[str, float])
    adapter: AdapterInfo | None = None
    policy_version: int | None = None


EngineEvent = Progress | Metrics | Log | EngineWarning | Samples | CheckpointReady
