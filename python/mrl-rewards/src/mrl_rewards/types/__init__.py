"""Reward SDK types: what a reward component receives, returns and declares (Doc 06 §1.3.1)."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Protocol, runtime_checkable

type JSONValue = bool | int | float | str | list[JSONValue] | dict[str, JSONValue] | None
type Scalar = bool | int | float | str

# Bumped when the meaning of these types or of the policy runtime changes; part of the bundle hash.
SDK_CONTRACT = "mrl-rewards/1"

# Contract size limits (proto comments in mrl/reward/v1).
MAX_DIAGNOSTICS_BYTES = 4096
MAX_FLAGS = 16


class RewardError(Exception):
    """A reward problem with a catalogued code (guidance/fragments/errors/p14.yaml)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


# Catalogued codes. Titles live in the guidance catalog; messages here add the detail.
POLICY_INVALID = "MRL-RWD-140"
EXPR_INVALID = "MRL-RWD-141"
COMPONENT_UNKNOWN = "MRL-RWD-142"
PARAMS_INVALID = "MRL-RWD-143"
BATCH_FAILED = "MRL-RWD-144"
REQUEST_MISMATCH = "MRL-RWD-145"
USER_CODE_REFUSED = "MRL-RWD-146"
BUNDLE_INVALID = "MRL-RWD-147"
STEP_SIGNAL_UNSUPPORTED = "MRL-RWD-148"
REQUEST_TOO_LARGE = "MRL-VAL-007"


class Status(StrEnum):
    """Per-component and per-sample outcome. Names match mrl.reward.v1.ScoreStatus."""

    OK = "OK"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    INVALID_INPUT = "INVALID_INPUT"


FAILED = frozenset({Status.TIMEOUT, Status.ERROR, Status.INVALID_OUTPUT, Status.INVALID_INPUT})


class Resolution(StrEnum):
    """How a sample's total was decided. Names match mrl.reward.v1.Resolution."""

    SCORED = "SCORED"
    MASKED = "MASKED"
    FALLBACK_APPLIED = "FALLBACK_APPLIED"


class Shape(StrEnum):
    SEQUENCE = "scalar.sequence"
    STEP = "scalar.step"


@dataclass(frozen=True)
class ComponentSpec:
    """What a component declares about itself. The registry key is `<ns>/<name>@<major>`."""

    name: str
    version: str  # semver; the major is part of the registry key
    value_range: tuple[float, float]
    deterministic: bool
    param_schema: Mapping[str, JSONValue]  # JSON Schema (2020-12) for `params`
    description: str
    shape: Shape = Shape.SEQUENCE
    tier: int = 1
    needs: tuple[str, ...] = ()  # sample fields read, e.g. ("reference",)
    stateful: bool = False
    beta: bool = False

    @property
    def major(self) -> int:
        return int(self.version.split(".", 1)[0])


@dataclass(frozen=True)
class RewardSample:
    sample_id: str
    group_id: str
    prompt: str
    completion: str
    completion_token_count: int
    finish_reason: str  # "stop" | "length" | "tool_calls" | "abort"
    reference: JSONValue = None  # decoded reference_json
    episode: JSONValue = None  # decoded episode_json
    policy_version: int = 0  # informational; components must not branch on it


@dataclass(frozen=True)
class RewardBatch:
    samples: Sequence[RewardSample]
    trainer_step: int = 0


def _diag_size(diagnostics: Mapping[str, Scalar]) -> int:
    return sum(len(k.encode()) + len(str(v).encode()) for k, v in diagnostics.items())


@dataclass(frozen=True)
class RewardResult:
    """One component's verdict on one sample. `value=None` means "does not apply"."""

    value: float | None
    status: Status = Status.OK
    diagnostics: Mapping[str, Scalar] = field(default_factory=dict[str, Scalar])
    flags: tuple[str, ...] = ()

    @staticmethod
    def ok(
        value: float, diagnostics: Mapping[str, Scalar] | None = None, *flags: str
    ) -> RewardResult:
        return RewardResult(value, Status.OK, dict(diagnostics or {}), flags)

    @staticmethod
    def not_applicable(reason: str) -> RewardResult:
        return RewardResult(None, Status.NOT_APPLICABLE, {"reason": reason})

    @staticmethod
    def invalid_input(reason: str) -> RewardResult:
        return RewardResult(None, Status.INVALID_INPUT, {"reason": reason})

    @staticmethod
    def failed(status: Status, reason: str, *flags: str) -> RewardResult:
        return RewardResult(None, status, {"reason": reason[:512]}, flags)

    def bounded(self) -> RewardResult:
        """Diagnostics cut to 4 KiB and flags to 16; flags `diagnostics_truncated` when cut."""
        flags = self.flags[:MAX_FLAGS]
        if _diag_size(self.diagnostics) <= MAX_DIAGNOSTICS_BYTES and flags == self.flags:
            return self
        kept: dict[str, Scalar] = {}
        budget = MAX_DIAGNOSTICS_BYTES
        for k, v in self.diagnostics.items():
            size = _diag_size({k: v})
            if size > budget:
                break
            kept[k] = v
            budget -= size
        if len(kept) < len(self.diagnostics):
            flags = (*flags[: MAX_FLAGS - 1], "diagnostics_truncated")
        return RewardResult(self.value, self.status, kept, flags)


def is_finite_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


@dataclass(frozen=True)
class RewardContext:
    """Given to `setup()` once per sandbox start."""

    params: Mapping[str, JSONValue]
    bundle_dir: Path | None = None  # read-only user bundle, when the policy has one
    seed: int = 0


@runtime_checkable
class RewardProvider(Protocol):
    """One reward component. Runs only inside mrl-reward-exec."""

    spec: ComponentSpec

    def setup(self, ctx: RewardContext) -> None: ...

    def score(self, batch: RewardBatch) -> list[RewardResult]:
        """Return one result per sample, same length and order."""
        ...

    def teardown(self) -> None: ...


__all__ = [
    "BATCH_FAILED",
    "BUNDLE_INVALID",
    "COMPONENT_UNKNOWN",
    "EXPR_INVALID",
    "FAILED",
    "MAX_DIAGNOSTICS_BYTES",
    "MAX_FLAGS",
    "PARAMS_INVALID",
    "POLICY_INVALID",
    "REQUEST_MISMATCH",
    "REQUEST_TOO_LARGE",
    "SDK_CONTRACT",
    "STEP_SIGNAL_UNSUPPORTED",
    "USER_CODE_REFUSED",
    "ComponentSpec",
    "JSONValue",
    "Resolution",
    "RewardBatch",
    "RewardContext",
    "RewardError",
    "RewardProvider",
    "RewardResult",
    "RewardSample",
    "Scalar",
    "Shape",
    "Status",
    "is_finite_number",
]
