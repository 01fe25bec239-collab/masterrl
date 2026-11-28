"""Plain data the harness hands to an adapter and gets back (Doc 05 §11, adapter API 1.0)."""

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common

Severity = Literal["info", "warning", "error"]


@dataclass(frozen=True)
class Issue:
    """One validation finding: same envelope the UI shows (code, path, message, hint)."""

    code: str
    message: str
    severity: Severity = "error"
    path: str = ""
    hint: str = ""


@dataclass(frozen=True)
class EnvInfo:
    """What the harness knows about the environment before importing anything heavy."""

    platform: str
    python: str
    mrl_runtime_version: str
    environment_digest: str = ""


@dataclass(frozen=True)
class ProbeReport:
    ok: bool
    upstream_versions: Mapping[str, str] = field(default_factory=dict[str, str])
    features: Mapping[str, bool] = field(default_factory=dict[str, bool])
    issues: tuple[Issue, ...] = ()


@dataclass(frozen=True)
class TargetDevice:
    """The machine an estimate is for when the job runs elsewhere (P28's CPU-only dry run
    estimates for the GPU the run will get). From the job spec's `target_device`."""

    kind: Literal["cuda", "mps", "cpu"]
    name: str = ""
    memory_gib: float = 0.0
    bf16: bool = False

    @classmethod
    def from_spec(cls, raw: object) -> "TargetDevice | None":
        if not isinstance(raw, Mapping):
            return None
        d = cast("Mapping[str, object]", raw)
        kinds: dict[object, Literal["cuda", "mps", "cpu"]] = {
            "cuda": "cuda",
            "mps": "mps",
            "cpu": "cpu",
        }
        kind = kinds.get(d.get("kind"))
        if kind is None:
            return None
        memory = d.get("memory_gib", 0.0)
        return cls(
            kind,
            str(d.get("name", "")),
            float(memory) if isinstance(memory, int | float) else 0.0,
            d.get("bf16") is True,
        )


@dataclass(frozen=True)
class HardwareProfile:
    gpu_indices: tuple[int, ...] = ()
    world_size: int = 1
    node_rank: int = 0
    # Estimate for this device instead of the local one (None: the local one).
    target: TargetDevice | None = None


@dataclass(frozen=True)
class ResolvedConfig:
    """The job spec as the harness received it: algorithm, common, engine, pinned switch."""

    algorithm: str
    common: Mapping[str, Any]
    engine: Mapping[str, Any]
    switch: Mapping[str, Any]
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class ValidationRequest:
    config: ResolvedConfig
    staged_paths: Mapping[str, Path]
    hardware: HardwareProfile


@dataclass(frozen=True)
class ValidationReport:
    issues: tuple[Issue, ...] = ()

    @property
    def ok(self) -> bool:
        return not any(i.severity == "error" for i in self.issues)


@dataclass(frozen=True)
class ResourceEstimate:
    """A labelled estimate: ranges plus the assumptions behind them."""

    gpu_mem_gib: tuple[float, float] = (0.0, 0.0)
    total_steps: int = 0
    wall_seconds: tuple[float, float] = (0.0, 0.0)
    assumptions: tuple[str, ...] = ()


@dataclass(frozen=True)
class Allocation:
    gpu_indices: tuple[int, ...] = ()
    world_size: int = 1
    node_rank: int = 0
    role_endpoints: Mapping[str, str] = field(default_factory=dict[str, str])


@dataclass(frozen=True)
class JobContext:
    """Read-only to the adapter. Nothing in it reaches the control plane or holds credentials."""

    attempt_id: str
    run_id: str
    role_id: str
    primary: bool
    config: ResolvedConfig
    staged_paths: Mapping[str, Path]
    resume_from: Path | None
    allocation: Allocation
    staging_dir: Path
    work_dir: Path
    scratch_dir: Path
    dry_run: bool
    smoke_steps: int
    reward_socket: str
    logger: logging.Logger

    @property
    def model_path(self) -> Path | None:
        return self.staged_paths.get("model")


@dataclass(frozen=True)
class PreparedJob:
    """effective_config: the resolved engine config, engine defaults included (rule 1)."""

    effective_config: Mapping[str, Any]
    total_steps: int = 0


@dataclass(frozen=True)
class ExportContext:
    checkpoint_path: Path
    format: str
    output_dir: Path
    config: ResolvedConfig
    logger: logging.Logger


@dataclass(frozen=True)
class ExportResult:
    """Files written under ExportContext.output_dir, as (relative path, role)."""

    format: str
    files: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class FailureInfo:
    """An adapter's classification of a failure. failure_class is a common.v1.FailureClass."""

    failure_class: common.FailureClass
    sub_code: str
    message: str
    retryable: bool = False
