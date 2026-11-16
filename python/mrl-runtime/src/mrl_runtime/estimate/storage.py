"""Checkpoint storage (Doc 07 §6.7), interconnect need (§6.8 with the §5.7 tier table) and
scratch / shared-memory outputs (§6.9)."""

from dataclasses import dataclass
from typing import Literal

from mrl_runtime.estimate.units import GB, Range

type Tier = Literal["supported", "experimental", "rejected"]

# Effective bandwidth per interconnect class in GB/s, order of magnitude only (Doc 07 §5.7:
# 100 GbE ≈ 12 GB/s ideal). NVLink and PCIe figures are typical per-GPU rates.
BANDWIDTH_GB_S = {
    "nvlink": 300.0,
    "pcie": 25.0,
    "rdma_100g": 12.0,
    "eth_25g": 3.0,
    "eth_slow": 1.25,
}
IMAGE_GB = (10.0, 15.0, 25.0)  # CUDA images / engine environments
LOGS_GB = (0.5, 1.0, 2.0)
SMALL_STATE_GB = 0.01  # scheduler, RNG, scaler, manifest
LORA_SIZED_GB = 1.0  # gradients this small tolerate eth_25g between nodes


@dataclass(frozen=True)
class TrainedModel:
    """One model whose weights and optimizer state a checkpoint saves."""

    saved_params: int  # full model, or adapter params for LoRA
    trainable_params: int
    optimizer_bytes: tuple[float, float, float]  # per trainable param (low, expected, high)
    save_bytes: tuple[float, float, float]  # per saved param


@dataclass(frozen=True)
class Storage:
    """Disk outputs in GB. `monthly_gb` needs a checkpoint cadence."""

    checkpoint_weights_gb: Range  # policy weights only (export / eval)
    checkpoint_resumable_gb: Range  # every trained model's weights + optimizer state
    retention_gb: Range  # keep_last x resumable + keep_best x weights only
    monthly_gb: Range | None
    scratch_gb: Range
    shm_gib: float


@dataclass(frozen=True)
class InterconnectNeed:
    """What one role moves between devices per step and which link classes can carry it."""

    role: str
    parallelism: str
    gb_per_step: float | None
    classes: dict[str, Tier]
    preferred: str | None
    seconds_per_step: dict[str, float]


@dataclass(frozen=True)
class StoragePlan:
    """Inputs for `storage()` that come from the run's settings."""

    keep_last: int
    keep_best: int
    checkpoints_per_month: float | None
    dataset_gb: float
    model_files_gb: float
    dataloader_workers: int
    system_ram_gib: float | None


def checkpoint(models: list[TrainedModel]) -> tuple[Range, Range]:
    """(weights only of the first model, resumable for all), in GB."""
    policy = models[0]
    weights = Range.of_bytes(*(policy.saved_params * b for b in policy.save_bytes), unit=GB)
    resumable = Range.exact(SMALL_STATE_GB)
    for m in models:
        resumable = resumable + Range.of_bytes(*(m.saved_params * b for b in m.save_bytes), unit=GB)
        resumable = resumable + Range.of_bytes(
            *(m.trainable_params * b for b in m.optimizer_bytes), unit=GB
        )
    return weights, resumable


def storage(models: list[TrainedModel], plan: StoragePlan) -> Storage:
    weights, resumable = checkpoint(models)
    retention = resumable.scale(plan.keep_last) + weights.scale(plan.keep_best)
    monthly = (
        None if plan.checkpoints_per_month is None else resumable.scale(plan.checkpoints_per_month)
    )
    scratch = (
        Range.exact(plan.model_files_gb + plan.dataset_gb)
        + resumable.scale(2)  # staging + upload retry
        + Range(*IMAGE_GB)
        + Range(*LOGS_GB)
    )
    shm = max(8.0, 2.0 * plan.dataloader_workers)
    if plan.system_ram_gib is not None:
        shm = min(shm, plan.system_ram_gib)
    return Storage(weights, resumable, retention, monthly, scratch, shm)


def _seconds(gb: float, classes: dict[str, Tier]) -> dict[str, float]:
    return {c: round(gb / BANDWIDTH_GB_S[c], 3) for c in classes}


def trainer_link(
    *, devices: int, nodes: int, sharding: str, full_params_gb: float, gradients_gb: float
) -> InterconnectNeed | None:
    """The trainer's need: FSDP/ZeRO-3 moves ≈ 3 x P x b_w per step, DDP ≈ P_t x b."""
    if devices * nodes <= 1:
        return None
    gathers = sharding in ("zero3", "fsdp")
    sharded = gathers or sharding == "zero2"
    moved = 3 * full_params_gb if gathers else gradients_gb
    classes: dict[str, Tier]
    if nodes == 1:
        classes = {"nvlink": "supported", "pcie": "experimental" if sharded else "supported"}
        preferred = "nvlink" if sharded else None
    elif sharded or moved >= LORA_SIZED_GB:
        classes = {"rdma_100g": "supported", "eth_25g": "experimental", "eth_slow": "rejected"}
        preferred = "rdma_100g"
    else:
        classes = {"rdma_100g": "supported", "eth_25g": "supported", "eth_slow": "experimental"}
        preferred = None
    parallelism = "sharded data parallel" if sharded else "data parallel"
    return InterconnectNeed(
        "trainer", parallelism, round(moved, 3), classes, preferred, _seconds(moved, classes)
    )


def rollout_link(tensor_parallel: int) -> InterconnectNeed | None:
    """A tensor-parallel rollout group must stay inside one node, on NVLink if it can."""
    if tensor_parallel <= 1:
        return None
    classes: dict[str, Tier] = {"nvlink": "supported", "pcie": "experimental"}
    return InterconnectNeed("rollout", "tensor parallel", None, classes, "nvlink", {})
