"""Preflight memory estimator: the Doc 07 §6.3 formulas as a library.

`estimate(model, config, topology)` reads only a `ModelInspection` (headers and config, never
weights) and returns, per device and phase, a low/expected/high range in GiB with a component
breakdown, a verdict (fits | tight | does_not_fit | unknown) under the catalog's headroom
rule, a confidence label and the assumptions it made. Caller-supplied calibration (Doc 07
§6.11) corrects the analytic prior.
"""

import dataclasses
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from mrl_runtime.estimate.hardware import DeviceSpec, Topology
from mrl_runtime.estimate.storage import (
    InterconnectNeed,
    Storage,
    StoragePlan,
    TrainedModel,
    rollout_link,
    storage,
    trainer_link,
)
from mrl_runtime.estimate.units import GB, GIB, Range, total
from mrl_runtime.model.errors import BAD_VALUE, MrlError
from mrl_runtime.model.headers import TensorInfo
from mrl_runtime.model.inspection import ModelInspection

type Verdict = Literal["fits", "tight", "does_not_fit", "unknown"]
type Confidence = Literal["low", "medium", "high"]
type Method = Literal["full", "lora", "qlora"]
type Optimizer = Literal["adamw", "adamw_8bit", "sgd", "adafactor"]
type Sharding = Literal["ddp", "zero1", "zero2", "zero3", "fsdp"]

DTYPE_BYTES = {
    "float64": 8.0, "float32": 4.0, "bfloat16": 2.0, "float16": 2.0,
    "float8": 1.0, "int8": 1.0, "4bit": 0.5,
}  # fmt: skip
# Optimizer state bytes per trainable parameter: (low, expected, high).
OPTIMIZER_BYTES: dict[str, tuple[float, float, float]] = {
    "adamw": (8.0, 8.0, 8.0),
    "adamw_8bit": (2.0, 2.0, 2.0),
    "sgd": (4.0, 4.0, 4.0),
    "adafactor": (0.1, 0.5, 1.0),
}
KAPPA = (0.6, 1.0, 1.3)  # activation architecture factor (prior)
LOGIT_FACTOR = (1.2, 2.0, 3.0)  # c: fp32 logits + log-softmax + gradient
EVAL_LOGIT_FACTOR = (1.0, 1.5, 2.0)  # forward only: fp32 logits + log-softmax
QLORA_META = (0.016, 0.0625)  # m_q bytes/param: double quantization .. plain
QLORA_HIGH = 1.1  # quant-state and allocator rounding on the 4-bit base
CKPT_LAYERS = (1.0, 1.5, 2.0)  # live per-layer terms with gradient checkpointing
OVERHEAD_LOW, OVERHEAD_FIRST, OVERHEAD_EXTRA, OVERHEAD_HIGH = 0.4, 0.5, 0.3, 1.0
ROLLOUT_OVERHEAD = (1.0, 1.5, 2.0)  # GiB inside the rollout engine
KV_SAFETY = 1.5
EMBEDDING = re.compile(r"(embed_tokens|wte|word_embeddings|lm_head|embed_out|shared)\.weight$")
LAYER = re.compile(r"^(.*?\.(?:layers|h|blocks|block|layer)\.\d+)\.")
DEFAULT_TARGETS = ("q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj")
SHARE_LOW_CONFIDENCE = 0.25


@dataclass(frozen=True)
class RolloutConfig:
    """A generation engine (vLLM-class) on the same devices (Doc 07 §6.3 "Rollout engine").

    `gpu_memory_utilization` None means: claim the recommended share.
    `released_during_update`: the engine frees its memory outside the rollout phase (update,
    eval, checkpoint), as the engine declares; it always holds its share at load.
    """

    concurrent_sequences: int
    prompt_tokens: int
    completion_tokens: int
    gpu_memory_utilization: float | None = None
    tensor_parallel: int = 1
    weight_dtype: str = "bfloat16"
    kv_dtype: str = "bfloat16"
    released_during_update: bool = False


@dataclass(frozen=True)
class TrainConfig:
    """What the run does. Doc 07 §6.2 notation in brackets.

    `trainable_dtype` None means float32 adapters for LoRA/QLoRA and the weight dtype for
    full fine-tuning. `loss_tokens` [s_loss] defaults to `seq_len` (completion tokens for RL).
    """

    method: Method = "lora"
    seq_len: int = 2048  # s
    micro_batch: int = 1  # b
    loss_tokens: int | None = None
    weight_dtype: str = "bfloat16"  # b_w
    trainable_dtype: str | None = None  # b_t
    optimizer: Optimizer = "adamw"
    master_weights: bool = False
    grad_checkpointing: bool = False
    flash_attention: bool = True
    activation_dtype: str = "bfloat16"
    lora_rank: int = 16  # r
    lora_targets: tuple[str, ...] = DEFAULT_TARGETS
    qlora_fp32_non_quantized: bool = False
    sharding: Sharding = "ddp"
    reference: Literal["none", "separate", "adapter_disabled"] = "none"
    reference_dtype: str = "bfloat16"
    critic: Literal["none", "separate", "value_head"] = "none"
    reward_model_params: int = 0
    reward_model_dtype: str = "bfloat16"
    reward_model_sharded: bool = False
    rollout: RolloutConfig | None = None
    dataloader_workers: int = 2
    save_dtype: str | None = None  # None: report bfloat16 .. float32
    keep_last: int = 2
    keep_best: int = 1
    checkpoints_per_month: float | None = None
    dataset_gb: float = 0.0


@dataclass(frozen=True)
class Calibration:
    """Per configuration class: robust mean `mu` and spread `sigma` of ln(actual/expected)."""

    mu: float
    sigma: float
    n: int


@dataclass(frozen=True)
class Component:
    name: str
    gib: Range
    note: str = ""


@dataclass(frozen=True)
class Phase:
    name: str
    components: tuple[Component, ...]
    total: Range


@dataclass(frozen=True)
class DeviceEstimate:
    """One device class (`count` identical devices): its phases, peak and verdict."""

    device: str
    count: int
    capacity_gib: float | None
    usable_gib: float | None
    phases: tuple[Phase, ...]
    peak: Range
    verdict: Verdict


@dataclass(frozen=True)
class Alternative:
    """A nearby configuration and how it would do (Doc 24: offer the nearest that fits)."""

    change: str
    peak: Range
    verdict: Verdict


@dataclass(frozen=True)
class Estimate:
    devices: tuple[DeviceEstimate, ...]
    verdict: Verdict
    confidence: Confidence
    assumptions: tuple[str, ...]
    recommended_gpu_memory_utilization: float | None = None
    calibration: Calibration | None = None
    storage: Storage | None = None
    interconnect: tuple[InterconnectNeed, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return dataclasses.asdict(self)


def _bytes(dtype: str) -> float:
    if dtype not in DTYPE_BYTES:
        raise MrlError(BAD_VALUE, "The estimator doesn't know this number format.", dtype)
    return DTYPE_BYTES[dtype]


def lora_params(tensors: Iterable[TensorInfo], rank: int, targets: Iterable[str]) -> int:
    """P_t = Σ over target modules r x (d_in + d_out), shapes from the weight headers."""
    names = tuple(targets)
    total = 0
    for t in tensors:
        if len(t.shape) == 2 and any(t.name.endswith(f".{m}.weight") for m in names):
            total += rank * (t.shape[0] + t.shape[1])
    return total


def quantized_split(tensors: Iterable[TensorInfo]) -> tuple[int, int]:
    """(P_q, P_nq): params in linear layers that 4-bit loading quantizes, and the rest."""
    q = nq = 0
    for t in tensors:
        if len(t.shape) == 2 and not EMBEDDING.search(t.name):
            q += t.numel
        else:
            nq += t.numel
    return q, nq


def largest_unit(tensors: Iterable[TensorInfo]) -> int:
    """Params of the largest FSDP-wrapped unit: one decoder block or one embedding."""
    units: dict[str, int] = {}
    for t in tensors:
        match = LAYER.match(t.name)
        key = match.group(1) if match else t.name
        units[key] = units.get(key, 0) + t.numel
    return max(units.values(), default=0)


@dataclass
class _Model:
    """The facts the formulas need, checked once."""

    p: int
    layers: int
    hidden: int
    heads: int
    kv_heads: int
    head_dim: int
    vocab: int
    tensors: tuple[TensorInfo, ...]
    disk_bytes: int


def _model_facts(model: ModelInspection, missing: list[str]) -> _Model | None:
    s = model.shape
    facts = {
        "parameter count": model.param_count,
        "layer count": s.num_layers,
        "hidden size": s.hidden_size,
        "attention heads": s.num_attention_heads,
        "vocabulary size": s.vocab_size,
    }
    missing.extend(name for name, value in facts.items() if not value)
    if missing:
        return None
    heads = s.num_attention_heads or 1
    hidden = s.hidden_size or 1
    return _Model(
        model.param_count or 0,
        s.num_layers or 0,
        hidden,
        heads,
        s.num_kv_heads or heads,
        s.head_dim or hidden // heads,
        s.vocab_size or 0,
        model.tensors,
        model.weight_bytes,
    )


class _Builder:
    """Collects components per phase for one device."""

    def __init__(self, m: _Model, cfg: TrainConfig, topo: Topology, notes: list[str]) -> None:
        self.m, self.cfg, self.topo, self.notes = m, cfg, topo, notes
        self.n = topo.data_parallel
        self.weights: list[Component] = []  # held in every phase
        self.state: list[Component] = []  # gradients and optimizer state: from the first step
        self.update: list[Component] = []
        self.eval: list[Component] = []
        self.save: list[Component] = []
        self.rollout: list[Component] = []
        self.rollout_verdict: Verdict = "fits"
        self.trained: list[TrainedModel] = []

    def shard(self, kind: Literal["w", "g", "o"]) -> float:
        divided = {
            "ddp": "",
            "zero1": "o",
            "zero2": "go",
            "zero3": "wgo",
            "fsdp": "wgo",
        }[self.cfg.sharding]
        return 1 / self.n if kind in divided else 1.0

    def add_static(self, name: str, r: Range, kind: Literal["w", "g", "o"], note: str = "") -> None:
        target = self.weights if kind == "w" else self.state
        target.append(Component(name, r.scale(self.shard(kind)), note))

    def policy(self, prefix: str = "") -> None:
        cfg, m = self.cfg, self.m
        b_w = _bytes(cfg.weight_dtype)
        k = OPTIMIZER_BYTES[cfg.optimizer]
        if cfg.method == "full":
            b_t = _bytes(cfg.trainable_dtype or cfg.weight_dtype)
            p_t = m.p
            self.add_static(f"{prefix}weights", Range.exact_bytes(m.p * b_w), "w")
        else:
            b_t = _bytes(cfg.trainable_dtype or "float32")
            p_t = lora_params(m.tensors, cfg.lora_rank, cfg.lora_targets)
            if p_t == 0:
                raise _NoTargetsError
            self.add_static(f"{prefix}base weights", self._base(), "w")
            self.add_static(f"{prefix}LoRA weights", Range.exact_bytes(p_t * b_t), "w")
        self.add_static(f"{prefix}gradients", Range.exact_bytes(p_t * b_t), "g")
        self.add_static(f"{prefix}optimizer state", Range.of_bytes(*(p_t * x for x in k)), "o")
        if cfg.master_weights:
            self.add_static(f"{prefix}master weights", Range.exact_bytes(p_t * 4.0), "o")
        save = (_bytes(cfg.save_dtype),) * 3 if cfg.save_dtype else (2.0, b_t, max(4.0, b_t))
        master = 4.0 if cfg.master_weights else 0.0
        opt = (k[0] + master, k[1] + master, k[2] + master)
        saved = m.p if cfg.method == "full" else p_t
        self.trained.append(TrainedModel(saved, p_t, opt, (save[0], save[1], save[2])))

    def _base(self) -> Range:
        cfg, m = self.cfg, self.m
        if cfg.method != "qlora":
            return Range.exact_bytes(m.p * _bytes(cfg.weight_dtype))
        p_q, p_nq = quantized_split(m.tensors)
        b_nq = 4.0 if cfg.qlora_fp32_non_quantized else _bytes(cfg.weight_dtype)
        low = p_q * (0.5 + QLORA_META[0]) + p_nq * b_nq
        expected = p_q * (0.5 + QLORA_META[1]) + p_nq * b_nq
        return Range.of_bytes(low, expected, expected * QLORA_HIGH)

    def activations(self) -> None:
        cfg, m = self.cfg, self.m
        sbh = cfg.seq_len * cfg.micro_batch * m.hidden
        scale = _bytes(cfg.activation_dtype) / 2
        attention = 0.0 if cfg.flash_attention else 5 * m.heads * cfg.seq_len / m.hidden
        per_layer = sbh * (34 + attention)
        if cfg.grad_checkpointing:
            kept = 2 * m.layers * sbh
            low, mid, high = (kept + k * per_layer for k in CKPT_LAYERS)
            r = Range.of_bytes(low, mid, high * KAPPA[2])
            note = "gradient checkpointing: layer inputs kept, one layer live"
        else:
            r = Range.of_bytes(*(k * m.layers * per_layer for k in KAPPA))
            note = "flash attention" if cfg.flash_attention else "attention matrix materialised"
        self.update.append(Component("activations", r.scale(scale), note))
        s_loss = cfg.loss_tokens or cfg.seq_len
        logits = s_loss * cfg.micro_batch * m.vocab * 4.0
        self.update.append(
            Component("logits and loss", Range.of_bytes(*(c * logits for c in LOGIT_FACTOR)))
        )
        live = Range.of_bytes(*(k * per_layer for k in CKPT_LAYERS)).scale(scale)
        self.eval.append(
            Component("eval activations", live, "forward only: one or two layers live")
        )
        forward = cfg.seq_len * cfg.micro_batch * m.vocab * 4.0
        self.eval.append(
            Component("eval logits", Range.of_bytes(*(c * forward for c in EVAL_LOGIT_FACTOR)))
        )

    def fsdp_unit(self) -> None:
        if self.cfg.sharding not in ("zero3", "fsdp") or self.n == 1:
            return
        unit = largest_unit(self.m.tensors) * _bytes(self.cfg.weight_dtype)
        self.update.append(
            Component(
                "unsharded unit",
                Range.of_bytes(unit, 1.5 * unit, 2 * unit),
                "largest block during backward",
            )
        )
        gather = Range.of_bytes(unit / 2, unit / 2, unit)
        self.eval.append(Component("unsharded unit", gather, "largest block during forward"))
        self.save.append(
            Component(
                "save gather",
                Range.of_bytes(unit / 2, unit / 2, unit),
                "one unit gathered at a time",
            )
        )

    def frozen_copies(self) -> None:
        cfg, m = self.cfg, self.m
        sharded = 1 / self.n if cfg.sharding in ("zero3", "fsdp") else 1.0
        if cfg.reference == "separate":
            ref = Range.exact_bytes(m.p * _bytes(cfg.reference_dtype)).scale(sharded)
            self.weights.append(Component("reference model", ref, "frozen copy"))
            sbh = cfg.seq_len * cfg.micro_batch * m.hidden * 34 * _bytes(cfg.activation_dtype) / 2
            self.update.append(
                Component("reference forward", Range.of_bytes(sbh, 1.5 * sbh, 2 * sbh))
            )
        elif cfg.reference == "adapter_disabled":
            self.notes.append("The reference model is the base model with the adapter turned off.")
        if cfg.critic == "separate":
            self.policy(prefix="critic ")
            self.notes.append("Actor and critic backward passes run one after the other.")
        elif cfg.critic == "value_head":
            self.notes.append(
                "The value head shares the policy's trunk (about h extra parameters)."
            )
        if cfg.reward_model_params:
            share = 1 / self.n if cfg.reward_model_sharded else 1.0
            rm = cfg.reward_model_params * _bytes(cfg.reward_model_dtype) * share
            self.weights.append(Component("reward model", Range.of_bytes(rm, rm, rm * 1.1)))

    def rollout_engine(self, capacity: float | None) -> float | None:
        """Adds the engine's claim; returns the recommended gpu_memory_utilization."""
        ro, m = self.cfg.rollout, self.m
        if ro is None:
            return None
        tp = ro.tensor_parallel
        if tp < 1:
            raise MrlError(
                BAD_VALUE, "The rollout tensor-parallel size must be at least 1.", str(tp)
            )
        if tp > self.topo.devices:
            # Doc 07 §5.7: a tensor-parallel group stays inside one node.
            self.rollout_verdict = "does_not_fit"
            self.notes.append(
                f"The rollout engine splits the model over {tp} GPUs, but each machine has "
                f"{self.topo.devices}; the group must fit on one machine."
            )
        kv_per_token = 2 * m.layers * m.kv_heads * m.head_dim * _bytes(ro.kv_dtype)
        tokens = ro.concurrent_sequences * (ro.prompt_tokens + ro.completion_tokens) * KV_SAFETY
        need = (
            m.p * _bytes(ro.weight_dtype) / tp
            + ROLLOUT_OVERHEAD[2] * GIB
            + tokens * kv_per_token / tp
        )
        recommended = None if capacity is None else math.ceil(100 * need / (capacity * GIB)) / 100
        u = ro.gpu_memory_utilization if ro.gpu_memory_utilization is not None else recommended
        if capacity is None or u is None:
            raise _NoCapacityError
        claim = u * capacity
        minimum = (
            m.p * _bytes(ro.weight_dtype) / tp
            + ROLLOUT_OVERHEAD[0] * GIB
            + (ro.prompt_tokens + ro.completion_tokens) * kv_per_token / tp
        )
        if claim * GIB < minimum:
            self.rollout_verdict = "does_not_fit"
            self.notes.append(
                f"The rollout engine's share {u:.2f} gives {claim:.1f} GiB, but it needs at "
                f"least {minimum / GIB:.1f} GiB for its weights and one sequence."
            )
        elif recommended is not None and u < recommended:
            self.rollout_verdict = "tight"
            self.notes.append(
                f"The rollout engine's share {u:.2f} is below the {recommended:.2f} it needs "
                "for all sequences at once; generation will run fewer at a time."
            )
        self.rollout.append(
            Component("rollout engine", Range.exact(claim), f"gpu_memory_utilization {u:.2f}")
        )
        return recommended

    def overhead(self) -> Component:
        processes = 1 + (1 if self.cfg.rollout else 0)
        expected = min(OVERHEAD_HIGH, OVERHEAD_FIRST + OVERHEAD_EXTRA * (processes - 1))
        return Component(
            "fixed overhead",
            Range(OVERHEAD_LOW, expected, OVERHEAD_HIGH),
            "context, kernels, allocator",
        )

    def phases(self, extra: list[Component]) -> tuple[Phase, ...]:
        """load, rollout (if any), update, eval and checkpoint; the peak is their max."""
        over = self.overhead()
        ro = self.cfg.rollout
        held = [] if ro is not None and ro.released_during_update else self.rollout
        plan: list[tuple[str, list[Component]]] = [("load", [*self.weights, *self.rollout])]
        if ro is not None:
            plan.append(("rollout", [*self.weights, *self.state, *self.rollout]))
        plan += [
            ("update", [*self.weights, *self.state, *self.update, *held]),
            ("eval", [*self.weights, *self.state, *self.eval, *held]),
            ("checkpoint", [*self.weights, *self.state, *self.save, *held]),
        ]
        out: list[Phase] = []
        for name, comps in plan:
            parts = (*comps, *extra, over)
            out.append(Phase(name, parts, total(c.gib for c in parts)))
        return tuple(out)


class _NoTargetsError(Exception):
    """No LoRA target module was found in the weight headers."""


class _NoCapacityError(Exception):
    """The device size is unknown, so the rollout share can't be turned into bytes."""


def _peak(phases: Iterable[Phase]) -> Range:
    totals = [p.total for p in phases]
    return Range(
        max(t.low for t in totals), max(t.expected for t in totals), max(t.high for t in totals)
    )


def verdict(peak: Range, capacity: float | None, usable: float | None) -> Verdict:
    """fits: high ≤ usable · tight: expected ≤ capacity but above usable somewhere ·
    does_not_fit: expected > capacity · unknown: capacity not known."""
    if capacity is None or usable is None:
        return "unknown"
    if peak.expected > capacity:
        return "does_not_fit"
    if peak.high > usable or peak.expected > usable:
        return "tight"
    return "fits"


def _calibrate(peak: Range, cal: Calibration | None) -> tuple[Range, Confidence | None]:
    if cal is None:
        return peak, None
    if cal.sigma < 0 or cal.n < 0:
        raise MrlError(
            BAD_VALUE, "The calibration values can't be used.", f"sigma={cal.sigma} n={cal.n}"
        )
    if cal.n >= 20 and cal.sigma <= 0.08:
        label: Confidence = "high"
    elif cal.n >= 5 and cal.sigma <= 0.15:
        label = "medium"
    else:
        return peak, "low"  # too little evidence: keep the analytic prior range
    expected = peak.expected * math.exp(cal.mu)
    spread = max(math.exp(1.64 * cal.sigma), 1.05)
    return Range(expected / spread, expected, expected * spread), label


_ORDER: dict[Verdict, int] = {"fits": 0, "tight": 1, "unknown": 2, "does_not_fit": 3}


def _unified_extra(cfg: TrainConfig) -> Component:
    return Component(
        "data loading", Range.exact(2.0 * cfg.dataloader_workers), "2 GiB per dataloader worker"
    )


def _host(m: _Model, cfg: TrainConfig, topo: Topology) -> DeviceEstimate | None:
    if topo.device.kind != "gpu" or topo.system_ram_gib is None:
        return None
    disk = m.disk_bytes / GIB
    staging = max(1.5 * disk, 16.0)
    comps = [
        Component("model staging", Range.exact(staging), "max(1.5 x weights on disk, 16 GiB)"),
        Component("dataloader workers", Range.exact(2.0 * cfg.dataloader_workers)),
    ]
    if cfg.rollout is not None:
        comps.append(Component("rollout process", Range.exact(2.0)))
    phase = Phase("load", tuple(comps), total(c.gib for c in comps))
    ram = topo.system_ram_gib
    usable = topo.device.headroom.ram_usable(ram)
    return DeviceEstimate(
        "host", topo.nodes, ram, usable, (phase,), phase.total, verdict(phase.total, ram, usable)
    )


def _device_name(spec: DeviceSpec) -> str:
    return {"gpu": "gpu", "unified": "unified memory", "cpu": "system memory"}[spec.kind]


def _unknown(topo: Topology, notes: list[str]) -> Estimate:
    spec = topo.device
    empty = DeviceEstimate(
        _device_name(spec),
        topo.devices,
        spec.memory_gib,
        spec.usable_gib(),
        (),
        Range.exact(0.0),
        "unknown",
    )
    return Estimate((empty,), "unknown", "low", tuple(notes))


def estimate(
    model: ModelInspection,
    config: TrainConfig,
    topology: Topology,
    calibration: Calibration | None = None,
) -> Estimate:
    """Memory per device and phase for one training configuration on one topology."""
    notes: list[str] = []
    missing: list[str] = []
    m = _model_facts(model, missing)
    if m is None:
        return _unknown(topology, [f"The model's {', '.join(missing)} is not known."])
    spec = topology.device
    builder = _Builder(m, config, topology, notes)
    try:
        builder.policy()
        builder.frozen_copies()
        recommended = builder.rollout_engine(spec.memory_gib)
    except _NoTargetsError:
        return _unknown(topology, ["None of the LoRA target modules were found in the weights."])
    except _NoCapacityError:
        return _unknown(topology, ["The device memory size is not known."])
    builder.fsdp_unit()
    builder.activations()
    extra = [_unified_extra(config)] if spec.kind in ("unified", "cpu") else []
    phases = builder.phases(extra)
    analytic = _peak(phases)
    peak, cal_label = _calibrate(analytic, calibration)
    usable = spec.usable_gib()
    memory_verdict = verdict(peak, spec.memory_gib, usable)
    device = DeviceEstimate(
        _device_name(spec),
        topology.devices,
        spec.memory_gib,
        usable,
        phases,
        peak.rounded(),
        _worst([memory_verdict, builder.rollout_verdict]),
    )
    devices = [device]
    host = _host(m, config, topology)
    if host is not None:
        devices.append(host)
    elif spec.kind == "gpu":
        notes.append("Host memory was not checked because the system RAM size is not known.")
    disk = storage(builder.trained, _storage_plan(model, config, topology))
    if topology.scratch_free_gib is not None:
        devices.append(_disk(disk, topology.scratch_free_gib))
    else:
        notes.append("Disk space was not checked because the free scratch space is not known.")
    notes.extend(_standard_notes(config, spec))
    return Estimate(
        tuple(devices),
        _worst([d.verdict for d in devices]),
        cal_label or _prior_confidence(config, phases, analytic),
        tuple(notes),
        recommended,
        calibration,
        disk,
        _links(m, config, topology),
    )


def _worst(verdicts: Iterable[Verdict]) -> Verdict:
    return max(verdicts, key=_ORDER.__getitem__)


def _storage_plan(model: ModelInspection, cfg: TrainConfig, topo: Topology) -> StoragePlan:
    files = sum(f.size for f in model.files) or model.weight_bytes
    return StoragePlan(
        cfg.keep_last,
        cfg.keep_best,
        cfg.checkpoints_per_month,
        cfg.dataset_gb,
        files / GB,
        cfg.dataloader_workers,
        topo.system_ram_gib,
    )


def _disk(disk: Storage, free_gib: float) -> DeviceEstimate:
    """Scratch space on the node (GB converted to GiB to sit beside the memory devices)."""
    need = disk.scratch_gb.scale(GB / GIB)
    phase = Phase(
        "scratch", (Component("scratch", need, "model, data, checkpoints, image, logs"),), need
    )
    return DeviceEstimate(
        "disk", 1, free_gib, free_gib, (phase,), need.rounded(), verdict(need, free_gib, free_gib)
    )


def _links(m: _Model, cfg: TrainConfig, topo: Topology) -> tuple[InterconnectNeed, ...]:
    b_w = _bytes(cfg.weight_dtype)
    p_t = m.p if cfg.method == "full" else lora_params(m.tensors, cfg.lora_rank, cfg.lora_targets)
    b_t = _bytes(cfg.trainable_dtype or (cfg.weight_dtype if cfg.method == "full" else "float32"))
    needs = [
        trainer_link(
            devices=topo.devices,
            nodes=topo.nodes,
            sharding=cfg.sharding,
            full_params_gb=m.p * b_w / GB,
            gradients_gb=p_t * b_t / GB,
        ),
        rollout_link(cfg.rollout.tensor_parallel if cfg.rollout else 1),
    ]
    return tuple(n for n in needs if n is not None)


def _prior_confidence(cfg: TrainConfig, phases: tuple[Phase, ...], peak: Range) -> Confidence:
    """Uncalibrated: medium, or low when a colocated rollout engine (version-dependent
    footprint) meets wide activation/logit terms or engine-declared memory release."""
    if cfg.rollout is None:
        return "medium"
    if cfg.rollout.released_during_update:
        return "low"
    update = next(p for p in phases if p.name == "update")  # the phase with the wide terms
    wide = sum(
        c.gib.expected for c in update.components if c.name in ("activations", "logits and loss")
    )
    return "low" if peak.expected and wide / peak.expected >= SHARE_LOW_CONFIDENCE else "medium"


def _standard_notes(cfg: TrainConfig, spec: DeviceSpec) -> list[str]:
    notes = [
        "Ranges are analytic estimates (Doc 07 §6.3), not guarantees.",
        "Allocator fragmentation is left to calibration.",
    ]
    if spec.kind == "unified":
        notes.append("On unified memory the model, the app and macOS share one budget.")
    if cfg.rollout is not None and cfg.rollout.released_during_update:
        notes.append("The rollout engine is assumed to free its memory during updates.")
    return notes


def alternatives(
    model: ModelInspection, config: TrainConfig, topology: Topology
) -> tuple[Alternative, ...]:
    """Nearby configurations that change the verdict (Doc 24: offer the nearest that fits)."""
    tries: list[tuple[str, TrainConfig]] = []
    if not config.grad_checkpointing:
        tries.append(
            ("turn on gradient checkpointing", dataclasses.replace(config, grad_checkpointing=True))
        )
    if config.method == "lora":
        tries.append(("use QLoRA 4-bit", dataclasses.replace(config, method="qlora")))
    if config.optimizer == "adamw":
        tries.append(("use 8-bit AdamW", dataclasses.replace(config, optimizer="adamw_8bit")))
    if config.micro_batch > 1:
        tries.append(
            (
                "halve the micro-batch",
                dataclasses.replace(config, micro_batch=config.micro_batch // 2),
            )
        )
    ro = config.rollout
    if ro is not None and ro.gpu_memory_utilization is not None:
        tries.append(
            (
                "use the recommended rollout memory share",
                dataclasses.replace(
                    config, rollout=dataclasses.replace(ro, gpu_memory_utilization=None)
                ),
            )
        )
    out: list[Alternative] = []
    for change, cfg in tries:
        est = estimate(model, cfg, topology)
        out.append(Alternative(change, est.devices[0].peak, est.verdict))
    return tuple(out)
