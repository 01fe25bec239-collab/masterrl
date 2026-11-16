"""Estimator: Doc 07 §6.4-6.6 worked examples as goldens, catalog headroom, calibration and
monotonicity properties.

Golden tolerances: components the doc derives exactly (weights, LoRA, activations, KV) within
2 %; totals within 6 % (expected) and 10 % (low/high), because the doc rounds and its example A
uses a logit factor c of 2.0-3.0 where the formula table says 1.2-3.0.
"""

import dataclasses
import math
from pathlib import Path

import make_fixtures
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mrl_runtime.estimate import (
    Calibration,
    Component,
    DeviceSpec,
    Headroom,
    Range,
    RolloutConfig,
    Topology,
    TrainConfig,
    alternatives,
    device_from_profile,
    estimate,
    lora_params,
    verdict,
)
from mrl_runtime.model import MrlError, inspect_model
from mrl_runtime.model.headers import TensorInfo
from mrl_runtime.model.inspection import GenerationDefaults, ModelInspection, Shape


def decoder(dims: Shape, *, qkv_bias: bool = False) -> ModelInspection:
    """A Llama/Qwen-style decoder: tensors named like Hugging Face checkpoints."""
    hidden, layers, heads = (
        dims.hidden_size or 0,
        dims.num_layers or 0,
        dims.num_attention_heads or 0,
    )
    kv_heads, head_dim = dims.num_kv_heads or 0, dims.head_dim or 0
    inter, vocab, tied = (
        dims.intermediate_size or 0,
        dims.vocab_size or 0,
        bool(dims.tie_word_embeddings),
    )
    kv = kv_heads * head_dim
    tensors = [TensorInfo("model.embed_tokens.weight", "BF16", (vocab, hidden))]
    for i in range(layers):
        p = f"model.layers.{i}."
        tensors += [
            TensorInfo(p + "self_attn.q_proj.weight", "BF16", (heads * head_dim, hidden)),
            TensorInfo(p + "self_attn.k_proj.weight", "BF16", (kv, hidden)),
            TensorInfo(p + "self_attn.v_proj.weight", "BF16", (kv, hidden)),
            TensorInfo(p + "self_attn.o_proj.weight", "BF16", (hidden, heads * head_dim)),
            TensorInfo(p + "mlp.gate_proj.weight", "BF16", (inter, hidden)),
            TensorInfo(p + "mlp.up_proj.weight", "BF16", (inter, hidden)),
            TensorInfo(p + "mlp.down_proj.weight", "BF16", (hidden, inter)),
            TensorInfo(p + "input_layernorm.weight", "BF16", (hidden,)),
            TensorInfo(p + "post_attention_layernorm.weight", "BF16", (hidden,)),
        ]
        if qkv_bias:
            tensors += [
                TensorInfo(p + "self_attn.q_proj.bias", "BF16", (heads * head_dim,)),
                TensorInfo(p + "self_attn.k_proj.bias", "BF16", (kv,)),
                TensorInfo(p + "self_attn.v_proj.bias", "BF16", (kv,)),
            ]
    tensors.append(TensorInfo("model.norm.weight", "BF16", (hidden,)))
    if not tied:
        tensors.append(TensorInfo("lm_head.weight", "BF16", (vocab, hidden)))
    params = sum(t.numel for t in tensors)
    return ModelInspection(
        path="synthetic",
        format="safetensors",
        kind="full_weights",
        refusal=None,
        inference_only_code=None,
        quantization=None,
        requires_remote_code=False,
        remote_code_code=None,
        auto_map={},
        architecture="LlamaForCausalLM",
        architectures=("LlamaForCausalLM",),
        model_type="llama",
        context_length=131072,
        rope_scaling=None,
        moe=None,
        vision=None,
        adapter=None,
        shape=dims,
        param_count=params,
        param_count_exact=True,
        active_param_count=params,
        dtype="bfloat16",
        params_by_dtype={"bfloat16": params},
        weight_bytes=params * 2,
        tensors=tuple(tensors),
        generation=GenerationDefaults((), None, None, None),
        chat_template_sha256=None,
        chat_template_names=(),
        has_generation_tags=False,
        tokenizer_sha256={},
        files=(),
    )


def llama_8b() -> ModelInspection:
    """Llama-3.1-8B-like (Doc 07 §6.4): P = 8.03 B."""
    return decoder(Shape(4096, 32, 32, 8, 128, 14_336, 128_256, False))


def qwen_1_5b() -> ModelInspection:
    """Qwen2.5-1.5B-like (Doc 07 §6.5): P = 1.54 B, tied embeddings."""
    return decoder(Shape(1536, 28, 12, 2, 128, 8960, 151_936, True), qkv_bias=True)


def qwen_7b() -> ModelInspection:
    """Qwen2.5-7B-like (Doc 07 §6.6): P = 7.62 B."""
    return decoder(Shape(3584, 28, 28, 4, 128, 18_944, 152_064, False), qkv_bias=True)


LLAMA_8B = llama_8b()
QWEN_1_5B = qwen_1_5b()
QWEN_7B = qwen_7b()


def gpu(gib: float, devices: int = 1) -> Topology:
    profile = "nvidia-24gb" if gib == 24 else "nvidia-32-80gb"
    return Topology(device_from_profile(profile, gib), devices=devices)


def close(value: float, want: float, rel: float) -> bool:
    return math.isclose(value, want, rel_tol=rel)


def component(components: tuple[Component, ...], name: str) -> float:
    for c in components:
        if c.name == name:
            return c.gib.expected
    raise AssertionError(name)


def assert_total(peak: Range, low: float, expected: float, high: float) -> None:
    assert close(peak.expected, expected, 0.06), (peak, expected)
    assert close(peak.low, low, 0.10), (peak, low)
    assert close(peak.high, high, 0.10), (peak, high)


def test_shapes_match_the_doc() -> None:
    assert round(LLAMA_8B.param_count or 0, -7) == 8.03e9
    assert round(QWEN_1_5B.param_count or 0, -7) == 1.54e9
    assert round(QWEN_7B.param_count or 0, -7) == 7.62e9


# Doc 07 §6.4, example A: LoRA r=16 SFT of an 8 B model on 1 x 24 GiB.
A_ROWS = [
    # config, Doc 07 total (low, expected, high), verdict
    pytest.param(TrainConfig(), (23.1, 27.0, 30.6), "does_not_fit", id="bf16"),
    pytest.param(TrainConfig(grad_checkpointing=True), (18.8, 19.5, 21.0), "fits", id="bf16-ckpt"),
    pytest.param(
        TrainConfig(method="qlora", grad_checkpointing=True),
        (9.1, 10.1, 12.8),
        "fits",
        id="qlora-ckpt",
    ),
    # The doc says LIKELY_FITS against ~23.5 GiB usable; the catalog keeps 10 % free
    # (21.6 GiB usable), so the 21.8 GiB high end is `tight`.
    pytest.param(TrainConfig(method="qlora"), (13.4, 17.6, 21.8), "tight", id="qlora"),
]


@pytest.mark.parametrize(("cfg", "total", "want"), A_ROWS)
def test_example_a(cfg: TrainConfig, total: tuple[float, float, float], want: str) -> None:
    est = estimate(LLAMA_8B, cfg, gpu(24))
    device = est.devices[0]
    assert_total(device.peak, *total)
    assert device.verdict == want
    assert est.verdict == want
    assert est.confidence == "medium"
    comps = device.phases[-1].components
    base = 5.6 if cfg.method == "qlora" else 14.96
    assert close(component(comps, "base weights"), base, 0.02)
    lora = sum(component(comps, n) for n in ("LoRA weights", "gradients", "optimizer state"))
    assert close(lora, 0.63, 0.02)
    assert close(component(comps, "activations"), 1.0 if cfg.grad_checkpointing else 8.5, 0.12)


def test_example_a_lora_params() -> None:
    assert close(lora_params(LLAMA_8B.tensors, 16, TrainConfig().lora_targets), 41.9e6, 0.01)


def test_example_a_alternatives() -> None:
    options = {a.change: a for a in alternatives(LLAMA_8B, TrainConfig(), gpu(24))}
    assert options["turn on gradient checkpointing"].verdict == "fits"
    assert close(options["turn on gradient checkpointing"].peak.high, 21.0, 0.05)
    assert options["use QLoRA 4-bit"].peak.expected < 18


# Doc 07 §6.5, example B: full-FT GRPO of a 1.5 B model, vLLM colocated, 1 x 80 GiB.
B_BASE = TrainConfig(
    method="full", weight_dtype="float32", seq_len=1536, micro_batch=8, loss_tokens=1024
)
B_ROWS = [
    # u, grad-ckpt, Doc 07 total (low, expected, high), (verdict, confidence)
    pytest.param(0.30, False, (63, 74, 84), ("tight", "low"), id="u30"),
    pytest.param(0.15, False, (51, 62, 72), ("fits", "low"), id="u15"),
    pytest.param(0.30, True, (54, 59, 65), ("fits", "medium"), id="u30-ckpt"),
]


@pytest.mark.parametrize(("u", "ckpt", "total", "outcome"), B_ROWS)
def test_example_b(
    u: float, ckpt: bool, total: tuple[float, float, float], outcome: tuple[str, str]
) -> None:
    want, confidence = outcome
    cfg = dataclasses.replace(
        B_BASE, grad_checkpointing=ckpt, rollout=RolloutConfig(64, 512, 1024, u)
    )
    est = estimate(QWEN_1_5B, cfg, gpu(80))
    device = est.devices[0]
    assert_total(device.peak, *total)
    assert device.verdict == want
    assert est.confidence == confidence
    comps = device.phases[-1].components
    policy = sum(component(comps, n) for n in ("weights", "gradients", "optimizer state"))
    assert close(policy, 22.9, 0.02)
    assert component(comps, "rollout engine") == pytest.approx(80 * u)
    assert close(component(comps, "logits and loss"), 9.3, 0.02)
    if not ckpt:
        assert close(component(comps, "activations"), 16.7, 0.02)
    assert est.recommended_gpu_memory_utilization is not None
    assert 0.10 <= est.recommended_gpu_memory_utilization <= 0.15  # doc: 0.12-0.15


# Doc 07 §6.6, example C: PPO of a 7 B model on 8 x 80 GiB, FSDP, vLLM per GPU at u=0.45.
def _ppo(release: bool, ckpt: bool) -> TrainConfig:
    return TrainConfig(
        method="full",
        weight_dtype="float32",
        seq_len=2048,
        micro_batch=4,
        sharding="fsdp",
        reference="separate",
        reference_dtype="float32",
        critic="separate",
        grad_checkpointing=ckpt,
        rollout=RolloutConfig(64, 512, 1536, 0.45, released_during_update=release),
    )


@pytest.mark.parametrize(
    ("release", "ckpt", "rollout_phase", "update_phase", "want"),
    [
        (True, True, 68.9, 49.4, "fits"),
        (True, False, 68.9, 72.9, "tight"),
        (False, False, 68.9, 108.9, "does_not_fit"),
    ],
)
def test_example_c(
    release: bool, ckpt: bool, rollout_phase: float, update_phase: float, want: str
) -> None:
    est = estimate(QWEN_7B, _ppo(release, ckpt), gpu(80, devices=8))
    device = est.devices[0]
    phases = {p.name: p for p in device.phases}
    assert close(phases["rollout"].total.expected, rollout_phase, 0.03)
    assert close(phases["update"].total.expected, update_phase, 0.06)
    assert device.verdict == want
    comps = phases["update"].components
    static = sum(
        component(comps, n)
        for n in (
            "weights",
            "gradients",
            "optimizer state",
            "critic weights",
            "critic gradients",
            "critic optimizer state",
            "reference model",
        )
    )
    assert close(static, 31.9, 0.02)
    assert close(component(comps, "unsharded unit"), 3.0, 0.05)
    if release:
        assert est.confidence == "low"


def test_kv_cache_per_token() -> None:
    # 2 x L x n_kv x d_head x 2 B: 28.7 kB (1.5 B) and 57.3 kB (7 B) per token.
    for model, want in ((QWEN_1_5B, 28_672), (QWEN_7B, 57_344)):
        s = model.shape
        assert 2 * (s.num_layers or 0) * (s.num_kv_heads or 0) * (s.head_dim or 0) * 2 == want


def test_rollout_share_below_need_warns() -> None:
    cfg = dataclasses.replace(B_BASE, rollout=RolloutConfig(64, 512, 1024, 0.05))
    est = estimate(QWEN_1_5B, cfg, gpu(80))
    assert any("rollout engine" in a for a in est.assumptions)
    recommended = estimate(
        QWEN_1_5B, dataclasses.replace(B_BASE, rollout=RolloutConfig(64, 512, 1024)), gpu(80)
    )
    assert recommended.recommended_gpu_memory_utilization == est.recommended_gpu_memory_utilization


def test_catalog_headroom_rules() -> None:
    nvidia = device_from_profile("nvidia-24gb")
    assert nvidia.usable_gib() == pytest.approx(21.6)
    mac = device_from_profile("apple-32gb")
    assert (mac.kind, mac.usable_gib()) == ("unified", pytest.approx(24.0))
    cpu = device_from_profile("cpu", 64)
    assert cpu.usable_gib() == pytest.approx(64 - 9.6)
    assert device_from_profile("cpu", 16).usable_gib() == pytest.approx(12)
    with pytest.raises(MrlError):
        device_from_profile("nvidia-32-80gb", 100)
    with pytest.raises(MrlError) as err:
        device_from_profile("tpu-v9")
    assert err.value.code == "MRL-VAL-003"
    assert device_from_profile("nvidia-32-80gb").memory_gib is None


def test_apple_unified_budget(tmp_path: Path) -> None:
    small = decoder(Shape(2048, 22, 32, 4, 64, 5632, 32_000, False))
    cfg = TrainConfig(method="qlora", grad_checkpointing=True, dataloader_workers=1)
    est = estimate(small, cfg, Topology(device_from_profile("apple-16gb")))
    device = est.devices[0]
    assert device.device == "unified memory"
    assert len(est.devices) == 1  # host RAM is the same budget
    assert component(device.phases[-1].components, "data loading") == 2.0
    assert device.usable_gib == pytest.approx(12.0)
    assert any("unified memory" in a for a in est.assumptions)
    big = estimate(LLAMA_8B, TrainConfig(), Topology(device_from_profile("apple-16gb")))
    assert big.verdict == "does_not_fit"


def test_host_ram_for_gpu_machines() -> None:
    est = estimate(
        LLAMA_8B,
        TrainConfig(grad_checkpointing=True),
        Topology(device_from_profile("nvidia-24gb"), system_ram_gib=24),
    )
    host = est.devices[1]
    assert host.device == "host"
    assert host.peak.expected == pytest.approx(max(1.5 * LLAMA_8B.weight_bytes / 2**30, 16) + 4)
    assert host.verdict == "does_not_fit"  # 26.4 GiB staging + workers; 20 GiB usable
    assert est.verdict == "does_not_fit"
    roomy = estimate(
        LLAMA_8B,
        TrainConfig(grad_checkpointing=True),
        Topology(device_from_profile("nvidia-24gb"), system_ram_gib=128),
    )
    assert roomy.verdict == "fits"
    unchecked = estimate(
        LLAMA_8B, TrainConfig(grad_checkpointing=True), Topology(device_from_profile("nvidia-24gb"))
    )
    assert len(unchecked.devices) == 1
    assert any("Host memory was not checked" in a for a in unchecked.assumptions)


def test_unknown_inputs(tmp_path: Path) -> None:
    pickle_like = dataclasses.replace(LLAMA_8B, param_count=None)
    est = estimate(pickle_like, TrainConfig(), gpu(24))
    assert (est.verdict, est.confidence) == ("unknown", "low")
    assert "parameter count" in est.assumptions[0]
    no_targets = estimate(LLAMA_8B, TrainConfig(lora_targets=("c_attn",)), gpu(24))
    assert no_targets.verdict == "unknown"
    rollout = dataclasses.replace(B_BASE, rollout=RolloutConfig(8, 64, 64))
    no_size = estimate(QWEN_1_5B, rollout, Topology(device_from_profile("nvidia-32-80gb")))
    assert no_size.verdict == "unknown"
    with pytest.raises(MrlError):
        estimate(LLAMA_8B, TrainConfig(weight_dtype="float7"), gpu(24))


def test_tiny_llama_fixture_estimate(tmp_path: Path) -> None:
    make_fixtures.build_models(tmp_path)
    tiny = inspect_model(tmp_path / "tiny-llama")
    est = estimate(
        tiny,
        TrainConfig(method="full", weight_dtype="float32", seq_len=512),
        Topology(device_from_profile("cpu", 16)),
    )
    assert est.verdict == "fits"
    assert est.devices[0].device == "system memory"


def test_calibration() -> None:
    base = estimate(LLAMA_8B, TrainConfig(grad_checkpointing=True), gpu(24))
    peak = base.devices[0].peak
    high = estimate(
        LLAMA_8B, TrainConfig(grad_checkpointing=True), gpu(24), Calibration(0.1, 0.05, 30)
    )
    assert high.confidence == "high"
    cal = high.devices[0].peak
    assert cal.expected == pytest.approx(peak.expected * math.exp(0.1), rel=1e-3)
    assert cal.high / cal.expected == pytest.approx(math.exp(1.64 * 0.05), rel=1e-3)
    narrow = estimate(
        LLAMA_8B, TrainConfig(grad_checkpointing=True), gpu(24), Calibration(0.0, 0.001, 30)
    )
    assert narrow.devices[0].peak.high / narrow.devices[0].peak.expected == pytest.approx(
        1.05, rel=1e-3
    )
    medium = estimate(
        LLAMA_8B, TrainConfig(grad_checkpointing=True), gpu(24), Calibration(0.0, 0.12, 6)
    )
    assert medium.confidence == "medium"
    weak = estimate(
        LLAMA_8B, TrainConfig(grad_checkpointing=True), gpu(24), Calibration(0.5, 0.3, 3)
    )
    assert weak.confidence == "low"
    assert weak.devices[0].peak == peak  # too little evidence: analytic prior kept
    with pytest.raises(MrlError):
        estimate(LLAMA_8B, TrainConfig(), gpu(24), Calibration(0.0, -1.0, 10))


def test_verdict_rules() -> None:
    assert verdict(Range(10, 12, 14), 24, 21.6) == "fits"
    assert verdict(Range(10, 20, 23), 24, 21.6) == "tight"
    assert verdict(Range(10, 23, 25), 24, 21.6) == "tight"
    assert verdict(Range(20, 25, 30), 24, 21.6) == "does_not_fit"
    assert verdict(Range(1, 1, 1), None, None) == "unknown"


def test_sharding_divides_state() -> None:
    cfg = TrainConfig(method="full", seq_len=512)
    one = estimate(QWEN_7B, cfg, gpu(80)).devices[0].peak.expected
    totals = {}
    for sharding in ("ddp", "zero1", "zero2", "zero3"):
        totals[sharding] = (
            estimate(QWEN_7B, dataclasses.replace(cfg, sharding=sharding), gpu(80, 8))
            .devices[0]
            .peak.expected
        )
    assert totals["ddp"] == pytest.approx(one)
    assert totals["ddp"] > totals["zero1"] > totals["zero2"] > totals["zero3"]


# Monotonicity properties: more work never needs less memory; more memory never a worse verdict.
ORDER = {"fits": 0, "tight": 1, "unknown": 2, "does_not_fit": 3}
configs = st.builds(
    TrainConfig,
    method=st.sampled_from(["full", "lora", "qlora"]),
    seq_len=st.integers(64, 8192),
    micro_batch=st.integers(1, 16),
    grad_checkpointing=st.booleans(),
    flash_attention=st.booleans(),
    optimizer=st.sampled_from(["adamw", "adamw_8bit", "sgd", "adafactor"]),
    lora_rank=st.integers(1, 256),
)
H100 = Topology(DeviceSpec("gpu", 80, "nvidia", Headroom(gpu_memory_free_pct=10)))


@settings(max_examples=60, deadline=None)
@given(cfg=configs)
def test_range_is_ordered(cfg: TrainConfig) -> None:
    peak = estimate(QWEN_1_5B, cfg, H100).devices[0].peak
    assert 0 < peak.low <= peak.expected <= peak.high


@settings(max_examples=60, deadline=None)
@given(cfg=configs, extra=st.integers(1, 4096))
def test_longer_sequences_need_more(cfg: TrainConfig, extra: int) -> None:
    longer = dataclasses.replace(cfg, seq_len=cfg.seq_len + extra)
    a, b = (
        estimate(QWEN_1_5B, cfg, H100).devices[0].peak,
        estimate(QWEN_1_5B, longer, H100).devices[0].peak,
    )
    assert b.low >= a.low
    assert b.expected >= a.expected
    assert b.high >= a.high


@settings(max_examples=60, deadline=None)
@given(cfg=configs, extra=st.integers(1, 16))
def test_bigger_batches_and_ranks_need_more(cfg: TrainConfig, extra: int) -> None:
    for bigger in (
        dataclasses.replace(cfg, micro_batch=cfg.micro_batch + extra),
        dataclasses.replace(cfg, lora_rank=cfg.lora_rank + extra),
    ):
        assert (
            estimate(QWEN_1_5B, bigger, H100).devices[0].peak.expected
            >= estimate(QWEN_1_5B, cfg, H100).devices[0].peak.expected
        )


@settings(max_examples=60, deadline=None)
@given(cfg=configs)
def test_checkpointing_and_qlora_never_need_more(cfg: TrainConfig) -> None:
    on = dataclasses.replace(cfg, grad_checkpointing=True)
    assert (
        estimate(QWEN_1_5B, on, H100).devices[0].peak.high
        <= estimate(QWEN_1_5B, dataclasses.replace(cfg, grad_checkpointing=False), H100)
        .devices[0]
        .peak.high
    )
    if cfg.method == "lora":
        q = dataclasses.replace(cfg, method="qlora")
        assert (
            estimate(QWEN_1_5B, q, H100).devices[0].peak.expected
            <= estimate(QWEN_1_5B, cfg, H100).devices[0].peak.expected
        )


@settings(max_examples=60, deadline=None)
@given(cfg=configs, small=st.floats(4, 80), more=st.floats(0, 120))
def test_more_memory_never_worse(cfg: TrainConfig, small: float, more: float) -> None:
    def run(gib: float) -> str:
        spec = DeviceSpec("gpu", gib, "nvidia", Headroom(gpu_memory_free_pct=10))
        return estimate(QWEN_1_5B, cfg, Topology(spec)).verdict

    assert ORDER[run(small + more)] <= ORDER[run(small)]


@settings(max_examples=40, deadline=None)
@given(cfg=configs, mu=st.floats(-1, 1), sigma=st.floats(0, 0.08))
def test_calibration_scales_expected(cfg: TrainConfig, mu: float, sigma: float) -> None:
    plain = estimate(QWEN_1_5B, cfg, H100).devices[0].peak
    cal = estimate(QWEN_1_5B, cfg, H100, Calibration(mu, sigma, 25)).devices[0].peak
    assert cal.expected == pytest.approx(plain.expected * math.exp(mu), rel=1e-2, abs=0.01)
    assert cal.low <= cal.expected <= cal.high
