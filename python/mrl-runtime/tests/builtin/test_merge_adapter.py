"""The merge_adapter task: LoRA merged into the base without torch, and its refusals."""

import json
import shutil
from pathlib import Path
from typing import Any

import make_fixtures
import numpy as np
import pytest
from safetensors.numpy import load_file, save_file

from mrl_runtime.builtin.import_model import INSPECTION_FILE
from mrl_runtime.builtin.merge_adapter import (
    MERGE_ADAPTER,
    SafetensorsFile,
    bf16_to_f32,
    f32_to_bf16,
)
from mrl_runtime.builtin.tasks import TaskContext, TaskOutput
from mrl_runtime.model import inspect_model

TARGET = "model.layers.0.self_attn.q_proj"


@pytest.fixture(scope="module")
def models(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("models")
    make_fixtures.build_models(out)
    return out


def make_adapter(
    folder: Path, base: Path, *, r: int = 4, alpha: float = 8.0, **cfg: Any
) -> dict[str, np.ndarray]:
    w = load_file(base / "model.safetensors")[f"{TARGET}.weight"]
    rng = np.random.default_rng(7)
    a = rng.standard_normal((r, w.shape[1])).astype(np.float32)
    b = rng.standard_normal((w.shape[0], r)).astype(np.float32)
    folder.mkdir(parents=True)
    tensors = {
        f"base_model.model.{TARGET}.lora_A.weight": a,
        f"base_model.model.{TARGET}.lora_B.weight": b,
    }
    save_file(tensors, folder / "adapter_model.safetensors")
    config = {"peft_type": "LORA", "r": r, "lora_alpha": alpha, "target_modules": ["q_proj"],
              "base_model_name_or_path": "tiny-llama"} | cfg  # fmt: skip
    (folder / "adapter_config.json").write_text(json.dumps(config))
    return {"a": a, "b": b, "w": w}


def run(staged: dict[str, Path], out: Path, dtype: str = "auto") -> dict[str, object]:
    inputs = {"dtype": dtype}
    assert MERGE_ADAPTER.validate(inputs, staged) == []
    outs = [
        e
        for e in MERGE_ADAPTER.run(TaskContext(inputs, staged, out, lambda: False))
        if isinstance(e, TaskOutput)
    ]
    assert outs[-1].path == INSPECTION_FILE
    return json.loads((out / INSPECTION_FILE).read_text())


def test_merge_adds_the_scaled_lora_update(models: Path, tmp_path: Path) -> None:
    base = models / "tiny-llama"
    parts = make_adapter(tmp_path / "adapter", base)
    r = run({"base": base, "adapter": tmp_path / "adapter"}, tmp_path / "out")
    assert r["kind"] == "full_weights"
    assert "error" not in r
    merged = load_file(tmp_path / "out/model/model.safetensors")
    original = load_file(base / "model.safetensors")
    expect = parts["w"] + (8.0 / 4) * (parts["b"] @ parts["a"])
    np.testing.assert_allclose(merged[f"{TARGET}.weight"], expect, rtol=1e-6, atol=1e-6)
    for name, value in original.items():
        if name != f"{TARGET}.weight":
            np.testing.assert_array_equal(merged[name], value)
    assert (tmp_path / "out/model/tokenizer.json").read_bytes() == (
        base / "tokenizer.json"
    ).read_bytes()
    assert r["param_count"] == inspect_model(base).param_count


def test_rslora_and_bfloat16_output(models: Path, tmp_path: Path) -> None:
    base = models / "tiny-llama"
    parts = make_adapter(tmp_path / "adapter", base, use_rslora=True)
    r = run({"base": base, "adapter": tmp_path / "adapter"}, tmp_path / "out", "bfloat16")
    assert r["dtype"] == "bfloat16"
    config = json.loads((tmp_path / "out/model/config.json").read_text())
    assert config["torch_dtype"] == "bfloat16"
    raw = (tmp_path / "out/model/model.safetensors").read_bytes()
    assert b'"BF16"' in raw
    assert b'"F32"' not in raw
    merged = SafetensorsFile(tmp_path / "out/model/model.safetensors").float32(f"{TARGET}.weight")
    expect = parts["w"] + (8.0 / 2) * (parts["b"] @ parts["a"])  # alpha / sqrt(r)
    np.testing.assert_allclose(merged, expect, rtol=1 / 128, atol=1e-6)


def test_bfloat16_conversion_rounds_to_nearest() -> None:
    x = np.array([1.0, -2.5, 3.140625, 1e-3, 65504.0], dtype=np.float32)
    np.testing.assert_allclose(bf16_to_f32(f32_to_bf16(x)), x, rtol=1 / 128)
    # 1 + 2^-8 sits halfway between two bfloat16 values: ties go to the even one (1.0).
    assert bf16_to_f32(f32_to_bf16(np.array([1 + 2**-8], np.float32)))[0] == 1.0


@pytest.mark.parametrize(
    ("change", "words"),
    [
        ({"use_dora": True}, "DoRA"),
        ({"rank_pattern": {"q_proj": 8}}, "per-module"),
    ],
)
def test_adapters_that_cannot_merge(
    models: Path, tmp_path: Path, change: dict[str, Any], words: str
) -> None:
    make_adapter(tmp_path / "adapter", models / "tiny-llama", **change)
    r = run({"base": models / "tiny-llama", "adapter": tmp_path / "adapter"}, tmp_path / "out")
    error = r["error"]
    assert isinstance(error, dict)
    assert error["code"] == "MRL-MODEL-264"
    assert words in error["message"]
    assert not (tmp_path / "out/model").exists()


def test_quantized_base_is_refused(models: Path, tmp_path: Path) -> None:
    base = Path(shutil.copytree(models / "tiny-llama", tmp_path / "base"))
    config = json.loads((base / "config.json").read_text())
    config["quantization_config"] = {"quant_method": "bitsandbytes", "load_in_4bit": True}
    (base / "config.json").write_text(json.dumps(config))
    make_adapter(tmp_path / "adapter", models / "tiny-llama")
    r = run({"base": base, "adapter": tmp_path / "adapter"}, tmp_path / "out")
    assert r["error"]["code"] == "MRL-MODEL-264"  # type: ignore[index]


def test_adapter_for_a_missing_weight_is_refused(models: Path, tmp_path: Path) -> None:
    make_adapter(tmp_path / "adapter", models / "tiny-llama")
    st = tmp_path / "adapter/adapter_model.safetensors"
    t = load_file(st)
    save_file({k.replace("layers.0", "layers.99"): v for k, v in t.items()}, st)
    r = run({"base": models / "tiny-llama", "adapter": tmp_path / "adapter"}, tmp_path / "out")
    assert "layers.99" in r["error"]["message"]  # type: ignore[index]


def test_merged_logits_match_peft(models: Path, tmp_path: Path) -> None:
    torch = pytest.importorskip("torch")
    transformers = pytest.importorskip("transformers")
    peft = pytest.importorskip("peft")
    base = models / "tiny-llama"
    make_adapter(tmp_path / "adapter", base)
    run({"base": base, "adapter": tmp_path / "adapter"}, tmp_path / "out")
    ids = torch.tensor([[1, 5, 9, 13, 2]])
    with torch.no_grad():
        model = transformers.AutoModelForCausalLM.from_pretrained(base)
        want = peft.PeftModel.from_pretrained(model, tmp_path / "adapter")(ids).logits
        got = transformers.AutoModelForCausalLM.from_pretrained(tmp_path / "out/model")(ids).logits
    torch.testing.assert_close(got, want, rtol=1e-4, atol=1e-4)
