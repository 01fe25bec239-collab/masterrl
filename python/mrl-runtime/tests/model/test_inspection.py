"""Model inspection against the P03 tiny-llama fixtures and hand-built headers."""

import json
import shutil
import struct
import subprocess
import sys
from pathlib import Path

import make_fixtures
import numpy as np
import pytest
from safetensors.numpy import load_file, save_file

from mrl_runtime.model import (
    MrlError,
    TensorInfo,
    inspect_model,
    read_gguf_header,
    read_safetensors_header,
)
from mrl_runtime.model.inspection import count_params

GOLDEN = json.loads(make_fixtures.GOLDEN_PATH.read_text())


@pytest.fixture(scope="module")
def models(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("models")
    make_fixtures.build_models(out)
    return out


@pytest.fixture
def tiny(models: Path, tmp_path: Path) -> Path:
    """A private copy of tiny-llama that a test may change."""
    return Path(shutil.copytree(models / "tiny-llama", tmp_path / "tiny-llama"))


def test_tiny_llama_matches_golden(models: Path) -> None:
    r = inspect_model(models / "tiny-llama")
    assert r.format == "safetensors"
    assert r.kind == "full_weights"
    assert r.refusal is None
    assert r.param_count == GOLDEN["param_count"]
    assert r.param_count_exact
    assert r.dtype == GOLDEN["dtype"]
    assert r.params_by_dtype == {"float32": GOLDEN["param_count"]}
    assert r.context_length == GOLDEN["context_length"]
    assert r.architecture == GOLDEN["architecture"]
    assert r.model_type == "llama"
    assert r.chat_template_sha256 == GOLDEN["chat_template_sha256"]
    assert r.has_generation_tags
    assert r.shape.vocab_size == GOLDEN["vocab_size"]
    assert r.shape.tie_word_embeddings is True
    assert (r.shape.num_layers, r.shape.num_kv_heads, r.shape.head_dim) == (4, 2, 32)
    assert r.tokenizer_sha256 == {
        name: GOLDEN["files_sha256"][name] for name in ("tokenizer.json", "tokenizer_config.json")
    }
    assert r.weight_bytes == GOLDEN["param_count"] * 4
    assert r.generation.eos_token_ids == (2,)
    assert not r.requires_remote_code
    assert r.rope_scaling is None
    assert r.moe is None
    assert r.vision is None
    roles = {f.path: f.role for f in r.files}
    assert roles["model.safetensors"] == "weights"
    assert roles["config.json"] == "config"
    json.dumps(r.to_dict())  # JSON-ready


def test_pickle_twin_is_refused_without_unpickling(models: Path) -> None:
    r = inspect_model(models / "tiny-llama-pickle")
    assert r.format == "pickle"
    assert r.refusal == "MRL-MODEL-001"
    assert r.kind is None
    assert r.param_count is None
    assert r.tensors == ()
    assert r.architecture == "LlamaForCausalLM"  # config is still read


def test_pickle_next_to_safetensors_is_ignored(tiny: Path, models: Path) -> None:
    shutil.copy(models / "tiny-llama-pickle/pytorch_model.bin", tiny)
    r = inspect_model(tiny)
    assert r.format == "safetensors"
    assert r.refusal is None
    assert any("pickle" in n for n in r.notes)


def test_single_safetensors_file(models: Path) -> None:
    r = inspect_model(models / "tiny-llama/model.safetensors")
    assert r.param_count == GOLDEN["param_count"]
    assert r.architecture is None


def test_sharded_checkpoint(tiny: Path) -> None:
    weights = load_file(tiny / "model.safetensors")
    names = sorted(weights)
    half = len(names) // 2
    shards = {
        "model-00001-of-00002.safetensors": names[:half],
        "model-00002-of-00002.safetensors": names[half:],
    }
    weight_map = {}
    for shard, keys in shards.items():
        save_file({k: weights[k] for k in keys}, str(tiny / shard))
        weight_map |= dict.fromkeys(keys, shard)
    (tiny / "model.safetensors").unlink()
    (tiny / "model.safetensors.index.json").write_text(json.dumps({"weight_map": weight_map}))
    assert inspect_model(tiny).param_count == GOLDEN["param_count"]
    (tiny / "model-00002-of-00002.safetensors").unlink()
    with pytest.raises(MrlError) as err:
        inspect_model(tiny)
    assert err.value.code == "MRL-MODEL-150"


def _header_file(path: Path, header: object, data: bytes = b"") -> Path:
    raw = json.dumps(header).encode()
    path.write_bytes(struct.pack("<Q", len(raw)) + raw + data)
    return path


@pytest.mark.parametrize(
    ("header", "data", "why"),
    [
        ({"a": {"dtype": "F32", "shape": [2], "data_offsets": [0, 8]}}, b"\0" * 4, "size"),
        ({"a": {"dtype": "F99", "shape": [1], "data_offsets": [0, 4]}}, b"\0" * 4, "dtype"),
        ({"a": {"dtype": "F32", "shape": [-1], "data_offsets": [0, 4]}}, b"\0" * 4, "shape"),
        (
            {
                "a": {"dtype": "F32", "shape": [1], "data_offsets": [0, 4]},
                "b": {"dtype": "F32", "shape": [1], "data_offsets": [0, 4]},
            },
            b"\0" * 8,
            "overlap",
        ),
        ({"a": {"dtype": "F32", "shape": [1], "data_offsets": [4, 8]}}, b"\0" * 8, "gap"),
        ([1, 2], b"", "not an object"),
        ({"__metadata__": {"x": 1}}, b"", "metadata"),
    ],
)
def test_bad_safetensors_headers_are_refused(
    tmp_path: Path, header: object, data: bytes, why: str
) -> None:
    path = _header_file(tmp_path / "m.safetensors", header, data)
    with pytest.raises(MrlError) as err:
        read_safetensors_header(path)
    assert err.value.code == "MRL-MODEL-150", why


def test_truncated_and_oversized_safetensors(tmp_path: Path) -> None:
    short = tmp_path / "short.safetensors"
    short.write_bytes(b"\x01\x02")
    huge = tmp_path / "huge.safetensors"
    huge.write_bytes(struct.pack("<Q", 2**40) + b"{}")
    garbage = tmp_path / "garbage.safetensors"
    garbage.write_bytes(struct.pack("<Q", 4) + b"\xff\xfe{}")
    for path in (short, huge, garbage):
        with pytest.raises(MrlError):
            read_safetensors_header(path)


def _gguf_string(s: str) -> bytes:
    raw = s.encode()
    return struct.pack("<Q", len(raw)) + raw


def write_gguf(path: Path) -> Path:
    """A minimal GGUF v3 file: llama metadata, a token array and two tensors (no data)."""
    kv = [
        ("general.architecture", 8, _gguf_string("llama")),
        ("llama.context_length", 4, struct.pack("<I", 4096)),
        ("llama.block_count", 4, struct.pack("<I", 2)),
        ("llama.embedding_length", 4, struct.pack("<I", 64)),
        ("llama.attention.head_count", 4, struct.pack("<I", 4)),
        ("tokenizer.chat_template", 8, _gguf_string("{{ messages[0]['content'] }}")),
        ("general.file_type", 4, struct.pack("<I", 15)),
    ]
    body = b"".join(_gguf_string(k) + struct.pack("<I", t) + v for k, t, v in kv)
    tokens = [_gguf_string(t) for t in ("a", "b", "c")]
    body += _gguf_string("tokenizer.ggml.tokens") + struct.pack("<IIQ", 9, 8, 3) + b"".join(tokens)
    tensors = [("token_embd.weight", (64, 3), 12), ("blk.0.attn_q.weight", (64, 64), 12)]
    for name, dims, kind in tensors:
        body += (
            _gguf_string(name) + struct.pack("<I", len(dims)) + struct.pack(f"<{len(dims)}Q", *dims)
        )
        body += struct.pack("<IQ", kind, 0)
    path.write_bytes(b"GGUF" + struct.pack("<IQQ", 3, len(tensors), len(kv) + 1) + body)
    return path


def test_gguf_is_inference_only(tmp_path: Path) -> None:
    path = write_gguf(tmp_path / "m.gguf")
    header = read_gguf_header(path)
    assert header.array_lengths["tokenizer.ggml.tokens"] == 3
    r = inspect_model(path)
    assert r.format == "gguf"
    assert r.kind == "inference_only"
    assert r.inference_only_code == "MRL-MODEL-151"
    assert r.quantization == "gguf-Q4_K"
    assert r.param_count == 64 * 3 + 64 * 64
    assert r.context_length == 4096
    assert r.shape.vocab_size == 3
    assert r.architecture == "llama"
    assert r.chat_template_sha256 is not None


def test_broken_gguf_is_refused(tmp_path: Path) -> None:
    good = write_gguf(tmp_path / "good.gguf").read_bytes()
    cases = {
        "magic": b"GGUX" + good[4:],
        "version": good[:4] + struct.pack("<I", 9) + good[8:],
        "truncated": good[: len(good) // 2],
        "huge_count": good[:8] + struct.pack("<Q", 2**62) + good[16:],
    }
    for name, data in cases.items():
        path = tmp_path / f"{name}.gguf"
        path.write_bytes(data)
        with pytest.raises(MrlError) as err:
            read_gguf_header(path)
        assert err.value.code == "MRL-MODEL-150", name


def test_onnx_is_inference_only(tmp_path: Path) -> None:
    (tmp_path / "model.onnx").write_bytes(b"\x08\x07")
    r = inspect_model(tmp_path)
    assert (r.format, r.kind) == ("onnx", "inference_only")


def test_no_weights(tmp_path: Path) -> None:
    (tmp_path / "config.json").write_text("{}")
    r = inspect_model(tmp_path)
    assert (r.format, r.kind) == ("none", None)


def test_missing_path_and_bad_config(tmp_path: Path, tiny: Path) -> None:
    with pytest.raises(MrlError):
        inspect_model(tmp_path / "nope")
    (tiny / "config.json").write_text("{not json")
    with pytest.raises(MrlError) as err:
        inspect_model(tiny)
    assert err.value.code == "MRL-MODEL-150"
    (tiny / "config.json").write_text("[" * 100_000)
    with pytest.raises(MrlError):
        inspect_model(tiny)


def _set_config(folder: Path, **changes: object) -> None:
    path = folder / "config.json"
    path.write_text(json.dumps(json.loads(path.read_text()) | changes))


def test_auto_map_needs_remote_code(tiny: Path) -> None:
    _set_config(tiny, auto_map={"AutoModelForCausalLM": "modeling_x.XForCausalLM"})
    r = inspect_model(tiny)
    assert r.requires_remote_code
    assert r.remote_code_code == "MRL-MODEL-002"
    assert r.auto_map == {"AutoModelForCausalLM": "modeling_x.XForCausalLM"}


def test_python_file_needs_remote_code(tiny: Path) -> None:
    (tiny / "modeling_custom.py").write_text("x = 1\n")
    r = inspect_model(tiny)
    assert r.requires_remote_code
    assert {f.path: f.role for f in r.files}["modeling_custom.py"] == "code"


def test_rope_moe_vision_and_generation(tiny: Path) -> None:
    _set_config(
        tiny,
        rope_scaling={"rope_type": "yarn", "factor": 4.0, "original_max_position_embeddings": 128},
        num_local_experts=8,
        num_experts_per_tok=2,
        vision_config={"model_type": "siglip_vision_model", "hidden_size": 32, "patch_size": 14},
    )
    (tiny / "generation_config.json").write_text(
        json.dumps({"eos_token_id": [2, 7], "max_length": 64})
    )
    r = inspect_model(tiny)
    assert r.rope_scaling is not None
    assert (r.rope_scaling.type, r.rope_scaling.factor, r.rope_scaling.original_context_length) == (
        "yarn",
        4.0,
        128,
    )
    assert r.moe is not None
    assert (r.moe.num_experts, r.moe.experts_per_token) == (8, 2)
    assert r.vision is not None
    assert (r.vision.model_type, r.vision.hidden_size, r.vision.patch_size) == (
        "siglip_vision_model",
        32,
        14,
    )
    assert r.generation.eos_token_ids == (2, 7)
    assert r.generation.max_length == 64


def test_moe_active_params(tmp_path: Path) -> None:
    weights: dict[str, np.ndarray] = {
        f"model.layers.0.mlp.experts.{i}.w1.weight": np.zeros((4, 4), np.float32) for i in range(4)
    }
    weights["model.embed_tokens.weight"] = np.zeros((10, 4), np.float32)
    save_file(weights, str(tmp_path / "model.safetensors"))
    (tmp_path / "config.json").write_text(
        json.dumps({"num_local_experts": 4, "num_experts_per_tok": 1})
    )
    r = inspect_model(tmp_path)
    assert r.param_count == 4 * 16 + 40
    assert r.active_param_count == 16 + 40


def test_vlm_text_config_and_vision_params(tmp_path: Path) -> None:
    weights = {
        "vision_tower.encoder.weight": np.zeros((8, 8), np.float32),
        "language_model.model.embed_tokens.weight": np.zeros((16, 8), np.float32),
    }
    save_file(weights, str(tmp_path / "model.safetensors"))
    config = {
        "architectures": ["LlavaForConditionalGeneration"],
        "model_type": "llava",
        "text_config": {"hidden_size": 8, "num_hidden_layers": 1, "max_position_embeddings": 1024},
        "vision_config": {"model_type": "clip_vision_model"},
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    r = inspect_model(tmp_path)
    assert r.context_length == 1024
    assert r.shape.hidden_size == 8
    assert r.vision is not None
    assert r.vision.param_count == 64


def test_prequantized_checkpoints_are_inference_only(tiny: Path) -> None:
    _set_config(tiny, quantization_config={"quant_method": "gptq", "bits": 4})
    r = inspect_model(tiny)
    assert (r.kind, r.quantization, r.inference_only_code) == (
        "inference_only",
        "gptq-4bit",
        "MRL-MODEL-151",
    )
    _set_config(tiny, quantization_config={"quant_method": "bitsandbytes", "load_in_4bit": True})
    assert inspect_model(tiny).quantization == "bitsandbytes-4bit"
    _set_config(tiny, quantization_config=None, quantization={"group_size": 64, "bits": 4})
    assert inspect_model(tiny).quantization == "mlx-4bit"


def test_packed_quantized_param_counts() -> None:
    gptq = (
        TensorInfo("l.q_proj.qweight", "I32", (8, 64)),  # 8 x 32/4 = 64 rows unpacked
        TensorInfo("l.q_proj.qzeros", "I32", (1, 8)),
        TensorInfo("l.q_proj.scales", "F16", (1, 64)),
        TensorInfo("l.q_proj.g_idx", "I32", (64,)),
    )
    assert count_params(gptq, "gptq-4bit") == (64 * 64, False)
    bnb = (
        TensorInfo("l.weight", "U8", (2048, 1)),
        TensorInfo("l.weight.absmax", "F32", (64,)),
        TensorInfo("l.weight.quant_state.bitsandbytes__nf4", "U8", (100,)),
        TensorInfo("norm.weight", "BF16", (64,)),
    )
    assert count_params(bnb, "bitsandbytes-4bit") == (4096 + 64, False)
    mlx = (
        TensorInfo("l.weight", "U32", (64, 8)),
        TensorInfo("l.scales", "BF16", (64, 1)),
        TensorInfo("l.biases", "BF16", (64, 1)),
    )
    assert count_params(mlx, "mlx-4bit") == (64 * 64, False)


def test_peft_adapter(tmp_path: Path) -> None:
    save_file(
        {"base_model.model.q_proj.lora_A.weight": np.zeros((4, 16), np.float32)},
        str(tmp_path / "adapter_model.safetensors"),
    )
    (tmp_path / "adapter_config.json").write_text(
        json.dumps(
            {
                "peft_type": "LORA",
                "base_model_name_or_path": "org/base",
                "r": 4,
                "lora_alpha": 8,
                "target_modules": ["q_proj"],
            }
        )
    )
    r = inspect_model(tmp_path)
    assert r.kind == "adapter"
    assert r.adapter is not None
    assert (r.adapter.base_model, r.adapter.r, r.adapter.target_modules) == (
        "org/base",
        4,
        ("q_proj",),
    )
    assert r.param_count == 64
    assert {f.path: f.role for f in r.files}["adapter_model.safetensors"] == "adapter"


def test_chat_template_sources(tiny: Path) -> None:
    config_path = tiny / "tokenizer_config.json"
    config = json.loads(config_path.read_text())
    config["chat_template"] = [
        {"name": "default", "template": "A"},
        {"name": "tool_use", "template": "B"},
    ]
    config_path.write_text(json.dumps(config))
    r = inspect_model(tiny)
    assert r.chat_template_names == ("default", "tool_use")
    assert not r.has_generation_tags
    del config["chat_template"]
    config_path.write_text(json.dumps(config))
    (tiny / "chat_template.jinja").write_text("C")
    assert inspect_model(tiny).chat_template_names == ("default",)


def test_weight_hashing_is_opt_in(models: Path) -> None:
    plain = {f.path: f.sha256 for f in inspect_model(models / "tiny-llama").files}
    hashed = {
        f.path: f.sha256 for f in inspect_model(models / "tiny-llama", hash_weights=True).files
    }
    assert plain["model.safetensors"] is None
    assert hashed["model.safetensors"] == make_fixtures.sha256(
        (models / "tiny-llama/model.safetensors").read_bytes()
    )


def test_no_ml_framework_is_imported() -> None:
    code = (
        "import sys, mrl_runtime.model, mrl_runtime.data.validate, mrl_runtime.data.readers,"
        " mrl_runtime.data.tokens, mrl_runtime.estimate;"
        "heavy = {'torch', 'transformers', 'trl', 'vllm', 'pyarrow', 'tokenizers'}"
        " & set(sys.modules);"
        "assert not heavy, heavy"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
