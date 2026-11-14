"""Build the CPU test fixtures.

    uv run python tools/fixtures/make_fixtures.py          # write fixtures
    uv run python tools/fixtures/make_fixtures.py --check  # verify against GOLDEN.json

Models go to tests/fixtures/models/.cache/ (never committed):
  tiny-llama/         random-init Llama, safetensors, BPE tokenizer, chat template
  tiny-llama-pickle/  same weights as a pickle `pytorch_model.bin`, for refusal tests
Datasets go to tests/fixtures/datasets/ (committed) and must match the generator.
"""

import argparse
import hashlib
import json
import pickle
import platform
import sys
import tempfile
from pathlib import Path
from typing import Any

import dataset_fixtures
import numpy as np
from safetensors.numpy import save_file
from tokenizers import Tokenizer, decoders, models, pre_tokenizers, processors, trainers

REPO = Path(__file__).resolve().parents[2]
MODELS_DIR = REPO / "tests/fixtures/models"
CACHE_DIR = MODELS_DIR / ".cache"
DATASETS_DIR = REPO / "tests/fixtures/datasets"
GOLDEN_PATH = MODELS_DIR / "GOLDEN.json"

SEED = 1234
PAD, BOS, EOS = "<|pad|>", "<|im_start|>", "<|im_end|>"
CONFIG: dict[str, Any] = {
    "architectures": ["LlamaForCausalLM"],
    "model_type": "llama",
    "vocab_size": 384,
    "hidden_size": 128,
    "intermediate_size": 512,
    "num_hidden_layers": 4,
    "num_attention_heads": 4,
    "num_key_value_heads": 2,
    "head_dim": 32,
    "max_position_embeddings": 512,
    "hidden_act": "silu",
    "rms_norm_eps": 1e-6,
    "rope_parameters": {"rope_type": "default", "rope_theta": 10000.0},
    "attention_bias": False,
    "attention_dropout": 0.0,
    "mlp_bias": False,
    "initializer_range": 0.02,
    "tie_word_embeddings": True,
    "pad_token_id": 0,
    "bos_token_id": 1,
    "eos_token_id": 2,
    "dtype": "float32",
    "use_cache": True,
}
GENERATION_CONFIG = {"bos_token_id": 1, "eos_token_id": 2, "pad_token_id": 0}
# ChatML-style; {% generation %} marks the tokens assistant-only loss trains on.
CHAT_TEMPLATE = (
    "{%- for message in messages -%}"
    "{%- if message['role'] == 'assistant' -%}"
    "{{- '<|im_start|>assistant\\n' -}}"
    "{%- generation -%}{{- message['content'] + '<|im_end|>' -}}{%- endgeneration -%}"
    "{{- '\\n' -}}"
    "{%- else -%}"
    "{{- '<|im_start|>' + message['role'] + '\\n' + message['content'] + '<|im_end|>\\n' -}}"
    "{%- endif -%}"
    "{%- endfor -%}"
    "{%- if add_generation_prompt -%}{{- '<|im_start|>assistant\\n' -}}{%- endif -%}"
)
TOKENIZER_CONFIG = {
    "tokenizer_class": "PreTrainedTokenizerFast",
    "model_max_length": CONFIG["max_position_embeddings"],
    "pad_token": PAD,
    "bos_token": None,
    "eos_token": EOS,
    "unk_token": None,
    "clean_up_tokenization_spaces": False,
    "chat_template": CHAT_TEMPLATE,
}
TEXT_FILES = ("config.json", "generation_config.json", "tokenizer.json", "tokenizer_config.json")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, indent=2, ensure_ascii=False) + "\n").encode()


def platform_key() -> str:
    return f"{sys.platform}-{platform.machine().lower()}"


def build_tokenizer() -> bytes:
    """Byte-level BPE trained on the fixture dataset text."""
    tok = Tokenizer(models.BPE())
    tok.pre_tokenizer = pre_tokenizers.ByteLevel(add_prefix_space=False)
    tok.decoder = decoders.ByteLevel()
    tok.post_processor = processors.ByteLevel(trim_offsets=False)
    trainer = trainers.BpeTrainer(
        vocab_size=CONFIG["vocab_size"],
        min_frequency=2,
        special_tokens=[PAD, BOS, EOS],
        initial_alphabet=pre_tokenizers.ByteLevel.alphabet(),
        show_progress=False,
    )
    corpus = ["system", "user", "assistant", dataset_fixtures.SYSTEM_PROMPT]
    corpus += [m["content"] for r in dataset_fixtures.sft_train() for m in r["messages"]]
    corpus += [r["input"] for r in dataset_fixtures.eval_held_out()]
    tok.train_from_iterator(corpus, trainer)
    assert tok.get_vocab_size() == CONFIG["vocab_size"], tok.get_vocab_size()
    assert [tok.token_to_id(t) for t in (PAD, BOS, EOS)] == [0, 1, 2]
    return (tok.to_str(pretty=True) + "\n").encode()


def build_weights() -> dict[str, np.ndarray]:
    """Llama init: normal(0, initializer_range) for linears, ones for norms; tied lm_head."""
    c = CONFIG
    rng = np.random.default_rng(SEED)
    hidden, inter, head_dim = c["hidden_size"], c["intermediate_size"], c["head_dim"]
    kv = c["num_key_value_heads"] * head_dim

    def normal(*shape: int) -> np.ndarray:
        return rng.standard_normal(shape, dtype=np.float32) * np.float32(c["initializer_range"])

    weights = {"model.embed_tokens.weight": normal(c["vocab_size"], hidden)}
    for i in range(c["num_hidden_layers"]):
        p = f"model.layers.{i}."
        weights |= {
            p + "self_attn.q_proj.weight": normal(c["num_attention_heads"] * head_dim, hidden),
            p + "self_attn.k_proj.weight": normal(kv, hidden),
            p + "self_attn.v_proj.weight": normal(kv, hidden),
            p + "self_attn.o_proj.weight": normal(hidden, c["num_attention_heads"] * head_dim),
            p + "mlp.gate_proj.weight": normal(inter, hidden),
            p + "mlp.up_proj.weight": normal(inter, hidden),
            p + "mlp.down_proj.weight": normal(hidden, inter),
            p + "input_layernorm.weight": np.ones(hidden, np.float32),
            p + "post_attention_layernorm.weight": np.ones(hidden, np.float32),
        }
    weights["model.norm.weight"] = np.ones(hidden, np.float32)
    return weights


def build_models(out: Path) -> dict[str, Any]:
    """Write both model folders under `out`; return the facts GOLDEN.json pins."""
    text = {
        "config.json": json_bytes(CONFIG),
        "generation_config.json": json_bytes(GENERATION_CONFIG),
        "tokenizer.json": build_tokenizer(),
        "tokenizer_config.json": json_bytes(TOKENIZER_CONFIG),
    }
    weights = build_weights()
    safe_dir, pickle_dir = out / "tiny-llama", out / "tiny-llama-pickle"
    for d in (safe_dir, pickle_dir):
        d.mkdir(parents=True, exist_ok=True)
        for name, data in text.items():
            (d / name).write_bytes(data)
    save_file(weights, str(safe_dir / "model.safetensors"), metadata={"format": "pt"})
    (pickle_dir / "pytorch_model.bin").write_bytes(pickle.dumps(weights, protocol=4))
    return {
        "model": "tiny-llama",
        "architecture": CONFIG["architectures"][0],
        "param_count": sum(w.size for w in weights.values()),
        "dtype": CONFIG["dtype"],
        "context_length": CONFIG["max_position_embeddings"],
        "vocab_size": CONFIG["vocab_size"],
        "seed": SEED,
        "chat_template_sha256": sha256(CHAT_TEMPLATE.encode()),
        "files_sha256": {name: sha256(data) for name, data in text.items()},
        "weights_sha256": {
            platform_key(): {
                "model.safetensors": sha256((safe_dir / "model.safetensors").read_bytes()),
                "pytorch_model.bin": sha256((pickle_dir / "pytorch_model.bin").read_bytes()),
            }
        },
    }


def dataset_bytes() -> dict[str, bytes]:
    files = {
        name: b"".join(line + b"\n" for line in lines)
        for name, lines in dataset_fixtures.all_files().items()
    }
    files["DEFECTS.json"] = json_bytes(dataset_fixtures.defects_manifest())
    return files


def check() -> list[str]:
    """Rebuild everything into a temp dir and compare. Returns a list of problems."""
    problems: list[str] = []
    golden = json.loads(GOLDEN_PATH.read_text())
    with tempfile.TemporaryDirectory() as tmp:
        facts = build_models(Path(tmp))
    pinned_weights = golden.pop("weights_sha256")
    built_weights = facts.pop("weights_sha256")[platform_key()]
    for key in golden.keys() | facts.keys():
        if golden.get(key) != facts.get(key):
            problems.append(
                f"GOLDEN.json {key}: expected {golden.get(key)}, built {facts.get(key)}"
            )
    if platform_key() in pinned_weights and pinned_weights[platform_key()] != built_weights:
        problems.append(f"weights differ from GOLDEN.json for {platform_key()}")
    expected = dataset_bytes()
    for name, data in expected.items():
        path = DATASETS_DIR / name
        if not path.exists() or path.read_bytes() != data:
            problems.append(f"{path.relative_to(REPO)} is stale; re-run make_fixtures.py")
    extra = {p.name for p in DATASETS_DIR.iterdir()} - expected.keys()
    problems += [f"tests/fixtures/datasets/{n} is not made by the generator" for n in sorted(extra)]
    return problems


def write() -> None:
    facts = build_models(CACHE_DIR)
    golden = json.loads(GOLDEN_PATH.read_text()) if GOLDEN_PATH.exists() else {}
    facts["weights_sha256"] = golden.get("weights_sha256", {}) | facts["weights_sha256"]
    GOLDEN_PATH.write_bytes(json_bytes(facts))
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    for name, data in dataset_bytes().items():
        (DATASETS_DIR / name).write_bytes(data)
    print(f"wrote {CACHE_DIR.relative_to(REPO)}, {GOLDEN_PATH.relative_to(REPO)}, datasets")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the CPU test fixtures.")
    parser.add_argument("--check", action="store_true", help="verify, write nothing")
    if not parser.parse_args().check:
        write()
        return 0
    problems = check()
    for p in problems:
        print(f"error: {p}", file=sys.stderr)
    if not problems:
        print(f"fixtures match GOLDEN.json and committed datasets ({platform_key()})")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
