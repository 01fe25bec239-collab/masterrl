"""Inspect a model folder (or one weight file) without loading weights or importing ML code.

`inspect_model(path)` returns a `ModelInspection`: format and kind, the safety gates
(pickle refused, remote code, inference-only), architecture facts from `config.json`,
parameter counts from weight headers, and hashes of the tokenizer files and chat template.
"""

import dataclasses
import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import Any, Literal

from mrl_runtime.model._json import Json, arr, integer, number, obj, text
from mrl_runtime.model.errors import (
    INFERENCE_ONLY,
    MODEL_UNREADABLE,
    PICKLE_REFUSED,
    REMOTE_CODE,
    MrlError,
)
from mrl_runtime.model.headers import (
    GgufHeader,
    Scalar,
    TensorInfo,
    read_gguf_header,
    read_safetensors_header,
)

type Format = Literal["safetensors", "gguf", "onnx", "pickle", "none"]
type Kind = Literal["full_weights", "adapter", "inference_only"]
type Role = Literal["weights", "config", "tokenizer", "adapter", "code", "other"]

PICKLE_SUFFIXES = (".bin", ".pt", ".pth", ".ckpt", ".pkl", ".pickle")
TOKENIZER_FILES = (
    "tokenizer.json",
    "tokenizer_config.json",
    "tokenizer.model",
    "spiece.model",
    "vocab.json",
    "vocab.txt",
    "merges.txt",
    "special_tokens_map.json",
    "added_tokens.json",
    "chat_template.jinja",
    "chat_template.json",
)
CONFIG_FILES = ("config.json", "generation_config.json", "preprocessor_config.json")
MAX_JSON_FILE = 64 * 1024 * 1024
MAX_SANE_CONTEXT = 100_000_000
HASH_CHUNK = 1024 * 1024

# safetensors dtype -> the name engines use.
DTYPE_NAMES = {
    "F64": "float64", "F32": "float32", "F16": "float16", "BF16": "bfloat16",
    "F8_E4M3": "float8_e4m3fn", "F8_E5M2": "float8_e5m2", "F8_E8M0": "float8_e8m0fnu",
    "F6_E2M3": "float6_e2m3", "F6_E3M2": "float6_e3m2", "F4": "float4_e2m1",
    "I64": "int64", "I32": "int32", "I16": "int16", "I8": "int8",
    "U64": "uint64", "U32": "uint32", "U16": "uint16", "U8": "uint8",
    "BOOL": "bool", "C64": "complex64",
}  # fmt: skip

# Tensors that hold quantization bookkeeping, not model parameters.
QUANT_AUX = re.compile(
    r"\.(absmax|quant_map|nested_absmax|nested_quant_map|quant_state\.[\w.]+|SCB|weight_format"
    r"|scales|qzeros|g_idx|biases|weight_scale|weight_scale_inv|input_scale|zeros"
    r"|weight_zero_point|weight_shape|weight_global_scale|input_global_scale)$"
)
VISION_PARTS = ("vision_tower", "vision_model", "visual.", "vision_encoder")
EXPERT_PART = re.compile(r"\.experts\.")
GENERATION_TAG = re.compile(r"{%-?\s*generation\s*-?%}")


@dataclass(frozen=True)
class RopeScaling:
    """How the model stretches its position encoding (from `rope_scaling`/`rope_parameters`)."""

    type: str
    factor: float | None
    original_context_length: int | None


@dataclass(frozen=True)
class MoeInfo:
    """Mixture-of-experts layout."""

    num_experts: int
    experts_per_token: int | None
    shared_experts: int
    expert_intermediate_size: int | None


@dataclass(frozen=True)
class VisionTower:
    """The image encoder of a vision-language model."""

    model_type: str | None
    hidden_size: int | None
    num_layers: int | None
    image_size: int | None
    patch_size: int | None
    param_count: int | None


@dataclass(frozen=True)
class Shape:
    """Text-model sizes the estimator needs (Doc 07 §6.2 notation in brackets)."""

    hidden_size: int | None  # h
    num_layers: int | None  # L
    num_attention_heads: int | None  # a
    num_kv_heads: int | None  # n_kv
    head_dim: int | None  # d_head
    intermediate_size: int | None
    vocab_size: int | None  # V
    tie_word_embeddings: bool | None


@dataclass(frozen=True)
class AdapterInfo:
    """A PEFT adapter folder (`adapter_config.json`)."""

    peft_type: str | None
    base_model: str | None
    r: int | None
    alpha: float | None
    target_modules: tuple[str, ...]
    use_dora: bool


@dataclass(frozen=True)
class GenerationDefaults:
    eos_token_ids: tuple[int, ...]
    bos_token_id: int | None
    pad_token_id: int | None
    max_length: int | None


@dataclass(frozen=True)
class ModelFile:
    path: str  # relative to the model folder, `/`-separated
    size: int
    role: Role
    sha256: str | None


@dataclass(frozen=True)
class ModelInspection:
    """Everything the app knows about a model without loading its weights.

    `refusal` is a catalog error code when the app must not use the files at all
    (pickle weights: MRL-MODEL-001). `inference_only_code` (MRL-MODEL-151) is set for formats
    that cannot be trained (GGUF, ONNX, pre-quantized AWQ/GPTQ/FP8/bitsandbytes/MLX exports).
    `requires_remote_code` is True when the model ships its own Python code (MRL-MODEL-002).
    """

    path: str
    format: Format
    kind: Kind | None
    refusal: str | None
    inference_only_code: str | None
    quantization: str | None
    requires_remote_code: bool
    remote_code_code: str | None
    auto_map: dict[str, str]
    architecture: str | None
    architectures: tuple[str, ...]
    model_type: str | None
    context_length: int | None
    rope_scaling: RopeScaling | None
    moe: MoeInfo | None
    vision: VisionTower | None
    adapter: AdapterInfo | None
    shape: Shape
    param_count: int | None
    param_count_exact: bool
    active_param_count: int | None
    dtype: str | None
    params_by_dtype: dict[str, int]
    weight_bytes: int
    tensors: tuple[TensorInfo, ...]
    generation: GenerationDefaults
    chat_template_sha256: str | None
    chat_template_names: tuple[str, ...]
    has_generation_tags: bool
    tokenizer_sha256: dict[str, str]
    files: tuple[ModelFile, ...]
    notes: tuple[str, ...] = field(default=())

    def to_dict(self) -> dict[str, Any]:
        """Plain JSON-ready dict (tensor list included)."""
        return dataclasses.asdict(self)


def _bad(path: Path, why: str) -> MrlError:
    return MrlError(
        MODEL_UNREADABLE, "The app could not read this model's files.", f"{path.name}: {why}"
    )


def read_json(path: Path) -> Json:
    """A JSON object file, size-capped; MRL-MODEL-150 if it is not one."""
    if path.stat().st_size > MAX_JSON_FILE:
        raise _bad(path, "file is too large")
    try:
        value: object = json.loads(path.read_bytes().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise _bad(path, "not valid JSON") from exc
    out = obj(value)
    if out is None:
        raise _bad(path, "not a JSON object")
    return out


def _optional_json(folder: Path, name: str) -> Json:
    p = folder / name
    return read_json(p) if p.is_file() else {}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(HASH_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _role(rel: str, adapter: bool) -> Role:
    name = rel.rsplit("/", 1)[-1]
    if name.endswith(".py"):
        return "code"
    if name == "adapter_config.json" or (adapter and name.startswith("adapter_model")):
        return "adapter"
    if name in TOKENIZER_FILES:
        return "tokenizer"
    if name in CONFIG_FILES or name.endswith(".safetensors.index.json"):
        return "config"
    if name.endswith((".safetensors", ".gguf", ".onnx", *PICKLE_SUFFIXES)):
        return "weights"
    return "other"


def _list_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    return sorted(
        p
        for p in root.rglob("*")
        if p.is_file() and not any(part.startswith(".") for part in p.relative_to(root).parts)
    )


def chat_templates(folder: Path, tokenizer_config: Json) -> dict[str, str]:
    """Named chat templates ("default" first) from tokenizer_config.json or template files."""
    found: dict[str, str] = {}
    value = tokenizer_config.get("chat_template")
    if isinstance(value, str):
        found["default"] = value
    for item in arr(value) or []:
        entry = obj(item) or {}
        name, template = text(entry.get("name")), text(entry.get("template"))
        if name and template is not None:
            found[name] = template
    jinja = folder / "chat_template.jinja"
    if "default" not in found and jinja.is_file():
        found["default"] = jinja.read_text(encoding="utf-8")
    template_json = folder / "chat_template.json"
    if "default" not in found and template_json.is_file():
        template = text(read_json(template_json).get("chat_template"))
        if template is not None:
            found["default"] = template
    return found


def _first_int(config: Json, *keys: str) -> int | None:
    for key in keys:
        value = integer(config.get(key))
        if value is not None:
            return value
    return None


def _rope(config: Json) -> RopeScaling | None:
    raw = obj(config.get("rope_scaling")) or obj(config.get("rope_parameters"))
    if raw is None:
        return None
    kind = text(raw.get("rope_type")) or text(raw.get("type")) or "default"
    if kind == "default" and "factor" not in raw:
        return None
    return RopeScaling(
        kind,
        number(raw.get("factor")),
        integer(raw.get("original_max_position_embeddings")),
    )


def _moe(config: Json) -> MoeInfo | None:
    experts = _first_int(
        config, "num_local_experts", "num_experts", "n_routed_experts", "moe_num_experts"
    )
    if not experts or experts <= 1:
        return None
    shared = _first_int(config, "n_shared_experts", "num_shared_experts") or 0
    if not shared and integer(config.get("shared_expert_intermediate_size")):
        shared = 1
    return MoeInfo(
        experts,
        _first_int(config, "num_experts_per_tok", "num_experts_per_token", "moe_topk", "top_k"),
        shared,
        _first_int(config, "moe_intermediate_size", "expert_intermediate_size"),
    )


def _shape(config: Json, tensors: Iterable[TensorInfo]) -> Shape:
    hidden = _first_int(config, "hidden_size", "n_embd", "d_model", "dim")
    heads = _first_int(config, "num_attention_heads", "n_head", "n_heads", "num_heads")
    kv = _first_int(config, "num_key_value_heads", "n_kv_heads", "num_kv_heads", "n_head_kv")
    if kv is None and config.get("multi_query") is True:
        kv = 1
    head_dim = _first_int(config, "head_dim", "kv_channels")
    if head_dim is None and hidden and heads:
        head_dim = hidden // heads
    tied = config.get("tie_word_embeddings")
    names = {t.name for t in tensors}
    if not isinstance(tied, bool) and names:
        tied = not any(n.endswith("lm_head.weight") for n in names)
    return Shape(
        hidden,
        _first_int(config, "num_hidden_layers", "n_layer", "num_layers", "n_layers"),
        heads,
        kv if kv is not None else heads,
        head_dim,
        _first_int(config, "intermediate_size", "n_inner", "ffn_dim", "ffn_hidden_size"),
        _first_int(config, "vocab_size", "padded_vocab_size"),
        tied if isinstance(tied, bool) else None,
    )


def _vision(config: Json, tensors: tuple[TensorInfo, ...]) -> VisionTower | None:
    vc = obj(config.get("vision_config"))
    if vc is None:
        return None
    params = sum(t.numel for t in tensors if any(p in t.name for p in VISION_PARTS))
    return VisionTower(
        text(vc.get("model_type")),
        _first_int(vc, "hidden_size", "embed_dim", "width"),
        _first_int(vc, "num_hidden_layers", "depth", "num_layers"),
        _first_int(vc, "image_size"),
        _first_int(vc, "patch_size", "spatial_patch_size"),
        params or None,
    )


def _quantization(config: Json, tensors: tuple[TensorInfo, ...]) -> str | None:
    qc = obj(config.get("quantization_config"))
    if qc is not None:
        method = text(qc.get("quant_method")) or "unknown"
        if qc.get("load_in_4bit") is True:
            return f"{method}-4bit"
        if qc.get("load_in_8bit") is True:
            return f"{method}-8bit"
        bits = integer(qc.get("bits")) or integer(qc.get("w_bit"))
        return f"{method}-{bits}bit" if bits else method
    mlx = obj(config.get("quantization"))
    if mlx is not None:
        bits = integer(mlx.get("bits"))
        return f"mlx-{bits}bit" if bits else "mlx"
    if any(t.dtype.startswith("F8") for t in tensors):
        return "fp8"
    return None


def _quant_bits(quantization: str | None) -> int:
    match = re.search(r"(\d+)bit", quantization or "")
    return int(match.group(1)) if match else 4


def count_params(tensors: tuple[TensorInfo, ...], quantization: str | None) -> tuple[int, bool]:
    """(parameter count, exact?) Packed quantized tensors are unpacked by their bit width."""
    if quantization is None or quantization == "fp8":
        return sum(t.numel for t in tensors), True
    names = {t.name for t in tensors}
    bits = _quant_bits(quantization)
    total = 0
    for t in tensors:
        if QUANT_AUX.search(t.name):
            continue
        stem = t.name.removesuffix(".weight")
        packed_bnb = t.dtype == "U8" and any(n.startswith(t.name + ".quant_state") for n in names)
        packed_int = t.dtype in ("I32", "U32") and (
            t.name.endswith(".qweight") or f"{stem}.scales" in names
        )
        if packed_bnb:
            total += t.numel * 2
        elif packed_int:
            total += t.numel * (32 // bits)
        else:
            total += t.numel
    return total, False


def _by_dtype(tensors: Iterable[TensorInfo]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for t in tensors:
        counts[DTYPE_NAMES.get(t.dtype, t.dtype)] += t.numel
    return dict(counts.most_common())


def _gguf_facts(header: GgufHeader) -> tuple[Json, str | None]:
    """A config-like dict from GGUF metadata, plus the embedded chat template."""
    m = header.metadata
    arch = text(m.get("general.architecture")) or "unknown"

    def key(name: str) -> Scalar | None:
        return m.get(f"{arch}.{name}")

    config: Json = {
        "architectures": [arch],
        "model_type": arch,
        "max_position_embeddings": key("context_length"),
        "num_hidden_layers": key("block_count"),
        "hidden_size": key("embedding_length"),
        "intermediate_size": key("feed_forward_length"),
        "num_attention_heads": key("attention.head_count"),
        "num_key_value_heads": key("attention.head_count_kv"),
        "head_dim": key("attention.key_length"),
        "vocab_size": header.array_lengths.get("tokenizer.ggml.tokens") or key("vocab_size"),
        "num_local_experts": key("expert_count"),
        "num_experts_per_tok": key("expert_used_count"),
        "eos_token_id": m.get("tokenizer.ggml.eos_token_id"),
        "bos_token_id": m.get("tokenizer.ggml.bos_token_id"),
    }
    scaling = text(key("rope.scaling.type"))
    if scaling and scaling != "none":
        config["rope_scaling"] = {
            "rope_type": scaling,
            "factor": key("rope.scaling.factor"),
            "original_max_position_embeddings": key("rope.scaling.original_context_length"),
        }
    return {k: v for k, v in config.items() if v is not None}, text(
        m.get("tokenizer.chat_template")
    )


def confined_model_file(folder: Path, name: str) -> Path:
    """Resolve a model file without accepting traversal or symlink components."""
    parts = name.split("/")
    if (
        "\\" in name
        or PureWindowsPath(name).drive
        or any(part in ("", ".", "..") for part in parts)
    ):
        raise _bad(folder, "a model file path leaves its folder")
    path = folder
    for part in parts:
        path = path / part
        if path.is_symlink():
            raise _bad(path, "model files cannot be symbolic links")
    if not path.is_file():
        raise _bad(path, "model file is missing")
    return path


def indexed_safetensors(folder: Path) -> list[Path] | None:
    """Read the shared Hugging Face shard list, confined to the model directory."""
    index = folder / "model.safetensors.index.json"
    if not index.exists() and not index.is_symlink():
        return None
    weight_map = obj(read_json(confined_model_file(folder, index.name)).get("weight_map"))
    if not weight_map:
        raise _bad(index, "weight_map is missing")
    names: set[str] = set()
    for name in weight_map.values():
        if not isinstance(name, str) or not name.endswith(".safetensors"):
            raise _bad(index, "weight_map must name safetensors shards")
        names.add(name)
    return [confined_model_file(folder, name) for name in sorted(names)]


def _safetensors_weights(folder: Path, files: list[Path], adapter: bool) -> list[Path]:
    if not adapter and (indexed := indexed_safetensors(folder)) is not None:
        return indexed
    top = [f for f in files if f.parent == folder and f.suffix == ".safetensors"]
    if adapter:
        return [f for f in top if f.name.startswith("adapter_model")]
    return [f for f in top if not f.name.startswith("adapter_model")]


def _adapter(config: Json) -> AdapterInfo:
    targets = config.get("target_modules")
    modules = (targets,) if isinstance(targets, str) else tuple(str(t) for t in arr(targets) or [])
    return AdapterInfo(
        text(config.get("peft_type")),
        text(config.get("base_model_name_or_path")),
        integer(config.get("r")),
        number(config.get("lora_alpha")),
        modules,
        config.get("use_dora") is True,
    )


def _generation(gen: Json, config: Json) -> GenerationDefaults:
    eos = gen.get("eos_token_id", config.get("eos_token_id"))
    eos_ids = tuple(v for v in (integer(e) for e in arr(eos) or [eos]) if v is not None)
    return GenerationDefaults(
        eos_ids,
        integer(gen.get("bos_token_id", config.get("bos_token_id"))),
        integer(gen.get("pad_token_id", config.get("pad_token_id"))),
        integer(gen.get("max_length")),
    )


def _context_length(config: Json, tokenizer_config: Json) -> int | None:
    found = _first_int(
        config,
        "max_position_embeddings",
        "n_positions",
        "max_sequence_length",
        "seq_length",
        "n_ctx",
    )
    if found is None:
        fallback = integer(tokenizer_config.get("model_max_length"))
        if fallback and fallback < MAX_SANE_CONTEXT:
            found = fallback
    return found


def _active_params(
    tensors: tuple[TensorInfo, ...], moe: MoeInfo | None, total: int | None
) -> int | None:
    if total is None or moe is None or not moe.experts_per_token:
        return total
    expert = sum(t.numel for t in tensors if EXPERT_PART.search(t.name))
    return total - expert + expert * moe.experts_per_token // moe.num_experts


@dataclass
class _Weights:
    format: Format
    tensors: tuple[TensorInfo, ...] = ()
    weight_bytes: int = 0
    gguf_config: Json = field(default_factory=dict[str, Any])
    gguf_template: str | None = None


def _read_weights(path: Path, files: list[Path], is_adapter: bool, notes: list[str]) -> _Weights:
    """Pick the weight format (safetensors > GGUF > ONNX > pickle) and read its headers."""
    folder = path if path.is_dir() else path.parent
    if path.is_file():
        safetensors = [path] if path.suffix == ".safetensors" else []
    else:
        safetensors = _safetensors_weights(folder, files, is_adapter)
    gguf = [f for f in files if f.suffix == ".gguf"]
    onnx = [f for f in files if f.suffix == ".onnx"]
    pickles = [f for f in files if f.suffix in PICKLE_SUFFIXES]
    if safetensors:
        out = _Weights("safetensors")
        seen: set[str] = set()
        collected: list[TensorInfo] = []
        for shard in safetensors:
            header = read_safetensors_header(shard)
            out.weight_bytes += header.data_bytes
            for t in header.tensors:
                if t.name in seen:
                    raise _bad(shard, f"tensor {t.name} appears in two files")
                seen.add(t.name)
                collected.append(t)
        out.tensors = tuple(collected)
    elif gguf:
        if len(gguf) > 1:
            notes.append(f"Found {len(gguf)} GGUF files; inspected {gguf[0].name}.")
        header_g = read_gguf_header(gguf[0])
        config, template = _gguf_facts(header_g)
        out = _Weights("gguf", header_g.tensors, gguf[0].stat().st_size, config, template)
    elif onnx:
        out = _Weights("onnx", weight_bytes=sum(f.stat().st_size for f in onnx))
    elif pickles:
        out = _Weights("pickle")
    else:
        out = _Weights("none")
        notes.append("No weight files were found.")
    if pickles and out.format != "pickle":
        notes.append(f"Ignored {len(pickles)} pickle file(s); the app never loads them.")
    return out


def _kind(fmt: Format, quantization: str | None, is_adapter: bool) -> Kind | None:
    if fmt in ("pickle", "none"):
        return None
    if fmt in ("gguf", "onnx") or quantization is not None:
        return "inference_only"
    return "adapter" if is_adapter else "full_weights"


def _model_files(
    root: Path, files: list[Path], is_adapter: bool, hash_weights: bool
) -> tuple[list[ModelFile], dict[str, str]]:
    model_files: list[ModelFile] = []
    tokenizer_hashes: dict[str, str] = {}
    for f in files:
        rel = f.relative_to(root).as_posix()
        role = _role(rel, is_adapter)
        size = f.stat().st_size
        small = role in ("config", "tokenizer", "adapter", "code") and size <= MAX_JSON_FILE
        digest = sha256_file(f) if small or (role == "weights" and hash_weights) else None
        if role == "tokenizer" and digest:
            tokenizer_hashes[rel] = digest
        model_files.append(ModelFile(rel, size, role, digest))
    return model_files, tokenizer_hashes


def inspect_model(path: Path, *, hash_weights: bool = False) -> ModelInspection:
    """Inspect a model folder (Hugging Face layout or PEFT adapter) or a single weight file.

    Never loads tensors and never unpickles. Raises MrlError MRL-MODEL-150 when a header or
    config file is damaged. Weight files are hashed only with `hash_weights=True`.
    """
    if not path.exists():
        raise _bad(path, "path does not exist")
    is_dir = path.is_dir()
    folder = path if is_dir else path.parent
    files = _list_files(path)
    adapter_config = _optional_json(folder, "adapter_config.json") if is_dir else {}
    is_adapter = bool(adapter_config)
    config = _optional_json(folder, "config.json") if is_dir else {}
    gen = _optional_json(folder, "generation_config.json") if is_dir else {}
    tokenizer_config = _optional_json(folder, "tokenizer_config.json") if is_dir else {}
    templates = chat_templates(folder, tokenizer_config) if is_dir else {}
    notes: list[str] = []

    weights = _read_weights(path, files, is_adapter, notes)
    tensors = weights.tensors
    config = {**weights.gguf_config, **config}
    if weights.gguf_template is not None:
        templates.setdefault("default", weights.gguf_template)
    text_config = {**config, **(obj(config.get("text_config")) or {})}
    quantization = (
        f"gguf-{Counter(t.dtype for t in tensors).most_common(1)[0][0]}"
        if weights.format == "gguf" and tensors
        else _quantization(config, tensors)
    )
    params, exact = count_params(tensors, quantization) if tensors else (None, False)
    moe = _moe(text_config)
    by_dtype = _by_dtype(tensors)
    auto_map = {
        str(k): v if isinstance(v, str) else json.dumps(v)
        for source in (config, tokenizer_config)
        for k, v in (obj(source.get("auto_map")) or {}).items()
    }
    remote = bool(auto_map) or any(f.suffix == ".py" for f in files)
    kind = _kind(weights.format, quantization, is_adapter)
    model_files, tokenizer_hashes = _model_files(
        path if is_dir else folder, files, is_adapter, hash_weights
    )
    architectures = tuple(str(a) for a in arr(config.get("architectures")) or [])
    template = templates.get("default")
    return ModelInspection(
        path=str(path),
        format=weights.format,
        kind=kind,
        refusal=PICKLE_REFUSED if weights.format == "pickle" else None,
        inference_only_code=INFERENCE_ONLY if kind == "inference_only" else None,
        quantization=quantization,
        requires_remote_code=remote,
        remote_code_code=REMOTE_CODE if remote else None,
        auto_map=auto_map,
        architecture=architectures[0] if architectures else None,
        architectures=architectures,
        model_type=text(config.get("model_type")),
        context_length=_context_length(text_config, tokenizer_config),
        rope_scaling=_rope(text_config),
        moe=moe,
        vision=_vision(config, tensors),
        adapter=_adapter(adapter_config) if is_adapter else None,
        shape=_shape(text_config, tensors),
        param_count=params,
        param_count_exact=exact,
        active_param_count=_active_params(tensors, moe, params),
        dtype=next(iter(by_dtype), None),
        params_by_dtype=by_dtype,
        weight_bytes=weights.weight_bytes,
        tensors=tensors,
        generation=_generation(gen, config),
        chat_template_sha256=None if template is None else sha256_text(template),
        chat_template_names=tuple(templates),
        has_generation_tags=template is not None and GENERATION_TAG.search(template) is not None,
        tokenizer_sha256=tokenizer_hashes,
        files=tuple(model_files),
        notes=tuple(notes),
    )
