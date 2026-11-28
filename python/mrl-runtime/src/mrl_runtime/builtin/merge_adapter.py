"""The `merge_adapter` task (MERGE_ADAPTER, `data_prep` sandbox): base model + LoRA adapter →
full weights, without torch.

Staged inputs: `base` (a full-precision safetensors model folder) and `adapter` (a PEFT LoRA
folder). Inputs: `{"dtype": "auto" | "float32" | "float16" | "bfloat16"}` (`auto` keeps each
tensor's dtype). Every targeted weight becomes `W + scaling · B @ A` (`scaling` = alpha / r, or
alpha / √r with rsLoRA; transposed for `fan_in_fan_out`; embeddings use `(B @ A)ᵀ`), computed
in float32. Weights keep the base's shard layout; config, tokenizer and license files are
copied. Refused (an `inspection.json` with `error`, MRL-MODEL-264): a quantized base, DoRA,
per-module rank patterns, or an adapter tensor the base has no weight for.
Outputs: `model/…` and `inspection.json` (as `import_model` writes it).
"""

import json
import math
import shutil
import struct
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import numpy as np

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.import_model import (
    INSPECTION_FILE,
    MODEL_DIR,
    license_of,
    report,
    write_outputs,
    write_refusal,
)
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent
from mrl_runtime.errors import TASK_INPUT_INVALID, MrlError
from mrl_runtime.model import inspect_model, read_safetensors_header
from mrl_runtime.model.inspection import confined_model_file, indexed_safetensors

ADAPTER_BASE = "MRL-MODEL-264"
UNREADABLE = "MRL-MODEL-150"
DTYPES = {"float32": "F32", "float16": "F16", "bfloat16": "BF16"}
_NUMPY = {
    "F32": np.dtype("<f4"),
    "F16": np.dtype("<f2"),
    "BF16": np.dtype("<u2"),
    "F64": np.dtype("<f8"),
}
_SIZES = {"F64": 8, "F32": 4, "F16": 2, "BF16": 2, "I64": 8, "I32": 4, "I16": 2, "I8": 1,
          "U64": 8, "U32": 4, "U16": 2, "U8": 1, "BOOL": 1}  # fmt: skip
PREFIX = "base_model.model."


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    out = [
        Issue(TASK_INPUT_INVALID, f"No {name} folder was staged.", path="/inputs")
        for name in ("base", "adapter")
        if name not in staged
    ]
    if inputs.get("dtype", "auto") not in ("auto", *DTYPES):
        out.append(
            Issue(
                TASK_INPUT_INVALID,
                "Pick the merged weights' number type.",
                path="/inputs/dtype",
                hint="Use auto, float32, float16 or bfloat16.",
            )
        )
    return out


def bf16_to_f32(raw: np.ndarray) -> np.ndarray:
    """bfloat16 bits (uint16) → float32."""
    return (raw.astype(np.uint32) << 16).view(np.float32)


def f32_to_bf16(values: np.ndarray) -> np.ndarray:
    """float32 → bfloat16 bits (uint16), rounding to nearest even."""
    bits = np.ascontiguousarray(values, dtype=np.float32).view(np.uint32)
    bits = bits + (np.uint32(0x7FFF) + ((bits >> 16) & np.uint32(1)))
    return (bits >> 16).astype(np.uint16)


class SafetensorsFile:
    """One `.safetensors` file: its header and lazily read tensors."""

    def __init__(self, path: Path) -> None:
        read_safetensors_header(path)  # P15 checks every offset and the size cap first
        self.path = path
        with path.open("rb") as f:
            (n,) = struct.unpack("<Q", f.read(8))
            header: dict[str, Any] = json.loads(f.read(n))
        self.start = 8 + n
        self.metadata: dict[str, str] = header.pop("__metadata__", {}) or {}
        self.entries: dict[str, dict[str, Any]] = header

    def raw(self, name: str) -> bytes:
        begin, end = self.entries[name]["data_offsets"]
        with self.path.open("rb") as f:
            f.seek(self.start + begin)
            return f.read(end - begin)

    def float32(self, name: str) -> np.ndarray:
        e = self.entries[name]
        dtype = e["dtype"]
        if dtype not in _NUMPY:
            raise MrlError(UNREADABLE, f"{name} is stored as {dtype}, not a float type.")
        arr: np.ndarray = np.frombuffer(self.raw(name), dtype=_NUMPY[dtype]).reshape(
            [int(n) for n in e["shape"]]
        )
        return bf16_to_f32(arr) if dtype == "BF16" else arr.astype(np.float32)


def write_safetensors(
    path: Path, tensors: list[tuple[str, str, list[int], bytes]], metadata: dict[str, str]
) -> None:
    """Writes (name, dtype, shape, bytes) tensors in order, header padded to 8 bytes."""
    header: dict[str, Any] = {"__metadata__": metadata} if metadata else {}
    offset = 0
    for name, dtype, shape, data in tensors:
        header[name] = {
            "dtype": dtype,
            "shape": shape,
            "data_offsets": [offset, offset + len(data)],
        }
        offset += len(data)
    text = json.dumps(header, separators=(",", ":")).encode()
    text += b" " * (-len(text) % 8)
    with path.open("wb") as f:
        f.write(struct.pack("<Q", len(text)))
        f.write(text)
        for *_, data in tensors:
            f.write(data)


def _encode(values: np.ndarray, dtype: str) -> bytes:
    if dtype == "BF16":
        return f32_to_bf16(values).tobytes()
    return np.ascontiguousarray(values, dtype=_NUMPY[dtype]).tobytes()


def _deltas(adapter: Path) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """LoRA updates and replaced tensors, keyed by the base tensor name."""
    cfg = json.loads((adapter / "adapter_config.json").read_text(encoding="utf-8"))
    if str(cfg.get("peft_type", "")).upper() != "LORA":
        raise ValueError("Only LoRA adapters can be merged.")
    if cfg.get("use_dora"):
        raise ValueError("DoRA adapters can't be merged in this release.")
    if cfg.get("rank_pattern") or cfg.get("alpha_pattern"):
        raise ValueError("Adapters with per-module ranks can't be merged in this release.")
    r = int(cfg["r"])
    alpha = float(cfg.get("lora_alpha", r))
    scaling = alpha / math.sqrt(r) if cfg.get("use_rslora") else alpha / r
    fan_in_fan_out = bool(cfg.get("fan_in_fan_out"))
    weights = adapter / "adapter_model.safetensors"
    if not weights.is_file():
        raise ValueError("The adapter has no adapter_model.safetensors.")
    st = SafetensorsFile(weights)
    pairs: dict[str, dict[str, np.ndarray]] = {}
    replaced: dict[str, np.ndarray] = {}
    for name in st.entries:
        key = name.removeprefix(PREFIX)
        for part in ("lora_A", "lora_B", "lora_embedding_A", "lora_embedding_B"):
            marker = f".{part}"
            if marker in key:
                module = key.split(marker)[0]
                pairs.setdefault(module, {})[part] = st.float32(name)
                break
        else:
            replaced[key.replace(".modules_to_save", "")] = st.float32(name)
    deltas: dict[str, np.ndarray] = {}
    for module, p in pairs.items():
        if "lora_A" in p and "lora_B" in p:
            d = p["lora_B"] @ p["lora_A"]
            deltas[f"{module}.weight"] = scaling * (d.T if fan_in_fan_out else d)
        elif "lora_embedding_A" in p and "lora_embedding_B" in p:
            deltas[f"{module}.weight"] = scaling * (p["lora_embedding_B"] @ p["lora_embedding_A"]).T
        else:
            raise ValueError(f"The adapter's {module} has only half of its LoRA pair.")
    return deltas, replaced


def merge(base: Path, adapter: Path, out: Path, dtype: str) -> None:
    """Writes the merged model folder `out` (see the module docs). Raises ValueError with a
    plain reason for adapters that can't be merged."""
    deltas, replaced = _deltas(adapter)
    indexed = indexed_safetensors(base)
    shards = indexed if indexed is not None else sorted(base.glob("*.safetensors"))
    out.mkdir(parents=True, exist_ok=True)
    for p in sorted(base.iterdir()):
        if (
            p.is_file()
            and not p.name.endswith(".safetensors")
            and p.name != "model.safetensors.index.json"
        ):
            shutil.copyfile(p, out / p.name)
    used: set[str] = set()
    total = 0
    weight_map: dict[str, str] = {}
    target = DTYPES.get(dtype)
    for path in shards:
        shard = path.relative_to(base).as_posix()
        st = SafetensorsFile(confined_model_file(base, shard))
        tensors: list[tuple[str, str, list[int], bytes]] = []
        for name, e in st.entries.items():
            src_dtype, shape = str(e["dtype"]), list(e["shape"])
            change = deltas.get(name)
            if (
                change is None
                and name not in replaced
                and (target is None or src_dtype not in _NUMPY)
            ):
                data = st.raw(name)
                tensors.append((name, src_dtype, shape, data))
            else:
                values = replaced[name] if name in replaced else st.float32(name)
                if values.shape != tuple(shape):
                    raise ValueError(f"The adapter's replacement for {name} has the wrong shape.")
                if change is not None:
                    if change.shape != values.shape:
                        raise ValueError(f"The adapter's update for {name} has the wrong shape.")
                    values = values + change
                used.add(name)
                out_dtype = target or src_dtype
                tensors.append((name, out_dtype, shape, _encode(values, out_dtype)))
            total += len(tensors[-1][3])
            weight_map[name] = shard
        target_path = out / shard
        target_path.parent.mkdir(parents=True, exist_ok=True)
        write_safetensors(target_path, tensors, st.metadata or {"format": "pt"})
    missing = (set(deltas) | set(replaced)) - used
    if missing:
        raise ValueError(f"The base model has no weight {sorted(missing)[0]} the adapter changes.")
    if indexed is not None or len(shards) > 1:
        (out / "model.safetensors.index.json").write_text(
            json.dumps({"metadata": {"total_size": total}, "weight_map": weight_map}, indent=2),
            encoding="utf-8",
        )
    if target is not None and (out / "config.json").is_file():
        config = json.loads((out / "config.json").read_text(encoding="utf-8"))
        config["torch_dtype"] = config["dtype"] = dtype
        (out / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    base, adapter = ctx.staged_paths["base"], ctx.staged_paths["adapter"]
    out = ctx.output_dir
    out.mkdir(parents=True, exist_ok=True)
    found = inspect_model(base)
    if found.kind != "full_weights" or found.format != "safetensors" or found.quantization:
        yield from write_refusal(
            out,
            ADAPTER_BASE,
            "The base model must be full-precision safetensors weights to merge into; "
            f"this one is {found.quantization or found.kind or found.format}.",
        )
        return
    if inspect_model(adapter).kind != "adapter":
        yield from write_refusal(out, ADAPTER_BASE, "The second model is not a LoRA adapter.")
        return
    yield Progress(step=0, total_steps=1, phase="merge")
    folder = out / MODEL_DIR
    try:
        merge(base, adapter, folder, str(ctx.inputs.get("dtype", "auto")))
    except ValueError as exc:
        shutil.rmtree(folder, ignore_errors=True)
        (out / INSPECTION_FILE).unlink(missing_ok=True)
        yield from write_refusal(out, ADAPTER_BASE, str(exc))
        return
    yield Progress(step=1, total_steps=1, phase="merge")
    doc = report(folder, license=license_of(folder), source={})
    yield from write_outputs(out, folder, doc, copied=True)


MERGE_ADAPTER = BuiltinTask(
    name="merge_adapter",
    job_kind="MERGE_ADAPTER",
    artifact_kind="model",
    validate=_validate,
    run=_run,
)
