"""Read weight-file headers (safetensors, GGUF) without loading any tensor data.

Both readers treat the file as untrusted: every length is checked against the file size and
fixed caps before anything is allocated.
"""

import json
import math
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from mrl_runtime.model._json import arr, integer, obj
from mrl_runtime.model.errors import MODEL_UNREADABLE, MrlError

# Same cap the safetensors library uses for its JSON header.
MAX_SAFETENSORS_HEADER = 100 * 1024 * 1024
MAX_GGUF_STRING = 16 * 1024 * 1024
MAX_GGUF_ITEMS = 50_000_000
MAX_DIMS = 8

# Bits per element of each safetensors dtype.
SAFETENSORS_BITS: dict[str, int] = {
    "BOOL": 8,
    "U8": 8,
    "I8": 8,
    "F8_E5M2": 8,
    "F8_E4M3": 8,
    "F8_E8M0": 8,
    "I16": 16,
    "U16": 16,
    "F16": 16,
    "BF16": 16,
    "I32": 32,
    "U32": 32,
    "F32": 32,
    "C64": 64,
    "F64": 64,
    "I64": 64,
    "U64": 64,
    "F4": 4,
    "F6_E2M3": 6,
    "F6_E3M2": 6,
}

# ggml tensor type ids (ggml.h) to names.
GGML_TYPES: dict[int, str] = {
    0: "F32", 1: "F16", 2: "Q4_0", 3: "Q4_1", 6: "Q5_0", 7: "Q5_1", 8: "Q8_0", 9: "Q8_1",
    10: "Q2_K", 11: "Q3_K", 12: "Q4_K", 13: "Q5_K", 14: "Q6_K", 15: "Q8_K", 16: "IQ2_XXS",
    17: "IQ2_XS", 18: "IQ3_XXS", 19: "IQ1_S", 20: "IQ4_NL", 21: "IQ3_S", 22: "IQ2_S",
    23: "IQ4_XS", 24: "I8", 25: "I16", 26: "I32", 27: "I64", 28: "F64", 29: "IQ1_M",
    30: "BF16", 34: "TQ1_0", 35: "TQ2_0", 39: "MXFP4",
}  # fmt: skip

type Scalar = str | int | float | bool


@dataclass(frozen=True)
class TensorInfo:
    """One tensor as a header describes it."""

    name: str
    dtype: str
    shape: tuple[int, ...]

    @property
    def numel(self) -> int:
        return math.prod(self.shape)


@dataclass(frozen=True)
class SafetensorsHeader:
    tensors: tuple[TensorInfo, ...]
    metadata: dict[str, str]
    data_bytes: int


@dataclass(frozen=True)
class GgufHeader:
    version: int
    metadata: dict[str, Scalar]
    array_lengths: dict[str, int]
    tensors: tuple[TensorInfo, ...]


def _bad(path: Path, why: str) -> MrlError:
    return MrlError(
        MODEL_UNREADABLE, "The app could not read this model's files.", f"{path.name}: {why}"
    )


def read_safetensors_header(path: Path) -> SafetensorsHeader:
    """Tensor names, dtypes and shapes from a `.safetensors` file; checks every offset."""
    size = path.stat().st_size
    with path.open("rb") as f:
        head = f.read(8)
        if len(head) < 8:
            raise _bad(path, "file is shorter than a safetensors header")
        (n,) = struct.unpack("<Q", head)
        if n > MAX_SAFETENSORS_HEADER or n > size - 8:
            raise _bad(path, f"header length {n} does not fit the file")
        raw = f.read(n)
    try:
        parsed: object = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise _bad(path, "header is not valid JSON") from exc
    header = obj(parsed)
    if header is None:
        raise _bad(path, "header is not a JSON object")
    data_bytes = size - 8 - n
    tensors: list[TensorInfo] = []
    metadata: dict[str, str] = {}
    spans: list[tuple[int, int]] = []
    for name, entry in header.items():
        if name == "__metadata__":
            meta = obj(entry)
            if meta is None or not all(isinstance(v, str) for v in meta.values()):
                raise _bad(path, "__metadata__ must map names to strings")
            metadata = {k: str(v) for k, v in meta.items()}
            continue
        tensors.append(_safetensors_tensor(path, name, entry, data_bytes, spans))
    spans.sort()
    position = 0
    for start, end in spans:
        if start != position:
            raise _bad(path, "tensor data has gaps or overlaps")
        position = end
    if position != data_bytes:
        raise _bad(path, "tensor data does not cover the file")
    return SafetensorsHeader(tuple(tensors), metadata, data_bytes)


def _safetensors_tensor(
    path: Path, name: str, value: object, data_bytes: int, spans: list[tuple[int, int]]
) -> TensorInfo:
    entry = obj(value)
    if entry is None:
        raise _bad(path, f"tensor {name} is not an object")
    dtype, shape, offsets = (
        entry.get("dtype"),
        arr(entry.get("shape")),
        arr(entry.get("data_offsets")),
    )
    if not isinstance(dtype, str) or dtype not in SAFETENSORS_BITS:
        raise _bad(path, f"tensor {name} has unknown dtype {dtype!r}")
    dims = _nonneg_ints(shape)
    if dims is None or len(dims) > MAX_DIMS:
        raise _bad(path, f"tensor {name} has a bad shape")
    span = _nonneg_ints(offsets)
    if span is None or len(span) != 2:
        raise _bad(path, f"tensor {name} has bad data offsets")
    start, end = span
    expected = math.ceil(math.prod(dims) * SAFETENSORS_BITS[dtype] / 8)
    if not start <= end <= data_bytes or end - start != expected:
        raise _bad(path, f"tensor {name} data size does not match its shape")
    if start != end:
        spans.append((start, end))
    return TensorInfo(name, dtype, dims)


def _nonneg_ints(values: list[object] | None) -> tuple[int, ...] | None:
    if values is None:
        return None
    out = tuple(v for v in (integer(x) for x in values) if v is not None and v >= 0)
    return out if len(out) == len(values) else None


class _Reader:
    """Bounded little-endian reader over a GGUF file."""

    def __init__(self, f: BinaryIO, path: Path, size: int) -> None:
        self.f, self.path, self.size = f, path, size

    def take(self, n: int) -> bytes:
        if n > self.size - self.f.tell():
            raise _bad(self.path, "file ends early")
        return self.f.read(n)

    def unpack(self, fmt: str) -> int | float:
        value: int | float = struct.unpack("<" + fmt, self.take(struct.calcsize(fmt)))[0]
        return value

    def u32(self) -> int:
        return int(self.unpack("I"))

    def u64(self) -> int:
        return int(self.unpack("Q"))

    def string(self) -> str:
        n = self.u64()
        if n > MAX_GGUF_STRING:
            raise _bad(self.path, "a text value is too long")
        try:
            return self.take(n).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _bad(self.path, "a text value is not UTF-8") from exc

    def count(self) -> int:
        n = self.u64()
        if n > MAX_GGUF_ITEMS:
            raise _bad(self.path, "a count is too large")
        return n


_GGUF_SCALARS = {0: "B", 1: "b", 2: "H", 3: "h", 4: "I", 5: "i", 6: "f", 10: "Q", 11: "q", 12: "d"}
_GGUF_BOOL, _GGUF_STRING, _GGUF_ARRAY = 7, 8, 9


def _gguf_value(r: _Reader, kind: int) -> Scalar:
    if kind in _GGUF_SCALARS:
        return r.unpack(_GGUF_SCALARS[kind])
    if kind == _GGUF_BOOL:
        return r.take(1) != b"\x00"
    if kind == _GGUF_STRING:
        return r.string()
    raise _bad(r.path, f"unknown metadata type {kind}")


def read_gguf_header(path: Path) -> GgufHeader:
    """Metadata and tensor infos from a GGUF (v2/v3) file. Arrays keep only their length."""
    size = path.stat().st_size
    with path.open("rb") as f:
        r = _Reader(f, path, size)
        if r.take(4) != b"GGUF":
            raise _bad(path, "not a GGUF file")
        version = r.u32()
        if version not in (2, 3):
            raise _bad(path, f"GGUF version {version} is not supported")
        n_tensors, n_kv = r.count(), r.count()
        metadata: dict[str, Scalar] = {}
        arrays: dict[str, int] = {}
        for _ in range(n_kv):
            key, kind = r.string(), r.u32()
            if kind != _GGUF_ARRAY:
                metadata[key] = _gguf_value(r, kind)
                continue
            item_kind, n = r.u32(), r.count()
            arrays[key] = n
            if item_kind in _GGUF_SCALARS:
                r.take(struct.calcsize(_GGUF_SCALARS[item_kind]) * n)
            elif item_kind == _GGUF_BOOL:
                r.take(n)
            elif item_kind == _GGUF_STRING:
                for _ in range(n):
                    r.take(r.u64())
            else:
                raise _bad(path, f"array {key} has an unsupported item type")
        tensors: list[TensorInfo] = []
        for _ in range(n_tensors):
            name, n_dims = r.string(), r.u32()
            if n_dims > MAX_DIMS:
                raise _bad(path, f"tensor {name} has too many dimensions")
            dims = tuple(r.u64() for _ in range(n_dims))
            kind = r.u32()
            r.u64()  # data offset: not needed without loading data
            tensors.append(TensorInfo(name, GGML_TYPES.get(kind, f"ggml_type_{kind}"), dims))
    return GgufHeader(version, metadata, arrays, tuple(tensors))
