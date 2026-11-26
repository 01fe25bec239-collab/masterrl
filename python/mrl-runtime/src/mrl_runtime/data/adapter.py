"""`DatasetAdapter`: how engines read a dataset version, and how DATA_PREP writes one.

A ready dataset version is a folder:

    dataset.json              # schema, row count, splits and the shard list (written last)
    shards/<split>-NNNNN.parquet

Every shard has one column, `record`, holding each record as canonical JSON (keys sorted,
no spaces), so records of any shape keep their exact values and the same records always
give the same bytes. The scheduler stages each dataset version a run names as the folder
input `dataset_<n>` (n = its place in the run's `dataset_version_ids`); engines open it with
`DatasetAdapter(staged_paths["dataset_0"])` and iterate `records("train")`.
"""

import importlib
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, cast

from mrl_runtime.model._json import Json, arr, integer, obj, text
from mrl_runtime.model.errors import DATASET_UNREADABLE, MrlError

MANIFEST = "dataset.json"
SHARD_DIR = "shards"
COLUMN = "record"
# A shard closes at whichever comes first.
SHARD_ROWS = 50_000
SHARD_BYTES = 128 * 1024 * 1024
_BATCH_ROWS = 1024


def input_name(index: int) -> str:
    """The staged input name of the run's `index`-th dataset version."""
    return f"dataset_{index}"


def canonical_json(record: object) -> str:
    """The stored form of a record: sorted keys, no spaces, UTF-8 text kept as is."""
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class Shard:
    path: str  # relative to the dataset folder, "/" separators
    split: str
    rows: int


def _unreadable(why: str) -> MrlError:
    return MrlError(DATASET_UNREADABLE, "The app could not read this dataset version.", why)


def _safe_relative(path: str) -> str:
    p = PurePosixPath(path)
    if p.is_absolute() or ".." in p.parts or "\\" in path or not p.parts:
        raise _unreadable(f"{MANIFEST} names a file outside the dataset folder: {path!r}")
    return p.as_posix()


def _pyarrow() -> tuple[Any, Any]:
    """pyarrow and pyarrow.parquet, loaded on first use (native library, no type stubs)."""
    return importlib.import_module("pyarrow"), importlib.import_module("pyarrow.parquet")


class DatasetAdapter:
    """Read-only view of a dataset version folder (staged by the scheduler or written by
    DATA_PREP)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        path = root / MANIFEST
        try:
            raw: object = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise _unreadable(f"{MANIFEST}: {exc}") from exc
        doc = obj(raw) or {}
        schema = text(doc.get("schema_id"))
        if not schema:
            raise _unreadable(f"{MANIFEST} has no schema_id")
        self.schema_id: str = schema
        self.manifest: Json = doc
        shards: list[Shard] = []
        for entry in arr(doc.get("shards")) or []:
            e = obj(entry) or {}
            rel, split, rows = text(e.get("path")), text(e.get("split")), integer(e.get("rows"))
            if rel is None or split is None or rows is None or rows < 0:
                raise _unreadable(f"{MANIFEST} has a malformed shard entry")
            rel = _safe_relative(rel)
            if not (root / rel).is_file():
                raise _unreadable(f"shard {rel} is missing")
            shards.append(Shard(rel, split, rows))
        self.shards: tuple[Shard, ...] = tuple(shards)

    @property
    def splits(self) -> dict[str, int]:
        """Rows per split."""
        out: dict[str, int] = {}
        for s in self.shards:
            out[s.split] = out.get(s.split, 0) + s.rows
        return out

    def __len__(self) -> int:
        return sum(s.rows for s in self.shards)

    def records(self, split: str | None = None) -> Iterator[Json]:
        """Every record (of one split, or of all splits in shard order), as a dict."""
        _, pq = _pyarrow()
        for shard in self.shards:
            if split is not None and shard.split != split:
                continue
            try:
                f = pq.ParquetFile(self.root / shard.path)
                for batch in f.iter_batches(columns=[COLUMN], batch_size=_BATCH_ROWS):
                    for value in cast(list[object], batch.column(0).to_pylist()):
                        if not isinstance(value, str):
                            raise _unreadable(f"{shard.path} has a row without a record")
                        record = obj(json.loads(value))
                        if record is None:
                            raise _unreadable(f"{shard.path} has a row that is not an object")
                        yield record
            except (OSError, ValueError) as exc:
                raise _unreadable(f"{shard.path}: {exc}") from exc


class ShardWriter:
    """Writes records into `shards/<split>-NNNNN.parquet` under `root`, one split at a time
    or interleaved; `close()` returns the shard list for `dataset.json`."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._open: dict[str, tuple[list[str], int]] = {}
        self._count: dict[str, int] = {}
        self.shards: list[Shard] = []

    def add(self, record: object, split: str = "train") -> None:
        line = canonical_json(record)
        rows, size = self._open.get(split, ([], 0))
        rows.append(line)
        size += len(line.encode("utf-8"))
        self._open[split] = (rows, size)
        if len(rows) >= SHARD_ROWS or size >= SHARD_BYTES:
            self._flush(split)

    def _flush(self, split: str) -> None:
        rows, _ = self._open.pop(split, ([], 0))
        if not rows:
            return
        pa, pq = _pyarrow()
        n = self._count.get(split, 0)
        self._count[split] = n + 1
        rel = f"{SHARD_DIR}/{split}-{n:05d}.parquet"
        (self.root / SHARD_DIR).mkdir(parents=True, exist_ok=True)
        table = pa.table({COLUMN: pa.array(rows, type=pa.string())})
        pq.write_table(table, self.root / rel, compression="zstd", row_group_size=_BATCH_ROWS)
        self.shards.append(Shard(rel, split, len(rows)))

    def close(self) -> list[Shard]:
        for split in sorted(self._open):
            self._flush(split)
        return sorted(self.shards, key=lambda s: s.path)


def write_manifest(root: Path, schema_id: str, shards: Iterable[Shard], **extra: Any) -> Path:
    """Write `dataset.json` (last, so a folder with one is complete)."""
    listed = list(shards)
    splits: dict[str, int] = {}
    for s in listed:
        splits[s.split] = splits.get(s.split, 0) + s.rows
    doc: Json = {
        "schema_id": schema_id,
        "rows": sum(s.rows for s in listed),
        "splits": splits,
        "shards": [{"path": s.path, "split": s.split, "rows": s.rows} for s in listed],
    }
    doc.update(extra)
    path = root / MANIFEST
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
