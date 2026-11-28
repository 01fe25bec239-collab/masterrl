"""DatasetAdapter: the version folder engines read (shards of canonical JSON + dataset.json)."""

import hashlib
import json
from pathlib import Path

import pytest

from mrl_runtime.data.adapter import (
    DatasetAdapter,
    ShardWriter,
    canonical_json,
    input_name,
    write_manifest,
)
from mrl_runtime.model import MrlError

RECORDS = [{"text": f"row {i}", "meta": {"b": 1, "a": [i, None]}} for i in range(5)]


def _write(root: Path, records: list[dict[str, object]]) -> None:
    w = ShardWriter(root)
    for i, r in enumerate(records):
        w.add(r, "train" if i < 3 else "test")
    write_manifest(root, "pretrain.text.v1", w.close(), passed=True)


def test_round_trip_by_split(tmp_path: Path) -> None:
    _write(tmp_path, RECORDS)
    data = DatasetAdapter(tmp_path)
    assert data.schema_id == "pretrain.text.v1"
    assert data.splits == {"train": 3, "test": 2}
    assert len(data) == 5
    assert list(data.records("train")) == RECORDS[:3]
    assert list(data.records("test")) == RECORDS[3:]
    assert sorted(map(canonical_json, data.records())) == sorted(map(canonical_json, RECORDS))
    assert data.manifest["passed"] is True
    assert input_name(1) == "dataset_1"


def test_same_records_give_the_same_bytes(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    _write(a, RECORDS)
    _write(b, RECORDS)
    for shard in DatasetAdapter(a).shards:
        digest = [hashlib.sha256((d / shard.path).read_bytes()).hexdigest() for d in (a, b)]
        assert digest[0] == digest[1]


@pytest.mark.parametrize("path", ["../escape.parquet", "/etc/passwd", "a\\b.parquet", ""])
def test_manifest_paths_stay_inside(tmp_path: Path, path: str) -> None:
    (tmp_path / "dataset.json").write_text(
        json.dumps({"schema_id": "eval.v1", "shards": [{"path": path, "split": "t", "rows": 1}]}),
        encoding="utf-8",
    )
    with pytest.raises(MrlError) as err:
        DatasetAdapter(tmp_path)
    assert err.value.code == "MRL-DATA-150"


def test_missing_shard_or_manifest_is_unreadable(tmp_path: Path) -> None:
    with pytest.raises(MrlError):
        DatasetAdapter(tmp_path)
    _write(tmp_path, RECORDS)
    next((tmp_path / "shards").iterdir()).unlink()
    with pytest.raises(MrlError, match="missing"):
        DatasetAdapter(tmp_path)
