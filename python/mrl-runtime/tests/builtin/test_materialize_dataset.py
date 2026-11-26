"""The materialize_dataset task: a JSONL export per split."""

from pathlib import Path

from mrl_runtime.builtin.materialize_dataset import MATERIALIZE_DATASET
from mrl_runtime.builtin.tasks import TaskContext, TaskOutput
from mrl_runtime.data.adapter import ShardWriter, canonical_json, write_manifest


def test_jsonl_export_per_split(tmp_path: Path) -> None:
    w = ShardWriter(tmp_path / "v")
    rows = [{"text": f"r{i}", "é": i} for i in range(5)]
    for i, r in enumerate(rows):
        w.add(r, "train" if i < 4 else "test")
    write_manifest(tmp_path / "v", "pretrain.text.v1", w.close())
    staged = {"dataset": tmp_path / "v"}
    assert MATERIALIZE_DATASET.validate({}, staged) == []
    assert MATERIALIZE_DATASET.validate({"format": "csv"}, staged)[0].path == "/inputs/format"
    out = tmp_path / "out"
    events = list(MATERIALIZE_DATASET.run(TaskContext({}, staged, out, lambda: False)))
    assert [e.path for e in events if isinstance(e, TaskOutput)] == [
        "export/test.jsonl",
        "export/train.jsonl",
    ]
    lines = (out / "export/train.jsonl").read_text("utf-8").splitlines()
    assert lines == [canonical_json(r) for r in rows[:4]]
