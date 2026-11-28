"""The materialize_dataset task: a JSONL export per split."""

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from mrl_runtime.builtin import materialize_dataset
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


def test_hub_push_materializes_only_rows_and_records_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Hub:
        def __init__(self, endpoint: str, token: str) -> None:
            assert endpoint == "https://huggingface.co"
            assert token == "test-token"

        def create_repo(self, repo: str, **options: Any) -> None:
            assert repo == "test/dataset"
            assert options["repo_type"] == "dataset"
            assert options["private"] is True

        def upload_folder(self, **options: Any) -> SimpleNamespace:
            assert options["path_in_repo"] == "masterrl/dsv_sample"
            assert (options["folder_path"] / "train.jsonl").read_text("utf-8") == '{"text":"one"}\n'
            return SimpleNamespace(oid="commit")

    parent = tmp_path / "input"
    writer = ShardWriter(parent)
    writer.add({"text": "one"})
    write_manifest(parent, "pretrain.text.v1", writer.close())
    out = tmp_path / "outputs"
    monkeypatch.setenv("MRL_CREDENTIAL_HF_TOKEN", "test-token")
    monkeypatch.setattr(materialize_dataset, "HfApi", Hub)
    task = materialize_dataset.PUSH_DATASET_TO_HUB
    inputs = {"repo": "test/dataset", "version_id": "dsv_sample"}
    staged = {"dataset": parent}
    assert task.validate(inputs, staged) == []
    assert [
        e.path
        for e in task.run(TaskContext(inputs, staged, out, lambda: False))
        if isinstance(e, TaskOutput)
    ] == ["hub_push.json"]
    assert json.loads((out / "hub_push.json").read_text("utf-8")) == {
        "repo": "test/dataset",
        "commit": "commit",
    }
