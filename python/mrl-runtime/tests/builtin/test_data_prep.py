"""The data_prep task: every canonical schema, invalid data with reasons, hostile archives,
leakage, content hashes, tokenization preview, and a run through the built-in engine."""

import asyncio
import hashlib
import io
import json
import os
import shutil
import tarfile
import zipfile
from pathlib import Path
from typing import Any

import make_fixtures
import pytest

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.builtin import BuiltinEngine, discover_tasks, load_builtin_manifest
from mrl_runtime.builtin.data_prep import DATA_PREP, MAX_RATIO, RATIO_FLOOR
from mrl_runtime.builtin.tasks import TaskContext, TaskOutput
from mrl_runtime.builtin.transform import TRANSFORM
from mrl_runtime.data.adapter import DatasetAdapter
from mrl_runtime.data.validate import DATASET_SCHEMAS, content_hashes
from mrl_runtime.testkit.agent import AgentScript
from mrl_runtime.testkit.runner import run_in_process

DATASETS = Path(__file__).resolve().parents[4] / "tests/fixtures/datasets"
# rl.rollout.v1 is written by rollouts, never uploaded, so it has no fixture file.
UPLOADABLE = [s for s in DATASET_SCHEMAS if (DATASETS / f"{s}.jsonl").exists()]


def _prep(tmp: Path, files: dict[str, bytes], inputs: dict[str, Any]) -> dict[str, Any]:
    src = tmp / "source"
    for name, body in files.items():
        (src / name).parent.mkdir(parents=True, exist_ok=True)
        (src / name).write_bytes(body)
    staged = {"source": src} | {k: tmp / k for k in (inputs.get("against") or {})}
    assert DATA_PREP.validate(inputs, staged) == []
    out = tmp / "outputs"
    events = list(DATA_PREP.run(TaskContext(inputs, staged, out, lambda: False)))
    listed = {e.path for e in events if isinstance(e, TaskOutput)}
    assert all((out / p).is_file() for p in listed)
    return json.loads((out / "dataset.json").read_text(encoding="utf-8"))


def _fixture(name: str) -> bytes:
    return (DATASETS / name).read_bytes()


@pytest.mark.parametrize("schema", UPLOADABLE)
def test_every_canonical_schema(tmp_path: Path, schema: str) -> None:
    ok = _prep(tmp_path / "ok", {"data.jsonl": _fixture(f"{schema}.jsonl")}, {"schema_id": schema})
    bad = _prep(
        tmp_path / "bad", {"data.jsonl": _fixture(f"{schema}-invalid.jsonl")}, {"schema_id": schema}
    )
    if schema == "mm.messages.v1":  # images wait for multimodal training (P47)
        assert ok["passed"] is False
        assert ok["error_code"] == "MRL-DATA-270"
        assert "images" in str(ok["reason"])
    else:
        assert ok["passed"] is True, ok["reason"]
        assert ok["rows"] == ok["valid"] - ok["duplicates"]
        data = DatasetAdapter(tmp_path / "ok" / "outputs")
        assert len(list(data.records("train"))) == ok["rows"]
    assert bad["passed"] is False
    assert bad["error_code"] == "MRL-DATA-001"
    assert "do not match" in str(bad["reason"])
    assert bad["shards"] == []
    report = json.loads((tmp_path / "bad/outputs/validation_report.json").read_text("utf-8"))
    assert report["examples"]
    assert all(e["message"] for e in report["examples"])


def test_same_bytes_same_shards_and_duplicates_dropped(tmp_path: Path) -> None:
    body = _fixture("sft.messages.v1.jsonl")
    first = _prep(tmp_path / "a", {"x.jsonl": body}, {"schema_id": "sft.messages.v1"})
    again = _prep(tmp_path / "b", {"x.jsonl": body + body}, {"schema_id": "sft.messages.v1"})
    assert again["duplicates"] == first["records"]
    assert again["rows"] == first["rows"]

    def shard_hashes(d: Path) -> list[str]:
        return [hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(d.rglob("*.parquet"))]

    assert shard_hashes(tmp_path / "a/outputs") == shard_hashes(tmp_path / "b/outputs")
    hashes = (tmp_path / "a/outputs/record_hashes.txt").read_text("utf-8").split()
    assert hashes == sorted(hashes)
    assert len(hashes) == first["rows"]


def test_huge_row_is_an_invalid_record(tmp_path: Path) -> None:
    huge = json.dumps({"text": "x" * (17 * 1024 * 1024)}).encode()
    m = _prep(
        tmp_path, {"big.jsonl": huge + b'\n{"text": "fine"}\n'}, {"schema_id": "pretrain.text.v1"}
    )
    assert m["passed"] is False
    assert m["invalid"] == 1
    report = json.loads((tmp_path / "outputs/validation_report.json").read_text("utf-8"))
    assert report["defect_counts"] == {"oversized_record": 1}


def _zip(entries: dict[str, bytes], level: int = zipfile.ZIP_DEFLATED) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", level) as z:
        for name, body in entries.items():
            z.writestr(name, body)
    return buf.getvalue()


def test_archives_are_unpacked(tmp_path: Path) -> None:
    body = _fixture("eval.v1.jsonl")
    zipped = _zip({"data/part.jsonl": body, "README.md": b"hi", "__MACOSX/._part.jsonl": b"x"})
    m = _prep(tmp_path, {"bundle.zip": zipped}, {"schema_id": "eval.v1"})
    assert m["passed"] is True
    assert m["rows"] == 50
    assert m["ignored_files"] == ["bundle.zip/README.md"]


def test_zip_bomb_is_refused(tmp_path: Path) -> None:
    bomb = _zip({"bomb.jsonl": b"\n" * (RATIO_FLOOR + MAX_RATIO * 1024 * 1024)})
    assert len(bomb) * MAX_RATIO < RATIO_FLOOR + MAX_RATIO * 1024 * 1024
    m = _prep(tmp_path, {"bomb.zip": bomb}, {"schema_id": "pretrain.text.v1"})
    assert m["passed"] is False
    assert m["error_code"] == "MRL-DATA-271"
    assert "times its packed size" in str(m["reason"])
    assert not (tmp_path / "data-prep-scratch").exists()


@pytest.mark.parametrize(
    "name", ["../escape.jsonl", "/abs.jsonl", "C:/win.jsonl", "a/../../b.jsonl"]
)
def test_path_traversal_is_refused(tmp_path: Path, name: str) -> None:
    m = _prep(
        tmp_path, {"evil.zip": _zip({name: b'{"text":"x"}\n'})}, {"schema_id": "pretrain.text.v1"}
    )
    assert m["error_code"] == "MRL-DATA-271"
    assert "outside its folder" in str(m["reason"])
    assert not (tmp_path / "escape.jsonl").exists()


def test_tar_links_are_refused(tmp_path: Path) -> None:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        link = tarfile.TarInfo("data.jsonl")
        link.type, link.linkname = tarfile.SYMTYPE, "/etc/passwd"
        t.addfile(link)
    m = _prep(tmp_path, {"evil.tar.gz": buf.getvalue()}, {"schema_id": "pretrain.text.v1"})
    assert m["error_code"] == "MRL-DATA-271"


def test_no_data_files(tmp_path: Path) -> None:
    m = _prep(tmp_path, {"notes.txt": b"hello"}, {"schema_id": "pretrain.text.v1"})
    assert m["passed"] is False
    assert m["error_code"] == "MRL-DATA-150"


def test_leakage_against_an_eval_set(tmp_path: Path) -> None:
    evals = [json.loads(line) for line in _fixture("eval.v1.jsonl").splitlines()]
    hashes = sorted({h for r in evals[:5] for h in content_hashes("eval.v1", r)})
    against = tmp_path / "against_0"
    against.mkdir(parents=True)
    (against / "content_hashes.txt").write_text("".join(h + "\n" for h in hashes), "utf-8")
    m = _prep(
        tmp_path,
        {"x.jsonl": _fixture("eval.v1.jsonl")},
        {"schema_id": "eval.v1", "against": {"against_0": "dsv_EVAL"}},
    )
    assert m["leakage"] == {"dsv_EVAL": {"records": 5, "percent": 10.0}}


def test_bad_inputs_are_named() -> None:
    issues = DATA_PREP.validate({"schema_id": "nope"}, {})
    assert {i.path for i in issues} >= {"/inputs/schema_id"}
    assert DATA_PREP.validate({"mode": "other"}, {})[0].path == "/inputs/mode"


@pytest.fixture(scope="module")
def tokenizer(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("models")
    make_fixtures.build_models(out)
    return out / "tiny-llama"


def test_tokenize_preview_histogram(tmp_path: Path, tokenizer: Path) -> None:
    _prep(
        tmp_path / "v",
        {"x.jsonl": _fixture("sft.messages.v1.jsonl")},
        {"schema_id": "sft.messages.v1"},
    )
    staged = {"dataset": tmp_path / "v/outputs", "tokenizer": tokenizer}
    inputs = {"mode": "tokenize", "context_length": 64}
    assert DATA_PREP.validate(inputs, staged) == []
    out = tmp_path / "tok"
    list(DATA_PREP.run(TaskContext(inputs, staged, out, lambda: False)))
    preview = json.loads((out / "token_preview.json").read_text("utf-8"))
    assert preview["records"] == 200
    assert preview["context_length"] == 64
    assert sum(b["count"] for b in preview["histogram"]) == preview["sequences"]
    assert preview["histogram"][-1]["high"] is None
    assert preview["mask_supported"] is True


def test_runs_in_the_builtin_engine(tmp_path: Path) -> None:
    tasks, problems = discover_tasks()
    assert problems == []
    assert {"data_prep", "import_dataset", "transform", "materialize_dataset"} <= set(tasks)
    assert tasks["import_dataset"].job_kind == "IMPORT"
    src = tmp_path / "source"
    src.mkdir()
    shutil.copy(DATASETS / "rl.prompt.v1.jsonl", src)
    script = AgentScript(
        job_spec={
            "job_kind": "DATA_PREP",
            "task": "data_prep",
            "inputs": {"schema_id": "rl.prompt.v1"},
        },
        staging_dir=tmp_path / "staging",
        staged_paths={"source": str(src)},
    )
    r = asyncio.run(run_in_process(BuiltinEngine(), load_builtin_manifest().engine, script))
    assert r.exit_report is not None
    assert r.exit_report.kind == pb.EXIT_KIND_COMPLETED
    out = tmp_path / "staging/outputs"
    manifest = json.loads((out / "artifact-manifest.json").read_text("utf-8"))
    paths = {f["path"] for f in manifest["files"]}
    assert {"dataset.json", "validation_report.json", "record_hashes.txt"} <= paths
    assert any(p.startswith("shards/") for p in paths)
    assert manifest["kind"] == "dataset"


P27 = DATASETS.parent / "p27"


def _version_folders(tmp: Path) -> dict[str, Path]:
    """What the server tests read: a READY, an INVALID and a filtered (transform) version."""
    out: dict[str, Path] = {}
    for name, src in [
        ("ready", "sft.messages.v1.jsonl"),
        ("invalid", "sft.messages.v1-invalid.jsonl"),
    ]:
        _prep(tmp / name, {"data.jsonl": _fixture(src)}, {"schema_id": "sft.messages.v1"})
        out[name] = tmp / name / "outputs"
    spec = {
        "schema_version": "transform/v1",
        "name": "long-chats",
        "to": "sft.messages.v1",
        "steps": [{"op": "filter", "expr": "len(messages) > 2"}],
    }
    staged = {"parent": out["ready"]}
    assert TRANSFORM.validate({"spec": spec}, staged) == []
    out["filtered"] = tmp / "filtered"
    list(TRANSFORM.run(TaskContext({"spec": spec}, staged, out["filtered"], lambda: False)))
    return out


def test_server_fixtures_are_what_the_tasks_write(tmp_path: Path) -> None:
    """tests/fixtures/p27/ holds real task outputs. MRL_UPDATE_FIXTURES=1 rewrites them.
    Parquet bytes carry the writer's version, so shards are compared by their records; JSON by
    value (tied defect counts come in any order)."""
    for name, made in _version_folders(tmp_path).items():
        kept = P27 / name
        if os.environ.get("MRL_UPDATE_FIXTURES") == "1":
            shutil.rmtree(kept, ignore_errors=True)
            shutil.copytree(made, kept)
        files = sorted(p.relative_to(made).as_posix() for p in made.rglob("*") if p.is_file())
        assert files == sorted(
            p.relative_to(kept).as_posix() for p in kept.rglob("*") if p.is_file()
        )
        for f in files:
            if f.endswith(".json"):
                same = json.loads((made / f).read_bytes()) == json.loads((kept / f).read_bytes())
                assert same, f"{name}/{f}"
            elif not f.endswith(".parquet"):
                assert (made / f).read_bytes() == (kept / f).read_bytes(), f"{name}/{f}"
        if name != "invalid":
            assert list(DatasetAdapter(kept).records()) == list(DatasetAdapter(made).records())
