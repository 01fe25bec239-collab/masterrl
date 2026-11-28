import asyncio
import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter import load_engine, resolve_engine
from mrl_runtime.builtin import HASH_FILES, BuiltinEngine, discover_tasks, load_builtin_manifest
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.protocol.schemas import schema_errors
from mrl_runtime.testkit.agent import AgentScript
from mrl_runtime.testkit.runner import AttemptResult, run_in_process


def test_manifest_and_task_registry() -> None:
    m = load_builtin_manifest()
    assert m.engine.name == "mrl-builtin"
    assert m.job_kinds["DATA_PREP"].sandbox == "data_prep"
    tasks, problems = discover_tasks()
    assert tasks["hash_files"] is HASH_FILES
    assert problems == []
    assert all(t.job_kind in m.job_kinds for t in tasks.values())
    assert isinstance(load_engine(resolve_engine("builtin")), BuiltinEngine)


def _run(
    tmp_path: Path,
    spec: dict[str, object],
    staged: dict[str, str],
    engine: BuiltinEngine | None = None,
    **script: Any,
) -> AttemptResult:
    s = AgentScript(job_spec=spec, staging_dir=tmp_path / "staging", staged_paths=staged, **script)
    return asyncio.run(run_in_process(engine or BuiltinEngine(), load_builtin_manifest().engine, s))


def test_hash_files_end_to_end(tmp_path: Path) -> None:
    data = tmp_path / "data"
    (data / "nested").mkdir(parents=True)
    for i in range(20):
        (data / "nested" / f"f{i}.txt").write_text(f"row {i}\n")
    single = tmp_path / "one.bin"
    single.write_bytes(b"\x00\x01")
    spec = {
        "job_kind": "DATA_PREP",
        "task": "hash_files",
        "inputs": {"roles": ["dataset.train", "extra"]},
    }
    r = _run(tmp_path, spec, {"dataset.train": str(data), "extra": str(single)})
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_COMPLETED
    progress = [m.progress for m in r.agent.of("progress")]
    assert progress[-1].step == progress[-1].total_steps == 21
    out = tmp_path / "staging" / "outputs"
    hashes = json.loads((out / "hashes.json").read_text())["files"]
    assert len(hashes) == 21
    row = next(h for h in hashes if h["path"] == "nested/f3.txt")
    assert row == {
        "role": "dataset.train",
        "path": "nested/f3.txt",
        "size": 6,
        "sha256": hashlib.sha256(b"row 3\n").hexdigest(),
    }
    manifest = json.loads((out / "artifact-manifest.json").read_text())
    assert schema_errors(manifest, "artifact-manifest/v1") == []
    assert [f["path"] for f in manifest["files"]] == ["hashes.json"]
    assert manifest["source"]["task"] == "hash_files"
    # The worker handed the manifest to the agent and exited only after the upload.
    assert r.agent.outputs_manifest == manifest


def test_outputs_that_cannot_be_uploaded_fail_the_attempt(tmp_path: Path) -> None:
    single = tmp_path / "one.bin"
    single.write_bytes(b"x")
    spec = {"job_kind": "DATA_PREP", "task": "hash_files", "inputs": {"roles": ["extra"]}}
    r = _run(tmp_path, spec, {"extra": str(single)}, outputs_reason="storage is down")
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_FAILED
    assert report.failure.sub_code == "OUTPUTS_NOT_UPLOADED"
    assert getattr(report.failure, "class") == common.FAILURE_CLASS_INFRA_TRANSIENT
    assert "storage is down" in report.failure.message


def test_a_builtin_export_runs_its_task_not_engine_export(tmp_path: Path) -> None:
    def run(ctx: TaskContext) -> Iterator[TaskEvent]:
        (ctx.output_dir / "model.bin").write_bytes(b"weights")
        yield TaskOutput("model.bin", "model")

    engine = BuiltinEngine()
    engine.tasks["export_x"] = BuiltinTask("export_x", "EXPORT", "export", lambda i, s: [], run)
    r = _run(tmp_path, {"job_kind": "EXPORT", "task": "export_x", "inputs": {}}, {}, engine)
    assert r.exit_report is not None
    assert r.exit_report.kind == pb.EXIT_KIND_COMPLETED
    assert r.agent.outputs_manifest is not None
    assert [f["path"] for f in r.agent.outputs_manifest["files"]] == ["model.bin"]


def test_unknown_task_and_bad_inputs(tmp_path: Path) -> None:
    r = _run(tmp_path / "a", {"job_kind": "DATA_PREP", "task": "nope"}, {})
    assert r.prepared is not None
    assert [i.code for i in r.prepared.issues] == ["MRL-CAP-130"]
    r = _run(tmp_path / "b", {"task": "hash_files", "inputs": {"roles": ["missing"]}}, {})
    assert r.prepared is not None
    assert [(i.code, i.path) for i in r.prepared.issues] == [("MRL-VAL-131", "/inputs/roles")]
    report = r.exit_report
    assert report is not None
    assert getattr(report.failure, "class") == common.FAILURE_CLASS_USER_CONFIG
    r = _run(
        tmp_path / "c", {"job_kind": "EXPORT", "task": "hash_files", "inputs": {"roles": []}}, {}
    )
    assert r.prepared is not None
    assert [i.path for i in r.prepared.issues] == ["/job_kind"]
