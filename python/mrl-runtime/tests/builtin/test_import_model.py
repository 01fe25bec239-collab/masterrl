"""The import_model task: staged, node-path and Hub sources, the format gate, the report."""

import asyncio
import json
import os
import re
import shutil
from pathlib import Path

import make_fixtures
import pytest

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.builtin import BuiltinEngine, discover_tasks, load_builtin_manifest
from mrl_runtime.builtin.import_model import IMPORT_MODEL, INSPECTION_FILE, license_of
from mrl_runtime.builtin.tasks import TaskContext, TaskOutput
from mrl_runtime.protocol.schemas import schema_errors
from mrl_runtime.testkit.agent import AgentScript
from mrl_runtime.testkit.runner import run_in_process

GOLDEN = json.loads(make_fixtures.GOLDEN_PATH.read_text())
# A public tiny repository, pinned (Hub test only).
HUB_REPO = "hf-internal-testing/tiny-random-LlamaForCausalLM"
HUB_COMMIT = "9fb191250dd56d0ba7ec9785a025ed29c03d5998"


@pytest.fixture(scope="module")
def models(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("models")
    make_fixtures.build_models(out)
    return out


def run(inputs: dict[str, object], staged: dict[str, Path], out: Path) -> list[TaskOutput]:
    assert IMPORT_MODEL.validate(inputs, staged) == []
    ctx = TaskContext(inputs, staged, out, lambda: False)
    return [e for e in IMPORT_MODEL.run(ctx) if isinstance(e, TaskOutput)]


def report(out: Path) -> dict[str, object]:
    return json.loads((out / INSPECTION_FILE).read_text())


def test_registered_for_import_jobs() -> None:
    tasks, problems = discover_tasks()
    assert problems == []
    assert tasks["import_model"] is IMPORT_MODEL
    assert load_builtin_manifest().job_kinds["IMPORT"].sandbox == "fetch"


def test_staged_model_gets_the_golden_inspection(models: Path, tmp_path: Path) -> None:
    outs = run({"source": {"kind": "staged"}}, {"model": models / "tiny-llama"}, tmp_path)
    assert [o.path for o in outs] == [INSPECTION_FILE]
    r = report(tmp_path)
    assert r["architecture"] == GOLDEN["architecture"]
    assert r["param_count"] == GOLDEN["param_count"]
    assert r["dtype"] == GOLDEN["dtype"]
    assert r["context_length"] == GOLDEN["context_length"]
    assert r["chat_template_sha256"] == GOLDEN["chat_template_sha256"]
    assert r["kind"] == "full_weights"
    assert r["refusal"] is None
    assert "tensors" not in r
    assert "path" not in r
    assert r["license"] == license_of(models / "tiny-llama")
    assert r["license"]["kind"] == "unknown"  # type: ignore[index]


def test_node_path_copies_kept_files_and_drops_pickles(models: Path, tmp_path: Path) -> None:
    src = Path(shutil.copytree(models / "tiny-llama", tmp_path / "src"))
    shutil.copyfile(models / "tiny-llama-pickle" / "pytorch_model.bin", src / "pytorch_model.bin")
    (src / "LICENSE").write_text("Apache License 2.0\n")
    (src / "onnx").mkdir()
    (src / "onnx" / "model.onnx").write_bytes(b"onnx")
    out = tmp_path / "out"
    outs = run({"source": {"kind": "node_path", "path": str(src)}}, {}, out)
    paths = sorted(o.path for o in outs)
    assert "model/pytorch_model.bin" not in paths
    assert "model/onnx/model.onnx" not in paths
    assert "model/model.safetensors" in paths
    assert "model/LICENSE" in paths
    roles = {o.path: o.role for o in outs}
    assert roles["model/model.safetensors"] == "weights"
    assert report(out)["license"]["sha256"] == license_of(src)["sha256"]  # type: ignore[index]


def test_node_path_refuses_links(models: Path, tmp_path: Path) -> None:
    src = Path(shutil.copytree(models / "tiny-llama", tmp_path / "src"))
    if os.name == "nt":
        pytest.skip("symbolic links need extra rights on Windows")
    (src / "secret.json").symlink_to("/etc/hosts")
    ctx = TaskContext(
        {"source": {"kind": "node_path", "path": str(src)}}, {}, tmp_path / "o", lambda: False
    )
    with pytest.raises(Exception, match="link"):
        list(IMPORT_MODEL.run(ctx))


def test_pickle_only_is_refused_and_conversion_is_not_offered(models: Path, tmp_path: Path) -> None:
    staged = {"model": models / "tiny-llama-pickle"}
    run({"source": {"kind": "staged"}}, staged, tmp_path / "a")
    assert report(tmp_path / "a")["error"]["code"] == "MRL-MODEL-001"  # type: ignore[index]
    run({"source": {"kind": "staged"}, "convert_pickle": True}, staged, tmp_path / "b")
    assert report(tmp_path / "b")["error"]["code"] == "MRL-MODEL-260"  # type: ignore[index]


def test_bad_inputs() -> None:
    v = IMPORT_MODEL.validate
    assert [
        i.path for i in v({"source": {"kind": "hub", "repo": "../x", "revision": "a..b"}}, {})
    ] == [
        "/inputs/source/repo",
        "/inputs/source/revision",
    ]
    assert [i.path for i in v({"source": {"kind": "node_path", "path": "rel"}}, {})] == [
        "/inputs/source/path"
    ]
    assert [i.path for i in v({"source": {"kind": "staged"}}, {})] == ["/inputs/source"]
    assert [i.path for i in v({}, {})] == ["/inputs/source/kind"]


def test_through_the_builtin_engine(models: Path, tmp_path: Path) -> None:
    spec = {"job_kind": "IMPORT", "task": "import_model", "inputs": {"source": {"kind": "staged"}}}
    script = AgentScript(
        job_spec=spec,
        staging_dir=tmp_path / "staging",
        staged_paths={"model": str(models / "tiny-llama")},
    )
    r = asyncio.run(run_in_process(BuiltinEngine(), load_builtin_manifest().engine, script))
    assert r.exit_report is not None
    assert r.exit_report.kind == pb.EXIT_KIND_COMPLETED
    manifest = json.loads((tmp_path / "staging/outputs/artifact-manifest.json").read_text())
    assert schema_errors(manifest, "artifact-manifest/v1") == []
    assert manifest["kind"] == "model"
    assert [f["path"] for f in manifest["files"]] == [INSPECTION_FILE]


@pytest.mark.skipif(not os.environ.get("MRL_TEST_NETWORK"), reason="set MRL_TEST_NETWORK=1 (Hub)")
def test_hub_revision_resolves_to_a_commit_and_downloads_only_safe_files(tmp_path: Path) -> None:
    out = tmp_path / "pinned"
    outs = run({"source": {"kind": "hub", "repo": HUB_REPO, "revision": HUB_COMMIT}}, {}, out)
    paths = {o.path for o in outs}
    assert "model/model.safetensors" in paths
    assert "model/config.json" in paths
    assert not any("onnx" in p for p in paths)
    r = report(out)
    assert r["source"] == {"repo": HUB_REPO, "revision": HUB_COMMIT, "commit": HUB_COMMIT}
    assert r["architecture"] == "LlamaForCausalLM"
    assert r["kind"] == "full_weights"
    branch = tmp_path / "branch"
    run({"source": {"kind": "hub", "repo": HUB_REPO, "revision": "main"}}, {}, branch)
    commit = report(branch)["source"]["commit"]  # type: ignore[index]
    assert re.fullmatch(r"[0-9a-f]{40}", str(commit))
    assert not (tmp_path / "import-download").exists()
