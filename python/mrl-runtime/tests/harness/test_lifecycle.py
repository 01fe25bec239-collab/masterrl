import asyncio
import json
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.harness import HarnessOptions
from mrl_runtime.testkit.agent import AgentScript, FakeAgent
from mrl_runtime.testkit.fake import FakeEngine, manifest
from mrl_runtime.testkit.runner import AttemptResult, Control, job_spec, run_in_process

ALGO = "x-fake.steps"


def _run(
    tmp_path: Path,
    engine: Mapping[str, Any],
    *,
    control: Control | None = None,
    options: HarnessOptions | None = None,
    **script: Any,
) -> AttemptResult:
    spec = script.pop("spec", None) or job_spec(ALGO, engine)
    s = AgentScript(job_spec=spec, staging_dir=tmp_path, **script)
    return asyncio.run(
        run_in_process(FakeEngine(), manifest(), s, control=control, options=options)
    )


def _cls(result: AttemptResult) -> str:
    report = result.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_FAILED
    return f"{common.FailureClass.Name(getattr(report.failure, 'class'))}/{report.failure.sub_code}"


def test_completed_run_reports_everything(tmp_path: Path) -> None:
    r = _run(
        tmp_path, {"steps": 4, "checkpoint_every": 2, "samples_every": 2, "emit_undeclared": True}
    )
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_COMPLETED
    assert r.exit_code == 0
    assert report.final_step == 4
    assert report.last_committed_checkpoint_id
    assert set(report.final_metrics) >= {"train/loss", "train/learning_rate"}
    assert r.prepared is not None
    assert r.prepared.estimate.total_steps == 4
    assert len(r.prepared.effective_config_sha256) == 64
    assert (tmp_path / "effective-config.json").is_file()
    names = {(p.name, p.undeclared, p.kind) for p in r.points()}
    assert ("train/loss", False, pb.METRIC_KIND_LIVE) in names
    assert ("train/learning_rate", False, pb.METRIC_KIND_SAMPLED) in names  # via namespace "lr"
    assert ("fake/debug/grad_norm", True, pb.METRIC_KIND_LIVE) in names  # via prefix "debug/"
    assert len(r.agent.of("samples")) == 2
    assert [m.progress.step for m in r.agent.of("progress")][-1] == 4
    assert r.agent.of("heartbeat")
    kinds = [m.WhichOneof("body") for m in r.agent.received]
    assert kinds[0] == "prepared"
    assert kinds[-1] == "exit"
    assert r.agent.protocol_errors == []


def test_pause_checkpoints_then_exits_paused(tmp_path: Path) -> None:
    async def pause(agent: FakeAgent, _: object) -> None:
        await agent.wait_for(lambda m: m.HasField("progress") and m.progress.step >= 2)
        await agent.send(pb.AgentMessage(pause=pb.PauseRequest(request_id="p1", deadline_s=10)))

    r = _run(tmp_path, {"steps": 200, "checkpoint_every": 0, "step_seconds": 0.01}, control=pause)
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_PAUSED
    assert "p1" in report.acknowledged_requests
    committed = r.committed()
    assert committed
    assert report.last_committed_checkpoint_id == committed[-1][0]
    assert report.final_step < 200


def test_on_demand_checkpoint_keeps_training(tmp_path: Path) -> None:
    async def ask(agent: FakeAgent, _: object) -> None:
        await agent.wait_for(lambda m: m.HasField("progress") and m.progress.step >= 2)
        request = pb.AgentMessage(checkpoint_request=pb.CheckpointRequest(request_id="c1"))
        await agent.send(request)
        await agent.wait_for(lambda m: m.HasField("checkpoint_staged"))
        await agent.send(request)

    r = _run(tmp_path, {"steps": 30, "checkpoint_every": 0, "step_seconds": 0.01}, control=ask)
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_COMPLETED
    assert "c1" in report.acknowledged_requests
    assert len(r.committed()) == 2  # the requested one and the final one


def test_cancel_before_start(tmp_path: Path) -> None:
    async def cancel(agent: FakeAgent, _: object) -> None:
        await agent.wait_for(lambda m: m.HasField("prepared"))
        await agent.send(pb.AgentMessage(cancel=pb.CancelRequest(request_id="x", grace_s=5)))

    r = _run(tmp_path, {"steps": 3}, control=cancel, auto_start=False)
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_CANCELLED
    assert "x" in report.acknowledged_requests


def test_gpu_oom_is_classified_and_traceback_staged(tmp_path: Path) -> None:
    r = _run(tmp_path, {"steps": 5, "fail_at_step": 3, "failure": "oom"})
    assert _cls(r) == "FAILURE_CLASS_OOM/OOM_GPU"
    assert r.exit_code == 1
    report = r.exit_report
    assert report is not None
    detail = tmp_path / report.failure.detail_ref
    assert "CUDA out of memory" in detail.read_text()
    assert report.final_step == 2


def test_adapter_hook_classifies_data_errors(tmp_path: Path) -> None:
    r = _run(tmp_path, {"steps": 5, "fail_at_step": 2, "failure": "data"})
    assert _cls(r) == "FAILURE_CLASS_DATA/DATA_FAKE_ROW"


def test_nan_loss_stops_the_run(tmp_path: Path) -> None:
    spec = job_spec(
        ALGO, {"steps": 50, "fail_at_step": 3, "failure": "nan"}, harness={"nan_patience": 2}
    )
    r = _run(tmp_path, {}, spec=spec)
    assert _cls(r) == "FAILURE_CLASS_NUMERIC_DIVERGENCE/NAN_LOSS"


def test_watchdog_reports_stalled_when_the_engine_hangs(tmp_path: Path) -> None:
    harness = {"stall_timeout_s": 0.3, "cancel_grace_s": 0.3}
    spec = job_spec(ALGO, {"steps": 5, "fail_at_step": 2, "failure": "hang"}, harness=harness)
    r = _run(tmp_path, {}, spec=spec)
    assert _cls(r) == "FAILURE_CLASS_STALLED/NO_PROGRESS"


def test_lease_deadline_stops_the_attempt(tmp_path: Path) -> None:
    async def lease(agent: FakeAgent, _: object) -> None:
        await agent.wait_for(lambda m: m.HasField("progress"))
        past = Timestamp()
        past.GetCurrentTime()
        past.seconds -= 1
        await agent.send(
            pb.AgentMessage(lease_deadline=pb.LeaseDeadline(deadline=past, fencing_epoch=2))
        )

    r = _run(tmp_path, {"steps": 500, "step_seconds": 0.01}, control=lease)
    assert _cls(r) == "FAILURE_CLASS_NODE_LOST/LEASE_EXPIRED"


def test_validation_errors_fail_before_start(tmp_path: Path) -> None:
    r = _run(tmp_path, {"steps": 0, "nope": 1})
    prepared = r.prepared
    assert prepared is not None
    assert not prepared.ok
    assert {i.path for i in prepared.issues} == {"/engine/steps", "/engine/nope"}
    assert _cls(r) == "FAILURE_CLASS_USER_CONFIG/MRL-VAL-003"
    assert not any(m.HasField("start") for m in r.agent.sent)


def test_unknown_algorithm_and_missing_model_hash(tmp_path: Path) -> None:
    spec = job_spec("sft", {})
    del spec["base_model_sha256"]
    r = _run(tmp_path, {}, spec=spec)
    prepared = r.prepared
    assert prepared is not None
    assert {(i.code, i.path) for i in prepared.issues} == {
        ("MRL-CAP-001", "/algorithm"),
        ("MRL-VAL-002", "/base_model_sha256"),
    }


def test_dry_run_and_smoke(tmp_path: Path) -> None:
    dry = _run(tmp_path / "dry", {"steps": 3}, dry_run=True)
    assert dry.exit_report is not None
    assert dry.exit_report.kind == pb.EXIT_KIND_DRY_RUN_OK
    assert not dry.agent.of("progress")
    smoke = _run(tmp_path / "smoke", {"steps": 50, "checkpoint_every": 1}, smoke_steps=2)
    report = smoke.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_COMPLETED
    assert report.final_step <= 3
    assert not smoke.agent.of("checkpoint_commit")  # smoke runs commit nothing
    assert report.last_committed_checkpoint_id == ""


def test_secondary_role_sends_only_logs_heartbeats_and_exit(tmp_path: Path) -> None:
    opts = HarnessOptions(
        attempt_id="att_2", nonce=secrets.token_hex(8), role_id="rank1", primary=False
    )
    r = _run(tmp_path, {"steps": 3}, options=opts)
    kinds = {m.WhichOneof("body") for m in r.agent.received}
    assert kinds <= {"prepared", "log", "exit"}
    assert r.exit_report is not None
    assert r.exit_report.kind == pb.EXIT_KIND_COMPLETED


def test_export_job_writes_artifact_manifest(tmp_path: Path) -> None:
    first = _run(tmp_path / "train", {"steps": 2})
    where = first.committed()[-1][2]
    spec = job_spec(ALGO, {}, job_kind="EXPORT", export={"format": "hf-safetensors"})
    r = _run(tmp_path / "export", {}, spec=spec, staged_paths={"checkpoint": str(where)})
    assert r.exit_report is not None
    assert r.exit_report.kind == pb.EXIT_KIND_COMPLETED
    doc = json.loads((tmp_path / "export" / "outputs" / "artifact-manifest.json").read_text())
    assert doc["kind"] == "export"
    assert [f["path"] for f in doc["files"]] == ["model.safetensors"]


def test_protocol_mismatch_exits_without_report(tmp_path: Path) -> None:
    r = _run(tmp_path, {"steps": 2}, protocol_versions=("2.0",))
    assert r.exit_code == 2
    assert r.exit_report is None


@pytest.mark.parametrize(
    ("harness", "path"),
    [
        ({"stall_timeout_s": "oops"}, "/harness/stall_timeout_s"),
        ({"stall_timeout_s": float("inf")}, "/harness/stall_timeout_s"),
        ({"cancel_grace_s": -1}, "/harness/cancel_grace_s"),
        ({"max_pending_checkpoints": 1.5}, "/harness/max_pending_checkpoints"),
        ({"nan_patience": True}, "/harness/nan_patience"),
        ({"speed": 2}, "/harness/speed"),
        ("fast", "/harness"),
    ],
)
def test_bad_harness_settings_are_reported_not_crashed(
    tmp_path: Path, harness: object, path: str
) -> None:
    r = _run(tmp_path, {}, spec=job_spec(ALGO, {"steps": 2}, harness=harness))
    prepared = r.prepared
    assert prepared is not None
    assert not prepared.ok
    assert [(i.code, i.path) for i in prepared.issues] == [("MRL-VAL-003", path)]
    assert _cls(r) == "FAILURE_CLASS_USER_CONFIG/MRL-VAL-003"
    assert r.exit_code == 1
