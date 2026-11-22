import asyncio
import json
from pathlib import Path

import pytest

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter import AdapterInfo, CheckpointReady
from mrl_runtime.checkpoint import (
    MANIFEST_FILE,
    CheckpointProvider,
    CheckpointSettings,
    file_role,
    new_checkpoint_id,
    sha256_file,
)
from mrl_runtime.protocol.schemas import schema_errors
from mrl_runtime.testkit.agent import AgentScript
from mrl_runtime.testkit.fake import FakeEngine, manifest
from mrl_runtime.testkit.runner import AttemptResult, job_spec, run_in_process


def _settings(staging: Path, **kw: object) -> CheckpointSettings:
    base: dict[str, object] = {
        "staging_dir": staging,
        "attempt_id": "att_1",
        "run_id": "run_1",
        "engine_name": "fake",
        "adapter_version": "0.1.0",
        "fmt": "fake-v1",
        "base_model_sha256": "a" * 64,
    }
    return CheckpointSettings(**(base | kw))  # type: ignore[arg-type]


def _engine_dir(root: Path, step: int) -> Path:
    d = root / f"out/checkpoint-{step}"
    (d / "sub").mkdir(parents=True)
    (d / "model.safetensors").write_bytes(b"w" * 1000)
    (d / "optimizer.pt").write_bytes(b"o" * 10)
    (d / "sub" / "tokenizer.json").write_text("{}")
    (d / "trainer_state.json").write_text("{}")
    return d


def test_ids_and_roles() -> None:
    a, b = new_checkpoint_id(), new_checkpoint_id()
    assert a.startswith("ckpt_")
    assert a != b
    assert a[5:].split("-")[2][0] == "7"  # UUIDv7
    assert [
        file_role(p)
        for p in (
            "model-00001.safetensors",
            "optimizer.pt",
            "rng_state_0.pth",
            "x/tokenizer.json",
            "config.json",
            "adapter_model.safetensors",
            "notes.txt",
        )
    ] == ["weights", "optimizer", "rng", "tokenizer", "config", "adapter", "other"]


async def _stage_and_commit(tmp_path: Path) -> None:
    sent: list[object] = []
    provider = CheckpointProvider(sent.append, _settings(tmp_path, max_pending=1))
    ready = CheckpointReady(
        10,
        _engine_dir(tmp_path, 10),
        kind="adapter_only",
        is_final=True,
        adapter=AdapterInfo("lora", 8, 16.0, ("q_proj",)),
    )
    staged = await provider.stage(ready, {"train/loss": 0.5})
    assert not ready.path.exists()  # moved into staging
    doc = json.loads((staged.directory / MANIFEST_FILE).read_text())
    assert schema_errors(doc, "checkpoint-manifest/v1") == []
    assert doc["kind"] == "adapter_only"
    assert {f["path"] for f in doc["files"]} == {
        "model.safetensors",
        "optimizer.pt",
        "sub/tokenizer.json",
        "trainer_state.json",
    }
    assert staged.manifest_sha256 == sha256_file(staged.directory / MANIFEST_FILE)
    assert [f.role for f in staged.message.files][-1] == "manifest"
    assert all(
        f.path.startswith(f"checkpoints/{staged.checkpoint_id}/") for f in staged.message.files
    )
    provider.submit(staged)
    # the only slot is taken until the commit resolves
    second = asyncio.create_task(provider.stage(CheckpointReady(11, _engine_dir(tmp_path, 11)), {}))
    await asyncio.sleep(0.05)
    assert not second.done()
    await asyncio.sleep(0)
    assert isinstance(sent[0], pb.CheckpointStaged)
    provider.on_grant(
        pb.UploadGrant(
            checkpoint_id=staged.checkpoint_id,
            targets={f.path: f"upload/{f.path}" for f in staged.message.files},
        )
    )
    await asyncio.sleep(0.05)
    assert isinstance(sent[1], pb.CheckpointCommitRequest)
    assert (tmp_path / "upload" / staged.message.files[0].path).is_file()  # placed per the grant
    provider.on_ack(
        pb.CheckpointCommitAck(checkpoint_id=staged.checkpoint_id, state=pb.COMMIT_STATE_COMMITTED)
    )
    assert await provider.wait_all(5)
    assert provider.last_committed_id == staged.checkpoint_id
    assert provider.final_result is not None
    assert provider.final_result.committed
    third = await asyncio.wait_for(second, 5)
    assert (
        json.loads((third.directory / MANIFEST_FILE).read_text())["parent_checkpoint_id"]
        == staged.checkpoint_id
    )


def test_stage_commit_and_max_pending(tmp_path: Path) -> None:
    asyncio.run(_stage_and_commit(tmp_path))


def test_older_ack_does_not_replace_newer_committed_checkpoint(tmp_path: Path) -> None:
    async def run() -> None:
        sent: asyncio.Queue[object] = asyncio.Queue()
        provider = CheckpointProvider(sent.put_nowait, _settings(tmp_path, max_pending=2))
        checkpoints = [
            await provider.stage(CheckpointReady(step, _engine_dir(tmp_path, step)), {})
            for step in (2, 3)
        ]
        for checkpoint in checkpoints:
            provider.submit(checkpoint)
            assert isinstance(await asyncio.wait_for(sent.get(), 5), pb.CheckpointStaged)
            provider.on_grant(pb.UploadGrant(checkpoint_id=checkpoint.checkpoint_id))

        for _ in checkpoints:
            assert isinstance(await asyncio.wait_for(sent.get(), 5), pb.CheckpointCommitRequest)
        for checkpoint in reversed(checkpoints):
            provider.on_ack(
                pb.CheckpointCommitAck(
                    checkpoint_id=checkpoint.checkpoint_id, state=pb.COMMIT_STATE_COMMITTED
                )
            )
            await asyncio.sleep(0)

        assert await provider.wait_all(5)
        assert provider.last_committed_step == 3
        assert provider.last_committed_id == checkpoints[-1].checkpoint_id

    asyncio.run(run())


async def _escape(tmp_path: Path) -> None:
    provider = CheckpointProvider(lambda _m: None, _settings(tmp_path))
    staged = await provider.stage(CheckpointReady(1, _engine_dir(tmp_path, 1)), {})
    provider.submit(staged)
    await asyncio.sleep(0)
    first = staged.message.files[0].path
    provider.on_grant(
        pb.UploadGrant(checkpoint_id=staged.checkpoint_id, targets={first: "../../etc/x"})
    )
    assert await provider.wait_all(5)
    assert provider.results[0].state == pb.COMMIT_STATE_REJECTED
    assert "outside the staging directory" in provider.results[0].reason


def test_grant_cannot_escape_staging(tmp_path: Path) -> None:
    asyncio.run(_escape(tmp_path / "staging"))


def test_restore_path_and_parent(tmp_path: Path) -> None:
    resume = tmp_path / "resume"
    resume.mkdir()
    (resume / MANIFEST_FILE).write_text(json.dumps({"checkpoint_id": "ckpt_parent"}))

    async def go() -> str:
        provider = CheckpointProvider(lambda _m: None, _settings(tmp_path, resume_from=resume))
        assert provider.restore_path() == resume
        staged = await provider.stage(CheckpointReady(5, _engine_dir(tmp_path, 5)), {})
        return json.loads((staged.directory / MANIFEST_FILE).read_text())["parent_checkpoint_id"]

    assert asyncio.run(go()) == "ckpt_parent"


def test_a_resumed_attempt_reports_the_resumed_checkpoint_until_a_new_one_commits(
    tmp_path: Path,
) -> None:
    resume = tmp_path / "resume"
    resume.mkdir()
    (resume / MANIFEST_FILE).write_text(json.dumps({"checkpoint_id": "ckpt_parent"}))
    provider = CheckpointProvider(lambda _m: None, _settings(tmp_path, resume_from=resume))
    assert provider.last_committed_id == "ckpt_parent"
    fresh = CheckpointProvider(lambda _m: None, _settings(tmp_path))
    assert fresh.last_committed_id == ""


@pytest.mark.parametrize("state", [pb.COMMIT_STATE_CORRUPT, pb.COMMIT_STATE_REJECTED])
def test_final_checkpoint_not_committed_fails_the_run(
    tmp_path: Path, state: pb.CommitState
) -> None:
    script = AgentScript(
        job_spec=job_spec("x-fake.steps", {"steps": 2}), staging_dir=tmp_path, commit_state=state
    )
    r: AttemptResult = asyncio.run(run_in_process(FakeEngine(), manifest(), script))
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_FAILED
    assert getattr(report.failure, "class") == common.FAILURE_CLASS_VERIFICATION_FAILED


def test_grant_to_other_paths_end_to_end(tmp_path: Path) -> None:
    script = AgentScript(
        job_spec=job_spec("x-fake.steps", {"steps": 2}), staging_dir=tmp_path, grant_prefix="up/"
    )
    r = asyncio.run(run_in_process(FakeEngine(), manifest(), script))
    assert r.exit_report is not None
    assert r.exit_report.kind == pb.EXIT_KIND_COMPLETED
    assert all(where.is_relative_to(tmp_path / "up") for _, _, where in r.committed())
