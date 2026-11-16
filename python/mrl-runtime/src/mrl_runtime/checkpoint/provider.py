"""CheckpointProvider (Doc 05 §12.7): stage, hash and commit the checkpoints an engine writes.

Flow per checkpoint: move the engine's directory into <staging>/checkpoints/<id>/, hash its
files in parallel, write checkpoint-manifest.json (P09 schema), send CheckpointStaged, place
files where the UploadGrant says, send CheckpointCommitRequest, await CheckpointCommitAck.
Commits run in the background so training keeps going; at most `max_pending` checkpoints wait
for an ack at once, after which staging the next one waits for a slot.
"""

import asyncio
import json
import secrets
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from google.protobuf.message import Message

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter.events import CheckpointReady
from mrl_runtime.checkpoint.files import hash_files, list_files, now_rfc3339, sha256_file
from mrl_runtime.errors import LOCAL_FILE_IO, MrlError
from mrl_runtime.protocol.schemas import schema_errors

MANIFEST_FILE = "checkpoint-manifest.json"
_KIND = {
    "full_resume": pb.CHECKPOINT_KIND_FULL_RESUME,
    "weights_only": pb.CHECKPOINT_KIND_WEIGHTS_ONLY,
    "adapter_only": pb.CHECKPOINT_KIND_ADAPTER_ONLY,
}
# File name hints -> checkpoint-manifest role. First match wins; default "other".
_ROLES: tuple[tuple[str, str], ...] = (
    ("adapter_model", "adapter"),
    ("adapter_config", "adapter"),
    ("optimizer", "optimizer"),
    ("scheduler", "scheduler"),
    ("rng_state", "rng"),
    ("trainer_state", "trainer_state"),
    ("tokenizer", "tokenizer"),
    ("special_tokens_map", "tokenizer"),
    ("vocab", "tokenizer"),
    ("merges.txt", "tokenizer"),
    ("config.json", "config"),
    ("generation_config", "config"),
    (".safetensors", "weights"),
    (".bin", "weights"),
    (".pt", "weights"),
    (".pth", "weights"),
    (".npz", "weights"),
    (".gguf", "weights"),
)


def file_role(rel_path: str) -> str:
    name = rel_path.rsplit("/", 1)[-1].lower()
    return next((role for hint, role in _ROLES if hint in name), "other")


def new_checkpoint_id() -> str:
    """ckpt_ + a UUIDv7 (time-ordered)."""
    value = (time.time_ns() // 1_000_000) << 80 | secrets.randbits(80)
    value = (value & ~(0xF << 76)) | (0x7 << 76)  # version 7
    value = (value & ~(0x3 << 62)) | (0x2 << 62)  # RFC 4122 variant
    h = f"{value:032x}"
    return f"ckpt_{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


@dataclass
class StagedCheckpoint:
    checkpoint_id: str
    step: int
    directory: Path
    manifest_sha256: str
    message: pb.CheckpointStaged
    is_final: bool


@dataclass
class CommitResult:
    checkpoint_id: str
    state: pb.CommitState
    reason: str = ""

    @property
    def committed(self) -> bool:
        return self.state == pb.COMMIT_STATE_COMMITTED


@dataclass
class _Pending:
    staged: StagedCheckpoint
    grant: asyncio.Future[pb.UploadGrant] = field(
        default_factory=lambda: asyncio.get_running_loop().create_future()
    )
    ack: asyncio.Future[pb.CheckpointCommitAck] = field(
        default_factory=lambda: asyncio.get_running_loop().create_future()
    )


@dataclass(frozen=True)
class CheckpointSettings:
    staging_dir: Path
    attempt_id: str
    run_id: str
    engine_name: str
    adapter_version: str
    fmt: str
    base_model_sha256: str
    resume_from: Path | None = None
    max_pending: int = 2
    commit: bool = True  # False for smoke runs: stage only


class CheckpointProvider:
    def __init__(self, send: Callable[[Message], None], settings: CheckpointSettings) -> None:
        self._send = send
        self._staging = settings.staging_dir
        self._attempt_id = settings.attempt_id
        self._run_id = settings.run_id
        self._engine = {"name": settings.engine_name, "adapter_version": settings.adapter_version}
        self._format = settings.fmt
        self._base_sha = settings.base_model_sha256
        self._resume_from = settings.resume_from
        self._commit = settings.commit
        max_pending = settings.max_pending
        self._slots = asyncio.Semaphore(max(1, max_pending))
        self._pending: dict[str, _Pending] = {}
        self._tasks: set[asyncio.Task[CommitResult]] = set()
        self._parent = self._resume_parent()
        self.results: list[CommitResult] = []
        self.last_committed_id = ""
        self.final_result: CommitResult | None = None

    def restore_path(self) -> Path | None:
        """resume_from: a COMMITTED checkpoint the agent already staged locally."""
        return self._resume_from if self._resume_from and self._resume_from.exists() else None

    def _resume_parent(self) -> str:
        resume = self.restore_path()
        manifest = resume / MANIFEST_FILE if resume else None
        if manifest is None or not manifest.is_file():
            return ""
        return str(json.loads(manifest.read_text(encoding="utf-8")).get("checkpoint_id", ""))

    async def stage(self, ready: CheckpointReady, metrics: Mapping[str, float]) -> StagedCheckpoint:
        """Wait for a commit slot, then move, hash and describe the checkpoint."""
        await self._slots.acquire()
        try:
            return await asyncio.to_thread(self._stage_sync, ready, dict(metrics))
        except BaseException:
            self._slots.release()
            raise

    def _stage_sync(self, ready: CheckpointReady, metrics: dict[str, float]) -> StagedCheckpoint:
        checkpoint_id = new_checkpoint_id()
        target = self._staging / "checkpoints" / checkpoint_id
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(ready.path, target)
            rels = list_files(target, exclude=[MANIFEST_FILE])
            infos = hash_files(target, rels)
        except OSError as exc:
            raise MrlError(
                LOCAL_FILE_IO,
                "The app could not read or write a file on this computer.",
                detail=str(exc),
            ) from exc
        doc: dict[str, Any] = {
            "schema_version": "checkpoint-manifest/v1",
            "checkpoint_id": checkpoint_id,
            "run_id": self._run_id,
            "attempt_id": self._attempt_id,
            "step": ready.step,
            "kind": ready.kind,
            "format": self._format,
            "engine": self._engine,
            "base_model_sha256": self._base_sha,
            "created_at": now_rfc3339(),
            "is_final": ready.is_final,
            "files": [
                {"path": i.path, "size": i.size, "sha256": i.sha256, "role": file_role(i.path)}
                for i in infos
            ],
        }
        if metrics:
            doc["metrics"] = metrics
        if self._parent:
            doc["parent_checkpoint_id"] = self._parent
        if ready.policy_version is not None:
            doc["policy_version"] = ready.policy_version
        if ready.adapter is not None:
            a = ready.adapter
            doc["adapter"] = {
                "method": a.method,
                "rank": a.rank,
                "alpha": a.alpha,
                "target_modules": list(a.target_modules),
            }
        problems = schema_errors(doc, "checkpoint-manifest/v1")
        if problems:
            raise ValueError(f"checkpoint manifest does not match its schema: {problems}")
        manifest_path = target / MANIFEST_FILE
        manifest_path.write_text(json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
        manifest_sha = sha256_file(manifest_path)
        self._parent = checkpoint_id
        prefix = target.relative_to(self._staging).as_posix()
        message = pb.CheckpointStaged(
            checkpoint_id=checkpoint_id,
            step=ready.step,
            format=self._format,
            kind=_KIND[ready.kind],
            is_final=ready.is_final,
            manifest_sha256=manifest_sha,
            metrics_at_step=metrics,
            policy_version=ready.policy_version or 0,
        )
        for i in infos:
            message.files.add(
                path=f"{prefix}/{i.path}", size=i.size, sha256=i.sha256, role=file_role(i.path)
            )
        message.files.add(
            path=f"{prefix}/{MANIFEST_FILE}",
            size=manifest_path.stat().st_size,
            sha256=manifest_sha,
            role="manifest",
        )
        return StagedCheckpoint(
            checkpoint_id, ready.step, target, manifest_sha, message, ready.is_final
        )

    def submit(self, staged: StagedCheckpoint) -> None:
        """Start the commit in the background. Smoke runs (commit=False) stage only."""
        if not self._commit:
            self._slots.release()
            return
        pending = _Pending(staged)
        self._pending[staged.checkpoint_id] = pending
        task = asyncio.create_task(self._commit_one(pending))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _commit_one(self, pending: _Pending) -> CommitResult:
        staged = pending.staged
        try:
            self._send(staged.message)
            grant = await pending.grant
            await asyncio.to_thread(self._place, grant)
            self._send(
                pb.CheckpointCommitRequest(
                    checkpoint_id=staged.checkpoint_id, manifest_sha256=staged.manifest_sha256
                )
            )
            ack = await pending.ack
            result = CommitResult(staged.checkpoint_id, ack.state, ack.reason)
        except (MrlError, OSError, ValueError) as exc:
            result = CommitResult(staged.checkpoint_id, pb.COMMIT_STATE_REJECTED, str(exc))
        finally:
            self._pending.pop(staged.checkpoint_id, None)
            self._slots.release()
        self.results.append(result)
        if result.committed:
            self.last_committed_id = staged.checkpoint_id
        if staged.is_final:
            self.final_result = result
        return result

    def _place(self, grant: pb.UploadGrant) -> None:
        """Move each staged file to the local path the agent picked. Paths stay in staging."""
        root = self._staging.resolve()
        for src_rel, dst_rel in grant.targets.items():
            src, dst = (root / src_rel).resolve(), (root / dst_rel).resolve()
            if not (src.is_relative_to(root) and dst.is_relative_to(root)):
                raise ValueError(
                    f"upload grant points outside the staging directory: {src_rel} -> {dst_rel}"
                )
            if src != dst:
                dst.parent.mkdir(parents=True, exist_ok=True)
                src.replace(dst)

    def on_grant(self, grant: pb.UploadGrant) -> None:
        pending = self._pending.get(grant.checkpoint_id)
        if pending and not pending.grant.done():
            pending.grant.set_result(grant)

    def on_ack(self, ack: pb.CheckpointCommitAck) -> None:
        pending = self._pending.get(ack.checkpoint_id)
        if pending and not pending.ack.done():
            pending.ack.set_result(ack)

    @property
    def in_flight(self) -> int:
        return len(self._pending)

    async def wait_all(self, timeout: float) -> bool:
        """Wait for every commit started so far. False if some are still waiting."""
        if not self._tasks:
            return True
        _, waiting = await asyncio.wait(set(self._tasks), timeout=timeout)
        return not waiting
