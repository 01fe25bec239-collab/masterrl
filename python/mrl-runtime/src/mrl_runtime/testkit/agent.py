"""FakeAgent: an in-process WorkerControlService server that plays the agent's side.

It answers Hello with HelloAck, sends Prepare, sends Start after a good Prepared, grants and
verifies checkpoint commits against the staged files, and acks every sequenced message. Tests
drive it with wait_for() and send(); restart() simulates an agent restart (it loses
the messages it received but had not acked, so the worker must replay them).

Transport: a Unix domain socket where the platform has one, TCP on 127.0.0.1 elsewhere.
"""

import asyncio
import contextlib
import hashlib
import json
import shutil
import socket
import sys
import tempfile
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import grpc
import grpc.aio

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb

SERVICE = "mrl.worker.v1.WorkerControlService"
UDS_SUPPORTED = sys.platform != "win32" and hasattr(socket, "AF_UNIX")
_Context = grpc.aio.ServicerContext[pb.WorkerMessage, pb.AgentMessage]


@dataclass
class AgentScript:
    job_spec: Mapping[str, Any]
    staging_dir: Path
    staged_paths: Mapping[str, str] = field(default_factory=dict[str, str])
    dry_run: bool = False
    smoke_steps: int = 0
    protocol_versions: tuple[str, ...] = ("1.0",)
    heartbeat_interval_s: int = 1
    max_message_bytes: int = 0
    auto_start: bool = True
    commit_state: pb.CommitState = pb.COMMIT_STATE_COMMITTED
    grant_prefix: str = ""  # non-empty: the grant moves each staged file under this prefix


async def _read(context: _Context) -> object:
    """The next request, or grpc.aio.EOF (the stubs leave EOF out of the type)."""
    return await context.read()


def _sha256(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


class FakeAgent:
    def __init__(self, script: AgentScript, *, use_uds: bool = UDS_SUPPORTED) -> None:
        self.script = script
        self._use_uds = use_uds
        self._tmp = Path(tempfile.mkdtemp(prefix="mrl-"))  # short: AF_UNIX paths max ~104 bytes
        self._port = 0
        self._server: grpc.aio.Server | None = None
        self._out: asyncio.Queue[pb.AgentMessage] | None = None
        self._cond = asyncio.Condition()
        self._resume_override: int | None = None
        self._prepare_sent = False
        self._start_sent = False
        self._staged: dict[str, pb.CheckpointStaged] = {}
        self._grants: dict[str, dict[str, str]] = {}
        self.accepted: int = 0  # highest contiguous seq received this agent lifetime
        self.durable: int = 0  # highest seq acked: survives restart()
        self.hold_acks = False  # True: receive but do not ack (messages stay non-durable)
        self.received: list[pb.WorkerMessage] = []
        self.unsequenced: list[pb.WorkerMessage] = []
        self.sent: list[pb.AgentMessage] = []
        self.duplicates = 0
        self.protocol_errors: list[str] = []
        self.sessions = 0
        self.commits: dict[str, tuple[pb.CommitState, int, Path]] = {}

    @property
    def target(self) -> str:
        return f"unix:{self._tmp / 'agent.sock'}" if self._use_uds else f"127.0.0.1:{self._port}"

    async def start(self) -> str:
        server = grpc.aio.server()
        handler = grpc.stream_stream_rpc_method_handler(
            cast(Any, self._session),
            request_deserializer=pb.WorkerMessage.FromString,
            response_serializer=pb.AgentMessage.SerializeToString,
        )
        server.add_generic_rpc_handlers(
            (grpc.method_handlers_generic_handler(SERVICE, {"Session": handler}),)
        )
        if self._use_uds:
            (self._tmp / "agent.sock").unlink(missing_ok=True)
            server.add_insecure_port(self.target)
        else:
            self._port = server.add_insecure_port(f"127.0.0.1:{self._port}")
        await server.start()
        self._server = server
        return self.target

    async def stop(self) -> None:
        if self._server is not None:
            await self._server.stop(grace=None)
            self._server = None
        self._out = None

    async def close(self) -> None:
        await self.stop()
        shutil.rmtree(self._tmp, ignore_errors=True)

    async def restart(self) -> None:
        """Simulate an agent restart: messages received but not yet acked (durable) are lost."""
        await self.stop()
        self.accepted = self.durable
        self.received = [m for m in self.received if m.seq <= self.durable]
        self.hold_acks = False
        await self.start()

    async def send(self, msg: pb.AgentMessage) -> None:
        """Send a control message (pause, cancel, ...) on the current session."""
        msg.ack_seq = self.durable
        self.sent.append(msg)
        if self._out is not None:
            await self._out.put(msg)

    async def wait_for(
        self, predicate: Callable[[pb.WorkerMessage], bool], timeout: float = 20.0
    ) -> pb.WorkerMessage:
        def find() -> pb.WorkerMessage | None:
            return next((m for m in [*self.received, *self.unsequenced] if predicate(m)), None)

        async with asyncio.timeout(timeout), self._cond:
            await self._cond.wait_for(lambda: find() is not None)
        found = find()
        assert found is not None
        return found

    def of(self, kind: str) -> list[pb.WorkerMessage]:
        return [m for m in [*self.received, *self.unsequenced] if m.WhichOneof("body") == kind]

    @property
    def exit_report(self) -> pb.ExitReport | None:
        found = self.of("exit")
        return found[-1].exit if found else None

    @property
    def prepared(self) -> pb.Prepared | None:
        found = self.of("prepared")
        return found[-1].prepared if found else None

    async def _session(self, _requests: AsyncIterator[pb.WorkerMessage], context: _Context) -> None:
        first = await _read(context)
        if not isinstance(first, pb.WorkerMessage) or not first.HasField("hello"):
            return
        self.sessions += 1
        await self._record(first)
        offered = set(first.hello.protocol_versions)
        common = [v for v in self.script.protocol_versions if v in offered]
        chosen = max(
            common or self.script.protocol_versions,
            key=lambda v: tuple(int(p) for p in v.split(".")),
        )
        ack = pb.HelloAck(
            protocol_version=chosen,
            heartbeat_interval_s=self.script.heartbeat_interval_s,
            max_message_bytes=self.script.max_message_bytes,
            resume_from_seq=self.durable + 1,
        )
        await context.write(pb.AgentMessage(ack_seq=self.durable, hello_ack=ack))
        if not common:
            return  # no common version: the session is refused
        out: asyncio.Queue[pb.AgentMessage] = asyncio.Queue()
        self._out = out
        if not self._prepare_sent:
            self._prepare_sent = True
            await self._queue(self._prepare_msg())

        async def write_loop() -> None:
            while True:
                await context.write(await out.get())

        writer = asyncio.create_task(write_loop())
        try:
            while True:
                msg = await _read(context)
                if not isinstance(msg, pb.WorkerMessage):
                    return
                await self._on_message(msg)
        finally:
            writer.cancel()
            with contextlib.suppress(
                asyncio.CancelledError, grpc.aio.AioRpcError, grpc.aio.UsageError
            ):
                await writer

    def _prepare_msg(self) -> pb.AgentMessage:
        s = self.script
        prepare = pb.Prepare(
            job_spec_json=json.dumps(dict(s.job_spec)),
            staged_paths=dict(s.staged_paths),
            dry_run=s.dry_run,
            smoke_steps=s.smoke_steps,
            staging_dir=str(s.staging_dir),
            allocation=pb.Allocation(world_size=1),
        )
        return pb.AgentMessage(prepare=prepare)

    async def _queue(self, msg: pb.AgentMessage) -> None:
        if self._out is not None:
            msg.ack_seq = self.durable
            self.sent.append(msg)
            await self._out.put(msg)

    async def _record(self, msg: pb.WorkerMessage) -> None:
        async with self._cond:
            (self.unsequenced if msg.seq == 0 else self.received).append(msg)
            self._cond.notify_all()

    async def _on_message(self, msg: pb.WorkerMessage) -> None:
        if msg.seq == 0:
            await self._record(msg)
            return
        if msg.seq <= self.accepted:
            self.duplicates += 1
            await self._queue(pb.AgentMessage())
            return
        if msg.seq != self.accepted + 1:
            self.protocol_errors.append(f"gap in worker seq: got {msg.seq} after {self.accepted}")
            return
        self.accepted = msg.seq
        if not self.hold_acks:
            self.durable = msg.seq
        await self._record(msg)
        await self._react(msg)
        await self._queue(pb.AgentMessage())  # ack

    async def _react(self, msg: pb.WorkerMessage) -> None:
        kind = msg.WhichOneof("body")
        if (
            kind == "prepared"
            and msg.prepared.ok
            and not self.script.dry_run
            and self.script.auto_start
            and not self._start_sent
        ):
            self._start_sent = True
            await self._queue(pb.AgentMessage(start=pb.Start()))
        elif kind == "checkpoint_staged":
            staged = msg.checkpoint_staged
            self._staged[staged.checkpoint_id] = staged
            prefix = self.script.grant_prefix
            targets = {f.path: f"{prefix}{f.path}" for f in staged.files}
            self._grants[staged.checkpoint_id] = targets
            grant = pb.UploadGrant(checkpoint_id=staged.checkpoint_id, targets=targets)
            await self._queue(pb.AgentMessage(upload_grant=grant))
        elif kind == "checkpoint_commit":
            req = msg.checkpoint_commit
            state, reason = self._verify(req)
            staged = self._staged[req.checkpoint_id]
            manifest = next(f.path for f in staged.files if f.role == "manifest")
            where = self.script.staging_dir / self._grants[req.checkpoint_id][manifest]
            self.commits[req.checkpoint_id] = (state, staged.step, where.parent)
            ack = pb.CheckpointCommitAck(
                checkpoint_id=req.checkpoint_id, state=state, reason=reason
            )
            await self._queue(pb.AgentMessage(checkpoint_commit_ack=ack))

    def _verify(self, req: pb.CheckpointCommitRequest) -> tuple[pb.CommitState, str]:
        staged = self._staged.get(req.checkpoint_id)
        if staged is None:
            return pb.COMMIT_STATE_REJECTED, "unknown checkpoint"
        if staged.manifest_sha256 != req.manifest_sha256:
            return pb.COMMIT_STATE_CORRUPT, "manifest hash changed"
        for f in staged.files:
            path = self.script.staging_dir / self._grants[req.checkpoint_id][f.path]
            if not path.is_file() or path.stat().st_size != f.size or _sha256(path) != f.sha256:
                return pb.COMMIT_STATE_CORRUPT, f"{f.path} does not match"
        if self.script.commit_state != pb.COMMIT_STATE_COMMITTED:
            return self.script.commit_state, "forced by the test script"
        return pb.COMMIT_STATE_COMMITTED, ""
