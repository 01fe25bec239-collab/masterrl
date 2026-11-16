"""Worker side of WorkerControlService.Session: one long-lived bidirectional stream.

Hello and Heartbeat are unsequenced (seq 0): they belong to one connection and are never
replayed. Every other message goes through the spool, gets a seq on first send, and is
resent from HelloAck.resume_from_seq after a reconnect. If the agent goes away the session
keeps reconnecting while the worker keeps training and spooling.
"""

import asyncio
import contextlib
import logging
from collections.abc import Sequence

import grpc.aio
from google.protobuf.message import Message

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.errors import PROTOCOL_MISMATCH, PROTOCOL_VIOLATION, MrlError
from mrl_runtime.protocol.spool import Spool

PROTOCOL_VERSIONS: tuple[str, ...] = ("1.0",)
METHOD = "/mrl.worker.v1.WorkerControlService/Session"
DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
_FIELD_BY_TYPE = {
    f.message_type.full_name: f.name
    for f in pb.WorkerMessage.DESCRIPTOR.oneofs_by_name["body"].fields
    if f.message_type is not None
}
_Call = grpc.aio.StreamStreamCall[pb.WorkerMessage, pb.AgentMessage]
_log = logging.getLogger(__name__)
# A dropped stream shows up as any of these; all mean "reconnect".
_STREAM_ERRORS = (
    grpc.aio.AioRpcError,
    grpc.aio.UsageError,
    asyncio.InvalidStateError,  # write on an RPC that already finished
    ConnectionError,
    OSError,
)


async def _read(call: _Call) -> pb.AgentMessage | None:
    msg = await call.read()
    return msg if isinstance(msg, pb.AgentMessage) else None  # anything else is grpc.aio.EOF


def grpc_target(socket: str) -> str:
    """A filesystem path becomes a unix: target; anything with a scheme or host:port is kept."""
    if socket.startswith(("unix:", "unix-abstract:", "dns:", "ipv4:", "ipv6:")):
        return socket
    host, _, port = socket.rpartition(":")
    if host and port.isdigit() and "/" not in socket and "\\" not in socket:
        return socket
    return f"unix:{socket}"


def wrap(body: Message, seq: int = 0) -> pb.WorkerMessage:
    """Put a body message (pb.Progress, pb.ExitReport, ...) into a WorkerMessage."""
    msg = pb.WorkerMessage(seq=seq)
    msg.sent_at.GetCurrentTime()
    getattr(msg, _FIELD_BY_TYPE[body.DESCRIPTOR.full_name]).CopyFrom(body)
    return msg


def negotiate(offered: Sequence[str], chosen: str) -> str:
    if chosen not in offered:
        raise MrlError(
            PROTOCOL_MISMATCH,
            "The training worker and the app use different protocol versions.",
            detail=f"worker offers {list(offered)}, agent chose {chosen!r}",
        )
    return chosen


class WorkerSession:
    def __init__(
        self,
        target: str,
        hello: pb.Hello,
        spool: Spool,
        *,
        versions: Sequence[str] = PROTOCOL_VERSIONS,
        reconnect_max_s: float = 2.0,
    ) -> None:
        hello.protocol_versions[:] = list(versions)
        self._target = grpc_target(target)
        self._hello = hello
        self._spool = spool
        self._versions = tuple(versions)
        self._reconnect_max_s = reconnect_max_s
        self._wake = asyncio.Event()
        self._write_lock = asyncio.Lock()  # gRPC allows one write at a time per stream
        self._acked = asyncio.Event()
        self._call: _Call | None = None
        self._task: asyncio.Task[None] | None = None
        self.inbox: asyncio.Queue[pb.AgentMessage] = asyncio.Queue()
        self.connected = asyncio.Event()
        self.hello_ack: pb.HelloAck | None = None
        self.fatal: MrlError | None = None
        self.failed = asyncio.Event()  # set with `fatal`: the session gave up for good
        self.connections = 0

    @property
    def max_message_bytes(self) -> int:
        cap = self.hello_ack.max_message_bytes if self.hello_ack else 0
        return cap or DEFAULT_MAX_MESSAGE_BYTES

    @property
    def heartbeat_interval_s(self) -> float:
        return float(self.hello_ack.heartbeat_interval_s if self.hello_ack else 0) or 10.0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="worker-session")

    def send(self, body: Message) -> None:
        """Spool a message; it is sent (and replayed) until the agent acks it. Never blocks."""
        self._spool.push(wrap(body))
        self._acked.clear()
        self._wake.set()

    async def send_live(self, body: Message) -> bool:
        """Send an unsequenced message now if connected (Heartbeat). Not spooled."""
        call = self._call
        if call is None or not self.connected.is_set():
            return False
        with contextlib.suppress(*_STREAM_ERRORS):
            async with self._write_lock:
                await call.write(wrap(body))
            return True
        return False

    async def wait_acked(self, timeout: float) -> bool:
        """Wait until the agent has acked everything spooled so far."""
        try:
            async with asyncio.timeout(timeout):
                while len(self._spool) and self.fatal is None:
                    self._acked.clear()
                    await self._acked.wait()
        except TimeoutError:
            return False
        return not len(self._spool)

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        delay = 0.05
        while True:
            try:
                await self._connect_once()
                delay = 0.05
            except MrlError as exc:
                self.fatal = exc
                self.failed.set()
                self._acked.set()
                return
            except _STREAM_ERRORS as exc:
                _log.debug("agent connection lost: %s", exc)
            finally:
                self.connected.clear()
                self._call = None
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._reconnect_max_s)

    async def _connect_once(self) -> None:
        cap = 64 * 1024 * 1024
        options = [("grpc.max_send_message_length", cap), ("grpc.max_receive_message_length", cap)]
        async with grpc.aio.insecure_channel(self._target, options=options) as channel:
            call = channel.stream_stream(
                METHOD,
                request_serializer=pb.WorkerMessage.SerializeToString,
                response_deserializer=pb.AgentMessage.FromString,
            )()
            await call.write(wrap(self._hello))
            first = await _read(call)
            if first is None:
                return
            if not first.HasField("hello_ack"):
                raise MrlError(
                    PROTOCOL_VIOLATION,
                    "The app and its training worker disagreed about the next step.",
                    detail=f"expected HelloAck, got {first.WhichOneof('body')}",
                )
            negotiate(self._versions, first.hello_ack.protocol_version)
            self.hello_ack = first.hello_ack
            self._spool.rewind(max(first.hello_ack.resume_from_seq, first.ack_seq + 1))
            self._call = call
            self.connections += 1
            self.connected.set()
            self._wake.set()
            writer = asyncio.create_task(self._write_loop(call))
            reader = asyncio.create_task(self._read_loop(call))
            try:
                # Either side ending means the stream is gone: reconnect and replay.
                done, _ = await asyncio.wait({reader, writer}, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()  # re-raise a stream error or a protocol violation
            finally:
                for task in (reader, writer):
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError, *_STREAM_ERRORS):
                    await asyncio.gather(reader, writer)

    async def _write_loop(self, call: _Call) -> None:
        while True:
            msg = self._spool.next_to_send()
            if msg is None:
                self._wake.clear()
                await self._wake.wait()
                continue
            async with self._write_lock:
                await call.write(msg)

    async def _read_loop(self, call: _Call) -> None:
        while True:
            msg = await _read(call)
            if msg is None:
                return
            self._spool.ack(msg.ack_seq)
            if not len(self._spool):
                self._acked.set()
            if msg.HasField("hello_ack"):
                raise MrlError(
                    PROTOCOL_VIOLATION,
                    "The app and its training worker disagreed about the next step.",
                    detail="HelloAck sent twice on one stream",
                )
            if msg.WhichOneof("body") is not None:
                await self.inbox.put(msg)
