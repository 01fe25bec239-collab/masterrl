"""Bounded on-disk spool of WorkerMessages the agent has not acknowledged yet (Doc 05 §13.1).

Sequence numbers are assigned when a message is first sent, so dropping unsent messages never
leaves a gap in what the agent sees. Sent messages are kept until acked and replayed after an
agent restart. When the spool is over its byte budget, unsent messages are dropped in this
order: LogChunk, SampleBatch, then every other MetricsBatch. CheckpointStaged,
CheckpointCommitRequest and ExitReport (and every other kind) are never dropped: the spool goes
over budget instead, because training never blocks on telemetry.
"""

import shutil
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb

# Drop tiers, first dropped first. "metrics" is thinned (every other batch), not emptied at once.
DROP_ORDER = ("log", "samples", "metrics")
# Superseded by the newest one: an unsent older Progress is replaced, not kept.
COALESCED = ("progress",)


@dataclass
class _Entry:
    id: int
    kind: str
    size: int
    seq: int = 0  # 0 until first sent


class Spool:
    def __init__(self, directory: Path, max_bytes: int = 64 * 1024 * 1024) -> None:
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        self._dir = directory
        self._max = max_bytes
        self._entries: list[_Entry] = []
        self._cursor = 0  # index of the next entry to send
        self._next_id = 1
        self._next_seq = 1
        self._bytes = 0
        self.dropped: Counter[str] = Counter()

    @property
    def bytes(self) -> int:
        return self._bytes

    def __len__(self) -> int:
        return len(self._entries)

    def kinds(self) -> list[str]:
        return [e.kind for e in self._entries]

    def last_assigned_seq(self) -> int:
        return self._next_seq - 1

    def push(self, msg: pb.WorkerMessage) -> None:
        """Queue a message (its seq is ignored and assigned at send time)."""
        kind = msg.WhichOneof("body")
        if kind is None:
            raise ValueError("a spooled WorkerMessage needs a body")
        if kind in COALESCED:
            for old in self._unsent():
                if old.kind == kind:
                    self._remove(old, dropped=False)
        msg.seq = 0
        data = msg.SerializeToString()
        entry = _Entry(self._next_id, kind, len(data))
        self._next_id += 1
        if not self._make_room(entry):
            self.dropped[kind] += 1
            return
        self._path(entry).write_bytes(data)
        self._entries.append(entry)
        self._bytes += entry.size

    def next_to_send(self) -> pb.WorkerMessage | None:
        """The next message to write to the stream, with its seq set; None when caught up."""
        if self._cursor >= len(self._entries):
            return None
        entry = self._entries[self._cursor]
        self._cursor += 1
        if entry.seq == 0:
            entry.seq = self._next_seq
            self._next_seq += 1
        msg = pb.WorkerMessage.FromString(self._path(entry).read_bytes())
        msg.seq = entry.seq
        return msg

    def ack(self, ack_seq: int) -> None:
        """Forget every sent message with seq <= ack_seq."""
        while self._entries and 0 < self._entries[0].seq <= ack_seq:
            self._remove(self._entries[0], dropped=False)

    def rewind(self, resume_from_seq: int) -> None:
        """After a reconnect: everything before resume_from_seq is accepted; resend the rest."""
        self.ack(resume_from_seq - 1)
        self._cursor = 0

    def _unsent(self) -> list[_Entry]:
        return [e for e in self._entries[self._cursor :] if e.seq == 0]

    def _make_room(self, new: _Entry) -> bool:
        """Drop unsent telemetry until `new` fits. False if `new` itself is the one to drop."""
        for tier in DROP_ORDER:
            while self._bytes + new.size > self._max:
                victims = [e for e in self._unsent() if e.kind == tier]
                if tier == "metrics":
                    victims = victims[:-1][::2]  # thin out, always keep the newest batch
                if not victims:
                    break
                for victim in victims:
                    self._remove(victim, dropped=True)
                    if self._bytes + new.size <= self._max:
                        break
            if self._bytes + new.size <= self._max:
                return True
            if new.kind == tier:
                return False
        return True  # only never-drop kinds are left: go over budget rather than lose them

    def _remove(self, entry: _Entry, *, dropped: bool) -> None:
        index = self._entries.index(entry)
        del self._entries[index]
        if index < self._cursor:
            self._cursor -= 1
        self._bytes -= entry.size
        self._path(entry).unlink()
        if dropped:
            self.dropped[entry.kind] += 1

    def _path(self, entry: _Entry) -> Path:
        return self._dir / f"{entry.id:012d}.{entry.kind}.pb"
