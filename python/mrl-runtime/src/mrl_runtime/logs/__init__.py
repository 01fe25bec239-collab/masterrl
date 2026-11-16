"""Log capture: line-buffered, redacted LogChunks with per-stream byte offsets.

LogStream turns writes into LogChunks. FdCapture tees a file descriptor (1 or 2) into a
LogStream, so output from the engine and its child processes is captured too.

A chunk never ends inside a secret, or inside one that may still be arriving: text is held
back from the start of any secret that crosses the cut, touches the end of what has arrived,
or is a private-key block whose END line has not come yet. Bytes are decoded incrementally,
so a multi-byte character split across writes is matched whole. If a key block has to be
flushed before its END line (HOLD_CAP, or a final flush), the stream stays "inside the key"
and drops what follows until the END line.
"""

import codecs
import os
import re
import threading
from collections import deque
from collections.abc import Callable

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.redact import Redactor

MAX_LINE = 64 * 1024  # flush a line this long even without a newline
HOLD_CAP = 1024 * 1024  # never hold more than this: flush it all, masked
TAIL_BYTES = 16 * 1024
_MIN_KEEP = 256  # longer than any pattern's minimum match, label included
_KEY_BEGIN = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")
_KEY_END = re.compile(r"-----END [A-Z ]*PRIVATE KEY-----")
_KEY_END_MAX = 64  # longer than any END line, so one split across writes is still found


def _open_key(text: str) -> bool:
    """Does `text` end inside a private-key block?"""
    begins = list(_KEY_BEGIN.finditer(text))
    return bool(begins) and _KEY_END.search(text, begins[-1].end()) is None


class LogStream:
    """Thread-safe."""

    def __init__(
        self,
        stream: pb.LogStream,
        redactor: Redactor,
        emit: Callable[[pb.LogChunk], None],
        *,
        rank: int = 0,
    ) -> None:
        self._stream = stream
        self._redactor = redactor
        self._emit = emit
        self._rank = rank
        self._buf = ""  # undecodable bytes kept as surrogates
        self._decoder = codecs.getincrementaldecoder("utf-8")("surrogateescape")
        self._in_key = False  # the start of a private key was already flushed (masked)
        self._key_held = False  # the buffer holds an unfinished key: wait for END or the cap
        self._offset = 0
        self._tail: deque[bytes] = deque()
        self._tail_bytes = 0
        self._lock = threading.Lock()

    def write(self, data: bytes) -> None:
        with self._lock:
            added = self._decoder.decode(data)
            self._buf += added
            if self._key_held and len(self._buf) < HOLD_CAP:
                # A held key cannot flush until its END line arrives: skip the full rescan.
                recent = self._buf[-(len(added) + _KEY_END_MAX) :]
                if _KEY_END.search(recent) is None:
                    return
            self._drain(final=False)

    def line(self, text: str) -> None:
        self.write(text.rstrip("\n").encode() + b"\n")

    def flush(self) -> None:
        with self._lock:
            self._buf += self._decoder.decode(b"", final=True)
            self._drain(final=True)

    def _drain(self, *, final: bool) -> None:
        if self._in_key and not self._skip_key(final=final):
            return
        text = self._buf
        spans = self._redactor.spans(text)
        cut = len(text) if final else self._cut(text, spans)
        self._key_held = not final and _open_key(text[max(cut, 0) :])
        if cut <= 0:
            return
        ready = self._redactor.mask(text[:cut], [sp for sp in spans if sp[2] <= cut])
        self._buf = text[cut:]
        self._in_key = _open_key(text[:cut])  # only a forced flush can end inside a key
        self._send(ready.encode("utf-8", "surrogateescape"))

    def _skip_key(self, *, final: bool) -> bool:
        """Drop the rest of a key whose start was flushed masked. True once past its END."""
        end = _KEY_END.search(self._buf)
        if end is None:
            self._buf = "" if final else self._buf[-_KEY_END_MAX:]
            return False
        self._buf = self._buf[end.end() :]
        self._in_key = False
        return True

    def _cut(self, text: str, spans: list[tuple[int, int, int]]) -> int:
        cut = text.rfind("\n") + 1
        if not cut and len(text) >= MAX_LINE:
            cut = len(text) - max(self._redactor.longest, _MIN_KEEP)
        for start, _, end in reversed(spans):
            unfinished = end >= len(text) or (
                "-----BEGIN" in text[start:end] and "-----END" not in text[start:end]
            )
            if start < cut and (end > cut or unfinished):
                cut = start
        if cut <= 0 and len(text) >= HOLD_CAP:
            return len(text)
        return cut

    def tail(self) -> str:
        with self._lock:
            return b"".join(self._tail).decode("utf-8", "replace")

    def _send(self, data: bytes) -> None:
        chunk = pb.LogChunk(stream=self._stream, data=data, rank=self._rank, offset=self._offset)
        self._offset += len(data)
        self._tail.append(data)
        self._tail_bytes += len(data)
        while self._tail_bytes > TAIL_BYTES and len(self._tail) > 1:
            self._tail_bytes -= len(self._tail.popleft())
        self._emit(chunk)


class FdCapture:
    """Redirect `fd` into a pipe; a thread copies it to the original fd and to `stream`."""

    def __init__(self, fd: int, stream: LogStream) -> None:
        self._fd = fd
        self._stream = stream
        self._saved = -1
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        read_end, write_end = os.pipe()
        self._saved = os.dup(self._fd)
        os.dup2(write_end, self._fd)
        os.close(write_end)
        self._thread = threading.Thread(target=self._pump, args=(read_end,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        os.dup2(self._saved, self._fd)  # closes our pipe's last write end: the pump sees EOF
        self._thread.join(timeout=5)
        os.close(self._saved)
        self._thread = None
        self._stream.flush()

    def _pump(self, read_end: int) -> None:
        try:
            while data := os.read(read_end, 65536):
                os.write(self._saved, data)
                self._stream.write(data)
        finally:
            os.close(read_end)
