import asyncio
import secrets
from collections.abc import Iterator
from pathlib import Path

import pytest

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter import (
    EngineEvent,
    EnvInfo,
    FailureInfo,
    Issue,
    JobContext,
    Log,
    ProbeReport,
    RolloutSample,
    Samples,
    ValidationReport,
    ValidationRequest,
)
from mrl_runtime.harness import HarnessOptions
from mrl_runtime.logs import HOLD_CAP, MAX_LINE, LogStream
from mrl_runtime.redact import MASK, Redactor
from mrl_runtime.testkit.agent import AgentScript
from mrl_runtime.testkit.fake import FakeEngine, manifest
from mrl_runtime.testkit.runner import job_spec, run_in_process


# Synthetic tokens are assembled at runtime so secret scanners never see a literal.
def _tok(*parts: str) -> str:
    return "".join(parts)


AWS = _tok("AK", "IA", "ABCDEFGHIJKLMNOP")
GITHUB = _tok("gh", "p_", "a" * 36)
HF = _tok("h", "f_", "b" * 34)
OPENAI = _tok("s", "k-", "c" * 40)
JWT = ".".join(
    [_tok("ey", "JhbGciOiJIUzI1NiJ9"), _tok("ey", "JzdWIiOiIxMjM0NTY3ODkwIn0"), "c" * 24]
)
PEM_BODY = "Q" * 64
PEM = "\n".join(
    [
        _tok("-----BEGIN RSA ", "PRIVATE KEY-----"),
        PEM_BODY,
        PEM_BODY,
        _tok("-----END RSA ", "PRIVATE KEY-----"),
    ]
)


@pytest.mark.parametrize(
    ("line", "secret"),
    [
        (f"aws key {AWS} in use", AWS),
        (f"token {GITHUB}", GITHUB),
        (HF, HF),
        ("Authorization: Bearer " + "d" * 32, "d" * 32),
        (f"OPENAI_API_KEY={OPENAI}", OPENAI),
        ('password="' + "e" * 14 + '"', "e" * 14),
        (JWT, JWT),
        (PEM, PEM_BODY),
    ],
)
def test_patterns_mask_secrets(line: str, secret: str) -> None:
    out = Redactor(from_env=False).text(line)
    assert MASK in out
    assert secret not in out


def _stream(secrets_: list[str]) -> tuple[LogStream, list[pb.LogChunk]]:
    chunks: list[pb.LogChunk] = []
    return LogStream(
        pb.LOG_STREAM_STDOUT, Redactor(secrets_, from_env=False), chunks.append
    ), chunks


def _joined(chunks: list[pb.LogChunk]) -> bytes:
    offsets = [c.offset for c in chunks]
    assert offsets == sorted(offsets)
    return b"".join(c.data for c in chunks)


def test_secret_across_the_long_line_flush_is_masked() -> None:
    known = "known-" + "k" * 40
    stream, chunks = _stream([known])
    # No newline: the stream must flush at MAX_LINE, with the known secret right on that edge.
    stream.write(b"x" * (MAX_LINE - 10) + known[:20].encode())
    stream.write(known[20:].encode() + b" " + GITHUB[:10].encode())
    stream.write(GITHUB[10:].encode() + b" " + b"y" * MAX_LINE)
    stream.flush()
    data = _joined(chunks)
    assert len(chunks) >= 2  # it really did flush mid-line
    assert known.encode() not in data
    assert known[:20].encode() not in data
    assert GITHUB[:10].encode() not in data
    assert GITHUB[10:].encode() not in data
    assert data.count(MASK.encode()) == 2


def test_private_key_written_line_by_line_is_masked() -> None:
    stream, chunks = _stream([])
    for line in PEM.split("\n"):
        stream.write(line.encode() + b"\n")
    stream.write(b"after the key\n")
    stream.flush()
    data = _joined(chunks)
    assert PEM_BODY.encode() not in data
    assert b"after the key" in data


def test_unterminated_private_key_is_masked_at_flush() -> None:
    stream, chunks = _stream([])
    stream.write(PEM.split("\n")[0].encode() + b"\n" + PEM_BODY.encode() + b"\n")
    assert chunks == []  # held: the END line has not arrived
    stream.flush()
    assert PEM_BODY.encode() not in _joined(chunks)


def test_unicode_secret_split_inside_a_character_is_masked() -> None:
    known = "canary-café-0123456789"
    stream, chunks = _stream([known])
    raw = known.encode()
    split = raw.index("é".encode()) + 1  # between the two bytes of é
    stream.write(b"start " + raw[:split])
    stream.write(raw[split:] + b" end\n")
    stream.write(b"bad \xff byte\n")
    stream.flush()
    data = _joined(chunks)
    assert raw not in data
    assert raw[:split] not in data
    assert data == f"start {MASK} end\n".encode() + b"bad \xff byte\n"


def test_private_key_longer_than_the_hold_cap_stays_masked() -> None:
    stream, chunks = _stream([])
    begin, _, _, end = PEM.split("\n")
    early, late = "Q" * 63 + "\n", "Z" * 63 + "\n"
    stream.write(begin.encode() + b"\n")
    for _ in range(HOLD_CAP // len(early) + 2):  # past the cap: the stream must flush
        stream.write(early.encode())
    assert chunks  # it really was forced out
    for _ in range(50):
        stream.write(late.encode())
    stream.write(end[:10].encode())  # END line split across writes
    stream.write(end[10:].encode() + b"\nafter the key\n")
    stream.flush()
    data = _joined(chunks)
    assert b"QQQQ" not in data
    assert b"ZZZZ" not in data
    assert MASK.encode() in data
    assert data.endswith(b"after the key\n")


def test_known_values_and_env() -> None:
    r = Redactor(["s3cr3t-value-1"], from_env=False)
    assert r.text("x s3cr3t-value-1 y") == f"x {MASK} y"
    env = Redactor(from_env=False)
    env.add("short")  # too short to be treated as a secret
    assert env.text("short") == "short"


def test_secret_split_across_writes_is_still_masked() -> None:
    chunks: list[pb.LogChunk] = []
    stream = LogStream(
        pb.LOG_STREAM_STDOUT, Redactor(["canary-0123456789"], from_env=False), chunks.append
    )
    stream.write(b"before canary-01234")
    stream.write(b"56789 after\n")
    stream.flush()
    data = b"".join(c.data for c in chunks)
    assert b"canary" not in data
    assert data == f"before {MASK} after\n".encode()
    assert [c.offset for c in chunks] == [0]


class LeakyEngine(FakeEngine):
    """Prints, logs and samples a secret; nothing that leaves the worker may contain it."""

    canary = ""

    def run(self, ctx: JobContext) -> Iterator[EngineEvent]:
        yield Log(f"connecting with {self.canary}")
        yield Samples(1, (RolloutSample("s1", prompt=f"key {self.canary}", completion="ok"),))
        yield from super().run(ctx)


def test_redaction_canary_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    canary = "canary-" + secrets.token_hex(12)
    monkeypatch.setenv("MRL_TEST_API_TOKEN", canary)
    nonce = secrets.token_hex(16)
    engine = LeakyEngine()
    engine.canary = canary
    script = AgentScript(job_spec=job_spec("x-fake.steps", {"steps": 2}), staging_dir=tmp_path)
    opts = HarnessOptions(attempt_id="att_1", nonce=nonce, exit_ack_timeout_s=5)
    result = asyncio.run(run_in_process(engine, manifest(), script, options=opts))
    assert result.exit_report is not None
    assert result.exit_report.kind == pb.EXIT_KIND_COMPLETED
    wire = b"".join(m.SerializeToString() for m in result.agent.received if not m.HasField("hello"))
    assert canary.encode() not in wire
    assert nonce.encode() not in wire
    assert MASK.encode() in wire
    leaked = [p for p in tmp_path.rglob("*") if p.is_file() and canary.encode() in p.read_bytes()]
    assert leaked == []


class LoudFailureEngine(FakeEngine):
    """Puts the secret into validation issues and its failure messages."""

    canary = ""

    def validate(self, req: ValidationRequest) -> ValidationReport:
        report = super().validate(req)
        warning = Issue("MRL-VAL-003", f"using {self.canary}", severity="warning", hint=self.canary)
        return ValidationReport((*report.issues, warning))

    def classify_failure(
        self, exc: BaseException | None, exit_code: int | None, log_tail: str
    ) -> FailureInfo | None:
        return FailureInfo(
            common.FAILURE_CLASS_ENGINE_ERROR, "LOUD", f"failed: {exc} {self.canary}"
        )


def test_failure_messages_and_issues_are_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    canary = "canary-" + secrets.token_hex(12)
    monkeypatch.setenv("MRL_TEST_SECRET", canary)
    engine = LoudFailureEngine()
    engine.canary = canary
    spec = job_spec("x-fake.steps", {"steps": 3, "fail_at_step": 2})
    script = AgentScript(job_spec=spec, staging_dir=tmp_path)
    result = asyncio.run(run_in_process(engine, manifest(), script))
    report = result.exit_report
    assert report is not None
    assert report.failure.sub_code == "LOUD"
    assert MASK in report.failure.message
    prepared = result.prepared
    assert prepared is not None
    assert MASK in prepared.issues[0].message
    wire = b"".join(m.SerializeToString() for m in result.agent.received)
    assert canary.encode() not in wire


def test_probe_failure_message_is_redacted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    canary = "canary-" + secrets.token_hex(12)
    monkeypatch.setenv("MRL_TEST_PASSWORD", canary)

    class BadProbe(FakeEngine):
        def probe(self, env: EnvInfo) -> ProbeReport:
            return ProbeReport(
                ok=False, issues=(Issue("MRL-ENV-011", f"cannot log in with {canary}"),)
            )

    script = AgentScript(job_spec=job_spec("x-fake.steps", {}), staging_dir=tmp_path)
    result = asyncio.run(run_in_process(BadProbe(), manifest(), script))
    report = result.exit_report
    assert report is not None
    assert getattr(report.failure, "class") == common.FAILURE_CLASS_DEPENDENCY_ENV
    assert canary not in report.failure.message
    assert MASK in report.failure.message
