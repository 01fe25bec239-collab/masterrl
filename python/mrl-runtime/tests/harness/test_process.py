import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.harness.main import main
from mrl_runtime.logs import FdCapture, LogStream
from mrl_runtime.redact import MASK, Redactor
from mrl_runtime.testkit.agent import AgentScript, FakeAgent
from mrl_runtime.testkit.runner import job_spec, leftover_processes, run_subprocess


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_sigterm_is_a_cooperative_cancel(tmp_path: Path) -> None:
    async def term(agent: FakeAgent, proc: Any) -> None:
        await agent.wait_for(lambda m: m.HasField("progress") and m.progress.step >= 2)
        proc.send_signal(signal.SIGTERM)

    spec = job_spec("x-fake.steps", {"steps": 400, "step_seconds": 0.02, "children": 1})
    r = asyncio.run(
        run_subprocess("fake", AgentScript(job_spec=spec, staging_dir=tmp_path), control=term)
    )
    assert r.exit_code == 0
    report = r.exit_report
    assert report is not None
    assert report.kind == pb.EXIT_KIND_CANCELLED
    assert "signal:SIGTERM" in report.acknowledged_requests
    assert not leftover_processes(r.pid)


def test_fd_capture_tees_and_redacts(tmp_path: Path) -> None:
    chunks: list[pb.LogChunk] = []
    stream = LogStream(
        pb.LOG_STREAM_STDOUT, Redactor(["canary-abcdefgh"], from_env=False), chunks.append
    )
    capture = FdCapture(1, stream)
    sys.stdout.flush()
    capture.start()
    try:
        os.write(1, b"from python canary-abcdefgh\n")
        subprocess.run([sys.executable, "-c", "print('from a child canary-abcdefgh')"], check=True)
    finally:
        capture.stop()
    data = b"".join(c.data for c in chunks)
    assert b"canary" not in data
    assert data.count(MASK.encode()) == 2
    assert b"from a child" in data


def test_worker_cli_needs_a_nonce(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MRL_WORKER_NONCE", raising=False)
    assert main(["--engine", "fake", "--socket", "/nowhere", "--attempt-id", "a"]) == 2
