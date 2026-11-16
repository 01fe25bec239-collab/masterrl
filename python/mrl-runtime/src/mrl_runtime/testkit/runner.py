"""Run one attempt against a FakeAgent: in this process, or as a real mrl-worker subprocess."""

import asyncio
import contextlib
import os
import secrets
import signal
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter.engine import TrainingEngine
from mrl_runtime.adapter.manifest import EngineManifest
from mrl_runtime.harness.main import NONCE_ENV
from mrl_runtime.harness.worker import Harness, HarnessOptions
from mrl_runtime.testkit.agent import AgentScript, FakeAgent

IN_PROCESS_TIMEOUT_S = 60.0
BASE_MODEL_SHA256 = "0" * 64  # the fake runs have no real base model
Control = Callable[[FakeAgent, "asyncio.subprocess.Process | None"], Awaitable[None]]


def job_spec(
    algorithm: str,
    engine: Mapping[str, Any],
    *,
    common: Mapping[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "run_id": "run_testkit",
        "job_kind": "TRAIN",
        "algorithm": algorithm,
        "common": dict(common or {"seed": 7}),
        "engine": dict(engine),
        "switch": {},
        "base_model_sha256": BASE_MODEL_SHA256,
        **extra,
    }


@dataclass
class AttemptResult:
    agent: FakeAgent
    exit_code: int | None
    pid: int = 0
    harness: Harness | None = None
    notes: list[str] = field(default_factory=list[str])

    @property
    def exit_report(self) -> pb.ExitReport | None:
        return self.agent.exit_report

    @property
    def prepared(self) -> pb.Prepared | None:
        return self.agent.prepared

    def series(self, metric: str) -> list[tuple[int, float]]:
        return [
            (p.step, p.value)
            for m in self.agent.of("metrics")
            for p in m.metrics.points
            if p.name == metric
        ]

    def points(self) -> list[pb.MetricPoint]:
        return [p for m in self.agent.of("metrics") for p in m.metrics.points]

    def committed(self) -> list[tuple[str, int, Path]]:
        return [
            (cid, step, where)
            for cid, (state, step, where) in self.agent.commits.items()
            if state == pb.COMMIT_STATE_COMMITTED
        ]


async def run_in_process(
    engine: TrainingEngine,
    manifest: EngineManifest,
    script: AgentScript,
    *,
    control: Control | None = None,
    options: HarnessOptions | None = None,
) -> AttemptResult:
    agent = FakeAgent(script)
    await agent.start()
    opts = options or HarnessOptions(
        attempt_id="att_testkit", nonce=secrets.token_hex(16), exit_ack_timeout_s=5
    )
    harness = Harness(engine, manifest, agent.target, opts)
    try:
        async with asyncio.timeout(IN_PROCESS_TIMEOUT_S):
            work = asyncio.create_task(harness.run())
            if control is not None:
                await control(agent, None)
            code = await work
        return AttemptResult(agent, code, harness=harness)
    finally:
        await agent.close()


async def run_subprocess(
    engine_ref: str,
    script: AgentScript,
    *,
    control: Control | None = None,
    timeout: float = 120.0,
    attempt_id: str = "att_testkit",
) -> AttemptResult:
    """A real mrl-worker process (new process group on POSIX, so leftovers can be found)."""
    agent = FakeAgent(script)
    await agent.start()
    env = {**os.environ, NONCE_ENV: secrets.token_hex(16)}
    posix = sys.platform != "win32"
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "mrl_runtime.harness",
        "--engine",
        engine_ref,
        "--socket",
        agent.target,
        "--attempt-id",
        attempt_id,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=posix,
    )
    result = AttemptResult(agent, None, pid=proc.pid)
    try:
        async with asyncio.timeout(timeout):
            if control is not None:
                await control(agent, proc)
            result.exit_code = await proc.wait()
        return result
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        await agent.close()


def leftover_processes(pid: int) -> bool:
    """POSIX: is anything still alive in the worker's process group? Kills what it finds."""
    if sys.platform == "win32":
        return False
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGKILL)
    return True
