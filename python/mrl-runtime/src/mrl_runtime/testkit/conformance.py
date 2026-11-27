"""Adapter conformance suite T-COMMON TC1-TC7 (Doc 05 §3), runnable against any adapter.

TC1 manifest lint + schema round-trip     TC2 L0-L2 dry run rejects a seeded bad config
TC3 short CPU run moves the objective      TC4 checkpoint -> kill -> resume without a gap
TC5 cooperative cancel within grace, no leftover processes
TC6 emitted metrics are declared; every required one appears
TC7 acceptance run on the reference hardware (CPU engines run it here; GPU engines need --gpu)
"""

import asyncio
import json
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter.engine import API_VERSION, TrainingEngine
from mrl_runtime.adapter.manifest import (
    EngineManifest,
    from_document,
    load_engine,
    version_in_range,
)
from mrl_runtime.protocol.schemas import schema_errors
from mrl_runtime.protocol.session import PROTOCOL_VERSIONS
from mrl_runtime.testkit.agent import AgentScript, FakeAgent
from mrl_runtime.testkit.runner import (
    AttemptResult,
    job_spec,
    leftover_processes,
    run_in_process,
    run_subprocess,
)

Status = Literal["pass", "fail", "skip"]
CANCEL_GRACE_S = 5


@dataclass(frozen=True)
class ConformanceProfile:
    """What the suite needs to drive an adapter on CPU.

    engine_ref: name in the mrl_runtime.engines entry-point group (or a manifest path), used
    for real subprocess runs. good / slow / bad: `engine` blocks for a short run, a slow run
    (kill and cancel cases) and a run L1 validation must reject with bad_code.
    objective: (canonical metric, "down" | "up") for TC3.
    """

    manifest: EngineManifest
    engine_ref: str
    algorithm: str
    good: Mapping[str, Any]
    slow: Mapping[str, Any]
    bad: Mapping[str, Any]
    bad_code: str
    objective: tuple[str, Literal["down", "up"]]
    common: Mapping[str, Any] = field(default_factory=lambda: {"seed": 7})


@dataclass(frozen=True)
class CaseResult:
    case: str
    status: Status
    detail: str


class _SkipCaseError(Exception):
    pass


class CaseFailedError(Exception):
    pass


def _require(ok: object, problem: str) -> None:
    """Fail the case with a plain message (not `assert`: python -O would drop it)."""
    if not ok:
        raise CaseFailedError(problem)


def _exit_of(run: AttemptResult, kind: pb.ExitKind) -> pb.ExitReport:
    report = run.exit_report
    _require(
        report is not None and report.kind == kind,
        f"expected {pb.ExitKind.Name(kind)}, got {_kind(report)}",
    )
    assert report is not None  # narrowed by _require
    return report


class Suite:
    def __init__(self, profile: ConformanceProfile, workdir: Path, *, gpu: bool = False) -> None:
        self.p = profile
        self.workdir = workdir
        self.gpu = gpu
        self._good: AttemptResult | None = None
        self._runs = 0

    def _engine(self) -> TrainingEngine:
        return load_engine(self.p.manifest)

    def _script(self, engine: Mapping[str, Any], **kw: Any) -> AgentScript:
        self._runs += 1
        staging = self.workdir / f"attempt-{self._runs}"
        staging.mkdir(parents=True)
        spec = job_spec(self.p.algorithm, engine, common=self.p.common)
        return AgentScript(job_spec=spec, staging_dir=staging, **kw)

    async def good_run(self) -> AttemptResult:
        if self._good is None:
            self._good = await run_in_process(
                self._engine(), self.p.manifest, self._script(self.p.good)
            )
        return self._good

    async def tc1(self) -> str:
        m = self.p.manifest
        raw = dict(m.raw)
        problems = schema_errors(raw, "engine-manifest/v1")
        _require(not problems, f"manifest does not match engine-manifest/v1: {problems[:3]}")
        again = from_document(json.loads(json.dumps(raw)))
        _require(dict(again.raw) == raw, "manifest changed in a JSON round trip")
        _require(
            version_in_range(API_VERSION, m.adapter_api),
            f"adapterApi {m.adapter_api} excludes {API_VERSION}",
        )
        speaks = any(version_in_range(v, m.worker_protocol) for v in PROTOCOL_VERSIONS)
        _require(speaks, f"workerProtocol {m.worker_protocol} excludes {list(PROTOCOL_VERSIONS)}")
        _require(self.p.algorithm in m.algorithms, f"{self.p.algorithm} is not declared")
        self._engine()  # load_engine checks the entrypoint implements TrainingEngine
        return f"{len(m.algorithms)} algorithm(s); entrypoint {m.entrypoint} loads"

    async def tc2(self) -> str:
        engine, manifest = self._engine(), self.p.manifest
        bad = await run_in_process(engine, manifest, self._script(self.p.bad, dry_run=True))
        prepared = bad.prepared
        _require(prepared is not None and not prepared.ok, "the bad config passed validation")
        codes = [i.code for i in prepared.issues] if prepared else []
        _require(self.p.bad_code in codes, f"expected {self.p.bad_code}, got {codes}")
        report = _exit_of(bad, pb.EXIT_KIND_FAILED)
        cls = getattr(report.failure, "class")
        allowed = (common.FAILURE_CLASS_USER_CONFIG, common.FAILURE_CLASS_DATA)
        _require(cls in allowed, f"bad config failed as {common.FailureClass.Name(cls)}")
        good = await run_in_process(
            self._engine(), manifest, self._script(self.p.good, dry_run=True)
        )
        issues = [i.message for i in good.prepared.issues] if good.prepared else ["no Prepared"]
        _require(good.prepared is not None and good.prepared.ok, f"good config failed: {issues}")
        _exit_of(good, pb.EXIT_KIND_DRY_RUN_OK)
        return f"rejected with {self.p.bad_code}; good config dry-runs OK"

    async def tc3(self) -> str:
        run = await self.good_run()
        report = _exit_of(run, pb.EXIT_KIND_COMPLETED)
        metric, direction = self.p.objective
        series = run.series(metric)
        _require(len(series) >= 2, f"{metric} was emitted {len(series)} time(s)")
        first, last = series[0][1], series[-1][1]
        moved = last < first if direction == "down" else last > first
        _require(moved, f"{metric} went {first:.4f} -> {last:.4f}, expected {direction}")
        _require(report.last_committed_checkpoint_id, "no checkpoint was committed")
        return f"{metric} {first:.4f} -> {last:.4f} over {len(series)} steps"

    async def tc4(self) -> str:
        metric = self.p.objective[0]

        async def kill_after_commit(agent: FakeAgent, proc: Any) -> None:
            await agent.wait_for(lambda m: m.HasField("checkpoint_commit"), timeout=60)
            while not any(s == pb.COMMIT_STATE_COMMITTED for s, _, _ in agent.commits.values()):
                await asyncio.sleep(0.01)
            proc.kill()

        script = self._script(self.p.slow)
        first = await run_subprocess(self.p.engine_ref, script, control=kill_after_commit)
        _require(first.exit_report is None, "the worker finished before it could be killed")
        _require(first.committed(), "no checkpoint committed before the kill")
        cid, step, where = max(first.committed(), key=lambda c: c[1])
        total = int(first.prepared.estimate.total_steps) if first.prepared else 0
        resume = self._script(self.p.slow, staged_paths={"resume_from": str(where)})
        second = await run_in_process(self._engine(), self.p.manifest, resume)
        _exit_of(second, pb.EXIT_KIND_COMPLETED)
        resumed = [s for s, _ in second.series(metric)]
        _require(resumed[:1] == [step + 1], f"resume from step {step} started at {resumed[:1]}")
        merged = sorted({s for s, _ in first.series(metric) if s <= step} | set(resumed))
        expected = list(range(merged[0], (total or merged[-1]) + 1))
        gaps = sorted(set(expected) - set(merged))[:5]
        _require(merged == expected, f"step series has gaps at {gaps}")
        return f"killed after commit {cid} at step {step}; resumed at {step + 1} with no gap"

    async def tc5(self) -> str:
        elapsed = {"s": 0.0}

        async def cancel(agent: FakeAgent, proc: Any) -> None:
            await agent.wait_for(lambda m: m.HasField("progress") and m.progress.step >= 2, 60)
            t0 = time.monotonic()
            req = pb.CancelRequest(request_id="tc5", grace_s=CANCEL_GRACE_S)
            await agent.send(pb.AgentMessage(cancel=req))
            await proc.wait()
            elapsed["s"] = time.monotonic() - t0

        run = await run_subprocess(self.p.engine_ref, self._script(self.p.slow), control=cancel)
        report = _exit_of(run, pb.EXIT_KIND_CANCELLED)
        _require("tc5" in report.acknowledged_requests, "cancel request id not acknowledged")
        took = elapsed["s"]
        _require(took <= CANCEL_GRACE_S, f"took {took:.1f}s, grace is {CANCEL_GRACE_S}s")
        _require(not leftover_processes(run.pid), "processes were left running after exit")
        note = "" if sys.platform != "win32" else " (leftover check needs POSIX; skipped here)"
        return f"cancelled in {took:.2f}s of {CANCEL_GRACE_S}s{note}"

    async def tc6(self) -> str:
        run = await self.good_run()
        metrics = self.p.manifest.algorithms[self.p.algorithm]
        declared = {m.name for m in metrics}
        # `resource/…` is the harness's own memory sampling, never an engine metric.
        emitted = [p for p in run.points() if not p.name.startswith("resource/")]
        undeclared = sorted({p.name for p in emitted if p.undeclared or p.name not in declared})
        _require(not undeclared, f"undeclared metrics emitted: {undeclared}")
        required = {m.name for m in metrics if m.required}
        missing = sorted(required - {p.name for p in emitted})
        _require(not missing, f"required metrics never emitted: {missing}")
        return f"{len(declared)} declared, {len(required)} required, all present"

    async def tc7(self) -> str:
        hardware = self.p.manifest.accelerators
        if "cpu" not in hardware and not self.gpu:
            need = ", ".join(hardware) or "GPU"
            raise _SkipCaseError(
                f"needs {need} hardware: rerun with --gpu on the reference profile"
            )
        run = await run_subprocess(self.p.engine_ref, self._script(self.p.good))
        _require(run.exit_code == 0, f"worker exit code {run.exit_code}")
        _exit_of(run, pb.EXIT_KIND_COMPLETED)
        final_ids = {
            m.checkpoint_staged.checkpoint_id
            for m in run.agent.of("checkpoint_staged")
            if m.checkpoint_staged.is_final
        }
        _require(any(c[0] in final_ids for c in run.committed()), "final checkpoint not committed")
        _require(not run.agent.protocol_errors, f"protocol errors: {run.agent.protocol_errors}")
        return f"completed as a real mrl-worker process on {', '.join(hardware)}"


CASES: tuple[str, ...] = ("TC1", "TC2", "TC3", "TC4", "TC5", "TC6", "TC7")


def _kind(report: pb.ExitReport | None) -> str:
    if report is None:
        return "missing"
    text = pb.ExitKind.Name(report.kind)
    if report.HasField("failure"):
        cls = common.FailureClass.Name(getattr(report.failure, "class"))
        text += f" {cls}/{report.failure.sub_code}"
    return text


async def run_suite(
    profile: ConformanceProfile, *, gpu: bool = False, only: tuple[str, ...] = CASES
) -> list[CaseResult]:
    results: list[CaseResult] = []
    with tempfile.TemporaryDirectory(prefix="mrl-conformance-") as tmp:
        suite = Suite(profile, Path(tmp), gpu=gpu)
        cases: dict[str, Callable[[], Awaitable[str]]] = {
            "TC1": suite.tc1,
            "TC2": suite.tc2,
            "TC3": suite.tc3,
            "TC4": suite.tc4,
            "TC5": suite.tc5,
            "TC6": suite.tc6,
            "TC7": suite.tc7,
        }
        for name in only:
            try:
                results.append(CaseResult(name, "pass", await cases[name]()))
            except _SkipCaseError as skip:
                results.append(CaseResult(name, "skip", str(skip)))
            except CaseFailedError as exc:
                results.append(CaseResult(name, "fail", str(exc)))
            except Exception as exc:  # any crash in a case is that case's failure, reported plainly
                results.append(CaseResult(name, "fail", f"{type(exc).__name__}: {exc}"))
    return results
