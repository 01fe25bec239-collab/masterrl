"""The harness: drives one TrainingEngine through Worker Protocol v1 (Doc 05 §11.2).

connect -> Hello -> Prepare (validate, prepare, estimate) -> Prepared -> Start -> run loop ->
cleanup -> ExitReport. The engine's run() iterates in its own thread; each event is handled on
the event loop before the engine continues, so checkpoint staging back-pressures the engine
only when max_pending_checkpoints commits are still waiting for an ack.

Job spec keys the harness reads (Prepare.job_spec_json): run_id, job_kind, algorithm, common,
engine, switch, base_model_sha256, export {format}, harness {stall_timeout_s,
max_pending_checkpoints, nan_patience, commit_timeout_s, cancel_grace_s}.
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import math
import platform
import re
import shutil
import signal
import sys
import tempfile
import threading
import time
import traceback
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from google.protobuf.message import Message

import mrl_runtime
from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter.engine import API_VERSION, TrainingEngine
from mrl_runtime.adapter.events import (
    CheckpointReady,
    EngineEvent,
    EngineWarning,
    Log,
    Metrics,
    Progress,
    Samples,
)
from mrl_runtime.adapter.manifest import EngineManifest
from mrl_runtime.adapter.types import (
    Allocation,
    EnvInfo,
    ExportContext,
    HardwareProfile,
    Issue,
    JobContext,
    ProbeReport,
    ResolvedConfig,
    ResourceEstimate,
    ValidationRequest,
)
from mrl_runtime.checkpoint.files import write_artifact_manifest
from mrl_runtime.checkpoint.provider import CheckpointProvider, CheckpointSettings
from mrl_runtime.errors import FIELD_INVALID, FIELD_MISSING, METHOD_UNSUPPORTED
from mrl_runtime.harness.classify import ExitState, Phase, classify, decide_exit, failure
from mrl_runtime.logs import FdCapture, LogStream
from mrl_runtime.metrics import MetricsMapper, SampleEmitter
from mrl_runtime.protocol.session import WorkerSession
from mrl_runtime.protocol.spool import Spool
from mrl_runtime.redact import Redactor

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SEVERITY = {
    "info": common.SEVERITY_INFO,
    "warning": common.SEVERITY_WARNING,
    "error": common.SEVERITY_ERROR,
}
EXIT_OK, EXIT_FAILED, EXIT_NO_SESSION, EXIT_REPORT_UNACKED = 0, 1, 2, 3
_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class HarnessOptions:
    attempt_id: str
    nonce: str
    role_id: str = "trainer"
    primary: bool = True
    spool_dir: Path | None = None
    spool_max_bytes: int = 64 * 1024 * 1024
    capture_fds: bool = False
    handle_signals: bool = False
    exit_ack_timeout_s: float = 30.0
    environment_digest: str = ""


def _positive_number(value: object, *, whole: bool) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    if not math.isfinite(value) or value <= 0 or (whole and value != int(value)):
        return None
    return float(value)


@dataclass(frozen=True)
class Settings:
    stall_timeout_s: float = 1800.0
    max_pending_checkpoints: int = 2
    nan_patience: int = 3
    commit_timeout_s: float = 600.0
    cancel_grace_s: float = 30.0

    @classmethod
    def from_spec(cls, raw: object) -> tuple["Settings", list[Issue]]:
        """Settings from the job spec's `harness` block, plus an issue per unusable value."""
        if raw is None:
            return cls(), []
        if not isinstance(raw, dict):
            return cls(), [
                Issue(FIELD_INVALID, "The harness settings must be an object.", path="/harness")
            ]
        block = cast(dict[str, Any], raw)
        defaults = asdict(cls())
        issues = [
            Issue(FIELD_INVALID, f"There is no harness setting {key!r}.", path=f"/harness/{key}")
            for key in block
            if key not in defaults
        ]
        values: dict[str, Any] = {}
        for key, default in defaults.items():
            whole = isinstance(default, int)
            number = _positive_number(block.get(key, default), whole=whole)
            if number is None:
                kind = "a whole number above 0" if whole else "a number of seconds above 0"
                issues.append(
                    Issue(FIELD_INVALID, f"{key} must be {kind}.", path=f"/harness/{key}")
                )
                number = default
            values[key] = int(number) if whole else float(number)
        return cls(**values), issues


class _EndAttemptError(Exception):
    """Ends the lifecycle early with a ready ExitReport (validation failed, dry run done)."""

    def __init__(self, kind: pb.ExitKind, fail: common.Failure | None = None) -> None:
        super().__init__(pb.ExitKind.Name(kind))
        self.kind = kind
        self.failure = fail


def _issue_pb(issue: Issue) -> common.Issue:
    return common.Issue(
        code=issue.code,
        severity=_SEVERITY[issue.severity],
        path=issue.path,
        message=issue.message,
        hint=issue.hint,
    )


def _mapping(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


class Harness:
    def __init__(
        self, engine: TrainingEngine, manifest: EngineManifest, target: str, options: HarnessOptions
    ) -> None:
        self.engine = engine
        self.manifest = manifest
        self.options = options
        self._target = target
        self.redactor = Redactor([options.nonce])
        self._own_spool = options.spool_dir is None
        self._spool_dir = options.spool_dir or Path(tempfile.mkdtemp(prefix="mrl-spool-"))
        self.session: WorkerSession | None = None
        self.provider: CheckpointProvider | None = None
        self.exit_report: pb.ExitReport | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._prepare: asyncio.Future[pb.Prepare] | None = None
        self._start: asyncio.Future[pb.Start] | None = None
        self._ctx: JobContext | None = None
        self._settings = Settings()
        self._logs: dict[int, LogStream] = {}
        self._metrics: dict[str, float] = {}
        self._nan_streak = 0
        self._last_step = 0
        self._start_step: int | None = None
        self._started_at = 0.0
        self._last_progress = 0.0
        self._final_staged = False
        self._cancel_request = ""
        self._pause_request = ""
        self._smoke_stop = False
        self._checkpoint_requests: list[str] = []
        self._seen_checkpoint_requests: set[str] = set()
        self._acknowledged: list[str] = []
        # Checkpoint id -> the requests it answers; acknowledged only once it committed.
        self._requests_by_checkpoint: dict[str, list[str]] = {}
        self._hard_deadline: float | None = None
        self._lease_deadline: float | None = None
        self._harness_failure: common.Failure | None = None
        self._engine_failure: common.Failure | None = None

    # ---- public entry ----

    async def run(self) -> int:
        """Run the whole attempt. Returns the process exit code."""
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._prepare = loop.create_future()
        self._start = loop.create_future()
        probe, probe_failure = self._probe()
        session = WorkerSession(
            self._target, self._hello(probe), Spool(self._spool_dir, self.options.spool_max_bytes)
        )
        self.session = session
        for stream in (pb.LOG_STREAM_HARNESS, pb.LOG_STREAM_STDOUT, pb.LOG_STREAM_STDERR):
            self._logs[stream] = LogStream(stream, self.redactor, self._emit_log)
        captures = [
            FdCapture(1, self._logs[pb.LOG_STREAM_STDOUT]),
            FdCapture(2, self._logs[pb.LOG_STREAM_STDERR]),
        ]
        if self.options.capture_fds:
            sys.stdout.flush()
            sys.stderr.flush()
            for c in captures:
                c.start()
        if self.options.handle_signals:
            self._install_signals(loop)
        session.start()
        background = [
            asyncio.create_task(self._dispatch()),
            asyncio.create_task(self._heartbeats()),
        ]
        try:
            kind, fail = await self._lifecycle(probe, probe_failure)
            return await self._exit(kind, fail)
        except _NoSessionError:
            return EXIT_NO_SESSION
        finally:
            if self.options.capture_fds:
                sys.stdout.flush()
                sys.stderr.flush()
                for c in captures:
                    c.stop()
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await session.close()
            if self._own_spool:
                shutil.rmtree(self._spool_dir, ignore_errors=True)

    def signal_cancel(self, signum: int) -> None:
        """SIGTERM/SIGINT: a cooperative cancel with the default grace."""
        name = signal.Signals(signum).name
        self._on_cancel(
            pb.CancelRequest(
                request_id=f"signal:{name}", grace_s=int(self._settings.cancel_grace_s)
            )
        )

    # ---- lifecycle ----

    async def _lifecycle(
        self, probe: ProbeReport, probe_failure: common.Failure | None
    ) -> tuple[pb.ExitKind, common.Failure | None]:
        assert self._prepare is not None
        assert self._start is not None
        try:
            prepare = await self._await(self._prepare)
            ctx, spec = self._context(prepare)
            self._ctx = ctx
            await self._validate_and_prepare(ctx, spec, probe, probe_failure)
            if ctx.dry_run:
                await self._cleanup(ctx)
                return pb.EXIT_KIND_DRY_RUN_OK, None
            await self._await(self._start)
            if str(spec.get("job_kind", "")).upper() in ("EXPORT", "CONVERT"):
                await self._export(ctx, spec)
            else:
                await self._run_engine(ctx, spec)
        except _EndAttemptError as stop:
            if self._ctx is not None:
                await self._cleanup(self._ctx)
            return stop.kind, stop.failure
        await self._cleanup(ctx)
        provider = self.provider
        kind, fail = decide_exit(
            ExitState(
                harness_failure=self._harness_failure,
                engine_failure=self._engine_failure,
                final_staged=self._final_staged and ctx.smoke_steps == 0,
                final_commit=provider.final_result if provider else None,
                cancelled=bool(self._cancel_request),
                paused=bool(self._pause_request),
                pause_saved=not self.manifest.pause
                or (provider is not None and provider.last_committed_step == self._last_step),
            )
        )
        if kind == pb.EXIT_KIND_CANCELLED:
            self._acknowledged.append(self._cancel_request)
        if kind == pb.EXIT_KIND_PAUSED:
            self._acknowledged.append(self._pause_request)
        return kind, fail

    def _context(self, prepare: pb.Prepare) -> tuple[JobContext, dict[str, Any]]:
        try:
            spec = _mapping(json.loads(prepare.job_spec_json or "{}"))
        except json.JSONDecodeError as exc:
            raise self._reject(
                [
                    Issue(
                        FIELD_INVALID,
                        f"The job spec is not valid JSON: {exc.msg}.",
                        path="/job_spec_json",
                    )
                ]
            ) from exc
        if not prepare.staging_dir:
            raise self._reject(
                [Issue(FIELD_MISSING, "The agent sent no staging directory.", path="/staging_dir")]
            )
        self._settings, problems = Settings.from_spec(spec.get("harness"))
        if problems:
            raise self._reject(problems)
        staging = Path(prepare.staging_dir)
        for sub in ("work", "scratch"):
            (staging / sub).mkdir(parents=True, exist_ok=True)
        staged = {k: Path(v) for k, v in prepare.staged_paths.items()}
        a = prepare.allocation
        config = ResolvedConfig(
            algorithm=str(spec.get("algorithm", "")),
            common=_mapping(spec.get("common")),
            engine=_mapping(spec.get("engine")),
            switch=_mapping(spec.get("switch")),
            raw=spec,
        )
        ctx = JobContext(
            attempt_id=self.options.attempt_id,
            run_id=str(spec.get("run_id", "")),
            role_id=self.options.role_id,
            primary=self.options.primary,
            config=config,
            staged_paths=staged,
            resume_from=staged.get("resume_from"),
            allocation=Allocation(
                tuple(a.gpu_indices), a.world_size or 1, a.node_rank, dict(a.role_endpoints)
            ),
            staging_dir=staging,
            work_dir=staging / "work",
            scratch_dir=staging / "scratch",
            dry_run=prepare.dry_run,
            smoke_steps=prepare.smoke_steps,
            reward_socket=prepare.reward_socket,
            logger=logging.getLogger(f"mrl.engine.{self.manifest.name}"),
        )
        return ctx, spec

    def _harness_issues(self, ctx: JobContext, spec: Mapping[str, Any]) -> list[Issue]:
        issues: list[Issue] = []
        algorithms = self.manifest.algorithms
        if algorithms and ctx.config.algorithm not in algorithms:
            issues.append(
                Issue(
                    METHOD_UNSUPPORTED,
                    f"The engine {self.manifest.name} does not offer {ctx.config.algorithm!r}.",
                    path="/algorithm",
                    hint=f"Pick one of: {', '.join(sorted(algorithms))}.",
                )
            )
        trains = algorithms and str(spec.get("job_kind", "TRAIN")).upper() == "TRAIN"
        if trains and ctx.primary and not ctx.dry_run:
            if not ctx.run_id.startswith("run_"):
                issues.append(Issue(FIELD_MISSING, "The job spec has no run id.", path="/run_id"))
            if not _SHA256.match(str(spec.get("base_model_sha256", ""))):
                issues.append(
                    Issue(
                        FIELD_MISSING,
                        "The job spec has no base model hash.",
                        path="/base_model_sha256",
                    )
                )
        return issues

    async def _validate_and_prepare(
        self,
        ctx: JobContext,
        spec: dict[str, Any],
        probe: ProbeReport,
        probe_failure: common.Failure | None,
    ) -> None:
        if probe_failure is not None:
            self._send_prepared(pb.Prepared(ok=False))
            raise _EndAttemptError(pb.EXIT_KIND_FAILED, probe_failure)
        if not probe.ok:
            self._send_prepared(pb.Prepared(ok=False, issues=[_issue_pb(i) for i in probe.issues]))
            first = (
                probe.issues[0].message
                if probe.issues
                else "The engine's environment check failed."
            )
            raise _EndAttemptError(
                pb.EXIT_KIND_FAILED,
                failure(common.FAILURE_CLASS_DEPENDENCY_ENV, "PROBE_FAILED", first, retryable=True),
            )
        hw = HardwareProfile(
            ctx.allocation.gpu_indices, ctx.allocation.world_size, ctx.allocation.node_rank
        )
        report = await self._call(
            "validate", self.engine.validate, ValidationRequest(ctx.config, ctx.staged_paths, hw)
        )
        issues = [*self._harness_issues(ctx, spec), *report.issues]
        if any(i.severity == "error" for i in issues):
            raise self._reject(issues)
        prepared = await self._call("prepare", self.engine.prepare, ctx)
        try:
            estimate = self.engine.estimate(ctx.config, hw)
        except Exception as exc:  # an estimate is advisory: never fail the run on it
            self._harness_line(f"[warning] the engine's estimate failed: {type(exc).__name__}")
            estimate = ResourceEstimate(assumptions=("estimate unavailable",))
        effective = {
            "algorithm": ctx.config.algorithm,
            "common": ctx.config.common,
            "engine": ctx.config.engine,
            "switch": ctx.config.switch,
            "effective": prepared.effective_config,
        }
        canonical = json.dumps(effective, sort_keys=True, separators=(",", ":"), default=str)
        (ctx.staging_dir / "effective-config.json").write_text(canonical, encoding="utf-8")
        self._send_prepared(
            pb.Prepared(
                ok=True,
                issues=[_issue_pb(i) for i in issues],
                effective_config_sha256=hashlib.sha256(canonical.encode()).hexdigest(),
                estimate=pb.ResourceEstimate(
                    gpu_mem_gib_lo=estimate.gpu_mem_gib[0],
                    gpu_mem_gib_hi=estimate.gpu_mem_gib[1],
                    total_steps=prepared.total_steps or estimate.total_steps,
                    wall_seconds_lo=estimate.wall_seconds[0],
                    wall_seconds_hi=estimate.wall_seconds[1],
                    assumptions=list(estimate.assumptions),
                ),
            )
        )

    def _reject(self, issues: list[Issue]) -> _EndAttemptError:
        self._send_prepared(pb.Prepared(ok=False, issues=[_issue_pb(i) for i in issues]))
        errors = [i for i in issues if i.severity == "error"]
        first = errors[0] if errors else Issue(FIELD_INVALID, "The job settings need a change.")
        cls = (
            common.FAILURE_CLASS_DATA
            if first.code.startswith("MRL-DATA-")
            else common.FAILURE_CLASS_USER_CONFIG
        )
        return _EndAttemptError(pb.EXIT_KIND_FAILED, failure(cls, first.code, first.message))

    def _send_prepared(self, msg: pb.Prepared) -> None:
        """Issue text comes from the adapter: redact it before it leaves the worker."""
        assert self.session is not None
        for issue in msg.issues:
            issue.message = self.redactor.text(issue.message)
            issue.hint = self.redactor.text(issue.hint)
            issue.path = self.redactor.text(issue.path)
        self.session.send(msg)

    # ---- run loop ----

    async def _run_engine(self, ctx: JobContext, spec: Mapping[str, Any]) -> None:
        assert self.session is not None
        assert self._loop is not None
        self.provider = CheckpointProvider(
            self.session.send,
            CheckpointSettings(
                staging_dir=ctx.staging_dir,
                attempt_id=ctx.attempt_id,
                run_id=ctx.run_id,
                engine_name=self.manifest.name,
                adapter_version=self.manifest.version,
                fmt=self.manifest.checkpoint_format,
                base_model_sha256=str(spec.get("base_model_sha256", "")),
                resume_from=ctx.resume_from,
                max_pending=self._settings.max_pending_checkpoints,
                commit=ctx.smoke_steps == 0,
            ),
        )
        mapper = MetricsMapper(
            self.manifest.algorithms.get(ctx.config.algorithm, ()), self.manifest.namespaces
        )
        samples = SampleEmitter(ctx.staging_dir, self.redactor, self.session.max_message_bytes)
        loop = self._loop
        done: asyncio.Future[BaseException | None] = loop.create_future()

        def finish(result: BaseException | None) -> None:
            if not done.done():
                done.set_result(result)

        def pump() -> None:
            outcome: BaseException | None = None
            try:
                for event in self.engine.run(ctx):
                    if loop.is_closed():  # the harness already exited without us
                        return
                    asyncio.run_coroutine_threadsafe(
                        self._handle(event, mapper, samples), loop
                    ).result()
            except BaseException as exc:  # reported, never swallowed: it becomes the ExitReport
                outcome = exc
            with contextlib.suppress(RuntimeError):  # the loop is gone: the harness already exited
                loop.call_soon_threadsafe(finish, outcome)

        self._started_at = self._last_progress = time.monotonic()
        threading.Thread(target=pump, name="mrl-engine-run", daemon=True).start()
        while not done.done():
            await asyncio.wait({done}, timeout=0.05)
            self._supervise(ctx)
            if self._hard_deadline is not None and time.monotonic() >= self._hard_deadline:
                self._harness_line(
                    "[warning] the engine did not stop in time; leaving it to the agent"
                )
                break
        if done.done() and (exc := done.result()) is not None:
            self._engine_failure = self._failure_for(exc, "run")
        remaining = self._settings.commit_timeout_s
        if self._hard_deadline is not None:
            remaining = max(0.0, self._hard_deadline - time.monotonic())
        await self.provider.wait_all(remaining)

    def _supervise(self, ctx: JobContext) -> None:
        now = time.monotonic()
        if (
            self._lease_deadline is not None
            and time.time() >= self._lease_deadline
            and self._harness_failure is None
        ):
            self._harness_failure = failure(
                common.FAILURE_CLASS_NODE_LOST,
                "LEASE_EXPIRED",
                "The attempt's lease ran out.",
                retryable=True,
            )
            self.engine.request_cancel()
            self._hard_deadline = now
        if (
            now - self._last_progress > self._settings.stall_timeout_s
            and self._harness_failure is None
        ):
            self._harness_failure = failure(
                common.FAILURE_CLASS_STALLED,
                "NO_PROGRESS",
                "The run stopped making progress.",
                retryable=True,
            )
            self.engine.request_cancel()
            self._limit_deadline(self._settings.cancel_grace_s)

    async def _handle(
        self, event: EngineEvent, mapper: MetricsMapper, samples: SampleEmitter
    ) -> None:
        assert self.session is not None
        assert self.provider is not None
        primary = self.options.primary
        match event:
            case Progress():
                self._step(event.step)
                if primary:
                    self.session.send(self._progress(event))
            case Metrics():
                self._step(event.step)
                batch = mapper.batch(event)
                for point in batch.points:
                    self._metrics[point.name] = point.value
                self._watch_divergence(batch)
                if primary:
                    self.session.send(batch)
            case Samples():
                if primary:
                    self.session.send(samples.batch(event))
            case CheckpointReady():
                if not primary:
                    self._harness_line("[warning] a non-primary role yielded a checkpoint; ignored")
                    return
                self._step(event.step)
                requests, self._checkpoint_requests = self._checkpoint_requests, []
                staged = await self.provider.stage(event, {**self._metrics, **event.metrics})
                self.provider.submit(staged)
                self._final_staged = self._final_staged or event.is_final
                self._requests_by_checkpoint[staged.checkpoint_id] = requests
            case Log():
                self._harness_line(f"[{event.level}] {event.message}")
            case EngineWarning():
                self._harness_line(f"[warning] {event.message}")

    def _step(self, step: int) -> None:
        if self._start_step is None:
            self._start_step = step - 1
        if step > self._last_step:
            self._last_step = step
            self._last_progress = time.monotonic()
        smoke = self._ctx.smoke_steps if self._ctx else 0
        if smoke and step >= smoke and not self._smoke_stop:  # L3 smoke: stop after N steps
            self._smoke_stop = True
            self.engine.request_cancel()

    def _progress(self, event: Progress) -> pb.Progress:
        msg = pb.Progress(
            step=event.step,
            total_steps=event.total_steps or 0,
            phase=event.phase,
            epoch=event.epoch or 0.0,
            tokens_seen=event.tokens_seen or 0,
        )
        done = event.step - (self._start_step or 0)
        if event.total_steps and done > 0:
            rate = (time.monotonic() - self._started_at) / done
            msg.eta_seconds = max(0.0, rate * (event.total_steps - event.step))
        return msg

    def _watch_divergence(self, batch: pb.MetricsBatch) -> None:
        losses = [p.value for p in batch.points if p.name.rsplit("/", 1)[-1].endswith("loss")]
        if not losses:
            return
        self._nan_streak = self._nan_streak + 1 if any(not math.isfinite(v) for v in losses) else 0
        if self._nan_streak >= self._settings.nan_patience and self._harness_failure is None:
            self._harness_failure = failure(
                common.FAILURE_CLASS_NUMERIC_DIVERGENCE,
                "NAN_LOSS",
                "Training numbers went out of range.",
            )
            self.engine.request_cancel()
            self._limit_deadline(self._settings.cancel_grace_s)

    async def _export(self, ctx: JobContext, spec: Mapping[str, Any]) -> None:
        fmt = str(_mapping(spec.get("export")).get("format", ""))
        checkpoint = ctx.staged_paths.get("checkpoint")
        if fmt not in self.manifest.exports or checkpoint is None:
            raise self._reject(
                [
                    Issue(
                        FIELD_INVALID,
                        f"This engine cannot export to {fmt!r}, or no checkpoint was staged.",
                        path="/export/format",
                        hint=f"Pick one of: {', '.join(self.manifest.exports) or 'none'}.",
                    )
                ]
            )
        out = ctx.staging_dir / "outputs"
        out.mkdir(parents=True, exist_ok=True)
        export_ctx = ExportContext(checkpoint, fmt, out, ctx.config, ctx.logger)
        result = await self._call("export", self.engine.export, export_ctx)
        write_artifact_manifest(
            out,
            [(path, role, "") for path, role in result.files],
            kind="export",
            source={"run_id": ctx.run_id, "attempt_id": ctx.attempt_id, "format": result.format},
        )

    async def _cleanup(self, ctx: JobContext) -> None:
        try:
            await asyncio.wait_for(asyncio.to_thread(self.engine.cleanup, ctx), timeout=30)
        except Exception as exc:  # cleanup is best effort; the agent tears down the sandbox anyway
            self._harness_line(f"[warning] engine cleanup failed: {type(exc).__name__}: {exc}")

    async def _call[T, A](self, phase: Phase, fn: Callable[[A], T], arg: A) -> T:
        try:
            return await asyncio.to_thread(fn, arg)
        except Exception as exc:
            raise _EndAttemptError(pb.EXIT_KIND_FAILED, self._failure_for(exc, phase)) from exc

    def _failure_for(self, exc: BaseException, phase: Phase) -> common.Failure:
        tail = "".join(s.tail() for s in self._logs.values())
        found = classify(exc, phase=phase, hook=self.engine.classify_failure, log_tail=tail)
        if self._ctx is not None:
            path = self._ctx.staging_dir / "failure" / "traceback.txt"
            path.parent.mkdir(parents=True, exist_ok=True)
            text = "".join(traceback.format_exception(exc)) + "\n--- log tail ---\n" + tail
            path.write_text(self.redactor.text(text), encoding="utf-8")
            found.detail_ref = path.relative_to(self._ctx.staging_dir).as_posix()
        return found

    # ---- control from the agent ----

    async def _dispatch(self) -> None:
        assert self.session is not None
        assert self._prepare is not None
        assert self._start is not None
        while True:
            msg = await self.session.inbox.get()
            kind = msg.WhichOneof("body")
            if kind == "prepare" and not self._prepare.done():
                self._prepare.set_result(msg.prepare)
            elif kind == "start" and not self._start.done():
                self._start.set_result(msg.start)
            elif kind == "upload_grant" and self.provider:
                self.provider.on_grant(msg.upload_grant)
            elif kind == "checkpoint_commit_ack" and self.provider:
                self.provider.on_ack(msg.checkpoint_commit_ack)
            elif kind == "pause":
                self._on_pause(msg.pause)
            elif kind == "cancel":
                self._on_cancel(msg.cancel)
            elif kind == "checkpoint_request":
                self._on_checkpoint_request(msg.checkpoint_request)
            elif kind == "lease_deadline":
                deadline = msg.lease_deadline.deadline
                self._lease_deadline = deadline.ToNanoseconds() / 1e9 if deadline.seconds else None

    def _committed(self) -> set[str]:
        provider = self.provider
        return {r.checkpoint_id for r in provider.results if r.committed} if provider else set()

    def _saved_requests(self) -> list[str]:
        """Checkpoint requests whose checkpoint committed (a rejected or unconfirmed one is not
        acknowledged, so the agent can ask again)."""
        committed = self._committed()
        return [
            r
            for cid, reqs in self._requests_by_checkpoint.items()
            if cid in committed
            for r in reqs
        ]

    def _on_pause(self, req: pb.PauseRequest) -> None:
        if self._pause_request:
            return
        self._pause_request = req.request_id or "pause"
        if self.manifest.pause:
            self.engine.request_pause()
        else:  # no safe pause point: stop now; resume uses the last committed checkpoint
            self.engine.request_cancel()
        if req.deadline_s:
            self._limit_deadline(req.deadline_s)

    def _on_cancel(self, req: pb.CancelRequest) -> None:
        if not self._cancel_request:
            self._cancel_request = req.request_id or "cancel"
        self.engine.request_cancel()
        self._limit_deadline(req.grace_s or self._settings.cancel_grace_s)
        for waiting in (self._prepare, self._start):  # cancelled before the run began
            if waiting is not None and not waiting.done():
                waiting.set_exception(_EndAttemptError(pb.EXIT_KIND_CANCELLED))
                self._acknowledged.append(self._cancel_request)
                return

    def _on_checkpoint_request(self, req: pb.CheckpointRequest) -> None:
        if not (self.manifest.on_demand_checkpoint and self.options.primary and self._ctx):
            self._harness_line(
                "[warning] this engine cannot save a checkpoint on demand; request ignored"
            )
            return
        if req.request_id in self._seen_checkpoint_requests:
            return
        self._seen_checkpoint_requests.add(req.request_id)
        self._checkpoint_requests.append(req.request_id)
        self.engine.checkpoint(self._ctx)

    def _limit_deadline(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        self._hard_deadline = (
            deadline if self._hard_deadline is None else min(self._hard_deadline, deadline)
        )

    # ---- plumbing ----

    async def _await[T](self, fut: asyncio.Future[T]) -> T:
        assert self.session is not None
        failed = asyncio.ensure_future(self.session.failed.wait())
        try:
            await asyncio.wait({fut, failed}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            failed.cancel()
        if not fut.done():
            fatal = self.session.fatal
            print(f"mrl-worker: {fatal}", file=sys.stderr)
            raise _NoSessionError
        return fut.result()

    async def _exit(self, kind: pb.ExitKind, fail: common.Failure | None) -> int:
        assert self.session is not None
        report = pb.ExitReport(
            kind=kind,
            final_step=self._last_step,
            last_committed_checkpoint_id=self.provider.last_committed_id if self.provider else "",
            final_metrics={k: v for k, v in self._metrics.items() if math.isfinite(v)},
            acknowledged_requests=[r for r in self._acknowledged + self._saved_requests() if r],
        )
        if fail is not None:
            report.failure.CopyFrom(fail)
            # Adapter hooks and probe issues may echo exception text: redact what leaves.
            report.failure.message = self.redactor.text(fail.message)
            report.failure.sub_code = self.redactor.text(fail.sub_code)
        self.exit_report = report
        for stream in self._logs.values():
            stream.flush()
        await asyncio.sleep(0)  # let thread-scheduled log chunks reach the spool first
        self.session.send(report)
        acked = await self.session.wait_acked(self.options.exit_ack_timeout_s)
        if not acked:
            return EXIT_REPORT_UNACKED
        return EXIT_FAILED if kind == pb.EXIT_KIND_FAILED else EXIT_OK

    async def _heartbeats(self) -> None:
        assert self.session is not None
        while True:
            await self.session.connected.wait()
            beat = pb.Heartbeat(last_step=self._last_step)
            beat.time.GetCurrentTime()
            await self.session.send_live(beat)
            await asyncio.sleep(self.session.heartbeat_interval_s)

    def _probe(self) -> tuple[ProbeReport, common.Failure | None]:
        env = EnvInfo(
            platform.platform(),
            platform.python_version(),
            mrl_runtime.__version__,
            self.options.environment_digest,
        )
        try:
            return self.engine.probe(env), None
        except Exception as exc:
            return ProbeReport(ok=False), classify(
                exc, phase="probe", hook=self.engine.classify_failure
            )

    def _hello(self, probe: ProbeReport) -> pb.Hello:
        algorithms = cast(
            list[dict[str, Any]], self.manifest.raw.get("spec", {}).get("algorithms", [])
        )
        return pb.Hello(
            attempt_id=self.options.attempt_id,
            nonce=self.options.nonce,
            role_id=self.options.role_id,
            primary=self.options.primary,
            mrl_runtime_version=mrl_runtime.__version__,
            adapter_name=self.manifest.name,
            adapter_version=self.manifest.version,
            adapter_api=API_VERSION,
            upstream_versions=dict(probe.upstream_versions),
            environment_digest=self.options.environment_digest,
            capabilities=pb.Capabilities(
                pause=self.manifest.pause,
                on_demand_checkpoint=self.manifest.on_demand_checkpoint,
                samples=any(a.get("samples", {}).get("emitted", False) for a in algorithms),
                algorithms=list(self.manifest.algorithms),
            ),
        )

    def _harness_line(self, text: str) -> None:
        stream = self._logs.get(pb.LOG_STREAM_HARNESS)
        if stream is not None:
            stream.line(text)

    def _emit_log(self, chunk: pb.LogChunk) -> None:
        """Called from any thread (fd capture pumps, the engine thread)."""
        loop, session = self._loop, self.session
        if loop is None or session is None:
            return
        with contextlib.suppress(RuntimeError):  # loop closed during shutdown
            loop.call_soon_threadsafe(_send, session, chunk)

    def _install_signals(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            if sys.platform == "win32":  # Windows event loops have no add_signal_handler
                signal.signal(
                    sig,
                    lambda signum, _frame: loop.call_soon_threadsafe(self.signal_cancel, signum),
                )
            else:
                loop.add_signal_handler(sig, self.signal_cancel, sig)


class _NoSessionError(Exception):
    """The agent refused the session for good (no common protocol version, bad handshake)."""


def _send(session: WorkerSession, body: Message) -> None:
    session.send(body)
