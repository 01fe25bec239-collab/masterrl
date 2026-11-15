"""mrl-reward-exec: the RewardService (Describe / Score / Health) over a UDS or loopback port.

Failure semantics follow Doc 06 §1.8.4: per-sample timeouts and the request deadline become
TIMEOUT samples resolved by the policy; too many failures -> FAILED_PRECONDITION with the
batch stats in trailing metadata (`mrl-batch-stats-bin`, a serialized BatchStats).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from concurrent import futures
from dataclasses import dataclass, field
from typing import IO, cast

import grpc

from mrl_rewards._generated.mrl.reward.v1 import reward_service_pb2 as pb
from mrl_rewards._generated.mrl.reward.v1 import reward_service_pb2_grpc as pb_grpc
from mrl_rewards.exec.bundle import LoadedBundle, canonical_json
from mrl_rewards.policy.runtime import (
    BatchFailedError,
    Job,
    NormalizerState,
    PolicyRuntime,
    SampleScore,
    batch_stats,
    run_job,
    sample_view,
)
from mrl_rewards.types import (
    MAX_DIAGNOSTICS_BYTES,
    MAX_FLAGS,
    REQUEST_MISMATCH,
    REQUEST_TOO_LARGE,
    JSONValue,
    RewardBatch,
    RewardError,
    RewardResult,
    RewardSample,
    Status,
)

STARTUP_FAILED = "MRL-RWD-149"
IDEM_CONFLICT = "MRL-IDEM-002"
STATS_METADATA_KEY = "mrl-batch-stats-bin"
_CACHEABLE = frozenset({Status.OK, Status.NOT_APPLICABLE, Status.INVALID_INPUT})


@dataclass(frozen=True)
class ExecLimits:
    max_batch: int = 256
    per_sample_timeout_ms: int = 5000  # default when a component sets no per_sample_timeout_ms
    hard_limit_s: float = 60.0  # a component stuck this long kills the process (launcher restarts)
    max_message_bytes: int = 32 << 20
    result_cache_entries: int = 65536  # 0 turns the deterministic-result cache off
    result_cache_bytes: int = 64 << 20
    response_cache_entries: int = 256  # replayed request_ids
    workers: int = 8


class Audit:
    """Structured audit lines (JSON, one per line) for the agent to collect."""

    def __init__(self, stream: IO[str] | None = None) -> None:
        self.stream = stream or sys.stdout
        self._lock = threading.Lock()

    def __call__(self, event: str, **fields: JSONValue) -> None:
        line = json.dumps({"event": f"reward_exec.{event}", "ts": time.time(), **fields})
        with self._lock:
            self.stream.write(line + "\n")
            self.stream.flush()


class ResultCache:
    """LRU of deterministic component results, bounded by entries and approximate bytes."""

    def __init__(self, entries: int, max_bytes: int) -> None:
        self.entries, self.max_bytes = entries, max_bytes
        self._data: OrderedDict[str, tuple[RewardResult, int]] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()

    @staticmethod
    def key(content_sha256: str, job: Job) -> str:
        view = sample_view(job.sample)
        for k in ("sample_id", "group_id"):
            view.pop(k)
        return hashlib.sha256(canonical_json([content_sha256, job.component_id, view])).hexdigest()

    def get(self, key: str) -> RewardResult | None:
        with self._lock:
            hit = self._data.get(key)
            if hit is None:
                return None
            self._data.move_to_end(key)
            return hit[0]

    def put(self, key: str, result: RewardResult) -> None:
        if self.entries <= 0 or result.status not in _CACHEABLE:
            return
        size = 128 + sum(len(k) + len(str(v)) for k, v in result.diagnostics.items())
        with self._lock:
            if key in self._data:
                return
            self._data[key] = (result, size)
            self._bytes += size
            while len(self._data) > self.entries or self._bytes > self.max_bytes:
                _, (_, old) = self._data.popitem(last=False)
                self._bytes -= old


@dataclass
class _Task:
    job: Job
    future: futures.Future[RewardResult] | None = None
    started: float | None = None
    key: str | None = None


@dataclass
class Scheduler:
    """Runs component jobs on a thread pool with per-sample timeouts and a request deadline.

    Python can't stop a busy thread, so a timed-out job keeps its thread until it returns; a
    watchdog ends the process if one runs past `hard_limit_s`.
    """

    limits: ExecLimits
    content_sha256: str
    audit: Audit
    on_stuck: Callable[[], None] = field(default=lambda: os._exit(70))
    cache: ResultCache = field(init=False)
    pool: futures.ThreadPoolExecutor = field(init=False)
    _running: dict[int, tuple[float, str]] = field(
        init=False, default_factory=dict[int, tuple[float, str]]
    )
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)
    _stop: threading.Event = field(init=False, default_factory=threading.Event)

    def __post_init__(self) -> None:
        self.cache = ResultCache(self.limits.result_cache_entries, self.limits.result_cache_bytes)
        self.pool = futures.ThreadPoolExecutor(self.limits.workers, thread_name_prefix="reward")
        threading.Thread(target=self._watchdog, name="reward-watchdog", daemon=True).start()

    def close(self) -> None:
        self._stop.set()
        self.pool.shutdown(wait=False, cancel_futures=True)

    def _watchdog(self) -> None:
        while not self._stop.wait(min(1.0, self.limits.hard_limit_s / 4)):
            now = time.monotonic()
            with self._lock:
                stuck = [
                    cid
                    for start, cid in self._running.values()
                    if now - start > self.limits.hard_limit_s
                ]
            if stuck:
                self.audit("stuck", component=stuck[0], hard_limit_s=self.limits.hard_limit_s)
                self.on_stuck()
                return

    def _call(self, task: _Task) -> RewardResult:
        task.started = time.monotonic()
        with self._lock:
            self._running[id(task)] = (task.started, task.job.component_id)
        try:
            return run_job(task.job)
        finally:
            with self._lock:
                self._running.pop(id(task), None)

    def run(self, jobs: Sequence[Job]) -> list[RewardResult]:
        return self.run_with_stats(jobs, None)[0]

    def run_with_stats(
        self, jobs: Sequence[Job], deadline: float | None
    ) -> tuple[list[RewardResult], int]:
        """Results in job order, and the number of cache hits."""
        results: list[RewardResult | None] = [None] * len(jobs)
        pending: dict[int, _Task] = {}
        hits = 0
        for i, job in enumerate(jobs):
            task = _Task(job)
            if job.deterministic and self.limits.result_cache_entries > 0:
                task.key = ResultCache.key(self.content_sha256, job)
                if (hit := self.cache.get(task.key)) is not None:
                    results[i] = hit
                    hits += 1
                    continue
            task.future = self.pool.submit(self._call, task)
            pending[i] = task
        while pending:
            futures.wait(
                [cast("futures.Future[RewardResult]", t.future) for t in pending.values()],
                timeout=0.005,
                return_when=futures.FIRST_COMPLETED,
            )
            now = time.monotonic()
            for i, task in list(pending.items()):
                fut = cast("futures.Future[RewardResult]", task.future)
                timeout_s = (task.job.timeout_ms or self.limits.per_sample_timeout_ms) / 1000
                if fut.done():
                    results[i] = result = fut.result()
                    if task.key is not None:
                        self.cache.put(task.key, result)
                elif task.started is not None and now - task.started > timeout_s:
                    results[i] = RewardResult.failed(
                        Status.TIMEOUT, f"took longer than {timeout_s:g} s", "sample_timeout"
                    )
                elif deadline is not None and now > deadline:
                    fut.cancel()
                    results[i] = RewardResult.failed(
                        Status.TIMEOUT, "the request deadline passed", "deadline"
                    )
                else:
                    continue
                del pending[i]
        return [cast("RewardResult", r) for r in results], hits


def _status(s: Status) -> str:
    return f"SCORE_STATUS_{s}"


def score_to_proto(s: SampleScore) -> pb.SampleScore:
    """One SampleScore on the wire; diagnostics capped at 4 KiB per sample, flags at 16."""
    msg = pb.SampleScore(
        sample_id=s.sample_id,
        status=_status(s.status),
        resolution=f"RESOLUTION_{s.resolution}",
        flags=list(s.flags[:MAX_FLAGS]),
    )
    if s.total is not None:
        msg.total = s.total
    budget = MAX_DIAGNOSTICS_BYTES
    for c in s.components:
        cm = msg.components.add(component_id=c.component_id, status=_status(c.status))
        if c.value is not None:
            cm.value = c.value
        flags = list(c.flags)
        for k, v in c.diagnostics.items():
            encoded = json.dumps(v, ensure_ascii=False)
            size = len(k.encode()) + len(encoded.encode())
            if size > budget:
                flags.append("diagnostics_truncated")
                break
            cm.diagnostics[k] = encoded
            budget -= size
        cm.flags.extend(flags[:MAX_FLAGS])
    return msg


def _decode(raw: str, name: str) -> tuple[JSONValue, str | None]:
    if not raw:
        return None, None
    try:
        return cast("JSONValue", json.loads(raw)), None
    except (ValueError, RecursionError):
        return None, f"{name} is not valid JSON"


class RewardExecService:
    """The RewardService implementation (registered with the generated gRPC glue)."""

    def __init__(  # noqa: PLR0913 (keyword-only settings)
        self,
        bundle: LoadedBundle,
        runtime: PolicyRuntime,
        *,
        nonce: str,
        reward_version_id: str,
        limits: ExecLimits | None = None,
        audit: Audit | None = None,
        on_stuck: Callable[[], None] | None = None,
    ) -> None:
        if not nonce:
            raise RewardError(STARTUP_FAILED, "The attempt nonce is empty.")
        self.bundle, self.runtime = bundle, runtime
        self.limits = limits or ExecLimits()
        self.audit = audit or Audit()
        self.version = reward_version_id
        self.state = NormalizerState()  # lives as long as this process (one attempt)
        self._nonce = nonce.encode()
        self._responses: OrderedDict[str, tuple[str, pb.ScoreResponse]] = OrderedDict()
        # ponytail: one lock serializes Score calls (jobs still run in parallel inside one);
        # per-request locking if harness chunk concurrency ever matters.
        self._lock = threading.Lock()
        kwargs = {"on_stuck": on_stuck} if on_stuck else {}
        self.scheduler = Scheduler(self.limits, bundle.content_sha256, self.audit, **kwargs)

    def close(self) -> None:
        self.scheduler.close()
        self.runtime.teardown()

    def _auth(self, nonce: str, context: grpc.ServicerContext) -> None:
        if not hmac.compare_digest(nonce.encode(), self._nonce):
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                f"{REQUEST_MISMATCH}: the attempt nonce does not match this reward service.",
            )

    def Describe(  # noqa: N802 (gRPC method name)
        self, request: pb.DescribeRequest, context: grpc.ServicerContext
    ) -> pb.DescribeResponse:
        self._auth(request.attempt_nonce, context)
        infos: list[pb.ComponentInfo] = []
        for c in self.runtime.policy.components:
            spec = self.runtime.providers[c.id].spec
            lo, hi = self.runtime.bounds[c.id]
            infos.append(
                pb.ComponentInfo(
                    component_id=c.id,
                    component_ref=c.use,
                    weight=c.weight,
                    value_min=lo,
                    value_max=hi,
                    tier=spec.tier,
                    deterministic=spec.deterministic,
                )
            )
        return pb.DescribeResponse(
            reward_version_id=self.version,
            content_sha256=self.bundle.content_sha256,
            signal=self.runtime.policy.signal,
            components=infos,
            max_batch=self.limits.max_batch,
            max_tier=max(i.tier for i in infos),
        )

    def Health(  # noqa: N802 (gRPC method name)
        self, request: pb.HealthRequest, context: grpc.ServicerContext
    ) -> pb.HealthResponse:
        self._auth(request.attempt_nonce, context)
        return pb.HealthResponse(serving=True, sandbox_restarts=0, detail="ready")

    def Score(  # noqa: N802 (gRPC method name)
        self, request: pb.ScoreRequest, context: grpc.ServicerContext
    ) -> pb.ScoreResponse:
        start = time.monotonic()
        self._auth(request.attempt_nonce, context)
        if request.reward_version_id != self.version:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"{REQUEST_MISMATCH}: the request is for reward version "
                f"{request.reward_version_id!r}, but this service runs {self.version!r}.",
            )
        if len(request.samples) > self.limits.max_batch:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"{REQUEST_TOO_LARGE}: {len(request.samples)} samples is more than the limit of "
                f"{self.limits.max_batch}; split the batch.",
            )
        fingerprint = hashlib.sha256(request.SerializeToString(deterministic=True)).hexdigest()
        with self._lock:
            if request.request_id and (cached := self._responses.get(request.request_id)):
                if cached[0] != fingerprint:
                    context.abort(
                        grpc.StatusCode.ALREADY_EXISTS,
                        f"{IDEM_CONFLICT}: request_id {request.request_id!r} was already used for "
                        "a different batch.",
                    )
                self._responses.move_to_end(request.request_id)
                return cached[1]
            response = self._score(request, start, context)
            if request.request_id:
                self._responses[request.request_id] = (fingerprint, response)
                while len(self._responses) > self.limits.response_cache_entries:
                    self._responses.popitem(last=False)
            return response

    def _score(
        self, request: pb.ScoreRequest, start: float, context: grpc.ServicerContext
    ) -> pb.ScoreResponse:
        samples: list[RewardSample] = []
        invalid: dict[int, str] = {}
        for i, s in enumerate(request.samples):
            reference, bad_ref = _decode(s.reference_json, "reference_json")
            episode, bad_ep = _decode(s.episode_json, "episode_json")
            if bad_ref or bad_ep:
                invalid[i] = cast("str", bad_ref or bad_ep)
            samples.append(
                RewardSample(
                    sample_id=s.sample_id,
                    group_id=s.group_id,
                    prompt=s.prompt,
                    completion=s.completion,
                    completion_token_count=s.completion_token_count,
                    finish_reason=s.finish_reason,
                    reference=reference,
                    episode=episode,
                    policy_version=s.policy_version,
                )
            )
        batch = RewardBatch(samples, request.trainer_step)
        deadline = start + request.deadline_ms / 1000 if request.deadline_ms else None
        remaining = cast("float | None", context.time_remaining())  # None: no gRPC deadline
        if remaining is not None:
            grpc_deadline = time.monotonic() + remaining - 0.05  # leave time to reply
            deadline = grpc_deadline if deadline is None else min(deadline, grpc_deadline)
        jobs, results = self.runtime.jobs(batch, invalid)
        outputs, hits = self.scheduler.run_with_stats(jobs, deadline)
        for job, out in zip(jobs, outputs, strict=True):
            results[(job.index, job.component_id)] = out
        timeouts = sum(o.status is Status.TIMEOUT for o in outputs)
        if timeouts:
            self.audit("timeouts", request_id=request.request_id, count=timeouts)
        try:
            scores = self.runtime.resolve(batch, results, self.state)
        except BatchFailedError as e:
            stats = pb.BatchStats(
                latency_ms=int((time.monotonic() - start) * 1000),
                failures_by_status=e.failures_by_status,
                cache_hits=hits,
                sample_count=len(samples),
            )
            self.audit("batch_failed", request_id=request.request_id, reason=e.message)
            context.set_trailing_metadata(((STATS_METADATA_KEY, stats.SerializeToString()),))
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, f"{e.code}: {e.message}")
        return pb.ScoreResponse(
            scores=[score_to_proto(s) for s in scores],
            stats=pb.BatchStats(
                latency_ms=int((time.monotonic() - start) * 1000),
                failures_by_status=batch_stats(scores),
                cache_hits=hits,
                sample_count=len(samples),
            ),
        )


def check_address(address: str) -> None:
    """Only a Unix socket or a loopback port: the service never listens on a network."""
    host = address.rpartition(":")[0]
    if address.startswith("unix:") and len(address) > len("unix:"):
        return
    if host in ("127.0.0.1", "[::1]", "localhost"):
        return
    raise RewardError(
        STARTUP_FAILED,
        f"The listen address {address!r} is not allowed. Use unix:/path/to.sock or "
        "127.0.0.1:<port>.",
    )


def serve(service: RewardExecService, address: str) -> tuple[grpc.Server, int]:
    """Start the gRPC server. Returns it and the bound port (0 for a Unix socket)."""
    check_address(address)
    size = service.limits.max_message_bytes
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="grpc"),
        options=[
            ("grpc.max_receive_message_length", size),
            ("grpc.max_send_message_length", size),
            ("grpc.so_reuseport", 0),
        ],
    )
    pb_grpc.add_RewardServiceServicer_to_server(service, server)  # pyright: ignore[reportUnknownMemberType]
    try:
        port = server.add_insecure_port(address)
    except RuntimeError as e:
        raise RewardError(STARTUP_FAILED, f"Could not listen on {address!r}: {e}") from e
    unix = address.startswith("unix:")
    if port == 0 and not unix:
        raise RewardError(STARTUP_FAILED, f"Could not listen on {address!r}.")
    server.start()
    return server, 0 if unix else port


__all__ = [
    "STARTUP_FAILED",
    "STATS_METADATA_KEY",
    "Audit",
    "ExecLimits",
    "ResultCache",
    "RewardExecService",
    "Scheduler",
    "check_address",
    "score_to_proto",
    "serve",
]
