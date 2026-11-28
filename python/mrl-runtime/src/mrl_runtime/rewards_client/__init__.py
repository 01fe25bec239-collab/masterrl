"""The harness `RewardClient`: the only thing an adapter sees of rewards (Doc 05 §12.5).

The reward service runs in its own sandbox; the client talks to it over a Unix socket and
treats every answer as untrusted.

    client = RewardClient(socket, nonce=…, reward_version_id="rwdv_…", attempt_id=…,
                          expected_sha256=job_spec["reward"]["exec_sha256"], emit=on_metrics)
    info = client.describe()            # hash mismatch → RewardHashMismatchError (MRL-RWD-292)
    results = client.score(samples, trainer_step=step)   # one RewardOutcome per sample
    rl_state["reward_client"] = client.state_dict()      # restored with load_state_dict()

- **Batching:** samples get stable ids (`<batch>:<index>`), are split into chunks of the
  service's `max_batch` and sent with bounded concurrency. Each chunk's `request_id` is a
  hash of (attempt, batch number, chunk), so a retried chunk is answered from the service's
  replay cache instead of being scored twice.
- **Timeouts:** each call has a deadline (`timeout_s`); a deadline or an unreachable
  service counts like a failed batch below.
- **Untrusted answers:** a reply with the wrong number of scores, foreign or reordered
  sample ids, non-finite values, component values outside their declared range or oversized
  diagnostics makes that sample INVALID_OUTPUT (failed, no value): never a crash, never a 0.
- **Failure policy:** a batch the service refuses for too many failures
  (FAILED_PRECONDITION), a timeout or a dropped connection → wait for the service to be
  healthy again (the agent restarts it), retry the batch once, then `RewardInfraError`
  (MRL-RWD-293; the harness fails the attempt REWARD_INFRA).
- **Masks and fallbacks:** a MASKED sample has `value=None, masked=True`; a fallback has the
  policy's value and `resolution="FALLBACK_APPLIED"`. Groups whose samples are all masked
  are counted (`rl/reward/groups_dropped`). Adapters apply their engine's mask semantics.
- **State:** the client owns its running statistics (per-component mean/std, the batch
  counter) and hands them out as `state_dict()` for `rl_state.json`; the service keeps its
  own `zscore_running` normalizer for the life of the attempt (the protocol has no field to
  carry it).
- **Metrics** per batch through `emit` (names from Doc 06 §5.3, all live):
  `rl/reward/mean`, `rl/reward/failure_rate`, `rl/reward/timeout_rate`,
  `rl/reward/fallback_rate`, `rl/reward/masked_rate`, `rl/reward/groups_dropped`,
  `rl/reward/latency_ms_p95`, `rl/reward/component/<id>/mean` and `…/std`.

No reward code is ever imported into the worker: this package speaks only the protocol.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import grpc

from mrl_runtime._generated.mrl.reward.v1 import reward_service_pb2 as pb
from mrl_runtime._generated.mrl.reward.v1 import reward_service_pb2_grpc as pb_grpc
from mrl_runtime.errors import MrlError

UNTRUSTED_ANSWER = "MRL-RWD-291"
HASH_MISMATCH = "MRL-RWD-292"
REWARD_INFRA = "MRL-RWD-293"
SANDBOX_VIOLATION = "MRL-RWD-294"
MAX_DIAGNOSTICS_BYTES = 4096
MAX_FLAGS = 16
_BATCH_SCOPE = "mrl-batch-scope"
_NORMALIZER_STATE = "mrl-normalizer-state-bin"
_MAX_STATE_BYTES = 6000
_RETRYABLE = frozenset(
    {
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.CANCELLED,
    }
)
_STATUS_NAMES = {
    pb.SCORE_STATUS_OK: "OK",
    pb.SCORE_STATUS_TIMEOUT: "TIMEOUT",
    pb.SCORE_STATUS_ERROR: "ERROR",
    pb.SCORE_STATUS_INVALID_OUTPUT: "INVALID_OUTPUT",
    pb.SCORE_STATUS_NOT_APPLICABLE: "NOT_APPLICABLE",
    pb.SCORE_STATUS_INVALID_INPUT: "INVALID_INPUT",
}
_RESOLUTION_NAMES = {
    pb.RESOLUTION_SCORED: "SCORED",
    pb.RESOLUTION_MASKED: "MASKED",
    pb.RESOLUTION_FALLBACK_APPLIED: "FALLBACK_APPLIED",
}
_FAILED = frozenset({"TIMEOUT", "ERROR", "INVALID_OUTPUT", "INVALID_INPUT"})


class RewardClientError(MrlError):
    """A reward client problem (codes MRL-RWD-291…293)."""


class RewardHashMismatchError(RewardClientError):
    """The service loaded another reward than the job pinned: never train on it."""


class RewardInfraError(RewardClientError):
    """The service kept failing after one retry: the attempt fails REWARD_INFRA."""


@dataclass(frozen=True)
class Sample:
    """One completion to score."""

    prompt: str
    completion: str
    group_id: str = ""
    reference: object = None  # any JSON: a string, or {"answer": …}
    completion_token_count: int = 0
    finish_reason: str = "stop"
    episode: object = None
    policy_version: int = 0


@dataclass(frozen=True)
class ComponentDescription:
    component_id: str
    component_ref: str
    weight: float
    value_min: float
    value_max: float
    tier: int
    deterministic: bool


@dataclass(frozen=True)
class RewardDescription:
    reward_version_id: str
    content_sha256: str
    signal: str
    components: tuple[ComponentDescription, ...]
    max_batch: int
    max_tier: int
    batch_scope: str = ""


@dataclass(frozen=True)
class RewardOutcome:
    """One sample's reward. `value` is None when the sample is masked or failed."""

    sample_id: str
    value: float | None
    resolution: str  # SCORED | MASKED | FALLBACK_APPLIED
    components: Mapping[str, float | None]
    statuses: Mapping[str, str]
    diagnostics: Mapping[str, Mapping[str, str]]
    flags: tuple[str, ...]
    chunk_index: int | None = None
    latency_ms: float | None = None

    @property
    def masked(self) -> bool:
        return self.value is None

    @property
    def failed(self) -> bool:
        return any(s in _FAILED for s in self.statuses.values()) or "invalid_output" in self.flags

    @property
    def worst_status(self) -> str:
        for s in ("INVALID_OUTPUT", "ERROR", "TIMEOUT", "INVALID_INPUT"):
            if s in self.statuses.values() or (
                s == "INVALID_OUTPUT" and "invalid_output" in self.flags
            ):
                return s
        return "OK"


@dataclass
class _Running:
    """Welford mean/variance."""

    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add(self, x: float) -> None:
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)

    @property
    def std(self) -> float:
        return math.sqrt(self.m2 / self.n) if self.n > 1 else 0.0


@dataclass
class _State:
    batches: int = 0
    components: dict[str, _Running] = field(default_factory=dict[str, _Running])
    normalizer: str = "{}"


def _channel(socket: str) -> grpc.Channel:
    path = socket.removeprefix("unix:")
    if not Path(path).is_absolute():
        raise RewardClientError(
            UNTRUSTED_ANSWER,
            f"The reward service address {socket!r} is not a Unix socket path.",
            hint="The agent passes the reward socket; only Unix sockets are accepted.",
        )
    return grpc.insecure_channel(
        f"unix:{path}",
        options=[
            ("grpc.default_authority", "localhost"),
            ("grpc.max_receive_message_length", 32 << 20),
        ],
    )


def _is_number(v: object) -> bool:
    return isinstance(v, float | int) and not isinstance(v, bool) and math.isfinite(v)


class RewardClient:
    """See the module docs."""

    def __init__(  # noqa: PLR0913 (keyword-only settings)
        self,
        socket: str,
        *,
        nonce: str,
        reward_version_id: str,
        attempt_id: str,
        expected_sha256: str | None = None,
        timeout_s: float = 60.0,
        max_concurrency: int = 2,
        health_wait_s: float = 30.0,
        emit: Callable[[Mapping[str, float]], None] | None = None,
        channel: grpc.Channel | None = None,
    ) -> None:
        if not socket.startswith(("/", "unix:/")):
            raise RewardClientError(
                UNTRUSTED_ANSWER,
                f"The reward service address {socket!r} is not a Unix socket path.",
                hint="The agent passes the reward socket; only Unix sockets are accepted.",
            )
        self._channel = channel or _channel(socket)
        # The generated stub is untyped; every answer is cast to its message type.
        self._stub: Any = pb_grpc.RewardServiceStub(self._channel)
        self._nonce = nonce
        self.version = reward_version_id
        self.attempt = attempt_id
        self.expected = expected_sha256
        self.timeout_s = timeout_s
        self.health_wait_s = health_wait_s
        self.emit = emit
        self._pool = ThreadPoolExecutor(
            max_workers=max(1, max_concurrency), thread_name_prefix="reward"
        )
        self._state = _State()
        self._lock = threading.Lock()
        self._normalizer_lock = threading.Lock()
        self.description: RewardDescription | None = None

    # ---- lifecycle ----

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
        self._channel.close()

    def state_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "batches": self._state.batches,
                "normalizer": json.loads(self._state.normalizer),
                "components": {
                    k: {"n": r.n, "mean": r.mean, "m2": r.m2}
                    for k, r in self._state.components.items()
                },
            }

    def load_state_dict(self, state: Mapping[str, Any]) -> None:
        comps = cast("Mapping[str, Mapping[str, Any]]", state.get("components") or {})
        with self._lock:
            self._state = _State(
                batches=int(state.get("batches", 0)),
                normalizer=json.dumps(
                    state.get("normalizer", {}), separators=(",", ":"), allow_nan=False
                ),
                components={
                    k: _Running(
                        int(v.get("n", 0)), float(v.get("mean", 0.0)), float(v.get("m2", 0.0))
                    )
                    for k, v in comps.items()
                },
            )

    # ---- describe ----

    def describe(self) -> RewardDescription:
        """Asks the service what it loaded and checks it against the pinned hash."""
        try:
            response, call = cast(
                "tuple[pb.DescribeResponse, grpc.Call]",
                self._stub.Describe.with_call(
                    pb.DescribeRequest(attempt_nonce=self._nonce), timeout=self.timeout_s
                ),
            )
            r = response
        except grpc.RpcError as e:
            raise RewardInfraError(
                REWARD_INFRA, f"The reward service didn't describe itself: {_rpc(e)}"
            ) from e
        if r.reward_version_id != self.version:
            raise RewardHashMismatchError(
                HASH_MISMATCH,
                f"The reward service runs {r.reward_version_id!r}, not {self.version!r}.",
            )
        if self.expected and r.content_sha256 != self.expected:
            raise RewardHashMismatchError(
                HASH_MISMATCH,
                "The reward service loaded other files than the pinned reward version "
                f"(hash {r.content_sha256[:12]}…, expected {self.expected[:12]}…).",
            )
        comps = tuple(
            ComponentDescription(
                c.component_id,
                c.component_ref,
                c.weight,
                c.value_min,
                c.value_max,
                c.tier,
                c.deterministic,
            )
            for c in r.components
        )
        if not comps or r.max_batch < 1:
            raise RewardClientError(UNTRUSTED_ANSWER, "The reward service described no components.")
        self.description = RewardDescription(
            r.reward_version_id,
            r.content_sha256,
            r.signal,
            comps,
            int(r.max_batch),
            int(r.max_tier),
            str(_trailer(call, _BATCH_SCOPE) or ""),
        )
        return self.description

    # ---- score ----

    def score(self, samples: Sequence[Sample], *, trainer_step: int = 0) -> list[RewardOutcome]:
        """One outcome per sample, in order."""
        desc = self.describe() if self.expected or self.description is None else self.description
        with self._lock:
            batch_no = self._state.batches
            self._state.batches += 1
        ids = [f"{batch_no}:{i}" for i in range(len(samples))]
        size = desc.max_batch
        if desc.batch_scope and len(samples) > size:
            raise RewardClientError(
                UNTRUSTED_ANSWER,
                f"This reward normalizes the whole batch. Send at most {size} samples.",
            )
        groups: dict[str | int, list[int]] = {}
        for index, sample in enumerate(samples):
            groups.setdefault(sample.group_id or index, []).append(index)
        chunks: list[list[int]] = []
        for group in groups.values():
            if len(group) > size:
                raise RewardClientError(
                    UNTRUSTED_ANSWER,
                    f"A reward group has {len(group)} samples, above the service limit of {size}.",
                )
            if not chunks or len(chunks[-1]) + len(group) > size:
                chunks.append([])
            chunks[-1].extend(group)
        start = time.monotonic()
        futures = [
            self._pool.submit(
                self._chunk,
                desc,
                batch_no,
                k,
                [samples[i] for i in idx],
                [ids[i] for i in idx],
                trainer_step,
            )
            for k, idx in enumerate(chunks)
        ]
        ordered: list[tuple[int, RewardOutcome]] = []
        latencies: list[float] = []
        for chunk_index, (indices, future) in enumerate(zip(chunks, futures, strict=True)):
            got, latency = future.result()
            ordered.extend(
                (index, replace(outcome, chunk_index=chunk_index, latency_ms=latency))
                for index, outcome in zip(indices, got, strict=True)
            )
            latencies.append(latency)
        ordered.sort(key=lambda pair: pair[0])
        out = [outcome for _, outcome in ordered]
        self._observe(desc, samples, out, latencies, time.monotonic() - start)
        return out

    def _request_id(self, batch_no: int, chunk: int) -> str:
        return hashlib.sha256(f"{self.attempt}\n{batch_no}\n{chunk}".encode()).hexdigest()[:32]

    def _chunk(  # noqa: PLR0913, PLR0917 (one chunk's whole context)
        self,
        desc: RewardDescription,
        batch_no: int,
        chunk: int,
        samples: Sequence[Sample],
        ids: Sequence[str],
        trainer_step: int,
    ) -> tuple[list[RewardOutcome], float]:
        req = pb.ScoreRequest(
            attempt_nonce=self._nonce,
            request_id=self._request_id(batch_no, chunk),
            reward_version_id=self.version,
            deadline_ms=int(self.timeout_s * 1000 * 0.9),
            trainer_step=trainer_step,
            samples=[
                pb.Sample(
                    sample_id=sid,
                    group_id=s.group_id or sid,
                    prompt=s.prompt,
                    completion=s.completion,
                    completion_token_count=max(0, s.completion_token_count),
                    finish_reason=s.finish_reason,
                    reference_json=json.dumps(s.reference),
                    episode_json="" if s.episode is None else json.dumps(s.episode),
                    policy_version=s.policy_version,
                )
                for s, sid in zip(samples, ids, strict=True)
            ],
        )
        if desc.batch_scope == "running":
            with self._normalizer_lock:
                return self._score_rpc(desc, ids, req)
        return self._score_rpc(desc, ids, req)

    def _score_rpc(
        self, desc: RewardDescription, ids: Sequence[str], req: pb.ScoreRequest
    ) -> tuple[list[RewardOutcome], float]:
        running = desc.batch_scope == "running"
        metadata = ((_NORMALIZER_STATE, self._state.normalizer.encode()),) if running else ()
        for attempt in (0, 1):
            t0 = time.monotonic()
            try:
                resp, call = cast(
                    "tuple[pb.ScoreResponse, grpc.Call]",
                    self._stub.Score.with_call(req, timeout=self.timeout_s, metadata=metadata),
                )
                elapsed = (time.monotonic() - t0) * 1000
                scores = check_response(desc, ids, resp)
                if running:
                    raw = _trailer(call, _NORMALIZER_STATE)
                    if not isinstance(raw, bytes) or len(raw) > _MAX_STATE_BYTES:
                        raise RewardClientError(
                            UNTRUSTED_ANSWER, "The reward sent no valid checkpoint."
                        )
                    try:
                        checkpoint = json.dumps(
                            json.loads(raw), separators=(",", ":"), allow_nan=False
                        )
                    except (ValueError, UnicodeError) as e:
                        raise RewardClientError(
                            UNTRUSTED_ANSWER, "The reward checkpoint is invalid."
                        ) from e
                    with self._lock:
                        self._state.normalizer = checkpoint
                return scores, elapsed
            except grpc.RpcError as e:
                call = cast("grpc.Call", e) if isinstance(e, grpc.Call) else None
                code = call.code() if call is not None else None
                if (
                    call is not None
                    and code == grpc.StatusCode.ABORTED
                    and call.details() == SANDBOX_VIOLATION
                ):
                    raise RewardClientError(
                        SANDBOX_VIOLATION, "The sandbox stopped the reward code."
                    ) from e
                if code not in _RETRYABLE or attempt == 1:
                    raise RewardInfraError(
                        REWARD_INFRA, f"The reward service failed a batch twice: {_rpc(e)}"
                    ) from e
                self._wait_healthy()
                if self.describe() != desc:
                    raise RewardHashMismatchError(
                        HASH_MISMATCH, "The reward service changed while scoring a batch."
                    ) from e
        raise RewardInfraError(REWARD_INFRA, "The reward service failed a batch twice.")

    def _wait_healthy(self) -> None:
        deadline = time.monotonic() + self.health_wait_s
        while time.monotonic() < deadline:
            try:
                h = cast(
                    "pb.HealthResponse",
                    self._stub.Health(pb.HealthRequest(attempt_nonce=self._nonce), timeout=2.0),
                )
                if h.serving:
                    return
            except grpc.RpcError:
                pass
            time.sleep(0.5)

    def _observe(
        self,
        desc: RewardDescription,
        samples: Sequence[Sample],
        out: Sequence[RewardOutcome],
        latencies: Sequence[float],
        _total_s: float,
    ) -> None:
        n = max(1, len(out))
        groups: dict[str, list[bool]] = {}
        for s, o in zip(samples, out, strict=True):
            groups.setdefault(s.group_id or o.sample_id, []).append(o.masked)
        values = [o.value for o in out if o.value is not None]
        m: dict[str, float] = {
            "rl/reward/failure_rate": sum(o.failed for o in out) / n,
            "rl/reward/timeout_rate": sum("TIMEOUT" in o.statuses.values() for o in out) / n,
            "rl/reward/fallback_rate": sum(o.resolution == "FALLBACK_APPLIED" for o in out) / n,
            "rl/reward/masked_rate": sum(o.masked for o in out) / n,
            "rl/reward/groups_dropped": float(sum(all(g) for g in groups.values())),
            "rl/reward/latency_ms_p95": _p95(latencies),
        }
        if values:
            m["rl/reward/mean"] = sum(values) / len(values)
        with self._lock:
            for c in desc.components:
                vals = [v for o in out if (v := o.components.get(c.component_id)) is not None]
                run = self._state.components.setdefault(c.component_id, _Running())
                for v in vals:
                    run.add(v)
                if vals:
                    mean = sum(vals) / len(vals)
                    m[f"rl/reward/component/{c.component_id}/mean"] = mean
                    m[f"rl/reward/component/{c.component_id}/std"] = math.sqrt(
                        sum((v - mean) ** 2 for v in vals) / len(vals)
                    )
        if self.emit is not None:
            self.emit(m)


def _trailer(call: grpc.Call, name: str) -> str | bytes | None:
    return next((value for key, value in call.trailing_metadata() or () if key == name), None)


def _p95(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, round((len(s) - 1) * 0.95))]


def _rpc(e: grpc.RpcError) -> str:
    if isinstance(e, grpc.Call):
        call = cast("grpc.Call", e)
        return f"{call.code().name}: {(call.details() or '')[:300]}"
    return type(e).__name__


def _invalid(sample_id: str, why: str) -> RewardOutcome:
    return RewardOutcome(
        sample_id,
        None,
        "MASKED",
        {},
        {"_response": "INVALID_OUTPUT"},
        {"_response": {"reason": why}},
        ("invalid_output",),
    )


def check_response(  # noqa: PLR0912, PLR0915 (one check per untrusted field)
    desc: RewardDescription, ids: Sequence[str], resp: pb.ScoreResponse
) -> list[RewardOutcome]:
    """Turns an untrusted reply into outcomes: anything that doesn't hold makes the sample
    (or, for a wrong shape, every sample) INVALID_OUTPUT."""
    if len(resp.scores) != len(ids):
        why = f"the reply had {len(resp.scores)} scores for {len(ids)} samples"
        return [_invalid(i, why) for i in ids]
    bounds = {c.component_id: (c.value_min, c.value_max) for c in desc.components}
    out: list[RewardOutcome] = []
    for sid, s in zip(ids, resp.scores, strict=True):
        if s.sample_id != sid:
            out.append(_invalid(sid, "the reply's scores were for other samples or out of order"))
            continue
        problem = ""
        diag_bytes = 0
        comps: dict[str, float | None] = {}
        statuses: dict[str, str] = {}
        diags: dict[str, dict[str, str]] = {}
        for c in s.components:
            if c.component_id not in bounds or c.component_id in comps:
                problem = f"unknown or repeated component {c.component_id[:40]!r}"
                break
            value = c.value if c.HasField("value") else None
            lo, hi = bounds[c.component_id]
            if value is not None and (not _is_number(value) or not lo <= value <= hi):
                problem = f"component {c.component_id} value outside [{lo}, {hi}]"
                break
            if c.status not in _STATUS_NAMES or (c.status == pb.SCORE_STATUS_OK) != (
                value is not None
            ):
                problem = f"component {c.component_id} sent an inconsistent status"
                break
            diag_bytes += sum(len(k.encode()) + len(v.encode()) for k, v in c.diagnostics.items())
            if diag_bytes > MAX_DIAGNOSTICS_BYTES or len(c.flags) > MAX_FLAGS:
                problem = f"component {c.component_id} sent oversized diagnostics"
                break
            comps[c.component_id] = value
            statuses[c.component_id] = _STATUS_NAMES.get(c.status, "ERROR")
            diags[c.component_id] = dict(c.diagnostics)
        resolution = _RESOLUTION_NAMES.get(s.resolution, "")
        total = s.total if s.HasField("total") else None
        status = _STATUS_NAMES.get(s.status)
        if not problem:
            if len(comps) != len(bounds):
                problem = "the reply is missing component results"
            elif status is None or (status == "OK") != (resolution == "SCORED"):
                problem = "the reply has an inconsistent sample status"
            elif not resolution:
                problem = "the reply has no resolution"
            elif total is not None and not _is_number(total):
                problem = "the total is not a finite number"
            elif (resolution == "MASKED") != (total is None):
                problem = "the total and the resolution disagree"
            elif len(s.flags) > MAX_FLAGS:
                problem = "too many flags"
        if problem:
            out.append(_invalid(sid, problem))
            continue
        if status in _FAILED and status not in statuses.values():
            statuses["_response"] = status
        flags = tuple(s.flags)
        if "sandbox_violation" not in flags and any(
            "sandbox_violation" in c.flags for c in s.components
        ):
            flags += ("sandbox_violation",)
        out.append(RewardOutcome(sid, total, resolution, comps, statuses, diags, flags))
    return out


__all__ = [
    "HASH_MISMATCH",
    "REWARD_INFRA",
    "UNTRUSTED_ANSWER",
    "ComponentDescription",
    "RewardClient",
    "RewardClientError",
    "RewardDescription",
    "RewardHashMismatchError",
    "RewardInfraError",
    "RewardOutcome",
    "Sample",
    "check_response",
]
