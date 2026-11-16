"""PolicyRuntime: component results -> one reward per sample, with a full breakdown.

Order per batch: `when` -> run components -> check outputs -> per-component normalization ->
gates -> aggregation -> final normalization -> clipping -> failure policy -> batch failure rate.
A failed sample is masked, gets its declared fallback, or fails the batch; never a silent 0.
"""

from __future__ import annotations

import copy
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

from mrl_rewards.policy.model import (
    Aggregation,
    ComponentConfig,
    FailureAction,
    Normalization,
    Policy,
)
from mrl_rewards.registry import Registry
from mrl_rewards.types import (
    BATCH_FAILED,
    FAILED,
    PARAMS_INVALID,
    JSONValue,
    Resolution,
    RewardBatch,
    RewardContext,
    RewardError,
    RewardProvider,
    RewardResult,
    RewardSample,
    Scalar,
    Status,
    is_finite_number,
)
from mrl_rewards.types.validate import first_error, validator

_STD_FLOOR = 1e-8
FINAL_KEY = "__final__"


@dataclass(frozen=True)
class ComponentScore:
    component_id: str
    value: float | None
    status: Status
    diagnostics: Mapping[str, Scalar]
    flags: tuple[str, ...]


@dataclass(frozen=True)
class SampleScore:
    sample_id: str
    total: float | None  # None when masked or not applicable
    status: Status
    resolution: Resolution
    components: tuple[ComponentScore, ...]
    flags: tuple[str, ...]


class BatchFailedError(RewardError):
    """The whole batch fails: a fail_batch action, or too many failed samples."""

    def __init__(
        self, reason: str, failures_by_status: Mapping[str, int], sample_count: int
    ) -> None:
        super().__init__(BATCH_FAILED, reason)
        self.failures_by_status = dict(failures_by_status)
        self.sample_count = sample_count


@dataclass(frozen=True)
class Job:
    index: int  # position of the sample in its batch
    component_id: str
    provider: RewardProvider
    sample: RewardSample
    trainer_step: int
    timeout_ms: int | None
    deterministic: bool


class Executor(Protocol):
    """Runs component jobs. Must return one result per job and never raise for a job's failure."""

    def run(self, jobs: Sequence[Job]) -> list[RewardResult]: ...


def run_job(job: Job) -> RewardResult:
    """Call the component on one sample. Exceptions and wrong output shapes become statuses."""
    try:
        out = job.provider.score(RewardBatch([job.sample], job.trainer_step))
    except Exception as e:  # component code is untrusted: any exception is a scored ERROR
        return RewardResult.failed(Status.ERROR, f"{type(e).__name__}: {e}")
    if not isinstance(cast("object", out), list) or len(out) != 1:
        return RewardResult.failed(
            Status.INVALID_OUTPUT,
            "the component did not return one result per sample",
            "wrong_length",
        )
    result = cast("object", out[0])
    if not isinstance(result, RewardResult):
        return RewardResult.failed(Status.INVALID_OUTPUT, "the component returned a non-result")
    return result


class DirectExecutor:
    """Runs jobs one after another in this thread (tests and local dev; no timeouts)."""

    def run(self, jobs: Sequence[Job]) -> list[RewardResult]:
        return [run_job(j) for j in jobs]


@dataclass
class _Running:
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0


@dataclass
class NormalizerState:
    """Running statistics for zscore_running. Owned and checkpointed by the caller."""

    stats: dict[str, _Running] = field(default_factory=dict[str, _Running])

    def update(self, key: str, values: Sequence[float]) -> bool:
        """Add values (Welford). Returns False, changing nothing, if the stats would overflow."""
        old = self.stats.get(key, _Running())
        s = _Running(old.n, old.mean, old.m2)
        for v in values:
            s.n += 1
            d = v - s.mean
            s.mean += d / s.n
            s.m2 += d * (v - s.mean)
        if not (math.isfinite(s.mean) and math.isfinite(s.m2)):
            return False
        self.stats[key] = s
        return True

    def zscore(self, key: str, v: float) -> float:
        s = self.stats.get(key, _Running())
        std = math.sqrt(s.m2 / (s.n - 1)) if s.n > 1 else 0.0
        return 0.0 if std < _STD_FLOOR else (v - s.mean) / std

    def to_dict(self) -> dict[str, JSONValue]:
        return {k: {"n": s.n, "mean": s.mean, "m2": s.m2} for k, s in sorted(self.stats.items())}

    @staticmethod
    def from_dict(data: Mapping[str, JSONValue]) -> NormalizerState:
        state = NormalizerState()
        for k, v in data.items():
            d = cast("dict[str, float]", v)
            state.stats[k] = _Running(int(d["n"]), float(d["mean"]), float(d["m2"]))
        return state


def _rank_in_group(values: Mapping[int, float], groups: Mapping[int, str]) -> dict[int, float]:
    """Average rank within each group, scaled to [0, 1]; a group of one gets 0.5."""
    by_group: dict[str, list[int]] = defaultdict(list)
    for i in values:
        by_group[groups[i]].append(i)
    out: dict[int, float] = {}
    for members in by_group.values():
        n = len(members)
        for i in members:
            below = sum(values[j] < values[i] for j in members)
            equal = sum(values[j] == values[i] for j in members)
            out[i] = 0.5 if n == 1 else (below + (equal - 1) / 2) / (n - 1)
    return out


def _normalize(  # noqa: PLR0913 (keyword-only inputs)
    method: Normalization,
    values: dict[int, float],
    *,
    key: str,
    bounds: tuple[float, float] | None,
    groups: Mapping[int, str],
    state: NormalizerState,
) -> dict[int, float]:
    if method is Normalization.NONE or not values:
        return values
    if method is Normalization.MINMAX:
        lo, hi = bounds or (min(values.values()), max(values.values()))
        return {i: 0.5 if hi == lo else (v - lo) / (hi - lo) for i, v in values.items()}
    if method is Normalization.ZSCORE_RUNNING:
        if not state.update(key, [values[i] for i in sorted(values)]):
            return dict.fromkeys(values, math.nan)  # the callers fail these samples
        return {i: state.zscore(key, v) for i, v in values.items()}
    return _rank_in_group(values, groups)


def _aggregate(method: Aggregation, weighted: Sequence[float]) -> float:
    """The combined value; NaN or inf when it doesn't fit in a float (callers fail the sample)."""
    if method is Aggregation.WEIGHTED_SUM:
        try:
            return math.fsum(weighted)
        except (OverflowError, ValueError):  # intermediate overflow, or inf + -inf
            return math.nan
    if method is Aggregation.MIN:
        return min(weighted)
    return math.prod(weighted)


def _sanitize(r: RewardResult) -> RewardResult:
    diags: dict[str, Scalar] = {}
    for k, v in cast("Mapping[object, object]", r.diagnostics).items():
        diags[str(k)] = v if isinstance(v, bool | int | float | str) else str(v)
    flags = tuple(str(f) for f in cast("tuple[object, ...]", r.flags))
    return RewardResult(r.value, r.status, diags, flags).bounded()


def check_result(r: RewardResult, bounds: tuple[float, float]) -> RewardResult:  # noqa: PLR0911
    """Enforce the provider contract: finite, in range, coherent status. No clamping."""
    r = _sanitize(r)
    if r.status in (Status.TIMEOUT, Status.INVALID_OUTPUT):
        return r
    if r.status in (Status.ERROR, Status.INVALID_INPUT, Status.NOT_APPLICABLE):
        if r.value is not None:
            return RewardResult.failed(
                Status.INVALID_OUTPUT, f"a {r.status} result carried a value"
            )
        return r
    if r.value is None:
        return RewardResult(None, Status.NOT_APPLICABLE, r.diagnostics, r.flags)
    if not is_finite_number(r.value):
        return RewardResult(None, Status.INVALID_OUTPUT, r.diagnostics, (*r.flags, "non_finite"))
    lo, hi = bounds
    if not lo <= r.value <= hi:
        diags = {**r.diagnostics, "out_of_range_value": float(r.value)}
        return RewardResult(
            None, Status.INVALID_OUTPUT, diags, (*r.flags, "out_of_range")
        ).bounded()
    return RewardResult(float(r.value), Status.OK, r.diagnostics, r.flags)


def sample_view(s: RewardSample) -> dict[str, JSONValue]:
    return {
        "sample_id": s.sample_id,
        "group_id": s.group_id,
        "prompt": s.prompt,
        "completion": s.completion,
        "completion_token_count": s.completion_token_count,
        "finish_reason": s.finish_reason,
        "reference": s.reference,
        "episode": s.episode,
    }


def batch_stats(scores: Sequence[SampleScore]) -> dict[str, int]:
    """Failed-sample count by status name (BatchStats.failures_by_status)."""
    return dict(Counter(str(s.status) for s in scores if s.status in FAILED))


class PolicyRuntime:
    """Scores batches for one loaded policy. Stateless apart from the caller's NormalizerState."""

    def __init__(self, policy: Policy, providers: Mapping[str, RewardProvider]) -> None:
        self.policy = policy
        self.providers = dict(providers)
        self.bounds = {
            c.id: c.range or self.providers[c.id].spec.value_range for c in policy.components
        }
        self._shaping = {s.component: s for s in policy.shaping}

    @staticmethod
    def build(
        policy: Policy, registry: Registry, *, bundle_dir: Path | None = None, seed: int = 0
    ) -> PolicyRuntime:
        """Resolve every `use:`, check params against each component's schema, run setup()."""
        providers: dict[str, RewardProvider] = {}
        for c in policy.components:
            provider = registry.create(c.use)
            _check_params(c, provider)
            provider.setup(RewardContext(params=c.params, bundle_dir=bundle_dir, seed=seed))
            providers[c.id] = provider
        return PolicyRuntime(policy, providers)

    def teardown(self) -> None:
        for p in self.providers.values():
            p.teardown()

    def weight(self, c: ComponentConfig, step: int) -> float:
        s = self._shaping.get(c.id)
        return c.weight if s is None else s.weight_at(step)

    def jobs(
        self, batch: RewardBatch, invalid: Mapping[int, str] | None = None
    ) -> tuple[list[Job], dict[tuple[int, str], RewardResult]]:
        """Jobs to run, plus results already known: `when` false, or the sample's input is bad
        (`invalid`: sample index -> reason)."""
        jobs: list[Job] = []
        known: dict[tuple[int, str], RewardResult] = {}
        for i, s in enumerate(batch.samples):
            if invalid and i in invalid:
                for c in self.policy.components:
                    known[(i, c.id)] = RewardResult.invalid_input(invalid[i])
                continue
            view = {"sample": sample_view(s)}
            for c in self.policy.components:
                if c.when is not None and not c.when.evaluate(view):
                    known[(i, c.id)] = RewardResult.not_applicable("when_false")
                    continue
                p = self.providers[c.id]
                jobs.append(
                    Job(i, c.id, p, s, batch.trainer_step, c.timeout_ms, p.spec.deterministic)
                )
        return jobs, known

    def score(
        self,
        batch: RewardBatch,
        *,
        state: NormalizerState | None = None,
        executor: Executor | None = None,
    ) -> list[SampleScore]:
        jobs, results = self.jobs(batch)
        outputs = (executor or DirectExecutor()).run(jobs)
        if len(outputs) != len(jobs):
            raise RewardError(BATCH_FAILED, "The executor lost component results.")
        for job, out in zip(jobs, outputs, strict=True):
            results[(job.index, job.component_id)] = out
        return self.resolve(batch, results, state or NormalizerState())

    def resolve(
        self,
        batch: RewardBatch,
        raw: Mapping[tuple[int, str], RewardResult],
        state: NormalizerState,
    ) -> list[SampleScore]:
        """Raw component results -> sample scores. A failed batch leaves `state` unchanged."""
        snapshot = copy.deepcopy(state.stats)
        try:
            return self._resolve(batch, raw, state)
        except BatchFailedError:
            state.stats = snapshot
            raise

    def _resolve(
        self,
        batch: RewardBatch,
        raw: Mapping[tuple[int, str], RewardResult],
        state: NormalizerState,
    ) -> list[SampleScore]:
        p, n = self.policy, len(batch.samples)
        res = {k: check_result(r, self.bounds[k[1]]) for k, r in raw.items()}
        groups = {i: s.group_id for i, s in enumerate(batch.samples)}
        failed = {i for (i, _), r in res.items() if r.status in FAILED}
        healthy = [i for i in range(n) if i not in failed]
        normed = {
            c.id: _normalize(
                p.per_component,
                {
                    i: cast("float", res[(i, c.id)].value)
                    for i in healthy
                    if res[(i, c.id)].status is Status.OK
                },
                key=c.id,
                bounds=self.bounds[c.id],
                groups=groups,
                state=state,
            )
            for c in p.components
        }
        out = _Composed()
        for i in healthy:
            self._compose(batch, i, res, normed, out)
        totals = self._finalize(out, groups, state)
        failed |= out.not_finite

        scores: list[SampleScore] = []
        fail_batch: list[str] = []
        for i, s in enumerate(batch.samples):
            breakdown = tuple(
                ComponentScore(
                    c.id,
                    res[(i, c.id)].value,
                    res[(i, c.id)].status,
                    {**res[(i, c.id)].diagnostics, **out.extra[(i, c.id)]},
                    res[(i, c.id)].flags,
                )
                for c in p.components
            )
            flags = tuple(out.flags[i])
            if i in failed:
                score, fails_batch = self._resolve_failure(s, breakdown, flags)
                if fails_batch:
                    fail_batch.append(s.sample_id)
            elif i in out.not_applicable:
                score = SampleScore(
                    s.sample_id, None, Status.NOT_APPLICABLE, Resolution.MASKED, breakdown, flags
                )
            else:
                score = SampleScore(
                    s.sample_id, totals[i], Status.OK, Resolution.SCORED, breakdown, flags
                )
            scores.append(score)

        if fail_batch:
            raise BatchFailedError(
                f"Sample {fail_batch[0]!r} failed to score and the policy fails the whole batch "
                "when that happens.",
                batch_stats(scores),
                n,
            )
        if n and len(failed) / n > p.max_failure_rate:
            raise BatchFailedError(
                f"{len(failed)} of {n} samples failed to score, above the policy's limit of "
                f"{p.max_failure_rate:.0%}.",
                batch_stats(scores),
                n,
            )
        return scores

    def _finalize(
        self, out: _Composed, groups: Mapping[int, str], state: NormalizerState
    ) -> dict[int, float]:
        """Final normalization and clipping. A non-finite result fails its sample."""
        p = self.policy
        finals = _normalize(
            p.final, out.totals, key=FINAL_KEY, bounds=None, groups=groups, state=state
        )
        for i, t in finals.items():
            if not math.isfinite(t):
                out.not_finite.add(i)
                out.flags[i].append("total_not_finite")
        totals = {**{i: t for i, t in finals.items() if i not in out.not_finite}, **out.gated}
        if p.clipping:
            lo, hi = p.clipping
            for i, t in totals.items():
                if not lo <= t <= hi:
                    totals[i] = min(max(t, lo), hi)
                    out.flags[i].append("clipped")
        return totals

    def _compose(
        self,
        batch: RewardBatch,
        i: int,
        res: Mapping[tuple[int, str], RewardResult],
        normed: Mapping[str, Mapping[int, float]],
        out: _Composed,
    ) -> None:
        """Gates, then the weighted aggregate, for one healthy sample. Any value that isn't a
        finite number (overflowing normalization or aggregate) fails the sample, gate or not."""
        p = self.policy
        view = self._gate_view(batch.samples[i], i, res)
        gate = next((g for g in p.gates if not g.require.evaluate(view)), None)
        weighted: list[float] = []
        broken: list[str] = []
        for c in p.components:
            if res[(i, c.id)].status is not Status.OK:
                continue
            w = self.weight(c, batch.trainer_step)
            v = normed[c.id][i]
            if not math.isfinite(v):
                broken.append(f"normalization_not_finite:{c.id}")
                continue
            if not math.isfinite(w * v):
                broken.append(f"weighted_not_finite:{c.id}")
                continue
            weighted.append(w * v)
            if c.id in self._shaping:
                out.extra[(i, c.id)]["policy.weight"] = w
            if p.per_component is not Normalization.NONE:
                out.extra[(i, c.id)]["policy.normalized"] = v
        if broken:
            out.not_finite.add(i)
            out.flags[i] += broken
        elif gate is not None:
            out.gated[i] = gate.else_value
            out.flags[i] += ["gated", f"gated:{gate.id}"]
        elif not weighted:
            out.not_applicable.add(i)
        elif math.isfinite(total := _aggregate(p.aggregation, weighted)):
            out.totals[i] = total
        else:
            out.not_finite.add(i)
            out.flags[i].append("total_not_finite")

    def _gate_view(
        self, s: RewardSample, i: int, res: Mapping[tuple[int, str], RewardResult]
    ) -> dict[str, JSONValue]:
        components: dict[str, JSONValue] = {}
        for c in self.policy.components:
            r = res[(i, c.id)]
            components[c.id] = {
                "value": r.value,
                "status": str(r.status),
                "diagnostics": dict(r.diagnostics),
                "flags": list(r.flags),
            }
        return {"sample": sample_view(s), "components": components}

    def _resolve_failure(
        self, s: RewardSample, breakdown: tuple[ComponentScore, ...], flags: tuple[str, ...]
    ) -> tuple[SampleScore, bool]:
        """Apply on_timeout / on_error. Returns the score and whether the batch must fail.
        Precedence when several components failed: fail_batch, then mask, then fallback
        (an error's fallback value before a timeout's)."""
        bad = [c for c in breakdown if c.status in FAILED]
        status = bad[0].status if bad else Status.ERROR
        pairs = [
            (
                c.status,
                self.policy.on_timeout if c.status is Status.TIMEOUT else self.policy.on_error,
            )
            for c in bad
        ] or [(Status.ERROR, self.policy.on_error)]
        flags = (*flags, *(f"{str(c.status).lower()}:{c.component_id}" for c in bad))
        kinds = {a.action for _, a in pairs}
        if kinds & {FailureAction.FAIL_BATCH, FailureAction.MASK}:
            masked = SampleScore(
                s.sample_id, None, status, Resolution.MASKED, breakdown, (*flags, "masked")
            )
            return masked, FailureAction.FAIL_BATCH in kinds
        chosen = sorted(pairs, key=lambda sa: sa[0] is Status.TIMEOUT)[0][1]
        return SampleScore(
            s.sample_id,
            cast("float", chosen.fallback_value),
            status,
            Resolution.FALLBACK_APPLIED,
            breakdown,
            (*flags, "fallback"),
        ), False


@dataclass
class _Composed:
    totals: dict[int, float] = field(default_factory=dict[int, float])
    gated: dict[int, float] = field(default_factory=dict[int, float])
    not_applicable: set[int] = field(default_factory=set[int])
    not_finite: set[int] = field(default_factory=set[int])
    flags: defaultdict[int, list[str]] = field(
        default_factory=lambda: defaultdict[int, list[str]](list)
    )
    extra: defaultdict[tuple[int, str], dict[str, Scalar]] = field(
        default_factory=lambda: defaultdict[tuple[int, str], dict[str, Scalar]](dict)
    )


def _check_params(c: ComponentConfig, provider: RewardProvider) -> None:
    if err := first_error(validator(provider.spec.param_schema), c.params):
        where = "" if err[0] == "/" else f" at {err[0]}"
        raise RewardError(PARAMS_INVALID, f"Component {c.id!r} ({c.use}){where}: {err[1][:200]}.")


__all__ = [
    "FINAL_KEY",
    "BatchFailedError",
    "ComponentScore",
    "DirectExecutor",
    "Executor",
    "Job",
    "NormalizerState",
    "PolicyRuntime",
    "SampleScore",
    "batch_stats",
    "check_result",
    "run_job",
    "sample_view",
]
