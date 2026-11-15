"""Policy runtime: RW-2 composition goldens and property, RW-4 failure matrix, load checks."""

import copy
import json
import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mrl_rewards.policy import (
    BatchFailedError,
    DirectExecutor,
    Job,
    NormalizerState,
    PolicyRuntime,
    load_policy,
)
from mrl_rewards.registry import Registry, builtin_registry
from mrl_rewards.types import (
    ComponentSpec,
    Resolution,
    RewardBatch,
    RewardContext,
    RewardError,
    RewardResult,
    RewardSample,
    Status,
)

ROOT = Path(__file__).parents[3]


class Echo:
    """Test component: returns reference[key], or misbehaves on request."""

    spec = ComponentSpec(
        name="echo",
        version="1.0.0",
        value_range=(-1e308, 1e308),
        deterministic=True,
        description="test echo",
        param_schema={
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
            "additionalProperties": False,
        },
    )

    def setup(self, ctx: RewardContext) -> None:
        self.key = str(ctx.params["key"])

    def teardown(self) -> None:
        pass

    def score(self, batch: RewardBatch) -> list[RewardResult]:
        out: list[RewardResult] = []
        for s in batch.samples:
            v = s.reference.get(self.key) if isinstance(s.reference, dict) else None
            match v:
                case "raise":
                    raise ValueError("boom")
                case "wrong_length":
                    return []
                case "nan":
                    out.append(RewardResult.ok(math.nan))
                case "inf":
                    out.append(RewardResult.ok(math.inf))
                case "error":
                    out.append(RewardResult.failed(Status.ERROR, "said error"))
                case "invalid_input":
                    out.append(RewardResult.invalid_input("bad row"))
                case "fake_timeout":
                    out.append(RewardResult.ok(1.0))  # replaced by TimeoutExecutor
                case None:
                    out.append(RewardResult(None))
                case _:
                    out.append(RewardResult.ok(float(v)))  # type: ignore[arg-type]
        return out


class TimeoutExecutor(DirectExecutor):
    """Injects TIMEOUT the way mrl-reward-exec's scheduler reports it."""

    def run(self, jobs: Sequence[Job]) -> list[RewardResult]:
        out = super().run(jobs)
        return [
            RewardResult.failed(Status.TIMEOUT, "slow", "sample_timeout")
            if isinstance(j.sample.reference, dict)
            and "fake_timeout" in j.sample.reference.values()
            and j.sample.reference.get(j.component_id) == "fake_timeout"
            else r
            for j, r in zip(jobs, out, strict=True)
        ]


def registry() -> Registry:
    reg = builtin_registry()
    reg.register(Echo, namespace="org")
    return reg


def doc(components: list[dict[str, Any]], **spec: Any) -> dict[str, Any]:
    comps = [
        {"use": "org/echo@1", "weight": 1.0, **c, "params": {"key": c["id"], **c.get("params", {})}}
        for c in components
    ]
    return {
        "apiVersion": "mrl.dev/reward/v1",
        "kind": "RewardPolicy",
        "metadata": {"name": "t", "signal": "scalar.sequence"},
        "spec": {
            "components": comps,
            "aggregation": {"method": spec.pop("method", "weighted_sum"), "where": "platform"},
            "failure": spec.pop(
                "failure",
                {
                    "on_timeout": {"action": "mask"},
                    "on_error": {"action": "mask"},
                    "max_failure_rate": 1,
                },
            ),
            "tests": "./tests/cases.yaml",
            **spec,
        },
    }


def runtime(d: dict[str, Any]) -> PolicyRuntime:
    return PolicyRuntime.build(load_policy(d), registry())


def samples(*refs: dict[str, Any], group: str = "g") -> list[RewardSample]:
    return [RewardSample(f"s{i}", group, "p", "c", 1, "stop", r) for i, r in enumerate(refs)]


def score(d: dict[str, Any], *refs: dict[str, Any], step: int = 0, **kw: Any):
    return runtime(d).score(RewardBatch(samples(*refs), step), **kw)


# --- RW-2 goldens -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "want"),
    [("weighted_sum", 0.5 + 0.5 * 4), ("product", 0.5 * (0.5 * 4)), ("min", 0.5)],
)
def test_aggregation(method: str, want: float) -> None:
    d = doc([{"id": "a"}, {"id": "b", "weight": 0.5}], method=method)
    (s,) = score(d, {"a": 0.5, "b": 4})
    assert s.total == pytest.approx(want)
    assert (s.status, s.resolution) == (Status.OK, Resolution.SCORED)
    assert [c.value for c in s.components] == [0.5, 4.0]


def test_gate_sets_else_value_and_flags() -> None:
    d = doc(
        [{"id": "a"}, {"id": "b"}],
        gates=[{"id": "needs-a", "require": "components.a.value > 0", "else_value": -0.5}],
    )
    ok, gated = score(d, {"a": 1, "b": 1}, {"a": 0, "b": 1})
    assert ok.total == 2.0
    assert gated.total == -0.5
    assert {"gated", "gated:needs-a"} <= set(gated.flags)
    assert len(gated.components) == 2  # breakdown kept


def test_clipping_flags() -> None:
    d = doc([{"id": "a"}], clipping={"min": -1, "max": 1})
    hi, lo, mid = score(d, {"a": 3}, {"a": -3}, {"a": 0.5})
    assert (hi.total, lo.total, mid.total) == (1.0, -1.0, 0.5)
    assert "clipped" in hi.flags
    assert "clipped" in lo.flags
    assert "clipped" not in mid.flags


def test_when_and_none() -> None:
    d = doc([{"id": "a"}, {"id": "b", "when": "sample.reference.use_b == true"}])
    with_b, without_b, none_a = score(
        d, {"a": 1, "b": 2, "use_b": True}, {"a": 1, "b": 2}, {"b": 2}
    )
    assert with_b.total == 3.0
    assert without_b.total == 1.0
    assert without_b.components[1].status == Status.NOT_APPLICABLE
    assert without_b.components[1].diagnostics["reason"] == "when_false"
    # a is None (not applicable) and b is gated off by `when`: nothing applies.
    assert none_a.total is None
    assert (none_a.status, none_a.resolution) == (Status.NOT_APPLICABLE, Resolution.MASKED)


def test_normalization_minmax_uses_declared_range() -> None:
    d = doc(
        [{"id": "a", "range": [0, 10]}], normalization={"per_component": "minmax", "final": "none"}
    )
    (s,) = score(d, {"a": 2.5})
    assert s.total == 0.25
    assert s.components[0].value == 2.5  # breakdown keeps the raw value
    assert s.components[0].diagnostics["policy.normalized"] == 0.25


def test_normalization_rank_in_group() -> None:
    d = doc([{"id": "a"}], normalization={"per_component": "rank_in_group", "final": "none"})
    rt = runtime(d)
    batch = samples({"a": 3}, {"a": 1}, {"a": 2}, {"a": 2})
    batch[3] = RewardSample("solo", "other", "p", "c", 1, "stop", {"a": 2})
    out = rt.score(RewardBatch(batch))
    assert [s.total for s in out] == [1.0, 0.0, 0.5, 0.5]


def test_zscore_running_state_is_callers_and_rolls_back_on_failure() -> None:
    d = doc(
        [{"id": "a"}],
        normalization={"per_component": "zscore_running", "final": "none"},
        failure={
            "on_timeout": {"action": "mask"},
            "on_error": {"action": "mask"},
            "max_failure_rate": 0.5,
        },
    )
    rt, state = runtime(d), NormalizerState()
    first = rt.score(RewardBatch(samples({"a": 1}, {"a": 3})), state=state)
    assert [s.total for s in first] == pytest.approx([-1 / math.sqrt(2), 1 / math.sqrt(2)])
    saved = copy.deepcopy(state.to_dict())
    with pytest.raises(BatchFailedError):
        rt.score(RewardBatch(samples({"a": 100}, {"a": "raise"}, {"a": "raise"})), state=state)
    assert state.to_dict() == saved
    restored = NormalizerState.from_dict(json.loads(json.dumps(saved)))
    again = rt.score(RewardBatch(samples({"a": 2})), state=restored)
    assert again[0].total == pytest.approx(0.0)


def test_final_normalization() -> None:
    d = doc([{"id": "a"}], normalization={"per_component": "none", "final": "minmax"})
    out = score(d, {"a": 10}, {"a": 20}, {"a": 15})
    assert [s.total for s in out] == [0.0, 1.0, 0.5]


@pytest.mark.parametrize(("step", "weight"), [(0, 0.3), (100, 0.2), (200, 0.1), (5000, 0.1)])
def test_shaping_is_piecewise_linear(step: int, weight: float) -> None:
    d = doc(
        [{"id": "a"}],
        shaping=[
            {
                "id": "warm",
                "component": "a",
                "schedule": [{"from_step": 0, "weight": 0.3}, {"from_step": 200, "weight": 0.1}],
            }
        ],
    )
    (s,) = score(d, {"a": 1}, step=step)
    assert s.total == pytest.approx(weight)
    assert s.components[0].diagnostics["policy.weight"] == pytest.approx(weight)


# --- RW-2 property -------------------------------------------------------------------------

finite = st.floats(min_value=-1e6, max_value=1e6, allow_nan=False)


@settings(max_examples=400, deadline=None)
@given(
    values=st.lists(finite, min_size=1, max_size=4),
    weights=st.lists(
        st.floats(min_value=-10, max_value=10, allow_nan=False), min_size=4, max_size=4
    ),
    method=st.sampled_from(["weighted_sum", "product", "min"]),
    norm=st.sampled_from(["none", "minmax", "rank_in_group", "zscore_running"]),
    final=st.sampled_from(["none", "minmax", "rank_in_group", "zscore_running"]),
    clip=st.tuples(finite, finite).map(sorted),
)
def test_property_finite_in_finite_out_within_clip(  # noqa: PLR0913, PLR0917 (hypothesis args)
    values: list[float], weights: list[float], method: str, norm: str, final: str, clip: list[float]
) -> None:
    comps = [
        {"id": f"c{i}", "weight": weights[i], "range": [-1e6, 1e6]} for i in range(len(values))
    ]
    d = doc(
        comps,
        method=method,
        normalization={"per_component": norm, "final": final},
        clipping={"min": clip[0], "max": clip[1]},
    )
    refs = [{f"c{i}": v for i, v in enumerate(values)}, {f"c{i}": -v for i, v in enumerate(values)}]
    for s in score(d, *refs):
        assert s.total is not None
        assert math.isfinite(s.total)
        assert clip[0] <= s.total <= clip[1]


def test_product_overflow_is_a_failure_not_inf() -> None:
    d = doc([{"id": "a"}, {"id": "b"}, {"id": "c"}], method="product")
    (s,) = score(d, {"a": 1e300, "b": 1e300, "c": 1e300})
    assert s.total is None
    assert "total_not_finite" in s.flags
    assert (s.status, s.resolution) == (Status.ERROR, Resolution.MASKED)


# --- RW-4 failure matrix --------------------------------------------------------------------

INJECT = [
    ("raise", Status.ERROR),
    ("error", Status.ERROR),
    ("nan", Status.INVALID_OUTPUT),
    ("inf", Status.INVALID_OUTPUT),
    ("99", Status.INVALID_OUTPUT),  # outside the policy range [0.5, 1]
    ("wrong_length", Status.INVALID_OUTPUT),
    ("invalid_input", Status.INVALID_INPUT),
    ("fake_timeout", Status.TIMEOUT),
]


def failure_policy(action: str) -> dict[str, Any]:
    act: dict[str, Any] = {"action": action}
    if action == "fallback":
        act["fallback_value"] = -1.0
    return {"on_timeout": dict(act), "on_error": dict(act), "max_failure_rate": 1}


@pytest.mark.parametrize(("inject", "status"), INJECT)
@pytest.mark.parametrize("action", ["mask", "fallback", "fail_batch"])
def test_injected_failure(inject: str, status: Status, action: str) -> None:
    # The legit range excludes 0, so a silent 0.0 for a failed sample would be visible.
    d = doc(
        [{"id": "a", "range": [0.5, 1]}, {"id": "b", "range": [0.5, 1]}],
        failure=failure_policy(action),
    )
    rt = runtime(d)
    batch = RewardBatch(samples({"a": inject, "b": 0.75}, {"a": 0.5, "b": 1}))
    if action == "fail_batch":
        with pytest.raises(BatchFailedError) as err:
            rt.score(batch, executor=TimeoutExecutor())
        assert err.value.code == "MRL-RWD-144"
        assert err.value.failures_by_status == {str(status): 1}
        assert err.value.sample_count == 2
        return
    bad, good = rt.score(batch, executor=TimeoutExecutor())
    assert good.total == 1.5
    assert good.resolution == Resolution.SCORED
    assert bad.status == status
    assert bad.components[0].status == status
    assert bad.components[0].value is None
    assert bad.components[1].value == 0.75  # the healthy component stays in the breakdown
    assert f"{str(status).lower()}:a" in bad.flags
    assert bad.total != 0.0
    if action == "mask":
        assert (bad.total, bad.resolution) == (None, Resolution.MASKED)
        assert "masked" in bad.flags
    else:
        assert (bad.total, bad.resolution) == (-1.0, Resolution.FALLBACK_APPLIED)
        assert "fallback" in bad.flags


def test_error_fallback_wins_over_timeout_fallback() -> None:
    d = doc(
        [{"id": "a"}, {"id": "b"}],
        failure={
            "on_timeout": {"action": "fallback", "fallback_value": -0.1},
            "on_error": {"action": "fallback", "fallback_value": -0.9},
            "max_failure_rate": 1,
        },
    )
    (s,) = runtime(d).score(
        RewardBatch(samples({"a": "fake_timeout", "b": "raise"})), executor=TimeoutExecutor()
    )
    assert s.total == -0.9
    assert s.status == Status.TIMEOUT  # first failed component in policy order


def test_mask_beats_fallback_when_mixed() -> None:
    d = doc(
        [{"id": "a"}, {"id": "b"}],
        failure={
            "on_timeout": {"action": "mask"},
            "on_error": {"action": "fallback", "fallback_value": -0.9},
            "max_failure_rate": 1,
        },
    )
    (s,) = runtime(d).score(
        RewardBatch(samples({"a": "fake_timeout", "b": "raise"})), executor=TimeoutExecutor()
    )
    assert (s.total, s.resolution) == (None, Resolution.MASKED)


@pytest.mark.parametrize(("failures", "raises"), [(1, False), (2, True)])
def test_max_failure_rate(failures: int, raises: bool) -> None:
    d = doc(
        [{"id": "a"}],
        failure={
            "on_timeout": {"action": "mask"},
            "on_error": {"action": "mask"},
            "max_failure_rate": 0.25,
        },
    )
    refs = [{"a": "raise"}] * failures + [{"a": 1}] * (4 - failures)
    if raises:
        with pytest.raises(BatchFailedError) as err:
            score(d, *refs)
        assert err.value.failures_by_status == {"ERROR": 2}
        assert "2 of 4" in err.value.message
    else:
        out = score(d, *refs)
        assert sum(s.total is None for s in out) == 1


def test_bounded_diagnostics_and_bad_types() -> None:
    class Noisy(Echo):
        spec = ComponentSpec(**{**Echo.spec.__dict__, "name": "noisy"})

        def score(self, batch: RewardBatch) -> list[RewardResult]:
            big = {f"k{i}": "x" * 100 for i in range(100)}
            return [RewardResult(1.0, Status.OK, {**big, "obj": [1, 2]})]  # type: ignore[dict-item]

    reg = registry()
    reg.register(Noisy, namespace="org")
    d = doc([{"id": "a", "use": "org/noisy@1"}])
    (s,) = PolicyRuntime.build(load_policy(d), reg).score(RewardBatch(samples({"a": 1})))
    c = s.components[0]
    assert "diagnostics_truncated" in c.flags
    assert sum(len(k) + len(str(v)) for k, v in c.diagnostics.items()) <= 4096


# --- load-time checks -----------------------------------------------------------------------


def test_bundled_schema_is_the_contract_schema() -> None:
    bundled = ROOT / "python/mrl-rewards/src/mrl_rewards/policy/reward-policy.v1.schema.json"
    assert bundled.read_bytes() == (ROOT / "schemas/reward-policy/v1.schema.json").read_bytes()


def _apply(base: Any, patch: dict[str, Any]) -> Any:
    out = copy.deepcopy(base)

    def walk(pointer: str) -> tuple[Any, str]:
        parts = pointer.strip("/").split("/")
        node = out
        for p in parts[:-1]:
            node = node[int(p)] if isinstance(node, list) else node[p]
        return node, parts[-1]

    for pointer, value in patch.get("set", {}).items():
        node, key = walk(pointer)
        node[int(key) if isinstance(node, list) else key] = value
    for pointer in patch.get("remove", []):
        node, key = walk(pointer)
        del node[int(key) if isinstance(node, list) else key]
    return out


def test_contract_examples_behave() -> None:
    folder = ROOT / "tests/contracts/examples/reward-policy/v1"
    base = json.loads((folder / "valid-math.json").read_text())
    load_policy(base)
    invalid = sorted(folder.glob("invalid-*.json"))
    assert len(invalid) >= 7
    for f in invalid:
        with pytest.raises(RewardError) as err:
            load_policy(_apply(base, json.loads(f.read_text())))
        assert err.value.code == "MRL-RWD-140", f.name


@pytest.mark.parametrize(
    ("change", "code"),
    [
        (lambda d: d["spec"]["components"].append(dict(d["spec"]["components"][0])), "MRL-RWD-140"),
        (
            lambda d: d["spec"].update(
                gates=[{"id": "g", "require": "components.zz.value > 0", "else_value": 0}]
            ),
            "MRL-RWD-140",
        ),
        (
            lambda d: d["spec"].update(
                gates=[{"id": "g", "require": "components.a.value >", "else_value": 0}]
            ),
            "MRL-RWD-141",
        ),
        (lambda d: d["spec"]["components"][0].update(when="components.a.value > 0"), "MRL-RWD-141"),
        (
            lambda d: d["spec"].update(
                shaping=[
                    {"id": "s", "component": "zz", "schedule": [{"from_step": 0, "weight": 1}]}
                ]
            ),
            "MRL-RWD-140",
        ),
        (
            lambda d: d["spec"].update(
                shaping=[
                    {
                        "id": "s",
                        "component": "a",
                        "schedule": [{"from_step": 5, "weight": 1}, {"from_step": 5, "weight": 2}],
                    }
                ]
            ),
            "MRL-RWD-140",
        ),
        (
            lambda d: d["spec"].update(
                clipping={"min": 0, "max": 1},
                gates=[{"id": "g", "require": "true", "else_value": -5}],
            ),
            "MRL-RWD-140",
        ),
        (lambda d: d["spec"].update(clipping={"min": 2, "max": 1}), "MRL-RWD-140"),
        (lambda d: d["spec"]["components"][0].update(range=[1, 1]), "MRL-RWD-140"),
        (
            lambda d: d["spec"]["components"][0]["params"].update(per_sample_timeout_ms=0),
            "MRL-RWD-140",
        ),
        (lambda d: d["metadata"].update(signal="scalar.step"), "MRL-RWD-148"),
    ],
)
def test_policy_rejections(change: Any, code: str) -> None:
    d = doc([{"id": "a"}])
    change(d)
    with pytest.raises(RewardError) as err:
        load_policy(d)
    assert err.value.code == code


def test_build_rejections() -> None:
    d = doc([{"id": "a", "use": "org/echo@2"}])
    with pytest.raises(RewardError) as err:
        runtime(d)
    assert err.value.code == "MRL-RWD-142"
    d = doc([{"id": "a", "params": {"extra": 1}}])
    with pytest.raises(RewardError) as err:
        runtime(d)
    assert err.value.code == "MRL-RWD-143"
