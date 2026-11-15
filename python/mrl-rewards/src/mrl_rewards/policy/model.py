"""RewardPolicy v1: validate against the contract schema, then parse into typed records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from importlib import resources
from itertools import pairwise
from typing import cast

from mrl_rewards.expr import Expr, parse
from mrl_rewards.types import (
    POLICY_INVALID,
    STEP_SIGNAL_UNSUPPORTED,
    JSONValue,
    RewardError,
)
from mrl_rewards.types.validate import first_error, validator

# Byte-identical copy of schemas/reward-policy/v1.schema.json (a test keeps them equal).
_SCHEMA = cast(
    "dict[str, JSONValue]",
    json.loads(
        resources.files("mrl_rewards.policy")
        .joinpath("reward-policy.v1.schema.json")
        .read_text(encoding="utf-8")
    ),
)
_VALIDATOR = validator(_SCHEMA)

# Common component param handled by the runtime, not passed to the component.
TIMEOUT_PARAM = "per_sample_timeout_ms"


class Aggregation(StrEnum):
    WEIGHTED_SUM = "weighted_sum"
    PRODUCT = "product"
    MIN = "min"


class Normalization(StrEnum):
    NONE = "none"
    MINMAX = "minmax"
    ZSCORE_RUNNING = "zscore_running"
    RANK_IN_GROUP = "rank_in_group"


class FailureAction(StrEnum):
    MASK = "mask"
    FALLBACK = "fallback"
    FAIL_BATCH = "fail_batch"


@dataclass(frozen=True)
class OnFailure:
    action: FailureAction
    fallback_value: float | None = None


@dataclass(frozen=True)
class ComponentConfig:
    id: str
    use: str  # <ns>/<name>@<major>
    params: dict[str, JSONValue]
    weight: float
    range: tuple[float, float] | None
    when: Expr | None
    timeout_ms: int | None


@dataclass(frozen=True)
class Gate:
    id: str
    require: Expr
    else_value: float


@dataclass(frozen=True)
class Shaping:
    id: str
    component: str
    schedule: tuple[tuple[int, float], ...]  # (from_step, weight), increasing steps

    def weight_at(self, step: int) -> float:
        """Piecewise-linear between points; flat before the first and after the last."""
        points = self.schedule
        if step <= points[0][0]:
            return points[0][1]
        for (s0, w0), (s1, w1) in pairwise(points):
            if step <= s1:
                return w0 + (w1 - w0) * (step - s0) / (s1 - s0)
        return points[-1][1]


@dataclass(frozen=True)
class Policy:
    name: str
    signal: str
    components: tuple[ComponentConfig, ...]
    gates: tuple[Gate, ...]
    aggregation: Aggregation
    where: str  # "platform" | "engine"
    per_component: Normalization
    final: Normalization
    clipping: tuple[float, float] | None
    shaping: tuple[Shaping, ...]
    on_timeout: OnFailure
    on_error: OnFailure
    max_failure_rate: float
    document: dict[str, JSONValue]  # the validated source, for hashing


def _bad(why: str) -> RewardError:
    return RewardError(POLICY_INVALID, why)


def _obj(v: JSONValue) -> dict[str, JSONValue]:
    return cast("dict[str, JSONValue]", v) if isinstance(v, dict) else {}


def _list(v: JSONValue) -> list[JSONValue]:
    return cast("list[JSONValue]", v) if isinstance(v, list) else []


def _num(v: JSONValue) -> float:
    return float(cast("float", v))


def _on_failure(v: JSONValue) -> OnFailure:
    o = _obj(v)
    fb = o.get("fallback_value")
    return OnFailure(FailureAction(str(o["action"])), None if fb is None else _num(fb))


def _component(c: dict[str, JSONValue]) -> ComponentConfig:
    cid = str(c["id"])
    rng = None
    if "range" in c:
        lo, hi = (_num(x) for x in _list(c["range"]))
        if not lo < hi:
            raise _bad(f"Component {cid!r} has a range whose low end is not below its high end.")
        rng = (lo, hi)
    params = dict(_obj(c.get("params", {})))
    timeout = params.pop(TIMEOUT_PARAM, None)
    if timeout is not None and (
        isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0
    ):
        raise _bad(f"Component {cid!r}: {TIMEOUT_PARAM} must be a whole number above 0.")
    when = c.get("when")
    return ComponentConfig(
        id=cid,
        use=str(c["use"]),
        params=params,
        weight=_num(c["weight"]),
        range=rng,
        when=None if when is None else parse(str(when), roots=("sample",)),
        timeout_ms=timeout,
    )


def _gate(g: dict[str, JSONValue], ids: set[str]) -> Gate:
    expr = parse(str(g["require"]), roots=("sample", "components"))
    for path in expr.paths():
        if path[0] == "components" and len(path) > 1 and not _names_component(path[1:], ids):
            raise _bad(f"Gate {g['id']!r} refers to a component the policy does not have.")
    return Gate(str(g["id"]), expr, _num(g["else_value"]))


def _shaping(s: dict[str, JSONValue], ids: set[str]) -> Shaping:
    target = str(s["component"])
    if target not in ids:
        raise _bad(f"Shaping {s['id']!r} names component {target!r}, which the policy lacks.")
    points = tuple(
        (int(cast("int", _obj(p)["from_step"])), _num(_obj(p)["weight"]))
        for p in _list(s["schedule"])
    )
    if any(b[0] <= a[0] for a, b in pairwise(points)):
        raise _bad(f"Shaping {s['id']!r}: from_step values must go up.")
    return Shaping(str(s["id"]), target, points)


def _check_clip(policy: Policy) -> None:
    """Declared outcomes (gate else_value, fallbacks) must sit inside the clipping range."""
    if policy.clipping is None:
        return
    lo, hi = policy.clipping
    if lo > hi:
        raise _bad("clipping.min is above clipping.max.")
    declared = [(f"gate {g.id!r} else_value", g.else_value) for g in policy.gates]
    declared += [
        (f"failure.{k}.fallback_value", f.fallback_value)
        for k, f in (("on_timeout", policy.on_timeout), ("on_error", policy.on_error))
        if f.action is FailureAction.FALLBACK and f.fallback_value is not None
    ]
    for what, value in declared:
        if not lo <= value <= hi:
            raise _bad(f"The {what} ({value}) is outside the clipping range [{lo}, {hi}].")


def _unique(kind: str, ids: list[str]) -> None:
    if dupes := sorted({i for i in ids if ids.count(i) > 1}):
        raise _bad(f"More than one {kind} uses the id {dupes[0]!r}.")


def load_policy(document: JSONValue) -> Policy:
    """Validate and parse a RewardPolicy v1 document. Raises RewardError with a plain reason."""
    if err := first_error(_VALIDATOR, document):
        raise _bad(f"The field {err[0]} is not valid: {err[1][:200]}.")
    doc = _obj(document)
    meta, spec = _obj(doc["metadata"]), _obj(doc["spec"])
    signal = str(meta["signal"])
    if signal != "scalar.sequence":
        raise RewardError(
            STEP_SIGNAL_UNSUPPORTED,
            f"The policy asks for signal {signal!r}; this version scores whole responses only "
            "(scalar.sequence).",
        )
    components = [_component(_obj(c)) for c in _list(spec["components"])]
    _unique("component", [c.id for c in components])
    ids = {c.id for c in components}
    gates = [_gate(_obj(g), ids) for g in _list(spec.get("gates", []))]
    _unique("gate", [g.id for g in gates])
    shaping = [_shaping(_obj(s), ids) for s in _list(spec.get("shaping", []))]
    _unique("shaping schedule for a component", [s.component for s in shaping])
    clip = _obj(spec["clipping"]) if "clipping" in spec else None
    agg, norm = _obj(spec["aggregation"]), _obj(spec.get("normalization", {}))
    fail = _obj(spec["failure"])
    policy = Policy(
        name=str(meta["name"]),
        signal=signal,
        components=tuple(components),
        gates=tuple(gates),
        aggregation=Aggregation(str(agg["method"])),
        where=str(agg["where"]),
        per_component=Normalization(str(norm.get("per_component", "none"))),
        final=Normalization(str(norm.get("final", "none"))),
        clipping=None if clip is None else (_num(clip["min"]), _num(clip["max"])),
        shaping=tuple(shaping),
        on_timeout=_on_failure(fail["on_timeout"]),
        on_error=_on_failure(fail["on_error"]),
        max_failure_rate=_num(fail["max_failure_rate"]),
        document=doc,
    )
    _check_clip(policy)
    return policy


def _names_component(rest: tuple[str, ...], ids: set[str]) -> bool:
    return any(".".join(rest[:j]) in ids for j in range(1, len(rest) + 1))


__all__ = [
    "TIMEOUT_PARAM",
    "Aggregation",
    "ComponentConfig",
    "FailureAction",
    "Gate",
    "Normalization",
    "OnFailure",
    "Policy",
    "Shaping",
    "load_policy",
]
