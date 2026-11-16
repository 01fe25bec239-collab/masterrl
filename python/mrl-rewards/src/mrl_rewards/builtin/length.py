"""Length shaping from the harness's token count (components never tokenize)."""

from __future__ import annotations

from mrl_rewards.builtin._base import Component, bad_param, schema
from mrl_rewards.types import ComponentSpec, JSONValue, RewardResult, RewardSample

_TOKENS: dict[str, JSONValue] = {"type": "integer", "minimum": 0, "maximum": 10_000_000}


class PenaltyLinear(Component):
    """0 up to `free_tokens`, then falls linearly to -1 at `max_tokens` and stays there."""

    spec = ComponentSpec(
        name="length.penalty_linear",
        version="1.0.0",
        value_range=(-1.0, 0.0),
        deterministic=True,
        needs=("completion_token_count",),
        description=__doc__ or "",
        param_schema=schema(
            {"max_tokens": {**_TOKENS, "minimum": 1}, "free_tokens": {**_TOKENS, "default": 0}},
            ("max_tokens",),
        ),
    )

    def configure(self) -> None:
        self.max = int(self.p_num("max_tokens", 1))
        self.free = int(self.p_num("free_tokens", 0))
        if self.free >= self.max:
            raise bad_param(self.spec.name, "free_tokens must be below max_tokens")

    def score_one(self, sample: RewardSample) -> RewardResult:
        n = sample.completion_token_count
        over = min(max(n - self.free, 0), self.max - self.free)
        return RewardResult.ok(-over / (self.max - self.free), {"tokens": n})


class SoftOverlong(Component):
    """DAPO soft overlong penalty: 0 up to max_tokens - cache_tokens, then falls linearly to
    -1 at max_tokens; -1 beyond it."""

    spec = ComponentSpec(
        name="length.soft_overlong",
        version="1.0.0",
        value_range=(-1.0, 0.0),
        deterministic=True,
        needs=("completion_token_count",),
        description=__doc__ or "",
        param_schema=schema(
            {"max_tokens": {**_TOKENS, "minimum": 1}, "cache_tokens": {**_TOKENS, "minimum": 1}},
            ("max_tokens", "cache_tokens"),
        ),
    )

    def configure(self) -> None:
        self.max = int(self.p_num("max_tokens", 1))
        self.cache = int(self.p_num("cache_tokens", 1))
        if self.cache > self.max:
            raise bad_param(self.spec.name, "cache_tokens must not be above max_tokens")

    def score_one(self, sample: RewardSample) -> RewardResult:
        n = sample.completion_token_count
        start = self.max - self.cache
        if n <= start:
            value = 0.0
        elif n <= self.max:
            value = (start - n) / self.cache
        else:
            value = -1.0
        flags = ("overlong",) if n > self.max else ()
        return RewardResult.ok(value, {"tokens": n}, *flags)


class TargetBand(Component):
    """1 inside [min_tokens, max_tokens]; outside it falls linearly to 0 over `slack_tokens`
    (0 slack: straight to 0)."""

    spec = ComponentSpec(
        name="length.target_band",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        needs=("completion_token_count",),
        description=__doc__ or "",
        param_schema=schema(
            {
                "min_tokens": _TOKENS,
                "max_tokens": _TOKENS,
                "slack_tokens": {**_TOKENS, "default": 0},
            },
            ("min_tokens", "max_tokens"),
        ),
    )

    def configure(self) -> None:
        self.lo = int(self.p_num("min_tokens", 0))
        self.hi = int(self.p_num("max_tokens", 0))
        self.slack = int(self.p_num("slack_tokens", 0))
        if self.lo > self.hi:
            raise bad_param(self.spec.name, "min_tokens must not be above max_tokens")

    def score_one(self, sample: RewardSample) -> RewardResult:
        n = sample.completion_token_count
        distance = max(self.lo - n, n - self.hi, 0)
        value = 1.0 if distance == 0 else max(0.0, 1 - distance / self.slack) if self.slack else 0.0
        return RewardResult.ok(value, {"tokens": n, "distance": distance})


__all__ = ["PenaltyLinear", "SoftOverlong", "TargetBand"]
