"""Text checks: exact match, contains, regex search."""

from __future__ import annotations

from typing import cast

from mrl_rewards.builtin._base import (
    EXTRACTOR_SCHEMA,
    NORMALIZE_SCHEMA,
    Component,
    Pattern,
    capped,
    compile_re2,
    echo,
    expected_answer,
    extract,
    normalize_text,
    schema,
)
from mrl_rewards.builtin.math import last_number
from mrl_rewards.types import ComponentSpec, JSONValue, RewardResult, RewardSample


class ExactMatch(Component):
    """1 when the extracted answer equals the reference after normalization (NFKC and
    whitespace on by default, case optional), else 0. Several different answers score 0."""

    spec = ComponentSpec(
        name="text.exact_match",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        needs=("reference",),
        description=__doc__ or "",
        param_schema=schema(
            {
                "answer_extractor": {**EXTRACTOR_SCHEMA, "default": "full"},
                "normalize": NORMALIZE_SCHEMA,
            }
        ),
    )

    def configure(self) -> None:
        self.extractor = self.p_str("answer_extractor", "full")
        self.rules = self.p_obj("normalize")

    def score_one(self, sample: RewardSample) -> RewardResult:
        expected = expected_answer(sample.reference)
        if expected is None:
            return RewardResult.invalid_input("no expected answer in the sample's reference")
        text, flags = capped(sample.completion)
        found = {normalize_text(a, self.rules) for a in extract(text, self.extractor, last_number)}
        if len(found) != 1:
            why = "no_answer" if not found else "multiple_answers"
            return RewardResult.ok(0.0, {"answer_found": bool(found)}, *flags, why)
        got = found.pop()
        want = normalize_text(expected, self.rules)
        return RewardResult.ok(
            float(got == want), {"answer": echo(got), "expected": echo(want)}, *flags
        )


class Contains(Component):
    """1 when the completion contains the given texts (`any` or `all` of them; the reference
    answer when none are given), after normalization, else 0."""

    spec = ComponentSpec(
        name="text.contains",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        description=__doc__ or "",
        param_schema=schema(
            {
                "texts": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1, "maxLength": 4096},
                    "maxItems": 64,
                },
                "mode": {"enum": ["any", "all"], "default": "any"},
                "normalize": NORMALIZE_SCHEMA,
            }
        ),
    )

    def configure(self) -> None:
        self.rules = self.p_obj("normalize")
        texts = cast("list[str]", self.params.get("texts", []))
        self.texts = [normalize_text(t, self.rules) for t in texts]
        self.all = self.p_str("mode", "any") == "all"

    def score_one(self, sample: RewardSample) -> RewardResult:
        needles = self.texts
        if not needles:
            expected = expected_answer(sample.reference)
            if expected is None:
                return RewardResult.invalid_input("no texts given and no reference answer")
            needles = [normalize_text(expected, self.rules)]
        text, flags = capped(sample.completion)
        hay = normalize_text(text, self.rules)
        hits = sum(n in hay for n in needles)
        ok = hits == len(needles) if self.all else hits > 0
        return RewardResult.ok(float(ok), {"matched": hits, "needed": len(needles)}, *flags)


REGEX_PARAMS: dict[str, JSONValue] = {
    "pattern": {"type": "string", "minLength": 1, "maxLength": 4096},
    "ignore_case": {"type": "boolean", "default": False},
    "dot_matches_newline": {"type": "boolean", "default": False},
}


class Regex(Component):
    """1 when the RE2 pattern is found anywhere in the completion, else 0 (`invert` swaps)."""

    spec = ComponentSpec(
        name="text.regex",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        description=__doc__ or "",
        param_schema=schema(
            {**REGEX_PARAMS, "invert": {"type": "boolean", "default": False}}, ("pattern",)
        ),
    )

    def configure(self) -> None:
        self.pattern: Pattern = compile_re2(
            self.p_str("pattern", ""),
            ignore_case=self.p_bool("ignore_case", False),
            dot_nl=self.p_bool("dot_matches_newline", False),
            component=self.spec.name,
        )
        self.invert = self.p_bool("invert", False)

    def score_one(self, sample: RewardSample) -> RewardResult:
        text, flags = capped(sample.completion)
        found = self.pattern.search(text) is not None
        return RewardResult.ok(float(found != self.invert), {"found": found}, *flags)


__all__ = ["REGEX_PARAMS", "Contains", "ExactMatch", "Regex"]
