"""Math answers: exact rational verification and tolerance-based numeric closeness."""

from __future__ import annotations

import math
import re
from fractions import Fraction

from mrl_rewards.builtin._base import (
    EXTRACTOR_SCHEMA,
    Component,
    capped,
    echo,
    expected_answer,
    extract,
)
from mrl_rewards.types import ComponentSpec, RewardResult, RewardSample

# Plain regexes over capped text; none can backtrack badly (no nested or overlapping repeats).
_LAST_NUMBER = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?(?:/\d+)?(?:[eE][-+]?\d{1,3})?(?:\\?%)?")
_DECIMAL = re.compile(r"[-+]?(?:\d+(?:,\d{3})*(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d{1,3})?")
_FRAC = re.compile(r"(-?)\\[dt]?frac\{([^{}]{1,64})\}\{([^{}]{1,64})\}")
_SLASH = re.compile(r"(-?[^/]{1,64})/([^/]{1,64})")
_ASSIGN = re.compile(r"^[A-Za-z]\s*=\s*")
_WRAPPERS = re.compile(r"\\(?:text|mathrm|textbf|mathbf)\{([^{}]{0,64})\}")
_UNIT_WORD = re.compile(r"\s*[A-Za-z][A-Za-z ]{0,30}$")
_STRIP = ("\\left", "\\right", "\\!", "\\,", "\\;", "\\:", "\\$", "$", "^\\circ", "°", "~")


def last_number(text: str) -> str | None:
    found = None
    for m in _LAST_NUMBER.finditer(text):
        found = m.group()
    return found


def clean(answer: str) -> str:
    s = answer.strip()
    for token in _STRIP:
        s = s.replace(token, "")
    s = _WRAPPERS.sub(r"\1", s)
    s = s.replace("\\%", "%").strip().rstrip(".").strip()
    s = _ASSIGN.sub("", s)
    if s.startswith("{") and s.endswith("}") and s.count("{") == 1:
        s = s[1:-1].strip()
    return s


def to_fraction(answer: str) -> Fraction | None:
    """Exact value, or None if it is not a number. A percent is its value: "50%" is 1/2."""
    s = clean(answer)
    scale = Fraction(1, 100) if s.endswith("%") else Fraction(1)
    s = s.removesuffix("%").strip()
    if _DECIMAL.fullmatch(s) is None and _UNIT_WORD.search(s):
        s = _UNIT_WORD.sub("", s).strip()  # "18 dollars" -> "18"
    value: Fraction | None = None
    if m := _FRAC.fullmatch(s):
        value = _ratio(m.group(2), m.group(3))
        if value is not None and m.group(1):
            value = -value
    elif _DECIMAL.fullmatch(s):
        value = Fraction(s.replace(",", ""))
    elif m := _SLASH.fullmatch(s):
        value = _ratio(m.group(1), m.group(2))
    return None if value is None else value * scale


def _ratio(num: str, den: str) -> Fraction | None:
    a, b = num.strip(), den.strip()
    if not (_DECIMAL.fullmatch(a) and _DECIMAL.fullmatch(b)):
        return None
    d = Fraction(b.replace(",", ""))
    return None if d == 0 else Fraction(a.replace(",", "")) / d


def _as_float(v: Fraction) -> float:
    try:
        return float(v)
    except OverflowError:
        return math.inf if v > 0 else -math.inf


def _string_form(answer: str) -> str:
    return " ".join(clean(answer).replace("{", "").replace("}", "").split()).casefold()


def _single_answer(found: list[str]) -> tuple[str | None, tuple[str, ...]]:
    """One answer, or None with a flag when there is none or several different ones."""
    distinct = list(dict.fromkeys(_string_form(a) for a in found))
    if not distinct:
        return None, ("no_answer",)
    if len(distinct) > 1:
        return None, ("multiple_answers",)
    return found[-1], ()


class MathVerify(Component):
    """1 when the extracted answer equals the reference: exact rational value (decimals,
    fractions, \\frac, percents as values so 50% = 0.5, thousands commas, units words) or,
    for non-numbers, the same normalized text. Several different answers score 0."""

    spec = ComponentSpec(
        name="math.verify",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        needs=("reference",),
        description=__doc__ or "",
        param_schema={
            "type": "object",
            "properties": {
                "answer_extractor": {**EXTRACTOR_SCHEMA, "default": "tag:answer"},
                "numeric_tolerance": {
                    "type": "number",
                    "minimum": 0,
                    "default": 0,
                    "description": "Absolute tolerance; 0 means the values must be equal.",
                },
            },
            "additionalProperties": False,
        },
    )

    def configure(self) -> None:
        self.extractor = self.p_str("answer_extractor", "tag:answer")
        self.tolerance = self.p_num("numeric_tolerance", 0.0)

    def score_one(self, sample: RewardSample) -> RewardResult:
        expected = expected_answer(sample.reference)
        if expected is None:
            return RewardResult.invalid_input("no expected answer in the sample's reference")
        text, flags = capped(sample.completion)
        answer, why = _single_answer(extract(text, self.extractor, last_number))
        if answer is None:
            return RewardResult.ok(0.0, {"answer_found": why != ("no_answer",)}, *flags, *why)
        got, want = to_fraction(answer), to_fraction(expected)
        diags: dict[str, bool | int | float | str] = {
            "answer_found": True,
            "parsed_answer": echo(clean(answer)),
            "expected": echo(clean(expected)),
        }
        if got is not None and want is not None:
            close = got == want or (
                self.tolerance > 0 and abs(got - want) <= Fraction(self.tolerance)
            )
            return RewardResult.ok(float(close), {**diags, "method": "number"}, *flags)
        same = _string_form(answer) == _string_form(expected)
        return RewardResult.ok(float(same), {**diags, "method": "text"}, *flags, "parse_fallback")


class NumericClose(Component):
    """1 when the extracted number is within the absolute or relative tolerance of the
    reference number, else 0. A non-numeric answer scores 0 (`parse_failed`)."""

    spec = ComponentSpec(
        name="math.numeric_close",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        needs=("reference",),
        description=__doc__ or "",
        param_schema={
            "type": "object",
            "properties": {
                "answer_extractor": {**EXTRACTOR_SCHEMA, "default": "last_number"},
                "abs_tolerance": {"type": "number", "minimum": 0, "default": 1e-6},
                "rel_tolerance": {"type": "number", "minimum": 0, "default": 0},
            },
            "additionalProperties": False,
        },
    )

    def configure(self) -> None:
        self.extractor = self.p_str("answer_extractor", "last_number")
        self.abs_tol = self.p_num("abs_tolerance", 1e-6)
        self.rel_tol = self.p_num("rel_tolerance", 0.0)

    def score_one(self, sample: RewardSample) -> RewardResult:
        expected = expected_answer(sample.reference)
        want = None if expected is None else to_fraction(expected)
        if want is None:
            return RewardResult.invalid_input("the sample's reference is not a number")
        text, flags = capped(sample.completion)
        answer, why = _single_answer(extract(text, self.extractor, last_number))
        if answer is None:
            return RewardResult.ok(0.0, {"answer_found": why != ("no_answer",)}, *flags, *why)
        got = to_fraction(answer)
        if got is None:
            return RewardResult.ok(0.0, {"parsed_answer": echo(answer)}, *flags, "parse_failed")
        a, b = _as_float(got), _as_float(want)
        close = math.isfinite(a) and math.isclose(a, b, rel_tol=self.rel_tol, abs_tol=self.abs_tol)
        return RewardResult.ok(float(close), {"parsed_answer": a, "expected": b}, *flags)


__all__ = ["MathVerify", "NumericClose", "clean", "last_number", "to_fraction"]
