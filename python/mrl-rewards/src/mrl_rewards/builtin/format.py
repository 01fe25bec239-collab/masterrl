"""Format and structure checks: think/answer tags, JSON Schema validity, full-text regex."""

from __future__ import annotations

import json
from typing import cast

from jsonschema import Draft202012Validator, SchemaError

from mrl_rewards.builtin._base import (
    EXTRACTOR_SCHEMA,
    Component,
    Pattern,
    bad_param,
    capped,
    compile_re2,
    echo,
    extract,
    schema,
)
from mrl_rewards.builtin.math import last_number
from mrl_rewards.builtin.text import REGEX_PARAMS
from mrl_rewards.types import ComponentSpec, JSONValue, RewardResult, RewardSample
from mrl_rewards.types.validate import first_error, validator

_TAG: dict[str, JSONValue] = {"type": "string", "pattern": "^[A-Za-z][A-Za-z0-9_-]{0,31}$"}


class ThinkAnswerTags(Component):
    """1 when the completion has exactly one <think>…</think> followed by exactly one
    <answer>…</answer> and no other text, else 0. Diagnostics: answer_tag_found,
    think_tag_found, answer_count, text_outside_tags."""

    spec = ComponentSpec(
        name="format.think_answer_tags",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        description=__doc__ or "",
        param_schema=schema(
            {
                "think_tag": {**_TAG, "default": "think"},
                "answer_tag": {**_TAG, "default": "answer"},
                "require_think": {"type": "boolean", "default": True},
                "allow_text_outside": {"type": "boolean", "default": False},
            }
        ),
    )

    def configure(self) -> None:
        self.think = self.p_str("think_tag", "think")
        self.answer = self.p_str("answer_tag", "answer")
        self.require_think = self.p_bool("require_think", True)
        self.allow_outside = self.p_bool("allow_text_outside", False)

    def score_one(self, sample: RewardSample) -> RewardResult:
        text, flags = capped(sample.completion)
        a_open, a_close = f"<{self.answer}>", f"</{self.answer}>"
        t_open, t_close = f"<{self.think}>", f"</{self.think}>"
        answer_count = text.count(a_open)
        think_count = text.count(t_open)
        one_answer = answer_count == 1 and text.count(a_close) == 1
        one_think = think_count == 1 and text.count(t_close) == 1
        a_start, a_end = text.find(a_open), text.find(a_close)
        t_start, t_end = text.find(t_open), text.find(t_close)
        ok_answer = one_answer and a_start < a_end
        ok_think = one_think and t_start < t_end and (not ok_answer or t_end < a_start)
        spans = [(a_start, a_end + len(a_close))] if ok_answer else []
        spans += [(t_start, t_end + len(t_close))] if ok_think else []
        rest = text
        for start, end in sorted(spans, reverse=True):
            rest = rest[:start] + rest[end:]
        outside = bool(rest.strip())
        ok = (
            ok_answer
            and (ok_think or not self.require_think)
            and (self.allow_outside or not outside)
        )
        diags: dict[str, bool | int | float | str] = {
            "answer_tag_found": answer_count >= 1 and a_end > a_start >= 0,
            "think_tag_found": think_count >= 1 and t_end > t_start >= 0,
            "answer_count": answer_count,
            "text_outside_tags": outside,
        }
        return RewardResult.ok(float(ok), diags, *flags)


def _remote_refs(node: JSONValue) -> list[str]:
    found: list[str] = []
    stack: list[JSONValue] = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in ("$ref", "$dynamicRef") and isinstance(v, str) and not v.startswith("#"):
                    found.append(v)
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return found


class JsonSchema(Component):
    """1 when the extracted text is JSON that the given schema (2020-12, local $ref only)
    accepts, else 0. Diagnostics: valid, error."""

    spec = ComponentSpec(
        name="format.json_schema",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        description=__doc__ or "",
        param_schema=schema(
            {
                "schema": {"type": "object"},
                "answer_extractor": {**EXTRACTOR_SCHEMA, "default": "full"},
                "max_bytes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 1048576,
                    "default": 65536,
                },
            },
            ("schema",),
        ),
    )

    def configure(self) -> None:
        target = self.p_obj("schema")
        if refs := _remote_refs(target):
            raise bad_param(self.spec.name, f"the schema refers to {refs[0]!r}; only #… refs work")
        try:
            Draft202012Validator.check_schema(target)
        except SchemaError as e:
            raise bad_param(self.spec.name, f"the schema is not valid ({e.message[:160]})") from e
        self.validator = validator(target, local_refs_only=True)
        self.extractor = self.p_str("answer_extractor", "full")
        self.max_bytes = int(self.p_num("max_bytes", 65536))

    def score_one(self, sample: RewardSample) -> RewardResult:
        text, flags = capped(sample.completion)
        found = extract(text, self.extractor, last_number)
        if len(found) != 1:
            why = "no_answer" if not found else "multiple_answers"
            return RewardResult.ok(0.0, {"valid": False, "error": why}, *flags, why)
        body = found[0].strip()
        if len(body.encode()) > self.max_bytes:
            return RewardResult.ok(0.0, {"valid": False, "error": "too_large"}, *flags)
        try:
            data = cast("JSONValue", json.loads(body))
        except (ValueError, RecursionError) as e:
            return RewardResult.ok(0.0, {"valid": False, "error": echo(f"not JSON: {e}")}, *flags)
        try:
            error = first_error(self.validator, data)
        except RecursionError:
            return RewardResult.ok(0.0, {"valid": False, "error": "nested too deeply"}, *flags)
        if error is not None:
            where, why = error
            return RewardResult.ok(0.0, {"valid": False, "error": echo(f"{where}: {why}")}, *flags)
        return RewardResult.ok(1.0, {"valid": True}, *flags)


class RegexFull(Component):
    """1 when the whole completion (trimmed unless `strip` is false) matches the RE2
    pattern, else 0."""

    spec = ComponentSpec(
        name="format.regex_full",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        description=__doc__ or "",
        param_schema=schema(
            {**REGEX_PARAMS, "strip": {"type": "boolean", "default": True}}, ("pattern",)
        ),
    )

    def configure(self) -> None:
        self.pattern: Pattern = compile_re2(
            self.p_str("pattern", ""),
            ignore_case=self.p_bool("ignore_case", False),
            dot_nl=self.p_bool("dot_matches_newline", False),
            component=self.spec.name,
        )
        self.strip = self.p_bool("strip", True)

    def score_one(self, sample: RewardSample) -> RewardResult:
        text, flags = capped(sample.completion)
        if self.strip:
            text = text.strip()
        matched = self.pattern.fullmatch(text) is not None
        return RewardResult.ok(float(matched), {"matched": matched}, *flags)


__all__ = ["JsonSchema", "RegexFull", "ThinkAnswerTags"]
