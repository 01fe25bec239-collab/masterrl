"""Shared pieces of the built-in components: base class, answer extraction, text rules, RE2."""

from __future__ import annotations

import abc
import unicodedata
from collections.abc import Callable
from typing import Protocol, cast

import re2  # pyright: ignore[reportMissingTypeStubs]

from mrl_rewards.types import (
    PARAMS_INVALID,
    ComponentSpec,
    JSONValue,
    RewardBatch,
    RewardContext,
    RewardError,
    RewardResult,
    RewardSample,
)

# Longest completion a built-in reads; longer text is cut and flagged `truncated_input`.
MAX_INPUT_CHARS = 256 * 1024
# Longest text echoed into diagnostics.
MAX_ECHO = 200

EXTRACTORS = ("full", "tag:<name>", "boxed", "last_number", "code_fence")
EXTRACTOR_SCHEMA: dict[str, JSONValue] = {
    "type": "string",
    "pattern": "^(full|boxed|last_number|code_fence|tag:[A-Za-z][A-Za-z0-9_-]{0,31})$",
    "description": "Where the answer is: full text, tag:<name>, boxed, last_number or code_fence.",
}
NORMALIZE_SCHEMA: dict[str, JSONValue] = {
    "type": "object",
    "properties": {
        "case": {"type": "boolean", "description": "Ignore upper/lower case."},
        "whitespace": {"type": "boolean", "description": "Trim and collapse spaces."},
        "nfkc": {"type": "boolean", "description": "Unicode NFKC normalization."},
    },
    "additionalProperties": False,
}


def schema(
    properties: dict[str, JSONValue], required: tuple[str, ...] = ()
) -> dict[str, JSONValue]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


class Component(abc.ABC):
    """A built-in: scores each sample on its own. Params are already schema-checked."""

    spec: ComponentSpec

    def __init__(self) -> None:
        self.params: dict[str, JSONValue] = {}

    def setup(self, ctx: RewardContext) -> None:
        self.params = dict(ctx.params)
        self.configure()

    def configure(self) -> None:  # noqa: B027 (optional hook)
        """Read and pre-compile params. Raise RewardError(PARAMS_INVALID) on a bad value."""

    def score(self, batch: RewardBatch) -> list[RewardResult]:
        return [self.score_one(s) for s in batch.samples]

    @abc.abstractmethod
    def score_one(self, sample: RewardSample) -> RewardResult: ...

    def teardown(self) -> None:  # noqa: B027 (optional hook)
        """Built-ins hold no resources."""

    # Typed param access (values were validated against spec.param_schema).
    def p_str(self, key: str, default: str) -> str:
        return str(self.params.get(key, default))

    def p_bool(self, key: str, default: bool) -> bool:
        return bool(self.params.get(key, default))

    def p_num(self, key: str, default: float) -> float:
        return float(cast("float", self.params.get(key, default)))

    def p_obj(self, key: str) -> dict[str, JSONValue]:
        v = self.params.get(key)
        return cast("dict[str, JSONValue]", v) if isinstance(v, dict) else {}


def bad_param(component: str, why: str) -> RewardError:
    return RewardError(PARAMS_INVALID, f"Component {component}: {why}.")


def capped(text: str) -> tuple[str, tuple[str, ...]]:
    if len(text) <= MAX_INPUT_CHARS:
        return text, ()
    return text[:MAX_INPUT_CHARS], ("truncated_input",)


def echo(text: str) -> str:
    return text if len(text) <= MAX_ECHO else text[:MAX_ECHO] + "…"


def expected_answer(reference: JSONValue) -> str | None:
    """The expected answer: a bare string/number reference, or its `answer`/`reference` key."""
    if isinstance(reference, bool) or reference is None:
        return None
    if isinstance(reference, str):
        return reference
    if isinstance(reference, int | float):
        return repr(reference)
    if isinstance(reference, dict):
        for key in ("answer", "reference"):
            if key in reference:
                return expected_answer(reference[key])
    return None


def reference_field(reference: JSONValue, key: str) -> JSONValue:
    return cast("dict[str, JSONValue]", reference).get(key) if isinstance(reference, dict) else None


def tag_contents(text: str, tag: str) -> list[str]:
    """Contents of every <tag>…</tag>, outermost-first, linear time (no regex)."""
    open_t, close_t = f"<{tag}>", f"</{tag}>"
    out: list[str] = []
    i = 0
    while (start := text.find(open_t, i)) >= 0:
        end = text.find(close_t, start + len(open_t))
        if end < 0:
            break
        out.append(text[start + len(open_t) : end])
        i = end + len(close_t)
    return out


def boxed_contents(text: str) -> list[str]:
    """Contents of every \\boxed{…} with balanced braces."""
    out: list[str] = []
    i = 0
    while (start := text.find("\\boxed{", i)) >= 0:
        j, depth = start + len("\\boxed{"), 1
        while j < len(text) and depth:
            depth += {"{": 1, "}": -1}.get(text[j], 0)
            j += 1
        if depth:
            break
        out.append(text[start + len("\\boxed{") : j - 1])
        i = j
    return out


def fence_contents(text: str) -> list[str]:
    """Contents of every ``` fenced block (the language word after the fence is dropped)."""
    parts = text.split("```")
    out: list[str] = []
    for block in parts[1:-1:2]:
        first, _, rest = block.partition("\n")
        out.append(rest if first.strip().isidentifier() or not first.strip() else block)
    return out


def extract(text: str, how: str, last_number: Callable[[str], str | None]) -> list[str]:
    if how == "full":
        return [text]
    if how == "boxed":
        return boxed_contents(text)
    if how == "code_fence":
        return fence_contents(text)
    if how == "last_number":
        n = last_number(text)
        return [] if n is None else [n]
    return tag_contents(text, how.removeprefix("tag:"))


def normalize_text(text: str, rules: dict[str, JSONValue]) -> str:
    if rules.get("nfkc", True):
        text = unicodedata.normalize("NFKC", text)
    if rules.get("whitespace", True):
        text = " ".join(text.split())
    if rules.get("case", False):
        text = text.casefold()
    return text


class Pattern(Protocol):
    def search(self, text: str) -> object | None: ...
    def fullmatch(self, text: str) -> object | None: ...


def compile_re2(pattern: str, *, ignore_case: bool, dot_nl: bool, component: str) -> Pattern:
    """Linear-time regex (RE2): no backtracking, so model output can't stall it."""
    opts = re2.Options()
    opts.log_errors = False
    opts.max_mem = 8 << 20
    opts.case_sensitive = not ignore_case
    opts.dot_nl = dot_nl
    try:
        return cast("Pattern", re2.compile(pattern, opts))  # pyright: ignore[reportUnknownMemberType]
    except Exception as e:  # re2.error is untyped; compile raises nothing else
        raise bad_param(component, f"the pattern can't be used ({e})") from e


__all__ = [
    "EXTRACTORS",
    "EXTRACTOR_SCHEMA",
    "MAX_INPUT_CHARS",
    "NORMALIZE_SCHEMA",
    "Component",
    "Pattern",
    "bad_param",
    "boxed_contents",
    "capped",
    "compile_re2",
    "echo",
    "expected_answer",
    "extract",
    "fence_contents",
    "normalize_text",
    "reference_field",
    "schema",
    "tag_contents",
]
