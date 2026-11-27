"""`code.compiles@1` and `code.unit_tests@1`: Tier-2 components that run model-written code.

Both are beta. Every sample runs in its own jail (see `mrl_rewards.code.jail`): nothing from
one sample is visible to the next, and printed text never decides the score. A time or memory
limit is a reward outcome (TIMEOUT / ERROR, resolved by the policy), not a violation; the
seccomp filter killing the program is flagged `sandbox_violation`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

from mrl_rewards.builtin._base import (
    EXTRACTOR_SCHEMA,
    Component,
    bad_param,
    capped,
    echo,
    extract,
    reference_field,
    schema,
)
from mrl_rewards.code.jail import SIGXCPU, Jail, JailLimits, JailResult
from mrl_rewards.types import ComponentSpec, JSONValue, RewardResult, RewardSample, Status

MAX_CODE_CHARS = 64 * 1024
MAX_TESTS_CHARS = 256 * 1024

_LIMIT_SCHEMA: dict[str, JSONValue] = {
    "timeout_s": {
        "type": "number",
        "exclusiveMinimum": 0,
        "maximum": 10,
        "description": "Wall-clock limit per run, in seconds (default 4, at most 10).",
    },
    "cpu_s": {
        "type": "integer",
        "minimum": 1,
        "maximum": 5,
        "description": "CPU time limit per run, in seconds (default 3, at most 5).",
    },
}


def _no_last_number(_text: str) -> str | None:
    return None


class _CodeComponent(Component):
    """Shared setup: the jail, the code extractor and the limits."""

    jail_factory: Callable[[JailLimits], Jail] = staticmethod(Jail.from_environment)

    def configure(self) -> None:
        self.extractor = self.p_str("answer_extractor", "code_fence")
        if self.extractor == "last_number":
            raise bad_param(self.spec.name, "code can't be read with last_number")
        limits = JailLimits(
            wall_s=self.p_num("timeout_s", 4.0), cpu_s=int(self.p_num("cpu_s", 3))
        ).checked()
        self.jail = type(self).jail_factory(limits)

    def code(self, sample: RewardSample) -> tuple[str | None, tuple[str, ...]]:
        text, flags = capped(sample.completion)
        found = extract(text, self.extractor, _no_last_number)
        if not found:
            return None, flags
        code = found[-1]
        if len(code) > MAX_CODE_CHARS:
            return code[:MAX_CODE_CHARS], (*flags, "code_truncated")
        return code, flags

    @staticmethod
    def outcome(run: JailResult) -> RewardResult | None:
        """The failure result for a run that ended without a usable verdict, else None."""
        failures: list[tuple[bool, Status, str, tuple[str, ...]]] = [
            (
                run.sandbox_violation,
                Status.ERROR,
                "the sandbox stopped the code",
                ("sandbox_violation",),
            ),
            (run.timed_out, Status.TIMEOUT, "the code ran past its time limit", ()),
            (
                (run.verdict or {}).get("child_signal") == SIGXCPU,
                Status.TIMEOUT,
                "the code used up its CPU time",
                (),
            ),
            (run.output_flooded, Status.ERROR, "the code printed too much", ("output_flood",)),
            (run.tests_changed, Status.ERROR, "the test files changed", ("tests_changed",)),
        ]
        for hit, status, reason, flags in failures:
            if hit:
                return RewardResult.failed(status, reason, *flags)
        if run.verdict is None:
            ended = f"exit code {run.exit_code}" if run.signal is None else f"signal {run.signal}"
            diag: dict[str, str] = {
                "reason": "the code ended before the checks finished",
                "ended": ended,
                "output": echo(run.output[-300:]),
            }
            return RewardResult.ok(0.0, diag, "no_verdict")
        return None


class CodeCompiles(_CodeComponent):
    """1 when the extracted Python code compiles (it is never run), else 0."""

    spec = ComponentSpec(
        name="code.compiles",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        tier=2,
        beta=True,
        description=__doc__ or "",
        param_schema=schema(
            {"answer_extractor": {**EXTRACTOR_SCHEMA, "default": "code_fence"}, **_LIMIT_SCHEMA}
        ),
    )

    def score_one(self, sample: RewardSample) -> RewardResult:
        code, flags = self.code(sample)
        if code is None:
            return RewardResult.ok(0.0, {"code_found": False}, *flags)
        run = self.jail.run("compile", code)
        failed = self.outcome(run)
        if failed is not None:
            return failed
        verdict = run.verdict or {}
        ok = verdict.get("compiled") is True
        diag: dict[str, str | bool] = {"code_found": True, "compiled": ok}
        if not ok:
            diag["error"] = echo(str(verdict.get("error", "")))
        return RewardResult.ok(float(ok), diag, *flags)


class CodeUnitTests(_CodeComponent):
    """Runs the reference's Python tests against the extracted code. `scoring`:
    `all_or_nothing` (default: 1 only when every test passes) or `fraction` (share passed).
    Tests come from the sample's `reference.verifier.tests`; top-level asserts count as one
    test, `test_*` functions as one each."""

    spec = ComponentSpec(
        name="code.unit_tests",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        tier=2,
        beta=True,
        needs=("reference",),
        description=__doc__ or "",
        param_schema=schema(
            {
                "answer_extractor": {**EXTRACTOR_SCHEMA, "default": "code_fence"},
                "scoring": {"enum": ["all_or_nothing", "fraction"], "default": "all_or_nothing"},
                **_LIMIT_SCHEMA,
            }
        ),
    )

    def configure(self) -> None:
        super().configure()
        self.scoring = self.p_str("scoring", "all_or_nothing")

    def score_one(self, sample: RewardSample) -> RewardResult:  # noqa: PLR0911
        verifier = reference_field(sample.reference, "verifier")
        tests = verifier.get("tests") if isinstance(verifier, dict) else None
        if not isinstance(tests, str) or not tests.strip():
            return RewardResult.invalid_input("the reference has no verifier.tests")
        if len(tests) > MAX_TESTS_CHARS:
            return RewardResult.invalid_input("verifier.tests is larger than 256 KiB")
        code, flags = self.code(sample)
        if code is None:
            return RewardResult.ok(0.0, {"code_found": False}, *flags)
        run = self.jail.run("tests", code, tests)
        failed = self.outcome(run)
        if failed is not None:
            return failed
        verdict = run.verdict or {}
        if verdict.get("compiled") is not True or verdict.get("loaded") is not True:
            why = echo(str(verdict.get("error", "the code did not load")))
            return RewardResult.ok(0.0, {"code_found": True, "error": why}, *flags)
        raw: JSONValue = verdict.get("results")
        items: Sequence[JSONValue] = (
            cast("Sequence[JSONValue]", raw) if isinstance(raw, list) else ()
        )
        rows: list[Sequence[JSONValue]] = []
        for r in items:
            if isinstance(r, list) and len(cast("Sequence[JSONValue]", r)) == 3:
                rows.append(cast("Sequence[JSONValue]", r))
        passed = sum(1 for r in rows if r[1] is True)
        total = len(items)
        if total == 0:
            return RewardResult.ok(0.0, {"code_found": True, "tests": 0}, *flags)
        value = passed / total if self.scoring == "fraction" else float(passed == total)
        first_fail = next((r for r in rows if r[1] is not True), None)
        diag: dict[str, str | int | bool] = {"code_found": True, "passed": passed, "tests": total}
        if first_fail is not None:
            diag["first_failure"] = echo(f"{first_fail[0]}: {first_fail[2]}")
        return RewardResult.ok(value, diag, *flags)


CODE_COMPONENTS: tuple[type[Component], ...] = (CodeCompiles, CodeUnitTests)

__all__ = ["CODE_COMPONENTS", "CodeCompiles", "CodeUnitTests"]
