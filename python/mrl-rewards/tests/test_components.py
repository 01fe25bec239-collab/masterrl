"""RW-1: every built-in against its golden case file; determinism; registry."""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from mrl_rewards.policy.model import load_policy
from mrl_rewards.policy.runtime import PolicyRuntime
from mrl_rewards.registry import Registry, builtin_registry, parse_ref
from mrl_rewards.types import RewardBatch, RewardContext, RewardError, RewardSample

GOLDEN = Path(__file__).parent / "golden" / "components"
CASE_FILES = sorted(GOLDEN.glob("*.yaml"))


def text_of(spec: Any) -> str:
    if isinstance(spec, dict):
        return spec["repeat"] * spec["times"] + spec.get("then", "")
    return spec


def cases() -> list[Any]:
    out = []
    for f in CASE_FILES:
        doc = yaml.safe_load(f.read_text(encoding="utf-8"))
        for case in doc["cases"]:
            out.append(
                pytest.param(doc["component"], doc["params"], case, id=f"{f.stem}:{case['name']}")
            )
    return out


def sample(case: dict[str, Any]) -> RewardSample:
    return RewardSample(
        sample_id=case["name"],
        group_id="g",
        prompt="p",
        completion=text_of(case.get("completion", "")),
        completion_token_count=case.get("tokens", 10),
        finish_reason="stop",
        reference=case.get("reference"),
    )


def build(ref: str, params: dict[str, Any]):
    """Through the policy runtime, so params are checked against the component's schema."""
    doc = {
        "apiVersion": "mrl.dev/reward/v1",
        "kind": "RewardPolicy",
        "metadata": {"name": "golden", "signal": "scalar.sequence"},
        "spec": {
            "components": [{"id": "c", "use": ref, "params": params, "weight": 1.0}],
            "aggregation": {"method": "weighted_sum", "where": "platform"},
            "failure": {
                "on_timeout": {"action": "mask"},
                "on_error": {"action": "mask"},
                "max_failure_rate": 1,
            },
            "tests": "x",
        },
    }
    return PolicyRuntime.build(load_policy(doc), builtin_registry()).providers["c"]


def test_every_builtin_has_a_case_file_with_six_cases() -> None:
    names = {f.stem for f in CASE_FILES}
    for row in builtin_registry().index():
        name = parse_ref(str(row["ref"])).name
        assert name in names, f"no golden file for {name}"
        doc = yaml.safe_load((GOLDEN / f"{name}.yaml").read_text(encoding="utf-8"))
        assert len(doc["cases"]) >= 6, name


@pytest.mark.parametrize(("ref", "params", "case"), cases())
def test_golden(ref: str, params: dict[str, Any], case: dict[str, Any]) -> None:
    expect = case["expect"]
    p = {**params, **case.get("params", {})}
    if "setup_error" in expect:
        with pytest.raises(RewardError) as err:
            build(ref, p)
        assert err.value.code == expect["setup_error"]
        return
    provider = build(ref, p)
    s = sample(case)
    (result,) = provider.score(RewardBatch([s]))
    (again,) = provider.score(RewardBatch([s]))
    assert result == again, "deterministic components give identical results"
    assert result.status == expect.get("status", "OK")
    if "value" in expect:
        assert result.value == pytest.approx(expect["value"], abs=1e-12)
    for flag in expect.get("flags", []):
        assert flag in result.flags
    for key, want in expect.get("diagnostics", {}).items():
        assert result.diagnostics[key] == want, key
    lo, hi = provider.spec.value_range
    assert result.value is None or lo <= result.value <= hi


def test_results_are_identical_across_processes() -> None:
    """RW-1: deterministic built-ins reproduce bit-identical values after a process restart."""
    script = """
import importlib.util, json, sys
spec = importlib.util.spec_from_file_location("golden", sys.argv[1])
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)
out = []
for p in g.cases():
    ref, params, case = p.values
    if "setup_error" not in case["expect"]:
        c = g.build(ref, {**params, **case.get("params", {})})
        (r,) = c.score(g.RewardBatch([g.sample(case)]))
        diags = sorted((k, repr(v)) for k, v in r.diagnostics.items())
        out.append([p.id, r.value, str(r.status), r.flags, diags])
print(json.dumps(out))
"""
    runs = [
        subprocess.run(
            [sys.executable, "-c", script, __file__], capture_output=True, text=True, check=True
        ).stdout
        for _ in range(2)
    ]
    assert runs[0] == runs[1]
    assert len(json.loads(runs[0])) > 60


def test_registry_resolution() -> None:
    reg = builtin_registry()
    assert reg.resolve("builtin/math.verify@1").spec.name == "math.verify"
    with pytest.raises(RewardError) as err:
        reg.resolve("builtin/math.verify@2")
    assert err.value.code == "MRL-RWD-142"
    assert "builtin/math.verify@1" in err.value.message
    with pytest.raises(RewardError):
        reg.resolve("math.verify")
    with pytest.raises(RewardError):
        reg.resolve("builtin/nope@1")


def test_registry_rejects_duplicates_and_bad_specs() -> None:
    reg = builtin_registry()
    cls = reg.resolve("builtin/text.regex@1")
    with pytest.raises(RewardError):
        reg.register(cls)

    class Bad(cls):  # type: ignore[valid-type,misc]
        spec = cls.spec.__class__(
            **{**cls.spec.__dict__, "name": "x.bad", "value_range": (1.0, 0.0)}
        )

    with pytest.raises(RewardError) as err:
        Registry().register(Bad)
    assert err.value.code == "MRL-RWD-147"


def test_index_lists_every_builtin_with_digest() -> None:
    reg = builtin_registry()
    rows = reg.index()
    assert len(rows) == 12
    assert all(len(str(r["code_sha256"])) == 64 for r in rows)
    assert reg.digest() == builtin_registry().digest()
    for r in rows:
        assert r["deterministic"] is True
        assert r["tier"] == 1


def test_setup_reads_params_only_from_context() -> None:
    reg = builtin_registry()
    p = reg.create("builtin/length.soft_overlong@1")
    p.setup(RewardContext(params={"max_tokens": 10, "cache_tokens": 5}))
    (r,) = p.score(RewardBatch([sample({"name": "s", "tokens": 9})]))
    assert r.value == pytest.approx(-0.8)
