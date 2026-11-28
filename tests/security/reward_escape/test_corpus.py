"""The escape corpus itself, on any machine: every case has an expectation, every bundle
loads, and the judge catches an escape, a leak and an unexpected ending. (Running the corpus
needs Linux with bubblewrap: see runner.py.)"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from mrl_rewards import server_bundle
from mrl_rewards.policy.runtime import PolicyRuntime

HERE = Path(__file__).resolve().parent


def _runner() -> ModuleType:
    spec = importlib.util.spec_from_file_location("reward_escape_runner", HERE / "runner.py")
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


runner = _runner()


@pytest.fixture
def planned(tmp_path: Path) -> dict[str, Any]:
    argv = [
        "plan",
        "--out",
        str(tmp_path),
        "--tcp",
        "127.0.0.1:9",
        "--abstract",
        "a",
        "--unix",
        "/nonexistent/u.sock",
        "--files",
        "/nonexistent/f",
        "--sentinel-pid",
        "1",
        "--sentinel-marker",
        "m",
    ]
    assert runner.main(argv) == 0
    return json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))


def test_every_case_has_an_expectation_and_every_expectation_a_case() -> None:
    assert runner.coverage_problems() == []
    want = runner.expected()
    assert want["t1"]["control"]["how"] == ["works"]
    assert want["t2"]["control"]["how"] == ["works"]
    assert len(want["t1"]) >= 30
    assert len(want["t2"]) >= 15


def test_every_bundle_loads_with_the_component_it_attacks_with(planned: dict[str, Any]) -> None:
    for case in planned["cases"]:
        root = Path(case["bundle"])
        bundle = server_bundle.load(
            root, user_code=case["user_code"], proof=server_bundle.Proof(True)
        )
        assert bundle.user_code is case["user_code"], case["id"]
        if case["tier"] == 1:  # (Tier-2 components start a jail at setup: Linux only)
            PolicyRuntime.build(bundle.policy, bundle.registry, bundle_dir=bundle.root)
        assert case["version"].startswith("rwdv_escape_")
    assert planned["program"].endswith("mrl-reward-sandboxed")


def _write(root: Path, case_id: str, result: dict[str, Any], ending: str = "ok") -> None:
    (root / "r" / f"{case_id}.json").write_text(json.dumps(result), encoding="utf-8")
    (root / "r" / f"{case_id}.exit.json").write_text(
        json.dumps({"ending": ending}), encoding="utf-8"
    )


def _blocked(case: dict[str, Any], rule: list[str]) -> tuple[dict[str, Any], str]:
    first = rule[0]
    if first == "violation" and case["tier"] == 1:
        return {"started": True, "outcomes": [], "error": "RewardInfraError: gone"}, "violation"
    if first == "died":
        return {"started": True, "outcomes": [], "error": "RewardInfraError: gone"}, "failed"
    value = {"works": 1.0}.get(first, 0.0)
    status = {"timeout": "TIMEOUT", "error": "ERROR", "violation": "ERROR"}.get(first, "OK")
    flags = ["sandbox_violation"] if first == "violation" else []
    outcome = {"value": value, "status": status, "flags": flags, "diagnostics": {}}
    return {"started": True, "outcomes": [outcome] * len(case["samples"]), "error": None}, "ok"


def test_the_judge_passes_a_blocked_corpus_and_catches_escapes_and_leaks(
    planned: dict[str, Any], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    want = runner.expected()
    for case in planned["cases"]:
        result, ending = _blocked(case, want[f"t{case['tier']}"][case["name"]]["how"])
        _write(tmp_path, case["id"], result, ending)
    secrets = tmp_path.parent / f"{tmp_path.name}-secrets.json"
    secrets.write_text(json.dumps({"file": "S3CRET-file", "net": "S3CRET-net"}), encoding="utf-8")
    args = ["judge", "--plan", str(tmp_path / "plan.json"), "--secrets", str(secrets)]
    assert runner.main(args) == 0, capsys.readouterr().err

    escaped = {
        "started": True,
        "error": None,
        "outcomes": [{"value": 1.0, "status": "OK", "flags": [], "diagnostics": {"evidence": "x"}}],
    }
    _write(tmp_path, "t1-tcp_canary", escaped)
    _write(
        tmp_path,
        "t2-spin",
        {
            "started": True,
            "error": None,
            "outcomes": [{"value": 0.0, "status": "OK", "flags": [], "diagnostics": {}}],
        },
    )
    (tmp_path / "r" / "log.txt").write_text("…S3CRET-net…", encoding="utf-8")
    assert runner.main(args) == 1
    err = capsys.readouterr().err
    assert "ESCAPE t1-tcp_canary" in err
    assert "t2-spin: ended ['zero']" in err
    assert "LEAK: the net secret is in r/log.txt" in err
