"""The reward escape suite (SB-1, SB-2): builds the corpus, scores it and judges it.

The agent side lives in `mrl-agent` (`sandbox::profiles::reward::escape`, an ignored Linux
test): it sets up the canaries (a secret file in folders the sandbox must not see, a TCP and
two Unix listeners that must never get a connection, a secret in the agent's environment,
a host sentinel process), then for every case starts the real `mrl-reward-sandboxed` in
the real `reward` (T1) or `code_exec` (T2) sandbox with bubblewrap, and calls:

    runner.py plan  --out DIR --tcp HOST:PORT... --abstract NAME --unix PATH --files PATH...
                    --sentinel-pid PID --sentinel-marker TEXT
    runner.py score --plan DIR/plan.json --case ID --socket PATH --nonce NONCE
    runner.py judge --plan DIR/plan.json --secrets FILE

`plan` writes one bundle per case and `plan.json` (the program, the folders it must read and
the cases). `score` talks to one running service with the RewardClient and writes
`DIR/r/<case>.json`; the driver adds `DIR/r/<case>.exit.json` (how the service ended).
`judge` compares every case with expected.yaml, looks for the secrets in every file under
DIR and exits 1 on any escape, leak or unexpected ending.

The secrets never go into a bundle: bundles only hold where to attack.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, cast

import yaml

HERE = Path(__file__).resolve().parent
T2_TESTS = "def test_answer():\n    assert solve() == 42\n"


def _policy(component: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "mrl.dev/reward/v1",
        "kind": "RewardPolicy",
        "metadata": {"name": "escape", "signal": "scalar.sequence"},
        "spec": {
            "components": [{"id": "probe", "weight": 1.0, **component}],
            "aggregation": {"method": "weighted_sum", "where": "platform"},
            "failure": {
                "on_timeout": {"action": "mask"},
                "on_error": {"action": "mask"},
                "max_failure_rate": 1.0,
            },
            "tests": "tests.yaml",
        },
    }


def _version(case_id: str) -> str:
    return "rwdv_escape_" + case_id.replace("-", "_")


def expected() -> dict[str, dict[str, dict[str, Any]]]:
    """expected.yaml."""
    return cast(
        "dict[str, dict[str, dict[str, Any]]]",
        yaml.safe_load((HERE / "expected.yaml").read_text(encoding="utf-8")),
    )


def probes() -> list[str]:
    """The T1 probe names (from the corpus module, without importing it as a package)."""
    spec = importlib.util.spec_from_file_location("reward_escape_t1", HERE / "t1" / "attacks.py")
    if spec is None or spec.loader is None:
        raise SystemExit("t1/attacks.py can't be loaded")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return sorted(cast("dict[str, Any]", mod.PROBES))


def completions() -> dict[str, dict[str, Any]]:
    """The T2 completions."""
    doc = yaml.safe_load((HERE / "t2" / "completions.yaml").read_text(encoding="utf-8"))
    return cast("dict[str, dict[str, Any]]", doc["completions"])


def coverage_problems() -> list[str]:
    """Cases without an expectation and expectations without a case."""
    want = expected()
    out: list[str] = []
    for tier, names in (("t1", set(probes())), ("t2", set(completions()))):
        listed = set(want.get(tier, {}))
        out += [f"{tier}/{n}: no expectation in expected.yaml" for n in sorted(names - listed)]
        out += [f"{tier}/{n}: expected but not in the corpus" for n in sorted(listed - names)]
    return out


def plan(args: argparse.Namespace) -> dict[str, Any]:
    """Writes the bundles and plan.json."""
    from mrl_rewards import server_bundle  # noqa: PLC0415 (only the plan step needs it)

    out: Path = args.out
    canary = {
        "tcp": list(args.tcp),
        "abstract": args.abstract,
        "unix": args.unix,
        "files": list(args.files),
        "sentinel_pid": args.sentinel_pid,
        "sentinel_marker": args.sentinel_marker,
    }
    # Only the corpus's own files (never a stray __pycache__) go into the T1 bundles.
    t1_code = out / "code-t1"
    t1_code.mkdir(parents=True, exist_ok=True)
    for f in ("components.yaml", "attacks.py"):
        (t1_code / f).write_bytes((HERE / "t1" / f).read_bytes())
    cases: list[dict[str, Any]] = []
    for name in probes():
        case_id = f"t1-{name}"
        bundle = server_bundle.assemble(
            _policy({"use": "org/escape.probe@1", "params": {"attack": name, "canary": canary}}),
            t1_code,
            out / "b" / case_id,
        )
        cases.append(
            {
                "id": case_id,
                "version": _version(case_id),
                "tier": 1,
                "name": name,
                "bundle": str(bundle),
                "user_code": True,
                "samples": [{"prompt": "p", "completion": "c"}],
            }
        )
    t2_canary = {
        **canary,
        "supervisor_paths": [str(out / "b" / "t1-control" / "policy.json"), str(out / "plan.json")],
    }
    for name, c in completions().items():
        case_id = f"t2-{name}"
        bundle = server_bundle.assemble(
            _policy({"use": "builtin/code.unit_tests@1", "params": {"timeout_s": 4, "cpu_s": 3}}),
            None,
            out / "b" / case_id,
        )
        code = f"CANARY = {json.dumps(t2_canary)}\n{c['code']}"
        sample = {
            "prompt": "Write solve().",
            "completion": f"```python\n{code}```",
            "reference": {"verifier": {"tests": T2_TESTS}},
        }
        cases.append(
            {
                "id": case_id,
                "version": _version(case_id),
                "tier": 2,
                "name": name,
                "bundle": str(bundle),
                "user_code": False,
                "samples": [sample] * int(c.get("samples", 1)),
            }
        )
    prefix = Path(sys.prefix)
    doc = {
        "program": str(prefix / "bin" / "mrl-reward-sandboxed"),
        "read_paths": [str(p) for p in server_bundle.python_paths()],
        "cases": cases,
    }
    (out / "r").mkdir(parents=True, exist_ok=True)
    (out / "plan.json").write_text(json.dumps(doc, indent=1), encoding="utf-8")
    return doc


def score(args: argparse.Namespace) -> dict[str, Any]:
    """Scores one case against its running service; writes r/<case>.json."""
    from mrl_runtime.rewards_client import RewardClient, Sample  # noqa: PLC0415

    doc = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    case = next(c for c in doc["cases"] if c["id"] == args.case)
    result: dict[str, Any] = {"id": case["id"], "started": False, "outcomes": [], "error": None}
    client = RewardClient(
        args.socket,
        nonce=args.nonce,
        reward_version_id=case["version"],
        attempt_id="escape",
        timeout_s=30.0,
        health_wait_s=2.0,
    )
    try:
        client.describe()
        result["started"] = True
        for s in case["samples"]:
            sample = Sample(
                prompt=s["prompt"],
                completion=s["completion"],
                group_id="g",
                reference=s.get("reference"),
            )
            (o,) = client.score([sample])
            result["outcomes"].append(
                {
                    "value": o.components.get("probe"),
                    "total": o.value,
                    "resolution": o.resolution,
                    "status": o.statuses.get("probe"),
                    "flags": list(o.flags),
                    "diagnostics": o.diagnostics.get("probe", {}),
                }
            )
    except Exception as e:  # the service died or refused: recorded, judged later
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        client.close()
    path = Path(args.plan).parent / "r" / f"{case['id']}.json"
    path.write_text(json.dumps(result, indent=1), encoding="utf-8")
    return result


def how(case: dict[str, Any], result: dict[str, Any], ending: dict[str, Any]) -> list[str]:
    """How each sample of a case ended (see expected.yaml)."""
    if not result.get("started"):
        return ["refused"]
    out: list[str] = []
    for o in result["outcomes"]:
        reason = str(o.get("diagnostics", {}).get("reason", ""))
        if o.get("value") == 1.0:
            out.append("works")
        elif "sandbox_violation" in o.get("flags", []) or "sandbox stopped" in reason:
            out.append("violation")
        elif o.get("status") == "TIMEOUT":
            out.append("timeout")
        elif o.get("status") not in (None, "OK"):
            out.append("error")
        else:
            out.append("zero")
    if result.get("error") or len(out) < len(case["samples"]):
        out.append("violation" if ending.get("ending") == "violation" else "died")
    return out


def judge(args: argparse.Namespace) -> list[str]:
    """Every problem found; empty when the corpus is fully blocked."""
    plan_path = Path(args.plan)
    root = plan_path.parent
    doc = json.loads(plan_path.read_text(encoding="utf-8"))
    want = expected()
    problems = coverage_problems()
    rows: list[str] = []
    for case in doc["cases"]:
        tier = f"t{case['tier']}"
        rule = want.get(tier, {}).get(case["name"], {})
        allowed = set(rule.get("how", []))
        r = root / "r" / f"{case['id']}.json"
        e = root / "r" / f"{case['id']}.exit.json"
        result: dict[str, Any] = (
            json.loads(r.read_text(encoding="utf-8")) if r.is_file() else {"started": False}
        )
        ending = json.loads(e.read_text(encoding="utf-8")) if e.is_file() else {}
        got = how(case, result, ending)
        rows.append(f"{case['id']:<34} {','.join(got):<18} {ending.get('ending', '?')}")
        if "works" in got and "works" not in allowed:
            evidence = [o.get("diagnostics", {}) for o in result.get("outcomes", [])]
            problems.append(f"ESCAPE {case['id']}: {rule.get('why', '')}; evidence {evidence}")
        elif got == ["refused"] and "refused" not in allowed:
            problems.append(
                f"{case['id']}: the service didn't start: {ending.get('log', '')[-1500:]}"
            )
        elif not set(got) <= allowed:
            want_how = sorted(allowed)
            problems.append(
                f"{case['id']}: ended {got}, expected {want_how}: {result.get('error')}"
            )
    secrets = json.loads(Path(args.secrets).read_text(encoding="utf-8"))
    for f in sorted(p for p in root.rglob("*") if p.is_file()):
        data = f.read_bytes()
        for kind, value in secrets.items():
            if value and value.encode() in data:
                problems.append(f"LEAK: the {kind} secret is in {f.relative_to(root)}")
    print("\n".join(rows))
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="reward-escape", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--tcp", nargs="*", default=[])
    p.add_argument("--abstract", default="")
    p.add_argument("--unix", default="")
    p.add_argument("--files", nargs="*", default=[])
    p.add_argument("--sentinel-pid", type=int, default=0)
    p.add_argument("--sentinel-marker", default="")
    s = sub.add_parser("score")
    s.add_argument("--plan", required=True)
    s.add_argument("--case", required=True)
    s.add_argument("--socket", required=True)
    s.add_argument("--nonce", required=True)
    j = sub.add_parser("judge")
    j.add_argument("--plan", required=True)
    j.add_argument("--secrets", required=True)
    args = ap.parse_args(argv)
    if args.cmd == "plan":
        plan(args)
        return 0
    if args.cmd == "score":
        score(args)
        return 0
    problems = judge(args)
    for line in problems:
        print(line, file=sys.stderr)
    print(
        f"reward escape suite: {'FAILED' if problems else 'all blocked'} ({len(problems)} problems)"
    )
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
