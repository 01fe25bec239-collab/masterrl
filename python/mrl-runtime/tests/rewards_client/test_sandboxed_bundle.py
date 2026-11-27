"""The sandboxed reward bundle (`mrl_rewards.server_bundle`) and the Tier-2 code components
(`mrl_rewards.code`) on any machine: custom code needs the sandbox proof, the bundle layout
refuses anything but its own files, and the code components map every jail ending to a plain
reward outcome. The real jail runs in the Linux sandbox suite (tests/security/reward_escape)."""

from __future__ import annotations

import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

import pytest

from mrl_rewards import server_bundle
from mrl_rewards.code import full_registry
from mrl_rewards.code.components import CodeCompiles, CodeUnitTests
from mrl_rewards.code.jail import Jail, JailLimits, JailResult, _verdict  # pyright: ignore[reportPrivateUsage]
from mrl_rewards.policy.model import load_policy
from mrl_rewards.policy.runtime import PolicyRuntime
from mrl_rewards.types import JSONValue, RewardBatch, RewardError, RewardSample

USER_COMPONENT = """
from mrl_rewards.types import ComponentSpec, RewardResult

class Length:
    spec = ComponentSpec(name="mine.length", version="1.0.0", value_range=(0.0, 1.0),
                         deterministic=True, param_schema={"type": "object"}, description="length")
    def setup(self, ctx): pass
    def score(self, batch):
        return [RewardResult.ok(min(1.0, len(s.completion) / 10)) for s in batch.samples]
    def teardown(self): pass
"""


def policy(use: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "apiVersion": "mrl.dev/reward/v1",
        "kind": "RewardPolicy",
        "metadata": {"name": "t", "signal": "scalar.sequence"},
        "spec": {
            "components": [{"id": "c", "use": use, "weight": 1.0, "params": params or {}}],
            "aggregation": {"method": "weighted_sum", "where": "platform"},
            "failure": {
                "on_timeout": {"action": "mask"},
                "on_error": {"action": "mask"},
                "max_failure_rate": 1.0,
            },
            "tests": "tests.yaml",
        },
    }


def staged_code(tmp_path: Path) -> Path:
    code = tmp_path / "staged"
    code.mkdir()
    (code / "components.yaml").write_text(
        "components: [{module: mine, class: Length}]\n", encoding="utf-8"
    )
    (code / "mine.py").write_text(USER_COMPONENT, encoding="utf-8")
    return code


# ---- the bundle ----


def test_assemble_writes_a_read_only_bundle_and_refuses_foreign_files(tmp_path: Path) -> None:
    code = staged_code(tmp_path)
    root = server_bundle.assemble(policy("org/mine.length@1"), code, tmp_path / "b")
    assert sorted(p.name for p in (root / "code").iterdir()) == ["components.yaml", "mine.py"]
    if sys.platform != "win32":
        assert (root / "code" / "mine.py").stat().st_mode & 0o222 == 0
    for bad in (".hidden.py", "sub", "not-an-identifier.py", "data.bin"):
        d = tmp_path / f"bad{abs(hash(bad))}"
        d.mkdir()
        target = d / Path(bad).name
        if bad == "sub":
            target.mkdir()
        else:
            target.write_text("x", encoding="utf-8")
        with pytest.raises(RewardError) as e:
            server_bundle.assemble(
                policy("builtin/length.penalty@1"), d, tmp_path / f"out{abs(hash(bad))}"
            )
        assert e.value.code == "MRL-RWD-147", bad
    if sys.platform != "win32":
        d = tmp_path / "linked"
        d.mkdir()
        (d / "mine.py").symlink_to(code / "mine.py")
        with pytest.raises(RewardError):
            server_bundle.assemble(policy("org/mine.length@1"), d, tmp_path / "out-link")
    with pytest.raises(RewardError):
        server_bundle.assemble([], None, tmp_path / "no-policy")  # pyright: ignore[reportArgumentType]


def test_custom_code_needs_the_flag_and_a_passing_sandbox_proof(tmp_path: Path) -> None:
    root = server_bundle.assemble(
        policy("org/mine.length@1"), staged_code(tmp_path), tmp_path / "b"
    )
    with pytest.raises(RewardError) as e:
        server_bundle.load(root, user_code=False)
    assert e.value.code == "MRL-RWD-295"
    with pytest.raises(RewardError) as e:
        server_bundle.load(
            root, user_code=True, proof=server_bundle.Proof(False, "no seccomp filter")
        )
    assert e.value.code == "MRL-RWD-295"
    assert "no seccomp filter" in e.value.message
    if not sys.platform.startswith("linux"):
        with pytest.raises(RewardError) as e:
            server_bundle.load(root, user_code=True)
        assert e.value.code == "MRL-RWD-295"
        assert sys.platform in e.value.message
    # With a passing proof (as inside the sandbox) the component loads and scores.
    bundle = server_bundle.load(root, user_code=True, proof=server_bundle.Proof(True))
    assert bundle.user_code
    runtime = PolicyRuntime.build(bundle.policy, bundle.registry, bundle_dir=bundle.root)
    s = RewardSample("s", "g", "p", "12345", 5, "stop", None)
    (r,) = runtime.providers["c"].score(RewardBatch([s]))
    assert r.value == 0.5


def test_a_built_in_bundle_loads_anywhere_and_hashes_the_shipped_registry(tmp_path: Path) -> None:
    root = server_bundle.assemble(policy("builtin/math.verify@1"), None, tmp_path / "b")
    a = server_bundle.load(root, user_code=False)
    b = server_bundle.load(root, user_code=True)
    assert a.content_sha256 == b.content_sha256
    assert not a.user_code
    full = {str(r["ref"]) for r in full_registry().index()}
    assert {"builtin/code.compiles@1", "builtin/code.unit_tests@1"} <= full


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the proof reads /proc")
def test_the_sandbox_proof_checks_every_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    status = tmp_path / "status"
    monkeypatch.setattr(server_bundle, "non_loopback_interfaces", list)
    monkeypatch.delenv("MRL_USER_CODE_ALLOWED", raising=False)
    status.write_text("Seccomp:\t2\nNoNewPrivs:\t1\n", encoding="utf-8")
    assert not server_bundle.sandbox_proof(status).ok, "no sandbox marker"
    monkeypatch.setenv("MRL_USER_CODE_ALLOWED", "1")
    monkeypatch.setenv("MRL_SANDBOX_PROFILE", "worker")
    assert not server_bundle.sandbox_proof(status).ok, "not a reward profile"
    monkeypatch.setenv("MRL_SANDBOX_PROFILE", "reward")
    assert server_bundle.sandbox_proof(status).ok
    status.write_text("Seccomp:\t0\nNoNewPrivs:\t1\n", encoding="utf-8")
    assert "seccomp" in server_bundle.sandbox_proof(status).why
    status.write_text("Seccomp:\t2\nNoNewPrivs:\t0\n", encoding="utf-8")
    assert "no_new_privs" in server_bundle.sandbox_proof(status).why
    status.write_text("Seccomp:\t2\nNoNewPrivs:\t1\n", encoding="utf-8")
    monkeypatch.setattr(server_bundle, "non_loopback_interfaces", lambda: ["eth0"])
    assert "eth0" in server_bundle.sandbox_proof(status).why
    assert not server_bundle.sandbox_proof(tmp_path / "missing").ok


# ---- the code components ----


class FakeJail(Jail):
    """Answers each run with the next scripted result."""

    def __init__(self, results: list[JailResult]) -> None:
        super().__init__(Path("/nonexistent/bwrap"), None, Path(sys.executable))
        self.results = results
        self.runs: list[tuple[str, str, str]] = []

    def run(self, mode: str, solution: str, tests: str = "") -> JailResult:
        self.runs.append((mode, solution, tests))
        return self.results.pop(0)


def result(verdict: dict[str, Any] | None = None, **kw: Any) -> JailResult:
    base: dict[str, Any] = {
        "verdict": verdict,
        "exit_code": 0,
        "signal": None,
        "timed_out": False,
        "output": "",
        "output_flooded": False,
        "tests_changed": False,
    }
    base.update(kw)
    return JailResult(**base)


def build(
    cls: type[CodeCompiles] | type[CodeUnitTests],
    jail: FakeJail,
    params: dict[str, Any] | None = None,
) -> Any:
    reg = full_registry()
    previous = cls.jail_factory
    cls.jail_factory = staticmethod(lambda _limits: jail)  # type: ignore[method-assign]
    try:
        name = "code.compiles" if cls is CodeCompiles else "code.unit_tests"
        return PolicyRuntime.build(load_policy(policy(f"builtin/{name}@1", params)), reg).providers[
            "c"
        ]
    finally:
        cls.jail_factory = previous  # type: ignore[method-assign]


def code_sample(code: str | None, tests: str | None = "assert add(1, 2) == 3") -> RewardSample:
    completion = "no code here" if code is None else f"```python\n{code}\n```"
    ref: JSONValue = None if tests is None else {"verifier": {"tests": tests}}
    return RewardSample("s", "g", "write add", completion, 10, "stop", ref)


def test_unit_tests_score_from_the_verdict_never_from_printed_text() -> None:
    rows = [["test_a", True, ""], ["test_b", False, "AssertionError: no"]]
    jail = FakeJail(
        [
            result({"compiled": True, "loaded": True, "results": rows}, output="ALL TESTS PASSED"),
            result({"compiled": True, "loaded": True, "results": rows}),
        ]
    )
    strict = build(CodeUnitTests, jail)
    (r,) = strict.score(RewardBatch([code_sample("def add(a, b): return a + b")]))
    assert r.value == 0.0
    assert r.diagnostics["passed"] == 1
    assert "test_b" in str(r.diagnostics["first_failure"])
    assert jail.runs[0][0] == "tests"
    assert "add" in jail.runs[0][1]
    frac = build(CodeUnitTests, jail, {"scoring": "fraction"})
    (r,) = frac.score(RewardBatch([code_sample("def add(a, b): return a + b")]))
    assert r.value == 0.5


@pytest.mark.parametrize(
    ("ending", "status", "flag"),
    [
        (result(None, signal=int(getattr(signal, "SIGSYS", 31))), "ERROR", "sandbox_violation"),
        (result(None, timed_out=True), "TIMEOUT", None),
        (result(None, output_flooded=True), "ERROR", "output_flood"),
        (
            result(
                {"compiled": True, "loaded": True, "results": [["t", True, ""]]}, tests_changed=True
            ),
            "ERROR",
            "tests_changed",
        ),
        (result(None, exit_code=1), "OK", "no_verdict"),
    ],
)
def test_every_jail_ending_is_a_reward_outcome(
    ending: JailResult, status: str, flag: str | None
) -> None:
    (r,) = build(CodeUnitTests, FakeJail([ending])).score(RewardBatch([code_sample("x = 1")]))
    assert r.status == status
    if flag:
        assert flag in r.flags
    if status == "OK":
        assert r.value == 0.0


def test_inputs_without_code_or_tests_never_reach_the_jail() -> None:
    jail = FakeJail([])
    (no_code,) = build(CodeUnitTests, jail).score(RewardBatch([code_sample(None)]))
    assert no_code.value == 0.0
    (no_tests,) = build(CodeUnitTests, jail).score(RewardBatch([code_sample("x = 1", tests=None)]))
    assert no_tests.status == "INVALID_INPUT"
    assert jail.runs == []
    jail = FakeJail([result({"compiled": False, "error": "SyntaxError: bad"})])
    (r,) = build(CodeCompiles, jail).score(RewardBatch([code_sample("def (")]))
    assert r.value == 0.0
    assert r.diagnostics["compiled"] is False
    assert jail.runs[0][0] == "compile"


def test_a_verdict_counts_only_with_this_run_s_token() -> None:
    assert _verdict(json.dumps({"compiled": True, "token": "abc"}).encode(), "abc") == {
        "compiled": True,
        "token": "abc",
    }
    assert _verdict(json.dumps({"compiled": True, "token": "forged"}).encode(), "abc") is None
    assert _verdict(b"\xff\xfe", "abc") is None
    assert _verdict(b"[1, 2]", "abc") is None
    assert _verdict(b"", "abc") is None


def test_limits_are_capped_and_the_jail_refuses_machines_that_cannot_isolate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RewardError):
        JailLimits(wall_s=60).checked()
    with pytest.raises(RewardError):
        JailLimits(cpu_s=50).checked()
    monkeypatch.delenv("MRL_JAIL_SECCOMP", raising=False)
    with pytest.raises(RewardError) as e:
        Jail.from_environment()
    assert e.value.code == "MRL-RWD-295"
    if os.name == "posix":
        argv = Jail(Path("/usr/bin/bwrap"), Path("/f.bpf"), Path(sys.executable)).argv(
            Path("/tmp/x"), "tests"
        )
        for must in ("--unshare-all", "--clearenv", "--cap-drop", "--seccomp", "--die-with-parent"):
            assert must in argv
        assert "--share-net" not in argv
        assert argv[argv.index("--ro-bind", argv.index("--tmpfs")) + 1 : argv.index("--chdir")] == [
            "/tmp/x",
            "/jail",
        ]
