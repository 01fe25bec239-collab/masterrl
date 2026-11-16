"""mrl-reward-exec over a real Unix socket (loopback TCP where sockets are missing)."""

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import grpc
import pytest
import yaml

from mrl_rewards._generated.mrl.reward.v1 import reward_service_pb2 as pb
from mrl_rewards._generated.mrl.reward.v1 import reward_service_pb2_grpc as pb_grpc
from mrl_rewards.exec import main as exec_main
from mrl_rewards.exec.bundle import load_bundle
from mrl_rewards.exec.server import STATS_METADATA_KEY, Audit, ExecLimits, Scheduler, score_to_proto
from mrl_rewards.exec.testkit import LocalService, start_local
from mrl_rewards.policy.runtime import ComponentScore, Job, SampleScore
from mrl_rewards.types import Resolution, RewardError, RewardSample, Status

UDS = pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets are not supported here")
NONCE = "testkit-nonce"
VERSION = "rwdv_testkit"

USER_CODE = """
import time
from mrl_rewards.types import ComponentSpec, RewardResult


class Behave:
    spec = ComponentSpec(
        name="behave", version="1.0.0", value_range=(0.0, 1.0), deterministic=True,
        description="test", param_schema={"type": "object"},
    )

    def setup(self, ctx):
        pass

    def teardown(self):
        pass

    def score(self, batch):
        out = []
        for s in batch.samples:
            ref = s.reference if isinstance(s.reference, dict) else {}
            if ref.get("how") == "sleep":
                time.sleep(float(ref["seconds"]))
            if ref.get("how") == "raise":
                raise RuntimeError("boom")
            out.append(RewardResult.ok(float(ref.get("value", 1.0))))
        return out
"""


def policy(max_failure_rate: float = 0.8, user_code: bool = True) -> dict[str, Any]:
    comps: list[dict[str, Any]] = [
        {
            "id": "len",
            "use": "builtin/length.penalty_linear@1",
            "params": {"max_tokens": 100},
            "weight": 1.0,
        }
    ]
    if user_code:
        comps.append({"id": "behave", "use": "org/behave@1", "weight": 1.0})
    return {
        "apiVersion": "mrl.dev/reward/v1",
        "kind": "RewardPolicy",
        "metadata": {"name": "exec-test", "signal": "scalar.sequence"},
        "spec": {
            "components": comps,
            "aggregation": {"method": "weighted_sum", "where": "platform"},
            "clipping": {"min": -2, "max": 2},
            "failure": {
                "on_timeout": {"action": "mask"},
                "on_error": {"action": "fallback", "fallback_value": -1.0},
                "max_failure_rate": max_failure_rate,
            },
            "tests": "./tests/cases.yaml",
        },
    }


def write_bundle(root: Path, doc: dict[str, Any], code: str | None = USER_CODE) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "policy.yaml").write_text(yaml.safe_dump(doc), encoding="utf-8")
    if code is not None:
        (root / "code").mkdir(exist_ok=True)
        (root / "code" / "behave.py").write_text(code, encoding="utf-8")
        (root / "code" / "components.yaml").write_text(
            "components:\n  - {module: behave, class: Behave, namespace: org}\n", encoding="utf-8"
        )
    return root


@pytest.fixture
def sock_dir() -> Iterator[Path]:
    # Unix socket paths must stay short (about 100 bytes), so not pytest's tmp_path.
    d = Path(tempfile.mkdtemp(prefix="mrl", dir=None if sys.platform == "win32" else "/tmp"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def serve(bundle: Path, sock_dir: Path, *, uds: bool = True, **limits: Any) -> LocalService:
    address = f"unix:{sock_dir / 'r.sock'}" if uds else "127.0.0.1:0"
    audit = Audit(io.StringIO())
    return start_local(bundle, address=address, limits=ExecLimits(**limits), audit=audit)


def stub(svc: LocalService) -> pb_grpc.RewardServiceStub:
    return pb_grpc.RewardServiceStub(grpc.insecure_channel(svc.target))


def sample(
    sid: str, tokens: int = 10, reference: Any = None, raw_ref: str | None = None, text: str = ""
) -> pb.Sample:
    return pb.Sample(
        sample_id=sid,
        group_id="g",
        prompt="p",
        completion=text or f"c-{sid}",
        completion_token_count=tokens,
        finish_reason="stop",
        reference_json=raw_ref if raw_ref is not None else json.dumps(reference),
    )


def request(*samples: pb.Sample, rid: str = "req-1", **kw: Any) -> pb.ScoreRequest:
    return pb.ScoreRequest(
        attempt_nonce=NONCE, request_id=rid, reward_version_id=VERSION, samples=samples, **kw
    )


@UDS
def test_describe_and_health_over_uds(tmp_path: Path, sock_dir: Path) -> None:
    with serve(write_bundle(tmp_path / "b", policy()), sock_dir) as svc:
        s = stub(svc)
        d = s.Describe(pb.DescribeRequest(attempt_nonce=NONCE))
        assert d.reward_version_id == VERSION
        assert d.content_sha256 == svc.bundle.content_sha256
        assert len(d.content_sha256) == 64
        assert d.signal == "scalar.sequence"
        assert [(c.component_id, c.component_ref) for c in d.components] == [
            ("len", "builtin/length.penalty_linear@1"),
            ("behave", "org/behave@1"),
        ]
        assert (d.components[0].value_min, d.components[0].value_max) == (-1.0, 0.0)
        assert d.max_batch == 256
        assert d.max_tier == 1
        assert s.Health(pb.HealthRequest(attempt_nonce=NONCE)).serving


@UDS
def test_batch_with_partial_failures_over_uds(tmp_path: Path, sock_dir: Path) -> None:
    with serve(write_bundle(tmp_path / "b", policy()), sock_dir, per_sample_timeout_ms=300) as svc:
        start = time.monotonic()
        resp = stub(svc).Score(
            request(
                sample("ok", tokens=50, reference={"value": 0.5}),
                sample("boom", reference={"how": "raise"}),
                sample("slow", reference={"how": "sleep", "seconds": 2}),
                sample("badjson", raw_ref="{not json"),
                sample("ok2", tokens=0, reference={"value": 1}),
            )
        )
        assert time.monotonic() - start < 1.5, "a slow sample must not hold up the batch"
    ok, boom, slow, bad, ok2 = resp.scores
    assert [s.sample_id for s in resp.scores] == ["ok", "boom", "slow", "badjson", "ok2"]
    assert (ok.total, ok.resolution) == (0.0, pb.RESOLUTION_SCORED)  # -0.5 + 0.5
    assert (ok2.total, ok2.status) == (1.0, pb.SCORE_STATUS_OK)
    assert (boom.status, boom.resolution, boom.total) == (
        pb.SCORE_STATUS_ERROR,
        pb.RESOLUTION_FALLBACK_APPLIED,
        -1.0,
    )
    assert "boom" in json.loads(boom.components[1].diagnostics["reason"])
    assert (slow.status, slow.resolution) == (pb.SCORE_STATUS_TIMEOUT, pb.RESOLUTION_MASKED)
    assert not slow.HasField("total"), "a masked sample has no total, never 0.0"
    assert "timeout:behave" in slow.flags
    assert bad.status == pb.SCORE_STATUS_INVALID_INPUT
    assert dict(resp.stats.failures_by_status) == {"ERROR": 1, "TIMEOUT": 1, "INVALID_INPUT": 1}
    assert resp.stats.sample_count == 5


@UDS
def test_deadline_returns_completed_samples(tmp_path: Path, sock_dir: Path) -> None:
    with serve(
        write_bundle(tmp_path / "b", policy()), sock_dir, per_sample_timeout_ms=10_000
    ) as svc:
        resp = stub(svc).Score(
            request(
                sample("fast", reference={}),
                sample("slow", reference={"how": "sleep", "seconds": 1.5}),
                deadline_ms=300,
            )
        )
    fast, slow = resp.scores
    assert fast.status == pb.SCORE_STATUS_OK
    assert slow.status == pb.SCORE_STATUS_TIMEOUT
    assert "deadline" in slow.components[1].flags


@UDS
def test_replay_and_cache(tmp_path: Path, sock_dir: Path) -> None:
    with serve(write_bundle(tmp_path / "b", policy()), sock_dir) as svc:
        s = stub(svc)
        req = request(
            sample("a", reference={"value": 0.25}, text="same"), sample("b", reference={})
        )
        first = s.Score(req)
        again = s.Score(req)
        assert again.SerializeToString(deterministic=True) == first.SerializeToString(
            deterministic=True
        )
        with pytest.raises(grpc.RpcError) as err:
            s.Score(request(sample("other", reference={}), rid="req-1"))
        assert err.value.code() == grpc.StatusCode.ALREADY_EXISTS
        assert "MRL-IDEM-002" in (err.value.details() or "")
        fresh = s.Score(request(sample("a2", reference={"value": 0.25}, text="same"), rid="req-2"))
        # Same content as sample "a": both deterministic components come from the cache.
        assert fresh.stats.cache_hits == 2
        assert fresh.scores[0].total == first.scores[0].total


@UDS
def test_request_checks(tmp_path: Path, sock_dir: Path) -> None:
    with serve(write_bundle(tmp_path / "b", policy()), sock_dir, max_batch=2) as svc:
        s = stub(svc)
        cases = [
            (
                pb.ScoreRequest(attempt_nonce="wrong", reward_version_id=VERSION),
                grpc.StatusCode.UNAUTHENTICATED,
                "MRL-RWD-145",
            ),
            (
                pb.ScoreRequest(attempt_nonce=NONCE, reward_version_id="rwdv_other"),
                grpc.StatusCode.FAILED_PRECONDITION,
                "MRL-RWD-145",
            ),
            (
                request(sample("1"), sample("2"), sample("3")),
                grpc.StatusCode.INVALID_ARGUMENT,
                "MRL-VAL-007",
            ),
        ]
        for req, code, mrl in cases:
            with pytest.raises(grpc.RpcError) as err:
                s.Score(req)
            assert err.value.code() == code
            assert mrl in (err.value.details() or "")
        with pytest.raises(grpc.RpcError) as err:
            s.Describe(pb.DescribeRequest(attempt_nonce=""))
        assert err.value.code() == grpc.StatusCode.UNAUTHENTICATED


@UDS
def test_too_many_failures_fail_the_batch_with_stats(tmp_path: Path, sock_dir: Path) -> None:
    with serve(write_bundle(tmp_path / "b", policy(max_failure_rate=0.25)), sock_dir) as svc:
        call = stub(svc).Score.with_call
        with pytest.raises(grpc.RpcError) as err:
            call(
                request(
                    sample("x", reference={"how": "raise"}), sample("y", reference={"how": "raise"})
                )
            )
    e = err.value
    assert e.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "MRL-RWD-144" in (e.details() or "")
    trailing: dict[str, Any] = {m.key: m.value for m in e.trailing_metadata() or ()}  # type: ignore[attr-defined]
    stats = pb.BatchStats.FromString(trailing[STATS_METADATA_KEY])
    assert dict(stats.failures_by_status) == {"ERROR": 2}
    assert stats.sample_count == 2


def test_loopback_tcp(tmp_path: Path, sock_dir: Path) -> None:
    with serve(write_bundle(tmp_path / "b", policy()), sock_dir, uds=False) as svc:
        assert svc.target.startswith("127.0.0.1:")
        resp = stub(svc).Score(request(sample("a", reference={})))
        assert resp.scores[0].status == pb.SCORE_STATUS_OK


def test_only_loopback_or_unix_addresses(tmp_path: Path) -> None:
    for address in ("0.0.0.0:0", "10.0.0.1:5000", "[::]:0", "unix:"):
        with pytest.raises(RewardError) as err:
            start_local(write_bundle(tmp_path / "b", policy()), address=address)
        assert err.value.code == "MRL-RWD-149"


# --- bundles ----------------------------------------------------------------------------------


def test_user_code_is_refused_outside_the_testkit(tmp_path: Path) -> None:
    b = write_bundle(tmp_path / "b", policy())
    with pytest.raises(RewardError) as err:
        load_bundle(b, allow_user_code=False)
    assert err.value.code == "MRL-RWD-146"
    assert load_bundle(b, allow_user_code=True).user_code


def test_bundle_hash_covers_policy_and_code(tmp_path: Path) -> None:
    b = write_bundle(tmp_path / "b", policy())
    h1 = load_bundle(b, allow_user_code=True).content_sha256
    assert load_bundle(b, allow_user_code=True).content_sha256 == h1
    (b / "code" / "behave.py").write_text(USER_CODE + "\n# changed\n", encoding="utf-8")
    h2 = load_bundle(b, allow_user_code=True).content_sha256
    write_bundle(b, policy(max_failure_rate=0.5))
    h3 = load_bundle(b, allow_user_code=True).content_sha256
    assert len({h1, h2, h3}) == 3
    # The same policy as JSON hashes the same as YAML: the hash is over canonical JSON.
    j = tmp_path / "j"
    j.mkdir()
    (j / "policy.json").write_text(json.dumps(policy(user_code=False)), encoding="utf-8")
    y = write_bundle(tmp_path / "y", policy(user_code=False), code=None)
    assert (
        load_bundle(j, allow_user_code=False).content_sha256
        == load_bundle(y, allow_user_code=False).content_sha256
    )


@pytest.mark.parametrize(
    ("setup", "code"),
    [
        (lambda b: (b / "policy.yaml").unlink(), "MRL-RWD-147"),
        (lambda b: (b / "policy.json").write_text("{}"), "MRL-RWD-147"),
        (lambda b: (b / "policy.yaml").write_text("a: [unclosed"), "MRL-RWD-140"),
        (lambda b: (b / "code" / "components.yaml").write_text("components: []"), "MRL-RWD-147"),
        (lambda b: (b / "code" / "behave.py").write_text("raise SystemError('x')"), "MRL-RWD-147"),
        (lambda b: (b / "code" / "behave.py").write_text("class Behave: pass"), "MRL-RWD-147"),
        (
            lambda b: (b / "code" / "components.yaml").write_text(
                "components: [{module: behave, class: Behave, namespace: builtin}]"
            ),
            "MRL-RWD-147",
        ),
    ],
)
def test_broken_bundles(tmp_path: Path, setup: Any, code: str) -> None:
    b = write_bundle(tmp_path / "b", policy())
    setup(b)
    with pytest.raises(RewardError) as err:
        load_bundle(b, allow_user_code=True)
    assert err.value.code == code


# --- scheduler and wire format ------------------------------------------------------------------


def test_watchdog_reports_a_stuck_component(tmp_path: Path) -> None:
    b = load_bundle(write_bundle(tmp_path / "b", policy()), allow_user_code=True)
    stuck = threading.Event()
    sched = Scheduler(
        ExecLimits(hard_limit_s=0.2, per_sample_timeout_ms=50),
        b.content_sha256,
        Audit(io.StringIO()),
        on_stuck=stuck.set,
    )
    provider = b.registry.create("org/behave@1")
    s = RewardSample("s", "g", "p", "c", 1, "stop", {"how": "sleep", "seconds": 1.0})
    (r,) = sched.run([Job(0, "behave", provider, s, 0, None, False)])
    assert r.status == Status.TIMEOUT
    assert stuck.wait(2.0), "the watchdog fires when a component runs past the hard limit"
    sched.close()


def test_wire_format_caps_diagnostics_per_sample() -> None:
    comps = tuple(
        ComponentScore(f"c{i}", 1.0, Status.OK, {f"k{j}": "x" * 200 for j in range(10)}, ())
        for i in range(5)
    )
    msg = score_to_proto(
        SampleScore(
            "s", 1.0, Status.OK, Resolution.SCORED, comps, tuple(f"f{i}" for i in range(30))
        )
    )
    size = sum(len(k) + len(v) for c in msg.components for k, v in c.diagnostics.items())
    assert size <= 4096
    assert any("diagnostics_truncated" in c.flags for c in msg.components)
    assert len(msg.flags) == 16


# --- the command ------------------------------------------------------------------------------


def nonce_fd() -> int:
    r, w = os.pipe()
    os.write(w, NONCE.encode())
    os.close(w)
    return r


def test_cli_refuses_user_code_and_network(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    b = write_bundle(tmp_path / "b", policy())
    args = ["--bundle", str(b), "--listen", "127.0.0.1:0", "--reward-version-id", VERSION]
    assert exec_main.main([*args, "--nonce-fd", str(nonce_fd()), "--allow-network"]) == 2
    assert "MRL-RWD-146" in capsys.readouterr().err
    monkeypatch.setattr(exec_main, "non_loopback_interfaces", lambda: ["eth0"])
    assert exec_main.main([*args, "--nonce-fd", str(nonce_fd())]) == 2
    err = capsys.readouterr().err
    assert "MRL-RWD-149" in err
    assert "eth0" in err


@UDS
def test_cli_serves_a_builtin_bundle(tmp_path: Path, sock_dir: Path) -> None:
    b = write_bundle(tmp_path / "b", policy(user_code=False), code=None)
    sock = sock_dir / "cli.sock"
    fd = nonce_fd()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "mrl_rewards.exec.main",
            "--bundle",
            str(b),
            "--listen",
            f"unix:{sock}",
            "--reward-version-id",
            VERSION,
            "--nonce-fd",
            str(fd),
            "--allow-network",
        ],
        stdout=subprocess.PIPE,
        text=True,
        pass_fds=(fd,),
    )
    os.close(fd)
    try:
        assert proc.stdout is not None
        events = [json.loads(proc.stdout.readline()) for _ in range(2)]
        assert [e["event"] for e in events] == [
            "reward_exec.network_check_skipped",
            "reward_exec.ready",
        ]
        ready = events[1]
        assert ready["components"] == ["builtin/length.penalty_linear@1"]
        s = pb_grpc.RewardServiceStub(grpc.insecure_channel(f"unix:{sock}"))
        d = s.Describe(pb.DescribeRequest(attempt_nonce=NONCE))
        assert d.content_sha256 == ready["content_sha256"]
        resp = s.Score(request(sample("a", tokens=25)))
        assert resp.scores[0].total == -0.25
    finally:
        proc.terminate()
        assert proc.wait(timeout=10) == 0
    tail = proc.stdout.read() if proc.stdout else ""
    assert "reward_exec.stopped" in textwrap.dedent(tail)
