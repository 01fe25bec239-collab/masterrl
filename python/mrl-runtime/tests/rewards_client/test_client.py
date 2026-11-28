"""RewardClient against the real reward service (P14's, in-process over a Unix socket) and
against a hostile fake service."""

from __future__ import annotations

import copy
import json
import math
import os
import sys
import tempfile
from collections.abc import Iterator
from concurrent import futures
from io import StringIO
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import grpc
import pytest

from mrl_rewards.code.components import CodeCompiles
from mrl_rewards.code.jail import Jail, JailResult
from mrl_rewards.exec.server import Audit, ExecLimits
from mrl_rewards.exec.testkit import start_local
from mrl_rewards.server_bundle import serve as sandbox_service
from mrl_runtime._generated.mrl.reward.v1 import reward_service_pb2 as pb
from mrl_runtime._generated.mrl.reward.v1 import reward_service_pb2_grpc as pb_grpc
from mrl_runtime.builtin.reward_preview import Service
from mrl_runtime.rewards_client import (
    SANDBOX_VIOLATION,
    ComponentDescription,
    RewardClient,
    RewardClientError,
    RewardDescription,
    RewardHashMismatchError,
    RewardInfraError,
    Sample,
    check_response,
)

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets")

POLICY = {
    "apiVersion": "mrl.dev/reward/v1",
    "kind": "RewardPolicy",
    "metadata": {"name": "math", "signal": "scalar.sequence"},
    "spec": {
        "components": [{"id": "correct", "use": "builtin/math.verify@1", "weight": 1.0}],
        "aggregation": {"method": "weighted_sum", "where": "platform"},
        "failure": {
            "on_timeout": {"action": "mask"},
            "on_error": {"action": "mask"},
            "max_failure_rate": 1.0,
        },
        "tests": "tests.yaml",
    },
}


@pytest.fixture
def sock_dir() -> Iterator[Path]:
    # Short path: Unix socket paths are limited to about 100 bytes.
    with tempfile.TemporaryDirectory(prefix="rw", dir="/tmp") as d:
        yield Path(d)


@pytest.fixture
def real(sock_dir: Path) -> Iterator[Any]:
    bundle = sock_dir / "bundle"
    bundle.mkdir()
    (bundle / "policy.json").write_text(json.dumps(POLICY), encoding="utf-8")
    svc = start_local(
        bundle,
        address=f"unix:{sock_dir / 'r.sock'}",
        nonce="n0nce",
        reward_version_id="rwdv_test",
        limits=ExecLimits(max_batch=2),
    )
    yield svc
    svc.close()


def client(path: Path, **kw: Any) -> RewardClient:
    args: dict[str, Any] = {
        "nonce": "n0nce",
        "reward_version_id": "rwdv_test",
        "attempt_id": "att_1",
    }
    args.update(kw)
    return RewardClient(str(path), **args)


def samples(n: int) -> list[Sample]:
    return [
        Sample(
            prompt="1+1?",
            completion=f"<answer>{2 if i % 2 == 0 else 3}</answer>",
            group_id=f"g{i // 2}",
            reference="2",
        )
        for i in range(n)
    ]


def test_describe_checks_the_pinned_hash_and_scores_in_order_across_chunks(
    real: Any, sock_dir: Path
) -> None:
    metrics: list[dict[str, float]] = []
    c = client(
        sock_dir / "r.sock",
        expected_sha256=real.bundle.content_sha256,
        emit=lambda m: metrics.append(dict(m)),
    )
    d = c.describe()
    assert d.max_batch == 2
    assert d.components[0].component_id == "correct"
    out = c.score(samples(5))
    assert [o.sample_id for o in out] == ["0:0", "0:1", "0:2", "0:3", "0:4"]
    assert [o.value for o in out] == [1.0, 0.0, 1.0, 0.0, 1.0]
    assert all(o.resolution == "SCORED" and not o.failed for o in out)
    m = metrics[-1]
    for name in (
        "rl/reward/failure_rate",
        "rl/reward/timeout_rate",
        "rl/reward/fallback_rate",
        "rl/reward/groups_dropped",
        "rl/reward/latency_ms_p95",
        "rl/reward/component/correct/mean",
        "rl/reward/component/correct/std",
        "rl/reward/mean",
    ):
        assert name in m, name
    assert m["rl/reward/component/correct/mean"] == pytest.approx(0.6)
    # State for rl_state.json survives a restart of the client.
    state = c.state_dict()
    c2 = client(sock_dir / "r.sock")
    c2.load_state_dict(state)
    assert c2.state_dict() == state
    assert c2.score(samples(1))[0].sample_id == "1:0", "the batch counter continues"
    c.close()
    c2.close()


def test_a_hash_mismatch_stops_before_any_scoring(real: Any, sock_dir: Path) -> None:
    c = client(sock_dir / "r.sock", expected_sha256="0" * 64)
    with pytest.raises(RewardHashMismatchError) as e:
        c.describe()
    assert e.value.code == "MRL-RWD-292"
    c = client(sock_dir / "r.sock", reward_version_id="rwdv_other")
    with pytest.raises(RewardHashMismatchError):
        c.score(samples(1))


def test_only_unix_sockets_are_accepted() -> None:
    for bad in ("127.0.0.1:5000", "dns:///reward", "relative.sock"):
        with pytest.raises(RewardClientError) as e:
            RewardClient(bad, nonce="n", reward_version_id="v", attempt_id="a")
        assert e.value.code == "MRL-RWD-291"


class Hostile:
    """A reward service that answers whatever the test says."""

    def __init__(self) -> None:
        self.mode = "ok"
        self.requests: list[pb.ScoreRequest] = []
        self.health = 0

    def Describe(  # noqa: N802 (gRPC method name)
        self, request: pb.DescribeRequest, context: grpc.ServicerContext
    ) -> pb.DescribeResponse:
        return pb.DescribeResponse(
            reward_version_id="rwdv_test",
            content_sha256="a" * 64,
            signal="scalar.sequence",
            max_batch=8,
            max_tier=1,
            components=[
                pb.ComponentInfo(
                    component_id="c",
                    component_ref="builtin/x@1",
                    weight=1,
                    value_min=0,
                    value_max=1,
                    tier=1,
                )
            ],
        )

    def Health(self, request: pb.HealthRequest, context: grpc.ServicerContext) -> pb.HealthResponse:  # noqa: N802 (gRPC method name)
        self.health += 1
        return pb.HealthResponse(serving=True)

    def Score(self, request: pb.ScoreRequest, context: grpc.ServicerContext) -> pb.ScoreResponse:  # noqa: N802 (gRPC method name)
        self.requests.append(request)
        if self.mode == "fail" or (self.mode == "flaky" and len(self.requests) == 1):
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "MRL-RWD-144: too many failures")
        ids = [s.sample_id for s in request.samples]

        def score(sid: str, value: float | None = 0.5, total: float | None = 0.5) -> pb.SampleScore:
            comp = pb.ComponentScore(component_id="c", status=pb.SCORE_STATUS_OK)
            if value is not None:
                comp.value = value
            s = pb.SampleScore(
                sample_id=sid,
                components=[comp],
                resolution=pb.RESOLUTION_SCORED,
                status=pb.SCORE_STATUS_OK,
            )
            if total is not None:
                s.total = total
            return s

        match self.mode:
            case "short":
                return pb.ScoreResponse(scores=[score(i) for i in ids[:-1]])
            case "reordered":
                return pb.ScoreResponse(scores=[score(i) for i in reversed(ids)])
            case "missing":
                return pb.ScoreResponse(
                    scores=[
                        pb.SampleScore(
                            sample_id=i,
                            total=0.5,
                            status=pb.SCORE_STATUS_OK,
                            resolution=pb.RESOLUTION_SCORED,
                        )
                        for i in ids
                    ]
                )
            case "garbage":
                return pb.ScoreResponse(
                    scores=[
                        score(ids[0], value=math.nan),
                        score(ids[1], value=7.0),
                        score(ids[2], total=math.inf),
                        score(ids[3], total=None),
                    ]
                )
            case _:
                return pb.ScoreResponse(scores=[score(i) for i in ids])


@pytest.fixture
def hostile(sock_dir: Path) -> Iterator[tuple[Hostile, Path]]:
    h = Hostile()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_RewardServiceServicer_to_server(h, server)  # pyright: ignore[reportUnknownMemberType]
    path = sock_dir / "h.sock"
    server.add_insecure_port(f"unix:{path}")
    server.start()
    yield h, path
    server.stop(None)


@pytest.mark.parametrize("mode", ["short", "reordered", "garbage", "missing"])
def test_untrusted_answers_become_invalid_output_never_a_crash(
    hostile: tuple[Hostile, Path], mode: str
) -> None:
    h, path = hostile
    h.mode = mode
    out = client(path).score(samples(4))
    assert len(out) == 4
    assert all(o.value is None and o.failed and o.worst_status == "INVALID_OUTPUT" for o in out), (
        out
    )


def test_a_refused_batch_preserves_a_sandbox_stop(
    sock_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jail = Mock(spec=Jail)
    jail.run.return_value = JailResult(
        verdict=None,
        exit_code=159,
        signal=None,
        timed_out=False,
        output="",
        output_flooded=False,
        tests_changed=False,
    )
    monkeypatch.setattr(CodeCompiles, "jail_factory", staticmethod(lambda _limits: jail))
    policy = copy.deepcopy(POLICY)
    policy["spec"]["components"][0]["use"] = "builtin/code.compiles@1"
    policy["spec"]["components"][0]["params"] = {"answer_extractor": "full"}
    policy["spec"]["failure"]["max_failure_rate"] = 0.0
    (sock_dir / "policy.json").write_text(json.dumps(policy), encoding="utf-8")
    path = sock_dir / "r.sock"
    read, write = os.pipe()
    with os.fdopen(write, "w") as nonce:
        nonce.write("n0nce")
    args = sandbox_service._parser().parse_args(
        [  # pyright: ignore[reportPrivateUsage]
            "--bundle",
            str(sock_dir),
            "--listen",
            f"unix:{path}",
            "--reward-version-id",
            "rwdv_test",
            "--nonce-fd",
            str(read),
            "--allow-network",
            "--tier",
            "2",
            "--cache-entries",
            "0",
        ]
    )
    service, server = sandbox_service.start(args, Audit(StringIO()))
    handler = client(path)
    try:
        with pytest.raises(RewardClientError) as rejected:
            handler.score([Sample("", "print(1)")])
        assert rejected.value.code == SANDBOX_VIOLATION
        jail.run.assert_called_once()
    finally:
        handler.close()
        cast("grpc.Server", server).stop(None).wait()
        service.close()


def test_a_failed_batch_is_retried_once_with_the_same_request_id_then_reward_infra(
    hostile: tuple[Hostile, Path],
) -> None:
    h, path = hostile
    h.mode = "flaky"
    out = client(path, health_wait_s=1).score(samples(2))
    assert [o.value for o in out] == [0.5, 0.5]
    assert h.requests[0].request_id == h.requests[1].request_id, (
        "a retry is a replay, not a new batch"
    )
    assert h.health >= 1, "it waits for the service to be healthy first"
    h.mode = "fail"
    with pytest.raises(RewardInfraError) as e:
        client(path, health_wait_s=1).score(samples(2))
    assert e.value.code == "MRL-RWD-293"


def test_check_response_accepts_masks_and_fallbacks() -> None:
    desc = RewardDescription(
        "v", "h", "scalar.sequence", (ComponentDescription("c", "r", 1, 0, 1, 1, True),), 8, 1
    )
    masked = pb.SampleScore(
        sample_id="0:0",
        status=pb.SCORE_STATUS_TIMEOUT,
        resolution=pb.RESOLUTION_MASKED,
        components=[pb.ComponentScore(component_id="c", status=pb.SCORE_STATUS_TIMEOUT)],
    )
    fallback = pb.SampleScore(
        sample_id="0:1",
        total=-1.0,
        status=pb.SCORE_STATUS_ERROR,
        resolution=pb.RESOLUTION_FALLBACK_APPLIED,
        components=[pb.ComponentScore(component_id="c", status=pb.SCORE_STATUS_ERROR)],
    )
    out = check_response(desc, ["0:0", "0:1"], pb.ScoreResponse(scores=[masked, fallback]))
    assert out[0].masked
    assert out[0].worst_status == "TIMEOUT"
    assert out[1].value == -1.0
    assert out[1].resolution == "FALLBACK_APPLIED"
    assert out[1].failed


def test_running_normalizer_scores_match_after_service_restart(sock_dir: Path) -> None:
    policy = copy.deepcopy(POLICY)
    policy["spec"]["normalization"] = {"final": "zscore_running"}
    reward = {"version_id": "rwdv_test", "policy": policy}
    a = Service(sock_dir, {}, reward, "reward")
    correct, incorrect = samples(2)
    try:
        a.start()
        a.client.score([correct])
        checkpoint = a.client.state_dict()
        uninterrupted = a.client.score([incorrect])[0].value
        assert uninterrupted == pytest.approx(-0.70710678)
    finally:
        a.close()
    b = Service(sock_dir, {}, reward, "reward")
    try:
        b.start()
        b.client.load_state_dict(checkpoint)
        assert b.client.score([incorrect])[0].value == uninterrupted
    finally:
        b.close()


def test_group_members_stay_together_and_results_keep_input_order(
    real: Any, sock_dir: Path
) -> None:
    c = client(sock_dir / "r.sock")
    try:
        good, bad = samples(2)
        separate = Sample("1+1?", "<answer>2</answer>", group_id="b", reference="2")
        result = c.score([good, separate, bad])
        assert [row.value for row in result] == [1.0, 1.0, 0.0]
        assert result[0].chunk_index == result[2].chunk_index
        assert result[0].chunk_index != result[1].chunk_index
        with pytest.raises(RewardClientError, match="above the service limit"):
            c.score([good, bad, good])
    finally:
        c.close()
