import asyncio
import sys
from pathlib import Path

import pytest

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.errors import PROTOCOL_MISMATCH, MrlError
from mrl_runtime.protocol import Spool, WorkerSession, grpc_target, negotiate
from mrl_runtime.testkit.agent import UDS_SUPPORTED, AgentScript, FakeAgent


def _agent(tmp_path: Path, **kw: object) -> FakeAgent:
    return FakeAgent(AgentScript(job_spec={}, staging_dir=tmp_path, auto_start=False), **kw)  # type: ignore[arg-type]


def test_grpc_target() -> None:
    assert grpc_target("/run/mrl/a.sock") == "unix:/run/mrl/a.sock"
    assert grpc_target("unix:/x") == "unix:/x"
    assert grpc_target("127.0.0.1:5000") == "127.0.0.1:5000"
    assert grpc_target("C:\\mrl\\a.sock") == "unix:C:\\mrl\\a.sock"


def test_negotiation() -> None:
    assert negotiate(["1.0"], "1.0") == "1.0"
    with pytest.raises(MrlError) as err:
        negotiate(["1.0"], "2.0")
    assert err.value.code == PROTOCOL_MISMATCH


async def _replay_after_restart(tmp_path: Path, agent: FakeAgent) -> None:
    await agent.start()
    session = WorkerSession(agent.target, pb.Hello(attempt_id="att_1"), Spool(tmp_path / "spool"))
    session.start()
    try:
        for step in range(1, 4):
            session.send(pb.MetricsBatch(points=[pb.MetricPoint(name="m", step=step)]))
        assert await session.wait_acked(10)
        # Messages 4..5 reach the agent, but it restarts before making them durable (no ack).
        agent.hold_acks = True
        for step in range(4, 6):
            session.send(pb.MetricsBatch(points=[pb.MetricPoint(name="m", step=step)]))
        await agent.wait_for(lambda m: m.seq == 5)
        await agent.restart()
        assert agent.durable == 3
        for step in range(6, 9):  # sent while reconnecting: spooled, then delivered
            session.send(pb.MetricsBatch(points=[pb.MetricPoint(name="m", step=step)]))
        assert await session.wait_acked(10)
        steps = [m.metrics.points[0].step for m in agent.received]
        assert steps == list(range(1, 9))
        assert [m.seq for m in agent.received] == list(range(1, 9))
        assert agent.sessions == 2
        assert session.connections == 2
        assert agent.protocol_errors == []
        hellos = agent.of("hello")
        assert len(hellos) == 2
        assert all(h.seq == 0 for h in hellos)
    finally:
        await session.close()
        await agent.close()


def test_replay_after_agent_restart(tmp_path: Path) -> None:
    asyncio.run(_replay_after_restart(tmp_path, _agent(tmp_path)))


@pytest.mark.skipif(
    not UDS_SUPPORTED or sys.platform == "win32", reason="needs Unix domain sockets"
)
def test_replay_over_unix_socket(tmp_path: Path) -> None:
    agent = _agent(tmp_path, use_uds=True)
    assert agent.target.startswith("unix:")
    asyncio.run(_replay_after_restart(tmp_path, agent))


async def _mismatch(tmp_path: Path) -> None:
    agent = FakeAgent(AgentScript(job_spec={}, staging_dir=tmp_path, protocol_versions=("2.0",)))
    await agent.start()
    session = WorkerSession(agent.target, pb.Hello(attempt_id="att_1"), Spool(tmp_path / "spool"))
    session.start()
    try:
        await asyncio.wait_for(session.failed.wait(), 10)
        assert session.fatal is not None
        assert session.fatal.code == PROTOCOL_MISMATCH
    finally:
        await session.close()
        await agent.close()


def test_no_common_version_is_fatal(tmp_path: Path) -> None:
    asyncio.run(_mismatch(tmp_path))


async def _heartbeats_during_writes(tmp_path: Path) -> None:
    agent = _agent(tmp_path)
    await agent.start()
    session = WorkerSession(agent.target, pb.Hello(attempt_id="att_1"), Spool(tmp_path / "spool"))
    session.start()

    async def beat() -> None:
        while True:
            await session.send_live(pb.Heartbeat())
            await asyncio.sleep(0)

    beats = asyncio.create_task(beat())
    try:
        for i in range(200):
            session.send(pb.LogChunk(data=b"x" * 2000, offset=i + 1))
            await asyncio.sleep(0)
        assert await session.wait_acked(10)
        assert [m.log.offset for m in agent.received] == list(range(1, 201))
        assert agent.protocol_errors == []
    finally:
        beats.cancel()
        await session.close()
        await agent.close()


def test_heartbeats_never_collide_with_spooled_writes(tmp_path: Path) -> None:
    # Regression: concurrent call.write() from the heartbeat task killed the writer silently.
    for attempt in range(3):
        asyncio.run(_heartbeats_during_writes(tmp_path / str(attempt)))


async def _mute_agent(tmp_path: Path) -> None:
    agent = _agent(tmp_path)
    agent.mute_hellos = 1
    await agent.start()
    session = WorkerSession(agent.target, pb.Hello(attempt_id="att_1"), Spool(tmp_path / "spool"))
    session.hello_timeout_s = 0.5
    session.start()
    try:
        session.send(pb.MetricsBatch(points=[pb.MetricPoint(name="m", step=1)]))
        assert await session.wait_acked(10)
        assert agent.sessions == 2
        assert session.connections == 1
    finally:
        await session.close()
        await agent.close()


def test_an_agent_that_never_answers_the_hello_is_left_for_a_new_connection(tmp_path: Path) -> None:
    asyncio.run(_mute_agent(tmp_path))
