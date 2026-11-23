from pathlib import Path

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.protocol import Spool, wrap


def _log(n: int) -> pb.WorkerMessage:
    return wrap(pb.LogChunk(data=b"x" * 200, offset=n + 1))


def _samples(step: int) -> pb.WorkerMessage:
    return wrap(pb.SampleBatch(step=step + 1, samples=[pb.Sample(prompt="p" * 200)]))


def _metrics(step: int) -> pb.WorkerMessage:
    return wrap(
        pb.MetricsBatch(points=[pb.MetricPoint(name="train/loss" + "x" * 150, step=step + 1)])
    )


def test_seq_is_assigned_at_send_and_acks_free_space(tmp_path: Path) -> None:
    spool = Spool(tmp_path / "s")
    for i in range(3):
        spool.push(_log(i))
    sent = [spool.next_to_send() for _ in range(3)]
    assert [m.seq for m in sent if m] == [1, 2, 3]
    assert spool.next_to_send() is None
    spool.ack(2)
    assert len(spool) == 1
    assert len(list((tmp_path / "s").iterdir())) == 1


def test_rewind_resends_unacked_with_same_seq(tmp_path: Path) -> None:
    spool = Spool(tmp_path / "s")
    for i in range(4):
        spool.push(_log(i))
    first = [m.seq for m in iter(spool.next_to_send, None)]
    spool.rewind(3)  # agent restarted having accepted 1..2
    again = [m.seq for m in iter(spool.next_to_send, None)]
    assert first == [1, 2, 3, 4]
    assert again == [3, 4]


def test_backpressure_drops_logs_then_samples_then_thins_metrics(tmp_path: Path) -> None:
    messages = [msg for i in range(3) for msg in (_log(i), _samples(i), _metrics(i))]
    # Protobuf timestamps vary in encoded length, so size the envelopes we enqueue.
    spool = Spool(tmp_path / "s", max_bytes=sum(msg.ByteSize() for msg in messages))
    for msg in messages:
        spool.push(msg)
    assert spool.dropped == {}
    spool.push(wrap(pb.CheckpointStaged(checkpoint_id="ckpt_a" + "x" * 300)))
    assert spool.dropped["log"] >= 1
    assert "samples" not in spool.dropped
    for i in range(6):
        spool.push(wrap(pb.CheckpointCommitRequest(checkpoint_id=f"ckpt_{i}" + "x" * 300)))
    kinds = spool.kinds()
    assert "log" not in kinds  # logs went first
    assert spool.dropped["samples"] >= 1
    assert kinds.count("checkpoint_commit") == 6  # never dropped, even over budget
    assert kinds.count("checkpoint_staged") == 1
    spool.push(wrap(pb.ExitReport(kind=pb.EXIT_KIND_COMPLETED)))
    assert "exit" in spool.kinds()
    assert spool.dropped["metrics"] >= 1
    assert spool.kinds().count("metrics") >= 1  # thinned, the newest batch stays


def test_sent_messages_are_never_dropped(tmp_path: Path) -> None:
    first, second = _log(0), _log(1)
    spool = Spool(tmp_path / "s", max_bytes=first.ByteSize() + second.ByteSize())
    spool.push(first)
    spool.push(second)
    sent = spool.next_to_send()
    assert sent is not None
    spool.push(_log(2))  # over budget: drops the unsent log 1, keeps the in-flight log 0
    assert spool.dropped["log"] == 1
    spool.rewind(1)
    assert [m.seq for m in iter(spool.next_to_send, None)] == [1, 2]


def test_progress_is_coalesced(tmp_path: Path) -> None:
    spool = Spool(tmp_path / "s")
    for step in range(5):
        spool.push(wrap(pb.Progress(step=step)))
    msgs = list(iter(spool.next_to_send, None))
    assert [m.progress.step for m in msgs] == [4]
