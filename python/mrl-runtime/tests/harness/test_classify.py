import errno
import json

import pytest

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter import FailureInfo
from mrl_runtime.checkpoint import CommitResult
from mrl_runtime.harness import ExitState, classify, decide_exit, failure
from mrl_runtime.harness.classify import Phase
from mrl_runtime.testkit.fake import FakeCudaOutOfMemoryError, FakeDataError, FakeEngine

FC = common.FailureClass


class OutOfMemoryError(RuntimeError):
    """Stands in for torch.OutOfMemoryError (module name is what the table checks)."""


OutOfMemoryError.__module__ = "torch.cuda"


def _wrapped(inner: BaseException) -> RuntimeError:
    try:
        raise inner
    except BaseException as exc:
        try:
            raise RuntimeError("training step failed") from exc
        except RuntimeError as outer:
            return outer


TABLE: list[tuple[str, BaseException, Phase, str, int, str]] = [
    ("torch oom type", OutOfMemoryError("x"), "run", "", common.FAILURE_CLASS_OOM, "OOM_GPU"),
    (
        "cuda oom text",
        FakeCudaOutOfMemoryError("CUDA out of memory."),
        "run",
        "",
        common.FAILURE_CLASS_OOM,
        "OOM_GPU",
    ),
    (
        "mps oom",
        RuntimeError("MPS backend out of memory"),
        "run",
        "",
        common.FAILURE_CLASS_OOM,
        "OOM_GPU",
    ),
    (
        "oom in child log",
        RuntimeError("exit 1"),
        "run",
        "torch.OutOfMemoryError: CUDA out of memory",
        common.FAILURE_CLASS_OOM,
        "OOM_GPU",
    ),
    (
        "oom wrapped",
        _wrapped(OutOfMemoryError("x")),
        "run",
        "",
        common.FAILURE_CLASS_OOM,
        "OOM_GPU",
    ),
    ("host oom", MemoryError(), "run", "", common.FAILURE_CLASS_OOM, "OOM_HOST"),
    (
        "shm",
        OSError(errno.ENOSPC, "No space left on device: /dev/shm/x"),
        "run",
        "",
        common.FAILURE_CLASS_INFRA_TRANSIENT,
        "SHM_EXHAUSTED",
    ),
    (
        "disk full",
        OSError(errno.ENOSPC, "No space left on device"),
        "run",
        "",
        common.FAILURE_CLASS_INFRA_TRANSIENT,
        "DISK_FULL",
    ),
    (
        "gpu fault",
        RuntimeError("CUDA error: an illegal memory access was encountered"),
        "run",
        "",
        common.FAILURE_CLASS_HARDWARE,
        "GPU_FAULT",
    ),
    (
        "nccl",
        RuntimeError("NCCL error in: ..."),
        "run",
        "",
        common.FAILURE_CLASS_HARDWARE,
        "GPU_FAULT",
    ),
    (
        "import",
        ModuleNotFoundError("No module named 'flash_attn'"),
        "prepare",
        "",
        common.FAILURE_CLASS_DEPENDENCY_ENV,
        "IMPORT_FAILED",
    ),
    (
        "fp error",
        FloatingPointError("overflow"),
        "run",
        "",
        common.FAILURE_CLASS_NUMERIC_DIVERGENCE,
        "NAN_LOSS",
    ),
    (
        "nan text",
        RuntimeError("loss is nan at step 4"),
        "run",
        "",
        common.FAILURE_CLASS_NUMERIC_DIVERGENCE,
        "NAN_LOSS",
    ),
    (
        "bad json",
        json.JSONDecodeError("x", "{", 0),
        "run",
        "",
        common.FAILURE_CLASS_DATA,
        "DATA_DECODE",
    ),
    (
        "bad utf8",
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "x"),
        "prepare",
        "",
        common.FAILURE_CLASS_DATA,
        "DATA_DECODE",
    ),
    (
        "connection",
        ConnectionResetError(),
        "run",
        "",
        common.FAILURE_CLASS_INFRA_TRANSIENT,
        "CONNECTION",
    ),
    (
        "config in prepare",
        ValueError("lora_r must be > 0"),
        "prepare",
        "",
        common.FAILURE_CLASS_USER_CONFIG,
        "CONFIG_INVALID",
    ),
    (
        "value error in run",
        ValueError("shape mismatch"),
        "run",
        "",
        common.FAILURE_CLASS_ENGINE_ERROR,
        "UNCAUGHT_EXCEPTION",
    ),
    (
        "anything else",
        RuntimeError("boom"),
        "run",
        "",
        common.FAILURE_CLASS_ENGINE_ERROR,
        "UNCAUGHT_EXCEPTION",
    ),
]


@pytest.mark.parametrize(
    ("exc", "phase", "tail", "cls", "sub"), [t[1:] for t in TABLE], ids=[t[0] for t in TABLE]
)
def test_fallback_table(exc: BaseException, phase: Phase, tail: str, cls: int, sub: str) -> None:
    f = classify(exc, phase=phase, hook=None, log_tail=tail)
    assert (FC.Name(getattr(f, "class")), f.sub_code) == (FC.Name(cls), sub)  # type: ignore[arg-type]
    assert f.classified_by == common.CLASSIFIED_BY_HARNESS
    assert f.message


def test_safety_halt_number_comes_from_the_proto() -> None:
    assert common.FAILURE_CLASS_SAFETY_HALT == 18


def test_adapter_hook_wins_and_a_broken_hook_falls_back() -> None:
    f = classify(FakeDataError("row 3"), phase="run", hook=FakeEngine().classify_failure)
    assert getattr(f, "class") == common.FAILURE_CLASS_DATA
    assert f.classified_by == common.CLASSIFIED_BY_ADAPTER

    def broken(exc: BaseException | None, code: int | None, tail: str) -> FailureInfo | None:
        raise KeyError("oops")

    f = classify(MemoryError(), phase="run", hook=broken)
    assert (getattr(f, "class"), f.sub_code) == (common.FAILURE_CLASS_OOM, "OOM_HOST")


_STALL = failure(common.FAILURE_CLASS_STALLED, "NO_PROGRESS", "stalled")
_ENGINE = failure(common.FAILURE_CLASS_ENGINE_ERROR, "UNCAUGHT_EXCEPTION", "boom")
_OK = CommitResult("ckpt_1", pb.COMMIT_STATE_COMMITTED)
_CORRUPT = CommitResult("ckpt_1", pb.COMMIT_STATE_CORRUPT)


@pytest.mark.parametrize(
    ("state", "kind", "cls"),
    [
        (
            ExitState(harness_failure=_STALL, engine_failure=_ENGINE, cancelled=True),
            pb.EXIT_KIND_FAILED,
            common.FAILURE_CLASS_STALLED,
        ),
        (
            ExitState(engine_failure=_ENGINE, cancelled=True, paused=True),
            pb.EXIT_KIND_FAILED,
            common.FAILURE_CLASS_ENGINE_ERROR,
        ),
        (
            ExitState(final_staged=True, final_commit=_CORRUPT),
            pb.EXIT_KIND_FAILED,
            common.FAILURE_CLASS_VERIFICATION_FAILED,
        ),
        (ExitState(final_staged=True), pb.EXIT_KIND_FAILED, common.FAILURE_CLASS_INFRA_TRANSIENT),
        (ExitState(cancelled=True, paused=True), pb.EXIT_KIND_CANCELLED, None),
        (ExitState(final_staged=True, cancelled=True), pb.EXIT_KIND_CANCELLED, None),
        (ExitState(paused=True), pb.EXIT_KIND_PAUSED, None),
        (ExitState(paused=True, final_staged=True, final_commit=_OK), pb.EXIT_KIND_COMPLETED, None),
        (ExitState(final_staged=True, final_commit=_OK), pb.EXIT_KIND_COMPLETED, None),
    ],
)
def test_exit_precedence(state: ExitState, kind: int, cls: int | None) -> None:
    got_kind, fail = decide_exit(state)
    assert got_kind == kind
    assert (getattr(fail, "class") if fail else None) == cls
