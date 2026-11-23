"""Failure classification and exit precedence (Doc 05 §14).

Order: the adapter's classify_failure() hook, then the harness fallback table below, then
ENGINE_ERROR. Messages are fixed one-liners safe to show; tracebacks go to detail_ref.
"""

import errno
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter.types import FailureInfo
from mrl_runtime.checkpoint.provider import CommitResult

Phase = Literal["probe", "validate", "prepare", "run", "export"]
Hook = Callable[[BaseException | None, int | None, str], FailureInfo | None]
FC = common.FailureClass
_log = logging.getLogger(__name__)

_GPU_OOM = re.compile(
    r"CUDA out of memory|MPS backend out of memory|HIP out of memory|"
    r"out of memory on device|CUBLAS_STATUS_ALLOC_FAILED|CUDNN_STATUS_ALLOC_FAILED",
    re.IGNORECASE,
)
_SHM = re.compile(r"/dev/shm|shared memory|bus error", re.IGNORECASE)
_GPU_FAULT = re.compile(
    r"illegal memory access|uncorrectable ECC|\bXid\b|unspecified launch failure|"
    r"GPU is lost|NCCL error|device-side assert",
    re.IGNORECASE,
)
_DIVERGED = re.compile(r"\b(loss|grad\w*)\s+(is|became|has)\s+(nan|inf)", re.IGNORECASE)


def failure(
    cls: FC,
    sub_code: str,
    message: str,
    *,
    retryable: bool = False,
    by: common.ClassifiedBy = common.CLASSIFIED_BY_HARNESS,
) -> common.Failure:
    out = common.Failure(
        sub_code=sub_code, message=message, retryable_hint=retryable, classified_by=by
    )
    setattr(out, "class", cls)  # `class` is a Python keyword
    return out


def _chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in out and len(out) < 10:
        out.append(current)
        current = current.__cause__ or current.__context__
    return out


def _type_names(exc: BaseException) -> set[str]:
    return {f"{t.__module__}.{t.__qualname__}" for t in type(exc).__mro__}


@dataclass(frozen=True)
class Rule:
    cls: FC
    sub_code: str
    message: str
    retryable: bool
    matches: Callable[[BaseException, Phase, str], bool]


def _any(exc: BaseException, test: Callable[[BaseException], bool]) -> bool:
    return any(test(e) for e in _chain(exc))


def _text(exc: BaseException) -> str:
    return "\n".join(str(e) for e in _chain(exc))


# First match wins. `tail` is the redacted log tail (child processes print OOMs there).
FALLBACK_RULES: tuple[Rule, ...] = (
    Rule(
        common.FAILURE_CLASS_OOM,
        "OOM_GPU",
        "The GPU ran out of memory.",
        False,
        lambda e, _p, tail: (
            _any(
                e,
                lambda x: any(
                    n.startswith("torch.") and n.endswith("OutOfMemoryError")
                    for n in _type_names(x)
                ),
            )
            or bool(_GPU_OOM.search(_text(e)))
            or bool(_GPU_OOM.search(tail))
        ),
    ),
    Rule(
        common.FAILURE_CLASS_INFRA_TRANSIENT,
        "SHM_EXHAUSTED",
        "Shared memory ran out.",
        True,
        lambda e, _p, _t: (
            _any(e, lambda x: isinstance(x, OSError) and x.errno == errno.ENOSPC)
            and bool(_SHM.search(_text(e)))
        ),
    ),
    Rule(
        common.FAILURE_CLASS_OOM,
        "OOM_HOST",
        "The computer ran out of memory.",
        False,
        lambda e, _p, _t: _any(e, lambda x: isinstance(x, MemoryError)),
    ),
    Rule(
        common.FAILURE_CLASS_INFRA_TRANSIENT,
        "DISK_FULL",
        "The disk is full.",
        True,
        lambda e, _p, _t: _any(e, lambda x: isinstance(x, OSError) and x.errno == errno.ENOSPC),
    ),
    Rule(
        common.FAILURE_CLASS_HARDWARE,
        "GPU_FAULT",
        "The GPU reported a hardware error.",
        True,
        lambda e, _p, tail: bool(_GPU_FAULT.search(_text(e))) or bool(_GPU_FAULT.search(tail)),
    ),
    Rule(
        common.FAILURE_CLASS_DEPENDENCY_ENV,
        "IMPORT_FAILED",
        "A library the engine needs could not be loaded.",
        True,
        lambda e, _p, _t: _any(e, lambda x: isinstance(x, ImportError)),
    ),
    Rule(
        common.FAILURE_CLASS_NUMERIC_DIVERGENCE,
        "NAN_LOSS",
        "Training numbers went out of range.",
        False,
        lambda e, _p, _t: (
            _any(e, lambda x: isinstance(x, FloatingPointError)) or bool(_DIVERGED.search(_text(e)))
        ),
    ),
    Rule(
        common.FAILURE_CLASS_DATA,
        "DATA_DECODE",
        "A dataset file could not be read.",
        False,
        lambda e, _p, _t: _any(
            e, lambda x: isinstance(x, UnicodeDecodeError | json.JSONDecodeError)
        ),
    ),
    Rule(
        common.FAILURE_CLASS_INFRA_TRANSIENT,
        "CONNECTION",
        "A connection the run needs failed.",
        True,
        lambda e, _p, _t: _any(e, lambda x: isinstance(x, ConnectionError | TimeoutError)),
    ),
    Rule(
        common.FAILURE_CLASS_USER_CONFIG,
        "CONFIG_INVALID",
        "A setting for this run has a value the engine can't use.",
        False,
        lambda e, p, _t: (
            p in ("validate", "prepare") and isinstance(e, ValueError | TypeError | KeyError)
        ),
    ),
)
DEFAULT_RULE = Rule(
    common.FAILURE_CLASS_ENGINE_ERROR,
    "UNCAUGHT_EXCEPTION",
    "The training engine stopped with an error.",
    True,
    lambda _e, _p, _t: True,
)


def classify(
    exc: BaseException, *, phase: Phase, hook: Hook | None, log_tail: str = ""
) -> common.Failure:
    if hook is not None:
        try:
            info = hook(exc, None, log_tail)
        except Exception:  # a broken hook must not hide the real failure
            _log.exception("the adapter's classify_failure hook raised; using harness defaults")
            info = None
        if info is not None:
            return failure(
                info.failure_class,
                info.sub_code,
                info.message,
                retryable=info.retryable,
                by=common.CLASSIFIED_BY_ADAPTER,
            )
    rule = next((r for r in FALLBACK_RULES if r.matches(exc, phase, log_tail)), DEFAULT_RULE)
    return failure(rule.cls, rule.sub_code, rule.message, retryable=rule.retryable)


@dataclass(frozen=True)
class ExitState:
    harness_failure: common.Failure | None = None
    engine_failure: common.Failure | None = None
    final_staged: bool = False
    final_commit: CommitResult | None = None
    cancelled: bool = False
    paused: bool = False
    # False when the engine saves a checkpoint to pause and that checkpoint did not commit.
    pause_saved: bool = True


def _final_commit_failure(state: ExitState) -> common.Failure | None:
    commit = state.final_commit
    if not state.final_staged or state.cancelled or (commit is not None and commit.committed):
        return None
    if commit is None:
        return failure(
            common.FAILURE_CLASS_INFRA_TRANSIENT,
            "COMMIT_TIMEOUT",
            "The final checkpoint was not confirmed in time.",
            retryable=True,
        )
    corrupt = commit.state == pb.COMMIT_STATE_CORRUPT
    return failure(
        common.FAILURE_CLASS_VERIFICATION_FAILED,
        "CHECKPOINT_CORRUPT" if corrupt else "CHECKPOINT_REJECTED",
        "The final checkpoint did not pass its checks.",
    )


def decide_exit(state: ExitState) -> tuple[pb.ExitKind, common.Failure | None]:
    """Exit precedence, highest first: harness-detected failure (stall, lease, divergence),
    engine failure, a final checkpoint that did not commit, user cancel, pause, completion.
    A pause that arrived after the final checkpoint was written is a completion; a pause whose
    checkpoint did not commit is a retryable failure (PAUSE_NOT_SAVED), never PAUSED."""
    fail = state.harness_failure or state.engine_failure or _final_commit_failure(state)
    if fail is not None:
        return pb.EXIT_KIND_FAILED, fail
    if state.cancelled:
        return pb.EXIT_KIND_CANCELLED, None
    if state.paused and not state.final_staged:
        if not state.pause_saved:
            return pb.EXIT_KIND_FAILED, failure(
                common.FAILURE_CLASS_INFRA_TRANSIENT,
                "PAUSE_NOT_SAVED",
                "The run stopped to pause, but its checkpoint was not saved. "
                "It will continue from the last saved checkpoint.",
                retryable=True,
            )
        return pb.EXIT_KIND_PAUSED, None
    return pb.EXIT_KIND_COMPLETED, None
