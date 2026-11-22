"""Failure classification for the TRL engines (the harness's fallback table runs after this).

Our own errors carry a guidance code and a plain message. Upstream errors we know get a class
and a next step; anything else is left to the harness (it returns ENGINE_ERROR).
"""

import re

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime.adapter import FailureInfo

NO_CHAT_TEMPLATE = "MRL-CAP-016"
_NO_TEMPLATE = re.compile(r"chat_template is not set|no chat template", re.IGNORECASE)
_NO_MASK = re.compile(
    r"chat template is not training-compatible|doesn't generate assistant masks|"
    r"missing the `\{% generation %\}` keyword",
    re.IGNORECASE,
)
_NAN = re.compile(
    r"returned nan values|probability tensor contains either `inf`, `nan`|"
    r"\b(loss|grad\w*)\s+(is|became|has)\s+(nan|inf)",
    re.IGNORECASE,
)


class AdapterError(Exception):
    """A failure the adapter found itself: a guidance code, a class and a plain message."""

    failure_class: "common.FailureClass" = common.FAILURE_CLASS_ENGINE_ERROR

    def __init__(self, code: str, message: str, hint: str = "") -> None:
        super().__init__(f"{code}: {message}" + (f" {hint}" if hint else ""))
        self.code = code
        self.message = message
        self.hint = hint


class ConfigError(AdapterError):
    """A setting, model or checkpoint this run can't use (USER_CONFIG, not retried)."""

    failure_class = common.FAILURE_CLASS_USER_CONFIG


class DatasetError(AdapterError):
    """A dataset record this run can't use (DATA, not retried)."""

    failure_class = common.FAILURE_CLASS_DATA


def _chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in out and len(out) < 10:
        out.append(current)
        current = current.__cause__ or current.__context__
    return out


def _is_cuda_oom(exc: BaseException) -> bool:
    return any(
        cls.__name__ == "OutOfMemoryError" and cls.__module__.startswith("torch")
        for cls in type(exc).__mro__
    ) or "CUDA out of memory" in str(exc)


def classify(  # noqa: PLR0911 (one return per known failure)
    exc: BaseException | None, exit_code: int | None, log_tail: str
) -> FailureInfo | None:
    """TrainingEngine.classify_failure for the TRL engines."""
    if exc is None:
        return None
    for e in _chain(exc):
        if isinstance(e, AdapterError):
            message = f"{e.message} {e.hint}".strip()
            return FailureInfo(e.failure_class, e.code, message)
        if _is_cuda_oom(e):
            return FailureInfo(
                common.FAILURE_CLASS_OOM,
                "OOM_GPU",
                "The GPU ran out of memory. Lower the batch size or max length, turn on "
                "gradient checkpointing, or use QLoRA.",
            )
        text = str(e)
        upstream = isinstance(e, ValueError | RuntimeError | KeyError)
        if upstream and _NO_TEMPLATE.search(text):
            return FailureInfo(
                common.FAILURE_CLASS_USER_CONFIG,
                NO_CHAT_TEMPLATE,
                "This model has no chat template, so it can't read conversations. Pick a chat "
                "model, or use prompt/completion text.",
            )
        if upstream and _NO_MASK.search(text):
            return FailureInfo(
                common.FAILURE_CLASS_USER_CONFIG,
                NO_CHAT_TEMPLATE,
                "This model's chat template can't mark the assistant's turns. Turn off "
                "assistant_only_loss, or pick a model whose template supports it.",
            )
        if isinstance(e, FloatingPointError) or _NAN.search(text):
            return FailureInfo(
                common.FAILURE_CLASS_NUMERIC_DIVERGENCE,
                "NAN_LOSS",
                "Training numbers went out of range. Lower the learning rate, or use bf16 "
                "instead of fp16.",
            )
    return None
