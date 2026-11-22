"""classify_failure: our errors keep their code; known upstream errors get a class and a hint."""

# pyright: reportMissingImports=false

import pytest
import torch

from mrl_adapter_trl.failures import ConfigError, DatasetError, classify
from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common


def _wrapped(inner: BaseException) -> RuntimeError:
    try:
        raise RuntimeError("training step failed") from inner
    except RuntimeError as outer:
        return outer


@pytest.mark.parametrize(
    ("exc", "cls", "sub_code"),
    [
        (ConfigError("MRL-CAP-251", "x"), common.FAILURE_CLASS_USER_CONFIG, "MRL-CAP-251"),
        (DatasetError("MRL-DATA-001", "x"), common.FAILURE_CLASS_DATA, "MRL-DATA-001"),
        (torch.cuda.OutOfMemoryError("CUDA out of memory."), common.FAILURE_CLASS_OOM, "OOM_GPU"),
        (_wrapped(torch.cuda.OutOfMemoryError("x")), common.FAILURE_CLASS_OOM, "OOM_GPU"),
        (
            ValueError(
                "Cannot use chat template functions because tokenizer.chat_template is not set"
            ),
            common.FAILURE_CLASS_USER_CONFIG,
            "MRL-CAP-016",
        ),
        (
            RuntimeError("Function 'MulBackward0' returned nan values in its 0th output."),
            common.FAILURE_CLASS_NUMERIC_DIVERGENCE,
            "NAN_LOSS",
        ),
        (FloatingPointError("overflow"), common.FAILURE_CLASS_NUMERIC_DIVERGENCE, "NAN_LOSS"),
    ],
)
def test_known_failures(exc: BaseException, cls: int, sub_code: str) -> None:
    info = classify(exc, None, "")
    assert info is not None
    assert (info.failure_class, info.sub_code) == (cls, sub_code)
    assert info.message
    assert not info.retryable


def test_unknown_failures_are_left_to_the_harness() -> None:
    assert classify(RuntimeError("something else"), None, "") is None
    assert classify(None, 137, "") is None
