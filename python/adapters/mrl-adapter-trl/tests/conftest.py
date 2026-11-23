"""Session fixtures: the tiny model and SFT dataset (tools/fixtures/make_fixtures.py).

Without the fixture the tests that need it skip, unless MRL_TEST_TRL_FIXTURES_REQUIRED=1
(ci-trl sets it), which turns a missing fixture into a failure.
"""

import os
from pathlib import Path

import pytest

from mrl_adapter_trl.conformance import fixtures

REQUIRED = os.environ.get("MRL_TEST_TRL_FIXTURES_REQUIRED") == "1"


def _fixture(role: str) -> Path:
    try:
        return fixtures()[role]
    except FileNotFoundError as exc:
        if REQUIRED:
            raise
        pytest.skip(str(exc))


@pytest.fixture(scope="session")
def model_dir() -> Path:
    return _fixture("model")


@pytest.fixture(scope="session")
def train_file() -> Path:
    return _fixture("train")
