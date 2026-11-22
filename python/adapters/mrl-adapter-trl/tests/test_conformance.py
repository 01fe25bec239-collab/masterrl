"""TC1-TC7 (Doc 05 §3) on CPU, as `mrl-conformance trl` runs them."""

# pyright: reportMissingImports=false

import asyncio
from pathlib import Path

import pytest

from mrl_adapter_trl.conformance import conformance_profile
from mrl_runtime.testkit.conformance import run_suite


@pytest.mark.slow
def test_conformance_suite_passes_on_cpu(model_dir: Path) -> None:
    assert model_dir.is_dir()  # the fixture the profile stages
    results = asyncio.run(run_suite(conformance_profile()))
    failed = [f"{r.case}: {r.detail}" for r in results if r.status != "pass"]
    assert not failed
    assert [r.case for r in results] == ["TC1", "TC2", "TC3", "TC4", "TC5", "TC6", "TC7"]
