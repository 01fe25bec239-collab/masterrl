import asyncio

from mrl_runtime.testkit.cli import load_profile, main
from mrl_runtime.testkit.conformance import CASES, run_suite
from mrl_runtime.testkit.fake import conformance_profile


def test_fake_engine_passes_tc1_to_tc7() -> None:
    results = asyncio.run(run_suite(conformance_profile()))
    assert [r.case for r in results] == list(CASES)
    failed = [f"{r.case}: {r.detail}" for r in results if r.status != "pass"]
    assert failed == []


def test_suite_catches_a_broken_adapter() -> None:
    profile = conformance_profile()
    broken = type(profile)(
        **{**profile.__dict__, "bad": {"steps": 3}, "objective": ("train/loss", "up")}
    )
    results = {r.case: r for r in asyncio.run(run_suite(broken, only=("TC2", "TC3")))}
    assert results["TC2"].status == "fail"
    assert results["TC3"].status == "fail"
    assert "expected up" in results["TC3"].detail


def test_cli_resolves_profiles() -> None:
    assert load_profile("fake").manifest.name == "fake"
    assert load_profile("mrl_runtime.testkit.fake:conformance_profile").engine_ref == "fake"
    assert main(["fake", "--only", "TC1", "TC6"]) == 0
