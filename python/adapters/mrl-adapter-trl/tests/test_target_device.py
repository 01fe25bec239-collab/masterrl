"""A dry run checks and estimates for the machine the run will get, not the probe's CPU."""

# pyright: reportMissingImports=false

from mrl_adapter_trl.common import Device, detect_device
from mrl_adapter_trl.sft import _device_for
from mrl_runtime.adapter import HardwareProfile, TargetDevice


def test_the_target_device_replaces_the_local_one() -> None:
    target = TargetDevice.from_spec({"kind": "cuda", "name": "A10", "memory_gib": 24, "bf16": True})
    assert _device_for(HardwareProfile(target=target)) == Device("cuda", "A10", True, 24.0)
    assert _device_for(HardwareProfile()) == detect_device()
    assert TargetDevice.from_spec({"kind": "tpu"}) is None
    assert TargetDevice.from_spec(None) is None
