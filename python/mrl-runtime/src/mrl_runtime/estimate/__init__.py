"""Preflight resource estimator (Doc 07 §6): `estimate(model, config, topology) -> Estimate`."""

from mrl_runtime.estimate.hardware import (
    DeviceSpec,
    Headroom,
    Topology,
    device_from_profile,
    hardware_catalog,
)
from mrl_runtime.estimate.memory import (
    Alternative,
    Calibration,
    Component,
    DeviceEstimate,
    Estimate,
    Phase,
    RolloutConfig,
    TrainConfig,
    alternatives,
    estimate,
    lora_params,
    verdict,
)
from mrl_runtime.estimate.storage import InterconnectNeed, Storage
from mrl_runtime.estimate.units import Range

__all__ = [
    "Alternative",
    "Calibration",
    "Component",
    "DeviceEstimate",
    "DeviceSpec",
    "Estimate",
    "Headroom",
    "InterconnectNeed",
    "Phase",
    "Range",
    "RolloutConfig",
    "Storage",
    "Topology",
    "TrainConfig",
    "alternatives",
    "device_from_profile",
    "estimate",
    "hardware_catalog",
    "lora_params",
    "verdict",
]
