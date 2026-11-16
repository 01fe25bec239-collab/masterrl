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
    Range,
    RolloutConfig,
    TrainConfig,
    alternatives,
    estimate,
    lora_params,
    verdict,
)

__all__ = [
    "Alternative",
    "Calibration",
    "Component",
    "DeviceEstimate",
    "DeviceSpec",
    "Estimate",
    "Headroom",
    "Phase",
    "Range",
    "RolloutConfig",
    "Topology",
    "TrainConfig",
    "alternatives",
    "device_from_profile",
    "estimate",
    "hardware_catalog",
    "lora_params",
    "verdict",
]
