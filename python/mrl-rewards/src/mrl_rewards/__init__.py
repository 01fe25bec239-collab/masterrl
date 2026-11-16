"""MasterRL mrl-rewards: the reward SDK, policy runtime, built-in components and mrl-reward-exec.

The top-level import is stdlib-only; components need jsonschema/re2, and gRPC loads only with
mrl_rewards.exec.
"""

from mrl_rewards.types import (
    SDK_CONTRACT,
    ComponentSpec,
    Resolution,
    RewardBatch,
    RewardContext,
    RewardError,
    RewardProvider,
    RewardResult,
    RewardSample,
    Shape,
    Status,
)
from mrl_rewards.version import __version__

__all__ = [
    "SDK_CONTRACT",
    "ComponentSpec",
    "Resolution",
    "RewardBatch",
    "RewardContext",
    "RewardError",
    "RewardProvider",
    "RewardResult",
    "RewardSample",
    "Shape",
    "Status",
    "__version__",
]
