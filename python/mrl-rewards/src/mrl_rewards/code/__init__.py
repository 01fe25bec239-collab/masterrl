"""Tier-2 reward components (`builtin/code.*@1`, beta) and the jail they run code in.

`full_registry()` is the built-in set the reward sandbox serves: P14's built-ins plus these.
The component list the server embeds (`tools/gen/rewards.py`) is generated from it.
"""

from mrl_rewards.code.components import CODE_COMPONENTS, CodeCompiles, CodeUnitTests
from mrl_rewards.code.jail import Jail, JailLimits, JailResult
from mrl_rewards.registry import Registry, builtin_registry


def full_registry() -> Registry:
    """Every component the reward sandbox ships: built-ins, then the code components."""
    reg = builtin_registry()
    for cls in CODE_COMPONENTS:
        reg.register(cls)
    return reg


__all__ = [
    "CODE_COMPONENTS",
    "CodeCompiles",
    "CodeUnitTests",
    "Jail",
    "JailLimits",
    "JailResult",
    "full_registry",
]
