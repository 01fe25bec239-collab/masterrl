"""Print the built-in component index as JSON: `python -m mrl_rewards.registry`."""

import json
import sys

from mrl_rewards.registry import builtin_registry

reg = builtin_registry()
json.dump({"digest": reg.digest(), "components": reg.index()}, sys.stdout, indent=2, sort_keys=True)
sys.stdout.write("\n")
