#!/usr/bin/env -S uv run python
"""Write the reward component list the server embeds. Run by `just gen`.

It reads the components the reward sandbox ships (mrl-rewards built-ins plus the Tier-2
`code.*` components, `mrl_rewards.code.full_registry`) and the executor's identity (the
mrl-rewards version and its pinned dependencies), and writes
crates/mrl-server/src/modules/rewards/components.json. The server seeds `reward_components`
from it and folds `executor_sha256` into every reward version's content hash.

Usage: rewards.py [--check]   fail if the file would change
"""

import hashlib
import json
import sys
import tomllib
from pathlib import Path
from typing import Any

from mrl_rewards.code import full_registry
from mrl_rewards.types import SDK_CONTRACT

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "crates/mrl-server/src/modules/rewards/components.json"
PYPROJECT = ROOT / "python/mrl-rewards/pyproject.toml"


def build() -> str:
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]
    reg = full_registry()
    executor: dict[str, Any] = {
        "package": project["name"],
        "version": project["version"],
        "dependencies": sorted(project.get("dependencies", [])),
        "components_digest": reg.digest(),
    }
    canonical = json.dumps(executor, sort_keys=True, separators=(",", ":")).encode()
    rows = []
    for row in reg.index():
        rows.append({**row, "kind": "code_exec" if row["tier"] == 2 else "builtin"})
    doc = {
        "format": "mrl-reward-components/1",
        "sdk_contract": SDK_CONTRACT,
        "executor": executor,
        "executor_sha256": hashlib.sha256(canonical).hexdigest(),
        "components": rows,
    }
    return json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: list[str]) -> int:
    text = build()
    if "--check" in argv:
        current = OUT.read_text(encoding="utf-8") if OUT.is_file() else ""
        if current != text:
            print(f"{OUT.relative_to(ROOT)} is stale: run `just gen`", file=sys.stderr)
            return 1
        return 0
    OUT.write_text(text, encoding="utf-8")
    print(f"wrote {OUT.relative_to(ROOT)} ({text.count('"ref"')} components)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
