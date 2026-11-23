#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12,<3.13"
# ///
"""Copy the dataset schemas into mrl-runtime so an installed runtime has them. Run by `just gen`.

Writes python/mrl-runtime/src/mrl_runtime/data/schemas/*.schema.json from schemas/datasets/
(the source of truth). Files there that no longer exist upstream are removed.
Usage: schemas.py [--check]   (--check: fail if any output would change)
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "schemas" / "datasets"
TARGET = ROOT / "python" / "mrl-runtime" / "src" / "mrl_runtime" / "data" / "schemas"


def main(check: bool) -> int:
    wanted = {p.name: p.read_bytes() for p in sorted(SOURCE.glob("*.schema.json"))}
    if not wanted:
        print(f"no schemas in {SOURCE}", file=sys.stderr)
        return 1
    have = {p.name: p.read_bytes() for p in sorted(TARGET.glob("*.schema.json"))}
    stale = sorted(set(have) - set(wanted))
    changed = sorted(n for n, b in wanted.items() if have.get(n) != b)
    if check:
        for n in stale + changed:
            print(f"out of date: {TARGET / n}", file=sys.stderr)
        return 1 if stale or changed else 0
    TARGET.mkdir(parents=True, exist_ok=True)
    for n in stale:
        (TARGET / n).unlink()
    for n in changed:
        (TARGET / n).write_bytes(wanted[n])
    return 0


if __name__ == "__main__":
    sys.exit(main("--check" in sys.argv[1:]))
