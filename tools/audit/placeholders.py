#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Fail when tracked files contain unfinished-work markers.

Skips the root tests/ and docs/ folders and any testkit/ folder.
Prints file:line for each hit.
Usage: placeholders.py [REPO_ROOT]
"""

import subprocess
import sys
from pathlib import Path

# Joined at runtime so this file does not flag itself.
PATTERNS = (
    "todo" + "!(",
    "unimplemented" + "!(",
    "NotImplemented" + "Error",
    "TODO" + "(later)",
    "FIX" + "ME",
)
ROOT_SKIP = ("tests/", "docs/")


def is_skipped(rel: str) -> bool:
    return rel.startswith(ROOT_SKIP) or "testkit" in rel.split("/")[:-1]


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True
    ).stdout
    return [p for p in out.decode().split("\0") if p]


def scan(root: Path) -> list[str]:
    hits = []
    for rel in tracked_files(root):
        path = root / rel
        if is_skipped(rel) or not path.is_file():
            continue
        data = path.read_bytes()
        if b"\0" in data:  # binary
            continue
        for n, line in enumerate(data.decode("utf-8", "replace").splitlines(), 1):
            if any(p in line for p in PATTERNS):
                hits.append(f"{rel}:{n}: {line.strip()}")
    return hits


def main(argv: list[str]) -> int:
    root = Path(argv[0]) if argv else Path.cwd()
    hits = scan(root)
    for hit in hits:
        print(hit)
    if hits:
        print(f"placeholders: {len(hits)} unfinished-work marker(s) found", file=sys.stderr)
        return 1
    print("placeholders: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
