#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Flag table and agent-command names in the plan that are not frozen.

Reads the frozen list (docs/architecture/names-frozen-v1.md, sections
"## Tables" and "## Agent commands") and scans docs/implementation/*.md.
On a line that mentions "table(s)", every back-ticked snake_case name must
be a frozen table. On a line that mentions "command(s)", every back-ticked
PascalCase name must be a frozen agent command.

Usage: names_check.py [MAIN_FOLDER]   (default: the main checkout)
"""

import re
import subprocess
import sys
from pathlib import Path

SNAKE = re.compile(r"`([a-z][a-z0-9]*(?:_[a-z0-9]+)*)`")
PASCAL = re.compile(r"`([A-Z][a-z0-9]+(?:[A-Z][a-z0-9]+)+)`")
TABLE_WORD = re.compile(r"\btables?\b", re.IGNORECASE)
COMMAND_WORD = re.compile(r"\bcommands?\b", re.IGNORECASE)


def main_folder() -> Path:
    common = subprocess.run(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return Path(common).parent


def frozen(names_md: str, heading: str) -> set[str]:
    body = names_md.split(f"\n## {heading}\n", 1)[1].split("\n## ", 1)[0]
    return set(re.findall(r"^- `([^`]+)`", body, re.MULTILINE))


def check(main: Path) -> list[str]:
    names_md = (main / "docs/architecture/names-frozen-v1.md").read_text()
    tables, commands = frozen(names_md, "Tables"), frozen(names_md, "Agent commands")
    problems = []
    for doc in sorted((main / "docs/implementation").glob("*.md")):
        for n, line in enumerate(doc.read_text().splitlines(), 1):
            where = f"{doc.relative_to(main)}:{n}"
            if TABLE_WORD.search(line):
                problems += [
                    f"{where}: table `{t}` is not frozen"
                    for t in SNAKE.findall(line)
                    if t not in tables
                ]
            if COMMAND_WORD.search(line):
                problems += [
                    f"{where}: command `{c}` is not frozen"
                    for c in PASCAL.findall(line)
                    if c not in commands
                ]
    return problems


def main(argv: list[str]) -> int:
    main_dir = Path(argv[0]) if argv else main_folder()
    problems = check(main_dir)
    for p in problems:
        print(p)
    if problems:
        print(f"names: {len(problems)} name(s) not in names-frozen-v1.md", file=sys.stderr)
        return 1
    print("names: all table and command names are frozen")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
