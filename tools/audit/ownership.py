#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Check that a branch changes only the paths its part owns.

Owned paths come from the local plan: the part's row in the "## 6." table of
docs/implementation/README.md, plus the "**Owns.**" block of its lane in
docs/implementation/wave-NN.md. Paths are matched from the repo root;
`{a,b}`, `*`, `**` and `<name>` are expanded, and a path also owns everything
below it.

Usage: ownership.py PART=P04 [BASE=ref] [--plan DIR]
       (PART and BASE may also be given positionally)
BASE defaults to the latest wave-*-done tag reachable from HEAD.
"""

import itertools
import re
import subprocess
import sys
from pathlib import Path

# Mirrors the local-only block in .gitignore.
LOCAL_ONLY = ("docs/architecture/", "docs/implementation/", "AGENTS.md", "CLAUDE.md", "memory.md")


def git(*args: str, cwd: Path | None = None) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def main_folder(cwd: Path | None = None) -> Path:
    """The main checkout (worktrees do not contain the local-only plan)."""
    return Path(git("rev-parse", "--path-format=absolute", "--git-common-dir", cwd=cwd)).parent


def expand_braces(pattern: str) -> list[str]:
    start = pattern.find("{")
    if start < 0:
        return [pattern]
    depth, end, cuts = 0, -1, [start]
    for i in range(start, len(pattern)):
        c = pattern[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
        elif c == "," and depth == 1:
            cuts.append(i)
    if end < 0:
        return [pattern]
    cuts.append(end)
    head, tail = pattern[:start], pattern[end + 1 :]
    options = [pattern[a + 1 : b] for a, b in itertools.pairwise(cuts)]
    return [x for opt in options for x in expand_braces(head + opt + tail)]


def expand_bars(pattern: str) -> list[str]:
    """`src/lib.rs|main.rs` -> `src/lib.rs`, `src/main.rs`."""
    first, *rest = pattern.split("|")
    parent = first.rsplit("/", 1)[0] + "/" if "/" in first else ""
    return [first] + [parent + r for r in rest]


def to_regex(pattern: str) -> re.Pattern[str]:
    body = ""
    for token in re.split(r"(\*\*/|\*\*|\*|<[^>/]+>)", pattern.rstrip("/")):
        if token == "**/":
            body += "(?:.*/)?"
        elif token == "**":
            body += ".*"
        elif token == "*":
            body += "[^/]*"
        elif token.startswith("<") and token.endswith(">"):
            body += "[^/]+"
        else:
            body += re.escape(token)
    return re.compile(f"{body}(?:/.*)?")


def split_top_commas(chunk: str) -> list[str]:
    items, depth, start = [], 0, 0
    for i, c in enumerate(chunk):
        depth += (c == "{") - (c == "}")
        if c == "," and depth == 0:
            items.append(chunk[start:i])
            start = i + 1
    return [*items, chunk[start:]]


def backticked_paths(text: str) -> list[str]:
    paths = []
    for chunk in re.findall(r"`([^`]+)`", text):
        for raw in split_top_commas(chunk):
            item = raw.strip()
            if item and " " not in item:
                paths.append(item)
    return paths


def section(text: str, heading: re.Pattern[str]) -> str:
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if heading.match(line):
            end = next(
                (j for j in range(i + 1, len(lines)) if lines[j].startswith("## ")), len(lines)
            )
            return "\n".join(lines[i:end])
    return ""


def readme_owns(plan: Path, part: str) -> list[str]:
    table = section((plan / "README.md").read_text(), re.compile(r"^## 6\."))
    row = next((ln for ln in table.splitlines() if ln.startswith(f"| {part} |")), "")
    return backticked_paths(row.split("|", 2)[-1]) if row else []


def wave_owns(plan: Path, part: str) -> list[str]:
    lane = re.compile(rf"^## .*\b{re.escape(part)}\b")
    for wave in sorted(plan.glob("wave-*.md")):
        lines = section(wave.read_text(), lane).splitlines()
        for i, line in enumerate(lines):
            if line.startswith("**Owns.**"):
                block = [line]
                for nxt in lines[i + 1 :]:
                    if not nxt.strip():
                        break
                    block.append(nxt)
                return backticked_paths("\n".join(block))
    return []


def owned_patterns(plan: Path, part: str) -> list[re.Pattern[str]]:
    raw = readme_owns(plan, part) + wave_owns(plan, part)
    return [to_regex(p) for r in raw for b in expand_braces(r) for p in expand_bars(b)]


def is_local_only(path: str) -> bool:
    return any(path == p or (p.endswith("/") and path.startswith(p)) for p in LOCAL_ONLY)


def violations(changed: list[str], patterns: list[re.Pattern[str]]) -> list[str]:
    return [
        f for f in changed if not is_local_only(f) and not any(p.fullmatch(f) for p in patterns)
    ]


def parse_args(argv: list[str]) -> tuple[str, str | None, Path | None]:
    named: dict[str, str] = {}
    positional: list[str] = []
    plan = None
    it = iter(argv)
    for arg in it:
        if arg == "--plan":
            plan = Path(next(it))
        elif re.match(r"^(PART|BASE)=", arg):
            key, value = arg.split("=", 1)
            named[key] = value
        else:
            positional.append(arg)
    part = named.get("PART") or (positional[0] if positional else "")
    base = named.get("BASE") or (positional[1] if len(positional) > 1 else None)
    if not re.fullmatch(r"P\d{2}", part):
        raise SystemExit("usage: ownership.py PART=PNN [BASE=ref] [--plan DIR]")
    return part, base or None, plan


def main(argv: list[str]) -> int:
    part, base, plan = parse_args(argv)
    plan = plan or main_folder() / "docs" / "implementation"
    if not (plan / "README.md").is_file():
        print(f"ownership: plan not found at {plan}", file=sys.stderr)
        return 2
    patterns = owned_patterns(plan, part)
    if not patterns:
        print(f"ownership: no owned paths found for {part} in {plan}", file=sys.stderr)
        return 2
    base = base or git("describe", "--tags", "--abbrev=0", "--match", "wave-*-done", "HEAD")
    changed = git("diff", "--name-only", f"{base}...HEAD").splitlines()
    bad = violations(changed, patterns)
    for path in bad:
        print(f"not owned by {part}: {path}")
    if bad:
        print(f"ownership: {len(bad)} path(s) outside {part} (base {base})", file=sys.stderr)
        return 1
    print(f"ownership: {len(changed)} changed path(s) all owned by {part} (base {base})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
