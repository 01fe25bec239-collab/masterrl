#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml==6.0.2"]
# ///
"""Print a markdown report: which tests cover which requirement.

A test claims a requirement with a comment tag (`req: <ID>` after `#` or
`//`) or with the pytest marker `@pytest.mark.req(<ID>)`. Several IDs may be
listed, separated by commas.
Requirements come from the local registry (a YAML list of id, text, release).

Usage: report.py [--registry FILE] [REPO_ROOT]
Exits 0 after printing; exits 2 if the registry is missing.
"""

import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import yaml

ID = r"(?:FR-[A-Z]+-\d+|NFR-\d+)"
COMMENT_TAG = re.compile(r"(?:#|//)\s*req:(.*)")
MARKER_TAG = re.compile(r"@pytest\.mark\.req\(([^)]*)\)")
REGISTRY = Path("docs/implementation/requirements/registry.yaml")


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
    ).stdout


def main_folder(root: Path) -> Path:
    return Path(git(root, "rev-parse", "--path-format=absolute", "--git-common-dir").strip()).parent


def collect_tags(root: Path) -> dict[str, list[str]]:
    tags: dict[str, list[str]] = defaultdict(list)
    for rel in filter(None, git(root, "ls-files", "-z").split("\0")):
        path = root / rel
        if not path.is_file():
            continue
        data = path.read_bytes()
        if b"\0" in data:
            continue
        for n, line in enumerate(data.decode("utf-8", "replace").splitlines(), 1):
            for m in (*COMMENT_TAG.finditer(line), *MARKER_TAG.finditer(line)):
                for req in re.findall(ID, m.group(1)):
                    tags[req].append(f"{rel}:{n}")
    return tags


def cell(text: object) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ") if text is not None else "—"


def render(registry: list[dict], tags: dict[str, list[str]]) -> str:
    known = {r["id"] for r in registry}
    covered = sum(1 for r in registry if tags.get(r["id"]))
    lines = [
        "# Requirement traceability",
        "",
        f"{len(registry)} requirements · {covered} covered · {len(registry) - covered} uncovered",
        "",
        "| Requirement | Release | Tests |",
        "|---|---|---|",
    ]
    for r in registry:
        tests = ", ".join(f"`{t}`" for t in tags.get(r["id"], [])) or "—"
        lines.append(f"| **{r['id']}** {cell(r.get('text'))} | {cell(r.get('release'))} | {tests} |")
    unknown = sorted(set(tags) - known)
    if unknown:
        lines += ["", "## Tagged in tests but not in the registry", ""]
        lines += [f"- {u}: " + ", ".join(f"`{t}`" for t in tags[u]) for u in unknown]
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    registry_path = None
    if argv[:1] == ["--registry"]:
        registry_path, argv = Path(argv[1]), argv[2:]
    root = Path(argv[0]) if argv else Path.cwd()
    registry_path = registry_path or main_folder(root) / REGISTRY
    if not registry_path.is_file():
        print(f"traceability: registry not found at {registry_path}", file=sys.stderr)
        return 2
    registry = yaml.safe_load(registry_path.read_text()) or []
    sys.stdout.write(render(registry, collect_tags(root)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
