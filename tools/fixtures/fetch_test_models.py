"""Download the pinned real test models and check every file's sha256.

    uv run python tools/fixtures/fetch_test_models.py             # L1 and L2
    uv run python tools/fixtures/fetch_test_models.py --level L1

Models are listed in tests/fixtures/models/test-models.yaml, pinned by commit.
Files go to the shared Hugging Face cache (HF_HOME or HF_HUB_CACHE), so all
worktrees reuse one copy. Prints each model's local folder.
"""

import argparse
import hashlib
import sys
from pathlib import Path
from typing import Any

import yaml
from huggingface_hub import snapshot_download

REGISTRY = Path(__file__).resolve().parents[2] / "tests/fixtures/models/test-models.yaml"


def load_registry(path: Path = REGISTRY) -> list[dict[str, Any]]:
    return yaml.safe_load(path.read_text())["models"]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def verify(folder: Path, files: dict[str, str]) -> list[str]:
    """Problems with the files in `folder`; empty when every hash matches."""
    problems = []
    for name, expected in files.items():
        path = folder / name
        if not path.is_file():
            problems.append(f"{name} is missing")
        elif (actual := sha256_file(path)) != expected:
            problems.append(f"{name} sha256 is {actual}, expected {expected}")
    return problems


def fetch(model: dict[str, Any]) -> Path:
    return Path(
        snapshot_download(
            repo_id=model["repo"],
            revision=model["revision"],
            allow_patterns=list(model["files"]),
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and verify the pinned test models.")
    parser.add_argument("--level", action="append", help="L1 or L2 (repeatable; default: all)")
    args = parser.parse_args()
    models = [m for m in load_registry() if not args.level or m["level"] in args.level]
    if not models:
        parser.error(f"no model for level {args.level}")
    failed = False
    for model in models:
        folder = fetch(model)
        problems = verify(folder, model["files"])
        for problem in problems:
            print(f"error: {model['repo']}: {problem}", file=sys.stderr)
        if problems:
            print(f"Delete {folder} and run again to re-download.", file=sys.stderr)
        else:
            print(f"{model['level']} {model['repo']}@{model['revision'][:12]} ok: {folder}")
        failed |= bool(problems)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
