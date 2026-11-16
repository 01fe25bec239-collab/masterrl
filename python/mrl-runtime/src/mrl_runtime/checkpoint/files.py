"""File hashing and artifact manifests for everything the worker stages for upload."""

import hashlib
import json
import os
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mrl_runtime.errors import LOCAL_FILE_IO, MrlError
from mrl_runtime.protocol.schemas import schema_errors

ARTIFACT_MANIFEST = "artifact-manifest.json"


@dataclass(frozen=True)
class FileInfo:
    path: str  # relative to the hashed root, "/" separators
    size: int
    sha256: str


def now_rfc3339() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def hash_files(root: Path, rel_paths: Sequence[str]) -> list[FileInfo]:
    """Hash files under `root` in parallel, keeping the given order."""

    def one(rel: str) -> FileInfo:
        path = root / rel
        return FileInfo(rel, path.stat().st_size, sha256_file(path))

    workers = min(8, (os.cpu_count() or 2), max(1, len(rel_paths)))
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(one, rel_paths))
    except OSError as exc:
        raise MrlError(
            LOCAL_FILE_IO,
            "The app could not read or write a file on this computer.",
            detail=str(exc),
        ) from exc


def list_files(root: Path, *, exclude: Sequence[str] = ()) -> list[str]:
    """Every regular file under root, relative, sorted, with "/" separators."""
    if root.is_file():
        return [root.name]
    out = [p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()]
    return sorted(p for p in out if p not in exclude)


def write_artifact_manifest(
    output_dir: Path,
    entries: Sequence[tuple[str, str, str]],
    *,
    kind: str,
    source: dict[str, Any],
) -> Path:
    """Hash (path, role, format) entries under output_dir and write artifact-manifest.json."""
    infos = hash_files(output_dir, [e[0] for e in entries])
    doc: dict[str, Any] = {
        "schema_version": "artifact-manifest/v1",
        "kind": kind,
        "created_at": now_rfc3339(),
        "files": [
            {"path": i.path, "size": i.size, "sha256": i.sha256, "role": role}
            | ({"format": fmt} if fmt else {})
            for i, (_, role, fmt) in zip(infos, entries, strict=True)
        ],
        "source": source,
    }
    problems = schema_errors(doc, "artifact-manifest/v1")
    if problems:
        raise ValueError(f"artifact manifest does not match its schema: {problems}")
    path = output_dir / ARTIFACT_MANIFEST
    path.write_text(json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
    return path
