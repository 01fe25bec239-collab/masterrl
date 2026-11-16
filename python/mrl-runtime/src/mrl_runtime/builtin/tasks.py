"""The built-in task contract, and the `hash_files` task.

A task is plain data plus two functions: validate(inputs, staged_paths) -> issues, and
run(ctx) -> Progress and TaskOutput events. Outputs are files the task wrote under
ctx.output_dir; the engine hashes them into an artifact manifest for the agent to upload.
Tasks register in the `mrl_runtime.builtin_tasks` entry-point group (name = task name).
"""

import json
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.checkpoint.files import hash_files, list_files
from mrl_runtime.errors import TASK_INPUT_INVALID


@dataclass(frozen=True)
class TaskContext:
    inputs: Mapping[str, Any]
    staged_paths: Mapping[str, Path]
    output_dir: Path
    cancelled: Callable[[], bool]


@dataclass(frozen=True)
class TaskOutput:
    path: str  # relative to output_dir
    role: str
    format: str = ""


TaskEvent = Progress | TaskOutput


@dataclass(frozen=True)
class BuiltinTask:
    name: str
    job_kind: str
    artifact_kind: str  # artifact-manifest/v1 kind of what the task produces
    validate: Callable[[Mapping[str, Any], Mapping[str, Path]], list[Issue]]
    run: Callable[[TaskContext], Iterator[TaskEvent]]


# ---- hash_files: sha256 and size of every file in the staged inputs ----

HASHES_FILE = "hashes.json"
_BATCH = 16


def _hash_files_validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    raw: object = inputs.get("roles")
    roles = cast(list[object], raw) if isinstance(raw, list) else []
    if not roles or not all(isinstance(r, str) for r in roles):
        return [
            Issue(
                TASK_INPUT_INVALID,
                "hash_files needs `roles`: a list of staged input names.",
                path="/inputs/roles",
                hint='For example: {"roles": ["dataset.train"]}.',
            )
        ]
    return [
        Issue(
            TASK_INPUT_INVALID,
            f"No staged input is named {role!r}.",
            path="/inputs/roles",
            hint=f"Staged inputs: {', '.join(sorted(staged)) or 'none'}.",
        )
        for role in cast(list[str], roles)
        if role not in staged or not staged[role].exists()
    ]


def _hash_files_run(ctx: TaskContext) -> Iterator[TaskEvent]:
    plan: list[tuple[str, Path, str]] = []
    for role in ctx.inputs["roles"]:
        root = ctx.staged_paths[role]
        base = root if root.is_dir() else root.parent
        plan.extend((role, base, rel) for rel in list_files(root))
    rows: list[dict[str, Any]] = []
    for start in range(0, len(plan), _BATCH):
        if ctx.cancelled():
            return
        batch = plan[start : start + _BATCH]
        by_base: dict[Path, list[tuple[str, str]]] = {}
        for role, base, rel in batch:
            by_base.setdefault(base, []).append((role, rel))
        for base, items in by_base.items():
            for (role, _), info in zip(
                items, hash_files(base, [rel for _, rel in items]), strict=True
            ):
                rows.append(
                    {"role": role, "path": info.path, "size": info.size, "sha256": info.sha256}
                )
        yield Progress(step=start + len(batch), total_steps=len(plan), phase="hash")
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    (ctx.output_dir / HASHES_FILE).write_text(
        json.dumps({"files": rows}, indent=2), encoding="utf-8"
    )
    yield TaskOutput(HASHES_FILE, role="data", format="json")


HASH_FILES = BuiltinTask(
    name="hash_files",
    job_kind="DATA_PREP",
    artifact_kind="other",
    validate=_hash_files_validate,
    run=_hash_files_run,
)
