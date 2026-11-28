"""The `materialize_dataset` task (DATA_PREP job, `data_prep` sandbox): a JSONL export.

Reads a ready version (staged as `dataset`) and writes one `export/<split>.jsonl` per split,
one record per line in its stored (canonical JSON) form. Parquet exports need no job: they
are the version's shards.
"""

import json
import os
import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from huggingface_hub import HfApi

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.data.adapter import DatasetAdapter, canonical_json
from mrl_runtime.errors import TASK_INPUT_INVALID, MrlError

DATASET_INPUT = "dataset"
EXPORT_DIR = "export"
_PROGRESS_EVERY = 10_000


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    issues: list[Issue] = []
    if inputs.get("format", "jsonl") != "jsonl":
        issues.append(Issue(TASK_INPUT_INVALID, "`format` must be jsonl.", path="/inputs/format"))
    if DATASET_INPUT not in staged or not staged[DATASET_INPUT].is_dir():
        issues.append(Issue(TASK_INPUT_INVALID, f"No staged folder is named {DATASET_INPUT!r}."))
    return issues


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    data = DatasetAdapter(ctx.staged_paths[DATASET_INPUT])
    yield from _write(ctx, data, ctx.output_dir)


def _write(ctx: TaskContext, data: DatasetAdapter, root: Path) -> Iterator[TaskEvent]:
    out = root / EXPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    done = 0
    for split in sorted(data.splits):
        rel = f"{EXPORT_DIR}/{split}.jsonl"
        with (root / rel).open("w", encoding="utf-8", newline="\n") as f:
            for record in data.records(split):
                if ctx.cancelled():
                    return
                f.write(canonical_json(record) + "\n")
                done += 1
                if done % _PROGRESS_EVERY == 0:
                    yield Progress(step=done, total_steps=len(data), phase="export")
        yield TaskOutput(rel, "data", "jsonl")
    yield Progress(step=max(done, 1), total_steps=max(len(data), 1), phase="export")


MATERIALIZE_DATASET = BuiltinTask(
    name="materialize_dataset",
    job_kind="DATA_PREP",
    artifact_kind="export",
    validate=_validate,
    run=_run,
)


_REPO = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*")
_VERSION = re.compile(r"dsv_[A-Za-z0-9]+")


def _validate_push(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    issues = _validate({"format": "jsonl"}, staged)
    if not _REPO.fullmatch(str(inputs.get("repo", ""))):
        issues.append(Issue(TASK_INPUT_INVALID, "Give a Hub repository as owner/name."))
    if not _VERSION.fullmatch(str(inputs.get("version_id", ""))):
        issues.append(Issue(TASK_INPUT_INVALID, "Give a dataset version ID."))
    if not os.environ.get("MRL_CREDENTIAL_HF_TOKEN"):
        issues.append(Issue(TASK_INPUT_INVALID, "The Hub push needs a granted credential."))
    return issues


def _push(ctx: TaskContext) -> Iterator[TaskEvent]:
    repo = str(ctx.inputs["repo"])
    token = os.environ.get("MRL_CREDENTIAL_HF_TOKEN")
    if not token or not _REPO.fullmatch(repo):
        raise MrlError(TASK_INPUT_INVALID, "The Hub push needs a repository and a credential.")
    data = DatasetAdapter(ctx.staged_paths[DATASET_INPUT])
    with TemporaryDirectory(dir=ctx.output_dir.parent) as tmp:
        root = Path(tmp)
        for event in _write(ctx, data, root):
            if isinstance(event, Progress):
                yield event
        if ctx.cancelled():
            return
        hub = HfApi(endpoint="https://huggingface.co", token=token)
        hub.create_repo(repo, repo_type="dataset", private=True, exist_ok=True)
        commit = hub.upload_folder(
            repo_id=repo,
            repo_type="dataset",
            folder_path=root / EXPORT_DIR,
            path_in_repo=f"masterrl/{ctx.inputs['version_id']}",
            allow_patterns=["*.jsonl"],
            commit_message="Publish a MasterRL dataset version",
        )
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    (ctx.output_dir / "hub_push.json").write_text(
        json.dumps({"repo": repo, "commit": commit.oid}), encoding="utf-8"
    )
    yield TaskOutput("hub_push.json", "report", "json")


PUSH_DATASET_TO_HUB = BuiltinTask(
    name="push_dataset_to_hub",
    job_kind="IMPORT",
    artifact_kind="export",
    validate=_validate_push,
    run=_push,
)
