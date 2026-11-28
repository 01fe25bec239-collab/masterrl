"""The `materialize_dataset` task (DATA_PREP job, `data_prep` sandbox): a JSONL export.

Reads a ready version (staged as `dataset`) and writes one `export/<split>.jsonl` per split,
one record per line in its stored (canonical JSON) form. Parquet exports need no job: they
are the version's shards.
"""

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.data.adapter import DatasetAdapter, canonical_json
from mrl_runtime.errors import TASK_INPUT_INVALID

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
    out = ctx.output_dir / EXPORT_DIR
    out.mkdir(parents=True, exist_ok=True)
    done = 0
    for split in sorted(data.splits):
        rel = f"{EXPORT_DIR}/{split}.jsonl"
        with (ctx.output_dir / rel).open("w", encoding="utf-8", newline="\n") as f:
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
