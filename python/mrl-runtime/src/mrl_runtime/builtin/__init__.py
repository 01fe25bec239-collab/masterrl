"""mrl-builtin: the utility engine for non-training jobs, with entry-point registered tasks."""

from mrl_runtime.builtin.engine import (
    BuiltinEngine,
    discover_tasks,
    load_builtin_manifest,
    manifest,
)
from mrl_runtime.builtin.tasks import HASH_FILES, BuiltinTask, TaskContext, TaskEvent, TaskOutput

__all__ = [
    "HASH_FILES",
    "BuiltinEngine",
    "BuiltinTask",
    "TaskContext",
    "TaskEvent",
    "TaskOutput",
    "discover_tasks",
    "load_builtin_manifest",
    "manifest",
]
