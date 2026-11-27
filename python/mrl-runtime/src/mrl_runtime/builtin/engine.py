"""`mrl-builtin`: the utility engine for non-training jobs (DATA_PREP first).

Job spec: {"job_kind": "DATA_PREP", "task": "<name>", "inputs": {...}, "sandbox": ...}. The
task runs with progress events; its outputs plus outputs/artifact-manifest.json
(artifact-manifest/v1) are handed to the agent (OutputsStaged), and the worker exits once they
are uploaded.
"""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import cache
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, cast

import yaml

import mrl_runtime
from mrl_runtime.adapter import (
    BaseEngine,
    EngineEvent,
    EngineManifest,
    EnvInfo,
    ExportContext,
    ExportResult,
    HardwareProfile,
    Issue,
    JobContext,
    Log,
    PreparedJob,
    ProbeReport,
    Progress,
    ResolvedConfig,
    ResourceEstimate,
    ValidationReport,
    ValidationRequest,
)
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskOutput
from mrl_runtime.checkpoint.files import write_artifact_manifest
from mrl_runtime.errors import MANIFEST_INVALID, METHOD_UNSUPPORTED, TASK_NOT_INSTALLED, MrlError

MANIFEST_FILE = Path(__file__).parent / "mrl-builtin.yaml"
TASKS_GROUP = "mrl_runtime.builtin_tasks"
# Baseline job kinds a built-in task may serve (TRAIN and EVAL belong to training engines).
JOB_KINDS = (
    "IMPORT",
    "DATA_PREP",
    "REWARD_PREVIEW",
    "MERGE_ADAPTER",
    "CONVERT",
    "QUANTIZE",
    "EXPORT",
    "GENERATE",
)


@dataclass(frozen=True)
class JobKind:
    kind: str
    sandbox: str


@dataclass(frozen=True)
class BuiltinManifest:
    engine: EngineManifest
    job_kinds: Mapping[str, JobKind]


def _invalid(detail: str) -> MrlError:
    return MrlError(
        MANIFEST_INVALID, "The engine manifest has a problem.", detail=f"mrl-builtin.yaml: {detail}"
    )


@cache
def load_builtin_manifest() -> BuiltinManifest:
    raw: object = yaml.safe_load(MANIFEST_FILE.read_text(encoding="utf-8"))
    doc = cast(dict[str, Any], raw) if isinstance(raw, dict) else {}
    if doc.get("apiVersion") != "mrl.dev/builtin/v1" or doc.get("kind") != "BuiltinManifest":
        raise _invalid("apiVersion must be mrl.dev/builtin/v1 and kind BuiltinManifest")
    try:
        meta, spec = doc["metadata"], doc["spec"]
        adapter = spec["adapter"]
        kinds = {k["kind"]: JobKind(str(k["kind"]), str(k["sandbox"])) for k in spec["jobKinds"]}
        engine = EngineManifest(
            name=str(meta["name"]),
            version=str(meta["version"]),
            entrypoint=str(adapter["entrypoint"]),
            adapter_api=str(adapter["adapterApi"]),
            worker_protocol=str(adapter["workerProtocol"]),
            pause=False,
            on_demand_checkpoint=False,
            checkpoint_format="builtin-v1",
            algorithms={},
            namespaces={},
            exports=(),
            accelerators=("cpu",),
            raw=doc,
        )
    except (KeyError, TypeError) as exc:
        raise _invalid(f"missing or malformed field {exc}") from exc
    unknown = sorted(set(kinds) - set(JOB_KINDS))
    if unknown:
        raise _invalid(f"unknown job kinds {unknown}")
    return BuiltinManifest(engine, kinds)


def manifest() -> EngineManifest:
    """Entry point for `mrl_runtime.engines`: lets `mrl-worker --engine builtin` find it."""
    return load_builtin_manifest().engine


def discover_tasks() -> tuple[dict[str, BuiltinTask], list[str]]:
    """Installed tasks, plus a problem line for each registration that cannot be used."""
    kinds = load_builtin_manifest().job_kinds
    tasks: dict[str, BuiltinTask] = {}
    problems: list[str] = []
    for ep in entry_points(group=TASKS_GROUP):
        obj: object = ep.load()
        if not isinstance(obj, BuiltinTask) or obj.name != ep.name:
            problems.append(
                f"{ep.name}: entry point {ep.value} is not a BuiltinTask named {ep.name!r}"
            )
        elif obj.job_kind not in kinds:
            problems.append(
                f"{ep.name}: job kind {obj.job_kind} is not declared in mrl-builtin.yaml"
            )
        else:
            tasks[obj.name] = obj
    return tasks, problems


class BuiltinEngine(BaseEngine):
    def __init__(self) -> None:
        super().__init__()
        self.tasks, self.problems = discover_tasks()

    def probe(self, env: EnvInfo) -> ProbeReport:
        warnings = tuple(Issue(TASK_NOT_INSTALLED, p, severity="warning") for p in self.problems)
        return ProbeReport(
            ok=True,
            upstream_versions={"mrl-runtime": mrl_runtime.__version__},
            features=dict.fromkeys(self.tasks, True),
            issues=warnings,
        )

    def _task(self, cfg: ResolvedConfig) -> BuiltinTask | None:
        return self.tasks.get(str(cfg.raw.get("task", "")))

    def validate(self, req: ValidationRequest) -> ValidationReport:
        raw = req.config.raw
        task = self._task(req.config)
        if task is None:
            return ValidationReport(
                (
                    Issue(
                        TASK_NOT_INSTALLED,
                        f"No built-in task named {raw.get('task')!r} is installed.",
                        path="/task",
                        hint=f"Installed tasks: {', '.join(sorted(self.tasks)) or 'none'}.",
                    ),
                )
            )
        kind = str(raw.get("job_kind", task.job_kind)).upper()
        if kind != task.job_kind:
            return ValidationReport(
                (
                    Issue(
                        TASK_NOT_INSTALLED,
                        f"The task {task.name} runs {task.job_kind} jobs, not {kind}.",
                        path="/job_kind",
                    ),
                )
            )
        inputs = cast(Mapping[str, Any], raw.get("inputs") or {})
        return ValidationReport(tuple(task.validate(inputs, req.staged_paths)))

    def estimate(self, cfg: ResolvedConfig, hw: HardwareProfile) -> ResourceEstimate:
        return ResourceEstimate(
            assumptions=("built-in task: CPU only, time depends on input size",)
        )

    def prepare(self, ctx: JobContext) -> PreparedJob:
        raw = ctx.config.raw
        return PreparedJob(
            effective_config={"task": raw.get("task"), "inputs": raw.get("inputs") or {}}
        )

    def run(self, ctx: JobContext) -> Iterator[EngineEvent]:
        task = self._task(ctx.config)
        if task is None:
            raise MrlError(TASK_NOT_INSTALLED, "This built-in task is not installed.")
        out = ctx.staging_dir / "outputs"
        out.mkdir(parents=True, exist_ok=True)
        inputs = cast(Mapping[str, Any], ctx.config.raw.get("inputs") or {})
        outputs: list[TaskOutput] = []
        for event in task.run(
            TaskContext(inputs, ctx.staged_paths, out, self.cancel_requested.is_set)
        ):
            if isinstance(event, Progress):
                yield event
            else:
                outputs.append(event)
        if self.cancel_requested.is_set():
            return
        write_artifact_manifest(
            out,
            [(o.path, o.role, o.format) for o in outputs],
            kind=task.artifact_kind,
            source={"task": task.name, "run_id": ctx.run_id, "attempt_id": ctx.attempt_id},
        )
        yield Log(f"{task.name}: wrote {len(outputs)} output file(s) and an artifact manifest")

    def export(self, ctx: ExportContext) -> ExportResult:
        raise MrlError(
            METHOD_UNSUPPORTED,
            "No engine here supports this method.",
            detail="mrl-builtin has no export formats",
        )

    def cleanup(self, ctx: JobContext) -> None:
        """Built-in tasks start no processes and keep no temporary state outside staging."""
