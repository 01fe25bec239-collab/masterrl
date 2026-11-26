"""FakeEngine: a deterministic CPU-only TrainingEngine for harness tests and conformance.

Engine config (job spec `engine` block), all optional:
  steps (6), checkpoint_every (2), step_seconds (0), learning_rate (0.3), samples_every (0),
  emit_undeclared (false), children (0: sleeping child processes, reaped by cleanup),
  fail_at_step (none) with failure: engine | oom | data | nan | hang.
The loss at step s depends only on (seed, s), so a resumed run continues the same series.
"""

import json
import math
import random
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import mrl_runtime
from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime.adapter import (
    BaseEngine,
    CheckpointReady,
    EngineEvent,
    EngineManifest,
    EnvInfo,
    ExportContext,
    ExportResult,
    FailureInfo,
    HardwareProfile,
    Issue,
    JobContext,
    Metrics,
    PreparedJob,
    ProbeReport,
    Progress,
    ResolvedConfig,
    ResourceEstimate,
    RolloutSample,
    Samples,
    ValidationReport,
    ValidationRequest,
    load_manifest,
)
from mrl_runtime.errors import FIELD_INVALID
from mrl_runtime.testkit.conformance import ConformanceProfile

MANIFEST_DIR = Path(__file__).parent / "fake_engine"
STATE_FILE = "trainer_state.json"
WEIGHTS_FILE = "weights.bin"
_FAILURES = ("engine", "oom", "data", "nan", "hang")
_DEFAULTS: dict[str, Any] = {
    "steps": 6,
    "checkpoint_every": 2,
    "step_seconds": 0.0,
    "learning_rate": 0.3,
    "samples_every": 0,
    "emit_undeclared": False,
    "children": 0,
    "fail_at_step": None,
    "failure": "engine",
}


class FakeDataError(Exception):
    """Raised by failure=data; FakeEngine.classify_failure maps it to DATA."""


class FakeCudaOutOfMemoryError(RuntimeError):
    """Looks like a CUDA OOM to the harness fallback table (no torch needed)."""


def manifest() -> EngineManifest:
    return load_manifest(MANIFEST_DIR)


def fake_loss(seed: int, step: int, lr: float) -> float:
    return 2.0 * math.exp(-lr * step) + 0.01 * random.Random(seed * 100_003 + step).random()


def _config(cfg: ResolvedConfig) -> dict[str, Any]:
    return {**_DEFAULTS, **cfg.engine}


class FakeEngine(BaseEngine):
    def __init__(self) -> None:
        super().__init__()
        self._children: list[subprocess.Popen[bytes]] = []

    def probe(self, env: EnvInfo) -> ProbeReport:
        return ProbeReport(ok=True, upstream_versions={"mrl-runtime": mrl_runtime.__version__})

    def validate(self, req: ValidationRequest) -> ValidationReport:
        issues: list[Issue] = []
        engine = req.config.engine
        for key in engine:
            if key not in _DEFAULTS:
                issues.append(
                    Issue(
                        FIELD_INVALID,
                        f"The fake engine has no setting {key!r}.",
                        path=f"/engine/{key}",
                    )
                )
        cfg = _config(req.config)
        ints = ("steps", "checkpoint_every", "samples_every", "children")
        for key in ints:
            if not isinstance(cfg[key], int) or isinstance(cfg[key], bool) or cfg[key] < 0:
                issues.append(
                    Issue(
                        FIELD_INVALID,
                        f"{key} must be a whole number of 0 or more.",
                        path=f"/engine/{key}",
                    )
                )
        if isinstance(cfg["steps"], int) and cfg["steps"] < 1:
            issues.append(
                Issue(
                    FIELD_INVALID,
                    "steps must be at least 1.",
                    path="/engine/steps",
                    hint="Set steps to 1 or more.",
                )
            )
        if not isinstance(cfg["learning_rate"], int | float) or cfg["learning_rate"] <= 0:
            issues.append(
                Issue(FIELD_INVALID, "learning_rate must be above 0.", path="/engine/learning_rate")
            )
        if cfg["failure"] not in _FAILURES:
            issues.append(
                Issue(
                    FIELD_INVALID,
                    f"failure must be one of {', '.join(_FAILURES)}.",
                    path="/engine/failure",
                )
            )
        return ValidationReport(tuple(issues))

    def estimate(self, cfg: ResolvedConfig, hw: HardwareProfile) -> ResourceEstimate:
        c = _config(cfg)
        wall = float(c["steps"]) * float(c["step_seconds"])
        return ResourceEstimate(
            (0.0, 0.0), int(c["steps"]), (wall, wall * 1.5 + 1), ("fake engine: no GPU",)
        )

    def prepare(self, ctx: JobContext) -> PreparedJob:
        cfg = _config(ctx.config)
        return PreparedJob(
            effective_config={**cfg, "seed": self._seed(ctx)}, total_steps=int(cfg["steps"])
        )

    def run(self, ctx: JobContext) -> Iterator[EngineEvent]:
        cfg = _config(ctx.config)
        steps, every, lr = (
            int(cfg["steps"]),
            int(cfg["checkpoint_every"]),
            float(cfg["learning_rate"]),
        )
        seed, fail_at = self._seed(ctx), cfg["fail_at_step"]
        for _ in range(int(cfg["children"])):
            self._children.append(
                subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
            )
        for step in range(self._resume_step(ctx) + 1, steps + 1):
            if self.cancel_requested.is_set():
                return
            if step == fail_at and cfg["failure"] != "nan":
                self._fail(str(cfg["failure"]))
            time.sleep(float(cfg["step_seconds"]))
            loss = (
                math.nan
                if cfg["failure"] == "nan" and fail_at and step >= fail_at
                else fake_loss(seed, step, lr)
            )
            yield Progress(step=step, total_steps=steps, epoch=step / steps)
            values = {"loss": loss, "lr": lr}
            if cfg["emit_undeclared"]:
                values["debug/grad_norm"] = 1.0 / step
            yield Metrics(step=step, values=values)
            if cfg["samples_every"] and step % int(cfg["samples_every"]) == 0:
                yield Samples(
                    step,
                    (RolloutSample(f"s{step}", prompt=f"count to {step}", completion=str(step)),),
                )
            final = step == steps
            wanted = self.checkpoint_requested.is_set() or self.pause_requested.is_set()
            if final or wanted or (every and step % every == 0):
                self.checkpoint_requested.clear()
                # One log line per checkpoint. stderr: the harness captures it into LogChunks,
                # and stdout stays clean for callers that run the engine in-process and parse it.
                print(
                    f"fake: checkpoint at step {step}, loss {loss:.4f}", file=sys.stderr, flush=True
                )
                yield CheckpointReady(
                    step, self._write(ctx, step, seed), is_final=final, metrics={"train/loss": loss}
                )
            if self.pause_requested.is_set():
                return

    def export(self, ctx: ExportContext) -> ExportResult:
        shutil.copyfile(ctx.checkpoint_path / WEIGHTS_FILE, ctx.output_dir / "model.safetensors")
        return ExportResult(ctx.format, (("model.safetensors", "weights"),))

    def cleanup(self, ctx: JobContext) -> None:
        for child in self._children:
            child.kill()
            child.wait()
        self._children.clear()

    def classify_failure(
        self, exc: BaseException | None, exit_code: int | None, log_tail: str
    ) -> FailureInfo | None:
        if isinstance(exc, FakeDataError):
            return FailureInfo(
                common.FAILURE_CLASS_DATA,
                "DATA_FAKE_ROW",
                "A row in the dataset could not be read.",
            )
        return None

    def _seed(self, ctx: JobContext) -> int:
        return int(ctx.config.common.get("seed", 0))

    def _resume_step(self, ctx: JobContext) -> int:
        if ctx.resume_from is None:
            return 0
        return int(json.loads((ctx.resume_from / STATE_FILE).read_text())["step"])

    def _write(self, ctx: JobContext, step: int, seed: int) -> Path:
        path = ctx.work_dir / f"checkpoint-{step}"
        path.mkdir(parents=True, exist_ok=True)
        (path / STATE_FILE).write_text(json.dumps({"step": step, "seed": seed}))
        (path / WEIGHTS_FILE).write_bytes(random.Random(seed + step).randbytes(256))
        return path

    def _fail(self, kind: str) -> None:
        if kind == "oom":
            raise FakeCudaOutOfMemoryError("CUDA out of memory. Tried to allocate 2.00 GiB")
        if kind == "data":
            raise FakeDataError("row 17: expected a list of messages")
        if kind == "hang":
            while True:  # ignores every flag: only the harness deadline or the agent ends it
                time.sleep(0.05)
        raise RuntimeError("the fake engine failed on purpose")


def conformance_profile() -> ConformanceProfile:
    return ConformanceProfile(
        manifest=manifest(),
        engine_ref="fake",
        algorithm="x-fake.steps",
        good={"steps": 6, "checkpoint_every": 2, "samples_every": 3},
        slow={"steps": 40, "checkpoint_every": 2, "step_seconds": 0.05, "children": 1},
        bad={"steps": 0},
        bad_code=FIELD_INVALID,
        objective=("train/loss", "down"),
    )
