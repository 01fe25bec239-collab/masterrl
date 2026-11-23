"""Test helpers: one in-process attempt of the real engine against the FakeAgent."""

# pyright: reportMissingImports=false

import asyncio
import json
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from mrl_adapter_trl import manifest
from mrl_adapter_trl.sft import TrlSftEngine
from mrl_runtime.testkit.agent import AgentScript
from mrl_runtime.testkit.runner import AttemptResult, Control, job_spec, run_in_process

DATASETS = Path(__file__).resolve().parents[4] / "tests" / "fixtures" / "datasets"
# Full fine-tune of the tiny model on CPU: fast, and the loss moves (DG-03 spike).
FAST: dict[str, Any] = {
    "learning_rate": 0.003,
    "per_device_train_batch_size": 8,
    "max_length": 128,
    "logging_steps": 1,
    "warmup_ratio": 0.1,
}
FULL = {"seed": 3, "update": {"strategy": "full"}}
LORA = {"seed": 3, "update": {"strategy": "lora", "lora": {"r": 4, "alpha": 8}}}


def attempt(  # noqa: PLR0913 (keyword-only job settings)
    tmp: Path,
    algorithm: str,
    engine: Mapping[str, Any],
    staged: Mapping[str, Path],
    *,
    common: Mapping[str, Any] = FULL,
    control: Control | None = None,
    dry_run: bool = False,
    **extra: Any,
) -> AttemptResult:
    """One attempt of the real `trl` engine, in this process, against the FakeAgent."""
    staging = tmp / f"attempt-{len(list(tmp.glob('attempt-*')))}"
    staging.mkdir(parents=True)
    script = AgentScript(
        job_spec=job_spec(algorithm, engine, common=common, **extra),
        staging_dir=staging,
        staged_paths={k: str(v) for k, v in staged.items()},
        dry_run=dry_run,
    )
    return asyncio.run(run_in_process(TrlSftEngine(), manifest(), script, control=control))


def issue_codes(run: AttemptResult) -> list[str]:
    return [i.code for i in run.prepared.issues] if run.prepared else []


def copy_model(src: Path, dst: Path, **tokenizer_config: Any) -> Path:
    """A copy of the fixture model with tokenizer_config.json keys replaced (None removes)."""
    shutil.copytree(src, dst)
    path = dst / "tokenizer_config.json"
    cfg = json.loads(path.read_text(encoding="utf-8"))
    for key, value in tokenizer_config.items():
        if value is None:
            cfg.pop(key, None)
        else:
            cfg[key] = value
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return dst
