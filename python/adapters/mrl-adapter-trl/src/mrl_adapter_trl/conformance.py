"""`mrl-conformance trl`: TC1-TC7 on CPU with the tiny-llama fixture from a repo checkout.

The testkit stages no inputs, so `ConformanceEngine` stages the fixture model and SFT dataset
when the job brings none. It is registered as its own engine (`trl-conformance`) so the real
`trl` engine never reads paths it was not given.
"""

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from mrl_adapter_trl import manifest as trl_manifest
from mrl_adapter_trl.sft import TrlSftEngine
from mrl_runtime.adapter import (
    EngineManifest,
    JobContext,
    PreparedJob,
    ValidationReport,
    ValidationRequest,
    from_document,
)
from mrl_runtime.errors import FIELD_INVALID
from mrl_runtime.testkit.conformance import ConformanceProfile

ENTRYPOINT = "mrl_adapter_trl.conformance:ConformanceEngine"
FIXTURE_HELP = "run `uv run python tools/fixtures/make_fixtures.py` at the repo root first"


def fixtures() -> dict[str, Path]:
    """The tiny model and SFT dataset of the checkout this package was installed from."""
    for parent in Path(__file__).resolve().parents:
        root = parent / "tests" / "fixtures"
        if (root / "datasets").is_dir():
            model = root / "models" / ".cache" / "tiny-llama"
            if not (model / "model.safetensors").is_file():
                raise FileNotFoundError(f"no tiny-llama fixture at {model}: {FIXTURE_HELP}")
            return {"model": model, "train": root / "datasets" / "sft.messages.v1.jsonl"}
    raise FileNotFoundError("mrl-conformance trl needs a MasterRL checkout (tests/fixtures)")


def _staged(staged: Mapping[str, Path]) -> dict[str, Path]:
    return {**fixtures(), **staged}


class ConformanceEngine(TrlSftEngine):
    """TrlSftEngine that stages the fixture model and dataset when the job stages none."""

    def validate(self, req: ValidationRequest) -> ValidationReport:
        return super().validate(replace(req, staged_paths=_staged(req.staged_paths)))

    def prepare(self, ctx: JobContext) -> PreparedJob:
        return super().prepare(replace(ctx, staged_paths=_staged(ctx.staged_paths)))


def manifest() -> EngineManifest:
    """The `trl` manifest with this module's engine as the entrypoint."""
    doc: dict[str, Any] = json.loads(json.dumps(trl_manifest().raw))
    doc["spec"]["adapter"]["entrypoint"] = ENTRYPOINT
    return from_document(doc)


# Full fine-tune on CPU: the loss falls fast enough to see in 12 steps (DG-03 spike).
_BASE: dict[str, Any] = {
    "learning_rate": 0.003,
    "per_device_train_batch_size": 8,
    "max_length": 128,
    "logging_steps": 1,
    "warmup_ratio": 0.1,
}


def conformance_profile() -> ConformanceProfile:
    return ConformanceProfile(
        manifest=manifest(),
        engine_ref="trl-conformance",
        algorithm="sft",
        good={**_BASE, "max_steps": 12, "save_steps": 6, "samples_every": 6},
        slow={**_BASE, "max_steps": 40, "save_steps": 2},
        bad={**_BASE, "learning_rate": 0},
        bad_code=FIELD_INVALID,
        objective=("train/loss", "down"),
        common={"seed": 7, "update": {"strategy": "full"}},
    )
