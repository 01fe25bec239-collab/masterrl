"""Manifest, lock pins, committed schemas, presets and config parsing."""

# pyright: reportMissingImports=false

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from mrl_adapter_trl import MANIFEST_DIR, manifest
from mrl_adapter_trl.config import ENGINE_MODELS, parse, schema_path, schema_text
from mrl_runtime.protocol.schemas import schema_errors

PROJECT = Path(__file__).resolve().parents[1]


def _algorithm(algorithm: str) -> dict[str, Any]:
    return next(a for a in manifest().raw["spec"]["algorithms"] if a["id"] == algorithm)


def _merge_patch(target: Any, patch: Any) -> Any:
    """RFC 7386 JSON Merge Patch."""
    if not isinstance(patch, dict):
        return patch
    out = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = _merge_patch(out.get(key), value)
    return out


def test_manifest_matches_schema_and_declares_the_sft_family() -> None:
    m = manifest()
    assert not schema_errors(dict(m.raw), "engine-manifest/v1")
    assert set(m.algorithms) == {"sft", "cpt"}
    assert m.checkpoint_format == "hf-trainer-v1"
    sft = _algorithm("sft")
    assert sft["requires"]["datasetSchemas"]["train"] == [
        "sft.messages.v1",
        "sft.prompt_completion.v1",
    ]
    assert _algorithm("cpt")["requires"]["datasetSchemas"]["train"] == ["pretrain.text.v1"]
    assert {c["error"]["code"] for c in m.raw["spec"]["constraints"]} == {"MRL-CAP-250"}


def test_every_environment_pins_the_current_lock() -> None:
    for env in manifest().raw["spec"]["runtime"]["environments"]:
        lock = PROJECT / env["lockfile"]
        assert hashlib.sha256(lock.read_bytes()).hexdigest() == env["lockSha256"], env["id"]


def test_cpu_lock_pins_cpu_torch_and_no_cuda_packages() -> None:
    text = (PROJECT / "runtimes" / "trainer.lock").read_text(encoding="utf-8")
    assert "torch==2.14.1+cpu" in text
    assert "nvidia-" not in text
    assert "bitsandbytes==" not in text
    assert "--hash=sha256:" in text


def test_committed_schemas_are_generated_from_the_models() -> None:
    for algorithm in ENGINE_MODELS:
        assert schema_path(algorithm).read_text(encoding="utf-8") == schema_text(algorithm), (
            "run: uv run python -m mrl_adapter_trl.config"
        )
        ref = _algorithm(algorithm)["config"]["schema"]
        assert (MANIFEST_DIR / ref).resolve() == schema_path(algorithm).resolve()


def test_schema_marks_pinned_fields_and_ui_tiers() -> None:
    schema = json.loads(schema_text("cpt"))
    fields = schema["$defs"]["CptEngine"]["properties"]
    assert fields["assistant_only_loss"]["x-mrl-ui"]["pinned"] is True
    assert fields["loss_type"]["x-mrl-ui"]["pinned"] is True
    assert fields["num_train_epochs"]["x-mrl-ui"]["tier"] == "beginner"


@pytest.mark.parametrize(
    "preset", ["sft-lora-beginner", "sft-lora-standard", "sft-full", "sft-qlora-24gb"]
)
def test_every_preset_resolves_to_a_valid_config(preset: str) -> None:
    sft = _algorithm("sft")
    patch = next(p for p in sft["config"]["presets"] if p["id"] == preset)["patch"]
    doc = _merge_patch({"common": {"seed": 1}, "engine": {}}, patch)
    settings, issues = parse("sft", doc["common"], doc["engine"], {}, sft["switch"])
    assert not issues
    assert settings is not None
    assert settings.strategy == doc["common"]["update"]["strategy"]


def test_pinned_values_cannot_change() -> None:
    switch = _algorithm("cpt")["switch"]
    _, issues = parse("cpt", {}, {"assistant_only_loss": True}, {}, switch)
    assert (issues[0].code, issues[0].path) == ("MRL-CAP-012", "/engine/assistant_only_loss")
    _, issues = parse("cpt", {}, {}, {"loss_type": "dft"}, switch)
    assert issues[0].code == "MRL-CAP-012"
    settings, issues = parse("cpt", {}, {"loss_type": "nll"}, switch, switch)
    assert not issues
    assert settings is not None
    assert settings.engine.loss_type == "nll"


def test_engine_block_is_closed_and_common_block_is_open() -> None:
    switch = _algorithm("sft")["switch"]
    _, issues = parse("sft", {}, {"learning_rat": 0.1}, {}, switch)
    assert [(i.code, i.path) for i in issues] == [("MRL-VAL-003", "/engine/learning_rat")]
    settings, issues = parse("sft", {"budget": {"hours": 2}, "seed": 4}, {}, {}, switch)
    assert not issues
    assert settings is not None
    assert settings.common.seed == 4


@pytest.mark.parametrize(
    ("engine", "path"),
    [
        ({"learning_rate": 0}, "/engine/learning_rate"),
        ({"max_steps": 0}, "/engine/max_steps"),
        ({"per_device_train_batch_size": True}, "/engine/per_device_train_batch_size"),
        ({"optim": "lion"}, "/engine/optim"),
    ],
)
def test_bad_engine_values_name_their_field(engine: dict[str, Any], path: str) -> None:
    _, issues = parse("sft", {}, engine, {}, _algorithm("sft")["switch"])
    assert [i.path for i in issues] == [path]


def test_hostile_lora_targets_are_refused() -> None:
    common = {"update": {"strategy": "lora", "lora": {"target_modules": ["q_proj", "../x"]}}}
    _, issues = parse("sft", common, {}, {}, {})
    assert issues
    assert issues[0].path.startswith("/common/update/lora/target_modules")
