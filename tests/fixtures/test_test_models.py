"""test-models.yaml is well formed and fetch_test_models.verify catches bad files."""

import hashlib
import re
from pathlib import Path

import fetch_test_models
import pytest

MODELS = {m["level"]: m for m in fetch_test_models.load_registry()}
PARAM_RANGES = {"L1": (100e6, 200e6), "L2": (300e6, 600e6)}


def test_registry_has_l1_and_l2() -> None:
    assert set(MODELS) == {"L1", "L2"}


@pytest.mark.parametrize("level", ["L1", "L2"])
def test_model_is_pinned_and_permissive(level: str) -> None:
    model = MODELS[level]
    assert re.fullmatch(r"[0-9a-f]{40}", model["revision"])
    assert model["license"] in {"apache-2.0", "mit"}
    low, high = PARAM_RANGES[level]
    assert low <= model["params"] <= high
    assert 0 < model["vram_gb"]["min"] < model["vram_gb"]["max"] <= 6
    assert {"config.json", "model.safetensors", "tokenizer.json"} <= set(model["files"])
    assert all(re.fullmatch(r"[0-9a-f]{64}", h) for h in model["files"].values())


def test_verify_reports_missing_and_changed_files(tmp_path: Path) -> None:
    (tmp_path / "good.json").write_bytes(b"{}")
    (tmp_path / "bad.json").write_bytes(b"changed")
    files = {
        "good.json": hashlib.sha256(b"{}").hexdigest(),
        "bad.json": hashlib.sha256(b"original").hexdigest(),
        "gone.json": hashlib.sha256(b"").hexdigest(),
    }
    problems = fetch_test_models.verify(tmp_path, files)
    assert len(problems) == 2
    assert problems[0].startswith("bad.json sha256 is ")
    assert problems[1] == "gone.json is missing"
