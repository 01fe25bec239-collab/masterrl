import copy
import json
from pathlib import Path
from typing import Any

import pytest

from mrl_runtime.adapter import (
    API_VERSION,
    from_document,
    load_engine,
    load_manifest,
    resolve_engine,
    version_in_range,
)
from mrl_runtime.adapter.manifest import merge
from mrl_runtime.errors import ADAPTER_API_MISMATCH, MANIFEST_INVALID, MrlError
from mrl_runtime.protocol.schemas import NAMES, schema_text
from mrl_runtime.testkit.fake import MANIFEST_DIR, FakeEngine

ROOT = Path(__file__).resolve().parents[4]
EXAMPLES = ROOT / "tests" / "contracts" / "examples" / "engine-manifest" / "v1"


def _pointer(doc: Any, pointer: str) -> tuple[Any, str | int]:
    parts = pointer.lstrip("/").split("/")
    for p in parts[:-1]:
        doc = doc[int(p)] if isinstance(doc, list) else doc[p]
    last = parts[-1]
    return doc, int(last) if isinstance(doc, list) else last


def _patched(example: Path) -> dict[str, Any]:
    patch = json.loads(example.read_text())
    doc = copy.deepcopy(json.loads((EXAMPLES / patch["base"]).read_text()))
    for pointer, value in patch.get("set", {}).items():
        parent, key = _pointer(doc, pointer)
        parent[key] = value
    for pointer in patch.get("remove", []):
        parent, key = _pointer(doc, pointer)
        del parent[key]
    return doc


def test_vendored_schemas_match_repo_schemas() -> None:
    for name in NAMES:
        assert schema_text(name) == (ROOT / "schemas" / f"{name}.schema.json").read_text(), name


@pytest.mark.parametrize("example", sorted(EXAMPLES.glob("valid-*.json")), ids=lambda p: p.name)
def test_valid_p09_examples_load(example: Path) -> None:
    m = from_document(json.loads(example.read_text()))
    assert m.algorithms
    assert version_in_range(API_VERSION, m.adapter_api)


@pytest.mark.parametrize("example", sorted(EXAMPLES.glob("invalid-*.json")), ids=lambda p: p.name)
def test_invalid_p09_examples_are_refused(example: Path) -> None:
    fails_at = json.loads(example.read_text())["fails_at"]
    with pytest.raises(MrlError) as err:
        from_document(_patched(example))
    assert err.value.code == MANIFEST_INVALID
    assert fails_at in err.value.detail


def test_fake_manifest_merges_its_fragment() -> None:
    m = load_manifest(MANIFEST_DIR)
    assert m.namespaces == {"lr": "train/learning_rate", "debug/": "fake/debug/"}
    assert m.name == "fake"
    assert isinstance(load_engine(m), FakeEngine)
    assert resolve_engine("fake").raw == m.raw
    assert resolve_engine(str(MANIFEST_DIR / "mrl-engine.yaml")).raw == m.raw


def test_fragment_may_add_but_not_change(tmp_path: Path) -> None:
    assert merge({"a": [1], "b": {"c": 1}}, {"a": [2], "b": {"d": 2}}) == {
        "a": [1, 2],
        "b": {"c": 1, "d": 2},
    }
    (tmp_path / "mrl-engine.yaml").write_text((MANIFEST_DIR / "mrl-engine.yaml").read_text())
    (tmp_path / "mrl-engine.d").mkdir()
    (tmp_path / "mrl-engine.d" / "x.yaml").write_text("metadata:\n  name: other\n")
    with pytest.raises(MrlError, match="/metadata/name"):
        load_manifest(tmp_path)


def test_duplicate_algorithm_from_fragment_is_refused(tmp_path: Path) -> None:
    (tmp_path / "mrl-engine.yaml").write_text((MANIFEST_DIR / "mrl-engine.yaml").read_text())
    (tmp_path / "mrl-engine.d").mkdir()
    base = load_manifest(MANIFEST_DIR).raw["spec"]["algorithms"][0]
    (tmp_path / "mrl-engine.d" / "dup.yaml").write_text(
        json.dumps({"spec": {"algorithms": [base]}})
    )
    with pytest.raises(MrlError, match="appears twice"):
        load_manifest(tmp_path)


def test_adapter_api_range_is_enforced() -> None:
    raw = copy.deepcopy(dict(load_manifest(MANIFEST_DIR).raw))
    raw["spec"]["adapter"]["adapterApi"] = ">=2.0,<3"
    with pytest.raises(MrlError) as err:
        load_engine(from_document(raw))
    assert err.value.code == ADAPTER_API_MISMATCH


@pytest.mark.parametrize(
    ("version", "spec", "ok"),
    [
        ("1.0", ">=1.0,<2", True),
        ("1.4", ">=1.0,<2", True),
        ("2.0", ">=1.0,<2", False),
        ("1.0", "==1", True),
        ("0.9", ">=1.0", False),
    ],
)
def test_version_ranges(version: str, spec: str, ok: bool) -> None:
    assert version_in_range(version, spec) is ok
