"""The transform task: the shared filter-language cases, every step, preview, and output
checked against the target schema."""

import json
from pathlib import Path
from typing import Any

import pytest

from mrl_runtime.builtin import transform
from mrl_runtime.builtin.tasks import TaskContext
from mrl_runtime.builtin.transform import TRANSFORM, Expr, ExprError, spec_problems
from mrl_runtime.data.adapter import DatasetAdapter, ShardWriter, write_manifest

ROOT = Path(__file__).resolve().parents[4]
CASES = json.loads((ROOT / "tests/fixtures/p27/expressions.json").read_text("utf-8"))


@pytest.mark.parametrize("case", CASES["cases"], ids=lambda c: c["expr"][:40])
def test_shared_expression_cases(case: dict[str, Any]) -> None:
    if case.get("error"):
        with pytest.raises(ExprError):
            Expr.parse(case["expr"])
        return
    got = Expr.parse(case["expr"]).evaluate(CASES["record"])
    assert got == case["result"]
    assert type(got) is type(case["result"])


def _parent(root: Path, rows: list[dict[str, Any]]) -> Path:
    w = ShardWriter(root)
    for r in rows:
        w.add(r)
    write_manifest(root, "eval.v1", w.close())
    return root


QA = [{"question": f"q{i}", "answer": str(i), "lang": "en" if i % 2 else "de"} for i in range(30)]


def _spec(steps: list[dict[str, Any]], to: str = "rl.prompt.v1") -> dict[str, Any]:
    return {"schema_version": "transform/v1", "name": "t", "to": to, "steps": steps}


def _run(tmp: Path, spec: dict[str, Any], rows: list[dict[str, Any]], **inputs: Any) -> Path:
    staged = {"parent": _parent(tmp / "parent", rows)}
    body = {"spec": spec} | inputs
    assert TRANSFORM.validate(body, staged) == []
    out = tmp / "outputs"
    list(TRANSFORM.run(TaskContext(body, staged, out, lambda: False)))
    return out


def test_steps_make_a_valid_version(tmp_path: Path) -> None:
    spec = _spec(
        [
            {"op": "filter", "expr": "lang == 'en' and len(question) > 0"},
            {"op": "map_columns", "columns": {"question": "prompt", "answer": "reference"}},
            {"op": "template", "to": "prompt", "template": "Solve: {prompt}\nAnswer in \\boxed{}."},
            {"op": "cast", "field": "reference", "type": "string"},
            {"op": "select", "fields": ["prompt", "reference"]},
            {"op": "dedupe"},
            {"op": "split", "train": 0.8, "validation": 0.2, "seed": 7},
            {"op": "sample", "n": 10, "seed": 1},
        ]
    )
    out = _run(tmp_path, spec, QA)
    manifest = json.loads((out / "dataset.json").read_text("utf-8"))
    assert manifest["passed"] is True, manifest["reason"]
    assert manifest["rows"] == 10
    data = DatasetAdapter(out)
    assert data.schema_id == "rl.prompt.v1"
    assert set(data.splits) <= {"train", "validation"}
    assert len(data) == 10
    rec = next(data.records())
    assert set(rec) == {"prompt", "reference"}
    assert rec["prompt"].startswith("Solve: q")
    assert rec["prompt"].endswith("\\boxed{}.")
    steps = json.loads((out / "transform_report.json").read_text("utf-8"))["steps"]
    assert steps[0] == {"op": "filter", "in": 30, "out": 15, "dropped": {"filtered out": 15}}
    assert steps[-1]["dropped"] == {"not sampled": 5}


def test_same_spec_same_result(tmp_path: Path) -> None:
    spec = _spec(
        [
            {"op": "rename", "from": "question", "to": "prompt"},
            {"op": "select", "fields": ["prompt"]},
            {"op": "sample", "n": 7, "seed": 3},
        ]
    )
    a = _run(tmp_path / "a", spec, QA)
    b = _run(tmp_path / "b", spec, QA)
    assert (a / "record_hashes.txt").read_text("utf-8") == (b / "record_hashes.txt").read_text(
        "utf-8"
    )


def test_rows_a_step_cannot_take_are_dropped_and_counted(tmp_path: Path) -> None:
    rows = [{"q": "a", "n": "1"}, {"q": "b", "n": "x"}, {"n": "3"}, {"q": "", "n": "4"}]
    spec = _spec(
        [
            {"op": "cast", "field": "n", "type": "integer"},
            {"op": "drop_empty", "field": "q"},
            {"op": "max_length", "field": "q", "max_chars": 5},
            {"op": "to_messages", "to": "prompt", "messages": [{"role": "user", "from": "q"}]},
            {"op": "select", "fields": ["prompt"]},
        ]
    )
    out = _run(tmp_path, spec, rows)
    steps = json.loads((out / "transform_report.json").read_text("utf-8"))["steps"]
    assert steps[0]["dropped"] == {"can't make 'x' a number": 1}
    assert steps[1]["dropped"] == {"q is empty": 2}
    data = DatasetAdapter(out)
    assert list(data.records()) == [{"prompt": [{"role": "user", "content": "a"}]}]


def test_result_that_does_not_fit_the_schema_is_invalid(tmp_path: Path) -> None:
    out = _run(tmp_path, _spec([{"op": "select", "fields": ["answer"]}]), QA)
    manifest = json.loads((out / "dataset.json").read_text("utf-8"))
    assert manifest["passed"] is False
    assert manifest["error_code"] == "MRL-DATA-001"


def test_preview_shows_twenty_rows(tmp_path: Path) -> None:
    spec = _spec(
        [
            {"op": "filter", "expr": "lang == 'en'"},
            {"op": "rename", "from": "question", "to": "prompt"},
        ]
    )
    out = _run(tmp_path, spec, QA, preview=True)
    preview = json.loads((out / "preview.json").read_text("utf-8"))
    assert len(preview["rows"]) == 20
    kept, dropped = preview["rows"][1], preview["rows"][0]
    assert kept["after"] == {"prompt": "q1", "answer": "1", "lang": "en"}
    assert kept["before"]["question"] == "q1"
    assert dropped["after"] is None
    assert dropped["dropped"] == {"step": 0, "op": "filter", "reason": "filtered out"}
    assert not (out / "dataset.json").exists()


def test_preview_respects_the_json_output_byte_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(transform, "PREVIEW_MAX_BYTES", 4096)
    spec = _spec([{"op": "rename", "from": "question", "to": "prompt"}])
    rows = [{**row, "answer": "é" * 256} for row in QA]
    rows[0]["answer"] = "é" * 4096
    out = _run(tmp_path, spec, rows, preview=True)
    payload = (out / "preview.json").read_bytes()
    assert len(payload) <= transform.PREVIEW_MAX_BYTES
    preview = json.loads(payload)
    assert preview["truncated"] is True
    assert len(preview["rows"]) == 20
    assert preview["rows"][0]["truncated"] is True
    assert preview["rows"][1]["after"]["answer"] == rows[1]["answer"]
    assert preview["steps"][0]["out"] == 20


@pytest.mark.parametrize(
    ("spec", "pointer"),
    [
        (_spec([{"op": "filter", "expr": "a =="}]), "/steps/0/expr"),
        (_spec([{"op": "split", "train": 0.5}]), "/steps/0"),
        (_spec([{"op": "nope"}]), "/steps/0/op"),
        (_spec([{"op": "rename", "from": "a"}]), "/steps/0/to"),
        (_spec([{"op": "rename", "from": "a", "to": "b", "x": 1}]), "/steps/0/x"),
        (_spec([{"op": "sample", "n": 0}]), "/steps/0/n"),
        (_spec([{"op": "select", "fields": ["a b"]}]), "/steps/0/fields"),
        (_spec([], to="nope"), "/to"),
    ],
)
def test_spec_problems_point_at_the_field(spec: dict[str, Any], pointer: str) -> None:
    assert pointer in {p for p, _ in spec_problems(spec)}


def test_filter_and_cast_preserve_splits_and_large_integers(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    writer = ShardWriter(parent)
    writer.add({"text": "train", "meta": {"id": "9007199254740993"}}, "train")
    writer.add({"text": "test", "meta": {"id": "9007199254740993"}}, "test")
    schema = "pretrain.text.v1"
    write_manifest(parent, schema, writer.close())
    spec = _spec(
        [
            {"op": "filter", "expr": "true"},
            {"op": "cast", "field": "meta.id", "type": "integer"},
        ],
        to=schema,
    )
    for preview in (False, True):
        out = tmp_path / str(preview)
        body = {"spec": spec, "preview": preview}
        staged = {"parent": parent}
        assert TRANSFORM.validate(body, staged) == []
        list(TRANSFORM.run(TaskContext(body, staged, out, lambda: False)))
        if preview:
            doc = json.loads((out / "preview.json").read_text("utf-8"))
            assert {r["split"] for r in doc["rows"]} == {"train", "test"}
            assert {r["after"]["meta"]["id"] for r in doc["rows"]} == {9007199254740993}
        else:
            dataset = DatasetAdapter(out)
            assert dataset.splits == {"train": 1, "test": 1}
            assert list(dataset.records("test")) == [
                {"text": "test", "meta": {"id": 9007199254740993}},
            ]


def test_oversized_template_result_cannot_publish_a_browse_page(tmp_path: Path) -> None:
    spec = _spec(
        [{"op": "template", "to": "text", "template": "{text}" * 17}],
        to="pretrain.text.v1",
    )
    out = _run(tmp_path, spec, [{"text": "x" * (1024 * 1024)}])
    assert json.loads((out / "dataset.json").read_text("utf-8"))["passed"] is False
    assert not (out / "browse").exists()
