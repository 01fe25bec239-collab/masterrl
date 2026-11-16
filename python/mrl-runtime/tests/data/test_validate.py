"""Validators against every P03 and P09 dataset fixture, plus dedup, leakage, stats and reports."""

import json
from pathlib import Path

import pytest

from mrl_runtime.data.readers import Limits, read_records
from mrl_runtime.data.validate import (
    DATASET_SCHEMAS,
    MESSAGES,
    CheckOptions,
    RecordValidator,
    content_hashes,
    dataset_content_hashes,
    find_schema_dir,
    record_hash,
    validate_dataset,
    write_report,
)
from mrl_runtime.model import MrlError

REPO = Path(__file__).resolve().parents[4]
FOLDERS = [REPO / "tests/fixtures/datasets", REPO / "tests/contracts/examples/datasets"]


def _cases() -> list[tuple[Path, str, list[dict[str, object]], int]]:
    cases = []
    for folder in FOLDERS:
        manifest = json.loads((folder / "DEFECTS.json").read_text())
        limit = manifest.get("max_record_bytes", Limits().max_record_bytes)
        for name, defects in sorted(manifest["files"].items()):
            cases.append((folder, name, defects, limit))
    return cases


CASES = _cases()


def test_every_schema_has_fixtures() -> None:
    covered = {name.removesuffix("-invalid.jsonl") for _, name, _, _ in CASES}
    assert covered == set(DATASET_SCHEMAS)


@pytest.mark.parametrize(
    ("folder", "name", "defects", "limit"), CASES, ids=[f"{c[0].parent.name}/{c[1]}" for c in CASES]
)
def test_invalid_lines_get_their_listed_code(
    folder: Path, name: str, defects: list[dict[str, object]], limit: int
) -> None:
    schema = name.removesuffix("-invalid.jsonl")
    validator = RecordValidator(schema)
    got = []
    for raw in read_records(folder / name, None, Limits(max_record_bytes=limit)):
        codes = [raw.defect] if raw.defect else [d.code for d in validator.defects(raw.value)]
        assert codes, f"line {raw.number} passed"
        assert all(c in MESSAGES for c in codes)
        got.append((raw.number, codes[0]))
    assert got == [(d["line"], d["code"]) for d in defects]


@pytest.mark.parametrize(
    ("folder", "name", "limit"), [(c[0], c[1].replace("-invalid", ""), c[3]) for c in CASES]
)
def test_valid_files_pass(folder: Path, name: str, limit: int) -> None:
    schema = name.removesuffix(".jsonl")
    report = validate_dataset(
        folder / name, schema, CheckOptions(limits=Limits(max_record_bytes=limit))
    )
    assert report.invalid == 0, report.examples
    assert report.passed
    assert report.error_code is None
    assert report.valid == report.records > 0


def test_invalid_file_report(tmp_path: Path) -> None:
    path = REPO / "tests/fixtures/datasets/sft.messages.v1-invalid.jsonl"
    report = validate_dataset(
        path, "sft.messages.v1", CheckOptions(limits=Limits(max_record_bytes=4096))
    )
    assert not report.passed
    assert report.error_code == "MRL-DATA-001"
    assert report.defect_counts["role_order"] == 3
    assert report.examples[0].record == 1
    assert report.examples[0].message == MESSAGES["role_order"]
    report_path, stats_path = write_report(report, tmp_path / "out")
    body = json.loads(report_path.read_text())
    assert body["defect_counts"] == report.defect_counts
    assert "record_hashes" not in body
    assert json.loads(stats_path.read_text())["records"] == 0


def test_max_invalid_fraction_and_sampling(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl"
    path.write_text('{"text": "a"}\n{"text": ""}\n{"text": "b"}\n{"text": "c"}\n')
    assert not validate_dataset(path, "pretrain.text.v1").passed
    assert validate_dataset(
        path, "pretrain.text.v1", CheckOptions(max_invalid_fraction=0.25)
    ).passed
    sampled = validate_dataset(path, "pretrain.text.v1", CheckOptions(limits=Limits(max_records=2)))
    assert sampled.records == 2
    assert sampled.checks["schema"] == "sampled"
    assert sampled.checks["leakage"] == "skipped"


def test_semantic_checks() -> None:
    sft = RecordValidator("sft.messages.v1")
    ok = {
        "messages": [
            {"role": "system", "content": "s"},
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": "a"},
        ]
    }
    assert sft.defects(ok) == []
    extra = {**ok, "colour": "red"}
    assert [d.code for d in sft.defects(extra)] == ["unknown_field"]
    pair = RecordValidator("pref.pairwise.v1")
    same = [{"role": "assistant", "content": "x"}]
    assert [d.code for d in pair.defects({"prompt": "p", "chosen": same, "rejected": same})] == [
        "chosen_equals_rejected"
    ]
    kto = RecordValidator("pref.unpaired.v1")
    assert [d.code for d in kto.defects({"prompt": "p", "completion": "c", "label": 1})] == [
        "label_not_bool"
    ]
    rl = RecordValidator("rl.prompt.v1")
    assert [d.code for d in rl.defects({"reference": "4"})] == ["missing_field"]
    defect = sft.defects({"messages": [{"role": "robot", "content": "x"}]})[0]
    assert (defect.code, defect.path) == ("unknown_role", "/messages/0/role")


def test_rollout_oneof_conflict() -> None:
    rec = {
        "sample_id": "s", "group_id": "g", "batch_no": 0, "prompt": "p", "completion": "c",
        "status": "ok", "resolution": "scored", "reward_total": 1.0,
        "policy_version": 1, "version_spans": [{"start": 0, "end": 1, "policy_version": 1}],
    }  # fmt: skip
    assert [d.code for d in RecordValidator("rl.rollout.v1").defects(rec)] == ["conflicting_fields"]


def test_unknown_schema() -> None:
    with pytest.raises(MrlError) as err:
        RecordValidator("chat.v9")
    assert err.value.code == "MRL-VAL-003"


def test_duplicates(tmp_path: Path) -> None:
    path = tmp_path / "d.jsonl"
    path.write_text(
        '{"text": "Cafe\\u0301"}\n{"text": "b"}\n{"text": "Caf\\u00e9"}\n{"text": "b"}\n'
    )
    report = validate_dataset(path, "pretrain.text.v1")
    assert report.duplicates == 2
    assert report.duplicate_examples == ((3, 1), (4, 2))
    assert record_hash({"b": 1, "a": 2}) == record_hash({"a": 2, "b": 1})
    assert len(report.record_hashes) == 2


def test_leakage_against_eval_set(tmp_path: Path) -> None:
    fixtures = REPO / "tests/fixtures/datasets"
    held_out = dataset_content_hashes(fixtures / "eval.v1.jsonl", "eval.v1")
    assert len(held_out) == 50
    clean = validate_dataset(
        fixtures / "sft.messages.v1.jsonl",
        "sft.messages.v1",
        CheckOptions(against={"held-out": held_out}),
    )
    assert clean.leakage["held-out"].records == 0
    assert clean.checks["leakage"] == "full"
    first_eval = json.loads((fixtures / "eval.v1.jsonl").read_text().splitlines()[0])
    leaky = tmp_path / "leaky.jsonl"
    rows = [
        {
            "messages": [
                {"role": "user", "content": "  " + first_eval["input"].upper()},
                {"role": "assistant", "content": "x"},
            ]
        },
        {
            "messages": [
                {"role": "user", "content": "unrelated"},
                {"role": "assistant", "content": "y"},
            ]
        },
    ]
    leaky.write_text("\n".join(json.dumps(r) for r in rows))
    report = validate_dataset(
        leaky, "sft.messages.v1", CheckOptions(against={"held-out": held_out})
    )
    assert report.leakage["held-out"].records == 1
    assert report.leakage["held-out"].percent == 50.0
    assert report.leakage["held-out"].examples == (1,)
    assert content_hashes("rl.prompt.v1", {"prompt": first_eval["input"]}) <= held_out


def test_stats() -> None:
    report = validate_dataset(
        REPO / "tests/fixtures/datasets/sft.messages.v1.jsonl", "sft.messages.v1"
    )
    stats = report.stats
    assert stats.records == 200
    assert stats.roles["user"] == stats.roles["assistant"]
    assert stats.turns is not None
    assert stats.turns.min >= 2
    lengths = stats.field_lengths["messages.content"]
    assert lengths.min <= lengths.p50 <= lengths.p95 <= lengths.max


def test_schema_dir_is_found_or_named(tmp_path: Path) -> None:
    assert (find_schema_dir() / "sft.messages.v1.schema.json").is_file()
    with pytest.raises(MrlError):
        find_schema_dir(tmp_path / "x.py")
