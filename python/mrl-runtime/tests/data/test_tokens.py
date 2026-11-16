"""Tokenization preview with the tiny-llama tokenizer and its ChatML template."""

import json
import shutil
from pathlib import Path

import make_fixtures
import pytest

from mrl_runtime.data.readers import read_records
from mrl_runtime.data.tokens import PreviewOptions, preview_tokens
from mrl_runtime.model import MrlError

DATASETS = Path(__file__).resolve().parents[4] / "tests/fixtures/datasets"


@pytest.fixture(scope="module")
def model(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("models")
    make_fixtures.build_models(out)
    return out / "tiny-llama"


def _records(schema: str) -> list[object]:
    return [r.value for r in read_records(DATASETS / f"{schema}.jsonl") if r.defect is None]


def test_sft_preview_and_assistant_mask(model: Path) -> None:
    preview = preview_tokens(model, "sft.messages.v1", _records("sft.messages.v1"))
    assert preview.records == preview.sequences == 200
    assert preview.failed == 0
    assert preview.context_length == 512
    assert preview.mask_supported
    assert preview.chat_template_sha256 == make_fixtures.sha256(
        make_fixtures.CHAT_TEMPLATE.encode()
    )
    assert sum(b.count for b in preview.histogram) == 200
    assert preview.histogram[-1].high is None
    sample = preview.samples[0]
    trained = [s.text for s in sample.segments if s.trained]
    record = _records("sft.messages.v1")[0]
    assert isinstance(record, dict)
    answers = [m["content"] for m in record["messages"] if m["role"] == "assistant"]
    assert trained == [a + "<|im_end|>" for a in answers]
    assert "".join(s.text for s in sample.segments) == sample.text
    assert sample.trained_tokens is not None
    assert 0 < sample.trained_tokens < sample.tokens


def test_over_context(model: Path) -> None:
    preview = preview_tokens(
        model,
        "sft.messages.v1",
        _records("sft.messages.v1"),
        PreviewOptions(context_length=30, bins=3),
    )
    assert preview.over_context > 0
    assert preview.over_context_pct == round(100 * preview.over_context / 200, 2)
    assert len(preview.histogram) == 4
    assert preview.histogram[-1].count == preview.over_context


@pytest.mark.parametrize(
    "schema",
    [
        "pretrain.text.v1", "sft.prompt_completion.v1", "pref.pairwise.v1", "pref.unpaired.v1",
        "rm.pairwise.v1", "rm.pointwise.v1", "rm.process.v1", "rl.prompt.v1", "agent.multiturn.v1",
        "distill.v1", "eval.v1",
    ],
)  # fmt: skip
def test_every_text_schema_previews(model: Path, schema: str) -> None:
    preview = preview_tokens(model, schema, _records(schema), PreviewOptions(max_records=10))
    assert preview.failed == 0
    assert preview.sequences >= preview.records > 0
    assert preview.lengths is not None


def test_template_failure_is_counted_not_raised(model: Path) -> None:
    preview = preview_tokens(model, "mm.messages.v1", _records("mm.messages.v1"))
    assert preview.failed == 1
    assert preview.failures[0][0] == 1


def test_template_runs_in_sandbox(model: Path, tmp_path: Path) -> None:
    folder = Path(shutil.copytree(model, tmp_path / "m"))
    config_path = folder / "tokenizer_config.json"
    config = json.loads(config_path.read_text())
    config["chat_template"] = "{{ ''.__class__.__mro__[1].__subclasses__() }}"
    config_path.write_text(json.dumps(config))
    preview = preview_tokens(folder, "sft.messages.v1", _records("sft.messages.v1")[:2])
    assert preview.failed == 2
    assert not preview.mask_supported


def test_missing_template_and_tokenizer(model: Path, tmp_path: Path) -> None:
    folder = Path(shutil.copytree(model, tmp_path / "m"))
    config_path = folder / "tokenizer_config.json"
    config = json.loads(config_path.read_text())
    del config["chat_template"]
    config_path.write_text(json.dumps(config))
    with pytest.raises(MrlError) as err:
        preview_tokens(folder, "sft.messages.v1", _records("sft.messages.v1")[:1])
    assert err.value.code == "MRL-CAP-016"
    assert preview_tokens(folder, "pretrain.text.v1", _records("pretrain.text.v1")).failed == 0
    (folder / "tokenizer.json").unlink()
    with pytest.raises(MrlError) as err:
        preview_tokens(folder, "pretrain.text.v1", [])
    assert err.value.code == "MRL-MODEL-152"
    (folder / "tokenizer.json").write_text("{broken")
    with pytest.raises(MrlError):
        preview_tokens(folder, "pretrain.text.v1", [])
