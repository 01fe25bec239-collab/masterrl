"""tiny-llama fixture: matches GOLDEN.json, rebuilds byte-identically, loads without torch."""

import json
import pickle
from pathlib import Path

import make_fixtures
import pytest
from safetensors.numpy import load_file

GOLDEN = json.loads(make_fixtures.GOLDEN_PATH.read_text())


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, object]]:
    out = tmp_path_factory.mktemp("models")
    return out, make_fixtures.build_models(out)


def test_check_passes() -> None:
    assert make_fixtures.check() == []


def test_rebuild_is_byte_identical(built: tuple[Path, dict[str, object]], tmp_path: Path) -> None:
    first, _ = built
    make_fixtures.build_models(tmp_path)
    for path in sorted(first.rglob("*.*")):
        assert (tmp_path / path.relative_to(first)).read_bytes() == path.read_bytes(), path.name


def test_weights_match_config(built: tuple[Path, dict[str, object]]) -> None:
    out, facts = built
    config = json.loads((out / "tiny-llama/config.json").read_text())
    weights = load_file(out / "tiny-llama/model.safetensors")
    assert sum(w.size for w in weights.values()) == GOLDEN["param_count"] == facts["param_count"]
    assert 900_000 < GOLDEN["param_count"] < 5_000_000
    assert weights["model.embed_tokens.weight"].shape == (
        config["vocab_size"],
        config["hidden_size"],
    )
    assert {str(w.dtype) for w in weights.values()} == {"float32"}
    assert "lm_head.weight" not in weights  # tied embeddings


def test_pickle_twin_holds_same_weights(built: tuple[Path, dict[str, object]]) -> None:
    out, _ = built
    raw = (out / "tiny-llama-pickle/pytorch_model.bin").read_bytes()
    assert raw[:2] == b"\x80\x04"  # pickle protocol 4 header: refusal tests sniff this
    twin = pickle.loads(raw)  # our own fixture, built just above
    weights = load_file(out / "tiny-llama/model.safetensors")
    assert twin.keys() == weights.keys()
    assert all((twin[k] == weights[k]).all() for k in weights)
    assert not (out / "tiny-llama-pickle/model.safetensors").exists()


def test_chat_template_marks_assistant_tokens(built: tuple[Path, dict[str, object]]) -> None:
    out, _ = built
    config = json.loads((out / "tiny-llama/tokenizer_config.json").read_text())
    template = config["chat_template"]
    assert "{%- generation -%}" in template
    assert "{%- endgeneration -%}" in template
    assert make_fixtures.sha256(template.encode()) == GOLDEN["chat_template_sha256"]
