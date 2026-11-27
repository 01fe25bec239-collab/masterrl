"""The export_model task: a standard Hugging Face folder, sent to an artifact, S3 or the Hub."""

import contextlib
import datetime
import json
import os
import urllib.request
import uuid
from collections.abc import Mapping
from pathlib import Path

import make_fixtures
import pytest

from mrl_runtime.builtin import discover_tasks, load_builtin_manifest
from mrl_runtime.builtin.export_model import (
    EXPORT_MODEL,
    RESULT_FILE,
    S3,
    model_card,
    s3_request,
)
from mrl_runtime.builtin.import_model import IMPORT_MODEL, INSPECTION_FILE
from mrl_runtime.builtin.tasks import TaskContext, TaskOutput

CARD = {"name": "tiny", "version": "mv_x", "revision": "abc", "license": "apache-2.0"}


@pytest.fixture(scope="module")
def models(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("models")
    make_fixtures.build_models(out)
    return out


def export(staged: Path, out: Path, target: Mapping[str, object] | None = None) -> list[TaskOutput]:
    inputs = {
        "card": CARD,
        "lineage": {"model_version": "mv_x"},
        "target": target or {"kind": "artifact"},
    }
    assert EXPORT_MODEL.validate(inputs, {"model": staged}) == []
    ctx = TaskContext(inputs, {"model": staged}, out, lambda: False)
    return [e for e in EXPORT_MODEL.run(ctx) if isinstance(e, TaskOutput)]


def test_registered_for_export_jobs() -> None:
    tasks, _ = discover_tasks()
    assert tasks["export_model"] is EXPORT_MODEL
    assert load_builtin_manifest().job_kinds["EXPORT"].sandbox == "fetch"
    assert load_builtin_manifest().job_kinds["MERGE_ADAPTER"].sandbox == "data_prep"


def test_bundle_has_card_lineage_and_reimports_with_the_same_hashes(
    models: Path, tmp_path: Path
) -> None:
    outs = export(models / "tiny-llama", tmp_path / "out")
    paths = [o.path for o in outs]
    assert "export/model.safetensors" in paths
    assert "export/README.md" in paths
    assert "export/mrl-lineage.json" in paths
    assert paths[-1] == RESULT_FILE
    folder = tmp_path / "out/export"
    assert "license: apache-2.0" in (folder / "README.md").read_text()
    assert json.loads((tmp_path / "out" / RESULT_FILE).read_text()) == {"kind": "artifact"}
    # Re-import: the same inspection as the source, and the card's license is found.
    ctx = TaskContext(
        {"source": {"kind": "staged"}}, {"model": folder}, tmp_path / "re", lambda: False
    )
    list(IMPORT_MODEL.run(ctx))
    again = json.loads((tmp_path / "re" / INSPECTION_FILE).read_text())
    assert again["license"]["spdx"] == "apache-2.0"
    src = {f["path"]: f for f in json.loads(json.dumps(again))["files"]}
    assert (
        src["model.safetensors"]["size"] == (models / "tiny-llama/model.safetensors").stat().st_size
    )
    assert again["chat_template_sha256"] == make_fixtures_golden()["chat_template_sha256"]


def make_fixtures_golden() -> dict[str, object]:
    return json.loads(make_fixtures.GOLDEN_PATH.read_text())


def test_gguf_and_missing_secrets_are_refused(models: Path) -> None:
    staged = {"model": models / "tiny-llama"}
    issues = EXPORT_MODEL.validate({"format": "gguf"}, staged)
    assert [(i.code, i.path) for i in issues] == [("MRL-MODEL-261", "/inputs/format")]
    issues = EXPORT_MODEL.validate(
        {"target": {"kind": "s3", "url": "s3://bucket/p", "credential_env": "MRL_CREDENTIAL_NONE"}},
        staged,
    )
    assert [i.path for i in issues] == ["/inputs/target/credential_env"]
    assert "license: unknown" in model_card({})


def test_s3_signature_matches_the_aws_example() -> None:
    # AWS's published Signature V4 example key pair, at a fixed time.
    creds = {
        "access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    }
    s3 = S3("https://s3.amazonaws.com", "us-east-1", creds)
    when = datetime.datetime(2013, 5, 24, tzinfo=datetime.UTC)
    req = s3_request(s3, "GET", "examplebucket", "test.txt", when)
    auth = req.get_header("Authorization")
    assert auth is not None
    assert auth.startswith(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="
    )
    assert req.full_url == "https://s3.amazonaws.com/examplebucket/test.txt"


@pytest.mark.skipif(
    not os.environ.get("MRL_TEST_S3_ENDPOINT"),
    reason="set MRL_TEST_S3_ENDPOINT (just dev: http://127.0.0.1:8333)",
)
def test_s3_export_lands_in_the_bucket(
    models: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint = os.environ["MRL_TEST_S3_ENDPOINT"]
    bucket = os.environ.get("MRL_TEST_S3_BUCKET", "masterrl-dev")
    creds = {"access_key_id": "mrl-dev", "secret_access_key": "mrl-dev-only"}
    monkeypatch.setenv("MRL_CREDENTIAL_S3", json.dumps(creds))
    s3 = S3(endpoint, "us-east-1", creds)
    put = s3_request(s3, "PUT", bucket, "")
    with contextlib.suppress(OSError):  # the bucket may exist already
        urllib.request.urlopen(put, timeout=30).read()
    prefix = f"exports/{uuid.uuid4().hex}"
    target = {
        "kind": "s3",
        "url": f"s3://{bucket}/{prefix}",
        "credential_env": "MRL_CREDENTIAL_S3",
        "endpoint": endpoint,
    }
    export(models / "tiny-llama", tmp_path / "out", target)
    result = json.loads((tmp_path / "out" / RESULT_FILE).read_text())
    assert f"{prefix}/model.safetensors" in result["keys"]
    get = s3_request(s3, "GET", bucket, f"{prefix}/model.safetensors")
    with urllib.request.urlopen(get, timeout=30) as resp:
        assert resp.read() == (models / "tiny-llama/model.safetensors").read_bytes()


def test_export_loads_in_vanilla_transformers(models: Path, tmp_path: Path) -> None:
    torch = pytest.importorskip("torch")
    transformers = pytest.importorskip("transformers")
    export(models / "tiny-llama", tmp_path / "out")
    folder = tmp_path / "out/export"
    model = transformers.AutoModelForCausalLM.from_pretrained(folder)
    tok = transformers.AutoTokenizer.from_pretrained(folder)
    ids = tok("hello", return_tensors="pt").input_ids
    with torch.no_grad():
        out = model.generate(ids, max_new_tokens=4, do_sample=False)
    assert out.shape[1] == ids.shape[1] + 4
