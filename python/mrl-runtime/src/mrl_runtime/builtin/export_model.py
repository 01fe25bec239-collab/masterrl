"""The `export_model` task (EXPORT, `fetch` sandbox): a model version as a standard Hugging Face
folder that loads without MasterRL.

Staged input: `model` (the version's files, already safetensors). Inputs:
- `format`: `hf-safetensors` (the default; GGUF and quantized formats are refused with
  MRL-MODEL-261 until a later release);
- `card`: facts for the generated `README.md` (name, version, revision, base, source,
  license), and `lineage` written as `mrl-lineage.json`;
- `target`: `{"kind": "artifact"}` (the default: download it from the app),
  `{"kind": "s3", "url": "s3://bucket/prefix", "credential_env": "MRL_CREDENTIAL_X",
  "endpoint": "https://…", "region": "us-east-1"}` (the secret holds
  `{"access_key_id", "secret_access_key", "session_token"?}`), or
  `{"kind": "hf_hub", "repo": "org/name", "private": true, "credential_env": "…"}` (the secret
  is a Hub write token). Secrets come in through the job's grant, never through the inputs.

Outputs: `export/…` (the folder, always kept as the job's artifact) and `export-result.json`
(where it was sent: S3 keys or the Hub commit).
"""

import datetime
import hashlib
import hmac
import json
import os
import re
import shutil
import urllib.parse
import urllib.request
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from huggingface_hub import HfApi

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.import_model import REPO, safe_relative
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.errors import TASK_INPUT_INVALID, MrlError
from mrl_runtime.model.inspection import PICKLE_SUFFIXES

EXPORT_DIR = "export"
RESULT_FILE = "export-result.json"
FORMAT = "hf-safetensors"
FORMAT_UNAVAILABLE = "MRL-MODEL-261"
TARGET_FAILED = "MRL-STORE-005"
S3_MAX_PUT = 5 * 1024**3
S3_URL = re.compile(r"^s3://([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])(/.*)?$")


def _mapping(raw: object) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], raw) if isinstance(raw, dict) else {}


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    out: list[Issue] = []
    fmt = inputs.get("format", FORMAT)
    if fmt != FORMAT:
        out.append(
            Issue(
                FORMAT_UNAVAILABLE,
                f"Exporting as {fmt!r} is not available in this release.",
                path="/inputs/format",
                hint=f"Use {FORMAT}.",
            )
        )
    if "model" not in staged:
        out.append(Issue(TASK_INPUT_INVALID, "No model folder was staged.", path="/inputs"))
    target = _mapping(inputs.get("target"))
    kind = target.get("kind", "artifact")
    if kind == "s3" and not S3_URL.match(str(target.get("url", ""))):
        out.append(
            Issue(TASK_INPUT_INVALID, "Give an s3://bucket/prefix URL.", path="/inputs/target/url")
        )
    elif kind == "hf_hub" and not REPO.match(str(target.get("repo", ""))):
        out.append(
            Issue(
                TASK_INPUT_INVALID,
                "Name a Hub repository like `org/name`.",
                path="/inputs/target/repo",
            )
        )
    elif kind not in ("artifact", "s3", "hf_hub"):
        out.append(
            Issue(
                TASK_INPUT_INVALID,
                "Say where to export to.",
                path="/inputs/target/kind",
                hint="Use artifact, s3 or hf_hub.",
            )
        )
    if kind in ("s3", "hf_hub") and not os.environ.get(str(target.get("credential_env", ""))):
        out.append(
            Issue(
                TASK_INPUT_INVALID,
                "The secret for this target was not handed to the job.",
                path="/inputs/target/credential_env",
            )
        )
    return out


def model_card(card: Mapping[str, Any]) -> str:
    """The generated README.md: what this model is and where it came from."""
    name = str(card.get("name", "model"))
    license_id = card.get("license") or "unknown"
    lines = [
        "---",
        f"license: {license_id}",
        "library_name: transformers",
        "tags: [masterrl]",
        "---",
        "",
        f"# {name}",
        "",
        "Exported from MasterRL as standard Hugging Face safetensors files.",
        "",
    ]
    for label, key in (
        ("Model version", "version"),
        ("Revision", "revision"),
        ("Base model version", "base"),
        ("Made from", "source"),
        ("Run", "run"),
    ):
        if card.get(key):
            lines.append(f"- {label}: `{card[key]}`")
    return "\n".join(lines) + "\n"


def _role(path: str) -> str:
    name = path.lower()
    if name.endswith(".safetensors"):
        return "adapter" if "adapter" in name else "weights"
    if "token" in name or name.endswith((".model", ".jinja", "merges.txt", "vocab.json")):
        return "tokenizer"
    if name.endswith("config.json"):
        return "config"
    return "other"


def _bundle(ctx: TaskContext) -> tuple[Path, list[str]]:
    src = ctx.staged_paths["model"]
    folder = ctx.output_dir / EXPORT_DIR
    folder.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for p in sorted(src.rglob("*")):
        rel = p.relative_to(src).as_posix()
        if p.is_symlink() or not p.is_file() or rel == "README.md":
            continue
        if rel.lower().endswith(PICKLE_SUFFIXES):
            raise MrlError(TARGET_FAILED, f"{rel} is a pickle file; it is never exported.")
        (folder / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(p, folder / rel)
        paths.append(rel)
    (folder / "README.md").write_text(
        model_card(_mapping(ctx.inputs.get("card"))), encoding="utf-8"
    )
    lineage: object = ctx.inputs.get("lineage") or {}
    (folder / "mrl-lineage.json").write_text(
        json.dumps(lineage, indent=2, sort_keys=True), encoding="utf-8"
    )
    return folder, sorted([*paths, "README.md", "mrl-lineage.json"])


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


@dataclass(frozen=True)
class S3:
    """Where and as whom to reach S3."""

    endpoint: str
    region: str
    creds: Mapping[str, str]


def s3_request(
    s3: S3, method: str, bucket: str, key: str, now: datetime.datetime | None = None
) -> urllib.request.Request:
    """A signed (AWS Signature V4, unsigned payload) path-style request for bucket/key."""
    endpoint, region, creds = s3.endpoint, s3.region, s3.creds
    now = now or datetime.datetime.now(datetime.UTC)
    amz_date, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    base = urllib.parse.urlsplit(endpoint)
    uri = urllib.parse.quote(f"{base.path.rstrip('/')}/{bucket}/{key}", safe="/-_.~")
    headers = {
        "host": base.netloc,
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
        "x-amz-date": amz_date,
    }
    if creds.get("session_token"):
        headers["x-amz-security-token"] = creds["session_token"]
    signed = ";".join(sorted(headers))
    canonical = "\n".join(
        [
            method,
            uri,
            "",
            *(f"{k}:{headers[k]}" for k in sorted(headers)),
            "",
            signed,
            "UNSIGNED-PAYLOAD",
        ]
    )
    scope = f"{day}/{region}/s3/aws4_request"
    to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    k = _hmac(("AWS4" + creds["secret_access_key"]).encode(), day)
    for part in (region, "s3", "aws4_request"):
        k = _hmac(k, part)
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={creds['access_key_id']}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature}"
    )
    del headers["host"]
    return urllib.request.Request(
        f"{base.scheme}://{base.netloc}{uri}", method=method, headers=headers
    )


def _to_s3(
    folder: Path, paths: list[str], target: Mapping[str, Any]
) -> Iterator[TaskEvent | dict[str, Any]]:
    match = S3_URL.match(str(target["url"]))
    assert match is not None  # checked by validate
    bucket, prefix = match.group(1), (match.group(2) or "/").strip("/")
    try:
        creds = json.loads(os.environ[str(target["credential_env"])])
    except (KeyError, json.JSONDecodeError) as exc:
        raise MrlError(
            TARGET_FAILED, "The S3 secret must be JSON with access_key_id and secret_access_key."
        ) from exc
    region = str(target.get("region") or creds.get("region") or "us-east-1")
    endpoint = str(
        target.get("endpoint") or creds.get("endpoint") or f"https://s3.{region}.amazonaws.com"
    )
    keys: list[str] = []
    for i, rel in enumerate(paths):
        if not safe_relative(rel):
            raise MrlError(TARGET_FAILED, f"{rel} is not a safe file name.")
        if (folder / rel).stat().st_size > S3_MAX_PUT:
            raise MrlError(
                TARGET_FAILED, f"{rel} is larger than 5 GiB, the most one S3 upload takes here."
            )
        key = f"{prefix}/{rel}" if prefix else rel
        req = s3_request(S3(endpoint, region, creds), "PUT", bucket, key)
        req.add_header("Content-Length", str((folder / rel).stat().st_size))
        with (folder / rel).open("rb") as body:
            req.data = body
            try:
                with urllib.request.urlopen(req, timeout=600) as resp:
                    resp.read()
            except OSError as exc:
                raise MrlError(TARGET_FAILED, f"S3 refused {key}: {exc}") from exc
        keys.append(key)
        yield Progress(step=i + 1, total_steps=len(paths), phase="upload")
    yield {"kind": "s3", "bucket": bucket, "keys": keys}


def _to_hub(folder: Path, target: Mapping[str, Any]) -> dict[str, Any]:
    api = HfApi(token=os.environ[str(target["credential_env"])])
    repo = str(target["repo"])
    api.create_repo(repo, private=bool(target.get("private", True)), exist_ok=True)
    info = api.upload_folder(
        folder_path=folder, repo_id=repo, commit_message="Export from MasterRL"
    )
    return {"kind": "hf_hub", "repo": repo, "commit": info.oid}


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    folder, paths = _bundle(ctx)
    target = _mapping(ctx.inputs.get("target"))
    result: dict[str, Any] = {"kind": "artifact"}
    if target.get("kind") == "s3":
        for event in _to_s3(folder, paths, target):
            if isinstance(event, dict):
                result = event
            else:
                yield event
    elif target.get("kind") == "hf_hub":
        result = _to_hub(folder, target)
    (ctx.output_dir / RESULT_FILE).write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
    for rel in paths:
        yield TaskOutput(f"{EXPORT_DIR}/{rel}", role=_role(rel))
    yield TaskOutput(RESULT_FILE, role="data", format="json")


EXPORT_MODEL = BuiltinTask(
    name="export_model",
    job_kind="EXPORT",
    artifact_kind="export",
    validate=_validate,
    run=_run,
)
