"""The `import_model` task (IMPORT, `fetch` sandbox): bring a model folder in and inspect it.

Inputs: `{"source": {...}, "convert_pickle": false}` with one source:
- `{"kind": "hub", "repo": "org/name", "revision": "main", "token_env": "MRL_CREDENTIAL_HF"}`:
  the revision is resolved to its commit SHA and only that commit is downloaded. A token, if
  any, comes from the granted secret's environment variable, never from the inputs.
- `{"kind": "staged"}`: the files the server staged as the `model` folder (uploads, promoted
  checkpoints), copied into the outputs so the version keeps its own copy.
- `{"kind": "node_path", "path": "/abs/folder"}`: a folder on this machine, copied in.

Format gate: pickle weights (`.bin`, `.pt`, …) are never downloaded or kept; a model that
has no other weights is refused (MRL-MODEL-001), and asking to convert it is refused too
(MRL-MODEL-260, not in this release). Outputs: `model/…` (fetched files) and
`inspection.json` (P15's report without the tensor list, plus `license` and `source`), or
`inspection.json` holding `{"error": {code, message}}` when a gate refuses the model.
"""

import hashlib
import json
import os
import re
import shutil
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, cast

from huggingface_hub import HfApi, hf_hub_download  # pyright: ignore[reportUnknownVariableType]
from packaging.licenses import InvalidLicenseExpression, canonicalize_license_expression

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.errors import TASK_INPUT_INVALID, MrlError
from mrl_runtime.model import inspect_model
from mrl_runtime.model.inspection import PICKLE_SUFFIXES

INSPECTION_FILE = "inspection.json"
MODEL_DIR = "model"
PICKLE_REFUSED = "MRL-MODEL-001"
CONVERSION_UNAVAILABLE = "MRL-MODEL-260"
UNREADABLE = "MRL-MODEL-150"
WEIGHT_SUFFIXES = (".safetensors", ".gguf", ".onnx")
ONNX_DATA_SUFFIXES = (".onnx_data", ".onnx.data")
# Files a model folder may bring; TF/Flax weights and images are left out.
KEPT_SUFFIXES = (
    *WEIGHT_SUFFIXES,
    *ONNX_DATA_SUFFIXES,
    ".json",
    ".txt",
    ".model",
    ".jinja",
    ".md",
    ".py",
    ".tiktoken",
)
LICENSE_NAMES = ("LICENSE", "LICENSE.md", "LICENSE.txt", "LICENCE", "COPYING")
REPO = re.compile(r"^[A-Za-z0-9][\w.-]{0,95}(/[A-Za-z0-9][\w.-]{0,95})?$")
REVISION = re.compile(r"^[\w./-]{1,255}$")
FRONT_LICENSE = re.compile(r"^license:\s*([\w.+-]+)\s*$", re.MULTILINE)


def _issue(message: str, path: str, hint: str = "") -> Issue:
    return Issue(TASK_INPUT_INVALID, message, path=path, hint=hint)


def _source(inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    raw: object = inputs.get("source")
    return cast(Mapping[str, Any], raw) if isinstance(raw, dict) else {}


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    src = _source(inputs)
    kind = src.get("kind")
    out: list[Issue] = []
    if kind == "hub":
        if not isinstance(src.get("repo"), str) or not REPO.match(src["repo"]):
            out.append(_issue("Name a Hub repository like `org/name`.", "/inputs/source/repo"))
        revision = src.get("revision", "main")
        if not isinstance(revision, str) or not REVISION.match(revision) or ".." in revision:
            out.append(_issue("Name a branch, tag or commit.", "/inputs/source/revision"))
    elif kind == "staged":
        if MODEL_DIR not in staged:
            out.append(_issue("No model folder was staged.", "/inputs/source"))
    elif kind == "node_path":
        path = src.get("path")
        if not isinstance(path, str) or not Path(path).is_absolute():
            out.append(_issue("Give the folder as an absolute path.", "/inputs/source/path"))
        elif not Path(path).is_dir():
            out.append(_issue("This folder does not exist on the machine.", "/inputs/source/path"))
    else:
        out.append(
            _issue(
                "Say where the model comes from.",
                "/inputs/source/kind",
                hint="Use `hub`, `staged` or `node_path`.",
            )
        )
    return out


def safe_relative(path: str) -> bool:
    """A relative `/` path that stays inside its folder."""
    parts = path.split("/")
    return (
        bool(path)
        and not path.startswith("/")
        and "\\" not in path
        and "\0" not in path
        and all(p not in ("", ".", "..") for p in parts)
    )


def _kept(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    return name in LICENSE_NAMES or name.lower().endswith(KEPT_SUFFIXES)


def _is_pickle(path: str) -> bool:
    return path.lower().endswith(PICKLE_SUFFIXES)


def _selected(paths: list[str]) -> Iterator[str]:
    preferred = next(
        (suffix for suffix in WEIGHT_SUFFIXES if any(p.lower().endswith(suffix) for p in paths)),
        None,
    )
    suffixes = (preferred,) if preferred else ()
    if preferred == ".onnx":
        suffixes += ONNX_DATA_SUFFIXES
    for path in paths:
        lower = path.lower()
        if _is_pickle(path):
            continue
        if lower.endswith((*WEIGHT_SUFFIXES, *ONNX_DATA_SUFFIXES)) and not lower.endswith(suffixes):
            continue
        yield path


def license_of(folder: Path, spdx: str | None = None) -> dict[str, Any]:
    """The license: its SPDX id (given, or from README front matter) and the hash of its text
    (the LICENSE file, else the id), which a project acknowledges."""
    text: bytes | None = None
    for name in LICENSE_NAMES:
        if (folder / name).is_file():
            text = (folder / name).read_bytes()
            break
    if spdx is None and (folder / "README.md").is_file():
        head = (folder / "README.md").read_text(encoding="utf-8", errors="replace")[:16384]
        if head.startswith("---"):
            found = FRONT_LICENSE.search(head.split("---", 2)[1] if head.count("---") >= 2 else "")
            spdx = found.group(1).lower() if found else None
    body = text if text is not None else f"license:{spdx or 'unknown'}".encode()
    kind = "custom" if text is not None or spdx else "unknown"
    try:
        canonical = canonicalize_license_expression(spdx or "")
    except InvalidLicenseExpression:
        canonical = ""
    if canonical and "licenseref-" not in canonical.lower():
        kind = "spdx"
    return {
        "spdx": canonical.lower() if kind == "spdx" else None,
        "kind": kind,
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def report(folder: Path, *, license: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    """P15's inspection of `folder` as stored by the registry (no tensor list or local path)."""
    doc = inspect_model(folder).to_dict()
    doc.pop("tensors", None)
    doc.pop("path", None)
    return doc | {"license": license, "source": source}


def write_refusal(out: Path, code: str, message: str) -> Iterator[TaskEvent]:
    out.mkdir(parents=True, exist_ok=True)
    (out / INSPECTION_FILE).write_text(
        json.dumps({"error": {"code": code, "message": message}}), encoding="utf-8"
    )
    yield TaskOutput(INSPECTION_FILE, role="data", format="json")


def _gate(paths: list[str], convert: bool) -> tuple[str, str] | None:
    """Why the files can't become a version, if they can't."""
    has_weights = any(p.lower().endswith(WEIGHT_SUFFIXES) for p in paths)
    if any(_is_pickle(p) for p in paths) and not has_weights:
        if convert:
            return (
                CONVERSION_UNAVAILABLE,
                "Converting pickle weights to safetensors is not available in this release.",
            )
        return (
            PICKLE_REFUSED,
            "This model has only pickle weights, which can run code when loaded. "
            "Only safetensors weights are accepted.",
        )
    if not has_weights:
        return (UNREADABLE, "No safetensors, GGUF or ONNX weights were found.")
    return None


def write_outputs(
    out: Path, folder: Path, doc: dict[str, Any], copied: bool
) -> Iterator[TaskEvent]:
    (out / INSPECTION_FILE).write_text(json.dumps(doc, sort_keys=True), encoding="utf-8")
    if copied:
        roles = {f["path"]: f["role"] for f in doc["files"]}
        for p in sorted(x.relative_to(folder).as_posix() for x in folder.rglob("*") if x.is_file()):
            yield TaskOutput(f"{MODEL_DIR}/{p}", role=roles.get(p, "other"))
    yield TaskOutput(INSPECTION_FILE, role="data", format="json")


def _hub(ctx: TaskContext, src: Mapping[str, Any], convert: bool) -> Iterator[TaskEvent]:
    env = src.get("token_env")
    token = os.environ.get(env) if isinstance(env, str) and env else None
    api = HfApi(token=token or False)
    repo, revision = str(src["repo"]), str(src.get("revision", "main"))
    info = api.model_info(repo, revision=revision, files_metadata=False)
    commit = str(info.sha)
    paths = [s.rfilename for s in info.siblings or []]
    if any(not safe_relative(p) for p in paths):
        raise MrlError(UNREADABLE, "The repository lists a file outside its folder.")
    refused = _gate(paths, convert)
    out = ctx.output_dir
    if refused:
        yield from write_refusal(out, *refused)
        return
    wanted = sorted(p for p in _selected(paths) if _kept(p))
    folder = out / MODEL_DIR
    scratch = out.parent / "import-download"
    for i, rel in enumerate(wanted):
        if ctx.cancelled():
            return
        got = Path(
            hf_hub_download(repo, rel, revision=commit, local_dir=scratch, token=token or False)
        )
        target = folder / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        Path(got).replace(target)
        yield Progress(step=i + 1, total_steps=len(wanted), phase="download")
    shutil.rmtree(scratch, ignore_errors=True)
    card = getattr(info, "card_data", None)
    spdx = card.get("license") if card is not None else None
    doc = report(
        folder,
        license=license_of(folder, spdx if isinstance(spdx, str) else None),
        source={"repo": repo, "revision": revision, "commit": commit},
    )
    yield from write_outputs(out, folder, doc, copied=True)


def _node_path(ctx: TaskContext, src: Mapping[str, Any], convert: bool) -> Iterator[TaskEvent]:
    root = Path(str(src["path"]))
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise MrlError(UNREADABLE, "Choose a model folder without symbolic links.")
    root = root.resolve()
    paths: list[str] = []
    for p in sorted(root.rglob("*")):
        # A link could point anywhere on the machine: only real files inside the folder.
        if p.is_symlink():
            raise MrlError(UNREADABLE, f"{p.relative_to(root)} is a link; copy real files instead.")
        if p.is_file():
            paths.append(p.relative_to(root).as_posix())
    out = ctx.output_dir
    refused = _gate(paths, convert)
    if refused:
        yield from write_refusal(out, *refused)
        return
    folder = out / MODEL_DIR
    wanted = [p for p in _selected(paths) if _kept(p)]
    for i, rel in enumerate(wanted):
        if ctx.cancelled():
            return
        (folder / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / rel, folder / rel)
        yield Progress(step=i + 1, total_steps=len(wanted), phase="copy")
    doc = report(folder, license=license_of(folder), source={"path": str(root)})
    yield from write_outputs(out, folder, doc, copied=True)


def _staged(ctx: TaskContext, convert: bool) -> Iterator[TaskEvent]:
    staged = ctx.staged_paths[MODEL_DIR]
    paths = sorted(
        p.relative_to(staged).as_posix()
        for p in staged.rglob("*")
        if p.is_file() and not p.is_symlink()
    )
    out = ctx.output_dir
    out.mkdir(parents=True, exist_ok=True)
    refused = _gate(paths, convert)
    if refused:
        yield from write_refusal(out, *refused)
        return
    folder = out / MODEL_DIR
    wanted = list(_selected(paths))
    for i, rel in enumerate(wanted):
        (folder / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(staged / rel, folder / rel)
        yield Progress(step=i + 1, total_steps=len(wanted), phase="copy")
    yield from write_outputs(
        out, folder, report(folder, license=license_of(folder), source={}), copied=True
    )


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    src = _source(ctx.inputs)
    convert = ctx.inputs.get("convert_pickle") is True
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    kind = src.get("kind")
    if kind == "hub":
        yield from _hub(ctx, src, convert)
    elif kind == "node_path":
        yield from _node_path(ctx, src, convert)
    else:
        yield from _staged(ctx, convert)


IMPORT_MODEL = BuiltinTask(
    name="import_model",
    job_kind="IMPORT",
    artifact_kind="model",
    validate=_validate,
    run=_run,
)
