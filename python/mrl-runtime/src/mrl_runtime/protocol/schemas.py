"""The P09 JSON Schemas the runtime writes or reads, vendored byte-for-byte from schemas/."""

import json
from collections.abc import Iterable
from functools import cache
from importlib.resources import files
from typing import Any, Literal, cast

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource

SchemaName = Literal["engine-manifest/v1", "checkpoint-manifest/v1", "artifact-manifest/v1"]
NAMES: tuple[SchemaName, ...] = (
    "engine-manifest/v1",
    "checkpoint-manifest/v1",
    "artifact-manifest/v1",
)
_BASE = "https://schemas.masterrl.dev/"


def schema_text(name: SchemaName) -> str:
    file = name.replace("/", ".") + ".schema.json"
    return files("mrl_runtime.protocol").joinpath("schemas", file).read_text(encoding="utf-8")


@cache
def _validator(name: SchemaName) -> Draft202012Validator:
    docs = {n: json.loads(schema_text(n)) for n in NAMES}
    resources = [
        (f"{_BASE}{n}.schema.json", Resource[Any].from_contents(d)) for n, d in docs.items()
    ]
    registry = Registry[Any]().with_resources(resources)
    return Draft202012Validator(docs[name], registry=registry)


def schema_errors(doc: dict[str, Any], name: SchemaName) -> list[str]:
    """Problems with `doc`, as "<json pointer>: <message>", plus the schema's x-mrl-rules."""
    found: Iterable[ValidationError] = _validator(name).iter_errors(doc)  # pyright: ignore[reportUnknownMemberType]
    out = [f"/{'/'.join(str(p) for p in err.absolute_path)}: {err.message}" for err in found]
    entries: Any = doc.get("files")
    if name != "engine-manifest/v1" and isinstance(entries, list):
        items = cast(list[Any], entries)
        paths = [str(cast(dict[str, Any], f).get("path")) for f in items if isinstance(f, dict)]
        if len(set(paths)) != len(paths):
            out.append("/files: each path must appear once (rule duplicate_path)")
    return out
