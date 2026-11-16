"""Engine manifest loader: mrl-engine.yaml plus mrl-engine.d/*.yaml, validated against P09.

Fragments are merged in file-name order: mappings merge key by key, lists are appended (so a
fragment can add algorithms or constraints), and a scalar may only repeat its existing value.
"""

import importlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Literal, cast

import yaml

from mrl_runtime.adapter.engine import API_VERSION, TrainingEngine
from mrl_runtime.errors import ADAPTER_API_MISMATCH, MANIFEST_INVALID, MrlError
from mrl_runtime.protocol.schemas import schema_errors

MANIFEST_FILE = "mrl-engine.yaml"
FRAGMENT_DIR = "mrl-engine.d"
ENGINES_GROUP = "mrl_runtime.engines"
MetricFreshness = Literal["live", "sampled", "delayed"]


@dataclass(frozen=True)
class DeclaredMetric:
    name: str
    source: str
    kind: MetricFreshness
    required: bool


@dataclass(frozen=True)
class EngineManifest:
    name: str
    version: str
    entrypoint: str
    adapter_api: str
    worker_protocol: str
    pause: bool
    on_demand_checkpoint: bool
    checkpoint_format: str
    algorithms: Mapping[str, tuple[DeclaredMetric, ...]]
    namespaces: Mapping[str, str]
    exports: tuple[str, ...]
    accelerators: tuple[str, ...]
    raw: Mapping[str, Any]


def _invalid(detail: str) -> MrlError:
    return MrlError(
        MANIFEST_INVALID,
        "The engine manifest has a problem.",
        hint="Fix the fields listed in Details, then load the engine again.",
        detail=detail,
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        doc: object = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise _invalid(f"{path.name}: {exc}") from exc
    if not isinstance(doc, dict):
        raise _invalid(f"{path.name}: the top level must be a mapping")
    return cast(dict[str, Any], doc)


def merge(base: Any, extra: Any, where: str = "") -> Any:
    if isinstance(base, dict) and isinstance(extra, dict):
        out = dict(cast(dict[str, Any], base))
        for key, value in cast(dict[str, Any], extra).items():
            out[key] = merge(out[key], value, f"{where}/{key}") if key in out else value
        return out
    if isinstance(base, list) and isinstance(extra, list):
        return [*cast(list[Any], base), *cast(list[Any], extra)]
    if base != extra:
        raise _invalid(f"{where}: a fragment changes this value ({base!r} -> {extra!r})")
    return cast(Any, base)


def load_manifest(path: Path) -> EngineManifest:
    """Load a manifest directory (or its mrl-engine.yaml) with every fragment merged in."""
    root = path if path.is_dir() else path.parent
    doc = _read_yaml(root / MANIFEST_FILE if path.is_dir() else path)
    for fragment in sorted((root / FRAGMENT_DIR).glob("*.yaml")):
        doc = cast(dict[str, Any], merge(doc, _read_yaml(fragment)))
    return from_document(doc)


def from_document(doc: dict[str, Any]) -> EngineManifest:
    """Validate a merged manifest document and pull out what the runtime uses."""
    doc = json.loads(json.dumps(doc))  # YAML-only types (dates, ...) are not valid manifests
    problems = schema_errors(doc, "engine-manifest/v1")
    if problems:
        raise _invalid("; ".join(problems[:10]))
    spec = doc["spec"]
    ids = [a["id"] for a in spec["algorithms"]]
    if len(set(ids)) != len(ids):
        raise _invalid(f"/spec/algorithms: an algorithm id appears twice in {ids}")
    caps = spec["capabilities"]
    vendors = [
        e["accelerator"]["vendor"] for e in spec["runtime"]["environments"] if "accelerator" in e
    ]
    return EngineManifest(
        name=doc["metadata"]["name"],
        version=doc["metadata"]["version"],
        entrypoint=spec["adapter"]["entrypoint"],
        adapter_api=spec["adapter"]["adapterApi"],
        worker_protocol=spec["adapter"]["workerProtocol"],
        pause=caps["pause"],
        on_demand_checkpoint=caps["onDemandCheckpoint"],
        checkpoint_format=spec["checkpoint"]["format"],
        algorithms={
            a["id"]: tuple(
                DeclaredMetric(m["name"], m.get("source", ""), m["kind"], m["required"])
                for m in a["metrics"]
            )
            for a in spec["algorithms"]
        },
        namespaces=spec.get("metrics", {}).get("namespaces", {}),
        exports=tuple(spec.get("exports", [])),
        accelerators=tuple(
            dict.fromkeys([*vendors, *spec.get("hardware", {}).get("accelerators", [])])
        ),
        raw=doc,
    )


_CLAUSE = re.compile(r"^\s*(>=|<=|==|!=|>|<)\s*(\d+(?:\.\d+)*)\s*$")


def _tuple(version: str, width: int) -> tuple[int, ...]:
    parts = [int(p) for p in version.split(".")]
    return tuple(parts + [0] * (width - len(parts)))


def version_in_range(version: str, spec: str) -> bool:
    """`spec` is comma-separated clauses like ">=1.0,<2"."""
    for clause in spec.split(","):
        match = _CLAUSE.match(clause)
        if match is None:
            raise _invalid(f"version range {spec!r}: cannot read {clause!r}")
        op, bound = match.groups()
        width = max(version.count("."), bound.count(".")) + 1
        a, b = _tuple(version, width), _tuple(bound, width)
        ok = {">=": a >= b, "<=": a <= b, "==": a == b, "!=": a != b, ">": a > b, "<": a < b}[op]
        if not ok:
            return False
    return True


def load_engine(manifest: EngineManifest) -> TrainingEngine:
    """Import manifest.entrypoint (module:Class) and check it speaks this adapter API."""
    if not version_in_range(API_VERSION, manifest.adapter_api):
        raise MrlError(
            ADAPTER_API_MISMATCH,
            "This engine needs a different version of the MasterRL runtime.",
            detail=f"engine wants adapter API {manifest.adapter_api}, runtime has {API_VERSION}",
        )
    module_name, _, class_name = manifest.entrypoint.partition(":")
    engine: object = getattr(importlib.import_module(module_name), class_name)()
    if not isinstance(engine, TrainingEngine):
        raise _invalid(f"{manifest.entrypoint} does not implement TrainingEngine")
    return engine


def resolve_engine(ref: str) -> EngineManifest:
    """A manifest path, or a name registered in the mrl_runtime.engines entry-point group."""
    path = Path(ref)
    if path.exists():
        return load_manifest(path)
    found = entry_points(group=ENGINES_GROUP, name=ref)
    if not found:
        names = sorted(ep.name for ep in entry_points(group=ENGINES_GROUP))
        raise _invalid(f"no manifest at {ref!r} and no installed engine with that name {names}")
    factory = next(iter(found)).load()
    manifest: object = factory()
    if not isinstance(manifest, EngineManifest):
        raise _invalid(f"entry point {ref!r} did not return an EngineManifest")
    return manifest
