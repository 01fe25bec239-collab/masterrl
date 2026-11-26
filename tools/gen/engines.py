#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["pyyaml==6.0.3", "jsonschema==4.25.1"]
# ///
"""Bundle the first-party engine manifests the server embeds. Run by `just gen`.

For each first-party adapter (python/adapters/*/), the FakeEngine (testkit, dev only) and the
built-in utility engine, this merges `mrl-engine.yaml` with `mrl-engine.d/*.yaml` (same rules
as mrl_runtime's loader: mappings merge, lists append, a scalar may only repeat its value),
checks it against schemas/engine-manifest/v1, inlines the config schemas its algorithms name,
and writes crates/mrl-server/engines/first_party/<engine>.json plus embedded.rs (the list
the server compiles in). An adapter with no manifest yet is skipped, and the run says so.

Usage: engines.py [--check]          fail if any output would change
       engines.py --validate <dir>   check one manifest folder (tests use it), write nothing
"""

import json
import sys
from pathlib import Path
from typing import Any, cast

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "crates/mrl-server/engines/first_party"
SCHEMA = ROOT / "schemas/engine-manifest/v1.schema.json"
ADAPTERS = ROOT / "python/adapters"
RUNTIME = ROOT / "python/mrl-runtime/src/mrl_runtime"
FAKE = RUNTIME / "testkit/fake_engine"
BUILTIN = RUNTIME / "builtin/mrl-builtin.yaml"
MANIFEST_FILE = "mrl-engine.yaml"
FRAGMENT_DIR = "mrl-engine.d"
# Platform metric names (resource peaks per phase); an engine may not emit into this prefix.
RESERVED_METRIC_PREFIX = "resource/"


class ManifestError(Exception):
    """A manifest the server must not embed."""


def read_yaml(path: Path) -> dict[str, Any]:
    try:
        doc: object = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ManifestError(f"{path}: {exc}") from exc
    if not isinstance(doc, dict):
        raise ManifestError(f"{path}: the top level must be a mapping")
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
        raise ManifestError(f"{where}: a fragment changes this value ({base!r} -> {extra!r})")
    return cast(Any, base)


def reserved_metrics(spec: dict[str, Any]) -> list[str]:
    """Canonical metric names in the platform's reserved prefix."""
    names = [m["name"] for a in spec.get("algorithms", []) for m in a.get("metrics", [])]
    mapped = spec.get("metrics", {}).get("namespaces", {}).values()
    bad = {n for n in names if n.startswith(RESERVED_METRIC_PREFIX)}
    # The runtime appends the original metric suffix to the mapping target.
    bad.update(
        n
        for n in mapped
        if n.startswith(RESERVED_METRIC_PREFIX) or RESERVED_METRIC_PREFIX.startswith(n)
    )
    return sorted(bad)


def check_manifest(doc: dict[str, Any], where: str) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    problems = [
        f"{'/' + '/'.join(str(p) for p in e.absolute_path)}: {e.message}"
        for e in sorted(Draft202012Validator(schema).iter_errors(doc), key=lambda e: e.json_path)
    ]
    if problems:
        raise ManifestError(f"{where}: " + "; ".join(problems[:10]))
    spec = doc["spec"]
    ids = [a["id"] for a in spec["algorithms"]]
    if len(set(ids)) != len(ids):
        raise ManifestError(f"{where}: an algorithm id appears twice in {ids}")
    catalog = json.loads((ROOT / "guidance/build/catalog.json").read_text(encoding="utf-8"))
    codes = {e["code"] for e in catalog["errors"]}
    for constraint in spec.get("constraints", []):
        Draft202012Validator.check_schema(constraint["schema"])
        if constraint["error"]["code"] not in codes:
            raise ManifestError(f"{where}: {constraint['id']} uses an uncatalogued error code")
        if unknown := set(constraint["appliesTo"]) - set(ids):
            raise ManifestError(
                f"{where}: {constraint['id']} names unknown methods {sorted(unknown)}"
            )
    if bad := reserved_metrics(spec):
        raise ManifestError(
            f"{where}: metric names starting with `{RESERVED_METRIC_PREFIX}` belong to the "
            f"platform (resource use per phase); rename {', '.join(bad)}"
        )


def inline_schemas(doc: dict[str, Any], root: Path, where: str) -> dict[str, Any]:
    """Every config schema and UI schema the algorithms name, by the path they use."""
    out: dict[str, Any] = {}
    for algo in doc["spec"]["algorithms"]:
        config = algo.get("config", {})
        for key in ("schema", "uiSchema"):
            if key not in config:
                continue
            rel = config[key]
            path = (root / rel).resolve()
            if not path.is_relative_to(root.resolve()):
                raise ManifestError(f"{where}: {rel} points outside the adapter")
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise ManifestError(f"{where}: {rel}: {exc}") from exc
            if key == "schema":
                Draft202012Validator.check_schema(loaded)
            if key == "uiSchema" and not isinstance(loaded, dict):
                raise ManifestError(f"{where}: {rel}: the UI schema must be an object")
            out[rel] = loaded
    return out


def load_engine(root: Path, dev_only: bool) -> tuple[str, dict[str, Any]]:
    where = root.relative_to(ROOT).as_posix() if root.is_relative_to(ROOT) else str(root)
    doc = read_yaml(root / MANIFEST_FILE)
    for fragment in sorted((root / FRAGMENT_DIR).glob("*.yaml")):
        doc = cast(dict[str, Any], merge(doc, read_yaml(fragment)))
    doc = json.loads(
        json.dumps(doc, allow_nan=False)
    )  # YAML-only types (dates, ...) are not valid manifests
    check_manifest(doc, where)
    bundle = {
        "kind": "engine",
        "source": where,
        "dev_only": dev_only,
        "manifest": doc,
        "schemas": inline_schemas(doc, root, where),
    }
    return doc["metadata"]["name"], bundle


def _job_kinds_ok(kinds: object) -> bool:
    """Every job kind names a sandbox; DATA_PREP is there and runs in data_prep."""
    if not isinstance(kinds, list) or not all(isinstance(k, dict) for k in kinds):
        return False
    if not all(isinstance(k.get(f), str) and k[f] for k in kinds for f in ("kind", "sandbox")):
        return False
    return {"kind": "DATA_PREP", "sandbox": "data_prep"} in kinds


def load_builtin() -> tuple[str, dict[str, Any]]:
    where = BUILTIN.relative_to(ROOT).as_posix()
    doc = json.loads(json.dumps(read_yaml(BUILTIN), allow_nan=False))
    if doc.get("apiVersion") != "mrl.dev/builtin/v1" or doc.get("kind") != "BuiltinManifest":
        raise ManifestError(
            f"{where}: expected apiVersion mrl.dev/builtin/v1, kind BuiltinManifest"
        )
    meta = doc.get("metadata", {})
    spec = doc.get("spec", {})
    adapter = spec.get("adapter", {}) if isinstance(spec, dict) else {}
    if (
        not all(isinstance(v, dict) for v in (meta, spec, adapter))
        or not all(
            isinstance(meta.get(k), str) and meta[k] for k in ("name", "version", "displayName")
        )
        or not all(
            isinstance(adapter.get(k), str) and adapter[k]
            for k in ("entrypoint", "adapterApi", "workerProtocol")
        )
        or not _job_kinds_ok(spec.get("jobKinds"))
    ):
        raise ManifestError(
            f"{where}: the built-in bundle needs adapter metadata and the DATA_PREP sandbox"
        )
    bundle = {"kind": "builtin", "source": where, "dev_only": False, "manifest": doc, "schemas": {}}
    return meta["name"], bundle


def manifest_dir(adapter: Path) -> Path | None:
    """The folder holding the adapter's mrl-engine.yaml (outside virtualenvs), if any."""
    found = sorted(
        p.parent
        for p in adapter.rglob(MANIFEST_FILE)
        if not any(part.startswith(".") or part == "node_modules" for part in p.parts)
    )
    if len(found) > 1:
        raise ManifestError(f"{adapter.relative_to(ROOT)}: more than one {MANIFEST_FILE}: {found}")
    return found[0] if found else None


def outputs() -> dict[Path, str]:
    bundles: list[tuple[str, dict[str, Any]]] = []
    for adapter in sorted(p for p in ADAPTERS.iterdir() if p.is_dir()):
        root = manifest_dir(adapter)
        if root is None:
            print(f"engines: skipped {adapter.relative_to(ROOT)} (no {MANIFEST_FILE} yet)")
            continue
        bundles.append(load_engine(root, dev_only=False))
    bundles.append(load_engine(FAKE, dev_only=True))
    bundles.append(load_builtin())
    names = [n for n, _ in bundles]
    if len(set(names)) != len(names):
        raise ManifestError(f"two first-party engines share a name: {names}")
    files: dict[Path, str] = {}
    for name, bundle in sorted(bundles):
        files[OUT / f"{name}.json"] = (
            json.dumps(bundle, indent=2, sort_keys=True, allow_nan=False) + "\n"
        )
    rows = "".join(f'    include_str!("{name}.json"),\n' for name, _ in sorted(bundles))
    files[OUT / "embedded.rs"] = (
        "// Generated by tools/gen/engines.py; do not edit.\n"
        "/// Every first-party engine bundle, by name.\n"
        f"pub const BUNDLES: &[&str] = &[\n{rows}];\n"
    )
    return files


def main() -> int:
    args = sys.argv[1:]
    check = "--check" in args
    try:
        if args[:1] == ["--validate"] and len(args) == 2:
            load_engine(Path(args[1]).resolve(), dev_only=False)
            print("engines: the manifest is valid")
            return 0
        files = outputs()
    except (ManifestError, SchemaError, TypeError, ValueError) as exc:
        print(f"engines: {exc}", file=sys.stderr)
        return 1
    stale = {p for p in OUT.glob("*") if p.is_file()} - set(files)
    changed = [p for p, text in files.items() if not p.exists() or p.read_text() != text]
    if check:
        for p in sorted([*changed, *stale]):
            print(f"engines: {p.relative_to(ROOT)} is out of date; run `just gen`", file=sys.stderr)
        return 1 if changed or stale else 0
    OUT.mkdir(parents=True, exist_ok=True)
    for p in stale:
        p.unlink()
    for p, text in files.items():
        p.write_text(text, encoding="utf-8", newline="\n")
    print(f"engines: wrote {len(files) - 1} bundles to {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
