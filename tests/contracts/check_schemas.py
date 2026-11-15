#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["jsonschema==4.26.0", "rfc3339-validator==0.1.4", "rfc3986-validator==0.1.1"]
# ///
"""Contract tests for every JSON Schema under schemas/.

Checks, for each schema:
- it is a valid draft 2020-12 schema whose $id is https://schemas.masterrl.dev/<path>;
- every object definition is closed (additionalProperties false or a value schema) unless it
  carries "x-mrl-open": "<why>";
- its valid examples pass and its invalid examples fail.

Dataset schemas use JSONL: tests/fixtures/datasets/<id>.jsonl and <id>-invalid.jsonl and/or the
same names under tests/contracts/examples/datasets/ (both are checked), with DEFECTS.json
naming each invalid line's defect. A defect must be caught by the right layer: the JSONL reader
(bad_utf8, invalid_json, oversized_record), a record rule the schema lists in "x-mrl-rules", or
the schema itself. Other schemas use tests/contracts/examples/<path>/: valid-*.json documents,
and
invalid-*.json patches of one valid document, each naming where it must fail:
    {"base": "valid-x.json", "set": {"/json/pointer": value}, "remove": ["/json/pointer"],
     "fails_at": "/json/pointer" or "rule:<code>"}

Run: uv run --script tests/contracts/check_schemas.py   (stand-alone: needs jsonschema)
"""

import json
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator  # pyright: ignore[reportMissingModuleSource]
from referencing import Registry, Resource  # pyright: ignore[reportMissingImports]

ROOT = Path(__file__).resolve().parents[2]
SCHEMAS = ROOT / "schemas"
EXAMPLES = Path(__file__).parent / "examples"
FIXTURES = ROOT / "tests" / "fixtures" / "datasets"
BASE = "https://schemas.masterrl.dev/"
DRAFT = "https://json-schema.org/draft/2020-12/schema"
DEFS_ONLY = {"datasets/common.v1.schema.json"}  # shared $defs, not a document schema
READER_DEFECTS = {"bad_utf8", "invalid_json", "oversized_record"}
MAX_RECORD_BYTES: int = json.loads((FIXTURES / "DEFECTS.json").read_text())["max_record_bytes"]


def role_order(rec: dict[str, Any]) -> bool:
    roles = [m["role"] for m in rec["messages"]]
    body = roles[1:] if roles[:1] == ["system"] else roles
    expected = ["user", "assistant"] * (len(body) // 2 + 1)
    return bool(body) and body == expected[: len(body)] and body[-1] == "assistant"


# Record rules JSON Schema cannot express. Key = defect code; value = True if the record passes.
RULES: dict[str, Callable[[dict[str, Any]], bool]] = {
    "role_order": role_order,
    "chosen_equals_rejected": lambda rec: rec["chosen"] != rec["rejected"],
    "length_mismatch": lambda rec: len(rec["completions"]) == len(rec["labels"]),
    "duplicate_path": lambda rec: len({f["path"] for f in rec["files"]}) == len(rec["files"]),
}


def load_schemas() -> dict[str, dict[str, Any]]:
    return {
        p.relative_to(SCHEMAS).as_posix(): json.loads(p.read_text())
        for p in sorted(SCHEMAS.rglob("*.schema.json"))
    }


def open_objects(node: Any, where: str) -> Iterator[str]:
    """Object definitions that accept unknown keys without saying why."""
    if isinstance(node, dict):
        kind = node.get("type")
        is_object = kind == "object" or (isinstance(kind, list) and "object" in kind)
        closed = "additionalProperties" in node or node.get("unevaluatedProperties") is False
        if is_object and not closed and "x-mrl-open" not in node:
            yield where
        for key, value in node.items():
            yield from open_objects(value, f"{where}/{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from open_objects(value, f"{where}/{i}")


def problems(validator: Any, schema: dict[str, Any], data: Any) -> list[str]:
    """Empty when data is valid; else "<json pointer>: <message>" per schema error, or
    "rule:<code>" per failed record rule (rules run only on schema-valid data)."""
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path))
    if errors:
        return [f"/{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in errors]
    return [f"rule:{code}" for code in schema.get("x-mrl-rules", {}) if not RULES[code](data)]


def pointer(doc: Any, path: str) -> tuple[Any, Any]:
    """(parent, key) for a JSON pointer."""
    parts = [p.replace("~1", "/").replace("~0", "~") for p in path.split("/")[1:]]
    for part in parts[:-1]:
        doc = doc[int(part)] if isinstance(doc, list) else doc[part]
    return doc, int(parts[-1]) if isinstance(doc, list) else parts[-1]


def patched(folder: Path, spec: dict[str, Any]) -> Any:
    doc = json.loads((folder / spec["base"]).read_text())
    for path, value in spec.get("set", {}).items():
        parent, key = pointer(doc, path)
        parent[key] = value
    for path in spec.get("remove", []):
        parent, key = pointer(doc, path)
        del parent[key]
    return doc


def read_line(line: bytes) -> tuple[Any, str | None]:
    """What the JSONL reader does before schema validation."""
    if len(line) > MAX_RECORD_BYTES:
        return None, "oversized_record"
    try:
        return json.loads(line.decode("utf-8")), None
    except UnicodeDecodeError:
        return None, "bad_utf8"
    except json.JSONDecodeError:
        return None, "invalid_json"


def dataset_cases(name: str) -> list[tuple[Path, Path, list[dict[str, Any]]]]:
    """(valid file, invalid file, defects) from every folder that has both files."""
    cases = []
    for folder in (FIXTURES, EXAMPLES / "datasets"):
        valid, invalid = folder / f"{name}.jsonl", folder / f"{name}-invalid.jsonl"
        if valid.exists() and invalid.exists():
            defects = json.loads((folder / "DEFECTS.json").read_text())["files"][invalid.name]
            cases.append((valid, invalid, defects))
    return cases


def check_dataset(name: str, validator: Any, schema: dict[str, Any]) -> list[str]:
    cases = dataset_cases(name)
    if not cases:
        return [f"{name}: no valid + invalid JSONL examples"]
    return [f for case in cases for f in check_case(*case, validator, schema)]


def check_case(
    valid: Path,
    invalid: Path,
    defects: list[dict[str, Any]],
    validator: Any,
    schema: dict[str, Any],
) -> list[str]:
    fails = []
    for n, line in enumerate(valid.read_bytes().splitlines(), 1):
        rec, reader = read_line(line)
        found = [reader] if reader else problems(validator, schema, rec)
        if found:
            fails.append(f"{valid.relative_to(ROOT)}:{n}: valid line rejected: {found}")
    lines = invalid.read_bytes().splitlines()
    if [d["line"] for d in defects] != list(range(1, len(lines) + 1)):
        fails.append(
            f"{invalid.relative_to(ROOT)}: DEFECTS.json must list every line once, in order"
        )
    for defect in defects:
        n, code = defect["line"], defect["code"]
        rec, reader = read_line(lines[n - 1])
        if code in READER_DEFECTS:
            ok, layer = reader == code, "the JSONL reader"
        elif code in RULES:
            ok, layer = (
                reader is None and problems(validator, schema, rec) == [f"rule:{code}"],
                "rule",
            )
        else:
            found = [] if reader else problems(validator, schema, rec)
            ok, layer = bool(found) and not found[0].startswith("rule:"), "the schema"
        if not ok:
            fails.append(f"{invalid.relative_to(ROOT)}:{n}: defect {code} not caught by {layer}")
    return fails


def check_documents(rel: str, validator: Any, schema: dict[str, Any]) -> list[str]:
    folder = EXAMPLES / rel.removesuffix(".schema.json")
    valid, invalid = sorted(folder.glob("valid-*.json")), sorted(folder.glob("invalid-*.json"))
    if not valid or not invalid:
        return [f"{rel}: needs valid-*.json and invalid-*.json in {folder.relative_to(ROOT)}"]
    fails = []
    for path in valid:
        found = problems(validator, schema, json.loads(path.read_text()))
        if found:
            fails.append(f"{path.relative_to(ROOT)}: valid example rejected: {found}")
    for path in invalid:
        spec = json.loads(path.read_text())
        found = problems(validator, schema, patched(folder, spec))
        if not any(f == spec["fails_at"] or f.startswith(spec["fails_at"] + ":") for f in found):
            fails.append(
                f"{path.relative_to(ROOT)}: expected failure at {spec['fails_at']}, got {found}"
            )
    return fails


def check(schemas: dict[str, dict[str, Any]]) -> list[str]:
    registry = Registry().with_resources(
        (s["$id"], Resource.from_contents(s)) for s in schemas.values() if "$id" in s
    )
    fails = []
    for rel, schema in schemas.items():
        if schema.get("$schema") != DRAFT or schema.get("$id") != BASE + rel:
            fails.append(f"{rel}: needs $schema {DRAFT} and $id {BASE + rel}")
            continue
        Draft202012Validator.check_schema(schema)
        fails += [f"{rel}: open object at {w} (add x-mrl-open)" for w in open_objects(schema, "#")]
        fails += [
            f"{rel}: unknown x-mrl-rules {c}"
            for c in schema.get("x-mrl-rules", {})
            if c not in RULES
        ]
        if rel in DEFS_ONLY:
            continue
        validator = Draft202012Validator(
            schema, registry=registry, format_checker=Draft202012Validator.FORMAT_CHECKER
        )
        if rel.startswith("datasets/"):
            fails += check_dataset(
                rel.removeprefix("datasets/").removesuffix(".schema.json"), validator, schema
            )
        else:
            fails += check_documents(rel, validator, schema)
    listed = schemas["engine-manifest/v1.schema.json"]["$defs"]["datasetSchema"]["enum"]
    present = sorted(
        r.removeprefix("datasets/").removesuffix(".schema.json")
        for r in schemas
        if r.startswith("datasets/")
    )
    if sorted(listed) != sorted(set(present) - {"common.v1"}):
        fails.append("engine-manifest datasetSchema enum must list exactly the dataset schemas")
    return fails


def main() -> int:
    schemas = load_schemas()
    fails = check(schemas)
    for fail in fails:
        print(fail)
    if fails:
        print(f"schemas: {len(fails)} problem(s)", file=sys.stderr)
        return 1
    print(f"schemas: {len(schemas)} schemas, all examples behave")
    return 0


if __name__ == "__main__":
    sys.exit(main())
