"""Validate canonical dataset records (Doc 08 §2.4 steps 2, 3, 5, 6, 8, 9).

Structure comes from the P09 JSON Schemas in `schemas/datasets/`; the record rules a schema
lists in `x-mrl-rules` (role order, chosen ≠ rejected, one label per step) and the KTO label
check run on top. Every problem becomes a `Defect` with a stable code (the codes P03's
`DEFECTS.json` files use, plus `unknown_field`, `bad_value`, `conflicting_fields`, `too_deep`,
`invalid_csv`).
"""

import dataclasses
import hashlib
import importlib
import json
import statistics
import unicodedata
from collections import Counter
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

from mrl_runtime.data.readers import FileFormat, Limits, RawRecord, detect_format, read_records
from mrl_runtime.model._json import Json, arr, obj, text
from mrl_runtime.model.errors import BAD_VALUE, DATASET_INVALID, FILE_IO, MrlError

DATASET_SCHEMAS = (
    "pretrain.text.v1",
    "sft.messages.v1",
    "sft.prompt_completion.v1",
    "pref.pairwise.v1",
    "pref.unpaired.v1",
    "rm.pairwise.v1",
    "rm.pointwise.v1",
    "rm.process.v1",
    "rl.prompt.v1",
    "rl.rollout.v1",
    "agent.multiturn.v1",
    "distill.v1",
    "eval.v1",
    "mm.messages.v1",
)
SCHEMA_BASE = "https://schemas.masterrl.dev/datasets/"

MESSAGES = {
    "missing_field": "A required field is missing.",
    "empty_field": "A field is empty.",
    "wrong_type": "A field has the wrong kind of value.",
    "bad_value": "A field has a value the format doesn't allow.",
    "unknown_field": "The record has a field this format doesn't have.",
    "unknown_value": "A field has a value that is not one of the allowed choices.",
    "unknown_role": "A message has a role other than system, user or assistant.",
    "unknown_part_type": "A message part is neither text nor an image reference.",
    "conflicting_fields": "The record sets fields that can't be used together.",
    "role_order": (
        "Messages must go user, assistant, user, assistant (after an optional system "
        "message) and end with assistant."
    ),
    "chosen_equals_rejected": "The chosen and rejected answers are the same.",
    "length_mismatch": "There must be one label for each step.",
    "label_not_bool": "The label must be true or false.",
    "missing_teacher": "The record needs a teacher answer or a teacher logits reference.",
    "bad_utf8": "The record is not valid UTF-8 text.",
    "invalid_json": "The record is not valid JSON.",
    "not_object": "The record is not a JSON object.",
    "oversized_record": "The record is larger than the size limit.",
    "too_deep": "The record is nested more deeply than the limit.",
    "invalid_csv": "The row does not match the CSV header.",
}

# (schema, field path, JSON Schema keyword) -> defect code, for checks Doc 08 names explicitly.
OVERRIDES: dict[tuple[str, tuple[str | int, ...], str], str] = {
    ("pref.unpaired.v1", ("label",), "type"): "label_not_bool",
    ("distill.v1", (), "anyOf"): "missing_teacher",
}
KEYWORD_CODES = {
    "required": "missing_field",
    "dependentRequired": "missing_field",
    "type": "wrong_type",
    "minLength": "empty_field",
    "minItems": "empty_field",
    "minProperties": "empty_field",
    "additionalProperties": "unknown_field",
    "unevaluatedProperties": "unknown_field",
    "enum": "unknown_value",
    "const": "unknown_value",
}


@dataclass(frozen=True)
class Defect:
    code: str
    path: str  # JSON pointer inside the record ("" = the record itself)
    message: str


def find_schema_dir(start: Path | None = None) -> Path:
    """`schemas/datasets` of this checkout, found by walking up from this file."""
    here = (start or Path(__file__)).resolve()
    for parent in here.parents:
        candidate = parent / "schemas" / "datasets"
        if (candidate / "common.v1.schema.json").is_file():
            return candidate
    raise MrlError(
        BAD_VALUE,
        "The app could not find the dataset formats.",
        "pass schema_dir pointing at schemas/datasets",
    )


@cache
def _registry_and_schemas(schema_dir: Path) -> tuple[Any, dict[str, Json]]:
    referencing = importlib.import_module("referencing")
    schemas: dict[str, Json] = {}
    for p in sorted(schema_dir.glob("*.schema.json")):
        doc = obj(json.loads(p.read_text(encoding="utf-8")))
        if doc is not None:
            schemas[p.name.removesuffix(".schema.json")] = doc
    resources = [
        (str(doc["$id"]), referencing.Resource.from_contents(doc))
        for doc in schemas.values()
        if "$id" in doc
    ]
    return referencing.Registry().with_resources(resources), schemas


def _json_pointer(parts: Iterable[str | int]) -> str:
    return "".join("/" + str(p).replace("~", "~0").replace("/", "~1") for p in parts)


def _branch_errors(error: Any) -> list[Any]:
    """The errors of the anyOf/oneOf branch that best fits the instance's shape."""
    branches: dict[int, list[Any]] = {}
    for sub in error.context:
        branches.setdefault(int(sub.schema_path[0]), []).append(sub)
    # A branch whose type doesn't even match the value is the wrong branch.
    shaped = [
        errs
        for errs in branches.values()
        if not any(e.validator == "type" and not e.relative_path for e in errs)
    ]
    # Among object branches with a const discriminator (mm parts), keep the one that matched.
    matched = [errs for errs in shaped if not any(e.validator == "const" for e in errs)]
    if matched:
        return matched[0]
    if not shaped:
        return []
    # No branch's discriminator matched: the discriminator itself is the problem.
    return [e for e in shaped[0] if e.validator == "const"] or shaped[0]


def _classify(error: Any, schema_id: str) -> tuple[str, tuple[str | int, ...]]:
    path = tuple(error.absolute_path)
    keyword = str(error.validator)
    code = OVERRIDES.get((schema_id, path, keyword))
    if code is None and keyword in ("anyOf", "oneOf"):
        if not error.context:
            code = "conflicting_fields"  # oneOf: more than one branch matched
        elif inner := _branch_errors(error):
            return _classify(inner[0], schema_id)
        else:
            code = "wrong_type"
    last = path[-1] if path else None
    if code is None and keyword == "enum" and last == "role":
        code = "unknown_role"
    if code is None and keyword == "const" and last == "type":
        code = "unknown_part_type"
    return code or KEYWORD_CODES.get(keyword, "bad_value"), path


def _role_order(rec: Json) -> bool:
    roles = [text((obj(m) or {}).get("role")) for m in arr(rec.get("messages")) or []]
    body = roles[1:] if roles[:1] == ["system"] else roles
    expected = ["user", "assistant"] * (len(body) // 2 + 1)
    return bool(body) and body == expected[: len(body)] and body[-1] == "assistant"


def _differs(rec: Json) -> bool:
    return rec.get("chosen") != rec.get("rejected")


def _one_label_per_step(rec: Json) -> bool:
    return len(arr(rec.get("completions")) or []) == len(arr(rec.get("labels")) or [])


# x-mrl-rules codes -> check (True = passes). Run only on schema-valid records, like P09.
RULES: dict[str, Callable[[Json], bool]] = {
    "role_order": _role_order,
    "chosen_equals_rejected": _differs,
    "length_mismatch": _one_label_per_step,
}


class RecordValidator:
    """Validates records of one canonical schema."""

    def __init__(self, schema_id: str, schema_dir: Path | None = None) -> None:
        if schema_id not in DATASET_SCHEMAS:
            raise MrlError(BAD_VALUE, "The app doesn't know this dataset format.", schema_id)
        registry, schemas = _registry_and_schemas((schema_dir or find_schema_dir()).resolve())
        if schema_id not in schemas:
            raise MrlError(BAD_VALUE, "The app could not find this dataset format.", schema_id)
        jsonschema = importlib.import_module("jsonschema")
        self.schema_id = schema_id
        self.schema = schemas[schema_id]
        self._validator: Any = jsonschema.Draft202012Validator(self.schema, registry=registry)
        rules = obj(self.schema.get("x-mrl-rules")) or {}
        unknown = [r for r in rules if r not in RULES]
        if unknown:
            raise MrlError(
                BAD_VALUE, "A dataset format has a rule the app doesn't know.", unknown[0]
            )
        self.rules = [(code, RULES[code]) for code in rules]

    def defects(self, record: object) -> list[Defect]:
        """Every defect of one parsed record (empty list = valid)."""
        found: list[Defect] = []
        seen: set[tuple[str, str]] = set()
        errors = sorted(
            self._validator.iter_errors(record), key=lambda e: list(map(str, e.absolute_path))
        )
        for error in errors:
            code, path = self._classify(error)
            key = (code, _json_pointer(path))
            if key not in seen:
                seen.add(key)
                found.append(Defect(code, key[1], MESSAGES.get(code, MESSAGES["bad_value"])))
        rec = obj(record)
        if found or rec is None:
            return found
        return [Defect(code, "", MESSAGES[code]) for code, check in self.rules if not check(rec)]

    def _classify(self, error: Any) -> tuple[str, tuple[str | int, ...]]:
        return _classify(error, self.schema_id)


def _normalize(value: object) -> object:
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if (o := obj(value)) is not None:
        return {unicodedata.normalize("NFC", k): _normalize(v) for k, v in o.items()}
    if (a := arr(value)) is not None:
        return [_normalize(v) for v in a]
    return value


def record_hash(record: object) -> str:
    """Exact-duplicate key: sha256 of the NFC-normalized record as canonical JSON."""
    canonical = json.dumps(
        _normalize(record), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def text_hash(value: str) -> str:
    """Leakage key for one text: NFC, case-folded, whitespace collapsed, then sha256."""
    norm = " ".join(unicodedata.normalize("NFC", value).casefold().split())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def _user_texts(value: object) -> list[str]:
    """A prompt's texts: the string itself, or the user messages' text."""
    if isinstance(value, str):
        return [value]
    out: list[str] = []
    for m in arr(value) or []:
        msg = obj(m) or {}
        if msg.get("role") != "user":
            continue
        content = msg.get("content")
        if isinstance(content, str):
            out.append(content)
        else:
            parts = [text((obj(p) or {}).get("text")) for p in arr(content) or []]
            out.append("\n".join(p for p in parts if p))
    return out


# Which field holds the "question" of a record, per schema (leakage looks at these).
PROMPT_FIELDS = {
    "pretrain.text.v1": "text",
    "sft.messages.v1": "messages",
    "mm.messages.v1": "messages",
    "agent.multiturn.v1": "turns",
    "eval.v1": "input",
}


def content_hashes(schema_id: str, record: object) -> set[str]:
    """Leakage keys of a record: hashes of its prompt / user texts."""
    rec = obj(record) or {}
    key = PROMPT_FIELDS.get(schema_id, "prompt")
    return {text_hash(t) for t in _user_texts(rec.get(key)) if t.strip()}


@dataclass(frozen=True)
class LengthStats:
    count: int
    min: int
    max: int
    mean: float
    p50: float
    p95: float

    @staticmethod
    def of(values: list[int]) -> "LengthStats | None":
        if not values:
            return None
        ordered = sorted(values)
        return LengthStats(
            len(ordered),
            ordered[0],
            ordered[-1],
            round(statistics.fmean(ordered), 2),
            _quantile(ordered, 0.5),
            _quantile(ordered, 0.95),
        )


def _quantile(ordered: list[int], q: float) -> float:
    if len(ordered) == 1:
        return float(ordered[0])
    pos = q * (len(ordered) - 1)
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return round(ordered[low] + (ordered[high] - ordered[low]) * (pos - low), 2)


@dataclass(frozen=True)
class Stats:
    """Character lengths per field, role counts and turns per record, over valid records."""

    records: int
    field_lengths: dict[str, LengthStats]
    roles: dict[str, int]
    turns: LengthStats | None


class _StatsBuilder:
    # ponytail: keeps one int per field per record; switch to a sketch past ~10M records.
    def __init__(self) -> None:
        self.records = 0
        self.lengths: dict[str, list[int]] = {}
        self.roles: Counter[str] = Counter()
        self.turns: list[int] = []

    def add(self, record: Json) -> None:
        self.records += 1
        for key, value in record.items():
            if isinstance(value, str):
                self.lengths.setdefault(key, []).append(len(value))
            items = arr(value)
            if items is None or key not in (
                "messages",
                "turns",
                "prompt",
                "chosen",
                "rejected",
                "completion",
                "input",
            ):
                continue
            if key in ("messages", "turns"):
                self.turns.append(len(items))
            for m in items:
                msg = obj(m) or {}
                role = text(msg.get("role"))
                if role:
                    self.roles[role] += 1
                content = msg.get("content")
                size = (
                    len(content)
                    if isinstance(content, str)
                    else sum(
                        len(text((obj(p) or {}).get("text")) or "") for p in arr(content) or []
                    )
                )
                self.lengths.setdefault(f"{key}.content", []).append(size)

    def build(self) -> Stats:
        fields = {k: s for k, v in sorted(self.lengths.items()) if (s := LengthStats.of(v))}
        return Stats(
            self.records, fields, dict(self.roles.most_common()), LengthStats.of(self.turns)
        )


@dataclass(frozen=True)
class InvalidExample:
    record: int
    code: str
    path: str
    message: str


@dataclass(frozen=True)
class Leak:
    """Records that share a prompt with a protected (e.g. evaluation) dataset."""

    records: int
    percent: float
    examples: tuple[int, ...]


@dataclass(frozen=True)
class ValidationReport:
    """The result of checking one dataset file against one canonical schema.

    `passed` is False when the share of invalid records is above `max_invalid_fraction`
    or the file has no valid record; `error_code` is then MRL-DATA-001.
    """

    schema: str
    source: str
    format: str
    records: int
    valid: int
    invalid: int
    defect_counts: dict[str, int]
    examples: tuple[InvalidExample, ...]
    duplicates: int
    duplicate_examples: tuple[tuple[int, int], ...]
    leakage: dict[str, Leak]
    stats: Stats
    checks: dict[str, str]
    passed: bool
    error_code: str | None
    record_hashes: frozenset[str] = field(default=frozenset(), repr=False)

    def to_dict(self) -> dict[str, Any]:
        out = dataclasses.asdict(self)
        out.pop("record_hashes")
        return out


@dataclass(frozen=True)
class CheckOptions:
    """How strict a check is and what it compares against.

    `against` maps a protected dataset's name to its `content_hashes` (e.g. registered eval
    sets); a record leaks when any of its prompt hashes is in the set.
    """

    limits: Limits = field(default_factory=Limits)
    against: Mapping[str, Collection[str]] = field(default_factory=dict[str, Collection[str]])
    max_examples: int = 20
    max_invalid_fraction: float = 0.0
    schema_dir: Path | None = None


def validate_records(
    records: Iterable[RawRecord],
    schema_id: str,
    options: CheckOptions | None = None,
    *,
    source: str = "",
    fmt: str = "",
) -> ValidationReport:
    """Check records: reader defects, schema, rules, exact duplicates, leakage and stats."""
    opts = options or CheckOptions()
    max_examples = opts.max_examples
    validator = RecordValidator(schema_id, opts.schema_dir)
    protected = {name: set(hashes) for name, hashes in opts.against.items()}
    counts: Counter[str] = Counter()
    examples: list[InvalidExample] = []
    first_seen: dict[str, int] = {}
    duplicates: list[tuple[int, int]] = []
    leaks: dict[str, list[int]] = {name: [] for name in protected}
    stats = _StatsBuilder()
    total = valid = 0
    for raw in records:
        total += 1
        found = (
            [Defect(raw.defect, "", MESSAGES[raw.defect])]
            if raw.defect
            else validator.defects(raw.value)
        )
        if found:
            counts.update({d.code for d in found})
            examples.extend(
                InvalidExample(raw.number, d.code, d.path, d.message)
                for d in found[: max(0, max_examples - len(examples))]
            )
            continue
        valid += 1
        rec: Json = raw.value
        stats.add(rec)
        digest = record_hash(rec)
        if digest in first_seen:
            duplicates.append((raw.number, first_seen[digest]))
        else:
            first_seen[digest] = raw.number
        if protected:
            mine = content_hashes(schema_id, rec)
            for name, hashes in protected.items():
                if mine & hashes:
                    leaks[name].append(raw.number)
    invalid = total - valid
    passed = valid > 0 and invalid <= opts.max_invalid_fraction * total
    return ValidationReport(
        schema=schema_id,
        source=source,
        format=fmt,
        records=total,
        valid=valid,
        invalid=invalid,
        defect_counts=dict(counts.most_common()),
        examples=tuple(examples),
        duplicates=len(duplicates),
        duplicate_examples=tuple(duplicates[:max_examples]),
        leakage={
            name: Leak(
                len(n), round(100 * len(n) / valid, 2) if valid else 0.0, tuple(n[:max_examples])
            )
            for name, n in leaks.items()
        },
        stats=stats.build(),
        checks=dict.fromkeys(("parse", "schema", "rules", "duplicates", "stats"), "full")
        | {"leakage": "full" if protected else "skipped"},
        passed=passed,
        error_code=None if passed else DATASET_INVALID,
        record_hashes=frozenset(first_seen),
    )


def validate_dataset(
    path: Path,
    schema_id: str,
    options: CheckOptions | None = None,
    *,
    fmt: FileFormat | None = None,
) -> ValidationReport:
    """Stream a dataset file through `validate_records`.

    When `limits.max_records` is reached, every check that ran is marked "sampled".
    """
    opts = options or CheckOptions()
    fmt = fmt or detect_format(path)
    counted = _Counted(read_records(path, fmt, opts.limits))
    report = validate_records(counted, schema_id, opts, source=path.name, fmt=fmt)
    cap = opts.limits.max_records
    if cap is not None and counted.n >= cap:
        sampled = {k: "sampled" if v == "full" else v for k, v in report.checks.items()}
        report = dataclasses.replace(report, checks=sampled)
    return report


class _Counted(Iterator[RawRecord]):
    def __init__(self, inner: Iterator[RawRecord]) -> None:
        self.inner, self.n = inner, 0

    def __next__(self) -> RawRecord:
        item = next(self.inner)
        self.n += 1
        return item


def dataset_content_hashes(
    path: Path, schema_id: str, limits: Limits | None = None
) -> frozenset[str]:
    """Prompt hashes of a whole dataset file, for use as an `against` set."""
    out: set[str] = set()
    for raw in read_records(path, None, limits):
        if raw.defect is None:
            out |= content_hashes(schema_id, raw.value)
    return frozenset(out)


def write_report(report: ValidationReport, out_dir: Path) -> tuple[Path, Path]:
    """Write `validation_report.json` and `stats.json` into `out_dir`."""
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path, stats_path = out_dir / "validation_report.json", out_dir / "stats.json"
        body = report.to_dict()
        report_path.write_text(
            json.dumps(body, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        stats_path.write_text(
            json.dumps(body["stats"], indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    except OSError as exc:
        raise MrlError(
            FILE_IO, "The app could not read or write a file on this computer.", str(exc)
        ) from exc
    return report_path, stats_path
