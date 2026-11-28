"""The `data_prep` task (DATA_PREP job, `data_prep` sandbox: no network, no secrets).

`mode: "ingest"` (the default) turns a version's original files into a dataset version
folder (see `mrl_runtime.data.adapter`):
1. Opens archives (.zip, .tar, .tar.gz/.tgz) with strict limits: a member outside the
   folder, a link, too many members, too many bytes or a compression ratio past the limit
   refuses the whole version (MRL-DATA-271).
2. Reads every data file with the P15 readers (16 MiB per record, depth 64, UTF-8, no NaN).
3. Checks each record against the schema and its rules (P15 `validate_records`), drops exact
   duplicates, counts records that share a prompt with each registered evaluation set (the
   server stages each set's `content_hashes.txt` as `against_<n>`), and builds stats.
4. Writes Parquet shards of the valid, de-duplicated records, browse pages, sorted record
   hashes (version diffs) and prompt hashes (leakage checks of later versions), the report,
   and `dataset.json` last. Records with images are refused until multimodal training
   exists (MRL-DATA-270).

`mode: "tokenize"` renders a ready version (staged as `dataset`) with a tokenizer and chat
template (staged as `tokenizer`) and writes `token_preview.json` (P15 `preview_tokens`).

Bad data never fails the job: it ends with `passed: false`, an error code and a reason in
`dataset.json`, so the version becomes INVALID with a report. Only broken inputs (a missing
staged folder) or internal errors fail the job.
"""

import dataclasses
import json
import queue
import shutil
import tarfile
import threading
import zipfile
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import IO, Any, cast

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.checkpoint.files import list_files
from mrl_runtime.data.adapter import (
    MANIFEST,
    SPLITS,
    DatasetAdapter,
    ShardWriter,
    canonical_json,
    parquet_records,
    write_manifest,
)
from mrl_runtime.data.readers import SUFFIXES, Limits, RawRecord, check_value, read_records
from mrl_runtime.data.tokens import PreviewOptions, preview_tokens
from mrl_runtime.data.validate import (
    DATASET_SCHEMAS,
    CheckOptions,
    RecordValidator,
    ValidationReport,
    content_hashes,
    record_hash,
    validate_records,
    write_report,
)
from mrl_runtime.errors import TASK_INPUT_INVALID
from mrl_runtime.model._json import Json, arr, integer, number, obj, text
from mrl_runtime.model.errors import (
    COLUMN_TYPE_REFUSED,
    DATASET_INVALID,
    DATASET_UNREADABLE,
    MrlError,
)

# Codes this part owns (guidance/fragments/errors/p27.yaml).
IMAGES_NOT_SUPPORTED = "MRL-DATA-270"
ARCHIVE_REFUSED = "MRL-DATA-271"

SOURCE_INPUT = "source"
DATASET_INPUT = "dataset"
TOKENIZER_INPUT = "tokenizer"
RECORD_HASHES = "record_hashes.txt"
CONTENT_HASHES = "content_hashes.txt"
TOKEN_PREVIEW = "token_preview.json"
BROWSE_DIR = "browse"
BROWSE_ROWS = 100
BROWSE_BYTES = 4 * 1024 * 1024
ARCHIVE_SUFFIXES = (".zip", ".tar", ".tar.gz", ".tgz")
MAX_MEMBERS = 10_000
MAX_EXTRACT_BYTES = 20 * 1024**3
MAX_RATIO = 200  # uncompressed : compressed, checked once a member passes RATIO_FLOOR
RATIO_FLOOR = 64 * 1024 * 1024
_COPY = 1024 * 1024
_PROGRESS_EVERY = 1000


class RefusedError(Exception):
    """The data can't become a version; `code` and `reason` go into its report."""

    def __init__(self, code: str, reason: str) -> None:
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason


# ---- archives ----


def is_archive(name: str) -> bool:
    return name.lower().endswith(ARCHIVE_SUFFIXES)


def _member_path(name: str) -> PurePosixPath | None:
    """The member's path inside the folder, or None for hidden files (`.x`, `__MACOSX/`).
    Anything that could land outside the folder refuses the archive."""
    p = PurePosixPath(name.replace("\\", "/"))
    if p.is_absolute() or ".." in p.parts or not p.parts or ":" in p.parts[0]:
        raise RefusedError(ARCHIVE_REFUSED, f"the archive has a file outside its folder: {name!r}")
    if any(part.startswith(".") or part == "__MACOSX" for part in p.parts):
        return None
    return p


@dataclass
class _Budget:
    members: int = 0
    written: int = 0

    def member(self) -> None:
        self.members += 1
        if self.members > MAX_MEMBERS:
            raise RefusedError(ARCHIVE_REFUSED, f"the archive has more than {MAX_MEMBERS} files")


def _copy(src: IO[bytes], dest: Path, budget: _Budget, packed: int | None, name: str) -> None:
    """Copy while counting real bytes: sizes written in archive headers are not trusted."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    out = 0
    with dest.open("xb") as f:
        while chunk := src.read(_COPY):
            out += len(chunk)
            budget.written += len(chunk)
            if budget.written > MAX_EXTRACT_BYTES:
                raise RefusedError(
                    ARCHIVE_REFUSED, f"the archive unpacks to more than {MAX_EXTRACT_BYTES} bytes"
                )
            if packed is not None and out > RATIO_FLOOR and out > MAX_RATIO * max(packed, 1):
                raise RefusedError(
                    ARCHIVE_REFUSED,
                    f"{name} unpacks to more than {MAX_RATIO} times its packed size",
                )
            f.write(chunk)


def _check_archive_size(decoded: int, packed: int) -> None:
    if decoded > MAX_EXTRACT_BYTES or (
        decoded > RATIO_FLOOR and decoded > MAX_RATIO * max(packed, 1)
    ):
        raise RefusedError(ARCHIVE_REFUSED, "The archive expands beyond its limit.")


def extract(archive: Path, dest: Path, budget: _Budget) -> None:
    """Unpack `archive` under `dest` with the limits above (members are never trusted)."""
    try:
        if archive.name.lower().endswith(".zip"):
            with zipfile.ZipFile(archive) as z:
                for info in z.infolist():
                    budget.member()
                    rel = _member_path(info.filename)
                    mode = info.external_attr >> 16
                    if (mode & 0o170000) == 0o120000:
                        raise RefusedError(
                            ARCHIVE_REFUSED, f"the archive has a link: {info.filename}"
                        )
                    if rel is None or info.is_dir():
                        continue
                    with z.open(info) as src:
                        _copy(src, dest / rel, budget, info.compress_size, info.filename)
        else:
            packed = archive.stat().st_size
            decoded = 0
            with tarfile.open(archive, mode="r:*") as t:
                for member in t:
                    budget.member()
                    rel = _member_path(member.name)
                    if member.isdir():
                        continue
                    decoded += member.size
                    _check_archive_size(decoded, packed)
                    if rel is None and member.isfile():
                        continue
                    if not member.isfile():
                        raise RefusedError(
                            ARCHIVE_REFUSED, f"the archive has a link or device: {member.name}"
                        )
                    src = t.extractfile(member)
                    if src is None or rel is None:
                        continue
                    with src:
                        _copy(src, dest / rel, budget, packed, member.name)
    except FileExistsError as exc:
        raise RefusedError(ARCHIVE_REFUSED, f"the archive has the same file twice: {exc}") from exc
    except (zipfile.BadZipFile, tarfile.TarError, EOFError, OSError) as exc:
        raise RefusedError(
            DATASET_UNREADABLE, f"{archive.name} is not a readable archive: {exc}"
        ) from exc


def gather(root: Path, scratch: Path) -> tuple[list[tuple[str, Path]], list[str]]:
    """Data files under `root` (archives unpacked into `scratch`) as (name, path), sorted,
    and the names of files that are not data (README, licence, …)."""
    budget = _Budget()
    data: list[tuple[str, Path]] = []
    ignored: list[str] = []
    for rel in list_files(root):
        path = root / rel
        if is_archive(rel):
            out = scratch / f"archive-{len(data)}-{len(ignored)}"
            extract(path, out, budget)
            for inner in list_files(out):
                if Path(inner).suffix.lower() in SUFFIXES:
                    data.append((f"{rel}/{inner}", out / inner))
                else:
                    ignored.append(f"{rel}/{inner}")
        elif Path(rel).suffix.lower() in SUFFIXES:
            data.append((rel, path))
        else:
            ignored.append(rel)
    return sorted(data), sorted(ignored)


# ---- records ----


def has_images(record: Json) -> bool:
    """True if the record carries images (sft `images`, or `image_ref` message parts)."""
    if arr(record.get("images")):
        return True
    for m in arr(record.get("messages")) or []:
        for part in arr((obj(m) or {}).get("content")) or []:
            if text((obj(part) or {}).get("type")) == "image_ref":
                return True
    return False


@dataclass
class _BrowseWriter:
    root: Path
    pages: list[Json] = field(default_factory=list[Json])
    _lines: list[str] = field(default_factory=list[str])
    _bytes: int = 0
    _first: int = 0

    def add(self, line: str, size: int) -> None:
        if self._bytes + size > BROWSE_BYTES:
            self.flush()
        self._lines.append(line)
        self._bytes += size
        if len(self._lines) >= BROWSE_ROWS or self._bytes >= BROWSE_BYTES:
            self.flush()

    def flush(self) -> None:
        if not self._lines:
            return
        rel = f"{BROWSE_DIR}/{len(self.pages):05d}.jsonl"
        (self.root / BROWSE_DIR).mkdir(parents=True, exist_ok=True)
        (self.root / rel).write_text("\n".join(self._lines) + "\n", encoding="utf-8", newline="\n")
        self.pages.append({"path": rel, "first": self._first, "rows": len(self._lines)})
        self._first += len(self._lines)
        self._lines, self._bytes = [], 0


@dataclass
class Normalized:
    """What `normalize` wrote (all paths relative to the output folder)."""

    report: ValidationReport
    passed: bool
    error_code: str | None
    reason: str
    outputs: list[TaskOutput]


def read_hash_set(path: Path) -> frozenset[str]:
    """A staged hash list: one lowercase hex sha256 per line."""
    out: set[str] = set()
    with path.open(encoding="utf-8") as f:
        for line in f:
            h = line.strip()
            if len(h) != 64 or any(c not in "0123456789abcdef" for c in h):
                raise MrlError(TASK_INPUT_INVALID, f"{path.name} has a line that is not a hash.")
            out.add(h)
    return frozenset(out)


@dataclass(frozen=True)
class Check:
    """How `normalize` checks: leakage sets by label, the share of invalid records allowed,
    cancellation and progress hooks, and extra fields for `dataset.json`."""

    against: Mapping[str, frozenset[str]] = field(default_factory=dict[str, frozenset[str]])
    max_invalid_fraction: float = 0.0
    cancelled: Callable[[], bool] = lambda: False
    progress: Callable[[int], None] = lambda _: None
    extra: Mapping[str, Any] = field(default_factory=dict[str, Any])


def _verdict(report: ValidationReport, images: int, schema_id: str) -> tuple[str | None, str]:
    if images:
        return (
            IMAGES_NOT_SUPPORTED,
            f"{images} record(s) have images; training on images is not available yet.",
        )
    if report.passed:
        return None, ""
    if not report.records:
        return report.error_code or DATASET_INVALID, "The files hold no records."
    return (
        report.error_code or DATASET_INVALID,
        f"{report.invalid} of {report.records} record(s) do not match {schema_id}.",
    )


def record_line(record: Json, limits: Limits) -> tuple[str, int, str | None]:
    """Serialize a row only after its structure and encoding pass the reader limits."""
    if defect := check_value(record, limits.max_depth):
        return "", 0, defect
    line = canonical_json(record)
    size = len(line.encode("utf-8")) + 1
    return line, size, "oversized_record" if size > limits.max_record_bytes else None


def normalize(  # noqa: PLR0915 (one streaming pass and one publication decision)
    records: Iterable[tuple[RawRecord, str]], schema_id: str, out: Path, check: Check
) -> Normalized:
    """Check `records` (each with its split) and write the version folder into `out`.

    Shards, browse pages and hash lists hold the valid records without exact duplicates.
    When the data does not pass, no shards are kept and `dataset.json` says why.
    """
    validator = RecordValidator(schema_id)
    shards = ShardWriter(out)
    browse = _BrowseWriter(out)
    seen: set[str] = set()
    prompts: set[str] = set()
    images = 0
    limits = Limits()

    def tap() -> Iterator[RawRecord]:
        nonlocal images
        for n, (raw, split) in enumerate(records, 1):
            if n % _PROGRESS_EVERY == 0:
                check.progress(n)
            if check.cancelled():
                return
            if raw.defect is not None:
                yield raw
                continue
            rec = cast(Json, raw.value)
            line, size, defect = record_line(rec, limits)
            if defect:
                yield RawRecord(raw.number, None, defect, size)
                continue
            yield raw
            if validator.defects(rec):
                continue
            if has_images(rec):
                images += 1
                continue
            digest = record_hash(rec)
            if digest in seen:
                continue
            seen.add(digest)
            prompts.update(content_hashes(schema_id, rec))
            shards.add(rec, split)
            browse.add(line, size)

    report = validate_records(
        tap(),
        schema_id,
        CheckOptions(
            against=check.against,
            max_invalid_fraction=check.max_invalid_fraction,
            unique_stats=True,
        ),
        source=schema_id,
        fmt="mixed",
    )
    browse.flush()
    listed = shards.close()
    write_report(report, out)
    error_code, reason = _verdict(report, images, schema_id)
    passed = error_code is None
    outputs = [
        TaskOutput("validation_report.json", "report", "json"),
        TaskOutput("stats.json", "report", "json"),
    ]
    if passed:
        (out / RECORD_HASHES).write_text(
            "".join(f"{h}\n" for h in sorted(seen)), "utf-8", newline="\n"
        )
        (out / CONTENT_HASHES).write_text(
            "".join(f"{h}\n" for h in sorted(prompts)), "utf-8", newline="\n"
        )
        outputs += [TaskOutput(s.path, "data", "parquet") for s in listed]
        outputs += [TaskOutput(p["path"], "data", "jsonl") for p in browse.pages]
        outputs += [
            TaskOutput(RECORD_HASHES, "data", "text"),
            TaskOutput(CONTENT_HASHES, "data", "text"),
        ]
    else:
        for d in ("shards", BROWSE_DIR):
            shutil.rmtree(out / d, ignore_errors=True)
        listed = []
    summary = {
        "passed": passed,
        "error_code": error_code,
        "reason": reason,
        "records": report.records,
        "valid": report.valid,
        "invalid": report.invalid,
        "duplicates": report.duplicates,
        "images": images,
        "leakage": {
            name: {"records": leak.records, "percent": leak.percent}
            for name, leak in report.leakage.items()
        },
        "browse": browse.pages if passed else [],
    } | dict(check.extra)
    write_manifest(out, schema_id, listed, **summary)
    outputs.append(TaskOutput(MANIFEST, "manifest", "json"))
    return Normalized(report, passed, error_code, reason, outputs)


def refused_manifest(
    out: Path, schema_id: str, error: RefusedError, extra: Mapping[str, Any]
) -> None:
    """`dataset.json` for data refused before any record was read."""
    write_manifest(
        out,
        schema_id,
        [],
        passed=False,
        error_code=error.code,
        reason=error.reason,
        records=0,
        valid=0,
        invalid=0,
        duplicates=0,
        images=0,
        leakage={},
        browse=[],
        **extra,
    )


def with_progress[T](
    work: Callable[[Callable[[int], None]], T], phase: str
) -> Generator[Progress, None, T]:
    """Run `work(report)` in a thread and yield a Progress event each time it reports a
    higher count, so the harness sees a long check advance (it stops jobs that stall)."""
    counts: queue.Queue[int] = queue.Queue()
    done: list[T] = []
    failed: list[BaseException] = []

    def target() -> None:
        try:
            done.append(work(counts.put))
        except BaseException as exc:  # re-raised in the task's own thread below
            failed.append(exc)

    thread = threading.Thread(target=target, name=f"data-prep-{phase}", daemon=True)
    thread.start()
    while thread.is_alive() or not counts.empty():
        try:
            yield Progress(step=counts.get(timeout=0.5), phase=phase)
        except queue.Empty:
            continue
    thread.join()
    if failed:
        raise failed[0]
    return done[0]


# ---- the task ----


def _schema_issue(inputs: Mapping[str, Any]) -> list[Issue]:
    if inputs.get("schema_id") in DATASET_SCHEMAS:
        return []
    return [
        Issue(
            TASK_INPUT_INVALID,
            "data_prep needs `schema_id`: one of the canonical dataset schemas.",
            path="/inputs/schema_id",
            hint=f"For example: {DATASET_SCHEMAS[1]}.",
        )
    ]


def _staged_issue(staged: Mapping[str, Path], name: str) -> list[Issue]:
    if name in staged and staged[name].is_dir():
        return []
    return [
        Issue(
            TASK_INPUT_INVALID,
            f"No staged folder is named {name!r}.",
            hint=f"Staged inputs: {', '.join(sorted(staged)) or 'none'}.",
        )
    ]


def against_inputs(inputs: Mapping[str, Any]) -> dict[str, str]:
    raw = obj(inputs.get("against")) or {}
    return {k: v for k, v in raw.items() if isinstance(v, str)}


def against_issues(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    raw = inputs.get("against")
    if raw is not None and (obj(raw) is None or len(against_inputs(inputs)) != len(obj(raw) or {})):
        return [Issue(TASK_INPUT_INVALID, "`against` must map staged names to labels.")]
    return [
        i
        for name in against_inputs(inputs)
        for i in _staged_issue(staged, name)
        + (
            []
            if (staged.get(name, Path("/nonexistent")) / CONTENT_HASHES).is_file()
            else [Issue(TASK_INPUT_INVALID, f"{name} has no {CONTENT_HASHES}.")]
        )
    ]


def load_against(
    inputs: Mapping[str, Any], staged: Mapping[str, Path]
) -> dict[str, frozenset[str]]:
    """Leakage sets keyed by their label (the evaluation version's id)."""
    return {
        label: read_hash_set(staged[name] / CONTENT_HASHES)
        for name, label in against_inputs(inputs).items()
    }


def fraction(inputs: Mapping[str, Any]) -> float:
    value = number(inputs.get("max_invalid_fraction"))
    return min(max(value, 0.0), 1.0) if value is not None else 0.0


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    mode = inputs.get("mode", "ingest")
    if mode == "tokenize":
        issues = _staged_issue(staged, DATASET_INPUT) + _staged_issue(staged, TOKENIZER_INPUT)
        ctx_len = inputs.get("context_length")
        if ctx_len is not None and (integer(ctx_len) is None or cast(int, ctx_len) < 1):
            issues.append(
                Issue(TASK_INPUT_INVALID, "`context_length` must be a whole number above 0.")
            )
        return issues
    if mode != "ingest":
        return [
            Issue(TASK_INPUT_INVALID, "`mode` must be ingest or tokenize.", path="/inputs/mode")
        ]
    return (
        _schema_issue(inputs) + _staged_issue(staged, SOURCE_INPUT) + against_issues(inputs, staged)
    )


def source_split(name: str) -> str:
    """Recognize split directories and shard names, falling back to train."""
    path = PurePosixPath(name.lower())
    for part in reversed(path.parts[:-1]):
        if part in SPLITS:
            return part
    for part in path.stem.replace("_", "-").split("-"):
        if part in SPLITS:
            return part
    return "train"


def _ingest(ctx: TaskContext) -> Iterator[TaskEvent]:
    schema_id = str(ctx.inputs["schema_id"])
    out = ctx.output_dir
    out.mkdir(parents=True, exist_ok=True)
    scratch = out.parent / "data-prep-scratch"
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    try:
        try:
            files, ignored = gather(ctx.staged_paths[SOURCE_INPUT], scratch)
            if not files:
                raise RefusedError(DATASET_UNREADABLE, "No file is a dataset file the app reads.")
        except RefusedError as r:
            refused_manifest(out, schema_id, r, {"files": [], "ignored_files": []})
            yield TaskOutput(MANIFEST, "manifest", "json")
            return
        listing: list[Json] = []

        def records() -> Iterator[tuple[RawRecord, str]]:
            n = 0
            for name, path in files:
                first = n + 1
                split = source_split(name)
                reader = (
                    parquet_records(path)
                    if path.suffix.lower() == ".parquet"
                    else read_records(path, None, Limits())
                )
                for raw in reader:
                    n += 1
                    yield RawRecord(n, raw.value, raw.defect, raw.size), split
                listing.append({"path": name, "first_record": first, "records": n - first + 1})

        against = load_against(ctx.inputs, ctx.staged_paths)
        try:
            result = yield from with_progress(
                lambda report: normalize(
                    records(),
                    schema_id,
                    out,
                    Check(
                        against=against,
                        max_invalid_fraction=fraction(ctx.inputs),
                        cancelled=ctx.cancelled,
                        progress=report,
                        extra={"files": listing, "ignored_files": ignored},
                    ),
                ),
                "check",
            )
        except MrlError as exc:
            if exc.code not in (DATASET_UNREADABLE, COLUMN_TYPE_REFUSED):
                raise
            refused_manifest(
                out,
                schema_id,
                RefusedError(exc.code, f"{exc.message} {exc.detail}".strip()),
                {"files": listing, "ignored_files": ignored},
            )
            yield TaskOutput(MANIFEST, "manifest", "json")
            return
        if ctx.cancelled():
            return
        yield from result.outputs
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def _tokenize(ctx: TaskContext) -> Iterator[TaskEvent]:
    data = DatasetAdapter(ctx.staged_paths[DATASET_INPUT])
    ctx_len = integer(ctx.inputs.get("context_length"))
    limit = integer(ctx.inputs.get("max_records")) or 2000
    preview = preview_tokens(
        ctx.staged_paths[TOKENIZER_INPUT],
        data.schema_id,
        data.records(),
        PreviewOptions(context_length=ctx_len, max_records=max(1, limit)),
    )
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    (ctx.output_dir / TOKEN_PREVIEW).write_text(
        json.dumps(dataclasses.asdict(preview), indent=2, ensure_ascii=False) + "\n", "utf-8"
    )
    yield Progress(step=preview.records, total_steps=preview.records, phase="tokenize")
    yield TaskOutput(TOKEN_PREVIEW, "report", "json")


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    if ctx.inputs.get("mode", "ingest") == "tokenize":
        yield from _tokenize(ctx)
    else:
        yield from _ingest(ctx)


DATA_PREP = BuiltinTask(
    name="data_prep",
    job_kind="DATA_PREP",
    artifact_kind="dataset",
    validate=_validate,
    run=_run,
)
