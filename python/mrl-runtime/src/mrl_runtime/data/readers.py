"""Streaming dataset readers with strict limits (Doc 08 §2.4 step 1).

Every reader yields `RawRecord`s one at a time and never holds more than one record (or one
Arrow batch) in memory. Per-record problems become reader defects (`bad_utf8`,
`invalid_json`, `oversized_record`, `too_deep`, `not_object`, `invalid_csv`); problems with the
file as a whole raise MrlError (MRL-DATA-150 unreadable, MRL-DATA-151 refused column type).
No `eval`, no pickle.
"""

import csv
import importlib
import json
import math
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Literal

from mrl_runtime.model._json import arr, obj, text
from mrl_runtime.model.errors import COLUMN_TYPE_REFUSED, DATASET_UNREADABLE, MrlError

type FileFormat = Literal["jsonl", "json", "csv", "tsv", "parquet", "arrow", "hf_dataset"]

SUFFIXES: dict[str, FileFormat] = {
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".json": "json",
    ".csv": "csv",
    ".tsv": "tsv",
    ".parquet": "parquet",
    ".arrow": "arrow",
    ".feather": "arrow",
}
READER_DEFECTS = frozenset(
    {"bad_utf8", "invalid_json", "oversized_record", "too_deep", "not_object", "invalid_csv"}
)
CHUNK = 1024 * 1024


@dataclass(frozen=True)
class Limits:
    """Parse limits. Defaults follow Doc 08 §2.4 (16 MiB per record)."""

    max_record_bytes: int = 16 * 1024 * 1024
    max_depth: int = 64
    max_records: int | None = None
    batch_rows: int = 1024


@dataclass(frozen=True)
class RawRecord:
    """One record: `value` is set when the reader parsed it, else `defect` says why not.

    `number` is the 1-based line for JSONL, the data row for CSV/TSV, else the record index.
    """

    number: int
    value: Any
    defect: str | None
    size: int


def _unreadable(path: Path, why: str) -> MrlError:
    return MrlError(
        DATASET_UNREADABLE, "The app could not read this dataset file.", f"{path.name}: {why}"
    )


def detect_format(path: Path) -> FileFormat:
    """The reader for a path, from its suffix (or a saved Hugging Face dataset folder)."""
    if path.is_dir():
        if (path / "state.json").is_file():
            return "hf_dataset"
        raise _unreadable(path, "folder is not a saved Hugging Face dataset")
    fmt = SUFFIXES.get(path.suffix.lower())
    if fmt is None:
        raise _unreadable(path, f"unsupported file type {path.suffix or '(none)'}")
    return fmt


def _utf8(text: str) -> bool:
    """False for strings UTF-8 can't encode (lone surrogates from `\\ud800`-style escapes)."""
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def check_value(value: object, max_depth: int) -> str | None:
    """The defect of a parsed value, or None: `too_deep`, `bad_utf8` (in a key or a string)
    or `invalid_json` (NaN / infinite numbers). Iterative, so deep input can't overflow."""
    stack = [(value, 0)]
    while stack:
        node, level = stack.pop()
        if isinstance(node, str):
            if not _utf8(node):
                return "bad_utf8"
            continue
        if isinstance(node, float) and not math.isfinite(node):
            return "invalid_json"
        o = obj(node)
        children = list(o.values()) if o is not None else arr(node)
        if children is None:
            continue
        if level + 1 > max_depth:
            return "too_deep"
        if o is not None and not all(_utf8(k) for k in o):
            return "bad_utf8"
        stack.extend((child, level + 1) for child in children)
    return None


def _reject_constant(name: str) -> float:
    raise ValueError(f"{name} is not valid JSON")


def parse_json(data: bytes | str, limits: Limits) -> tuple[Any, str | None]:
    """(value, None) or (None, defect) for one JSON text, enforcing UTF-8 and depth."""
    try:
        source = data.decode("utf-8") if isinstance(data, bytes) else data
    except UnicodeDecodeError:
        return None, "bad_utf8"
    try:
        value: Any = json.loads(source, parse_constant=_reject_constant)
    except RecursionError:
        return None, "too_deep"
    except ValueError:  # JSONDecodeError, NaN/Infinity, over-long integers
        return None, "invalid_json"
    defect = check_value(value, limits.max_depth)
    return (None, defect) if defect else (value, None)


def _record(number: int, value: Any, defect: str | None, size: int) -> RawRecord:
    if defect is None and obj(value) is None:
        defect = "not_object"
    return RawRecord(number, value if defect is None else None, defect, size)


def read_jsonl(path: Path, limits: Limits) -> Iterator[RawRecord]:
    """One JSON object per line; blank lines are skipped, a leading BOM is dropped."""
    with path.open("rb") as f:
        number = 0
        while True:
            line = f.readline(limits.max_record_bytes + 1)
            if not line:
                return
            number += 1
            size = len(line)
            if size > limits.max_record_bytes and not line.endswith(b"\n"):
                while (rest := f.readline(CHUNK)) and not rest.endswith(b"\n"):
                    size += len(rest)
                yield RawRecord(number, None, "oversized_record", size + len(rest))
                continue
            line = line.rstrip(b"\r\n")
            if number == 1:
                line = line.removeprefix(b"\xef\xbb\xbf")
            if len(line) > limits.max_record_bytes:
                yield RawRecord(number, None, "oversized_record", len(line))
                continue
            if not line.strip():
                continue
            yield _record(number, *parse_json(line, limits), len(line))


class _JsonArray:
    """Incremental decoder over a file holding one top-level JSON array."""

    def __init__(self, f: IO[bytes], path: Path, limits: Limits) -> None:
        self.f, self.path, self.limits = f, path, limits
        self.decoder = json.JSONDecoder(parse_constant=_reject_constant)
        self.buf, self.pos, self.eof, self.pending = "", 0, False, b""

    def fill(self, n: int) -> None:
        raw = self.f.read(n)
        self.eof = not raw
        data = self.pending + raw
        try:
            chunk, self.pending = data.decode("utf-8"), b""
        except UnicodeDecodeError as exc:
            if self.eof or exc.start < len(data) - 3:
                raise _unreadable(self.path, "file is not UTF-8") from exc
            chunk, self.pending = data[: exc.start].decode("utf-8"), data[exc.start :]
        self.buf, self.pos = self.buf[self.pos :] + chunk, 0

    def peek(self) -> str:
        """The next non-space character ("" at the end of the file)."""
        while True:
            while self.pos < len(self.buf) and self.buf[self.pos] in " \t\r\n\ufeff":
                self.pos += 1
            if self.pos < len(self.buf):
                return self.buf[self.pos]
            if self.eof:
                return ""
            self.fill(CHUNK)

    def value(self, number: int) -> tuple[Any, int]:
        """Decode the next value; returns (value, its size in bytes)."""
        want = CHUNK
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buf, self.pos)
                if end < len(self.buf) or self.eof:
                    break
                self.fill(want)  # a number may continue past the buffer
            except RecursionError:
                raise _unreadable(self.path, f"record {number} is nested too deeply") from None
            except ValueError as exc:
                if self.eof or len(self.buf) - self.pos > self.limits.max_record_bytes:
                    why = "is not valid JSON" if self.eof else "is too large"
                    raise _unreadable(self.path, f"record {number} {why}") from exc
                self.fill(want)
                want *= 2
        size = len(self.buf[self.pos : end].encode("utf-8"))
        self.pos = end
        return value, size


def read_json_array(path: Path, limits: Limits) -> Iterator[RawRecord]:
    """A top-level JSON array of records, decoded one element at a time.

    One record may be at most `max_records_bytes`; past that the file cannot be resynced, so
    it raises MRL-DATA-150 instead of yielding a defect.
    """
    with path.open("rb") as f:
        stream = _JsonArray(f, path, limits)
        stream.fill(CHUNK)
        if stream.peek() != "[":
            raise _unreadable(path, "a JSON dataset must be an array of records")
        stream.pos += 1
        number = 0
        while (char := stream.peek()) != "]":
            if not char:
                raise _unreadable(path, "the array is not closed")
            if number:
                if char != ",":
                    raise _unreadable(path, f"expected a comma after record {number}")
                stream.pos += 1
                stream.peek()
            number += 1
            value, size = stream.value(number)
            if size > limits.max_record_bytes:
                yield RawRecord(number, None, "oversized_record", size)
            elif defect := check_value(value, limits.max_depth):
                yield RawRecord(number, None, defect, size)
            else:
                yield _record(number, value, None, size)
        stream.pos += 1  # the closing bracket
        if stream.peek():
            raise _unreadable(path, "the file has more data after the closing bracket")


class _BoundedLines:
    """Text lines for csv.reader; counts the UTF-8 bytes read.

    A line may be up to twice the record limit, so a record a little over the limit is
    reported as `oversized_record`; a longer line stops the read (CSV can't resync).
    """

    def __init__(self, f: IO[str], limit: int) -> None:
        self.f, self.limit, self.bytes = f, limit, 0

    def __iter__(self) -> "_BoundedLines":
        return self

    def __next__(self) -> str:
        line = self.f.readline(2 * self.limit + 1)
        if not line:
            raise StopIteration
        size = len(line.encode("utf-8"))
        if size > 2 * self.limit:
            raise csv.Error("a line is larger than the record limit")
        self.bytes += size
        return line


def _csv_cell(cell: str) -> object:
    """CSV cells are text; JSON arrays/objects and `true`/`false` are decoded."""
    stripped = cell.strip()
    if stripped in ("true", "false"):
        return stripped == "true"
    if stripped[:1] in ("[", "{"):
        try:
            value: object = json.loads(stripped, parse_constant=_reject_constant)
        except (ValueError, RecursionError):
            return cell
        return value
    return cell


def read_csv(path: Path, limits: Limits, delimiter: str) -> Iterator[RawRecord]:
    """CSV/TSV with an explicit dialect: header row, `"` quoting, strict parsing."""
    # ponytail: csv.field_size_limit is process-wide; set for the read, restored after.
    previous = csv.field_size_limit(2 * limits.max_record_bytes)
    try:
        with path.open(encoding="utf-8-sig", newline="") as f:  # Excel adds a BOM
            lines = _BoundedLines(f, limits.max_record_bytes)
            reader = csv.reader(lines, delimiter=delimiter, quotechar='"', strict=True)
            try:
                header = next(reader, None)
                if header is None:
                    return
                if len(set(header)) != len(header) or not all(header):
                    raise _unreadable(path, "the header row has empty or repeated column names")
                start = lines.bytes
                for number, row in enumerate(reader, 1):
                    size, start = lines.bytes - start, lines.bytes
                    if len(row) != len(header):
                        yield RawRecord(number, None, "invalid_csv", size)
                    elif size > limits.max_record_bytes:
                        yield RawRecord(number, None, "oversized_record", size)
                    else:
                        value = {k: _csv_cell(v) for k, v in zip(header, row, strict=True)}
                        if defect := check_value(value, limits.max_depth):
                            yield RawRecord(number, None, defect, size)
                        else:
                            yield RawRecord(number, value, None, size)
            except csv.Error as exc:
                where = f"CSV parsing stopped near line {reader.line_num}: {exc}"
                raise _unreadable(path, where) from exc
            except UnicodeDecodeError as exc:
                raise _unreadable(path, "file is not UTF-8") from exc
    finally:
        csv.field_size_limit(previous)


def _pyarrow() -> Any:
    """pyarrow, imported on first use (heavy native library; it ships no type stubs)."""
    return importlib.import_module("pyarrow")


def _arrow_type_ok(pa: Any, kind: Any, max_depth: int) -> str | None:
    """None if the Arrow type is allowed, else the refused type's name."""
    types = pa.types
    if max_depth < 0:
        return "nesting deeper than the limit"
    if (
        types.is_string(kind)
        or types.is_large_string(kind)
        or types.is_string_view(kind)
        or types.is_boolean(kind)
        or types.is_null(kind)
        or types.is_integer(kind)
        or types.is_floating(kind)
    ):
        return None
    if types.is_list(kind) or types.is_large_list(kind) or types.is_list_view(kind):
        return _arrow_type_ok(pa, kind.value_type, max_depth - 1)
    if types.is_struct(kind):
        for i in range(kind.num_fields):
            bad = _arrow_type_ok(pa, kind.field(i).type, max_depth - 1)
            if bad:
                return bad
        return None
    return str(kind)


def _check_schema(pa: Any, path: Path, schema: Any, limits: Limits) -> None:
    for i in range(len(schema.names)):
        field = schema.field(i)
        bad = _arrow_type_ok(pa, field.type, limits.max_depth - 1)
        if bad:
            raise MrlError(
                COLUMN_TYPE_REFUSED,
                "This dataset file has a column type the app doesn't read.",
                f"{path.name}: column {field.name} is {bad}",
            )


def _arrow_rows(
    pa: Any, path: Path, batches: Iterator[Any], limits: Limits, start: int
) -> Iterator[RawRecord]:
    number = start
    try:
        for batch in batches:
            rows: list[dict[str, Any]] = batch.to_pylist()
            for row in rows:
                number += 1
                if defect := check_value(row, limits.max_depth):
                    yield RawRecord(number, None, defect, 0)
                    continue
                size = len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
                if size > limits.max_record_bytes:
                    yield RawRecord(number, None, "oversized_record", size)
                else:
                    yield RawRecord(number, row, None, size)
    except (pa.ArrowException, OSError) as exc:
        raise _unreadable(path, f"reading stopped after record {number}: {exc}") from exc


def read_parquet(path: Path, limits: Limits) -> Iterator[RawRecord]:
    pa = _pyarrow()
    pq = importlib.import_module("pyarrow.parquet")

    try:
        pf: Any = pq.ParquetFile(path)
    except (pa.ArrowException, OSError) as exc:
        raise _unreadable(path, f"not a readable Parquet file: {exc}") from exc
    _check_schema(pa, path, pf.schema_arrow, limits)
    yield from _arrow_rows(pa, path, pf.iter_batches(batch_size=limits.batch_rows), limits, 0)


def _open_ipc(pa: Any, path: Path) -> Any:
    """An Arrow IPC reader for the file format, falling back to the stream format."""
    try:
        return pa.ipc.open_file(path)
    except (pa.ArrowException, OSError):
        try:
            return pa.ipc.open_stream(pa.OSFile(str(path)))
        except (pa.ArrowException, OSError) as exc:
            raise _unreadable(path, f"not a readable Arrow file: {exc}") from exc


def _ipc_batches(reader: Any) -> Iterator[Any]:
    if hasattr(reader, "num_record_batches"):
        for i in range(reader.num_record_batches):
            yield reader.get_batch(i)
    else:
        yield from reader


def read_arrow(path: Path, limits: Limits, start: int = 0) -> Iterator[RawRecord]:
    pa = _pyarrow()

    reader = _open_ipc(pa, path)
    _check_schema(pa, path, reader.schema, limits)
    yield from _arrow_rows(pa, path, _ipc_batches(reader), limits, start)


def read_hf_dataset(path: Path, limits: Limits) -> Iterator[RawRecord]:
    """A `datasets.Dataset.save_to_disk` folder: Arrow stream files listed in state.json."""
    try:
        state = obj(json.loads((path / "state.json").read_text(encoding="utf-8")))
    except (ValueError, RecursionError, OSError) as exc:
        raise _unreadable(path, "state.json is not readable") from exc
    files = arr((state or {}).get("_data_files"))
    if not files:
        raise _unreadable(path, "state.json lists no data files")
    count = 0
    for entry in files:
        name = text((obj(entry) or {}).get("filename"))
        if not name or "/" in name or "\\" in name or name.startswith("."):
            raise _unreadable(path, "state.json has a bad data file name")
        for record in read_arrow(path / name, limits, count):
            count = record.number
            yield record


def read_records(
    path: Path, fmt: FileFormat | None = None, limits: Limits | None = None
) -> Iterator[RawRecord]:
    """Stream the records of a dataset file, at most `limits.max_records` of them."""
    limits = limits or Limits()
    fmt = fmt or detect_format(path)
    if fmt == "jsonl":
        records = read_jsonl(path, limits)
    elif fmt == "json":
        records = read_json_array(path, limits)
    elif fmt in ("csv", "tsv"):
        records = read_csv(path, limits, "," if fmt == "csv" else "\t")
    elif fmt == "parquet":
        records = read_parquet(path, limits)
    elif fmt == "arrow":
        records = read_arrow(path, limits)
    else:
        records = read_hf_dataset(path, limits)
    for i, record in enumerate(records):
        if limits.max_records is not None and i >= limits.max_records:
            return
        yield record
