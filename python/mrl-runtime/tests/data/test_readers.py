"""Streaming readers, including the malicious-input corpus (deep JSON, giant lines, bad Parquet)."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mrl_runtime.data.readers import Limits, RawRecord, detect_format, read_records
from mrl_runtime.model import MrlError

SMALL = Limits(max_record_bytes=1024, max_depth=8)


def _read(path: Path, limits: Limits = SMALL) -> list[RawRecord]:
    return list(read_records(path, None, limits))


def _codes(path: Path, limits: Limits = SMALL) -> list[str | None]:
    return [r.defect for r in _read(path, limits)]


def test_jsonl_basics(tmp_path: Path) -> None:
    path = tmp_path / "a.jsonl"
    path.write_bytes(b'\xef\xbb\xbf{"a": 1}\r\n\n   \n{"a": 2}\n[1]\n{"a": NaN}\n{"a": 1e999999}')
    records = _read(path)
    assert [(r.number, r.defect) for r in records] == [
        (1, None),
        (4, None),
        (5, "not_object"),
        (6, "invalid_json"),
        (7, None),
    ]
    assert records[0].value == {"a": 1}


def test_jsonl_deep_nesting(tmp_path: Path) -> None:
    path = tmp_path / "deep.jsonl"
    nested = "[" * 9 + "]" * 9
    abyss = "[" * 200_000 + "]" * 200_000
    path.write_text(json.dumps({"ok": [[1]]}) + "\n" + nested + "\n" + abyss + "\n")
    assert _codes(path, Limits(max_depth=8)) == [None, "too_deep", "too_deep"]


def test_jsonl_giant_line_is_skipped_and_reading_continues(tmp_path: Path) -> None:
    path = tmp_path / "giant.jsonl"
    with path.open("wb") as f:
        f.write(b'{"a": "' + b"x" * 5_000_000 + b'"}\n')
        f.write(b'{"a": "ok"}\n')
        f.write(b"y" * 3_000_000)  # no newline at all
    records = _read(path)
    assert [r.defect for r in records] == ["oversized_record", None, "oversized_record"]
    assert records[0].size > 5_000_000


def test_jsonl_bad_utf8(tmp_path: Path) -> None:
    path = tmp_path / "u.jsonl"
    path.write_bytes(b'{"a": "caf\xe9"}\n')
    assert _codes(path) == ["bad_utf8"]


def test_json_array(tmp_path: Path) -> None:
    path = tmp_path / "a.json"
    path.write_text(
        ' [ {"a": 1}, {"a": "' + "é" * 300 + '"} , 7, [[[[[[[[[[1]]]]]]]]]] ] ', encoding="utf-8"
    )
    records = _read(path)
    assert [r.defect for r in records] == [None, None, "not_object", "too_deep"]
    assert records[1].size == len(json.dumps({"a": "é" * 300}, ensure_ascii=False).encode())
    big = tmp_path / "big.json"
    big.write_text('[{"a": "' + "x" * 5000 + '"}, {"a": 2}]')
    assert _codes(big) == ["oversized_record", None]
    empty = tmp_path / "empty.json"
    empty.write_text("[]")
    assert _read(empty) == []


def test_json_array_spanning_buffer_chunks(tmp_path: Path) -> None:
    path = tmp_path / "big.json"
    rows = [{"i": i, "text": "word " * 50} for i in range(10_000)]
    path.write_text(json.dumps(rows, ensure_ascii=False))
    records = list(read_records(path))
    assert [r.value["i"] for r in records] == list(range(10_000))


@pytest.mark.parametrize(
    "body",
    [
        '{"a": 1}',
        '[{"a": 1} {"a": 2}]',
        '[{"a": 1}, ',
        '[{"a": "' + "x" * 3_000_000 + '"}]',  # still open past the limit: no resync
        "[" + "[" * 100_000,
    ],
)
def test_json_unreadable(tmp_path: Path, body: str) -> None:
    path = tmp_path / "bad.json"
    path.write_text(body)
    with pytest.raises(MrlError) as err:
        _read(path)
    assert err.value.code == "MRL-DATA-150"


def test_csv_and_tsv(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_text(
        'prompt,completion,label\n"line one\nline two",yes,true\n'
        'x,"[{""role"": ""user""}]",false\nshort\n',
        encoding="utf-8",
    )
    records = _read(path)
    assert records[0].value == {"prompt": "line one\nline two", "completion": "yes", "label": True}
    assert records[1].value["completion"] == [{"role": "user"}]
    assert records[2].defect == "invalid_csv"
    bom = tmp_path / "bom.csv"
    bom.write_bytes(b"\xef\xbb\xbftext\nhi\n")
    assert _read(bom)[0].value == {"text": "hi"}
    tsv = tmp_path / "a.tsv"
    tsv.write_text("text\tmeta\nhello\t{}\n")
    assert _read(tsv)[0].value == {"text": "hello", "meta": {}}


def test_csv_giant_line_and_bad_header(tmp_path: Path) -> None:
    giant = tmp_path / "giant.csv"
    giant.write_text("text\n" + "x" * 5000 + "\n")
    with pytest.raises(MrlError):
        _read(giant)
    dup = tmp_path / "dup.csv"
    dup.write_text("a,a\n1,2\n")
    with pytest.raises(MrlError):
        _read(dup)
    latin = tmp_path / "latin.csv"
    latin.write_bytes(b"text\ncaf\xe9\n")
    with pytest.raises(MrlError):
        _read(latin)


def test_parquet_and_arrow(tmp_path: Path) -> None:
    table = pa.table(
        {
            "prompt": ["a", "b", "c"],
            "label": [True, False, True],
            "messages": [[{"role": "user", "content": "hi"}]] * 3,
            "score": [1.0, float("nan"), 2.0],
        }
    )
    pq.write_table(table, tmp_path / "a.parquet", row_group_size=1)
    records = list(read_records(tmp_path / "a.parquet", None, Limits(batch_rows=2)))
    assert [r.defect for r in records] == [None, "invalid_json", None]
    assert records[0].value["messages"] == [{"role": "user", "content": "hi"}]
    with (
        pa.OSFile(str(tmp_path / "a.arrow"), "wb") as sink,
        pa.ipc.new_file(sink, table.schema) as writer,
    ):
        writer.write_table(table)
    assert len(_read(tmp_path / "a.arrow")) == 3
    with (
        pa.OSFile(str(tmp_path / "s.arrow"), "wb") as sink,
        pa.ipc.new_stream(sink, table.schema) as writer,
    ):
        writer.write_table(table)
    assert len(_read(tmp_path / "s.arrow")) == 3


def test_hf_saved_dataset(tmp_path: Path) -> None:
    table = pa.table({"text": ["one", "two"]})
    folder = tmp_path / "ds"
    folder.mkdir()
    for name in ("data-00000-of-00002.arrow", "data-00001-of-00002.arrow"):
        with (
            pa.OSFile(str(folder / name), "wb") as sink,
            pa.ipc.new_stream(sink, table.schema) as w,
        ):
            w.write_table(table)
    files = [{"filename": "data-00000-of-00002.arrow"}, {"filename": "data-00001-of-00002.arrow"}]
    (folder / "state.json").write_text(json.dumps({"_data_files": files}))
    assert detect_format(folder) == "hf_dataset"
    assert [r.number for r in _read(folder)] == [1, 2, 3, 4]
    (folder / "state.json").write_text(json.dumps({"_data_files": [{"filename": "../x.arrow"}]}))
    with pytest.raises(MrlError):
        _read(folder)


def test_malformed_parquet(tmp_path: Path) -> None:
    junk = tmp_path / "junk.parquet"
    junk.write_bytes(b"PAR1" + b"\x00garbage" * 50 + b"PAR1")
    good = tmp_path / "good.parquet"
    pq.write_table(pa.table({"text": ["x" * 100] * 100}), good)
    cut = tmp_path / "cut.parquet"
    cut.write_bytes(good.read_bytes()[: good.stat().st_size // 2])
    empty = tmp_path / "empty.parquet"
    empty.write_bytes(b"")
    for path in (junk, cut, empty):
        with pytest.raises(MrlError) as err:
            _read(path)
        assert err.value.code == "MRL-DATA-150", path.name


def test_refused_column_types(tmp_path: Path) -> None:
    pq.write_table(pa.table({"image": [b"\x89PNG"]}), tmp_path / "bin.parquet")
    with pytest.raises(MrlError) as err:
        _read(tmp_path / "bin.parquet")
    assert err.value.code == "MRL-DATA-151"
    assert "image" in err.value.detail
    deep: pa.DataType = pa.int64()
    for _ in range(10):
        deep = pa.list_(deep)
    pq.write_table(pa.table({"x": pa.array([None], type=deep)}), tmp_path / "deep.parquet")
    with pytest.raises(MrlError):
        _read(tmp_path / "deep.parquet")


def test_oversized_parquet_row(tmp_path: Path) -> None:
    pq.write_table(pa.table({"text": ["x" * 2000, "ok"]}), tmp_path / "big.parquet")
    assert _codes(tmp_path / "big.parquet") == ["oversized_record", None]


def test_max_records_and_unknown_suffix(tmp_path: Path) -> None:
    path = tmp_path / "a.jsonl"
    path.write_text('{"a": 1}\n' * 10)
    assert len(list(read_records(path, None, Limits(max_records=3)))) == 3
    with pytest.raises(MrlError):
        detect_format(tmp_path / "a.xlsx")
    with pytest.raises(MrlError):
        detect_format(tmp_path)
