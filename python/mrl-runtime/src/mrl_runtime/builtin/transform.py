"""The `transform` task (DATA_PREP job, `data_prep` sandbox) and the filter language.

A transform spec (`transform/v1`, Doc 08 §2.5) lists steps run in order over a ready parent
version (staged as `parent`); the result is checked against `to` like an upload and becomes
a new immutable version. `preview: true` runs the steps on the first 20 parent records and
writes `preview.json` (each record before and after, or the step that dropped it) instead.

Steps (`op`): `filter {expr}`, `map_columns {columns: {from: to}}`, `rename {from, to}`,
`template {to, template}` (`{field}` placeholders, other braces kept), `to_messages {to,
messages: [{role, from}]}`, `cast {field, type, true_values?}`, `select {fields}`,
`drop_empty {field}`, `dedupe {fields?}` (whole record without `fields`), `max_length {field,
max_chars}`, `split {train, validation, test, seed}`, `sample {n, seed}`. Fields use dots for
nested values. A record a step can't apply to (a missing field, a value that can't be cast)
is dropped and counted in `transform_report.json`.

Filter expressions (the same language filters records in the API; the server has a twin of
this evaluator and both run `tests/fixtures/p27/expressions.json`):
- values: field paths (`a.b`, `turns[0].content`; missing = null), 'text' or "text"
  (escapes \\\\ \\' \\" \\n \\t \\r), whole and decimal numbers, true, false, null, [lists];
- `==` `!=` (numbers by value, true/false never equal numbers), `<` `<=` `>` `>=` (two numbers
  or two texts, else false), `in` / `not in` (text in text, value in list, key in object),
  `and` `or` `not` (only `true` counts as true), parentheses;
- functions: len (text characters, list or object size, else 0), lower, upper,
  contains(a, b), startswith, endswith, has(path).
At most 2000 characters and 32 levels of nesting.
"""

import hashlib
import json
import math
import random
import re
import shutil
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from itertools import islice
from pathlib import Path
from typing import Any, cast

from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.data_prep import (
    Check,
    against_issues,
    fraction,
    load_against,
    normalize,
    record_line,
    with_progress,
)
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.data.adapter import SPLITS, DatasetAdapter, canonical_json
from mrl_runtime.data.readers import Limits, RawRecord
from mrl_runtime.data.validate import DATASET_SCHEMAS, record_hash
from mrl_runtime.errors import TASK_INPUT_INVALID, MrlError
from mrl_runtime.model._json import Json, arr, integer, number, obj, text

PARENT_INPUT = "parent"
PREVIEW_FILE = "preview.json"
REPORT_FILE = "transform_report.json"
PREVIEW_ROWS = 20
# Matches the server limit for a built-in JSON output.
PREVIEW_MAX_BYTES = 8 * 1024 * 1024
MAX_EXPR_CHARS = 2000
MAX_DEPTH = 32

# ---- the filter language ----


class ExprError(ValueError):
    """An expression that can't be read; `pos` is the character it stopped at."""

    def __init__(self, message: str, pos: int) -> None:
        super().__init__(f"{message} (at character {pos + 1})")
        self.pos = pos


_TOKEN = re.compile(
    r"""(?P<ws>\s+)
      |(?P<num>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
      |(?P<str>'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")
      |(?P<name>[A-Za-z_][A-Za-z0-9_]*)
      |(?P<op>==|!=|<=|>=|<|>|\(|\)|\[|\]|,|\.)""",
    re.VERBOSE,
)
_ESCAPES = {"\\": "\\", "'": "'", '"': '"', "n": "\n", "t": "\t", "r": "\r"}
_KEYWORDS = {"and", "or", "not", "in", "true", "false", "null"}
_FUNCS = {
    "len": 1,
    "lower": 1,
    "upper": 1,
    "contains": 2,
    "startswith": 2,
    "endswith": 2,
    "has": 1,
}
_I64 = 2**63


@dataclass(frozen=True)
class _Tok:
    kind: str  # num, str, name, op, end
    text: str
    pos: int
    value: object = None


def _string(raw: str, pos: int) -> str:
    out: list[str] = []
    i = 1
    while i < len(raw) - 1:
        c = raw[i]
        if c == "\\":
            nxt = raw[i + 1]
            if nxt not in _ESCAPES:
                raise ExprError(f"unknown escape \\{nxt}", pos + i)
            out.append(_ESCAPES[nxt])
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _tokens(src: str) -> list[_Tok]:
    if len(src) > MAX_EXPR_CHARS:
        raise ExprError(f"the expression is longer than {MAX_EXPR_CHARS} characters", 0)
    out: list[_Tok] = []
    pos = 0
    while pos < len(src):
        m = _TOKEN.match(src, pos)
        if m is None:
            raise ExprError(f"unexpected character {src[pos]!r}", pos)
        kind = m.lastgroup or ""
        raw = m.group()
        if kind == "num":
            if "." in raw or "e" in raw.lower():
                value: object = float(raw)
                if not math.isfinite(value):
                    raise ExprError("the number is too large", pos)
            else:
                value = int(raw)
                if value >= _I64:
                    raise ExprError("the number is too large", pos)
            out.append(_Tok("num", raw, pos, value))
        elif kind == "str":
            out.append(_Tok("str", raw, pos, _string(raw, pos)))
        elif kind in ("name", "op"):
            out.append(_Tok(kind, raw, pos))
        pos = m.end()
    out.append(_Tok("end", "", len(src)))
    return out


type Node = tuple[Any, ...]


class _Parser:
    def __init__(self, src: str) -> None:
        self.toks = _tokens(src)
        self.i = 0
        self.depth = 0

    def peek(self) -> _Tok:
        return self.toks[self.i]

    def take(self) -> _Tok:
        t = self.toks[self.i]
        self.i += 1
        return t

    def word(self, w: str) -> bool:
        t = self.peek()
        if t.kind == "name" and t.text == w:
            self.i += 1
            return True
        return False

    def sym(self, s: str) -> bool:
        t = self.peek()
        if t.kind == "op" and t.text == s:
            self.i += 1
            return True
        return False

    def expect(self, s: str) -> None:
        if not self.sym(s):
            t = self.peek()
            raise ExprError(f"expected {s!r} but found {t.text or 'the end'!r}", t.pos)

    def nest(self) -> None:
        self.depth += 1
        if self.depth > MAX_DEPTH:
            raise ExprError(f"the expression nests deeper than {MAX_DEPTH} levels", self.peek().pos)

    def parse(self) -> Node:
        node = self.or_expr()
        t = self.peek()
        if t.kind != "end":
            raise ExprError(f"unexpected {t.text!r}", t.pos)
        return node

    def or_expr(self) -> Node:
        self.nest()
        node = self.and_expr()
        while self.word("or"):
            node = ("or", node, self.and_expr())
        self.depth -= 1
        return node

    def and_expr(self) -> Node:
        node = self.not_expr()
        while self.word("and"):
            node = ("and", node, self.not_expr())
        return node

    def not_expr(self) -> Node:
        if self.word("not"):
            self.nest()
            node = ("not", self.not_expr())
            self.depth -= 1
            return node
        return self.comparison()

    def comparison(self) -> Node:
        left = self.operand()
        t = self.peek()
        if t.kind == "op" and t.text in ("==", "!=", "<", "<=", ">", ">="):
            self.take()
            return ("cmp", t.text, left, self.operand())
        if self.word("in"):
            return ("in", left, self.operand())
        if t.kind == "name" and t.text == "not":
            nxt = self.toks[self.i + 1]
            if nxt.kind == "name" and nxt.text == "in":
                self.i += 2
                return ("not", ("in", left, self.operand()))
        return left

    def operand(self) -> Node:
        t = self.take()
        if t.kind in ("num", "str"):
            return ("lit", t.value)
        if t.kind == "op" and t.text == "(":
            node = self.or_expr()
            self.expect(")")
            return node
        if t.kind == "op" and t.text == "[":
            self.nest()
            items: list[Node] = []
            if not self.sym("]"):
                items.append(self.or_expr())
                while self.sym(","):
                    items.append(self.or_expr())
                self.expect("]")
            self.depth -= 1
            return ("list", tuple(items))
        if t.kind == "name":
            if t.text in ("true", "false", "null"):
                return ("lit", {"true": True, "false": False, "null": None}[t.text])
            if t.text in _KEYWORDS:
                raise ExprError(f"unexpected {t.text!r}", t.pos)
            if t.text in _FUNCS and self.sym("("):
                return self.call(t)
            return self.path(t)
        raise ExprError(f"unexpected {t.text or 'end of the expression'!r}", t.pos)

    def call(self, name: _Tok) -> Node:
        self.nest()
        args: list[Node] = []
        if not self.sym(")"):
            if name.text == "has":
                first = self.take()
                if first.kind != "name" or first.text in _KEYWORDS:
                    raise ExprError("has() takes a field name", first.pos)
                args.append(self.path(first))
            else:
                args.append(self.or_expr())
            while self.sym(","):
                args.append(self.or_expr())
            self.expect(")")
        self.depth -= 1
        if len(args) != _FUNCS[name.text]:
            raise ExprError(f"{name.text}() takes {_FUNCS[name.text]} value(s)", name.pos)
        return ("call", name.text, tuple(args))

    def path(self, first: _Tok) -> Node:
        parts: list[str | int] = [first.text]
        while True:
            if self.sym("."):
                t = self.take()
                if t.kind != "name":
                    raise ExprError("expected a field name after '.'", t.pos)
                parts.append(t.text)
            elif self.sym("["):
                t = self.take()
                if t.kind != "num" or not isinstance(t.value, int):
                    raise ExprError("expected a whole number inside [ ]", t.pos)
                parts.append(t.value)
                self.expect("]")
            else:
                return ("path", tuple(parts))


def _is_num(v: object) -> bool:
    return isinstance(v, int | float) and not isinstance(v, bool)


def _eq(a: object, b: object) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a is b
    if _is_num(a) and _is_num(b):
        return cast(float, a) == cast(float, b)
    la, lb = arr(a), arr(b)
    if la is not None and lb is not None:
        return len(la) == len(lb) and all(_eq(x, y) for x, y in zip(la, lb, strict=True))
    oa, ob = obj(a), obj(b)
    if oa is not None and ob is not None:
        return oa.keys() == ob.keys() and all(_eq(v, ob[k]) for k, v in oa.items())
    if type(a) is not type(b):
        return False
    return a == b


def _order(op: str, a: object, b: object) -> bool:
    if not ((_is_num(a) and _is_num(b)) or (isinstance(a, str) and isinstance(b, str))):
        return False
    x, y = cast(Any, a), cast(Any, b)
    return {"<": x < y, "<=": x <= y, ">": x > y, ">=": x >= y}[op]


def _in(needle: object, hay: object) -> bool:
    if isinstance(hay, str):
        return isinstance(needle, str) and needle in hay
    items = arr(hay)
    if items is not None:
        return any(_eq(needle, x) for x in items)
    o = obj(hay)
    return o is not None and isinstance(needle, str) and needle in o


def lookup(record: object, parts: Iterable[str | int]) -> tuple[bool, object]:
    """(found, value) of a path in a record."""
    cur: object = record
    for p in parts:
        if isinstance(p, int):
            items = arr(cur)
            if items is None or p >= len(items):
                return False, None
            cur = items[p]
        else:
            o = obj(cur)
            if o is None or p not in o:
                return False, None
            cur = o[p]
    return True, cur


def _eval(node: Node, rec: object) -> object:  # noqa: PLR0911 (one case per node kind)
    match node[0]:
        case "lit":
            return node[1]
        case "path":
            return lookup(rec, node[1])[1]
        case "list":
            return [_eval(n, rec) for n in node[1]]
        case "or":
            return _eval(node[1], rec) is True or _eval(node[2], rec) is True
        case "and":
            return _eval(node[1], rec) is True and _eval(node[2], rec) is True
        case "not":
            return _eval(node[1], rec) is not True
        case "in":
            return _in(_eval(node[1], rec), _eval(node[2], rec))
        case "cmp":
            a, b = _eval(node[2], rec), _eval(node[3], rec)
            if node[1] == "==":
                return _eq(a, b)
            if node[1] == "!=":
                return not _eq(a, b)
            return _order(node[1], a, b)
        case _:
            return _call(node[1], node[2], rec)


def _call(name: str, args: tuple[Node, ...], rec: object) -> object:  # noqa: PLR0911 (per function)
    if name == "has":
        return lookup(rec, args[0][1])[0]
    vals = [_eval(a, rec) for a in args]
    v = vals[0]
    if name == "len":
        if isinstance(v, str):
            return len(v)
        items = arr(v)
        if items is not None:
            return len(items)
        o = obj(v)
        return len(o) if o is not None else 0
    if name in ("lower", "upper"):
        return (v.lower() if name == "lower" else v.upper()) if isinstance(v, str) else None
    if name == "contains":
        return _in(vals[1], v)
    if not (isinstance(v, str) and isinstance(vals[1], str)):
        return False
    return v.startswith(vals[1]) if name == "startswith" else v.endswith(vals[1])


@dataclass(frozen=True)
class Expr:
    """A parsed filter expression."""

    source: str
    _node: Node = field(repr=False)

    @staticmethod
    def parse(source: str) -> "Expr":
        return Expr(source, _Parser(source).parse())

    def evaluate(self, record: object) -> object:
        return _eval(self._node, record)

    def matches(self, record: object) -> bool:
        return self.evaluate(record) is True


# ---- steps ----


class Dropped(Exception):  # noqa: N818 (a control-flow signal, not an error)
    """The current record can't go through this step; `reason` is counted."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class Item:
    index: int
    record: Json
    split: str = "train"


def _parts(path: str) -> list[str]:
    return path.split(".")


def _get(rec: Json, path: str) -> object:
    found, value = lookup(rec, _parts(path))
    if not found:
        raise Dropped(f"missing field {path}")
    return value


def _set(rec: Json, path: str, value: object) -> None:
    *head, last = _parts(path)
    cur = rec
    for p in head:
        nxt = obj(cur.get(p))
        if nxt is None:
            nxt = {}
            cur[p] = nxt
        cur = nxt
    cur[last] = value


def _delete(rec: Json, path: str) -> None:
    *head, last = _parts(path)
    found, parent = lookup(rec, head)
    o = obj(parent) if found else None
    if o is not None:
        o.pop(last, None)


def _as_text(value: object) -> str:
    return value if isinstance(value, str) else canonical_json(value)


_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\}")


def _empty(value: object) -> bool:
    return value is None or value in ("", [], {})


def _cast(value: object, kind: str, true_values: list[str]) -> object:
    if kind == "string":
        return _as_text(value)
    if kind == "boolean":
        if isinstance(value, bool):
            return value
        if true_values and isinstance(value, str):
            return value in true_values
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        raise Dropped(f"can't make {value!r} true or false")
    if kind == "integer":
        return _integer_cast(value)
    try:
        n = float(value) if isinstance(value, str) else number(value)
    except (ValueError, OverflowError):
        n = None
    if n is None or not math.isfinite(n):
        raise Dropped(f"can't make {value!r} a number")
    return n


def _integer_cast(value: object) -> int:
    if (n := integer(value)) is not None:
        return n
    try:
        exact = Decimal(value) if isinstance(value, (str, float)) else None
    except InvalidOperation:
        exact = None
    if exact is None or not exact.is_finite():
        raise Dropped(f"can't make {value!r} a number")
    if exact.adjusted() >= 4000 or exact != exact.to_integral_value():
        raise Dropped(f"{value!r} is not a whole number")
    return int(exact)


type Stage = Callable[[Iterator[Item]], Iterator[Item]]


def _per_record(fn: Callable[[Item], Item | None], drop: Callable[[int, str], None]) -> Stage:
    def stage(items: Iterator[Item]) -> Iterator[Item]:
        for it in items:
            try:
                out = fn(it)
            except Dropped as d:
                drop(it.index, d.reason)
                continue
            if out is None:
                drop(it.index, "filtered out")
            else:
                yield out

    return stage


def _split_of(record: Json, seed: int, cuts: list[tuple[str, float]]) -> str:
    digest = hashlib.sha256(f"{seed}:{canonical_json(record)}".encode()).digest()
    u = int.from_bytes(digest[:8], "big") / 2**64
    for name, upto in cuts:
        if u < upto:
            return name
    return cuts[-1][0]


@dataclass
class StepCount:
    op: str
    seen: int = 0
    kept: int = 0
    dropped: dict[str, int] = field(default_factory=dict[str, int])


type Drop = Callable[[int, str], None]


def _filter(step: Json, drop: Drop) -> Stage:
    expr = Expr.parse(str(step["expr"]))
    return _per_record(lambda it: it if expr.matches(it.record) else None, drop)


def _move(pairs: list[tuple[str, str]], drop: Drop) -> Stage:
    def move(it: Item) -> Item:
        values = [(dst, _get(it.record, src)) for src, dst in pairs]
        for src, _ in pairs:
            _delete(it.record, src)
        for dst, v in values:
            _set(it.record, dst, v)
        return it

    return _per_record(move, drop)


def _map_columns(step: Json, drop: Drop) -> Stage:
    return _move(list(cast(dict[str, str], step["columns"]).items()), drop)


def _rename(step: Json, drop: Drop) -> Stage:
    return _move([(str(step["from"]), str(step["to"]))], drop)


def _template(step: Json, drop: Drop) -> Stage:
    template, to = str(step["template"]), str(step["to"])

    def fill(it: Item) -> Item:
        remaining = Limits().max_record_bytes - len(template.encode("utf-8"))

        def substitute(match: re.Match[str]) -> str:
            nonlocal remaining
            value = _as_text(_get(it.record, match.group(1)))
            remaining += len(match.group(0)) - len(value.encode("utf-8"))
            if remaining < 0:
                raise Dropped("the template output is larger than the record limit")
            return value

        if remaining < 0:
            raise Dropped("the template is larger than the record limit")
        _set(it.record, to, _PLACEHOLDER.sub(substitute, template))
        return it

    return _per_record(fill, drop)


def _to_messages(step: Json, drop: Drop) -> Stage:
    spec = [(str(m["role"]), str(m["from"])) for m in cast(list[Json], step["messages"])]
    to = str(step["to"])

    def messages(it: Item) -> Item:
        built = [{"role": r, "content": _as_text(_get(it.record, f))} for r, f in spec]
        _set(it.record, to, built)
        return it

    return _per_record(messages, drop)


def _cast_step(step: Json, drop: Drop) -> Stage:
    f, kind = str(step["field"]), str(step["type"])
    trues = [str(v) for v in arr(step.get("true_values")) or []]

    def convert(it: Item) -> Item:
        _set(it.record, f, _cast(_get(it.record, f), kind, trues))
        return it

    return _per_record(convert, drop)


def _select(step: Json, drop: Drop) -> Stage:
    fields = [str(x) for x in cast(list[object], step["fields"])]

    def keep(it: Item) -> Item:
        out: Json = {}
        for f in fields:
            found, v = lookup(it.record, _parts(f))
            if found:
                _set(out, f, v)
        it.record = out
        return it

    return _per_record(keep, drop)


def _drop_empty(step: Json, drop: Drop) -> Stage:
    f = str(step["field"])
    return _per_record(
        lambda it: None if _empty(lookup(it.record, _parts(f))[1]) else it,
        lambda i, _: drop(i, f"{f} is empty"),
    )


def _max_length(step: Json, drop: Drop) -> Stage:
    f, cap = str(step["field"]), cast(int, step["max_chars"])

    def short(it: Item) -> Item | None:
        v = lookup(it.record, _parts(f))[1]
        return None if isinstance(v, str) and len(v) > cap else it

    return _per_record(short, lambda i, _: drop(i, f"{f} is longer than {cap}"))


def _dedupe(step: Json, drop: Drop) -> Stage:
    fields = [str(x) for x in arr(step.get("fields")) or []]
    seen: set[str] = set()

    def first(it: Item) -> Item | None:
        key = record_hash(
            [lookup(it.record, _parts(f))[1] for f in fields] if fields else it.record
        )
        if key in seen:
            return None
        seen.add(key)
        return it

    return _per_record(first, lambda i, _: drop(i, "duplicate"))


def _split(step: Json, drop: Drop) -> Stage:
    seed = integer(step.get("seed")) or 0
    cuts: list[tuple[str, float]] = []
    total = 0.0
    for name in SPLITS:
        total += number(step.get(name)) or 0.0
        cuts.append((name, total))

    def assign(it: Item) -> Item:
        it.split = _split_of(it.record, seed, cuts)
        return it

    return _per_record(assign, drop)


def _sample(step: Json, drop: Drop) -> Stage:
    """A seeded reservoir, emitted in the original order."""
    want, rng = cast(int, step["n"]), random.Random(integer(step.get("seed")) or 0)

    def sample(items: Iterator[Item]) -> Iterator[Item]:
        # ponytail: holds `n` records in memory; a two-pass index sample if n gets huge.
        kept: list[Item] = []
        for seen_n, it in enumerate(items):
            if len(kept) < want:
                kept.append(it)
                continue
            j = rng.randint(0, seen_n)
            if j < want:
                drop(kept[j].index, "not sampled")
                kept[j] = it
            else:
                drop(it.index, "not sampled")
        yield from sorted(kept, key=lambda i: i.index)

    return sample


_STEPS: dict[str, Callable[[Json, Drop], Stage]] = {
    "filter": _filter,
    "map_columns": _map_columns,
    "rename": _rename,
    "template": _template,
    "to_messages": _to_messages,
    "cast": _cast_step,
    "select": _select,
    "drop_empty": _drop_empty,
    "dedupe": _dedupe,
    "max_length": _max_length,
    "split": _split,
    "sample": _sample,
}


def _stage(step: Json, count: StepCount, drops: dict[int, tuple[int, str]], n: int) -> Stage:
    def drop(index: int, reason: str) -> None:
        count.dropped[reason] = count.dropped.get(reason, 0) + 1
        drops.setdefault(index, (n, reason))

    return _STEPS[count.op](step, drop)


@dataclass
class Pipeline:
    """Compiled steps plus their counts and the first drop of each record."""

    steps: list[Json]
    counts: list[StepCount] = field(default_factory=list[StepCount])
    drops: dict[int, tuple[int, str]] = field(default_factory=dict[int, tuple[int, str]])

    def run(self, items: Iterator[Item]) -> Iterator[Item]:
        self.counts = [StepCount(str(s["op"])) for s in self.steps]
        stream = items
        for n, (step, count) in enumerate(zip(self.steps, self.counts, strict=True)):
            stream = self._counted(
                _stage(step, count, self.drops, n)(self._seen(stream, count)), count
            )
        return stream

    @staticmethod
    def _seen(items: Iterator[Item], count: StepCount) -> Iterator[Item]:
        for it in items:
            count.seen += 1
            yield it

    @staticmethod
    def _counted(items: Iterator[Item], count: StepCount) -> Iterator[Item]:
        for it in items:
            count.kept += 1
            yield it

    def report(self) -> list[Json]:
        return [
            {"op": c.op, "in": c.seen, "out": c.kept, "dropped": c.dropped} for c in self.counts
        ]


# ---- spec checks ----

_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
_REQUIRED: dict[str, dict[str, str]] = {
    "filter": {"expr": "text"},
    "map_columns": {"columns": "mapping"},
    "rename": {"from": "field", "to": "field"},
    "template": {"to": "field", "template": "text"},
    "to_messages": {"to": "field", "messages": "messages"},
    "cast": {"field": "field", "type": "cast"},
    "select": {"fields": "fields"},
    "drop_empty": {"field": "field"},
    "dedupe": {},
    "max_length": {"field": "field", "max_chars": "count"},
    "split": {},
    "sample": {"n": "count"},
}
_OPTIONAL: dict[str, dict[str, str]] = {
    "cast": {"true_values": "texts"},
    "dedupe": {"fields": "fields"},
    "split": {"train": "share", "validation": "share", "test": "share", "seed": "seed"},
    "sample": {"seed": "seed"},
}


def _kind_ok(kind: str, v: object) -> bool:  # noqa: PLR0911 (one line per kind)
    match kind:
        case "text":
            return isinstance(v, str) and v != ""
        case "field":
            return isinstance(v, str) and _FIELD.match(v) is not None
        case "fields":
            items = arr(v)
            return bool(items) and all(_kind_ok("field", x) for x in items or [])
        case "mapping":
            o = obj(v)
            return bool(o) and all(
                _kind_ok("field", k) and _kind_ok("field", x) for k, x in (o or {}).items()
            )
        case "messages":
            items = arr(v)
            return bool(items) and all(
                (m := obj(x)) is not None
                and set(m) == {"role", "from"}
                and m["role"] in ("system", "user", "assistant")
                and _kind_ok("field", m["from"])
                for x in items or []
            )
        case "cast":
            return v in ("string", "number", "integer", "boolean")
        case "count":
            n = integer(v)
            return n is not None and n >= 1
        case "columns":
            items = arr(v)
            return items is not None and all(isinstance(x, str) and x for x in items)
        case "texts":
            items = arr(v)
            return items is not None and all(isinstance(x, str) for x in items)
        case "share":
            n = number(v)
            return n is not None and 0.0 <= n <= 1.0
        case _:  # seed
            n = integer(v)
            return n is not None and 0 <= n < _I64


def spec_problems(spec: object) -> list[tuple[str, str]]:
    """(JSON pointer, plain message) for everything wrong with a transform spec."""
    s = obj(spec)
    if s is None:
        return [("", "The transform must be an object.")]
    out: list[tuple[str, str]] = []
    if s.get("schema_version") != "transform/v1":
        out.append(("/schema_version", 'Set schema_version to "transform/v1".'))
    if not isinstance(s.get("name"), str) or not s.get("name"):
        out.append(("/name", "Give the transform a name."))
    if "description" in s and not isinstance(s["description"], str):
        out.append(("/description", "Describe the transform in text."))
    if "from" in s:
        source = obj(s["from"])
        if (
            source is None
            or set(source) - {"format", "columns"}
            or source.get("format") not in ("jsonl", "json", "csv", "parquet", "hf")
            or ("columns" in source and not _kind_ok("columns", source["columns"]))
        ):
            out.append(("/from", "Name the source format and any expected columns."))
    if s.get("to") not in DATASET_SCHEMAS:
        out.append(("/to", "Set `to` to the dataset schema the result must match."))
    known = {"schema_version", "name", "description", "from", "to", "steps"}
    out += [(f"/{k}", f"Unknown field {k}.") for k in s if k not in known]
    steps = arr(s.get("steps"))
    if not steps:
        out.append(("/steps", "Add at least one step."))
    for i, raw in enumerate(steps or []):
        out += [(f"/steps/{i}{p}", m) for p, m in _step_problems(raw)]
    return out


def _step_problems(raw: object) -> list[tuple[str, str]]:
    step = obj(raw)
    op = text((step or {}).get("op"))
    if step is None or op not in _REQUIRED:
        return [("/op", f"Each step needs `op`: one of {', '.join(_REQUIRED)}.")]
    required, optional = _REQUIRED[op], _OPTIONAL.get(op, {})
    out: list[tuple[str, str]] = []
    for k, kind in (required | optional).items():
        if k not in step:
            if k in required:
                out.append((f"/{k}", f"{op} needs `{k}`."))
        elif not _kind_ok(kind, step[k]):
            out.append((f"/{k}", f"{op}: `{k}` has a value this step can't use."))
    out += [
        (f"/{k}", f"{op} has no field `{k}`.")
        for k in step
        if k not in {"op", *required, *optional}
    ]
    if op == "filter" and isinstance(step.get("expr"), str):
        try:
            Expr.parse(cast(str, step["expr"]))
        except ExprError as exc:
            out.append(("/expr", f"The filter can't be read: {exc}."))
    if op == "split" and not out:
        total = sum(number(step.get(n)) or 0.0 for n in SPLITS)
        if abs(total - 1.0) > 1e-9:
            out.append(("", "The split shares must add up to 1."))
    return out


# ---- the task ----


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    issues = [
        Issue(TASK_INPUT_INVALID, m, path=f"/inputs/spec{p}")
        for p, m in spec_problems(inputs.get("spec"))
    ]
    if PARENT_INPUT not in staged or not staged[PARENT_INPUT].is_dir():
        issues.append(Issue(TASK_INPUT_INVALID, f"No staged folder is named {PARENT_INPUT!r}."))
    return issues + against_issues(inputs, staged)


def _items(parent: DatasetAdapter, limit: int | None = None) -> Iterator[Item]:
    rows = parent.split_records()
    if limit is not None:
        rows = islice(rows, limit)
    for i, (split, rec) in enumerate(rows, 1):
        yield Item(i, rec, split)


def _preview(ctx: TaskContext, spec: Json, parent: DatasetAdapter) -> Iterator[TaskEvent]:
    before = {it.index: canonical_json(it.record) for it in _items(parent, PREVIEW_ROWS)}
    pipe = Pipeline(cast(list[Json], spec["steps"]))
    after = {it.index: it for it in pipe.run(_items(parent, PREVIEW_ROWS))}

    def encode(value: object) -> bytes:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    rows: list[bytes] = []
    report = encode({"schema_id": spec["to"], "steps": pipe.report()})
    truncated = b',"truncated":true'
    suffix = b"}\n"
    prefix = b',"rows":['
    placeholders = {
        index: encode({"index": index, "before": None, "after": None, "truncated": True})
        for index in before
    }
    reserved = len(prefix + truncated + suffix) + max(0, len(before) - 1)
    reserved += sum(map(len, placeholders.values()))
    report_truncated = len(report) + reserved > PREVIEW_MAX_BYTES
    if report_truncated:
        report = encode({"schema_id": spec["to"], "steps": []})
    remaining = PREVIEW_MAX_BYTES - len(report) - reserved
    limits = Limits()
    for index, raw in before.items():
        done = after.get(index)
        dropped = pipe.drops.get(index)
        defect = record_line(done.record, limits)[2] if done else None
        row = encode(
            {
                "index": index,
                "before": json.loads(raw),
                "after": done.record if done and not defect else None,
                "split": done.split if done else None,
                "dropped": (
                    {"step": dropped[0], "op": pipe.counts[dropped[0]].op, "reason": dropped[1]}
                    if dropped and not done
                    else {"reason": defect}
                    if defect
                    else None
                ),
            }
        )
        placeholder = placeholders[index]
        cost = len(row) - len(placeholder)
        if cost <= remaining:
            rows.append(row)
            remaining -= cost
        else:
            rows.append(placeholder)
            report_truncated = True
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    (ctx.output_dir / PREVIEW_FILE).write_bytes(
        report[:-1]
        + prefix
        + b",".join(rows)
        + b"]"
        + (truncated if report_truncated else b"")
        + suffix
    )
    yield TaskOutput(PREVIEW_FILE, "report", "json")


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    spec = cast(Json, ctx.inputs["spec"])
    parent = DatasetAdapter(ctx.staged_paths[PARENT_INPUT])
    if ctx.inputs.get("preview"):
        yield from _preview(ctx, spec, parent)
        return
    out = ctx.output_dir
    out.mkdir(parents=True, exist_ok=True)
    pipe = Pipeline(cast(list[Json], spec["steps"]))
    against = load_against(ctx.inputs, ctx.staged_paths)

    def records() -> Iterator[tuple[RawRecord, str]]:
        for n, it in enumerate(pipe.run(_items(parent)), 1):
            yield RawRecord(n, it.record, None, 0), it.split

    def work(report: Callable[[int], None]) -> Any:
        return normalize(
            records(),
            str(spec["to"]),
            out,
            Check(
                against=against,
                max_invalid_fraction=fraction(ctx.inputs),
                cancelled=ctx.cancelled,
                progress=report,
                extra={"parent_rows": len(parent)},
            ),
        )

    try:
        result = yield from with_progress(work, "transform")
    except ExprError as exc:  # checked up front; kept for specs changed in flight
        shutil.rmtree(out, ignore_errors=True)
        raise MrlError(TASK_INPUT_INVALID, f"The filter can't be read: {exc}") from exc
    if ctx.cancelled():
        return
    (out / REPORT_FILE).write_text(
        json.dumps({"steps": pipe.report()}, indent=2) + "\n", encoding="utf-8"
    )
    yield from result.outputs
    yield TaskOutput(REPORT_FILE, "report", "json")


TRANSFORM = BuiltinTask(
    name="transform",
    job_kind="DATA_PREP",
    artifact_kind="dataset",
    validate=_validate,
    run=_run,
)
