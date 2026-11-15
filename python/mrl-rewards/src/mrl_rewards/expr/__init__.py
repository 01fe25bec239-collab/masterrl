"""The `when` / `require` condition language. Closed grammar, own parser, never eval.

    expr  := or
    or    := and ("or" and)*
    and   := not ("and" not)*
    not   := "not" not | cmp
    cmp   := atom (("==" | "!=" | "<" | "<=" | ">" | ">=") atom)?
    atom  := "(" expr ")" | number | string | "true" | "false" | "null" | path
    path  := root ("." segment)*        e.g. components.format.diagnostics.answer_tag_found

Semantics (total: evaluation never raises):
- A missing path is null. A dotted component id (`a.b`) is found by the longest matching key.
- `==`/`!=` compare JSON values; booleans never equal numbers.
- `<` `<=` `>` `>=` hold only between two numbers or two strings; anything else is false.
- In and/or/not, only `true` counts as true.
"""

from __future__ import annotations

import math
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass

from mrl_rewards.types import EXPR_INVALID, JSONValue, RewardError

MAX_TEXT = 2048
MAX_TOKENS = 256
MAX_DEPTH = 32

_KEYWORDS = {"and", "or", "not", "true", "false", "null"}
_OPS = ("==", "!=", "<=", ">=", "<", ">")
_NUMBER = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]{1,3})?")
_PATH = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*(\.[A-Za-z0-9_][A-Za-z0-9_-]*)*")


class ExprError(RewardError):
    def __init__(self, text: str, why: str) -> None:
        super().__init__(EXPR_INVALID, f"The condition {text[:80]!r} could not be read: {why}.")


@dataclass(frozen=True)
class Lit:
    value: JSONValue


@dataclass(frozen=True)
class Path:
    segments: tuple[str, ...]


@dataclass(frozen=True)
class Cmp:
    op: str
    left: Node
    right: Node


@dataclass(frozen=True)
class BoolOp:
    op: str  # "and" | "or"
    items: tuple[Node, ...]


@dataclass(frozen=True)
class Not:
    item: Node


type Node = Lit | Path | Cmp | BoolOp | Not


@dataclass(frozen=True)
class Expr:
    text: str
    root: Node

    def evaluate(self, context: Mapping[str, JSONValue]) -> bool:
        return _truth(_eval(self.root, context))

    def paths(self) -> list[tuple[str, ...]]:
        out: list[tuple[str, ...]] = []
        stack: list[Node] = [self.root]
        while stack:
            node = stack.pop()
            match node:
                case Path(segments):
                    out.append(segments)
                case Cmp(_, left, right):
                    stack += [left, right]
                case BoolOp(_, items):
                    stack += items
                case Not(item):
                    stack.append(item)
                case Lit():
                    pass
        return out


def _tokenize(text: str) -> list[tuple[str, str]]:
    """Tokens as (kind, text): op, paren, num, str, kw, path."""
    tokens: list[tuple[str, str]] = []
    i = 0
    while i < len(text):
        c = text[i]
        if c in " \t\r\n":
            i += 1
            continue
        if len(tokens) >= MAX_TOKENS:
            raise ExprError(text, f"it has more than {MAX_TOKENS} parts")
        if c in "()":
            tokens.append(("paren", c))
            i += 1
            continue
        op = next((o for o in _OPS if text.startswith(o, i)), None)
        if op:
            tokens.append(("op", op))
            i += len(op)
            continue
        if c in "\"'":
            j, chars = i + 1, list[str]()
            while j < len(text) and text[j] != c:
                if text[j] == "\\" and j + 1 < len(text) and text[j + 1] in "\\\"'":
                    j += 1
                chars.append(text[j])
                j += 1
            if j >= len(text):
                raise ExprError(text, "a quoted text is not closed")
            tokens.append(("str", "".join(chars)))
            i = j + 1
            continue
        if m := _NUMBER.match(text, i):
            tokens.append(("num", m.group()))
            i = m.end()
            continue
        if m := _PATH.match(text, i):
            word = m.group()
            tokens.append(("kw" if word in _KEYWORDS else "path", word))
            i = m.end()
            continue
        raise ExprError(text, f"unexpected character {c!r} at position {i + 1}")
    return tokens


class _Parser:
    def __init__(self, text: str, roots: Collection[str]) -> None:
        self.text = text
        self.roots = roots
        self.tokens = _tokenize(text)
        self.pos = 0

    def peek(self) -> tuple[str, str] | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def take(self) -> tuple[str, str]:
        tok = self.peek()
        if tok is None:
            raise ExprError(self.text, "it ends too early")
        self.pos += 1
        return tok

    def parse(self) -> Node:
        if not self.tokens:
            raise ExprError(self.text, "it is empty")
        node = self.bool_op("or", 0)
        if (tok := self.peek()) is not None:
            raise ExprError(self.text, f"unexpected {tok[1]!r}")
        return node

    def bool_op(self, op: str, depth: int) -> Node:
        if depth > MAX_DEPTH:
            raise ExprError(self.text, f"it nests deeper than {MAX_DEPTH} levels")

        def sub(d: int) -> Node:
            return self.bool_op("and", d) if op == "or" else self.negation(d)

        items = [sub(depth + 1)]
        while self.peek() == ("kw", op):
            self.pos += 1
            items.append(sub(depth + 1))
        return items[0] if len(items) == 1 else BoolOp(op, tuple(items))

    def negation(self, depth: int) -> Node:
        if depth > MAX_DEPTH:
            raise ExprError(self.text, f"it nests deeper than {MAX_DEPTH} levels")
        if self.peek() == ("kw", "not"):
            self.pos += 1
            return Not(self.negation(depth + 1))
        left = self.atom(depth)
        tok = self.peek()
        if tok is not None and tok[0] == "op":
            self.pos += 1
            return Cmp(tok[1], left, self.atom(depth))
        return left

    def atom(self, depth: int) -> Node:
        kind, text = self.take()
        if (kind, text) == ("paren", "("):
            node = self.bool_op("or", depth + 1)
            if self.take() != ("paren", ")"):
                raise ExprError(self.text, "a bracket is not closed")
            return node
        if kind == "num":
            value = float(text) if any(ch in text for ch in ".eE") else int(text)
            if not math.isfinite(value):
                raise ExprError(self.text, f"the number {text} is too large")
            return Lit(value)
        if kind == "str":
            return Lit(text)
        if kind == "kw" and text in ("true", "false", "null"):
            return Lit({"true": True, "false": False, "null": None}[text])
        if kind == "path":
            segments = tuple(text.split("."))
            if segments[0] not in self.roots:
                allowed = ", ".join(sorted(self.roots))
                raise ExprError(self.text, f"{segments[0]!r} is not a known name (use {allowed})")
            return Path(segments)
        raise ExprError(self.text, f"unexpected {text!r}")


def parse(text: str, roots: Collection[str] = ("sample", "components")) -> Expr:
    """Parse a condition. Raises only ExprError."""
    if len(text) > MAX_TEXT:
        raise ExprError(text, f"it is longer than {MAX_TEXT} characters")
    return Expr(text, _Parser(text, roots).parse())


def lookup(root: JSONValue, segments: tuple[str, ...]) -> JSONValue:
    cur, i = root, 0
    while i < len(segments):
        if isinstance(cur, list):
            seg = segments[i]
            if not seg.isdigit() or int(seg) >= len(cur):
                return None
            cur, i = cur[int(seg)], i + 1
            continue
        if not isinstance(cur, dict):
            return None
        for j in range(len(segments), i, -1):
            key = ".".join(segments[i:j])
            if key in cur:
                cur, i = cur[key], j
                break
        else:
            return None
    return cur


def _kind(v: JSONValue) -> str:
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int | float):
        return "num"
    return type(v).__name__


def _equal(a: JSONValue, b: JSONValue) -> bool:
    return _kind(a) == _kind(b) and a == b


def _order(op: str, a: JSONValue, b: JSONValue) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return False
    if isinstance(a, int | float) and isinstance(b, int | float):
        x, y = float(a), float(b)
    elif isinstance(a, str) and isinstance(b, str):
        return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[op]
    else:
        return False
    return {"<": x < y, "<=": x <= y, ">": x > y, ">=": x >= y}[op]


def _truth(v: JSONValue) -> bool:
    return v is True


def _eval(node: Node, ctx: Mapping[str, JSONValue]) -> JSONValue:  # noqa: PLR0911
    match node:
        case Lit(value):
            return value
        case Path(segments):
            return lookup(ctx.get(segments[0]), segments[1:])
        case Cmp(op, left, right):
            a, b = _eval(left, ctx), _eval(right, ctx)
            if op == "==":
                return _equal(a, b)
            if op == "!=":
                return not _equal(a, b)
            return _order(op, a, b)
        case BoolOp("and", items):
            return all(_truth(_eval(i, ctx)) for i in items)
        case BoolOp(_, items):
            return any(_truth(_eval(i, ctx)) for i in items)
        case Not(item):
            return not _truth(_eval(item, ctx))


__all__ = ["MAX_DEPTH", "MAX_TEXT", "MAX_TOKENS", "Expr", "ExprError", "lookup", "parse"]
