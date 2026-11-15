"""The condition language: goldens, limits and fuzzing (only ExprError, never anything else)."""

from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mrl_rewards.expr import MAX_TEXT, ExprError, parse

CTX: dict[str, Any] = {
    "sample": {"reference": {"lang": "en", "n": 3, "tags": ["a", "b"]}, "finish_reason": "stop"},
    "components": {
        "format": {"value": 1.0, "diagnostics": {"answer_tag_found": True}},
        "my.comp": {"value": 0.25, "diagnostics": {}},
        "x-y": {"value": None},
    },
}


@pytest.mark.parametrize(
    ("text", "want"),
    [
        ("components.format.diagnostics.answer_tag_found == true", True),
        ("sample.reference.lang != null", True),
        ("sample.reference.missing != null", False),
        ("sample.reference.missing == null", True),
        ("sample.reference.n >= 3 and sample.reference.n < 4", True),
        ("sample.reference.n > 3 or sample.finish_reason == 'stop'", True),
        ('not (sample.finish_reason == "length")', True),
        ("components.my.comp.value == 0.25", True),  # dotted component id
        ("components.x-y.value == null", True),  # hyphenated id
        ("sample.reference.tags.1 == 'b'", True),
        ("sample.reference.tags.9 == null", True),
        ("sample.reference.n == true", False),  # bools never equal numbers
        ("sample.reference.lang < 3", False),  # mixed types order as false
        ("sample.reference.lang < 'fr'", True),
        ("components.format.value == 1", True),
        ("sample.reference", False),  # only true counts as true
        ("components.format.diagnostics.answer_tag_found", True),
        ("-0.5 < 0", True),
        ("1e3 == 1000", True),
        ("not not true", True),
    ],
)
def test_golden(text: str, want: bool) -> None:
    assert parse(text).evaluate(CTX) is want


@pytest.mark.parametrize(
    "text",
    [
        "",
        "   ",
        "sample.a ==",
        "(sample.a == 1",
        "sample.a == 1)",
        "sample.a = 1",
        "sample.a + 1",
        "__import__('os')",
        "open.x == 1",
        "sample.a == 'unclosed",
        "1e9999 == 1",
        "sample.a == 1 and",
        "sample..a == 1",
        "x" * (MAX_TEXT + 1),
        "(" * 40 + "true" + ")" * 40,
        "not " * 40 + "true",
        " or ".join(["true"] * 200),
    ],
)
def test_rejected(text: str) -> None:
    with pytest.raises(ExprError) as err:
        parse(text)
    assert err.value.code == "MRL-RWD-141"


def test_roots_are_restricted() -> None:
    parse("sample.a == 1", roots=("sample",))
    with pytest.raises(ExprError):
        parse("components.a.value == 1", roots=("sample",))


def test_paths() -> None:
    paths = parse("components.a.value > 0 and not sample.b == null").paths()
    assert sorted(paths) == [("components", "a", "value"), ("sample", "b")]


@settings(max_examples=2000, deadline=None)
@given(st.text(max_size=300))
def test_fuzz_any_text_raises_only_expr_error(text: str) -> None:
    try:
        expr = parse(text)
    except ExprError:
        return
    assert isinstance(expr.evaluate(CTX), bool)


TOKENS = st.sampled_from(
    [
        "sample.reference.n",
        "components.format.value",
        "sample.x",
        "1",
        "-2.5",
        "'a'",
        "true",
        "false",
        "null",
        "==",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "and",
        "or",
        "not",
        "(",
        ")",
    ]
)


@settings(max_examples=2000, deadline=None)
@given(st.lists(TOKENS, max_size=40))
def test_fuzz_token_soup(tokens: list[str]) -> None:
    try:
        expr = parse(" ".join(tokens))
    except ExprError:
        return
    assert isinstance(expr.evaluate(CTX), bool)


json_values = st.recursive(
    st.none() | st.booleans() | st.floats(allow_nan=True) | st.integers() | st.text(max_size=5),
    lambda inner: (
        st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=5), inner, max_size=3)
    ),
    max_leaves=10,
)


@settings(max_examples=500, deadline=None)
@given(json_values)
def test_evaluation_never_raises_on_any_context(value: Any) -> None:
    ctx = {"sample": {"reference": value}, "components": value}
    for text in (
        "sample.reference.a.b > 1",
        "components.a.value == null",
        "sample.reference.0 != 'x'",
    ):
        assert isinstance(parse(text).evaluate(ctx), bool)
