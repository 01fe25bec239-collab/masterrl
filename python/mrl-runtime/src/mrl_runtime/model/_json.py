"""Typed views of parsed JSON values (json.loads returns Any)."""

from typing import Any, cast

type Json = dict[str, Any]


def obj(value: object) -> Json | None:
    """The value as a JSON object, or None."""
    return cast(Json, value) if isinstance(value, dict) else None


def arr(value: object) -> list[Any] | None:
    """The value as a JSON array, or None."""
    return cast(list[Any], value) if isinstance(value, list) else None


def integer(value: object) -> int | None:
    """The value as an int (bools excluded), or None."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def number(value: object) -> float | None:
    """The value as a float (bools excluded), or None."""
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return None


def text(value: object) -> str | None:
    return value if isinstance(value, str) else None
