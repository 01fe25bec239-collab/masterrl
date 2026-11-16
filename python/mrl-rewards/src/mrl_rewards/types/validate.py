"""Typed JSON Schema helpers (the one place that touches jsonschema's loosely typed API)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import cast

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry

from mrl_rewards.types import JSONValue


def validator(
    schema: Mapping[str, JSONValue], *, local_refs_only: bool = False
) -> Draft202012Validator:
    """A 2020-12 validator. `local_refs_only` gives it an empty registry: no remote $ref."""
    if local_refs_only:
        return Draft202012Validator(dict(schema), registry=Registry())
    return Draft202012Validator(dict(schema))


def first_error(v: Draft202012Validator, instance: JSONValue) -> tuple[str, str] | None:
    """(json-pointer path, message) of the first error by path, or None when valid."""
    errors = cast(
        "Iterable[ValidationError]",
        v.iter_errors(instance),  # pyright: ignore[reportUnknownMemberType]
    )
    found = sorted(errors, key=lambda e: [str(p) for p in e.absolute_path])
    if not found:
        return None
    e = found[0]
    return "/" + "/".join(str(p) for p in e.absolute_path), e.message


__all__ = ["first_error", "validator"]
