"""Component registry: `<namespace>/<name>@<major>` -> component class.

Namespaces follow the policy schema: builtin (shipped), org (tenant code), hub (imported),
judge (managed judges, later parts).
"""

from __future__ import annotations

import hashlib
import inspect
import re
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator, SchemaError

from mrl_rewards.builtin import BUILTINS
from mrl_rewards.types import (
    BUNDLE_INVALID,
    COMPONENT_UNKNOWN,
    ComponentSpec,
    JSONValue,
    RewardError,
    RewardProvider,
)

_REF = re.compile(r"^(builtin|hub|org|judge)/([a-z0-9][a-z0-9._-]*)@([0-9]+)$")
_SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


@dataclass(frozen=True)
class ComponentRef:
    namespace: str
    name: str
    major: int

    def __str__(self) -> str:
        return f"{self.namespace}/{self.name}@{self.major}"


def parse_ref(ref: str) -> ComponentRef:
    m = _REF.match(ref)
    if not m:
        raise RewardError(
            COMPONENT_UNKNOWN,
            f"{ref!r} is not a component reference. Use <namespace>/<name>@<major>, "
            "like builtin/math.verify@1.",
        )
    return ComponentRef(m.group(1), m.group(2), int(m.group(3)))


def check_spec(spec: ComponentSpec) -> None:
    """Reject a component whose declared spec can't be used."""
    lo, hi = spec.value_range
    problems = [
        (not _SEMVER.match(spec.version), "version must look like 1.0.0"),
        (not lo < hi, "value_range low end must be below its high end"),
        (spec.tier not in (1, 2), "tier must be 1 or 2"),
    ]
    try:
        Draft202012Validator.check_schema(dict(spec.param_schema))
    except SchemaError as e:
        problems.append((True, f"param_schema is not a valid JSON Schema ({e.message[:120]})"))
    for bad, why in problems:
        if bad:
            raise RewardError(BUNDLE_INVALID, f"Component {spec.name!r}: {why}.")


def code_digest(cls: type[RewardProvider]) -> str:
    """sha256 of the source file that defines the component."""
    source = inspect.getsourcefile(cls)
    return hashlib.sha256(Path(source).read_bytes() if source else b"").hexdigest()


class Registry:
    def __init__(self) -> None:
        self._classes: dict[ComponentRef, type[RewardProvider]] = {}

    def register(self, cls: type[RewardProvider], namespace: str = "builtin") -> ComponentRef:
        spec = cls.spec
        check_spec(spec)
        ref = parse_ref(f"{namespace}/{spec.name}@{spec.major}")
        if ref in self._classes:
            raise RewardError(BUNDLE_INVALID, f"Component {ref} is registered twice.")
        self._classes[ref] = cls
        return ref

    def resolve(self, ref: str) -> type[RewardProvider]:
        key = parse_ref(ref)
        cls = self._classes.get(key)
        if cls is None:
            known = sorted(
                str(r) for r in self._classes if (r.namespace, r.name) == (key.namespace, key.name)
            )
            hint = f" Available: {', '.join(known)}." if known else ""
            raise RewardError(COMPONENT_UNKNOWN, f"No component {ref} is installed.{hint}")
        return cls

    def create(self, ref: str) -> RewardProvider:
        return self.resolve(ref)()

    def refs(self) -> list[str]:
        return sorted(str(r) for r in self._classes)

    def index(self) -> list[dict[str, JSONValue]]:
        """One row per component: what the server seeds `reward_components` from."""
        rows: list[dict[str, JSONValue]] = []
        for ref in sorted(self._classes, key=str):
            cls = self._classes[ref]
            s = cls.spec
            rows.append(
                {
                    "ref": str(ref),
                    "version": s.version,
                    "shape": str(s.shape),
                    "tier": s.tier,
                    "deterministic": s.deterministic,
                    "stateful": s.stateful,
                    "beta": s.beta,
                    "value_range": [s.value_range[0], s.value_range[1]],
                    "needs": list(s.needs),
                    "description": s.description,
                    "param_schema": dict(s.param_schema),
                    "code_sha256": code_digest(cls),
                }
            )
        return rows

    def digest(self) -> str:
        """sha256 over every registered ref and its code digest (part of the bundle hash)."""
        h = hashlib.sha256()
        for row in self.index():
            h.update(f"{row['ref']}={row['code_sha256']}\n".encode())
        return h.hexdigest()


def builtin_registry() -> Registry:
    reg = Registry()
    for cls in BUILTINS:
        reg.register(cls)
    return reg


__all__ = ["ComponentRef", "Registry", "builtin_registry", "check_spec", "code_digest", "parse_ref"]
