"""Reward bundles: a read-only directory with a policy and, optionally, user code.

    <bundle>/
      policy.yaml | policy.json      RewardPolicy v1 (exactly one of the two)
      code/                          optional user components (pure Python)
        components.yaml              components: [{module: my_reward, class: MyReward,
                                                   namespace: org}]   (namespace: org | hub)
        my_reward.py ...

The bundle hash covers the policy, the built-in set, every file under code/ and the SDK
contract, so Describe reports what was actually loaded.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml

from mrl_rewards.policy.model import Policy, load_policy
from mrl_rewards.registry import Registry, builtin_registry
from mrl_rewards.types import (
    BUNDLE_INVALID,
    POLICY_INVALID,
    SDK_CONTRACT,
    USER_CODE_REFUSED,
    ComponentSpec,
    JSONValue,
    RewardError,
    RewardProvider,
)

MAX_POLICY_BYTES = 1 << 20
MAX_CODE_FILES = 512
MAX_CODE_BYTES = 16 << 20
HASH_FORMAT = "mrl-reward-bundle/1"


@dataclass(frozen=True)
class LoadedBundle:
    root: Path
    policy: Policy
    registry: Registry
    content_sha256: str
    user_code: bool


def canonical_json(value: JSONValue) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _bad(why: str) -> RewardError:
    return RewardError(BUNDLE_INVALID, why)


def read_policy(root: Path) -> JSONValue:
    found = [p for p in (root / "policy.yaml", root / "policy.json") if p.is_file()]
    if len(found) != 1:
        raise _bad(f"The bundle at {root} needs exactly one policy.yaml or policy.json.")
    path = found[0]
    if path.stat().st_size > MAX_POLICY_BYTES:
        raise _bad(f"{path.name} is larger than {MAX_POLICY_BYTES} bytes.")
    text = path.read_text(encoding="utf-8")
    try:
        if path.suffix == ".json":
            return cast("JSONValue", json.loads(text))
        return cast("JSONValue", yaml.safe_load(text))
    except (ValueError, yaml.YAMLError) as e:
        raise RewardError(POLICY_INVALID, f"{path.name} could not be read: {e}") from e


def _code_files(code: Path) -> list[Path]:
    files = sorted(p for p in code.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    if len(files) > MAX_CODE_FILES:
        raise _bad(f"code/ has more than {MAX_CODE_FILES} files.")
    if sum(p.stat().st_size for p in files) > MAX_CODE_BYTES:
        raise _bad(f"code/ is larger than {MAX_CODE_BYTES} bytes.")
    return files


def bundle_hash(root: Path, document: JSONValue, registry_digest: str) -> str:
    h = hashlib.sha256()
    h.update(f"{HASH_FORMAT}\n{SDK_CONTRACT}\n".encode())
    h.update(b"policy " + hashlib.sha256(canonical_json(document)).hexdigest().encode() + b"\n")
    h.update(f"builtins {registry_digest}\n".encode())
    code = root / "code"
    if code.is_dir():
        for f in _code_files(code):
            rel = f.relative_to(code).as_posix()
            h.update(f"file {rel} {hashlib.sha256(f.read_bytes()).hexdigest()}\n".encode())
    return h.hexdigest()


def _load_user_components(code: Path, registry: Registry, tag: str) -> None:
    index = code / "components.yaml"
    if not index.is_file():
        raise _bad("code/ needs a components.yaml that lists the components.")
    try:
        doc = cast("JSONValue", yaml.safe_load(index.read_text(encoding="utf-8")))
    except yaml.YAMLError as e:
        raise _bad(f"code/components.yaml could not be read: {e}") from e
    entries = cast("dict[str, JSONValue]", doc).get("components") if isinstance(doc, dict) else None
    if not isinstance(entries, list) or not entries:
        raise _bad("code/components.yaml needs a non-empty `components:` list.")
    for raw in entries:
        e = cast("dict[str, JSONValue]", raw) if isinstance(raw, dict) else {}
        module, cls_name = e.get("module"), e.get("class")
        namespace = e.get("namespace", "org")
        if not (isinstance(module, str) and module.isidentifier() and isinstance(cls_name, str)):
            raise _bad("Each component needs `module` (a Python name) and `class`.")
        if namespace not in ("org", "hub"):
            raise _bad(f"Component {cls_name!r}: namespace must be org or hub.")
        path = code / f"{module}.py"
        if not path.is_file():
            raise _bad(f"code/{module}.py is missing.")
        name = f"mrl_bundle_{tag}_{module}"
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise _bad(f"code/{module}.py could not be loaded.")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception as ex:  # user code: report any import-time failure plainly
            raise _bad(f"code/{module}.py failed to load: {type(ex).__name__}: {ex}") from ex
        cls = cast("object", getattr(mod, cls_name, None))
        if not isinstance(cls, type) or not _looks_like_provider(cls):
            raise _bad(
                f"{module}.{cls_name} is not a reward component (it needs spec, setup, score, "
                "teardown)."
            )
        registry.register(cast("type[RewardProvider]", cls), namespace=str(namespace))


def _looks_like_provider(cls: type) -> bool:
    has_methods = all(callable(getattr(cls, m, None)) for m in ("setup", "score", "teardown"))
    return has_methods and isinstance(getattr(cls, "spec", None), ComponentSpec)


def load_bundle(root: Path, *, allow_user_code: bool) -> LoadedBundle:
    """Load a bundle. User code loads only when `allow_user_code` (the testkit, until P29's
    sandbox launcher runs it)."""
    root = root.resolve()
    if not root.is_dir():
        raise _bad(f"There is no bundle directory at {root}.")
    document = read_policy(root)
    policy = load_policy(document)
    registry = builtin_registry()
    builtins_digest = registry.digest()
    code = root / "code"
    user_code = code.is_dir() and any(code.iterdir())
    content = bundle_hash(root, document, builtins_digest)
    if user_code:
        if not allow_user_code:
            raise RewardError(
                USER_CODE_REFUSED,
                "This bundle has custom code (code/). Custom code runs only inside the reward "
                "sandbox, which this setup doesn't have yet. Use built-in components only.",
            )
        _load_user_components(code, registry, content[:12])
    for c in policy.components:
        registry.resolve(c.use)  # unknown refs fail at load, not at first score
    return LoadedBundle(root, policy, registry, content, user_code)


__all__ = [
    "HASH_FORMAT",
    "LoadedBundle",
    "bundle_hash",
    "canonical_json",
    "load_bundle",
    "read_policy",
]
