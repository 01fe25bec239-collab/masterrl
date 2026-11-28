"""The reward bundle as the server ships it, and loading it inside the reward sandbox.

**Server form.** A reward version is staged for a job as:

    job spec `reward` (or the REWARD_PREVIEW task's inputs):
      {"version_id": "rwdv_…", "content_sha256": …, "exec_sha256": …, "sandbox_tier": …,
       "policy": {RewardPolicy v1}}
    staged files `reward/code/<file>`: the version's code bundle, when it has one
      (`components.yaml` plus top-level `<module>.py` files; nothing else)

`assemble()` turns that into P14's bundle directory (`policy.json` + `code/`), read-only.

**Loading.** `load()` is P14's bundle loader with two differences: the Tier-2 `code.*`
components are installed too (`mrl_rewards.code.full_registry`), and custom code loads only
when `sandbox_proof()` holds: Linux, the agent's sandbox marker, a seccomp filter on this
process (`Seccomp: 2`), `no_new_privs` and no network interface besides loopback. Anywhere
else custom code is refused with MRL-RWD-295 (macOS and Windows always refuse it).
"""

from __future__ import annotations

import errno
import importlib.util
import json
import os
import shutil
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml

from mrl_rewards.code import full_registry
from mrl_rewards.exec.bundle import LoadedBundle, bundle_hash, read_policy
from mrl_rewards.exec.main import non_loopback_interfaces
from mrl_rewards.policy.model import load_policy
from mrl_rewards.registry import Registry
from mrl_rewards.types import (
    BUNDLE_INVALID,
    ComponentSpec,
    JSONValue,
    RewardError,
    RewardProvider,
)

USER_CODE_REFUSED = "MRL-RWD-295"
USER_CODE_ENV = "MRL_USER_CODE_ALLOWED"
PROFILE_ENV = "MRL_SANDBOX_PROFILE"
SANDBOX_PROFILES = ("reward", "code_exec")
MAX_CODE_FILES = 512
MAX_CODE_BYTES = 16 << 20


@dataclass(frozen=True)
class Proof:
    """Whether this process runs in the reward sandbox, and why not."""

    ok: bool
    why: str = ""


def sandbox_proof(  # noqa: PLR0911 (one answer per marker)
    status_path: Path = Path("/proc/self/status"),
) -> Proof:
    """Checks that this process is inside the agent's reward sandbox."""
    if not sys.platform.startswith("linux"):
        return Proof(False, f"custom reward code runs only on Linux, not {sys.platform}")
    if os.environ.get(USER_CODE_ENV) != "1" or os.environ.get(PROFILE_ENV) not in SANDBOX_PROFILES:
        return Proof(False, "the agent didn't start this process in the reward sandbox")
    try:
        status = status_path.read_text(encoding="utf-8")
    except OSError as e:
        return Proof(False, f"{status_path} can't be read ({e})")
    fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    if fields.get("Seccomp", "").strip() != "2":
        return Proof(False, "no seccomp filter is active on this process")
    if fields.get("NoNewPrivs", "").strip() != "1":
        return Proof(False, "no_new_privs is not set on this process")
    if extra := network_interfaces():
        return Proof(False, f"network interfaces are up ({', '.join(extra[:4])})")
    return Proof(True)


def network_interfaces() -> list[str]:
    """Network interfaces other than loopback this process could use. Inside the agent's
    sandbox `/sys` isn't mounted and the seccomp filter refuses every socket but AF_UNIX
    (EAFNOSUPPORT), which itself proves there is no network to use; elsewhere it is P14's
    check (`/sys/class/net` flags, else the interface list)."""
    if not Path("/sys/class/net").is_dir():
        try:
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM).close()
        except OSError as e:
            if e.errno == errno.EAFNOSUPPORT:
                return []
    return non_loopback_interfaces()


def symlinked_dirs(path: Path) -> set[Path]:
    """Every symlinked folder on the way from `path` to the file it finally names (uv's
    `cpython-3.12-…` links to `cpython-3.12.13-…`). A sandbox binds them too, or the link
    dangles inside."""
    out: set[Path] = set()
    cur = path
    for _ in range(40):
        out |= {a for a in cur.parents if a.is_symlink() and a.is_dir()}
        if not cur.is_symlink():
            break
        cur = cur.parent / cur.readlink()
    return out


def python_paths() -> list[Path]:
    """The folders this Python needs read-only in a sandbox: its environment, its install,
    the symlinked folders between them and every import path."""
    paths = {
        Path(sys.prefix),
        Path(sys.base_prefix).resolve(),
        Path(sys.executable).resolve().parent,
    }
    paths |= symlinked_dirs(Path(sys.executable))
    paths |= {Path(p).resolve() for p in sys.path if p and Path(p).is_dir()}
    return sorted(paths)


def _safe_name(name: str) -> bool:
    stem = name.removesuffix(".py")
    return name == "components.yaml" or (
        name.endswith(".py") and stem.isidentifier() and stem.isascii()
    )


def assemble(policy: JSONValue, code_dir: Path | None, dest: Path) -> Path:
    """Writes P14's bundle layout under `dest` from the server form; returns `dest`.

    Only `components.yaml` and top-level `<name>.py` files are taken from `code_dir`;
    links, folders and anything else are refused (MRL-RWD-147)."""
    if not isinstance(policy, dict):
        raise RewardError(BUNDLE_INVALID, "The staged reward has no policy object.")
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "policy.json").write_text(json.dumps(policy, sort_keys=True), encoding="utf-8")
    if code_dir is None or not code_dir.exists():
        return dest
    files = sorted(code_dir.iterdir())
    if len(files) > MAX_CODE_FILES:
        raise RewardError(BUNDLE_INVALID, f"The code bundle has more than {MAX_CODE_FILES} files.")
    out = dest / "code"
    out.mkdir(exist_ok=True)
    total = 0
    for f in files:
        if f.is_symlink() or not f.is_file() or not _safe_name(f.name):
            raise RewardError(
                BUNDLE_INVALID,
                f"{f.name!r} can't be in a code bundle: use components.yaml and top-level "
                "Python files.",
            )
        total += f.stat().st_size
        if total > MAX_CODE_BYTES:
            raise RewardError(BUNDLE_INVALID, f"The code bundle is over {MAX_CODE_BYTES} bytes.")
        shutil.copyfile(f, out / f.name, follow_symlinks=False)
    for p in [*out.iterdir(), out, dest / "policy.json", dest]:
        p.chmod(0o555 if p.is_dir() else 0o444)
    return dest


def _looks_like_provider(cls: type) -> bool:
    has_methods = all(callable(getattr(cls, m, None)) for m in ("setup", "score", "teardown"))
    return has_methods and isinstance(getattr(cls, "spec", None), ComponentSpec)


def _load_user_components(code: Path, registry: Registry, tag: str) -> None:
    index = code / "components.yaml"
    if not index.is_file():
        raise RewardError(BUNDLE_INVALID, "code/ needs a components.yaml listing its components.")
    try:
        doc = cast("JSONValue", yaml.safe_load(index.read_text(encoding="utf-8")))
    except yaml.YAMLError as e:
        raise RewardError(BUNDLE_INVALID, f"code/components.yaml can't be read: {e}") from e
    raw = cast("dict[str, JSONValue]", doc).get("components") if isinstance(doc, dict) else None
    if not isinstance(raw, list) or not raw:
        raise RewardError(BUNDLE_INVALID, "code/components.yaml needs a `components:` list.")
    for item in raw:
        e = cast("dict[str, JSONValue]", item) if isinstance(item, dict) else {}
        module, cls_name, ns = e.get("module"), e.get("class"), e.get("namespace", "org")
        if not (isinstance(module, str) and module.isidentifier() and isinstance(cls_name, str)):
            raise RewardError(BUNDLE_INVALID, "Each component needs `module` and `class`.")
        if ns not in ("org", "hub"):
            raise RewardError(BUNDLE_INVALID, f"Component {cls_name!r}: namespace is org or hub.")
        path = code / f"{module}.py"
        if not path.is_file():
            raise RewardError(BUNDLE_INVALID, f"code/{module}.py is missing.")
        name = f"mrl_bundle_{tag}_{module}"
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RewardError(BUNDLE_INVALID, f"code/{module}.py can't be loaded.")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception as ex:  # user code: report any import-time failure plainly
            raise RewardError(
                BUNDLE_INVALID, f"code/{module}.py failed to load: {type(ex).__name__}: {ex}"
            ) from ex
        cls = cast("object", getattr(mod, cls_name, None))
        if not isinstance(cls, type) or not _looks_like_provider(cls):
            raise RewardError(BUNDLE_INVALID, f"{module}.{cls_name} is not a reward component.")
        registry.register(cast("type[RewardProvider]", cls), namespace=str(ns))


def load(root: Path, *, user_code: bool, proof: Proof | None = None) -> LoadedBundle:
    """Loads a bundle with every shipped component. Custom code needs `user_code` and a
    passing sandbox proof (checked here unless the caller passes one)."""
    root = root.resolve()
    if not root.is_dir():
        raise RewardError(BUNDLE_INVALID, f"There is no bundle directory at {root}.")
    document = read_policy(root)
    policy = load_policy(document)
    registry = full_registry()
    content = bundle_hash(root, document, registry.digest())
    code = root / "code"
    has_code = code.is_dir() and any(code.iterdir())
    if has_code:
        if not user_code:
            raise RewardError(
                USER_CODE_REFUSED, "This bundle has custom code, but custom code wasn't enabled."
            )
        p = proof if proof is not None else sandbox_proof()
        if not p.ok:
            raise RewardError(
                USER_CODE_REFUSED,
                f"Custom reward code runs only inside the reward sandbox: {p.why}.",
            )
        _load_user_components(code, registry, content[:12])
    for c in policy.components:
        registry.resolve(c.use)
    return LoadedBundle(root, policy, registry, content, has_code)


__all__ = [
    "USER_CODE_REFUSED",
    "Proof",
    "assemble",
    "load",
    "network_interfaces",
    "python_paths",
    "sandbox_proof",
    "symlinked_dirs",
]
