"""Hardware profiles and headroom rules from the P05 guidance catalog (Doc 24 §3, §7)."""

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any, Literal

from mrl_runtime.model._json import Json, number, obj, text
from mrl_runtime.model.errors import BAD_VALUE, MrlError

type DeviceKind = Literal["gpu", "unified", "cpu"]
KINDS: dict[str, DeviceKind] = {"gpu": "gpu", "unified": "unified", "system": "cpu"}


@dataclass(frozen=True)
class Headroom:
    """Memory kept free (Doc 24 §3). Percentages are of total memory."""

    gpu_memory_free_pct: float = 0.0
    system_ram_free_min_gib: float = 0.0
    system_ram_free_pct: float = 0.0
    unified_memory_reserved_pct: float = 0.0

    def ram_usable(self, total_gib: float) -> float:
        return total_gib - max(
            self.system_ram_free_min_gib, total_gib * self.system_ram_free_pct / 100
        )


@dataclass(frozen=True)
class DeviceSpec:
    """One accelerator (or the CPU's RAM). Catalog sizes in GB are read as GiB."""

    kind: DeviceKind
    memory_gib: float | None
    family: str
    headroom: Headroom
    profile_id: str | None = None

    def usable_gib(self) -> float | None:
        """Memory a run may use after the headroom rule."""
        if self.memory_gib is None:
            return None
        if self.kind == "gpu":
            return self.memory_gib * (1 - self.headroom.gpu_memory_free_pct / 100)
        if self.kind == "unified":
            return self.memory_gib * (1 - self.headroom.unified_memory_reserved_pct / 100)
        return self.headroom.ram_usable(self.memory_gib)


@dataclass(frozen=True)
class Topology:
    """The machine a run would use: `devices` identical accelerators per node, all data-parallel."""

    device: DeviceSpec
    devices: int = 1
    nodes: int = 1
    system_ram_gib: float | None = None
    scratch_free_gib: float | None = None

    @property
    def data_parallel(self) -> int:
        return self.devices * self.nodes


def find_catalog(start: Path | None = None) -> Path:
    """`guidance/build/catalog.json` of this checkout, found by walking up from this file."""
    here = (start or Path(__file__)).resolve()
    for parent in here.parents:
        candidate = parent / "guidance" / "build" / "catalog.json"
        if candidate.is_file():
            return candidate
    raise MrlError(
        BAD_VALUE,
        "The app could not find its hardware profiles.",
        "pass catalog_path pointing at guidance/build/catalog.json",
    )


@cache
def _load(path: Path) -> Json:
    value: Any = json.loads(path.read_text(encoding="utf-8"))
    return obj(value) or {}


def hardware_catalog(catalog_path: Path | None = None) -> Json:
    """The catalog's `hardware` section: {"families": [...], "profiles": [...]}."""
    return obj(_load((catalog_path or find_catalog()).resolve()).get("hardware")) or {}


def _headroom(family: Json) -> Headroom:
    h = obj(family.get("headroom")) or {}
    return Headroom(
        number(h.get("gpu_memory_free_pct")) or 0.0,
        number(h.get("system_ram_free_min_gb")) or 0.0,
        number(h.get("system_ram_free_pct")) or 0.0,
        number(h.get("unified_memory_reserved_pct")) or 0.0,
    )


def device_from_profile(
    profile_id: str, memory_gib: float | None = None, catalog_path: Path | None = None
) -> DeviceSpec:
    """A DeviceSpec for a catalog hardware profile.

    Profiles that cover a range (e.g. 32-80 GB) or set only a minimum need `memory_gib`
    inside that range. Raises MRL-VAL-003 for an unknown profile or an out-of-range size.
    """
    hw = hardware_catalog(catalog_path)
    profiles = {text(p.get("id")): p for p in (obj(x) or {} for x in hw.get("profiles", []))}
    families = {text(f.get("id")): f for f in (obj(x) or {} for x in hw.get("families", []))}
    profile = profiles.get(profile_id)
    if profile is None:
        raise MrlError(BAD_VALUE, "The app doesn't know this hardware profile.", profile_id)
    family_id = text(profile.get("family")) or ""
    size = obj(profile.get("memory_gb")) or {}
    low, high = number(size.get("min")), number(size.get("max"))
    if memory_gib is None and low is not None and low == high:
        memory_gib = low
    if memory_gib is not None and (
        (low is not None and memory_gib < low) or (high is not None and memory_gib > high)
    ):
        raise MrlError(
            BAD_VALUE,
            "The memory size is outside this hardware profile's range.",
            f"{profile_id}: {memory_gib} GB",
        )
    kind = KINDS.get(text(profile.get("memory_kind")) or "", "gpu")
    return DeviceSpec(
        kind, memory_gib, family_id, _headroom(families.get(family_id) or {}), profile_id
    )
