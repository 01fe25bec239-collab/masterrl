"""MasterRL adapter for Hugging Face TRL: the `trl` engine (sft, cpt)."""

from pathlib import Path

from mrl_adapter_trl.version import __version__
from mrl_runtime.adapter import EngineManifest, load_manifest

MANIFEST_DIR = Path(__file__).parent


def manifest() -> EngineManifest:
    """The `trl` engine manifest: mrl-engine.yaml with every mrl-engine.d/ fragment merged."""
    return load_manifest(MANIFEST_DIR)


__all__ = ["MANIFEST_DIR", "__version__", "manifest"]
