"""`mrl-conformance <adapter>`: run TC1-TC7 against an installed adapter's conformance profile."""

import argparse
import asyncio
import importlib
import sys
from importlib.metadata import entry_points

from mrl_runtime.testkit.conformance import CASES, ConformanceProfile, run_suite

GROUP = "mrl_runtime.conformance"


def load_profile(ref: str) -> ConformanceProfile:
    """A name in the mrl_runtime.conformance entry-point group, or module:function."""
    found = entry_points(group=GROUP, name=ref)
    if found:
        factory = next(iter(found)).load()
    elif ":" in ref:
        module, _, attr = ref.partition(":")
        factory = getattr(importlib.import_module(module), attr)
    else:
        names = sorted(ep.name for ep in entry_points(group=GROUP))
        raise SystemExit(
            f"mrl-conformance: no adapter {ref!r}; installed: {', '.join(names) or 'none'}"
        )
    profile: object = factory()
    if not isinstance(profile, ConformanceProfile):
        raise SystemExit(f"mrl-conformance: {ref!r} did not return a ConformanceProfile")
    return profile


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="mrl-conformance", description="Run the adapter conformance suite."
    )
    p.add_argument("adapter", help="installed adapter name (e.g. fake) or module:function")
    p.add_argument(
        "--gpu", action="store_true", help="this machine is the reference GPU profile: run TC7"
    )
    p.add_argument("--only", nargs="+", choices=CASES, default=list(CASES))
    args = p.parse_args(argv)
    profile = load_profile(args.adapter)
    results = asyncio.run(run_suite(profile, gpu=args.gpu, only=tuple(args.only)))
    for r in results:
        print(f"{r.case}  {r.status.upper():4}  {r.detail}")
    failed = [r.case for r in results if r.status == "fail"]
    print(
        f"{profile.manifest.name}: {len(results) - len(failed)}/{len(results)} cases did not fail"
        + (f"; failed: {', '.join(failed)}" if failed else "")
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
