"""`mrl-reward-exec` command: load a bundle read-only and serve RewardService.

Built-in components only: a bundle with user code is refused (MRL-RWD-146) until P29's sandbox
launcher runs this process; see mrl_rewards.exec.testkit for the test-only user-code path.
The attempt nonce arrives on an inherited file descriptor, never in argv or the environment.
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import sys
import threading
from pathlib import Path

from mrl_rewards.exec.bundle import load_bundle
from mrl_rewards.exec.server import STARTUP_FAILED, Audit, ExecLimits, RewardExecService, serve
from mrl_rewards.policy.runtime import PolicyRuntime
from mrl_rewards.types import RewardError


def read_nonce(fd: int) -> str:
    try:
        with os.fdopen(fd, "r", encoding="utf-8", closefd=True) as f:
            nonce = f.read(4096).strip()
    except OSError as e:
        raise RewardError(STARTUP_FAILED, f"Could not read the nonce from fd {fd}: {e}.") from e
    if not nonce:
        raise RewardError(STARTUP_FAILED, f"The nonce read from fd {fd} is empty.")
    return nonce


_IFF_UP = 0x1


def non_loopback_interfaces() -> list[str]:
    """Network interfaces other than loopback that are up. Linux reads /sys/class/net (a fresh
    network namespace still lists tunnel devices, but they are down); elsewhere any
    non-loopback interface counts."""
    sys_net = Path("/sys/class/net")
    if sys_net.is_dir():
        up: list[str] = []
        for dev in sorted(sys_net.iterdir()):
            if dev.name == "lo" or not dev.is_dir():  # bonding_masters is a file
                continue
            try:
                flags = int((dev / "flags").read_text().strip(), 16)
            except (OSError, ValueError):
                flags = _IFF_UP  # unreadable: assume it is up
            if flags & _IFF_UP:
                up.append(dev.name)
        return up
    try:
        names = [name for _, name in socket.if_nameindex()]
    except OSError:
        return ["<unknown: interfaces could not be listed>"]
    return [n for n in names if not n.lower().startswith("lo")]


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="mrl-reward-exec", description=__doc__)
    ap.add_argument(
        "--bundle", type=Path, required=True, help="reward bundle directory (read-only)"
    )
    ap.add_argument("--listen", required=True, help="unix:/path/to.sock or 127.0.0.1:<port>")
    ap.add_argument("--reward-version-id", required=True, help="the rwdv_… id being served")
    ap.add_argument("--nonce-fd", type=int, required=True, help="fd to read the attempt nonce from")
    d = ExecLimits()
    ap.add_argument("--max-batch", type=int, default=d.max_batch)
    ap.add_argument("--per-sample-timeout-ms", type=int, default=d.per_sample_timeout_ms)
    ap.add_argument("--hard-limit-s", type=float, default=d.hard_limit_s)
    ap.add_argument("--cache-entries", type=int, default=d.result_cache_entries)
    ap.add_argument("--workers", type=int, default=d.workers)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument(
        "--allow-network",
        action="store_true",
        help="local development only: skip the check that refuses to start with a network",
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    audit = Audit()
    limits = ExecLimits(
        max_batch=args.max_batch,
        per_sample_timeout_ms=args.per_sample_timeout_ms,
        hard_limit_s=args.hard_limit_s,
        result_cache_entries=args.cache_entries,
        workers=args.workers,
    )
    try:
        nonce = read_nonce(args.nonce_fd)
        if args.allow_network:
            audit("network_check_skipped")
        elif extra := non_loopback_interfaces():
            raise RewardError(
                STARTUP_FAILED,
                "This machine has network interfaces other than loopback "
                f"({', '.join(extra[:4])}). "
                "The reward service must run without a network: start it in the reward sandbox, "
                "or pass --allow-network for local development.",
            )
        bundle = load_bundle(args.bundle, allow_user_code=False)
        runtime = PolicyRuntime.build(
            bundle.policy, bundle.registry, bundle_dir=bundle.root, seed=args.seed
        )
        service = RewardExecService(
            bundle,
            runtime,
            nonce=nonce,
            reward_version_id=args.reward_version_id,
            limits=limits,
            audit=audit,
        )
        server, port = serve(service, args.listen)
    except RewardError as e:
        audit("start_failed", code=e.code, message=e.message)
        sys.stderr.write(f"{e.code}: {e.message}\n")
        return 2
    audit(
        "ready",
        listen=args.listen,
        port=port,
        reward_version_id=args.reward_version_id,
        content_sha256=bundle.content_sha256,
        builtins_sha256=bundle.registry.digest(),
        components=[c.use for c in bundle.policy.components],
    )
    stop = threading.Event()

    def on_signal(_sig: int, _frame: object) -> None:
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)
    while not stop.wait(0.5):
        pass
    server.stop(grace=5).wait()
    service.close()
    audit("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
