"""`mrl-reward-sandboxed`: the reward service the agent starts in the reward sandbox.

Same command line and behaviour as P14's `mrl-reward-exec` (RewardService over a Unix
socket, nonce on an inherited fd, refuses to start with a network), plus:

- the Tier-2 `code.*` components are installed (they need the `code_exec` sandbox: without
  the jail's seccomp filter they refuse to start, MRL-RWD-295);
- `--user-code` loads the bundle's custom code, but only after `sandbox_proof()` holds;
- audit lines add `sandbox` (the profile and the proof) to `ready`;
- `--jail` first moves the service into its own bubblewrap jail (used by the REWARD_PREVIEW
  task, so custom code never shares a sandbox with the harness that writes the report): new
  namespaces, no network, all capabilities dropped, an empty environment, the system and
  Python folders and the bundle read-only, only the socket folder writable, a 512 MiB
  in-memory `/tmp`, and the agent's seccomp filter (`MRL_JAIL_SECCOMP`; with `--tier 2`
  `MRL_SUPERVISOR_SECCOMP`, so `code.*` components can start their own jails). Without a
  jail (not Linux, no bubblewrap, no filter) a bundle with custom code is refused
  (MRL-RWD-295) and a built-in-only bundle runs as it is;
- `--allow-network` (built-in components only, never with `--user-code`) skips the check
  that refuses to start with a network, for machines with no sandbox (macOS, Windows):
  the shipped components are trusted code, and the audit line `network_check_skipped`
  records it.
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import sys
import threading
from pathlib import Path

from mrl_rewards.exec.main import non_loopback_interfaces, read_nonce
from mrl_rewards.exec.server import STARTUP_FAILED, Audit, ExecLimits, RewardExecService, serve
from mrl_rewards.policy.runtime import PolicyRuntime
from mrl_rewards.server_bundle import (
    PROFILE_ENV,
    USER_CODE_ENV,
    USER_CODE_REFUSED,
    load,
    sandbox_proof,
)
from mrl_rewards.types import RewardError

JAIL_ENV = "MRL_JAIL_SECCOMP"
SUPERVISOR_ENV = "MRL_SUPERVISOR_SECCOMP"
JAIL_DIR = "/run/mrl-jail"
SYSTEM_PATHS = ("/usr", "/bin", "/sbin", "/lib", "/lib32", "/lib64", "/etc")
SECCOMP_FD = 3


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="mrl-reward-sandboxed", description=__doc__)
    ap.add_argument("--bundle", type=Path, required=True, help="reward bundle directory")
    ap.add_argument("--listen", required=True, help="unix:/path/to.sock or 127.0.0.1:<port>")
    ap.add_argument("--reward-version-id", required=True, help="the rwdv_… id being served")
    ap.add_argument("--nonce-fd", type=int, required=True, help="fd to read the attempt nonce from")
    ap.add_argument("--user-code", action="store_true", help="load the bundle's custom code")
    ap.add_argument("--jail", action="store_true", help="first move into a bubblewrap jail")
    ap.add_argument(
        "--tier", type=int, choices=(1, 2), default=1, help="sandbox tier of the reward"
    )
    ap.add_argument(
        "--allow-network",
        action="store_true",
        help="built-in components only, where no sandbox exists: skip the network check",
    )
    d = ExecLimits()
    ap.add_argument("--max-batch", type=int, default=d.max_batch)
    ap.add_argument("--per-sample-timeout-ms", type=int, default=d.per_sample_timeout_ms)
    ap.add_argument("--hard-limit-s", type=float, default=d.hard_limit_s)
    ap.add_argument("--workers", type=int, default=d.workers)
    ap.add_argument("--seed", type=int, default=0)
    return ap


def start(args: argparse.Namespace, audit: Audit) -> tuple[RewardExecService, object]:
    """Loads the bundle and starts serving; raises RewardError with a catalogued code."""
    nonce = read_nonce(args.nonce_fd)
    if args.allow_network and args.user_code:
        raise RewardError(
            USER_CODE_REFUSED, "Custom reward code never runs with a network; drop --allow-network."
        )
    if args.allow_network:
        audit("network_check_skipped", reason="built-in components only")
    elif extra := non_loopback_interfaces():
        raise RewardError(
            STARTUP_FAILED,
            f"Network interfaces other than loopback are up ({', '.join(extra[:4])}); the "
            "reward service runs only without a network.",
        )
    bundle = load(args.bundle, user_code=args.user_code)
    runtime = PolicyRuntime.build(
        bundle.policy, bundle.registry, bundle_dir=bundle.root, seed=args.seed
    )
    limits = ExecLimits(
        max_batch=args.max_batch,
        per_sample_timeout_ms=args.per_sample_timeout_ms,
        hard_limit_s=args.hard_limit_s,
        workers=args.workers,
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
    proof = sandbox_proof()
    audit(
        "ready",
        listen=args.listen,
        port=port,
        reward_version_id=args.reward_version_id,
        content_sha256=bundle.content_sha256,
        components=[c.use for c in bundle.policy.components],
        user_code=bundle.user_code,
        sandbox={
            "profile": os.environ.get(PROFILE_ENV, ""),
            "isolated": proof.ok,
            "why": proof.why,
        },
    )
    return service, server


def jail_argv(args: argparse.Namespace, raw: list[str]) -> list[str] | None:
    """The bubblewrap command that runs this service (without `--jail`) in a jail, or None
    when no jail can be made here."""
    bwrap = shutil.which("bwrap")
    filt = os.environ.get(SUPERVISOR_ENV if args.tier == 2 else JAIL_ENV, "")
    if (
        not sys.platform.startswith("linux")
        or bwrap is None
        or not filt
        or not Path(filt).is_file()
    ):
        return None
    socket_dir = Path(str(args.listen).removeprefix("unix:")).parent
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/tmp",
        "TMPDIR": "/tmp",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        PROFILE_ENV: "code_exec" if args.tier == 2 else "reward",
        USER_CODE_ENV: os.environ.get(USER_CODE_ENV, "0"),
    }
    if args.tier == 2:
        env[JAIL_ENV] = os.environ.get(JAIL_ENV, "")
    a = [
        bwrap,
        "--die-with-parent",
        "--new-session",
        "--unshare-all",
        "--cap-drop",
        "ALL",
        "--hostname",
        "mrl-reward",
        "--clearenv",
    ]
    for k, v in env.items():
        a += ["--setenv", k, v]
    for p in SYSTEM_PATHS:
        a += ["--ro-bind-try", p, p]
    for p in sorted(
        {
            Path(sys.prefix).resolve(),
            Path(sys.base_prefix).resolve(),
            Path(sys.executable).resolve().parent,
        }
    ):
        a += ["--ro-bind-try", str(p), str(p)]
    if args.tier == 2 and Path(JAIL_DIR).is_dir():
        a += ["--ro-bind", JAIL_DIR, JAIL_DIR]
    bundle = str(args.bundle.resolve())
    a += ["--proc", "/proc", "--dev", "/dev", "--size", str(512 << 20), "--tmpfs", "/tmp"]
    a += ["--ro-bind", bundle, bundle, "--bind", str(socket_dir), str(socket_dir)]
    a += ["--chdir", bundle, "--seccomp", str(SECCOMP_FD), "--remount-ro", "/", "--"]
    inner = [x for x in raw if x != "--jail"]
    return [*a, sys.executable, "-I", "-m", "mrl_rewards.server_bundle.serve", *inner]


def enter_jail(args: argparse.Namespace, raw: list[str], audit: Audit) -> None:
    """Replaces this process with the jailed service, or returns when no jail can be made
    (refusing custom code then)."""
    if args.nonce_fd == SECCOMP_FD:
        raise RewardError(STARTUP_FAILED, f"The nonce can't come on fd {SECCOMP_FD}; use fd 5.")
    argv = jail_argv(args, raw)
    if argv is None:
        if args.user_code:
            raise RewardError(
                USER_CODE_REFUSED,
                "Custom reward code needs a jail: Linux, bubblewrap and the agent's seccomp "
                "filter for jails. This machine has none of that set up.",
            )
        audit("jail_skipped", reason="no jail here; built-in components only")
        return
    filt = os.environ[SUPERVISOR_ENV if args.tier == 2 else JAIL_ENV]
    fd = os.open(filt, os.O_RDONLY)
    if fd != SECCOMP_FD:
        os.dup2(fd, SECCOMP_FD, inheritable=True)
        os.close(fd)
    os.set_inheritable(args.nonce_fd, True)
    sys.stdout.flush()
    os.execv(argv[0], argv)


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    args = _parser().parse_args(raw)
    audit = Audit()
    if args.jail:
        try:
            enter_jail(args, raw, audit)
        except (RewardError, OSError) as e:
            code = e.code if isinstance(e, RewardError) else STARTUP_FAILED
            audit("start_failed", code=code, message=str(e))
            sys.stderr.write(f"{code}: {e}\n")
            return 2
    try:
        service, server = start(args, audit)
    except RewardError as e:
        audit("start_failed", code=e.code, message=e.message)
        sys.stderr.write(f"{e.code}: {e.message}\n")
        return 2
    stop = threading.Event()

    def on_signal(_sig: int, _frame: object) -> None:
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)
    while not stop.wait(0.5):
        pass
    server.stop(grace=5).wait()  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
    service.close()
    audit("stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
