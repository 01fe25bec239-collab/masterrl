"""SB-1: hostile *reward code* (Tier 1). Loaded by the sandboxed reward service as a user
component (`org/escape.probe@1`); its `attack` param picks one probe.

A probe returns (achieved, evidence). For an attack, "achieved" means it got out: the
corpus expects 0.0 every time. The `control` probe does what the sandbox allows (it must
score 1.0, so a broken sandbox can't pass by refusing everything). A probe that the kernel
kills (seccomp) ends the service: the agent reports that as a sandbox violation.
"""

from __future__ import annotations

import ctypes
import json
import os
import platform
import signal
import socket
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mrl_rewards.types import ComponentSpec, RewardResult

Canary = dict[str, Any]
ProbeFn = Callable[[Canary], tuple[bool, str]]

# Syscall numbers (x86_64, aarch64) for the calls Python has no wrapper for.
_NR = {
    "x86_64": {
        "ptrace": 101,
        "process_vm_readv": 310,
        "bpf": 321,
        "keyctl": 250,
        "perf_event_open": 298,
        "userfaultfd": 323,
        "open_by_handle_at": 304,
        "init_module": 175,
        "kexec_load": 246,
        "mount": 165,
        "setns": 308,
        "clone": 56,
        "io_uring_setup": 425,
    },
    "aarch64": {
        "ptrace": 117,
        "process_vm_readv": 270,
        "bpf": 280,
        "keyctl": 219,
        "perf_event_open": 241,
        "userfaultfd": 282,
        "open_by_handle_at": 265,
        "init_module": 105,
        "kexec_load": 104,
        "mount": 40,
        "setns": 268,
        "clone": 220,
        "io_uring_setup": 425,
    },
}
CLONE_NEWNS = 0x0002_0000
SIGCHLD = 17


def _syscall(name: str, *args: int) -> tuple[int, int]:
    libc = ctypes.CDLL(None, use_errno=True)
    nr = _NR[platform.machine()][name]
    fn = libc.syscall
    fn.restype = ctypes.c_long
    r = fn(ctypes.c_long(nr), *(ctypes.c_long(a) for a in args))
    return int(r), ctypes.get_errno()


def _raw(name: str, *args: int) -> ProbeFn:
    def probe(_c: Canary) -> tuple[bool, str]:
        r, err = _syscall(name, *args)
        if r == 0 and name == "clone":
            os._exit(0)  # the clone's child, if the call ever got through
        return r >= 0, f"{name} returned {r} (errno {err})"

    return probe


def control(_c: Canary) -> tuple[bool, str]:
    Path("/tmp/mrl-probe").write_text("ok", encoding="utf-8")
    policy = json.loads(Path("policy.json").read_text(encoding="utf-8"))
    a, b = socket.socketpair(socket.AF_UNIX)
    a.close()
    b.close()
    return bool(policy.get("spec")), "wrote /tmp, read its bundle, made a unix socketpair"


def tcp_canary(c: Canary) -> tuple[bool, str]:
    last = "no canary listener given"
    for addr in c["tcp"]:
        host, port = addr.rsplit(":", 1)
        try:
            with socket.create_connection((host, int(port)), timeout=2) as s:
                return True, f"{addr}: {s.recv(256)!r}"
        except OSError as e:
            last = f"{addr}: {e}"
    return False, last


def tcp_internet(_c: Canary) -> tuple[bool, str]:
    last = ""
    for addr in (("1.1.1.1", 443), ("8.8.8.8", 53)):
        try:
            with socket.create_connection(addr, timeout=2):
                return True, f"connected to {addr}"
        except OSError as e:
            last = str(e)
    return False, last


def dns(_c: Canary) -> tuple[bool, str]:
    try:
        got = socket.getaddrinfo("example.com", 443)
    except OSError as e:
        return False, str(e)
    return bool(got), repr(got[:1])


def udp_send(_c: Canary) -> tuple[bool, str]:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.sendto(b"x", ("8.8.8.8", 53))
    except OSError as e:
        return False, str(e)
    return True, "sent a UDP packet"


def _socket(family: int, kind: int, proto: int = 0) -> tuple[bool, str]:
    try:
        socket.socket(family, kind, proto).close()
    except OSError as e:
        return False, str(e)
    return True, f"opened a family {family} socket"


def packet_socket(_c: Canary) -> tuple[bool, str]:
    return _socket(17, socket.SOCK_RAW)  # AF_PACKET


def netlink_socket(_c: Canary) -> tuple[bool, str]:
    return _socket(16, socket.SOCK_RAW)  # AF_NETLINK


def inet6_socket(_c: Canary) -> tuple[bool, str]:
    return _socket(socket.AF_INET6, socket.SOCK_STREAM)


def _unix_connect(target: str) -> tuple[bool, str]:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect(target)
            return True, f"connected to {target!r}: {s.recv(256)!r}"
    except OSError as e:
        return False, f"{target!r}: {e}"


def unix_abstract(c: Canary) -> tuple[bool, str]:
    return _unix_connect("\0" + c["abstract"])


def unix_path(c: Canary) -> tuple[bool, str]:
    return _unix_connect(c["unix"])


def docker_socket(_c: Canary) -> tuple[bool, str]:
    for p in ("/var/run/docker.sock", "/run/docker.sock", "/run/containerd/containerd.sock"):
        ok, why = _unix_connect(p)
        if ok:
            return ok, why
    return False, "no container runtime socket"


def read_canary_files(c: Canary) -> tuple[bool, str]:
    for p in c["files"]:
        try:
            return True, f"{p}: {Path(p).read_text(encoding='utf-8')[:64]}"
        except OSError:
            continue
    return False, "no canary file is visible"


def env_secrets(_c: Canary) -> tuple[bool, str]:
    bad = sorted(k for k in os.environ if k.startswith(("MRL_CREDENTIAL", "MRL_ESCAPE", "AWS_")))
    return bool(bad), f"environment: {sorted(os.environ)}"


def proc_environ(_c: Canary) -> tuple[bool, str]:
    seen: list[str] = []
    for d in Path("/proc").iterdir():
        if d.name.isdigit():
            try:
                text = (d / "environ").read_bytes().decode("utf-8", "replace")
            except OSError:
                continue
            seen.append(text[:2000])
    joined = "\n".join(seen)
    return "MRL_CREDENTIAL" in joined or "MRL_ESCAPE" in joined, joined[:3000]


def host_processes(c: Canary) -> tuple[bool, str]:
    marker = c["sentinel_marker"]
    for d in Path("/proc").iterdir():
        if d.name.isdigit():
            try:
                cmd = (d / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
            except OSError:
                continue
            if marker in cmd:
                return True, f"saw the host process {d.name}: {cmd}"
    return False, "no host process is visible"


def kill_sentinel(c: Canary) -> tuple[bool, str]:
    try:
        os.kill(int(c["sentinel_pid"]), signal.SIGKILL)
    except OSError as e:
        return False, str(e)
    # A process with that number exists in here: the driver checks the real one lives.
    return False, "signalled a process of the same number inside the sandbox"


def kill_everything(_c: Canary) -> tuple[bool, str]:
    os.kill(-1, signal.SIGKILL)
    return False, "kill(-1) returned"


def write_bundle(_c: Canary) -> tuple[bool, str]:
    for p in ("policy.json", "new-file.txt"):
        try:
            with Path(p).open("a", encoding="utf-8") as f:
                f.write("x")
            return True, f"wrote {p} in the bundle"
        except OSError:
            continue
    return False, "the bundle is read-only"


def write_system(_c: Canary) -> tuple[bool, str]:
    for p in ("/etc/mrl-escape", "/usr/mrl-escape", "/mrl-escape", "/proc/sys/kernel/hostname"):
        try:
            with Path(p).open("w", encoding="utf-8") as f:
                f.write("x")
            return True, f"wrote {p}"
        except OSError:
            continue
    return False, "system folders are read-only"


def fill_tmp(_c: Canary) -> tuple[bool, str]:
    chunk = b"\0" * (1 << 20)
    written = 0
    try:
        with Path("/tmp/fill").open("wb") as f:
            for _ in range(1024):
                f.write(chunk)
                written += len(chunk)
    except OSError as e:
        return written > (512 << 20), f"stopped at {written >> 20} MiB: {e}"
    finally:
        Path("/tmp/fill").unlink(missing_ok=True)
    return True, f"wrote {written >> 20} MiB to /tmp"


def setuid_root(_c: Canary) -> tuple[bool, str]:
    last = ""
    for cmd in (["sudo", "-n", "id", "-u"], ["su", "-c", "id -u"]):
        try:
            out = subprocess.run(cmd, capture_output=True, timeout=5, text=True, check=False)
        except (OSError, subprocess.TimeoutExpired) as e:
            last = str(e)
            continue
        if out.returncode == 0 and out.stdout.strip() == "0":
            return True, f"{cmd[0]} gave root"
        last = (out.stderr or out.stdout)[:200]
    return False, last


def devices(_c: Canary) -> tuple[bool, str]:
    found = [
        p
        for p in (
            "/dev/nvidia0",
            "/dev/nvidiactl",
            "/dev/dri",
            "/dev/kvm",
            "/dev/mem",
            "/dev/kmsg",
            "/dev/sda",
            "/dev/nvme0",
        )
        if Path(p).exists()
    ]
    return bool(found), f"devices: {found}"


def drop_no_new_privs(_c: Canary) -> tuple[bool, str]:
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl(38, 0, 0, 0, 0)  # PR_SET_NO_NEW_PRIVS 0 (refused by the kernel)
    status = Path("/proc/self/status").read_text(encoding="utf-8")
    fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    nnp, sec = fields.get("NoNewPrivs", "").strip(), fields.get("Seccomp", "").strip()
    return nnp != "1" or sec != "2", f"NoNewPrivs {nnp}, Seccomp {sec}"


def unshare_user(_c: Canary) -> tuple[bool, str]:
    linux: Any = os  # os.unshare exists only on Linux
    linux.unshare(linux.CLONE_NEWUSER)
    return True, "made a user namespace"


def chroot(_c: Canary) -> tuple[bool, str]:
    os.chroot("/tmp")
    return True, "chrooted"


PROBES: dict[str, ProbeFn] = {
    "control": control,
    "tcp_canary": tcp_canary,
    "tcp_internet": tcp_internet,
    "dns": dns,
    "udp_send": udp_send,
    "packet_socket": packet_socket,
    "netlink_socket": netlink_socket,
    "inet6_socket": inet6_socket,
    "unix_abstract": unix_abstract,
    "unix_path": unix_path,
    "docker_socket": docker_socket,
    "read_canary_files": read_canary_files,
    "env_secrets": env_secrets,
    "proc_environ": proc_environ,
    "host_processes": host_processes,
    "kill_sentinel": kill_sentinel,
    "kill_everything": kill_everything,
    "write_bundle": write_bundle,
    "write_system": write_system,
    "fill_tmp": fill_tmp,
    "setuid_root": setuid_root,
    "devices": devices,
    "drop_no_new_privs": drop_no_new_privs,
    "unshare_user": unshare_user,
    "chroot": chroot,
    "ptrace": _raw("ptrace", 16, 1, 0, 0),  # PTRACE_ATTACH to pid 1
    "process_vm_readv": _raw("process_vm_readv", 1, 0, 0, 0, 0, 0),
    "bpf": _raw("bpf", 0, 0, 0),
    "keyctl": _raw("keyctl", 0, -3, 0),  # KEYCTL_GET_KEYRING_ID of the session keyring
    "perf_event_open": _raw("perf_event_open", 0, 0, -1, -1, 0),
    "userfaultfd": _raw("userfaultfd", 0),
    "open_by_handle_at": _raw("open_by_handle_at", -100, 0, 0),
    "init_module": _raw("init_module", 0, 0, 0),
    "kexec_load": _raw("kexec_load", 0, 0, 0, 0),
    "mount": _raw("mount", 0, 0, 0, 0, 0),
    "setns": _raw("setns", 0, 0),
    "clone_newns": _raw("clone", CLONE_NEWNS | SIGCHLD, 0, 0, 0, 0),
    "io_uring": _raw("io_uring_setup", 1, 0),
}


class Probe:
    """Runs one probe per sample."""

    spec = ComponentSpec(
        name="escape.probe",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        param_schema={"type": "object"},
        description="SB-1 escape probe",
    )

    def setup(self, ctx: Any) -> None:
        self.attack = str(ctx.params["attack"])
        self.canary = dict(ctx.params.get("canary") or {})

    def score(self, batch: Any) -> list[RewardResult]:
        out: list[RewardResult] = []
        for _ in batch.samples:
            try:
                achieved, evidence = PROBES[self.attack](self.canary)
            except Exception as e:  # a refused call: the probe got nowhere
                achieved, evidence = False, f"{type(e).__name__}: {e}"
            out.append(RewardResult.ok(1.0 if achieved else 0.0, {"evidence": evidence[:3000]}))
        return out

    def teardown(self) -> None:
        return None
