"""The Tier-2 jail: one disposable bubblewrap sandbox per execution of model-written code.

The reward service (the Tier-1 supervisor, which runs no untrusted code itself) writes the
generated code and a harness it authored into a private folder, and starts one jail:

- new user, pid, network, ipc, uts and mount namespaces; no network at all; all
  capabilities dropped; an empty environment; dies with the supervisor;
- the system folders and the Python install read-only, the harness, the code and the
  tests read-only at /jail (outside the writable area), and a fresh in-memory /work
  (64 MiB) that vanishes when the execution ends, so nothing carries over to the next one;
- the agent's seccomp filter for jails (`MRL_JAIL_SECCOMP`, written by the agent's
  `code_exec` profile): ptrace, mount, bpf, module loading, namespace calls and every
  socket but AF_UNIX are refused;
- limits: 5 s of CPU and 10 s wall clock at most (defaults 3 s / 4 s, inside the reward
  service's 5 s per-sample budget), 512 MiB of memory, 64 processes, 64 KiB of output.

The verdict travels on a pipe only the harness holds. The harness makes itself
non-dumpable (nothing else in the jail can read its memory or its `/proc` files), reads a
one-time token, then forks: the child forgets the token, closes the verdict pipe and runs
the code and the tests, reporting to the harness on a pipe of its own; the harness keeps
only well-formed fields of that report and writes `{"token": …, …}`. So generated code can
never reach the token or the verdict pipe. Printed text never counts, an early exit leaves
no results (a failure), a seccomp kill of the child is reported (`child_signal`) and the
test files are re-hashed after the run. Code that shares the interpreter with assert-style
tests can still fake *their* outcomes (the tests aren't secret), which is why `code.*` is
beta.
"""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import io
import json
import os
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from mrl_rewards.types import JSONValue, RewardError

JAIL_UNAVAILABLE = "MRL-RWD-295"
SECCOMP_ENV = "MRL_JAIL_SECCOMP"
SYSTEM_PATHS = ("/usr", "/bin", "/sbin", "/lib", "/lib32", "/lib64", "/etc")
VERDICT_FD = 4
TOKEN_FD = 5
SECCOMP_FD = 3
MAX_VERDICT_BYTES = 256 << 10
SIGSYS = 31  # Linux (jails exist only there): what a seccomp kill sends
SIGXCPU = 24  # Linux: what the CPU-time limit sends

# Runs first in the new process (single-threaded, so it is safe where `preexec_fn` is not):
# sets the limits, moves the pipes to their fixed numbers, then becomes bubblewrap.
LAUNCHER = r"""
import fcntl, json, os, resource, sys
cfg = json.loads(sys.argv[1])
for name, value in cfg["rlimits"].items():
    resource.setrlimit(getattr(resource, name), tuple(value))
high = {int(t): fcntl.fcntl(s, fcntl.F_DUPFD, 100) for t, s in cfg["fds"].items()}
for s in cfg["fds"].values():
    os.close(s)
for target, fd in high.items():
    os.dup2(fd, target, inheritable=True)
    os.close(fd)
os.execv(sys.argv[2], sys.argv[2:])
"""

HARNESS = r"""
import ctypes, json, os, sys

MAX_REPORT = 200 << 10


def _err(e):
    return type(e).__name__ + ": " + str(e)[:300]


def _child(mode, folder, w):
    # Runs the generated code. This process never holds the token or the verdict pipe.
    out = os.fdopen(w, "w", encoding="utf-8")

    def report(**fields):
        out.write(json.dumps(fields))
        out.flush()
        os._exit(0)

    with open(folder + "/solution.py", encoding="utf-8", errors="replace") as f:
        source = f.read()
    try:
        code = compile(source, "solution.py", "exec")
    except BaseException as e:
        report(compiled=False, error=_err(e))
    if mode == "compile":
        report(compiled=True)
    with open(folder + "/tests.py", encoding="utf-8", errors="replace") as f:
        tests_source = f.read()
    ns = {"__name__": "solution"}
    try:
        exec(code, ns)
    except BaseException as e:
        report(compiled=True, loaded=False, error=_err(e))
    tests_ns = dict(ns)
    tests_ns["__name__"] = "tests"
    results = []
    try:
        exec(compile(tests_source, "tests.py", "exec"), tests_ns)
    except BaseException as e:
        results.append(["<module>", False, _err(e)])
    else:
        names = sorted(n for n, v in tests_ns.items() if n.startswith("test_") and callable(v))
        if not names:
            results.append(["<module>", True, ""])
        for name in names:
            try:
                tests_ns[name]()
                results.append([name, True, ""])
            except BaseException as e:
                results.append([name, False, _err(e)])
    report(compiled=True, loaded=True, results=results)


def _clean(raw):
    # Only the expected fields and shapes pass from the child's report into the verdict.
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return {}
    if not isinstance(doc, dict):
        return {}
    out = {k: doc[k] for k in ("compiled", "loaded") if isinstance(doc.get(k), bool)}
    if isinstance(doc.get("error"), str):
        out["error"] = doc["error"][:300]
    rows = doc.get("results")
    if isinstance(rows, list):
        out["results"] = [
            [r[0][:200], r[1], r[2][:300]]
            for r in rows[:1000]
            if isinstance(r, list) and len(r) == 3 and isinstance(r[0], str)
            and isinstance(r[1], bool) and isinstance(r[2], str)
        ]
    return out


def _main():
    # Nothing in the jail (the child included) may read this process's memory or files.
    # (Jails exist only on Linux; elsewhere the harness runs only in tests.)
    linux = sys.platform.startswith("linux")
    if linux and ctypes.CDLL(None).prctl(4, 0, 0, 0, 0) != 0:  # PR_SET_DUMPABLE = 0
        os._exit(3)
    token = bytearray(256)
    n = os.readv(5, [token])
    os.close(5)
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        token[:] = bytes(len(token))
        os.close(4)
        os.close(r)
        try:
            _child(sys.argv[1], sys.argv[2], w)
        finally:
            os._exit(0)
    os.close(w)
    raw = bytearray()
    while len(raw) <= MAX_REPORT:
        chunk = os.read(r, 65536)
        if not chunk:
            break
        raw += chunk
    _, status = os.waitpid(pid, 0)
    verdict = _clean(bytes(raw)) if len(raw) <= MAX_REPORT else {}
    if os.WIFSIGNALED(status):
        verdict["child_signal"] = os.WTERMSIG(status)
    verdict["token"] = bytes(token[:n]).decode("ascii", "replace")
    with os.fdopen(4, "w", encoding="utf-8") as out:
        out.write(json.dumps(verdict))


_main()
"""


@dataclass(frozen=True)
class JailLimits:
    """Limits of one execution (Doc 06 §1.8: at most 10 s wall, 5 s CPU)."""

    wall_s: float = 4.0
    cpu_s: int = 3
    memory_bytes: int = 512 << 20
    pids: int = 64
    output_bytes: int = 64 << 10
    tmpfs_bytes: int = 64 << 20

    def checked(self) -> JailLimits:
        if not (0 < self.wall_s <= 10 and 0 < self.cpu_s <= 5):
            raise RewardError(
                "MRL-RWD-143", "Code limits: wall time up to 10 s and CPU time 1 to 5 s."
            )
        return self


@dataclass(frozen=True)
class JailResult:
    """What one execution did. `verdict` is None when the harness wrote none."""

    verdict: dict[str, JSONValue] | None
    exit_code: int | None
    signal: int | None
    timed_out: bool
    output: str
    output_flooded: bool
    tests_changed: bool

    @property
    def sandbox_violation(self) -> bool:
        """The seccomp filter killed the harness or the code it ran (SIGSYS; bubblewrap
        passes on a signal death as exit code 128 + the signal)."""
        child = (self.verdict or {}).get("child_signal")
        return SIGSYS in (self.signal, child) or self.exit_code == 128 + SIGSYS


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Jail:
    """Starts jails. Build it with [`Jail.from_environment`] in the reward service."""

    def __init__(
        self,
        bwrap: Path,
        seccomp: Path | None,
        python: Path,
        read_paths: tuple[Path, ...] = (),
        limits: JailLimits | None = None,
    ) -> None:
        self.bwrap = bwrap
        self.seccomp = seccomp
        self.python = python
        self.read_paths = read_paths
        self.limits = (limits or JailLimits()).checked()

    @classmethod
    def from_environment(cls, limits: JailLimits | None = None) -> Jail:
        """The jail for this machine, or MRL-RWD-295 when it can't isolate code here: not
        Linux, no bubblewrap, or no seccomp filter from the agent's `code_exec` profile."""
        if not sys.platform.startswith("linux"):
            raise RewardError(
                JAIL_UNAVAILABLE,
                "Code-running reward components need Linux with bubblewrap; this machine "
                f"runs {sys.platform}.",
            )
        bwrap = shutil.which("bwrap")
        if bwrap is None:
            raise RewardError(JAIL_UNAVAILABLE, "bubblewrap (bwrap) is not installed here.")
        filt = os.environ.get(SECCOMP_ENV, "")
        if not filt or not Path(filt).is_file():
            raise RewardError(
                JAIL_UNAVAILABLE,
                "No seccomp filter for code jails was given (start the reward service in "
                "the agent's code_exec sandbox).",
            )
        prefixes = {Path(sys.base_prefix).resolve(), Path(sys.prefix).resolve()}
        return cls(Path(bwrap), Path(filt), Path(sys.executable).resolve(), tuple(prefixes), limits)

    def argv(self, folder: Path, mode: str) -> list[str]:
        """The bubblewrap command line for one execution (the program comes after `--`)."""
        a = [
            str(self.bwrap),
            "--die-with-parent",
            "--new-session",
            "--unshare-all",
            "--cap-drop",
            "ALL",
            "--clearenv",
            "--setenv",
            "PATH",
            "/usr/bin:/bin",
            "--setenv",
            "HOME",
            "/work",
            "--setenv",
            "PYTHONDONTWRITEBYTECODE",
            "1",
            "--hostname",
            "jail",
        ]
        for p in SYSTEM_PATHS:
            a += ["--ro-bind-try", p, p]
        for p in self.read_paths:
            a += ["--ro-bind-try", str(p), str(p)]
        a += ["--proc", "/proc", "--dev", "/dev"]
        a += ["--size", str(self.limits.tmpfs_bytes), "--tmpfs", "/work"]
        a += ["--ro-bind", str(folder), "/jail", "--chdir", "/work"]
        if self.seccomp is not None:
            a += ["--seccomp", str(SECCOMP_FD)]
        a += ["--", str(self.python), "-I", "-S", "-B", "/jail/harness.py", mode, "/jail"]
        return a

    def _rlimits(self) -> dict[str, tuple[int, int]]:
        """(soft, hard) per limit. CPU time gets one more hard second, so running out of it
        arrives as SIGXCPU (reported as a timeout) before the kernel's SIGKILL."""
        lim = self.limits
        return {
            "RLIMIT_CPU": (lim.cpu_s, lim.cpu_s + 1),
            "RLIMIT_AS": (lim.memory_bytes, lim.memory_bytes),
            "RLIMIT_FSIZE": (lim.tmpfs_bytes, lim.tmpfs_bytes),
            "RLIMIT_CORE": (0, 0),
            # Counted per user, so the processes this user already runs are added.
            "RLIMIT_NPROC": (_own_processes() + lim.pids, _own_processes() + lim.pids),
        }

    def run(self, mode: str, solution: str, tests: str = "") -> JailResult:
        """Run `solution` (mode `compile` or `tests`) in a fresh jail."""
        with tempfile.TemporaryDirectory(prefix="mrl-jail-") as tmp:
            folder = Path(tmp)
            files = {"harness.py": HARNESS, "solution.py": solution, "tests.py": tests}
            for name, text in files.items():
                (folder / name).write_text(text, encoding="utf-8")
                (folder / name).chmod(0o444)
            tests_sha = _sha((folder / "tests.py").read_bytes())
            result = self._execute(folder, mode)
            changed = _sha((folder / "tests.py").read_bytes()) != tests_sha
        return dataclasses.replace(result, tests_changed=changed)

    def _execute(self, folder: Path, mode: str) -> JailResult:
        token = secrets.token_hex(16)
        verdict_r, verdict_w = os.pipe()
        token_r, token_w = os.pipe()
        os.write(token_w, token.encode())
        os.close(token_w)
        seccomp_fd = os.open(self.seccomp, os.O_RDONLY) if self.seccomp is not None else None
        fd_map = {VERDICT_FD: verdict_w, TOKEN_FD: token_r}
        if seccomp_fd is not None:
            fd_map[SECCOMP_FD] = seccomp_fd
        cfg = json.dumps({"rlimits": self._rlimits(), "fds": fd_map})
        try:
            proc = subprocess.Popen(
                [str(self.python), "-I", "-S", "-c", LAUNCHER, cfg, *self.argv(folder, mode)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env={"PATH": "/usr/bin:/bin"},
                start_new_session=True,
                pass_fds=tuple(fd_map.values()),
            )
        finally:
            for fd in (verdict_w, token_r, seccomp_fd):
                if fd is not None:
                    os.close(fd)
        verdict_bytes = bytearray()
        reader = threading.Thread(target=_drain, args=(verdict_r, verdict_bytes), daemon=True)
        reader.start()
        output, flooded, timed_out = self._watch(proc)
        reader.join(timeout=1.0)
        os.close(verdict_r)
        code = proc.returncode
        sig = -code if code is not None and code < 0 else None
        return JailResult(
            verdict=_verdict(bytes(verdict_bytes), token),
            exit_code=code if sig is None else None,
            signal=sig,
            timed_out=timed_out,
            output=output,
            output_flooded=flooded,
            tests_changed=False,
        )

    def _watch(self, proc: subprocess.Popen[bytes]) -> tuple[str, bool, bool]:
        if proc.stdout is None:
            raise RewardError(JAIL_UNAVAILABLE, "The jail's output could not be read.")
        stdout = cast("io.BufferedReader", proc.stdout)
        cap = self.limits.output_bytes
        buf = bytearray()
        flooded = False
        done = threading.Event()

        def read() -> None:
            nonlocal flooded
            while chunk := stdout.read1(65536):
                if len(buf) + len(chunk) > cap:
                    buf.extend(chunk[: cap - len(buf)])
                    flooded = True
                    _kill(proc)
                    break
                buf.extend(chunk)
            done.set()

        t = threading.Thread(target=read, daemon=True)
        t.start()
        deadline = time.monotonic() + self.limits.wall_s
        timed_out = False
        try:
            proc.wait(timeout=self.limits.wall_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            _kill(proc)
            proc.wait()
        done.wait(timeout=max(0.5, deadline - time.monotonic()))
        _kill(proc)  # a child that kept the session alive
        stdout.close()
        return buf.decode("utf-8", "replace"), flooded, timed_out


def _kill(proc: subprocess.Popen[bytes]) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGKILL)


def _drain(fd: int, into: bytearray) -> None:
    while len(into) <= MAX_VERDICT_BYTES:
        try:
            chunk = os.read(fd, 65536)
        except OSError:
            return
        if not chunk:
            return
        into.extend(chunk)


def _verdict(raw: bytes, token: str) -> dict[str, JSONValue] | None:
    if not raw or len(raw) > MAX_VERDICT_BYTES:
        return None
    try:
        doc: object = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(doc, dict):
        return None
    out = cast("dict[str, JSONValue]", doc)
    return out if out.get("token") == token else None


def _own_processes() -> int:
    uid = os.getuid()
    count = 0
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                if entry.stat().st_uid == uid:
                    count += 1
            except OSError:
                continue
    return count


__all__ = ["HARNESS", "JAIL_UNAVAILABLE", "SECCOMP_ENV", "Jail", "JailLimits", "JailResult"]
