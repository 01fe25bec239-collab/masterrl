//! Hardened sandbox profiles (P29): `reward` (Tier 1), `code_exec` (Tier 2) and `command`.
//!
//! Each is a registered [`SandboxProfile`] (no network, no GPU) plus a [`Hardening`] that the
//! bubblewrap backend enforces on top of its usual isolation (P12: new namespaces, read-only
//! root and system folders, private `/proc`, minimal `/dev`, dies with the agent):
//!
//! - **seccomp**: a filter compiled here ([`Filter`]), handed to `bwrap --seccomp` on an
//!   inherited fd. `Strict` kills the program on `ptrace`, `mount`, `bpf`, `kexec`, module
//!   loading, `unshare`/`setns`, namespace flags on `clone`, keyrings, `perf_event_open` and
//!   more; every `socket`/`socketpair` that isn't AF_UNIX fails with EAFNOSUPPORT; `clone3`
//!   and io_uring fail with ENOSYS. `Supervisor` (the `code_exec` profile) allows the
//!   namespace and mount calls bubblewrap needs to start Tier-2 jails, nothing else more.
//! - **capabilities**: all dropped (`--cap-drop ALL`).
//! - **environment**: cleared; only the profile's allow-list is set again, so no
//!   `MRL_CREDENTIAL_*` (no secrets) reaches the program. The values travel in an args file
//!   read through `bwrap --args` (deleted before bwrap starts), never on a command line.
//! - **memory filesystem**: `/tmp` is a size-capped tmpfs (`--size`); `TMPDIR` and `HOME`
//!   point there.
//! - **limits**: cgroup CPU, memory and process caps (the profile default when the job asks
//!   for none, never above the profile's cap); a wall-clock limit where the profile has one
//!   (the caller enforces it: see [`command::run`]).
//!
//! On systems without bubblewrap (macOS, Windows, Linux with `sandbox.backend = "none"`) the
//! profiles still apply their limits, but [`user_code_gate`] refuses custom reward code
//! (MRL-RWD-295): it runs only inside an isolating sandbox. Built-in components still run.

pub mod code_exec;
pub mod command;
pub mod reward;

use std::path::{Path, PathBuf};

use super::{SandboxBackend, SandboxProfile};
use crate::exec::{ExecSpec, ExitReport, Limits};
use crate::{AgentError, Result};

/// Code of the platform gate's refusal (guidance: p29.yaml).
pub const USER_CODE_REFUSED: &str = "MRL-RWD-295";
/// Code of a sandbox violation (guidance: p29.yaml).
pub const VIOLATION: &str = "MRL-RWD-294";
/// Environment variable naming the profile inside the sandbox.
pub const PROFILE_ENV: &str = "MRL_SANDBOX_PROFILE";
/// Set to `1` inside an isolating sandbox where custom reward code may load.
pub const USER_CODE_ENV: &str = "MRL_USER_CODE_ALLOWED";
/// The fd bubblewrap reads the seccomp program from.
pub const SECCOMP_FD: u32 = 3;
/// The fd bubblewrap reads the extra arguments from.
pub const ARGS_FD: u32 = 4;
/// The fd a launched reward service reads its nonce from (`--nonce-fd 5`).
pub const NONCE_FD: u32 = 5;
/// Folder under the job folder (outside every sandbox mount) for filter and args files.
pub const SANDBOX_DIR: &str = "sandbox";

/// What the hardened profiles report beyond [`AgentError`].
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// An agent error (sandbox unavailable, state folder, executor).
    #[error(transparent)]
    Agent(#[from] AgentError),
    /// Custom reward code can't run here (the platform gate).
    #[error("custom reward code can't run here: {0}")]
    UserCodeRefused(String),
    /// The sandbox stopped the program for a forbidden system call.
    #[error("the sandbox stopped the program: {0}")]
    Violation(String),
    /// A command ran past its time limit or printed past its output limit.
    #[error("the command was stopped: {0}")]
    CommandLimit(String),
}

impl ProfileError {
    /// The catalog code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Agent(e) => e.code(),
            Self::UserCodeRefused(_) => USER_CODE_REFUSED,
            Self::Violation(_) => VIOLATION,
            Self::CommandLimit(_) => COMMAND_LIMIT,
        }
    }
}

/// Code of a command stopped at its time or output limit (guidance: p29.yaml).
pub const COMMAND_LIMIT: &str = "MRL-RWD-298";

/// Environment names every hardened profile keeps (values from the job spec).
pub const BASE_ENV: &[&str] = &[
    "LANG",
    "LC_ALL",
    "TZ",
    "PYTHONHASHSEED",
    "MRL_ATTEMPT_TOKEN",
];

/// Which seccomp filter a profile runs under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Filter {
    /// Tier 1, Tier-2 jails and commands: no namespace or mount calls, AF_UNIX sockets only.
    Strict,
    /// `Strict`, but any socket family (commands the user allowed onto the network).
    StrictNetwork,
    /// The Tier-2 supervisor: may start bubblewrap jails (namespace and mount calls, and
    /// NETLINK_ROUTE sockets, which bubblewrap uses to bring up a jail's loopback).
    Supervisor,
}

/// What a hardened profile adds to its [`SandboxProfile`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Hardening {
    /// The seccomp filter.
    pub filter: Filter,
    /// Size of the in-memory `/tmp`, bytes.
    pub tmpfs_bytes: u64,
    /// Limits used when the job asks for none.
    pub default_limits: Limits,
    /// The most a job may ask for (requests above are lowered to this).
    pub max_limits: Limits,
    /// Environment names kept besides [`BASE_ENV`].
    pub env: &'static [&'static str],
    /// Keep every variable the job spec sets (commands: the caller chose them), not only the
    /// allow-list; `MRL_CREDENTIAL_*` still never passes.
    pub keep_spec_env: bool,
    /// Wall-clock limit, seconds (enforced by the caller), if the profile has one.
    pub wall_clock_secs: Option<u64>,
    /// Most bytes of output (stdout + stderr) the caller lets the program write.
    pub output_cap_bytes: Option<u64>,
}

/// The hardening of profile `name`, if it is a hardened profile.
pub fn hardening(name: &str) -> Option<Hardening> {
    match name {
        reward::NAME => Some(reward::hardening()),
        code_exec::NAME => Some(code_exec::hardening()),
        command::NAME => Some(command::hardening(false)),
        command::NAME_NET => Some(command::hardening(true)),
        _ => None,
    }
}

/// The hardened profiles (registered in `sandbox::profiles()`).
pub fn all() -> Vec<SandboxProfile> {
    vec![
        reward::profile(),
        code_exec::profile(),
        command::profile(),
        command::profile_net(),
    ]
}

/// The limits a hardened job runs with: each limit the job asks for, lowered to the cap;
/// the profile default where it asks for none.
pub fn effective_limits(h: &Hardening, asked: Limits) -> Limits {
    let pick = |asked: Option<u64>, default: Option<u64>, cap: Option<u64>| {
        let v = asked.or(default);
        match (v, cap) {
            (Some(v), Some(c)) => Some(v.min(c)),
            (v, None) => v,
            (None, c) => c,
        }
    };
    Limits {
        cpu_millis: pick(
            asked.cpu_millis,
            h.default_limits.cpu_millis,
            h.max_limits.cpu_millis,
        ),
        memory_bytes: pick(
            asked.memory_bytes,
            h.default_limits.memory_bytes,
            h.max_limits.memory_bytes,
        ),
        pids: pick(asked.pids, h.default_limits.pids, h.max_limits.pids),
    }
}

/// Refuses custom reward code unless `backend` isolates it on Linux (MRL-RWD-295).
pub fn user_code_gate(backend: &dyn SandboxBackend) -> std::result::Result<(), ProfileError> {
    if cfg!(target_os = "linux") && backend.isolates() && backend.name() == "bubblewrap" {
        return Ok(());
    }
    Err(ProfileError::UserCodeRefused(format!(
            "custom reward code runs only in the bubblewrap reward sandbox on Linux; this machine uses the `{}` sandbox on {}",
        backend.name(),
        std::env::consts::OS
    )))
}

/// How a hardened program ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    /// Exit code 0.
    Ok,
    /// It failed on its own (code or a signal it didn't get from the sandbox).
    Failed(String),
    /// The memory limit stopped it (a reward outcome, not a violation).
    OutOfMemory,
    /// The seccomp filter killed it: the program tried a forbidden system call.
    Violation(String),
}

/// SIGSYS: what the seccomp filter's kill sends.
pub const SIGSYS: i32 = 31;

/// Classifies `report` (Doc 06 §1.8.3): SIGSYS is a violation; OOM and timeouts are not.
/// bubblewrap passes on its program's death by a signal as exit code 128 + the signal, so
/// code 159 counts as SIGSYS too (a program that exits 159 on purpose only fails itself).
pub fn classify(report: &ExitReport) -> Ending {
    if report.signal == Some(SIGSYS) || report.code == Some(128 + SIGSYS) {
        return Ending::Violation("the sandbox's system-call filter stopped the program".into());
    }
    if report.oom_killed {
        return Ending::OutOfMemory;
    }
    match (report.code, report.signal) {
        (Some(0), _) => Ending::Ok,
        (Some(c), _) => Ending::Failed(format!("exited with code {c}")),
        (None, Some(s)) => Ending::Failed(format!("ended by signal {s}")),
        (None, None) => Ending::Failed("ended without an exit status".into()),
    }
}

// ---- seccomp ----

const BPF_LD_W_ABS: u16 = 0x20;
const BPF_JEQ_K: u16 = 0x15;
const BPF_JGE_K: u16 = 0x35;
const BPF_JSET_K: u16 = 0x45;
const BPF_RET_K: u16 = 0x06;
const RET_KILL_PROCESS: u32 = 0x8000_0000;
const RET_ERRNO: u32 = 0x0005_0000;
const RET_ALLOW: u32 = 0x7fff_0000;
const OFF_NR: u32 = 0;
const OFF_ARCH: u32 = 4;
const OFF_ARG0_LO: u32 = 16;
const ENOSYS: u32 = 38;
const EAFNOSUPPORT: u32 = 97;
const AF_UNIX: u32 = 1;
const AF_NETLINK: u32 = 16;
const NETLINK_ROUTE: u32 = 0;
const OFF_ARG2_LO: u32 = 32;
/// Every `CLONE_NEW*` flag (NEWTIME, NEWNS, NEWCGROUP, NEWUTS, NEWIPC, NEWUSER, NEWPID, NEWNET).
const CLONE_NS_FLAGS: u32 = 0x80
    | 0x2_0000
    | 0x0200_0000
    | 0x0400_0000
    | 0x0800_0000
    | 0x1000_0000
    | 0x2000_0000
    | 0x4000_0000;

/// One BPF instruction (`struct sock_filter`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Insn {
    /// Opcode.
    pub code: u16,
    /// Jump if true.
    pub jt: u8,
    /// Jump if false.
    pub jf: u8,
    /// Operand.
    pub k: u32,
}

const fn insn(code: u16, jt: u8, jf: u8, k: u32) -> Insn {
    Insn { code, jt, jf, k }
}

/// System call numbers for one architecture.
#[derive(Debug, Clone, Copy)]
pub struct Arch {
    /// `AUDIT_ARCH_*`.
    pub audit: u32,
    /// x32 calls (x86_64 only) are refused.
    pub x32_bit: Option<u32>,
    clone: u32,
    clone3: u32,
    socket: u32,
    socketpair: u32,
    /// Always killed.
    kill: &'static [u32],
    /// Killed under `Strict` only (namespace and mount calls).
    kill_strict: &'static [u32],
    /// ENOSYS.
    nosys: &'static [u32],
}

/// x86_64 numbers (arch/x86/entry/syscalls/syscall_64.tbl).
pub const X86_64: Arch = Arch {
    audit: 0xC000_003E,
    x32_bit: Some(0x4000_0000),
    clone: 56,
    clone3: 435,
    socket: 41,
    socketpair: 53,
    // ptrace, process_vm_readv/writev, kcmp, bpf, kexec_load, kexec_file_load, init_module,
    // finit_module, delete_module, perf_event_open, keyctl, add_key, request_key,
    // open_by_handle_at, name_to_handle_at, iopl, ioperm, swapon, swapoff, reboot,
    // settimeofday, clock_settime, clock_adjtime, adjtimex, acct, quotactl, userfaultfd,
    // fanotify_init, syslog, uselib, vhangup, lookup_dcookie.
    kill: &[
        101, 310, 311, 312, 321, 246, 320, 175, 313, 176, 298, 250, 248, 249, 304, 303, 172, 173,
        167, 168, 169, 164, 227, 305, 159, 163, 179, 323, 300, 103, 134, 153, 212,
    ],
    // mount, umount2, pivot_root, chroot, unshare, setns, open_tree, move_mount, fsopen,
    // fsconfig, fsmount, fspick, mount_setattr.
    kill_strict: &[
        165, 166, 155, 161, 272, 308, 428, 429, 430, 431, 432, 433, 442,
    ],
    // io_uring_setup, io_uring_enter, io_uring_register.
    nosys: &[425, 426, 427],
};

/// aarch64 numbers (include/uapi/asm-generic/unistd.h).
pub const AARCH64: Arch = Arch {
    audit: 0xC000_00B7,
    x32_bit: None,
    clone: 220,
    clone3: 435,
    socket: 198,
    socketpair: 199,
    kill: &[
        117, 270, 271, 272, 280, 104, 294, 105, 273, 106, 241, 219, 217, 218, 265, 264, 224, 225,
        142, 170, 112, 266, 171, 89, 60, 282, 262, 116, 58, 18,
    ],
    kill_strict: &[40, 39, 41, 51, 97, 268, 428, 429, 430, 431, 432, 433, 442],
    nosys: &[425, 426, 427],
};

/// The architecture of this build, if the filter supports it.
pub fn native_arch() -> Option<Arch> {
    if cfg!(target_arch = "x86_64") {
        Some(X86_64)
    } else if cfg!(target_arch = "aarch64") {
        Some(AARCH64)
    } else {
        None
    }
}

/// The filter program for `arch`.
pub fn program(filter: Filter, arch: &Arch) -> Vec<Insn> {
    let mut p = vec![
        insn(BPF_LD_W_ABS, 0, 0, OFF_ARCH),
        insn(BPF_JEQ_K, 1, 0, arch.audit),
        insn(BPF_RET_K, 0, 0, RET_KILL_PROCESS),
        insn(BPF_LD_W_ABS, 0, 0, OFF_NR),
    ];
    if let Some(bit) = arch.x32_bit {
        p.push(insn(BPF_JGE_K, 0, 1, bit));
        p.push(insn(BPF_RET_K, 0, 0, RET_KILL_PROCESS));
    }
    let mut rule = |nr: u32, ret: u32| {
        p.push(insn(BPF_JEQ_K, 0, 1, nr));
        p.push(insn(BPF_RET_K, 0, 0, ret));
    };
    for nr in arch.kill {
        rule(*nr, RET_KILL_PROCESS);
    }
    let strict = matches!(filter, Filter::Strict | Filter::StrictNetwork);
    if strict {
        for nr in arch.kill_strict {
            rule(*nr, RET_KILL_PROCESS);
        }
    }
    for nr in arch.nosys {
        rule(*nr, RET_ERRNO | ENOSYS);
    }
    rule(arch.clone3, RET_ERRNO | ENOSYS);
    if strict {
        // clone with any CLONE_NEW* flag: killed.
        p.push(insn(BPF_JEQ_K, 0, 4, arch.clone));
        p.push(insn(BPF_LD_W_ABS, 0, 0, OFF_ARG0_LO));
        p.push(insn(BPF_JSET_K, 0, 1, CLONE_NS_FLAGS));
        p.push(insn(BPF_RET_K, 0, 0, RET_KILL_PROCESS));
        p.push(insn(BPF_RET_K, 0, 0, RET_ALLOW));
    }
    for nr in [arch.socket, arch.socketpair]
        .into_iter()
        .filter(|_| filter != Filter::StrictNetwork)
    {
        // socket(domain, …, protocol): only AF_UNIX, and for the supervisor's
        // `socket()` also AF_NETLINK with NETLINK_ROUTE.
        let netlink = filter == Filter::Supervisor && nr == arch.socket;
        p.push(insn(BPF_JEQ_K, 0, if netlink { 8 } else { 4 }, nr));
        p.push(insn(BPF_LD_W_ABS, 0, 0, OFF_ARG0_LO));
        p.push(insn(BPF_JEQ_K, 0, 1, AF_UNIX));
        p.push(insn(BPF_RET_K, 0, 0, RET_ALLOW));
        if netlink {
            p.push(insn(BPF_JEQ_K, 0, 3, AF_NETLINK));
            p.push(insn(BPF_LD_W_ABS, 0, 0, OFF_ARG2_LO));
            p.push(insn(BPF_JEQ_K, 0, 1, NETLINK_ROUTE));
            p.push(insn(BPF_RET_K, 0, 0, RET_ALLOW));
        }
        p.push(insn(BPF_RET_K, 0, 0, RET_ERRNO | EAFNOSUPPORT));
    }
    p.push(insn(BPF_RET_K, 0, 0, RET_ALLOW));
    p
}

/// The program as bytes (`struct sock_filter[]`, native byte order), as `bwrap --seccomp`
/// reads it.
pub fn program_bytes(prog: &[Insn]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prog.len() * 8);
    for i in prog {
        out.extend_from_slice(&i.code.to_ne_bytes());
        out.push(i.jt);
        out.push(i.jf);
        out.extend_from_slice(&i.k.to_ne_bytes());
    }
    out
}

/// What the filter answers for system call `nr` with first argument `arg0` (a small BPF
/// interpreter, used by the tests to check the program means what it says).
pub fn evaluate(prog: &[Insn], arch: u32, nr: u32, arg0: u32) -> u32 {
    evaluate_with(prog, arch, nr, arg0, 0)
}

/// [`evaluate`] with the third argument (the low 32 bits of each) too.
pub fn evaluate_with(prog: &[Insn], arch: u32, nr: u32, arg0: u32, arg2: u32) -> u32 {
    let (mut acc, mut pc) = (0u32, 0usize);
    while let Some(i) = prog.get(pc) {
        match i.code {
            BPF_LD_W_ABS => {
                acc = match i.k {
                    OFF_NR => nr,
                    OFF_ARCH => arch,
                    OFF_ARG0_LO => arg0,
                    OFF_ARG2_LO => arg2,
                    _ => 0,
                };
            }
            BPF_JEQ_K | BPF_JGE_K | BPF_JSET_K => {
                let yes = match i.code {
                    BPF_JEQ_K => acc == i.k,
                    BPF_JGE_K => acc >= i.k,
                    _ => acc & i.k != 0,
                };
                pc += usize::from(if yes { i.jt } else { i.jf });
            }
            BPF_RET_K => return i.k,
            _ => return RET_KILL_PROCESS,
        }
        pc += 1;
    }
    RET_KILL_PROCESS
}

/// Writes the filter for this machine to `path` (0600).
pub fn write_filter(filter: Filter, path: &Path) -> Result<()> {
    let arch = native_arch().ok_or_else(|| {
        AgentError::SandboxUnavailable(format!(
            "the reward sandbox's system-call filter doesn't support {}",
            std::env::consts::ARCH
        ))
    })?;
    let bytes = program_bytes(&program(filter, &arch));
    write_private(path, &bytes)
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| AgentError::state(dir, e))?;
    }
    let tmp = path.with_extension("tmp");
    {
        use std::io::Write as _;
        let mut o = std::fs::OpenOptions::new();
        o.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            o.mode(0o600);
        }
        let mut f = o.open(&tmp).map_err(|e| AgentError::state(&tmp, e))?;
        f.write_all(bytes).map_err(|e| AgentError::state(&tmp, e))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| AgentError::state(path, e))
}

// ---- wrapping ----

/// Folder for this job's filter and args files: next to `work/`, never mounted.
pub fn sandbox_dir(spec: &ExecSpec) -> PathBuf {
    spec.work_dir
        .parent()
        .map_or_else(|| spec.work_dir.join(SANDBOX_DIR), |p| p.join(SANDBOX_DIR))
}

/// The environment set inside the sandbox (everything else is cleared).
pub fn sandbox_env(
    profile: &SandboxProfile,
    h: &Hardening,
    spec: &ExecSpec,
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = vec![
        ("PATH".into(), "/usr/local/bin:/usr/bin:/bin".into()),
        ("HOME".into(), "/tmp".into()),
        ("TMPDIR".into(), "/tmp".into()),
        ("PYTHONDONTWRITEBYTECODE".into(), "1".into()),
        ("PYTHONNOUSERSITE".into(), "1".into()),
        (PROFILE_ENV.into(), profile.name.into()),
        (
            "MRL_STAGING_DIR".into(),
            spec.staging_dir.display().to_string(),
        ),
    ];
    for (k, v) in &spec.env {
        let keep = h.keep_spec_env || BASE_ENV.contains(&k.as_str()) || h.env.contains(&k.as_str());
        if keep && !k.starts_with("MRL_CREDENTIAL_") && !env.iter().any(|(e, _)| e == k) {
            env.push((k.clone(), v.clone()));
        }
    }
    env
}

/// Rewrites a bubblewrap-wrapped `spec` (the output of `Bubblewrap::wrap`) with `extra`
/// bubblewrap options read from an args file, the seccomp filter on fd 3 and the tmpfs size.
/// The program becomes `/bin/sh`, which opens both files, deletes the args file and runs
/// bwrap. The outer command gets no environment of its own (the sandbox's is in the args
/// file), and the program runs as pid 1 of its namespace.
pub fn harden_bwrap(
    mut spec: ExecSpec,
    h: &Hardening,
    extra: &[String],
    files: &Path,
    nonce: Option<&str>,
) -> Result<ExecSpec> {
    if let Some(at) = spec
        .args
        .windows(2)
        .position(|w| w[0] == "--tmpfs" && w[1] == "/tmp")
    {
        let size = h.tmpfs_bytes.to_string();
        spec.args.splice(at..at, ["--size".to_owned(), size]);
    }
    // Extra mounts go before `--remount-ro /` (a read-only root takes no new mount points).
    let at = spec
        .args
        .iter()
        .position(|a| a == "--remount-ro")
        .or_else(|| spec.args.iter().position(|a| a == "--"))
        .ok_or_else(|| {
            AgentError::SandboxUnavailable("the bubblewrap command has no `--`".into())
        })?;
    // `--as-pid-1`: the program is pid 1, so no bubblewrap init (which keeps the outer
    // environment in `/proc/1/environ`) lives inside the sandbox.
    let mut opts: Vec<String> = vec![
        "--cap-drop".into(),
        "ALL".into(),
        "--hostname".into(),
        "mrl-sandbox".into(),
        "--as-pid-1".into(),
    ];
    opts.extend(extra.iter().cloned());
    let filter_path = files.join("seccomp.bpf");
    write_filter(h.filter, &filter_path)?;
    let args_path = files.join("args");
    let mut nul = Vec::new();
    for o in &opts {
        if o.contains('\0') {
            return Err(AgentError::SandboxUnavailable(
                "a sandbox setting contains a NUL byte".into(),
            ));
        }
        nul.extend_from_slice(o.as_bytes());
        nul.push(0);
    }
    write_private(&args_path, &nul)?;
    let inject = [
        "--args".to_owned(),
        ARGS_FD.to_string(),
        "--seccomp".to_owned(),
        SECCOMP_FD.to_string(),
    ];
    let mut args = spec.args.clone();
    let tail = args.split_off(at);
    args.extend(inject);
    args.extend(tail);
    let bwrap = std::mem::take(&mut spec.program);
    let mut sh = vec!["-c".to_owned()];
    match nonce {
        None => sh.extend([
            r#"exec 3<"$1" 4<"$2" && rm -f "$2" && shift 2 && exec "$@""#.to_owned(),
            "mrl-sandbox".to_owned(),
            filter_path.display().to_string(),
            args_path.display().to_string(),
        ]),
        Some(n) => {
            let nonce_path = files.join("nonce");
            write_private(&nonce_path, n.as_bytes())?;
            sh.extend([
                r#"exec 3<"$1" 4<"$2" 5<"$3" && rm -f "$2" "$3" && shift 3 && exec "$@""#
                    .to_owned(),
                "mrl-sandbox".to_owned(),
                filter_path.display().to_string(),
                args_path.display().to_string(),
                nonce_path.display().to_string(),
            ]);
        }
    }
    sh.push(bwrap);
    sh.extend(args);
    spec.program = "/bin/sh".into();
    spec.args = sh;
    // Everything the sandbox gets is in the args file; the outer command needs none of it.
    spec.env.clear();
    Ok(spec)
}

/// The bubblewrap options that set the environment (`--clearenv`, then `--setenv` pairs).
pub fn env_options(env: &[(String, String)]) -> Vec<String> {
    let mut out = vec!["--clearenv".to_owned()];
    for (k, v) in env {
        out.extend(["--setenv".to_owned(), k.clone(), v.clone()]);
    }
    out
}

/// Wraps `spec` for the hardened profile `profile` with `backend`. Profiles that aren't
/// hardened go to `backend.wrap` unchanged. Without an isolating bubblewrap backend the
/// limits still apply and custom code stays refused ([`user_code_gate`]).
pub fn wrap(
    backend: &dyn SandboxBackend,
    profile: &SandboxProfile,
    mut spec: ExecSpec,
    allowed_paths: &[PathBuf],
) -> Result<ExecSpec> {
    let Some(h) = hardening(profile.name) else {
        return backend.wrap(profile, spec, allowed_paths);
    };
    spec.limits = effective_limits(&h, spec.limits);
    spec.gpus.clear();
    let isolating = backend.isolates() && backend.name() == "bubblewrap";
    if !isolating {
        spec.env.insert(PROFILE_ENV.into(), profile.name.into());
        spec.env.retain(|k, _| !k.starts_with("MRL_CREDENTIAL_"));
        return backend.wrap(profile, spec, allowed_paths);
    }
    let files = sandbox_dir(&spec);
    let mut env = sandbox_env(profile, &h, &spec);
    let mut extra = Vec::new();
    if profile.name == code_exec::NAME {
        let (vars, binds) = code_exec::jail_files(&spec)?;
        env.extend(vars);
        extra.extend(binds);
    }
    if cfg!(target_os = "linux") {
        env.push((USER_CODE_ENV.into(), "1".into()));
    }
    let mut opts = env_options(&env);
    opts.extend(extra);
    let wrapped = backend.wrap(profile, spec, allowed_paths)?;
    harden_bwrap(wrapped, &h, &opts, &files, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALLOW: u32 = RET_ALLOW;
    const KILL: u32 = RET_KILL_PROCESS;

    #[test]
    fn strict_filter_kills_escapes_and_allows_ordinary_calls() {
        for arch in [X86_64, AARCH64] {
            let p = program(Filter::Strict, &arch);
            assert!(p.len() < 255, "jumps must stay short");
            let ev = |nr, arg| evaluate(&p, arch.audit, nr, arg);
            for nr in arch.kill.iter().chain(arch.kill_strict) {
                assert_eq!(ev(*nr, 0), KILL, "{nr}");
            }
            // Ordinary calls: read/openat/write/exit_group (x86: 0, 257, 1, 231).
            let ordinary: &[u32] = if arch.audit == X86_64.audit {
                &[0, 257, 1, 231, 59]
            } else {
                &[63, 56, 64, 94, 221]
            };
            for nr in ordinary {
                assert_eq!(ev(*nr, 0), ALLOW, "{nr}");
            }
            assert_eq!(ev(arch.socket, AF_UNIX), ALLOW);
            assert_eq!(ev(arch.socket, 2), RET_ERRNO | EAFNOSUPPORT, "AF_INET");
            assert_eq!(ev(arch.socket, 10), RET_ERRNO | EAFNOSUPPORT, "AF_INET6");
            assert_eq!(ev(arch.socket, 17), RET_ERRNO | EAFNOSUPPORT, "AF_PACKET");
            assert_eq!(ev(arch.socketpair, 2), RET_ERRNO | EAFNOSUPPORT);
            assert_eq!(ev(arch.clone, 0x11), ALLOW, "plain fork");
            assert_eq!(ev(arch.clone, 0x1000_0000), KILL, "CLONE_NEWUSER");
            assert_eq!(ev(arch.clone, 0x4000_0000 | 0x11), KILL, "CLONE_NEWNET");
            assert_eq!(ev(arch.clone3, 0), RET_ERRNO | ENOSYS);
            assert_eq!(ev(425, 0), RET_ERRNO | ENOSYS, "io_uring");
            assert_eq!(
                evaluate(&p, 0x4000_0003, 0, 0),
                KILL,
                "another architecture"
            );
        }
        let p = program(Filter::Strict, &X86_64);
        assert_eq!(
            evaluate(&p, X86_64.audit, 0x4000_0000, 0),
            KILL,
            "x32 calls"
        );
    }

    #[test]
    fn supervisor_filter_lets_bubblewrap_make_jails_but_nothing_else() {
        for arch in [X86_64, AARCH64] {
            let p = program(Filter::Supervisor, &arch);
            for nr in arch.kill_strict {
                assert_eq!(evaluate(&p, arch.audit, *nr, 0), ALLOW, "{nr}");
            }
            assert_eq!(evaluate(&p, arch.audit, arch.clone, 0x1000_0000), ALLOW);
            for nr in arch.kill {
                assert_eq!(evaluate(&p, arch.audit, *nr, 0), KILL, "{nr}");
            }
            assert_eq!(
                evaluate(&p, arch.audit, arch.socket, 2),
                RET_ERRNO | EAFNOSUPPORT
            );
            // bubblewrap brings up a jail's loopback over NETLINK_ROUTE; nothing else.
            let netlink = |filter, protocol| {
                let p = program(filter, &arch);
                evaluate_with(&p, arch.audit, arch.socket, AF_NETLINK, protocol)
            };
            assert_eq!(netlink(Filter::Supervisor, NETLINK_ROUTE), ALLOW);
            assert_eq!(
                netlink(Filter::Supervisor, 9),
                RET_ERRNO | EAFNOSUPPORT,
                "audit"
            );
            assert_eq!(
                netlink(Filter::Strict, NETLINK_ROUTE),
                RET_ERRNO | EAFNOSUPPORT
            );
            assert_eq!(
                evaluate_with(&p, arch.audit, arch.socketpair, AF_NETLINK, NETLINK_ROUTE),
                RET_ERRNO | EAFNOSUPPORT
            );
            assert_eq!(evaluate(&p, arch.audit, arch.socket, AF_UNIX), ALLOW);
        }
    }

    #[test]
    fn the_network_filter_only_differs_in_sockets() {
        let p = program(Filter::StrictNetwork, &X86_64);
        assert_eq!(evaluate(&p, X86_64.audit, X86_64.socket, 2), ALLOW);
        assert_eq!(evaluate(&p, X86_64.audit, 101, 0), KILL, "ptrace");
        assert_eq!(evaluate(&p, X86_64.audit, X86_64.clone, 0x1000_0000), KILL);
    }

    #[test]
    fn program_bytes_are_sock_filter_records() {
        let p = program(Filter::Strict, &X86_64);
        let b = program_bytes(&p);
        assert_eq!(b.len(), p.len() * 8);
        assert_eq!(&b[..2], &BPF_LD_W_ABS.to_ne_bytes());
        assert_eq!(&b[4..8], &OFF_ARCH.to_ne_bytes());
    }

    #[test]
    fn limits_take_the_default_and_never_pass_the_cap() {
        let h = reward::hardening();
        let none = effective_limits(&h, Limits::default());
        assert_eq!(none, h.default_limits);
        let big = effective_limits(
            &h,
            Limits {
                cpu_millis: Some(64_000),
                memory_bytes: Some(1 << 40),
                pids: Some(1),
            },
        );
        assert_eq!(big.cpu_millis, h.max_limits.cpu_millis);
        assert_eq!(big.memory_bytes, h.max_limits.memory_bytes);
        assert_eq!(big.pids, Some(1), "asking for less is kept");
    }

    #[test]
    fn violations_are_sigsys_only() {
        let r = |code, signal, oom| ExitReport {
            code,
            signal,
            oom_killed: oom,
            limits: Default::default(),
            runtime_ms: 1,
        };
        assert!(matches!(
            classify(&r(None, Some(SIGSYS), false)),
            Ending::Violation(_)
        ));
        assert_eq!(classify(&r(None, Some(9), true)), Ending::OutOfMemory);
        assert_eq!(classify(&r(Some(0), None, false)), Ending::Ok);
        assert!(matches!(
            classify(&r(Some(2), None, false)),
            Ending::Failed(_)
        ));
        // Under bubblewrap the sandboxed program's signal death arrives as 128 + signal.
        assert!(matches!(
            classify(&r(Some(128 + SIGSYS), None, false)),
            Ending::Violation(_)
        ));
        assert!(matches!(
            classify(&r(Some(128 + 9), None, false)),
            Ending::Failed(_)
        ));
    }

    #[test]
    fn the_environment_keeps_only_the_allow_list_and_never_credentials() {
        let mut spec = ExecSpec::new("j", "prog", &[], Path::new("/s/j"));
        spec.env
            .insert("MRL_CREDENTIAL_HF".into(), "hf_secret".into());
        spec.env
            .insert("AWS_SECRET_ACCESS_KEY".into(), "nope".into());
        spec.env.insert("MRL_WORKER_NONCE".into(), "n".into());
        spec.env.insert("LANG".into(), "C.UTF-8".into());
        let env = sandbox_env(&reward::profile(), &reward::hardening(), &spec);
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"LANG"));
        assert!(!names
            .iter()
            .any(|n| n.starts_with("MRL_CREDENTIAL") || *n == "AWS_SECRET_ACCESS_KEY"));
        assert!(
            !names.contains(&"MRL_WORKER_NONCE"),
            "the reward service gets its nonce on an fd"
        );
        let env = sandbox_env(&code_exec::profile(), &code_exec::hardening(), &spec);
        assert!(
            env.iter().any(|(k, _)| k == "MRL_WORKER_NONCE"),
            "the preview worker talks to the agent"
        );
        assert!(!env.iter().any(|(k, _)| k.starts_with("MRL_CREDENTIAL")));
    }

    #[test]
    fn the_platform_gate_refuses_user_code_without_bubblewrap() {
        let e = user_code_gate(&super::super::NoSandbox).unwrap_err();
        assert_eq!(e.code(), USER_CODE_REFUSED);
        assert!(e.to_string().contains("bubblewrap"));
    }
}
