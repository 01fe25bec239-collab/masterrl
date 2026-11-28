//! The Tier-2 `code_exec` profile: the supervisor sandbox for rewards that run model-written
//! code, and for REWARD_PREVIEW jobs (which test and preview a reward in the sandbox tier it
//! will use).
//!
//! It is the `reward` profile (no network, no GPU, no secrets, 512 MiB in-memory `/tmp`,
//! 2 CPUs / 4 GiB / 256 processes by default), except that its seccomp filter is
//! [`Filter::Supervisor`]: it lets bubblewrap start **one disposable jail per execution**
//! (`mrl_rewards.code.jail`). Each jail gets a fresh 64 MiB in-memory work folder, the
//! system and Python folders read-only, its code and tests read-only outside the writable
//! area, 1 CPU's worth of CPU time (5 s at most), 512 MiB, 64 processes, 10 s wall clock and
//! 64 KiB of output, and the [`Filter::Strict`] filter, which the agent writes for it.
//!
//! The filters for jails are written to the job's private `sandbox/jail/` folder and bound
//! read-only at `/run/mrl-jail` inside: `MRL_JAIL_SECCOMP` (strict: Tier-2 jails and
//! Tier-1 jails for custom code) and `MRL_SUPERVISOR_SECCOMP` (a reward service that must
//! itself start Tier-2 jails).

use super::{sandbox_dir, write_filter, Filter, Hardening};
use crate::exec::{ExecSpec, Limits};
use crate::sandbox::SandboxProfile;
use crate::Result;

/// Profile name.
pub const NAME: &str = "code_exec";
/// Where the jail filters appear inside the sandbox.
pub const JAIL_DIR: &str = "/run/mrl-jail";
/// Names the strict filter for jails.
pub const JAIL_ENV: &str = "MRL_JAIL_SECCOMP";
/// Names the supervisor filter.
pub const SUPERVISOR_ENV: &str = "MRL_SUPERVISOR_SECCOMP";
/// Environment the REWARD_PREVIEW worker needs besides the base list: its session with the
/// agent and offline model loading.
pub const ENV: &[&str] = &[
    "MRL_WORKER_NONCE",
    "MRL_ENVIRONMENT_DIGEST",
    "MRL_ROLE_ID",
    "MRL_ATTEMPT_ID",
    "HF_HUB_OFFLINE",
    "HF_DATASETS_OFFLINE",
    "TRANSFORMERS_OFFLINE",
    "OMP_NUM_THREADS",
];

/// Tier 2 supervisor: no network, no GPU, no secrets; may start disposable jails.
pub fn profile() -> SandboxProfile {
    SandboxProfile {
        name: NAME,
        description: "reward code that runs model-written code (Tier 2): no network, no GPU, no secrets; one disposable jail per execution",
        network: false,
        gpu_devices: false,
    }
}

/// Same limits as `reward`, with the supervisor filter.
pub fn hardening() -> Hardening {
    Hardening {
        filter: Filter::Supervisor,
        env: ENV,
        ..super::reward::hardening()
    }
}

/// The limits of one jail (enforced inside the supervisor with rlimits; the jail's processes
/// also count against the supervisor's cgroup).
pub const JAIL_LIMITS: Limits = Limits {
    cpu_millis: Some(1000),
    memory_bytes: Some(512 << 20),
    pids: Some(64),
};

/// Environment variables and bubblewrap options that make the jail filters visible.
pub type JailFiles = (Vec<(String, String)>, Vec<String>);

/// Writes the jail filters for `spec` and returns the environment and bind options that
/// make them visible inside.
pub fn jail_files(spec: &ExecSpec) -> Result<JailFiles> {
    jail_files_in(&sandbox_dir(spec).join("jail"))
}

/// [`jail_files`] with the filters written to `dir` (a private folder that is never mounted
/// writable).
pub fn jail_files_in(dir: &std::path::Path) -> Result<JailFiles> {
    write_filter(Filter::Strict, &dir.join("strict.bpf"))?;
    write_filter(Filter::Supervisor, &dir.join("supervisor.bpf"))?;
    let env = vec![
        (JAIL_ENV.to_owned(), format!("{JAIL_DIR}/strict.bpf")),
        (
            SUPERVISOR_ENV.to_owned(),
            format!("{JAIL_DIR}/supervisor.bpf"),
        ),
    ];
    let binds = vec![
        "--ro-bind".to_owned(),
        dir.display().to_string(),
        JAIL_DIR.to_owned(),
    ];
    Ok((env, binds))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::sandbox::bwrap::Bubblewrap;
    use std::path::Path;

    #[test]
    fn the_supervisor_gets_jail_filters_and_keeps_its_worker_session() {
        let dir = tempfile::tempdir().unwrap();
        let mut spec = ExecSpec::new("w", "/bin/sh", &["-c", "true"], dir.path());
        spec.env.insert("MRL_WORKER_NONCE".into(), "n".into());
        spec.env.insert("MRL_CREDENTIAL_HF".into(), "hf_x".into());
        let backend = Bubblewrap::new(Some("/usr/bin/bwrap".into()));
        let s = super::super::wrap(
            &backend,
            &profile(),
            spec,
            &[Path::new("/env").to_path_buf()],
        )
        .unwrap();
        assert_eq!(s.program, "/bin/sh");
        assert!(s.limits.memory_bytes.is_some());
        let files = dir.path().join("sandbox");
        let extra = std::fs::read(files.join("args")).unwrap();
        let text = String::from_utf8_lossy(&extra).replace('\0', " ");
        assert!(
            text.contains("MRL_JAIL_SECCOMP /run/mrl-jail/strict.bpf"),
            "{text}"
        );
        assert!(text.contains("MRL_WORKER_NONCE n"));
        // Nothing of the outer environment is left where the sandbox could read it.
        assert!(text.contains("--as-pid-1"), "{text}");
        assert!(s.env.is_empty(), "{:?}", s.env);
        assert!(!text.contains("hf_x"), "no secrets: {text}");
        assert!(
            files.join("jail/strict.bpf").is_file() && files.join("jail/supervisor.bpf").is_file()
        );
        let bpf = std::fs::read(files.join("seccomp.bpf")).unwrap();
        let strict = std::fs::read(files.join("jail/strict.bpf")).unwrap();
        assert_ne!(
            bpf, strict,
            "the supervisor runs under the supervisor filter"
        );
        // The args file is read before `--remount-ro /`, so its bind can make a mount point.
        let pos = |n: &str| s.args.iter().position(|a| a == n).unwrap();
        assert!(pos("--args") < pos("--remount-ro"));
    }
}
