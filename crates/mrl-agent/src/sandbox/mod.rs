//! Sandboxes for job programs.
//!
//! A [`SandboxProfile`] says what a program may reach (network, GPUs); a [`SandboxBackend`]
//! enforces it by rewriting an [`ExecSpec`] (for bubblewrap: the program becomes `bwrap` with
//! the real program after `--`). Every profile gets a read-only root, read-only system folders
//! and allowed node paths, a read-only work folder, and one writable folder: the staging
//! folder (plus an in-memory `/tmp` that vanishes with the sandbox).
//!
//! Registries: [`profiles`] (P29 adds `reward`/`code_exec`) and [`backends`] (P49 adds gVisor
//! and microVM backends). Each part adds one line inside its `// <mrl:register>` block.

pub mod bwrap;
pub mod profiles;

use std::path::PathBuf;

use serde::Serialize;

use crate::config::{SandboxChoice, SandboxConfig};
use crate::exec::ExecSpec;
use crate::{AgentError, Result};

/// System folders bound read-only into every sandbox (missing ones are skipped).
pub const SYSTEM_PATHS: &[&str] = &[
    "/usr", "/bin", "/sbin", "/lib", "/lib32", "/lib64", "/etc", "/opt",
];

/// What a sandboxed program may reach.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SandboxProfile {
    /// Name, like `worker`.
    pub name: &'static str,
    /// What it is for.
    pub description: &'static str,
    /// The program may use the network.
    pub network: bool,
    /// GPU device files are visible.
    pub gpu_devices: bool,
}

/// Training and inference workers: GPUs, no network, writes only to staging.
pub fn worker() -> SandboxProfile {
    SandboxProfile {
        name: "worker",
        description: "training and inference workers: GPUs, no network, writes only to staging",
        network: false,
        gpu_devices: true,
    }
}

/// Dataset preparation: no GPUs, no network, writes only to staging.
pub fn data_prep() -> SandboxProfile {
    SandboxProfile {
        name: "data_prep",
        description: "dataset preparation: no GPUs, no network, writes only to staging",
        network: false,
        gpu_devices: false,
    }
}

/// Downloads and uploads (Hub imports, exports): network, no GPUs, writes only to staging.
pub fn fetch() -> SandboxProfile {
    SandboxProfile {
        name: "fetch",
        description: "downloads and uploads: network, no GPUs, writes only to staging",
        network: true,
        gpu_devices: false,
    }
}

/// Every registered sandbox profile.
pub fn profiles() -> Vec<SandboxProfile> {
    vec![
        // <mrl:register>
        worker(),
        data_prep(),
        fetch(),
        profiles::reward::profile(),
        profiles::code_exec::profile(),
        profiles::command::profile(),
        profiles::command::profile_net(),
        // </mrl:register>
    ]
}

/// The profile called `name`.
pub fn profile(name: &str) -> Result<SandboxProfile> {
    let all = profiles();
    let known = all.iter().map(|p| p.name).collect::<Vec<_>>().join(", ");
    all.into_iter()
        .find(|p| p.name == name)
        .ok_or_else(|| AgentError::UnknownName {
            kind: "sandbox profile",
            name: name.to_owned(),
            known,
        })
}

/// Enforces sandbox profiles.
pub trait SandboxBackend: Send + Sync {
    /// Name, like `bubblewrap`.
    fn name(&self) -> &'static str;
    /// False for the pass-through backend.
    fn isolates(&self) -> bool;
    /// Fails with [`AgentError::SandboxUnavailable`] if it can't run on this computer.
    fn check(&self) -> Result<()>;
    /// Rewrites `spec` so it runs inside the sandbox. `allowed_paths` are node paths the
    /// program may read.
    fn wrap(
        &self,
        profile: &SandboxProfile,
        spec: ExecSpec,
        allowed_paths: &[PathBuf],
    ) -> Result<ExecSpec>;
}

/// The pass-through backend: no isolation. Selected with `sandbox.backend = "none"`, and by
/// `auto` on systems with no sandbox backend yet (macOS, Windows).
#[derive(Debug, Default)]
pub struct NoSandbox;

impl SandboxBackend for NoSandbox {
    fn name(&self) -> &'static str {
        "none"
    }
    fn isolates(&self) -> bool {
        false
    }
    fn check(&self) -> Result<()> {
        Ok(())
    }
    fn wrap(&self, _: &SandboxProfile, spec: ExecSpec, _: &[PathBuf]) -> Result<ExecSpec> {
        Ok(spec)
    }
}

/// Every registered isolating backend.
pub fn backends(config: &SandboxConfig) -> Vec<Box<dyn SandboxBackend>> {
    vec![
        // <mrl:register>
        Box::new(bwrap::Bubblewrap::new(config.bwrap_path.clone())),
        // </mrl:register>
    ]
}

/// The backend `config` selects, checked to work here.
pub fn select(config: &SandboxConfig) -> Result<Box<dyn SandboxBackend>> {
    let name = match config.backend {
        SandboxChoice::None => return Ok(Box::new(NoSandbox)),
        SandboxChoice::Auto if !cfg!(target_os = "linux") => return Ok(Box::new(NoSandbox)),
        SandboxChoice::Auto | SandboxChoice::Bubblewrap => "bubblewrap",
    };
    let all = backends(config);
    let known = all.iter().map(|b| b.name()).collect::<Vec<_>>().join(", ");
    let backend =
        all.into_iter()
            .find(|b| b.name() == name)
            .ok_or_else(|| AgentError::UnknownName {
                kind: "sandbox backend",
                name: name.to_owned(),
                known,
            })?;
    backend.check()?;
    Ok(backend)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_are_registered() {
        assert_eq!(profile("worker").unwrap(), worker());
        assert!(!profile("data_prep").unwrap().gpu_devices);
        let e = profile("no_such_profile").unwrap_err();
        assert_eq!(e.code(), "MRL-VAL-126");
        assert!(e.to_string().contains("worker, data_prep"));
    }

    #[test]
    fn none_passes_through_and_auto_matches_the_os() {
        let cfg = SandboxConfig {
            backend: SandboxChoice::None,
            bwrap_path: None,
        };
        let b = select(&cfg).unwrap();
        assert!(!b.isolates());
        let spec = ExecSpec::new("j", "prog", &["a"], std::path::Path::new("/tmp/j"));
        assert_eq!(b.wrap(&worker(), spec.clone(), &[]).unwrap(), spec);
        if !cfg!(target_os = "linux") {
            assert_eq!(select(&SandboxConfig::default()).unwrap().name(), "none");
            let e = select(&SandboxConfig {
                backend: SandboxChoice::Bubblewrap,
                bwrap_path: None,
            })
            .err()
            .unwrap();
            assert_eq!(e.code(), "MRL-ENV-122");
        }
    }
}
