//! The bubblewrap backend (Linux, unprivileged user namespaces).
//!
//! The sandbox gets new user, pid, network (unless the profile allows it), ipc, uts and cgroup
//! namespaces; dies with the agent; runs in its own session (no terminal injection); and sees
//! only: read-only system folders, read-only allowed node paths, the read-only work folder,
//! the writable staging folder, a private `/proc`, a minimal `/dev` (plus GPU device files
//! for profiles that allow GPUs) and an in-memory `/tmp`. The root is remounted read-only.

use std::path::{Path, PathBuf};

use super::{SandboxBackend, SandboxProfile, SYSTEM_PATHS};
use crate::exec::ExecSpec;
use crate::{AgentError, Result};

/// The bubblewrap backend.
#[derive(Debug, Clone)]
pub struct Bubblewrap {
    program: Option<PathBuf>,
}

/// Finds `name` on `PATH`.
pub(crate) fn which(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|d| d.join(name))
            .find(|p| p.is_file())
    })
}

/// GPU device files to expose: NVIDIA nodes, the WSL2 GPU (`/dev/dxg`) and DRM render nodes.
fn gpu_devices() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir("/dev")
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("nvidia") || n == "dxg")
                })
                .collect()
        })
        .unwrap_or_default();
    if Path::new("/dev/dri").is_dir() {
        out.push("/dev/dri".into());
    }
    out.sort();
    out
}

impl Bubblewrap {
    /// Uses `program` as `bwrap`, or finds it on `PATH`.
    pub fn new(program: Option<PathBuf>) -> Self {
        Self { program }
    }

    fn program(&self) -> Result<PathBuf> {
        self.program
            .clone()
            .or_else(|| which("bwrap"))
            .ok_or_else(|| {
                AgentError::SandboxUnavailable(
                    "bubblewrap (bwrap) is not installed or not on PATH".into(),
                )
            })
    }

    /// The `bwrap` arguments for `profile` around `spec`'s program (no `bwrap` itself).
    pub fn args(
        &self,
        profile: &SandboxProfile,
        spec: &ExecSpec,
        allowed_paths: &[PathBuf],
        gpu_devices: &[PathBuf],
    ) -> Vec<String> {
        let mut a: Vec<String> = Vec::new();
        let mut push = |items: &[&str]| a.extend(items.iter().map(|s| (*s).to_owned()));
        push(&["--die-with-parent", "--new-session", "--unshare-all"]);
        if profile.network {
            push(&["--share-net"]);
        }
        for p in SYSTEM_PATHS {
            push(&["--ro-bind-try", p, p]);
        }
        push(&["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]);
        if profile.gpu_devices {
            for d in gpu_devices {
                let d = d.display().to_string();
                push(&["--dev-bind-try", &d, &d]);
            }
            // WSL2 GPU user-space driver.
            push(&["--ro-bind-try", "/usr/lib/wsl", "/usr/lib/wsl"]);
        }
        for p in allowed_paths {
            let p = p.display().to_string();
            push(&["--ro-bind-try", &p, &p]);
        }
        let work = spec.work_dir.display().to_string();
        let staging = spec.staging_dir.display().to_string();
        push(&["--ro-bind", &work, &work, "--bind", &staging, &staging]);
        push(&["--chdir", &work, "--remount-ro", "/", "--", &spec.program]);
        a.extend(spec.args.iter().cloned());
        a
    }
}

impl SandboxBackend for Bubblewrap {
    fn name(&self) -> &'static str {
        "bubblewrap"
    }

    fn isolates(&self) -> bool {
        true
    }

    fn check(&self) -> Result<()> {
        if !cfg!(target_os = "linux") {
            return Err(AgentError::SandboxUnavailable(
                "bubblewrap runs only on Linux".into(),
            ));
        }
        let program = self.program()?;
        let out = std::process::Command::new(&program)
            .args([
                "--die-with-parent",
                "--unshare-all",
                "--ro-bind",
                "/",
                "/",
                "--",
                "true",
            ])
            .output()
            .map_err(|e| {
                AgentError::SandboxUnavailable(format!(
                    "{} could not start: {e}",
                    program.display()
                ))
            })?;
        if out.status.success() {
            Ok(())
        } else {
            Err(AgentError::SandboxUnavailable(format!(
                "bubblewrap could not create a sandbox ({}); this system may block unprivileged user namespaces",
                String::from_utf8_lossy(&out.stderr).trim()
            )))
        }
    }

    fn wrap(
        &self,
        profile: &SandboxProfile,
        spec: ExecSpec,
        allowed_paths: &[PathBuf],
    ) -> Result<ExecSpec> {
        let program = self.program()?;
        // bwrap binds the folders, so they must exist before it starts.
        for d in [&spec.work_dir, &spec.staging_dir] {
            std::fs::create_dir_all(d).map_err(|e| AgentError::state(d, e))?;
        }
        let args = self.args(profile, &spec, allowed_paths, &gpu_devices());
        Ok(ExecSpec {
            program: program.display().to_string(),
            args,
            ..spec
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::{data_prep, worker};
    use super::*;

    fn pos(args: &[String], needle: &[&str]) -> Option<usize> {
        args.windows(needle.len())
            .position(|w| w.iter().zip(needle).all(|(a, b)| a == b))
    }

    #[test]
    fn data_prep_gets_no_network_no_gpu_and_one_writable_folder() {
        let spec = ExecSpec::new("j", "python3", &["prep.py", "--fast"], Path::new("/s/j"));
        let gpus = vec![PathBuf::from("/dev/nvidia0")];
        let b = Bubblewrap::new(Some("/usr/bin/bwrap".into()));
        let a = b.args(&data_prep(), &spec, &["/data/sets".into()], &gpus);
        assert!(pos(&a, &["--unshare-all"]).is_some());
        assert!(pos(&a, &["--share-net"]).is_none());
        assert!(
            pos(&a, &["--dev-bind-try"]).is_none(),
            "no GPUs for data_prep"
        );
        assert!(pos(&a, &["--ro-bind-try", "/data/sets", "/data/sets"]).is_some());
        assert!(pos(&a, &["--ro-bind", "/s/j/work", "/s/j/work"]).is_some());
        // The only writable bind is staging, and it comes after the /tmp tmpfs.
        let binds: Vec<_> = a.iter().filter(|x| *x == "--bind").collect();
        assert_eq!(binds.len(), 1);
        let staging = pos(&a, &["--bind", "/s/j/staging", "/s/j/staging"]).unwrap();
        assert!(staging > pos(&a, &["--tmpfs", "/tmp"]).unwrap());
        // The root is read-only and the real command follows `--` unchanged.
        let dashes = pos(&a, &["--remount-ro", "/", "--"]).unwrap();
        assert_eq!(&a[dashes + 3..], ["python3", "prep.py", "--fast"]);
    }

    #[test]
    fn worker_sees_gpu_device_files() {
        let spec = ExecSpec::new("j", "train", &[], Path::new("/s/j"));
        let gpus = vec![
            PathBuf::from("/dev/nvidia0"),
            PathBuf::from("/dev/nvidiactl"),
        ];
        let a = Bubblewrap::new(None).args(&worker(), &spec, &[], &gpus);
        assert!(pos(&a, &["--dev-bind-try", "/dev/nvidia0", "/dev/nvidia0"]).is_some());
        assert!(pos(&a, &["--dev-bind-try", "/dev/nvidiactl", "/dev/nvidiactl"]).is_some());
        assert!(pos(&a, &["--share-net"]).is_none());
    }

    #[test]
    fn missing_bwrap_is_reported() {
        let b = Bubblewrap::new(Some("/no/such/bwrap".into()));
        let e = b.check().unwrap_err();
        assert_eq!(e.code(), "MRL-ENV-122");
    }
}
