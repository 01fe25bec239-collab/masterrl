//! cgroup v2 job cgroups (Linux).
//!
//! The agent needs a cgroup it may write: the systemd user unit sets `Delegate=yes`, so the
//! service's own cgroup works (and is used only when the agent runs in `mrl-agent.service`).
//! Because cgroup v2 forbids processes in a cgroup that hands out controllers, the agent runs
//! in an `agent` leaf (systemd starts it there with `DelegateSubgroup=agent`; otherwise it
//! moves itself), then enables `cpu`, `memory` and `pids` for the service cgroup's children
//! and puts each job in `job-<name>`. A configured `exec.cgroup_root` must already be empty
//! of processes.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::{LimitSupport, Limits};

const MOUNT: &str = "/sys/fs/cgroup";

/// The writable cgroup the agent puts jobs under.
#[derive(Debug)]
pub(crate) struct Cgroups {
    root: PathBuf,
    support: LimitSupport,
}

fn read(p: &Path) -> std::io::Result<String> {
    std::fs::read_to_string(p)
}

fn write(p: &Path, text: &str) -> std::io::Result<()> {
    std::fs::write(p, text)
}

/// The cgroup this process is in, from `/proc/self/cgroup` (`0::/path`).
fn own_cgroup() -> Option<PathBuf> {
    let text = read(Path::new("/proc/self/cgroup")).ok()?;
    let rel = text.lines().find_map(|l| l.strip_prefix("0::"))?;
    Some(Path::new(MOUNT).join(rel.trim().trim_start_matches('/')))
}

/// The service cgroup jobs go under: the parent when systemd started the agent in its
/// `agent` leaf (`DelegateSubgroup=agent`), else the agent's own cgroup.
fn job_root(own: PathBuf) -> PathBuf {
    let in_leaf = own.file_name().is_some_and(|n| n == "agent")
        && own
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|n| n == crate::service::SYSTEMD_UNIT);
    match own.parent() {
        Some(unit) if in_leaf => unit.to_path_buf(),
        _ => own,
    }
}

impl Cgroups {
    /// Sets up the job root. `Err` explains, in plain words, why limits are unavailable.
    pub(crate) fn open(configured: Option<&Path>) -> Result<Self, String> {
        if !Path::new(MOUNT).join("cgroup.controllers").exists() {
            return Err("cgroup v2 is not mounted at /sys/fs/cgroup".into());
        }
        let (root, own) = match configured {
            Some(p) => (p.to_path_buf(), false),
            None => {
                let own = own_cgroup().ok_or("this process's cgroup could not be found")?;
                // Only claim our own cgroup inside the installed unit, which delegates it.
                // Anywhere else (a terminal, a test run) other processes share it.
                let in_unit = own
                    .components()
                    .any(|c| c.as_os_str() == crate::service::SYSTEMD_UNIT);
                if !in_unit {
                    return Err(format!(
                        "limits need the agent to run as its installed service ({}) or exec.cgroup_root to be set",
                        crate::service::SYSTEMD_UNIT
                    ));
                }
                (job_root(own), true)
            }
        };
        let has_procs = read(&root.join("cgroup.procs"))
            .map_err(|e| format!("{} can't be read: {e}", root.display()))?
            .lines()
            .any(|l| !l.trim().is_empty());
        if has_procs {
            if !own {
                return Err(format!(
                    "{} still has processes; a job root must be empty",
                    root.display()
                ));
            }
            let leaf = root.join("agent");
            std::fs::create_dir_all(&leaf)
                .and_then(|()| write(&leaf.join("cgroup.procs"), &std::process::id().to_string()))
                .map_err(|e| {
                    format!(
                        "the agent can't manage cgroup {} ({e}); run it as the installed service, which delegates its cgroup",
                        root.display()
                    )
                })?;
        }
        let available = read(&root.join("cgroup.controllers")).unwrap_or_default();
        let wanted: Vec<&str> = ["cpu", "memory", "pids"]
            .into_iter()
            .filter(|c| available.split_whitespace().any(|a| a == *c))
            .collect();
        if !wanted.is_empty() {
            let line: Vec<String> = wanted.iter().map(|c| format!("+{c}")).collect();
            write(&root.join("cgroup.subtree_control"), &line.join(" "))
                .map_err(|e| format!("controllers can't be enabled in {}: {e}", root.display()))?;
        }
        let enabled = read(&root.join("cgroup.subtree_control")).unwrap_or_default();
        let on = |c: &str| enabled.split_whitespace().any(|e| e == c);
        let missing: Vec<&str> = ["cpu", "memory", "pids"]
            .into_iter()
            .filter(|c| !on(c))
            .collect();
        let support = LimitSupport {
            cpu: on("cpu"),
            memory: on("memory"),
            pids: on("pids"),
            oom_detection: on("memory"),
            contained: true,
            note: (!missing.is_empty())
                .then(|| format!("these cgroup controllers are off: {}", missing.join(", "))),
        };
        Ok(Self { root, support })
    }

    pub(crate) fn support(&self) -> &LimitSupport {
        &self.support
    }

    /// Creates the job's cgroup with its limits. A leftover empty cgroup of the same name
    /// (from a crash) is replaced.
    pub(crate) fn create(&self, name: &str, limits: &Limits) -> std::io::Result<JobCgroup> {
        let path = self.root.join(format!("job-{name}"));
        if path.exists() {
            let _ = std::fs::remove_dir(&path);
        }
        std::fs::create_dir(&path)?;
        let job = JobCgroup { path };
        if let Err(e) = job.apply(limits, &self.support) {
            job.remove();
            return Err(e);
        }
        Ok(job)
    }
}

/// One job's cgroup.
#[derive(Debug)]
pub(crate) struct JobCgroup {
    path: PathBuf,
}

impl JobCgroup {
    fn apply(&self, limits: &Limits, s: &LimitSupport) -> std::io::Result<()> {
        if s.memory {
            if let Some(m) = limits.memory_bytes {
                write(&self.path.join("memory.max"), &m.to_string())?;
                // No swap, so the limit is real; one OOM kill ends the whole job.
                let _ = write(&self.path.join("memory.swap.max"), "0");
            }
            write(&self.path.join("memory.oom.group"), "1")?;
        }
        if s.cpu {
            if let Some(c) = limits.cpu_millis {
                // Quota per 100 ms period: 1000 millis (one core) = 100000 µs.
                write(
                    &self.path.join("cpu.max"),
                    &format!("{} 100000", (c * 100).max(1000)),
                )?;
            }
        }
        if s.pids {
            if let Some(p) = limits.pids {
                write(&self.path.join("pids.max"), &p.to_string())?;
            }
        }
        Ok(())
    }

    /// The file a process writes its pid to, to join.
    pub(crate) fn procs_file(&self) -> PathBuf {
        self.path.join("cgroup.procs")
    }

    /// OOM kills so far (`memory.events`).
    pub(crate) fn oom_kills(&self) -> u64 {
        read(&self.path.join("memory.events"))
            .unwrap_or_default()
            .lines()
            .find_map(|l| l.strip_prefix("oom_kill "))
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or(0)
    }

    pub(crate) fn pids(&self) -> Vec<i32> {
        read(&self.procs_file())
            .unwrap_or_default()
            .lines()
            .filter_map(|l| l.trim().parse().ok())
            .collect()
    }

    fn populated(&self) -> bool {
        read(&self.path.join("cgroup.events"))
            .map(|t| t.lines().any(|l| l.trim() == "populated 1"))
            .unwrap_or(false)
    }

    /// Sends `sig` to every process in the cgroup.
    pub(crate) fn signal(&self, sig: nix::sys::signal::Signal) {
        for pid in self.pids() {
            let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), sig);
        }
    }

    /// Kills every process in the cgroup, including ones that left the process group.
    pub(crate) fn kill(&self) {
        if write(&self.path.join("cgroup.kill"), "1").is_ok() {
            return;
        }
        // Kernels before 5.14 have no cgroup.kill: kill until empty (new forks get caught too).
        for _ in 0..50 {
            if self.pids().is_empty() {
                return;
            }
            self.signal(nix::sys::signal::Signal::SIGKILL);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Kills what is left, waits up to 2 s for the cgroup to empty, then removes it.
    pub(crate) fn remove(&self) {
        self.kill();
        for _ in 0..200 {
            if !self.populated() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = std::fs::remove_dir(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jobs_go_next_to_the_agent_leaf_systemd_started() {
        let unit = Path::new("/sys/fs/cgroup/user.slice/app.slice/mrl-agent.service");
        assert_eq!(job_root(unit.join("agent")), unit);
        // Not in the unit's leaf: its own cgroup.
        assert_eq!(job_root(unit.to_path_buf()), unit);
        let other = Path::new("/sys/fs/cgroup/user.slice/other.service/agent");
        assert_eq!(job_root(other.to_path_buf()), other);
    }
}
