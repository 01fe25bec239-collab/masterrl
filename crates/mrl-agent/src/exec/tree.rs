//! Finds a job's processes where there are no cgroups (macOS, Windows, Linux without a
//! delegated cgroup). A process group misses children that start their own session
//! (`setsid`), and a child whose parent exits is re-parented to init and loses its parent
//! link, so two marks are used:
//!
//! - Every job gets a unique [`TOKEN_ENV`] value in its environment. Children inherit it
//!   through `fork`, `setsid` and re-parenting, so at kill time every process that still
//!   carries the token is found, however fast it detached. This needs the OS to show other
//!   processes' environments: Linux does (`/proc/<pid>/environ`); macOS does not, so there
//!   only the parent-link scan works and a process that detaches and loses its parent within
//!   one scan can outlive its job (reported as `LimitSupport::contained = false`).
//! - Parent links are also rescanned while the job runs, which catches a descendant that later
//!   clears its environment.
//!
//! Pids are remembered with their start time, so a reused pid is never signalled.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::sync::{Mutex, MutexGuard};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, Signal, System, UpdateKind};

/// Environment variable that marks every process of one job.
pub const TOKEN_ENV: &str = "MRL_JOB_TOKEN";

/// The processes of one job.
pub(crate) struct Tree {
    root: Pid,
    /// `MRL_JOB_TOKEN=<token>`, as it appears in a process environment.
    mark: OsString,
    sys: Mutex<Option<System>>,
    /// pid -> start time of every descendant seen so far.
    known: Mutex<HashMap<Pid, u64>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Tree {
    /// Tracks the job whose main process is `root` and whose environment has `token`.
    pub(crate) fn new(root: u32, token: &str) -> Self {
        Self {
            root: Pid::from_u32(root),
            mark: format!("{TOKEN_ENV}={token}").into(),
            sys: Mutex::new(System::new().ok()),
            known: Mutex::new(HashMap::new()),
        }
    }

    /// Refreshes the process list and adds every new descendant (by parent link) of the root
    /// or of a process already known. With `by_token`, also adds every process whose
    /// environment carries the job's token. Returns the live known processes.
    pub(crate) fn scan(&self, by_token: bool) -> Vec<(Pid, u64)> {
        let mut sys = lock(&self.sys);
        let Some(sys) = sys.as_mut() else {
            return Vec::new();
        };
        let kind = if by_token {
            ProcessRefreshKind::nothing().with_environ(UpdateKind::Always)
        } else {
            ProcessRefreshKind::nothing()
        };
        sys.refresh_processes_specifics(ProcessesToUpdate::All, true, kind);
        let procs = sys.processes();
        let me = Pid::from_u32(std::process::id());
        let mut known = lock(&self.known);
        known.retain(|pid, start| procs.get(pid).is_some_and(|p| p.start_time() == *start));
        if by_token {
            for (pid, p) in procs {
                if *pid != me && *pid != self.root && p.environ().contains(&self.mark) {
                    known.insert(*pid, p.start_time());
                }
            }
        }
        let mut children: HashMap<Pid, Vec<Pid>> = HashMap::new();
        for (pid, p) in procs {
            if let Some(parent) = p.parent() {
                children.entry(parent).or_default().push(*pid);
            }
        }
        let mut seen: HashSet<Pid> = known.keys().copied().collect();
        let mut stack: Vec<Pid> = std::iter::once(self.root)
            .chain(seen.iter().copied())
            .collect();
        while let Some(p) = stack.pop() {
            for c in children.get(&p).into_iter().flatten() {
                if *c != self.root && *c != me && seen.insert(*c) {
                    if let Some(proc_) = procs.get(c) {
                        known.insert(*c, proc_.start_time());
                    }
                    stack.push(*c);
                }
            }
        }
        known.iter().map(|(p, s)| (*p, *s)).collect()
    }

    /// Signals every process of the job except the root (after a full scan, tokens
    /// included). `force` = kill, else terminate (terminate is not available on Windows;
    /// those are left to the forced kill).
    pub(crate) fn signal(&self, force: bool) {
        let targets = self.scan(true);
        let sys = lock(&self.sys);
        let Some(sys) = sys.as_ref() else { return };
        for (pid, start) in targets {
            if let Some(p) = sys.process(pid).filter(|p| p.start_time() == start) {
                if force {
                    p.kill();
                } else {
                    let _ = p.kill_with(Signal::Term);
                }
            }
        }
    }
}
