//! Finds a job's descendants by parent links where there are no cgroups (macOS, Windows, Linux
//! without a delegated cgroup). A process group misses children that start their own session
//! (`setsid`), and a child whose parent exits is re-parented to init and loses its link, so the
//! tree is rescanned every second while the job runs and every process seen is remembered
//! (with its start time, so a reused pid is never signalled).

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, Signal, System};

/// The descendants of one root process.
pub(crate) struct Tree {
    root: Pid,
    sys: Mutex<Option<System>>,
    /// pid -> start time of every descendant seen so far.
    known: Mutex<HashMap<Pid, u64>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Tree {
    pub(crate) fn new(root: u32) -> Self {
        Self {
            root: Pid::from_u32(root),
            sys: Mutex::new(System::new().ok()),
            known: Mutex::new(HashMap::new()),
        }
    }

    /// Refreshes the process list and adds every new descendant of the root or of a process
    /// already known. Returns the live known descendants as (pid, start time).
    pub(crate) fn scan(&self) -> Vec<(Pid, u64)> {
        let mut sys = lock(&self.sys);
        let Some(sys) = sys.as_mut() else {
            return Vec::new();
        };
        sys.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing(),
        );
        let procs = sys.processes();
        let mut known = lock(&self.known);
        known.retain(|pid, start| procs.get(pid).is_some_and(|p| p.start_time() == *start));
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
                if *c != self.root && seen.insert(*c) {
                    if let Some(proc_) = procs.get(c) {
                        known.insert(*c, proc_.start_time());
                    }
                    stack.push(*c);
                }
            }
        }
        known.iter().map(|(p, s)| (*p, *s)).collect()
    }

    /// Signals every known descendant (after a fresh scan). `force` = kill, else terminate
    /// (terminate is not available on Windows; those are left to the forced kill).
    pub(crate) fn signal(&self, force: bool) {
        let targets = self.scan();
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
