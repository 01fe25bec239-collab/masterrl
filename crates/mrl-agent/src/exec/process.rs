//! The process executor: plain OS processes, one cgroup each on Linux.
//!
//! - The environment is cleared, then filled from the allow-list (`exec.env_allow`), the
//!   spec's own variables, `CUDA_VISIBLE_DEVICES`, `MRL_STAGING_DIR` and `TMPDIR`.
//! - Linux: the job starts as `sh -c 'echo $$ > <cgroup>/cgroup.procs && exec "$@"'`, so it is
//!   inside its cgroup before the real program runs (no window to fork outside it).
//! - Unix: each job leads its own process group, so the tree can be signalled without cgroups.
//!   Windows: the tree is ended with `taskkill /T`.
//! - stdout and stderr go to rotating files in the spec's `log_dir`.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use super::logs::RotatingLog;
use super::{ExecSpec, Execution, Executor, ExitReport, LimitSupport};
use crate::config::ExecConfig;
use crate::{AgentError, Result};

/// The process executor.
#[derive(Debug)]
pub struct ProcessExecutor {
    env_allow: Vec<String>,
    log_max_bytes: u64,
    log_keep: u32,
    #[cfg(target_os = "linux")]
    cgroups: Result<super::cgroup::Cgroups, String>,
}

impl ProcessExecutor {
    /// Sets up the executor. On Linux it claims the cgroup root; if that is not possible,
    /// jobs still run and [`Self::limit_support`] says why limits are off.
    pub fn new(config: &ExecConfig) -> Result<Self> {
        Ok(Self {
            env_allow: config.env_allow.clone(),
            log_max_bytes: config.log_max_bytes,
            log_keep: config.log_keep,
            #[cfg(target_os = "linux")]
            cgroups: super::cgroup::Cgroups::open(config.cgroup_root.as_deref()),
        })
    }

    fn env_for(&self, spec: &ExecSpec) -> Vec<(String, String)> {
        let mut env: std::collections::BTreeMap<String, String> = self
            .env_allow
            .iter()
            .filter_map(|k| std::env::var(k).ok().map(|v| (k.clone(), v)))
            .collect();
        env.extend(spec.env.clone());
        env.insert("CUDA_VISIBLE_DEVICES".into(), spec.gpus.join(","));
        env.insert(
            "MRL_STAGING_DIR".into(),
            spec.staging_dir.display().to_string(),
        );
        env.insert(
            "TMPDIR".into(),
            spec.staging_dir.join("tmp").display().to_string(),
        );
        env.into_iter().collect()
    }
}

fn check_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && !name.starts_with('.');
    if ok {
        Ok(())
    } else {
        Err(AgentError::UnknownName {
            kind: "job name",
            name: name.to_owned(),
            known: "1-128 letters, digits, '.', '_' or '-', not starting with '.'".into(),
        })
    }
}

fn mkdir(p: &Path) -> Result<()> {
    std::fs::create_dir_all(p).map_err(|e| AgentError::state(p, e))
}

#[async_trait::async_trait]
impl Executor for ProcessExecutor {
    fn limit_support(&self) -> LimitSupport {
        #[cfg(target_os = "linux")]
        {
            match &self.cgroups {
                Ok(c) => c.support().clone(),
                Err(reason) => LimitSupport {
                    note: Some(reason.clone()),
                    ..LimitSupport::default()
                },
            }
        }
        #[cfg(not(target_os = "linux"))]
        LimitSupport {
            note: Some(
                "this OS has no cgroups, so CPU, memory and process limits are not enforced".into(),
            ),
            ..LimitSupport::default()
        }
    }

    async fn spawn(&self, spec: ExecSpec) -> Result<Box<dyn Execution>> {
        check_name(&spec.name)?;
        // Checked here so a missing program fails the same way with or without the cgroup
        // wrapper (which would otherwise report it as exit code 127).
        let program = Path::new(&spec.program);
        if program.components().count() > 1 && !program.is_file() {
            return Err(AgentError::exec(
                &spec.program,
                "the program file does not exist",
            ));
        }
        for d in [
            &spec.work_dir,
            &spec.staging_dir,
            &spec.staging_dir.join("tmp"),
            &spec.log_dir,
        ] {
            mkdir(d)?;
        }
        let support = self.limit_support();
        // Logs are opened before anything starts, so a log failure never leaves a job running.
        let open = |stream: &str| {
            RotatingLog::open(
                spec.log_dir.join(format!("{stream}.log")),
                self.log_max_bytes,
                self.log_keep,
            )
        };
        let log_err = |e| AgentError::state(&spec.log_dir, e);
        let out_log = open("stdout").await.map_err(log_err)?;
        let err_log = open("stderr").await.map_err(log_err)?;

        #[cfg(target_os = "linux")]
        let cgroup = match &self.cgroups {
            Ok(c) => Some(c.create(&spec.name, &spec.limits).map_err(|e| {
                AgentError::exec(
                    &spec.program,
                    format!("its cgroup could not be set up ({e})"),
                )
            })?),
            Err(_) => None,
        };
        #[cfg(target_os = "linux")]
        let has_cgroup = cgroup.is_some();
        #[cfg(not(target_os = "linux"))]
        let has_cgroup = false;

        #[cfg(target_os = "linux")]
        let mut cmd = match &cgroup {
            Some(cg) => {
                let mut c = tokio::process::Command::new("/bin/sh");
                c.arg("-c")
                    .arg(r#"echo $$ > "$0" && exec "$@""#)
                    .arg(cg.procs_file())
                    .arg(&spec.program);
                c
            }
            None => tokio::process::Command::new(&spec.program),
        };
        #[cfg(not(target_os = "linux"))]
        let mut cmd = tokio::process::Command::new(&spec.program);

        cmd.args(&spec.args)
            .env_clear()
            .envs(self.env_for(&spec))
            .current_dir(&spec.work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        cmd.process_group(0);

        let mut child = cmd.spawn().map_err(|e| {
            #[cfg(target_os = "linux")]
            if let Some(cg) = &cgroup {
                cg.remove();
            }
            AgentError::exec(&spec.program, e)
        })?;
        // Nothing below can fail: the job is running and owned by the execution.
        let mut pumps = Vec::new();
        if let Some(out) = child.stdout.take() {
            pumps.push(tokio::spawn(out_log.pump(out)));
        }
        if let Some(err) = child.stderr.take() {
            pumps.push(tokio::spawn(err_log.pump(err)));
        }
        let pid = child.id();
        // Without a cgroup, descendants are found by parent links (see `tree`).
        let tree = pid
            .filter(|_| !has_cgroup)
            .map(|p| Arc::new(super::tree::Tree::new(p)));
        let tracker = tree.clone().map(|t| {
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(TREE_SCAN);
                loop {
                    tick.tick().await;
                    let t = Arc::clone(&t);
                    if tokio::task::spawn_blocking(move || t.scan()).await.is_err() {
                        return;
                    }
                }
            })
        });
        Ok(Box::new(ProcessExecution {
            program: spec.program,
            child,
            pid,
            pumps,
            started: Instant::now(),
            limits: if spec.limits.is_empty() {
                LimitSupport {
                    note: support.note.clone(),
                    oom_detection: support.oom_detection,
                    ..LimitSupport::default()
                }
            } else {
                support
            },
            report: None,
            tree,
            tracker,
            #[cfg(target_os = "linux")]
            cgroup,
        }))
    }
}

/// How often the process tree is rescanned when there is no cgroup.
const TREE_SCAN: Duration = Duration::from_millis(500);

/// A running process tree.
struct ProcessExecution {
    program: String,
    child: tokio::process::Child,
    pid: Option<u32>,
    pumps: Vec<JoinHandle<std::io::Result<()>>>,
    started: Instant,
    limits: LimitSupport,
    report: Option<ExitReport>,
    tree: Option<Arc<super::tree::Tree>>,
    tracker: Option<JoinHandle<()>>,
    #[cfg(target_os = "linux")]
    cgroup: Option<super::cgroup::JobCgroup>,
}

impl ProcessExecution {
    /// Signals the whole tree. `force` = SIGKILL, else SIGTERM.
    fn signal_tree(&self, force: bool) {
        #[cfg(target_os = "linux")]
        if let Some(cg) = &self.cgroup {
            if force {
                cg.kill();
            } else {
                cg.signal(nix::sys::signal::Signal::SIGTERM);
            }
            return;
        }
        // Descendants that left the process group (setsid) or lost their parent.
        if let Some(tree) = &self.tree {
            tree.signal(force);
        }
        #[cfg(unix)]
        if let Some(pid) = self.pid {
            use nix::sys::signal::{killpg, Signal};
            let sig = if force {
                Signal::SIGKILL
            } else {
                Signal::SIGTERM
            };
            let _ = killpg(nix::unistd::Pid::from_raw(pid as i32), sig);
        }
        #[cfg(windows)]
        if let Some(pid) = self.pid {
            let mut c = std::process::Command::new("taskkill");
            c.args(["/T", "/PID", &pid.to_string()]);
            if force {
                c.arg("/F");
            }
            let _ = c.stdout(Stdio::null()).stderr(Stdio::null()).status();
        }
    }
}

#[async_trait::async_trait]
impl Execution for ProcessExecution {
    fn pid(&self) -> Option<u32> {
        self.pid
    }

    async fn wait(&mut self) -> Result<ExitReport> {
        if let Some(r) = &self.report {
            return Ok(r.clone());
        }
        let status = self
            .child
            .wait()
            .await
            .map_err(|e| AgentError::exec(&self.program, e))?;
        #[cfg(target_os = "linux")]
        let oom_killed = self.cgroup.as_ref().is_some_and(|cg| cg.oom_kills() > 0);
        #[cfg(not(target_os = "linux"))]
        let oom_killed = false;
        // The main process is gone: end whatever it left running, so the logs reach EOF.
        self.signal_tree(true);
        if let Some(t) = self.tracker.take() {
            t.abort();
        }
        #[cfg(target_os = "linux")]
        if let Some(cg) = self.cgroup.take() {
            tokio::task::spawn_blocking(move || cg.remove())
                .await
                .map_err(|e| AgentError::exec(&self.program, e))?;
        }
        for pump in self.pumps.drain(..) {
            // A grandchild that left the group can hold the pipe open; don't wait forever.
            if let Ok(Ok(Err(e))) = tokio::time::timeout(Duration::from_secs(5), pump).await {
                return Err(AgentError::exec(
                    &self.program,
                    format!("its log could not be written ({e})"),
                ));
            }
        }
        #[cfg(unix)]
        let signal = std::os::unix::process::ExitStatusExt::signal(&status);
        #[cfg(not(unix))]
        let signal = None;
        let report = ExitReport {
            code: status.code(),
            signal,
            oom_killed,
            limits: self.limits.clone(),
            runtime_ms: self.started.elapsed().as_millis() as u64,
        };
        self.report = Some(report.clone());
        Ok(report)
    }

    async fn kill(&mut self) -> Result<()> {
        if self.report.is_none() {
            self.signal_tree(true);
        }
        Ok(())
    }

    async fn stop(&mut self, grace: Duration) -> Result<ExitReport> {
        if self.report.is_none() {
            self.signal_tree(false);
            if let Ok(r) = tokio::time::timeout(grace, self.child.wait()).await {
                r.map_err(|e| AgentError::exec(&self.program, e))?;
            } else {
                self.signal_tree(true);
            }
        }
        self.wait().await
    }
}

impl Drop for ProcessExecution {
    fn drop(&mut self) {
        if let Some(t) = self.tracker.take() {
            t.abort();
        }
        if self.report.is_none() {
            self.signal_tree(true);
            #[cfg(target_os = "linux")]
            if let Some(cg) = self.cgroup.take() {
                cg.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn executor() -> ProcessExecutor {
        ProcessExecutor::new(&ExecConfig::default()).unwrap()
    }

    #[cfg(unix)]
    fn shell(dir: &Path, name: &str, script: &str) -> ExecSpec {
        ExecSpec::new(name, "/bin/sh", &["-c", script], dir)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runs_with_a_clean_environment_and_logs() {
        let dir = tempfile::tempdir().unwrap();
        let mut spec = shell(
            dir.path(),
            "env-test",
            "echo cuda=$CUDA_VISIBLE_DEVICES; echo secret=$MRL_TEST_SECRET; echo extra=$EXTRA; echo staging=$MRL_STAGING_DIR; pwd; echo oops >&2; exit 3",
        );
        spec.gpus = vec!["GPU-a".into(), "GPU-b".into()];
        spec.env.insert("EXTRA".into(), "yes".into());
        // Not on the allow-list, so it must not reach the job.
        std::env::set_var("MRL_TEST_SECRET", "leak");
        let mut run = executor().spawn(spec.clone()).await.unwrap();
        assert!(run.pid().is_some());
        let report = run.wait().await.unwrap();
        assert_eq!(report.code, Some(3));
        assert!(!report.success() && !report.oom_killed);
        let out = std::fs::read_to_string(spec.log_dir.join("stdout.log")).unwrap();
        assert!(out.contains("cuda=GPU-a,GPU-b"), "{out}");
        assert!(out.contains("secret=\n"), "{out}");
        assert!(out.contains("extra=yes"), "{out}");
        assert!(out.contains(&format!("staging={}", spec.staging_dir.display())));
        let work = spec.work_dir.canonicalize().unwrap();
        assert!(out.contains(work.to_str().unwrap()), "{out}");
        assert_eq!(
            std::fs::read_to_string(spec.log_dir.join("stderr.log")).unwrap(),
            "oops\n"
        );
        // A second wait returns the same report.
        assert_eq!(run.wait().await.unwrap(), report);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stop_ends_the_whole_tree() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("child.pid");
        let script = format!("sleep 300 & echo $! > {}; wait", marker.display());
        let mut run = executor()
            .spawn(shell(dir.path(), "tree", &script))
            .await
            .unwrap();
        for _ in 0..200 {
            if std::fs::read_to_string(&marker).is_ok_and(|s| s.ends_with('\n')) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let child: i32 = std::fs::read_to_string(&marker)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let report = run.stop(Duration::from_secs(5)).await.unwrap();
        assert!(report.signal.is_some() || report.code.is_some());
        // The grandchild is gone too (signal 0 probes for existence).
        let mut gone = false;
        for _ in 0..200 {
            if nix::sys::signal::kill(nix::unistd::Pid::from_raw(child), None).is_err() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(gone, "grandchild {child} survived");
    }

    /// Gone, or a zombie waiting for init to reap it.
    #[cfg(unix)]
    fn gone(pid: i32) -> bool {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err()
            || std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .is_ok_and(|s| s.split_whitespace().nth(2) == Some("Z"))
    }

    #[cfg(unix)]
    async fn wait_gone(pid: i32) -> bool {
        for _ in 0..300 {
            if gone(pid) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[cfg(unix)]
    async fn read_pid(file: &Path) -> i32 {
        for _ in 0..300 {
            if let Ok(s) = std::fs::read_to_string(file) {
                if let Ok(p) = s.trim().parse() {
                    return p;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{} was never written", file.display())
    }

    /// Starts `sleep 300` in a new session (out of the job's process group), like a worker
    /// that daemonizes. perl is on macOS and on Linux CI images.
    #[cfg(unix)]
    fn setsid_child(pid_file: &Path) -> String {
        format!(
            "perl -e 'use POSIX; POSIX::setsid() or die; exec qw(sleep 300)' & echo $! > {}",
            pid_file.display()
        )
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn children_that_leave_the_group_are_killed() {
        let dir = tempfile::tempdir().unwrap();
        let exec = executor();

        // 1. stop() while the job runs.
        let f1 = dir.path().join("a.pid");
        let script = format!("{}; wait", setsid_child(&f1));
        let mut run = exec
            .spawn(shell(dir.path(), "setsid-stop", &script))
            .await
            .unwrap();
        let child = read_pid(&f1).await;
        tokio::time::sleep(TREE_SCAN * 2).await;
        run.stop(Duration::from_secs(5)).await.unwrap();
        assert!(wait_gone(child).await, "setsid child {child} survived stop");

        // 2. The main process exits on its own and leaves the child behind.
        let f2 = dir.path().join("b.pid");
        let script = format!("{}; sleep 1.5", setsid_child(&f2));
        let mut run = exec
            .spawn(shell(dir.path(), "setsid-exit", &script))
            .await
            .unwrap();
        let child = read_pid(&f2).await;
        assert!(run.wait().await.unwrap().success());
        assert!(
            wait_gone(child).await,
            "setsid child {child} outlived its job"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_log_failure_starts_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ran");
        let spec = shell(
            dir.path(),
            "logfail",
            &format!("touch {}", marker.display()),
        );
        // A folder where the log file must go.
        std::fs::create_dir_all(spec.log_dir.join("stderr.log")).unwrap();
        let e = executor().spawn(spec).await.err().unwrap();
        assert_eq!(e.code(), "MRL-NODE-121");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!marker.exists(), "the job ran although spawn failed");
    }

    #[tokio::test]
    async fn bad_names_and_missing_programs_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let e = executor()
            .spawn(ExecSpec::new("../x", "true", &[], dir.path()))
            .await
            .err()
            .unwrap();
        assert_eq!(e.code(), "MRL-VAL-126");
        let e = executor()
            .spawn(ExecSpec::new("ok", "/no/such/program", &[], dir.path()))
            .await
            .err()
            .unwrap();
        assert_eq!(e.code(), "MRL-NODE-123");
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn limits_are_reported_as_unsupported_without_cgroups() {
        let s = executor().limit_support();
        assert!(!s.cpu && !s.memory && !s.pids && !s.oom_detection);
        assert!(s.note.unwrap().contains("not enforced"));
    }
}
