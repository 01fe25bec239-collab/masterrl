//! The Tier-1 `reward` profile and the reward service launcher (Doc 06 §1.8.2–1.8.3).
//!
//! The reward service (`mrl-reward-sandboxed`, from mrl-rewards) runs as a sibling of the
//! worker, sharing no namespace with it: the only shared object is the socket folder,
//! bound writable into the sandbox and readable by the worker. Inside it sees the system
//! folders and its Python environment read-only, the reward bundle read-only (as its working
//! folder), a 512 MiB in-memory `/tmp`, and nothing of the node: no job folder, no datasets,
//! no checkpoints, no agent state, no GPU, no network, no secrets. Its nonce comes on fd 5.
//!
//! [`supervise`] keeps it running for the attempt: a crash is restarted with backoff (1, 2,
//! 4 s), at most 3 times, then the attempt fails REWARD_INFRA (retryable). A seccomp kill
//! (SIGSYS) is a sandbox violation: no restart, the attempt fails SANDBOX_VIOLATION. Running
//! out of memory is a crash like any other, not a violation.

use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{
    classify, effective_limits, env_options, harden_bwrap, sandbox_env, Ending, Filter, Hardening,
    ProfileError, NONCE_FD,
};
use crate::exec::{ExecSpec, Executor, Limits};
use crate::sandbox::{SandboxBackend, SandboxProfile};

/// Profile name.
pub const NAME: &str = "reward";
/// Restarts after crashes before the attempt fails REWARD_INFRA.
pub const MAX_RESTARTS: u32 = 3;
/// Socket file inside the socket folder.
pub const SOCKET_FILE: &str = "reward.sock";

/// Tier 1: no network, no GPU, no secrets; bundle read-only; writes only to its socket folder.
pub fn profile() -> SandboxProfile {
    SandboxProfile {
        name: NAME,
        description: "reward code (Tier 1): no network, no GPU, no secrets; the bundle read-only; writes only its socket folder and a 512 MiB in-memory /tmp",
        network: false,
        gpu_devices: false,
    }
}

/// Limits: 2 CPUs, 4 GiB, 256 processes by default; at most 4 CPUs, 8 GiB, 512.
pub fn hardening() -> Hardening {
    Hardening {
        filter: Filter::Strict,
        tmpfs_bytes: 512 << 20,
        default_limits: Limits {
            cpu_millis: Some(2000),
            memory_bytes: Some(4 << 30),
            pids: Some(256),
        },
        max_limits: Limits {
            cpu_millis: Some(4000),
            memory_bytes: Some(8 << 30),
            pids: Some(512),
        },
        env: &[],
        keep_spec_env: false,
        wall_clock_secs: None,
        output_cap_bytes: None,
    }
}

/// What to launch.
#[derive(Debug, Clone)]
pub struct RewardExecLaunch {
    /// Execution name (cgroup and logs): letters, digits, `.`, `_`, `-`.
    pub name: String,
    /// Where the logs and the private filter/args/nonce files go (never mounted).
    pub job_dir: PathBuf,
    /// The reward service program (`…/bin/mrl-reward-sandboxed` of its environment).
    pub program: PathBuf,
    /// Folders the program needs read-only (its Python environment and interpreter).
    pub read_paths: Vec<PathBuf>,
    /// The reward bundle (read-only, the working folder).
    pub bundle: PathBuf,
    /// The socket folder (writable; the worker reads the socket from here).
    pub socket_dir: PathBuf,
    /// `rwdv_…` it serves.
    pub reward_version_id: String,
    /// The attempt's nonce (given on fd 5, never in argv or the environment).
    pub nonce: String,
    /// Limits asked for (lowered to the profile's caps).
    pub limits: Limits,
    /// The bundle has custom code.
    pub user_code: bool,
}

impl RewardExecLaunch {
    /// The socket path the worker connects to.
    pub fn socket(&self) -> PathBuf {
        self.socket_dir.join(SOCKET_FILE)
    }
}

/// The sandboxed command for `l`. Custom code needs an isolating bubblewrap backend
/// (MRL-RWD-295 otherwise).
pub fn spec(backend: &dyn SandboxBackend, l: &RewardExecLaunch) -> Result<ExecSpec, ProfileError> {
    if l.user_code {
        super::user_code_gate(backend)?;
    }
    let listen = format!("unix:{}", l.socket().display());
    let mut args: Vec<String> = vec![
        "--bundle".into(),
        l.bundle.display().to_string(),
        "--listen".into(),
        listen,
        "--reward-version-id".into(),
        l.reward_version_id.clone(),
        "--nonce-fd".into(),
        NONCE_FD.to_string(),
    ];
    if l.user_code {
        args.push("--user-code".into());
    }
    let program = l.program.display().to_string();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let mut spec = ExecSpec::new(&l.name, &program, &arg_refs, &l.job_dir);
    spec.work_dir = l.bundle.clone();
    spec.staging_dir = l.socket_dir.clone();
    spec.log_dir = l.job_dir.join("logs").join(&l.name);
    let h = hardening();
    spec.limits = effective_limits(&h, l.limits);
    let p = profile();
    let isolating = backend.isolates() && backend.name() == "bubblewrap";
    if !isolating {
        // Built-ins only (the gate refused custom code): the nonce still goes through a file
        // descriptor, opened by a shell from a private file it deletes.
        let files = l.job_dir.join(super::SANDBOX_DIR).join(&l.name);
        let nonce_path = files.join("nonce");
        super::write_private(&nonce_path, l.nonce.as_bytes())?;
        let mut sh = vec![
            "-c".to_owned(),
            r#"exec 5<"$1" && rm -f "$1" && shift && exec "$@""#.to_owned(),
            "mrl-reward".to_owned(),
            nonce_path.display().to_string(),
            spec.program.clone(),
        ];
        sh.extend(spec.args.clone());
        spec.program = "/bin/sh".into();
        spec.args = sh;
        return Ok(spec);
    }
    let env = sandbox_env(&p, &h, &spec);
    let mut opts = env_options(&env);
    opts.extend([
        "--setenv".to_owned(),
        super::USER_CODE_ENV.to_owned(),
        "1".to_owned(),
    ]);
    let files = l.job_dir.join(super::SANDBOX_DIR).join(&l.name);
    let wrapped = backend.wrap(&p, spec, &l.read_paths)?;
    Ok(harden_bwrap(wrapped, &h, &opts, &files, Some(&l.nonce))?)
}

/// How supervision ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Stopped because the attempt ended (`cancel`).
    Stopped,
    /// Crashed more than [`MAX_RESTARTS`] times: fail the attempt REWARD_INFRA.
    GaveUp {
        /// Crashes seen.
        crashes: u32,
        /// The last one.
        last: String,
    },
    /// The sandbox stopped it: fail the attempt SANDBOX_VIOLATION (and audit it).
    Violation(String),
}

/// One supervision event (for the attempt log and the CONTROL lane).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Started (the `n`-th time, 0 = first).
    Started(u32),
    /// Ended; it will be restarted.
    Crashed(String),
}

/// The first restart's delay; each later one doubles it.
pub const BACKOFF: Duration = Duration::from_secs(1);

/// Runs the reward service until `cancel`, restarting it after crashes (see module docs).
pub async fn supervise(
    executor: &dyn Executor,
    backend: &dyn SandboxBackend,
    launch: &RewardExecLaunch,
    cancel: CancellationToken,
    on_event: impl FnMut(Event) + Send,
) -> Result<Outcome, ProfileError> {
    supervise_with(executor, backend, launch, cancel, BACKOFF, on_event).await
}

/// [`supervise`] with another first backoff (tests).
pub async fn supervise_with(
    executor: &dyn Executor,
    backend: &dyn SandboxBackend,
    launch: &RewardExecLaunch,
    cancel: CancellationToken,
    backoff: Duration,
    mut on_event: impl FnMut(Event) + Send,
) -> Result<Outcome, ProfileError> {
    let mut crashes = 0u32;
    loop {
        // The socket of a crashed run is removed so the next one can bind it.
        let _ = std::fs::remove_file(launch.socket());
        let spec = spec(backend, launch)?;
        let mut run = executor.spawn(spec).await?;
        on_event(Event::Started(crashes));
        let report = tokio::select! {
            r = run.wait() => r?,
            () = cancel.cancelled() => {
                run.stop(Duration::from_secs(5)).await?;
                return Ok(Outcome::Stopped);
            }
        };
        let why = match classify(&report) {
            Ending::Violation(why) => return Ok(Outcome::Violation(why)),
            Ending::Ok => "it stopped on its own".to_owned(),
            Ending::OutOfMemory => "it ran out of memory".to_owned(),
            Ending::Failed(why) => why,
        };
        crashes += 1;
        if crashes > MAX_RESTARTS {
            return Ok(Outcome::GaveUp { crashes, last: why });
        }
        on_event(Event::Crashed(why));
        let delay = backoff * (1 << (crashes - 1));
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = cancel.cancelled() => return Ok(Outcome::Stopped),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{Execution, ExitReport};
    use crate::sandbox::NoSandbox;
    use std::sync::{Arc, Mutex};

    fn launch(dir: &std::path::Path, user_code: bool) -> RewardExecLaunch {
        RewardExecLaunch {
            name: "reward-1".into(),
            job_dir: dir.to_path_buf(),
            program: "/env/bin/mrl-reward-sandboxed".into(),
            read_paths: vec!["/env".into()],
            bundle: dir.join("bundle"),
            socket_dir: dir.join("sock"),
            reward_version_id: "rwdv_x".into(),
            nonce: "secret-nonce".into(),
            limits: Limits::default(),
            user_code,
        }
    }

    #[test]
    fn custom_code_is_refused_without_bubblewrap_and_the_nonce_never_reaches_argv() {
        let dir = tempfile::tempdir().unwrap();
        let e = spec(&NoSandbox, &launch(dir.path(), true)).unwrap_err();
        assert_eq!(e.code(), "MRL-RWD-295");
        let s = spec(&NoSandbox, &launch(dir.path(), false)).unwrap();
        assert!(!s.args.iter().any(|a| a.contains("secret-nonce")));
        assert!(!s.env.values().any(|v| v.contains("secret-nonce")));
        assert!(s
            .args
            .windows(2)
            .any(|w| w[0] == "--nonce-fd" && w[1] == "5"));
        assert_eq!(s.limits, hardening().default_limits);
    }

    #[cfg(unix)]
    #[test]
    fn under_bubblewrap_the_service_sees_only_its_env_bundle_and_socket_folder() {
        let dir = tempfile::tempdir().unwrap();
        let backend = crate::sandbox::bwrap::Bubblewrap::new(Some("/usr/bin/bwrap".into()));
        let l = launch(dir.path(), true);
        let r = spec(&backend, &l);
        if !cfg!(target_os = "linux") {
            assert_eq!(r.unwrap_err().code(), "MRL-RWD-295");
            return;
        }
        let s = r.unwrap();
        assert_eq!(s.program, "/bin/sh");
        let joined = s.args.join(" ");
        assert!(!joined.contains("secret-nonce"));
        let pos = |needle: &[&str]| {
            s.args
                .windows(needle.len())
                .position(|w| w.iter().zip(needle).all(|(a, b)| a == b))
        };
        let bundle = l.bundle.display().to_string();
        let sock = l.socket_dir.display().to_string();
        assert!(pos(&["--ro-bind", &bundle, &bundle]).is_some());
        assert!(pos(&["--bind", &sock, &sock]).is_some());
        assert!(pos(&["--ro-bind-try", "/env", "/env"]).is_some());
        assert_eq!(
            s.args.iter().filter(|a| *a == "--bind").count(),
            1,
            "one writable folder"
        );
        assert!(pos(&["--seccomp", "3"]).is_some() && pos(&["--args", "4"]).is_some());
        assert!(pos(&["--share-net"]).is_none() && pos(&["--dev-bind-try"]).is_none());
        assert!(pos(&["--size", &(512u64 << 20).to_string(), "--tmpfs", "/tmp"]).is_some());
        let files = dir.path().join("sandbox").join("reward-1");
        assert_eq!(std::fs::read(files.join("nonce")).unwrap(), b"secret-nonce");
        let extra = std::fs::read(files.join("args")).unwrap();
        let extra: Vec<&[u8]> = extra.split(|b| *b == 0).collect();
        assert!(extra.contains(&&b"--clearenv"[..]) && extra.contains(&&b"ALL"[..]));
        let filter = std::fs::metadata(files.join("seccomp.bpf")).unwrap();
        assert_eq!(filter.len() % 8, 0);
    }

    /// An executor that ends each run with the next scripted report.
    struct Scripted {
        reports: Mutex<Vec<ExitReport>>,
        spawned: Arc<Mutex<u32>>,
    }

    struct Run(Option<ExitReport>);

    #[async_trait::async_trait]
    impl Execution for Run {
        fn pid(&self) -> Option<u32> {
            None
        }
        async fn wait(&mut self) -> crate::Result<ExitReport> {
            match self.0.take() {
                Some(r) => Ok(r),
                None => std::future::pending().await,
            }
        }
        async fn kill(&mut self) -> crate::Result<()> {
            Ok(())
        }
        async fn stop(&mut self, _: Duration) -> crate::Result<ExitReport> {
            Ok(report(Some(0), None))
        }
    }

    #[async_trait::async_trait]
    impl Executor for Scripted {
        fn limit_support(&self) -> crate::exec::LimitSupport {
            crate::exec::LimitSupport::default()
        }
        async fn spawn(&self, _: ExecSpec) -> crate::Result<Box<dyn Execution>> {
            *self.spawned.lock().unwrap() += 1;
            let next = {
                let mut r = self.reports.lock().unwrap();
                (!r.is_empty()).then(|| r.remove(0))
            };
            Ok(Box::new(Run(next)))
        }
    }

    fn report(code: Option<i32>, signal: Option<i32>) -> ExitReport {
        ExitReport {
            code,
            signal,
            oom_killed: false,
            limits: Default::default(),
            runtime_ms: 1,
        }
    }

    fn scripted(reports: Vec<ExitReport>) -> (Scripted, Arc<Mutex<u32>>) {
        let spawned = Arc::new(Mutex::new(0));
        (
            Scripted {
                reports: Mutex::new(reports),
                spawned: spawned.clone(),
            },
            spawned,
        )
    }

    #[tokio::test]
    async fn three_restarts_then_reward_infra() {
        let dir = tempfile::tempdir().unwrap();
        let (exec, spawned) = scripted(vec![report(Some(1), None); 4]);
        let mut events = Vec::new();
        let ms = Duration::from_millis(1);
        let out = supervise_with(
            &exec,
            &NoSandbox,
            &launch(dir.path(), false),
            CancellationToken::new(),
            ms,
            |e| events.push(e),
        )
        .await
        .unwrap();
        assert!(matches!(out, Outcome::GaveUp { crashes: 4, .. }), "{out:?}");
        assert_eq!(*spawned.lock().unwrap(), 4);
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::Crashed(_)))
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn a_seccomp_kill_is_a_violation_and_never_restarted() {
        let dir = tempfile::tempdir().unwrap();
        let (exec, spawned) = scripted(vec![
            report(Some(1), None),
            report(None, Some(super::super::SIGSYS)),
        ]);
        let ms = Duration::from_millis(1);
        let out = supervise_with(
            &exec,
            &NoSandbox,
            &launch(dir.path(), false),
            CancellationToken::new(),
            ms,
            |_| {},
        )
        .await
        .unwrap();
        assert!(matches!(out, Outcome::Violation(_)), "{out:?}");
        assert_eq!(*spawned.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn the_attempt_ending_stops_it() {
        let dir = tempfile::tempdir().unwrap();
        let (exec, _) = scripted(vec![]);
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let l = launch(dir.path(), false);
        let task = async { supervise(&exec, &NoSandbox, &l, cancel, |_| {}).await };
        let (out, ()) = tokio::join!(task, async { stop.cancel() });
        assert_eq!(out.unwrap(), Outcome::Stopped);
    }
}
