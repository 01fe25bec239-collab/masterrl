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
    /// The reward has code-running (Tier-2) components: the service runs in the
    /// [`code_exec`](super::code_exec) profile and may start one jail per execution.
    pub tier2: bool,
}

impl RewardExecLaunch {
    /// The socket path the worker connects to.
    pub fn socket(&self) -> PathBuf {
        self.socket_dir.join(SOCKET_FILE)
    }
}

/// The sandboxed command for `l`. Custom code and Tier-2 rewards need an isolating
/// bubblewrap backend (MRL-RWD-295 otherwise).
pub fn spec(backend: &dyn SandboxBackend, l: &RewardExecLaunch) -> Result<ExecSpec, ProfileError> {
    if l.user_code || l.tier2 {
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
    if l.tier2 {
        args.extend(["--tier".into(), "2".into()]);
    }
    let program = l.program.display().to_string();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let mut spec = ExecSpec::new(&l.name, &program, &arg_refs, &l.job_dir);
    spec.work_dir = l.bundle.clone();
    spec.staging_dir = l.socket_dir.clone();
    spec.log_dir = l.job_dir.join("logs").join(&l.name);
    let (h, p) = if l.tier2 {
        (super::code_exec::hardening(), super::code_exec::profile())
    } else {
        (hardening(), profile())
    };
    spec.limits = effective_limits(&h, l.limits);
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
    let files = l.job_dir.join(super::SANDBOX_DIR).join(&l.name);
    let mut env = sandbox_env(&p, &h, &spec);
    let mut binds = Vec::new();
    if l.tier2 {
        let (vars, b) = super::code_exec::jail_files_in(&files.join("jail"))?;
        env.extend(vars);
        binds = b;
    }
    let mut opts = env_options(&env);
    opts.extend([
        "--setenv".to_owned(),
        super::USER_CODE_ENV.to_owned(),
        "1".to_owned(),
    ]);
    opts.extend(binds);
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
            tier2: false,
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
        // No process inside keeps the outer environment (`/proc/1/environ` would show it).
        assert!(extra.contains(&&b"--as-pid-1"[..]));
        assert!(
            s.env.is_empty(),
            "the outer command gets no environment: {:?}",
            s.env
        );
        let filter = std::fs::metadata(files.join("seccomp.bpf")).unwrap();
        assert_eq!(filter.len() % 8, 0);
    }

    #[cfg(unix)]
    #[test]
    fn a_tier2_reward_runs_as_a_jail_supervisor_and_never_without_bubblewrap() {
        let dir = tempfile::tempdir().unwrap();
        let mut l = launch(dir.path(), false);
        l.tier2 = true;
        assert_eq!(spec(&NoSandbox, &l).unwrap_err().code(), "MRL-RWD-295");
        let backend = crate::sandbox::bwrap::Bubblewrap::new(Some("/usr/bin/bwrap".into()));
        let r = spec(&backend, &l);
        if !cfg!(target_os = "linux") {
            assert_eq!(r.unwrap_err().code(), "MRL-RWD-295");
            return;
        }
        let s = r.unwrap();
        assert!(s.args.windows(2).any(|w| w[0] == "--tier" && w[1] == "2"));
        let files = dir.path().join("sandbox").join("reward-1");
        let extra =
            String::from_utf8_lossy(&std::fs::read(files.join("args")).unwrap()).replace('\0', " ");
        assert!(
            extra.contains("MRL_JAIL_SECCOMP /run/mrl-jail/strict.bpf"),
            "{extra}"
        );
        assert!(extra.contains("MRL_SANDBOX_PROFILE code_exec"), "{extra}");
        let own = std::fs::read(files.join("seccomp.bpf")).unwrap();
        let strict = std::fs::read(files.join("jail/strict.bpf")).unwrap();
        assert_ne!(
            own, strict,
            "the service itself runs under the supervisor filter"
        );
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

/// SB-1 / SB-2: the reward escape corpus (`tests/security/reward_escape`) against the real
/// `reward` and `code_exec` sandboxes with bubblewrap. Ignored; needs Linux, bubblewrap,
/// unprivileged user namespaces and the workspace's Python environment (`.venv`, or
/// `MRL_ESCAPE_PYTHON`). Without them it prints SKIPPED, unless `MRL_TEST_SANDBOX_REQUIRED=1`
/// (CI: `.github/workflows/ci-security-rewards.yml`). `MRL_ESCAPE_KEEP=1` keeps the run
/// folder (bundles, results, service logs) for a look afterwards.
#[cfg(all(test, target_os = "linux"))]
mod escape {
    use super::*;
    use crate::config::{ExecConfig, SandboxChoice, SandboxConfig};
    use crate::exec::process::ProcessExecutor;
    use std::io::Write as _;
    use std::os::linux::net::SocketAddrExt as _;
    use std::os::unix::net::{SocketAddr, UnixListener};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn skip(why: &str) {
        let required = std::env::var("MRL_TEST_SANDBOX_REQUIRED").as_deref() == Ok("1");
        assert!(!required, "{why}");
        eprintln!("SKIPPED: {why}");
    }

    fn secret() -> String {
        let mut b = [0u8; 16];
        getrandom::fill(&mut b).unwrap();
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    /// Counts connections and answers each with `secret`.
    fn tcp_canary(secret: &str, hits: Arc<AtomicUsize>) -> u16 {
        let l = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let s = secret.to_owned();
        std::thread::spawn(move || {
            for mut c in l.incoming().flatten() {
                hits.fetch_add(1, Ordering::SeqCst);
                let _ = c.write_all(s.as_bytes());
            }
        });
        port
    }

    fn unix_canary(l: UnixListener, secret: &str, hits: Arc<AtomicUsize>) {
        let s = secret.to_owned();
        std::thread::spawn(move || {
            for mut c in l.incoming().flatten() {
                hits.fetch_add(1, Ordering::SeqCst);
                let _ = c.write_all(s.as_bytes());
            }
        });
    }

    /// This machine's address on its default route (no packet is sent).
    fn host_ip() -> Option<std::net::IpAddr> {
        let s = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        s.connect("192.0.2.1:9").ok()?;
        Some(s.local_addr().ok()?.ip())
    }

    fn gone(pid: u32) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map_or(true, |s| s.split_whitespace().nth(2) == Some("Z"))
    }

    fn python(root: &Path, args: &[&str]) -> std::process::Output {
        let py = std::env::var_os("MRL_ESCAPE_PYTHON")
            .map_or_else(|| root.join(".venv/bin/python"), PathBuf::from);
        std::process::Command::new(py)
            .arg(root.join("tests/security/reward_escape/runner.py"))
            .args(args)
            .output()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "needs Linux, bubblewrap and the workspace Python environment"]
    async fn reward_escape_corpus_is_blocked() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = std::fs::canonicalize(root).unwrap();
        let py = std::env::var_os("MRL_ESCAPE_PYTHON")
            .map_or_else(|| root.join(".venv/bin/python"), PathBuf::from);
        if !py.exists() {
            return skip(&format!("no Python environment at {}", py.display()));
        }
        let backend = match crate::sandbox::select(&SandboxConfig {
            backend: SandboxChoice::Bubblewrap,
            bwrap_path: None,
        }) {
            Ok(b) => b,
            Err(e) => return skip(&e.to_string()),
        };
        let exec = ProcessExecutor::new(&ExecConfig::default()).unwrap();
        let tmp = tempfile::Builder::new()
            .prefix("rwe")
            .tempdir_in("/tmp")
            .unwrap();
        let (canary, run) = (tmp.path().join("canary"), tmp.path().join("run"));
        std::fs::create_dir_all(canary.join("agent-state")).unwrap();
        std::fs::create_dir_all(&run).unwrap();

        // Canaries: secrets the sandbox must never see, listeners it must never reach.
        let (file_secret, net_secret, env_secret) = (secret(), secret(), secret());
        let mut files = vec![
            canary.join("secret.txt"),
            canary.join("agent-state/state.json"),
        ];
        let home_canary = std::env::var_os("HOME")
            .filter(|_| std::env::var("MRL_ESCAPE_HOME_CANARY").as_deref() == Ok("1"))
            .map(|h| PathBuf::from(h).join(format!(".mrl-escape-canary-{}", secret())));
        files.extend(home_canary.clone());
        for f in &files {
            std::fs::write(f, &file_secret).unwrap();
        }
        let hits = Arc::new(AtomicUsize::new(0));
        let port = tcp_canary(&net_secret, hits.clone());
        let mut tcp = vec![format!("127.0.0.1:{port}")];
        tcp.extend(host_ip().map(|ip| format!("{ip}:{port}")));
        // The workflow's own canary server (ci-security-rewards.yml).
        let extra = std::env::var("MRL_ESCAPE_EXTRA_TCP").unwrap_or_default();
        tcp.extend(extra.split_whitespace().map(str::to_owned));
        let abstract_name = format!("mrl-escape-{}", secret());
        let addr = SocketAddr::from_abstract_name(abstract_name.as_bytes()).unwrap();
        unix_canary(
            UnixListener::bind_addr(&addr).unwrap(),
            &net_secret,
            hits.clone(),
        );
        let unix_path = canary.join("agent.sock");
        unix_canary(
            UnixListener::bind(&unix_path).unwrap(),
            &net_secret,
            hits.clone(),
        );
        let mut sentinel = std::process::Command::new("sleep")
            .arg("777.25")
            .spawn()
            .unwrap();
        let secrets = canary.join("secrets.json");
        let workflow = std::env::var("MRL_ESCAPE_EXTRA_SECRET").unwrap_or_default();
        let doc = serde_json::json!({
            "file": file_secret, "net": net_secret, "env": env_secret, "workflow": workflow,
        });
        std::fs::write(&secrets, doc.to_string()).unwrap();

        let mut plan_args = vec![
            "plan".to_owned(),
            "--out".into(),
            run.display().to_string(),
            "--abstract".into(),
            abstract_name,
            "--unix".into(),
            unix_path.display().to_string(),
            "--sentinel-pid".into(),
            sentinel.id().to_string(),
            "--sentinel-marker".into(),
            "777.25".into(),
            "--tcp".into(),
        ];
        plan_args.extend(tcp);
        plan_args.push("--files".into());
        plan_args.extend(files.iter().map(|f| f.display().to_string()));
        let refs: Vec<&str> = plan_args.iter().map(String::as_str).collect();
        let out = python(&root, &refs);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let plan_path = run.join("plan.json");
        let plan: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&plan_path).unwrap()).unwrap();
        let read_paths: Vec<PathBuf> = plan["read_paths"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| PathBuf::from(p.as_str().unwrap()))
            .collect();

        for (i, case) in plan["cases"].as_array().unwrap().iter().enumerate() {
            let id = case["id"].as_str().unwrap();
            let launch = RewardExecLaunch {
                name: format!("esc-{i}"),
                job_dir: run.join("job"),
                program: PathBuf::from(plan["program"].as_str().unwrap()),
                read_paths: read_paths.clone(),
                bundle: PathBuf::from(case["bundle"].as_str().unwrap()),
                socket_dir: run.join("s").join(i.to_string()),
                reward_version_id: case["version"].as_str().unwrap().to_owned(),
                nonce: secret(),
                limits: Limits::default(),
                user_code: case["user_code"].as_bool().unwrap(),
                tier2: case["tier"].as_u64() == Some(2),
            };
            std::fs::create_dir_all(&launch.socket_dir).unwrap();
            let mut s = spec(backend.as_ref(), &launch).unwrap();
            // As if the agent's own environment leaked into the launch: it must not get in.
            for k in [
                "MRL_CREDENTIAL_ESCAPE",
                "MRL_ESCAPE_ENV_CANARY",
                "AWS_SECRET_ACCESS_KEY",
            ] {
                s.env.insert(k.into(), env_secret.clone());
            }
            let logs = s.log_dir.clone();
            let mut proc = exec.spawn(s).await.unwrap();
            let pid = proc.pid().unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while !launch.socket().exists() && !gone(pid) && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let sock = launch.socket().display().to_string();
            let args = [
                "score",
                "--plan",
                plan_path.to_str().unwrap(),
                "--case",
                id,
                "--socket",
                &sock,
                "--nonce",
                &launch.nonce,
            ];
            let root2 = root.clone();
            let owned: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
            let scored = tokio::task::spawn_blocking(move || {
                let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
                python(&root2, &refs)
            })
            .await
            .unwrap();
            assert!(
                scored.status.success(),
                "{id}: {}",
                String::from_utf8_lossy(&scored.stderr)
            );
            let report = if gone(pid) {
                proc.wait().await.unwrap()
            } else {
                proc.stop(Duration::from_secs(5)).await.unwrap()
            };
            let ending = match classify(&report) {
                Ending::Ok => "ok".to_owned(),
                Ending::Violation(_) => "violation".to_owned(),
                Ending::OutOfMemory => "oom".to_owned(),
                Ending::Failed(why) => format!("failed: {why}"),
            };
            let tail = |f: &str| {
                let t = std::fs::read_to_string(logs.join(f)).unwrap_or_default();
                let cut = t.len().saturating_sub(1500);
                t.get(cut..).unwrap_or_default().to_owned()
            };
            let exit = serde_json::json!({
                "ending": ending.split(':').next(), "detail": ending,
                "log": format!("{}{}", tail("stdout.log"), tail("stderr.log")),
            });
            std::fs::write(
                run.join("r").join(format!("{id}.exit.json")),
                exit.to_string(),
            )
            .unwrap();
        }

        let judged = python(
            &root,
            &[
                "judge",
                "--plan",
                plan_path.to_str().unwrap(),
                "--secrets",
                secrets.to_str().unwrap(),
            ],
        );
        let alive = sentinel.try_wait().unwrap().is_none();
        if std::env::var("MRL_ESCAPE_KEEP").as_deref() == Ok("1") {
            eprintln!("kept the run folder: {}", tmp.keep().display());
        }
        let _ = sentinel.kill();
        if let Some(h) = &home_canary {
            let _ = std::fs::remove_file(h);
        }
        let report = format!(
            "{}\n{}",
            String::from_utf8_lossy(&judged.stdout),
            String::from_utf8_lossy(&judged.stderr)
        );
        eprintln!("{report}");
        assert!(
            judged.status.success(),
            "the corpus was not fully blocked:\n{report}"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "a canary listener got a connection"
        );
        assert!(alive, "the host sentinel process was killed from a sandbox");
    }
}
