//! Attempt lifecycle (Unix) against the real `mrl-worker` harness and its FakeEngine.
//!
//! The `#[ignore]`d tests need `uv` (they build the mrl-runtime environment from the repo's
//! `uv.lock`). Without it they print SKIPPED and pass, unless `MRL_TEST_ATTEMPTS_REQUIRED=1`
//! (set in ci-attempts), which makes a missing prerequisite fail. Unix only: the worker socket
//! is a Unix socket.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::routing::put;
use mrl_agent::attempts::record::{AttemptRecord, ProcIdentity};
use mrl_agent::attempts::{AttemptEvent, AttemptEvents, AttemptSpec, Attempts};
use mrl_agent::config::{AgentConfig, CacheConfig, SandboxChoice, SandboxConfig};
use mrl_agent::hw::fake::{FakeHw, Fault};
use mrl_agent::hw::HwBackend;
use mrl_agent::AgentContext;
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{AttemptPhase, Command, Lane, UploadArtifacts, UploadItem};
use mrl_proto::common::v1::FailureClass;
use mrl_proto::worker::v1::worker_message::Body;
use mrl_proto::worker::v1::ExitKind;

const WAIT: Duration = Duration::from_secs(180);

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// True when uv is here; SKIPPED (or a failure when required) otherwise.
fn ready(test: &str) -> bool {
    if mrl_agent::envs::which("uv").is_some() {
        return true;
    }
    assert!(
        std::env::var("MRL_TEST_ATTEMPTS_REQUIRED").is_err(),
        "{test}: uv is required (MRL_TEST_ATTEMPTS_REQUIRED=1) but not on PATH"
    );
    eprintln!("SKIPPED {test}: uv is not installed");
    false
}

fn ctx(dir: &Path, hw: Option<Arc<FakeHw>>) -> Arc<AgentContext> {
    let config = AgentConfig {
        state_dir: dir.join("state"),
        cache: CacheConfig {
            max_bytes: Some(1 << 30),
            min_free_bytes: 0,
        },
        sandbox: SandboxConfig {
            backend: SandboxChoice::None,
            bwrap_path: None,
        },
        ..AgentConfig::default()
    };
    let backends: Vec<Arc<dyn HwBackend>> =
        hw.into_iter().map(|h| h as Arc<dyn HwBackend>).collect();
    Arc::new(AgentContext::with_hw(config, backends).unwrap())
}

fn lock_ref() -> String {
    let lock = std::fs::read(repo().join("uv.lock")).unwrap();
    format!("uv:sha256:{}", mrl_storage::sha256_hex(&lock))
}

fn spec(id: &str, engine: serde_json::Value, extra: serde_json::Value) -> AttemptSpec {
    let rt = lock_ref();
    let mut v = serde_json::json!({
        "attempt": {"job_id": "job_t", "attempt_id": id, "fencing_epoch": 1},
        "engine": "fake",
        "job_spec": {
            "run_id": "run_t", "job_kind": "TRAIN", "algorithm": "x-fake.steps",
            "common": {"seed": 7}, "engine": engine, "switch": {},
            "base_model_sha256": "0".repeat(64)
        },
        "runtimes": [{"runtime_ref": rt, "project": repo(), "package": "mrl-runtime"}],
        "roles": [{"role_id": "trainer", "kind": "trainer", "primary": true, "runtime_ref": rt}]
    });
    if let (Some(base), Some(more)) = (v.as_object_mut(), extra.as_object()) {
        base.extend(more.clone());
    }
    AttemptSpec::from_json(&v.to_string()).unwrap()
}

/// Every event seen so far, shared with the test.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<AttemptEvent>>>);

impl Seen {
    fn spawn(mut rx: AttemptEvents) -> Self {
        let seen = Self::default();
        let s = seen.clone();
        tokio::spawn(async move {
            while let Some(e) = rx.recv().await {
                s.0.lock().unwrap().push(e);
            }
        });
        seen
    }

    fn all(&self) -> Vec<AttemptEvent> {
        self.0.lock().unwrap().clone()
    }

    fn bodies(&self) -> Vec<(Lane, Body)> {
        self.all()
            .into_iter()
            .filter_map(|e| match e {
                AttemptEvent::Relay { lane, relay } => {
                    relay.message.and_then(|m| m.body).map(|b| (lane, b))
                }
                _ => None,
            })
            .collect()
    }

    async fn until(&self, what: &str, f: impl Fn(&[AttemptEvent]) -> bool) {
        tokio::time::timeout(WAIT, async {
            while !f(&self.all()) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    async fn until_step(&self, step: u64) {
        let s = self.clone();
        self.until(&format!("step {step}"), move |_| {
            s.bodies()
                .iter()
                .any(|(_, b)| matches!(b, Body::Progress(p) if p.step >= step))
        })
        .await;
    }
}

fn manager(ctx: &Arc<AgentContext>) -> (Arc<Attempts>, Seen) {
    let (tx, rx) = mrl_agent::attempts::attempt_events(1024);
    (
        Attempts::new(Arc::clone(ctx), tx, None).unwrap(),
        Seen::spawn(rx),
    )
}

fn primary_identity(ctx: &AgentContext, id: &str) -> ProcIdentity {
    let rec = AttemptRecord::load(&ctx.state.job_dir(id))
        .unwrap()
        .unwrap();
    rec.workers
        .into_iter()
        .find(|w| w.primary)
        .unwrap()
        .identity
}

fn pid_of(identity: &ProcIdentity) -> i32 {
    match identity {
        ProcIdentity::Pid { pid, .. } => *pid as i32,
        ProcIdentity::Container { .. } => panic!("process mode"),
    }
}

/// Stores PUT bodies by path.
type Uploads = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

async fn upload_sink() -> (String, Uploads) {
    let got: Uploads = Arc::default();
    async fn store(
        State(s): State<Uploads>,
        UrlPath(p): UrlPath<String>,
        body: Bytes,
    ) -> StatusCode {
        s.lock().unwrap().insert(p, body.to_vec());
        StatusCode::OK
    }
    let app = axum::Router::new()
        .route("/up/{*p}", put(store))
        .with_state(Arc::clone(&got));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/up"), got)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn fake_engine_full_lifecycle_with_ticket_uploads() {
    if !ready("fake_engine_full_lifecycle_with_ticket_uploads") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let (attempts, seen) = manager(&ctx);
    let data = dir.path().join("train.jsonl");
    std::fs::write(&data, b"{\"x\": 1}\n").unwrap();
    let spec = spec(
        "att_life",
        serde_json::json!({"steps": 4, "checkpoint_every": 2}),
        serde_json::json!({"inputs": [{
            "name": "dataset.train", "local_path": "data/train.jsonl",
            "sha256": mrl_storage::sha256_hex(b"{\"x\": 1}\n"), "size": 9, "file": data
        }]}),
    );
    attempts.launch(spec).unwrap();
    let (base, got) = upload_sink().await;

    // Answer each staged checkpoint with an UploadArtifacts command carrying tickets.
    let mut answered = std::collections::HashSet::new();
    let decision = loop {
        for (_, b) in seen.bodies() {
            if let Body::CheckpointStaged(c) = b {
                if answered.insert(c.checkpoint_id.clone()) {
                    let expires = prost_types::Timestamp {
                        seconds: time::OffsetDateTime::now_utc().unix_timestamp() + 600,
                        nanos: 0,
                    };
                    let items = c
                        .files
                        .iter()
                        .map(|f| UploadItem {
                            local_path: f.path.clone(),
                            url: format!("{base}/{}", f.path),
                            sha256: f.sha256.clone(),
                            size: f.size,
                            expires_at: Some(expires),
                        })
                        .collect();
                    let cmd = Command {
                        command_id: format!("up-{}", c.checkpoint_id),
                        kind: Some(Kind::UploadArtifacts(UploadArtifacts {
                            attempt: Some(mrl_proto::common::v1::AttemptRef {
                                job_id: "job_t".into(),
                                attempt_id: "att_life".into(),
                                fencing_epoch: 1,
                            }),
                            checkpoint_id: c.checkpoint_id.clone(),
                            items,
                        })),
                    };
                    let r = attempts.handle(&cmd).await.unwrap();
                    assert!(r.accepted, "{r:?}");
                }
            }
        }
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(200), attempts.wait("att_life")).await
        {
            break d;
        }
    };
    assert_eq!(
        decision.exit_kind,
        ExitKind::Completed as i32,
        "{decision:?}"
    );
    assert!(decision.failure.is_none());

    // Every staged file reached the sink, byte for byte.
    let staged: Vec<_> = seen
        .bodies()
        .into_iter()
        .filter_map(|(_, b)| match b {
            Body::CheckpointStaged(c) => Some(c),
            _ => None,
        })
        .collect();
    assert_eq!(staged.len(), 2);
    let got = got.lock().unwrap();
    for f in staged.iter().flat_map(|c| &c.files) {
        assert_eq!(
            mrl_storage::sha256_hex(&got[&f.path]),
            f.sha256,
            "{}",
            f.path
        );
    }
    // Lanes per Doc 09 §7.
    let lanes: Vec<(Lane, &'static str)> = seen
        .bodies()
        .iter()
        .map(|(l, b)| {
            let kind = match b {
                Body::Metrics(_) => "metrics",
                Body::Log(_) => "log",
                Body::Progress(_) => "progress",
                Body::Exit(_) => "exit",
                _ => "other",
            };
            (*l, kind)
        })
        .collect();
    assert!(lanes.contains(&(Lane::Telemetry, "metrics")));
    assert!(lanes.contains(&(Lane::Control, "progress")));
    assert!(lanes.contains(&(Lane::Control, "exit")));
    // Each data message is relayed once (de-duplicated by seq).
    let seqs: Vec<u64> = seen
        .all()
        .into_iter()
        .filter_map(|e| match e {
            AttemptEvent::Relay { relay, .. } => relay.message.map(|m| m.seq).filter(|s| *s > 0),
            _ => None,
        })
        .collect();
    let unique: std::collections::BTreeSet<_> = seqs.iter().collect();
    assert_eq!(unique.len(), seqs.len());
    // Phases went through to EXITED; the input was staged read-only.
    let phases: Vec<i32> = seen
        .all()
        .into_iter()
        .filter_map(|e| match e {
            AttemptEvent::Status(s) => Some(s.phase),
            _ => None,
        })
        .collect();
    for p in [
        AttemptPhase::Staging,
        AttemptPhase::Preparing,
        AttemptPhase::Running,
        AttemptPhase::Exited,
    ] {
        assert!(phases.contains(&(p as i32)), "{p:?} in {phases:?}");
    }
    let input = ctx
        .state
        .job_dir("att_life")
        .join("inputs/data/train.jsonl");
    assert!(std::fs::metadata(&input).unwrap().permissions().readonly());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn killed_worker_exits_with_a_failure_class() {
    if !ready("killed_worker_exits_with_a_failure_class") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let (attempts, seen) = manager(&ctx);
    let spec = spec(
        "att_kill",
        serde_json::json!({"steps": 60, "step_seconds": 0.5}),
        serde_json::json!({"upload_dir": dir.path().join("out")}),
    );
    attempts.launch(spec).unwrap();
    seen.until_step(2).await;
    let pid = pid_of(&primary_identity(&ctx, "att_kill"));
    nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    let d = tokio::time::timeout(WAIT, attempts.wait("att_kill"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.phase, AttemptPhase::Exited as i32);
    assert_eq!(d.exit_kind, ExitKind::Failed as i32);
    assert_eq!(d.class(), Some(FailureClass::HarnessError));
    assert_eq!(d.failure.unwrap().sub_code, "NO_EXIT_REPORT");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn agent_restart_readopts_or_reports_lost() {
    if !ready("agent_restart_readopts_or_reports_lost") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ctx1 = ctx(dir.path(), None);
    let (first, seen1) = manager(&ctx1);
    let out = dir.path().join("out");
    first
        .launch(spec(
            "att_re",
            serde_json::json!({"steps": 8, "step_seconds": 0.5}),
            serde_json::json!({"upload_dir": out}),
        ))
        .unwrap();
    first
        .launch(spec(
            "att_lost",
            serde_json::json!({"steps": 60, "step_seconds": 0.5}),
            serde_json::json!({"upload_dir": out}),
        ))
        .unwrap();
    seen1.until_step(2).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    // The agent goes away; its workers keep running.
    first.detach_all().await;
    drop(first);
    let lost = primary_identity(&ctx1, "att_lost");
    assert!(lost.alive());
    nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pid_of(&lost)),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    for _ in 0..50 {
        if !lost.alive() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // A new agent on the same state folder.
    let ctx2 = ctx(dir.path(), None);
    let (second, seen2) = manager(&ctx2);
    let statuses = second.readopt().await;
    let of = |id: &str| {
        statuses
            .iter()
            .find(|s| s.attempt.as_ref().is_some_and(|a| a.attempt_id == id))
            .cloned()
            .unwrap()
    };
    assert!(of("att_re").failure.is_none(), "live worker re-adopted");
    let l = of("att_lost");
    assert_eq!(l.failure.unwrap().class, FailureClass::NodeLost as i32);
    let d = tokio::time::timeout(WAIT, second.wait("att_re"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.exit_kind, ExitKind::Completed as i32, "{d:?}");
    assert!(
        d.failure.is_none(),
        "final checkpoint committed after re-adoption"
    );
    // The new agent saw the rest of the run (replayed or new), ending with the exit report.
    assert!(seen2
        .bodies()
        .iter()
        .any(|(_, b)| matches!(b, Body::Exit(_))));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn janitor_leaves_no_processes_or_scratch() {
    if !ready("janitor_leaves_no_processes_or_scratch") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let (attempts, seen) = manager(&ctx);
    // Two sleeping child processes; cancel mid-run.
    let spec = spec(
        "att_jan",
        serde_json::json!({"steps": 60, "step_seconds": 0.5, "children": 2}),
        serde_json::json!({"upload_dir": dir.path().join("out")}),
    );
    attempts.launch(spec).unwrap();
    seen.until_step(2).await;
    let cancel = Command {
        command_id: "c1".into(),
        kind: Some(Kind::CancelAttempt(mrl_proto::agent::v1::CancelAttempt {
            attempt: Some(mrl_proto::common::v1::AttemptRef {
                job_id: "job_t".into(),
                attempt_id: "att_jan".into(),
                fencing_epoch: 1,
            }),
            request_id: "r1".into(),
            grace_s: 10,
            reason: "test".into(),
        })),
    };
    assert!(attempts.handle(&cancel).await.unwrap().accepted);
    let d = tokio::time::timeout(WAIT, attempts.wait("att_jan"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.exit_kind, ExitKind::Cancelled as i32, "{d:?}");
    let job = ctx.state.job_dir("att_jan");
    assert!(
        mrl_agent::janitor::leftovers(
            &job,
            &mrl_agent::attempts::runner::attempt_token(
                &AttemptRecord::load(&job).unwrap().unwrap().nonce
            )
        )
        .is_empty(),
        "no process left in the attempt folder"
    );
    assert!(!primary_identity(&ctx, "att_jan").alive());
    let cleanup = Command {
        command_id: "c2".into(),
        kind: Some(Kind::Cleanup(mrl_proto::agent::v1::Cleanup {
            attempt: Some(mrl_proto::common::v1::AttemptRef {
                job_id: "job_t".into(),
                attempt_id: "att_jan".into(),
                fencing_epoch: 1,
            }),
            purge_scratch: true,
            cache_limit_bytes: 0,
        })),
    };
    assert!(attempts.handle(&cleanup).await.unwrap().accepted);
    for sub in ["work", "staging", "inputs"] {
        assert!(!job.join(sub).exists(), "{sub} wiped");
    }
    // The environment is no longer pinned by the attempt.
    assert!(attempts
        .envs()
        .installed()
        .iter()
        .all(|e| e.pinned_by.is_empty()));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn environment_cache_miss_then_hit() {
    if !ready("environment_cache_miss_then_hit") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let (attempts, _) = manager(&ctx);
    let s = spec("att_env", serde_json::json!({}), serde_json::json!({}));
    let rt = &s.runtimes[0];
    let lines = Arc::new(Mutex::new(Vec::new()));
    let l = Arc::clone(&lines);
    let progress = move |p: mrl_agent::envs::EnvProgress| l.lock().unwrap().push(p.stage);
    let first = attempts
        .envs()
        .materialize(rt, "att_a", &progress)
        .await
        .unwrap();
    assert!(!first.cache_hit);
    assert!(lines.lock().unwrap().contains(&"installing"));
    let second = attempts
        .envs()
        .materialize(rt, "att_b", &progress)
        .await
        .unwrap();
    assert!(second.cache_hit);
    let env = &attempts.envs().installed()[0];
    assert_eq!(env.pinned_by.len(), 2);
    // The verified lock file is in the node cache, pinned.
    let sha = rt.runtime_ref.trim_start_matches("uv:sha256:");
    assert!(ctx.cache.get(sha).unwrap().is_some());
    assert_eq!(attempts.envs().unpin_all("att_a").unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn fake_gpu_oom_is_oom() {
    if !ready("fake_gpu_oom_is_oom") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let hw = Arc::new(FakeHw::with_gpus(1, "Fake A100", 80 << 30));
    let ctx = ctx(dir.path(), Some(Arc::clone(&hw)));
    let (attempts, seen) = manager(&ctx);
    let mut s = spec(
        "att_oom",
        serde_json::json!({"steps": 8, "step_seconds": 0.5, "fail_at_step": 4, "failure": "oom"}),
        serde_json::json!({"upload_dir": dir.path().join("out")}),
    );
    s.roles[0].gpus = vec![0];
    attempts.launch(s).unwrap();
    seen.until_step(2).await;
    hw.inject(Fault::Oom { index: 0 });
    let events: Vec<_> = ctx.hw.iter().flat_map(|b| b.drain_events()).collect();
    assert!(!events.is_empty());
    attempts.hw_events(&events);
    let d = tokio::time::timeout(WAIT, attempts.wait("att_oom"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.class(), Some(FailureClass::Oom), "{d:?}");
}

/// Portable: needs no uv, the lock check fails before anything is installed.
#[tokio::test(flavor = "multi_thread")]
async fn tampered_lock_hash_is_dependency_env() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let (attempts, seen) = manager(&ctx);
    let project = dir.path().join("adapter");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("uv.lock"), b"version = 1\n# tampered\n").unwrap();
    let bad = format!("uv:sha256:{}", mrl_storage::sha256_hex(b"version = 1\n"));
    let v = serde_json::json!({
        "attempt": {"job_id": "job_t", "attempt_id": "att_lock"},
        "engine": "fake",
        "job_spec": {},
        "runtimes": [{"runtime_ref": bad, "project": project}],
        "roles": [{"role_id": "trainer", "primary": true, "runtime_ref": bad}]
    });
    attempts
        .launch(AttemptSpec::from_json(&v.to_string()).unwrap())
        .unwrap();
    let d = tokio::time::timeout(WAIT, attempts.wait("att_lock"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.class(), Some(FailureClass::DependencyEnv));
    assert_eq!(d.failure.as_ref().unwrap().sub_code, "MRL-ENV-201");
    seen.until("the exited status", |e| {
        e.iter().any(|e| matches!(e, AttemptEvent::Status(s) if s.phase == AttemptPhase::Exited as i32 && s.failure.is_some()))
    })
    .await;
    // A second launch of the same attempt id is refused.
    let again = AttemptSpec::from_json(&v.to_string()).unwrap();
    assert_eq!(attempts.launch(again).unwrap_err().code(), "MRL-STATE-204");
}

/// Starts a job for `id` whose child leaves its folder and session, then "crashes" the agent
/// (nothing kills the job) and lets the main process die. Returns the record and the orphan.
#[cfg(target_os = "linux")]
async fn orphaned_job(
    ctx: &AgentContext,
    dir: &Path,
    id: &str,
    nonce: &str,
) -> (AttemptRecord, ProcIdentity) {
    use mrl_agent::attempts::runner::{attempt_token, TOKEN_ENV};
    let job = ctx.state.job_dir(id);
    let pid_file = dir.join(format!("{id}-{nonce}.pid"));
    let script = format!(
        "setsid sh -c 'cd /tmp && echo $$ > {} && exec sleep 300' & exec sleep 300",
        pid_file.display()
    );
    let mut exec_spec =
        mrl_agent::exec::ExecSpec::new(&format!("{id}-trainer"), "/bin/sh", &["-c", &script], &job);
    exec_spec.env.insert(TOKEN_ENV.into(), attempt_token(nonce));
    let exec = ctx.executor.spawn(exec_spec).await.unwrap();
    let main = ProcIdentity::of_pid(exec.pid().unwrap()).unwrap();
    let orphan = loop {
        if let Some(p) = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|t| t.trim().parse::<u32>().ok())
        {
            break p;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    std::mem::forget(exec);
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid_of(&main)),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    let orphan = ProcIdentity::of_pid(orphan).unwrap();
    assert!(orphan.alive());
    let mut spec = spec(id, serde_json::json!({}), serde_json::json!({}));
    spec.attempt.attempt_id = id.into();
    let rec = AttemptRecord {
        spec,
        nonce: nonce.into(),
        socket: dir.join("x.sock"),
        phase: AttemptPhase::Running as i32,
        workers: vec![mrl_agent::attempts::record::WorkerProc {
            role_id: "trainer".into(),
            primary: true,
            identity: main,
            cgroup: None,
        }],
        prepared: Vec::new(),
        started: Vec::new(),
        last_step: 0,
        checkpoints: Default::default(),
        cancel_requested: false,
        exit: None,
        staged_paths: Default::default(),
        lease_deadline_unix_ms: None,
        stop_deadline_unix_ms: None,
        exit_report: None,
        requests: Vec::new(),
        uploads: Default::default(),
        results: Default::default(),
        adhoc_uploads: Default::default(),
    };
    (rec, orphan)
}

#[cfg(target_os = "linux")]
async fn gone(p: &ProcIdentity) -> bool {
    for _ in 0..50 {
        if !p.alive() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// An orphan that changed folder and session after the agent crashed is still found (by the
/// launch token in its environment) and stopped; another agent's job with the same attempt id
/// is left alone. Linux: other processes' environments are readable there.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn janitor_stops_its_orphans_and_never_another_agents() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ctx_a, ctx_b) = (ctx(dir_a.path(), None), ctx(dir_b.path(), None));
    let (rec_a, orphan_a) = orphaned_job(&ctx_a, dir_a.path(), "att_same", "nonce-a").await;
    let (rec_b, orphan_b) = orphaned_job(&ctx_b, dir_b.path(), "att_same", "nonce-b").await;
    let envs_a = mrl_agent::envs::Envs::open(ctx_a.state.root(), Arc::clone(&ctx_a.cache)).unwrap();
    let report = mrl_agent::janitor::release(&ctx_a, &envs_a, &rec_a, &Default::default()).await;
    assert!(report.killed >= 1, "{report:?}");
    assert!(gone(&orphan_a).await, "its own orphan was stopped");
    assert!(
        orphan_b.alive(),
        "the other agent's worker (same attempt id) was not touched"
    );
    let envs_b = mrl_agent::envs::Envs::open(ctx_b.state.root(), Arc::clone(&ctx_b.cache)).unwrap();
    mrl_agent::janitor::release(&ctx_b, &envs_b, &rec_b, &Default::default()).await;
    assert!(gone(&orphan_b).await);
}

/// Credentials sent with UpdateCredentials before the launch reach the worker's environment
/// (checked by hash, never printed).
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn credentials_reach_the_running_worker() {
    if !ready("credentials_reach_the_running_worker") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let (attempts, seen) = manager(&ctx);
    attempts.set_credential(
        "att_cred",
        "hf_hub",
        mrl_agent::attempts::spec::Secret::new("tok-xyz"),
    );
    attempts
        .launch(spec(
            "att_cred",
            serde_json::json!({"steps": 40, "step_seconds": 0.5}),
            serde_json::json!({"upload_dir": dir.path().join("out")}),
        ))
        .unwrap();
    seen.until_step(1).await;
    let pid = pid_of(&primary_identity(&ctx, "att_cred"));
    let environ = std::fs::read(format!("/proc/{pid}/environ")).unwrap();
    let value = environ
        .split(|b| *b == 0)
        .find_map(|kv| kv.strip_prefix(b"MRL_CREDENTIAL_HF_HUB=".as_slice()))
        .expect("the credential is in the worker environment");
    assert_eq!(
        mrl_storage::sha256_hex(value),
        mrl_storage::sha256_hex(b"tok-xyz")
    );
    let cancel = Command {
        command_id: "c".into(),
        kind: Some(Kind::CancelAttempt(mrl_proto::agent::v1::CancelAttempt {
            attempt: Some(mrl_proto::common::v1::AttemptRef {
                job_id: "job_t".into(),
                attempt_id: "att_cred".into(),
                fencing_epoch: 1,
            }),
            request_id: "r".into(),
            grace_s: 5,
            reason: "done".into(),
        })),
    };
    attempts.handle(&cancel).await.unwrap();
    tokio::time::timeout(WAIT, attempts.wait("att_cred"))
        .await
        .unwrap();
}

/// A real agent crash: `mrl-agent attempt run` is SIGKILLed mid-run, its worker keeps running,
/// and running the command again re-adopts it to a COMPLETED end with every checkpoint.
async fn crash_and_readopt(test: &str, sandbox: &str, id: &str) {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let config = dir.path().join("agent.toml");
    std::fs::write(
        &config,
        format!(
            // No disk floor: this test is about re-adoption, not the space check.
            "state_dir = {:?}\n[cache]\nmin_free_bytes = 0\n[sandbox]\nbackend = {sandbox:?}\n",
            state.display().to_string()
        ),
    )
    .unwrap();
    let out = dir.path().join("out");
    let mut s = spec(
        id,
        serde_json::json!({"steps": 12, "checkpoint_every": 2, "step_seconds": 0.5}),
        serde_json::json!({"upload_dir": out}),
    );
    s.attempt.attempt_id = id.into();
    let spec_file = dir.path().join("spec.json");
    std::fs::write(&spec_file, serde_json::to_vec(&s).unwrap()).unwrap();
    let agent = |log: &str| {
        let log = std::fs::File::create(dir.path().join(log)).unwrap();
        std::process::Command::new(env!("CARGO_BIN_EXE_mrl-agent"))
            .args([
                "--config",
                &config.display().to_string(),
                "attempt",
                "run",
                "--spec",
            ])
            .arg(&spec_file)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap()
    };
    let job = state.join("jobs").join(id);
    let mut first = agent("first.log");
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        let staged = AttemptRecord::load(&job)
            .ok()
            .flatten()
            .is_some_and(|r| !r.checkpoints.is_empty());
        if staged {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{test}: no checkpoint before the crash"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    first.kill().unwrap();
    first.wait().unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let worker = AttemptRecord::load(&job).unwrap().unwrap().workers[0]
        .identity
        .clone();
    assert!(worker.alive(), "{test}: the worker outlived the agent");

    let mut second = agent("second.log");
    let status = tokio::task::spawn_blocking(move || second.wait())
        .await
        .unwrap()
        .unwrap();
    let log = std::fs::read_to_string(dir.path().join("second.log")).unwrap();
    assert!(status.success(), "{test}: {log}");
    assert!(log.contains(&format!("re-adopted attempt {id}")), "{log}");
    assert!(log.contains("Attempt ended: EXIT_KIND_COMPLETED"), "{log}");
    let ckpts = std::fs::read_dir(out.join("checkpoints")).unwrap().count();
    assert_eq!(ckpts, 6, "{test}: every checkpoint was committed");
}

/// Two `attempt run`s on one state folder at once, for the same attempt and for another one:
/// the second process refuses to start (one agent process per state folder) and the first
/// finishes undisturbed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn a_second_cli_run_never_takes_over() {
    if !ready("a_second_cli_run_never_takes_over") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let config = dir.path().join("agent.toml");
    std::fs::write(
        &config,
        format!(
            "state_dir = {:?}\n[cache]\nmin_free_bytes = 0\n[sandbox]\nbackend = \"none\"\n",
            state.display().to_string()
        ),
    )
    .unwrap();
    let out = dir.path().join("out");
    let s = spec(
        "att_two_cli",
        serde_json::json!({"steps": 8, "checkpoint_every": 2, "step_seconds": 0.5}),
        serde_json::json!({"upload_dir": out}),
    );
    let spec_file = dir.path().join("spec.json");
    std::fs::write(&spec_file, serde_json::to_vec(&s).unwrap()).unwrap();
    let mut other = s.clone();
    other.attempt.attempt_id = "att_other_cli".into();
    let other_file = dir.path().join("other.json");
    std::fs::write(&other_file, serde_json::to_vec(&other).unwrap()).unwrap();
    let agent = |log: &str, spec: &Path| {
        let log = std::fs::File::create(dir.path().join(log)).unwrap();
        std::process::Command::new(env!("CARGO_BIN_EXE_mrl-agent"))
            .args([
                "--config",
                &config.display().to_string(),
                "attempt",
                "run",
                "--spec",
            ])
            .arg(spec)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap()
    };
    let job = state.join("jobs").join("att_two_cli");
    let mut first = agent("first.log", &spec_file);
    let deadline = std::time::Instant::now() + WAIT;
    while !AttemptRecord::load(&job)
        .ok()
        .flatten()
        .is_some_and(|r| !r.workers.is_empty())
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the first run never started"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for (log, spec) in [("same.log", &spec_file), ("other.log", &other_file)] {
        let second = agent(log, spec);
        let status = tokio::task::spawn_blocking(move || second.wait_with_output())
            .await
            .unwrap()
            .unwrap()
            .status;
        let text = std::fs::read_to_string(dir.path().join(log)).unwrap();
        assert!(!status.success(), "{log}: {text}");
        assert!(text.contains("another mrl-agent"), "{log}: {text}");
    }
    assert!(
        !state.join("jobs").join("att_other_cli").exists(),
        "the refused run touched nothing"
    );
    let status = tokio::task::spawn_blocking(move || first.wait())
        .await
        .unwrap()
        .unwrap();
    let log = std::fs::read_to_string(dir.path().join("first.log")).unwrap();
    assert!(status.success(), "{log}");
    assert!(log.contains("Attempt ended: EXIT_KIND_COMPLETED"), "{log}");
    assert_eq!(
        std::fs::read_dir(out.join("checkpoints")).unwrap().count(),
        4
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn real_agent_crash_is_readopted() {
    if ready("real_agent_crash_is_readopted") {
        crash_and_readopt("no sandbox", "none", "att_crash_plain").await;
    }
}

/// The same crash with the bubblewrap `worker` profile (Linux, needs bwrap and user namespaces;
/// required in CI with `MRL_TEST_SANDBOX_REQUIRED=1`).
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv and bubblewrap; runs in ci-attempts"]
async fn real_agent_crash_is_readopted_under_bubblewrap() {
    if !ready("real_agent_crash_is_readopted_under_bubblewrap") {
        return;
    }
    let cfg = mrl_agent::config::SandboxConfig {
        backend: SandboxChoice::Bubblewrap,
        bwrap_path: None,
    };
    if let Err(e) = mrl_agent::sandbox::select(&cfg) {
        assert!(
            std::env::var("MRL_TEST_SANDBOX_REQUIRED").is_err(),
            "bubblewrap is required but can't run: {}",
            e.explain()
        );
        eprintln!(
            "SKIPPED real_agent_crash_is_readopted_under_bubblewrap: {}",
            e.explain()
        );
        return;
    }
    crash_and_readopt("bubblewrap", "bubblewrap", "att_crash_bwrap").await;
}

fn old_record(
    dir: &Path,
    id: &str,
    nonce: &str,
    identity: ProcIdentity,
    cgroup: Option<PathBuf>,
) -> AttemptRecord {
    let mut spec = spec(id, serde_json::json!({}), serde_json::json!({}));
    spec.attempt.attempt_id = id.into();
    AttemptRecord {
        spec,
        nonce: nonce.into(),
        socket: dir.join("x.sock"),
        phase: AttemptPhase::Exited as i32,
        workers: vec![mrl_agent::attempts::record::WorkerProc {
            role_id: "trainer".into(),
            primary: true,
            identity,
            cgroup,
        }],
        prepared: Vec::new(),
        started: Vec::new(),
        last_step: 0,
        checkpoints: Default::default(),
        cancel_requested: false,
        exit: None,
        staged_paths: Default::default(),
        lease_deadline_unix_ms: None,
        stop_deadline_unix_ms: None,
        exit_report: None,
        requests: Vec::new(),
        uploads: Default::default(),
        results: Default::default(),
        adhoc_uploads: Default::default(),
    }
}

/// An old record whose saved cgroup now holds another launch's processes leaves them alone.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn an_old_record_never_kills_another_launchs_cgroup() {
    use mrl_agent::attempts::runner::{attempt_token, TOKEN_ENV};
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let envs = mrl_agent::envs::Envs::open(ctx.state.root(), Arc::clone(&ctx.cache)).unwrap();
    let mut newer = std::process::Command::new("sleep")
        .arg("300")
        .env(TOKEN_ENV, attempt_token("new-launch"))
        .spawn()
        .unwrap();
    // A stand-in cgroup folder (same files the kernel's has) holding the newer process.
    let cg = dir.path().join("job-att_same-trainer");
    std::fs::create_dir_all(&cg).unwrap();
    std::fs::write(cg.join("cgroup.procs"), format!("{}\n", newer.id())).unwrap();
    let gone_worker = ProcIdentity::Pid {
        pid: u32::MAX - 3,
        start_time: 1,
    };
    let rec = old_record(
        dir.path(),
        "att_same",
        "old-launch",
        gone_worker,
        Some(cg.clone()),
    );
    let report = mrl_agent::janitor::release(&ctx, &envs, &rec, &Default::default()).await;
    assert!(!cg.join("cgroup.kill").exists(), "nothing was killed");
    assert!(
        newer.try_wait().unwrap().is_none(),
        "the newer launch keeps running"
    );
    assert!(
        report
            .errors
            .iter()
            .any(|e| e.contains("not this launch's")),
        "{report:?}"
    );
    newer.kill().unwrap();
    newer.wait().unwrap();
}

fn docker_ready(test: &str) -> bool {
    let ok = mrl_agent::envs::which("docker").is_some_and(|d| {
        std::process::Command::new(d)
            .args(["pull", "-q", "busybox:1.36"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    });
    if !ok {
        assert!(
            std::env::var("MRL_TEST_DOCKER_REQUIRED").is_err(),
            "{test}: Docker is required but can't run busybox"
        );
        eprintln!("SKIPPED {test}: Docker is not available");
    }
    ok
}

/// An old record never removes a container a newer launch runs under the same name.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; runs in ci-attempts"]
async fn an_old_record_never_removes_a_newer_container() {
    use mrl_agent::attempts::runner::{attempt_token, TOKEN_ENV};
    if !docker_ready("an_old_record_never_removes_a_newer_container") {
        return;
    }
    let docker = mrl_agent::envs::which("docker").unwrap();
    let name = format!("mrl-test-{}", std::process::id());
    let run = std::process::Command::new(&docker)
        .args(["run", "-d", "--rm", "--name", &name, "-e"])
        .arg(format!("{TOKEN_ENV}={}", attempt_token("new-launch")))
        .args(["busybox:1.36", "sleep", "300"])
        .stdout(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(run.success());
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let envs = mrl_agent::envs::Envs::open(ctx.state.root(), Arc::clone(&ctx.cache)).unwrap();
    let identity = ProcIdentity::Container {
        name: name.clone(),
        id: None,
    };
    let old = old_record(dir.path(), "att_c1", "old-launch", identity.clone(), None);
    let report = mrl_agent::janitor::release(&ctx, &envs, &old, &Default::default()).await;
    assert!(
        identity.alive(),
        "the newer launch's container survived: {report:?}"
    );
    let new = old_record(dir.path(), "att_c1", "new-launch", identity.clone(), None);
    mrl_agent::janitor::release(&ctx, &envs, &new, &Default::default()).await;
    for _ in 0..50 {
        if !identity.alive() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!identity.alive(), "its own launch removes it");
}

/// After an agent restart, an expired lease on an old record never stops the newer launch's
/// container that took over the name; the old attempt is reported lost. The container's own
/// launch, re-adopted with the same expired lease, is bound to its id and stopped.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; runs in ci-attempts"]
async fn a_restart_never_adopts_a_newer_container() {
    use mrl_agent::attempts::runner::{attempt_token, now_ms, TOKEN_ENV};
    if !docker_ready("a_restart_never_adopts_a_newer_container") {
        return;
    }
    let docker = mrl_agent::envs::which("docker").unwrap();
    let name = format!("mrl-test-r{}", std::process::id());
    let run = std::process::Command::new(&docker)
        .args(["run", "-d", "--rm", "--name", &name, "-e"])
        .arg(format!("{TOKEN_ENV}={}", attempt_token("new-launch")))
        .args(["busybox:1.36", "sleep", "300"])
        .stdout(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(run.success());
    let identity = ProcIdentity::Container {
        name: name.clone(),
        id: None,
    };
    let save = |ctx: &AgentContext, id: &str, nonce: &str| {
        let root = ctx.state.root();
        let mut rec = old_record(root, id, nonce, identity.clone(), None);
        rec.phase = AttemptPhase::Running as i32;
        rec.socket = mrl_agent::attempts::runner::socket_path(root, id);
        rec.lease_deadline_unix_ms = Some(now_ms() - 1000);
        let job = ctx.state.job_dir(id);
        std::fs::create_dir_all(&job).unwrap();
        rec.save(&job).unwrap();
    };
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    save(&ctx, "att_r1", "old-launch");
    let (attempts, _seen) = manager(&ctx);
    let statuses = attempts.readopt().await;
    let old = statuses
        .iter()
        .find(|s| s.attempt.as_ref().is_some_and(|a| a.attempt_id == "att_r1"))
        .unwrap();
    let failure = old.failure.as_ref().unwrap();
    assert_eq!(failure.sub_code, "WORKER_LOST", "{old:?}");
    // Had it been adopted, its expired lease would have fenced it and stopped the container
    // before this returns.
    let _ = tokio::time::timeout(WAIT, attempts.wait("att_r1")).await;
    assert!(identity.alive(), "the newer launch's container survived");

    // The container's own launch: adopted by id, fenced by its expired lease, stopped.
    let dir2 = tempfile::tempdir().unwrap();
    let ctx2 = self::ctx(dir2.path(), None);
    save(&ctx2, "att_r2", "new-launch");
    let (attempts2, _seen2) = manager(&ctx2);
    attempts2.readopt().await;
    let rec = AttemptRecord::load(&ctx2.state.job_dir("att_r2"))
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            &rec.workers[0].identity,
            ProcIdentity::Container { id: Some(_), .. }
        ),
        "bound to the container id: {:?}",
        rec.workers
    );
    let d = tokio::time::timeout(WAIT, attempts2.wait("att_r2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.phase, AttemptPhase::SelfFenced as i32, "{d:?}");
    for _ in 0..100 {
        if !identity.alive() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!identity.alive(), "its own launch stopped it");
}

/// A container that ended while the agent was down: its saved report says it was cancelled
/// cleanly, Docker says exit code 3. Recovery reads Docker's status and reports the mismatch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; runs in ci-attempts"]
async fn a_container_that_exited_while_the_agent_was_down_is_checked() {
    use mrl_agent::attempts::runner::{attempt_token, TOKEN_ENV};
    if !docker_ready("a_container_that_exited_while_the_agent_was_down_is_checked") {
        return;
    }
    let docker = mrl_agent::envs::which("docker").unwrap();
    let name = format!("mrl-test-x{}", std::process::id());
    let _ = std::process::Command::new(&docker)
        .args(["rm", "-f", &name])
        .output();
    let ran = std::process::Command::new(&docker)
        .args(["run", "--name", &name, "-e"])
        .arg(format!("{TOKEN_ENV}={}", attempt_token("down-launch")))
        .args(["busybox:1.36", "sh", "-c", "exit 3"])
        .stdout(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(ran.code(), Some(3));
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let identity = ProcIdentity::Container {
        name: name.clone(),
        id: None,
    };
    let mut rec = old_record(ctx.state.root(), "att_down", "down-launch", identity, None);
    rec.phase = AttemptPhase::Running as i32;
    rec.socket = mrl_agent::attempts::runner::socket_path(ctx.state.root(), "att_down");
    rec.cancel_requested = true;
    rec.exit_report = Some(prost::Message::encode_to_vec(
        &mrl_proto::worker::v1::ExitReport {
            kind: ExitKind::Cancelled as i32,
            ..Default::default()
        },
    ));
    let job = ctx.state.job_dir("att_down");
    std::fs::create_dir_all(&job).unwrap();
    rec.save(&job).unwrap();
    let (attempts, _seen) = manager(&ctx);
    let statuses = attempts.readopt().await;
    let s = statuses
        .iter()
        .find(|s| {
            s.attempt
                .as_ref()
                .is_some_and(|a| a.attempt_id == "att_down")
        })
        .unwrap();
    assert_eq!(
        s.failure.as_ref().map(|f| f.sub_code.as_str()),
        Some("EXIT_STATUS_MISMATCH"),
        "{s:?}"
    );
    let _ = std::process::Command::new(&docker)
        .args(["rm", "-f", &name])
        .output();
}

/// `attempt stop-all` (what `service uninstall` runs once the service is down) ends the
/// workers the stopped service left running and records how their attempts ended.
#[tokio::test(flavor = "multi_thread")]
async fn stop_all_ends_workers_the_service_left_running() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let mut worker = std::process::Command::new("sleep")
        .arg("300")
        .spawn()
        .unwrap();
    let identity = ProcIdentity::of_pid(worker.id()).unwrap();
    let mut rec = old_record(ctx.state.root(), "att_left", "left-launch", identity, None);
    rec.phase = AttemptPhase::Running as i32;
    rec.socket = mrl_agent::attempts::runner::socket_path(ctx.state.root(), "att_left");
    let job = ctx.state.job_dir("att_left");
    std::fs::create_dir_all(&job).unwrap();
    rec.save(&job).unwrap();
    let config = dir.path().join("agent.toml");
    std::fs::write(
        &config,
        format!(
            "state_dir = {:?}\n[cache]\nmin_free_bytes = 0\n",
            ctx.state.root().display().to_string()
        ),
    )
    .unwrap();
    // This test's own manager must not hold the state folder.
    drop(ctx);
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_mrl-agent"))
            .args([
                "--config",
                &config.display().to_string(),
                "attempt",
                "stop-all",
            ])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{err}");
    assert!(err.contains("stopped 1 running attempt"), "{err}");
    assert!(
        worker.try_wait().unwrap().is_some(),
        "the worker was stopped"
    );
    let rec = AttemptRecord::load(&job).unwrap().unwrap();
    assert!(rec.exit.is_some(), "its outcome is recorded");
}

/// The lease runs out while the event reader is stuck: the launch ends fenced and no worker
/// is ever started.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs uv; runs in ci-attempts"]
async fn an_expired_launch_starts_no_worker() {
    if !ready("an_expired_launch_starts_no_worker") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let ctx = ctx(dir.path(), None);
    let (tx, _never_read) = mrl_agent::attempts::attempt_events(1);
    let attempts = Attempts::new(Arc::clone(&ctx), tx, None).unwrap();
    attempts
        .launch(spec(
            "att_exp",
            serde_json::json!({"steps": 4}),
            serde_json::json!({"lease_s": 2, "upload_dir": dir.path().join("out")}),
        ))
        .unwrap();
    let d = tokio::time::timeout(WAIT, attempts.wait("att_exp"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.phase, AttemptPhase::SelfFenced as i32, "{d:?}");
    let rec = AttemptRecord::load(&ctx.state.job_dir("att_exp"))
        .unwrap()
        .unwrap();
    assert!(rec.workers.is_empty(), "no worker was started");
}
