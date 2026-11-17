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
        mrl_agent::janitor::leftovers(&job).is_empty(),
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
