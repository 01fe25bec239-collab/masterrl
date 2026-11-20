//! Attempt commands that must be refused (every OS).

use std::sync::Arc;

use mrl_agent::attempts::Attempts;
use mrl_agent::config::{AgentConfig, CacheConfig, SandboxChoice, SandboxConfig};
use mrl_agent::AgentContext;
use mrl_proto::agent::v1::command::Kind;
use mrl_proto::agent::v1::{Cleanup, Command};
use mrl_proto::common::v1::AttemptRef;

#[tokio::test]
async fn cleanup_refuses_an_attempt_id_that_leaves_the_state_folder() {
    let dir = tempfile::tempdir().unwrap();
    let config = AgentConfig {
        state_dir: dir.path().join("agent").join("state"),
        cache: CacheConfig {
            max_bytes: Some(1 << 20),
            min_free_bytes: 0,
        },
        sandbox: SandboxConfig {
            backend: SandboxChoice::None,
            bwrap_path: None,
        },
        ..AgentConfig::default()
    };
    let ctx = Arc::new(AgentContext::with_hw(config, Vec::new()).unwrap());
    let (tx, _rx) = mrl_agent::attempts::attempt_events(8);
    let attempts = Attempts::new(ctx, tx, None).unwrap();
    // `<state>/jobs/../../victim` is `<dir>/agent/victim`.
    let victim = dir.path().join("agent").join("victim").join("work");
    std::fs::create_dir_all(&victim).unwrap();
    for id in ["../../victim", "..", "a/b", ""] {
        let cmd = Command {
            command_id: "c".into(),
            kind: Some(Kind::Cleanup(Cleanup {
                attempt: Some(AttemptRef {
                    job_id: "job_x".into(),
                    attempt_id: id.into(),
                    fencing_epoch: 0,
                }),
                purge_scratch: true,
                cache_limit_bytes: 0,
            })),
        };
        let r = attempts.handle(&cmd).await.unwrap();
        if !id.is_empty() {
            assert!(!r.accepted, "{id:?} refused");
            assert_eq!(r.failure.unwrap().sub_code, "MRL-VAL-200");
        }
    }
    assert!(
        victim.exists(),
        "nothing outside the state folder was deleted"
    );
}

fn manager(
    dir: &std::path::Path,
) -> (
    Arc<AgentContext>,
    Arc<Attempts>,
    mrl_agent::attempts::AttemptEvents,
) {
    let config = AgentConfig {
        state_dir: dir.join("state"),
        cache: CacheConfig {
            max_bytes: Some(1 << 20),
            min_free_bytes: 0,
        },
        sandbox: SandboxConfig {
            backend: SandboxChoice::None,
            bwrap_path: None,
        },
        ..AgentConfig::default()
    };
    let ctx = Arc::new(AgentContext::with_hw(config, Vec::new()).unwrap());
    let (tx, rx) = mrl_agent::attempts::attempt_events(256);
    let attempts = Attempts::new(Arc::clone(&ctx), tx, None).unwrap();
    (ctx, attempts, rx)
}

/// A spec whose runtime lock never matches (it fails right after staging, without uv).
fn bad_lock_spec(
    dir: &std::path::Path,
    id: &str,
    job: &str,
    epoch: u64,
    extra: serde_json::Value,
) -> mrl_agent::attempts::AttemptSpec {
    let project = dir.join("adapter");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("uv.lock"), b"version = 1\n# changed\n").unwrap();
    let rt = format!("uv:sha256:{}", mrl_storage::sha256_hex(b"version = 1\n"));
    let mut v = serde_json::json!({
        "attempt": {"job_id": job, "attempt_id": id, "fencing_epoch": epoch},
        "engine": "fake",
        "job_spec": {},
        "runtimes": [{"runtime_ref": rt, "project": project}],
        "roles": [{"role_id": "trainer", "primary": true, "runtime_ref": rt}]
    });
    if let (Some(b), Some(m)) = (v.as_object_mut(), extra.as_object()) {
        b.extend(m.clone());
    }
    mrl_agent::attempts::AttemptSpec::from_json(&v.to_string()).unwrap()
}

const WAIT: std::time::Duration = std::time::Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
async fn inputs_are_read_only_and_ended_attempts_are_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, first, _rx) = manager(dir.path());
    let data = dir.path().join("train.jsonl");
    std::fs::write(&data, b"{}\n").unwrap();
    let input = serde_json::json!({"inputs": [{
        "name": "dataset.train", "local_path": "data/train.jsonl",
        "sha256": mrl_storage::sha256_hex(b"{}\n"), "size": 3, "file": data
    }]});
    first
        .launch(bad_lock_spec(dir.path(), "att_e1", "job_e", 5, input))
        .unwrap();
    let d = tokio::time::timeout(WAIT, first.wait("att_e1"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.failure.is_some());
    let staged = ctx
        .state
        .job_dir("att_e1")
        .join("inputs")
        .join("data")
        .join("train.jsonl");
    assert!(
        std::fs::metadata(&staged).unwrap().permissions().readonly(),
        "inputs are read-only"
    );
    // A process-mode worker runs as the agent's user: undoing the read-only bit and writing
    // its input never reaches the cached object.
    let mut perms = std::fs::metadata(&staged).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    std::fs::set_permissions(&staged, perms).unwrap();
    std::fs::write(&staged, b"tampered").unwrap();
    let object = ctx
        .cache
        .get(&mrl_storage::sha256_hex(b"{}\n"))
        .unwrap()
        .unwrap();
    assert_eq!(
        std::fs::read(&object).unwrap(),
        b"{}\n",
        "the cache kept its bytes"
    );

    // A new agent on the same state: the ended attempt is reported, its epoch remembered.
    let (tx, _rx2) = mrl_agent::attempts::attempt_events(16);
    let second = Attempts::new(Arc::clone(&ctx), tx, None).unwrap();
    let older = bad_lock_spec(dir.path(), "att_e2", "job_e", 3, serde_json::json!({}));
    assert_eq!(
        second.launch(older).unwrap_err().code(),
        "MRL-STATE-204",
        "stale epoch"
    );
    let statuses = second.readopt().await;
    let s = statuses
        .iter()
        .find(|s| s.attempt.as_ref().is_some_and(|a| a.attempt_id == "att_e1"))
        .expect("the saved outcome is in the reconcile report");
    assert_eq!(s.failure.as_ref().unwrap().sub_code, "MRL-ENV-201");
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_sent_before_launch_reach_the_worker_environment() {
    let dir = tempfile::tempdir().unwrap();
    let (_ctx, attempts, _rx) = manager(dir.path());
    let cmd = Command {
        command_id: "cred".into(),
        kind: Some(Kind::UpdateCredentials(
            mrl_proto::agent::v1::UpdateCredentials {
                attempt: Some(AttemptRef {
                    job_id: "job_c".into(),
                    attempt_id: "att_c".into(),
                    fencing_epoch: 1,
                }),
                credentials: vec![mrl_proto::agent::v1::Credential {
                    name: "hf_hub".into(),
                    value: b"tok-123".to_vec(),
                    expires_at: None,
                }],
            },
        )),
    };
    assert!(attempts.handle(&cmd).await.unwrap().accepted);
    let spec = bad_lock_spec(dir.path(), "att_c", "job_c", 1, serde_json::json!({}));
    let env = attempts.secrets_for(&spec).unwrap();
    let (name, value) = env
        .iter()
        .find(|(n, _)| n == "MRL_CREDENTIAL_HF_HUB")
        .expect("granted");
    assert_eq!(name, "MRL_CREDENTIAL_HF_HUB");
    assert_eq!(
        mrl_storage::sha256_hex(value.expose().as_bytes()),
        mrl_storage::sha256_hex(b"tok-123")
    );
    assert!(!format!("{env:?}").contains("tok-123"), "never printed");
}

/// Two managers on one state folder (two `attempt run`s, or an agent and an `attempt run`):
/// the second never re-adopts, declares lost or relaunches what the first runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_manager_leaves_a_running_attempt_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, first, _rx) = manager(dir.path());
    let url = stuck_link().await;
    let spec = bad_lock_spec(dir.path(), "att_own", "job_o", 1, slow_input(&url));
    first.launch(spec.clone()).unwrap();
    let (tx, _rx2) = mrl_agent::attempts::attempt_events(16);
    let second = Attempts::new(Arc::clone(&ctx), tx, None).unwrap();
    assert!(
        second.readopt().await.is_empty(),
        "nothing taken over or reported"
    );
    assert!(second.running().is_empty());
    assert!(second.launch(spec).is_err());
    let rec = mrl_agent::attempts::record::AttemptRecord::load(&ctx.state.job_dir("att_own"))
        .unwrap()
        .unwrap();
    assert!(
        rec.exit.is_none(),
        "still the first manager's, not declared lost"
    );
    assert_eq!(first.running().len(), 1);
    first.handle(&cancel_cmd("job_o", "att_own")).await.unwrap();
    tokio::time::timeout(WAIT, first.wait("att_own"))
        .await
        .unwrap();
}

/// Data sent ahead of a launch belongs to the job and epoch that sent it: another job or an
/// older epoch is refused, a launch for another epoch is refused, and an accepted launch
/// consumes the credentials.
#[tokio::test(flavor = "multi_thread")]
async fn pre_launch_data_must_match_the_launch() {
    let dir = tempfile::tempdir().unwrap();
    let (_ctx, attempts, _rx) = manager(dir.path());
    let cred = |job: &str, epoch: u64| Command {
        command_id: "cred".into(),
        kind: Some(Kind::UpdateCredentials(
            mrl_proto::agent::v1::UpdateCredentials {
                attempt: Some(AttemptRef {
                    job_id: job.into(),
                    attempt_id: "att_m".into(),
                    fencing_epoch: epoch,
                }),
                credentials: vec![mrl_proto::agent::v1::Credential {
                    name: "hf_hub".into(),
                    value: b"tok".to_vec(),
                    expires_at: None,
                }],
            },
        )),
    };
    assert!(attempts.handle(&cred("job_m", 2)).await.unwrap().accepted);
    assert!(
        !attempts
            .handle(&cred("job_other", 2))
            .await
            .unwrap()
            .accepted,
        "another job"
    );
    assert!(
        !attempts.handle(&cred("job_m", 1)).await.unwrap().accepted,
        "older epoch"
    );
    let url = stuck_link().await;
    let other_epoch = bad_lock_spec(dir.path(), "att_m", "job_m", 3, slow_input(&url));
    let e = attempts.launch(other_epoch).unwrap_err();
    assert_eq!(e.code(), "MRL-STATE-204", "{}", e.explain());
    let spec = bad_lock_spec(dir.path(), "att_m", "job_m", 2, slow_input(&url));
    attempts.launch(spec.clone()).unwrap();
    // Used by the launch, then gone: nothing is left for this attempt id.
    let env = attempts.secrets_for(&spec).unwrap();
    assert!(
        env.iter().all(|(n, _)| n != "MRL_CREDENTIAL_HF_HUB"),
        "consumed"
    );
    assert!(
        !attempts.handle(&cred("job_m", 2)).await.unwrap().accepted,
        "after the launch"
    );
    let mut cancel = cancel_cmd("job_m", "att_m");
    if let Some(Kind::CancelAttempt(c)) = &mut cancel.kind {
        c.attempt.as_mut().unwrap().fencing_epoch = 2;
    }
    assert!(attempts.handle(&cancel).await.unwrap().accepted);
    tokio::time::timeout(WAIT, attempts.wait("att_m"))
        .await
        .unwrap();
}

/// A link that never answers, so staging hangs.
async fn stuck_link() -> String {
    let app = axum::Router::new().route(
        "/slow",
        axum::routing::get(|| async {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            "late"
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/slow")
}

fn slow_input(url: &str) -> serde_json::Value {
    let expires = time::OffsetDateTime::now_utc() + time::Duration::minutes(10);
    serde_json::json!({"inputs": [{
        "name": "model", "local_path": "model/w.bin", "sha256": "c".repeat(64), "size": 4,
        "url": url, "expires_at": expires.format(&time::format_description::well_known::Rfc3339).unwrap()
    }]})
}

#[tokio::test(flavor = "multi_thread")]
async fn the_lease_and_a_cancel_both_stop_a_launch_that_is_still_staging() {
    let dir = tempfile::tempdir().unwrap();
    let (_ctx, attempts, _rx) = manager(dir.path());
    let url = stuck_link().await;
    let mut fenced = slow_input(&url);
    fenced["lease_s"] = serde_json::json!(1);
    attempts
        .launch(bad_lock_spec(dir.path(), "att_sl", "job_s", 1, fenced))
        .unwrap();
    let d = tokio::time::timeout(WAIT, attempts.wait("att_sl"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        d.phase,
        mrl_proto::agent::v1::AttemptPhase::SelfFenced as i32,
        "{d:?}"
    );

    attempts
        .launch(bad_lock_spec(
            dir.path(),
            "att_sc",
            "job_t",
            1,
            slow_input(&url),
        ))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let cancel = Command {
        command_id: "c".into(),
        kind: Some(Kind::CancelAttempt(mrl_proto::agent::v1::CancelAttempt {
            attempt: Some(AttemptRef {
                job_id: "job_t".into(),
                attempt_id: "att_sc".into(),
                fencing_epoch: 1,
            }),
            request_id: "r".into(),
            grace_s: 5,
            reason: "test".into(),
        })),
    };
    assert!(attempts.handle(&cancel).await.unwrap().accepted);
    let d = tokio::time::timeout(WAIT, attempts.wait("att_sc"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        d.exit_kind,
        mrl_proto::worker::v1::ExitKind::Cancelled as i32,
        "{d:?}"
    );
}

fn attempt_ref(job: &str, id: &str) -> Option<AttemptRef> {
    Some(AttemptRef {
        job_id: job.into(),
        attempt_id: id.into(),
        fencing_epoch: 1,
    })
}

fn cancel_cmd(job: &str, id: &str) -> Command {
    Command {
        command_id: format!("cancel-{id}"),
        kind: Some(Kind::CancelAttempt(mrl_proto::agent::v1::CancelAttempt {
            attempt: attempt_ref(job, id),
            request_id: "r".into(),
            grace_s: 5,
            reason: "test".into(),
        })),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_launch_keeps_its_inputs_and_an_id_launches_once() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, attempts, _rx) = manager(dir.path());
    let url = stuck_link().await;
    let stage = Command {
        command_id: "stage".into(),
        kind: Some(Kind::StageArtifacts(mrl_proto::agent::v1::StageArtifacts {
            attempt: attempt_ref("job_g", "att_g"),
            items: vec![mrl_proto::agent::v1::StageItem {
                name: "model".into(),
                url: url.clone(),
                sha256: "d".repeat(64),
                size: 4,
                local_path: "model/w.bin".into(),
                expires_at: Some(prost_types::Timestamp {
                    seconds: time::OffsetDateTime::now_utc().unix_timestamp() + 600,
                    nanos: 0,
                }),
            }],
        })),
    };
    assert!(attempts.handle(&stage).await.unwrap().accepted);
    // The installed adapter project for the gateway path.
    let rt = format!("uv:sha256:{}", "e".repeat(64));
    let project = ctx.state.root().join("runtimes").join(rt.replace(':', "-"));
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("uv.lock"), b"version = 1\n").unwrap();
    let launch = |json: &str| Command {
        command_id: "launch".into(),
        kind: Some(Kind::LaunchAttempt(mrl_proto::agent::v1::LaunchAttempt {
            attempt: attempt_ref("job_g", "att_g"),
            engine_name: "fake".into(),
            job_spec_json: json.into(),
            roles: vec![mrl_proto::agent::v1::RoleLaunch {
                role_id: "trainer".into(),
                kind: "trainer".into(),
                primary: true,
                count: 1,
                gpu_indices: Vec::new(),
                runtime_ref: rt.clone(),
            }],
            ..Default::default()
        })),
    };
    assert!(!attempts.handle(&launch("not json")).await.unwrap().accepted);
    assert!(
        attempts.handle(&launch("{}")).await.unwrap().accepted,
        "the retry is accepted"
    );
    let rec = mrl_agent::attempts::record::AttemptRecord::load(&ctx.state.job_dir("att_g"))
        .unwrap()
        .unwrap();
    assert_eq!(
        rec.spec.inputs.len(),
        1,
        "the staged input survived the rejected launch"
    );
    attempts
        .handle(&cancel_cmd("job_g", "att_g"))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, attempts.wait("att_g"))
        .await
        .unwrap();

    // Two launches of one id at the same moment: exactly one is accepted.
    let spec = bad_lock_spec(dir.path(), "att_twice", "job_h", 1, slow_input(&url));
    let (a, b) = (Arc::clone(&attempts), Arc::clone(&attempts));
    let (s1, s2) = (spec.clone(), spec);
    let (h1, h2) = (
        tokio::runtime::Handle::current(),
        tokio::runtime::Handle::current(),
    );
    let t1 = std::thread::spawn(move || {
        let _rt = h1.enter();
        a.launch(s1).is_ok()
    });
    let t2 = std::thread::spawn(move || {
        let _rt = h2.enter();
        b.launch(s2).is_ok()
    });
    let oks = [t1.join().unwrap(), t2.join().unwrap()];
    assert_eq!(oks.iter().filter(|o| **o).count(), 1, "{oks:?}");
    attempts
        .handle(&cancel_cmd("job_h", "att_twice"))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, attempts.wait("att_twice"))
        .await
        .unwrap();
}

/// A stage racing a launch of the same attempt either lands in the launched spec or is refused;
/// it is never accepted and then lost.
#[tokio::test(flavor = "multi_thread")]
async fn a_stage_racing_its_launch_is_kept_or_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, attempts, _rx) = manager(dir.path());
    let url = stuck_link().await;
    let rt = format!("uv:sha256:{}", "e".repeat(64));
    let project = ctx.state.root().join("runtimes").join(rt.replace(':', "-"));
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("uv.lock"), b"version = 1\n").unwrap();
    let stage = |id: &str, name: &str| Command {
        command_id: format!("stage-{name}"),
        kind: Some(Kind::StageArtifacts(mrl_proto::agent::v1::StageArtifacts {
            attempt: attempt_ref("job_s", id),
            items: vec![mrl_proto::agent::v1::StageItem {
                name: name.into(),
                url: url.clone(),
                sha256: "d".repeat(64),
                size: 4,
                local_path: format!("{name}/w.bin"),
                expires_at: Some(prost_types::Timestamp {
                    seconds: time::OffsetDateTime::now_utc().unix_timestamp() + 600,
                    nanos: 0,
                }),
            }],
        })),
    };
    let launch = |id: &str| Command {
        command_id: "launch".into(),
        kind: Some(Kind::LaunchAttempt(mrl_proto::agent::v1::LaunchAttempt {
            attempt: attempt_ref("job_s", id),
            engine_name: "fake".into(),
            job_spec_json: "{}".into(),
            roles: vec![mrl_proto::agent::v1::RoleLaunch {
                role_id: "trainer".into(),
                kind: "trainer".into(),
                primary: true,
                count: 1,
                gpu_indices: Vec::new(),
                runtime_ref: rt.clone(),
            }],
            ..Default::default()
        })),
    };
    for n in 0..10 {
        let id = format!("att_s{n}");
        assert!(attempts.handle(&stage(&id, "a")).await.unwrap().accepted);
        let (a, b) = (Arc::clone(&attempts), Arc::clone(&attempts));
        let (l, s) = (launch(&id), stage(&id, "b"));
        let launched = tokio::spawn(async move { a.handle(&l).await.unwrap().accepted });
        let staged = tokio::spawn(async move { b.handle(&s).await.unwrap().accepted });
        assert!(launched.await.unwrap());
        let staged = staged.await.unwrap();
        let rec = mrl_agent::attempts::record::AttemptRecord::load(&ctx.state.job_dir(&id))
            .unwrap()
            .unwrap();
        let names: Vec<&str> = rec.spec.inputs.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(
            names.contains(&"b"),
            staged,
            "accepted stages reach the spec: {names:?}"
        );
        attempts.handle(&cancel_cmd("job_s", &id)).await.unwrap();
        tokio::time::timeout(WAIT, attempts.wait(&id))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_must_name_the_right_job_and_drain_cancels_the_running() {
    let dir = tempfile::tempdir().unwrap();
    let (_ctx, attempts, _rx) = manager(dir.path());
    let url = stuck_link().await;
    attempts
        .launch(bad_lock_spec(
            dir.path(),
            "att_j",
            "job_right",
            1,
            slow_input(&url),
        ))
        .unwrap();
    assert_eq!(attempts.running()[0].job_id, "job_right");
    let wrong = attempts
        .handle(&cancel_cmd("job_wrong", "att_j"))
        .await
        .unwrap();
    assert!(!wrong.accepted, "another job's command is refused");
    let drain = Command {
        command_id: "drain".into(),
        kind: Some(Kind::Drain(mrl_proto::agent::v1::Drain {
            cancel_running: true,
            reason: "update".into(),
        })),
    };
    assert!(attempts.handle(&drain).await.unwrap().accepted);
    let d = tokio::time::timeout(WAIT, attempts.wait("att_j"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        d.exit_kind,
        mrl_proto::worker::v1::ExitKind::Cancelled as i32
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pause_during_staging_keeps_its_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, attempts, _rx) = manager(dir.path());
    let url = stuck_link().await;
    attempts
        .launch(bad_lock_spec(
            dir.path(),
            "att_p",
            "job_p",
            1,
            slow_input(&url),
        ))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let pause = Command {
        command_id: "pause".into(),
        kind: Some(Kind::PauseAttempt(mrl_proto::agent::v1::PauseAttempt {
            attempt: attempt_ref("job_p", "att_p"),
            request_id: "p1".into(),
            deadline_s: 1,
            reason: 1,
        })),
    };
    assert!(attempts.handle(&pause).await.unwrap().accepted);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let rec = mrl_agent::attempts::record::AttemptRecord::load(&ctx.state.job_dir("att_p"))
        .unwrap()
        .unwrap();
    assert_eq!(rec.requests.len(), 1, "kept for the workers");
    assert!(rec.stop_deadline_unix_ms.is_some(), "its deadline is saved");
    let d = tokio::time::timeout(WAIT, attempts.wait("att_p"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        d.exit_kind,
        mrl_proto::worker::v1::ExitKind::Paused as i32,
        "{d:?}"
    );
    assert!(rec.workers.is_empty());
}

/// Serves `body` after a short delay, counting requests.
async fn delayed_file(body: &'static [u8]) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let h = Arc::clone(&hits);
    let app = axum::Router::new().route(
        "/obj",
        axum::routing::get(move || {
            let h = Arc::clone(&h);
            async move {
                h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                body
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/obj"), hits)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_attempts_fill_one_input_once_and_keep_their_own_pins() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _attempts, _rx) = manager(dir.path());
    let (url, hits) = delayed_file(b"shared!").await;
    let sha = mrl_storage::sha256_hex(b"shared!");
    let expires = time::OffsetDateTime::now_utc() + time::Duration::minutes(10);
    let input = serde_json::json!({"inputs": [{
        "name": "model", "local_path": "model/w.bin", "sha256": sha, "size": 7, "url": url,
        "expires_at": expires.format(&time::format_description::well_known::Rfc3339).unwrap()
    }]});
    let a = bad_lock_spec(dir.path(), "att_fa", "job_f", 1, input.clone());
    let b = bad_lock_spec(dir.path(), "att_fb", "job_f", 1, input);
    let (da, db) = (dir.path().join("a"), dir.path().join("b"));
    let (ra, rb) = tokio::join!(
        mrl_agent::attempts::files::stage_inputs(&ctx, &a, &da, "att_fa"),
        mrl_agent::attempts::files::stage_inputs(&ctx, &b, &db, "att_fb"),
    );
    for (r, d) in [(ra, &da), (rb, &db)] {
        r.unwrap();
        assert_eq!(
            std::fs::read(d.join("model").join("w.bin")).unwrap(),
            b"shared!"
        );
    }
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "downloaded once"
    );
    ctx.cache.unpin_all("att_fa").unwrap();
    assert!(
        ctx.cache.usage().pinned_bytes > 0,
        "the other attempt's pin stays"
    );
    // A different input at a path already taken is refused, never silently reused.
    let other = bad_lock_spec(
        dir.path(),
        "att_fc",
        "job_f",
        1,
        serde_json::json!({"inputs": [{
            "name": "model", "local_path": "model/w.bin",
            "sha256": mrl_storage::sha256_hex(b"other"), "size": 5, "file": write_tmp(dir.path(), b"other")
        }]}),
    );
    let e = mrl_agent::attempts::files::stage_inputs(&ctx, &other, &da, "att_fc")
        .await
        .unwrap_err();
    assert_eq!(e.code(), "MRL-VAL-200");
}

fn write_tmp(dir: &std::path::Path, bytes: &[u8]) -> std::path::PathBuf {
    let p = dir.join(format!("f-{}", mrl_storage::sha256_hex(bytes)));
    std::fs::write(&p, bytes).unwrap();
    p
}

#[tokio::test(flavor = "multi_thread")]
async fn node_cleanup_purges_ended_attempts_and_prunes_unpinned_data() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, attempts, _rx) = manager(dir.path());
    let url = stuck_link().await;
    attempts
        .launch(bad_lock_spec(
            dir.path(),
            "att_live",
            "job_l",
            1,
            slow_input(&url),
        ))
        .unwrap();
    let data = write_tmp(dir.path(), b"{}\n");
    let input = serde_json::json!({"inputs": [{
        "name": "dataset.train", "local_path": "data/t.jsonl",
        "sha256": mrl_storage::sha256_hex(b"{}\n"), "size": 3, "file": data
    }]});
    attempts
        .launch(bad_lock_spec(dir.path(), "att_done", "job_d", 1, input))
        .unwrap();
    tokio::time::timeout(WAIT, attempts.wait("att_done"))
        .await
        .unwrap();
    // Two environments: one pinned by the live attempt, one unused.
    let envs = ctx.state.root().join("envs");
    for (name, pin) in [("uv-sha256-pinned", true), ("uv-sha256-unused", false)] {
        let d = envs.join(name);
        std::fs::create_dir_all(d.join(".pins")).unwrap();
        let marker =
            serde_json::json!({"runtime_ref": format!("uv:sha256:{name}"), "last_used_unix": 1});
        std::fs::write(d.join(".mrl-env.json"), marker.to_string()).unwrap();
        std::fs::write(d.join("blob"), vec![0u8; 4096]).unwrap();
        if pin {
            std::fs::write(d.join(".pins").join("att_live"), b"").unwrap();
        }
    }
    let cleanup = Command {
        command_id: "clean".into(),
        kind: Some(Kind::Cleanup(Cleanup {
            attempt: None,
            purge_scratch: true,
            cache_limit_bytes: 1,
        })),
    };
    let r = attempts.handle(&cleanup).await.unwrap();
    assert!(r.accepted, "{r:?}");
    assert!(
        !ctx.state.job_dir("att_done").join("inputs").exists(),
        "ended scratch purged"
    );
    assert!(
        ctx.state.job_dir("att_live").exists(),
        "the live attempt is untouched"
    );
    assert!(
        !envs.join("uv-sha256-unused").exists(),
        "unused environment pruned"
    );
    assert!(
        envs.join("uv-sha256-pinned").exists(),
        "pinned environment kept"
    );
    attempts
        .handle(&cancel_cmd("job_l", "att_live"))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, attempts.wait("att_live"))
        .await
        .unwrap();
}

/// Windows: a restarted agent can't reconnect to a surviving worker (its TCP port is gone),
/// so the worker is stopped and reported lost, never "re-adopted".
#[cfg(windows)]
#[tokio::test(flavor = "multi_thread")]
async fn windows_reports_a_surviving_worker_lost() {
    use mrl_agent::attempts::record::{AttemptRecord, ProcIdentity, WorkerProc};
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _first, _rx) = manager(dir.path());
    let mut worker = std::process::Command::new("ping")
        .args(["-n", "300", "127.0.0.1"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let identity = ProcIdentity::of_pid(worker.id()).unwrap();
    let spec = bad_lock_spec(dir.path(), "att_win", "job_w", 1, serde_json::json!({}));
    let rec = AttemptRecord {
        socket: dir.path().join("w.sock"),
        nonce: "n".into(),
        spec,
        phase: mrl_proto::agent::v1::AttemptPhase::Running as i32,
        workers: vec![WorkerProc {
            role_id: "trainer".into(),
            primary: true,
            identity: identity.clone(),
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
    };
    let job = ctx.state.job_dir("att_win");
    std::fs::create_dir_all(&job).unwrap();
    rec.save(&job).unwrap();
    let (tx, _rx2) = mrl_agent::attempts::attempt_events(16);
    let restarted = Attempts::new(Arc::clone(&ctx), tx, None).unwrap();
    let statuses = restarted.readopt().await;
    let s = statuses
        .iter()
        .find(|s| {
            s.attempt
                .as_ref()
                .is_some_and(|a| a.attempt_id == "att_win")
        })
        .unwrap();
    assert_eq!(s.failure.as_ref().unwrap().sub_code, "WORKER_LOST", "{s:?}");
    assert!(restarted.running().is_empty(), "not re-adopted");
    for _ in 0..50 {
        if !identity.alive() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(!identity.alive(), "the worker was stopped");
    let _ = worker.kill();
    let _ = worker.wait();
}
