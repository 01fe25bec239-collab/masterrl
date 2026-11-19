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
