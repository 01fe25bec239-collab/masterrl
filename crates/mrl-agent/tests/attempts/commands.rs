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
