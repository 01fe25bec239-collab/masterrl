//! P30 desktop module tests against a real PostgreSQL (`just test-integration`).
//! Compiled into mrl-testkit (`p30_desktop`).

use crate::pg::TestDb;
use crate::server::{TestServer, TEST_SECRET};
use crate::supabase::core::{config, Auth, CoreServer};
use crate::supabase::MockSupabase;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::Method;
use mrl_server::config::Mode;
use mrl_server::modules::checkpoints::service::EmergencyOutcome;
use mrl_server::modules::desktop::doctor::DoctorSource;
use mrl_server::modules::desktop::update::EmergencyCheckpoints;
use mrl_server::modules::desktop::DesktopModule;
use mrl_server::seams::LAUNCH_SECRET_HEADER;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

async fn solo(sb: &MockSupabase) -> CoreServer {
    CoreServer::start(Mode::Solo, sb, TestDb::new().await, Arc::default()).await
}

async fn audits(core: &CoreServer, action: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE action = $1")
        .bind(action)
        .fetch_one(core.db.pool())
        .await
        .unwrap_or_else(|e| panic!("{e}"))
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn onboarding_moves_through_its_steps_and_stays_done() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let state = core
        .call(Method::GET, "/api/v1/desktop/state", &Auth::Launch, None)
        .await;
    assert_eq!(state.status, 200, "{}", state.text());
    assert_eq!(state.json()["onboarding"]["step"], "welcome");
    assert_eq!(state.json()["onboarding"]["completed_at"], Value::Null);
    assert_eq!(state.json()["bundled_database"], false);

    let put = |step: &'static str| {
        let core = &core;
        async move {
            core.call(
                Method::PUT,
                "/api/v1/desktop/onboarding",
                &Auth::Launch,
                Some(json!({ "step": step })),
            )
            .await
        }
    };
    let r = put("restore").await;
    assert_eq!(
        (r.status, r.json()["step"].as_str()),
        (200, Some("restore"))
    );
    let done = put("done").await;
    let finished = done.json()["completed_at"].clone();
    assert!(finished.is_string(), "{}", done.text());
    // Going back to a step later never brings first-run back.
    let again = put("check").await;
    assert_eq!(again.json()["completed_at"], finished);
    let bad = put("nowhere").await;
    assert_eq!(
        (bad.status, bad.json()["errors"][0]["path"].as_str()),
        (422, Some("/step"))
    );
    assert_eq!(audits(&core, "desktop.onboarding").await, 3);
    // Survives a restart of the core.
    let core = core.restart(config(Mode::Solo, &sb)).await;
    let state = core
        .call(Method::GET, "/api/v1/desktop/state", &Auth::Launch, None)
        .await;
    assert_eq!(state.json()["onboarding"]["completed_at"], finished);
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn a_team_core_refuses_desktop_routes() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let core = core.restart(config(Mode::Team, &sb)).await;
    let owner = crate::supabase::MockUser::new("owner@example.com", "Owner");
    let jwt = sb.access_token(&owner, 600);
    let r = core
        .call(
            Method::GET,
            "/api/v1/desktop/state",
            &Auth::Bearer(jwt),
            None,
        )
        .await;
    // A first-seen person has no role yet: refused before the mode matters.
    assert_eq!(r.status, 403, "{}", r.text());
    core.stop().await;
}

async fn personal_org(core: &CoreServer) -> Uuid {
    sqlx::query_scalar("SELECT id FROM organizations WHERE kind = 'personal' LIMIT 1")
        .fetch_one(core.db.pool())
        .await
        .unwrap_or_else(|e| panic!("{e}"))
}

async fn job(core: &CoreServer, org: Uuid, state: &str) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, org_id, project_id, kind, state, resource_request, retry_policy,
                           created_by, started_at)
         VALUES ($1, $2, $3, 'DATA_PREP', $4, '{}', '{}', 'local-owner', now())",
    )
    .bind(id)
    .bind(org)
    .bind(Uuid::now_v7())
    .bind(state)
    .execute(core.db.pool())
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    id
}

async fn set_state(core: &CoreServer, id: Uuid, state: &str) {
    sqlx::query(
        "UPDATE jobs SET state = $2, state_version = state_version + 1, updated_at = now()
          WHERE id = $1",
    )
    .bind(id)
    .bind(state)
    .execute(core.db.pool())
    .await
    .unwrap_or_else(|e| panic!("{e}"));
}

async fn activity(core: &CoreServer, since: Option<&str>) -> Value {
    let path = match since {
        Some(s) => format!(
            "/api/v1/desktop/activity?since={}",
            s.replace('+', "%2B").replace(':', "%3A")
        ),
        None => "/api/v1/desktop/activity".to_owned(),
    };
    let r = core.call(Method::GET, &path, &Auth::Launch, None).await;
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn activity_shows_running_jobs_and_each_change_after_the_cursor() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let org = personal_org(&core).await;
    let first = activity(&core, None).await;
    assert_eq!(first["training_active"], false);
    assert_eq!(first["running"], json!([]));

    let a = job(&core, org, "RUNNING").await;
    let b = job(&core, org, "ALLOCATING").await;
    let now = activity(&core, first["cursor"].as_str()).await;
    assert_eq!(
        now["training_active"], true,
        "a running job keeps the computer awake"
    );
    assert_eq!(now["running"].as_array().map(Vec::len), Some(2));
    assert_eq!(now["running"][0]["name"], "data prep job");

    set_state(&core, a, "COMPLETED").await;
    set_state(&core, b, "FAILED").await;
    let after = activity(&core, now["cursor"].as_str()).await;
    let changes: Vec<(String, String)> = after["changes"]
        .as_array()
        .unwrap_or_else(|| panic!("no changes"))
        .iter()
        .map(|c| {
            (
                c["job_id"].as_str().unwrap_or_default().to_owned(),
                c["state"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    assert_eq!(changes.len(), 2, "{after}");
    assert!(changes.iter().any(|(_, s)| s == "COMPLETED"));
    assert!(changes.iter().any(|(_, s)| s == "FAILED"));
    assert_eq!(after["training_active"], false);
    // A replayed cursor answers the same changes again (the app shows each once).
    let replay = activity(&core, now["cursor"].as_str()).await;
    assert_eq!(replay["changes"], after["changes"]);
    let bad = core
        .call(
            Method::GET,
            "/api/v1/desktop/activity?since=yesterday",
            &Auth::Launch,
            None,
        )
        .await;
    assert_eq!(
        (bad.status, bad.json()["code"].as_str()),
        (400, Some("MRL-VAL-005"))
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn backups_need_the_bundled_database() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let list = core
        .call(Method::GET, "/api/v1/desktop/backups", &Auth::Launch, None)
        .await;
    assert_eq!(list.status, 200);
    assert_eq!(
        list.json(),
        json!({"available": false, "folder": null, "items": []})
    );
    for path in [
        "/api/v1/desktop/backups",
        "/api/v1/desktop/backups/masterrl-20261127T093000123Z-daily.dump/restore",
    ] {
        let r = core
            .call(Method::POST, path, &Auth::Launch, Some(json!({})))
            .await;
        assert_eq!(
            (r.status, r.json()["code"].as_str()),
            (409, Some("MRL-ENV-300")),
            "{path}"
        );
    }
    // Without a bundled database an update needs no backup: ready, nothing to checkpoint.
    let prep = core
        .call(
            Method::POST,
            "/api/v1/desktop/update/prepare",
            &Auth::Launch,
            None,
        )
        .await;
    assert_eq!(prep.status, 200, "{}", prep.text());
    assert_eq!(
        prep.json(),
        json!({"ready": true, "checkpoints": [], "backup": null, "backup_error": null})
    );
    assert_eq!(audits(&core, "desktop.prepare_update").await, 1);
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn the_computer_check_reads_the_agent_settings_it_is_given() {
    let sb = MockSupabase::start().await;
    let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
    let file = dir.path().join("agent.toml");
    std::fs::write(
        &file,
        format!(
            "state_dir = {:?}\n",
            dir.path().join("state").to_string_lossy()
        ),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let mut c = config(Mode::Solo, &sb);
    c.desktop.agent_config = Some(file.clone());
    let core = CoreServer::try_start(c.clone(), TestDb::new().await, Arc::default())
        .await
        .unwrap_or_else(|e| panic!("{}", e.explain()));
    let r = core
        .call(Method::POST, "/api/v1/desktop/doctor", &Auth::Launch, None)
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.json()["checks"].as_array().is_some_and(|c| !c.is_empty()));
    assert!(r.json()["platform"].is_string());
    std::fs::write(&file, "state_dir = [").unwrap_or_else(|e| panic!("{e}"));
    let broken = core
        .call(Method::POST, "/api/v1/desktop/doctor", &Auth::Launch, None)
        .await;
    assert_eq!(
        (broken.status, broken.json()["code"].as_str()),
        (503, Some("MRL-ENV-301"))
    );
    core.stop().await;
}

/// Answers every emergency checkpoint request from a script, and records them.
struct ScriptedCheckpoints {
    asked: Mutex<Vec<(Uuid, String)>>,
    answer: fn(usize) -> Result<EmergencyOutcome, String>,
}

#[async_trait]
impl EmergencyCheckpoints for ScriptedCheckpoints {
    async fn checkpoint(
        &self,
        job: Uuid,
        reason: &str,
        _deadline: Duration,
    ) -> Result<EmergencyOutcome, String> {
        let n = {
            let mut asked = self.asked.lock().unwrap_or_else(|p| p.into_inner());
            asked.push((job, reason.to_owned()));
            asked.len() - 1
        };
        (self.answer)(n)
    }
}

struct NoDoctor;

#[async_trait]
impl DoctorSource for NoDoctor {
    async fn run(&self) -> Result<mrl_agent::doctor::DoctorReport, String> {
        Err("not used here".into())
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn an_update_checkpoints_every_running_job_first() {
    let db = TestDb::new().await;
    let org = Uuid::now_v7();
    let mut running = Vec::new();
    for state in ["RUNNING", "RUNNING", "QUEUED"] {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO jobs (id, org_id, project_id, kind, state, resource_request,
                               retry_policy, created_by)
             VALUES ($1, $2, $3, 'DATA_PREP', $4, '{}', '{}', 'local-owner')",
        )
        .bind(id)
        .bind(org)
        .bind(Uuid::now_v7())
        .bind(state)
        .execute(db.pool())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
        if state == "RUNNING" {
            running.push(id);
        }
    }
    let script = Arc::new(ScriptedCheckpoints {
        asked: Mutex::new(Vec::new()),
        answer: |n| match n {
            0 => Ok(EmergencyOutcome::Committed(Uuid::now_v7())),
            _ => Ok(EmergencyOutcome::TimedOut),
        },
    });
    let config = mrl_server::Config::default();
    let module =
        DesktopModule::with_parts(db.db().clone(), &config, Arc::new(NoDoctor), script.clone());
    let server = TestServer::solo(vec![Box::new(module)]).await;
    let r = server
        .request(
            Method::POST,
            "/api/v1/desktop/update/prepare",
            &[(LAUNCH_SECRET_HEADER, TEST_SECRET)],
            Body::empty(),
        )
        .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let mut asked: Vec<Uuid> = script
        .asked
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .map(|(j, why)| {
            assert_eq!(why, "app update");
            *j
        })
        .collect();
    asked.sort();
    running.sort();
    assert_eq!(asked, running, "only running jobs are asked");
    let outcomes: Vec<&str> = r.json()["checkpoints"]
        .as_array()
        .unwrap_or_else(|| panic!("no checkpoints"))
        .iter()
        .filter_map(|c| c["outcome"].as_str())
        .map(|s| {
            if s == "committed" {
                "committed"
            } else {
                "timed_out"
            }
        })
        .collect();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.contains(&"committed") && outcomes.contains(&"timed_out"));
    // A slow checkpoint doesn't block the update: training goes on through it.
    assert_eq!(r.json()["ready"], true);
    server
        .shutdown()
        .await
        .unwrap_or_else(|e| panic!("{}", e.explain()));
}
