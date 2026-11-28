//! P16 integration tests: a production-wired core against the mock Supabase and a real
//! PostgreSQL (`just test-integration`).

use super::core::{config, Auth, CoreServer};
use super::{MockSupabase, MockUser};
use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::TestResponse;
use axum::http::Method;
use mrl_domain::ids::{
    DatasetVersionId, ModelVersionId, NodeId, OrgId, PoolId, ProjectId, UserId, WorkspaceId,
};
use mrl_domain::rbac::{allowed, Action, Role};
use mrl_server::config::Mode;
use mrl_server::modules::auth::supabase::{JwtVerifier, Supabase};
use mrl_server::modules::tenancy::{self, Rbac, Scope};
use mrl_server::seams::{
    AuditSink, Authorizer, Decision, Principal, PrincipalKind, RequestContext, RequestMeta,
    Resource, Secret, SecretStore,
};
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::Arc;

const PASSPHRASE: &str = "correct horse battery staple";

async fn solo(sb: &MockSupabase) -> CoreServer {
    CoreServer::start(Mode::Solo, sb, TestDb::new().await, Arc::default()).await
}

fn ctx(principal: Principal) -> RequestContext {
    RequestContext::new(
        principal,
        &RequestMeta {
            trace_id: "t".into(),
            request_id: "r".into(),
        },
    )
}

fn profile() -> Value {
    json!({
        "schema": "setup-profile/v1",
        "machines": [
            {"name": "gpu-box", "address": "10.0.0.7", "transport": {"kind": "ssh", "port": 2222, "user": "ada", "key_ref": "ssh.gpu-box"}},
            {"name": "laptop", "address": "laptop.tail1234.ts.net", "transport": {"kind": "tailscale"}}
        ],
        "preferences": {"theme": "dark", "units": "metric"},
        "team_memberships": [{"name": "Lab", "address": "https://lab.example:8443", "role": "researcher"}],
        "ui": {"sidebar": "collapsed"}
    })
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn solo_sign_in_links_the_owner_and_issues_a_working_token() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let r = core.sign_in(&sb, &Auth::Launch, true).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let body = r.json();
    let token = body["token"]["token"].as_str().unwrap().to_owned();
    assert!(token.starts_with("mrl_pat_") && token.len() == 8 + 43);
    assert_eq!(body["user"]["email"], "ada@example.com");

    let me = core
        .call(
            Method::GET,
            "/api/v1/auth/me",
            &Auth::Bearer(token.clone()),
            None,
        )
        .await;
    assert_eq!(me.status, 200, "{}", me.text());
    let me = me.json();
    assert_eq!(me["principal_kind"], "service");
    assert_eq!(me["user"]["display_name"], "Ada Lovelace");
    assert_eq!(me["roles"][0]["role"], "owner");
    assert_eq!(me["sign_in"], "active");

    // The linked account's Supabase JWT also signs in, as the local owner; others do not.
    let jwt = sb.access_token(&sb.user(), 300);
    let as_owner = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Bearer(jwt), None)
        .await;
    assert_eq!(as_owner.json()["principal_kind"], "local_owner");
    let eve = sb.access_token(&MockUser::new("eve@example.com", "Eve"), 300);
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Bearer(eve), None)
            .await
            .status,
        401
    );
    assert_eq!(
        core.call(
            Method::GET,
            "/api/v1/auth/me",
            &Auth::Bearer("mrl_pat_nope".into()),
            None
        )
        .await
        .status,
        401
    );
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::None, None)
            .await
            .status,
        401
    );

    // A flow is one-shot.
    let started = core
        .call(
            Method::POST,
            "/api/v1/auth/login",
            &Auth::Launch,
            Some(json!({"provider": "google", "redirect_uri": "masterrl://auth/callback"})),
        )
        .await
        .json();
    let back = sb
        .approve(started["authorize_url"].as_str().unwrap())
        .unwrap();
    assert!(back.starts_with("masterrl://auth/callback?flow="));
    let code = back.split("code=").nth(1).unwrap();
    let finish = json!({"flow_id": started["flow_id"], "code": code});
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/auth/callback",
            &Auth::Launch,
            Some(finish.clone())
        )
        .await
        .status,
        200
    );
    let again = core
        .call(
            Method::POST,
            "/api/v1/auth/callback",
            &Auth::Launch,
            Some(finish),
        )
        .await;
    assert_eq!(again.status, 401);
    assert_eq!(again.json()["code"], "MRL-AUTH-161");
    let bad_redirect = core
        .call(
            Method::POST,
            "/api/v1/auth/login",
            &Auth::Launch,
            Some(json!({"provider": "github", "redirect_uri": "https://evil.example/cb"})),
        )
        .await;
    assert_eq!(bad_redirect.status, 422);

    // Another account cannot take over while this one is signed in.
    sb.set_user(MockUser::new("bob@example.com", "Bob"));
    let other = core.sign_in(&sb, &Auth::Launch, false).await;
    assert_eq!(other.status, 409);
    assert_eq!(other.json()["code"], "MRL-AUTH-163");
    assert_eq!(
        other.json()["title"],
        "This app is signed in to a different account."
    );

    // Signing out revokes the calling token and the Supabase session.
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/auth/logout",
            &Auth::Bearer(token.clone()),
            None
        )
        .await
        .status,
        204
    );
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Bearer(token), None)
            .await
            .status,
        401
    );
    let me = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(me["sign_in"], "signed_out");
    assert_eq!(
        core.sign_in(&sb, &Auth::Launch, false).await.status,
        200,
        "after sign-out another account may link"
    );
    let audit: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE action = 'auth.session'")
            .fetch_one(core.db.pool())
            .await
            .unwrap();
    assert!(audit >= 4, "sign-ins and sign-out are audited ({audit})");
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn jwt_verification_uses_the_jwks_and_works_offline_from_the_cache() {
    let sb = MockSupabase::start().await;
    let other = MockSupabase::start().await;
    let db = TestDb::new().await;
    let project = || Supabase::from_config(&sb.auth_config()).unwrap();
    let v = JwtVerifier::new(project(), db.db().clone());
    let user = sb.user();
    let claims = v
        .verify(&sb.access_token(&user, 300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claims.sub, user.id);
    assert_eq!(claims.profile().email.as_deref(), Some("ada@example.com"));
    assert!(
        v.verify(&sb.access_token(&user, -120))
            .await
            .unwrap()
            .is_none(),
        "expired"
    );
    // Claims this project would accept: only the key decides below.
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let good = json!({"iss": sb.issuer(), "aud": "authenticated", "sub": user.id.to_string(), "exp": now + 300});
    assert!(v.verify(&sb.sign(&good)).await.unwrap().is_some());
    assert!(
        v.verify(&other.sign(&good)).await.unwrap().is_none(),
        "same issuer, audience and kid, another key"
    );
    assert!(
        v.verify(&sb.sign_as("unknown-kid", &good))
            .await
            .unwrap()
            .is_none(),
        "unknown kid"
    );
    assert!(v.verify("not.a.jwt").await.unwrap().is_none());
    // A valid payload for someone else under the original signature.
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let signed = sb.sign(&good);
    let parts: Vec<&str> = signed.split('.').collect();
    let mut forged = good.clone();
    forged["sub"] = json!(uuid::Uuid::now_v7().to_string());
    let swapped = format!(
        "{}.{}.{}",
        parts[0],
        b64.encode(forged.to_string()),
        parts[2]
    );
    assert!(
        v.verify(&swapped).await.unwrap().is_none(),
        "payload swapped under a valid signature"
    );

    // Offline: a new process (empty memory) still checks tokens with the keys in the database.
    sb.set_offline(true);
    let restarted = JwtVerifier::new(project(), db.db().clone());
    assert!(restarted
        .verify(&sb.access_token(&user, 300))
        .await
        .unwrap()
        .is_some());
    db.pool().close().await;
}

/// Every (role × action) of P06's matrix through the real `Rbac` over role bindings.
#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn rbac_matches_the_p06_matrix_for_every_role_and_action() {
    let db = TestDb::new().await;
    let (org, other_org, ws) = (OrgId::new(), OrgId::new(), WorkspaceId::new());
    let (project, other_project) = (ProjectId::new(), ProjectId::new());
    db.seed(&format!(
        "INSERT INTO organizations (id, slug, name, kind) VALUES ('{o}', 'lab', 'Lab', 'team'), ('{o2}', 'other', 'Other', 'team');
         INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{w}', '{o}', 'main', 'Main');
         INSERT INTO projects (id, org_id, workspace_id, slug, name) VALUES ('{p}', '{o}', '{w}', 'a', 'A'), ('{p2}', '{o}', '{w}', 'b', 'B');",
        o = org.as_uuid(), o2 = other_org.as_uuid(), w = ws.as_uuid(), p = project.as_uuid(), p2 = other_project.as_uuid()
    ))
    .await;
    let rbac = Rbac::new(db.db().clone(), Mode::Solo);
    let in_org = |org: OrgId| Resource {
        org: Some(org.to_string()),
        ..Resource::kind("thing")
    };
    let mut checked = 0;
    for role in Role::ALL.into_iter().filter(|r| r.is_human()) {
        let user = UserId::new();
        db.seed(&format!(
            "INSERT INTO users (id, display_name) VALUES ('{}', '{role}')",
            user.as_uuid()
        ))
        .await;
        let mut tx = db.db().begin().await.unwrap();
        tenancy::bind_role(&mut tx, org, user, role, Scope::Org)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let c = ctx(Principal {
            id: user.to_string(),
            kind: PrincipalKind::User,
            org: Some(org.to_string()),
        });
        for &action in Action::ALL {
            let want = if allowed(role, action) {
                Decision::Allow
            } else {
                Decision::Deny
            };
            let got = rbac
                .authorize(&c, action.as_str(), &in_org(org))
                .await
                .unwrap();
            assert_eq!(got, want, "{role} × {action}");
            let elsewhere = rbac
                .authorize(&c, action.as_str(), &in_org(other_org))
                .await
                .unwrap();
            let want_elsewhere = if action == Action::AuthSession {
                Decision::Allow
            } else {
                Decision::Hide
            };
            assert_eq!(
                elsewhere, want_elsewhere,
                "{role} × {action} in another org"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 5 * Action::ALL.len());

    // A project-scoped researcher acts on that project only.
    let r = UserId::new();
    db.seed(&format!(
        "INSERT INTO users (id, display_name) VALUES ('{}', 'r')",
        r.as_uuid()
    ))
    .await;
    let mut tx = db.db().begin().await.unwrap();
    tenancy::bind_role(&mut tx, org, r, Role::Researcher, Scope::Project(project))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let c = ctx(Principal {
        id: r.to_string(),
        kind: PrincipalKind::User,
        org: Some(org.to_string()),
    });
    let on = |p: ProjectId| Resource {
        org: Some(org.to_string()),
        project: Some(p.to_string()),
        ..Resource::kind("run")
    };
    assert_eq!(
        rbac.authorize(&c, "runs.submit", &on(project))
            .await
            .unwrap(),
        Decision::Allow
    );
    assert_eq!(
        rbac.authorize(&c, "runs.cancel_any", &on(project))
            .await
            .unwrap(),
        Decision::Deny
    );
    assert_eq!(
        rbac.authorize(&c, "runs.submit", &on(other_project))
            .await
            .unwrap(),
        Decision::Hide
    );

    // The solo owner gets owner verdicts; machines' actions stay closed to people.
    let owner = ctx(Principal::local_owner());
    assert_eq!(
        rbac.authorize(&owner, "admin.manage", &Resource::kind("x"))
            .await
            .unwrap(),
        Decision::Allow
    );
    assert_eq!(
        rbac.authorize(&owner, "agent.gateway", &Resource::kind("x"))
            .await
            .unwrap(),
        Decision::Deny
    );
    assert_eq!(
        rbac.authorize(&owner, "no.such_action", &Resource::kind("x"))
            .await
            .unwrap(),
        Decision::Deny
    );

    // A disabled user may do nothing.
    sqlx::query("UPDATE users SET status = 'disabled' WHERE id = $1")
        .bind(r.as_uuid())
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        rbac.authorize(&c, "runs.read", &on(project)).await.unwrap(),
        Decision::Deny
    );
    db.pool().close().await;
}

/// The routes registered so far × every human role, through HTTP in team mode.
#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn every_route_answers_by_role_in_team_mode() {
    // (method, path, action, body). Every signed-in route must be listed here or below.
    let base: &[(Method, &str, Action, Option<Value>)] = &[
        (Method::GET, "/api/v1/auth/me", Action::AuthSession, None),
        (
            Method::POST,
            "/api/v1/auth/logout",
            Action::AuthSession,
            None,
        ),
        (
            Method::POST,
            "/api/v1/auth/session",
            Action::AuthSession,
            None,
        ),
        (
            Method::DELETE,
            "/api/v1/auth/session",
            Action::AuthSession,
            None,
        ),
        (
            Method::GET,
            "/api/v1/auth/tokens",
            Action::AuthManageTokens,
            None,
        ),
        (
            Method::POST,
            "/api/v1/auth/tokens",
            Action::AuthManageTokens,
            Some(json!({"name": "ci", "scopes": ["runs:read"]})),
        ),
        (
            Method::DELETE,
            "/api/v1/auth/tokens/tok_00000000000000000000000000",
            Action::AuthManageTokens,
            None,
        ),
        (Method::GET, "/api/v1/audit", Action::AuditRead, None),
        (
            Method::GET,
            "/api/v1/sync/profile",
            Action::AuthSession,
            None,
        ),
        (
            Method::PUT,
            "/api/v1/sync/profile",
            Action::AuthSession,
            Some(json!({"schema": "setup-profile/v1"})),
        ),
        (
            Method::POST,
            "/api/v1/sync/setup",
            Action::AuthSession,
            Some(json!({"passphrase": "short"})),
        ),
        (Method::POST, "/api/v1/sync/push", Action::AuthSession, None),
        (
            Method::POST,
            "/api/v1/sync/restore",
            Action::AuthSession,
            Some(json!({})),
        ),
        (
            Method::GET,
            "/api/v1/sync/status",
            Action::AuthSession,
            None,
        ),
    ];
    // Transfer tickets are their own credential (P19).
    let public = [
        "/api/v1/auth/login",
        "/api/v1/auth/callback",
        "/api/v1/transfer/{ticket}",
    ];
    let sb = MockSupabase::start().await;
    // Every role calls every route in one org, past the default rate limits.
    let mut cfg = config(Mode::Team, &sb);
    let bucket = mrl_server::config::BucketConfig {
        per_second: 1000,
        burst: 5000,
    };
    cfg.server.rate_limit.principal = bucket;
    cfg.server.rate_limit.org = bucket;
    let core = match CoreServer::try_start(cfg, TestDb::new().await, Arc::default()).await {
        Ok(c) => c,
        Err(e) => panic!("test core did not start: {}", e.explain()),
    };
    let (org, ws) = (OrgId::new(), WorkspaceId::new());
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{o}', 'lab', 'Lab', 'team');
             INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{w}', '{o}', 'main', 'Main');",
            o = org.as_uuid(),
            w = ws.as_uuid()
        ))
        .await;
    // Runs and jobs (P17): one project, and one run (with its job and default experiment)
    // made by another person, so every role below acts on someone else's work.
    let prj = ProjectId::new();
    core.db
        .seed(&format!(
            "INSERT INTO projects (id, org_id, workspace_id, slug, name)
             VALUES ('{p}', '{o}', '{w}', 'rbac', 'RBAC');",
            p = prj.as_uuid(),
            o = org.as_uuid(),
            w = ws.as_uuid()
        ))
        .await;
    let submission = json!({
        "name": "rbac",
        "algorithm_id": "sft",
        "base_model_version_id": ModelVersionId::new().to_string(),
        "dataset_version_ids": [DatasetVersionId::new().to_string()],
        "config": {"epochs": 1},
    });
    let seeder = MockUser::new("seed@example.com", "Seed");
    let seed_jwt = sb.access_token(&seeder, 600);
    let me = core
        .call(
            Method::GET,
            "/api/v1/auth/me",
            &Auth::Bearer(seed_jwt.clone()),
            None,
        )
        .await;
    assert_eq!(me.status, 200, "{}", me.text());
    let seed_user: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM users WHERE supabase_user_id = $1")
            .bind(seeder.id)
            .fetch_one(core.db.pool())
            .await
            .unwrap();
    let mut tx = core.db.db().begin().await.unwrap();
    tenancy::bind_role(
        &mut tx,
        org,
        UserId::from_uuid(seed_user),
        Role::Owner,
        Scope::Org,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let seeded = core
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{prj}/runs"),
            &Auth::Bearer(seed_jwt.clone()),
            Some(submission.clone()),
            &[("idempotency-key", "rbac-seed")],
        )
        .await;
    assert_eq!(seeded.status, 201, "{}", seeded.text());
    let seeded = seeded.json();
    let field = |k: &str| seeded[k].as_str().unwrap().to_owned();
    let (run, job, exp) = (field("id"), field("job_id"), field("experiment_id"));
    // P22 routes: a second run (for POST /runs:compare) and one OPEN alert on the P17 run.
    // GET /streams never ends, so the loops below can't call it: it is checked here by status
    // only (owner 200, annotator 403) and left out of the coverage check (A1, 2026-11-22).
    let p22_second = core
        .call_with(
            Method::POST,
            &format!("/api/v1/projects/{prj}/runs"),
            &Auth::Bearer(seed_jwt.clone()),
            Some(submission.clone()),
            &[("idempotency-key", "rbac-seed-p22")],
        )
        .await;
    assert_eq!(p22_second.status, 201, "{}", p22_second.text());
    let p22_run = p22_second.json()["id"].as_str().unwrap().to_owned();
    let p22_pat = core.call(Method::POST, "/api/v1/auth/tokens", &Auth::Bearer(seed_jwt.clone()),
        Some(json!({"name": "P22 metrics only", "scopes": ["metrics_logs:read"], "project_id": prj.to_string()}))).await;
    assert_eq!(p22_pat.status, 201, "{}", p22_pat.text());
    let p22_auth = Auth::Bearer(p22_pat.json()["token"].as_str().unwrap().to_owned());
    let telemetry = core
        .call(
            Method::GET,
            &format!("/api/v1/runs/{run}/metrics"),
            &p22_auth,
            None,
        )
        .await;
    assert_eq!(telemetry.status, 200, "{}", telemetry.text());
    let comparison = core
        .call(
            Method::POST,
            "/api/v1/runs:compare",
            &p22_auth,
            Some(json!({"run_ids": [run, p22_run]})),
        )
        .await;
    assert_eq!(comparison.status, 403, "{}", comparison.text());
    let p22_alert = mrl_domain::ids::AlertId::new();
    core.db
        .seed(&format!(
            "INSERT INTO alerts (id, org_id, project_id, scope_kind, scope_id, source, code,
                    severity, title, dedup_key, state)
             VALUES ('{a}', '{o}', '{p}', 'run', '{r}', 'safety', 'safety.rbac', 'warning',
                    'Role table check.', 'rbac', 'OPEN');",
            a = p22_alert.as_uuid(),
            o = org.as_uuid(),
            p = prj.as_uuid(),
            r = mrl_domain::ids::RunId::from_str(&run).unwrap().as_uuid()
        ))
        .await;
    let p22_stream_status = |jwt: String| {
        let addr = core.server.api();
        let path = format!("/api/v1/streams?topics=run:{run}:state");
        async move {
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (mut send, conn) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
                    .await
                    .unwrap();
            tokio::spawn(conn);
            let req = axum::http::Request::builder()
                .uri(path)
                .header("host", addr.to_string())
                .header("authorization", format!("Bearer {jwt}"))
                .body(axum::body::Body::empty())
                .unwrap();
            // The status only; the response and its endless body are dropped here.
            send.send_request(req).await.unwrap().status().as_u16()
        }
    };
    assert_eq!(p22_stream_status(seed_jwt.clone()).await, 200);
    let (p22_annotator_jwt, p22_annotator) = member(&core, &sb, "p22-annotator").await;
    grant(&core, org, p22_annotator, Role::Annotator, Scope::Org).await;
    assert_eq!(p22_stream_status(p22_annotator_jwt).await, 403);
    let public: Vec<&str> = public.iter().copied().chain(["/api/v1/streams"]).collect();
    // P23 routes: one COMMITTED checkpoint of the P17 run, from an ended attempt. Its file is
    // not in storage.
    let ckpt = mrl_domain::ids::CheckpointId::new();
    {
        let (o, store_org) = (org.as_uuid(), org.to_string());
        let r = mrl_domain::ids::RunId::from_str(&run).unwrap().as_uuid();
        let j = mrl_domain::ids::JobId::from_str(&job).unwrap().as_uuid();
        let (a, c) = (uuid::Uuid::new_v4(), ckpt.as_uuid());
        core.db
            .seed(&format!(
                "INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state)
                 VALUES ('{a}', '{o}', '{j}', 1, 0, 'EXITED_ERROR');
                 INSERT INTO checkpoints (id, org_id, storage_org, run_id, job_id, attempt_id,
                   attempt_epoch, global_step, kind, layout, state)
                 VALUES ('{c}', '{o}', '{store_org}', '{r}', '{j}', '{a}', 0, 1, 'full_resume',
                   'fake-v1', 'COMMITTED');
                 INSERT INTO checkpoint_files (checkpoint_id, org_id, path, role, object_key,
                   sha256, size_bytes)
                 VALUES ('{c}', '{o}', 'w.bin', 'weights',
                   'orgs/{store_org}/runs/{run}/checkpoints/{ckpt}/w.bin',
                   decode(repeat('00', 32), 'hex'), 1);"
            ))
            .await;
    }
    // P24 routes: one listed engine with methods (the golden rejection fixture).
    let golden = mrl_domain::capability::manifest::Bundle::parse(include_str!(
        "../../../../tests/fixtures/rejections/engines/golden.json"
    ))
    .unwrap();
    mrl_server::modules::engines::store::register(core.db.db(), &[&golden], true)
        .await
        .unwrap();
    // <mrl:roles-seed P26> P26 seed data for the role table goes right below this line.
    // <mrl:roles-seed P27> P27 seed data for the role table goes right below this line.
    // P27 routes: a dataset with one READY version (no rows, an empty hash list), and a
    // second dataset the allowed roles delete.
    let (p27_ds, p27_dsv, p27_gone) = (
        mrl_domain::ids::DatasetId::new(),
        mrl_domain::ids::DatasetVersionId::new(),
        mrl_domain::ids::DatasetId::new(),
    );
    core.db
        .seed(&format!(
            "INSERT INTO datasets (id, org_id, project_id, name, created_by)
             VALUES ('{d}', '{o}', '{p}', 'rbac-data', 'seed'),
                    ('{g}', '{o}', '{p}', 'rbac-gone', 'seed');
             INSERT INTO dataset_versions (id, org_id, dataset_id, seq, schema_id, state,
               source_kind, row_count, byte_size, content_sha256, summary, created_by, ready_at)
             VALUES ('{v}', '{o}', '{d}', 1, 'sft.messages.v1', 'READY', 'upload', 0, 0,
               decode(repeat('00', 32), 'hex'), '{{\"browse\": []}}', 'seed', now());
             INSERT INTO dataset_files (dataset_version_id, org_id, path, role, object_key,
               sha256, size_bytes)
             VALUES ('{v}', '{o}', 'record_hashes.txt', 'derived', 'orgs/{o}/blobs/none',
               decode(repeat('00', 32), 'hex'), 0);",
            d = p27_ds.as_uuid(),
            g = p27_gone.as_uuid(),
            v = p27_dsv.as_uuid(),
            o = org.as_uuid(),
            p = prj.as_uuid()
        ))
        .await;
    let p27_tokenizer = mrl_domain::ids::ArtifactId::new();
    let p27_work = mrl_domain::ids::JobId::new();
    core.db.seed(&format!(
        r#"UPDATE datasets SET next_seq=2 WHERE id='{ds}';
         INSERT INTO artifacts (id,org_id,project_id,name,kind,prefix,manifest,size_bytes,state,created_by,committed_at)
         VALUES ('{tokenizer}','{o}','{p}','rbac-tokenizer','other','orgs/{o}/tmp/tokenizer',
         '{{"schema_version":"artifact-manifest/v1","kind":"other","created_at":"2026-11-27T00:00:00Z","files":[{{"path":"tokenizer.json","size":0,"sha256":"{sha}","role":"tokenizer"}}]}}',
         0,'COMMITTED','seed',now());
         INSERT INTO jobs (id,org_id,project_id,kind,state,resource_request,retry_policy,created_by)
         VALUES ('{job}','{org_uuid}','{project_uuid}','DATA_PREP','QUEUED','{{}}','{{}}','seed');
         INSERT INTO dataset_work (job_id,version_id,purpose) VALUES ('{job}','{v}','tokenize');"#,
        ds=p27_ds.as_uuid(), o=org, p=prj, tokenizer=p27_tokenizer.as_uuid(),
        org_uuid=org.as_uuid(), project_uuid=prj.as_uuid(), sha="0".repeat(64), job=p27_work.as_uuid(), v=p27_dsv.as_uuid(),
    )).await;
    // <mrl:roles-seed P28> P28 seed data for the role table goes right below this line.
    // <mrl:roles-seed P29> P29 seed data for the role table goes right below this line.
    // <mrl:roles-seed P30> P30 seed data for the role table goes right below this line.
    // Compute (P18): a pool made over the API and one machine in it (no agent connected).
    let pool = core
        .call(
            Method::POST,
            "/api/v1/pools",
            &Auth::Bearer(seed_jwt.clone()),
            Some(json!({"name": "seed-pool"})),
        )
        .await;
    assert_eq!(pool.status, 201, "{}", pool.text());
    let pool = pool.json()["id"].as_str().unwrap().to_owned();
    let node = NodeId::new();
    // Storage (P19): one finished upload registered as an artifact, an open upload, and a
    // secret. Every role works on these (another person's).
    let seed = |m: Method, p: String, body: Value| {
        let (core, jwt) = (&core, seed_jwt.clone());
        async move {
            let r = core.call(m, &p, &Auth::Bearer(jwt), Some(body)).await;
            assert!(r.status < 300, "seed {p}: {} {}", r.status, r.text());
            r.json()
        }
    };
    let sha = "2552b1c0272ab701af519fff4e4299f03720e72a3e3b57f7214b81d13c7eceff"; // "rbac"
    let new_upload = json!({"filename": "rbac.bin", "size": 4, "sha256": sha});
    let done = seed(Method::POST, "/api/v1/uploads".into(), new_upload.clone()).await;
    let done = done["id"].as_str().unwrap().to_owned();
    let bearer = format!("Bearer {seed_jwt}");
    let put = core
        .server
        .request(
            Method::PUT,
            &format!("/api/v1/uploads/{done}/parts/0"),
            &[
                ("authorization", &bearer),
                ("content-type", "application/octet-stream"),
            ],
            b"rbac".to_vec(),
        )
        .await;
    assert!(put.status < 300, "seed part: {} {}", put.status, put.text());
    seed(
        Method::POST,
        format!("/api/v1/uploads/{done}:complete"),
        json!({}),
    )
    .await;
    let manifest = json!({"schema_version": "artifact-manifest/v1", "kind": "eval",
        "created_at": "2026-11-16T00:00:00Z",
        "files": [{"path": "rbac.bin", "size": 4, "sha256": sha, "role": "report"}]});
    let art = seed(
        Method::POST,
        "/api/v1/artifacts".into(),
        json!({"name": "seed-artifact", "manifest": manifest, "uploads": {"rbac.bin": done}}),
    )
    .await;
    let art = art["id"].as_str().unwrap().to_owned();
    let open = seed(Method::POST, "/api/v1/uploads".into(), new_upload.clone()).await;
    let open = open["id"].as_str().unwrap().to_owned();
    let new_secret = |name: &str| json!({"name": name, "kind": "generic", "allowed_uses": ["run"]});
    let sec = seed(
        Method::POST,
        "/api/v1/secrets".into(),
        new_secret("seed-secret"),
    )
    .await;
    let sec = sec["id"].as_str().unwrap().to_owned();
    // An allowed role deletes the secret; put it back before each call, so the next role
    // still meets a real secret (and is refused, not told it is missing).
    core.db
        .seed(&format!(
            "CREATE TABLE rbac_secret_copy AS SELECT * FROM secrets_metadata WHERE id = '{}';",
            mrl_domain::ids::SecretId::from_str(&sec).unwrap().as_uuid()
        ))
        .await;
    let restore = || {
        core.db.seed(
            "INSERT INTO secrets_metadata SELECT * FROM rbac_secret_copy ON CONFLICT DO NOTHING;",
        )
    };
    core.db
        .seed(&format!(
            "INSERT INTO compute_nodes (id, pool_id, hostname, lifecycle, status)
             VALUES ('{n}', '{p}', 'rbac-host', 'STATIC', 'ONLINE');",
            n = node.as_uuid(),
            p = PoolId::from_str(&pool).unwrap().as_uuid()
        ))
        .await;
    let mut routes: Vec<(Method, String, Action, Option<Value>)> = base
        .iter()
        .map(|(m, p, a, b)| (m.clone(), (*p).to_owned(), *a, b.clone()))
        .collect();
    // Taken slugs answer 409 once allowed, so only a refusal is a 403.
    routes.extend([
        (
            Method::GET,
            "/api/v1/orgs".to_owned(),
            Action::AuthSession,
            None,
        ),
        (
            Method::POST,
            "/api/v1/orgs".to_owned(),
            Action::AdminManage,
            Some(json!({"slug": "lab", "name": "Lab"})),
        ),
        (
            Method::GET,
            format!("/api/v1/orgs/{org}/workspaces"),
            Action::TenancyRead,
            None,
        ),
        (
            Method::POST,
            format!("/api/v1/orgs/{org}/workspaces"),
            Action::TenancyManageProjects,
            Some(json!({"slug": "main", "name": "Main"})),
        ),
        (
            Method::GET,
            format!("/api/v1/workspaces/{ws}/projects"),
            Action::TenancyRead,
            None,
        ),
        (
            Method::POST,
            format!("/api/v1/workspaces/{ws}/projects"),
            Action::TenancyManageProjects,
            Some(json!({"slug": "taken", "name": "Taken"})),
        ),
    ]);
    // Runs and jobs (P17). Cancels and priority changes on another person's work need
    // runs.cancel_any; the job list is filtered by project so it authorizes (unfiltered,
    // it only hides rows).
    let get = |p: String| (Method::GET, p, Action::RunsRead, None);
    routes.extend([
        get(format!("/api/v1/projects/{prj}/experiments")),
        (
            Method::POST,
            format!("/api/v1/projects/{prj}/experiments"),
            Action::RunsSubmit,
            Some(json!({"name": "rbac-experiment"})),
        ),
        get(format!("/api/v1/experiments/{exp}")),
        (
            Method::PATCH,
            format!("/api/v1/experiments/{exp}"),
            Action::RunsSubmit,
            Some(json!({"description": "checked by every role"})),
        ),
        get(format!("/api/v1/projects/{prj}/runs")),
        (
            Method::POST,
            format!("/api/v1/projects/{prj}/runs"),
            Action::RunsSubmit,
            Some(submission),
        ),
        get(format!("/api/v1/runs/{run}")),
        get(format!("/api/v1/runs/{run}/config")),
        get(format!("/api/v1/runs/{run}/attempts")),
        get(format!("/api/v1/runs/{run}/timeline")),
        (
            Method::POST,
            format!("/api/v1/runs/{run}:cancel"),
            Action::RunsCancelAny,
            None,
        ),
        get(format!("/api/v1/jobs?project={prj}")),
        get(format!("/api/v1/jobs/{job}")),
        (
            Method::PATCH,
            format!("/api/v1/jobs/{job}"),
            Action::RunsCancelAny,
            Some(json!({"priority": 1})),
        ),
        (
            Method::POST,
            format!("/api/v1/jobs/{job}:cancel"),
            Action::RunsCancelAny,
            None,
        ),
    ]);
    // Compute (P18): pools, enrollment tokens and machines.
    let manage = |m: Method, p: String, body: Option<Value>| (m, p, Action::ComputeManage, body);
    routes.extend([
        (
            Method::GET,
            "/api/v1/pools".to_owned(),
            Action::ComputeRead,
            None,
        ),
        manage(
            Method::POST,
            "/api/v1/pools".to_owned(),
            Some(json!({"name": "rbac-pool"})),
        ),
        manage(
            Method::POST,
            format!("/api/v1/pools/{pool}/enrollment-tokens"),
            Some(json!({})),
        ),
        (
            Method::GET,
            format!("/api/v1/pools/{pool}/nodes"),
            Action::ComputeRead,
            None,
        ),
        (
            Method::GET,
            format!("/api/v1/queue?pool={pool}"),
            Action::ComputeRead,
            None,
        ),
        (
            Method::GET,
            format!("/api/v1/nodes/{node}"),
            Action::ComputeRead,
            None,
        ),
        manage(Method::POST, format!("/api/v1/nodes/{node}:drain"), None),
        manage(
            Method::POST,
            format!("/api/v1/nodes/{node}:decommission"),
            None,
        ),
    ]);
    // Storage (P19): uploads, artifacts, lineage and secrets.
    let row = |m: Method, p: String, a: Action, body: Option<Value>| (m, p, a, body);
    routes.extend([
        row(
            Method::POST,
            "/api/v1/uploads".into(),
            Action::UploadsWrite,
            Some(new_upload),
        ),
        row(
            Method::GET,
            format!("/api/v1/uploads/{open}"),
            Action::UploadsWrite,
            None,
        ),
        row(
            Method::POST,
            format!("/api/v1/uploads/{open}:presign"),
            Action::UploadsWrite,
            Some(json!({})),
        ),
        row(
            Method::PUT,
            format!("/api/v1/uploads/{open}/parts/0"),
            Action::UploadsWrite,
            Some(json!("not the bytes")),
        ),
        row(
            Method::POST,
            format!("/api/v1/uploads/{open}:complete"),
            Action::UploadsWrite,
            Some(json!({})),
        ),
        row(
            Method::POST,
            format!("/api/v1/uploads/{open}:abort"),
            Action::UploadsWrite,
            None,
        ),
        row(
            Method::GET,
            "/api/v1/artifacts".into(),
            Action::LineageRead,
            None,
        ),
        row(
            Method::POST,
            "/api/v1/artifacts".into(),
            Action::UploadsWrite,
            Some(json!({"name": "rbac", "manifest": {}, "uploads": {}})),
        ),
        row(
            Method::GET,
            format!("/api/v1/artifacts/{art}"),
            Action::LineageRead,
            None,
        ),
        row(
            Method::POST,
            format!("/api/v1/artifacts/{art}:download"),
            Action::LineageRead,
            Some(json!({"path": "rbac.bin"})),
        ),
        row(
            Method::GET,
            format!("/api/v1/lineage/{art}"),
            Action::LineageRead,
            None,
        ),
        row(
            Method::GET,
            "/api/v1/secrets".into(),
            Action::TenancyRead,
            None,
        ),
        row(
            Method::POST,
            "/api/v1/secrets".into(),
            Action::SecretsManage,
            Some(new_secret("rbac-secret")),
        ),
        row(
            Method::GET,
            format!("/api/v1/secrets/{sec}"),
            Action::TenancyRead,
            None,
        ),
        row(
            Method::PUT,
            format!("/api/v1/secrets/{sec}/value"),
            Action::SecretsManage,
            Some(json!({"value": "not-a-real-secret"})),
        ),
        row(
            Method::DELETE,
            format!("/api/v1/secrets/{sec}"),
            Action::SecretsManage,
            None,
        ),
        // P22 routes
        row(
            Method::GET,
            format!("/api/v1/runs/{run}/metrics"),
            Action::MetricsLogsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/runs/{run}/metrics/query"),
            Action::MetricsLogsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/runs/{run}/logs"),
            Action::MetricsLogsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/runs/{run}/logs:download"),
            Action::MetricsLogsDownload,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/runs/{run}/samples"),
            Action::MetricsLogsRead,
            None,
        ),
        row(
            Method::POST,
            "/api/v1/runs:compare".into(),
            Action::MetricsLogsRead,
            Some(json!({"run_ids": [run, p22_run]})),
        ),
        row(
            Method::GET,
            format!("/api/v1/alerts?run={run}"),
            Action::MetricsLogsRead,
            None,
        ),
        row(
            Method::POST,
            format!("/api/v1/alerts/{p22_alert}:ack"),
            Action::RunsCancelAny,
            None,
        ),
        row(
            Method::POST,
            format!("/api/v1/alerts/{p22_alert}:resolve"),
            Action::RunsCancelAny,
            None,
        ),
        // P23 routes
        row(
            Method::GET,
            format!("/api/v1/runs/{run}/checkpoints"),
            Action::CheckpointsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/checkpoints/{ckpt}"),
            Action::CheckpointsRead,
            None,
        ),
        row(
            Method::POST,
            format!("/api/v1/checkpoints/{ckpt}:verify"),
            Action::CheckpointsWrite,
            None,
        ),
        row(
            Method::DELETE,
            format!("/api/v1/checkpoints/{ckpt}"),
            Action::CheckpointsWrite,
            None,
        ),
        // P24 routes: engines, capabilities and a method's form (every GET answers 200).
        row(
            Method::GET,
            "/api/v1/engines".into(),
            Action::EnginesRead,
            None,
        ),
        row(
            Method::GET,
            "/api/v1/engines/golden/versions/1.0.0".into(),
            Action::EnginesRead,
            None,
        ),
        row(
            Method::GET,
            "/api/v1/capabilities".into(),
            Action::EnginesRead,
            None,
        ),
        row(
            Method::GET,
            "/api/v1/algorithms/sft/schema?engine_version=golden@1.0.0".into(),
            Action::EnginesRead,
            None,
        ),
        // <mrl:roles-routes P26> P26 role-table rows go right below this line.
        // <mrl:roles-routes P27> P27 role-table rows go right below this line.
        row(
            Method::GET,
            format!("/api/v1/projects/{prj}/datasets"),
            Action::DatasetsRead,
            None,
        ),
        row(
            Method::POST,
            format!("/api/v1/projects/{prj}/datasets"),
            Action::DatasetsImport,
            Some(json!({"name": "rbac-new", "license": "MIT"})),
        ),
        row(
            Method::GET,
            format!("/api/v1/datasets/{p27_ds}"),
            Action::DatasetsRead,
            None,
        ),
        row(
            Method::PATCH,
            format!("/api/v1/datasets/{p27_ds}"),
            Action::DatasetsImport,
            Some(json!({"description": "Checked by the role table."})),
        ),
        row(
            Method::DELETE,
            format!("/api/v1/datasets/{p27_gone}"),
            Action::DatasetsImport,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/datasets/{p27_ds}/versions"),
            Action::DatasetsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/dataset-versions/{p27_dsv}"),
            Action::DatasetsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/dataset-versions/{p27_dsv}/validation"),
            Action::DatasetsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/dataset-versions/{p27_dsv}/stats"),
            Action::DatasetsRead,
            None,
        ),
        row(
            Method::GET,
            format!("/api/v1/dataset-versions/{p27_dsv}/records?filter=len(messages)%20%3E%200"),
            Action::DatasetsRead,
            None,
        ),
        row(
            Method::POST,
            format!("/api/v1/dataset-versions/{p27_dsv}:diff"),
            Action::DatasetsRead,
            Some(json!({"against": p27_dsv.to_string()})),
        ),
        row(
            Method::POST,
            format!("/api/v1/dataset-versions/{p27_dsv}:export"),
            Action::DatasetsExport,
            Some(json!({"format": "parquet"})),
        ),
        row(
            Method::POST,
            format!("/api/v1/datasets/{p27_ds}/versions"),
            Action::DatasetsImport,
            Some(json!({"source":"remote","schema_id":"sft.messages.v1",
                "url":"https://example.org/train.jsonl"})),
        ),
        row(
            Method::POST,
            format!("/api/v1/dataset-versions/{p27_dsv}:tokenize-preview"),
            Action::DatasetsRead,
            Some(json!({"tokenizer_artifact_id":p27_tokenizer.to_string()})),
        ),
        row(
            Method::POST,
            format!("/api/v1/dataset-versions/{p27_dsv}:transform-preview"),
            Action::DatasetsImport,
            Some(json!({"transform_spec":{"schema_version":"transform/v1",
                "name":"preview","to":"sft.messages.v1","steps":[{"op":"filter","expr":"true"}]}})),
        ),
        row(
            Method::GET,
            format!("/api/v1/dataset-versions/{p27_dsv}/work/{p27_work}"),
            Action::DatasetsRead,
            None,
        ),
        // <mrl:roles-routes P28> P28 role-table rows go right below this line.
        // <mrl:roles-routes P29> P29 role-table rows go right below this line.
        // <mrl:roles-routes P30> P30 role-table rows go right below this line.
    ]);
    // A run submission needs an Idempotency-Key (keys are per person).
    let key_for = |method: &Method, path: &str| -> &'static [(&'static str, &'static str)] {
        if *method == Method::POST && path.ends_with("/runs") {
            &[("idempotency-key", "rbac")]
        } else {
            &[]
        }
    };
    let matches = |pattern: &str, path: &str| {
        let path = path.split('?').next().unwrap_or(path);
        let (a, b): (Vec<&str>, Vec<&str>) =
            (pattern.split('/').collect(), path.split('/').collect());
        a.len() == b.len() && a.iter().zip(&b).all(|(x, y)| x.starts_with('{') || x == y)
    };
    // The table covers every route in the OpenAPI document (health and docs aside).
    let doc: Value = core
        .call(Method::GET, "/api-docs/openapi.json", &Auth::None, None)
        .await
        .json();
    for (path, ops) in doc["paths"].as_object().unwrap() {
        if !path.starts_with("/api/v1/") || public.contains(&path.as_str()) {
            continue;
        }
        for method in ops.as_object().unwrap().keys() {
            let listed = routes
                .iter()
                .any(|(m, p, _, _)| m.as_str().eq_ignore_ascii_case(method) && matches(path, p));
            assert!(listed, "{method} {path} has no row in the role table");
        }
    }
    // What an allowed call answers. Sync is personal, so a team core refuses it (409
    // MRL-STATE-161); taken slugs and experiment names are 409; the token id below does not
    // exist (404).
    let expected = |method: &Method, path: &str| -> &'static [u16] {
        match (method.as_str(), path) {
            // P22 routes: the first allowed role acknowledges, then resolves; later roles find
            // the alert resolved (ack 409 MRL-STATE-220, resolve answers it again).
            ("POST", "/api/v1/runs:compare") => &[200],
            ("POST", p) if p.starts_with("/api/v1/alerts/") && p.ends_with(":ack") => &[200, 409],
            ("POST", p) if p.starts_with("/api/v1/alerts/") && p.ends_with(":resolve") => &[200],
            // P23 routes: the first allowed verify finds the file missing (CORRUPT, 200); later
            // ones meet a deleted checkpoint (409). Deleting is 204, also when already deleted.
            ("POST", p) if p.ends_with(":verify") => &[200, 409],
            ("DELETE", p) if p.starts_with("/api/v1/checkpoints/") => &[204],
            // <mrl:roles-expected P26> P26 expected-status arms go right below this line.
            // <mrl:roles-expected P27> P27 expected-status arms go right below this line.
            // P27 routes: the first allowed role makes the dataset, later ones meet its name
            // (409 MRL-CONFLICT-270). Deleting is 204, also when already deleted.
            ("POST", p) if p.ends_with("/datasets") => &[201, 409],
            ("PATCH", p) if p.starts_with("/api/v1/datasets/") => &[200],
            ("DELETE", p) if p.starts_with("/api/v1/datasets/") => &[204],
            ("POST", p) if p.ends_with("/versions") && p.starts_with("/api/v1/datasets/") => &[202],
            ("POST", p)
                if p.starts_with("/api/v1/dataset-versions/") && p.ends_with("-preview") =>
            {
                &[202]
            }
            ("POST", p) if p.starts_with("/api/v1/dataset-versions/") => &[200],
            // <mrl:roles-expected P28> P28 expected-status arms go right below this line.
            // <mrl:roles-expected P29> P29 expected-status arms go right below this line.
            // <mrl:roles-expected P30> P30 expected-status arms go right below this line.
            (_, p) if p.starts_with("/api/v1/sync/") => &[409],
            ("POST", "/api/v1/auth/session") | ("POST", "/api/v1/auth/tokens") => &[201],
            ("DELETE", "/api/v1/auth/session") => &[204],
            ("DELETE", p) if p.starts_with("/api/v1/auth/tokens/") => &[404],
            ("POST", p) if p.ends_with("/workspaces") => &[409],
            ("POST", p) if p.ends_with("/projects") => &[201, 409],
            ("POST", p) if p.ends_with("/experiments") => &[201, 409],
            ("PATCH", p) if p.contains("/experiments/") => &[200],
            ("POST", p) if p.ends_with("/runs") => &[201],
            // The run is REJECTED (no engine yet), so its job can't change any more.
            ("POST", p) if p.ends_with(":cancel") => &[409],
            ("PATCH", p) if p.contains("/jobs/") => &[409],
            ("POST", "/api/v1/pools") => &[201, 409, 422],
            ("POST", p) if p.ends_with("/enrollment-tokens") => &[201],
            ("POST", p) if p.ends_with(":drain") || p.ends_with(":decommission") => &[200, 422],
            // The first allowed role aborts the open upload; later ones find it finished
            // (409). Parts must be the upload's bytes (this body isn't), so they are 422.
            ("POST", "/api/v1/uploads") => &[201],
            ("POST", p) if p.ends_with(":presign") => &[200, 409],
            ("PUT", p) if p.contains("/parts/") => &[409, 422],
            ("POST", p) if p.ends_with(":complete") => &[409],
            ("POST", p) if p.ends_with(":abort") => &[200],
            ("POST", "/api/v1/artifacts") => &[422],
            ("POST", p) if p.ends_with(":download") => &[200],
            ("POST", "/api/v1/secrets") => &[201, 409],
            ("PUT", p) if p.ends_with("/value") => &[204],
            ("DELETE", p) if p.starts_with("/api/v1/secrets/") => &[204],
            ("GET", _) => &[200],
            _ => &[],
        }
    };
    for role in Role::ALL.into_iter().filter(|r| r.is_human()) {
        let person = MockUser::new(&format!("{role}@example.com"), role.as_str());
        let jwt = sb.access_token(&person, 600);
        // First sight makes the user (no roles yet): only auth.session works.
        let me = core
            .call(
                Method::GET,
                "/api/v1/auth/me",
                &Auth::Bearer(jwt.clone()),
                None,
            )
            .await;
        assert_eq!(me.status, 200);
        assert_eq!(me.json()["roles"], json!([]));
        let user: uuid::Uuid =
            sqlx::query_scalar("SELECT id FROM users WHERE supabase_user_id = $1")
                .bind(person.id)
                .fetch_one(core.db.pool())
                .await
                .unwrap();
        let mut tx = core.db.db().begin().await.unwrap();
        tenancy::bind_role(&mut tx, org, UserId::from_uuid(user), role, Scope::Org)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        for (method, path, action, body) in &routes {
            if *method == Method::POST && path == "/api/v1/auth/logout" {
                continue; // checked last: it would end nothing here, but keep the table honest
            }
            let key = key_for(method, path);
            restore().await;
            let r = core
                .call_with(
                    method.clone(),
                    path,
                    &Auth::Bearer(jwt.clone()),
                    body.clone(),
                    key,
                )
                .await;
            let refused = r.status == 403 && r.json()["code"] == "MRL-PERM-001";
            // Creating orgs needs the platform grant, which no org role gives.
            let platform = *method == Method::POST && path == "/api/v1/orgs";
            let want_refused = platform || !allowed(role, *action);
            assert_eq!(
                refused,
                want_refused,
                "{role}: {method} {path} answered {} {}",
                r.status,
                r.text()
            );
            // Allowed routes do their work (or answer the documented outcome), not just
            // "anything but 403".
            if !want_refused {
                assert!(
                    expected(method, path).contains(&r.status),
                    "{role}: {method} {path} answered {} {}",
                    r.status,
                    r.text()
                );
            }
        }
        let out = core
            .call(
                Method::POST,
                "/api/v1/auth/logout",
                &Auth::Bearer(jwt),
                None,
            )
            .await;
        assert_eq!(out.status == 403, !allowed(role, Action::AuthSession));
        if allowed(role, Action::AuthSession) {
            assert_eq!(out.status, 204, "{role}: logout answered {}", out.text());
        }
    }
    let token = |jwt: String, body: Value| {
        let core = &core;
        async move {
            let r = core
                .call(
                    Method::POST,
                    "/api/v1/auth/tokens",
                    &Auth::Bearer(jwt),
                    Some(body),
                )
                .await;
            assert_eq!(r.status, 201, "{}", r.text());
            r.json()["token"].as_str().unwrap().to_owned()
        }
    };
    // A `runs:read` token of an owner: everything outside that scope is refused.
    let (owner_jwt, owner) = member(&core, &sb, "pat-owner").await;
    grant(&core, org, owner, Role::Owner, Scope::Org).await;
    let scopes = vec!["runs:read".to_owned()];
    let narrow = token(owner_jwt, json!({"name": "narrow", "scopes": scopes})).await;
    for (method, path, action, body) in &routes {
        if *method == Method::POST && path == "/api/v1/auth/logout" {
            continue;
        }
        restore().await;
        let r = core
            .call_with(
                method.clone(),
                path,
                &Auth::Bearer(narrow.clone()),
                body.clone(),
                key_for(method, path),
            )
            .await;
        if path.starts_with("/api/v1/sync/") {
            assert_eq!(r.status, 409, "narrow token: {method} {path}: {}", r.text());
            continue;
        }
        let refused = r.status == 403 && r.json()["code"] == "MRL-PERM-001";
        let platform = *method == Method::POST && path == "/api/v1/orgs";
        let want = platform
            || !mrl_server::modules::auth::credentials::scopes_allow(&scopes, *action)
            || !allowed(Role::Owner, *action);
        assert_eq!(
            refused,
            want,
            "narrow token: {method} {path} answered {} {}",
            r.status,
            r.text()
        );
    }
    // A researcher bound to one project, and a token bound to that project.
    let prj = ProjectId::new();
    core.db
        .seed(&format!(
            "INSERT INTO projects (id, org_id, workspace_id, slug, name) VALUES ('{}', '{}', '{}', 'one', 'One');",
            prj.as_uuid(),
            org.as_uuid(),
            ws.as_uuid()
        ))
        .await;
    let (rjwt, researcher) = member(&core, &sb, "pat-researcher").await;
    grant(
        &core,
        org,
        researcher,
        Role::Researcher,
        Scope::Project(prj),
    )
    .await;
    let own = core
        .call(
            Method::GET,
            "/api/v1/auth/tokens",
            &Auth::Bearer(rjwt.clone()),
            None,
        )
        .await;
    assert_eq!(
        own.status,
        200,
        "own tokens, with only a project role: {}",
        own.text()
    );
    let bound = token(
        rjwt,
        json!({"name": "one", "scopes": ["*"], "project_id": prj.to_string()}),
    )
    .await;
    for (method, path, want) in [
        (Method::GET, "/api/v1/auth/me".to_owned(), 200),
        (Method::GET, "/api/v1/auth/tokens".to_owned(), 200),
        (Method::GET, "/api/v1/orgs".to_owned(), 403),
        (Method::GET, "/api/v1/audit".to_owned(), 403),
        (Method::GET, format!("/api/v1/orgs/{org}/workspaces"), 403),
        (
            Method::POST,
            format!("/api/v1/workspaces/{ws}/projects"),
            403,
        ),
    ] {
        let body = (method == Method::POST).then(|| json!({"slug": "other", "name": "Other"}));
        let r = core
            .call(method.clone(), &path, &Auth::Bearer(bound.clone()), body)
            .await;
        assert_eq!(
            r.status,
            want,
            "project token: {method} {path}: {}",
            r.text()
        );
    }
    let child = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Bearer(bound),
            Some(json!({"name": "child", "scopes": ["runs:read"]})),
        )
        .await;
    assert_eq!(child.status, 201, "{}", child.text());
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn setup_sync_round_trips_through_a_wiped_data_folder() {
    let sb = MockSupabase::start().await;
    let secrets: Arc<InMemorySecretStore> = Arc::default();
    let core = CoreServer::start(Mode::Solo, &sb, TestDb::new().await, secrets.clone()).await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    let put = core
        .call(
            Method::PUT,
            "/api/v1/sync/profile",
            &Auth::Launch,
            Some(profile()),
        )
        .await;
    assert_eq!(put.status, 200, "{}", put.text());
    let early = core
        .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await;
    assert_eq!(early.status, 409);
    assert_eq!(early.json()["code"], "MRL-STATE-160", "{}", early.text());
    let short = core
        .call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": "short"})),
        )
        .await;
    assert_eq!(short.status, 422);
    let setup = core
        .call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE})),
        )
        .await;
    assert_eq!(setup.status, 200, "{}", setup.text());
    let recovery = setup.json()["recovery_key"].as_str().unwrap().to_owned();
    for v in 1..=2 {
        let p = core
            .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
            .await;
        assert_eq!(p.status, 200, "{}", p.text());
        assert_eq!(p.json()["version"], v);
    }
    let status = core
        .call(Method::GET, "/api/v1/sync/status", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(status["set_up"], true);
    assert_eq!(status["last_pushed_version"], 2);
    assert_eq!(status["copies"].as_array().unwrap().len(), 2);
    assert!(
        !sb.stored_rows().to_string().contains("gpu-box"),
        "the copy is encrypted"
    );
    core.stop().await;

    // Wipe the data folder: a new, empty database. The keychain (sign-in) is kept.
    let fresh = CoreServer::start(Mode::Solo, &sb, TestDb::new().await, secrets.clone()).await;
    let empty = fresh
        .call(Method::GET, "/api/v1/sync/profile", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(empty["machines"], json!([]));
    let wrong = fresh
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"passphrase": "not the passphrase"})),
        )
        .await;
    assert_eq!(wrong.status, 422);
    assert_eq!(wrong.json()["code"], "MRL-VAL-160");
    let restored = fresh
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE})),
        )
        .await;
    assert_eq!(restored.status, 200, "{}", restored.text());
    assert_eq!(restored.json()["version"], 2);
    assert_eq!(restored.json()["profile"], profile());
    assert_eq!(
        fresh
            .call(Method::GET, "/api/v1/sync/profile", &Auth::Launch, None)
            .await
            .json(),
        profile()
    );
    let by_key = fresh
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"recovery_key": recovery, "version": 1})),
        )
        .await;
    assert_eq!(by_key.status, 200, "{}", by_key.text());
    assert_eq!(by_key.json()["version"], 1);
    let missing = fresh
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"recovery_key": recovery, "version": 9})),
        )
        .await;
    assert_eq!(missing.status, 404);
    assert_eq!(missing.json()["code"], "MRL-RES-160");

    // Keep the 5 newest copies; the restored key pushes without a new setup.
    for _ in 0..5 {
        assert_eq!(
            fresh
                .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
                .await
                .status,
            200
        );
    }
    let copies: Vec<i64> = fresh
        .call(Method::GET, "/api/v1/sync/status", &Auth::Launch, None)
        .await
        .json()["copies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["version"].as_i64().unwrap())
        .collect();
    assert_eq!(copies, vec![7, 6, 5, 4, 3]);
    fresh.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn offline_or_expired_sign_in_never_blocks_local_work() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE}))
        )
        .await
        .status,
        200
    );

    sb.set_offline(true);
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
            .await
            .status,
        200
    );
    assert_eq!(
        core.call(
            Method::PUT,
            "/api/v1/sync/profile",
            &Auth::Launch,
            Some(profile())
        )
        .await
        .status,
        200
    );
    assert_eq!(
        core.call(Method::GET, "/api/v1/sync/profile", &Auth::Launch, None)
            .await
            .json(),
        profile()
    );
    let push = core
        .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await;
    assert_eq!(push.status, 503);
    assert_eq!(push.json()["code"], "MRL-LINK-160", "{}", push.text());
    let status = core
        .call(Method::GET, "/api/v1/sync/status", &Auth::Launch, None)
        .await;
    assert_eq!(status.status, 200);
    assert!(status.json()["copies"].is_null());
    sb.set_offline(false);

    // The access token is used up and Supabase has revoked the refresh token.
    sb.set_access_ttl(30);
    core.call(Method::POST, "/api/v1/auth/logout", &Auth::Launch, None)
        .await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    sb.revoke_refresh_tokens();
    let push = core
        .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await;
    assert_eq!(push.status, 401);
    assert_eq!(push.json()["code"], "MRL-AUTH-160");
    let me = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
        .await;
    assert_eq!(me.json()["sign_in"], "expired");
    assert_eq!(
        core.call(Method::GET, "/api/v1/sync/profile", &Auth::Launch, None)
            .await
            .status,
        200
    );
    assert_eq!(
        core.call(Method::GET, "/api/v1/audit", &Auth::Launch, None)
            .await
            .status,
        200
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn renderer_sessions_need_the_csrf_token_or_the_same_origin() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let started = core
        .call(Method::POST, "/api/v1/auth/session", &Auth::Launch, None)
        .await;
    assert_eq!(started.status, 201, "{}", started.text());
    let set_cookie = started.headers["set-cookie"].to_str().unwrap().to_owned();
    assert!(
        set_cookie.contains("HttpOnly")
            && set_cookie.contains("SameSite=Strict")
            && !set_cookie.contains("Secure")
    );
    let cookie = set_cookie
        .split(';')
        .next()
        .unwrap()
        .trim_start_matches("mrl_session=")
        .to_owned();
    let csrf = started.json()["csrf_token"].as_str().unwrap().to_owned();
    let with = |csrf: Option<&str>, same_origin| Auth::Cookie {
        cookie: cookie.clone(),
        csrf: csrf.map(str::to_owned),
        same_origin,
    };
    let me = core
        .call(
            Method::GET,
            "/api/v1/auth/me",
            &with(Some(&csrf), false),
            None,
        )
        .await;
    assert_eq!(me.status, 200);
    assert_eq!(me.json()["principal_kind"], "local_owner");
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &with(None, true), None)
            .await
            .status,
        200
    );
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &with(None, false), None)
            .await
            .status,
        401
    );
    assert_eq!(
        core.call(
            Method::GET,
            "/api/v1/auth/me",
            &with(Some("wrong"), false),
            None
        )
        .await
        .status,
        401
    );
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM sessions")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    assert_eq!(stored, 1);
    assert_eq!(
        core.call(
            Method::DELETE,
            "/api/v1/auth/session",
            &with(Some(&csrf), false),
            None
        )
        .await
        .status,
        204
    );
    assert_eq!(
        core.call(
            Method::GET,
            "/api/v1/auth/me",
            &with(Some(&csrf), false),
            None
        )
        .await
        .status,
        401
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn tokens_are_scoped_listed_and_revoked_and_the_audit_log_pages() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let make = |auth: Auth, body: Value| {
        let core = &core;
        async move {
            core.call(Method::POST, "/api/v1/auth/tokens", &auth, Some(body))
                .await
        }
    };
    assert_eq!(
        make(Auth::Launch, json!({"name": "x", "scopes": ["runs:admin"]}))
            .await
            .status,
        422
    );
    assert_eq!(
        make(
            Auth::Launch,
            json!({"name": "x", "scopes": ["*"], "days": 400})
        )
        .await
        .status,
        422
    );
    let narrow = make(
        Auth::Launch,
        json!({"name": "reader", "scopes": ["runs:read"], "days": 7}),
    )
    .await;
    assert_eq!(narrow.status, 201, "{}", narrow.text());
    let narrow = narrow.json();
    let reader = narrow["token"].as_str().unwrap().to_owned();
    let widen = make(
        Auth::Bearer(reader.clone()),
        json!({"name": "wider", "scopes": ["runs:write"]}),
    )
    .await;
    assert_eq!(widen.status, 403, "{}", widen.text());
    let listed = core
        .call(Method::GET, "/api/v1/auth/tokens", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(listed["items"].as_array().unwrap().len(), 1);
    assert!(
        !listed.to_string().contains(&reader),
        "lists never show the token"
    );
    // A runs:read token may not read the audit log.
    assert_eq!(
        core.call(
            Method::GET,
            "/api/v1/audit",
            &Auth::Bearer(reader.clone()),
            None
        )
        .await
        .status,
        403
    );
    let path = format!("/api/v1/auth/tokens/{}", narrow["id"].as_str().unwrap());
    assert_eq!(
        core.call(Method::DELETE, &path, &Auth::Launch, None)
            .await
            .status,
        204
    );
    assert_eq!(
        core.call(Method::DELETE, &path, &Auth::Launch, None)
            .await
            .status,
        404
    );
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Bearer(reader), None)
            .await
            .status,
        401
    );

    // Seam writes and handler writes both land; pages are newest first.
    let sink = mrl_server::modules::audit::PgAuditSink::new(core.db.db().clone());
    // A success through the seam could outlive a rolled-back change, so it is refused and
    // nothing is written.
    let before = core.db.count("audit_events").await;
    let success = sink
        .record(mrl_server::modules::audit::event(
            time::OffsetDateTime::now_utc(),
            "local-owner",
            None,
            "runs.cancel_own",
            "run_1",
            mrl_server::seams::AuditOutcome::Success,
            "trace",
        ))
        .await;
    assert!(success.is_err());
    assert_eq!(core.db.count("audit_events").await, before);
    sink.record(mrl_server::modules::audit::event(
        time::OffsetDateTime::now_utc(),
        "local-owner",
        None,
        "runs.cancel_own",
        "run_1",
        mrl_server::seams::AuditOutcome::Denied,
        "trace",
    ))
    .await
    .unwrap();
    let first = core
        .call(Method::GET, "/api/v1/audit?limit=1", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(first["items"][0]["action"], "runs.cancel_own");
    assert_eq!(first["items"][0]["outcome"], "denied");
    let cursor = first["next_cursor"].as_str().unwrap();
    let second = core
        .call(
            Method::GET,
            &format!("/api/v1/audit?limit=50&cursor={cursor}"),
            &Auth::Launch,
            None,
        )
        .await
        .json();
    let actions: Vec<&str> = second["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["action"].as_str().unwrap())
        .collect();
    assert_eq!(actions, vec!["auth.manage_tokens", "auth.manage_tokens"]);
    let filtered = core
        .call(
            Method::GET,
            "/api/v1/audit?action=runs.cancel_own",
            &Auth::Launch,
            None,
        )
        .await
        .json();
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
    let change = sqlx::query("UPDATE audit_events SET actor = 'someone else'")
        .execute(core.db.pool())
        .await;
    assert!(change.is_err(), "audit rows cannot be changed");
    assert!(sqlx::query("DELETE FROM audit_events")
        .execute(core.db.pool())
        .await
        .is_err());
    core.stop().await;
}

/// Canary: secret values never reach the database, the logs or the sync payload.
#[tokio::test(flavor = "current_thread")]
#[ignore = "needs DATABASE_URL"]
async fn secrets_never_reach_the_database_logs_or_sync_payload() {
    let logs = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(logs.clone())
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    // With a single registered dispatcher, tracing caches callsites first hit on other test
    // threads as disabled for good; a second live dispatcher makes it ask this one too.
    let _second = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .finish(),
    );

    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let canary = "hf_CANARYsecret8f3e1c";
    core.secrets
        .set("hf.token", Secret::new(canary))
        .await
        .unwrap();
    let signed = core.sign_in(&sb, &Auth::Launch, true).await.json();
    let pat = signed["token"]["token"].as_str().unwrap().to_owned();
    let mut p = profile();
    p["machines"][0]["transport"]["key_ref"] = json!("hf.token");
    assert_eq!(
        core.call(
            Method::PUT,
            "/api/v1/sync/profile",
            &Auth::Bearer(pat.clone()),
            Some(p.clone())
        )
        .await
        .status,
        200
    );
    let mut leaky = p.clone();
    leaky["preferences"]["note"] = json!(canary);
    let refused = core
        .call(
            Method::PUT,
            "/api/v1/sync/profile",
            &Auth::Launch,
            Some(leaky),
        )
        .await;
    assert_eq!(refused.status, 422, "a secret-shaped value is refused");
    assert!(!refused.text().contains(canary));
    // Bodies the API cannot take are refused without repeating them (field names, values,
    // broken JSON), in the answer or the log.
    let mut unknown = p.clone();
    unknown[canary] = json!(1);
    let raw_bodies = [
        unknown.to_string(),
        json!({"passphrase": 7, "x": canary}).to_string(),
        format!(r#"{{"passphrase": "{canary}"#),
    ];
    for (path, body) in [
        ("/api/v1/sync/profile", &raw_bodies[0]),
        ("/api/v1/sync/setup", &raw_bodies[1]),
        ("/api/v1/sync/restore", &raw_bodies[2]),
    ] {
        let method = if path.ends_with("profile") {
            Method::PUT
        } else {
            Method::POST
        };
        let r = core
            .server
            .request(
                method,
                path,
                &[
                    ("content-type", "application/json"),
                    ("x-mrl-launch-secret", crate::server::TEST_SECRET),
                ],
                axum::body::Body::from(body.clone()),
            )
            .await;
        assert!(r.status >= 400 && r.status < 500, "{path}: {}", r.text());
        assert!(!r.text().contains(canary), "{path}: {}", r.text());
    }
    // A secret-like name is refused whatever its value's type.
    for (key, value) in [
        ("password", json!(["p16-secret-canary"])),
        ("api_key", json!({"value": "p16-secret-canary"})),
        ("db_password", json!(12345)),
    ] {
        let mut leaky = p.clone();
        leaky["preferences"][key] = value;
        let r = core
            .call(
                Method::PUT,
                "/api/v1/sync/profile",
                &Auth::Launch,
                Some(leaky),
            )
            .await;
        assert_eq!(r.status, 422, "{key}: {}", r.text());
        assert_eq!(r.json()["errors"][0]["path"], format!("/preferences/{key}"));
    }
    // Credentials hidden in addresses, transport settings or header-shaped values.
    let leaks: [(&str, Value, &str); 6] = [
        (
            "/machines/0/address",
            json!("ada:p16-secret-canary@10.0.0.7"),
            "/machines/0/address",
        ),
        (
            "/team_memberships/0/address",
            json!("https://ada:p16-secret-canary@lab.example:8443"),
            "/team_memberships/0/address",
        ),
        (
            "/machines/0/transport/options",
            json!({"password": "p16-secret-canary"}),
            "",
        ),
        (
            "/preferences/proxy_header",
            json!("Bearer p16-secret-canary"),
            "/preferences/proxy_header",
        ),
        (
            "/ui/proxy",
            json!("http://ada:p16-secret-canary@proxy:8080"),
            "/ui/proxy",
        ),
        (
            "/machines/0/transport/user",
            json!("ada:p16-secret-canary"),
            "/machines/0/transport/user",
        ),
    ];
    for (pointer, value, path) in leaks {
        let mut leaky = p.clone();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        leaky.pointer_mut(parent).unwrap()[key] = value;
        let r = core
            .call(
                Method::PUT,
                "/api/v1/sync/profile",
                &Auth::Launch,
                Some(leaky),
            )
            .await;
        assert_eq!(r.status, 422, "{pointer}: {}", r.text());
        if !path.is_empty() {
            assert_eq!(r.json()["errors"][0]["path"], path, "{pointer}");
        }
    }
    let setup = core
        .call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Bearer(pat.clone()),
            Some(json!({"passphrase": PASSPHRASE})),
        )
        .await
        .json();
    let recovery = setup["recovery_key"].as_str().unwrap().replace('-', "");
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/sync/push",
            &Auth::Bearer(pat.clone()),
            None
        )
        .await
        .status,
        200
    );
    let session: Value = serde_json::from_str(
        core.secrets
            .get("auth.supabase_session")
            .await
            .unwrap()
            .unwrap()
            .expose(),
    )
    .unwrap();
    let pepper = core
        .secrets
        .get("auth.token_pepper")
        .await
        .unwrap()
        .unwrap()
        .expose()
        .to_owned();
    let secrets = [
        ("canary", canary.to_owned()),
        ("access token", pat.clone()),
        (
            "supabase access token",
            session["access_token"].as_str().unwrap().to_owned(),
        ),
        (
            "supabase refresh token",
            session["refresh_token"].as_str().unwrap().to_owned(),
        ),
        ("passphrase", PASSPHRASE.to_owned()),
        ("sync key", recovery),
        ("token pepper", pepper),
        ("secret-named canary", "p16-secret-canary".to_owned()),
    ];

    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
    )
    .fetch_all(core.db.pool())
    .await
    .unwrap();
    let mut dump = String::new();
    for t in &tables {
        let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT row_to_json(t)::text FROM \"{t}\" t"
        )))
        .fetch_all(core.db.pool())
        .await
        .unwrap();
        dump.push_str(&rows.join("\n"));
    }
    assert!(dump.contains("hf.token"), "the reference is stored");
    let payload = sb.stored_rows().to_string();
    let log_text = logs.text();
    assert!(
        log_text.contains("listening"),
        "the server's own logs were captured"
    );
    // bytea columns show as hex in row_to_json: look for the hex form too.
    let hex = |v: &str| v.bytes().map(|b| format!("{b:02x}")).collect::<String>();
    for (what, value) in &secrets {
        assert!(!dump.contains(value.as_str()), "{what} is in the database");
        assert!(
            !dump.contains(&hex(value)),
            "{what} is in the database as bytes"
        );
        assert!(!log_text.contains(value.as_str()), "{what} is in the logs");
        assert!(
            !payload.contains(value.as_str()),
            "{what} is in the sync payload"
        );
    }
    core.stop().await;
}

#[derive(Clone, Default)]
struct LogBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

impl LogBuffer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|p| p.into_inner())).into_owned()
    }
}

impl std::io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBuffer;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn member(core: &CoreServer, sb: &MockSupabase, name: &str) -> (String, UserId) {
    let person = MockUser::new(&format!("{name}@example.com"), name);
    let jwt = sb.access_token(&person, 600);
    let me = core
        .call(
            Method::GET,
            "/api/v1/auth/me",
            &Auth::Bearer(jwt.clone()),
            None,
        )
        .await;
    assert_eq!(me.status, 200, "{}", me.text());
    let user: uuid::Uuid = sqlx::query_scalar("SELECT id FROM users WHERE supabase_user_id = $1")
        .bind(person.id)
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    (jwt, UserId::from_uuid(user))
}

async fn grant(core: &CoreServer, org: OrgId, user: UserId, role: Role, scope: Scope) {
    let mut tx = core.db.db().begin().await.unwrap();
    tenancy::bind_role(&mut tx, org, user, role, scope)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn tenancy_endpoints_create_list_refuse_duplicates_in_solo_mode() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let post = |path: String, slug: &str| {
        let body = json!({"slug": slug, "name": format!("{slug} name")});
        let core = &core;
        async move {
            core.call(Method::POST, &path, &Auth::Launch, Some(body))
                .await
        }
    };
    let get = |path: String| {
        let core = &core;
        async move { core.call(Method::GET, &path, &Auth::Launch, None).await }
    };
    let org = post("/api/v1/orgs".into(), "vision").await;
    assert_eq!(org.status, 201, "{}", org.text());
    let org = org.json();
    assert_eq!(
        (org["slug"].as_str(), org["kind"].as_str()),
        (Some("vision"), Some("team"))
    );
    let org_id = org["id"].as_str().unwrap().to_owned();
    assert!(org_id.starts_with("org_"));
    let orgs = get("/api/v1/orgs".into()).await.json();
    let slugs: Vec<&str> = orgs["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["slug"].as_str().unwrap())
        .collect();
    assert_eq!(slugs, vec!["personal", "vision"]);
    assert!(orgs["next_cursor"].is_null());
    let first = get("/api/v1/orgs?limit=1".into()).await.json();
    let next = get(format!(
        "/api/v1/orgs?limit=1&cursor={}",
        first["next_cursor"].as_str().unwrap()
    ))
    .await
    .json();
    assert_eq!(next["items"][0]["slug"], "vision");

    for (level, path) in [("org", "/api/v1/orgs".to_owned())] {
        let dup = post(path.clone(), "vision").await;
        assert_eq!(dup.status, 409, "{level}: {}", dup.text());
        assert_eq!(dup.headers["content-type"], "application/problem+json");
        let p = dup.json();
        assert_eq!(p["code"], "MRL-CONFLICT-160");
        assert_eq!(p["title"], "This short name is already used here.");
        assert_eq!(p["instance"], "/api/v1/orgs");
        assert!(!p["trace_id"].as_str().unwrap().is_empty());
        assert_eq!(
            post(path, "personal").await.status,
            409,
            "the personal org's slug is taken too"
        );
    }
    for bad in ["Bad Slug", "-x", "", &"a".repeat(64)] {
        assert_eq!(
            post("/api/v1/orgs".into(), bad).await.status,
            422,
            "{bad:?}"
        );
    }

    let ws_path = format!("/api/v1/orgs/{org_id}/workspaces");
    let ws = post(ws_path.clone(), "research").await;
    assert_eq!(ws.status, 201, "{}", ws.text());
    let ws = ws.json();
    assert_eq!(ws["org_id"], org_id.as_str());
    let ws_id = ws["id"].as_str().unwrap().to_owned();
    assert_eq!(
        post(ws_path.clone(), "research").await.json()["code"],
        "MRL-CONFLICT-160"
    );
    assert_eq!(
        get(ws_path).await.json()["items"].as_array().unwrap().len(),
        1
    );

    let prj_path = format!("/api/v1/workspaces/{ws_id}/projects");
    let prj = post(prj_path.clone(), "grpo-math").await;
    assert_eq!(prj.status, 201, "{}", prj.text());
    let prj = prj.json();
    assert_eq!(
        (prj["workspace_id"].as_str(), prj["org_id"].as_str()),
        (Some(ws_id.as_str()), Some(org_id.as_str()))
    );
    assert!(prj["id"].as_str().unwrap().starts_with("prj_"));
    assert_eq!(post(prj_path.clone(), "grpo-math").await.status, 409);
    let listed = get(prj_path).await.json();
    assert_eq!(listed["items"][0]["slug"], "grpo-math");
    assert_eq!(listed["items"].as_array().unwrap().len(), 1);
    // The same slug is fine in another workspace.
    let other = post(format!("/api/v1/orgs/{org_id}/workspaces"), "other")
        .await
        .json();
    assert_eq!(
        post(
            format!(
                "/api/v1/workspaces/{}/projects",
                other["id"].as_str().unwrap()
            ),
            "grpo-math"
        )
        .await
        .status,
        201
    );

    // Unknown or malformed ids are 404.
    assert_eq!(
        get(format!("/api/v1/orgs/{}/workspaces", OrgId::new()))
            .await
            .status,
        404
    );
    assert_eq!(
        get("/api/v1/orgs/nonsense/workspaces".into()).await.status,
        404
    );
    assert_eq!(
        post(
            format!("/api/v1/workspaces/{}/projects", WorkspaceId::new()),
            "x"
        )
        .await
        .status,
        404
    );
    let audited: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_events WHERE action = 'tenancy.manage_projects'",
    )
    .fetch_one(core.db.pool())
    .await
    .unwrap();
    assert_eq!(audited, 5, "every create is audited");
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn tenancy_endpoints_follow_roles_and_hide_other_orgs_in_team_mode() {
    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let (lab, rival, ws) = (OrgId::new(), OrgId::new(), WorkspaceId::new());
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{a}', 'lab', 'Lab', 'team'), ('{b}', 'rival', 'Rival', 'team');
             INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{w}', '{a}', 'main', 'Main');",
            a = lab.as_uuid(), b = rival.as_uuid(), w = ws.as_uuid()
        ))
        .await;
    let (alice, a) = member(&core, &sb, "alice").await;
    let (bob, b) = member(&core, &sb, "bob").await;
    let (carol, c) = member(&core, &sb, "carol").await;
    let (dave, d) = member(&core, &sb, "dave").await;
    let (eve, e) = member(&core, &sb, "eve").await;
    let (nobody, _) = member(&core, &sb, "nobody").await;
    grant(&core, lab, a, Role::Owner, Scope::Org).await;
    grant(&core, lab, b, Role::Researcher, Scope::Org).await;
    grant(&core, lab, c, Role::Maintainer, Scope::Org).await;
    grant(&core, lab, d, Role::Maintainer, Scope::Workspace(ws)).await;
    grant(&core, rival, e, Role::Owner, Scope::Org).await;
    let call = |jwt: &str, method: Method, path: String, body: Option<Value>| {
        let (core, auth) = (&core, Auth::Bearer(jwt.to_owned()));
        async move { core.call(method, &path, &auth, body).await }
    };
    let item = |slug: &str| Some(json!({"slug": slug, "name": slug}));
    let workspaces = format!("/api/v1/orgs/{lab}/workspaces");
    let projects = format!("/api/v1/workspaces/{ws}/projects");

    // Creating an org needs the platform grant: owning an org is not enough.
    let as_owner = call(
        &alice,
        Method::POST,
        "/api/v1/orgs".into(),
        item("alice-org"),
    )
    .await;
    assert_eq!(as_owner.status, 403, "{}", as_owner.text());
    assert_eq!(as_owner.json()["code"], "MRL-PERM-001");
    let mut tx = core.db.db().begin().await.unwrap();
    tenancy::set_platform_admin(&mut tx, a, true).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        call(
            &alice,
            Method::POST,
            "/api/v1/orgs".into(),
            item("alice-org")
        )
        .await
        .status,
        201
    );
    for jwt in [&bob, &carol, &nobody, &eve] {
        let r = call(jwt, Method::POST, "/api/v1/orgs".into(), item("nope")).await;
        assert_eq!(r.status, 403, "{}", r.text());
    }
    let alices: Vec<String> = call(&alice, Method::GET, "/api/v1/orgs".into(), None)
        .await
        .json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["slug"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(alices.len(), 2, "lab and the org alice made: {alices:?}");
    assert_eq!(
        call(&nobody, Method::GET, "/api/v1/orgs".into(), None)
            .await
            .json()["items"],
        json!([])
    );

    // A researcher reads but may not create; owner and maintainer may.
    assert_eq!(
        call(&bob, Method::GET, workspaces.clone(), None)
            .await
            .status,
        200
    );
    let denied = call(&bob, Method::POST, workspaces.clone(), item("bobs")).await;
    assert_eq!(denied.status, 403);
    assert_eq!(denied.json()["code"], "MRL-PERM-001");
    assert_eq!(
        call(&bob, Method::POST, projects.clone(), item("bobs"))
            .await
            .status,
        403
    );
    assert_eq!(
        call(&carol, Method::POST, workspaces.clone(), item("carols"))
            .await
            .status,
        201
    );
    assert_eq!(
        call(&alice, Method::POST, projects.clone(), item("alices"))
            .await
            .status,
        201
    );
    // A workspace maintainer creates projects there but sees nothing org-wide.
    assert_eq!(
        call(&dave, Method::POST, projects.clone(), item("daves"))
            .await
            .status,
        201
    );
    assert_eq!(
        call(&dave, Method::GET, projects.clone(), None)
            .await
            .json()["items"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        call(&dave, Method::GET, workspaces.clone(), None)
            .await
            .status,
        404
    );

    // Another org's owner gets 404 everywhere in this org, the same as for an unknown id.
    for (method, path, body) in [
        (Method::GET, workspaces.clone(), None),
        (Method::POST, workspaces.clone(), item("eves")),
        (Method::GET, projects.clone(), None),
        (Method::POST, projects.clone(), item("eves")),
    ] {
        let r = call(&eve, method.clone(), path.clone(), body).await;
        assert_eq!(r.status, 404, "{method} {path}: {}", r.text());
        assert_eq!(r.json()["code"], "MRL-RES-001");
    }
    let eves: Vec<String> = call(&eve, Method::GET, "/api/v1/orgs".into(), None)
        .await
        .json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["slug"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(eves, vec!["rival"]);
    core.stop().await;
}

/// Makes `audit_events` refuse inserts until the returned guard SQL runs (simulates a
/// failing audit write inside the caller's transaction).
async fn break_audit(core: &CoreServer) {
    core.db
        .seed(
            "CREATE FUNCTION p16_audit_down() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'audit is down'; END $$;
             CREATE TRIGGER p16_audit_down BEFORE INSERT ON audit_events
               FOR EACH ROW EXECUTE FUNCTION p16_audit_down();",
        )
        .await;
}

async fn mend_audit(core: &CoreServer) {
    core.db
        .seed("DROP TRIGGER p16_audit_down ON audit_events;")
        .await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_one_project_token_acts_only_in_that_project() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let post = |path: String, body: Value, auth: Auth| {
        let core = &core;
        async move { core.call(Method::POST, &path, &auth, Some(body)).await }
    };
    let org = post(
        "/api/v1/orgs".into(),
        json!({"slug": "lab", "name": "Lab"}),
        Auth::Launch,
    )
    .await
    .json();
    let ws = post(
        format!("/api/v1/orgs/{}/workspaces", org["id"].as_str().unwrap()),
        json!({"slug": "main", "name": "Main"}),
        Auth::Launch,
    )
    .await
    .json();
    let ws_id = ws["id"].as_str().unwrap();
    let a = post(
        format!("/api/v1/workspaces/{ws_id}/projects"),
        json!({"slug": "a", "name": "A"}),
        Auth::Launch,
    )
    .await
    .json();
    let b = post(
        format!("/api/v1/workspaces/{ws_id}/projects"),
        json!({"slug": "b", "name": "B"}),
        Auth::Launch,
    )
    .await
    .json();
    let (pa, pb) = (
        a["id"].as_str().unwrap().to_owned(),
        b["id"].as_str().unwrap().to_owned(),
    );
    let made = post(
        "/api/v1/auth/tokens".into(),
        json!({"name": "a only", "scopes": ["*"], "project_id": pa}),
        Auth::Launch,
    )
    .await;
    assert_eq!(made.status, 201, "{}", made.text());
    let made = made.json();
    let token_id = made["id"].as_str().unwrap().to_owned();

    // The owner's role does not lift the token's project limit.
    let rbac = Rbac::new(core.db.db().clone(), Mode::Solo);
    let c = ctx(Principal {
        id: token_id.clone(),
        kind: PrincipalKind::Service,
        org: None,
    });
    let on = |p: &str| Resource {
        org: org["id"].as_str().map(str::to_owned),
        project: Some(p.to_owned()),
        ..Resource::kind("run")
    };
    assert_eq!(
        rbac.authorize(&c, "runs.submit", &on(&pa)).await.unwrap(),
        Decision::Allow
    );
    assert_eq!(
        rbac.authorize(&c, "runs.submit", &on(&pb)).await.unwrap(),
        Decision::Hide
    );
    assert_eq!(
        rbac.authorize(&c, "tenancy.manage_projects", &Resource::kind("org"))
            .await
            .unwrap(),
        Decision::Deny
    );
    assert_eq!(
        rbac.authorize(&c, "auth.session", &Resource::kind("account"))
            .await
            .unwrap(),
        Decision::Allow
    );
    // Through HTTP: org-wide endpoints are closed to it.
    let token = made["token"].as_str().unwrap().to_owned();
    assert_eq!(
        post(
            format!("/api/v1/workspaces/{ws_id}/projects"),
            json!({"slug": "c", "name": "C"}),
            Auth::Bearer(token.clone())
        )
        .await
        .status,
        403
    );

    // Tokens it makes stay on project A.
    let other = post(
        "/api/v1/auth/tokens".into(),
        json!({"name": "b", "scopes": ["*"], "project_id": pb}),
        Auth::Bearer(token.clone()),
    )
    .await;
    assert_eq!(other.status, 403, "{}", other.text());
    let child = post(
        "/api/v1/auth/tokens".into(),
        json!({"name": "child", "scopes": ["runs:read"]}),
        Auth::Bearer(token),
    )
    .await;
    assert_eq!(child.status, 201, "{}", child.text());
    let child_project: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT project_id FROM api_tokens WHERE token_prefix = $1")
            .bind(&child.json()["token"].as_str().unwrap()[..12])
            .fetch_one(core.db.pool())
            .await
            .unwrap();
    assert_eq!(
        child_project.map(|u| ProjectId::from_uuid(u).to_string()),
        Some(pa.clone())
    );
    let missing = post(
        "/api/v1/auth/tokens".into(),
        json!({"name": "x", "scopes": ["*"], "project_id": ProjectId::new().to_string()}),
        Auth::Launch,
    )
    .await;
    assert_eq!(missing.status, 404);
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn jwts_must_carry_exp_iss_aud_and_sub() {
    let sb = MockSupabase::start().await;
    let db = TestDb::new().await;
    let v = JwtVerifier::new(
        Supabase::from_config(&sb.auth_config()).unwrap(),
        db.db().clone(),
    );
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let good = json!({"iss": sb.issuer(), "aud": "authenticated", "sub": sb.user().id.to_string(), "exp": now + 300});
    assert!(
        v.verify(&sb.sign(&good)).await.unwrap().is_some(),
        "complete claims pass"
    );
    for drop in ["iss", "aud", "exp", "sub"] {
        let mut c = good.clone();
        c.as_object_mut().unwrap().remove(drop);
        assert!(
            v.verify(&sb.sign(&c)).await.unwrap().is_none(),
            "missing {drop} must fail"
        );
    }
    for (claim, value) in [
        ("iss", json!("https://other.supabase.co/auth/v1")),
        ("aud", json!("anon")),
        ("exp", json!(now - 300)),
    ] {
        let mut c = good.clone();
        c[claim] = value;
        assert!(
            v.verify(&sb.sign(&c)).await.unwrap().is_none(),
            "wrong {claim} must fail"
        );
    }
    db.pool().close().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn logout_revokes_the_token_the_cli_sends_next_to_the_launch_secret() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let token = core.sign_in(&sb, &Auth::Launch, true).await.json()["token"]["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let out = core
        .call(
            Method::POST,
            "/api/v1/auth/logout",
            &Auth::LaunchAndBearer(token.clone()),
            None,
        )
        .await;
    assert_eq!(out.status, 204, "{}", out.text());
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Bearer(token), None)
            .await
            .status,
        401
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_failing_audit_write_rolls_back_the_change_it_records() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let token = core.sign_in(&sb, &Auth::Launch, true).await.json()["token"]["token"]
        .as_str()
        .unwrap()
        .to_owned();
    break_audit(&core).await;
    let count = |table: &str| {
        let (core, table) = (&core, table.to_owned());
        async move { core.db.count(&table).await }
    };
    let tokens_before = count("api_tokens").await;
    assert_eq!(
        core.call(Method::POST, "/api/v1/auth/session", &Auth::Launch, None)
            .await
            .status,
        503
    );
    assert_eq!(
        count("sessions").await,
        0,
        "no session without its audit row"
    );
    let made = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Launch,
            Some(json!({"name": "x", "scopes": ["*"]})),
        )
        .await;
    assert_eq!(made.status, 503);
    assert_eq!(count("api_tokens").await, tokens_before);
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/auth/logout",
            &Auth::LaunchAndBearer(token.clone()),
            None
        )
        .await
        .status,
        503
    );
    assert_eq!(
        core.call(
            Method::GET,
            "/api/v1/auth/me",
            &Auth::Bearer(token.clone()),
            None
        )
        .await
        .status,
        200,
        "logout rolled back: the token still works"
    );
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
            .await
            .json()["sign_in"],
        "signed_out",
        "the Supabase session ends first; running logout again finishes the rest"
    );
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/orgs",
            &Auth::Launch,
            Some(json!({"slug": "lab", "name": "Lab"}))
        )
        .await
        .status,
        503
    );
    assert_eq!(count("organizations").await, 1, "only the personal org");
    mend_audit(&core).await;
    assert_eq!(
        core.call(Method::POST, "/api/v1/auth/session", &Auth::Launch, None)
            .await
            .status,
        201
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_failed_re_setup_keeps_the_old_sync_key_working() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    assert_eq!(
        core.call(
            Method::PUT,
            "/api/v1/sync/profile",
            &Auth::Launch,
            Some(profile())
        )
        .await
        .status,
        200
    );
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE}))
        )
        .await
        .status,
        200
    );
    assert_eq!(
        core.call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
            .await
            .status,
        200
    );

    break_audit(&core).await;
    let new_pass = "a different passphrase entirely";
    let failed = core
        .call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": new_pass})),
        )
        .await;
    assert_eq!(failed.status, 503, "{}", failed.text());
    mend_audit(&core).await;

    // The old key and KDF settings are still the active pair.
    let pushed = core
        .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await;
    assert_eq!(pushed.status, 200, "{}", pushed.text());
    let old = core
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE})),
        )
        .await;
    assert_eq!(old.status, 200, "{}", old.text());
    assert_eq!(old.json()["version"], 2);
    let new = core
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"passphrase": new_pass})),
        )
        .await;
    assert_eq!(new.status, 422);
    assert_eq!(new.json()["code"], "MRL-VAL-160");
    assert_eq!(new.json()["errors"][0]["code"], "MRL-VAL-160");

    // A successful re-setup switches both at once and drops the old key.
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": new_pass}))
        )
        .await
        .status,
        200
    );
    let refs: Vec<Option<String>> = sqlx::query_scalar("SELECT key_ref FROM sync_state")
        .fetch_all(core.db.pool())
        .await
        .unwrap();
    let active = refs[0].clone().unwrap();
    assert!(active.starts_with("sync.key."));
    assert!(core.secrets.get(&active).await.unwrap().is_some());
    assert_eq!(
        core.call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
            .await
            .status,
        200
    );
    let newest = core
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"passphrase": new_pass})),
        )
        .await;
    assert_eq!(newest.json()["version"], 3);
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn secrets_are_never_kept_for_idempotent_replay() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    let keyed = |path: &'static str, body: Option<Value>, key: &'static str| {
        let core = &core;
        async move {
            core.call_with(
                Method::POST,
                path,
                &Auth::Launch,
                body,
                &[("idempotency-key", key)],
            )
            .await
        }
    };
    let routes: [(&str, Option<Value>); 4] = [
        (
            "/api/v1/auth/tokens",
            Some(json!({"name": "x", "scopes": ["*"]})),
        ),
        ("/api/v1/auth/session", None),
        (
            "/api/v1/sync/setup",
            Some(json!({"passphrase": PASSPHRASE})),
        ),
        (
            "/api/v1/sync/restore",
            Some(json!({"passphrase": PASSPHRASE})),
        ),
    ];
    for (path, body) in routes {
        for attempt in 0..2 {
            let r = keyed(path, body.clone(), path).await;
            assert_eq!(r.status, 400, "{path} try {attempt}: {}", r.text());
            assert_eq!(r.json()["code"], "MRL-IDEM-003");
            let text = r.text();
            assert!(
                !text.contains("mrl_pat_")
                    && !text.contains("recovery_key")
                    && !text.contains("csrf_token"),
                "{text}"
            );
        }
    }
    assert_eq!(core.db.count("api_tokens").await, 0);
    assert_eq!(core.db.count("sessions").await, 0);
    assert_eq!(core.db.count("sync_state").await, 0);
    // Not even the refusal or a fingerprint of the body (a fast passphrase check) is kept.
    assert_eq!(core.db.count("idempotency_keys").await, 0);
    // Without the key the same requests work; other endpoints still replay.
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Launch,
            Some(json!({"name": "x", "scopes": ["*"]}))
        )
        .await
        .status,
        201
    );
    let first = keyed(
        "/api/v1/orgs",
        Some(json!({"slug": "lab", "name": "Lab"})),
        "k-org-1",
    )
    .await;
    assert_eq!(first.status, 201);
    let again = keyed(
        "/api/v1/orgs",
        Some(json!({"slug": "lab", "name": "Lab"})),
        "k-org-1",
    )
    .await;
    assert_eq!(again.status, 201, "replayed, not a 409");
    assert_eq!(again.headers["idempotent-replayed"], "true");
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn org_lists_follow_roles_and_token_limits() {
    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let (lab, rival, other) = (OrgId::new(), OrgId::new(), OrgId::new());
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{a}', 'lab', 'Lab', 'team'), ('{b}', 'rival', 'Rival', 'team'), ('{c}', 'other', 'Other', 'team');",
            a = lab.as_uuid(), b = rival.as_uuid(), c = other.as_uuid()
        ))
        .await;
    let (carol, c) = member(&core, &sb, "carol").await;
    grant(&core, lab, c, Role::Maintainer, Scope::Org).await;
    grant(&core, rival, c, Role::Viewer, Scope::Org).await;
    grant(&core, other, c, Role::Annotator, Scope::Org).await;
    let slugs = |r: TestResponse| -> Vec<String> {
        assert_eq!(r.status, 200, "{}", r.text());
        r.json()["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["slug"].as_str().unwrap().to_owned())
            .collect()
    };
    let get = |auth: Auth| {
        let core = &core;
        async move { core.call(Method::GET, "/api/v1/orgs", &auth, None).await }
    };
    // An annotator role does not allow tenancy.read, so that org is not listed.
    let mut both = slugs(get(Auth::Bearer(carol.clone())).await);
    both.sort();
    assert_eq!(both, vec!["lab", "rival"]);
    let token = |scopes: Value| {
        let (core, carol) = (&core, carol.clone());
        async move {
            let r = core
                .call(
                    Method::POST,
                    "/api/v1/auth/tokens",
                    &Auth::Bearer(carol),
                    Some(json!({"name": "t", "scopes": scopes})),
                )
                .await;
            assert_eq!(r.status, 201, "{}", r.text());
            r.json()["token"].as_str().unwrap().to_owned()
        }
    };
    // Carol's token is bound to her first org (lab): it sees only that one.
    let reader = token(json!(["tenancy:read"])).await;
    assert_eq!(slugs(get(Auth::Bearer(reader)).await), vec!["lab"]);
    let runs_only = token(json!(["runs:read"])).await;
    let refused = get(Auth::Bearer(runs_only)).await;
    assert_eq!(refused.status, 403, "{}", refused.text());
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_child_token_never_outlives_its_parent() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let parent = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Launch,
            Some(json!({"name": "short", "scopes": ["*"], "days": 1})),
        )
        .await
        .json();
    let child = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Bearer(parent["token"].as_str().unwrap().to_owned()),
            Some(json!({"name": "long", "scopes": ["runs:read"], "days": 365})),
        )
        .await;
    assert_eq!(child.status, 201, "{}", child.text());
    let ends: Vec<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT expires_at FROM api_tokens ORDER BY created_at")
            .fetch_all(core.db.pool())
            .await
            .unwrap();
    assert_eq!(ends.len(), 2);
    assert!(
        ends[1] <= ends[0],
        "child {} outlives parent {}",
        ends[1],
        ends[0]
    );
    // Once the parent's time is up, the child stops too.
    sqlx::query("UPDATE api_tokens SET expires_at = now() - interval '1 second' WHERE expires_at = $1 AND name = 'short'")
        .bind(ends[0])
        .execute(core.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE api_tokens SET expires_at = LEAST(expires_at, now() - interval '1 second') WHERE name = 'long'")
        .execute(core.db.pool())
        .await
        .unwrap();
    let child_token = child.json()["token"].as_str().unwrap().to_owned();
    assert_eq!(
        core.call(
            Method::GET,
            "/api/v1/auth/me",
            &Auth::Bearer(child_token),
            None
        )
        .await
        .status,
        401
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn two_devices_pushing_at_once_both_keep_their_copy() {
    let sb = MockSupabase::start().await;
    let (laptop, desktop) = (solo(&sb).await, solo(&sb).await);
    for core in [&laptop, &desktop] {
        assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
        assert_eq!(
            core.call(
                Method::POST,
                "/api/v1/sync/setup",
                &Auth::Launch,
                Some(json!({"passphrase": PASSPHRASE}))
            )
            .await
            .status,
            200
        );
    }
    let mut versions = Vec::new();
    for _ in 0..5 {
        let (a, b) = tokio::join!(push(&laptop), push(&desktop));
        for r in [a, b] {
            assert_eq!(r.status, 200, "{}", r.text());
            versions.push(r.json()["version"].as_i64().unwrap());
        }
    }
    versions.sort_unstable();
    assert_eq!(
        versions,
        (1..=10).collect::<Vec<i64>>(),
        "every push got its own version"
    );
    let kept: Vec<i64> = laptop
        .call(Method::GET, "/api/v1/sync/status", &Auth::Launch, None)
        .await
        .json()["copies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["version"].as_i64().unwrap())
        .collect();
    assert_eq!(kept, vec![10, 9, 8, 7, 6]);
    laptop.stop().await;
    desktop.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_rate_limited_refresh_is_temporary() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    sb.set_access_ttl(30); // inside the refresh margin: every push refreshes first
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE}))
        )
        .await
        .status,
        200
    );
    sb.rate_limit_refreshes(1);
    let limited = core
        .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await;
    assert_eq!(limited.status, 503, "{}", limited.text());
    assert_eq!(limited.json()["code"], "MRL-LINK-160");
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
            .await
            .json()["sign_in"],
        "active"
    );
    let later = core
        .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await;
    assert_eq!(later.status, 200, "{}", later.text());
    // A dead refresh token still ends the session.
    sb.revoke_refresh_tokens();
    let dead = core
        .call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await;
    assert_eq!(dead.json()["code"], "MRL-AUTH-160");
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
            .await
            .json()["sign_in"],
        "expired"
    );
    core.stop().await;
}

async fn push(core: &CoreServer) -> TestResponse {
    core.call(Method::POST, "/api/v1/sync/push", &Auth::Launch, None)
        .await
}

/// Applies this build's migrations below `before` (all of them with `None`).
async fn migrate_to(db: &TestDb, before: Option<i64>) {
    let mut m = mrl_server::db::migrate::migrator();
    if let Some(v) = before {
        m.migrations = m
            .migrations
            .iter()
            .filter(|x| x.version < v)
            .cloned()
            .collect::<Vec<_>>()
            .into();
    }
    mrl_server::db::migrate::apply_with(db.db(), &m)
        .await
        .unwrap();
}

/// A completed replay record whose kept response is `body`.
fn kept(key: &str, body: &str) -> String {
    format!(
        "INSERT INTO idempotency_keys (scope, key, request_sha256, state, response_status, response_body, expires_at) \
         VALUES ('usr_1', '{key}', '\\x00', 'DONE', 201, convert_to('{body}', 'UTF8'), now() + interval '1 day');"
    )
}

async fn keys(db: &TestDb) -> Vec<String> {
    sqlx::query_scalar("SELECT key FROM idempotency_keys ORDER BY key")
        .fetch_all(db.pool())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn the_upgrade_from_wave_03_keeps_every_replay_record() {
    let db = TestDb::empty().await;
    migrate_to(&db, Some(16001)).await;
    db.seed(&kept(
        "run-1",
        r#"{"id":"run_1","token":"mrl_pat_lookalike"}"#,
    ))
    .await;
    db.seed("INSERT INTO idempotency_keys (scope, key, request_sha256, state, owner, claim, expires_at) \
             VALUES ('usr_1', 'run-2', '\\x01', 'IN_PROGRESS', 1, gen_random_uuid(), now() + interval '1 day');")
        .await;
    migrate_to(&db, None).await;
    assert_eq!(keys(&db).await, vec!["run-1", "run-2"]);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn the_upgrade_clears_only_what_earlier_p16_revisions_kept() {
    let db = TestDb::empty().await;
    // A database that ran P16 before 16007 (like 3791ab7's parent).
    migrate_to(&db, Some(16007)).await;
    for (key, body) in [
        ("token", r#"{"id":"tok_1","token":"mrl_pat_x"}"#),
        ("session", r#"{"csrf_token":"c","expires_at":"x"}"#),
        ("setup", r#"{"recovery_key":"abcd-ef01"}"#),
        (
            "restore",
            r#"{"version":1,"profile":{"schema":"setup-profile/v1"}}"#,
        ),
        ("refused", r#"{"code":"MRL-IDEM-003"}"#),
        ("org", r#"{"id":"org_1","slug":"lab"}"#),
    ] {
        db.seed(&kept(key, body)).await;
    }
    migrate_to(&db, None).await;
    assert_eq!(keys(&db).await, vec!["org"]);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn an_org_bound_token_of_a_platform_admin_cannot_create_orgs() {
    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let lab = OrgId::new();
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{}', 'lab', 'Lab', 'team');",
            lab.as_uuid()
        ))
        .await;
    let (jwt, admin) = member(&core, &sb, "ada").await;
    grant(&core, lab, admin, Role::Owner, Scope::Org).await;
    sqlx::query("UPDATE users SET platform_admin = true WHERE id = $1")
        .bind(admin.as_uuid())
        .execute(core.db.pool())
        .await
        .unwrap();
    let token = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Bearer(jwt.clone()),
            Some(json!({"name": "lab only", "scopes": ["*"]})),
        )
        .await;
    assert_eq!(token.status, 201, "{}", token.text());
    let token = token.json()["token"].as_str().unwrap().to_owned();
    let create = |auth: Auth, slug: &'static str| {
        let core = &core;
        async move {
            core.call(
                Method::POST,
                "/api/v1/orgs",
                &auth,
                Some(json!({"slug": slug, "name": slug})),
            )
            .await
        }
    };
    let refused = create(Auth::Bearer(token), "rival").await;
    assert_eq!(refused.status, 403, "{}", refused.text());
    assert_eq!(create(Auth::Bearer(jwt), "rival").await.status, 201);
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_viewer_login_token_can_revoke_itself() {
    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let lab = OrgId::new();
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{}', 'lab', 'Lab', 'team');",
            lab.as_uuid()
        ))
        .await;
    assert_eq!(core.sign_in(&sb, &Auth::None, false).await.status, 200);
    let viewer: uuid::Uuid = sqlx::query_scalar("SELECT id FROM users WHERE supabase_user_id = $1")
        .bind(sb.user().id)
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    grant(
        &core,
        lab,
        UserId::from_uuid(viewer),
        Role::Viewer,
        Scope::Org,
    )
    .await;
    // What `mrl login` does: sign in with a token, then drop it if the keychain fails.
    let issue = || async {
        let done = core.sign_in(&sb, &Auth::None, true).await;
        assert_eq!(done.status, 200, "{}", done.text());
        let t = &done.json()["token"];
        (
            t["id"].as_str().unwrap().to_owned(),
            t["token"].as_str().unwrap().to_owned(),
        )
    };
    let (id, token) = issue().await;
    let (other_id, _) = issue().await;
    let revoke = |id: String, token: String| {
        let core = &core;
        async move {
            core.call(
                Method::DELETE,
                &format!("/api/v1/auth/tokens/{id}"),
                &Auth::Bearer(token),
                None,
            )
            .await
        }
    };
    // A viewer may not manage tokens in general, so not another token of theirs.
    let other = revoke(other_id, token.clone()).await;
    assert_eq!(other.status, 403, "{}", other.text());
    let own = revoke(id, token.clone()).await;
    assert_eq!(own.status, 204, "{}", own.text());
    let after = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Bearer(token), None)
        .await;
    assert_eq!(after.status, 401, "{}", after.text());
    // Team sign-ins are audited in the member's org, so /audit shows them.
    let orgs: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT org_id FROM audit_events WHERE details->>'step' = 'signed_in' ORDER BY at",
    )
    .fetch_all(core.db.pool())
    .await
    .unwrap();
    assert_eq!(orgs.last(), Some(&Some(lab.to_string())), "{orgs:?}");
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn every_live_token_is_reachable_through_the_list_pages() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let owner: uuid::Uuid = sqlx::query_scalar("SELECT id FROM users WHERE is_local_owner")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    let ids: Vec<uuid::Uuid> = (0..205).map(|_| uuid::Uuid::now_v7()).collect();
    let rows: Vec<String> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            format!(
                "('{id}', '{owner}', 't{i}', 'mrl_pat_x{i}', decode(md5('{id}'), 'hex'), ARRAY['*'], now() + interval '1 day')"
            )
        })
        .collect();
    core.db
        .seed(&format!(
            "INSERT INTO api_tokens (id, user_id, name, token_prefix, token_hash, scopes, expires_at) VALUES {};",
            rows.join(", ")
        ))
        .await;
    let mut seen = Vec::new();
    let mut path = "/api/v1/auth/tokens?limit=200".to_owned();
    loop {
        let page = core.call(Method::GET, &path, &Auth::Launch, None).await;
        assert_eq!(page.status, 200, "{}", page.text());
        let page = page.json();
        seen.extend(
            page["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["id"].as_str().unwrap().to_owned()),
        );
        match page["next_cursor"].as_str() {
            Some(c) => path = format!("/api/v1/auth/tokens?limit=200&cursor={c}"),
            None => break,
        }
    }
    assert_eq!(seen.len(), 205);
    let oldest = mrl_domain::ids::TokenId::from_uuid(ids[0]).to_string();
    assert_eq!(
        seen.last(),
        Some(&oldest),
        "token 205 (the oldest) is on the last page"
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_new_supabase_project_never_gets_the_old_projects_session() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    let state = |r: TestResponse| r.json()["sign_in"].as_str().unwrap_or_default().to_owned();
    assert_eq!(
        state(
            core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
                .await
        ),
        "active"
    );
    // Same keychain, another project in the settings.
    let next = MockSupabase::start().await;
    let core = core.restart(config(Mode::Solo, &next)).await;
    assert_eq!(
        state(
            core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
                .await
        ),
        "signed_out"
    );
    // Signing in to the new project works and replaces the old session.
    assert_eq!(core.sign_in(&next, &Auth::Launch, false).await.status, 200);
    assert_eq!(
        state(
            core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
                .await
        ),
        "active"
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn overlapping_requests_refresh_the_rotating_token_once_at_a_time() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    sb.set_access_ttl(30); // inside the refresh margin: every use refreshes first
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    assert_eq!(
        core.call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE}))
        )
        .await
        .status,
        200
    );
    let status = || core.call(Method::GET, "/api/v1/sync/status", &Auth::Launch, None);
    let (a, b, c, d, e, f) = tokio::join!(
        push(&core),
        push(&core),
        status(),
        status(),
        push(&core),
        status()
    );
    for r in [a, b, c, d, e, f] {
        assert_eq!(r.status, 200, "{}", r.text());
        assert!(!r.text().contains("MRL-AUTH-160"), "{}", r.text());
    }
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
            .await
            .json()["sign_in"],
        "active"
    );
    // Sign-out is never undone by a refresh that was waiting.
    let (out, late) = tokio::join!(
        core.call(Method::POST, "/api/v1/auth/logout", &Auth::Launch, None),
        push(&core)
    );
    assert_eq!(out.status, 204, "{}", out.text());
    assert!(late.status == 200 || late.status == 401, "{}", late.text());
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
            .await
            .json()["sign_in"],
        "signed_out"
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_team_core_on_a_former_solo_database_has_no_local_owner() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let done = core.sign_in(&sb, &Auth::Launch, true).await;
    assert_eq!(done.status, 200, "{}", done.text());
    let pat = done.json()["token"]["token"].as_str().unwrap().to_owned();
    let core = core.restart(config(Mode::Team, &sb)).await;
    let jwt = sb.access_token(&sb.user(), 600);
    let me = core
        .call(
            Method::GET,
            "/api/v1/auth/me",
            &Auth::Bearer(jwt.clone()),
            None,
        )
        .await;
    assert_eq!(me.status, 200, "{}", me.text());
    assert_eq!(me.json()["principal_kind"], "user", "{}", me.text());
    // No owner shortcut and no implicit platform grant: neither credential creates orgs.
    for auth in [Auth::Bearer(jwt), Auth::Bearer(pat)] {
        let r = core
            .call(
                Method::POST,
                "/api/v1/orgs",
                &auth,
                Some(json!({"slug": "new-lab", "name": "New lab"})),
            )
            .await;
        assert_eq!(r.status, 403, "{}", r.text());
    }
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_named_project_must_exist_in_the_named_org() {
    let db = TestDb::new().await;
    let (a, b, ws_a, ws_b) = (
        OrgId::new(),
        OrgId::new(),
        WorkspaceId::new(),
        WorkspaceId::new(),
    );
    let (pa, pb) = (ProjectId::new(), ProjectId::new());
    let owner = UserId::new();
    db.seed(&format!(
        "INSERT INTO organizations (id, slug, name, kind) VALUES ('{a}', 'a', 'A', 'team'), ('{b}', 'b', 'B', 'team');
         INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{wa}', '{a}', 'main', 'Main'), ('{wb}', '{b}', 'main', 'Main');
         INSERT INTO projects (id, org_id, workspace_id, slug, name) VALUES ('{pa}', '{a}', '{wa}', 'p', 'P'), ('{pb}', '{b}', '{wb}', 'p', 'P');
         INSERT INTO users (id, display_name) VALUES ('{u}', 'o');",
        a = a.as_uuid(), b = b.as_uuid(), wa = ws_a.as_uuid(), wb = ws_b.as_uuid(),
        pa = pa.as_uuid(), pb = pb.as_uuid(), u = owner.as_uuid()
    ))
    .await;
    let mut tx = db.db().begin().await.unwrap();
    tenancy::bind_role(&mut tx, a, owner, Role::Owner, Scope::Org)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let on = |p: String| Resource {
        org: Some(a.to_string()),
        project: Some(p),
        ..Resource::kind("run")
    };
    let cases = [
        (pb.to_string(), Decision::Hide), // another org's project
        (ProjectId::new().to_string(), Decision::Hide), // no such project
        ("prj_not-an-id".to_owned(), Decision::Hide),
        (pa.to_string(), Decision::Allow), // same org
    ];
    let org_owner = ctx(Principal {
        id: owner.to_string(),
        kind: PrincipalKind::User,
        org: Some(a.to_string()),
    });
    let team = Rbac::new(db.db().clone(), Mode::Team);
    let solo = Rbac::new(db.db().clone(), Mode::Solo);
    for (project, want) in &cases {
        assert_eq!(
            team.authorize(&org_owner, "runs.submit", &on(project.clone()))
                .await
                .unwrap(),
            *want,
            "org owner, {project}"
        );
        assert_eq!(
            solo.authorize(
                &ctx(Principal::local_owner()),
                "runs.submit",
                &on(project.clone())
            )
            .await
            .unwrap(),
            *want,
            "solo owner, {project}"
        );
    }
    // A soft-deleted project and a malformed named org also look missing, to the solo
    // owner too.
    db.seed(&format!(
        "UPDATE projects SET deleted_at = now() WHERE id = '{}'",
        pa.as_uuid()
    ))
    .await;
    let bad_org = Resource {
        org: Some("org_not-an-id".to_owned()),
        project: Some(pb.to_string()),
        ..Resource::kind("run")
    };
    for (rbac, who) in [(&team, &org_owner), (&solo, &ctx(Principal::local_owner()))] {
        assert_eq!(
            rbac.authorize(who, "runs.submit", &on(pa.to_string()))
                .await
                .unwrap(),
            Decision::Hide,
            "deleted project"
        );
        assert_eq!(
            rbac.authorize(who, "runs.submit", &bad_org).await.unwrap(),
            Decision::Hide,
            "malformed org"
        );
    }
    db.pool().close().await;
}

/// Starts a sign-in and approves it in the mock as its current user; returns the callback
/// body (with a CLI token requested).
async fn approved(core: &CoreServer, sb: &MockSupabase) -> Value {
    let started = core
        .call(
            Method::POST,
            "/api/v1/auth/login",
            &Auth::Launch,
            Some(
                json!({ "provider": "github", "redirect_uri": "http://127.0.0.1:53111/callback" }),
            ),
        )
        .await
        .json();
    let back = sb
        .approve(started["authorize_url"].as_str().unwrap())
        .unwrap();
    let q: std::collections::HashMap<String, String> = reqwest::Url::parse(&back)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect();
    json!({ "flow_id": q["flow"], "code": q["code"], "issue_token": true, "token_name": "race" })
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn two_accounts_finishing_sign_in_at_once_link_only_one() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let first = approved(&core, &sb).await;
    sb.set_user(MockUser::new("bob@example.com", "Bob"));
    let second = approved(&core, &sb).await;
    let finish = |body: Value| {
        let core = &core;
        async move {
            core.call(
                Method::POST,
                "/api/v1/auth/callback",
                &Auth::Launch,
                Some(body),
            )
            .await
        }
    };
    let (a, b) = tokio::join!(finish(first), finish(second));
    let mut statuses = [a.status, b.status];
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 409], "{} / {}", a.text(), b.text());
    let winner = if a.status == 200 { a.json() } else { b.json() };
    assert!(winner["token"]["token"].as_str().is_some());
    assert_eq!(
        core.db.count("api_tokens").await,
        1,
        "only one account got a token"
    );
    // The owner link and the keychain session name the same account.
    let linked: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT supabase_user_id FROM users WHERE is_local_owner")
            .fetch_one(core.db.pool())
            .await
            .unwrap();
    let me = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(me["sign_in"], "active");
    assert_eq!(me["user"]["email"], winner["user"]["email"]);
    let stored = core
        .secrets
        .get("auth.supabase_session")
        .await
        .unwrap()
        .unwrap();
    let stored: Value = serde_json::from_str(stored.expose()).unwrap();
    assert_eq!(
        stored["user"]["id"].as_str().map(str::to_owned),
        linked.map(|l| l.to_string())
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_team_core_never_syncs_with_the_former_solo_owners_session() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    let setup = |auth: Auth| {
        let core = &core;
        async move {
            core.call(
                Method::POST,
                "/api/v1/sync/setup",
                &auth,
                Some(json!({"passphrase": PASSPHRASE})),
            )
            .await
        }
    };
    assert_eq!(setup(Auth::Launch).await.status, 200);
    assert_eq!(push(&core).await.status, 200);
    let history = sb.stored_rows();
    let core = core.restart(config(Mode::Team, &sb)).await;
    let (bob, _) = member(&core, &sb, "bob").await;
    // Setup sync is personal: a team core refuses all of it, before any key is made.
    for (method, path, body) in [
        (
            Method::POST,
            "/api/v1/sync/setup",
            Some(json!({"passphrase": PASSPHRASE})),
        ),
        (Method::POST, "/api/v1/sync/push", None),
        (
            Method::POST,
            "/api/v1/sync/restore",
            Some(json!({"passphrase": PASSPHRASE})),
        ),
        (Method::GET, "/api/v1/sync/status", None),
        (Method::GET, "/api/v1/sync/profile", None),
    ] {
        let r = core
            .call(method, path, &Auth::Bearer(bob.clone()), body)
            .await;
        assert_eq!(r.status, 409, "{path}: {}", r.text());
        assert_eq!(r.json()["code"], "MRL-STATE-161", "{path}");
        assert!(!r.text().contains("recovery_key"), "{path}");
    }
    assert_eq!(
        core.db.count("sync_state").await,
        1,
        "only the former owner's"
    );
    assert_eq!(
        sb.stored_rows(),
        history,
        "the former owner's history is unchanged"
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn the_audit_seam_commits_and_rolls_back_with_the_callers_transaction() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    // The production seam, as any module gets it from `state.seams.audit`.
    let store: Arc<dyn SecretStore> = core.secrets.clone();
    let sink: Arc<dyn AuditSink> = mrl_server::modules::auth::production_seams(
        &core.config,
        core.db.db(),
        store,
        crate::server::TEST_SECRET,
    )
    .await
    .unwrap()
    .build(Mode::Solo)
    .unwrap()
    .audit;
    let success = |target: &str| {
        mrl_server::modules::audit::event(
            time::OffsetDateTime::now_utc(),
            "local-owner",
            None,
            "runs.submit",
            target,
            mrl_server::seams::AuditOutcome::Success,
            "trace",
        )
    };
    let count = |target: &'static str| {
        let core = &core;
        async move {
            let n: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events WHERE target = $1")
                .bind(target)
                .fetch_one(core.db.pool())
                .await
                .unwrap();
            n
        }
    };
    let mut tx = core.db.db().begin().await.unwrap();
    sink.record_in(&mut tx, success("kept")).await.unwrap();
    tx.commit().await.unwrap();
    let mut tx = core.db.db().begin().await.unwrap();
    sink.record_in(&mut tx, success("undone")).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(count("kept").await, 1);
    assert_eq!(count("undone").await, 0);
    // On its own, the seam still refuses a success.
    assert!(sink.record(success("alone")).await.is_err());
    assert_eq!(count("alone").await, 0);
    core.stop().await;
}

/// Migration 16007 exactly as `3791ab7` shipped it.
const OLD_16007: &str =
    "-- Earlier P16 revisions let keyed requests to the secret-bearing endpoints (tokens, sessions,
-- sync setup and restore) reach the idempotency store, which keeps a SHA-256 of the body (a
-- fast check of a sync passphrase) and the response (a new token or recovery key). Those
-- endpoints now bypass the store; this clears what was kept. Rows only live 24 hours, so the
-- cost is that a retry in flight during the upgrade runs again instead of replaying.
DELETE FROM idempotency_keys;
";

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_database_made_by_3791ab7_takes_later_migrations_and_keeps_its_data() {
    use sqlx::migrate::{Migration, MigrationType};
    use sqlx::SqlSafeStr;
    let db = TestDb::empty().await;
    // What 3791ab7 applied: this build's migrations, with its own 16007 text.
    let mut old = mrl_server::db::migrate::migrator();
    old.migrations = old
        .migrations
        .iter()
        .map(|m| {
            if m.version == 16007 {
                Migration::new(
                    16007,
                    m.description.clone(),
                    MigrationType::Simple,
                    sqlx::AssertSqlSafe(OLD_16007.to_owned()).into_sql_str(),
                    false,
                )
            } else {
                m.clone()
            }
        })
        .collect::<Vec<_>>()
        .into();
    mrl_server::db::migrate::apply_with(db.db(), &old)
        .await
        .unwrap();
    db.seed(&kept("org", r#"{"id":"org_1","slug":"lab"}"#))
        .await;
    db.seed("INSERT INTO users (id, display_name) VALUES (gen_random_uuid(), 'kept');")
        .await;
    // A later build: this build's migrations plus one more.
    let mut later = mrl_server::db::migrate::migrator();
    let mut list = later.migrations.to_vec();
    list.push(Migration::new(
        99_001,
        "later".into(),
        MigrationType::Simple,
        sqlx::AssertSqlSafe("SELECT 1;".to_owned()).into_sql_str(),
        false,
    ));
    later.migrations = list.into();
    // What `migrate --dry-run` reads leaves the recorded checksum as it was.
    let recorded = || async {
        let c: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 16007")
                .fetch_one(db.pool())
                .await
                .unwrap();
        c
    };
    let before = recorded().await;
    let listed = mrl_server::db::migrate::pending(db.db(), &later)
        .await
        .unwrap();
    assert_eq!(
        listed.iter().map(|m| m.version).collect::<Vec<_>>(),
        vec![99_001]
    );
    assert_eq!(recorded().await, before);
    let refused = mrl_server::db::migrate::apply_with(db.db(), &later).await;
    assert!(
        refused.is_err(),
        "without the repair SQLx refuses the changed checksum"
    );
    mrl_server::modules::auth::repair_migration_checksums(db.db())
        .await
        .unwrap();
    assert_eq!(
        mrl_server::db::migrate::apply_with(db.db(), &later)
            .await
            .unwrap(),
        vec![99_001]
    );
    assert_eq!(keys(&db).await, vec!["org"], "replay records kept");
    let users: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE display_name = 'kept'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(users, 1);
    // A database made by this build is left as it is.
    let fresh = TestDb::new().await;
    mrl_server::modules::auth::repair_migration_checksums(fresh.db())
        .await
        .unwrap();
    assert!(
        mrl_server::db::migrate::pending(fresh.db(), &mrl_server::db::migrate::migrator())
            .await
            .unwrap()
            .is_empty()
    );
    fresh.pool().close().await;
    db.pool().close().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn audit_rows_written_before_their_month_partition_survive_a_restart() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    // The month partition is missing (the background role was down past the last one made).
    let month: String = sqlx::query_scalar("SELECT 'audit_events_' || to_char(now(), 'YYYY_MM')")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    core.db
        .seed(&format!(
            "DELETE FROM idempotency_keys; ALTER TABLE audit_events DETACH PARTITION {month}; DROP TABLE {month};"
        ))
        .await;
    // An audited change lands in the default partition.
    let made = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Launch,
            Some(json!({"name": "while down", "scopes": ["*"]})),
        )
        .await;
    assert_eq!(made.status, 201, "{}", made.text());
    let in_default: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events_default")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    assert!(in_default >= 1);
    let total = core.db.count("audit_events").await;
    // The restart makes this month's partition and keeps every row.
    let config = core.config.clone();
    let core = core.restart(config).await;
    assert_eq!(core.db.count("audit_events").await, total);
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_events_default WHERE at >= date_trunc('month', now())",
    )
    .fetch_one(core.db.pool())
    .await
    .unwrap();
    assert_eq!(left, 0, "moved into the month partition");
    let in_month: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {month}")))
            .fetch_one(core.db.pool())
            .await
            .unwrap();
    assert!(in_month >= 1);
    // Still read-only after the move.
    let changed = sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {month}")))
        .execute(core.db.pool())
        .await;
    assert!(changed.is_err());
    let changed = sqlx::query("DELETE FROM audit_events_default")
        .execute(core.db.pool())
        .await;
    assert!(changed.is_err() || changed.unwrap().rows_affected() == 0);
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn partition_attachment_waits_for_the_first_uncommitted_audit_row() {
    let db = TestDb::new().await;
    let mut writer = db.db().begin().await.unwrap();
    sqlx::query("INSERT INTO audit_events (id, at, actor, action, target, outcome) VALUES (gen_random_uuid(), '2091-03-04T00:00:00Z', 'actor', 'change', 'target', 'success')")
        .execute(&mut *writer).await.unwrap();
    let pool = db.pool().clone();
    let attach = tokio::spawn(async move {
        sqlx::query("SELECT audit_ensure_partition('2091-03-01'::date)")
            .execute(&pool)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND relation = 'audit_events_default'::regclass)",
            ).fetch_one(db.pool()).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    writer.commit().await.unwrap();
    attach.await.unwrap().unwrap();
    let moved: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_events_2091_03")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(moved, 1);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_scoped_token_stays_inside_its_org_and_project_for_account_actions() {
    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let (lab, rival, wl, wr) = (
        OrgId::new(),
        OrgId::new(),
        WorkspaceId::new(),
        WorkspaceId::new(),
    );
    let (pa, pb) = (ProjectId::new(), ProjectId::new());
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{a}', 'lab', 'Lab', 'team'), ('{b}', 'rival', 'Rival', 'team');
             INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{wa}', '{a}', 'main', 'Main'), ('{wb}', '{b}', 'main', 'Main');
             INSERT INTO projects (id, org_id, workspace_id, slug, name) VALUES ('{pa}', '{a}', '{wa}', 'p', 'P'), ('{pb}', '{b}', '{wb}', 'p', 'P');",
            a = lab.as_uuid(), b = rival.as_uuid(), wa = wl.as_uuid(), wb = wr.as_uuid(),
            pa = pa.as_uuid(), pb = pb.as_uuid()
        ))
        .await;
    let (carol, c) = member(&core, &sb, "carol").await;
    grant(&core, lab, c, Role::Maintainer, Scope::Org).await;
    grant(&core, rival, c, Role::Maintainer, Scope::Org).await;
    let make = |auth: Auth, project: ProjectId| {
        let core = &core;
        async move {
            core.call(
                Method::POST,
                "/api/v1/auth/tokens",
                &auth,
                Some(json!({"name": "t", "scopes": ["*"], "project_id": project.to_string()})),
            )
            .await
        }
    };
    let ta = make(Auth::Bearer(carol.clone()), pa).await;
    assert_eq!(ta.status, 201, "{}", ta.text());
    let tb = make(Auth::Bearer(carol.clone()), pb).await;
    assert_eq!(tb.status, 201, "{}", tb.text());
    let (ta_id, ta_token) = (
        ta.json()["id"].as_str().unwrap().to_owned(),
        ta.json()["token"].as_str().unwrap().to_owned(),
    );
    let tb_id = tb.json()["id"].as_str().unwrap().to_owned();
    let a = Auth::Bearer(ta_token.clone());
    // The list shows only the token's own project.
    let listed = core
        .call(Method::GET, "/api/v1/auth/tokens", &a, None)
        .await;
    assert_eq!(listed.status, 200, "{}", listed.text());
    let ids: Vec<String> = listed.json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids, vec![ta_id.clone()]);
    // It cannot revoke the sibling in the other org.
    let sibling = core
        .call(
            Method::DELETE,
            &format!("/api/v1/auth/tokens/{tb_id}"),
            &a,
            None,
        )
        .await;
    assert_eq!(sibling.status, 404, "{}", sibling.text());
    // /auth/me shows only bindings inside the grant.
    let me = core
        .call(Method::GET, "/api/v1/auth/me", &a, None)
        .await
        .json();
    let orgs: Vec<&str> = me["roles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["org"].as_str().unwrap())
        .collect();
    assert_eq!(orgs, vec![lab.to_string().as_str()], "{me}");
    // Losing write access in this project is not offset by a write role on a sibling.
    let sibling_in_lab = ProjectId::new();
    core.db.seed(&format!(
        "INSERT INTO projects (id, org_id, workspace_id, slug, name) VALUES ('{}', '{}', '{}', 'sibling', 'Sibling');
         UPDATE role_bindings SET role = 'viewer' WHERE org_id = '{}' AND principal_id = '{}';",
        sibling_in_lab.as_uuid(), lab.as_uuid(), wl.as_uuid(), lab.as_uuid(), c.as_uuid(),
    )).await;
    grant(
        &core,
        lab,
        c,
        Role::Researcher,
        Scope::Project(sibling_in_lab),
    )
    .await;
    let now_forbidden = core
        .call(Method::GET, "/api/v1/auth/tokens", &a, None)
        .await;
    assert_eq!(now_forbidden.status, 403, "{}", now_forbidden.text());
    // Self-revocation still works.
    let own = core
        .call(
            Method::DELETE,
            &format!("/api/v1/auth/tokens/{ta_id}"),
            &a,
            None,
        )
        .await;
    assert_eq!(own.status, 204, "{}", own.text());
    // A researcher bound to one project only may make tokens for that project.
    let (dave, d) = member(&core, &sb, "dave").await;
    grant(&core, lab, d, Role::Researcher, Scope::Project(pa)).await;
    let made = make(Auth::Bearer(dave.clone()), pa).await;
    assert_eq!(made.status, 201, "{}", made.text());
    let org_wide = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Bearer(dave.clone()),
            Some(json!({"name":"too broad","scopes":["*"]})),
        )
        .await;
    assert_eq!(org_wide.status, 404, "{}", org_wide.text());
    let elsewhere = make(Auth::Bearer(dave), pb).await;
    assert_eq!(elsewhere.status, 404, "{}", elsewhere.text());
    core.stop().await;
}

async fn sync_ready(sb: &MockSupabase) -> CoreServer {
    let core = solo(sb).await;
    assert_eq!(core.sign_in(sb, &Auth::Launch, false).await.status, 200);
    let setup = core
        .call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE})),
        )
        .await;
    assert_eq!(setup.status, 200, "{}", setup.text());
    core
}

fn kept_versions(sb: &MockSupabase) -> Vec<i64> {
    sb.stored_rows()
        .as_object()
        .and_then(|rows| rows.values().next().cloned())
        .and_then(|c| c.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .map(|c| c["version"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_retried_push_whose_reply_was_lost_takes_one_slot() {
    let sb = MockSupabase::start().await;
    let core = sync_ready(&sb).await;
    for _ in 0..5 {
        assert_eq!(push(&core).await.status, 200);
    }
    assert_eq!(kept_versions(&sb), vec![5, 4, 3, 2, 1]);
    let op = json!({"op_id": uuid::Uuid::now_v7().to_string()});
    let send = |body: Value| {
        let core = &core;
        async move {
            core.call(Method::POST, "/api/v1/sync/push", &Auth::Launch, Some(body))
                .await
        }
    };
    // Saved, but the reply never came back.
    sb.lose_push_replies(1);
    let lost = send(op.clone()).await;
    assert_eq!(lost.status, 503, "{}", lost.text());
    assert_eq!(lost.json()["code"], "MRL-LINK-160");
    // The retry with the same op id answers the saved copy; only one older copy went.
    let retry = send(op.clone()).await;
    assert_eq!(retry.status, 200, "{}", retry.text());
    assert_eq!(retry.json()["version"], 6);
    assert_eq!(kept_versions(&sb), vec![6, 5, 4, 3, 2]);
    // Status never goes back to an older copy, whatever order replies come in.
    core.db
        .seed("UPDATE sync_state SET last_pushed_version = 100")
        .await;
    assert_eq!(send(op.clone()).await.status, 200);
    let status = core
        .call(Method::GET, "/api/v1/sync/status", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(status["last_pushed_version"], 100, "{status}");
    for _ in 0..5 {
        assert_eq!(push(&core).await.status, 200);
    }
    let before = kept_versions(&sb);
    let replay = send(op).await;
    assert_eq!(replay.json()["version"], 6, "{}", replay.text());
    assert_eq!(kept_versions(&sb), before);
    let bad = send(json!({"op_id": "not-a-uuid"})).await;
    assert_eq!(bad.status, 422, "{}", bad.text());
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_key_change_whose_commit_reply_was_lost_keeps_the_active_key() {
    let sb = MockSupabase::start().await;
    let core = sync_ready(&sb).await;
    let owner: uuid::Uuid = sqlx::query_scalar("SELECT id FROM users WHERE is_local_owner")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    let owner = UserId::from_uuid(owner);
    let active: String = sqlx::query_scalar("SELECT key_ref FROM sync_state")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    let key = core.secrets.get(&active).await.unwrap().unwrap();
    let lost = || {
        mrl_server::error::ApiError::new(
            mrl_server::error::ErrorCode::Unavailable,
            "the commit reply was lost",
        )
    };
    // The commit landed (the row names the staged key) but its reply was lost.
    core.secrets
        .set("sync.key.landed", Secret::new(key.expose()))
        .await
        .unwrap();
    core.db
        .seed("UPDATE sync_state SET key_ref = 'sync.key.landed'")
        .await;
    mrl_server::modules::sync::settle_key(
        core.db.db(),
        core.secrets.as_ref(),
        owner,
        Err(lost()),
        "sync.key.landed",
        Some(active.clone()),
    )
    .await
    .unwrap();
    assert!(core.secrets.get("sync.key.landed").await.unwrap().is_some());
    assert!(
        core.secrets.get(&active).await.unwrap().is_none(),
        "old key retired"
    );
    assert_eq!(push(&core).await.status, 200, "sync still works");
    // A change that really rolled back drops only its staged key.
    core.secrets
        .set("sync.key.rolled", Secret::new(key.expose()))
        .await
        .unwrap();
    let failed = mrl_server::modules::sync::settle_key(
        core.db.db(),
        core.secrets.as_ref(),
        owner,
        Err(lost()),
        "sync.key.rolled",
        Some("sync.key.landed".to_owned()),
    )
    .await;
    assert!(failed.is_err());
    assert!(core.secrets.get("sync.key.rolled").await.unwrap().is_none());
    assert!(core.secrets.get("sync.key.landed").await.unwrap().is_some());
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn pushes_overlapping_a_key_rotation_never_see_sync_as_off() {
    let sb = MockSupabase::start().await;
    let core = sync_ready(&sb).await;
    for _ in 0..6 {
        let rotate = core.call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE})),
        );
        let (a, b, c) = tokio::join!(push(&core), rotate, push(&core));
        assert_eq!(b.status, 200, "{}", b.text());
        for r in [a, c] {
            assert_eq!(r.status, 200, "{}", r.text());
        }
    }
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_push_snapshots_the_profile_and_key_before_a_concurrent_restore() {
    let sb = MockSupabase::start().await;
    let core = sync_ready(&sb).await;
    assert_eq!(push(&core).await.status, 200);
    let other_pass = "a different sync passphrase";
    let second = core
        .call(
            Method::POST,
            "/api/v1/sync/setup",
            &Auth::Launch,
            Some(json!({"passphrase": other_pass})),
        )
        .await;
    assert_eq!(second.status, 200, "{}", second.text());
    let updated = core
        .call(
            Method::PUT,
            "/api/v1/sync/profile",
            &Auth::Launch,
            Some(json!({"schema":"setup-profile/v1", "preferences":{"theme":"dark"}})),
        )
        .await;
    assert_eq!(updated.status, 200, "{}", updated.text());
    let active: String = sqlx::query_scalar("SELECT key_ref FROM sync_state")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    let gate = core.pause_next_read(active);
    let restore = |version: i64, passphrase: &'static str| {
        let core = &core;
        async move {
            core.call(
                Method::POST,
                "/api/v1/sync/restore",
                &Auth::Launch,
                Some(json!({"version": version, "passphrase": passphrase})),
            )
            .await
        }
    };
    let (pushed, restored) = tokio::join!(push(&core), async {
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.read.notified())
            .await
            .unwrap();
        let restored = restore(1, PASSPHRASE).await;
        gate.resume.notify_one();
        restored
    });
    assert_eq!(restored.status, 200, "{}", restored.text());
    assert_eq!(pushed.status, 200, "{}", pushed.text());
    let latest = restore(2, other_pass).await;
    assert_eq!(latest.status, 200, "{}", latest.text());
    assert_eq!(latest.json()["profile"]["preferences"]["theme"], "dark");
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn credentials_in_keys_or_inside_longer_text_never_reach_storage_or_backups() {
    use base64::Engine as _;
    let sb = MockSupabase::start().await;
    let core = sync_ready(&sb).await;
    let canary = "hf_CANARYembedded8f3e1c9a";
    let put = |profile: Value| {
        let core = &core;
        async move {
            core.call(
                Method::PUT,
                "/api/v1/sync/profile",
                &Auth::Launch,
                Some(profile),
            )
            .await
        }
    };
    let with_ui = |ui: Value| {
        let mut p = profile();
        p["ui"] = ui;
        p
    };
    for bad in [
        with_ui(json!({"note": format!("my hub token is {canary}, keep it")})),
        with_ui(json!({ canary: true })),
        with_ui(json!({"nested": {"deeper": [format!("Bearer {canary}")]}})),
    ] {
        let r = put(bad).await;
        assert_eq!(r.status, 422, "{}", r.text());
        assert!(
            !r.text().contains(canary),
            "the refusal repeats nothing: {}",
            r.text()
        );
    }
    let stored: Option<Value> = sqlx::query_scalar("SELECT profile FROM setup_profiles")
        .fetch_optional(core.db.pool())
        .await
        .unwrap();
    assert!(!stored.unwrap_or_default().to_string().contains(canary));
    // A profile saved under older rules is checked again on load and push.
    core.db
        .seed(&format!(
            "INSERT INTO setup_profiles (user_id, profile) SELECT id, '{}'::jsonb FROM users WHERE is_local_owner \
             ON CONFLICT (user_id) DO UPDATE SET profile = EXCLUDED.profile",
            with_ui(json!({"note": format!("old {canary}")}))
        ))
        .await;
    for r in [
        core.call(Method::GET, "/api/v1/sync/profile", &Auth::Launch, None)
            .await,
        push(&core).await,
    ] {
        assert_eq!(r.status, 422, "{}", r.text());
        assert!(!r.text().contains(canary));
    }
    assert!(!sb.stored_rows().to_string().contains(canary));
    // A backup holding one (made elsewhere) is refused on restore and not saved here.
    let (key_ref, kdf): (String, Value) = sqlx::query_as("SELECT key_ref, kdf FROM sync_state")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    let hex = core.secrets.get(&key_ref).await.unwrap().unwrap();
    let key = mrl_server::modules::sync::crypto::key_from_hex(hex.expose()).unwrap();
    let aad = format!("mrl-sync/v1:{}", sb.user().id);
    let plain = with_ui(json!({"note": format!("restored {canary}")})).to_string();
    let sealed =
        mrl_server::modules::sync::crypto::seal(&key, aad.as_bytes(), plain.as_bytes()).unwrap();
    let blob = json!({
        "format": "mrl-sync/v1",
        "kdf": kdf,
        "sealed": base64::engine::general_purpose::STANDARD.encode(sealed),
    });
    let sent = reqwest::Client::new()
        .post(format!("{}/rest/v1/rpc/push_setup_copy", sb.url()))
        .header("apikey", sb.auth_config().supabase_anon_key.unwrap())
        .bearer_auth(sb.access_token(&sb.user(), 600))
        .json(&json!({ "p_blob": blob }))
        .send()
        .await
        .unwrap();
    assert!(sent.status().is_success());
    let restored = core
        .call(
            Method::POST,
            "/api/v1/sync/restore",
            &Auth::Launch,
            Some(json!({"passphrase": PASSPHRASE})),
        )
        .await;
    assert_eq!(restored.status, 422, "{}", restored.text());
    assert!(!restored.text().contains(canary));
    let stored: Value = sqlx::query_scalar("SELECT profile FROM setup_profiles")
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    assert!(!stored.to_string().contains("restored hf_"), "{stored}");
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn sign_in_uses_one_database_connection_in_both_modes() {
    let sb = MockSupabase::start().await;
    for mode in [Mode::Solo, Mode::Team] {
        let mut settings = config(mode, &sb);
        settings.server.roles = vec![mrl_server::config::Role::Api];
        settings.database.max_connections = 1;
        settings.database.acquire_timeout_seconds = 2;
        let core = CoreServer::try_start(settings, TestDb::new().await, Arc::default())
            .await
            .unwrap();
        let auth = if mode == Mode::Solo {
            Auth::Launch
        } else {
            Auth::None
        };
        let response = core.sign_in(&sb, &auth, true).await;
        assert_eq!(response.status, 200, "{mode:?}: {}", response.text());
        core.stop().await;
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_pending_sign_in_is_published_only_if_its_transaction_committed() {
    use mrl_server::modules::auth::signin::{PENDING_SECRET, SESSION_SECRET};
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    assert_eq!(core.sign_in(&sb, &Auth::Launch, false).await.status, 200);
    let raw = core.secrets.get(SESSION_SECRET).await.unwrap().unwrap();
    let session: Value = serde_json::from_str(raw.expose()).unwrap();
    core.secrets.delete(SESSION_SECRET).await.unwrap();

    let stage = |id: uuid::Uuid| {
        let store = core.secrets.clone();
        let pending = json!({"operation": id.to_string(), "session": session});
        async move {
            store
                .set(PENDING_SECRET, Secret::new(pending.to_string()))
                .await
                .unwrap()
        }
    };
    let state = || async {
        core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
            .await
            .json()["sign_in"]
            .clone()
    };
    stage(uuid::Uuid::new_v4()).await;
    assert_eq!(state().await, "signed_out");
    let committed: uuid::Uuid =
        sqlx::query_scalar("SELECT signin_operation FROM users WHERE is_local_owner")
            .fetch_one(core.db.pool())
            .await
            .unwrap();
    stage(committed).await;
    assert_eq!(state().await, "active");
    assert!(core.secrets.get(PENDING_SECRET).await.unwrap().is_none());
    let recovered = core.secrets.get(SESSION_SECRET).await.unwrap().unwrap();
    assert_eq!(recovered.expose(), raw.expose());
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn sign_in_and_sign_out_never_stop_half_way() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let signed_in = || async {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM audit_events WHERE details->>'step' = 'signed_in'",
        )
        .fetch_one(core.db.pool())
        .await
        .unwrap();
        n
    };
    // The keychain prompt is cancelled while the session is saved: nothing is left behind.
    core.fail_next_write(
        "auth.supabase_pending",
        "MRL-ENV-162: the keychain request was cancelled, so the app could not save a secret",
    );
    let refused = core.sign_in(&sb, &Auth::Launch, true).await;
    assert_eq!(refused.status, 503, "{}", refused.text());
    assert_eq!(refused.json()["code"], "MRL-ENV-162");
    assert_eq!(core.db.count("api_tokens").await, 0, "no token issued");
    assert_eq!(signed_in().await, 0, "no success audited");
    let linked: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT supabase_user_id FROM users WHERE is_local_owner")
            .fetch_one(core.db.pool())
            .await
            .unwrap();
    assert!(linked.is_none(), "the owner is not linked");
    // Trying again works.
    let done = core.sign_in(&sb, &Auth::Launch, true).await;
    assert_eq!(done.status, 200, "{}", done.text());
    let pat = done.json()["token"]["token"].as_str().unwrap().to_owned();
    // Sign-out whose database step fails has already ended the Supabase session, and
    // running it again finishes the rest.
    break_audit(&core).await;
    let failed = core
        .call(
            Method::POST,
            "/api/v1/auth/logout",
            &Auth::LaunchAndBearer(pat.clone()),
            None,
        )
        .await;
    assert!(failed.status >= 500, "{}", failed.text());
    let me = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
        .await
        .json();
    assert_eq!(me["sign_in"], "signed_out");
    mend_audit(&core).await;
    let again = core
        .call(
            Method::POST,
            "/api/v1/auth/logout",
            &Auth::LaunchAndBearer(pat.clone()),
            None,
        )
        .await;
    assert_eq!(again.status, 204, "{}", again.text());
    let after = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Bearer(pat), None)
        .await;
    assert_eq!(after.status, 401, "the token is revoked");
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_cold_key_cache_backs_off_while_the_sign_in_service_is_down() {
    let sb = MockSupabase::start().await;
    let db = TestDb::new().await;
    sb.set_offline(true);
    let v = JwtVerifier::new(
        Supabase::from_config(&sb.auth_config()).unwrap(),
        db.db().clone(),
    );
    let token = sb.access_token(&sb.user(), 300);
    for _ in 0..5 {
        assert!(v.verify(&token).await.unwrap().is_none());
    }
    assert_eq!(sb.jwks_requests(), 1, "one failed fetch, then backoff");
    db.pool().close().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_solo_owners_token_reads_the_whole_audit_log() {
    let sb = MockSupabase::start().await;
    let core = solo(&sb).await;
    let made = core
        .call(
            Method::POST,
            "/api/v1/auth/tokens",
            &Auth::Launch,
            Some(json!({"name": "sdk", "scopes": ["*"]})),
        )
        .await;
    assert_eq!(made.status, 201, "{}", made.text());
    let token = Auth::Bearer(made.json()["token"].as_str().unwrap().to_owned());
    let read = |auth: Auth| {
        let core = &core;
        async move {
            let r = core.call(Method::GET, "/api/v1/audit", &auth, None).await;
            assert_eq!(r.status, 200, "{}", r.text());
            r.json()["items"].as_array().unwrap().len()
        }
    };
    let by_owner = read(Auth::Launch).await;
    assert!(by_owner > 0);
    assert_eq!(
        read(token).await,
        by_owner,
        "the owner's token sees what the owner sees"
    );
    core.stop().await;
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_token_held_to_a_project_never_reaches_setup_sync() {
    let sb = MockSupabase::start().await;
    let core = sync_ready(&sb).await;
    let post = |path: String, body: Value| {
        let core = &core;
        async move {
            let r = core
                .call(Method::POST, &path, &Auth::Launch, Some(body))
                .await;
            assert_eq!(r.status, 201, "{}", r.text());
            r.json()
        }
    };
    let org = post("/api/v1/orgs".into(), json!({"slug": "lab", "name": "Lab"})).await;
    let ws = post(
        format!("/api/v1/orgs/{}/workspaces", org["id"].as_str().unwrap()),
        json!({"slug": "main", "name": "Main"}),
    )
    .await;
    let prj = post(
        format!("/api/v1/workspaces/{}/projects", ws["id"].as_str().unwrap()),
        json!({"slug": "p", "name": "P"}),
    )
    .await;
    let bound = post(
        "/api/v1/auth/tokens".into(),
        json!({"name": "p", "scopes": ["*"], "project_id": prj["id"]}),
    )
    .await;
    let unbound = post(
        "/api/v1/auth/tokens".into(),
        json!({"name": "all", "scopes": ["*"]}),
    )
    .await;
    let call = |method: Method, path: &'static str, token: &Value, body: Option<Value>| {
        let core = &core;
        let auth = Auth::Bearer(token["token"].as_str().unwrap().to_owned());
        async move { core.call(method, path, &auth, body).await.status }
    };
    for (method, path, body) in [
        (Method::GET, "/api/v1/sync/profile", None),
        (Method::PUT, "/api/v1/sync/profile", Some(profile())),
        (
            Method::POST,
            "/api/v1/sync/setup",
            Some(json!({"passphrase": PASSPHRASE})),
        ),
        (Method::POST, "/api/v1/sync/push", None),
        (
            Method::POST,
            "/api/v1/sync/restore",
            Some(json!({"passphrase": PASSPHRASE})),
        ),
        (Method::GET, "/api/v1/sync/status", None),
    ] {
        assert_eq!(
            call(method.clone(), path, &bound, body).await,
            403,
            "{method} {path}"
        );
    }
    assert_eq!(
        call(Method::GET, "/api/v1/sync/profile", &unbound, None).await,
        200
    );
    assert_eq!(
        call(Method::GET, "/api/v1/sync/status", &unbound, None).await,
        200
    );
    let restricted = post(
        "/api/v1/auth/tokens".into(),
        json!({"name":"reader","scopes":["runs:read"]}),
    )
    .await;
    let login = Some(json!({"provider":"github","redirect_uri":"masterrl://auth/callback"}));
    for token in [&bound, &restricted] {
        assert_eq!(
            call(Method::POST, "/api/v1/auth/login", token, login.clone()).await,
            403
        );
        assert_eq!(
            call(Method::DELETE, "/api/v1/auth/session", token, None).await,
            403
        );
    }
    assert_eq!(
        call(Method::POST, "/api/v1/auth/login", &unbound, login).await,
        200
    );
    assert_eq!(
        call(Method::POST, "/api/v1/auth/logout", &bound, None).await,
        204
    );
    assert_eq!(
        call(Method::GET, "/api/v1/auth/me", &bound, None).await,
        401
    );
    let local = core
        .call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None)
        .await;
    assert_eq!(local.json()["sign_in"], "active");
    core.stop().await;
}
