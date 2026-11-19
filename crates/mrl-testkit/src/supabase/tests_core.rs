//! P16 integration tests: a production-wired core against the mock Supabase and a real
//! PostgreSQL (`just test-integration`).

use super::core::{Auth, CoreServer};
use super::{MockSupabase, MockUser};
use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::TestResponse;
use axum::http::Method;
use mrl_domain::ids::{OrgId, ProjectId, UserId, WorkspaceId};
use mrl_domain::rbac::{allowed, Action, Role};
use mrl_server::config::Mode;
use mrl_server::modules::auth::supabase::{JwtVerifier, Supabase};
use mrl_server::modules::tenancy::{self, Rbac, Scope};
use mrl_server::seams::{
    AuditSink, Authorizer, Decision, Principal, PrincipalKind, RequestContext, RequestMeta,
    Resource, Secret, SecretStore,
};
use serde_json::{json, Value};
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
    assert!(
        v.verify(&other.access_token(&user, 300))
            .await
            .unwrap()
            .is_none(),
        "other project's key and issuer"
    );
    assert!(v.verify("not.a.jwt").await.unwrap().is_none());
    let mut parts: Vec<String> = sb
        .access_token(&user, 300)
        .split('.')
        .map(str::to_owned)
        .collect();
    parts[1] = parts[1].chars().rev().collect();
    assert!(
        v.verify(&parts.join(".")).await.unwrap().is_none(),
        "tampered"
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
    let rbac = Rbac::new(db.db().clone());
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
    let public = ["/api/v1/auth/login", "/api/v1/auth/callback"];
    let sb = MockSupabase::start().await;
    let core = CoreServer::start(Mode::Team, &sb, TestDb::new().await, Arc::default()).await;
    let (org, ws) = (OrgId::new(), WorkspaceId::new());
    core.db
        .seed(&format!(
            "INSERT INTO organizations (id, slug, name, kind) VALUES ('{o}', 'lab', 'Lab', 'team');
             INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{w}', '{o}', 'main', 'Main');",
            o = org.as_uuid(),
            w = ws.as_uuid()
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
    let matches = |pattern: &str, path: &str| {
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
            let r = core
                .call(
                    method.clone(),
                    path,
                    &Auth::Bearer(jwt.clone()),
                    body.clone(),
                )
                .await;
            let refused = r.status == 403 && r.json()["code"] == "MRL-PERM-001";
            // Creating orgs needs the platform grant, which no org role gives.
            let platform = *method == Method::POST && path == "/api/v1/orgs";
            assert_eq!(
                refused,
                platform || !allowed(role, *action),
                "{role}: {method} {path} answered {} {}",
                r.status,
                r.text()
            );
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
    }
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
    let rbac = Rbac::new(core.db.db().clone());
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
        "active",
        "and the sign-in is kept"
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
    let routes: [(&str, Option<Value>); 3] = [
        (
            "/api/v1/auth/tokens",
            Some(json!({"name": "x", "scopes": ["*"]})),
        ),
        ("/api/v1/auth/session", None),
        (
            "/api/v1/sync/setup",
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
    let stored: Vec<String> = sqlx::query_scalar(
        "SELECT encode(coalesce(response_body, ''::bytea), 'escape') FROM idempotency_keys",
    )
    .fetch_all(core.db.pool())
    .await
    .unwrap();
    assert!(
        stored.iter().all(|b| !b.contains("mrl_pat_")
            && !b.contains("recovery_key")
            && !b.contains("csrf_token")),
        "{stored:?}"
    );
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
