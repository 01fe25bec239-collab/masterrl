//! Integration tests: real listeners on loopback, testkit seams.

use super::*;
use crate::seams::{self as ts, user};
use mrl_server::config::BucketConfig;
use mrl_server::error::ErrorCode;
use mrl_server::seams::Seams as ServerSeams;
use std::net::Ipv4Addr;

fn module(m: TestModule) -> Vec<Box<dyn ServerModule>> {
    vec![Box::new(m)]
}

fn team_config() -> Config {
    let mut c = Config::default();
    c.server.mode = Mode::Team;
    let any = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    c.server.listeners.api = any;
    c.server.listeners.grpc = any;
    c.server.listeners.internal = any;
    c
}

async fn team_server(config: Config, m: TestModule) -> TestServer {
    let seams = ts::build(ts::team(&[("tok-1", user("usr_1", "org_1"))]), Mode::Team);
    match TestServer::start(config, seams, module(m)).await {
        Ok(s) => s,
        Err(e) => panic!("{}", e.explain()),
    }
}

#[tokio::test]
async fn solo_boots_and_serves_health_metrics_openapi_and_module_route() {
    let srv = TestServer::solo(module(TestModule::default())).await;
    assert!(srv.api().ip().is_loopback());
    assert_eq!(
        srv.running().internal_addr(),
        None,
        "solo binds one port only"
    );
    assert_eq!(
        srv.running().grpc_addr(),
        None,
        "no gRPC module, no gRPC port"
    );

    for path in [
        "/healthz",
        "/readyz",
        "/api-docs/openapi.json",
        "/api/v1/hello",
        "/metrics",
        "/nope",
    ] {
        let r = srv.get(path).await;
        assert_eq!(r.status, 401, "{path} needs the launch secret in solo");
        assert_eq!(r.json()["code"], "MRL-AUTH-001", "{path}");
    }
    assert_eq!(srv.get_authed("/healthz").await.json()["status"], "ok");
    let ready = srv.get_authed("/readyz").await;
    assert_eq!(ready.status, 200);
    assert_eq!(ready.json()["checks"][0]["name"], "testkit");

    let doc = srv.get_authed("/api-docs/openapi.json").await;
    assert_eq!(doc.status, 200);
    assert!(doc.json()["paths"]["/api/v1/ping"].is_object());
    assert!(doc.json()["paths"]["/healthz"].is_object());

    let denied = srv.get("/api/v1/ping").await;
    assert_eq!(denied.status, 401);
    assert_eq!(denied.json()["code"], "MRL-AUTH-001");
    assert_eq!(srv.get_authed("/api/v1/ping").await.json()["pong"], true);
    assert_eq!(srv.get_authed("/api/v1/hello").await.text(), "hello");
    let metrics = srv.get_authed("/metrics").await.text();
    assert!(metrics.contains(
        "mrl_http_requests_total{method=\"GET\",route=\"/api/v1/ping\",status=\"200\"} 1"
    ));
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn team_boots_with_identity_provider() {
    let srv = team_server(team_config(), TestModule::default()).await;
    assert_eq!(srv.get("/api/v1/ping").await.status, 401);
    let ok = srv
        .request(
            Method::GET,
            "/api/v1/ping",
            &[("authorization", "Bearer tok-1")],
            Body::empty(),
        )
        .await;
    assert_eq!(ok.status, 200);
    let bad = srv
        .request(
            Method::GET,
            "/api/v1/ping",
            &[("authorization", "Bearer nope")],
            Body::empty(),
        )
        .await;
    assert_eq!(bad.status, 401);
    let internal = srv.running().internal_addr().unwrap();
    let m = TestServer::request_to(internal, Method::GET, "/metrics", &[], Body::empty()).await;
    assert_eq!(m.status, 200, "internal metrics need no auth");
    assert!(m.text().contains("mrl_server_build_info"));
    assert_eq!(
        srv.get("/metrics").await.status,
        404,
        "metrics stay off the api port in team mode"
    );
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_seams_stop_startup_with_catalogued_errors() {
    let no_auth = ServerSeams::builder()
        .authorizer(ts::StaticAuthorizer(mrl_server::seams::Decision::Allow))
        .build(Mode::Solo)
        .err()
        .unwrap();
    assert_eq!(no_auth.code, ErrorCode::SeamMissing);
    assert!(no_auth.detail.contains("authenticator"));
    assert!(no_auth.detail.contains("idempotency store"));

    let no_idp = ts::solo("x").build(Mode::Team).err().unwrap();
    assert_eq!(no_idp.code, ErrorCode::IdentityProviderMissing);

    let prod = ServerSeams::production(Mode::Team, "x")
        .build(Mode::Team)
        .err()
        .unwrap();
    assert_eq!(prod.code, ErrorCode::IdentityProviderMissing);
    assert!(ServerSeams::production(Mode::Solo, "x")
        .build(Mode::Solo)
        .is_ok());
}

#[tokio::test]
async fn graceful_shutdown_finishes_in_flight_requests() {
    let srv = TestServer::solo(module(TestModule::default())).await;
    let addr = srv.api();
    let secret = srv.secret.clone();
    let slow = tokio::spawn(async move {
        TestServer::request_to(
            addr,
            Method::GET,
            "/api/v1/slow",
            &[(LAUNCH_SECRET_HEADER, &secret)],
            Body::empty(),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    srv.shutdown().await.unwrap();
    let res = slow.await.unwrap();
    assert_eq!((res.status, res.text().as_str()), (200, "done"));
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "listener is closed"
    );
}

#[tokio::test]
async fn drain_timeout_is_reported() {
    let mut config = Config::default();
    config.server.shutdown_grace_seconds = 1;
    let m = TestModule {
        task: TaskBehaviour::IgnoresCancel,
        ..TestModule::default()
    };
    let srv = TestServer::start(
        config,
        ts::build(ts::solo(TEST_SECRET), Mode::Solo),
        module(m),
    )
    .await
    .unwrap();
    assert_eq!(
        srv.shutdown().await.unwrap_err().code,
        ErrorCode::DrainTimeout
    );
}

#[tokio::test]
async fn background_task_runs_and_stops_on_shutdown() {
    let m = TestModule {
        task: TaskBehaviour::UntilCancelled,
        ..TestModule::default()
    };
    let probe = m.probe.clone();
    let srv = TestServer::solo(module(m)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(probe.task_started.load(Ordering::SeqCst));
    srv.shutdown().await.unwrap();
    assert!(probe.task_stopped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failing_task_shuts_the_server_down() {
    let m = TestModule {
        task: TaskBehaviour::Fails,
        ..TestModule::default()
    };
    let srv = TestServer::solo(module(m)).await;
    let err = tokio::time::timeout(Duration::from_secs(5), srv.running.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::TaskFailed);
    assert!(err.detail.contains("breaker"));
}

#[tokio::test]
async fn roles_decide_what_runs() {
    let mut config = Config::default();
    config.server.roles = vec![Role::Api];
    let m = TestModule {
        task: TaskBehaviour::UntilCancelled,
        grpc: true,
        ..TestModule::default()
    };
    let probe = m.probe.clone();
    let srv = TestServer::start(
        config,
        ts::build(ts::solo(TEST_SECRET), Mode::Solo),
        module(m),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !probe.task_started.load(Ordering::SeqCst),
        "background role is off"
    );
    assert_eq!(srv.running().grpc_addr(), None, "gateway role is off");
    srv.shutdown().await.unwrap();

    let mut config = Config::default();
    config.server.roles = vec![Role::Background];
    let srv = TestServer::start(
        config,
        ts::build(ts::solo(TEST_SECRET), Mode::Solo),
        module(TestModule::default()),
    )
    .await
    .unwrap();
    assert_eq!(srv.running().api_addr(), None, "api role is off");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_step_failure_stops_startup() {
    let m = TestModule {
        failing_step: true,
        ..TestModule::default()
    };
    let probe = m.probe.clone();
    let err = TestServer::start(
        Config::default(),
        ts::build(ts::solo(TEST_SECRET), Mode::Solo),
        module(m),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(err.code, ErrorCode::StartupStep);
    assert!(err.detail.contains("testkit/prepare"));
    assert_eq!(probe.steps_run.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn readyz_reports_failed_checks() {
    let srv = TestServer::solo(module(TestModule {
        not_ready: true,
        ..TestModule::default()
    }))
    .await;
    let r = srv.get_authed("/readyz").await;
    assert_eq!(r.status, 503);
    assert_eq!(r.json()["checks"][0]["status"], "not_ready");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_listener_binds_only_with_a_grpc_module() {
    let srv = TestServer::solo(module(TestModule {
        grpc: true,
        ..TestModule::default()
    }))
    .await;
    let grpc = srv.running().grpc_addr().unwrap();
    assert!(grpc.ip().is_loopback());
    assert_ne!(grpc, srv.api());
    let denied = TestServer::request_to(
        grpc,
        Method::POST,
        "/mrl.testkit.Ping/Call",
        &[],
        Body::empty(),
    )
    .await;
    assert_eq!(denied.status, 401, "solo gRPC needs the launch secret");
    let ok = TestServer::request_to(
        grpc,
        Method::POST,
        "/mrl.testkit.Ping/Call",
        &[(LAUNCH_SECRET_HEADER, TEST_SECRET)],
        Body::empty(),
    )
    .await;
    assert_eq!(ok.headers["grpc-status"], "0");
    srv.shutdown().await.unwrap();

    let team = team_server(team_config(), TestModule::default()).await;
    assert_eq!(team.running().grpc_addr(), None);
    team.shutdown().await.unwrap();
}

#[tokio::test]
async fn rate_limit_returns_429_with_retry_after() {
    let mut config = Config::default();
    config.server.rate_limit.principal = BucketConfig {
        per_second: 1,
        burst: 2,
    };
    let srv = TestServer::start(
        config,
        ts::build(ts::solo(TEST_SECRET), Mode::Solo),
        module(TestModule::default()),
    )
    .await
    .unwrap();
    assert_eq!(srv.get_authed("/api/v1/ping").await.status, 200);
    assert_eq!(srv.get_authed("/api/v1/ping").await.status, 200);
    let limited = srv.get_authed("/api/v1/ping").await;
    assert_eq!(limited.status, 429);
    assert_eq!(limited.headers["retry-after"], "1");
    assert_eq!(limited.json()["code"], "MRL-QUOTA-001");
    assert_eq!(
        srv.get_authed("/healthz").await.status,
        200,
        "health is never limited"
    );
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn idempotent_post_replays_over_the_wire() {
    let srv = TestServer::solo(module(TestModule::default())).await;
    let headers = [
        (LAUNCH_SECRET_HEADER, TEST_SECRET),
        ("content-type", "application/json"),
        ("idempotency-key", "abc"),
    ];
    let first = srv
        .request(Method::POST, "/api/v1/items", &headers, r#"{"n":1}"#)
        .await;
    assert_eq!(first.status, 201);
    let again = srv
        .request(Method::POST, "/api/v1/items", &headers, r#"{"n":1}"#)
        .await;
    assert_eq!(
        (again.status, again.body.clone()),
        (201, first.body.clone())
    );
    assert_eq!(again.headers["idempotent-replayed"], "true");
    let other = srv
        .request(Method::POST, "/api/v1/items", &headers, r#"{"n":2}"#)
        .await;
    assert_eq!(other.status, 422);
    assert_eq!(other.json()["code"], "MRL-IDEM-002");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn port_in_use_is_a_catalogued_error() {
    let taken = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let mut config = team_config();
    config.server.listeners.api = taken.local_addr().unwrap();
    let seams = ts::build(ts::team(&[]), Mode::Team);
    let err = TestServer::start(config, seams, module(TestModule::default()))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code, ErrorCode::Bind);
}

#[tokio::test]
async fn authorize_maps_decisions_to_errors() {
    use mrl_server::seams::{Decision, RequestContext, RequestMeta, Resource};
    let meta = RequestMeta {
        trace_id: "t".into(),
        request_id: "r".into(),
    };
    let ctx = RequestContext::new(user("usr_1", "org_1"), &meta).with_project("prj_1");
    assert_eq!(ctx.org.as_deref(), Some("org_1"));
    for (decision, want) in [
        (Decision::Allow, None),
        (Decision::Deny, Some(ErrorCode::Forbidden)),
        (Decision::Hide, Some(ErrorCode::NotFound)),
    ] {
        let seams = ts::build(
            ts::solo("s").authorizer(ts::StaticAuthorizer(decision)),
            Mode::Solo,
        );
        let state = AppState::new(Config::default(), seams, &[]).unwrap();
        let got = state
            .authorize(&ctx, "runs.read", &Resource::kind("run"))
            .await;
        assert_eq!(got.err().map(|e| e.code), want, "{decision:?}");
    }
}

#[tokio::test]
async fn oversized_body_over_the_wire_is_problem_413() {
    let srv = TestServer::solo(module(TestModule::default())).await;
    let big = "x".repeat(mrl_server::http::BODY_LIMIT + 1);
    let len = big.len().to_string();
    let headers = [
        (LAUNCH_SECRET_HEADER, TEST_SECRET),
        ("content-type", "application/json"),
        ("content-length", len.as_str()),
    ];
    let r = srv
        .request(Method::POST, "/api/v1/items", &headers, big)
        .await;
    assert_eq!(r.status, 413);
    assert_eq!(r.headers["content-type"], "application/problem+json");
    assert_eq!(r.json()["code"], "MRL-VAL-007");
    assert_eq!(r.json()["trace_id"].as_str().unwrap().len(), 32);
    srv.shutdown().await.unwrap();
}

/// Writes a throwaway self-signed `localhost` cert and key; returns (dir, cert DER).
fn self_signed(tag: &str) -> (std::path::PathBuf, Vec<u8>) {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let dir = std::env::temp_dir().join(format!("mrl-tls-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("cert.pem"), ck.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), ck.signing_key.serialize_pem()).unwrap();
    (dir, ck.cert.der().to_vec())
}

fn tls_config(dir: &std::path::Path) -> Config {
    let mut c = team_config();
    c.server.tls = Some(mrl_server::config::TlsConfig {
        cert_path: dir.join("cert.pem"),
        key_path: dir.join("key.pem"),
    });
    c
}

#[tokio::test]
async fn team_mode_serves_https_when_tls_is_configured() {
    use tokio_rustls::rustls;
    let (dir, der) = self_signed("ok");
    let srv = team_server(tls_config(&dir), TestModule::default()).await;
    let api = srv.api();

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(der))
        .unwrap();
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    let tcp = tokio::net::TcpStream::connect(api).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tls = connector.connect(name, tcp).await.unwrap();
    let res = TestServer::request_on(
        tls,
        "localhost",
        Method::GET,
        "/api/v1/ping",
        &[("authorization", "Bearer tok-1")],
        Body::empty(),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["pong"], true);

    // Plain HTTP to the HTTPS port gets no HTTP answer.
    let plain = tokio::net::TcpStream::connect(api).await.unwrap();
    let mut sender =
        hyper::client::conn::http1::handshake::<_, Body>(hyper_util::rt::TokioIo::new(plain))
            .await
            .map(|(s, conn)| {
                tokio::spawn(conn);
                s
            })
            .unwrap();
    let req = Request::get("/healthz")
        .header("host", "x")
        .body(Body::empty())
        .unwrap();
    assert!(sender.send_request(req).await.is_err());

    // The internal listener stays plain HTTP.
    let internal = srv.running().internal_addr().unwrap();
    let health =
        TestServer::request_to(internal, Method::GET, "/healthz", &[], Body::empty()).await;
    assert_eq!(health.status, 200);
    srv.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn unusable_tls_files_stop_startup() {
    let (dir, _) = self_signed("bad");
    std::fs::write(dir.join("key.pem"), "not a key").unwrap();
    let seams = ts::build(ts::team(&[]), Mode::Team);
    let err = TestServer::start(tls_config(&dir), seams, module(TestModule::default()))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code, ErrorCode::TlsFiles);
    assert!(err.detail.contains("server.tls.key_path"), "{}", err.detail);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn solo_ignores_tls_and_stays_plain_loopback() {
    let (dir, _) = self_signed("solo");
    let mut config = tls_config(&dir);
    config.server.mode = Mode::Solo;
    let srv = TestServer::start(
        config,
        ts::build(ts::solo(TEST_SECRET), Mode::Solo),
        module(TestModule::default()),
    )
    .await
    .unwrap();
    assert!(srv.api().ip().is_loopback());
    assert_eq!(srv.get_authed("/healthz").await.status, 200);
    srv.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
