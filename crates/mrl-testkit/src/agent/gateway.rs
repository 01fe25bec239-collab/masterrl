//! A real gateway (server `nodes` module on a test database) and in-process agents with fake
//! GPUs that speak the real protocol over mutual TLS on loopback.
//!
//! ```no_run
//! # async fn demo() {
//! use mrl_testkit::agent::gateway::{GatewayHarness, TestAgent};
//! let gw = GatewayHarness::start(mrl_testkit::pg::TestDb::new().await).await;
//! let token = gw.token(serde_json::json!({})).await;
//! let agent = TestAgent::new(2, |_| {});
//! let node = agent.enroll(&gw, &token).await.unwrap();
//! let link = agent.run();
//! gw.wait_status(&node, "ONLINE").await;
//! link.stop().await;
//! # }
//! ```

use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use axum::body::Body;
use axum::http::Method;
use mrl_agent::config::{AgentConfig, CacheConfig, SandboxChoice, SandboxConfig};
use mrl_agent::hw::fake::FakeHw;
use mrl_agent::hw::HwBackend;
use mrl_agent::registry::AgentService;
use mrl_agent::session::{GatewayService, LinkError};
use mrl_agent::AgentContext;
use mrl_server::config::{Config, Mode};
use mrl_server::modules::nodes::lanes::LaneRouter;
use mrl_server::modules::nodes::{Gateway, GatewayConfig, NodesModule};
use mrl_server::seams::{SecretStore, LAUNCH_SECRET_HEADER};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Settings fast enough for tests: 1 s heartbeats, checks and revocation re-checks.
pub fn fast_config() -> GatewayConfig {
    GatewayConfig {
        heartbeat_interval_seconds: 1,
        degraded_after_seconds: 2,
        offline_after_seconds: 3,
        health_check_seconds: 1,
        revocation_check_seconds: 1,
        ..GatewayConfig::default()
    }
}

/// A solo-mode server with only the `nodes` module, on its own test database.
pub struct GatewayHarness {
    /// The HTTP server.
    pub server: TestServer,
    /// The gateway state (address, CA, command senders).
    pub gw: Arc<Gateway>,
    /// The database.
    pub db: TestDb,
    /// `pool_…` created at start.
    pub pool: String,
    secrets: Arc<dyn SecretStore>,
    lanes: Arc<LaneRouter>,
    config: GatewayConfig,
}

impl GatewayHarness {
    /// Starts with [`fast_config`] and creates pool `test`.
    pub async fn start(db: TestDb) -> Self {
        Self::start_with(
            db,
            fast_config(),
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
        )
        .await
    }

    /// Starts with these settings, secret store and lane router.
    pub async fn start_with(
        db: TestDb,
        config: GatewayConfig,
        secrets: Arc<dyn SecretStore>,
        lanes: Arc<LaneRouter>,
    ) -> Self {
        let (server, gw) = Self::boot(&db, &config, secrets.clone(), lanes.clone()).await;
        let mut h = Self {
            server,
            gw,
            db,
            pool: String::new(),
            secrets,
            lanes,
            config,
        };
        let r = h.post("/api/v1/pools", json!({ "name": "test" })).await;
        assert_eq!(r.status, 201, "create pool: {}", r.text());
        h.pool = r.json()["id"].as_str().unwrap_or_default().to_owned();
        h
    }

    async fn boot(
        db: &TestDb,
        config: &GatewayConfig,
        secrets: Arc<dyn SecretStore>,
        lanes: Arc<LaneRouter>,
    ) -> (TestServer, Arc<Gateway>) {
        let cfg = Config {
            gateway: config.clone(),
            ..Config::default()
        };
        let module = NodesModule::new(db.db().clone(), &cfg, secrets, lanes);
        let gw = module.gateway();
        let seams = crate::seams::build(crate::seams::solo(TEST_SECRET), Mode::Solo);
        match TestServer::start(cfg, seams, vec![Box::new(module)]).await {
            Ok(s) => (s, gw),
            Err(e) => panic!("gateway test server did not start: {}", e.explain()),
        }
    }

    /// A second server (replica) on the same database and secret store, with its own gateway port.
    pub async fn replica(&self) -> (TestServer, Arc<Gateway>) {
        let config = GatewayConfig {
            listen: None,
            ..self.config.clone()
        };
        Self::boot(&self.db, &config, self.secrets.clone(), self.lanes.clone()).await
    }

    /// Stops the server and starts a new one on the same database, CA and gateway port.
    pub async fn restart(self) -> Self {
        let addr = self.addr();
        if let Err(e) = self.server.shutdown().await {
            panic!("shutdown: {}", e.explain());
        }
        let config = GatewayConfig {
            listen: Some(addr),
            ..self.config.clone()
        };
        let (server, gw) =
            Self::boot(&self.db, &config, self.secrets.clone(), self.lanes.clone()).await;
        Self {
            server,
            gw,
            config,
            ..self
        }
    }

    /// The gateway address agents dial.
    pub fn addr(&self) -> SocketAddr {
        self.gw
            .addr()
            .unwrap_or_else(|| panic!("the gateway is not bound"))
    }

    /// Authenticated JSON POST.
    pub async fn post(&self, path: &str, body: Value) -> TestResponse {
        self.server
            .request(
                Method::POST,
                path,
                &[
                    (LAUNCH_SECRET_HEADER, TEST_SECRET),
                    ("content-type", "application/json"),
                ],
                Body::from(body.to_string()),
            )
            .await
    }

    /// Authenticated GET.
    pub async fn get(&self, path: &str) -> TestResponse {
        self.server.get_authed(path).await
    }

    /// A join token for the test pool (`body` as for `POST …/enrollment-tokens`).
    pub async fn token(&self, body: Value) -> String {
        let r = self
            .post(
                &format!("/api/v1/pools/{}/enrollment-tokens", self.pool),
                body,
            )
            .await;
        assert_eq!(r.status, 201, "token: {}", r.text());
        r.json()["token"].as_str().unwrap_or_default().to_owned()
    }

    /// `GET /nodes/{node}` as JSON.
    pub async fn node(&self, node: &str) -> Value {
        let r = self.get(&format!("/api/v1/nodes/{node}")).await;
        assert_eq!(r.status, 200, "node: {}", r.text());
        r.json()
    }

    /// Waits (≤ 20 s) until the node has `status`.
    pub async fn wait_status(&self, node: &str, status: &str) -> Value {
        self.wait(node, |n| n["status"] == status, &format!("status {status}"))
            .await
    }

    /// Waits (≤ 20 s) until `ok(GET /nodes/{node})`.
    pub async fn wait(&self, node: &str, ok: impl Fn(&Value) -> bool, what: &str) -> Value {
        let mut last = Value::Null;
        for _ in 0..200 {
            last = self.node(node).await;
            if ok(&last) {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("node {node} never reached {what}: {last}");
    }
}

/// An agent with fake GPUs and its own state folder.
pub struct TestAgent {
    /// Its context.
    pub ctx: Arc<AgentContext>,
    /// Its fake GPUs.
    pub hw: Arc<FakeHw>,
    _dir: tempfile::TempDir,
}

impl TestAgent {
    /// An agent with `gpus` fake GPUs; `tune` edits its settings (like `gateway.listen`).
    pub fn new(gpus: u32, tune: impl FnOnce(&mut AgentConfig)) -> Self {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir: {e}"));
        let mut config = AgentConfig {
            state_dir: dir.path().join("state"),
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
        config.gateway.telemetry_interval_secs = 1;
        tune(&mut config);
        let hw = Arc::new(FakeHw::with_gpus(
            gpus,
            super::FAKE_GPU_MODEL,
            super::FAKE_GPU_MEMORY,
        ));
        let backends: Vec<Arc<dyn HwBackend>> = vec![hw.clone()];
        let ctx = Arc::new(
            AgentContext::with_hw(config, backends)
                .unwrap_or_else(|e| panic!("agent: {}", e.explain())),
        );
        Self { ctx, hw, _dir: dir }
    }

    /// Enrolls by dialing the harness; returns the `node_…` id.
    pub async fn enroll(&self, gw: &GatewayHarness, token: &str) -> Result<String, LinkError> {
        let server = gw.addr().to_string();
        mrl_agent::enroll::enroll_dial(
            &self.ctx,
            &server,
            token,
            mrl_agent::transports::TransportKind::Direct,
            false,
        )
        .await
        .map(|id| id.link.node_id)
    }

    /// Runs the gateway service until [`Running::stop`].
    pub fn run(&self) -> Running {
        let stop = CancellationToken::new();
        let (ctx, s) = (self.ctx.clone(), stop.clone());
        let task = tokio::spawn(async move { GatewayService::new().run(ctx, s).await });
        Running { stop, task }
    }
}

/// A running agent link.
pub struct Running {
    stop: CancellationToken,
    task: tokio::task::JoinHandle<mrl_agent::Result<()>>,
}

impl Running {
    /// Stops the link and waits for it.
    pub async fn stop(self) {
        self.stop.cancel();
        match tokio::time::timeout(Duration::from_secs(10), self.task).await {
            Ok(Ok(Ok(()))) => {}
            other => panic!("agent link did not stop cleanly: {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_agent::session::{CommandHandler, Link};
    use mrl_domain::ids::NodeId;
    use mrl_proto::agent::v1::{
        agent_message, command::Kind, AttemptStatus, Command, Drain, Probe,
    };
    use mrl_proto::common::v1::Failure;
    use mrl_server::modules::nodes::commands::{CommandKind, CommandSender, NewCommand};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Count(AtomicUsize);

    #[async_trait::async_trait]
    impl CommandHandler for Count {
        async fn handle(&self, _: Command) -> Result<(), Failure> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn probe() -> NewCommand {
        NewCommand::new(Kind::Probe(Probe {
            request_id: "r1".into(),
            ..Probe::default()
        }))
    }

    async fn state_of(gw: &GatewayHarness, id: uuid::Uuid) -> String {
        sqlx::query_scalar("SELECT state FROM agent_commands WHERE id = $1")
            .bind(id)
            .fetch_one(gw.db.pool())
            .await
            .unwrap()
    }

    async fn wait_state(gw: &GatewayHarness, id: uuid::Uuid, want: &str) {
        for _ in 0..200 {
            if state_of(gw, id).await == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("command never reached {want}: {}", state_of(gw, id).await);
    }

    fn uuid_of(node: &str) -> uuid::Uuid {
        NodeId::from_str(node).unwrap().as_uuid()
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn enrollment_is_single_use_and_rejects_bad_tokens() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let token = gw.token(json!({ "labels": { "rack": "r1" } })).await;
        let a = TestAgent::new(2, |_| {});
        let node = a.enroll(&gw, &token).await.unwrap();
        let n = gw.node(&node).await;
        assert_eq!(n["status"], "ENROLLING");
        assert_eq!(n["labels"]["rack"], "r1");

        let b = TestAgent::new(1, |_| {});
        let again = b.enroll(&gw, &token).await.unwrap_err();
        assert_eq!(again.code(), "MRL-NODE-180");
        assert!(again.to_string().contains("already used"), "{again}");

        let expired = gw.token(json!({ "ttl_seconds": 60 })).await;
        sqlx::query("UPDATE node_enrollment_tokens SET expires_at = now() - interval '1 second' WHERE used_at IS NULL")
            .execute(gw.db.pool())
            .await
            .unwrap();
        let e = b.enroll(&gw, &expired).await.unwrap_err();
        assert!(e.to_string().contains("expired"), "{e}");

        // The command shown to the user names the port the gateway really listens on.
        let r = gw
            .post(
                &format!("/api/v1/pools/{}/enrollment-tokens", gw.pool),
                json!({}),
            )
            .await;
        let cmd = r.json()["enroll_command"].as_str().unwrap().to_owned();
        assert!(cmd.contains(&format!(":{} ", gw.addr().port())), "{cmd}");
        assert_ne!(gw.addr().port(), 0);

        let pin = gw.gw.ca_fingerprint().unwrap();
        let unknown = format!("mrlj_{}_{pin}", "0".repeat(64));
        assert!(b
            .enroll(&gw, &unknown)
            .await
            .unwrap_err()
            .to_string()
            .contains("not known"));

        // A token whose CA pin is not this server's is never sent.
        let wrong_pin = format!("mrlj_{}_{}", "0".repeat(64), "f".repeat(64));
        let e = b.enroll(&gw, &wrong_pin).await.unwrap_err();
        assert_eq!(e.code(), "MRL-NODE-181", "{e}");
        assert_eq!(gw.db.count("compute_nodes").await, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn node_shows_fake_gpus_and_live_telemetry_then_goes_offline() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(2, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let link = a.run();
        let n = gw
            .wait(
                &node,
                |n| n["status"] == "ONLINE" && n["gpus"][1]["observed_at"].is_string(),
                "online with telemetry",
            )
            .await;
        assert_eq!(n["gpus"].as_array().unwrap().len(), 2);
        assert_eq!(n["gpus"][0]["model"], super::super::FAKE_GPU_MODEL);
        assert_eq!(n["gpus"][0]["arch"], "sm_80");
        assert!(n["telemetry"]["ram_used_bytes"].as_u64().unwrap() > 0);
        assert_eq!(n["transport"], "direct");
        let epoch = n["session_epoch"].as_i64().unwrap();

        link.stop().await;
        gw.wait_status(&node, "DEGRADED").await;
        gw.wait_status(&node, "OFFLINE").await;
        let link = a.run();
        let n = gw.wait_status(&node, "ONLINE").await;
        assert!(n["session_epoch"].as_i64().unwrap() > epoch);
        let kinds: Vec<String> =
            sqlx::query_scalar("SELECT kind FROM node_events WHERE node_id = $1")
                .bind(uuid_of(&node))
                .fetch_all(gw.db.pool())
                .await
                .unwrap();
        assert!(kinds.iter().any(|k| k == "offline"), "{kinds:?}");
        let events: Vec<String> = sqlx::query_scalar(
            "SELECT type FROM outbox_events WHERE aggregate_id = $1 ORDER BY id",
        )
        .bind(uuid_of(&node))
        .fetch_all(gw.db.pool())
        .await
        .unwrap();
        for e in [
            "node.registered",
            "node.enrolled",
            "node.degraded",
            "node.disconnected",
            "node.reconnected",
        ] {
            assert!(events.iter().any(|x| x == e), "{e} missing from {events:?}");
        }
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn commands_survive_server_restart_and_reconnect_without_double_apply() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let count = Arc::new(Count(AtomicUsize::new(0)));
        Link::of(&a.ctx).unwrap().register("Probe", count.clone());
        let node_id = NodeId::from_str(&node).unwrap();

        // Written while the agent is away: delivered once it connects.
        let sender = CommandSender::new(&gw.gw, &[CommandKind::Probe]);
        let first = sender.send(node_id, probe()).await.unwrap();
        assert_eq!(state_of(&gw, first.id).await, "PENDING");
        let link = a.run();
        wait_state(&gw, first.id, "COMPLETED").await;
        link.stop().await;

        // Server restart with a command pending; the agent reconnects to the new server.
        let gw = gw.restart().await;
        let sender = CommandSender::new(&gw.gw, &[CommandKind::Probe]);
        let second = sender.send(node_id, probe()).await.unwrap();
        assert_eq!(second.seq, first.seq + 1);
        let link = a.run();
        wait_state(&gw, second.id, "COMPLETED").await;

        // A lost ack makes the server resend: the journal stops a second run.
        sqlx::query("UPDATE agent_commands SET state = 'SENT' WHERE id = $1")
            .bind(second.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        link.stop().await;
        let link = a.run();
        wait_state(&gw, second.id, "RECEIVED").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            2,
            "each command ran exactly once"
        );
        assert_eq!(
            Link::of(&a.ctx).unwrap().spooled(),
            0,
            "every result was acked"
        );
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn commands_without_a_handler_fail_and_senders_refuse_foreign_kinds() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node =
            NodeId::from_str(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap()).unwrap();
        let probes = CommandSender::new(&gw.gw, &[CommandKind::Probe]);
        let e = probes
            .send(node, NewCommand::new(Kind::Drain(Drain::default())))
            .await
            .unwrap_err();
        assert_eq!(e.code(), "MRL-NODE-187");
        assert_eq!(
            gw.db.count("agent_commands").await,
            0,
            "nothing written for a refused kind"
        );

        let drains = CommandSender::new(&gw.gw, &[CommandKind::Drain]);
        let c = drains
            .send(node, NewCommand::new(Kind::Drain(Drain::default())))
            .await
            .unwrap();
        let link = a.run();
        wait_state(&gw, c.id, "FAILED").await;
        let r = drains.status(c.id).await.unwrap().unwrap();
        assert_eq!(
            r.result.unwrap()["failure"]["sub_code"],
            "unsupported_command"
        );

        // Superseded and expired commands are never sent.
        let old = probes.send(node, probe().superseding()).await.unwrap();
        let new = probes.send(node, probe().superseding()).await.unwrap();
        assert!(matches!(
            state_of(&gw, old.id).await.as_str(),
            "SUPERSEDED" | "SENT" | "RECEIVED" | "COMPLETED"
        ));
        wait_state(&gw, new.id, "FAILED").await; // no Probe handler on this agent
        link.stop().await;
        let late = probes
            .send(node, probe().deadline(Duration::from_secs(1)))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(state_of(&gw, late.id).await, "EXPIRED");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn revoked_certificates_are_refused_at_handshake_and_mid_stream() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let link = a.run();
        gw.wait_status(&node, "ONLINE").await;
        let epoch: i64 = gw.node(&node).await["session_epoch"].as_i64().unwrap();

        // Revoked by "another replica" (SQL only): the open stream notices within a second or two.
        sqlx::query("UPDATE node_certificates SET revoked_at = now(), revoked_reason = 'operator' WHERE node_id = $1")
            .bind(uuid_of(&node))
            .execute(gw.db.pool())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(4)).await;
        let holder: Option<String> =
            sqlx::query_scalar("SELECT session_holder FROM compute_nodes WHERE id = $1")
                .bind(uuid_of(&node))
                .fetch_one(gw.db.pool())
                .await
                .unwrap();
        assert!(holder.is_none(), "the revoked stream was closed");
        link.stop().await;

        // And a new connection is refused in the TLS handshake itself (certificate_revoked).
        let id = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let cfg = mrl_agent::session::tls::client(&id.ca, id.chain(), id.key()).unwrap();
            let tcp = tokio::net::TcpStream::connect(gw.addr()).await.unwrap();
            let name = tokio_rustls::rustls::pki_types::ServerName::try_from(
                mrl_agent::session::tls::CORE_NAME,
            )
            .unwrap();
            let refused = match tokio_rustls::TlsConnector::from(Arc::new(cfg))
                .connect(name, tcp)
                .await
            {
                Err(e) => e.to_string(),
                Ok(mut tls) => {
                    let _ = tls.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await;
                    let mut buf = [0u8; 64];
                    match tls.read(&mut buf).await {
                        Err(e) => e.to_string(),
                        Ok(n) => format!("read {n} bytes"),
                    }
                }
            };
            assert!(
                refused.contains("CertificateRevoked"),
                "TLS must refuse a revoked certificate: {refused}"
            );
        }
        let t = mrl_agent::transports::Direct::new(&gw.addr().to_string());
        let link = Link::of(&a.ctx).unwrap();
        let e =
            mrl_agent::session::connect_once(a.ctx.clone(), link, id, &t, CancellationToken::new())
                .await
                .unwrap_err();
        // The TLS refusal is proven above; the agent reports it as the certificate problem, or —
        // when the connection drops before the alert is read — as an unreachable core.
        assert!(
            matches!(e.code().as_str(), "MRL-NODE-181" | "MRL-LINK-184"),
            "{e}"
        );
        let now: i64 = gw.node(&node).await["session_epoch"].as_i64().unwrap();
        assert_eq!(now, epoch, "no new session was opened");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn decommission_revokes_and_closes_at_once_and_rotation_replaces_the_cert() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = a
            .enroll(&gw, &gw.token(json!({ "lifecycle": "EPHEMERAL" })).await)
            .await
            .unwrap();

        // Rotation: make the certificate due, connect, and the agent renews over the stream.
        let mut id = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        let old = id.cert_sha256();
        id.link.not_after = id.link.issued_at + 3; // due for renewal right away
        id.save(a.ctx.state.root()).unwrap();
        let link = a.run();
        gw.wait_status(&node, "ONLINE").await;
        let mut renewed = false;
        for _ in 0..100 {
            let now = mrl_agent::enroll::Identity::load(a.ctx.state.root())
                .unwrap()
                .unwrap();
            if now.cert_sha256() != old {
                renewed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(renewed, "the agent saved a renewed certificate");
        // The old certificate is retired once the new one has authenticated.
        let mut reason = None;
        for _ in 0..100 {
            reason = sqlx::query_scalar::<_, Option<String>>(
                "SELECT revoked_reason FROM node_certificates WHERE sha256 = $1",
            )
            .bind(&old)
            .fetch_one(gw.db.pool())
            .await
            .unwrap();
            if reason.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(reason.as_deref(), Some("rotated"));
        let n = gw.wait_status(&node, "ONLINE").await;
        assert_eq!(n["lifecycle"], "EPHEMERAL");

        // Decommission: drain first, then remove; open streams close at once.
        let r = gw
            .post(&format!("/api/v1/nodes/{node}:decommission"), json!({}))
            .await;
        assert_eq!(r.status, 422, "{}", r.text());
        assert_eq!(
            gw.post(&format!("/api/v1/nodes/{node}:drain"), json!({}))
                .await
                .json()["status"],
            "DRAINING"
        );
        let r = gw
            .post(&format!("/api/v1/nodes/{node}:decommission"), json!({}))
            .await;
        assert_eq!(r.json()["status"], "DECOMMISSIONED", "{}", r.text());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM node_certificates WHERE node_id = $1 AND revoked_at IS NULL",
        )
        .bind(uuid_of(&node))
        .fetch_one(gw.db.pool())
        .await
        .unwrap();
        assert_eq!(open, 0);
        assert!(!gw.gw.ca_fingerprint().unwrap().is_empty());
        link.stop().await;
        let r = gw
            .post(&format!("/api/v1/nodes/{node}:frobnicate"), json!({}))
            .await;
        assert_eq!(r.status, 404);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn control_messages_without_a_sink_are_journaled() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let l = Link::of(&a.ctx).unwrap();
        // Spooled while offline, replayed after connecting.
        l.send_control(agent_message::Body::AttemptStatus(AttemptStatus::default()))
            .unwrap();
        let link = a.run();
        for _ in 0..100 {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM node_events WHERE node_id = $1 AND kind = 'control_unrouted'",
            )
            .bind(uuid_of(&node))
            .fetch_one(gw.db.pool())
            .await
            .unwrap();
            if n == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let details: Value = sqlx::query_scalar(
            "SELECT details FROM node_events WHERE node_id = $1 AND kind = 'control_unrouted'",
        )
        .bind(uuid_of(&node))
        .fetch_one(gw.db.pool())
        .await
        .unwrap();
        assert_eq!(details["body"], "attempt_status");
        for _ in 0..50 {
            if l.spooled() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(l.spooled(), 0, "acked messages leave the spool");
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn the_core_can_dial_an_agent_that_listens() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let a = TestAgent::new(1, |c| c.gateway.listen = Some(listen));
        let token = gw
            .token(json!({ "endpoint": { "kind": "direct", "address": listen.to_string() } }))
            .await;
        let id = mrl_agent::enroll::enroll_listen(
            &a.ctx,
            listen,
            &token,
            mrl_agent::transports::TransportKind::Direct,
            false,
            CancellationToken::new(),
        );
        let id = tokio::time::timeout(Duration::from_secs(30), id)
            .await
            .unwrap()
            .unwrap();
        let node = id.link.node_id.clone();
        let link = a.run();
        let n = gw.wait_status(&node, "ONLINE").await;
        assert_eq!(n["transport"], "direct");
        gw.wait(
            &node,
            |n| n["gpus"][0]["observed_at"].is_string(),
            "telemetry",
        )
        .await;
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn renewal_rechecks_the_presenting_cert_and_a_lost_reply_strands_nobody() {
        use mrl_server::modules::nodes::store::{self, Lifecycle};
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = uuid_of(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap());
        let id = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        // Revoked by another replica, then a renewal arrives before any periodic check.
        sqlx::query("UPDATE node_certificates SET revoked_at = now(), revoked_reason = 'operator' WHERE node_id = $1")
            .bind(node)
            .execute(gw.db.pool())
            .await
            .unwrap();
        let (_, csr) = mrl_agent::enroll::new_key().unwrap();
        let pki = gw.gw.pki().unwrap();
        let e = store::renew(
            gw.db.db(),
            pki.clone(),
            gw.gw.config(),
            node,
            Lifecycle::Static,
            &id.cert_sha256(),
            &csr,
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), "MRL-NODE-181", "{e}");
        let certs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM node_certificates WHERE node_id = $1")
                .bind(node)
                .fetch_one(gw.db.pool())
                .await
                .unwrap();
        assert_eq!(certs, 1, "no certificate was issued");

        // A renewal whose reply never reaches the agent: the old certificate still works.
        let b = TestAgent::new(1, |_| {});
        let b_node = b.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let b_id = mrl_agent::enroll::Identity::load(b.ctx.state.root())
            .unwrap()
            .unwrap();
        let lost = store::renew(
            gw.db.db(),
            pki,
            gw.gw.config(),
            uuid_of(&b_node),
            Lifecycle::Static,
            &b_id.cert_sha256(),
            &csr,
        )
        .await
        .unwrap();
        let link = b.run();
        gw.wait_status(&b_node, "ONLINE").await;
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM node_certificates WHERE node_id = $1 AND revoked_at IS NULL",
        )
        .bind(uuid_of(&b_node))
        .fetch_one(gw.db.pool())
        .await
        .unwrap();
        assert_eq!(
            open, 2,
            "the unused new certificate does not retire the one in use"
        );
        assert_ne!(lost.sha256, b_id.cert_sha256());
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_sent_command_the_agent_never_got_is_sent_again() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node =
            NodeId::from_str(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap()).unwrap();
        let count = Arc::new(Count(AtomicUsize::new(0)));
        Link::of(&a.ctx).unwrap().register("Probe", count.clone());
        let c = CommandSender::new(&gw.gw, &[CommandKind::Probe])
            .send(node, probe())
            .await
            .unwrap();
        // SENT committed, then the server stopped before the bytes left.
        sqlx::query("UPDATE agent_commands SET state = 'SENT', sent_at = now() WHERE id = $1")
            .bind(c.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        let link = a.run();
        wait_state(&gw, c.id, "COMPLETED").await;
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn replicas_share_one_ca_and_a_replica_with_another_store_refuses_to_start() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let (replica, replica_gw) = gw.replica().await;
        assert_eq!(replica_gw.ca_fingerprint(), gw.gw.ca_fingerprint());
        // Enrolled through replica A, connected through replica B.
        let b_addr = replica_gw.addr().unwrap().to_string();
        let a = TestAgent::new(1, |c| c.gateway.server = Some(b_addr.clone()));
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let link = a.run();
        gw.wait_status(&node, "ONLINE").await;
        link.stop().await;
        replica.shutdown().await.unwrap();

        // Another secret store (another machine's local files): refused, no second CA.
        let cfg = mrl_server::config::Config {
            gateway: fast_config(),
            ..mrl_server::config::Config::default()
        };
        let other = NodesModule::new(
            gw.db.db().clone(),
            &cfg,
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
        );
        let seams = crate::seams::build(crate::seams::solo(TEST_SECRET), Mode::Solo);
        let e = match TestServer::start(cfg, seams, vec![Box::new(other)]).await {
            Ok(_) => panic!("a replica without the CA key must not start"),
            Err(e) => e,
        };
        assert!(
            e.detail.contains("MRL-NODE-183")
                && e.detail.contains("not in this server's secret store"),
            "{}",
            e.detail
        );
        let cas: i64 = sqlx::query_scalar("SELECT count(*) FROM gateway_ca")
            .fetch_one(gw.db.pool())
            .await
            .unwrap();
        assert_eq!(cas, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn the_gateway_and_shared_grpc_services_start_together_in_team_mode() {
        let db = TestDb::new().await;
        let grpc_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut cfg = mrl_server::config::Config::default();
        cfg.server.mode = Mode::Team;
        cfg.server.listeners.api = "127.0.0.1:0".parse().unwrap();
        cfg.server.listeners.internal = "127.0.0.1:0".parse().unwrap();
        cfg.server.listeners.grpc = format!("127.0.0.1:{grpc_port}").parse().unwrap();
        cfg.gateway = GatewayConfig {
            listen: Some("127.0.0.1:0".parse().unwrap()),
            ..fast_config()
        };
        cfg.validate().unwrap();
        let nodes = NodesModule::new(
            db.db().clone(),
            &cfg,
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
        );
        let gw = nodes.gateway();
        let other = crate::server::TestModule {
            grpc: true,
            ..Default::default()
        };
        let seams = crate::seams::build(crate::seams::team(&[]), Mode::Team);
        let server =
            match TestServer::start(cfg, seams, vec![Box::new(nodes), Box::new(other)]).await {
                Ok(s) => s,
                Err(e) => panic!("both must start: {}", e.explain()),
            };
        assert_eq!(
            server.running().grpc_addr().map(|a| a.port()),
            Some(grpc_port)
        );
        assert_ne!(gw.addr().unwrap().port(), grpc_port);
        server.shutdown().await.unwrap();
    }

    /// Records `(stream, seq)` of every metrics item.
    struct Seen(std::sync::Mutex<Vec<(i32, u64)>>);

    #[async_trait::async_trait]
    impl mrl_server::modules::nodes::lanes::LaneSink for Seen {
        fn name(&self) -> &'static str {
            "seen"
        }
        fn lanes(&self) -> &'static [mrl_server::modules::nodes::lanes::LaneKind] {
            &[mrl_server::modules::nodes::lanes::LaneKind::Metrics]
        }
        async fn deliver(
            &self,
            item: mrl_server::modules::nodes::lanes::LaneItem,
        ) -> Result<(), mrl_server::modules::nodes::lanes::SinkError> {
            assert!(item.session_epoch > 0, "items carry the real session epoch");
            self.0.lock().unwrap().push((item.stream as i32, item.seq));
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn data_items_keep_unique_ids_across_reconnects() {
        let router = LaneRouter::new();
        let seen = Arc::new(Seen(std::sync::Mutex::new(Vec::new())));
        router.register(seen.clone());
        let gw = GatewayHarness::start_with(
            TestDb::new().await,
            fast_config(),
            Arc::new(InMemorySecretStore::default()),
            router,
        )
        .await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let link = Link::of(&a.ctx).unwrap();
        let relay = || mrl_proto::agent::v1::WorkerRelay {
            message: Some(mrl_proto::worker::v1::WorkerMessage {
                body: Some(mrl_proto::worker::v1::worker_message::Body::Metrics(
                    Default::default(),
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        let wait = |n: usize| {
            let seen = seen.clone();
            async move {
                for _ in 0..100 {
                    if seen.0.lock().unwrap().len() >= n {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                panic!("only {:?} arrived", seen.0.lock().unwrap());
            }
        };
        let run = a.run();
        gw.wait_status(&node, "ONLINE").await;
        for _ in 0..3 {
            link.send_metrics(relay());
        }
        wait(3).await;
        run.stop().await;
        let run = a.run();
        gw.wait_status(&node, "ONLINE").await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        for _ in 0..3 {
            link.send_metrics(relay());
        }
        wait(6).await;
        run.stop().await;
        let got = seen.0.lock().unwrap().clone();
        let unique: std::collections::HashSet<_> = got.iter().collect();
        assert_eq!(
            unique.len(),
            got.len(),
            "a deduplicating sink keeps every item: {got:?}"
        );
    }
}
