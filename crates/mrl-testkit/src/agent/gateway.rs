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

/// Settings fast enough for tests: 1 s heartbeats, checks and revocation re-checks; no free-disk floor.
pub fn fast_config() -> GatewayConfig {
    GatewayConfig {
        heartbeat_interval_seconds: 1,
        degraded_after_seconds: 2,
        offline_after_seconds: 3,
        health_check_seconds: 1,
        revocation_check_seconds: 1,
        // CI hosts often have < 5 GiB free; `disk_low` would mark every test node DEGRADED.
        min_disk_free_bytes: 0,
        ..GatewayConfig::default()
    }
}

/// Bearer token of the team-mode harness user.
const TEAM_TOKEN: &str = "tok-gateway";

/// Settings for a test server with `gateway`, every listener on a free loopback port (`grpc`
/// on `grpc_port`, 0 = any), in `mode`.
pub fn server_config(gateway: GatewayConfig, mode: Mode, grpc_port: u16) -> Config {
    let mut cfg = Config {
        gateway,
        ..Config::default()
    };
    cfg.server.mode = mode;
    cfg.server.listeners.api = SocketAddr::from(([127, 0, 0, 1], 0));
    cfg.server.listeners.internal = SocketAddr::from(([127, 0, 0, 1], 0));
    cfg.server.listeners.grpc = SocketAddr::from(([127, 0, 0, 1], grpc_port));
    cfg
}

/// A server with the `nodes` module and another gRPC service on the same listener, on its own
/// test database. Agents dial the shared gRPC listener.
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
    mode: Mode,
}

impl GatewayHarness {
    /// Starts in `mode` with [`fast_config`].
    pub async fn start_in(db: TestDb, mode: Mode) -> Self {
        Self::open(
            db,
            fast_config(),
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
            mode,
        )
        .await
    }

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
        Self::open(db, config, secrets, lanes, Mode::Solo).await
    }

    async fn open(
        db: TestDb,
        config: GatewayConfig,
        secrets: Arc<dyn SecretStore>,
        lanes: Arc<LaneRouter>,
        mode: Mode,
    ) -> Self {
        let (server, gw) = Self::boot(&db, &config, 0, mode, secrets.clone(), lanes.clone()).await;
        let mut h = Self {
            server,
            gw,
            db,
            pool: String::new(),
            secrets,
            lanes,
            config,
            mode,
        };
        let r = h.post("/api/v1/pools", json!({ "name": "test" })).await;
        assert_eq!(r.status, 201, "create pool: {}", r.text());
        h.pool = r.json()["id"].as_str().unwrap_or_default().to_owned();
        h
    }

    async fn boot(
        db: &TestDb,
        config: &GatewayConfig,
        grpc_port: u16,
        mode: Mode,
        secrets: Arc<dyn SecretStore>,
        lanes: Arc<LaneRouter>,
    ) -> (TestServer, Arc<Gateway>) {
        let cfg = server_config(config.clone(), mode, grpc_port);
        let module = NodesModule::new(db.db().clone(), &cfg, secrets, lanes);
        let gw = module.gateway();
        let seams = match mode {
            Mode::Solo => crate::seams::build(crate::seams::solo(TEST_SECRET), Mode::Solo),
            Mode::Team => crate::seams::build(
                crate::seams::team(&[(
                    TEAM_TOKEN,
                    crate::seams::user("u-gateway", &mrl_domain::ids::OrgId::new().to_string()),
                )]),
                Mode::Team,
            ),
        };
        // Another gRPC service shares the listener (`mrl.testkit.Ping`).
        let modules: Vec<Box<dyn mrl_server::app::ServerModule>> = vec![
            Box::new(module),
            Box::new(crate::server::TestModule {
                grpc: true,
                ..Default::default()
            }),
        ];
        match TestServer::start(cfg, seams, modules).await {
            Ok(s) => (s, gw),
            Err(e) => panic!("gateway test server did not start: {}", e.explain()),
        }
    }

    /// A second server (replica) on the same database and secret store, with its own gRPC port.
    pub async fn replica(&self) -> (TestServer, Arc<Gateway>) {
        Self::boot(
            &self.db,
            &self.config,
            0,
            self.mode,
            self.secrets.clone(),
            self.lanes.clone(),
        )
        .await
    }

    /// Stops the server and starts a new one on the same database, CA and gRPC port.
    pub async fn restart(self) -> Self {
        let addr = self.addr();
        if let Err(e) = self.server.shutdown().await {
            panic!("shutdown: {}", e.explain());
        }
        let (server, gw) = Self::boot(
            &self.db,
            &self.config,
            addr.port(),
            self.mode,
            self.secrets.clone(),
            self.lanes.clone(),
        )
        .await;
        Self { server, gw, ..self }
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
                &[self.auth(), ("content-type", "application/json")],
                Body::from(body.to_string()),
            )
            .await
    }

    /// Authenticated GET.
    pub async fn get(&self, path: &str) -> TestResponse {
        self.server
            .request(Method::GET, path, &[self.auth()], Body::empty())
            .await
    }

    /// The sign-in header for this mode.
    fn auth(&self) -> (&'static str, &'static str) {
        match self.mode {
            Mode::Solo => (LAUNCH_SECRET_HEADER, TEST_SECRET),
            Mode::Team => ("authorization", "Bearer tok-gateway"),
        }
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

    /// Seeds a RUNNING job with `attempt` as its current attempt at `epoch`: the gateway only
    /// renews leases of a live, current attempt (INT-3).
    pub async fn track_attempt(&self, attempt: mrl_domain::ids::AttemptId, epoch: u64) {
        let job = uuid::Uuid::now_v7();
        self.db
            .seed(&format!(
                "INSERT INTO jobs (id, org_id, project_id, kind, state, epoch, resource_request, retry_policy, created_by)
                 VALUES ('{job}', '{job}', '{job}', 'DATA_PREP', 'RUNNING', {epoch}, '{{}}', '{{}}', 'test');
                 INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state)
                 VALUES ('{a}', '{job}', '{job}', 1, {epoch}, 'RUNNING');
                 UPDATE jobs SET current_attempt_id = '{a}' WHERE id = '{job}';",
                a = attempt.as_uuid(),
            ))
            .await;
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
        config.gateway.key_store = mrl_agent::enroll::keys::KeyStoreChoice::File;
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

        // (A lost ack is tested for real, through a proxy: see a_lost_ack_neither_reruns_….)
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            2,
            "each command ran exactly once"
        );
        // The ack follows the COMPLETED write; give it a moment to arrive.
        let link_state = Link::of(&a.ctx).unwrap();
        for _ in 0..100 {
            if link_state.spooled() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(link_state.spooled(), 0, "every result was acked");
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn an_identity_from_before_the_key_store_moves_its_key_and_reconnects() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let root = a.ctx.state.root();
        let id = mrl_agent::enroll::Identity::load(root).unwrap().unwrap();
        // Rewrite it as the older format did: the hex key inside identity.json, no key store.
        let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        let dir = mrl_agent::enroll::identity_dir(root);
        let legacy = json!({
            "link": id.link, "cert": hex(&id.cert), "ca": hex(&id.ca),
            "key": hex(id.key().secret_der()),
        });
        std::fs::write(
            dir.join(mrl_agent::enroll::IDENTITY_FILE),
            legacy.to_string(),
        )
        .unwrap();
        std::fs::remove_dir_all(dir.join("keys")).unwrap();

        let link = a.run();
        gw.wait_status(&node, "ONLINE").await;
        link.stop().await;
        let back = mrl_agent::enroll::Identity::load(root).unwrap().unwrap();
        assert_eq!(back.link.node_id, node, "same node");
        assert_eq!(back.cert_sha256(), id.cert_sha256(), "same certificate");
        let json = std::fs::read_to_string(dir.join(mrl_agent::enroll::IDENTITY_FILE)).unwrap();
        assert!(
            !json.contains(&hex(id.key().secret_der())),
            "the key moved to the store"
        );
        assert!(dir.join("keys").exists());
    }

    /// INT-2: one live command per (attempt, epoch, kind); a rolled-back transaction writes
    /// none; a withdrawal supersedes unsent ones and drops their payload.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn commands_are_written_in_the_callers_transaction_once_per_identity() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node =
            NodeId::from_str(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap()).unwrap();
        let attempt = mrl_domain::ids::AttemptId::new();
        let s = CommandSender::new(
            &gw.gw,
            &[CommandKind::LaunchAttempt, CommandKind::UpdateCredentials],
        );
        let launch = || {
            NewCommand::new(Kind::LaunchAttempt(
                mrl_proto::agent::v1::LaunchAttempt::default(),
            ))
            .attempt(attempt, 1)
        };
        let first = s.send(node, launch()).await.unwrap();
        let again = s.send(node, launch()).await.unwrap();
        assert!(first.new && !again.new);
        assert_eq!(again.id, first.id, "the live launch is the same command");
        let pool = gw.db.pool().clone();
        let rows = |kind: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (String, Vec<u8>)>(
                    "SELECT state, payload FROM agent_commands WHERE attempt_id = $1 AND kind = $2",
                )
                .bind(attempt.as_uuid())
                .bind(kind)
                .fetch_all(&pool)
                .await
                .unwrap()
            }
        };
        assert_eq!(rows("LaunchAttempt").await.len(), 1);

        let creds =
            NewCommand::new(Kind::UpdateCredentials(Default::default())).attempt(attempt, 1);
        let mut tx = gw.db.db().begin().await.unwrap();
        s.send_in(&mut tx, node, creds.clone()).await.unwrap();
        tx.rollback().await.unwrap();
        assert!(rows("UpdateCredentials").await.is_empty(), "rolled back");

        let mut tx = gw.db.db().begin().await.unwrap();
        s.send_in(&mut tx, node, creds).await.unwrap();
        let n = CommandSender::withdraw_in(
            &mut tx,
            attempt.as_uuid(),
            1,
            &[CommandKind::UpdateCredentials],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            rows("UpdateCredentials").await,
            vec![("SUPERSEDED".to_owned(), vec![])]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_relaunch_at_a_newer_epoch_gets_a_lease_after_the_old_one_was_cancelled() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node_s = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let node = NodeId::from_str(&node_s).unwrap();
        let attempt = mrl_domain::ids::AttemptId::new();
        let s = CommandSender::new(
            &gw.gw,
            &[CommandKind::LaunchAttempt, CommandKind::CancelAttempt],
        );
        let launch = |epoch| {
            NewCommand::new(Kind::LaunchAttempt(
                mrl_proto::agent::v1::LaunchAttempt::default(),
            ))
            .attempt(attempt, epoch)
        };
        let l1 = s.send(node, launch(1)).await.unwrap();
        let c1 = s
            .send(
                node,
                NewCommand::new(Kind::CancelAttempt(Default::default())).attempt(attempt, 1),
            )
            .await
            .unwrap();
        let l2 = s.send(node, launch(2)).await.unwrap();
        for (id, state) in [
            (l1.id, "COMPLETED"),
            (c1.id, "COMPLETED"),
            (l2.id, "RECEIVED"),
        ] {
            sqlx::query("UPDATE agent_commands SET state = $2 WHERE id = $1")
                .bind(id)
                .bind(state)
                .execute(gw.db.pool())
                .await
                .unwrap();
        }
        // The attempt runs at epoch 2 and is its job's current attempt.
        gw.track_attempt(attempt, 2).await;
        let running = |epoch| mrl_proto::common::v1::AttemptRef {
            attempt_id: attempt.to_string(),
            fencing_epoch: epoch,
            ..Default::default()
        };
        let granted = || async {
            mrl_server::modules::nodes::store::leases(
                gw.db.db(),
                uuid_of(&node_s),
                &[running(1), running(2)],
            )
            .await
            .unwrap()
            .iter()
            .filter_map(|l| l.attempt.as_ref().map(|a| a.fencing_epoch))
            .collect::<Vec<u64>>()
        };
        assert_eq!(
            granted().await,
            vec![2],
            "epoch 1 was cancelled, epoch 2 runs"
        );

        // INT-3: no renewal once the job's epoch moved on (a lost lease bumps it) …
        gw.db.seed("UPDATE jobs SET epoch = 3").await;
        assert_eq!(granted().await, Vec::<u64>::new(), "superseded epoch");
        // … or once the attempt is LOST.
        gw.db
            .seed("UPDATE jobs SET epoch = 2; UPDATE job_attempts SET state = 'LOST'")
            .await;
        assert_eq!(granted().await, Vec::<u64>::new(), "lost attempt");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn after_a_core_outage_nodes_get_the_offline_time_again_before_going_offline() {
        let mut cfg = fast_config();
        cfg.offline_after_seconds = 6;
        cfg.degraded_after_seconds = 5;
        cfg.outage_threshold_seconds = 2;
        let gw = GatewayHarness::start_with(
            TestDb::new().await,
            cfg,
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
        )
        .await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        // The agent is not running: its last heartbeat (enrollment) is old, and so is the
        // last health check, as after the core was down for a minute.
        gw.db
            .seed(&format!(
                "UPDATE compute_nodes SET status = 'ONLINE', last_heartbeat_at = now() - interval '1 minute'
                  WHERE id = '{}';
                 INSERT INTO node_health_ticks (id, last_check_at, resumed_at)
                   VALUES (1, now() - interval '1 minute', now() - interval '1 minute')
                 ON CONFLICT (id) DO UPDATE SET last_check_at = EXCLUDED.last_check_at,
                   resumed_at = EXCLUDED.resumed_at;",
                uuid_of(&node)
            ))
            .await;
        mrl_server::modules::nodes::health::check_once(&gw.gw)
            .await
            .unwrap();
        assert_eq!(
            gw.node(&node).await["status"],
            "ONLINE",
            "grace after the outage"
        );
        // Without heartbeats it still goes OFFLINE once the offline time has passed.
        gw.wait_status(&node, "OFFLINE").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn every_gateway_connection_is_a_full_handshake_so_revocation_is_always_checked() {
        use mrl_agent::transports::Transport;
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let id = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        // One client config (one session cache) for both connections: a resumable server would
        // resume the second one and skip the certificate check.
        let config = mrl_agent::session::tls::client(&id.ca, id.chain(), id.key()).unwrap();
        let mut kinds = Vec::new();
        for _ in 0..2 {
            let io = mrl_agent::transports::Direct::new(&gw.addr().to_string())
                .connect()
                .await
                .unwrap();
            let tls =
                mrl_agent::session::connect_tls(config.clone(), io, &CancellationToken::new())
                    .await
                    .unwrap()
                    .unwrap();
            let mut tls = tls;
            kinds.push(tls.get_ref().1.handshake_kind());
            // Reading takes in any session tickets the server sends after the handshake.
            let mut buf = [0u8; 64];
            let _ = tokio::time::timeout(
                Duration::from_millis(300),
                tokio::io::AsyncReadExt::read(&mut tls, &mut buf),
            )
            .await;
        }
        assert_eq!(
            kinds,
            vec![Some(tokio_rustls::rustls::HandshakeKind::Full); 2],
            "no resumption"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_machine_with_a_work_start_in_flight_is_not_removed() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node_s = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let node = NodeId::from_str(&node_s).unwrap();
        let launches = CommandSender::new(&gw.gw, &[CommandKind::LaunchAttempt]);
        let launch = launches
            .send(
                node,
                NewCommand::new(Kind::LaunchAttempt(
                    mrl_proto::agent::v1::LaunchAttempt::default(),
                )),
            )
            .await
            .unwrap();
        // The agent got it just before the drain; no heartbeat shows the attempt yet.
        sqlx::query("UPDATE agent_commands SET state = 'RECEIVED' WHERE id = $1")
            .bind(launch.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        let link = a.run();
        gw.wait_status(&node_s, "ONLINE").await;
        gw.post(&format!("/api/v1/nodes/{node_s}:drain"), json!({}))
            .await;
        tokio::time::sleep(Duration::from_secs(2)).await; // heartbeats with 0 running arrive
        let r = gw
            .post(&format!("/api/v1/nodes/{node_s}:decommission"), json!({}))
            .await;
        assert_eq!(r.status, 422, "{}", r.text());
        assert!(r.text().contains("work start"), "{}", r.text());
        // The launch finished (the attempt started); only a heartbeat after that counts.
        sqlx::query("UPDATE agent_commands SET state = 'COMPLETED', updated_at = now() + interval '1 hour' WHERE id = $1")
            .bind(launch.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let r = gw
            .post(&format!("/api/v1/nodes/{node_s}:decommission"), json!({}))
            .await;
        assert_eq!(
            r.status,
            422,
            "a heartbeat must follow the started work: {}",
            r.text()
        );
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn drain_waits_for_an_expired_start_handler_but_allows_never_connected_nodes() {
        use mrl_proto::agent::v1::{Command, LaunchAttempt};
        use mrl_proto::common::v1::Failure;
        #[derive(Default)]
        struct Starting {
            entered: tokio::sync::Notify,
            release: tokio::sync::Notify,
        }
        #[async_trait::async_trait]
        impl mrl_agent::session::CommandHandler for Starting {
            async fn handle(&self, _: Command) -> Result<(), Failure> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(())
            }
        }

        let gw = GatewayHarness::start(TestDb::new().await).await;
        let agent = TestAgent::new(1, |_| {});
        let node = agent.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let offline = TestAgent::new(1, |_| {});
        let unused = offline
            .enroll(&gw, &gw.token(json!({})).await)
            .await
            .unwrap();
        assert_eq!(
            gw.post(&format!("/api/v1/nodes/{unused}:drain"), json!({}))
                .await
                .status,
            200
        );
        assert_eq!(
            gw.post(&format!("/api/v1/nodes/{unused}:decommission"), json!({}))
                .await
                .status,
            200
        );

        let handler = Arc::new(Starting::default());
        let state = Link::of(&agent.ctx).unwrap();
        state.register("LaunchAttempt", handler.clone());
        let running = agent.run();
        gw.wait_status(&node, "ONLINE").await;
        let command = CommandSender::new(&gw.gw, &[CommandKind::LaunchAttempt])
            .send(
                NodeId::from_str(&node).unwrap(),
                NewCommand::new(Kind::LaunchAttempt(LaunchAttempt::default())),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), handler.entered.notified())
            .await
            .unwrap();
        sqlx::query("UPDATE agent_commands SET state = 'EXPIRED' WHERE id = $1")
            .bind(command.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        assert_eq!(
            gw.post(&format!("/api/v1/nodes/{node}:drain"), json!({}))
                .await
                .status,
            200
        );
        let decommission = format!("/api/v1/nodes/{node}:decommission");
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(gw.post(&decommission, json!({})).await.status, 422);
        handler.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let response = gw.post(&decommission, json!({})).await;
                if response.status == 200 {
                    break;
                }
                assert_eq!(response.status, 422, "{}", response.text());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        running.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_long_running_attempt_keeps_its_lease_across_retention() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node_s = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let node = NodeId::from_str(&node_s).unwrap();
        let attempt = mrl_domain::ids::AttemptId::new();
        gw.track_attempt(attempt, 1).await;
        let launch = CommandSender::new(&gw.gw, &[CommandKind::LaunchAttempt])
            .send(
                node,
                NewCommand::new(Kind::LaunchAttempt(
                    mrl_proto::agent::v1::LaunchAttempt::default(),
                ))
                .attempt(attempt, 1),
            )
            .await
            .unwrap();
        // Launched 31 days ago and still running.
        sqlx::query(
            "UPDATE agent_commands SET state = 'COMPLETED', updated_at = now() - interval '31 days' WHERE id = $1",
        )
        .bind(launch.id)
        .execute(gw.db.pool())
        .await
        .unwrap();
        let running = [mrl_proto::common::v1::AttemptRef {
            attempt_id: attempt.to_string(),
            fencing_epoch: 1,
            ..Default::default()
        }];
        let leases =
            || mrl_server::modules::nodes::store::leases(gw.db.db(), uuid_of(&node_s), &running);
        assert_eq!(leases().await.unwrap().len(), 1, "a heartbeat renews it");
        mrl_server::modules::nodes::health::retention(&gw.gw)
            .await
            .unwrap();
        assert_eq!(
            leases().await.unwrap().len(),
            1,
            "the launch a live attempt renews from is kept"
        );
        // A sweep already holding the launch row wins. An eligibility read outside the
        // update would still see the old version and grant an unrenewable lease.
        sqlx::query(
            "UPDATE agent_commands SET updated_at = now() - interval '31 days' WHERE id = $1",
        )
        .bind(launch.id)
        .execute(gw.db.pool())
        .await
        .unwrap();
        let mut sweep = gw.db.pool().begin().await.unwrap();
        sqlx::query("DELETE FROM agent_commands WHERE id = $1")
            .bind(launch.id)
            .execute(&mut *sweep)
            .await
            .unwrap();
        let grant = leases();
        tokio::pin!(grant);
        assert!(tokio::time::timeout(Duration::from_millis(200), &mut grant)
            .await
            .is_err());
        sweep.commit().await.unwrap();
        assert!(grant.await.unwrap().is_empty());
    }

    /// Hides pools outside the caller's organization, including platform pools.
    struct OrgScoped;

    #[async_trait::async_trait]
    impl mrl_server::seams::Authorizer for OrgScoped {
        async fn authorize(
            &self,
            ctx: &mrl_server::seams::RequestContext,
            _: &str,
            resource: &mrl_server::seams::Resource,
        ) -> Result<mrl_server::seams::Decision, mrl_server::seams::SeamError> {
            Ok(match &resource.org {
                Some(o) if Some(o) != ctx.org.as_ref() => mrl_server::seams::Decision::Hide,
                None if resource.id.is_some() => mrl_server::seams::Decision::Hide,
                _ => mrl_server::seams::Decision::Allow,
            })
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn private_pools_of_two_orgs_stay_apart() {
        let db = TestDb::new().await;
        let (org_a, org_b) = (
            mrl_domain::ids::OrgId::new().to_string(),
            mrl_domain::ids::OrgId::new().to_string(),
        );
        let cfg = server_config(fast_config(), Mode::Team, 0);
        let module = NodesModule::new(
            db.db().clone(),
            &cfg,
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
        );
        let gw = module.gateway();
        let seams = crate::seams::build(
            crate::seams::team(&[
                ("tok-a", crate::seams::user("u-a", &org_a)),
                ("tok-b", crate::seams::user("u-b", &org_b)),
            ])
            .authorizer(OrgScoped),
            Mode::Team,
        );
        let server = TestServer::start(cfg, seams, vec![Box::new(module)])
            .await
            .unwrap_or_else(|e| panic!("{}", e.explain()));
        let call = |who: &'static str, method: Method, path: String, body: Value| {
            let server = &server;
            async move {
                let auth = format!("Bearer {who}");
                server
                    .request(
                        method,
                        &path,
                        &[
                            ("authorization", auth.as_str()),
                            ("content-type", "application/json"),
                        ],
                        Body::from(body.to_string()),
                    )
                    .await
            }
        };
        let mut hidden = Vec::new();
        for name in ["platform A", "platform B", "platform C"] {
            hidden.push(
                mrl_server::modules::nodes::store::create_pool(
                    db.db(),
                    None,
                    name,
                    "agent",
                    90,
                    &[],
                )
                .await
                .unwrap()
                .unwrap()
                .id,
            );
        }
        let pa = call(
            "tok-a",
            Method::POST,
            "/api/v1/pools".into(),
            json!({"name": "gpus"}),
        )
        .await;
        assert_eq!(pa.status, 201, "{}", pa.text());
        let pa = pa.json()["id"].as_str().unwrap().to_owned();
        let pb = call(
            "tok-b",
            Method::POST,
            "/api/v1/pools".into(),
            json!({"name": "gpus"}),
        )
        .await;
        assert_eq!(pb.status, 201, "same name, other org: {}", pb.text());
        let pb = pb.json()["id"].as_str().unwrap().to_owned();
        let listed = call(
            "tok-b",
            Method::GET,
            "/api/v1/pools?limit=1".into(),
            json!({}),
        )
        .await
        .json();
        let ids: Vec<&str> = listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["id"].as_str())
            .collect();
        assert_eq!(ids, vec![pb.as_str()], "only org B's pool");
        assert!(listed["next_cursor"].is_null());
        for id in hidden {
            let path = format!("/api/v1/pools/{id}/nodes");
            assert_eq!(
                call("tok-b", Method::GET, path, json!({})).await.status,
                404
            );
        }
        for (m, path) in [
            (Method::GET, format!("/api/v1/pools/{pa}/nodes")),
            (
                Method::POST,
                format!("/api/v1/pools/{pa}/enrollment-tokens"),
            ),
        ] {
            assert_eq!(
                call("tok-b", m, path.clone(), json!({})).await.status,
                404,
                "{path}"
            );
        }
        // A machine in org A's pool is invisible to org B.
        let token = call(
            "tok-a",
            Method::POST,
            format!("/api/v1/pools/{pa}/enrollment-tokens"),
            json!({}),
        )
        .await;
        assert_eq!(token.status, 201, "{}", token.text());
        let token = token.json()["token"].as_str().unwrap().to_owned();
        let agent = TestAgent::new(1, |_| {});
        let node = mrl_agent::enroll::enroll_dial(
            &agent.ctx,
            &gw.addr().unwrap().to_string(),
            &token,
            mrl_agent::transports::TransportKind::Direct,
            false,
        )
        .await
        .unwrap()
        .link
        .node_id;
        assert_eq!(
            call(
                "tok-a",
                Method::GET,
                format!("/api/v1/nodes/{node}"),
                json!({})
            )
            .await
            .status,
            200
        );
        assert_eq!(
            call(
                "tok-b",
                Method::GET,
                format!("/api/v1/nodes/{node}"),
                json!({})
            )
            .await
            .status,
            404
        );
        assert_eq!(
            call(
                "tok-b",
                Method::POST,
                format!("/api/v1/nodes/{node}:drain"),
                json!({})
            )
            .await
            .status,
            404
        );
        server.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_machine_that_lost_the_enrollment_answer_gets_it_again_with_the_same_key() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let token = gw.token(json!({})).await;
        let pki = gw.gw.pki().unwrap();
        let (key, csr) = mrl_agent::enroll::new_key().unwrap();
        let req = |csr: Vec<u8>| mrl_proto::agent::v1::EnrollRequest {
            join_token: token.clone(),
            csr_der: csr,
            hostname: "gpu-box".into(),
            ..Default::default()
        };
        let enroll = |r: mrl_proto::agent::v1::EnrollRequest| {
            let (db, pki) = (gw.db.db().clone(), pki.clone());
            async move { mrl_server::modules::nodes::store::enroll(&db, pki, &fast_config(), &r).await }
        };
        let first = enroll(req(csr.clone())).await.unwrap();
        // The answer was lost: the same machine (same key, same token) asks again.
        let again = enroll(req(csr)).await.unwrap();
        assert_eq!(again.node, first.node, "the same node, not a second one");
        assert_ne!(again.cert.sha256, first.cert.sha256);
        assert_eq!(gw.db.count("compute_nodes").await, 1);
        // Another key with the same token is still refused.
        let (_, other) = mrl_agent::enroll::new_key().unwrap();
        assert_eq!(enroll(req(other)).await.unwrap_err().code(), "MRL-NODE-180");
        sqlx::query("UPDATE node_certificates SET revoked_at = now() WHERE sha256 = $1")
            .bind(&first.cert.sha256)
            .execute(gw.db.pool())
            .await
            .unwrap();
        let csr_for_retry = || {
            rcgen::CertificateParams::new(Vec::<String>::new())
                .unwrap()
                .serialize_request(&key)
                .unwrap()
                .der()
                .to_vec()
        };
        assert_eq!(
            enroll(req(csr_for_retry())).await.unwrap_err().code(),
            "MRL-NODE-180"
        );
        sqlx::query("UPDATE node_certificates SET revoked_at = NULL WHERE sha256 = $1")
            .bind(&first.cert.sha256)
            .execute(gw.db.pool())
            .await
            .unwrap();
        // Once the machine has connected, the token is spent for good.
        sqlx::query("UPDATE compute_nodes SET session_epoch = 1 WHERE id = $1")
            .bind(first.node)
            .execute(gw.db.pool())
            .await
            .unwrap();
        let csr = rcgen::CertificateParams::new(Vec::<String>::new())
            .unwrap()
            .serialize_request(&key)
            .unwrap()
            .der()
            .to_vec();
        assert_eq!(enroll(req(csr)).await.unwrap_err().code(), "MRL-NODE-180");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_join_token_never_lands_in_the_idempotency_cache() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let r = gw
            .server
            .request(
                Method::POST,
                &format!("/api/v1/pools/{}/enrollment-tokens", gw.pool),
                &[
                    (LAUNCH_SECRET_HEADER, TEST_SECRET),
                    ("content-type", "application/json"),
                    ("idempotency-key", "tok-key-1"),
                ],
                Body::from("{}"),
            )
            .await;
        assert_eq!(r.status, 422, "{}", r.text());
        assert!(!r.text().contains("mrlj_"));
        let kept: Vec<String> =
            sqlx::query_scalar("SELECT row_to_json(k)::text FROM idempotency_keys k")
                .fetch_all(gw.db.pool())
                .await
                .unwrap();
        assert!(kept.iter().all(|row| !row.contains("mrlj_")), "{kept:?}");
        // Without a key the token is made, and only its hash is stored.
        let token = gw.token(json!({})).await;
        let hashes: Vec<String> =
            sqlx::query_scalar("SELECT row_to_json(t)::text FROM node_enrollment_tokens t")
                .fetch_all(gw.db.pool())
                .await
                .unwrap();
        assert!(
            hashes.iter().all(|row| !row.contains(&token)),
            "the raw token is not stored"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_command_that_expired_undelivered_is_never_marked_received() {
        // The health pass (expiry sweep) runs once at start and then not for an hour.
        let gw = GatewayHarness::start_with(
            TestDb::new().await,
            GatewayConfig {
                health_check_seconds: 3600,
                ..fast_config()
            },
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
        )
        .await;
        let a = TestAgent::new(1, |_| {});
        let node =
            NodeId::from_str(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap()).unwrap();
        let probes = CommandSender::new(&gw.gw, &[CommandKind::Probe]);
        // Claimed (SENT), then the connection dropped before the agent got it.
        let lost = probes
            .send(node, probe().deadline(Duration::from_secs(1)))
            .await
            .unwrap();
        sqlx::query("UPDATE agent_commands SET state = 'SENT', sent_at = now() WHERE id = $1")
            .bind(lost.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await; // past its deadline
        let next = probes.send(node, probe()).await.unwrap();
        let link = a.run();
        wait_state(&gw, next.id, "FAILED").await; // delivered and acked (no Probe handler)
        assert_eq!(state_of(&gw, lost.id).await, "EXPIRED", "never RECEIVED");
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_stored_command_too_large_to_send_fails_and_the_next_one_goes() {
        use prost::Message;
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node =
            NodeId::from_str(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap()).unwrap();
        let probes = CommandSender::new(&gw.gw, &[CommandKind::Probe]);
        // Stored by an older server, before sizes were checked.
        let big = probes.send(node, probe()).await.unwrap();
        let payload = Command {
            command_id: big.id.to_string(),
            kind: Some(Kind::Probe(Probe {
                request_id: "x".repeat(5 << 20),
                ..Probe::default()
            })),
        }
        .encode_to_vec();
        sqlx::query("UPDATE agent_commands SET payload = $2 WHERE id = $1")
            .bind(big.id)
            .bind(payload)
            .execute(gw.db.pool())
            .await
            .unwrap();
        let next = probes.send(node, probe()).await.unwrap();
        let link = a.run();
        wait_state(&gw, next.id, "FAILED").await; // delivered (no Probe handler)
        let st = probes.status(big.id).await.unwrap().unwrap();
        assert_eq!(st.state, "FAILED");
        assert_eq!(
            st.result.unwrap()["failure"]["sub_code"],
            "command_too_large"
        );
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_launch_queued_while_offline_is_never_sent_after_a_drain() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node_s = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let node = NodeId::from_str(&node_s).unwrap();
        let launches = CommandSender::new(&gw.gw, &[CommandKind::LaunchAttempt]);
        let queued = launches
            .send(
                node,
                NewCommand::new(Kind::LaunchAttempt(
                    mrl_proto::agent::v1::LaunchAttempt::default(),
                )),
            )
            .await
            .unwrap();
        // Another launch was claimed (SENT) just before the connection dropped: never delivered.
        let lost = launches
            .send(
                node,
                NewCommand::new(Kind::LaunchAttempt(
                    mrl_proto::agent::v1::LaunchAttempt::default(),
                )),
            )
            .await
            .unwrap();
        sqlx::query("UPDATE agent_commands SET state = 'SENT', sent_at = now() WHERE id = $1")
            .bind(lost.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        let r = gw
            .post(&format!("/api/v1/nodes/{node_s}:drain"), json!({}))
            .await;
        assert_eq!(r.json()["status"], "DRAINING", "{}", r.text());
        let st = launches.status(queued.id).await.unwrap().unwrap();
        assert_eq!(st.state, "FAILED");
        assert_eq!(st.result.unwrap()["failure"]["sub_code"], "node_draining");
        // Reconnecting: other commands still go, the launch never does.
        let probes = CommandSender::new(&gw.gw, &[CommandKind::Probe]);
        let p = probes.send(node, probe()).await.unwrap();
        let link = a.run();
        wait_state(&gw, p.id, "FAILED").await; // delivered (no Probe handler)
        assert_eq!(state_of(&gw, queued.id).await, "FAILED");
        // The agent acked the later probe; the undelivered launch is failed, not RECEIVED.
        let st = launches.status(lost.id).await.unwrap().unwrap();
        assert_eq!(st.state, "FAILED");
        assert_eq!(st.result.unwrap()["failure"]["sub_code"], "node_draining");
        let sent: Option<time::OffsetDateTime> =
            sqlx::query_scalar("SELECT sent_at FROM agent_commands WHERE id = $1")
                .bind(queued.id)
                .fetch_one(gw.db.pool())
                .await
                .unwrap();
        assert!(sent.is_none(), "the launch was never sent");
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
        // A command too large to send is refused before it is stored.
        let huge = NewCommand::new(Kind::Probe(Probe {
            request_id: "x".repeat(5 << 20),
            ..Probe::default()
        }));
        assert_eq!(
            probes.send(node, huge).await.unwrap_err().code(),
            "MRL-NODE-187"
        );
        assert_eq!(gw.db.count("agent_commands").await, 0, "nothing stored");

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
        // Right after the drain no heartbeat has shown that nothing runs: not yet.
        let r = gw
            .post(&format!("/api/v1/nodes/{node}:decommission"), json!({}))
            .await;
        assert_eq!(r.status, 422, "{}", r.text());
        assert!(r.text().contains("heartbeat"), "{}", r.text());
        let mut r = r;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            r = gw
                .post(&format!("/api/v1/nodes/{node}:decommission"), json!({}))
                .await;
            if r.status == 200 {
                break;
            }
        }
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
    async fn a_decommission_ends_streams_on_every_replica_and_refuses_later_sessions() {
        // Re-checks five minutes apart: only the decommission itself can end the stream in time.
        let gw = GatewayHarness::start_with(
            TestDb::new().await,
            GatewayConfig {
                revocation_check_seconds: 300,
                ..fast_config()
            },
            Arc::new(InMemorySecretStore::default()),
            LaneRouter::new(),
        )
        .await;
        let (_replica, b) = gw.replica().await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let id = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        // The agent's session runs on replica B; the operator's request goes to replica A.
        let stop = CancellationToken::new();
        let conn = {
            let (ctx, link, id, s) = (
                a.ctx.clone(),
                Link::of(&a.ctx).unwrap(),
                id.clone(),
                stop.clone(),
            );
            let addr = b.addr().unwrap().to_string();
            tokio::spawn(async move {
                mrl_agent::session::connect_once(
                    ctx,
                    link,
                    id,
                    &mrl_agent::transports::Direct::new(&addr),
                    s,
                )
                .await
            })
        };
        gw.wait_status(&node, "ONLINE").await;
        let r = gw
            .post(&format!("/api/v1/nodes/{node}:drain"), json!({}))
            .await;
        assert_eq!(r.json()["status"], "DRAINING", "{}", r.text());
        let mut r = r;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            r = gw
                .post(&format!("/api/v1/nodes/{node}:decommission"), json!({}))
                .await;
            if r.status == 200 {
                break;
            }
        }
        assert_eq!(r.json()["status"], "DECOMMISSIONED", "{}", r.text());
        let ended = tokio::time::timeout(Duration::from_secs(10), conn)
            .await
            .expect("the stream on the other replica ended at once, not at its next re-check")
            .unwrap();
        assert!(ended.is_err(), "{ended:?}");
        // A session the handshake let through just before the decommission committed is
        // refused when it opens.
        let opened = mrl_server::modules::nodes::store::open_control(
            gw.gw.db(),
            uuid_of(&node),
            &id.cert_sha256(),
            "core-test",
            &mrl_proto::agent::v1::AgentHello::default(),
            None,
        )
        .await;
        assert!(
            matches!(
                opened,
                Err(mrl_server::modules::nodes::NodesError::Certificate(_))
            ),
            "{opened:?}"
        );
        stop.cancel();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn cancelling_a_control_send_cleans_up_the_session() {
        use mrl_proto::agent::v1::{AgentHello, AgentMessage, CoreMessage, Lane};
        use mrl_server::grpc::{session, PeerInfo};
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let identity = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        let peer = PeerInfo {
            cert_sha256: Some(identity.cert_sha256()),
            connection: PeerInfo::new_connection(),
            ..PeerInfo::default()
        };
        let hello = AgentMessage {
            body: Some(agent_message::Body::Hello(AgentHello {
                node_id: node.clone(),
                lane: Lane::Control as i32,
                protocol_versions: vec![mrl_server::grpc::PROTOCOL_VERSION.to_owned()],
                ..AgentHello::default()
            })),
            ..AgentMessage::default()
        };
        let (out, _unread) = tokio::sync::mpsc::channel(1);
        out.try_send(Ok(CoreMessage::default())).unwrap();
        let stop = CancellationToken::new();
        let task = tokio::spawn(session::run(
            gw.gw.clone(),
            peer,
            Box::pin(tonic::codegen::tokio_stream::once(Ok(hello))),
            out,
            stop.clone(),
        ));
        gw.wait(&node, |n| n["session_epoch"] == 1, "open CONTROL")
            .await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        let holder: Option<String> =
            sqlx::query_scalar("SELECT session_holder FROM compute_nodes WHERE id = $1")
                .bind(uuid_of(&node))
                .fetch_one(gw.db.pool())
                .await
                .unwrap();
        assert!(
            holder.is_none(),
            "cancelled CONTROL still holds the node: {holder:?}"
        );

        let (out, _unread) = tokio::sync::mpsc::channel(1);
        out.try_send(Ok(CoreMessage::default())).unwrap();
        tokio::time::timeout(
            Duration::from_secs(3),
            session::run(
                gw.gw.clone(),
                PeerInfo::default(),
                Box::pin(tonic::codegen::tokio_stream::empty()),
                out,
                CancellationToken::new(),
            ),
        )
        .await
        .expect("a terminal status cannot wait on a full channel");
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
        let cfg = server_config(fast_config(), Mode::Solo, 0);
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

    /// Calls `mrl.testkit.Ping` on the gateway's listener over TLS (no client certificate):
    /// the HTTP status and `grpc-status`.
    async fn ping(gw: &GatewayHarness, secret: Option<&str>) -> (u16, Option<String>) {
        use mrl_agent::transports::Transport;
        use tower::{Service, ServiceExt};
        let pin = gw.gw.ca_fingerprint().unwrap();
        let tls = mrl_agent::session::tls::enroll_client(&pin).unwrap();
        let io = mrl_agent::transports::Direct::new(&gw.addr().to_string())
            .connect()
            .await
            .unwrap();
        let tls = mrl_agent::session::connect_tls(tls, io, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        let mut channel = mrl_agent::session::channel_over(tls, &CancellationToken::new())
            .await
            .unwrap();
        let mut req = axum::http::Request::builder()
            .method("POST")
            .uri("http://core.masterrl.internal/mrl.testkit.Ping/Ping")
            .header("content-type", "application/grpc")
            .header("te", "trailers");
        if let Some(s) = secret {
            req = req.header(LAUNCH_SECRET_HEADER, s);
        }
        let res = channel
            .ready()
            .await
            .unwrap()
            .call(req.body(tonic::body::Body::empty()).unwrap())
            .await
            .unwrap();
        (
            res.status().as_u16(),
            res.headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
        )
    }

    /// Enrollment and an mTLS session on the shared gRPC listener, next to another service.
    async fn agents_use_the_shared_grpc_listener(mode: Mode) {
        let gw = GatewayHarness::start_in(TestDb::new().await, mode).await;
        assert_eq!(
            gw.server.running().grpc_addr(),
            gw.gw.addr(),
            "the gateway is on the shared gRPC listener"
        );
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let link = a.run();
        let n = gw.wait_status(&node, "ONLINE").await;
        assert_eq!(n["transport"], "direct");
        // The other service shares the TLS and keeps its own rules: in solo it still needs the
        // launch secret (only the agent gateway authenticates its callers itself).
        let ok = (200, Some("0".to_owned()));
        match mode {
            Mode::Solo => {
                assert_ne!(ping(&gw, None).await, ok, "no launch secret: refused");
                assert_eq!(ping(&gw, Some(TEST_SECRET)).await, ok);
            }
            Mode::Team => assert_eq!(ping(&gw, None).await, ok),
        }
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn agents_enroll_and_connect_on_the_shared_grpc_listener_in_solo_mode() {
        agents_use_the_shared_grpc_listener(Mode::Solo).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn agents_enroll_and_connect_on_the_shared_grpc_listener_in_team_mode() {
        agents_use_the_shared_grpc_listener(Mode::Team).await;
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

    /// A TCP proxy whose agent→core direction can be silently dropped, and whose connections can
    /// be cut (new connections still pass).
    struct Proxy {
        addr: SocketAddr,
        drop_up: Arc<std::sync::atomic::AtomicBool>,
        generation: Arc<std::sync::Mutex<CancellationToken>>,
    }

    impl Proxy {
        async fn start(target: SocketAddr) -> Self {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            let drop_up = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let generation = Arc::new(std::sync::Mutex::new(CancellationToken::new()));
            let (d, g) = (drop_up.clone(), generation.clone());
            tokio::spawn(async move {
                while let Ok((agent, _)) = l.accept().await {
                    let Ok(core) = tokio::net::TcpStream::connect(target).await else {
                        continue;
                    };
                    let cut = g.lock().unwrap().clone();
                    let (mut ar, mut aw) = agent.into_split();
                    let (mut cr, mut cw) = core.into_split();
                    let d = d.clone();
                    let c1 = cut.clone();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 16 * 1024];
                        loop {
                            let n = tokio::select! { () = c1.cancelled() => break, r = ar.read(&mut buf) => match r { Ok(0) | Err(_) => break, Ok(n) => n } };
                            if d.load(Ordering::SeqCst) {
                                continue; // dropped on the way to the core
                            }
                            if cw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    });
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 16 * 1024];
                        loop {
                            let n = tokio::select! { () = cut.cancelled() => break, r = cr.read(&mut buf) => match r { Ok(0) | Err(_) => break, Ok(n) => n } };
                            if aw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
            Self {
                addr,
                drop_up,
                generation,
            }
        }

        fn drop_agent_to_core(&self, on: bool) {
            self.drop_up.store(on, Ordering::SeqCst);
        }

        fn cut(&self) {
            let mut g = self.generation.lock().unwrap();
            g.cancel();
            *g = CancellationToken::new();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_lost_ack_neither_reruns_the_command_nor_loses_its_result() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let proxy = Proxy::start(gw.addr()).await;
        let via = proxy.addr.to_string();
        let a = TestAgent::new(1, |c| c.gateway.server = Some(via.clone()));
        let node_s = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let node = NodeId::from_str(&node_s).unwrap();
        let count = Arc::new(Count(AtomicUsize::new(0)));
        Link::of(&a.ctx).unwrap().register("Probe", count.clone());
        let link = a.run();
        gw.wait_status(&node_s, "ONLINE").await;

        // Everything the agent sends from now on is lost: its RECEIVED ack and its result.
        proxy.drop_agent_to_core(true);
        let c = CommandSender::new(&gw.gw, &[CommandKind::Probe])
            .send(node, probe())
            .await
            .unwrap();
        for _ in 0..100 {
            if count.0.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            1,
            "the agent got and ran the command"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            state_of(&gw, c.id).await,
            "SENT",
            "the ack never reached the core"
        );

        // The connection breaks; the agent reconnects through a healthy path.
        proxy.drop_agent_to_core(false);
        proxy.cut();
        wait_state(&gw, c.id, "COMPLETED").await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(count.0.load(Ordering::SeqCst), 1, "ran exactly once");
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn claims_skip_superseded_and_expired_rows_and_refuse_a_stale_epoch() {
        use mrl_server::modules::nodes::store::{self, Claim};
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node =
            NodeId::from_str(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap()).unwrap();
        let sender = CommandSender::new(&gw.gw, &[CommandKind::Probe]);
        let c1 = sender.send(node, probe()).await.unwrap();
        let c2 = sender.send(node, probe()).await.unwrap();
        let c3 = sender.send(node, probe()).await.unwrap();
        sqlx::query(
            "UPDATE compute_nodes SET session_epoch = 5, session_holder = 'test' WHERE id = $1",
        )
        .bind(node.as_uuid())
        .execute(gw.db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE agent_commands SET state = 'SUPERSEDED' WHERE id = $1")
            .bind(c1.id)
            .execute(gw.db.pool())
            .await
            .unwrap();
        sqlx::query(
            "UPDATE agent_commands SET deadline_at = now() - interval '1 second' WHERE id = $1",
        )
        .bind(c2.id)
        .execute(gw.db.pool())
        .await
        .unwrap();
        assert!(matches!(
            store::claim_commands(gw.db.db(), node.as_uuid(), 4, 0)
                .await
                .unwrap(),
            Claim::Stale
        ));
        assert_eq!(
            state_of(&gw, c3.id).await,
            "PENDING",
            "a stale session claims nothing"
        );
        let Claim::Send(rows) = store::claim_commands(gw.db.db(), node.as_uuid(), 5, 0)
            .await
            .unwrap()
        else {
            panic!("current epoch claims")
        };
        assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), vec![c3.id]);
        assert_eq!(
            (
                state_of(&gw, c1.id).await,
                state_of(&gw, c2.id).await,
                state_of(&gw, c3.id).await
            ),
            // Past its deadline and above the claim point: expired by the claim itself.
            ("SUPERSEDED".into(), "EXPIRED".into(), "SENT".into())
        );
        assert!(
            !store::commands_received(gw.db.db(), node.as_uuid(), 4, 10)
                .await
                .unwrap(),
            "a stale ack changes nothing"
        );
        assert_eq!(state_of(&gw, c3.id).await, "SENT");
    }

    fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for e in std::fs::read_dir(from).unwrap() {
            let e = e.unwrap();
            let dest = to.join(e.file_name());
            if e.file_type().unwrap().is_dir() {
                copy_dir(&e.path(), &dest);
            } else {
                std::fs::copy(e.path(), dest).unwrap();
            }
        }
    }

    type RawClient = mrl_proto::agent::v1::agent_gateway_service_client::AgentGatewayServiceClient<
        tonic::transport::Channel,
    >;

    /// A bare gateway connection with the identity `id`; its streams share one connection.
    async fn raw_client(addr: &str, id: &mrl_agent::enroll::Identity) -> RawClient {
        use mrl_agent::transports::Transport;
        let stop = CancellationToken::new();
        let io = mrl_agent::transports::Direct::new(addr)
            .connect()
            .await
            .unwrap();
        let tls = mrl_agent::session::tls::client(&id.ca, id.chain(), id.key()).unwrap();
        let tls = mrl_agent::session::connect_tls(tls, io, &stop)
            .await
            .unwrap()
            .unwrap();
        RawClient::new(
            mrl_agent::session::channel_over(tls, &CancellationToken::new())
                .await
                .unwrap(),
        )
    }

    /// A bare stream on `lane` of `client`'s connection, past the hellos.
    async fn raw_stream(
        client: &mut RawClient,
        id: &mrl_agent::enroll::Identity,
        lane: mrl_proto::agent::v1::Lane,
    ) -> (
        tokio::sync::mpsc::Sender<mrl_proto::agent::v1::AgentMessage>,
        tonic::Streaming<mrl_proto::agent::v1::CoreMessage>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tx.send(mrl_proto::agent::v1::AgentMessage {
            body: Some(agent_message::Body::Hello(
                mrl_proto::agent::v1::AgentHello {
                    node_id: id.link.node_id.clone(),
                    certificate_sha256: id.cert_sha256(),
                    protocol_versions: vec![mrl_agent::session::PROTOCOL_VERSION.to_owned()],
                    lane: lane as i32,
                    resume_from_seq: 1,
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut inbound = client
            .session(tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(
                rx,
            ))
            .await
            .unwrap()
            .into_inner();
        let hello = inbound.message().await.unwrap().unwrap();
        assert!(matches!(
            hello.body,
            Some(mrl_proto::agent::v1::core_message::Body::Hello(_))
        ));
        (tx, inbound)
    }

    /// Reads `inbound` until it ends with a status (`seen` counts the commands on the way).
    async fn closing_status(
        inbound: &mut tonic::Streaming<mrl_proto::agent::v1::CoreMessage>,
        commands: &mut usize,
    ) -> tonic::Status {
        loop {
            match tokio::time::timeout(Duration::from_secs(15), inbound.message())
                .await
                .expect("the stream closes")
            {
                Ok(Some(m)) => {
                    if matches!(
                        m.body,
                        Some(mrl_proto::agent::v1::core_message::Body::Command(_))
                    ) {
                        *commands += 1;
                    }
                }
                Ok(None) => panic!("closed without a status"),
                Err(status) => return status,
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn after_a_takeover_on_another_replica_the_old_stream_sends_and_records_nothing() {
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
        let (_replica, b) = gw.replica().await;
        let first = TestAgent::new(1, |_| {});
        let node_s = first.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let second = TestAgent::new(1, |_| {});
        copy_dir(
            &mrl_agent::enroll::identity_dir(first.ctx.state.root()),
            &mrl_agent::enroll::identity_dir(second.ctx.state.root()),
        );
        let id = mrl_agent::enroll::Identity::load(first.ctx.state.root())
            .unwrap()
            .unwrap();

        let l2 = Link::of(&second.ctx).unwrap();
        let s2 = CancellationToken::new();
        // The old connection, on replica A: CONTROL, TELEMETRY and BULK on one connection.
        let mut old = raw_client(&gw.addr().to_string(), &id).await;
        let (_old_ctl, mut old_ctl_in) =
            raw_stream(&mut old, &id, mrl_proto::agent::v1::Lane::Control).await;
        // (No inventory on this bare stream: the node holds a session but is not ONLINE yet.)
        let e1 = gw
            .wait(
                &node_s,
                |n| n["session_epoch"].as_i64() >= Some(1),
                "a session",
            )
            .await["session_epoch"]
            .as_i64()
            .unwrap();
        let (old_bulk, mut old_bulk_in) =
            raw_stream(&mut old, &id, mrl_proto::agent::v1::Lane::Bulk).await;
        let (old_tel, mut old_tel_in) =
            raw_stream(&mut old, &id, mrl_proto::agent::v1::Lane::Telemetry).await;
        let new = {
            let (ctx, s, a) = (
                second.ctx.clone(),
                s2.clone(),
                b.addr().unwrap().to_string(),
            );
            tokio::spawn(async move {
                mrl_agent::session::connect_once(
                    ctx,
                    l2,
                    id,
                    &mrl_agent::transports::Direct::new(&a),
                    s,
                )
                .await
            })
        };
        gw.wait(
            &node_s,
            |n| n["session_epoch"].as_i64() > Some(e1),
            "a takeover",
        )
        .await;
        // The old CONTROL stream, on replica A, learns it is stale and closes.
        let mut old_commands = 0;
        let ended = closing_status(&mut old_ctl_in, &mut old_commands).await;
        assert_eq!(ended.code(), tonic::Code::Aborted, "{ended:?}");
        let c = CommandSender::new(&gw.gw, &[CommandKind::Probe])
            .send(NodeId::from_str(&node_s).unwrap(), probe())
            .await
            .unwrap();
        wait_state(&gw, c.id, "FAILED").await; // no Probe handler; delivered through replica B
        assert_eq!(old_commands, 0, "the old stream never sent it");
        assert!(Link::of(&second.ctx).unwrap().journal_seq() >= c.seq);
        // Whatever the old data streams send now is refused: no CONTROL-class record, no
        // metrics to the sink, no telemetry (a far-future reading would win otherwise).
        let send = |m: mrl_proto::agent::v1::AgentMessage| {
            let (b, t) = (old_bulk.clone(), old_tel.clone());
            async move {
                let _ = b.send(m.clone()).await;
                let _ = t.send(m).await;
            }
        };
        send(mrl_proto::agent::v1::AgentMessage {
            seq: 777_777,
            body: Some(agent_message::Body::AttemptStatus(AttemptStatus::default())),
            ..Default::default()
        })
        .await;
        send(mrl_proto::agent::v1::AgentMessage {
            seq: 777_778,
            body: Some(agent_message::Body::Worker(
                mrl_proto::agent::v1::WorkerRelay {
                    message: Some(mrl_proto::worker::v1::WorkerMessage {
                        body: Some(mrl_proto::worker::v1::worker_message::Body::Metrics(
                            Default::default(),
                        )),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .await;
        send(mrl_proto::agent::v1::AgentMessage {
            seq: 777_779,
            body: Some(agent_message::Body::Telemetry(
                mrl_proto::agent::v1::NodeTelemetry {
                    time: Some(prost_types::Timestamp {
                        seconds: 4_000_000_000,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .await;
        for inbound in [&mut old_bulk_in, &mut old_tel_in] {
            let refused = closing_status(inbound, &mut 0).await;
            assert_eq!(refused.code(), tonic::Code::Aborted, "{refused:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            seen.0.lock().unwrap().iter().all(|(_, seq)| *seq < 777_777),
            "no metrics from the old session reached the sink"
        );
        let future: bool = sqlx::query_scalar(
            "SELECT COALESCE(telemetry_at > now() + interval '1 year', false) FROM compute_nodes WHERE id = $1",
        )
        .bind(uuid_of(&node_s))
        .fetch_one(gw.db.pool())
        .await
        .unwrap();
        assert!(!future, "the old session's telemetry was not saved");
        let kept: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM node_events WHERE node_id = $1 AND kind = 'control_unrouted'
               AND (details->>'seq')::bigint = 777777",
        )
        .bind(uuid_of(&node_s))
        .fetch_one(gw.db.pool())
        .await
        .unwrap();
        assert_eq!(kept, 0, "the stale stream recorded nothing");
        s2.cancel();
        let _ = new.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn metrics_stop_the_moment_a_takeover_commits_before_the_old_stream_hears() {
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
        let id = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        let mut conn = raw_client(&gw.addr().to_string(), &id).await;
        let (_ctl, _ctl_in) = raw_stream(&mut conn, &id, mrl_proto::agent::v1::Lane::Control).await;
        gw.wait(
            &node,
            |n| n["session_epoch"].as_i64() >= Some(1),
            "a session",
        )
        .await;
        let (bulk, _bulk_in) = raw_stream(&mut conn, &id, mrl_proto::agent::v1::Lane::Bulk).await;
        let metrics = |seq: u64| mrl_proto::agent::v1::AgentMessage {
            seq,
            body: Some(agent_message::Body::Worker(
                mrl_proto::agent::v1::WorkerRelay {
                    message: Some(mrl_proto::worker::v1::WorkerMessage {
                        body: Some(mrl_proto::worker::v1::worker_message::Body::Metrics(
                            Default::default(),
                        )),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        bulk.send(metrics(1)).await.unwrap();
        for _ in 0..100 {
            if !seen.0.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            seen.0.lock().unwrap().len(),
            1,
            "the current session's metrics arrive"
        );
        // Another replica takes over: committed, but this replica is not woken (no notify), so
        // its streams stay open until their next check.
        sqlx::query("UPDATE compute_nodes SET session_epoch = session_epoch + 1 WHERE id = $1")
            .bind(uuid_of(&node))
            .execute(gw.db.pool())
            .await
            .unwrap();
        let _ = bulk.send(metrics(888_888)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            seen.0.lock().unwrap().iter().all(|(_, s)| *s != 888_888),
            "nothing from the old session reaches the sink"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn telemetry_sink_gets_the_saved_time_before_takeover_can_commit() {
        use mrl_proto::agent::v1::{AgentMessage, Lane, NodeTelemetry};
        use mrl_server::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink, SinkError};
        struct Observation {
            saved: tokio::sync::mpsc::Sender<prost_types::Timestamp>,
            release: tokio::sync::Notify,
        }
        #[async_trait::async_trait]
        impl LaneSink for Observation {
            fn name(&self) -> &'static str {
                "observation"
            }
            fn lanes(&self) -> &'static [LaneKind] {
                &[LaneKind::Telemetry]
            }
            async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
                let Some(agent_message::Body::Telemetry(telemetry)) = item.message.body else {
                    panic!("expected a telemetry reading");
                };
                self.saved.send(telemetry.time.unwrap()).await.unwrap();
                self.release.notified().await;
                Ok(())
            }
        }
        let (saved, mut observed) = tokio::sync::mpsc::channel(1);
        let sink = Arc::new(Observation {
            saved,
            release: tokio::sync::Notify::new(),
        });
        let lanes = LaneRouter::new();
        lanes.register(sink.clone());
        let gw = GatewayHarness::start_with(
            TestDb::new().await,
            fast_config(),
            Arc::new(InMemorySecretStore::default()),
            lanes,
        )
        .await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let identity = mrl_agent::enroll::Identity::load(a.ctx.state.root())
            .unwrap()
            .unwrap();
        let mut conn = raw_client(&gw.addr().to_string(), &identity).await;
        let (_control, _replies) = raw_stream(&mut conn, &identity, Lane::Control).await;
        let (telemetry, _replies) = raw_stream(&mut conn, &identity, Lane::Telemetry).await;
        telemetry
            .send(AgentMessage {
                seq: 1,
                body: Some(agent_message::Body::Telemetry(NodeTelemetry {
                    time: Some(mrl_server::grpc::session::ts(
                        time::OffsetDateTime::now_utc() + time::Duration::days(1),
                    )),
                    ..NodeTelemetry::default()
                })),
                ..AgentMessage::default()
            })
            .await
            .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(5), observed.recv())
            .await
            .unwrap()
            .unwrap();
        let stored: time::OffsetDateTime =
            sqlx::query_scalar("SELECT telemetry_at FROM compute_nodes WHERE id = $1")
                .bind(uuid_of(&node))
                .fetch_one(gw.db.pool())
                .await
                .unwrap();
        assert_eq!(observed, mrl_server::grpc::session::ts(stored));
        assert!(stored < time::OffsetDateTime::now_utc() + time::Duration::minutes(1));

        let hello = mrl_proto::agent::v1::AgentHello::default();
        let sha = identity.cert_sha256();
        let takeover = mrl_server::modules::nodes::store::open_control(
            gw.db.db(),
            uuid_of(&node),
            &sha,
            "next-core",
            &hello,
            None,
        );
        tokio::pin!(takeover);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut takeover)
                .await
                .is_err()
        );
        sink.release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), takeover)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_late_data_stream_never_borrows_another_connections_session() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let first = TestAgent::new(1, |_| {});
        let node = first.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let second = TestAgent::new(1, |_| {});
        copy_dir(
            &mrl_agent::enroll::identity_dir(first.ctx.state.root()),
            &mrl_agent::enroll::identity_dir(second.ctx.state.root()),
        );
        let id = mrl_agent::enroll::Identity::load(first.ctx.state.root())
            .unwrap()
            .unwrap();
        // Two overlapping connections on one replica: the second takes over.
        let a = first.run();
        let e1 = gw.wait_status(&node, "ONLINE").await["session_epoch"]
            .as_i64()
            .unwrap();
        let b = second.run();
        gw.wait(
            &node,
            |n| n["session_epoch"].as_i64() > Some(e1),
            "a takeover",
        )
        .await;
        // A data stream from yet another connection (like a late lane of the old one) is
        // refused: it may not use the current connection's session.
        use mrl_agent::transports::Transport;
        let stop = CancellationToken::new();
        let io = mrl_agent::transports::Direct::new(&gw.addr().to_string())
            .connect()
            .await
            .unwrap();
        let tls = mrl_agent::session::tls::client(&id.ca, id.chain(), id.key()).unwrap();
        let tls = mrl_agent::session::connect_tls(tls, io, &stop)
            .await
            .unwrap()
            .unwrap();
        let mut client =
            mrl_proto::agent::v1::agent_gateway_service_client::AgentGatewayServiceClient::new(
                mrl_agent::session::channel_over(tls, &CancellationToken::new())
                    .await
                    .unwrap(),
            );
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(mrl_proto::agent::v1::AgentMessage {
            body: Some(agent_message::Body::Hello(
                mrl_proto::agent::v1::AgentHello {
                    node_id: id.link.node_id.clone(),
                    certificate_sha256: id.cert_sha256(),
                    protocol_versions: vec![mrl_agent::session::PROTOCOL_VERSION.to_owned()],
                    lane: mrl_proto::agent::v1::Lane::Telemetry as i32,
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut inbound = client
            .session(tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(
                rx,
            ))
            .await
            .unwrap()
            .into_inner();
        let refused = loop {
            match tokio::time::timeout(Duration::from_secs(15), inbound.message())
                .await
                .expect("the late stream is answered")
            {
                Ok(Some(_hello)) => continue,
                Ok(None) => panic!("closed without a status"),
                Err(status) => break status,
            }
        };
        assert_eq!(
            refused.code(),
            tonic::Code::FailedPrecondition,
            "{refused:?}"
        );
        drop(tx);
        b.stop().await;
        a.stop().await;
    }

    /// A CONTROL sink that answers every item with a command to the same node.
    struct Answers {
        sender: std::sync::OnceLock<CommandSender>,
        seen: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl mrl_server::modules::nodes::lanes::LaneSink for Answers {
        fn name(&self) -> &'static str {
            "answers"
        }
        fn lanes(&self) -> &'static [mrl_server::modules::nodes::lanes::LaneKind] {
            &[mrl_server::modules::nodes::lanes::LaneKind::Control]
        }
        async fn deliver(
            &self,
            item: mrl_server::modules::nodes::lanes::LaneItem,
        ) -> Result<(), mrl_server::modules::nodes::lanes::SinkError> {
            let err = |detail: String| mrl_server::modules::nodes::lanes::SinkError {
                sink: "answers",
                detail,
            };
            let sender = self.sender.get().ok_or_else(|| err("not ready".into()))?;
            sender
                .send(NodeId::from_uuid(item.node_id), probe())
                .await
                .map_err(|e| err(e.explain()))?;
            self.seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A CONTROL sink that needs a connection from `0` for every item.
    struct UsesPool(mrl_server::db::Db, AtomicUsize);

    #[async_trait::async_trait]
    impl mrl_server::modules::nodes::lanes::LaneSink for UsesPool {
        fn name(&self) -> &'static str {
            "uses-pool"
        }
        fn lanes(&self) -> &'static [mrl_server::modules::nodes::lanes::LaneKind] {
            &[mrl_server::modules::nodes::lanes::LaneKind::Control]
        }
        async fn deliver(
            &self,
            _: mrl_server::modules::nodes::lanes::LaneItem,
        ) -> Result<(), mrl_server::modules::nodes::lanes::SinkError> {
            sqlx::query("SELECT 1")
                .execute(self.0.pool())
                .await
                .map_err(|e| mrl_server::modules::nodes::lanes::SinkError {
                    sink: "uses-pool",
                    detail: e.to_string(),
                })?;
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Keeps one CONTROL message for `node` in the inbox, as a stream would.
    async fn keep(gw: &GatewayHarness, node: uuid::Uuid, seq: i64) {
        use prost::Message;
        let msg = mrl_proto::agent::v1::AgentMessage {
            seq: seq as u64,
            body: Some(agent_message::Body::AttemptStatus(AttemptStatus::default())),
            ..Default::default()
        };
        sqlx::query(
            "INSERT INTO node_control_inbox (node_id, stream, seq, session_epoch, message)
             VALUES ($1, 1, $2, 1, $3)",
        )
        .bind(node)
        .bind(seq)
        .bind(msg.encode_to_vec())
        .execute(gw.db.pool())
        .await
        .unwrap();
    }

    /// A CONTROL sink that takes `0` to answer.
    struct Slow(Duration, AtomicUsize);

    #[async_trait::async_trait]
    impl mrl_server::modules::nodes::lanes::LaneSink for Slow {
        fn name(&self) -> &'static str {
            "slow"
        }
        fn lanes(&self) -> &'static [mrl_server::modules::nodes::lanes::LaneKind] {
            &[mrl_server::modules::nodes::lanes::LaneKind::Control]
        }
        async fn deliver(
            &self,
            _: mrl_server::modules::nodes::lanes::LaneItem,
        ) -> Result<(), mrl_server::modules::nodes::lanes::SinkError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.0).await;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_sink_slower_than_the_lease_keeps_it_and_a_lost_lease_touches_nothing() {
        use mrl_server::modules::nodes::store::dispatch_with_lease;
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = uuid_of(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap());
        let lease = Duration::from_secs(1);

        // The sink runs three leases long: the lease is renewed, so a second dispatcher that
        // starts meanwhile delivers nothing.
        keep(&gw, node, 1).await;
        let slow = Arc::new(Slow(Duration::from_secs(3), AtomicUsize::new(0)));
        let first = {
            let (db, slow) = (gw.db.db().clone(), slow.clone());
            tokio::spawn(async move { dispatch_with_lease(&db, slow.as_ref(), node, lease).await })
        };
        tokio::time::sleep(Duration::from_millis(1500)).await;
        dispatch_with_lease(gw.db.db(), slow.as_ref(), node, lease)
            .await
            .unwrap();
        assert_eq!(
            slow.1.load(Ordering::SeqCst),
            1,
            "still leased: not delivered twice"
        );
        first.await.unwrap().unwrap();
        assert_eq!(slow.1.load(Ordering::SeqCst), 1);
        assert_eq!(gw.db.count("node_control_inbox").await, 0);

        // A dispatcher whose lease was taken over never deletes the new owner's row.
        keep(&gw, node, 2).await;
        let slow = Arc::new(Slow(Duration::from_secs(2), AtomicUsize::new(0)));
        let first = {
            let (db, slow) = (gw.db.db().clone(), slow.clone());
            tokio::spawn(async move { dispatch_with_lease(&db, slow.as_ref(), node, lease).await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        sqlx::query(
            "UPDATE node_control_inbox SET lease_token = gen_random_uuid(),
                leased_until = now() + interval '1 hour'",
        )
        .execute(gw.db.pool())
        .await
        .unwrap();
        first.await.unwrap().unwrap();
        assert_eq!(
            gw.db.count("node_control_inbox").await,
            1,
            "the other owner's row stays"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_database_from_before_inbox_leases_upgrades_and_delivers_its_kept_rows() {
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = uuid_of(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap());
        // The database as the previous revision left it: 18010 applied, 18011 not.
        sqlx::raw_sql(
            "DROP INDEX node_control_inbox_node;
             ALTER TABLE node_control_inbox DROP COLUMN leased_until, DROP COLUMN lease_token;
             DELETE FROM _sqlx_migrations WHERE version = 18011;",
        )
        .execute(gw.db.pool())
        .await
        .unwrap();
        keep(&gw, node, 1).await;
        let applied = mrl_server::db::migrate::apply(gw.db.db()).await.unwrap();
        assert_eq!(applied, vec![18011]);
        let sink = Slow(Duration::ZERO, AtomicUsize::new(0));
        mrl_server::modules::nodes::store::dispatch(gw.db.db(), &sink, node)
            .await
            .unwrap();
        assert_eq!(
            sink.1.load(Ordering::SeqCst),
            1,
            "the pending row is delivered"
        );
        assert_eq!(gw.db.count("node_control_inbox").await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn kept_control_messages_reach_a_sink_that_shares_a_one_connection_pool() {
        use prost::Message;
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = uuid_of(&a.enroll(&gw, &gw.token(json!({})).await).await.unwrap());
        for seq in [1i64, 2] {
            let msg = mrl_proto::agent::v1::AgentMessage {
                seq: seq as u64,
                body: Some(agent_message::Body::AttemptStatus(AttemptStatus::default())),
                ..Default::default()
            };
            sqlx::query(
                "INSERT INTO node_control_inbox (node_id, stream, seq, session_epoch, message)
                 VALUES ($1, 1, $2, 1, $3)",
            )
            .bind(node)
            .bind(seq)
            .bind(msg.encode_to_vec())
            .execute(gw.db.pool())
            .await
            .unwrap();
        }
        let one = gw.db.replica_with(&mrl_server::config::DatabaseConfig {
            max_connections: 1,
            min_connections: 0,
            acquire_timeout_seconds: 5,
            ..crate::pg::test_config()
        });
        let sink = UsesPool(one.clone(), AtomicUsize::new(0));
        tokio::time::timeout(
            Duration::from_secs(30),
            mrl_server::modules::nodes::store::dispatch(&one, &sink, node),
        )
        .await
        .expect("the dispatcher never waits for its own connection")
        .unwrap();
        assert_eq!(sink.1.load(Ordering::SeqCst), 2);
        assert_eq!(gw.db.count("node_control_inbox").await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_control_sink_may_send_a_command_to_the_same_node() {
        let router = LaneRouter::new();
        let sink = Arc::new(Answers {
            sender: std::sync::OnceLock::new(),
            seen: AtomicUsize::new(0),
        });
        router.register(sink.clone());
        let gw = GatewayHarness::start_with(
            TestDb::new().await,
            fast_config(),
            Arc::new(InMemorySecretStore::default()),
            router,
        )
        .await;
        let _ = sink
            .sender
            .set(CommandSender::new(&gw.gw, &[CommandKind::Probe]));
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        Link::of(&a.ctx)
            .unwrap()
            .send_control(agent_message::Body::AttemptStatus(AttemptStatus::default()))
            .unwrap();
        let link = a.run();
        // The sink's command reaches the agent (no Probe handler there: FAILED), so the gateway
        // never waited on the sink while holding the node row.
        let mut answered = None;
        for _ in 0..100 {
            answered = sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT id FROM agent_commands WHERE node_id = $1 ORDER BY seq LIMIT 1",
            )
            .bind(uuid_of(&node))
            .fetch_optional(gw.db.pool())
            .await
            .unwrap();
            if answered.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        wait_state(&gw, answered.expect("the sink sent a command"), "FAILED").await;
        assert!(sink.seen.load(Ordering::SeqCst) >= 1);
        let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM node_control_inbox")
            .fetch_one(gw.db.pool())
            .await
            .unwrap();
        assert_eq!(kept, 0, "delivered messages leave the inbox");
        link.stop().await;
    }

    /// Records the seq of every LOGS item.
    struct Logs(std::sync::Mutex<Vec<u64>>);

    #[async_trait::async_trait]
    impl mrl_server::modules::nodes::lanes::LaneSink for Logs {
        fn name(&self) -> &'static str {
            "logs"
        }
        fn lanes(&self) -> &'static [mrl_server::modules::nodes::lanes::LaneKind] {
            &[mrl_server::modules::nodes::lanes::LaneKind::Logs]
        }
        async fn deliver(
            &self,
            item: mrl_server::modules::nodes::lanes::LaneItem,
        ) -> Result<(), mrl_server::modules::nodes::lanes::SinkError> {
            self.0.lock().unwrap().push(item.seq);
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn a_log_queued_offline_arrives_after_an_idle_reconnect() {
        let router = LaneRouter::new();
        let logs = Arc::new(Logs(std::sync::Mutex::new(Vec::new())));
        router.register(logs.clone());
        let gw = GatewayHarness::start_with(
            TestDb::new().await,
            fast_config(),
            Arc::new(InMemorySecretStore::default()),
            router,
        )
        .await;
        let a = TestAgent::new(1, |_| {});
        let _node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        Link::of(&a.ctx)
            .unwrap()
            .send_logs(mrl_proto::agent::v1::WorkerRelay {
                message: Some(mrl_proto::worker::v1::WorkerMessage {
                    body: Some(mrl_proto::worker::v1::worker_message::Body::Log(
                        Default::default(),
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            });
        let link = a.run(); // nothing else is queued: no further wake-up comes
        for _ in 0..100 {
            if !logs.0.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(logs.0.lock().unwrap().len(), 1);
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn an_older_reading_never_replaces_a_newer_one_and_hot_gpus_degrade_the_node() {
        use mrl_server::modules::nodes::store;
        let gw = GatewayHarness::start(TestDb::new().await).await;
        let a = TestAgent::new(1, |_| {});
        let node = a.enroll(&gw, &gw.token(json!({})).await).await.unwrap();
        let link = a.run();
        gw.wait(
            &node,
            |n| n["status"] == "ONLINE" && n["gpus"][0]["observed_at"].is_string(),
            "telemetry",
        )
        .await;

        // Hot GPU while heartbeats stay fresh: DEGRADED, then ONLINE once it cools.
        a.hw.update_sample(0, |s| s.temperature_c = Some(95));
        let n = gw.wait_status(&node, "DEGRADED").await;
        assert_eq!(n["status_reason"], "health:gpu_hot");
        a.hw.update_sample(0, |s| s.temperature_c = Some(40));
        gw.wait_status(&node, "ONLINE").await;

        // Out of order: the newer reading is written first, the older one after it.
        let ts = |secs: i64, nanos: i32| {
            Some(prost_types::Timestamp {
                seconds: secs,
                nanos,
            })
        };
        let now = time::OffsetDateTime::now_utc().unix_timestamp() + 60;
        let reading = |t, temp, ram| mrl_proto::agent::v1::NodeTelemetry {
            time: t,
            ram_used_bytes: ram,
            gpus: vec![mrl_proto::agent::v1::GpuSample {
                index: 0,
                temperature_c: temp,
                ..Default::default()
            }],
            ..Default::default()
        };
        let id = uuid_of(&node);
        let epoch: i64 =
            sqlx::query_scalar("SELECT session_epoch FROM compute_nodes WHERE id = $1")
                .bind(id)
                .fetch_one(gw.db.pool())
                .await
                .unwrap();
        let ahead = time::Duration::minutes(5);
        let save = |t: mrl_proto::agent::v1::NodeTelemetry| {
            let db = gw.db.db().clone();
            async move { store::telemetry(&db, id, epoch, &t, 90, ahead).await }
        };
        assert!(save(reading(ts(now, 500_000_000), 51, 2))
            .await
            .unwrap()
            .is_some());
        assert!(
            save(reading(ts(now, 100_000_000), 77, 1))
                .await
                .unwrap()
                .is_none(),
            "older: not saved, so no sink sees it"
        );
        let n = gw.node(&node).await;
        assert_eq!(n["gpus"][0]["temperature_c"], 51);
        assert_eq!(n["telemetry"]["ram_used_bytes"], 2);
        // A clock a day ahead can't hold back the readings after it: that reading counts as
        // taken now (older than the stored one here), and the next real one is saved.
        assert!(save(reading(ts(now + 86_400, 0), 60, 3))
            .await
            .unwrap()
            .is_none());
        assert!(save(reading(ts(now + 120, 0), 61, 4))
            .await
            .unwrap()
            .is_some());
        assert_eq!(gw.node(&node).await["telemetry"]["ram_used_bytes"], 4);
        let later = reading(ts(now + 300, 0), 62, 5);
        let other_session = store::telemetry(gw.db.db(), id, epoch + 7, &later, 90, ahead).await;
        assert!(
            other_session.unwrap().is_none(),
            "only the current session writes"
        );
        link.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs PostgreSQL (just test-integration)"]
    async fn hosts_with_separate_folders_share_the_ca_through_the_sealed_store() {
        let db = TestDb::new().await;
        let kek = "ab".repeat(32);
        let start = |dir: std::path::PathBuf, kek: String| {
            let db = db.db().clone();
            async move {
                let cfg = server_config(
                    GatewayConfig {
                        secrets_dir: Some(dir),
                        ..fast_config()
                    },
                    Mode::Solo,
                    0,
                );
                let m = NodesModule::from_config_with_kek(db, &cfg, Some(&kek));
                let gw = m.gateway();
                let seams = crate::seams::build(crate::seams::solo(TEST_SECRET), Mode::Solo);
                TestServer::start(cfg, seams, vec![Box::new(m)])
                    .await
                    .map(|s| (s, gw))
            }
        };
        let (h1, h2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (s1, g1) = start(h1.path().to_path_buf(), kek.clone()).await.unwrap();
        let (s2, g2) = start(h2.path().to_path_buf(), kek.clone()).await.unwrap();
        assert_eq!(
            g1.ca_fingerprint(),
            g2.ca_fingerprint(),
            "one CA for both hosts"
        );
        assert_eq!(
            std::fs::read_dir(h1.path()).map(|d| d.count()).unwrap_or(0),
            0,
            "nothing in the local folder"
        );
        // Both hosts stop before the next start: fewer connections at once on the shared server.
        s1.shutdown().await.unwrap();
        s2.shutdown().await.unwrap();
        let e = match start(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            "cd".repeat(32),
        )
        .await
        {
            Ok(_) => panic!("another KEK must not start"),
            Err(e) => e,
        };
        assert!(
            e.detail.contains("key-encryption key differs"),
            "{}",
            e.detail
        );
    }
}
