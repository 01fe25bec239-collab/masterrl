//! A core wired like production: P16's seams (`production_seams`) and every registered
//! module, on its own [`TestDb`], signing in through a [`MockSupabase`]. Secrets are in
//! memory so tests can share them across a "wiped" data folder.
//!
//! ```text
//! let sb = MockSupabase::start().await;
//! let core = CoreServer::start(Mode::Solo, &sb, TestDb::new().await, secrets).await;
//! let me = core.call(Method::GET, "/api/v1/auth/me", &Auth::Launch, None).await;
//! ```

use super::MockSupabase;
use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use axum::body::Body;
use axum::http::Method;
use mrl_server::config::{Config, Mode};
use mrl_server::seams::{SecretStore, LAUNCH_SECRET_HEADER};
use mrl_server::{registry, ServerError};
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

/// How a test request signs in.
#[derive(Debug, Clone)]
pub enum Auth {
    /// No credentials.
    None,
    /// The solo launch secret.
    Launch,
    /// `Authorization: Bearer …` (an access token or a Supabase JWT).
    Bearer(String),
    /// The renderer session cookie, with the CSRF header or `Sec-Fetch-Site: same-origin`.
    Cookie {
        /// Cookie value.
        cookie: String,
        /// `x-csrf-token`.
        csrf: Option<String>,
        /// Send `Sec-Fetch-Site: same-origin`.
        same_origin: bool,
    },
}

/// A production-wired core for tests.
pub struct CoreServer {
    /// The running server.
    pub server: TestServer,
    /// Its database.
    pub db: TestDb,
    /// Its secret store (stands in for the OS keychain).
    pub secrets: Arc<InMemorySecretStore>,
    /// Its settings.
    pub config: Config,
}

/// Settings for a test core in `mode` that signs in through `sb`.
pub fn config(mode: Mode, sb: &MockSupabase) -> Config {
    let mut c = Config::default();
    c.server.mode = mode;
    let any = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    c.server.listeners.api = any;
    c.server.listeners.grpc = any;
    c.server.listeners.internal = any;
    c.auth = sb.auth_config();
    c.database = crate::pg::test_config();
    c
}

impl CoreServer {
    /// Starts a core. Panics with the catalogued error if it does not start.
    pub async fn start(
        mode: Mode,
        sb: &MockSupabase,
        db: TestDb,
        secrets: Arc<InMemorySecretStore>,
    ) -> Self {
        match Self::try_start(config(mode, sb), db, secrets).await {
            Ok(c) => c,
            Err(e) => panic!("test core did not start: {}", e.explain()),
        }
    }

    /// Starts a core with these settings.
    pub async fn try_start(
        config: Config,
        db: TestDb,
        secrets: Arc<InMemorySecretStore>,
    ) -> Result<Self, ServerError> {
        let store: Arc<dyn SecretStore> = secrets.clone();
        let seams =
            mrl_server::modules::auth::production_seams(&config, db.db(), store, TEST_SECRET)
                .await?
                .build(config.server.mode)?;
        let modules = registry::modules(db.db(), &config);
        let server = TestServer::start(config.clone(), seams, modules).await?;
        Ok(Self {
            server,
            db,
            secrets,
            config,
        })
    }

    /// Stops the server and closes its connections, so dropping the database is quick.
    /// Call it at the end of a test.
    pub async fn stop(self) {
        if let Err(e) = self.server.shutdown().await {
            panic!("test core did not stop cleanly: {}", e.explain());
        }
        self.db.pool().close().await;
    }

    /// Sends a JSON request (or none) with `auth`.
    pub async fn call(
        &self,
        method: Method,
        path: &str,
        auth: &Auth,
        body: Option<Value>,
    ) -> TestResponse {
        let bearer;
        let cookie;
        let mut headers: Vec<(&str, &str)> = vec![("content-type", "application/json")];
        match auth {
            Auth::None => {}
            Auth::Launch => headers.push((LAUNCH_SECRET_HEADER, TEST_SECRET)),
            Auth::Bearer(t) => {
                bearer = format!("Bearer {t}");
                headers.push(("authorization", &bearer));
            }
            Auth::Cookie {
                cookie: c,
                csrf,
                same_origin,
            } => {
                cookie = format!("mrl_session={c}");
                headers.push(("cookie", &cookie));
                if let Some(t) = csrf {
                    headers.push(("x-csrf-token", t));
                }
                if *same_origin {
                    headers.push(("sec-fetch-site", "same-origin"));
                }
            }
        }
        let body = match body {
            Some(v) => Body::from(v.to_string()),
            None => Body::empty(),
        };
        self.server.request(method, path, &headers, body).await
    }

    /// Signs in through the mock (the browser step included) and returns the callback answer.
    /// `issue_token` asks for a CLI access token.
    pub async fn sign_in(&self, sb: &MockSupabase, auth: &Auth, issue_token: bool) -> TestResponse {
        let started = self
            .call(
                Method::POST,
                "/api/v1/auth/login",
                auth,
                Some(serde_json::json!({ "provider": "github", "redirect_uri": "http://127.0.0.1:53111/callback" })),
            )
            .await;
        assert_eq!(started.status, 200, "login: {}", started.text());
        let started = started.json();
        let back = match sb.approve(started["authorize_url"].as_str().unwrap_or_default()) {
            Ok(b) => b,
            Err(e) => panic!("mock refused the authorize URL: {e}"),
        };
        let q: std::collections::HashMap<String, String> = match reqwest::Url::parse(&back) {
            Ok(u) => u.query_pairs().into_owned().collect(),
            Err(e) => panic!("bad redirect {back}: {e}"),
        };
        assert_eq!(
            q.get("flow").map(String::as_str),
            started["flow_id"].as_str()
        );
        self.call(
            Method::POST,
            "/api/v1/auth/callback",
            auth,
            Some(serde_json::json!({
                "flow_id": q.get("flow"), "code": q.get("code"), "issue_token": issue_token, "token_name": "test CLI",
            })),
        )
        .await
    }
}
