//! P27 integration tests: dataset routes on a real team core, job outputs (the version
//! folders in `tests/fixtures/p27/`, written by the real Python tasks) recorded as READY or
//! INVALID, record browsing, diffs, exports, the registry lookup and the launch contributor.
//!
//! Compiled by `mrl-testkit` (see its `lib.rs`, P27 marker), not by this crate.
//! `#[ignore]`d; run them with `just dev && just test-integration`.

use crate::pg::TestDb;
use crate::supabase::core::{config, Auth, CoreServer};
use crate::supabase::{MockSupabase, MockUser};
use axum::http::Method;
use mrl_domain::ids::{DatasetId, DatasetVersionId, OrgId, ProjectId, UserId, WorkspaceId};
use mrl_domain::rbac::Role;
use mrl_server::config::Mode;
use mrl_server::modules::artifacts::{ArtifactStore, ArtifactsModule};
use mrl_server::modules::audit::PgAuditSink;
use mrl_server::modules::checkpoints::service::storage_org;
use mrl_server::modules::datasets::launch::DatasetInputs;
use mrl_server::modules::datasets::records::DatasetRecords;
use mrl_server::modules::datasets::store::{self, Checked, Outputs};
use mrl_server::modules::engines::{RecordScope, RegistryRecords};
use mrl_server::modules::jobs::JobTransitions;
use mrl_server::modules::scheduler::pipeline::{
    LaunchContributor, LaunchError, LaunchPlan, Placement,
};
use mrl_server::modules::tenancy::{bind_role, Scope};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/p27")
        .join(name)
}

fn fixture_manifest(name: &str) -> Value {
    let root = fixture(name);
    let mut files = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let rel = p
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if rel != ".gitattributes" {
                let bytes = std::fs::read(&p).unwrap();
                files.push(json!({"path":rel,"size":bytes.len(),
                    "sha256":mrl_storage::sha256_hex(&bytes),"role":"data"}));
            }
        }
    }
    json!({"schema_version":"artifact-manifest/v1","kind":"dataset",
           "created_at":"2026-11-27T00:00:00Z","files":files})
}

struct Env {
    core: CoreServer,
    store: Arc<ArtifactStore>,
    org: OrgId,
    prj: ProjectId,
    /// Another project of the same org.
    side: ProjectId,
    /// A project of another org.
    far: ProjectId,
    far_org: OrgId,
    /// Owner of `org`.
    owner: Auth,
    /// Owner of the other org.
    outsider: Auth,
    /// Viewer of `org`.
    viewer: Auth,
    _sb: MockSupabase,
    _objects: tempfile::TempDir,
}

async fn person(core: &CoreServer, sb: &MockSupabase, name: &str, org: OrgId, role: Role) -> Auth {
    let who = MockUser::new(&format!("{name}@example.com"), name);
    let auth = Auth::Bearer(sb.access_token(&who, 600));
    assert_eq!(
        core.call(Method::GET, "/api/v1/auth/me", &auth, None)
            .await
            .status,
        200
    );
    let id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE supabase_user_id = $1")
        .bind(who.id)
        .fetch_one(core.db.pool())
        .await
        .unwrap();
    let mut tx = core.db.db().begin().await.unwrap();
    bind_role(&mut tx, org, UserId::from_uuid(id), role, Scope::Org)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    auth
}

impl Env {
    async fn start() -> Self {
        Self::start_mode(Mode::Team).await
    }

    async fn start_mode(mode: Mode) -> Self {
        let sb = MockSupabase::start().await;
        let objects = tempfile::tempdir().unwrap();
        let mut cfg = config(mode, &sb);
        // These fixtures supply worker outputs explicitly, with no scheduler or real node.
        cfg.server
            .roles
            .retain(|role| *role != mrl_server::config::Role::Scheduler);
        cfg.object_store.url = Some(
            reqwest::Url::from_directory_path(objects.path())
                .unwrap()
                .into(),
        );
        let core = CoreServer::try_start(cfg.clone(), TestDb::new().await, Arc::default())
            .await
            .unwrap_or_else(|e| panic!("{}", e.explain()));
        let store = ArtifactsModule::new(core.db.db().clone(), &cfg).store();
        let (org, far_org) = (OrgId::new(), OrgId::new());
        let org = if mode == Mode::Solo {
            let id: Uuid = sqlx::query_scalar("SELECT id FROM organizations WHERE kind='personal'")
                .fetch_one(core.db.pool())
                .await
                .unwrap();
            OrgId::from_uuid(id)
        } else {
            org
        };
        let (prj, side, far) = (ProjectId::new(), ProjectId::new(), ProjectId::new());
        for (o, projects) in [(org, vec![prj, side]), (far_org, vec![far])] {
            let (o, w) = (o.as_uuid(), WorkspaceId::new().as_uuid());
            let mut sql = format!(
                "INSERT INTO organizations (id, slug, name, kind) VALUES ('{o}', '{o}', 'Lab', 'team') ON CONFLICT (id) DO NOTHING;
                 INSERT INTO workspaces (id, org_id, slug, name) VALUES ('{w}', '{o}', 'main', 'Main');"
            );
            for p in projects {
                let p = p.as_uuid();
                sql.push_str(&format!(
                    "INSERT INTO projects (id, org_id, workspace_id, slug, name)
                     VALUES ('{p}', '{o}', '{w}', '{p}', 'Project');"
                ));
            }
            core.db.seed(&sql).await;
        }
        let (owner, outsider, viewer) = if mode == Mode::Solo {
            (Auth::Launch, Auth::None, Auth::None)
        } else {
            (
                person(&core, &sb, "owner", org, Role::Owner).await,
                person(&core, &sb, "outsider", far_org, Role::Owner).await,
                person(&core, &sb, "viewer", org, Role::Viewer).await,
            )
        };
        Self {
            core,
            store,
            org,
            prj,
            side,
            far,
            far_org,
            owner,
            outsider,
            viewer,
            _sb: sb,
            _objects: objects,
        }
    }

    async fn call(
        &self,
        auth: &Auth,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let r = self
            .core
            .call(method, &format!("/api/v1{path}"), auth, body)
            .await;
        (r.status, r.json())
    }

    /// A dataset made over the API in `prj`.
    async fn dataset(&self, name: &str, body: Value) -> String {
        let mut b = json!({"name": name});
        b.as_object_mut()
            .unwrap()
            .extend(body.as_object().unwrap().clone());
        let (s, d) = self
            .call(
                &self.owner,
                Method::POST,
                &format!("/projects/{}/datasets", self.prj),
                Some(b),
            )
            .await;
        assert_eq!(s, 201, "{d}");
        d["id"].as_str().unwrap().to_owned()
    }

    /// A new VALIDATING version of `ds`, as a job would find it.
    async fn pending(&self, ds: &str, parent: Option<Uuid>) -> Uuid {
        let id = DatasetVersionId::new().as_uuid();
        let ds: DatasetId = ds.parse().unwrap();
        sqlx::query(
            "INSERT INTO dataset_versions (id, org_id, dataset_id, seq, schema_id, state, source_kind, \
               parent_version_id, created_by) \
             SELECT $1, org_id, id, next_seq, 'sft.messages.v1', 'VALIDATING', \
               CASE WHEN $3::uuid IS NULL THEN 'upload' ELSE 'transform' END, $3, 'test' \
               FROM datasets WHERE id = $2; \
             ",
        )
        .bind(id)
        .bind(ds.as_uuid())
        .bind(parent)
        .execute(self.core.db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE datasets SET next_seq = next_seq + 1 WHERE id = $1")
            .bind(ds.as_uuid())
            .execute(self.core.db.pool())
            .await
            .unwrap();
        id
    }

    /// Puts the fixture's checked blobs in the org's storage for tests that start after registration.
    async fn publish(&self, name: &str) -> Value {
        let manifest = fixture_manifest(name);
        let st = self
            .store
            .org_store(&storage_org(self.org.as_uuid()))
            .await
            .unwrap();
        for file in manifest["files"].as_array().unwrap() {
            let bytes = std::fs::read(fixture(name).join(file["path"].as_str().unwrap())).unwrap();
            let key = st.keys().blob(file["sha256"].as_str().unwrap()).unwrap();
            st.put(&key, bytes.into()).await.unwrap();
        }
        manifest
    }

    /// Reads the outputs and records the decision, like the verifier does.
    async fn decide(&self, version: Uuid, manifest: &Value) -> (Checked, bool) {
        let st = self
            .store
            .org_store(&storage_org(self.org.as_uuid()))
            .await
            .unwrap();
        let outputs = Outputs::new(Uuid::now_v7(), manifest).unwrap();
        let checked = store::read_outputs(&st, &outputs, "sft.messages.v1")
            .await
            .unwrap();
        let mut tx = self.core.db.db().begin().await.unwrap();
        let changed = store::record_version(
            &mut tx,
            &PgAuditSink::new(self.core.db.db().clone()),
            self.core.config.server.mode,
            version,
            &checked,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        (checked, changed)
    }

    async fn ready(&self, ds: &str, fixture: &str, parent: Option<Uuid>) -> Uuid {
        let v = self.pending(ds, parent).await;
        let (checked, changed) = self.decide(v, &self.publish(fixture).await).await;
        assert!(matches!(checked, Checked::Ready { .. }), "{checked:?}");
        assert!(changed);
        v
    }
}

fn dsv(id: Uuid) -> String {
    DatasetVersionId::from_uuid(id).to_string()
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn datasets_are_made_named_changed_and_deleted_inside_their_project() {
    let env = Env::start().await;
    let list = format!("/projects/{}/datasets", env.prj);
    let body = json!({"name": "chats", "license": "MIT", "sensitivity": "confidential",
        "permitted_use": ["research"], "source_urls": ["https://example.com/chats"],
        "card_md": "# Chats"});
    let r = env
        .core
        .call(
            Method::POST,
            &format!("/api/v1{list}"),
            &env.owner,
            Some(body.clone()),
        )
        .await;
    assert_eq!(r.status, 201, "{}", r.text());
    let d = r.json();
    let ds = d["id"].as_str().unwrap().to_owned();
    assert_eq!(
        r.headers["location"],
        format!("/api/v1/datasets/{ds}").as_str()
    );
    assert_eq!(
        (d["license"].clone(), d["sensitivity"].clone()),
        (json!("MIT"), json!("confidential"))
    );
    assert_eq!(
        d["policy"],
        json!({"export_allowed": true, "hub_push_allowed": true, "sample_logging": true})
    );

    // The same name again is MRL-CONFLICT-270; another project may use it.
    let (s, e) = env
        .call(&env.owner, Method::POST, &list, Some(body.clone()))
        .await;
    assert_eq!(
        (s, e["code"].as_str()),
        (409, Some("MRL-CONFLICT-270")),
        "{e}"
    );
    let (s, _) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/projects/{}/datasets", env.side),
            Some(body),
        )
        .await;
    assert_eq!(s, 201);

    // Bad values are named; unknown fields are refused.
    for bad in [
        json!({"name": "x", "sensitivity": "secret"}),
        json!({"name": "x", "permitted_use": ["everything"]}),
        json!({"name": ""}),
        json!({"name": "x", "colour": "red"}),
    ] {
        let (s, e) = env
            .call(&env.owner, Method::POST, &list, Some(bad.clone()))
            .await;
        assert!(s == 422 || s == 400, "{bad}: {s} {e}");
    }

    let (s, page) = env.call(&env.viewer, Method::GET, &list, None).await;
    assert_eq!(s, 200);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    let path = format!("/datasets/{ds}");
    let (s, d) = env
        .call(
            &env.owner,
            Method::PATCH,
            &path,
            Some(json!({"sensitivity": "restricted-pii", "eval_set": true})),
        )
        .await;
    assert_eq!(s, 200, "{d}");
    assert_eq!(
        d["policy"],
        json!({"export_allowed": false, "hub_push_allowed": false, "sample_logging": false})
    );
    assert_eq!(d["eval_set"], true);
    // A viewer reads but doesn't change.
    let (s, _) = env
        .call(
            &env.viewer,
            Method::PATCH,
            &path,
            Some(json!({"description": "x"})),
        )
        .await;
    assert_eq!(s, 403);

    // Another org sees nothing: every route is the same 404.
    for (m, p, b) in [
        (Method::GET, path.clone(), None),
        (
            Method::PATCH,
            path.clone(),
            Some(json!({"description": "x"})),
        ),
        (Method::DELETE, path.clone(), None),
        (Method::GET, format!("{path}/versions"), None),
        (Method::GET, list.clone(), None),
        (Method::POST, list.clone(), Some(json!({"name": "mine"}))),
    ] {
        let (s, e) = env.call(&env.outsider, m.clone(), &p, b).await;
        assert_eq!(s, 404, "{m} {p}: {e}");
    }

    // Deleting frees the name; deleting again is 204 again.
    assert_eq!(
        env.call(&env.owner, Method::DELETE, &path, None).await.0,
        204
    );
    assert_eq!(
        env.call(&env.owner, Method::DELETE, &path, None).await.0,
        204
    );
    assert_eq!(env.call(&env.owner, Method::GET, &path, None).await.0, 404);
    let (s, _) = env
        .call(
            &env.owner,
            Method::POST,
            &list,
            Some(json!({"name": "chats"})),
        )
        .await;
    assert_eq!(s, 201);

    // Every change is audited once, in its own transaction.
    let audited: Vec<String> = sqlx::query_scalar(
        "SELECT action FROM audit_events WHERE action LIKE 'datasets.%' AND outcome = 'success' ORDER BY at",
    )
    .fetch_all(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(
        audited,
        [
            "datasets.create",
            "datasets.create",
            "datasets.update",
            "datasets.delete",
            "datasets.create"
        ]
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn job_outputs_make_a_ready_version_once_and_the_same_bytes_the_same_hash() {
    let env = Env::start().await;
    let ds = env.dataset("chats", json!({})).await;
    let manifest = env.publish("ready").await;
    let v = env.pending(&ds, None).await;
    let (checked, changed) = env.decide(v, &manifest).await;
    assert!(changed);
    // A replayed decision changes nothing.
    let mut tx = env.core.db.db().begin().await.unwrap();
    assert!(!store::record_version(
        &mut tx,
        &PgAuditSink::new(env.core.db.db().clone()),
        Mode::Team,
        v,
        &checked
    )
    .await
    .unwrap());
    tx.commit().await.unwrap();

    let (s, got) = env
        .call(
            &env.viewer,
            Method::GET,
            &format!("/dataset-versions/{}", dsv(v)),
            None,
        )
        .await;
    assert_eq!(s, 200, "{got}");
    assert_eq!(
        (got["state"].as_str(), got["row_count"].as_i64()),
        (Some("READY"), Some(200))
    );
    assert_eq!(got["splits"], json!({"train": 200}));
    let files = got["files"].as_array().unwrap();
    assert!(files
        .iter()
        .any(|f| f["role"] == "shard" && f["rows"] == 200));
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox_events WHERE type = 'dataset_version.ready' AND aggregate_id = $1",
    )
    .bind(v)
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(events, 1);

    // The same bytes again: the same content hash, and no new objects.
    let again = env.ready(&ds, "ready", None).await;
    let hashes: Vec<Option<Vec<u8>>> = sqlx::query_scalar(
        "SELECT content_sha256 FROM dataset_versions WHERE id = ANY($1) ORDER BY seq",
    )
    .bind(vec![v, again])
    .fetch_all(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(hashes[0], hashes[1]);
    assert!(hashes[0].is_some());
    let (all, distinct): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(DISTINCT object_key) FROM dataset_files WHERE dataset_version_id = ANY($1)",
    )
    .bind(vec![v, again])
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(
        all,
        2 * distinct,
        "the second version points at the same objects"
    );

    // Validation and stats are served from what the job wrote.
    let (s, val) = env
        .call(
            &env.viewer,
            Method::GET,
            &format!("/dataset-versions/{}/validation", dsv(v)),
            None,
        )
        .await;
    assert_eq!(s, 200);
    assert_eq!(val["report"]["valid"], 200);
    let (s, stats) = env
        .call(
            &env.viewer,
            Method::GET,
            &format!("/dataset-versions/{}/stats", dsv(v)),
            None,
        )
        .await;
    assert_eq!(s, 200, "{stats}");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn invalid_or_broken_outputs_record_a_reason() {
    let env = Env::start().await;
    let ds = env.dataset("chats", json!({})).await;
    let v = env.pending(&ds, None).await;
    let (checked, _) = env.decide(v, &env.publish("invalid").await).await;
    assert!(matches!(checked, Checked::Invalid { .. }));
    let (_, got) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/dataset-versions/{}", dsv(v)),
            None,
        )
        .await;
    assert_eq!(got["state"], "INVALID");
    assert_eq!(got["state_reason"]["code"], "MRL-DATA-001");
    assert!(got["state_reason"]["message"]
        .as_str()
        .unwrap()
        .contains("do not match"));
    let (_, val) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/dataset-versions/{}/validation", dsv(v)),
            None,
        )
        .await;
    let examples = val["report"]["examples"].as_array().unwrap();
    assert!(!examples.is_empty() && examples.iter().all(|e| e["message"].is_string()));
    // Not READY: stats, records, diff and export are refused with MRL-DATA-002.
    for (m, p, b) in [
        (
            Method::GET,
            format!("/dataset-versions/{}/stats", dsv(v)),
            None,
        ),
        (
            Method::GET,
            format!("/dataset-versions/{}/records", dsv(v)),
            None,
        ),
        (
            Method::POST,
            format!("/dataset-versions/{}:export", dsv(v)),
            Some(json!({"format": "parquet"})),
        ),
    ] {
        let (s, e) = env.call(&env.owner, m, &p, b).await;
        assert_eq!(
            (s, e["code"].as_str()),
            (409, Some("MRL-DATA-002")),
            "{p}: {e}"
        );
    }

    let mut duplicated = env.publish("ready").await;
    let mut summary: Value =
        serde_json::from_slice(&std::fs::read(fixture("ready/dataset.json")).unwrap()).unwrap();
    let shard = summary["shards"][0].clone();
    summary["shards"].as_array_mut().unwrap().push(shard);
    let body = serde_json::to_vec(&summary).unwrap();
    let hash = mrl_storage::sha256_hex(&body);
    let storage = env.store.org_store(&env.org.to_string()).await.unwrap();
    let bytes = body.len();
    storage
        .put(&storage.keys().blob(&hash).unwrap(), body.into())
        .await
        .unwrap();
    let entry = duplicated["files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|f| f["path"] == "dataset.json")
        .unwrap();
    entry["sha256"] = json!(hash);
    entry["size"] = json!(bytes);
    let pending = env.pending(&ds, None).await;
    let (checked, _) = env.decide(pending, &duplicated).await;
    assert!(matches!(checked, Checked::Invalid {code, reason, ..}
        if code == "MRL-DATA-273" && reason.contains("listed twice")));

    // Outputs whose files don't match their list are MRL-DATA-273, not READY.
    let mut tampered = env.publish("ready").await;
    for f in tampered["files"].as_array_mut().unwrap() {
        if f["path"] == "dataset.json" {
            f["sha256"] = json!("0".repeat(64));
        }
    }
    let b = env.pending(&ds, None).await;
    env.decide(b, &tampered).await;
    let (_, got) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/dataset-versions/{}", dsv(b)),
            None,
        )
        .await;
    assert_eq!(
        (got["state"].as_str(), got["state_reason"]["code"].as_str()),
        (Some("INVALID"), Some("MRL-DATA-273"))
    );
    let mut missing = env.publish("ready").await;
    missing["files"]
        .as_array_mut()
        .unwrap()
        .retain(|f| !f["path"].as_str().unwrap().starts_with("shards/"));
    let c = env.pending(&ds, None).await;
    env.decide(c, &missing).await;
    let (_, got) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/dataset-versions/{}", dsv(c)),
            None,
        )
        .await;
    assert_eq!(got["state_reason"]["code"], "MRL-DATA-273", "{got}");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn records_page_and_filter_and_a_transform_diffs_against_its_parent() {
    let env = Env::start().await;
    let ds = env.dataset("chats", json!({})).await;
    let parent = env.ready(&ds, "ready", None).await;
    let child = env.ready(&ds, "filtered", Some(parent)).await;

    // Paging walks every record once, in order, across browse pages.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let q = cursor
            .as_ref()
            .map_or(String::new(), |c| format!("&cursor={c}"));
        let (s, page) = env
            .call(
                &env.viewer,
                Method::GET,
                &format!("/dataset-versions/{}/records?limit=70{q}", dsv(parent)),
                None,
            )
            .await;
        assert_eq!(s, 200, "{page}");
        seen.extend(
            page["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|i| i["index"].as_u64().unwrap()),
        );
        match page["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_owned()),
            None => break,
        }
    }
    assert_eq!(seen, (0..200).collect::<Vec<u64>>());
    // A filter keeps the matching records: the same ones the transform kept.
    let (s, page) = env
        .call(
            &env.viewer,
            Method::GET,
            &format!(
                "/dataset-versions/{}/records?limit=100&filter=len(messages)%20%3E%202",
                dsv(parent)
            ),
            None,
        )
        .await;
    assert_eq!(s, 200);
    let items = page["items"].as_array().unwrap();
    assert!(items
        .iter()
        .all(|i| i["record"]["messages"].as_array().unwrap().len() > 2));
    let (s, e) = env
        .call(
            &env.viewer,
            Method::GET,
            &format!("/dataset-versions/{}/records?filter=len(", dsv(parent)),
            None,
        )
        .await;
    assert_eq!((s, e["code"].as_str()), (400, Some("MRL-VAL-006")), "{e}");
    let (s, _) = env
        .call(
            &env.viewer,
            Method::GET,
            &format!("/dataset-versions/{}/records?limit=500", dsv(parent)),
            None,
        )
        .await;
    assert_eq!(s, 400);

    // Lineage: the child derives from its parent.
    let edge: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lineage_edges WHERE from_kind = 'dsv' AND from_id = $1 \
           AND relation = 'derived_from' AND to_kind = 'dsv' AND to_id = $2",
    )
    .bind(child)
    .bind(parent)
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(edge, 1);
    let (status, graph) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/lineage/{}", dsv(child)),
            None,
        )
        .await;
    assert_eq!(status, 200, "{graph}");
    assert!(graph["edges"].as_array().unwrap().contains(&json!({
        "from": dsv(child),
        "to": dsv(parent),
        "relation": "derived_from"
    })));

    let (s, diff) = env
        .call(
            &env.viewer,
            Method::POST,
            &format!("/dataset-versions/{}:diff", dsv(parent)),
            Some(json!({"against": dsv(child)})),
        )
        .await;
    assert_eq!(s, 200, "{diff}");
    let kept = diff["against"]["row_count"].as_i64().unwrap();
    assert_eq!(kept, 80);
    assert_eq!(diff["row_delta"], kept - 200);
    assert_eq!(diff["records"]["added"], 0);
    assert_eq!(diff["records"]["removed"], 200 - kept);
    assert_eq!(diff["records"]["unchanged"], kept);
    assert_eq!(diff["same_schema"], true);
    assert_eq!(diff["same_content"], false);
    assert!(!diff["base_sample"].as_array().unwrap().is_empty());
    // A version of another org is "no such version".
    let far = env.outsider_version().await;
    let (s, _) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/dataset-versions/{}:diff", dsv(parent)),
            Some(json!({"against": dsv(far)})),
        )
        .await;
    assert_eq!(s, 404);
    let (s, _) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/dataset-versions/{}:nope", dsv(parent)),
            Some(json!({})),
        )
        .await;
    assert_eq!(s, 404);
}

impl Env {
    /// A READY version in the other org (its data never leaves it).
    async fn outsider_version(&self) -> Uuid {
        let (d, v) = (Uuid::now_v7(), Uuid::now_v7());
        self.core
            .db
            .seed(&format!(
                "INSERT INTO datasets (id, org_id, project_id, name, created_by)
                 VALUES ('{d}', '{o}', '{p}', 'theirs', 'seed');
                 INSERT INTO dataset_versions (id, org_id, dataset_id, seq, schema_id, state,
                   source_kind, row_count, content_sha256, created_by, ready_at)
                 VALUES ('{v}', '{o}', '{d}', 1, 'sft.messages.v1', 'READY', 'upload', 1,
                   decode(repeat('11', 32), 'hex'), 'seed', now());",
                o = self.far_org.as_uuid(),
                p = self.far.as_uuid()
            ))
            .await;
        v
    }
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn exports_hand_out_shard_links_unless_the_data_is_restricted() {
    let env = Env::start().await;
    let ds = env.dataset("chats", json!({})).await;
    let v = env.ready(&ds, "ready", None).await;
    let path = format!("/dataset-versions/{}:export", dsv(v));
    let (s, out) = env
        .call(
            &env.owner,
            Method::POST,
            &path,
            Some(json!({"format": "parquet"})),
        )
        .await;
    assert_eq!(s, 200, "{out}");
    let files = out["files"].as_array().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0]["path"], "shards/train-00000.parquet");
    let shard = std::fs::read(fixture("ready/shards/train-00000.parquet")).unwrap();
    assert_eq!(files[0]["sha256"], mrl_storage::sha256_hex(&shard));
    assert!(files[0]["url"].as_str().is_some_and(|u| !u.is_empty()));
    let (s, _) = env
        .call(
            &env.owner,
            Method::POST,
            &path,
            Some(json!({"format": "csv"})),
        )
        .await;
    assert!(s == 422 || s == 400);
    // A viewer may read but not export.
    let (s, _) = env
        .call(
            &env.viewer,
            Method::POST,
            &path,
            Some(json!({"format": "parquet"})),
        )
        .await;
    assert_eq!(s, 403);
    // Restricted personal data never leaves: MRL-PERM-270.
    let (s, _) = env
        .call(
            &env.owner,
            Method::PATCH,
            &format!("/datasets/{ds}"),
            Some(json!({"sensitivity": "restricted-pii"})),
        )
        .await;
    assert_eq!(s, 200);
    let (s, e) = env
        .call(
            &env.owner,
            Method::POST,
            &path,
            Some(json!({"format": "parquet"})),
        )
        .await;
    assert_eq!((s, e["code"].as_str()), (403, Some("MRL-PERM-270")), "{e}");
    for format in ["jsonl", "hub"] {
        let (status, error) = env
            .call(
                &env.owner,
                Method::POST,
                &path,
                Some(json!({"format":format})),
            )
            .await;
        assert_eq!(status, 403, "{error}");
        assert_eq!(error["code"], "MRL-PERM-270");
    }
    let refused: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_events WHERE action = 'datasets.export' AND outcome = 'success'",
    )
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(
        refused, 1,
        "only the allowed export is audited as a success"
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn the_registry_knows_only_ready_versions_of_the_asking_project() {
    let env = Env::start().await;
    let ds = env.dataset("chats", json!({})).await;
    let v = env.ready(&ds, "ready", None).await;
    let pending = env.pending(&ds, None).await;
    let records = DatasetRecords::new(env.core.db.db().clone());
    let scope = |org: OrgId, p: ProjectId| RecordScope {
        org_id: org.as_uuid(),
        project_id: p.as_uuid(),
    };
    let id = DatasetVersionId::from_uuid(v);
    let facts = records
        .dataset(&scope(env.org, env.prj), id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (facts.schema.as_str(), facts.rows),
        ("sft.messages.v1", Some(200))
    );
    for (s, which) in [
        (scope(env.org, env.side), id),
        (scope(env.far_org, env.far), id),
        (scope(env.far_org, env.prj), id),
        (
            scope(env.org, env.prj),
            DatasetVersionId::from_uuid(pending),
        ),
        (scope(env.org, env.prj), DatasetVersionId::new()),
    ] {
        assert!(records.dataset(&s, which).await.unwrap().is_none());
    }
    let ds_id: DatasetId = ds.parse().unwrap();
    sqlx::query("UPDATE datasets SET deleted_at = now() WHERE id = $1")
        .bind(ds_id.as_uuid())
        .execute(env.core.db.pool())
        .await
        .unwrap();
    assert!(records
        .dataset(&scope(env.org, env.prj), id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn runs_and_probes_stage_each_dataset_version_as_one_checked_folder() {
    let env = Env::start().await;
    let ds = env.dataset("chats", json!({})).await;
    let first = env.ready(&ds, "ready", None).await;
    let second = env.ready(&ds, "filtered", Some(first)).await;
    let (o, p) = (env.org.as_uuid(), env.prj.as_uuid());
    let (e, r, j, a) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    let seed = |versions: &[Uuid]| {
        let list = versions
            .iter()
            .map(|v| format!("'{v}'"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "INSERT INTO experiments (id, org_id, project_id, name, created_by)
               VALUES ('{e}', '{o}', '{p}', 'Experiment', 'seed');
             INSERT INTO runs (id, org_id, project_id, experiment_id, name, kind, algorithm_id,
                 base_model_version_id, submit_key, submit_fingerprint, created_by)
               VALUES ('{r}', '{o}', '{p}', '{e}', 'Run', 'TRAIN', 'sft', '{a}', '{r}', '', 'seed');
             INSERT INTO run_configs (run_id, org_id, submitted, resolved, resolved_sha256,
                 dataset_version_ids, seeds)
               VALUES ('{r}', '{o}', '{{}}', '{{}}', decode(repeat('00', 32), 'hex'),
                 ARRAY[{list}]::uuid[], '{{}}');
             INSERT INTO jobs (id, org_id, project_id, kind, run_id, state, resource_request,
                 retry_policy, created_by)
               VALUES ('{j}', '{o}', '{p}', 'TRAIN', '{r}', 'QUEUED', '{{}}', '{{}}', 'seed');
             INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state)
               VALUES ('{a}', '{o}', '{j}', 1, 1, 'PENDING');"
        )
    };
    env.core.db.seed(&seed(&[first, second])).await;
    let mut tx = env.core.db.db().begin().await.unwrap();
    let job = JobTransitions::lock(&mut tx, env.org.as_uuid(), j)
        .await
        .unwrap()
        .unwrap();
    let attempt = JobTransitions::lock_attempt(&mut tx, env.org.as_uuid(), a)
        .await
        .unwrap()
        .unwrap();
    tx.rollback().await.unwrap();
    let placement = Placement {
        node_id: Uuid::nil(),
        roles: vec![],
        cpu_millis: 0,
        mem_bytes: 0,
    };
    let inputs = DatasetInputs::new(env.core.db.db().clone());
    let mut plan = LaunchPlan::default();
    inputs
        .contribute(&job, &attempt, &placement, &mut plan)
        .await
        .unwrap();
    let staged: Vec<(&str, &str)> = plan
        .inputs
        .iter()
        .map(|i| (i.name.as_str(), i.local_path.as_str()))
        .collect();
    assert_eq!(
        staged,
        [
            ("dataset_0", "dataset_0/dataset.json"),
            ("dataset_0", "dataset_0/shards/train-00000.parquet"),
            ("dataset_1", "dataset_1/dataset.json"),
            ("dataset_1", "dataset_1/shards/train-00000.parquet"),
        ]
    );
    // Every staged file is the stored object the agent will check against its hash.
    let st = env.store.org_store(&storage_org(o)).await.unwrap();
    for i in &plan.inputs {
        let bytes = st.get(&i.key).await.unwrap();
        assert_eq!(
            (mrl_storage::sha256_hex(&bytes), bytes.len() as u64),
            (i.sha256.clone(), i.size)
        );
    }
    let shard = &plan.inputs[1];
    assert_eq!(
        shard.sha256,
        mrl_storage::sha256_hex(
            &std::fs::read(fixture("ready/shards/train-00000.parquet")).unwrap()
        )
    );

    let mut probe = job.clone();
    probe.run_id = None;
    let mut probe_plan = LaunchPlan::default();
    probe_plan.launch.job_spec_json =
        json!({"dataset_version_ids": [dsv(first), dsv(second)]}).to_string();
    inputs
        .contribute(&probe, &attempt, &placement, &mut probe_plan)
        .await
        .unwrap();
    assert!(probe_plan.inputs.is_empty());

    probe.subject_ref = Some(json!({"kind": "run_probe"}));
    inputs
        .contribute(&probe, &attempt, &placement, &mut probe_plan)
        .await
        .unwrap();
    assert_eq!(probe_plan.inputs, plan.inputs);

    let mut empty = LaunchPlan::default();
    empty.launch.job_spec_json = json!({"dataset_version_ids": []}).to_string();
    inputs
        .contribute(&probe, &attempt, &placement, &mut empty)
        .await
        .unwrap();
    assert!(empty.inputs.is_empty());

    let mut malformed = LaunchPlan::default();
    malformed.launch.job_spec_json =
        json!({"dataset_version_ids": ["not-a-dataset-id"]}).to_string();
    let err = inputs
        .contribute(&probe, &attempt, &placement, &mut malformed)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            LaunchError::Fail(mrl_domain::failure::FailureClass::HarnessError, _)
        ),
        "{err:?}"
    );
    assert!(malformed.inputs.is_empty());

    probe.project_id = env.side.as_uuid();
    probe_plan.inputs.clear();
    let err = inputs
        .contribute(&probe, &attempt, &placement, &mut probe_plan)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            LaunchError::Fail(mrl_domain::failure::FailureClass::UserConfig, _)
        ),
        "{err:?}"
    );
    assert!(probe_plan.inputs.is_empty());

    // A version that is no longer usable stops the launch with a plain reason.
    sqlx::query("UPDATE datasets SET deleted_at = now() WHERE id = $1")
        .bind(ds.parse::<DatasetId>().unwrap().as_uuid())
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let err = inputs
        .contribute(&job, &attempt, &placement, &mut LaunchPlan::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, LaunchError::Fail(_, ref m) if m.contains("no longer ready")),
        "{err:?}"
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn dataset_posts_apply_current_permissions_and_create_replays_durably() {
    let env = Env::start().await;
    let path = format!("/api/v1/projects/{}/datasets", env.prj);
    let headers = &[("Idempotency-Key", "dataset-post-test")];
    let post = |path: String, body: Value| {
        let core = &env.core;
        let owner = &env.owner;
        async move {
            core.call_with(Method::POST, &path, owner, Some(body), headers)
                .await
        }
    };
    let meta = json!({"name": "replayed"});
    let r = post(path.clone(), meta.clone()).await;
    assert_eq!(r.status, 201, "{}", r.text());
    let ds = r.json()["id"].as_str().unwrap().to_owned();
    let replay = post(path.clone(), meta.clone()).await;
    assert_eq!(replay.status, 201, "{}", replay.text());
    assert_eq!(r.json(), replay.json());
    assert_eq!(
        post(path.clone(), json!({"name": "different"}))
            .await
            .status,
        422
    );

    let version = dsv(env.ready(&ds, "ready", None).await);
    let diff_path = format!("/api/v1/dataset-versions/{version}:diff");
    let diff = json!({"against": version});
    let get_diff = || {
        env.core.call_with(
            Method::POST,
            &diff_path,
            &env.owner,
            Some(diff.clone()),
            &[("Idempotency-Key", "diff-test")],
        )
    };
    assert_eq!(get_diff().await.status, 200);
    let export_path = format!("/api/v1/dataset-versions/{version}:export");
    let export = || {
        env.core.call_with(
            Method::POST,
            &export_path,
            &env.owner,
            Some(json!({"format":"parquet"})),
            &[("Idempotency-Key", "export-test")],
        )
    };
    assert_eq!(export().await.status, 200);

    let action_headers = &[("Idempotency-Key", "dataset-work-replay")];
    let queue = || {
        env.core.call_with(
            Method::POST,
            &export_path,
            &env.owner,
            Some(json!({"format":"jsonl"})),
            action_headers,
        )
    };
    let (first, second) = tokio::join!(queue(), queue());
    assert_eq!(first.status, 202, "{}", first.text());
    assert_eq!(second.status, 202, "{}", second.text());
    assert_eq!(first.json(), second.json());
    let mismatch = env
        .core
        .call_with(
            Method::POST,
            &format!("/api/v1/dataset-versions/{version}:transform-preview"),
            &env.owner,
            Some(
                json!({"transform_spec":{"schema_version":"transform/v1","name":"preview",
            "to":"sft.messages.v1","steps":[{"op":"dedupe"}]}}),
            ),
            action_headers,
        )
        .await;
    assert_eq!(mismatch.status, 422, "{}", mismatch.text());
    let audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_events WHERE action='datasets.jsonl' AND outcome='success'",
    )
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(audits, 1);

    let user: Uuid = sqlx::query_scalar(
        "DELETE FROM role_bindings WHERE org_id = $1 AND role = 'owner' RETURNING principal_id",
    )
    .bind(env.org.as_uuid())
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    let mut tx = env.core.db.db().begin().await.unwrap();
    bind_role(
        &mut tx,
        env.org,
        UserId::from_uuid(user),
        Role::Viewer,
        Scope::Project(env.side),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(get_diff().await.status, 404);
    assert_eq!(export().await.status, 404);
    assert_eq!(queue().await.status, 404);
    assert_eq!(post(path, meta).await.status, 404);
}

impl Env {
    async fn upload_worker_outputs(&self, job: Uuid, folder: &str, manifest: &Value) -> Uuid {
        use mrl_domain::ids::{AttemptId, JobId};
        use mrl_proto::agent::v1::{command, Command, CommandResult};
        use mrl_proto::common::v1::AttemptRef;
        use mrl_proto::worker::v1::OutputsStaged;
        use mrl_server::modules::builtin::sink::{register_pass, Outputs as WorkerOutputs};
        use mrl_server::modules::nodes::{lanes::LaneRouter, store as nodes, NodesModule};
        use mrl_server::modules::scheduler::launch::Transfer;
        use prost::Message;

        let db = self.core.db.db();
        let cfg = &self.core.config;
        let node = Uuid::now_v7();
        let pool = Uuid::now_v7();
        let attempt = AttemptId::new();
        let org = self.org.as_uuid();
        self.core.db.seed(&format!(
            "INSERT INTO compute_pools (id, org_id, name) VALUES ('{pool}', '{org}', 'worker');
             INSERT INTO compute_nodes (id, pool_id, hostname, lifecycle, status, session_holder, session_epoch)
               VALUES ('{node}', '{pool}', 'worker', 'STATIC', 'ONLINE', 'test', 1);
             INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state)
               VALUES ('{attempt_id}', '{org}', '{job}', 1, 1, 'RUNNING');
             INSERT INTO attempt_placements (attempt_id, node_id, org_id, node_rank, rank_lo, rank_hi, role)
               VALUES ('{attempt_id}', '{node}', '{org}', 0, 0, 0, 'service');
             UPDATE jobs SET state='RUNNING', epoch=1, current_attempt_id='{attempt_id}' WHERE id='{job}';",
            attempt_id = attempt.as_uuid(),
        )).await;

        let gateway = NodesModule::new(
            db.clone(),
            cfg,
            self.core.secrets.clone(),
            LaneRouter::new(),
        )
        .gateway();
        let resolver = ArtifactsModule::new(db.clone(), cfg).secrets();
        let transfer = Transfer::new(self.store.clone(), resolver, self.core.secrets.clone());
        let outputs = WorkerOutputs::new(db.clone(), self.store.clone(), transfer, &gateway);
        let at = AttemptRef {
            job_id: JobId::from_uuid(job).to_string(),
            attempt_id: attempt.to_string(),
            fencing_epoch: 1,
        };
        let staged = OutputsStaged {
            manifest_json: manifest.to_string(),
        };
        outputs.on_staged(node, &at, &staged).await.unwrap();
        outputs.on_staged(node, &at, &staged).await.unwrap();
        let nodes::Claim::Send(commands) = nodes::claim_commands(db, node, 1, 0).await.unwrap()
        else {
            panic!("the worker session should own its commands");
        };
        assert_eq!(commands.len(), 1);
        let (_, _, payload) = &commands[0];
        let command = Command::decode(payload.as_slice()).unwrap();
        let Some(command::Kind::UploadArtifacts(upload)) = command.kind else {
            panic!("expected an upload command");
        };
        assert_eq!(
            upload.items.len(),
            manifest["files"].as_array().unwrap().len()
        );
        for item in upload.items {
            let path = item.local_path.strip_prefix("outputs/").unwrap();
            let bytes = std::fs::read(fixture(folder).join(path)).unwrap();
            let r = self
                .core
                .server
                .request(Method::PUT, &item.url, &[], bytes)
                .await;
            assert_eq!(r.status, 200, "{}", r.text());
        }
        let accept = CommandResult {
            command_id: command.command_id,
            accepted: true,
            failure: None,
        };
        let mut tx = db.begin().await.unwrap();
        nodes::command_result(&mut tx, node, &accept).await.unwrap();
        tx.commit().await.unwrap();
        register_pass(&outputs).await;
        assert!(mrl_server::modules::builtin::outputs(db, org, job)
            .await
            .unwrap()
            .is_none());

        let mut tx = db.begin().await.unwrap();
        nodes::command_result(&mut tx, node, &accept).await.unwrap();
        sqlx::query("UPDATE job_attempts SET state='EXITED_OK' WHERE id=$1")
            .bind(attempt.as_uuid())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE jobs SET state='FINALIZING' WHERE id=$1")
            .bind(job)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        register_pass(&outputs).await;
        mrl_server::modules::builtin::outputs(db, org, job)
            .await
            .unwrap()
            .unwrap()
            .artifact_id
    }

    // Simulates the registered INT-6 output of a finished worker. The background task and
    // completion chain run normally against the same database and blob store.
    async fn worker_outputs(&self, job: Uuid, manifest: Value) {
        let artifact = self.register_manifest(manifest).await;
        self.finish_outputs(job, artifact).await;
    }

    async fn finish_outputs(&self, job: Uuid, artifact: Uuid) {
        let org = self.org.as_uuid();
        let attempt = Uuid::now_v7();
        let mut tx = self.core.db.db().begin().await.unwrap();
        sqlx::query(
            "INSERT INTO job_attempts (id,org_id,job_id,attempt_no,epoch,state)
                     VALUES ($1,$2,$3,1,1,'EXITED_OK')",
        )
        .bind(attempt)
        .bind(org)
        .bind(job)
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("UPDATE jobs SET state='FINALIZING',epoch=1,current_attempt_id=$2 WHERE id=$1")
            .bind(job)
            .bind(attempt)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE builtin_jobs SET upload_attempt_id=$2,artifact_id=$3 WHERE job_id=$1")
            .bind(job)
            .bind(attempt)
            .bind(artifact)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    async fn register_manifest(&self, manifest: Value) -> Uuid {
        let artifact = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO artifacts (id,org_id,project_id,name,kind,prefix,manifest,size_bytes,state,created_by,committed_at)
             VALUES ($1,$2,$3,$4,'dataset',$5,$6,0,'COMMITTED','system',now())",
        )
        .bind(artifact).bind(self.org.to_string()).bind(self.prj.to_string())
        .bind(artifact.to_string()).bind(format!("orgs/{}/fixture/{artifact}", self.org))
        .bind(manifest).execute(self.core.db.pool()).await.unwrap();
        artifact
    }

    async fn blob_manifest(&self, files: &[(&str, &[u8])]) -> Value {
        let st = self.store.org_store(&self.org.to_string()).await.unwrap();
        let mut entries = Vec::new();
        for (path, bytes) in files {
            let sha = mrl_storage::sha256_hex(bytes);
            st.put(&st.keys().blob(&sha).unwrap(), bytes.to_vec().into())
                .await
                .unwrap();
            entries.push(json!({"path":path,"size":bytes.len(),"sha256":sha,"role":"data"}));
        }
        json!({"schema_version":"artifact-manifest/v1","kind":"dataset",
            "created_at":"2026-11-27T00:00:00Z","files":entries})
    }

    async fn version_until(&self, version: &str, state: &str) -> Value {
        let path = format!("/dataset-versions/{version}");
        tokio::time::timeout(std::time::Duration::from_secs(25), async {
            loop {
                let (status, row) = self.call(&self.owner, Method::GET, &path, None).await;
                assert_eq!(status, 200, "{row}");
                if row["state"] == state {
                    return row;
                }
                assert!(
                    !matches!(row["state"].as_str(), Some("READY" | "INVALID")),
                    "{row}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("dataset {version} did not reach {state}"))
    }
}

fn uuid_job(value: &Value) -> Uuid {
    value["job_id"]
        .as_str()
        .unwrap()
        .parse::<mrl_domain::ids::JobId>()
        .unwrap()
        .as_uuid()
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn upload_to_ready_and_transform_use_int6_and_retry_unregistered_outputs() {
    let env = Env::start().await;
    let ds = env.dataset("int6 uploads", json!({})).await;
    let original = include_bytes!("../../../../../tests/fixtures/datasets/sft.messages.v1.jsonl");
    let (status, up) = env
        .call(
            &env.owner,
            Method::POST,
            "/uploads",
            Some(
                json!({"project_id":env.prj,"filename":"train.jsonl","size":original.len(),
            "sha256":mrl_storage::sha256_hex(original)}),
            ),
        )
        .await;
    assert_eq!(status, 201, "{up}");
    let upload = up["id"].as_str().unwrap();
    let path = format!("/uploads/{upload}:presign");
    let first = env.call(&env.owner, Method::POST, &path, None).await;
    assert_eq!(first.0, 200, "{}", first.1);
    let (status, resumed) = env.call(&env.owner, Method::POST, &path, None).await;
    assert_eq!(status, 200, "{resumed}");
    let ticket = resumed["tickets"][0]["ticket"]["url"].as_str().unwrap();
    assert_eq!(
        env.core
            .server
            .request(Method::PUT, ticket, &[], original.as_slice())
            .await
            .status,
        200
    );
    assert_eq!(
        env.call(
            &env.owner,
            Method::POST,
            &format!("/uploads/{upload}:complete"),
            None
        )
        .await
        .0,
        200
    );

    let body = json!({"source":"upload","schema_id":"sft.messages.v1","uploads":[upload]});
    let path = format!("/api/v1/datasets/{ds}/versions");
    let post = || {
        env.core.call_with(
            Method::POST,
            &path,
            &env.owner,
            Some(body.clone()),
            &[("idempotency-key", "int6-upload")],
        )
    };
    let response = post().await;
    assert_eq!(response.status, 202, "{}", response.text());
    let created = response.json();
    let version = created["id"].as_str().unwrap();
    let job = uuid_job(&created);
    assert_eq!(post().await.json()["id"], version);
    let mut tx = env.core.db.db().begin().await.unwrap();
    let mut row = JobTransitions::lock(&mut tx, env.org.as_uuid(), job)
        .await
        .unwrap()
        .unwrap();
    tx.rollback().await.unwrap();
    // The stored job stays queued while we verify snapshots without racing the jobs poller.
    row.state = "FINALIZING".into();
    use mrl_server::modules::jobs::CompletionVerifier as _;
    let verifier =
        mrl_server::modules::datasets::work::DatasetVerifier::new(env.core.db.db().clone());
    assert!(verifier.verify(&row).await.is_err());
    let manifest = fixture_manifest("ready");
    let artifact = env.upload_worker_outputs(job, "ready", &manifest).await;
    let ready = env.version_until(version, "READY").await;
    assert_eq!(
        env.store
            .get(&env.org.to_string(), artifact)
            .await
            .unwrap()
            .unwrap()
            .created_by,
        "system:builtin",
    );
    assert_eq!(ready["row_count"], 200);
    assert!(matches!(
        verifier.verify(&row).await.unwrap(),
        mrl_server::modules::jobs::Verification::Verified
    ));
    let (_, with_files) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/dataset-versions/{version}"),
            None,
        )
        .await;
    assert!(with_files["files"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f["role"] == "original"));

    let spec = json!({"schema_version":"transform/v1","name":"filter","to":"sft.messages.v1",
        "steps":[{"op":"filter","expr":"len(messages) > 2"}]});
    let (status, preview) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/dataset-versions/{version}:transform-preview"),
            Some(json!({"transform_spec":spec})),
        )
        .await;
    assert_eq!(status, 202, "{preview}");
    let preview_job = uuid_job(&preview);
    let preview_path = format!(
        "/dataset-versions/{version}/work/{}",
        preview["job_id"].as_str().unwrap()
    );
    env.worker_outputs(
        preview_job,
        env.blob_manifest(&[("preview.json", br#"{"rows":[{}]}"#)])
            .await,
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(25), async {
        loop {
            let (status, r) = env.call(&env.owner, Method::GET, &preview_path, None).await;
            assert_eq!(status, 200, "{r}");
            assert!(r["failure"].is_null(), "{r}");
            if r["report"]["rows"].is_array() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();

    let (status, child) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/datasets/{ds}/versions"),
            Some(json!({"source":"transform","parent_version_id":version,"transform_spec":spec})),
        )
        .await;
    assert_eq!(status, 202, "{child}");
    env.worker_outputs(uuid_job(&child), env.publish("filtered").await)
        .await;
    let child = env
        .version_until(child["id"].as_str().unwrap(), "READY")
        .await;
    assert_eq!(child["parent_version_id"], version);
    assert_eq!(child["transform_spec"], spec);
    assert_ne!(child["content_sha256"], ready["content_sha256"]);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn hub_pinned_revision_reaches_prep_and_registered_output_sets_invalid() {
    let env = Env::start().await;
    let ds = env.dataset("int6 hub", json!({})).await;
    let (status, created) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/datasets/{ds}/versions"),
            Some(json!({"source":"hub","repo":"openai/gsm8k","schema_id":"sft.messages.v1"})),
        )
        .await;
    assert_eq!(status, 202, "{created}");
    let fetch = uuid_job(&created);
    let version = created["id"].as_str().unwrap();
    let sha = "a".repeat(40);
    let report = json!({"source":"hub","repo":"openai/gsm8k","revision":"main","sha":sha});
    let report = serde_json::to_vec(&report).unwrap();
    let files: &[(&str, &[u8])] = &[("import.json", &report), ("files/train.jsonl", b"{}\n")];
    env.worker_outputs(fetch, env.blob_manifest(files).await)
        .await;
    let checking = env.version_until(version, "VALIDATING").await;
    assert_eq!(checking["source_ref"]["sha"], sha);
    let prep = uuid_job(&checking);
    assert_ne!(prep, fetch);
    let staged: Value = sqlx::query_scalar("SELECT files FROM builtin_jobs WHERE job_id=$1")
        .bind(prep)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    assert_eq!(staged[0]["local_path"], "source/train.jsonl");
    env.worker_outputs(prep, env.publish("invalid").await).await;
    let invalid = env.version_until(version, "INVALID").await;
    assert_eq!(invalid["state_reason"]["code"], "MRL-DATA-001");

    let missing = env.call(&env.outsider, Method::POST, &format!("/datasets/{ds}/versions"),
        Some(json!({"source":"remote","schema_id":"sft.messages.v1","url":"https://example.org/data"}))).await;
    assert_eq!(missing.0, 404);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn solo_dataset_lineage_uses_the_local_storage_namespace() {
    use mrl_server::modules::artifacts::{NodeLookup, LOCAL_ORG};

    let env = Env::start_mode(Mode::Solo).await;
    let ds = env.dataset("local chats", json!({})).await;
    let parent = env.ready(&ds, "ready", None).await;
    let child = env.ready(&ds, "filtered", Some(parent)).await;
    let edge_org: String = sqlx::query_scalar(
        "SELECT org_id FROM lineage_edges WHERE from_kind='dsv' AND from_id=$1
         AND relation='derived_from' AND to_kind='dsv' AND to_id=$2",
    )
    .bind(child)
    .bind(parent)
    .fetch_one(env.core.db.pool())
    .await
    .unwrap();
    assert_eq!(edge_org, LOCAL_ORG);
    let records = DatasetRecords::new(env.core.db.db().clone());
    let (_, resource) = records.find(LOCAL_ORG, child).await.unwrap().unwrap();
    assert_eq!(resource.org, None);
    assert!(records
        .find(&env.far_org.to_string(), child)
        .await
        .unwrap()
        .is_none());

    let (status, graph) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/lineage/{}", dsv(child)),
            None,
        )
        .await;
    assert_eq!(status, 200, "{graph}");
    assert_eq!(
        graph["edges"],
        json!([{
            "from": dsv(child), "to": dsv(parent), "relation": "derived_from"
        }])
    );
    env.core.stop().await;
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn solo_tokenizers_stage_in_the_job_org() {
    use mrl_domain::ids::ArtifactId;
    use mrl_server::modules::artifacts::LOCAL_ORG;

    let env = Env::start_mode(Mode::Solo).await;
    let ds = env.dataset("local tokenizer", json!({})).await;
    let version = dsv(env.ready(&ds, "ready", None).await);
    let bytes = b"{}";
    let manifest = env.blob_manifest(&[("tokenizer.json", bytes)]).await;
    let artifact = env.register_manifest(manifest).await;
    let sha = mrl_storage::sha256_hex(bytes);
    let local = env.store.org_store(LOCAL_ORG).await.unwrap();
    local
        .put(&local.keys().blob(&sha).unwrap(), bytes.as_slice().into())
        .await
        .unwrap();
    let target = env.store.org_store(&env.org.to_string()).await.unwrap();
    let key = target.keys().blob(&sha).unwrap();
    target.delete(&key).await.unwrap();
    let set_org_project = |project: ProjectId| {
        sqlx::query("UPDATE artifacts SET org_id=$1, project_id=$2 WHERE id=$3")
            .bind(LOCAL_ORG)
            .bind(project.to_string())
            .bind(artifact)
            .execute(env.core.db.pool())
    };
    let tokenize = || async {
        env.call(
            &env.owner,
            Method::POST,
            &format!("/dataset-versions/{version}:tokenize-preview"),
            Some(json!({"tokenizer_artifact_id":ArtifactId::from_uuid(artifact)})),
        )
        .await
    };
    set_org_project(env.side).await.unwrap();
    assert_eq!(tokenize().await.0, 404);
    set_org_project(env.prj).await.unwrap();
    let (status, body) = tokenize().await;
    assert_eq!(status, 202, "{body}");
    let job = uuid_job(&body);
    let files: Value = sqlx::query_scalar("SELECT files FROM builtin_jobs WHERE job_id=$1")
        .bind(job)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    assert!(files.as_array().unwrap().iter().any(|f| f["key"] == key));
    assert_eq!(target.get_range(&key, 0..2).await.unwrap().as_ref(), bytes);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn derived_versions_keep_restricted_personal_data_inside_the_dataset() {
    use mrl_domain::ids::ArtifactId;

    let env = Env::start().await;
    let source = env
        .dataset("restricted source", json!({"sensitivity":"restricted-pii"}))
        .await;
    let target = env
        .dataset("derived", json!({"sensitivity":"public"}))
        .await;
    let parent = dsv(env.ready(&source, "ready", None).await);
    let (status, child) = env.call(
        &env.owner, Method::POST, &format!("/datasets/{target}/versions"),
        Some(json!({"source":"transform","parent_version_id":parent,"transform_spec":{
            "schema_version":"transform/v1","name":"dedupe","to":"sft.messages.v1","steps":[{"op":"dedupe"}]
        }})),
    ).await;
    assert_eq!(status, 202, "{child}");
    let job = uuid_job(&child);
    env.worker_outputs(job, env.publish("filtered").await).await;
    let child = child["id"].as_str().unwrap();
    env.version_until(child, "READY").await;

    assert_eq!(
        env.call(
            &env.owner,
            Method::PATCH,
            &format!("/datasets/{source}"),
            Some(json!({"sensitivity":"internal"}))
        )
        .await
        .0,
        200
    );
    let (status, dataset) = env
        .call(
            &env.owner,
            Method::GET,
            &format!("/datasets/{target}"),
            None,
        )
        .await;
    assert_eq!(status, 200, "{dataset}");
    assert_eq!(dataset["policy"]["sample_logging"], false);

    let path = format!("/dataset-versions/{child}:export");
    for format in ["parquet", "jsonl", "hub"] {
        let (status, error) = env
            .call(
                &env.owner,
                Method::POST,
                &path,
                Some(json!({"format":format})),
            )
            .await;
        assert_eq!(
            (status, error["code"].as_str()),
            (403, Some("MRL-PERM-270")),
            "{error}"
        );
    }
    let artifact: Uuid = sqlx::query_scalar("SELECT artifact_id FROM builtin_jobs WHERE job_id=$1")
        .bind(job)
        .fetch_one(env.core.db.pool())
        .await
        .unwrap();
    let (status, error) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/artifacts/{}:download", ArtifactId::from_uuid(artifact)),
            Some(json!({"path":"dataset.json"})),
        )
        .await;
    assert_eq!(
        (status, error["code"].as_str()),
        (403, Some("MRL-PERM-270")),
        "{error}"
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn generic_artifact_downloads_follow_the_datasets_export_policy() {
    use mrl_domain::ids::{ArtifactId, AttemptId};

    let env = Env::start().await;
    let ds = env.dataset("job outputs", json!({})).await;
    let version = env.ready(&ds, "ready", None).await;
    let manifest = env.blob_manifest(&[("data.jsonl", b"{}\n")]).await;
    let unrelated = env.register_manifest(manifest.clone()).await;
    let version_output = env.register_manifest(manifest.clone()).await;
    let job_output = env.register_manifest(manifest.clone()).await;
    let pending_output = env.register_manifest(manifest).await;
    sqlx::query("UPDATE dataset_versions SET artifact_id=$2 WHERE id=$1")
        .bind(version)
        .bind(version_output)
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let (status, work) = env
        .call(
            &env.owner,
            Method::POST,
            &format!("/datasets/{ds}/versions"),
            Some(json!({"source":"hub","repo":"openai/gsm8k","schema_id":"sft.messages.v1"})),
        )
        .await;
    assert_eq!(status, 202, "{work}");
    sqlx::query("UPDATE builtin_jobs SET artifact_id=$2 WHERE job_id=$1")
        .bind(uuid_job(&work))
        .bind(job_output)
        .execute(env.core.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE artifacts SET created_by='system:builtin', name=$2 WHERE id=$1")
        .bind(pending_output)
        .bind(format!(
            "{}-{}",
            work["job_id"].as_str().unwrap(),
            AttemptId::new()
        ))
        .execute(env.core.db.pool())
        .await
        .unwrap();
    let core = &env.core;
    let download = |auth, artifact| async move {
        core.call(
            Method::POST,
            &format!(
                "/api/v1/artifacts/{}:download",
                ArtifactId::from_uuid(artifact)
            ),
            auth,
            Some(json!({"path":"data.jsonl"})),
        )
        .await
    };
    assert_eq!(download(&env.viewer, unrelated).await.status, 200);
    for artifact in [version_output, job_output, pending_output] {
        assert_eq!(download(&env.owner, artifact).await.status, 200);
        assert_eq!(download(&env.viewer, artifact).await.status, 403);
        assert_eq!(download(&env.outsider, artifact).await.status, 404);
    }
    let (status, value) = env
        .call(
            &env.owner,
            Method::PATCH,
            &format!("/datasets/{ds}"),
            Some(json!({"sensitivity":"restricted-pii"})),
        )
        .await;
    assert_eq!(status, 200, "{value}");
    for artifact in [version_output, job_output, pending_output] {
        let response = download(&env.owner, artifact).await;
        assert_eq!(
            (response.status, response.json()["code"].as_str()),
            (403, Some("MRL-PERM-270")),
            "{}",
            response.text()
        );
    }
    let (status, _) = env
        .call(&env.owner, Method::DELETE, &format!("/datasets/{ds}"), None)
        .await;
    assert_eq!(status, 204);
    for artifact in [version_output, job_output, pending_output] {
        assert_eq!(download(&env.owner, artifact).await.status, 404);
    }
    env.core.stop().await;
}
