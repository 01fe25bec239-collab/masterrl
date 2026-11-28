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
        let sb = MockSupabase::start().await;
        let objects = tempfile::tempdir().unwrap();
        let mut cfg = config(Mode::Team, &sb);
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
        let (prj, side, far) = (ProjectId::new(), ProjectId::new(), ProjectId::new());
        for (o, projects) in [(org, vec![prj, side]), (far_org, vec![far])] {
            let (o, w) = (o.as_uuid(), WorkspaceId::new().as_uuid());
            let mut sql = format!(
                "INSERT INTO organizations (id, slug, name, kind) VALUES ('{o}', '{o}', 'Lab', 'team');
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
        let owner = person(&core, &sb, "owner", org, Role::Owner).await;
        let outsider = person(&core, &sb, "outsider", far_org, Role::Owner).await;
        let viewer = person(&core, &sb, "viewer", org, Role::Viewer).await;
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

    /// Puts every file of the fixture folder `name` in the org's storage, as the agent uploads
    /// a job's outputs, and returns their `artifact-manifest/v1`.
    async fn publish(&self, name: &str) -> Value {
        let st = self
            .store
            .org_store(&storage_org(self.org.as_uuid()))
            .await
            .unwrap();
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
                if rel == ".gitattributes" {
                    continue;
                }
                let bytes = std::fs::read(&p).unwrap();
                let sha = mrl_storage::sha256_hex(&bytes);
                st.put(&st.keys().blob(&sha).unwrap(), bytes.clone().into())
                    .await
                    .unwrap();
                files
                    .push(json!({"path": rel, "size": bytes.len(), "sha256": sha, "role": "data"}));
            }
        }
        json!({"schema_version": "artifact-manifest/v1", "kind": "dataset",
               "created_at": "2026-11-27T00:00:00Z", "files": files})
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
        let changed = store::record_version(&mut tx, version, &checked)
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
    assert!(!store::record_version(&mut tx, v, &checked).await.unwrap());
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
async fn a_run_stages_each_dataset_version_as_one_checked_folder() {
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
