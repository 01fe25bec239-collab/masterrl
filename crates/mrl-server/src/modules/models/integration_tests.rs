//! P26 integration tests: model versions recorded from job outputs, the routes, the gates and
//! the launch contributor, on a real database and object store.
//!
//! Compiled by `mrl-testkit` (see its `lib.rs`, P26 marker), not by this crate: they need
//! `TestServer` and `TestDb`. `#[ignore]`d; run them with `just dev && just test-integration`.
//!
//! Jobs here are rows only: the test plays the built-in task by putting what `import_model`
//! writes (`tests/fixtures/p26/tiny-llama.inspection.json`, the model files) into storage
//! and handing the verifier their manifest, exactly as the job's outputs would arrive.

use crate::pg::TestDb;
use crate::seams as ts;
use crate::server::{TestResponse, TestServer, TEST_SECRET};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::Method;
use mrl_domain::ids::{ModelVersionId, ProjectId};
use mrl_server::config::Mode;
use mrl_server::modules::artifacts::{ArtifactStore, ArtifactsModule};
use mrl_server::modules::engines::{RecordScope, RegistryRecords};
use mrl_server::modules::jobs::CompletionVerifier;
use mrl_server::modules::jobs::{
    AttemptRow, JobRow, JobSpec, JobValidator, Projects, Verdict, Verification,
};
use mrl_server::modules::models::gates::{ModelGates, ModelRecords};
use mrl_server::modules::models::launch::{ModelInputs, MODEL_INPUT};
use mrl_server::modules::models::outputs::{self, ModelJobEnds, ModelOutputs, Produced};
use mrl_server::modules::models::store::{self, NewVersion};
use mrl_server::modules::models::{Models, ModelsModule};
use mrl_server::modules::scheduler::pipeline::{
    LaunchContributor, LaunchError, LaunchPlan, Placement,
};
use mrl_server::seams::{AuditEvent, AuditSink, SeamError, LAUNCH_SECRET_HEADER};
use mrl_storage::{sha256_hex, BackendConfig};
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const INSPECTION: &str =
    include_str!("../../../../../tests/fixtures/p26/tiny-llama.inspection.json");
const GOLDEN: &str = include_str!("../../../../../tests/fixtures/models/GOLDEN.json");
const COMMIT: &str = "9fb191250dd56d0ba7ec9785a025ed29c03d5998";

/// Keeps the events recorded inside transactions.
#[derive(Default)]
struct Audit(Mutex<Vec<AuditEvent>>);

impl Audit {
    fn actions(&self) -> Vec<(String, String)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|e| (e.action.clone(), e.target.clone()))
            .collect()
    }
}

#[async_trait]
impl AuditSink for Audit {
    async fn record(&self, event: AuditEvent) -> Result<(), SeamError> {
        self.0.lock().unwrap().push(event);
        Ok(())
    }
    async fn record_in(
        &self,
        _: &mut mrl_server::db::Tx,
        event: AuditEvent,
    ) -> Result<(), SeamError> {
        self.record(event).await
    }
}

struct Env {
    server: TestServer,
    db: TestDb,
    models: Arc<Models>,
    store: Arc<ArtifactStore>,
    audit: Arc<Audit>,
    project: ProjectId,
    _dirs: tempfile::TempDir,
}

/// One version's job outputs, as the task wrote them.
struct Outputs {
    /// `(path under outputs/, bytes)`.
    files: Vec<(String, Vec<u8>)>,
}

impl Outputs {
    /// The tiny-llama import: its report (changed by `edit`) and model files.
    fn tiny(edit: impl FnOnce(&mut Value)) -> Self {
        let mut report: Value = serde_json::from_str(INSPECTION).unwrap();
        report["source"] = json!({"repo": "org/tiny", "revision": "main", "commit": COMMIT});
        edit(&mut report);
        let mut files = vec![(
            "inspection.json".to_owned(),
            report.to_string().into_bytes(),
        )];
        for f in report["files"].as_array().cloned().unwrap_or_default() {
            let path = f["path"].as_str().unwrap();
            files.push((
                format!("model/{path}"),
                format!("bytes of {path}").into_bytes(),
            ));
        }
        Self { files }
    }

    fn sha(&self, path: &str) -> String {
        sha256_hex(&self.files.iter().find(|(p, _)| p == path).unwrap().1)
    }
}

impl Env {
    async fn start() -> Self {
        let db = TestDb::new().await;
        let dirs = tempfile::tempdir().unwrap();
        let artifacts = ArtifactsModule::with_backend(
            db.db().clone(),
            BackendConfig::Local {
                root: dirs.path().join("objects"),
            },
            Mode::Solo,
            dirs.path().join("scratch"),
        );
        let store = artifacts.store();
        let module = ModelsModule::with_parts(db.db().clone(), store.clone(), Projects::default());
        let models = module.models();
        // Live engine versions, so versions get a compatibility table.
        let bundles = mrl_server::modules::engines::embedded_bundles().unwrap();
        let refs: Vec<_> = bundles.iter().collect();
        mrl_server::modules::engines::store::register(db.db(), &refs, false)
            .await
            .unwrap();
        db.seed("UPDATE engine_versions SET status = 'ENABLED'")
            .await;
        let audit = Arc::new(Audit::default());
        let seams = ts::build(ts::solo(TEST_SECRET).audit_arc(audit.clone()), Mode::Solo);
        let cfg = crate::agent::gateway::server_config(
            crate::agent::gateway::fast_config(),
            Mode::Solo,
            0,
        );
        // Checkpoint routes for `:promote` (no machine joins; the gateway only has to exist).
        let secrets: Arc<dyn mrl_server::seams::SecretStore> =
            Arc::new(ts::InMemorySecretStore::default());
        let lanes = mrl_server::modules::nodes::lanes::LaneRouter::new();
        let nodes = mrl_server::modules::nodes::NodesModule::new(
            db.db().clone(),
            &cfg,
            secrets.clone(),
            lanes.clone(),
        );
        let checkpoints = mrl_server::modules::checkpoints::CheckpointsModule::with_parts(
            db.db().clone(),
            store.clone(),
            secrets,
            &nodes.gateway(),
            &lanes,
            mrl_server::modules::checkpoints::CheckpointsConfig::default(),
            mrl_server::modules::jobs::Chains::new(vec![], vec![], vec![]),
        );
        let server = TestServer::start(
            cfg,
            seams,
            vec![
                Box::new(nodes),
                Box::new(artifacts),
                Box::new(checkpoints),
                Box::new(module),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("server: {}", e.explain()));
        Self {
            server,
            db,
            models,
            store,
            audit,
            project: ProjectId::new(),
            _dirs: dirs,
        }
    }

    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> TestResponse {
        let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
        self.server
            .request(
                method,
                &format!("/api/v1{path}"),
                &[
                    (LAUNCH_SECRET_HEADER, TEST_SECRET),
                    ("content-type", "application/json"),
                ],
                body,
            )
            .await
    }

    /// The model `name` of the test project, created through the API.
    async fn model(&self, name: &str) -> store::ModelRow {
        let r = self
            .call(
                Method::POST,
                &format!("/projects/{}/models", self.project),
                Some(json!({"name": name})),
            )
            .await;
        assert_eq!(r.status, 201, "{}", r.text());
        let id: mrl_domain::ids::ModelId = r.json()["id"].as_str().unwrap().parse().unwrap();
        store::model(self.db.db(), id.as_uuid())
            .await
            .unwrap()
            .unwrap()
    }

    /// A new IMPORTING version of `model` with its job (a job row in QUEUED).
    async fn version(&self, model: &store::ModelRow, base: Option<Uuid>) -> (Uuid, Uuid) {
        let job = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO jobs (id, org_id, project_id, kind, state, resource_request, retry_policy, created_by)
             VALUES ($1, $2, $3, 'IMPORT', 'QUEUED', '{}', '{}', 'test')",
        )
        .bind(job)
        .bind(model.org_id)
        .bind(model.project_id)
        .execute(self.db.pool())
        .await
        .unwrap();
        let mut tx = self.db.db().begin().await.unwrap();
        let v = store::insert_version(
            &mut tx,
            model,
            &NewVersion {
                source_kind: "hub",
                source_ref: json!({"repo": "org/tiny", "revision": "main"}),
                base_model_version_id: base,
                files: vec![],
                created_by: "test".into(),
            },
        )
        .await
        .unwrap();
        store::set_job(&mut tx, v.id, job).await.unwrap();
        tx.commit().await.unwrap();
        (v.id, job)
    }

    /// Stores `out` and returns what the verifier is handed.
    async fn produce(&self, out: &Outputs) -> Produced {
        let st = self.store.org_store("local").await.unwrap();
        let prefix = format!(
            "{}jobs/test-{}/outputs/",
            st.keys().prefix(),
            Uuid::now_v7()
        );
        let mut entries = vec![];
        for (path, bytes) in &out.files {
            let sha = sha256_hex(bytes);
            st.put(&format!("{prefix}{path}"), bytes.clone().into())
                .await
                .unwrap();
            entries.push(json!({"path": path, "sha256": sha, "size": bytes.len()}));
        }
        Produced {
            artifact_id: Uuid::now_v7(),
            manifest: json!({"files": entries}),
            prefix,
        }
    }

    /// A READY tiny-llama version (report changed by `edit`).
    async fn ready(&self, name: &str, edit: impl FnOnce(&mut Value)) -> (Uuid, Outputs) {
        let m = self.model(name).await;
        let (mv, job) = self.version(&m, None).await;
        let out = Outputs::tiny(edit);
        let produced = self.produce(&out).await;
        let got = outputs::record(&self.models, job, Some(&produced))
            .await
            .unwrap();
        assert_eq!(got, Verification::Verified);
        (mv, out)
    }

    async fn get_version(&self, mv: Uuid) -> Value {
        let r = self
            .call(
                Method::GET,
                &format!("/model-versions/{}", ModelVersionId::from_uuid(mv)),
                None,
            )
            .await;
        assert_eq!(r.status, 200, "{}", r.text());
        r.json()
    }

    async fn action(&self, mv: Uuid, action: &str, body: Value) -> TestResponse {
        self.call(
            Method::POST,
            &format!("/model-versions/{}:{action}", ModelVersionId::from_uuid(mv)),
            Some(body),
        )
        .await
    }

    fn spec(&self, mv: Uuid) -> JobSpec {
        JobSpec {
            org_id: Uuid::nil(),
            project_id: self.project.as_uuid(),
            kind: "TRAIN".into(),
            algorithm_id: "sft".into(),
            base_model_version_id: mv,
            reward_version_id: None,
            dataset_version_ids: vec![],
            resolved: json!({}),
            resources: serde_json::from_value(json!({"roles": [{"role": "trainer", "replicas": 1,
                "gpu_count": 0, "cpu_millis": 1000, "mem_bytes": 1073741824}]}))
            .unwrap(),
            max_runtime_seconds: None,
        }
    }

    /// A run job row of the test project (in `org`) with one attempt.
    async fn run_job(&self, org: Uuid) -> (JobRow, AttemptRow) {
        let (job, attempt) = (Uuid::now_v7(), Uuid::now_v7());
        let p = self.project.as_uuid();
        self.db
            .seed(&format!(
                "INSERT INTO jobs (id, org_id, project_id, kind, state, resource_request, retry_policy, created_by)
                   VALUES ('{job}', '{org}', '{p}', 'IMPORT', 'ALLOCATING', '{{}}', '{{}}', 'test');
                 INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state)
                   VALUES ('{attempt}', '{org}', '{job}', 1, 1, 'PENDING');"
            ))
            .await;
        let mut row: JobRow = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job)
            .fetch_one(self.db.pool())
            .await
            .unwrap();
        // The contributor only looks at run jobs; the run itself is not read.
        row.run_id = Some(Uuid::now_v7());
        let attempt: AttemptRow = sqlx::query_as("SELECT * FROM job_attempts WHERE id = $1")
            .bind(attempt)
            .fetch_one(self.db.pool())
            .await
            .unwrap();
        (row, attempt)
    }

    async fn launch(&self, org: Uuid, mv: Uuid) -> Result<LaunchPlan, LaunchError> {
        let (job, attempt) = self.run_job(org).await;
        let mut plan = LaunchPlan::default();
        plan.launch.job_spec_json =
            json!({"base_model_version_id": ModelVersionId::from_uuid(mv).to_string()}).to_string();
        let placement = Placement {
            node_id: Uuid::nil(),
            roles: vec![],
            cpu_millis: 0,
            mem_bytes: 0,
        };
        ModelInputs::new(self.db.db().clone())
            .contribute(&job, &attempt, &placement, &mut plan)
            .await
            .map(|()| plan)
    }
}

/// What the job-end consumer does for a failed or cancelled job.
async fn ended(env: &Env, job: Uuid, why: &str) {
    let mut tx = env.db.db().begin().await.unwrap();
    outputs::job_ended(&mut tx, job, why).await.unwrap();
    tx.commit().await.unwrap();
}

fn codes(v: &Verdict) -> Vec<String> {
    match v {
        Verdict::Reject(r) => r.iter().map(|r| r.code.clone()).collect(),
        _ => vec![],
    }
}

/// The tiny-llama import becomes READY with its golden inspection, files, weights hash and
/// compatibility; replaying the verifier answers the same and changes nothing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn an_imported_tiny_llama_becomes_ready_with_its_golden_inspection() {
    let env = Env::start().await;
    let first = env.model("tiny-llama").await;
    assert_eq!(
        env.model("tiny-llama").await.id,
        first.id,
        "same name, same model"
    );
    let (mv, job) = env.version(&first, None).await;
    let out = Outputs::tiny(|_| {});
    let produced = env.produce(&out).await;
    assert_eq!(
        outputs::record(&env.models, job, Some(&produced))
            .await
            .unwrap(),
        Verification::Verified
    );
    let before = env.get_version(mv).await;
    // Replayed, reordered after a job end, or without outputs: the settled answer stands.
    ended(&env, job, "cancelled").await;
    assert_eq!(
        outputs::record(&env.models, job, None).await.unwrap(),
        Verification::Verified
    );
    let v = env.get_version(mv).await;
    assert_eq!(v, before);

    let golden: Value = serde_json::from_str(GOLDEN).unwrap();
    assert_eq!(v["state"], "READY");
    assert_eq!(v["stage"], "EXPERIMENTAL");
    assert_eq!(v["kind"], "full_weights");
    assert_eq!(v["family"], "decoder");
    assert_eq!(v["architecture"], golden["architecture"]);
    assert_eq!(v["param_count"], golden["param_count"]);
    assert_eq!(v["context_length"], golden["context_length"]);
    assert_eq!(v["chat_template_sha256"], golden["chat_template_sha256"]);
    assert_eq!(v["dtypes"], json!(["float32"]));
    assert_eq!(v["revision"], COMMIT);
    assert_eq!(v["source_ref"]["commit"], COMMIT);
    assert_eq!(v["weights_sha256"], out.sha("model/model.safetensors"));
    assert_eq!(v["license"]["spdx"], "apache-2.0");
    assert_eq!(v["license"]["acknowledged"], false);
    assert_eq!(v["requires_remote_code"], false);
    assert_eq!(v["remote_code_approved"], true);
    assert!(v["inspection"]["tensors"].is_null());
    let files = v["files"].as_array().unwrap();
    assert_eq!(files.len(), 5);
    let weights = files
        .iter()
        .find(|f| f["path"] == "model.safetensors")
        .unwrap();
    assert_eq!(weights["role"], "weights");
    assert_eq!(weights["format"], "safetensors");
    let compat = v["compatibility"].as_object().unwrap();
    assert!(!compat.is_empty(), "every live engine version is listed");
    assert!(
        compat.values().any(|m| m["sft"]["ok"] == true),
        "some engine trains tiny-llama with sft: {compat:?}"
    );

    let list = env
        .call(
            Method::GET,
            &format!("/models/{}/versions", v["model_id"].as_str().unwrap()),
            None,
        )
        .await;
    assert_eq!(list.json()["items"][0]["id"], v["id"]);
    assert!(list.json()["items"][0].get("files").is_none());
    let models = env
        .call(
            Method::GET,
            &format!("/projects/{}/models", env.project),
            None,
        )
        .await;
    assert_eq!(models.json()["items"][0]["family"], "decoder");

    // P24 sees exactly this version, only in its own project.
    let records = ModelRecords::new(env.db.db().clone());
    let scope = RecordScope {
        org_id: Uuid::nil(),
        project_id: env.project.as_uuid(),
    };
    let facts = records
        .model(&scope, ModelVersionId::from_uuid(mv))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(facts.family, "decoder");
    assert!(facts.has_chat_template);
    assert_eq!(facts.params, Some(1_033_344.0));
    let other = RecordScope {
        project_id: Uuid::now_v7(),
        ..scope
    };
    assert!(records
        .model(&other, ModelVersionId::from_uuid(mv))
        .await
        .unwrap()
        .is_none());
    assert!(env
        .audit
        .actions()
        .iter()
        .any(|(a, _)| a == "models.create"));
}

/// The gates: a run on a version needs its license accepted in the project and, for
/// versions shipping code, a maintainer's approval of that exact revision (both audited).
/// The launch stages the version's files as the `model` folder with the weights hash.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn runs_need_the_license_accepted_and_custom_code_approved() {
    let env = Env::start().await;
    let (mv, out) = env
        .ready("custom", |r| {
            r["requires_remote_code"] = json!(true);
            r["auto_map"] = json!({"AutoModelForCausalLM": "modeling.Custom"});
        })
        .await;
    let gates = ModelGates::new(env.db.db().clone());
    let verdict = gates.validate(&env.spec(mv)).await.unwrap();
    assert_eq!(codes(&verdict), ["MRL-MODEL-002", "MRL-MODEL-262"]);
    let facts = ModelRecords::new(env.db.db().clone())
        .model(
            &RecordScope {
                org_id: Uuid::nil(),
                project_id: env.project.as_uuid(),
            },
            ModelVersionId::from_uuid(mv),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(facts.requires_remote_code && !facts.remote_code_approved);
    let refused = env.launch(Uuid::nil(), mv).await;
    assert!(matches!(refused, Err(LaunchError::Fail(..))), "{refused:?}");

    // Another license text or revision than the version holds is a stale confirmation.
    let v = env.get_version(mv).await;
    let wrong = env
        .action(
            mv,
            "acknowledge-license",
            json!({"license_sha256": "00".repeat(32)}),
        )
        .await;
    assert_eq!(wrong.status, 409);
    assert_eq!(wrong.json()["code"], "MRL-MODEL-266");
    let ok = env
        .action(
            mv,
            "acknowledge-license",
            json!({"license_sha256": v["license"]["sha256"]}),
        )
        .await;
    assert_eq!(ok.status, 200, "{}", ok.text());
    assert_eq!(ok.json()["license"]["acknowledged"], true);
    let stale = env
        .action(mv, "approve-remote-code", json!({"revision": "main"}))
        .await;
    assert_eq!(stale.status, 409);
    assert_eq!(stale.json()["code"], "MRL-MODEL-266");
    let approved = env
        .action(mv, "approve-remote-code", json!({"revision": COMMIT}))
        .await;
    assert_eq!(approved.status, 200, "{}", approved.text());
    assert_eq!(approved.json()["remote_code_approved"], true);
    assert_eq!(approved.json()["remote_code_approved_by"], "local-owner");
    let id = ModelVersionId::from_uuid(mv);
    let actions = env.audit.actions();
    assert!(actions.contains(&(
        "models.approve_remote_code".into(),
        format!("{id}@{COMMIT}")
    )));
    assert!(actions
        .iter()
        .any(|(a, t)| a == "models.acknowledge_license" && t.starts_with(&id.to_string())));
    // Approving again is a no-op: one audit row.
    env.action(mv, "approve-remote-code", json!({"revision": COMMIT}))
        .await;
    assert_eq!(
        env.audit
            .actions()
            .iter()
            .filter(|(a, _)| a == "models.approve_remote_code")
            .count(),
        1
    );
    assert_eq!(
        gates.validate(&env.spec(mv)).await.unwrap(),
        Verdict::Abstain
    );

    let plan = env.launch(Uuid::nil(), mv).await.unwrap();
    assert_eq!(plan.inputs.len(), 5);
    for input in &plan.inputs {
        assert_eq!(input.name, MODEL_INPUT);
        let path = input.local_path.strip_prefix("model/").unwrap();
        assert_eq!(input.sha256, out.sha(&format!("model/{path}")), "{path}");
        assert!(
            input.key.ends_with(&format!("/outputs/model/{path}")),
            "{}",
            input.key
        );
    }
    let spec: Value = serde_json::from_str(&plan.launch.job_spec_json).unwrap();
    assert_eq!(
        spec["base_model_sha256"],
        out.sha("model/model.safetensors")
    );
    // A run of another org never stages it.
    let foreign = env.launch(Uuid::now_v7(), mv).await;
    assert!(matches!(foreign, Err(LaunchError::Fail(..))), "{foreign:?}");
}

/// Refused models, broken outputs and ended jobs leave the version INVALID with a reason
/// and fail the job with that code; inference-only versions can't be trained.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn refused_or_broken_outputs_make_the_version_invalid() {
    let env = Env::start().await;
    let m = env.model("refused").await;
    let failed = |v: Verification| match v {
        Verification::Failed(r) => r.code,
        other => panic!("expected a failure, got {other:?}"),
    };

    // The task refused pickle-only weights.
    let (pickle, job) = env.version(&m, None).await;
    let refusal = Outputs {
        files: vec![(
            "inspection.json".into(),
            json!({"error": {"code": "MRL-MODEL-001", "message": "pickle only"}})
                .to_string()
                .into_bytes(),
        )],
    };
    let produced = env.produce(&refusal).await;
    let got = outputs::record(&env.models, job, Some(&produced))
        .await
        .unwrap();
    assert_eq!(failed(got.clone()), "MRL-MODEL-001");
    assert_eq!(
        outputs::record(&env.models, job, None).await.unwrap(),
        got,
        "replay"
    );
    let v = env.get_version(pickle).await;
    assert_eq!(v["state"], "INVALID");
    assert_eq!(v["invalid_reason"]["code"], "MRL-MODEL-001");

    // Pickle files among the outputs are refused even when the report misses them.
    let (sneaky, job) = env.version(&m, None).await;
    let mut out = Outputs::tiny(|_| {});
    out.files
        .push(("model/pytorch_model.bin".into(), b"pickle".to_vec()));
    let produced = env.produce(&out).await;
    let got = outputs::record(&env.models, job, Some(&produced))
        .await
        .unwrap();
    assert_eq!(failed(got), "MRL-MODEL-001");
    assert_eq!(env.get_version(sneaky).await["state"], "INVALID");

    // An adapter needs a READY base.
    let (_, job) = env.version(&m, None).await;
    let produced = env
        .produce(&Outputs::tiny(|r| r["kind"] = json!("adapter")))
        .await;
    let got = outputs::record(&env.models, job, Some(&produced))
        .await
        .unwrap();
    assert_eq!(failed(got), "MRL-MODEL-264");

    // A report whose stored bytes don't match its hash.
    let (_, job) = env.version(&m, None).await;
    let produced = env.produce(&Outputs::tiny(|_| {})).await;
    let st = env.store.org_store("local").await.unwrap();
    st.put(
        &format!("{}inspection.json", produced.prefix),
        br#"{"kind": "full_weights"}"#.to_vec().into(),
    )
    .await
    .unwrap();
    let got = outputs::record(&env.models, job, Some(&produced))
        .await
        .unwrap();
    assert_eq!(failed(got), "MRL-MODEL-150");

    // No outputs at all, or a job that ended first.
    let (_, job) = env.version(&m, None).await;
    assert_eq!(
        failed(outputs::record(&env.models, job, None).await.unwrap()),
        "MRL-MODEL-150"
    );
    let (gone, job) = env.version(&m, None).await;
    ended(&env, job, "The import job was cancelled.").await;
    let v = env.get_version(gone).await;
    assert_eq!(
        v["invalid_reason"]["message"],
        "The import job was cancelled."
    );
    let produced = env.produce(&Outputs::tiny(|_| {})).await;
    assert_eq!(
        failed(
            outputs::record(&env.models, job, Some(&produced))
                .await
                .unwrap()
        ),
        "MRL-MODEL-150",
        "late outputs don't revive it"
    );
    // A job that builds no version: not ours.
    assert_eq!(
        outputs::record(&env.models, Uuid::now_v7(), None)
            .await
            .unwrap(),
        Verification::Abstain
    );

    // INVALID versions are unknown to runs; inference-only ones are refused.
    let gates = ModelGates::new(env.db.db().clone());
    assert_eq!(
        gates.validate(&env.spec(pickle)).await.unwrap(),
        Verdict::Abstain
    );
    let (infer, _) = env
        .ready("inference", |r| r["kind"] = json!("inference_only"))
        .await;
    let verdict = gates.validate(&env.spec(infer)).await.unwrap();
    assert!(
        codes(&verdict).contains(&"MRL-MODEL-151".to_owned()),
        "{verdict:?}"
    );
    // Acting on a version that is not READY.
    let r = env.action(pickle, "acknowledge-license", json!({})).await;
    assert_eq!(r.status, 409);
    assert_eq!(r.json()["code"], "MRL-MODEL-263");
}

/// Malformed ids, unknown actions and bad names are refused. (Other orgs' models are 404
/// in team mode: the role table in `supabase/tests_core.rs` checks it; the solo owner sees
/// every local org by design.)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn bad_ids_actions_and_names_are_refused() {
    let env = Env::start().await;
    let (mv, _) = env.ready("mine", |_| {}).await;
    let r = env.call(Method::GET, "/model-versions/mv_nope", None).await;
    assert!(matches!(r.status, 400 | 404 | 422), "{}", r.text());
    let r = env.action(mv, "delete", json!({})).await;
    assert_eq!(r.status, 404);
    let r = env
        .action(
            mv,
            "acknowledge-license",
            json!({"license_sha256": "ab", "extra": 1}),
        )
        .await;
    assert_eq!(r.status, 422, "{}", r.text());
    let r = env
        .call(
            Method::POST,
            &format!("/projects/{}/models", env.project),
            Some(json!({"name": "../etc"})),
        )
        .await;
    assert_eq!(r.status, 422, "{}", r.text());
}

// Milestone 2: the jobs behind the routes. INT-6 is played by the test: it registers the
// outputs the task would write as the job's artifact, exactly where the sink puts them.

impl Env {
    /// A finished upload of `bytes`.
    async fn upload(&self, name: &str, bytes: &[u8]) -> String {
        let r = self
            .call(
                Method::POST,
                "/uploads",
                Some(json!({"filename": name, "size": bytes.len(), "sha256": sha256_hex(bytes)})),
            )
            .await;
        assert_eq!(r.status, 201, "{}", r.text());
        let up = r.json()["id"].as_str().unwrap().to_owned();
        let put = self
            .server
            .request(
                Method::PUT,
                &format!("/api/v1/uploads/{up}/parts/0"),
                &[
                    (LAUNCH_SECRET_HEADER, TEST_SECRET),
                    ("content-type", "application/octet-stream"),
                ],
                bytes.to_vec(),
            )
            .await;
        assert!(put.status < 300, "{}", put.text());
        let done = self
            .call(
                Method::POST,
                &format!("/uploads/{up}:complete"),
                Some(json!({})),
            )
            .await;
        assert!(done.status < 300, "{}", done.text());
        up
    }

    /// `(task, inputs, files, grants)` of built-in job `job`.
    async fn builtin(&self, job: Uuid) -> (String, Value, Value, Value) {
        sqlx::query_as("SELECT task, inputs, files, grants FROM builtin_jobs WHERE job_id = $1")
            .bind(job)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }

    async fn job_row(&self, job: Uuid) -> JobRow {
        sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }

    /// What INT-6 does once the job's outputs are uploaded: one artifact, on the job's row.
    async fn register_outputs(&self, job: Uuid, out: &Outputs) {
        let produced = self.produce(out).await;
        let size: usize = out.files.iter().map(|(_, b)| b.len()).sum();
        sqlx::query(
            "INSERT INTO artifacts (id, org_id, project_id, name, kind, prefix, manifest, size_bytes, state, created_by)
             VALUES ($1, 'local', $2, $3, 'model', $4, $5, $6, 'COMMITTED', 'system')",
        )
        .bind(produced.artifact_id)
        .bind(self.project.to_string())
        .bind(format!("outputs-{job}"))
        .bind(&produced.prefix)
        .bind(&produced.manifest)
        .bind(size as i64)
        .execute(self.db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE builtin_jobs SET artifact_id = $2 WHERE job_id = $1")
            .bind(job)
            .bind(produced.artifact_id)
            .execute(self.db.pool())
            .await
            .unwrap();
    }

    async fn verify(&self, job: Uuid) -> Verification {
        self.try_verify(job).await.unwrap()
    }

    /// The verifier's answer; `Err` means "wait" (the job stays FINALIZING).
    async fn try_verify(&self, job: Uuid) -> Result<Verification, SeamError> {
        ModelOutputs::with(self.models.clone())
            .verify(&self.job_row(job).await)
            .await
    }

    /// POSTs `body` to `path` and expects 202: the new version (or export job).
    async fn started(&self, path: &str, body: Value) -> Value {
        let r = self.call(Method::POST, path, Some(body)).await;
        assert_eq!(r.status, 202, "{path}: {}", r.text());
        r.json()
    }

    async fn edge(&self, from: (&str, Uuid), relation: &str, to: (&str, Uuid)) -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM lineage_edges WHERE from_kind = $1 AND from_id = $2
                AND relation = $3 AND to_kind = $4 AND to_id = $5)",
        )
        .bind(from.0)
        .bind(from.1)
        .bind(relation)
        .bind(to.0)
        .bind(to.1)
        .fetch_one(self.db.pool())
        .await
        .unwrap()
    }
}

fn uuid_of(v: &Value, key: &str) -> Uuid {
    let text = v[key].as_str().unwrap_or_else(|| panic!("{key} in {v}"));
    let (_, rest) = text.split_once('_').unwrap();
    mrl_domain::ids::JobId::from_str(&format!("job_{rest}"))
        .unwrap()
        .as_uuid()
}

/// Upload and Hub imports run as built-in IMPORT jobs: the files are staged as `model/…`, a
/// token secret goes in as a credential (never an input), and the job's registered outputs
/// make the version READY. A failed job leaves it INVALID.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn imports_run_as_builtin_jobs_and_their_outputs_make_the_version_ready() {
    let env = Env::start().await;
    let out = Outputs::tiny(|_| {});
    let mut files = serde_json::Map::new();
    for (path, bytes) in out.files.iter().filter(|(p, _)| p.starts_with("model/")) {
        let name = path.strip_prefix("model/").unwrap();
        files.insert(name.into(), env.upload(name, bytes).await.into());
    }
    let prj = format!("/projects/{}/models:import", env.project);
    let v = env
        .started(
            &prj,
            json!({"name": "uploaded", "source": {"kind": "upload", "files": files}}),
        )
        .await;
    assert_eq!(v["state"], "IMPORTING");
    assert_eq!(v["source_kind"], "upload");
    let (mv, job) = (uuid_of(&v, "id"), uuid_of(&v, "job_id"));
    let (task, inputs, staged, grants) = env.builtin(job).await;
    assert_eq!(task, "import_model");
    assert_eq!(inputs["source"], json!({"kind": "staged"}));
    assert_eq!(grants, json!([]));
    let staged = staged.as_array().unwrap();
    assert_eq!(staged.len(), 5);
    for f in staged {
        assert_eq!(f["name"], "model");
        let path = f["local_path"].as_str().unwrap();
        assert_eq!(f["sha256"], out.sha(path), "{path}");
    }
    assert_eq!(env.job_row(job).await.state, "QUEUED");

    // Not registered yet: wait. Registered: READY, files from the job's own outputs.
    assert!(
        env.try_verify(job).await.is_err(),
        "waits, never RetryLater"
    );
    env.register_outputs(job, &out).await;
    assert_eq!(env.verify(job).await, Verification::Verified);
    assert_eq!(env.verify(job).await, Verification::Verified, "replayed");
    let ready = env.get_version(mv).await;
    assert_eq!(ready["state"], "READY");
    assert_eq!(ready["weights_sha256"], out.sha("model/model.safetensors"));
    assert!(env.audit.actions().contains(&(
        "models.import".into(),
        ModelVersionId::from_uuid(mv).to_string()
    )));

    // A Hub import with a token: the secret is a grant, its id is not in the inputs.
    let secret = mrl_domain::ids::SecretId::new().to_string();
    let v = env
        .started(
            &prj,
            json!({"name": "hub", "source": {"kind": "hub", "repo": "org/tiny", "revision": "main", "secret_id": secret}}),
        )
        .await;
    let job = uuid_of(&v, "job_id");
    let (_, inputs, _, grants) = env.builtin(job).await;
    assert_eq!(
        inputs["source"],
        json!({"kind": "hub", "repo": "org/tiny", "revision": "main", "token_env": "MRL_CREDENTIAL_HF"})
    );
    assert!(!inputs.to_string().contains(&secret));
    assert_eq!(grants[0]["allowed_use"], "model_download");
    assert_eq!(grants[0]["name"], "hf");
    // The job fails (say the token was refused): the version is INVALID.
    let event = mrl_server::modules::events::Event {
        id: 1,
        event_id: Uuid::now_v7(),
        org_id: None,
        aggregate_kind: "job".into(),
        aggregate_id: job,
        aggregate_seq: 9,
        event_type: "job.failed".into(),
        schema_version: 1,
        payload: json!({}),
        trace_context: None,
        created_at: time::OffsetDateTime::now_utc(),
        xid: 1,
    };
    let mut tx = env.db.db().begin().await.unwrap();
    mrl_server::modules::events::EventConsumer::handle(&ModelJobEnds, &mut tx, &event)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let failed = env.get_version(uuid_of(&v, "id")).await;
    assert_eq!(failed["state"], "INVALID");
    assert_eq!(
        failed["invalid_reason"]["message"],
        "The job making this version failed."
    );

    // Hostile or broken requests.
    for (body, path) in [
        (
            json!({"name": "x", "source": {"kind": "hub", "repo": "../../etc"}}),
            "/source/repo",
        ),
        (
            json!({"name": "x", "source": {"kind": "upload", "files": {"../w.safetensors": "up_x"}}}),
            "/source/files/../w.safetensors",
        ),
        (
            json!({"name": "x", "source": {"kind": "upload", "files": {"pytorch_model.bin": "up_x"}}}),
            "/source/files/pytorch_model.bin",
        ),
        (
            json!({"name": "x", "source": {"kind": "node_path", "path": "relative/dir"}}),
            "/source/path",
        ),
    ] {
        let r = env.call(Method::POST, &prj, Some(body)).await;
        assert_eq!(r.status, 422, "{path}: {}", r.text());
    }
    let r = env
        .call(
            Method::POST,
            &prj,
            Some(json!({"name": "x", "source": {"kind": "upload", "files": {"model.safetensors": mrl_domain::ids::UploadId::new().to_string()}}})),
        )
        .await;
    assert_eq!(r.status, 404, "{}", r.text());
}

/// Promote → merge → export: a checkpoint (fully re-hashed first, pickle files left out)
/// becomes a version; a READY adapter merges into its base; the merged version exports, and
/// every step leaves its lineage edge. A staged blob changed after the fact fails its check.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs PostgreSQL (just test-integration)"]
async fn promote_merge_and_export_round_trip() {
    let env = Env::start().await;
    let (base, _) = env.ready("base", |_| {}).await;

    // A run of the project with one COMMITTED checkpoint (never re-hashed yet).
    let (exp, run, job, attempt, ckpt) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    let p = env.project.as_uuid();
    let st = env.store.org_store("local").await.unwrap();
    let prefix = format!("{}runs/{run}/checkpoints/{ckpt}/", st.keys().prefix());
    let ckpt_files = [
        (
            "model.safetensors",
            "weights",
            b"checkpoint weights".to_vec(),
        ),
        ("config.json", "config", b"{}".to_vec()),
        ("optimizer.pt", "optimizer", b"pickled state".to_vec()),
    ];
    let mut rows = String::new();
    for (path, role, bytes) in &ckpt_files {
        st.put(&format!("{prefix}{path}"), bytes.clone().into())
            .await
            .unwrap();
        rows.push_str(&format!(
            "INSERT INTO checkpoint_files (checkpoint_id, org_id, path, role, object_key, sha256, size_bytes)
             VALUES ('{ckpt}', '{n}', '{path}', '{role}', '{prefix}{path}', decode('{sha}', 'hex'), {len});",
            n = Uuid::nil(),
            sha = sha256_hex(bytes),
            len = bytes.len()
        ));
    }
    env.db
        .seed(&format!(
            "INSERT INTO experiments (id, org_id, project_id, name, created_by)
               VALUES ('{exp}', '{n}', '{p}', 'e', 'seed');
             INSERT INTO runs (id, org_id, project_id, experiment_id, name, kind, algorithm_id,
                 base_model_version_id, submit_key, submit_fingerprint, created_by)
               VALUES ('{run}', '{n}', '{p}', '{exp}', 'my sft run', 'TRAIN', 'sft', '{base}', '{run}', '', 'seed');
             INSERT INTO jobs (id, org_id, project_id, kind, run_id, state, resource_request, retry_policy, created_by)
               VALUES ('{job}', '{n}', '{p}', 'TRAIN', '{run}', 'COMPLETED', '{{}}', '{{}}', 'seed');
             INSERT INTO job_attempts (id, org_id, job_id, attempt_no, epoch, state)
               VALUES ('{attempt}', '{n}', '{job}', 1, 1, 'EXITED_OK');
             INSERT INTO checkpoints (id, org_id, storage_org, run_id, job_id, attempt_id, attempt_epoch,
                 global_step, kind, layout, state)
               VALUES ('{ckpt}', '{n}', 'local', '{run}', '{job}', '{attempt}', 1, 40, 'weights_only',
                 'hf-trainer-v1', 'COMMITTED');
             {rows}",
            n = Uuid::nil()
        ))
        .await;
    let ckpt_id = mrl_domain::ids::CheckpointId::from_uuid(ckpt);
    let v = env
        .started(&format!("/checkpoints/{ckpt_id}:promote"), json!({}))
        .await;
    assert_eq!(v["source_kind"], "checkpoint");
    assert_eq!(v["source_ref"]["checkpoint_id"], ckpt_id.to_string());
    assert_eq!(
        v["base_model_version_id"],
        ModelVersionId::from_uuid(base).to_string()
    );
    let level: Option<String> =
        sqlx::query_scalar("SELECT verification_level FROM checkpoints WHERE id = $1")
            .bind(ckpt)
            .fetch_one(env.db.pool())
            .await
            .unwrap();
    assert_eq!(level.as_deref(), Some("full_rehash"));
    let (promoted, job) = (uuid_of(&v, "id"), uuid_of(&v, "job_id"));
    let (_, _, staged, _) = env.builtin(job).await;
    let paths: Vec<&str> = staged
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["local_path"].as_str().unwrap())
        .collect();
    assert_eq!(
        paths,
        ["model/config.json", "model/model.safetensors"],
        "no pickles"
    );
    env.register_outputs(job, &Outputs::tiny(|_| {})).await;
    assert_eq!(env.verify(job).await, Verification::Verified);
    let model = env.get_version(promoted).await;
    assert_eq!(model["state"], "READY");
    let name = env
        .call(
            Method::GET,
            &format!("/models/{}", model["model_id"].as_str().unwrap()),
            None,
        )
        .await
        .json()["name"]
        .clone();
    assert_eq!(name, "my-sft-run");
    assert!(
        env.edge(("ckpt", ckpt), "promoted_to", ("mv", promoted))
            .await
    );
    // A checkpoint whose stored bytes changed since commit is not promoted.
    st.put(
        &format!("{prefix}model.safetensors"),
        b"changed bytes!!!!!".to_vec().into(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE checkpoints SET verification_level = NULL WHERE id = $1")
        .bind(ckpt)
        .execute(env.db.pool())
        .await
        .unwrap();
    let r = env
        .call(
            Method::POST,
            &format!("/checkpoints/{ckpt_id}:promote"),
            Some(json!({})),
        )
        .await;
    assert!(r.status >= 400, "{}", r.text());

    // A READY adapter on the base merges into a new full-weights version.
    let lora = env.model("lora").await;
    let (adapter, job) = env.version(&lora, Some(base)).await;
    let produced = env
        .produce(&Outputs::tiny(|r| r["kind"] = json!("adapter")))
        .await;
    assert_eq!(
        outputs::record(&env.models, job, Some(&produced))
            .await
            .unwrap(),
        Verification::Verified
    );
    let adapter_id = ModelVersionId::from_uuid(adapter);
    let refused = env.action(base, "merge-adapter", json!({})).await;
    assert_eq!(refused.status, 409);
    assert_eq!(refused.json()["code"], "MRL-MODEL-264");
    let v = env
        .started(
            &format!("/model-versions/{adapter_id}:merge-adapter"),
            json!({"name": "merged", "dtype": "bfloat16"}),
        )
        .await;
    assert_eq!(v["source_kind"], "merge");
    assert_eq!(v["source_ref"]["adapter"], adapter_id.to_string());
    let (merged, job) = (uuid_of(&v, "id"), uuid_of(&v, "job_id"));
    let (task, inputs, staged, _) = env.builtin(job).await;
    assert_eq!(task, "merge_adapter");
    assert_eq!(inputs, json!({"dtype": "bfloat16"}));
    let staged = staged.as_array().unwrap();
    assert_eq!(staged.len(), 10);
    assert!(staged
        .iter()
        .any(|f| f["local_path"] == "base/model.safetensors"));
    assert!(staged
        .iter()
        .any(|f| f["local_path"] == "adapter/model.safetensors"));
    assert_eq!(env.job_row(job).await.kind, "MERGE_ADAPTER");
    env.register_outputs(job, &Outputs::tiny(|_| {})).await;
    assert_eq!(env.verify(job).await, Verification::Verified);
    assert_eq!(env.get_version(merged).await["kind"], "full_weights");
    assert!(
        env.edge(("mv", merged), "derived_from", ("mv", adapter))
            .await
    );
    assert!(env.edge(("mv", merged), "derived_from", ("mv", base)).await);

    // Export: only hf-safetensors; S3 and the Hub need a secret, handed over as a grant.
    for (body, code) in [
        (json!({"format": "gguf"}), "MRL-MODEL-261"),
        (
            json!({"target": {"kind": "s3", "url": "s3://b/p"}}),
            "MRL-VAL-004",
        ),
        (json!({"target": {"kind": "ftp"}}), "MRL-VAL-004"),
    ] {
        let r = env.action(merged, "export", body.clone()).await;
        assert_eq!(r.status, 422, "{body}: {}", r.text());
        assert_eq!(r.json()["code"], code, "{body}");
    }
    let secret = mrl_domain::ids::SecretId::new().to_string();
    let hub = env
        .started(
            &format!(
                "/model-versions/{}:export",
                ModelVersionId::from_uuid(merged)
            ),
            json!({"target": {"kind": "hf_hub", "repo": "me/merged", "secret_id": secret}}),
        )
        .await;
    let (_, inputs, _, grants) = env.builtin(uuid_of(&hub, "job_id")).await;
    assert_eq!(inputs["target"]["credential_env"], "MRL_CREDENTIAL_EXPORT");
    assert_eq!(inputs["target"]["private"], true);
    assert_eq!(grants[0]["allowed_use"], "model_export");
    let export = env
        .started(
            &format!(
                "/model-versions/{}:export",
                ModelVersionId::from_uuid(merged)
            ),
            json!({}),
        )
        .await;
    let job = uuid_of(&export, "job_id");
    let (task, inputs, staged, _) = env.builtin(job).await;
    assert_eq!(task, "export_model");
    assert_eq!(inputs["format"], "hf-safetensors");
    assert_eq!(inputs["target"], json!({"kind": "artifact"}));
    assert_eq!(inputs["card"]["name"], "merged");
    assert_eq!(staged.as_array().unwrap().len(), 5);
    assert!(
        env.try_verify(job).await.is_err(),
        "waits, never RetryLater"
    );
    let bundle = Outputs {
        files: vec![
            ("export/model.safetensors".into(), b"w".to_vec()),
            (
                "export-result.json".into(),
                br#"{"kind": "artifact"}"#.to_vec(),
            ),
        ],
    };
    env.register_outputs(job, &bundle).await;
    assert_eq!(env.verify(job).await, Verification::Verified);
    let artifact: Uuid =
        sqlx::query_scalar("SELECT artifact_id FROM builtin_jobs WHERE job_id = $1")
            .bind(job)
            .fetch_one(env.db.pool())
            .await
            .unwrap();
    assert!(
        env.edge(("mv", merged), "exported_as", ("art", artifact))
            .await
    );

    // A run of the merged version stages its files with their hashes; a blob changed later
    // fails that check (the agent downloads with the expected sha256).
    let plan = env.launch(Uuid::nil(), merged).await;
    let plan = match plan {
        Ok(p) => p,
        Err(e) => panic!("{e:?}"),
    };
    let input = &plan.inputs[0];
    let dir = tempfile::tempdir().unwrap();
    st.download_file(
        &input.key,
        dir.path().join("ok"),
        Some(&input.sha256),
        Default::default(),
    )
    .await
    .unwrap();
    st.put(&input.key, b"tampered".to_vec().into())
        .await
        .unwrap();
    assert!(st
        .download_file(
            &input.key,
            dir.path().join("bad"),
            Some(&input.sha256),
            Default::default(),
        )
        .await
        .is_err());
}
