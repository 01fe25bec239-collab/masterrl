//! Engine test helpers (P24), compiled by `mrl-testkit` as `mrl_testkit::engines` (see its
//! `lib.rs`, P24 marker), not by this crate.
//!
//! - [`TinyFixture`]: model and dataset records for the P03 tiny fixtures (the CPU
//!   `tiny-llama` model, `tests/fixtures/models/GOLDEN.json`, and the
//!   `tests/fixtures/datasets/sft.messages.v1.jsonl` dataset), so an SFT run on them can pass
//!   L0 before the wave 6 registries exist.
//! - [`start_server`]: a solo server with the nodes, engines and runs modules, whose L0 check
//!   reads the given records.

use crate::agent::gateway::{fast_config, server_config};
use crate::pg::TestDb;
use crate::seams::InMemorySecretStore;
use crate::server::{TestServer, TEST_SECRET};
use async_trait::async_trait;
use mrl_domain::capability::matcher::{DatasetFacts, ModelFacts};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId};
use mrl_server::app::ServerModule;
use mrl_server::config::Mode;
use mrl_server::modules::engines::{
    CapabilityValidator, EnginesConfig, EnginesModule, Records, RegistryRecords,
};
use mrl_server::modules::jobs::{Chains, SchemaChecks};
use mrl_server::modules::nodes::lanes::LaneRouter;
use mrl_server::modules::nodes::NodesModule;
use mrl_server::modules::runs::RunsModule;
use mrl_server::seams::SeamError;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

const TINY_MODEL: &str = include_str!("../../../../../tests/fixtures/models/GOLDEN.json");
const TINY_DATASET: &str =
    include_str!("../../../../../tests/fixtures/datasets/sft.messages.v1.jsonl");
/// Schema of the tiny dataset fixture.
pub const TINY_DATASET_SCHEMA: &str = "sft.messages.v1";

/// One model version and one dataset version that stand for the P03 tiny fixtures (new ids
/// each time). Any org may read them; other ids are unknown.
#[derive(Debug, Clone, Copy)]
pub struct TinyFixture {
    /// The tiny-llama model version.
    pub model: ModelVersionId,
    /// The `sft.messages.v1` dataset version.
    pub dataset: DatasetVersionId,
}

impl Default for TinyFixture {
    fn default() -> Self {
        Self {
            model: ModelVersionId::new(),
            dataset: DatasetVersionId::new(),
        }
    }
}

impl TinyFixture {
    /// What the model registry would know about tiny-llama, read from its `GOLDEN.json`.
    pub fn model_facts() -> ModelFacts {
        let g: Value = serde_json::from_str(TINY_MODEL).expect("GOLDEN.json reads");
        let architecture = g["architecture"].as_str().unwrap_or_default();
        assert!(
            architecture.ends_with("ForCausalLM"),
            "tiny fixture is a decoder, got {architecture}"
        );
        ModelFacts {
            family: "decoder".into(),
            is_moe: false,
            params: g["param_count"].as_f64(),
            has_chat_template: g["chat_template_sha256"].is_string(),
            requires_remote_code: false,
        }
    }

    /// What the dataset registry would know about the tiny `sft.messages.v1` dataset.
    pub fn dataset_facts() -> DatasetFacts {
        DatasetFacts {
            schema: TINY_DATASET_SCHEMA.into(),
            rows: Some(
                TINY_DATASET
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .count() as u64,
            ),
            streaming: false,
        }
    }

    /// These records as the engines module's lookups.
    pub fn records(self) -> Records {
        Records::new(vec![Arc::new(self)])
    }
}

#[async_trait]
impl RegistryRecords for TinyFixture {
    async fn model(&self, _: Uuid, id: ModelVersionId) -> Result<Option<ModelFacts>, SeamError> {
        Ok((id == self.model).then(Self::model_facts))
    }

    async fn dataset(
        &self,
        _: Uuid,
        id: DatasetVersionId,
    ) -> Result<Option<DatasetFacts>, SeamError> {
        Ok((id == self.dataset).then(Self::dataset_facts))
    }
}

/// A solo server on `db` with the nodes, engines and runs modules; the job chain is the
/// schema checks plus the L0 capability check reading `records`. `extra` are more bundle
/// texts to register. Keep `secrets` across restarts on the same `db`: it holds the
/// gateway CA.
pub async fn start_server(
    db: &TestDb,
    secrets: &Arc<InMemorySecretStore>,
    engines: EnginesConfig,
    extra: Vec<String>,
    records: Records,
) -> Result<TestServer, String> {
    let mut gateway = fast_config();
    // The test agent reports this host's real free disk: a full laptop disk would mark it
    // DEGRADED (`disk_low`) and probes only go to ONLINE machines.
    gateway.min_disk_free_bytes = 0;
    let mut cfg = server_config(gateway, Mode::Solo, 0);
    cfg.engines = engines.clone();
    let lanes = LaneRouter::new();
    let nodes = NodesModule::new(db.db().clone(), &cfg, secrets.clone(), lanes.clone());
    let gw = nodes.gateway();
    let validator = CapabilityValidator::with_records(db.db().clone(), records.clone());
    let chains = Chains::new(
        vec![Arc::new(SchemaChecks), Arc::new(validator)],
        vec![],
        vec![],
    );
    let modules: Vec<Box<dyn ServerModule>> = vec![
        Box::new(nodes),
        Box::new(EnginesModule::with(
            db.db().clone(),
            engines,
            &gw,
            &lanes,
            extra,
            records,
        )),
        Box::new(RunsModule::with_chains(db.db().clone(), chains)),
    ];
    let seams = crate::seams::build(crate::seams::solo(TEST_SECRET), Mode::Solo);
    TestServer::start(cfg, seams, modules)
        .await
        .map_err(|e| e.explain())
}
