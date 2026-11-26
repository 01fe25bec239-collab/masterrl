//! A solo core for wave-exit runs until the Wave 6 model and dataset registries exist: the
//! production modules (`registry::modules`), except that run validation and the engines
//! module also know two fixture records, the tiny model (`tests/fixtures/models`) and the tiny
//! `sft.messages.v1` dataset. Their ids are printed at start; set `MRL_EXIT_MODEL` and
//! `MRL_EXIT_DATASET` to keep them across restarts.
//!
//! ```text
//! MRL_DATABASE__URL=postgres://… MRL_LAUNCH_SECRET=s MRL_ENGINES__DEV_ENGINES=true \
//!   MRL_KEYCHAIN__BACKEND=file MRL_PGBUNDLE__DATA_DIR=<scratch dir> \
//!   cargo run -p mrl-testkit --example exit_core
//! ```
//! Then use the `mrl` CLI and a real `mrl-agent` against the printed ports. With the argument
//! `invariants`, it prints P21's invariant check of the database instead and exits. A TRL run uses
//! `trl-conformance` (see [`FixtureLaunch`]), so the agent's state folder must sit inside
//! this checkout, with `tests/fixtures/models/.cache` made (`tools/fixtures/make_fixtures.py`).

use async_trait::async_trait;
use mrl_domain::capability::matcher::{DatasetFacts, ModelFacts};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId};
use mrl_server::modules::alerts::AlertsModule;
use mrl_server::modules::artifacts::ArtifactsModule;
use mrl_server::modules::audit::AuditModule;
use mrl_server::modules::auth::{production_seams, AuthModule};
use mrl_server::modules::checkpoints::{CheckpointsModule, FinalCheckpointVerifier};
use mrl_server::modules::engines::{
    CapabilityValidator, EnginesModule, RecordScope, Records, RegistryRecords,
};
use mrl_server::modules::events::EventsModule;
use mrl_server::modules::jobs::{AttemptRow, Chains, JobRow, Projects, SchemaChecks};
use mrl_server::modules::keychain::KeychainStore;
use mrl_server::modules::nodes::lanes::LaneRouter;
use mrl_server::modules::nodes::NodesModule;
use mrl_server::modules::runs::RunsModule;
use mrl_server::modules::scheduler::launch::Transfer;
use mrl_server::modules::scheduler::pipeline::{
    LaunchContributor, LaunchError, LaunchPlan, Placement,
};
use mrl_server::modules::scheduler::SchedulerModule;
use mrl_server::modules::streams::StreamsModule;
use mrl_server::modules::sync::SyncModule;
use mrl_server::modules::telemetry::TelemetryModule;
use mrl_server::modules::tenancy::TenancyModule;
use mrl_server::registry;
use mrl_server::seams::SeamError;
use mrl_server::{config::Overrides, App, Config, Db, ServerModule};
use mrl_testkit::engines::TinyFixture;
use std::str::FromStr;
use std::sync::Arc;

/// The tiny fixtures, visible in every project.
struct Fixtures(TinyFixture);

#[async_trait]
impl RegistryRecords for Fixtures {
    async fn model(
        &self,
        _: &RecordScope,
        id: ModelVersionId,
    ) -> Result<Option<ModelFacts>, SeamError> {
        Ok((id == self.0.model).then(TinyFixture::model_facts))
    }

    async fn dataset(
        &self,
        _: &RecordScope,
        id: DatasetVersionId,
    ) -> Result<Option<DatasetFacts>, SeamError> {
        Ok((id == self.0.dataset).then(TinyFixture::dataset_facts))
    }
}

/// Stands in for the Wave 6 model registry at launch. A run on the fixture model gets the
/// fixture's weights hash, and a `trl` run uses the adapter's `trl-conformance` engine, which
/// stages the fixture model and dataset from the checkout its environment was built from.
struct FixtureLaunch {
    model: ModelVersionId,
}

#[async_trait]
impl LaunchContributor for FixtureLaunch {
    fn name(&self) -> &'static str {
        "exit_fixture"
    }

    async fn contribute(
        &self,
        _job: &JobRow,
        _attempt: &AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        let mut spec: serde_json::Value =
            serde_json::from_str(&plan.launch.job_spec_json).unwrap_or_default();
        if spec["base_model_version_id"] != self.model.to_string() {
            return Ok(());
        }
        let golden: serde_json::Value =
            serde_json::from_str(include_str!("../../../tests/fixtures/models/GOLDEN.json"))
                .map_err(|e| LaunchError::Transient(format!("GOLDEN.json: {e}")))?;
        spec["base_model_sha256"] =
            golden["weights_sha256"]["darwin-arm64"]["model.safetensors"].clone();
        plan.launch.job_spec_json = spec.to_string();
        if plan.launch.engine_name == "trl" {
            plan.launch.engine_name = "trl-conformance".into();
        }
        Ok(())
    }
}

fn id_from_env<T: FromStr>(name: &str, new: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(new)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let env: Vec<(String, String)> = std::env::vars().collect();
    let config =
        Config::load(None, env.iter().cloned(), &Overrides::default()).map_err(|e| e.explain())?;
    let telemetry = mrl_server::telemetry::init(&config.telemetry).map_err(|e| e.explain())?;
    let url = config
        .database
        .url
        .clone()
        .ok_or("set MRL_DATABASE__URL (the dev stack's PostgreSQL)")?;
    let db = Db::connect_lazy(
        &config.database,
        url.parse().map_err(|_| "bad database url")?,
    );
    if std::env::args().nth(1).as_deref() == Some("invariants") {
        // P21's invariant checker over this database (P31 adds the operator command).
        let found = mrl_server::modules::scheduler::invariants::check(&db)
            .await
            .map_err(|e| format!("the database did not answer: {e}"))?;
        println!("{} invariant violations", found.len());
        for v in &found {
            println!("  {v}");
        }
        return if found.is_empty() {
            Ok(())
        } else {
            Err("invariants violated".into())
        };
    }
    let secret = std::env::var("MRL_LAUNCH_SECRET").map_err(|_| "set MRL_LAUNCH_SECRET")?;

    let fixture = TinyFixture {
        model: id_from_env("MRL_EXIT_MODEL", ModelVersionId::new()),
        dataset: id_from_env("MRL_EXIT_DATASET", DatasetVersionId::new()),
    };
    let records = Records::new(vec![Arc::new(Fixtures(fixture))]);
    // ponytail: mirrors `Chains::registered` with the fixture records; update it when a part
    // adds a validator or verifier there.
    let chains = Chains::new(
        vec![
            Arc::new(SchemaChecks),
            Arc::new(CapabilityValidator::with_records(
                db.clone(),
                records.clone(),
            )),
        ],
        vec![Arc::new(FinalCheckpointVerifier::new(db.clone()))],
        vec![],
    );
    let gateway = NodesModule::from_config(db.clone(), &config).gateway();
    let lanes = LaneRouter::global();
    let pipeline = mrl_server::modules::scheduler::registry::pipeline(&db, &config.scheduler)
        .with_launch(Arc::new(FixtureLaunch {
            model: fixture.model,
        }));
    // ponytail: `registry::modules` with three members swapped. Built here rather than swapped
    // afterwards: the scheduler and engines modules register lane sinks when constructed.
    // Keep in step with `registry::modules`.
    let modules: Vec<Box<dyn ServerModule>> = vec![
        Box::new(EventsModule::new(db.clone(), registry::consumers(&db))),
        Box::new(TenancyModule::new(db.clone(), config.server.mode)),
        Box::new(AuditModule::new(db.clone())),
        Box::new(AuthModule::new(db.clone(), &config)),
        Box::new(SyncModule::new(db.clone(), &config.auth)),
        Box::new(
            RunsModule::with_chains(db.clone(), chains.clone())
                .with_projects(Projects::registered(&db)),
        ),
        Box::new(NodesModule::from_config(db.clone(), &config)),
        Box::new(ArtifactsModule::new(db.clone(), &config)),
        Box::new(SchedulerModule::with_parts(
            db.clone(),
            config.scheduler.clone(),
            &gateway,
            &lanes,
            chains.clone(),
            pipeline,
            Transfer::from_config(&db, &config),
        )),
        Box::new(TelemetryModule::new(db.clone(), &config)),
        Box::new(StreamsModule::new(db.clone(), config.live.clone())),
        Box::new(AlertsModule::new(db.clone())),
        Box::new(CheckpointsModule::new(db.clone(), &config)),
        Box::new(EnginesModule::with(
            db.clone(),
            config.engines.clone(),
            &gateway,
            &lanes,
            Vec::new(),
            records.clone(),
        )),
    ];
    // The server's own seams and keychain, as in `mrl-server`'s main: checkpoints sign transfer
    // links with the keychain's key, which the transfer endpoint checks.
    let secrets_dir = mrl_server::pgbundle::data_dir(&config.pgbundle)
        .map_err(|e| e.explain())?
        .join("secrets");
    let store: Arc<dyn mrl_server::seams::SecretStore> = Arc::new(
        KeychainStore::open(
            &config.auth.keychain_service,
            &secrets_dir,
            config.keychain.backend,
        )
        .await,
    );
    let seams = production_seams(&config, &db, store, &secret)
        .await
        .and_then(|b| b.build(config.server.mode))
        .map_err(|e| e.explain())?;
    let running = App::new(config, seams, modules)
        .start()
        .await
        .map_err(|e| e.explain())?;
    println!(
        "{}",
        serde_json::json!({
            "event": "listening",
            "port": running.api_addr().map(|a| a.port()),
            "grpc_port": running.grpc_addr().map(|a| a.port()),
            "model": fixture.model.to_string(),
            "dataset": fixture.dataset.to_string(),
        })
    );
    let token = running.shutdown_token();
    tokio::select! {
        () = mrl_server::app::shutdown_signal() => token.cancel(),
        () = token.cancelled() => {},
    }
    let result = running.wait().await.map_err(|e| e.explain());
    telemetry.shutdown();
    result
}
