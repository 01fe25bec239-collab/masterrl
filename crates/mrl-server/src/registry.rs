//! The list of modules this build registers. Each part adds one line inside the block,
//! like `Box::new(crate::modules::runs::RunsModule::new(db.clone())),`.
//!
//! Event consumers go inside `// <mrl:consumers>`, like
//! `Arc::new(crate::modules::runs::RunSummary::new(db.clone())),`.

use crate::app::ServerModule;
use crate::db::Db;
use crate::modules::events::{EventConsumer, EventsModule};
use std::sync::Arc;

/// Every event consumer, delivered to by the `events` dispatcher.
pub fn consumers(db: &Db) -> Vec<Arc<dyn EventConsumer>> {
    let _ = db;
    vec![
        // <mrl:consumers>
        // <mrl:consumers P16> P16 consumers go right below this line.

        // <mrl:consumers P17> P17 consumers go right below this line.

        // <mrl:consumers P18> P18 consumers go right below this line.

        // <mrl:consumers P19> P19 consumers go right below this line.
        // <mrl:consumers P21> P21 consumers go right below this line.
        Arc::new(crate::modules::scheduler::Wake),
        // <mrl:consumers P22> P22 consumers go right below this line.
        Arc::new(crate::modules::alerts::NodeHealthAlerts),
        // <mrl:consumers P23> P23 consumers go right below this line.
        // <mrl:consumers P24> P24 consumers go right below this line.
        // <mrl:consumers P26> P26 consumers go right below this line.
        // <mrl:consumers P27> P27 consumers go right below this line.
        // <mrl:consumers P28> P28 consumers go right below this line.
        // <mrl:consumers P29> P29 consumers go right below this line.
        // <mrl:consumers P30> P30 consumers go right below this line.
        // </mrl:consumers>
    ]
}

/// Every registered module, in startup order. `events` comes first: it applies migrations.
/// Modules read their own settings from `config`.
pub fn modules(db: &Db, config: &crate::Config) -> Vec<Box<dyn ServerModule>> {
    let _ = config;
    vec![
        // <mrl:register>
        Box::new(EventsModule::new(db.clone(), consumers(db))),
        Box::new(crate::modules::builtin::BuiltinModule::new(
            db.clone(),
            config,
        )),
        // <mrl:register P16> P16 modules go right below this line.
        Box::new(crate::modules::tenancy::TenancyModule::new(
            db.clone(),
            config.server.mode,
        )),
        Box::new(crate::modules::audit::AuditModule::new(db.clone())),
        Box::new(crate::modules::auth::AuthModule::new(db.clone(), config)),
        Box::new(crate::modules::sync::SyncModule::new(
            db.clone(),
            &config.auth,
        )),
        // <mrl:register P17> P17 modules go right below this line.
        Box::new(crate::modules::runs::RunsModule::new(db.clone(), config)),
        // <mrl:register P18> P18 modules go right below this line.
        Box::new(crate::modules::nodes::NodesModule::from_config(
            db.clone(),
            config,
        )),
        // <mrl:register P19> P19 modules go right below this line.
        Box::new(crate::modules::artifacts::ArtifactsModule::new(
            db.clone(),
            config,
        )),
        // <mrl:register P21> P21 modules go right below this line.
        Box::new(crate::modules::scheduler::SchedulerModule::new(
            db.clone(),
            config,
        )),
        // <mrl:register P22> P22 modules go right below this line.
        Box::new(crate::modules::telemetry::TelemetryModule::new(
            db.clone(),
            config,
        )),
        Box::new(crate::modules::streams::StreamsModule::new(
            db.clone(),
            config.live.clone(),
        )),
        Box::new(crate::modules::alerts::AlertsModule::new(db.clone())),
        // <mrl:register P23> P23 modules go right below this line.
        Box::new(crate::modules::checkpoints::CheckpointsModule::new(
            db.clone(),
            config,
        )),
        // <mrl:register P24> P24 modules go right below this line.
        Box::new(crate::modules::engines::EnginesModule::new(
            db.clone(),
            config,
        )),
        // <mrl:register P26> P26 modules go right below this line.
        // <mrl:register P27> P27 modules go right below this line.
        // <mrl:register P28> P28 modules go right below this line.
        // <mrl:register P29> P29 modules go right below this line.
        // <mrl:register P30> P30 modules go right below this line.
        Box::new(crate::modules::desktop::DesktopModule::new(
            db.clone(),
            config,
        )),
        // </mrl:register>
    ]
}
