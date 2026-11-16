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
        // </mrl:consumers>
    ]
}

/// Every registered module, in startup order. `events` comes first: it applies migrations.
pub fn modules(db: &Db) -> Vec<Box<dyn ServerModule>> {
    vec![
        // <mrl:register>
        Box::new(EventsModule::new(db.clone(), consumers(db))),
        // </mrl:register>
    ]
}
