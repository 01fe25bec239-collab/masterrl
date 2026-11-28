//! The artifact store and settings the rewards module's chain members use.
//!
//! The completion verifier ([`super::verifier::ReportVerifier`]) and the launch contributor
//! ([`super::launch::RewardInputs`]) are built by shared registries that only hand them the
//! database, but both read object storage. [`super::RewardsModule::new`] registers its store
//! and settings here, keyed by the database it serves (host, port and name), so several
//! servers in one process (tests) never read each other's storage.

use super::RewardsConfig;
use crate::db::Db;
use crate::modules::artifacts::ArtifactStore;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

/// What a registered rewards module shares with its chain members.
#[derive(Clone)]
pub struct Shared {
    /// Object storage (reports, code bundles).
    pub store: Arc<ArtifactStore>,
    /// The `[rewards]` settings.
    pub config: RewardsConfig,
}

static SHARED: LazyLock<RwLock<HashMap<String, Shared>>> = LazyLock::new(Default::default);

fn key(db: &Db) -> String {
    let o = db.pool().connect_options();
    format!(
        "{}:{}/{}",
        o.get_host(),
        o.get_port(),
        o.get_database().unwrap_or_default()
    )
}

/// Registers `shared` for `db` (the latest registration wins).
pub fn register(db: &Db, shared: Shared) {
    SHARED
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key(db), shared);
}

/// What was registered for `db`, if a rewards module serves it.
pub fn lookup(db: &Db) -> Option<Shared> {
    SHARED
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key(db))
        .cloned()
}
