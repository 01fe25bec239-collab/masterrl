//! Validates a run's reward pin and records its lineage.

use super::store;
use crate::db::{Db, Tx};
use crate::error::{ApiError, ErrorCode, FieldError};
use crate::modules::artifacts::{Error, NodeLookup, LOCAL_ORG};
use crate::modules::jobs::{scope, ResolveError, SubmissionResolver, SubmitScope};
use crate::modules::lineage::{LineageWriter, NodeRef, Relation};
use crate::seams::{Resource, SeamError};
use async_trait::async_trait;
use mrl_domain::ids::RewardId;
use serde_json::Value;
use uuid::Uuid;

/// Owns submission and lineage checks for reward versions.
pub struct RewardVersions(Db);

impl RewardVersions {
    /// Uses the core's reward records.
    pub fn new(db: Db) -> Self {
        Self(db)
    }
}

fn usable(v: &store::VersionRow, scope: &SubmitScope) -> bool {
    v.org_id == scope.org_id && v.project_id == scope.project_id && v.state == "PUBLISHED"
}

fn invalid() -> ResolveError {
    ResolveError::Invalid(vec![FieldError::new(
        "/reward_version_id",
        ErrorCode::FieldValue,
        "Choose a published reward version from this project.",
        "Publish this project's reward version first, or choose another published version.",
    )])
}

fn seam(e: impl std::fmt::Display) -> SeamError {
    SeamError::new("rewards", e.to_string())
}

#[async_trait]
impl SubmissionResolver for RewardVersions {
    fn name(&self) -> &'static str {
        "reward-version"
    }

    async fn resolve(
        &self,
        scope: &mut SubmitScope,
        _submitted: &Value,
        resolved: Value,
    ) -> Result<Value, ResolveError> {
        if let Some(id) = scope.reward_version_id {
            store::version(&self.0, id)
                .await
                .map_err(seam)?
                .filter(|v| usable(v, scope))
                .ok_or_else(invalid)?;
        }
        Ok(resolved)
    }

    async fn record(
        &self,
        tx: &mut Tx,
        run_id: Uuid,
        scope: &SubmitScope,
    ) -> Result<(), SeamError> {
        let Some(id) = scope.reward_version_id else {
            return Ok(());
        };
        // Deprecation and accepting a new pin must have a consistent order.
        store::lock_version(tx, id)
            .await
            .map_err(seam)?
            .filter(|v| usable(v, scope))
            .ok_or_else(|| seam("The reward version changed while submitting this run."))?;
        let org = scope::org_text(scope.org_id);
        LineageWriter::link(
            tx,
            org.as_deref().unwrap_or(LOCAL_ORG),
            NodeRef::new("run", run_id),
            Relation::RewardedBy,
            NodeRef::new("rwdv", id),
        )
        .await
        .map_err(seam)
    }
}

#[async_trait]
impl NodeLookup for RewardVersions {
    async fn find(&self, org: &str, id: Uuid) -> Result<Option<(&'static str, Resource)>, Error> {
        let version = store::version(&self.0, id)
            .await
            .map_err(|e| ApiError::from(seam(e)))?;
        Ok(version
            .filter(|v| scope::org_text(v.org_id).as_deref().unwrap_or(LOCAL_ORG) == org)
            .map(|v| {
                let reward = RewardId::from_uuid(v.reward_id).to_string();
                (
                    "rewards.read",
                    scope::resource("reward", Some(reward), v.org_id, v.project_id),
                )
            }))
    }
}
