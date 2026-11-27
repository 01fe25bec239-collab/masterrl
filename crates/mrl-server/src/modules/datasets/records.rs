//! [`DatasetRecords`]: what the engine matcher knows about a dataset version (P24's
//! `RegistryRecords`), answered from this module's tables.

use crate::db::Db;
use crate::modules::engines::{RecordScope, RegistryRecords};
use crate::seams::SeamError;
use async_trait::async_trait;
use mrl_domain::capability::matcher::{DatasetFacts, ModelFacts};
use mrl_domain::ids::{DatasetVersionId, ModelVersionId};

/// Facts about READY dataset versions. A version that is missing, of another org, of
/// another project, not READY, or of a deleted dataset is `None` (P24 answers MRL-DATA-240
/// the same way for each). Knows no models.
pub struct DatasetRecords {
    db: Db,
}

impl DatasetRecords {
    /// Reads `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait]
impl RegistryRecords for DatasetRecords {
    async fn model(
        &self,
        _scope: &RecordScope,
        _id: ModelVersionId,
    ) -> Result<Option<ModelFacts>, SeamError> {
        Ok(None)
    }

    async fn dataset(
        &self,
        scope: &RecordScope,
        id: DatasetVersionId,
    ) -> Result<Option<DatasetFacts>, SeamError> {
        let row: Option<(String, Option<i64>)> = sqlx::query_as(
            "SELECT v.schema_id, v.row_count FROM dataset_versions v \
               JOIN datasets d ON d.id = v.dataset_id \
              WHERE v.id = $1 AND v.org_id = $2 AND d.org_id = $2 AND d.project_id = $3 \
                AND v.state = 'READY' AND d.deleted_at IS NULL",
        )
        .bind(id.as_uuid())
        .bind(scope.org_id)
        .bind(scope.project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|e| SeamError::new("datasets", e.to_string()))?;
        Ok(row.map(|(schema, rows)| DatasetFacts {
            schema,
            rows: rows.and_then(|r| u64::try_from(r).ok()),
            streaming: false,
        }))
    }
}

#[async_trait]
impl crate::modules::artifacts::NodeLookup for DatasetRecords {
    async fn find(
        &self,
        org: &str,
        id: uuid::Uuid,
    ) -> Result<Option<(&'static str, crate::seams::Resource)>, crate::modules::artifacts::Error>
    {
        let Some(row) = lineage_version(&self.db, id).await? else {
            return Ok(None);
        };
        let owns_node = if org == crate::modules::artifacts::LOCAL_ORG {
            super::store::is_local_lineage_org(self.db.pool(), row.org_id).await?
        } else {
            crate::modules::checkpoints::service::storage_org(row.org_id) == org
        };
        if !owns_node {
            return Ok(None);
        }
        Ok(Some((
            "datasets.read",
            crate::modules::jobs::scope::resource(
                "dataset",
                Some(mrl_domain::ids::DatasetId::from_uuid(row.dataset_id).to_string()),
                row.org_id,
                row.project_id,
            ),
        )))
    }
}

async fn lineage_version(
    db: &Db,
    id: uuid::Uuid,
) -> Result<Option<super::store::VersionRow>, crate::modules::artifacts::Error> {
    super::store::version(db, id).await.map_err(|error| {
        crate::modules::artifacts::Error::from(SeamError::new("datasets", error.to_string()))
    })
}
