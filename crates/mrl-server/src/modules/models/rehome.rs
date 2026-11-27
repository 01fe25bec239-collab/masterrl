//! Move verified model blobs before making versions visible to the destination org.

use super::outputs::storage_org;
use super::store::hex;
use crate::modules::artifacts::{ArtifactStore, LOCAL_ORG};
use uuid::Uuid;

pub(crate) async fn copy_blobs(
    tx: &mut crate::Tx,
    store: Option<&ArtifactStore>,
    org: Uuid,
    projects: &[Uuid],
) -> Result<(), sqlx::Error> {
    let rows: Vec<(String, Vec<u8>, i64)> = sqlx::query_as(
        "SELECT DISTINCT f.object_key, f.sha256, f.size_bytes
           FROM model_files f JOIN model_versions v ON v.id = f.model_version_id
          WHERE v.project_id = ANY($1) AND v.org_id IN ($2, $3)
            AND f.object_key LIKE 'orgs/local/blobs/sha256/%'",
    )
    .bind(projects)
    .bind(org)
    .bind(Uuid::nil())
    .fetch_all(&mut **tx)
    .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let store =
        store.ok_or_else(|| sqlx::Error::Protocol("model storage is not configured".into()))?;
    let storage = |e: crate::modules::artifacts::Error| sqlx::Error::Protocol(e.to_string());
    let from = store.org_store(LOCAL_ORG).await.map_err(storage)?;
    let to = store.org_store(&storage_org(org)).await.map_err(storage)?;
    let backend = |e: mrl_storage::StorageError| sqlx::Error::Protocol(e.to_string());
    for (old, hash, size) in rows {
        let hash = hex(&hash);
        let size = u64::try_from(size).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let next = to.keys().blob(&hash).map_err(backend)?;
        let dir = store.scratch_dir().map_err(storage)?;
        let local = dir.path().join("model-blob");
        let downloaded = from
            .download_file(
                &old,
                &local,
                Some(&hash),
                mrl_storage::TransferOptions::default(),
            )
            .await
            .map_err(backend)?;
        if downloaded.size != size {
            return Err(sqlx::Error::Protocol(
                "model blob size does not match the registry".into(),
            ));
        }
        store
            .put_blob(&to, &local, &next, size, &hash)
            .await
            .map_err(storage)?;
        sqlx::query(
            "UPDATE model_files f SET object_key = $1
               FROM model_versions v
              WHERE v.id = f.model_version_id AND f.object_key = $2
                AND v.project_id = ANY($3) AND v.org_id IN ($4, $5)",
        )
        .bind(next)
        .bind(old)
        .bind(projects)
        .bind(org)
        .bind(Uuid::nil())
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}
