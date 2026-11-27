//! Retention, optimizer stripping, staging GC and object deletion.
//!
//! Objects are never deleted in the transaction that stops referencing them: the transaction
//! queues their keys in `checkpoint_object_deletions`, and [`delete_queued`] removes them
//! afterwards. A crash in between leaves the queue, never a row pointing at a missing object.
//! Staging and published checkpoint objects have separate prefixes and cleanup records.

use super::service::{
    ckpt_cols, hex, lock_row, lock_run, metrics, published_prefix, repoint, run_setting,
    same_committed_files, step, system_audit, CheckpointRow, CheckpointService, STATE_ROLES,
};
use crate::db::{DbError, Tx};
use crate::modules::jobs::JobsError;
use mrl_domain::state::CheckpointEvent;
use serde_json::Value;
use std::collections::HashSet;
use uuid::Uuid;

/// One cleanup pass. Errors are logged; the next pass tries again.
pub async fn run(service: &CheckpointService) {
    for (what, r) in [
        ("retention", retention(service).await),
        ("strip", strip_completed(service).await),
        ("staging", staging_gc(service).await),
        ("delete", delete_queued(service).await.map(|_| ())),
    ] {
        if let Err(e) = r {
            tracing::warn!(code = e.code(), error = %e, what, "checkpoint cleanup failed; trying again later");
        }
    }
}

/// Queues every object of `ckpt` for deletion.
async fn queue_all(tx: &mut Tx, row: &CheckpointRow) -> Result<(), JobsError> {
    sqlx::query(
        "INSERT INTO checkpoint_object_deletions (storage_org, object_key, checkpoint_id) \
         SELECT $2, object_key, checkpoint_id FROM checkpoint_files WHERE checkpoint_id = $1 \
         ON CONFLICT DO NOTHING",
    )
    .bind(row.id)
    .bind(&row.storage_org)
    .execute(&mut **tx)
    .await?;
    // A publisher can die after writing checked bytes but before switching object_key.
    if row.committed_at.is_none() {
        sqlx::query(
            "INSERT INTO checkpoint_object_deletions (storage_org, object_key, checkpoint_id)
             SELECT $2, $3 || CASE WHEN staging_key IS NULL
                                  THEN regexp_replace(path, '^checkpoints/[^/]+/', '')
                                  ELSE path END, checkpoint_id
             FROM checkpoint_files WHERE checkpoint_id = $1
             ON CONFLICT DO NOTHING",
        )
        .bind(row.id)
        .bind(&row.storage_org)
        .bind(published_prefix(row))
        .execute(&mut **tx)
        .await?;
    }
    sqlx::query(
        "INSERT INTO checkpoint_object_deletions (storage_org, object_key, checkpoint_id)
         VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
    )
    .bind(&row.storage_org)
    .bind(stripped_manifest_key(row))
    .bind(row.id)
    .execute(&mut **tx)
    .await?;
    queue_staging(tx, row).await
}

/// Queues staging with a grace period after the last known upload ticket expires.
pub(crate) async fn queue_staging(tx: &mut Tx, row: &CheckpointRow) -> Result<(), JobsError> {
    sqlx::query(
        "INSERT INTO checkpoint_object_deletions (storage_org, object_key, checkpoint_id, delete_after)
         SELECT $2, staging_key, checkpoint_id, GREATEST(now(), $3) + interval '10 minutes'
         FROM checkpoint_files WHERE checkpoint_id = $1 AND staging_key IS NOT NULL
         ON CONFLICT (storage_org, object_key) DO UPDATE
         SET delete_after = GREATEST(checkpoint_object_deletions.delete_after, EXCLUDED.delete_after)",
    )
    .bind(row.id).bind(&row.storage_org).bind(row.tickets_expire_at)
    .execute(&mut **tx).await?;
    Ok(())
}

/// TOMBSTONED (from COMMITTED or CORRUPT) with its objects queued and the run repointed. The
/// run and the row must be locked.
pub(crate) async fn tombstone(
    tx: &mut Tx,
    row: &CheckpointRow,
) -> Result<CheckpointRow, JobsError> {
    let row = step(tx, row, CheckpointEvent::Tombstone).await?;
    queue_all(tx, &row).await?;
    repoint(tx, row.run_id, row.id).await?;
    Ok(row)
}

/// Why a checkpoint must be kept no matter what (`None`: it may go). Checked under the run
/// lock (and, for deletes, the job lock), so a launch can't pick it up meanwhile.
pub(crate) async fn in_use(
    tx: &mut Tx,
    row: &CheckpointRow,
) -> Result<Option<&'static str>, JobsError> {
    let (latest, fin): (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
        "SELECT latest_committed_checkpoint_id, final_checkpoint_id FROM runs WHERE id = $1",
    )
    .bind(row.run_id)
    .fetch_one(&mut **tx)
    .await?;
    if fin == Some(row.id) || row.is_final {
        return Ok(Some("it is the run's final checkpoint"));
    }
    if latest == Some(row.id) {
        return Ok(Some("the run resumes from it"));
    }
    Ok(linked(tx, row.id)
        .await?
        .then_some("another run or an attempt references it"))
}

async fn linked(tx: &mut Tx, id: Uuid) -> Result<bool, JobsError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM runs WHERE parent_checkpoint_id = $1)
             OR EXISTS (SELECT 1 FROM job_attempts WHERE resume_from_checkpoint_id = $1)
             OR EXISTS (
                 SELECT 1 FROM model_versions v JOIN jobs j ON j.id = v.job_id
                 WHERE v.source_kind = 'checkpoint'
                   AND v.source_ref ->> 'checkpoint_id' = $2
                   AND j.state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'REJECTED')
             )",
    )
    .bind(id)
    .bind(mrl_domain::ids::CheckpointId::from_uuid(id).to_string())
    .fetch_one(&mut **tx)
    .await?)
}

/// The policy for a run: its config's `checkpoint.retention` over the server's settings.
struct Policy {
    last: usize,
    best: usize,
    metric: String,
    max: bool,
    every: i64,
}

fn policy(service: &CheckpointService, resolved: &Value) -> Policy {
    let c = service.config();
    let r = run_setting(resolved, "retention");
    let n = |k: &str, d: u64| {
        r.and_then(|r| r.get(k))
            .and_then(Value::as_u64)
            .unwrap_or(d)
    };
    Policy {
        last: usize::try_from(n("keep_last_n", u64::from(c.keep_last_n)).max(1))
            .unwrap_or(usize::MAX),
        best: usize::try_from(n("keep_best_n", u64::from(c.keep_best_n))).unwrap_or(usize::MAX),
        metric: r
            .and_then(|r| r.get("best_metric"))
            .and_then(Value::as_str)
            .unwrap_or(&c.best_metric)
            .to_owned(),
        max: r
            .and_then(|r| r.get("best_mode"))
            .and_then(Value::as_str)
            .unwrap_or(&c.best_mode)
            == "max",
        every: i64::try_from(n("keep_every_k", c.keep_every_k)).unwrap_or(0),
    }
}

/// The checkpoints `policy` keeps out of `rows` (COMMITTED, newest step first).
fn keep(rows: &[CheckpointRow], p: &Policy) -> HashSet<Uuid> {
    let mut kept: HashSet<Uuid> = rows.iter().take(p.last).map(|r| r.id).collect();
    if p.best > 0 {
        let mut scored: Vec<(f64, Uuid)> = rows
            .iter()
            .filter_map(|r| Some((*metrics(r).get(&p.metric)?, r.id)))
            .filter(|(v, _)| v.is_finite())
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        if p.max {
            scored.reverse();
        }
        kept.extend(scored.into_iter().take(p.best).map(|(_, id)| id));
    }
    if p.every > 0 {
        kept.extend(
            rows.iter()
                .filter(|r| r.global_step % p.every == 0)
                .map(|r| r.id),
        );
    }
    kept.extend(rows.iter().filter(|r| r.is_final).map(|r| r.id));
    kept
}

/// Tombstones COMMITTED checkpoints the run's policy doesn't keep (never final, the resume
/// point of an unfinished run, a run's parent or a running attempt's start).
pub async fn retention(service: &CheckpointService) -> Result<(), JobsError> {
    let pool = service.db().pool();
    let mut after = Uuid::nil();
    loop {
        let runs: Vec<(Uuid, Option<Value>)> = sqlx::query_as(
            "SELECT c.run_id, rc.resolved FROM checkpoints c LEFT JOIN run_configs rc ON rc.run_id = c.run_id
             WHERE c.state = 'COMMITTED' AND c.run_id > $1
             GROUP BY c.run_id, rc.resolved HAVING count(*) > 1 ORDER BY c.run_id LIMIT 200",
        )
        .bind(after)
        .fetch_all(pool)
        .await?;
        let Some((last, _)) = runs.last() else {
            break;
        };
        after = *last;
        for (run, resolved) in runs {
            let p = policy(service, &resolved.unwrap_or(Value::Null));
            let mut tx = service.db().begin().await?;
            lock_run(&mut tx, run).await?;
            let rows = sqlx::query_as::<_, CheckpointRow>(concat!(
                "SELECT ",
                ckpt_cols!(),
                " FROM checkpoints WHERE run_id = $1 AND state = 'COMMITTED'
                   ORDER BY global_step DESC, committed_at DESC, id DESC FOR UPDATE"
            ))
            .bind(run)
            .fetch_all(&mut *tx)
            .await?;
            let kept = keep(&rows, &p);
            for row in rows.iter().filter(|r| !kept.contains(&r.id)) {
                if in_use(&mut tx, row).await?.is_none() {
                    tombstone(&mut tx, row).await?;
                    system_audit(&mut tx, service.db(), row, "checkpoints.retain").await?;
                }
            }
            tx.commit().await.map_err(DbError::from)?;
        }
    }
    Ok(())
}

/// After a run COMPLETED, drops optimizer, scheduler, RNG and trainer state from its
/// full-resume checkpoints (they become weights- or adapter-only), unless the run config
/// sets `checkpoint.strip_optimizer = false`.
pub async fn strip_completed(service: &CheckpointService) -> Result<(), JobsError> {
    let mut after = Uuid::nil();
    loop {
        let rows = sqlx::query_as::<_, CheckpointRow>(concat!(
            "SELECT ",
            ckpt_cols!(),
            " FROM checkpoints c WHERE c.id > $2 AND c.state = 'COMMITTED' AND c.kind = 'full_resume'
               AND EXISTS (SELECT 1 FROM jobs j WHERE j.run_id = c.run_id AND j.state = 'COMPLETED')
               AND EXISTS (SELECT 1 FROM checkpoint_files f WHERE f.checkpoint_id = c.id AND f.role = ANY($1))
               AND NOT EXISTS (SELECT 1 FROM runs WHERE parent_checkpoint_id = c.id)
               AND NOT EXISTS (SELECT 1 FROM job_attempts WHERE resume_from_checkpoint_id = c.id)
               AND COALESCE((SELECT CASE rc.resolved #> '{checkpoint,strip_optimizer}'
                               WHEN 'true'::jsonb THEN true WHEN 'false'::jsonb THEN false END
                             FROM run_configs rc WHERE rc.run_id = c.run_id), $3)
             ORDER BY c.id LIMIT 100"
        ))
        .bind(&STATE_ROLES[..])
        .bind(after)
        .bind(service.config().strip_optimizer_after_completion)
        .fetch_all(service.db().pool())
        .await?;
        let Some(last) = rows.last() else {
            break;
        };
        after = last.id;
        for row in rows {
            if let Err(e) = strip_one(service, &row).await {
                tracing::warn!(code = e.code(), checkpoint = %row.id, "optimizer state was not stripped: {e}");
            }
        }
    }
    Ok(())
}

async fn strip_one(service: &CheckpointService, row: &CheckpointRow) -> Result<(), JobsError> {
    let Some(old_key) = &row.manifest_key else {
        return Ok(());
    };
    let store = service.org_store(&row.storage_org).await?;
    let bytes = store
        .get(old_key)
        .await
        .map_err(|e| storage_error(e.to_string()))?;
    if row.manifest_sha256.as_deref().map(hex) != Some(mrl_storage::sha256_hex(&bytes)) {
        return Err(storage_error("the manifest digest changed"));
    }
    let mut doc: Value =
        serde_json::from_slice(&bytes).map_err(|e| storage_error(e.to_string()))?;
    let Some(listed) = doc.get_mut("files").and_then(Value::as_array_mut) else {
        return Err(storage_error("the manifest has no file list"));
    };
    listed.retain(|f| !f["role"].as_str().is_some_and(|r| STATE_ROLES.contains(&r)));
    // Full checkpoints with adapter weights also need adapter metadata to be published as such.
    let kind = if listed.iter().any(|f| f["role"] == "weights") {
        "weights_only"
    } else if listed.iter().any(|f| f["role"] == "adapter") && doc["adapter"].is_object() {
        "adapter_only"
    } else {
        return Ok(());
    };
    doc["kind"] = kind.into();
    doc["is_final"] = row.is_final.into();
    let bytes = serde_json::to_vec(&doc).map_err(|e| storage_error(e.to_string()))?;
    let digest = mrl_storage::sha256_hex(&bytes);
    let key = stripped_manifest_key(row);

    let mut tx = service.db().begin().await?;
    lock_run(&mut tx, row.run_id).await?;
    let Some(locked) = lock_row(&mut tx, row.id).await? else {
        return Ok(());
    };
    if !same_committed_files(&locked, row)
        || locked.kind != "full_resume"
        || linked(&mut tx, row.id).await?
    {
        return Ok(());
    }
    // The replacement key is deterministic, so a retry after an interrupted write reuses it.
    let size = bytes.len() as i64;
    store
        .put(&key, bytes.into())
        .await
        .map_err(|e| storage_error(e.to_string()))?;
    let etag = store
        .head(&key)
        .await
        .map_err(|e| storage_error(e.to_string()))?
        .etag;
    sqlx::query(
        "INSERT INTO checkpoint_object_deletions (storage_org, object_key, checkpoint_id)
         SELECT $2, object_key, checkpoint_id FROM checkpoint_files
         WHERE checkpoint_id = $1 AND (role = 'manifest' OR role = ANY($3))
         ON CONFLICT DO NOTHING",
    )
    .bind(row.id)
    .bind(&row.storage_org)
    .bind(&STATE_ROLES[..])
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM checkpoint_files WHERE checkpoint_id = $1 AND role = ANY($2)")
        .bind(row.id)
        .bind(&STATE_ROLES[..])
        .execute(&mut *tx)
        .await?;
    let digest = super::service::sha_bytes(&digest);
    sqlx::query(
        "UPDATE checkpoint_files SET object_key = $2, sha256 = $3, size_bytes = $4, storage_checksum = $5
         WHERE checkpoint_id = $1 AND role = 'manifest'",
    )
    .bind(row.id).bind(&key).bind(&digest).bind(size).bind(etag)
    .execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE checkpoints c SET kind = $2, manifest_key = $3, manifest_sha256 = $4,
           verification_level = 'manifest_head', verified_at = now(),
           total_bytes = (SELECT COALESCE(sum(size_bytes), 0) FROM checkpoint_files WHERE checkpoint_id = c.id),
           file_count = (SELECT count(*) FROM checkpoint_files WHERE checkpoint_id = c.id)
         WHERE c.id = $1",
    )
    .bind(row.id).bind(kind).bind(key).bind(digest)
    .execute(&mut *tx).await?;
    system_audit(&mut tx, service.db(), row, "checkpoints.strip_optimizer").await?;
    tx.commit().await.map_err(DbError::from)?;
    Ok(())
}

fn stripped_manifest_key(row: &CheckpointRow) -> String {
    format!(
        "{}-manifests/stripped.json",
        published_prefix(row).trim_end_matches('/')
    )
}

fn storage_error(message: impl Into<String>) -> JobsError {
    JobsError::Seam(crate::seams::SeamError::new("storage", message))
}

/// Staging GC: a CORRUPT checkpoint's files go once it has been corrupt for the staging TTL
/// (kept that long to look into what went wrong); the row becomes TOMBSTONED.
pub async fn staging_gc(service: &CheckpointService) -> Result<(), JobsError> {
    let ids: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT id, run_id FROM checkpoints WHERE state = 'CORRUPT' \
           AND COALESCE(verified_at, created_at) < now() - make_interval(secs => $1)
           AND NOT is_final
           AND NOT EXISTS (SELECT 1 FROM runs WHERE parent_checkpoint_id = checkpoints.id)
           AND NOT EXISTS (SELECT 1 FROM job_attempts WHERE resume_from_checkpoint_id = checkpoints.id)
           ORDER BY created_at LIMIT 200",
    )
    .bind(service.config().staging_ttl_seconds as f64)
    .fetch_all(service.db().pool())
    .await?;
    for (id, run) in ids {
        let mut tx = service.db().begin().await?;
        lock_run(&mut tx, run).await?;
        if let Some(row) = lock_row(&mut tx, id).await? {
            if row.state == "CORRUPT" && in_use(&mut tx, &row).await?.is_none() {
                tombstone(&mut tx, &row).await?;
                system_audit(&mut tx, service.db(), &row, "checkpoints.staging_gc").await?;
            }
        }
        tx.commit().await.map_err(DbError::from)?;
    }
    Ok(())
}

/// Deletes queued objects (a missing object counts as deleted). Returns how many went.
pub async fn delete_queued(service: &CheckpointService) -> Result<usize, JobsError> {
    let rows: Vec<(String, String, Uuid)> = sqlx::query_as(
        "SELECT storage_org, object_key, checkpoint_id FROM checkpoint_object_deletions
         WHERE delete_after <= now() ORDER BY delete_after, queued_at LIMIT 1000",
    )
    .fetch_all(service.db().pool())
    .await?;
    let mut done = 0;
    for (org, key, ckpt) in rows {
        let deleted = async {
            let store = service.org_store(&org).await?;
            let mut tx = service.db().begin().await?;
            let available: bool = sqlx::query_scalar(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 23))",
            )
            .bind(ckpt.to_string()).fetch_one(&mut *tx).await?;
            if !available {
                return Ok(false);
            }
            let due: Option<bool> = sqlx::query_scalar(
                "SELECT delete_after <= now() FROM checkpoint_object_deletions
                 WHERE storage_org = $1 AND object_key = $2 FOR UPDATE",
            )
            .bind(&org).bind(&key).fetch_optional(&mut *tx).await?;
            if due != Some(true) {
                return Ok(false);
            }
            store.delete(&key).await.map_err(|e| storage_error(e.to_string()))?;
            sqlx::query(
                "DELETE FROM checkpoint_object_deletions WHERE storage_org = $1 AND object_key = $2",
            )
            .bind(&org).bind(&key).execute(&mut *tx).await?;
            tx.commit().await.map_err(DbError::from)?;
            Ok::<_, JobsError>(true)
        }.await;
        match deleted {
            Ok(true) => done += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    code = e.code(),
                    key,
                    "checkpoint object was not deleted: {e}"
                );
                sqlx::query(
                    "UPDATE checkpoint_object_deletions SET delete_after = now() + interval '10 minutes'
                     WHERE storage_org = $1 AND object_key = $2 AND delete_after <= now()",
                )
                .bind(&org).bind(&key).execute(service.db().pool()).await?;
            }
        }
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(step: i64, loss: f64, is_final: bool) -> CheckpointRow {
        let now = time::OffsetDateTime::UNIX_EPOCH;
        CheckpointRow {
            id: Uuid::from_u128(step as u128 + 1),
            org_id: Uuid::nil(),
            storage_org: "local".into(),
            run_id: Uuid::nil(),
            job_id: Uuid::nil(),
            attempt_id: Uuid::nil(),
            attempt_epoch: 1,
            global_step: step,
            policy_version: None,
            kind: "full_resume".into(),
            layout: "fake-v1".into(),
            is_final,
            state: "COMMITTED".into(),
            state_version: 3,
            verification_level: None,
            manifest_key: None,
            manifest_sha256: None,
            total_bytes: None,
            file_count: None,
            metrics_at_save: Some(json!({"eval/loss": loss})),
            corrupt_reason: None,
            commit_requested_at: None,
            upload_command_id: None,
            tickets_expire_at: None,
            node_id: None,
            created_at: now,
            committed_at: None,
            verified_at: None,
        }
    }

    #[test]
    fn policy_keeps_last_best_every_k_and_final() {
        // Newest first, as retention reads them.
        let rows: Vec<_> = (1..=10)
            .rev()
            .map(|s| row(s * 10, if s == 2 { 0.1 } else { 1.0 }, s == 1))
            .collect();
        let p = Policy {
            last: 2,
            best: 1,
            metric: "eval/loss".into(),
            max: false,
            every: 50,
        };
        let mut steps: Vec<i64> = keep(&rows, &p)
            .iter()
            .filter_map(|id| rows.iter().find(|r| r.id == *id).map(|r| r.global_step))
            .collect();
        steps.sort();
        // final 10, best 20, every 50: 50 and 100, last 2: 90 and 100
        assert_eq!(steps, vec![10, 20, 50, 90, 100]);
    }
}
