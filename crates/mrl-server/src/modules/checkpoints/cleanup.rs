//! Retention, optimizer stripping, staging GC and object deletion.
//!
//! Objects are never deleted in the transaction that stops referencing them: the transaction
//! queues their keys in `checkpoint_object_deletions`, and [`delete_queued`] removes them
//! afterwards. A crash in between leaves the queue, never a row pointing at a missing object.
//! Checkpoint objects live under `runs/…/checkpoints/…/_staging/`, which P19's cleanup never
//! lists, so these rows are the only references to them.

use super::service::{
    ckpt_cols, lock_row, lock_run, metrics, repoint, run_setting, step, CheckpointRow,
    CheckpointService, STATE_ROLES,
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
    let (latest, fin, job_state): (Option<Uuid>, Option<Uuid>, Option<String>) = sqlx::query_as(
        "SELECT r.latest_committed_checkpoint_id, r.final_checkpoint_id, j.state FROM runs r \
         LEFT JOIN jobs j ON j.run_id = r.id WHERE r.id = $1",
    )
    .bind(row.run_id)
    .fetch_one(&mut **tx)
    .await?;
    if fin == Some(row.id) || row.is_final {
        return Ok(Some("it is the run's final checkpoint"));
    }
    let job_done = job_state
        .as_deref()
        .is_some_and(|s| matches!(s, "COMPLETED" | "FAILED" | "CANCELLED" | "REJECTED"));
    if latest == Some(row.id) && !job_done {
        return Ok(Some("the run resumes from it"));
    }
    let used: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM runs WHERE parent_checkpoint_id = $1) \
             OR EXISTS (SELECT 1 FROM job_attempts WHERE resume_from_checkpoint_id = $1 \
                          AND state IN ('PENDING','LAUNCHING','RUNNING'))",
    )
    .bind(row.id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(used.then_some("another run or a running attempt starts from it"))
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
        last: n("keep_last_n", u64::from(c.keep_last_n)).max(1) as usize,
        best: n("keep_best_n", u64::from(c.keep_best_n)) as usize,
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
    let runs: Vec<(Uuid, Option<Value>)> = sqlx::query_as(
        "SELECT c.run_id, rc.resolved FROM checkpoints c LEFT JOIN run_configs rc ON rc.run_id = c.run_id \
         WHERE c.state = 'COMMITTED' GROUP BY c.run_id, rc.resolved HAVING count(*) > 1 LIMIT 200",
    )
    .fetch_all(pool)
    .await?;
    for (run, resolved) in runs {
        let p = policy(service, &resolved.unwrap_or(Value::Null));
        let mut tx = service.db().begin().await?;
        lock_run(&mut tx, run).await?;
        let rows = sqlx::query_as::<_, CheckpointRow>(concat!(
            "SELECT ",
            ckpt_cols!(),
            " FROM checkpoints WHERE run_id = $1 AND state = 'COMMITTED' \
               ORDER BY global_step DESC, committed_at DESC FOR UPDATE"
        ))
        .bind(run)
        .fetch_all(&mut *tx)
        .await?;
        let kept = keep(&rows, &p);
        for row in rows.iter().filter(|r| !kept.contains(&r.id)) {
            if in_use(&mut tx, row).await?.is_none() {
                tombstone(&mut tx, row).await?;
            }
        }
        tx.commit().await.map_err(DbError::from)?;
    }
    Ok(())
}

/// After a run COMPLETED, drops optimizer, scheduler, RNG and trainer state from its
/// full-resume checkpoints (they become weights- or adapter-only), unless the run config
/// sets `checkpoint.strip_optimizer = false`.
pub async fn strip_completed(service: &CheckpointService) -> Result<(), JobsError> {
    let rows = sqlx::query_as::<_, CheckpointRow>(concat!(
        "SELECT ",
        ckpt_cols!(),
        " FROM checkpoints c WHERE c.state = 'COMMITTED' AND c.kind = 'full_resume' \
           AND EXISTS (SELECT 1 FROM jobs j WHERE j.run_id = c.run_id AND j.state = 'COMPLETED') \
           AND EXISTS (SELECT 1 FROM checkpoint_files f WHERE f.checkpoint_id = c.id AND f.role = ANY($1)) \
         LIMIT 100"
    ))
    .bind(&STATE_ROLES[..])
    .fetch_all(service.db().pool())
    .await?;
    for row in rows {
        let resolved: Option<Value> =
            sqlx::query_scalar("SELECT resolved FROM run_configs WHERE run_id = $1")
                .bind(row.run_id)
                .fetch_optional(service.db().pool())
                .await?;
        let wanted = resolved
            .as_ref()
            .and_then(|r| run_setting(r, "strip_optimizer"))
            .and_then(Value::as_bool)
            .unwrap_or(service.config().strip_optimizer_after_completion);
        if !wanted {
            continue;
        }
        let mut tx = service.db().begin().await?;
        lock_run(&mut tx, row.run_id).await?;
        let Some(locked) = lock_row(&mut tx, row.id).await? else {
            continue;
        };
        if locked.state != "COMMITTED" || locked.kind != "full_resume" {
            continue;
        }
        sqlx::query(
            "INSERT INTO checkpoint_object_deletions (storage_org, object_key, checkpoint_id) \
             SELECT $2, object_key, checkpoint_id FROM checkpoint_files \
             WHERE checkpoint_id = $1 AND role = ANY($3) ON CONFLICT DO NOTHING",
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
        sqlx::query(
            "UPDATE checkpoints c SET \
               kind = CASE WHEN EXISTS (SELECT 1 FROM checkpoint_files f WHERE f.checkpoint_id = c.id \
                                          AND f.role = 'adapter') THEN 'adapter_only' ELSE 'weights_only' END, \
               total_bytes = (SELECT COALESCE(sum(size_bytes), 0) FROM checkpoint_files WHERE checkpoint_id = c.id), \
               file_count = (SELECT count(*) FROM checkpoint_files WHERE checkpoint_id = c.id) \
             WHERE c.id = $1",
        )
        .bind(row.id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await.map_err(DbError::from)?;
    }
    Ok(())
}

/// Staging GC: a CORRUPT checkpoint's files go once it has been corrupt for the staging TTL
/// (kept that long to look into what went wrong); the row becomes TOMBSTONED.
pub async fn staging_gc(service: &CheckpointService) -> Result<(), JobsError> {
    let ids: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT id, run_id FROM checkpoints WHERE state = 'CORRUPT' \
           AND COALESCE(verified_at, created_at) < now() - make_interval(secs => $1) LIMIT 200",
    )
    .bind(service.config().staging_ttl_seconds as f64)
    .fetch_all(service.db().pool())
    .await?;
    for (id, run) in ids {
        let mut tx = service.db().begin().await?;
        lock_run(&mut tx, run).await?;
        if let Some(row) = lock_row(&mut tx, id).await? {
            if row.state == "CORRUPT" {
                tombstone(&mut tx, &row).await?;
            }
        }
        tx.commit().await.map_err(DbError::from)?;
    }
    Ok(())
}

/// Deletes queued objects (a missing object counts as deleted). Returns how many went.
pub async fn delete_queued(service: &CheckpointService) -> Result<usize, JobsError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT storage_org, object_key FROM checkpoint_object_deletions ORDER BY queued_at LIMIT 1000",
    )
    .fetch_all(service.db().pool())
    .await?;
    let mut done = 0;
    for (org, key) in rows {
        let store = service.org_store(&org).await?;
        match store.delete(&key).await {
            Ok(()) | Err(mrl_storage::StorageError::NotFound { .. }) => {}
            Err(e) => {
                tracing::warn!(error = %e, key, "checkpoint object not deleted yet");
                continue;
            }
        }
        sqlx::query(
            "DELETE FROM checkpoint_object_deletions WHERE storage_org = $1 AND object_key = $2",
        )
        .bind(&org)
        .bind(&key)
        .execute(service.db().pool())
        .await?;
        done += 1;
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
