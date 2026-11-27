//! Model submissions keep their replay record in the same transaction as the job and audit.

use crate::db::Db;
use crate::error::{ApiError, ErrorCode};
use crate::http::idempotency::{IDEMPOTENCY_KEY, REPLAYED};
use crate::modules::jobs::JobsError;
use crate::seams::RequestContext;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(super) struct Submission<'a> {
    org: Uuid,
    project: Uuid,
    principal: &'a str,
    key: Option<&'a str>,
    fingerprint: Vec<u8>,
}

impl<'a> Submission<'a> {
    pub(super) fn new(
        (org, project): (Uuid, Uuid),
        ctx: &'a RequestContext,
        headers: &'a HeaderMap,
        operation: &str,
        resource: &str,
        body: &impl Serialize,
    ) -> Result<Self, JobsError> {
        let key = headers.get(IDEMPOTENCY_KEY).and_then(|h| h.to_str().ok());
        let fingerprint = if key.is_some() {
            let bytes = serde_json::to_vec(&(operation, resource, body)).map_err(|_| {
                ApiError::new(
                    ErrorCode::Internal,
                    "The model submission could not be encoded.",
                )
            })?;
            Sha256::digest(bytes).to_vec()
        } else {
            Vec::new()
        };
        Ok(Self {
            org,
            project,
            principal: &ctx.principal.id,
            key,
            fingerprint,
        })
    }

    async fn lookup(
        &self,
        executor: impl sqlx::PgExecutor<'_>,
        key: &str,
    ) -> Result<Option<Response>, JobsError> {
        let row: Option<(Vec<u8>, Option<Value>)> = sqlx::query_as(
            "SELECT fingerprint, response FROM model_submission_keys
             WHERE org_id = $1 AND project_id = $2 AND principal_id = $3 AND key = $4",
        )
        .bind(self.org)
        .bind(self.project)
        .bind(self.principal)
        .bind(key)
        .fetch_optional(executor)
        .await?;
        match row {
            Some((hash, _)) if hash != self.fingerprint => Err(ApiError::new(
                ErrorCode::IdempotencyMismatch,
                "This key was already used with a different model request.",
            )
            .into()),
            Some((_, Some(body))) => Ok(Some(
                (StatusCode::ACCEPTED, [(REPLAYED, "true")], Json(body)).into_response(),
            )),
            Some((_, None)) => Err(ApiError::new(
                ErrorCode::Internal,
                "The original model submission is missing its response.",
            )
            .into()),
            None => Ok(None),
        }
    }

    pub(super) async fn replay(&self, db: &Db) -> Result<Option<Response>, JobsError> {
        match self.key {
            Some(key) => self.lookup(db.pool(), key).await,
            None => Ok(None),
        }
    }

    pub(super) async fn claim(&self, tx: &mut crate::Tx) -> Result<Option<Response>, JobsError> {
        let Some(key) = self.key else {
            return Ok(None);
        };
        let claimed = sqlx::query(
            "INSERT INTO model_submission_keys (org_id, project_id, principal_id, key, fingerprint)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .bind(self.org)
        .bind(self.project)
        .bind(self.principal)
        .bind(key)
        .bind(&self.fingerprint)
        .execute(&mut **tx)
        .await?
        .rows_affected()
            != 0;
        if claimed {
            Ok(None)
        } else {
            self.lookup(&mut **tx, key).await
        }
    }

    pub(super) async fn save(
        &self,
        tx: &mut crate::Tx,
        body: &impl Serialize,
    ) -> Result<(), JobsError> {
        if let Some(key) = self.key {
            let body = serde_json::to_value(body).map_err(|_| {
                ApiError::new(
                    ErrorCode::Internal,
                    "The model submission could not be encoded.",
                )
            })?;
            sqlx::query(
                "UPDATE model_submission_keys SET response = $5
                 WHERE org_id = $1 AND project_id = $2 AND principal_id = $3 AND key = $4",
            )
            .bind(self.org)
            .bind(self.project)
            .bind(self.principal)
            .bind(key)
            .bind(body)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}
