//! Request helpers shared by the runs and jobs endpoints: tenant, ids, authorization,
//! audit and pages.

use super::error::JobsError;
use crate::app::AppState;
use crate::error::{ApiError, ErrorCode};
use crate::http::pagination::{encode_cursor, PageRequest};
use crate::seams::{AuditEvent, AuditOutcome, RequestContext, Resource};
use mrl_domain::ids::{OrgId, ProjectId};
use std::str::FromStr;
use uuid::Uuid;

/// The org a request acts in. A caller without an org (the solo owner, before P16 gives
/// solo an org) acts in the nil org: one local tenant.
pub fn tenant(ctx: &RequestContext) -> Result<Uuid, JobsError> {
    match ctx.org.as_deref() {
        None => Ok(Uuid::nil()),
        Some(org) => org.parse::<OrgId>().map(|o| o.as_uuid()).map_err(|_| {
            JobsError::Api(ApiError::new(
                ErrorCode::Forbidden,
                "Your sign-in names an org the server can't read.",
            ))
        }),
    }
}

/// The external org id, or `None` for the local tenant.
pub fn org_text(org: Uuid) -> Option<String> {
    (!org.is_nil()).then(|| OrgId::from_uuid(org).to_string())
}

/// Parses a prefixed id from a path; anything else is "not found".
pub fn parse_id<T: FromStr>(text: &str, kind: &str) -> Result<T, JobsError> {
    text.parse().map_err(|_| JobsError::not_found(kind))
}

/// A resource for the authorizer.
pub fn resource(kind: &str, id: Option<String>, org: Uuid, project: Uuid) -> Resource {
    Resource {
        kind: kind.to_owned(),
        id,
        org: org_text(org),
        project: Some(ProjectId::from_uuid(project).to_string()),
    }
}

/// Asks the authorizer: Deny → 403, Hide → 404.
pub async fn authorize(
    state: &AppState,
    ctx: &RequestContext,
    action: &str,
    res: &Resource,
) -> Result<(), JobsError> {
    Ok(state.authorize(ctx, action, res).await?)
}

/// True if the authorizer allows the action, false on Deny or Hide (used to filter list
/// pages). An authorizer that fails is an error (503), never an empty page.
pub async fn allowed(
    state: &AppState,
    ctx: &RequestContext,
    action: &str,
    res: &Resource,
) -> Result<bool, JobsError> {
    let decision = state.seams.authorizer.authorize(ctx, action, res).await?;
    Ok(decision == crate::seams::Decision::Allow)
}

/// Cancelling (and pausing) needs `runs.cancel_own` for your own work, else `runs.cancel_any`.
pub async fn authorize_cancel(
    state: &AppState,
    ctx: &RequestContext,
    res: &Resource,
    created_by: &str,
) -> Result<(), JobsError> {
    if created_by == ctx.principal.id && allowed(state, ctx, "runs.cancel_own", res).await? {
        return Ok(());
    }
    authorize(state, ctx, "runs.cancel_any", res).await
}

/// Records a successful action on something in `org`/`project`, inside the change's
/// transaction `tx` and before it commits: if the record can't be written, the change rolls
/// back. `_tx` is that transaction: P16 adds `AuditSink::record_in(tx, …)`, which stores
/// the record in it, and at merge this call becomes `record_in(_tx, event)`.
pub async fn audit(
    _tx: &mut crate::db::Tx,
    state: &AppState,
    ctx: &RequestContext,
    (org, project): (Uuid, Uuid),
    action: &str,
    target: String,
) -> Result<(), JobsError> {
    let event = AuditEvent {
        at: state.seams.clock.now(),
        actor: ctx.principal.id.clone(),
        org: org_text(org),
        project: Some(ProjectId::from_uuid(project).to_string()),
        action: action.to_owned(),
        target,
        outcome: AuditOutcome::Success,
        trace_id: ctx.trace_id.clone(),
    };
    state.seams.audit.record(event).await?;
    Ok(())
}

/// Refuses text PostgreSQL can't store: a NUL character in any string or key of `v`.
/// The field error points at the first place it is found.
pub fn no_nul(v: &serde_json::Value) -> Result<(), JobsError> {
    fn find(v: &serde_json::Value) -> Option<Vec<String>> {
        match v {
            serde_json::Value::String(s) if s.contains('\0') => Some(Vec::new()),
            serde_json::Value::Array(items) => items.iter().enumerate().find_map(|(i, item)| {
                let mut path = find(item)?;
                path.push(i.to_string());
                Some(path)
            }),
            serde_json::Value::Object(map) => map.iter().find_map(|(key, value)| {
                let mut path = if key.contains('\0') {
                    Vec::new()
                } else {
                    find(value)?
                };
                path.push(key.replace('~', "~0").replace('/', "~1"));
                Some(path)
            }),
            _ => None,
        }
    }
    match find(v) {
        None => Ok(()),
        Some(mut path) => {
            path.reverse();
            let pointer = format!("/{}", path.join("/"));
            Err(ApiError::validation(vec![crate::error::FieldError::new(
                &pointer,
                ErrorCode::FieldValue,
                "Text can't contain the NUL character (\\u0000).",
                "Remove the NUL character from this value.",
            )])
            .into())
        }
    }
}

/// The id a page continues after (`cursor` holds the last id of the previous page).
pub fn after<T: FromStr>(
    page: &PageRequest,
    id: impl Fn(T) -> Uuid,
) -> Result<Option<Uuid>, JobsError> {
    page.after
        .as_deref()
        .map(|c| {
            c.parse::<T>().map(&id).map_err(|_| {
                JobsError::Api(ApiError::new(
                    ErrorCode::Pagination,
                    "cursor must be the next_cursor value from an earlier page, unchanged.",
                ))
            })
        })
        .transpose()
}

/// Cuts `rows` (fetched with `limit + 1`) to a page; the cursor is the last kept row's id.
pub fn cut<T>(
    mut rows: Vec<T>,
    page: &PageRequest,
    id: impl Fn(&T) -> String,
) -> (Vec<T>, Option<String>) {
    let more = rows.len() > page.limit;
    rows.truncate(page.limit);
    let next = if more {
        rows.last().map(|r| encode_cursor(&id(r)))
    } else {
        None
    };
    (rows, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seams::{Principal, RequestMeta};

    fn ctx(org: Option<&str>) -> RequestContext {
        let mut p = Principal::local_owner();
        p.org = org.map(str::to_owned);
        RequestContext::new(
            p,
            &RequestMeta {
                trace_id: "t".into(),
                request_id: "r".into(),
            },
        )
    }

    #[test]
    fn tenant_is_nil_without_org_and_parses_org_ids() {
        assert_eq!(tenant(&ctx(None)).unwrap(), Uuid::nil());
        let org = OrgId::new();
        assert_eq!(tenant(&ctx(Some(&org.to_string()))).unwrap(), org.as_uuid());
        assert_eq!(
            tenant(&ctx(Some("acme"))).unwrap_err().code(),
            "MRL-PERM-001"
        );
        assert_eq!(org_text(Uuid::nil()), None);
        assert_eq!(org_text(org.as_uuid()), Some(org.to_string()));
    }

    #[test]
    fn nul_error_points_to_the_escaped_nested_path() {
        let valid = serde_json::json!({"a/b": [{"~name": "ok"}]});
        assert!(no_nul(&valid).is_ok());
        let bad = serde_json::json!({"a/b": [{}, {"~name": "before\0after"}]});
        let JobsError::Api(err) = no_nul(&bad).unwrap_err() else {
            panic!("expected a validation error");
        };
        assert_eq!(err.errors[0].path, "/a~1b/1/~0name");
    }

    #[test]
    fn bad_ids_are_not_found_and_pages_cut() {
        assert_eq!(
            parse_id::<ProjectId>("run_x", "project")
                .unwrap_err()
                .code(),
            "MRL-RES-001"
        );
        let page = PageRequest {
            limit: 2,
            after: None,
        };
        let (rows, next) = cut(vec![1, 2, 3], &page, |n| n.to_string());
        assert_eq!(rows, vec![1, 2]);
        assert!(next.is_some());
        let (_, last) = cut(vec![1, 2], &page, |n| n.to_string());
        assert_eq!(last, None);
        let bad = PageRequest {
            limit: 2,
            after: Some("nope".into()),
        };
        assert_eq!(
            after::<ProjectId>(&bad, |p| p.as_uuid())
                .unwrap_err()
                .code(),
            "MRL-VAL-005"
        );
    }
}
