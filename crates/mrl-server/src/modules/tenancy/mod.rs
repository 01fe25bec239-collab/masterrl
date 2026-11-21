//! Users, orgs, workspaces, projects and role bindings, and the RBAC `Authorizer`.
//!
//! Solo mode: one local owner, owner of the implicit `personal` org (made at startup).
//! Team mode: people are Supabase users; their roles come from `role_bindings` (P43 adds
//! the invite and membership flows on top).

use crate::config::Mode;
use crate::db::{Db, DbError, Tx};
use crate::modules::auth::credentials;
use crate::seams::{
    Authorizer, Decision, Principal, PrincipalKind, RequestContext, Resource, SeamError,
    StartupStep,
};
use crate::ServerModule;
use async_trait::async_trait;
use mrl_domain::ids::{OrgId, ProjectId, UserId, WorkspaceId};
use mrl_domain::rbac::{allowed, Action, Role};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

mod routes;

/// Orgs, workspaces and projects (`/orgs`, `/orgs/{org}/workspaces`,
/// `/workspaces/{ws}/projects`), and in solo mode the personal org and its local owner.
pub struct TenancyModule {
    db: Db,
    mode: Mode,
}

impl TenancyModule {
    /// The module for this mode.
    pub fn new(db: Db, mode: Mode) -> Self {
        Self { db, mode }
    }
}

impl ServerModule for TenancyModule {
    fn name(&self) -> &'static str {
        "tenancy"
    }

    fn routes(&self) -> axum::Router<crate::AppState> {
        routes::router(self.db.clone())
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        routes::doc()
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        match self.mode {
            Mode::Solo => vec![Arc::new(LocalOwnerStep(self.db.clone()))],
            Mode::Team => Vec::new(),
        }
    }
}

struct LocalOwnerStep(Db);

#[async_trait]
impl StartupStep for LocalOwnerStep {
    fn name(&self) -> &str {
        "local-owner"
    }

    async fn run(&self) -> Result<(), SeamError> {
        ensure_local_owner(&self.0).await?;
        Ok(())
    }
}

/// The solo owner and their personal org, made on first start. Returns the owner's id.
pub async fn ensure_local_owner(db: &Db) -> Result<UserId, DbError> {
    db.transaction(|tx| {
        Box::pin(async move {
            // One server per data folder (P11's lock), so check-then-insert is enough.
            if let Some(id) = local_owner_in(tx).await? {
                return Ok(id);
            }
            let (org, ws, user) = (OrgId::new(), WorkspaceId::new(), UserId::new());
            sqlx::query(
                "INSERT INTO organizations (id, slug, name, kind) VALUES ($1, 'personal', 'Personal', 'personal')",
            )
            .bind(org.as_uuid())
            .execute(&mut **tx)
            .await?;
            sqlx::query("INSERT INTO workspaces (id, org_id, slug, name) VALUES ($1, $2, 'default', 'Default')")
                .bind(ws.as_uuid())
                .bind(org.as_uuid())
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO users (id, display_name, is_local_owner) VALUES ($1, 'You', true)")
                .bind(user.as_uuid())
                .execute(&mut **tx)
                .await?;
            bind_role(tx, org, user, Role::Owner, Scope::Org).await?;
            Ok(user)
        })
    })
    .await
}

async fn local_owner_in(tx: &mut Tx) -> Result<Option<UserId>, DbError> {
    let id: Option<Uuid> = sqlx::query_scalar("SELECT id FROM users WHERE is_local_owner")
        .fetch_optional(&mut **tx)
        .await?;
    Ok(id.map(UserId::from_uuid))
}

/// The solo owner: their id and the Supabase account they are linked to, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalOwner {
    /// User id.
    pub id: UserId,
    /// The linked Supabase user.
    pub supabase_user_id: Option<Uuid>,
}

/// The local owner, if this database has one (solo mode).
pub async fn local_owner(db: &Db) -> Result<Option<LocalOwner>, DbError> {
    let row: Option<(Uuid, Option<Uuid>)> =
        sqlx::query_as("SELECT id, supabase_user_id FROM users WHERE is_local_owner")
            .fetch_optional(db.pool())
            .await?;
    Ok(row.map(|(id, sb)| LocalOwner {
        id: UserId::from_uuid(id),
        supabase_user_id: sb,
    }))
}

/// What the sign-in provider says about a person.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    /// Supabase user id (`sub`).
    pub supabase_user_id: Uuid,
    /// Email address.
    pub email: Option<String>,
    /// Display name.
    pub display_name: String,
    /// Avatar URL.
    pub avatar_url: Option<String>,
}

/// Links the local owner to `profile` and refreshes its name, email and avatar.
pub async fn link_local_owner(
    tx: &mut Tx,
    owner: UserId,
    profile: &Profile,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE users SET supabase_user_id = $2, email = $3, display_name = $4, avatar_url = $5, \
         updated_at = now() WHERE id = $1",
    )
    .bind(owner.as_uuid())
    .bind(profile.supabase_user_id)
    .bind(&profile.email)
    .bind(&profile.display_name)
    .bind(&profile.avatar_url)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The user for a Supabase account (team mode), made on first sight with no roles.
pub async fn upsert_supabase_user(db: &Db, profile: &Profile) -> Result<UserId, DbError> {
    let mut tx = db.begin().await?;
    let user = upsert_supabase_user_in(&mut tx, profile).await?;
    tx.commit().await.map_err(DbError::from)?;
    Ok(user)
}

/// [`upsert_supabase_user`] inside `tx`, so it commits with the sign-in's audit row.
pub async fn upsert_supabase_user_in(tx: &mut Tx, profile: &Profile) -> Result<UserId, DbError> {
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO users (id, supabase_user_id, email, display_name, avatar_url) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (supabase_user_id) DO UPDATE SET email = EXCLUDED.email, \
           display_name = EXCLUDED.display_name, avatar_url = EXCLUDED.avatar_url, updated_at = now() \
         RETURNING id",
    )
    .bind(UserId::new().as_uuid())
    .bind(profile.supabase_user_id)
    .bind(&profile.email)
    .bind(&profile.display_name)
    .bind(&profile.avatar_url)
    .fetch_one(&mut **tx)
    .await?;
    Ok(UserId::from_uuid(id))
}

/// Resource kind of platform-wide actions, like creating an org. Only the solo owner and
/// users with `platform_admin` pass.
pub const PLATFORM: &str = "platform";

/// Resource kind of setup sync (the person's own setup profile and sync keys). Tokens bound
/// to an org or a project are refused it.
pub const SETUP_PROFILE: &str = "setup_profile";
/// Unscoped account views. Token rows are filtered separately by their own grants.
pub const TOKEN_LIST: &str = "token_list";

/// Whether `user` holds the platform grant (`users.platform_admin`).
pub async fn is_platform_admin(db: &Db, user: UserId) -> Result<bool, DbError> {
    let flag: Option<bool> = sqlx::query_scalar("SELECT platform_admin FROM users WHERE id = $1")
        .bind(user.as_uuid())
        .fetch_optional(db.pool())
        .await?;
    Ok(flag == Some(true))
}

/// Gives or takes the platform grant (team setup, P43). Audit it in the same `tx`.
pub async fn set_platform_admin(tx: &mut Tx, user: UserId, on: bool) -> Result<(), DbError> {
    sqlx::query("UPDATE users SET platform_admin = $2, updated_at = now() WHERE id = $1")
        .bind(user.as_uuid())
        .bind(on)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Human roles that may take `action` (for filtering lists in SQL).
pub fn roles_allowing(action: Action) -> Vec<&'static str> {
    Role::ALL
        .into_iter()
        .filter(|&r| r.is_human() && allowed(r, action))
        .map(Role::as_str)
        .collect()
}

/// Whether `user` is active (not disabled).
pub async fn is_active(db: &Db, user: UserId) -> Result<bool, DbError> {
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM users WHERE id = $1")
        .bind(user.as_uuid())
        .fetch_optional(db.pool())
        .await?;
    Ok(status.as_deref() == Some("active"))
}

/// The org a user acts in by default: the oldest org they hold a role in.
pub async fn primary_org(db: &Db, user: UserId) -> Result<Option<OrgId>, DbError> {
    primary_org_with(db.pool(), user).await
}

/// Resolves the default org using an existing transaction connection or the pool.
pub async fn primary_org_with<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    user: UserId,
) -> Result<Option<OrgId>, DbError> {
    let org: Option<Uuid> = sqlx::query_scalar(
        "SELECT org_id FROM role_bindings WHERE principal_kind = 'user' AND principal_id = $1 \
         ORDER BY created_at, org_id LIMIT 1",
    )
    .bind(user.as_uuid())
    .fetch_optional(executor)
    .await?;
    Ok(org.map(OrgId::from_uuid))
}

/// The principal for `user`: the local owner in solo mode, else a team-mode user. A Team
/// core on a former Solo database treats the former owner as an ordinary user.
pub async fn principal_for(db: &Db, mode: Mode, user: UserId) -> Result<Principal, DbError> {
    if mode == Mode::Solo && local_owner(db).await?.is_some_and(|o| o.id == user) {
        return Ok(Principal::local_owner());
    }
    Ok(Principal {
        id: user.to_string(),
        kind: PrincipalKind::User,
        org: primary_org(db, user).await?.map(|o| o.to_string()),
    })
}

/// The person behind a principal: the local owner, the user, or the owner of a token.
pub async fn acting_user(db: &Db, principal: &Principal) -> Result<Option<UserId>, DbError> {
    match principal.kind {
        PrincipalKind::LocalOwner => Ok(local_owner(db).await?.map(|o| o.id)),
        PrincipalKind::User => Ok(UserId::from_str(&principal.id).ok()),
        PrincipalKind::Service => Ok(credentials::token_grant(db, &principal.id)
            .await?
            .map(|g| g.user)),
    }
}

/// What a role binding covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// The whole org.
    Org,
    /// One workspace and its projects.
    Workspace(WorkspaceId),
    /// One project.
    Project(ProjectId),
}

/// Gives `user` `role` over `scope` in `org` (no-op if it already has it).
pub async fn bind_role(
    tx: &mut Tx,
    org: OrgId,
    user: UserId,
    role: Role,
    scope: Scope,
) -> Result<(), DbError> {
    let (kind, id) = match scope {
        Scope::Org => ("org", org.as_uuid()),
        Scope::Workspace(w) => ("workspace", w.as_uuid()),
        Scope::Project(p) => ("project", p.as_uuid()),
    };
    sqlx::query(
        "INSERT INTO role_bindings (id, org_id, principal_kind, principal_id, role, scope_kind, scope_id) \
         VALUES ($1, $2, 'user', $3, $4, $5, $6) ON CONFLICT DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(org.as_uuid())
    .bind(user.as_uuid())
    .bind(role.as_str())
    .bind(kind)
    .bind(id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The roles `user` holds over `project` or `workspace` (only org-wide roles when both are
/// `None`). A workspace role covers the workspace and its projects.
pub async fn roles(
    db: &Db,
    user: UserId,
    org: OrgId,
    project: Option<ProjectId>,
    workspace: Option<WorkspaceId>,
) -> Result<Vec<Role>, DbError> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT rb.role FROM role_bindings rb \
         WHERE rb.principal_kind = 'user' AND rb.principal_id = $1 AND rb.org_id = $2 AND ( \
           rb.scope_kind = 'org' \
           OR (rb.scope_kind = 'project' AND rb.scope_id = $3) \
           OR (rb.scope_kind = 'workspace' AND (rb.scope_id = $4 OR rb.scope_id = \
               (SELECT workspace_id FROM projects WHERE id = $3 AND org_id = $2))))",
    )
    .bind(user.as_uuid())
    .bind(org.as_uuid())
    .bind(project.map(|p| p.as_uuid()))
    .bind(workspace.map(|w| w.as_uuid()))
    .fetch_all(db.pool())
    .await?;
    Ok(names.iter().filter_map(|n| role_from_str(n)).collect())
}

fn role_from_str(name: &str) -> Option<Role> {
    Role::ALL.into_iter().find(|r| r.as_str() == name)
}

/// The action named `name`, like `runs.submit`.
pub fn action_from_str(name: &str) -> Option<Action> {
    Action::ALL.iter().copied().find(|a| a.as_str() == name)
}

/// RBAC over P06's matrix. Roles come from `role_bindings`; tokens are also limited by
/// their scopes. Someone with no role in the resource's org gets `Hide` (404), so other
/// orgs' resources never show; `auth.session` needs no role.
pub struct Rbac {
    db: Db,
    mode: Mode,
}

impl Rbac {
    /// RBAC over this database. The local owner's shortcuts apply in solo mode only.
    pub fn new(db: Db, mode: Mode) -> Self {
        Self { db, mode }
    }

    async fn decide(
        &self,
        ctx: &RequestContext,
        action: Action,
        resource: &Resource,
    ) -> Result<Decision, DbError> {
        // A named org that is not an org id names nothing: it looks missing.
        let named_org = match resource.org.as_deref().or(ctx.org.as_deref()) {
            None => None,
            Some(o) => match OrgId::from_str(o) {
                Ok(o) => Some(o),
                Err(_) => return Ok(Decision::Hide),
            },
        };
        // A named project must exist and sit in the named org, before any Allow path (the
        // solo owner's included); otherwise it looks missing, like another org's.
        let (project, project_org) = match resource.project.as_deref().or(ctx.project.as_deref()) {
            None => (None, None),
            Some(raw) => {
                let Ok(p) = ProjectId::from_str(raw) else {
                    return Ok(Decision::Hide);
                };
                let org: Option<Uuid> = sqlx::query_scalar(
                    "SELECT org_id FROM projects WHERE id = $1 AND deleted_at IS NULL",
                )
                .bind(p.as_uuid())
                .fetch_optional(self.db.pool())
                .await?;
                let Some(org) = org.map(OrgId::from_uuid) else {
                    return Ok(Decision::Hide);
                };
                if named_org.is_some_and(|n| n != org) {
                    return Ok(Decision::Hide);
                }
                (Some(p), Some(org))
            }
        };
        let (user, token_org) = match ctx.principal.kind {
            PrincipalKind::LocalOwner if self.mode == Mode::Solo => {
                return Ok(verdict(&[Role::Owner], action))
            }
            PrincipalKind::LocalOwner => return Ok(Decision::Deny),
            PrincipalKind::User => match UserId::from_str(&ctx.principal.id) {
                Ok(u) => (u, None),
                Err(_) => return Ok(Decision::Deny),
            },
            PrincipalKind::Service => {
                let Some(g) = credentials::token_grant(&self.db, &ctx.principal.id).await? else {
                    return Ok(Decision::Deny);
                };
                if let (Some(t), Some(p)) = (g.org, project_org) {
                    if t != p {
                        return Ok(Decision::Hide);
                    }
                }
                // Token limits apply before any role, the solo owner's included.
                if !credentials::scopes_allow(&g.scopes, action) {
                    return Ok(Decision::Deny);
                }
                // The setup profile holds the person's whole setup, outside every org, so a
                // token held to an org or a project never reaches it.
                if resource.kind == SETUP_PROFILE && (g.org.is_some() || g.project.is_some()) {
                    return Ok(Decision::Deny);
                }
                let personal = matches!(action, Action::AuthSession | Action::AuthManageTokens);
                if let (Some(bound), false) = (g.project, personal) {
                    match project {
                        Some(p) if p == bound => {}
                        // Another project looks missing, like another org.
                        Some(_) => return Ok(Decision::Hide),
                        // Org-wide actions are outside a one-project token.
                        None => return Ok(Decision::Deny),
                    }
                }
                (g.user, g.org)
            }
        };
        // An org-bound token stays in its org before any role, platform grant or the solo
        // owner's shortcut: platform actions are outside it, another org looks missing.
        if let Some(bound) = token_org {
            if resource.kind == PLATFORM {
                return Ok(Decision::Deny);
            }
            if named_org.is_some_and(|o| o != bound) {
                return Ok(Decision::Hide);
            }
        }
        if !is_active(&self.db, user).await? {
            return Ok(Decision::Deny);
        }
        // The local-owner flag stays in the database after a switch to team mode; it counts
        // only in solo mode.
        if let Some(owner) = local_owner(&self.db)
            .await?
            .filter(|_| self.mode == Mode::Solo)
        {
            if owner.id == user {
                return Ok(verdict(&[Role::Owner], action));
            }
        }
        // Platform actions (like creating orgs) need the explicit platform grant; an org
        // role, even owner, does not reach them.
        if resource.kind == PLATFORM {
            return Ok(if is_platform_admin(&self.db, user).await? {
                verdict(&[Role::Owner], action)
            } else {
                Decision::Deny
            });
        }
        if action == Action::AuthSession {
            return Ok(Decision::Allow);
        }
        let org = named_org.or(project_org).or(token_org);
        let Some(org) = org else {
            return Ok(Decision::Deny);
        };
        if token_org.is_some_and(|t| t != org) {
            return Ok(Decision::Hide);
        }
        // A `workspace` resource (by id) also counts roles bound to that workspace.
        let workspace = (resource.kind == "workspace")
            .then(|| {
                resource
                    .id
                    .as_deref()
                    .and_then(|w| WorkspaceId::from_str(w).ok())
            })
            .flatten();
        let held = if resource.kind == TOKEN_LIST
            && action == Action::AuthManageTokens
            && project.is_none()
            && ctx.principal.kind == PrincipalKind::User
        {
            let names: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT role FROM role_bindings WHERE principal_kind = 'user' AND principal_id = $1",
            )
            .bind(user.as_uuid())
            .fetch_all(self.db.pool())
            .await?;
            names.iter().filter_map(|n| role_from_str(n)).collect()
        } else {
            roles(&self.db, user, org, project, workspace).await?
        };
        // No role over this resource (another org, or a project they are not on): 404.
        if held.is_empty() {
            return Ok(Decision::Hide);
        }
        Ok(verdict(&held, action))
    }
}

fn verdict(roles: &[Role], action: Action) -> Decision {
    if roles.iter().any(|&r| allowed(r, action)) {
        Decision::Allow
    } else {
        Decision::Deny
    }
}

#[async_trait]
impl Authorizer for Rbac {
    async fn authorize(
        &self,
        ctx: &RequestContext,
        action: &str,
        resource: &Resource,
    ) -> Result<Decision, SeamError> {
        let Some(action) = action_from_str(action) else {
            tracing::warn!(
                action,
                "authorization asked about an unknown action; refusing"
            );
            return Ok(Decision::Deny);
        };
        Ok(self.decide(ctx, action, resource).await?)
    }
}
