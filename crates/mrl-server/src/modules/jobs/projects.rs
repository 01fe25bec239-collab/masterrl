//! Which org owns a project. Runs, jobs and experiments are stored under their project's
//! org, whoever asks (the solo launch secret, a person, a project-bound token).
//!
//! P16 owns projects, so it registers the lookup: one line inside
//! `// <mrl:project-directory>` in [`Projects::registered`]. P17 never reads P16's tables.
//! With nothing registered (P17 on its own), a request acts in the caller's org.
//!
//! [`RehomeLocalRows`] runs at startup: rows stored under the nil org (by a solo build
//! without a directory) move to their project's org once a directory can say which. All of
//! an org's projects move in one transaction (runs can link across projects), and an
//! experiment the org already has under the same name absorbs the local one.

use super::error::JobsError;
use super::scope;
use crate::db::Db;
use crate::seams::{RequestContext, SeamError, StartupStep};
use async_trait::async_trait;
use mrl_domain::ids::{OrgId, ProjectId};
use std::sync::Arc;
use uuid::Uuid;

/// Answers which org owns a project (P16 implements it).
#[async_trait]
pub trait ProjectDirectory: Send + Sync {
    /// The org that owns `project`, or `None` when no such project exists.
    async fn owner(&self, project: ProjectId) -> Result<Option<OrgId>, SeamError>;
}

/// The registered [`ProjectDirectory`], if any.
#[derive(Clone, Default)]
pub struct Projects(Option<Arc<dyn ProjectDirectory>>);

impl Projects {
    /// The directory this build registers.
    pub fn registered(db: &Db) -> Self {
        let _ = db;
        let found: Vec<Arc<dyn ProjectDirectory>> = vec![
            // <mrl:project-directory> P16's lookup goes right below this line.
        ];
        Self(found.into_iter().next())
    }

    /// Exactly this directory (tests use this).
    pub fn with(directory: Arc<dyn ProjectDirectory>) -> Self {
        Self(Some(directory))
    }

    /// The org a request on `project` acts in: the project's owner (a missing project is
    /// 404), or the caller's org when no directory is registered.
    pub async fn org_for(
        &self,
        ctx: &RequestContext,
        project: ProjectId,
    ) -> Result<Uuid, JobsError> {
        match &self.0 {
            None => scope::tenant(ctx),
            Some(d) => d
                .owner(project)
                .await?
                .map(|o| o.as_uuid())
                .ok_or_else(|| JobsError::not_found("project")),
        }
    }
}

/// Startup step: moves nil-org rows to their project's org (see the module docs).
pub struct RehomeLocalRows {
    db: Db,
    projects: Projects,
}

impl RehomeLocalRows {
    /// The step for `db` and `projects`.
    pub fn new(db: Db, projects: Projects) -> Self {
        Self { db, projects }
    }

    /// Moves every nil-org row of `projects` to `org` in one transaction, so runs linked
    /// across these projects (parent and child) move together.
    async fn rehome(&self, org: Uuid, projects: &[Uuid]) -> Result<(), sqlx::Error> {
        let mut tx = self.db.pool().begin().await?;
        // The (org_id, id) foreign keys are checked at commit, when every row has moved.
        sqlx::query("SET CONSTRAINTS ALL DEFERRED")
            .execute(&mut *tx)
            .await?;
        for sql in [
            // An experiment the org already has under the same name (like `default`, made
            // once per credential) absorbs the local one: its runs point at the org's copy.
            // ponytail: the local copy's description and tags are dropped with it.
            "UPDATE runs r SET experiment_id = keep.id FROM experiments dup \
               JOIN experiments keep ON keep.org_id = $1 AND keep.project_id = dup.project_id \
                AND keep.name = dup.name \
             WHERE dup.org_id = $3 AND dup.project_id = ANY($2) AND r.experiment_id = dup.id",
            "DELETE FROM experiments dup USING experiments keep \
             WHERE dup.org_id = $3 AND dup.project_id = ANY($2) AND keep.org_id = $1 \
               AND keep.project_id = dup.project_id AND keep.name = dup.name",
            "UPDATE experiments SET org_id = $1 WHERE org_id = $3 AND project_id = ANY($2)",
            "UPDATE runs SET org_id = $1 WHERE org_id = $3 AND project_id = ANY($2)",
            "UPDATE run_configs SET org_id = $1 WHERE org_id = $3 \
               AND run_id IN (SELECT id FROM runs WHERE project_id = ANY($2))",
            "UPDATE jobs SET org_id = $1 WHERE org_id = $3 AND project_id = ANY($2)",
            "UPDATE job_attempts SET org_id = $1 WHERE org_id = $3 \
               AND job_id IN (SELECT id FROM jobs WHERE project_id = ANY($2))",
            "UPDATE attempt_placements SET org_id = $1 WHERE org_id = $3 AND attempt_id IN \
               (SELECT a.id FROM job_attempts a JOIN jobs j ON j.id = a.job_id \
                WHERE j.project_id = ANY($2))",
        ] {
            sqlx::query(sql)
                .bind(org)
                .bind(projects)
                .bind(Uuid::nil())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await
    }
}

#[async_trait]
impl StartupStep for RehomeLocalRows {
    fn name(&self) -> &str {
        "runs-rehome"
    }

    async fn run(&self) -> Result<(), SeamError> {
        let Some(directory) = &self.projects.0 else {
            return Ok(());
        };
        let fail = |e: sqlx::Error| SeamError::new("runs-rehome", e.to_string());
        let projects: Vec<Uuid> = sqlx::query_scalar(
            "SELECT project_id FROM experiments WHERE org_id = $1 \
             UNION SELECT project_id FROM runs WHERE org_id = $1 \
             UNION SELECT project_id FROM jobs WHERE org_id = $1",
        )
        .bind(Uuid::nil())
        .fetch_all(self.db.pool())
        .await
        .map_err(fail)?;
        let mut by_org: std::collections::BTreeMap<Uuid, Vec<Uuid>> = Default::default();
        for project in projects {
            let id = ProjectId::from_uuid(project);
            match directory.owner(id).await? {
                Some(org) => by_org.entry(org.as_uuid()).or_default().push(project),
                None => {
                    tracing::warn!(project = %id, "local runs name a project that no longer exists; left as they are")
                }
            }
        }
        for (org, projects) in by_org {
            // An org whose rows can't move is logged and left as it is, rather than
            // stopping the server; the next start tries again.
            if let Err(e) = self.rehome(org, &projects).await {
                let org = OrgId::from_uuid(org);
                tracing::error!(%org, error = %e, "could not move local runs to their project's org");
            }
        }
        Ok(())
    }
}
