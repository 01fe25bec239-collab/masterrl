//! Which org owns a project. Runs, jobs and experiments are stored under their project's
//! org, whoever asks (the solo launch secret, a person, a project-bound token).
//!
//! P16 owns projects, so it registers the lookup: one line inside
//! `// <mrl:project-directory>` in [`Projects::registered`]. P17 never reads P16's tables.
//! With nothing registered (P17 on its own), a request acts in the caller's org.
//!
//! [`RehomeLocalRows`] runs at startup: rows stored under the nil org (by a solo build
//! without a directory) move to their project's org once a directory can say which.
//! Projects linked by a parent run move together in one transaction; a local experiment
//! whose name the org already uses gets a fixed new name, keeping its id and metadata.

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
            // A local experiment whose name the org already uses in the project (like a
            // `default` made once per credential) keeps its id, runs and metadata and gets
            // a fixed new name: `<name> (local <last 8 hex of its id>)`, within 200 chars.
            "UPDATE experiments dup \
                SET name = left(dup.name, 183) || ' (local ' || right(replace(dup.id::text, '-', ''), 8) || ')', \
                    version = dup.version + 1, updated_at = now() \
               FROM experiments keep \
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
            // The retained job and attempt events (the timeline) follow their rows.
            "UPDATE outbox_events SET org_id = $1 WHERE org_id = $3 AND ( \
               (aggregate_kind = 'job' AND aggregate_id IN \
                  (SELECT id FROM jobs WHERE project_id = ANY($2))) OR \
               (aggregate_kind = 'attempt' AND aggregate_id IN \
                  (SELECT a.id FROM job_attempts a JOIN jobs j ON j.id = a.job_id \
                   WHERE j.project_id = ANY($2))))",
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

/// Groups `projects` so that projects linked by a parent run are in the same group.
// ponytail: relabels groups on every merge (quadratic); fine for the few local projects.
fn linked_groups(projects: &[Uuid], links: &[(Uuid, Uuid)]) -> Vec<Vec<Uuid>> {
    let mut group: std::collections::BTreeMap<Uuid, usize> =
        projects.iter().enumerate().map(|(i, p)| (*p, i)).collect();
    for (a, b) in links {
        let (Some(&ga), Some(&gb)) = (group.get(a), group.get(b)) else {
            continue;
        };
        if ga != gb {
            for g in group.values_mut() {
                if *g == gb {
                    *g = ga;
                }
            }
        }
    }
    let mut out: std::collections::BTreeMap<usize, Vec<Uuid>> = Default::default();
    for (p, g) in group {
        out.entry(g).or_default().push(p);
    }
    out.into_values().collect()
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
        // Projects linked by a parent run must move together, so each group moves in one
        // transaction. A group with a project the directory doesn't know, or spanning two
        // orgs, can't keep its links and is left as it is (logged); other groups still move.
        let links: Vec<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT DISTINCT c.project_id, p.project_id FROM runs c \
               JOIN runs p ON p.id = c.parent_run_id \
              WHERE c.org_id = $1 AND c.project_id <> p.project_id",
        )
        .bind(Uuid::nil())
        .fetch_all(self.db.pool())
        .await
        .map_err(fail)?;
        let mut owners = std::collections::BTreeMap::new();
        for project in &projects {
            owners.insert(
                *project,
                directory.owner(ProjectId::from_uuid(*project)).await?,
            );
        }
        for group in linked_groups(&projects, &links) {
            let orgs: std::collections::BTreeSet<Option<OrgId>> = group
                .iter()
                .map(|p| owners.get(p).copied().flatten())
                .collect();
            let names: Vec<String> = group
                .iter()
                .map(|p| ProjectId::from_uuid(*p).to_string())
                .collect();
            let org = match orgs.into_iter().collect::<Vec<_>>().as_slice() {
                [Some(org)] => *org,
                _ => {
                    tracing::warn!(projects = ?names, "local runs are linked to a project that is missing or in another org; left as they are");
                    continue;
                }
            };
            // A group that can't move is logged and left as it is, rather than stopping
            // the server; the next start tries again.
            if let Err(e) = self.rehome(org.as_uuid(), &group).await {
                tracing::error!(%org, projects = ?names, error = %e, "could not move local runs to their project's org");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linked_projects_group_together() {
        let [a, b, c, d] = [1u128, 2, 3, 4].map(Uuid::from_u128);
        let groups = linked_groups(&[a, b, c, d], &[(a, c), (c, d)]);
        assert_eq!(groups, vec![vec![a, c, d], vec![b]]);
        assert_eq!(linked_groups(&[a, b], &[]), vec![vec![a], vec![b]]);
        // Links to projects with no local rows are ignored.
        assert_eq!(linked_groups(&[a], &[(a, b)]), vec![vec![a]]);
    }
}
