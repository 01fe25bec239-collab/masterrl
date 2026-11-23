//! The engine's part of a launch (A1, at the P24 merge): the run's engine version, picked at
//! submit (`runs.engine_version_id`), gives the engine name and version and, for every role, the
//! runtime environment that matches the hardware the role was placed on.
//!
//! - [`EngineRuntime`] (launch chain): an NVIDIA/AMD/Apple role takes the role's environment
//!   with that accelerator on the machine's OS; a processor-only role takes a CPU environment
//!   (its OS first, else any: a uv lock resolves on the machine). `uv` environments are named
//!   `uv:sha256:<lockSha256>`, images `oci:<image>`. A run config's pinned runtime stays.
//! - [`EnginePause`] (pause chain): the method's `pause` flag in the manifest.

use crate::db::Db;
use crate::modules::jobs::{AttemptRow, JobRow};
use crate::modules::scheduler::pipeline::{
    LaunchContributor, LaunchError, LaunchPlan, PauseSupport, Placement,
};
use crate::modules::scheduler::SchedError;
use mrl_domain::failure::FailureClass;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

/// The run's engine version: name, version and stored manifest.
async fn engine_of(db: &Db, job: &JobRow) -> Result<Option<(String, String, Value)>, sqlx::Error> {
    let Some(run) = job.run_id else {
        return Ok(None);
    };
    let row = sqlx::query(
        "SELECT te.name, ev.version, ev.manifest FROM runs r
         JOIN engine_versions ev ON ev.id = r.engine_version_id
         JOIN training_engines te ON te.id = ev.engine_id
         WHERE r.id = $1 AND r.org_id = $2",
    )
    .bind(run)
    .bind(job.org_id)
    .fetch_optional(db.pool())
    .await?;
    row.map(|r| {
        Ok((
            r.try_get("name")?,
            r.try_get("version")?,
            r.try_get("manifest")?,
        ))
    })
    .transpose()
}

/// The OS part of an environment platform (`linux/amd64` → `linux`) the machine runs.
async fn node_os(db: &Db, node: Uuid) -> Result<Option<&'static str>, sqlx::Error> {
    let inventory: Option<Value> =
        sqlx::query_scalar("SELECT inventory FROM compute_nodes WHERE id = $1")
            .bind(node)
            .fetch_optional(db.pool())
            .await?
            .flatten();
    Ok(inventory.and_then(|i| match i["platform"].as_str()? {
        "PLATFORM_LINUX" | "PLATFORM_WSL2" => Some("linux"),
        "PLATFORM_MACOS" => Some("darwin"),
        "PLATFORM_WINDOWS" => Some("windows"),
        _ => None,
    }))
}

/// The environment of `spec` for `role` on hardware `class` (`cpu`, `nvidia`, …) and `os`.
pub fn environment<'a>(
    spec: &'a Value,
    role: &str,
    class: &str,
    os: Option<&str>,
) -> Option<&'a Value> {
    let envs = spec["runtime"]["environments"].as_array()?;
    let listed: Vec<&str> = spec["runtimes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|r| r["role"] == role)
        .filter_map(|r| r["runtime_ref"].as_str())
        .collect();
    let candidates = envs.iter().filter(|e| {
        (listed.is_empty() || e["id"].as_str().is_some_and(|id| listed.contains(&id)))
            && e["accelerator"]["vendor"].as_str().unwrap_or("cpu") == class
    });
    let same_os = |e: &&Value| {
        os.is_some_and(|os| {
            e["platform"]
                .as_str()
                .is_some_and(|p| p.split('/').next() == Some(os))
        })
    };
    let mut candidates: Vec<&Value> = candidates.collect();
    if let Some(e) = candidates.iter().find(|e| same_os(e)) {
        return Some(e);
    }
    // A processor-only uv lock resolves on any machine; GPU runtimes need their own OS.
    if class == "cpu" {
        return candidates.drain(..).next();
    }
    None
}

/// How the agent names an environment.
pub fn runtime_ref(env: &Value) -> Option<String> {
    match env["kind"].as_str() {
        Some("uv") => env["lockSha256"].as_str().map(|h| format!("uv:sha256:{h}")),
        _ => env["image"].as_str().map(|i| format!("oci:{i}")),
    }
}

/// Launch contributor: engine name, version and each role's runtime.
pub struct EngineRuntime {
    db: Db,
}

impl EngineRuntime {
    /// Reads runs and engine versions from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl LaunchContributor for EngineRuntime {
    fn name(&self) -> &'static str {
        "engine_runtime"
    }

    async fn contribute(
        &self,
        job: &JobRow,
        _attempt: &AttemptRow,
        placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        let transient =
            |e: sqlx::Error| LaunchError::Transient(format!("the database did not answer: {e}"));
        // No engine recorded (not a run, or submitted before P24): the launch check decides.
        let Some((name, version, manifest)) = engine_of(&self.db, job).await.map_err(transient)?
        else {
            return Ok(());
        };
        let os = node_os(&self.db, placement.node_id)
            .await
            .map_err(transient)?;
        let spec = &manifest["spec"];
        for role in &mut plan.launch.roles {
            if !role.runtime_ref.is_empty() {
                continue;
            }
            let class = placement
                .roles
                .iter()
                .find(|(r, _)| *r == role.kind)
                .and_then(|(_, gpus)| gpus.first())
                .map_or("cpu", |g| g.vendor.as_str());
            let env = environment(spec, &role.kind, class, os).ok_or_else(|| {
                LaunchError::Fail(
                    FailureClass::DependencyEnv,
                    format!(
                        "The engine {name} {version} has no {} runtime for the {} role on this machine.",
                        class, role.kind
                    ),
                )
            })?;
            role.runtime_ref = runtime_ref(env).ok_or_else(|| {
                LaunchError::Fail(
                    FailureClass::DependencyEnv,
                    format!(
                        "The engine {name} {version} names an environment it does not describe."
                    ),
                )
            })?;
            if role.primary {
                plan.launch.environment_id = env["id"].as_str().unwrap_or_default().to_owned();
                plan.launch.environment_digest =
                    env["lockSha256"].as_str().unwrap_or_default().to_owned();
            }
        }
        plan.launch.engine_name = name;
        plan.launch.engine_version = version;
        Ok(())
    }
}

/// Pause support: the method's `pause` flag in the run's engine manifest.
pub struct EnginePause {
    db: Db,
}

impl EnginePause {
    /// Reads runs and engine versions from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl PauseSupport for EnginePause {
    fn name(&self) -> &'static str {
        "engine_pause"
    }

    async fn can_pause(&self, job: &JobRow) -> Result<Option<bool>, SchedError> {
        let db = |e: sqlx::Error| SchedError::Db(crate::db::DbError::from(e));
        let Some((_, _, manifest)) = engine_of(&self.db, job).await.map_err(db)? else {
            return Ok(None);
        };
        let Some(run) = job.run_id else {
            return Ok(None);
        };
        let algorithm: String = sqlx::query_scalar("SELECT algorithm_id FROM runs WHERE id = $1")
            .bind(run)
            .fetch_one(self.db.pool())
            .await
            .map_err(db)?;
        Ok(manifest["spec"]["algorithms"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|a| a["id"] == algorithm.as_str())
            .map(|a| a["pause"].as_bool().unwrap_or(false)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> Value {
        json!({
            "runtime": {"environments": [
                {"id": "cuda-uv", "kind": "uv", "lockSha256": "aa", "platform": "linux/amd64",
                 "accelerator": {"vendor": "nvidia"}},
                {"id": "cpu-uv", "kind": "uv", "lockSha256": "bb", "platform": "linux/amd64",
                 "accelerator": {"vendor": "cpu"}},
                {"id": "mps-uv", "kind": "uv", "lockSha256": "cc", "platform": "darwin/arm64",
                 "accelerator": {"vendor": "apple"}},
                {"id": "img", "kind": "oci", "image": "r/x@sha256:dd", "platform": "linux/amd64",
                 "accelerator": {"vendor": "amd"}}
            ]},
            "runtimes": [{"role": "trainer", "runtime_ref": "cuda-uv"},
                         {"role": "trainer", "runtime_ref": "cpu-uv"},
                         {"role": "trainer", "runtime_ref": "mps-uv"},
                         {"role": "trainer", "runtime_ref": "img"}]
        })
    }

    #[test]
    fn each_role_gets_the_runtime_of_its_hardware() {
        let s = spec();
        let pick = |class, os| environment(&s, "trainer", class, os).and_then(runtime_ref);
        assert_eq!(
            pick("nvidia", Some("linux")).as_deref(),
            Some("uv:sha256:aa")
        );
        assert_eq!(
            pick("nvidia", Some("darwin")),
            None,
            "a GPU runtime needs its OS"
        );
        assert_eq!(
            pick("apple", Some("darwin")).as_deref(),
            Some("uv:sha256:cc")
        );
        assert_eq!(
            pick("cpu", Some("darwin")).as_deref(),
            Some("uv:sha256:bb"),
            "CPU on any OS"
        );
        assert_eq!(pick("cpu", None).as_deref(), Some("uv:sha256:bb"));
        assert_eq!(
            pick("amd", Some("linux")).as_deref(),
            Some("oci:r/x@sha256:dd")
        );
        assert_eq!(
            environment(&s, "reference", "cpu", None).map(|e| e["id"].clone()),
            Some(json!("cpu-uv")),
            "unlisted role: any environment"
        );
    }
}
