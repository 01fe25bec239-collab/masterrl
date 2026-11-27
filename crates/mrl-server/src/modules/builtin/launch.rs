//! The built-in job's part of a launch: its task, inputs and sandbox in the job spec, the
//! built-in engine and runtime for every role, and its stored files to stage.

use crate::db::Db;
use crate::modules::artifacts::InputFile;
use crate::modules::jobs::{AttemptRow, JobRow};
use crate::modules::scheduler::pipeline::{LaunchContributor, LaunchError, LaunchPlan, Placement};
use mrl_domain::failure::FailureClass;
use serde_json::{Map, Value};

/// The `mrl_runtime.engines` entry point the worker loads (`mrl-worker --engine builtin`).
pub const ENGINE: &str = "builtin";

fn environment() -> Option<&'static Value> {
    super::manifest()["spec"]["runtime"]["environments"]
        .as_array()?
        .first()
}

/// How the agent names the built-in runtime (`uv:sha256:<lock>`).
pub fn runtime_ref() -> Option<String> {
    crate::modules::engines::launch::runtime_ref(environment()?)
}

/// Launch contributor for jobs with a `builtin_jobs` row; abstains for every other job.
pub struct BuiltinLaunch {
    db: Db,
}

impl BuiltinLaunch {
    /// Reads `builtin_jobs` from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl LaunchContributor for BuiltinLaunch {
    fn name(&self) -> &'static str {
        "builtin_launch"
    }

    async fn contribute(
        &self,
        job: &JobRow,
        _attempt: &AttemptRow,
        _placement: &Placement,
        plan: &mut LaunchPlan,
    ) -> Result<(), LaunchError> {
        let row: Option<(String, Value, sqlx::types::Json<Vec<InputFile>>)> =
            sqlx::query_as("SELECT task, inputs, files FROM builtin_jobs WHERE job_id = $1")
                .bind(job.id)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|e| LaunchError::Transient(format!("the database did not answer: {e}")))?;
        let Some((task, inputs, files)) = row else {
            return Ok(());
        };
        let fail = |class, why: String| LaunchError::Fail(class, why);
        let sandbox = super::sandbox(&job.kind).ok_or_else(|| {
            fail(
                FailureClass::UserConfig,
                format!("{} is not a built-in job kind.", job.kind),
            )
        })?;
        let (Some(env), Some(rt)) = (environment(), runtime_ref()) else {
            return Err(fail(
                FailureClass::DependencyEnv,
                "The built-in engine has no runtime environment.".into(),
            ));
        };
        let mut spec = match serde_json::from_str::<Value>(&plan.launch.job_spec_json) {
            Ok(Value::Object(m)) => m,
            _ => Map::new(),
        };
        spec.insert("task".into(), Value::String(task));
        spec.insert("inputs".into(), inputs);
        spec.insert("sandbox".into(), Value::String(sandbox.to_owned()));
        plan.launch.job_spec_json = Value::Object(spec).to_string();
        for role in &mut plan.launch.roles {
            role.runtime_ref.clone_from(&rt);
            if role.primary {
                plan.launch.environment_id = env["id"].as_str().unwrap_or_default().to_owned();
                plan.launch.environment_digest =
                    env["lockSha256"].as_str().unwrap_or_default().to_owned();
            }
        }
        let meta = &super::manifest()["metadata"];
        plan.launch.engine_name = ENGINE.to_owned();
        plan.launch.engine_version = meta["version"].as_str().unwrap_or_default().to_owned();
        plan.inputs.extend(files.0);
        Ok(())
    }
}
