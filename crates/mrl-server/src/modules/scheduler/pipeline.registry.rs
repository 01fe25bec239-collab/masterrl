//! The registered pipeline members. Each part adds one line inside its block, like
//! `Arc::new(crate::modules::engines::EngineRuntime::new(db.clone())),`.

use super::admission::{MaxQueuedPerProject, PoolFlags, PoolMembership};
use super::launch::RunConfigSpec;
use super::pipeline::Pipeline;
use super::placement::SingleNodePacking;
use super::SchedulerConfig;
use crate::db::Db;
use crate::modules::compute::LocalAgentPool;
use std::sync::Arc;

/// Every registered member.
pub fn pipeline(db: &Db, cfg: &SchedulerConfig) -> Pipeline {
    Pipeline::new(
        vec![
            Arc::new(PoolMembership),
            Arc::new(PoolFlags),
            Arc::new(MaxQueuedPerProject {
                max: cfg.max_queued_per_project,
            }),
            // <mrl:admission-policies> Admission policies go right below this line.
        ],
        vec![
            Arc::new(SingleNodePacking),
            // <mrl:placement-strategies> Placement strategies go right below this line.
        ],
        vec![
            Arc::new(RunConfigSpec::new(db.clone())),
            // <mrl:launch-contributors> Launch contributors (runtime, inputs, credentials) go right below this line.
            Arc::new(crate::modules::engines::launch::EngineRuntime::new(
                db.clone(),
            )),
        ],
        vec![
            // <mrl:pause-support> Engines that can pause go right below this line.
            Arc::new(crate::modules::engines::launch::EnginePause::new(
                db.clone(),
            )),
        ],
        vec![
            Arc::new(LocalAgentPool::new(db.clone())),
            // <mrl:compute-providers> Compute providers go right below this line.
        ],
    )
}
