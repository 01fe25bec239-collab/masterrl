//! Guardian preflight (Doc 24 §3): will the estimated memory fit the machines that can run
//! this, with their headroom kept free?
//!
//! - Headroom ([`Headroom`]): keep ≥ 10 % of GPU memory free; ≥ max(4 GB, 15 %) of system
//!   RAM (CPU-only runs); ≥ 25 % of unified memory on Apple Silicon.
//! - Per machine: `does_not_fit` when the expected need is above the memory, `tight` when the
//!   high end or the expected need is above the usable memory, else `fits`. The run's verdict
//!   is the best machine's; no estimate (or no machine) is `unknown`.
//! - `tight`: safety settings as a recorded config change ([`ConfigChange`]), each with a
//!   plain reason. `does_not_fit`: the nearest alternatives. `unknown`: probe mode (the first
//!   steps are measured, then [`ProbeModeGuard`] decides).

use crate::db::Db;
use crate::modules::estimates::{EstimateRange, Verdict};
use crate::modules::jobs::{JobTransitions, JobsError};
use crate::modules::telemetry::{IngestedBatch, MetricObserver};
use mrl_domain::ids::NodeId;
use mrl_domain::resources::{GpuVendor, ResourceRequest, RoleRequest, WorkloadRole};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

const GIB: f64 = (1u64 << 30) as f64;

/// Memory each machine keeps free (Doc 24 §3). Settings `[submission.headroom]`.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Headroom {
    /// Share of each GPU's memory kept free for drivers and fragmentation (0–0.9).
    pub gpu_free_fraction: f64,
    /// System RAM kept free at least, GiB.
    pub ram_free_min_gib: f64,
    /// Share of system RAM kept free (0–0.9); the larger of the two wins.
    pub ram_free_fraction: f64,
    /// Share of unified memory kept free on Apple Silicon (0–0.9).
    pub unified_free_fraction: f64,
}

impl Default for Headroom {
    fn default() -> Self {
        Self {
            gpu_free_fraction: 0.10,
            ram_free_min_gib: 4.0,
            ram_free_fraction: 0.15,
            unified_free_fraction: 0.25,
        }
    }
}

impl Headroom {
    /// GPU memory a run may use, GiB.
    pub fn usable_gpu(&self, vram_gib: f64) -> f64 {
        vram_gib * (1.0 - self.gpu_free_fraction)
    }

    /// System RAM a run may use, GiB.
    pub fn usable_ram(&self, ram_gib: f64) -> f64 {
        (ram_gib - self.ram_free_min_gib.max(ram_gib * self.ram_free_fraction)).max(0.0)
    }

    /// Unified memory a run may use on Apple Silicon, GiB.
    pub fn usable_unified(&self, total_gib: f64) -> f64 {
        total_gib * (1.0 - self.unified_free_fraction)
    }

    /// Settings that can't be used, in plain words.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (name, v) in [
            ("gpu_free_fraction", self.gpu_free_fraction),
            ("ram_free_fraction", self.ram_free_fraction),
            ("unified_free_fraction", self.unified_free_fraction),
        ] {
            if !(0.0..=0.9).contains(&v) {
                out.push(format!(
                    "submission.headroom.{name}: use 0 to 0.9 (got {v})"
                ));
            }
        }
        if !(0.0..=1024.0).contains(&self.ram_free_min_gib) {
            out.push(format!(
                "submission.headroom.ram_free_min_gib: use 0 to 1024 (got {})",
                self.ram_free_min_gib
            ));
        }
        out
    }
}

/// Which memory a machine check is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    /// Each GPU's own memory.
    Gpu,
    /// Apple Silicon unified memory.
    Unified,
    /// System RAM (CPU-only runs).
    System,
}

/// What the preflight knows about one connected machine.
#[derive(Debug, Clone, PartialEq)]
pub struct MachineFacts {
    /// Node id.
    pub node_id: Uuid,
    /// Host name.
    pub hostname: String,
    /// Healthy GPUs: maker and memory in bytes, by index.
    pub gpus: Vec<(String, i64)>,
    /// System RAM, bytes.
    pub ram_bytes: Option<u64>,
    /// Unified memory, bytes (Apple Silicon).
    pub unified_bytes: Option<u64>,
}

/// The role the estimate is about: the trainer, else the first one.
pub fn main_role(r: &ResourceRequest) -> Option<&RoleRequest> {
    r.roles
        .iter()
        .find(|x| x.role == WorkloadRole::Trainer)
        .or_else(|| r.roles.first())
}

fn vendor_name(v: GpuVendor) -> &'static str {
    match v {
        GpuVendor::Nvidia => "nvidia",
        GpuVendor::Amd => "amd",
        GpuVendor::Apple => "apple",
        GpuVendor::Intel => "intel",
    }
}

/// The GPU bucket of a request for calibration keys: its maker, `gpu` (any) or `cpu`.
pub fn gpu_bucket(r: &ResourceRequest) -> &'static str {
    match main_role(r) {
        Some(role) if role.gpu_count > 0 => role.gpu.vendor.map_or("gpu", vendor_name),
        _ => "cpu",
    }
}

/// The memory `role` would use on `m` and how much of it: `(kind, total GiB, usable GiB)`,
/// or `None` when the machine can't run the role at all (too few GPUs of the asked maker).
pub fn capacity(
    m: &MachineFacts,
    role: Option<&RoleRequest>,
    h: &Headroom,
) -> Option<(MemoryKind, f64, f64)> {
    let count = role.map_or(0, |r| r.gpu_count) as usize;
    if count == 0 {
        let ram = m.ram_bytes? as f64 / GIB;
        return Some((MemoryKind::System, ram, h.usable_ram(ram)));
    }
    let want = role.and_then(|r| r.gpu.vendor).map(vendor_name);
    let mut fitting: Vec<i64> = m
        .gpus
        .iter()
        .filter(|(v, _)| want.is_none_or(|w| w == v))
        .map(|(_, b)| *b)
        .collect();
    if fitting.len() < count {
        return None;
    }
    // The run gets the biggest GPUs it asks for; the smallest of those bounds it.
    fitting.sort_unstable_by(|a, b| b.cmp(a));
    let smallest = fitting[count - 1] as f64 / GIB;
    let apple = m.gpus.iter().any(|(v, _)| v == "apple");
    if apple {
        let total = m
            .unified_bytes
            .or(m.ram_bytes)
            .map_or(smallest, |b| b as f64 / GIB);
        return Some((MemoryKind::Unified, total, h.usable_unified(total)));
    }
    Some((MemoryKind::Gpu, smallest, h.usable_gpu(smallest)))
}

/// The verdict for a need of `range` GiB against `capacity` (all of it) and `usable` GiB.
pub fn fit(range: Option<EstimateRange>, capacity: f64, usable: f64) -> Verdict {
    match range {
        None => Verdict::Unknown,
        Some(r) if r.expected > capacity => Verdict::DoesNotFit,
        Some(r) if r.hi > usable || r.expected > usable => Verdict::Tight,
        Some(_) => Verdict::Fits,
    }
}

/// One machine's answer.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct PreflightMachine {
    /// `node_…`.
    pub node_id: String,
    /// Host name.
    pub hostname: String,
    /// Which memory was compared.
    pub memory: MemoryKind,
    /// Memory per GPU (or all of it), GiB.
    pub capacity_gib: f64,
    /// What the run may use after the headroom, GiB.
    pub usable_gib: f64,
    /// This machine's verdict.
    pub verdict: Verdict,
}

/// One setting the preflight changed, and why.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct ConfigChange {
    /// JSON Pointer into the config, like `/engine/gradient_checkpointing`.
    pub path: String,
    /// The value before (null when unset).
    #[schema(value_type = Object)]
    pub from: Value,
    /// The value after.
    #[schema(value_type = Object)]
    pub to: Value,
    /// Why, in plain words.
    pub why: String,
}

/// A configuration that may fit instead.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct PreflightAlternative {
    /// `qlora`, `smaller_model` or `machine`.
    pub kind: String,
    /// What to do, in plain words.
    pub message: String,
    /// The machine, for `machine`.
    pub node_id: Option<String>,
}

/// The preflight answer.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunPreflight {
    /// `fits`, `tight`, `does_not_fit` or `unknown`.
    pub verdict: Verdict,
    /// What happens next, in plain words.
    pub message: String,
    /// Every connected machine that could run this, with its own verdict.
    pub machines: Vec<PreflightMachine>,
    /// For `tight`: the safety settings applied (already in the returned config).
    pub changes: Vec<ConfigChange>,
    /// For `does_not_fit`: what may fit instead.
    pub alternatives: Vec<PreflightAlternative>,
    /// For `unknown`: the run's first steps are measured, then the app decides.
    pub probe_steps: Option<u32>,
}

/// Connected machines in pools the org may use (ONLINE, pool not paused), or just `node`.
pub async fn machines(
    db: &Db,
    org: Uuid,
    node: Option<Uuid>,
) -> Result<Vec<MachineFacts>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT n.id, n.hostname, n.inventory,
                coalesce(array_agg(g.vendor ORDER BY g.index_on_node)
                         FILTER (WHERE g.id IS NOT NULL), '{}') AS vendors,
                coalesce(array_agg(g.vram_total_bytes ORDER BY g.index_on_node)
                         FILTER (WHERE g.id IS NOT NULL), '{}') AS vram
           FROM compute_nodes n JOIN compute_pools p ON p.id = n.pool_id
           LEFT JOIN gpu_devices g ON g.node_id = n.id AND g.present AND g.health = 'OK'
                AND g.parent_device_id IS NULL
          WHERE CASE WHEN $2::uuid IS NULL
                     THEN n.status = 'ONLINE' AND (p.org_id IS NULL OR p.org_id = $1)
                          AND NOT ($3 = ANY (p.flags))
                     ELSE n.id = $2 END
          GROUP BY n.id ORDER BY n.hostname, n.id",
    )
    .bind(org)
    .bind(node)
    .bind(crate::modules::scheduler::admission::PAUSED_FLAG)
    .fetch_all(db.pool())
    .await?;
    rows.iter()
        .map(|r| {
            let inv: Option<Value> = r.try_get("inventory")?;
            let inv = inv.unwrap_or(Value::Null);
            let vendors: Vec<String> = r.try_get("vendors")?;
            let vram: Vec<i64> = r.try_get("vram")?;
            Ok(MachineFacts {
                node_id: r.try_get("id")?,
                hostname: r.try_get("hostname")?,
                gpus: vendors.into_iter().zip(vram).collect(),
                ram_bytes: inv["ram_bytes"].as_u64().filter(|b| *b > 0),
                unified_bytes: inv["unified_memory_bytes"].as_u64().filter(|b| *b > 0),
            })
        })
        .collect()
}

/// What the preflight looks at.
pub struct PreflightInput<'a> {
    /// What the run asks the scheduler for.
    pub resources: &'a ResourceRequest,
    /// The estimate per GPU (or for system/unified memory), GiB.
    pub estimate: Option<EstimateRange>,
    /// The resolved config.
    pub config: &'a Value,
    /// The method's config schema, if the engine has one.
    pub schema: Option<&'a Value>,
    /// The engine offers QLoRA.
    pub supports_qlora: bool,
    /// Probe-mode step count for `unknown`.
    pub probe_steps: u32,
}

/// Decides for `input` over `all` connected machines; returns the answer and the config with
/// the safety settings applied (unchanged unless `tight`).
pub fn decide(input: &PreflightInput, all: &[MachineFacts], h: &Headroom) -> (RunPreflight, Value) {
    let role = main_role(input.resources);
    let pins: Vec<Uuid> = role
        .map(|r| r.node_ids.iter().map(|n| n.as_uuid()).collect())
        .unwrap_or_default();
    let check = |m: &MachineFacts| {
        capacity(m, role, h).map(|(memory, cap, usable)| PreflightMachine {
            node_id: NodeId::from_uuid(m.node_id).to_string(),
            hostname: m.hostname.clone(),
            memory,
            capacity_gib: cap,
            usable_gib: usable,
            verdict: fit(input.estimate, cap, usable),
        })
    };
    let (candidates, others): (Vec<&MachineFacts>, Vec<&MachineFacts>) = all
        .iter()
        .partition(|m| pins.is_empty() || pins.contains(&m.node_id));
    let machines: Vec<PreflightMachine> = candidates.iter().filter_map(|m| check(m)).collect();
    let rank = |v: Verdict| match v {
        Verdict::Fits => 0,
        Verdict::Tight => 1,
        Verdict::DoesNotFit => 2,
        Verdict::Unknown => 3,
    };
    let best = machines.iter().min_by_key(|m| rank(m.verdict));
    let mut out = RunPreflight {
        verdict: Verdict::Unknown,
        message: String::new(),
        machines: machines.clone(),
        changes: Vec::new(),
        alternatives: Vec::new(),
        probe_steps: None,
    };
    let mut config = input.config.clone();
    let Some(estimate) = input.estimate else {
        out.probe_steps = Some(input.probe_steps);
        out.message = format!(
            "There is no memory estimate yet, so the run starts in probe mode: its first {} steps are measured, then the app decides whether it can go on.",
            input.probe_steps
        );
        return (out, config);
    };
    let Some(best) = best else {
        out.probe_steps = Some(input.probe_steps);
        out.message = "No connected machine can run this yet, so the fit can't be checked. The run waits for one and starts in probe mode.".into();
        return (out, config);
    };
    out.verdict = best.verdict;
    match best.verdict {
        Verdict::Fits => {
            out.message = format!(
                "Estimated to fit on {}: about {:.1} GiB needed, {:.1} GiB usable.",
                best.hostname, estimate.expected, best.usable_gib
            );
        }
        Verdict::Tight => {
            let nvidia = candidates
                .iter()
                .any(|m| m.gpus.iter().any(|(v, _)| v == "nvidia"));
            out.changes = safety_changes(&config, input.schema, nvidia);
            apply(&mut config, &out.changes);
            out.message = if out.changes.is_empty() {
                format!(
                    "Estimated to be close to the memory limit on {} (about {:.1} of {:.1} GiB usable). No safety setting is left to apply.",
                    best.hostname, estimate.hi, best.usable_gib
                )
            } else {
                format!(
                    "Estimated to be close to the memory limit on {} (up to {:.1} of {:.1} GiB usable), so the run starts with safety settings: {}.",
                    best.hostname,
                    estimate.hi,
                    best.usable_gib,
                    out.changes
                        .iter()
                        .map(|c| c.path.trim_start_matches('/').replace('/', "."))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
        }
        Verdict::DoesNotFit | Verdict::Unknown => {
            out.message = format!(
                "Estimated to need about {:.1} GiB, more than the {:.1} GiB that {} has, so the run won't start.",
                estimate.expected, best.capacity_gib, best.hostname
            );
            out.alternatives = alternatives(input, &candidates, &others, best, h);
        }
    }
    (out, config)
}

fn alternatives(
    input: &PreflightInput,
    candidates: &[&MachineFacts],
    others: &[&MachineFacts],
    best: &PreflightMachine,
    h: &Headroom,
) -> Vec<PreflightAlternative> {
    let mut out = Vec::new();
    let strategy = input
        .config
        .pointer("/common/update/strategy")
        .and_then(Value::as_str);
    let nvidia = candidates
        .iter()
        .any(|m| m.gpus.iter().any(|(v, _)| v == "nvidia"));
    if input.supports_qlora && nvidia && strategy != Some("qlora") {
        out.push(PreflightAlternative {
            kind: "qlora".into(),
            message:
                "Use QLoRA: it keeps the base model in 4-bit, so it needs much less GPU memory."
                    .into(),
            node_id: None,
        });
    }
    out.push(PreflightAlternative {
        kind: "smaller_model".into(),
        message: format!(
            "Use a smaller model: about {:.1} GiB is usable on {}.",
            best.usable_gib, best.hostname
        ),
        node_id: None,
    });
    let role = main_role(input.resources);
    for m in others {
        if let Some((_, cap, usable)) = capacity(m, role, h) {
            if matches!(
                fit(input.estimate, cap, usable),
                Verdict::Fits | Verdict::Tight
            ) {
                out.push(PreflightAlternative {
                    kind: "machine".into(),
                    message: format!("Use machine {}: estimated to fit there.", m.hostname),
                    node_id: Some(NodeId::from_uuid(m.node_id).to_string()),
                });
            }
        }
    }
    out
}

/// The `engine` block's settings in `schema` (a schema of the whole config, like TRL's, or of
/// the engine block alone).
fn engine_props(schema: &Value) -> Option<&serde_json::Map<String, Value>> {
    let block = match schema.pointer("/properties/engine") {
        Some(e) => crate::modules::submission::resolve::deref(schema, e),
        None => schema,
    };
    block.get("properties")?.as_object()
}

fn prop_default<'a>(props: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a Value> {
    props.get(key)?.get("default")
}

/// The safety settings for a tight fit (Doc 24 §3), as changes to `config`; only settings
/// the method's schema offers, and only ones that make a difference.
pub fn safety_changes(config: &Value, schema: Option<&Value>, nvidia: bool) -> Vec<ConfigChange> {
    let mut out = Vec::new();
    let props = schema.and_then(engine_props);
    let current = |key: &str| -> Value {
        config
            .pointer(&format!("/engine/{key}"))
            .cloned()
            .or_else(|| props.and_then(|p| prop_default(p, key)).cloned())
            .unwrap_or(Value::Null)
    };
    let offered = |key: &str| props.is_some_and(|p| p.contains_key(key));
    if offered("gradient_checkpointing") && current("gradient_checkpointing") != json!(true) {
        out.push(ConfigChange {
            path: "/engine/gradient_checkpointing".into(),
            from: current("gradient_checkpointing"),
            to: json!(true),
            why: "Gradient checkpointing keeps fewer activations in memory; steps take about 30% longer.".into(),
        });
    }
    if offered("per_device_train_batch_size") && offered("gradient_accumulation_steps") {
        let b = current("per_device_train_batch_size").as_u64().unwrap_or(1);
        let acc = current("gradient_accumulation_steps").as_u64().unwrap_or(1);
        if b >= 2 {
            out.push(ConfigChange {
                path: "/engine/per_device_train_batch_size".into(),
                from: json!(b),
                to: json!(b / 2),
                why: "Fewer examples per step need less memory for activations.".into(),
            });
            out.push(ConfigChange {
                path: "/engine/gradient_accumulation_steps".into(),
                from: json!(acc),
                to: json!(acc * 2),
                why: "Twice the accumulation keeps about the same number of examples per update."
                    .into(),
            });
        }
    }
    if let Some(u) = config
        .pointer("/rollout/gpu_memory_utilization")
        .and_then(Value::as_f64)
    {
        let lower = ((u - 0.1).max(0.1) * 100.0).round() / 100.0;
        if lower < u {
            out.push(ConfigChange {
                path: "/rollout/gpu_memory_utilization".into(),
                from: json!(u),
                to: json!(lower),
                why:
                    "A smaller share for the generation engine leaves more GPU memory for training."
                        .into(),
            });
        }
    }
    let eight_bit = props
        .and_then(|p| p.get("optim"))
        .and_then(|o| o.get("enum"))
        .and_then(Value::as_array)
        .is_some_and(|e| e.iter().any(|v| v == "adamw_8bit"));
    if nvidia && eight_bit && current("optim") == json!("adamw_torch") {
        out.push(ConfigChange {
            path: "/engine/optim".into(),
            from: current("optim"),
            to: json!("adamw_8bit"),
            why: "An 8-bit optimizer keeps its state in about a quarter of the memory.".into(),
        });
    }
    out
}

/// Writes `changes` (paths `/<block>/<key>`) into `config`.
pub fn apply(config: &mut Value, changes: &[ConfigChange]) {
    for c in changes {
        let Some((dir, key)) = c.path.trim_start_matches('/').split_once('/') else {
            continue;
        };
        let Some(o) = config.as_object_mut() else {
            continue;
        };
        let block = o.entry(dir).or_insert_with(|| json!({}));
        if !block.is_object() {
            *block = json!({});
        }
        if let Some(b) = block.as_object_mut() {
            b.insert(key.to_owned(), c.to.clone());
        }
    }
}

/// The verdict of `range` on the machine the job's latest attempt ran on (`unknown` when
/// it never ran or there is no estimate).
pub async fn verdict_on_last_machine(
    db: &Db,
    h: &Headroom,
    job: Uuid,
    range: Option<EstimateRange>,
) -> Result<Verdict, JobsError> {
    let row = sqlx::query(
        "SELECT p.node_id, j.resource_request FROM jobs j
           JOIN job_attempts a ON a.job_id = j.id
           JOIN attempt_placements p ON p.attempt_id = a.id
          WHERE j.id = $1 ORDER BY a.attempt_no DESC LIMIT 1",
    )
    .bind(job)
    .fetch_optional(db.pool())
    .await?;
    let Some(row) = row else {
        return Ok(Verdict::Unknown);
    };
    let node: Uuid = row.try_get("node_id")?;
    let request: ResourceRequest = serde_json::from_value(row.try_get("resource_request")?)
        .unwrap_or(ResourceRequest { roles: Vec::new() });
    let all = machines(db, Uuid::nil(), Some(node)).await?;
    Ok(all
        .first()
        .and_then(|m| capacity(m, main_role(&request), h))
        .map_or(Verdict::Unknown, |(_, cap, usable)| fit(range, cap, usable)))
}

/// Probe mode (Doc 24 §3, `unknown`): once a run with no estimate of its config has measured
/// `steps` steps, a GPU peak above that GPU's memory (less its headroom) cancels the run
/// with a plain reason; otherwise it goes on.
pub struct ProbeModeGuard {
    db: Db,
    headroom: Headroom,
    steps: u32,
}

impl ProbeModeGuard {
    /// Reads runs, estimates and peaks from `db`.
    pub fn new(db: Db, headroom: Headroom, steps: u32) -> Self {
        Self {
            db,
            headroom,
            steps,
        }
    }

    /// Decides for `run`'s attempt `attempt`. Returns true when it cancelled the job.
    pub async fn decide(&self, run: Uuid, attempt: Uuid) -> Result<bool, JobsError> {
        let over: Option<(Uuid, Uuid, String, f64, f64)> = sqlx::query_as(
            "SELECT j.id, j.org_id, g.device_uuid, ra.peak, g.vram_total_bytes::float8
               FROM runs r JOIN jobs j ON j.run_id = r.id
               JOIN run_configs rc ON rc.run_id = r.id
               JOIN resource_actuals ra ON ra.attempt_id = $2 AND ra.run_id = r.id
               JOIN attempt_placements p ON p.attempt_id = $2
               JOIN gpu_devices g ON g.node_id = p.node_id AND g.device_uuid = ra.device
              WHERE r.id = $1 AND j.current_attempt_id = $2
                AND ra.signal IN ('process_peak', 'torch_reserved_peak')
                AND NOT EXISTS (SELECT 1 FROM resource_estimates e
                                 WHERE e.org_id = r.org_id AND e.project_id = r.project_id
                                   AND e.inputs_sha256 = rc.resolved_sha256
                                   AND e.state = 'READY' AND e.gpu_mem_hi_gib > 0)
                AND ra.peak > g.vram_total_bytes * (1 - $3)
              ORDER BY ra.peak DESC LIMIT 1",
        )
        .bind(run)
        .bind(attempt)
        .bind(self.headroom.gpu_free_fraction)
        .fetch_optional(self.db.pool())
        .await?;
        let Some((job, org, device, peak, vram)) = over else {
            return Ok(false);
        };
        let mut tx = self.db.begin().await?;
        let Some(row) = JobTransitions::lock(&mut tx, org, job).await? else {
            return Ok(false);
        };
        if !matches!(row.state.as_str(), "RUNNING" | "PREPARING" | "PAUSING") {
            return Ok(false);
        }
        tracing::warn!(
            job = %job, device = %device, peak_gib = peak / GIB, vram_gib = vram / GIB,
            "probe mode: the measured peak leaves too little GPU memory free; cancelling"
        );
        JobTransitions::request_cancel(&mut tx, &row, "system:guardian").await?;
        tx.commit().await?;
        Ok(true)
    }
}

#[async_trait::async_trait]
impl MetricObserver for ProbeModeGuard {
    fn name(&self) -> &'static str {
        "guardian-probe-mode"
    }

    async fn observe(&self, batch: Arc<IngestedBatch>) {
        let measured = batch
            .points
            .iter()
            .filter(|p| p.name.starts_with("resource/"))
            .any(|p| p.step >= i64::from(self.steps));
        if !measured {
            return;
        }
        let (Ok(run), Ok(attempt)) = (
            batch.run_id.parse::<mrl_domain::ids::RunId>(),
            batch.attempt_id.parse::<mrl_domain::ids::AttemptId>(),
        ) else {
            return;
        };
        // ponytail: not durable (P22 observers drop after a restart); the next batch with
        // resource samples decides again.
        if let Err(e) = self.decide(run.as_uuid(), attempt.as_uuid()).await {
            tracing::warn!(error = %e, "probe mode could not decide; the next batch retries");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_domain::resources::GpuConstraints;

    fn role(gpus: u32, vendor: Option<GpuVendor>) -> RoleRequest {
        RoleRequest {
            role: WorkloadRole::Trainer,
            replicas: 1,
            gpu_count: gpus,
            gpu: GpuConstraints {
                vendor,
                ..GpuConstraints::default()
            },
            node_ids: vec![],
            cpu_millis: 1000,
            mem_bytes: 1 << 30,
        }
    }

    fn machine(gpus: &[(&str, u64)], ram_gib: u64) -> MachineFacts {
        MachineFacts {
            node_id: Uuid::now_v7(),
            hostname: "box".into(),
            gpus: gpus
                .iter()
                .map(|(v, g)| ((*v).to_owned(), (*g << 30) as i64))
                .collect(),
            ram_bytes: Some(ram_gib << 30),
            unified_bytes: None,
        }
    }

    #[test]
    fn headroom_rules() {
        let h = Headroom::default();
        assert!(
            (h.usable_gpu(24.0) - 21.6).abs() < 1e-9,
            "10 % of a GPU stays free"
        );
        assert!(
            (h.usable_ram(16.0) - 12.0).abs() < 1e-9,
            "4 GB beats 15 % of 16 GB"
        );
        assert!(
            (h.usable_ram(64.0) - 54.4).abs() < 1e-9,
            "15 % beats 4 GB of 64 GB"
        );
        assert_eq!(h.usable_ram(3.0), 0.0);
        assert!(
            (h.usable_unified(32.0) - 24.0).abs() < 1e-9,
            "25 % on a Mac"
        );
        assert!(h.problems().is_empty());
        let bad = Headroom {
            gpu_free_fraction: 1.5,
            ..h
        };
        assert_eq!(bad.problems().len(), 1);
    }

    #[test]
    fn machine_memory_by_kind() {
        let h = Headroom::default();
        let gpu = machine(&[("nvidia", 24), ("nvidia", 12)], 64);
        let r1 = role(1, None);
        assert_eq!(
            capacity(&gpu, Some(&r1), &h),
            Some((MemoryKind::Gpu, 24.0, 21.6)),
            "the biggest GPU"
        );
        let r2 = role(2, Some(GpuVendor::Nvidia));
        assert_eq!(capacity(&gpu, Some(&r2), &h).map(|c| c.1), Some(12.0));
        assert_eq!(capacity(&gpu, Some(&role(3, None)), &h), None);
        assert_eq!(
            capacity(&gpu, Some(&role(1, Some(GpuVendor::Amd))), &h),
            None
        );
        let cpu = capacity(&gpu, Some(&role(0, None)), &h).unwrap();
        assert_eq!(cpu.0, MemoryKind::System);
        let mut mac = machine(&[("apple", 32)], 32);
        mac.unified_bytes = Some(32 << 30);
        assert_eq!(
            capacity(&mac, Some(&r1), &h),
            Some((MemoryKind::Unified, 32.0, 24.0))
        );
    }

    #[test]
    fn verdict_rules() {
        let r = EstimateRange::analytic(18.0, 22.0);
        assert_eq!(
            fit(Some(r), 24.0, 21.6),
            Verdict::Tight,
            "high end over usable"
        );
        assert_eq!(fit(Some(r), 40.0, 36.0), Verdict::Fits);
        assert_eq!(fit(Some(r), 16.0, 14.4), Verdict::DoesNotFit);
        assert_eq!(fit(None, 16.0, 14.4), Verdict::Unknown);
    }

    fn schema() -> Value {
        json!({"properties": {"common": {}, "engine": {"$ref": "#/$defs/E"}}, "$defs": {"E": {
        "properties": {
            "gradient_checkpointing": {"default": false},
            "per_device_train_batch_size": {"default": 4},
            "gradient_accumulation_steps": {"default": 1},
            "optim": {"default": "adamw_torch", "enum": ["adamw_torch", "adamw_8bit"]}
        }}}})
    }

    #[test]
    fn tight_applies_safety_settings_with_reasons() {
        let cfg = json!({"engine": {"per_device_train_batch_size": 8}, "rollout": {"gpu_memory_utilization": 0.3}});
        let s = schema();
        let changes = safety_changes(&cfg, Some(&s), true);
        let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "/engine/gradient_checkpointing",
                "/engine/per_device_train_batch_size",
                "/engine/gradient_accumulation_steps",
                "/rollout/gpu_memory_utilization",
                "/engine/optim"
            ]
        );
        assert!(changes.iter().all(|c| !c.why.is_empty()));
        let mut out = cfg.clone();
        apply(&mut out, &changes);
        assert_eq!(out["engine"]["per_device_train_batch_size"], 4);
        assert_eq!(out["engine"]["gradient_accumulation_steps"], 2);
        assert_eq!(out["engine"]["gradient_checkpointing"], true);
        assert_eq!(out["engine"]["optim"], "adamw_8bit");
        assert_eq!(out["rollout"]["gpu_memory_utilization"], 0.2);
        // Not NVIDIA: no 8-bit optimizer. Already safe: nothing to change.
        assert!(safety_changes(&cfg, Some(&s), false)
            .iter()
            .all(|c| c.path != "/engine/optim"));
        let safe = json!({"engine": {"gradient_checkpointing": true, "per_device_train_batch_size": 1, "optim": "adamw_8bit"}});
        assert!(safety_changes(&safe, Some(&s), true).is_empty());
        assert!(
            safety_changes(&cfg, None, true).len() == 1,
            "only the rollout share without a schema"
        );
    }

    #[test]
    fn decide_picks_the_best_machine_and_explains() {
        let h = Headroom::default();
        let request = ResourceRequest {
            roles: vec![role(1, Some(GpuVendor::Nvidia))],
        };
        let cfg = json!({"common": {"update": {"strategy": "lora"}}, "engine": {}});
        let s = schema();
        let input = |lo, hi| PreflightInput {
            resources: &request,
            estimate: Some(EstimateRange::analytic(lo, hi)),
            config: &cfg,
            schema: Some(&s),
            supports_qlora: true,
            probe_steps: 5,
        };
        let big = machine(&[("nvidia", 24)], 64);
        let small = machine(&[("nvidia", 4)], 16);
        let (p, out) = decide(&input(8.0, 12.0), &[small.clone(), big.clone()], &h);
        assert_eq!(p.verdict, Verdict::Fits);
        assert_eq!(p.machines.len(), 2);
        assert_eq!(out, cfg);
        let (p, _) = decide(&input(8.0, 12.0), std::slice::from_ref(&small), &h);
        assert_eq!(p.verdict, Verdict::DoesNotFit);
        let kinds: Vec<&str> = p.alternatives.iter().map(|a| a.kind.as_str()).collect();
        assert_eq!(kinds, ["qlora", "smaller_model"]);
        let (p, out) = decide(&input(19.0, 23.0), std::slice::from_ref(&big), &h);
        assert_eq!(p.verdict, Verdict::Tight);
        assert!(!p.changes.is_empty() && out["engine"]["gradient_checkpointing"] == true);
        assert!(p.message.contains("safety settings"), "{}", p.message);
        let none = PreflightInput {
            estimate: None,
            ..input(1.0, 2.0)
        };
        let (p, _) = decide(&none, std::slice::from_ref(&big), &h);
        assert_eq!((p.verdict, p.probe_steps), (Verdict::Unknown, Some(5)));
        let (p, _) = decide(&input(1.0, 2.0), &[], &h);
        assert_eq!(p.verdict, Verdict::Unknown);
        // Pinned to the small machine: the big one is offered instead.
        let pinned = ResourceRequest {
            roles: vec![RoleRequest {
                node_ids: vec![NodeId::from_uuid(small.node_id)],
                ..role(1, None)
            }],
        };
        let pin = PreflightInput {
            resources: &pinned,
            ..input(8.0, 12.0)
        };
        let (p, _) = decide(&pin, &[small, big.clone()], &h);
        assert_eq!(p.verdict, Verdict::DoesNotFit);
        assert_eq!(
            p.alternatives.last().and_then(|a| a.node_id.clone()),
            Some(NodeId::from_uuid(big.node_id).to_string())
        );
    }
}
