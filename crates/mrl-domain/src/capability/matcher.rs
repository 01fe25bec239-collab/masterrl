//! The L0 capability matcher: engine × method × model × dataset × hardware × settings.
//!
//! [`check`] is a pure function of one engine version's manifest and a [`Request`]. It runs
//! the checks of Doc 05 §18 in order and collects every problem: method present → dataset
//! schema per role → required inputs → model family → update strategy, precision and
//! parallelism → hardware (engine environments, connected machines, probe results) → the
//! method's config schema and pinned fields → the manifest's `constraints[]`. Maturity policy
//! and resume compatibility are not checked here (no workspace policy or checkpoint input yet).
//!
//! The submitted config has two blocks: `common` (platform settings) and `engine` (the
//! engine's own block, checked against the method's config schema). From `common` the matcher
//! reads `update_strategy` (default `full`), `precision`, `parallelism` (default `single`),
//! `teacher_model_version_id` and `engine` (an engine name or `name@version`, used by the
//! caller to pick candidates).

use super::codes::CapabilityRejection as Cap;
use super::manifest::{EngineManifest, Environment, HardwareClass};
use crate::errors::ErrorCode;
use crate::resources::{Platform, ResourceRequest, RoleRequest, WorkloadRole};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Codes this part adds (guidance/fragments/errors/p24.yaml).
pub mod codes {
    use crate::errors::{ErrorCode, ErrorFamily};

    /// The engine can't train with this update strategy, precision or parallelism.
    pub const SETTING_UNSUPPORTED: ErrorCode = ErrorCode::known(ErrorFamily::Cap, 240);
    /// The engine version has not passed a probe on this hardware class yet.
    pub const NOT_PROBED: ErrorCode = ErrorCode::known(ErrorFamily::Cap, 241);
    /// The method needs an input (reward, teacher) the run doesn't have.
    pub const INPUT_MISSING: ErrorCode = ErrorCode::known(ErrorFamily::Cap, 242);
    /// The dataset's schema is not one the method accepts.
    pub const DATASET_SCHEMA: ErrorCode = ErrorCode::known(ErrorFamily::Cap, 243);
    /// The model family is not one the method trains.
    pub const MODEL_FAMILY: ErrorCode = ErrorCode::known(ErrorFamily::Cap, 244);
    /// No connected machine has the requested hardware class.
    pub const NO_MACHINE: ErrorCode = ErrorCode::known(ErrorFamily::Cap, 245);
    /// No model version with this id is registered for the org.
    pub const MODEL_UNKNOWN: ErrorCode = ErrorCode::known(ErrorFamily::Model, 240);
    /// No dataset version with this id is registered for the org.
    pub const DATASET_UNKNOWN: ErrorCode = ErrorCode::known(ErrorFamily::Data, 240);
    /// A config value is not accepted (reused from the base catalog).
    pub const FIELD_VALUE: ErrorCode = ErrorCode::known(ErrorFamily::Val, 3);
}

/// What the model registry knows about a model version (Doc 05 §12.4, reduced `ModelInfo`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelFacts {
    /// `decoder`, `decoder-moe`, `encoder`, `encoder-decoder` or `vlm`.
    pub family: String,
    /// Mixture of experts.
    #[serde(default)]
    pub is_moe: bool,
    /// Parameter count.
    #[serde(default)]
    pub params: Option<f64>,
    /// Ships a chat template.
    #[serde(default)]
    pub has_chat_template: bool,
    /// Needs custom code to load.
    #[serde(default)]
    pub requires_remote_code: bool,
    /// The workspace approved the custom code at this exact model revision.
    #[serde(default)]
    pub remote_code_approved: bool,
}

/// What the dataset registry knows about a dataset version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetFacts {
    /// Canonical schema id, like `sft.messages.v1`.
    pub schema: String,
    /// Row count, if known.
    #[serde(default)]
    pub rows: Option<u64>,
    /// Read as a stream (no known length).
    #[serde(default)]
    pub streaming: bool,
}

/// A record the request names: not named at all, named but unknown, or found.
#[derive(Debug, Clone, PartialEq)]
pub enum Lookup<T> {
    /// Not part of the request (capability listing without it).
    NotGiven,
    /// Named, but no such record exists for the caller.
    Missing,
    /// Found.
    Found(T),
}

/// Which hardware the run asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wanted {
    /// One class.
    Class(HardwareClass),
    /// Any GPU maker.
    AnyGpu,
    /// Anything (capability listing).
    Any,
}

/// The hardware part of a request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HardwareRequest {
    /// Class.
    pub wanted: Wanted,
    /// GPUs per machine.
    pub gpus: u32,
    /// Machines.
    pub nodes: u32,
    /// Smallest GPU memory, in GiB.
    pub gpu_mem_gib: Option<f64>,
}

/// The eligible hardware on one connected node.
#[derive(Debug, Clone)]
pub struct Machine {
    /// Node platform from inventory.
    pub platform: Option<Platform>,
    /// CPU architecture from inventory.
    pub arch: String,
    /// Present, healthy devices.
    pub devices: Vec<Device>,
}

/// A present, healthy accelerator.
#[derive(Debug, Clone)]
pub struct Device {
    /// Device class.
    pub class: HardwareClass,
    /// Available physical memory in bytes.
    pub memory: u64,
    /// Device architecture capability, such as 8.0.
    pub compute_capability: Option<String>,
    /// Driver version reported in inventory.
    pub driver: Option<String>,
}

#[derive(Clone, Default)]
struct Fit {
    class: Option<HardwareClass>,
    accelerator: Option<String>,
    compute_capability: Option<String>,
    gpu_mem_gib: Option<f64>,
    gpus: u32,
}

fn version(s: &str) -> Option<Vec<u32>> {
    let mut parts = s
        .split('.')
        .map(str::parse)
        .collect::<Result<Vec<u32>, _>>()
        .ok()?;
    while parts.last() == Some(&0) {
        parts.pop();
    }
    Some(parts)
}

impl Machine {
    /// Hardware classes on this machine.
    pub fn classes(&self) -> impl Iterator<Item = HardwareClass> + '_ {
        std::iter::once(HardwareClass::Cpu).chain(self.devices.iter().map(|d| d.class))
    }

    /// Whether this machine can host an engine runtime of this class.
    pub fn supports(&self, manifest: &EngineManifest, class: HardwareClass) -> bool {
        self.fits(manifest, class, None, None, None).is_some()
    }

    fn fits(
        &self,
        manifest: &EngineManifest,
        class: HardwareClass,
        gpus: Option<u32>,
        role: Option<&RoleRequest>,
        environment: Option<&str>,
    ) -> Option<Fit> {
        if role
            .and_then(|r| r.gpu.platform)
            .is_some_and(|p| Some(p) != self.platform)
        {
            return None;
        }
        let hw = manifest.spec.hardware.as_ref();
        let (os, target_os) = match self.platform? {
            Platform::Linux => ("linux", "linux"),
            Platform::Wsl2 => ("windows", "linux"),
            Platform::Windows => ("windows", "windows"),
            Platform::AppleSilicon => ("macos", "darwin"),
        };
        if hw.is_some_and(|h| {
            h.os.as_ref()
                .is_some_and(|allowed| !allowed.iter().any(|o| o == os))
                || (self.platform == Some(Platform::Wsl2) && h.wsl2 == Some(false))
                || (self.platform == Some(Platform::AppleSilicon) && h.apple_silicon == Some(false))
        }) {
            return None;
        }
        let arch = match self.arch.as_str() {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            other => other,
        };
        let platform = format!("{target_os}/{arch}");
        manifest
            .spec
            .runtime
            .environments
            .iter()
            .filter(|e| {
                e.platform == platform
                    && environment.is_none_or(|id| id == e.id)
                    && e.accelerator
                        .as_ref()
                        .map_or(HardwareClass::Cpu, |a| a.vendor)
                        == class
            })
            .find_map(|e| {
                self.fit_environment(
                    e,
                    class,
                    gpus,
                    role,
                    hw.and_then(|h| h.min_gpus)
                        .unwrap_or(1)
                        .max(1)
                        .max(role.map_or(0, |r| r.gpu_count)),
                )
            })
    }

    fn fit_environment(
        &self,
        env: &Environment,
        class: HardwareClass,
        gpus: Option<u32>,
        role: Option<&RoleRequest>,
        minimum: u32,
    ) -> Option<Fit> {
        if class == HardwareClass::Cpu {
            return Some(Fit {
                class: Some(class),
                accelerator: Some(class.as_str().into()),
                ..Fit::default()
            });
        }
        let accelerator = env.accelerator.as_ref()?;
        let gpus = gpus.unwrap_or(minimum);
        let enough = |observed: Option<&str>, required: Option<&str>| {
            required.is_none_or(|needed| {
                version(needed).is_some_and(|needed| {
                    observed
                        .and_then(version)
                        .is_some_and(|observed| observed >= needed)
                })
            })
        };
        let mut devices: Vec<_> = self
            .devices
            .iter()
            .filter(|d| {
                d.class == class
                    && d.memory >= role.and_then(|r| r.gpu.min_vram_bytes).unwrap_or(0)
                    && enough(d.driver.as_deref(), accelerator.min_driver.as_deref())
                    && enough(
                        d.compute_capability.as_deref(),
                        accelerator.min_compute_capability.as_deref(),
                    )
            })
            .collect();
        devices.sort_by_key(|d| {
            std::cmp::Reverse((
                version(d.compute_capability.as_deref().unwrap_or_default()),
                d.memory,
            ))
        });
        let devices = devices.get(..usize::try_from(gpus).ok()?)?;
        Some(Fit {
            class: Some(class),
            accelerator: Some(accelerator.runtime.as_ref().map_or_else(
                || class.as_str().to_owned(),
                |runtime| format!("{}-{runtime}", class.as_str()),
            )),
            compute_capability: devices.last().and_then(|d| d.compute_capability.clone()),
            gpu_mem_gib: devices
                .iter()
                .map(|d| d.memory)
                .min()
                .map(|v| v as f64 / (1u64 << 30) as f64),
            gpus,
        })
    }
}

/// One question for the matcher.
#[derive(Debug, Clone)]
pub struct Request {
    /// Method id.
    pub algorithm: String,
    /// The resolved config (`{common, engine}`); `None` checks no settings (capability listing).
    pub config: Option<Value>,
    /// The base model.
    pub model: Lookup<ModelFacts>,
    /// Datasets in submission order: the first trains, the rest evaluate.
    pub datasets: Vec<Lookup<DatasetFacts>>,
    /// A reward version is attached.
    pub reward_attached: bool,
    /// Requested hardware.
    pub hardware: HardwareRequest,
    /// Hardware classes of the machines the run could use now.
    pub available: BTreeSet<HardwareClass>,
    /// Live machine inventory, when available.
    pub machines: Option<Vec<Machine>>,
    /// Full role requirements for submissions.
    pub resources: Option<ResourceRequest>,
    /// Explicit reference, critic and teacher lookups (omitted reference and critic use the base).
    pub model_signals: BTreeMap<String, Lookup<ModelFacts>>,
    /// Rollout engine settings were supplied.
    pub rollout: bool,
}

/// One engine version as the matcher sees it.
#[derive(Debug, Clone, Copy)]
pub struct Engine<'a> {
    /// Its manifest.
    pub manifest: &'a EngineManifest,
    /// Its inlined config schemas, by path.
    pub schemas: &'a BTreeMap<String, Value>,
    /// Hardware classes where a probe passed.
    pub enabled: &'a BTreeSet<HardwareClass>,
}

/// One problem found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Problem {
    /// Catalog code.
    pub code: ErrorCode,
    /// JSON Pointer into the submission, like `/config/common/update_strategy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// What is wrong, in plain words.
    pub message: String,
    /// What to do, when the manifest gave one (`constraints[].error.hint`); otherwise the
    /// catalog's steps for `code` apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl Problem {
    fn new(code: ErrorCode, path: Option<&str>, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.map(str::to_owned),
            message: message.into(),
            hint: None,
        }
    }
}

/// The settings the matcher reads from `config.common`.
struct Settings<'a> {
    update: &'a str,
    precision: Option<&'a str>,
    parallelism: &'a str,
}

fn text<'a>(common: Option<&'a Map<String, Value>>, key: &str) -> Result<Option<&'a str>, ()> {
    match common.and_then(|c| c.get(key)) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(()),
    }
}

/// Whether the listing can offer QLoRA on its connected machines.
pub fn offers_qlora(engine: Engine<'_>, req: &Request) -> bool {
    engine
        .manifest
        .spec
        .capabilities
        .quantized_training
        .iter()
        .any(|q| q == "bnb-4bit")
        && !hardware(
            engine,
            req,
            &Settings {
                update: "qlora",
                precision: None,
                parallelism: "single",
            },
            &engine.manifest.hardware_classes(),
            &mut Vec::new(),
        )
        .is_empty()
}

/// What the matcher decides for `engine` and `req`: no problems means it can run.
pub fn check(engine: Engine<'_>, req: &Request) -> Vec<Problem> {
    let m = engine.manifest;
    let name = &m.metadata.display_name;
    let Some(algo) = m.algorithm(&req.algorithm) else {
        // Doc 05 §18 row 1 has its own code.
        let code = if req.algorithm == "ppo" && m.metadata.name == "trl" {
            Cap::PpoUnavailableOnTrl.code()
        } else {
            Cap::NoEngineForMethod.code()
        };
        return vec![Problem::new(
            code,
            Some("/algorithm_id"),
            format!("{name} has no method `{}`.", req.algorithm),
        )];
    };
    let mut out = Vec::new();

    // Datasets, per role.
    for (i, d) in req.datasets.iter().enumerate() {
        let path = format!("/dataset_version_ids/{i}");
        match d {
            Lookup::NotGiven => {}
            Lookup::Missing => out.push(Problem::new(
                codes::DATASET_UNKNOWN,
                Some(&path),
                "The app has no record of this dataset version.",
            )),
            Lookup::Found(f) => {
                let schemas = &algo.requires.dataset_schemas;
                let (role, allowed) = if i == 0 || schemas.eval.is_empty() {
                    (
                        if i == 0 { "training" } else { "evaluation" },
                        &schemas.train,
                    )
                } else {
                    ("evaluation", &schemas.eval)
                };
                if !allowed.contains(&f.schema) {
                    // Doc 05 §18 row 2 has its own code.
                    let code = if algo.id == "dpo" && f.schema == "sft.messages.v1" {
                        Cap::DpoNeedsPreferencePairs.code()
                    } else {
                        codes::DATASET_SCHEMA
                    };
                    out.push(Problem::new(
                        code,
                        Some(&path),
                        format!(
                            "{} needs {role} data in {}, but this dataset is {}.",
                            algo.display_name,
                            allowed.join(" or "),
                            f.schema
                        ),
                    ));
                }
            }
        }
    }

    // Inputs the method needs.
    let common = req
        .config
        .as_ref()
        .and_then(|c| c.get("common"))
        .and_then(Value::as_object);
    if algo.requires.signals.reward == "required" && !req.reward_attached {
        // Doc 05 §18 row 3 has its own code.
        let code = if algo.id == "grpo" {
            Cap::GrpoNeedsReward.code()
        } else {
            codes::INPUT_MISSING
        };
        out.push(Problem::new(
            code,
            Some("/reward_version_id"),
            format!(
                "{} learns from a reward, but this run has none.",
                algo.display_name
            ),
        ));
    }
    if req.config.is_some() {
        let signals = &algo.requires.signals;
        for (signal, required, path, inherited) in [
            (
                "reference",
                &signals.reference,
                "/config/inputs/reference_model_version_id",
                true,
            ),
            (
                "critic",
                &signals.critic,
                "/config/inputs/critic_model_version_id",
                true,
            ),
            (
                "teacher",
                &signals.teacher,
                "/config/common/teacher_model_version_id",
                false,
            ),
        ] {
            match req.model_signals.get(signal) {
                Some(Lookup::Missing) => out.push(Problem::new(
                    codes::MODEL_UNKNOWN,
                    Some(path),
                    "The app has no record of this model version.",
                )),
                Some(Lookup::Found(f)) => check_model_policy(f, m, path, &mut out),
                _ if required == "required" && !inherited => out.push(Problem::new(
                    codes::INPUT_MISSING,
                    Some(path),
                    format!("{} needs a {signal} model.", algo.display_name),
                )),
                _ => {}
            }
        }
        if (signals.rollout_engine == "required" || req.rollout)
            && (!req.rollout
                || !req.resources.as_ref().is_some_and(|resources| {
                    resources
                        .roles
                        .iter()
                        .any(|r| r.role == WorkloadRole::Rollout)
                }))
        {
            out.push(Problem::new(
                codes::INPUT_MISSING,
                Some("/config/rollout"),
                "Give this run rollout settings and resources for its rollout role.",
            ));
        }
        if req
            .datasets
            .iter()
            .any(|d| matches!(d, Lookup::Found(f) if f.streaming))
            && !req
                .config
                .as_ref()
                .and_then(|c| {
                    c.pointer("/common/max_steps")
                        .or_else(|| c.pointer("/engine/max_steps"))
                })
                .and_then(Value::as_u64)
                .is_some_and(|n| n > 0)
        {
            out.push(Problem::new(
                Cap::StreamingNeedsStepLimit.code(),
                Some("/config/common/max_steps"),
                "Set a positive step limit for a streaming dataset.",
            ));
        }
    }

    // The model.
    match &req.model {
        Lookup::NotGiven => {}
        Lookup::Missing => out.push(Problem::new(
            codes::MODEL_UNKNOWN,
            Some("/base_model_version_id"),
            "The app has no record of this model version.",
        )),
        Lookup::Found(f) => {
            check_model_policy(f, m, "/base_model_version_id", &mut out);
            let families = &algo.requires.model_families;
            if !families.is_empty() && !families.contains(&f.family) {
                let code = if f.family == "encoder" {
                    Cap::EncoderOnlyModel.code()
                } else {
                    codes::MODEL_FAMILY
                };
                out.push(Problem::new(
                    code,
                    Some("/base_model_version_id"),
                    format!(
                        "{} trains {} models; this model is {}.",
                        algo.display_name,
                        families.join(" or "),
                        f.family
                    ),
                ));
            }
        }
    }

    // Update strategy, precision, parallelism.
    let caps = &m.spec.capabilities;
    let mut settings = Settings {
        update: "full",
        precision: None,
        parallelism: "single",
    };
    if req.config.is_some() {
        for (key, slot) in [("update_strategy", 0), ("precision", 1), ("parallelism", 2)] {
            match text(common, key) {
                Err(()) => out.push(Problem::new(
                    codes::FIELD_VALUE,
                    Some(&format!("/config/common/{key}")),
                    format!("`{key}` must be text, like the names in the method's settings."),
                )),
                Ok(None) => {}
                Ok(Some(v)) => match slot {
                    0 => settings.update = v,
                    1 => settings.precision = Some(v),
                    _ => settings.parallelism = v,
                },
            }
        }
        let updates = algo
            .supports
            .update_strategies
            .as_ref()
            .unwrap_or(&caps.update_strategies);
        if !updates.iter().any(|u| u == settings.update) {
            out.push(Problem::new(
                codes::SETTING_UNSUPPORTED,
                Some("/config/common/update_strategy"),
                format!(
                    "{} with {name} can't use the `{}` update strategy. It can use: {}.",
                    algo.display_name,
                    settings.update,
                    updates.join(", ")
                ),
            ));
        } else if settings.update == "qlora"
            && !caps.quantized_training.iter().any(|q| q == "bnb-4bit")
        {
            out.push(Problem::new(
                codes::SETTING_UNSUPPORTED,
                Some("/config/common/update_strategy"),
                format!("{name} lists QLoRA but not 4-bit quantized training (bnb-4bit), which QLoRA needs."),
            ));
        }
        if let Some(p) = settings.precision {
            if !caps.precision.iter().any(|x| x == p) {
                out.push(Problem::new(
                    codes::SETTING_UNSUPPORTED,
                    Some("/config/common/precision"),
                    format!(
                        "{name} can't train in `{p}`. It can use: {}.",
                        caps.precision.join(", ")
                    ),
                ));
            }
        }
        let parallel = algo
            .supports
            .parallelism
            .as_ref()
            .unwrap_or(&caps.parallelism);
        if !parallel.iter().any(|x| x == settings.parallelism) {
            out.push(Problem::new(
                codes::SETTING_UNSUPPORTED,
                Some("/config/common/parallelism"),
                format!(
                    "{} with {name} can't use `{}` parallelism. It can use: {}.",
                    algo.display_name,
                    settings.parallelism,
                    parallel.join(", ")
                ),
            ));
        }
    }
    if req.hardware.nodes > 1 {
        let parallel = algo
            .supports
            .parallelism
            .as_ref()
            .unwrap_or(&caps.parallelism);
        if !caps.multi_node
            || settings.parallelism == "single"
            || parallel.iter().all(|p| p == "single")
        {
            out.push(Problem::new(
                Cap::SingleMachineOnly.code(),
                Some("/resources"),
                format!(
                    "{} with {name} runs on one machine; this run asks for {}.",
                    algo.display_name, req.hardware.nodes
                ),
            ));
        }
    }

    // Hardware.
    let runnable = m.hardware_classes();
    let hardware = hardware(engine, req, &settings, &runnable, &mut out);
    check_topology(m, req, &mut out);

    // Config schema and pinned fields.
    if let Some(config) = &req.config {
        let block = config.get("engine").cloned().unwrap_or_else(|| json!({}));
        if let Some(form) = &algo.config {
            if let Some(schema) = engine.schemas.get(&form.schema) {
                match jsonschema::draft202012::new(schema) {
                    Ok(v) => out.extend(v.iter_errors(&block).map(|e| {
                        Problem::new(
                            codes::FIELD_VALUE,
                            Some(&format!("/config/engine{}", e.instance_path().as_str())),
                            format!(
                                "This setting doesn't fit {}: {}.",
                                algo.display_name,
                                e.masked()
                            ),
                        )
                    })),
                    // Checked when the bundle was read; a schema that still fails can't
                    // vouch for any config.
                    Err(_) => out.push(Problem::new(
                        codes::FIELD_VALUE,
                        Some("/config/engine"),
                        format!(
                            "{name}'s settings form for {} can't be used.",
                            algo.display_name
                        ),
                    )),
                }
            }
        }
        for (key, pinned) in &algo.switch {
            if block.get(key).is_some_and(|v| v != pinned) {
                out.push(Problem::new(
                    Cap::FieldLocked.code(),
                    Some(&format!("/config/engine/{}", escape(key))),
                    format!(
                        "`{key}` is fixed to {pinned} for {}; it is what makes the method what it is.",
                        algo.display_name
                    ),
                ));
            }
        }

        // constraints[] over the validation document.
        let constraints: Vec<_> = m
            .spec
            .constraints
            .iter()
            .filter(|c| c.applies_to.iter().any(|a| a == &req.algorithm))
            .map(|c| (c, jsonschema::draft202012::new(&c.schema)))
            .collect();
        let mut best = None;
        for fit in hardware.into_iter().chain(std::iter::once(Fit::default())) {
            let doc = document(req, config, &fit);
            let mut problems = Vec::new();
            for (c, validator) in &constraints {
                let ok = validator.as_ref().is_ok_and(|v| v.is_valid(&doc));
                if !ok {
                    problems.push(Problem {
                        code: c.error.code,
                        path: None,
                        message: c.error.message.clone(),
                        hint: Some(c.error.hint.clone()),
                    });
                }
            }
            if best.as_ref().is_none_or(|previous: &Vec<Problem>| {
                fit.class.is_some() && problems.len() < previous.len()
            }) {
                if problems.is_empty() {
                    best = Some(problems);
                    break;
                }
                best = Some(problems);
            }
        }
        out.extend(best.unwrap_or_default());
    }
    out
}

/// Checks the hardware request; returns the class the run would use, if one fits.
fn hardware(
    engine: Engine<'_>,
    req: &Request,
    settings: &Settings<'_>,
    runnable: &BTreeSet<HardwareClass>,
    out: &mut Vec<Problem>,
) -> Vec<Fit> {
    let m = engine.manifest;
    let name = &m.metadata.display_name;
    let hw = &req.hardware;
    let path = Some("/resources");
    let classes: Vec<HardwareClass> = match hw.wanted {
        Wanted::Class(c) => vec![c],
        Wanted::AnyGpu => vec![
            HardwareClass::Nvidia,
            HardwareClass::Amd,
            HardwareClass::Apple,
        ],
        Wanted::Any => HardwareClass::ALL.to_vec(),
    };
    let needs_cuda = settings.update == "qlora";
    let fits: Vec<HardwareClass> = classes
        .iter()
        .copied()
        .filter(|c| runnable.contains(c) && (!needs_cuda || *c == HardwareClass::Nvidia))
        .collect();
    let wanted_words = match hw.wanted {
        Wanted::Class(c) => c.describe().to_owned(),
        Wanted::AnyGpu => "a GPU".to_owned(),
        Wanted::Any => "any machine".to_owned(),
    };
    if fits.is_empty() {
        let message = if needs_cuda && classes.iter().any(|c| runnable.contains(c)) {
            format!("QLoRA trains with 4-bit bitsandbytes, which needs an NVIDIA GPU; this run asks for {wanted_words}.")
        } else {
            format!(
                "{name} has no setup for {wanted_words}. It runs on: {}.",
                runnable
                    .iter()
                    .map(|c| c.describe())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        out.push(Problem::new(Cap::NoEngineForHardware.code(), path, message));
        return Vec::new();
    }
    if let Some(h) = &m.spec.hardware {
        if hw.wanted != Wanted::Any && fits.iter().all(|c| *c != HardwareClass::Cpu) {
            if let Some(min) = h.min_gpus.filter(|min| hw.gpus < *min) {
                out.push(Problem::new(
                    Cap::NoEngineForHardware.code(),
                    path,
                    format!(
                        "{name} needs at least {min} GPU(s) per machine; this run asks for {}.",
                        hw.gpus
                    ),
                ));
            }
            if let Some(max) = h.max_gpus.filter(|max| hw.gpus > *max) {
                out.push(Problem::new(
                    Cap::NoEngineForHardware.code(),
                    path,
                    format!(
                        "{name} uses at most {max} GPU(s) per machine; this run asks for {}.",
                        hw.gpus
                    ),
                ));
            }
        }
    }
    let connected: Vec<HardwareClass> = fits
        .iter()
        .copied()
        .filter(|c| req.available.contains(c))
        .collect();
    if connected.is_empty() {
        out.push(Problem::new(
            codes::NO_MACHINE,
            path,
            format!("No connected machine has {wanted_words} for {name} to use."),
        ));
        return Vec::new();
    }
    let trainer = req
        .resources
        .as_ref()
        .and_then(|r| r.roles.iter().find(|r| r.role == WorkloadRole::Trainer));
    let mut physical = Vec::new();
    for class in &connected {
        let matches = req.machines.as_ref().map_or_else(
            || {
                vec![Fit {
                    class: Some(*class),
                    accelerator: Some(class.as_str().into()),
                    gpus: hw.gpus,
                    gpu_mem_gib: hw.gpu_mem_gib,
                    ..Fit::default()
                }]
            },
            |machines| {
                machines
                    .iter()
                    .filter_map(|machine| {
                        machine.fits(
                            m,
                            *class,
                            (hw.wanted != Wanted::Any).then_some(hw.gpus),
                            trainer,
                            None,
                        )
                    })
                    .collect()
            },
        );
        if matches.len() >= usize::try_from(hw.nodes).unwrap_or(usize::MAX)
            || req.machines.is_none()
        {
            physical.extend(matches);
        }
    }
    if physical.is_empty() {
        out.push(Problem::new(Cap::NoEngineForHardware.code(), path,
            format!("No connected machine setup meets {name}'s runtime and this run's hardware requirements.")));
        return Vec::new();
    }
    physical.retain(|f| f.class.is_some_and(|c| engine.enabled.contains(&c)));
    if physical.is_empty() {
        out.push(Problem::new(
            codes::NOT_PROBED,
            path,
            format!(
                "{name} hasn't passed its check on {} yet.",
                connected
                    .iter()
                    .map(|c| c.describe())
                    .collect::<Vec<_>>()
                    .join(" or ")
            ),
        ));
    }
    physical
}

fn check_model_policy(
    f: &ModelFacts,
    manifest: &EngineManifest,
    path: &str,
    out: &mut Vec<Problem>,
) {
    if f.requires_remote_code
        && (manifest.spec.capabilities.trust_remote_code == "never" || !f.remote_code_approved)
    {
        out.push(Problem::new(
            Cap::ModelBlockedByPolicy.code(),
            Some(path),
            format!(
                "{} cannot load a model that requires custom code.",
                manifest.metadata.display_name
            ),
        ));
    }
}

fn workload(kind: &str) -> WorkloadRole {
    match kind {
        "trainer" => WorkloadRole::Trainer,
        "rollout_server" => WorkloadRole::Rollout,
        "reward_exec" | "scorer" => WorkloadRole::Reward,
        "ray_head" => WorkloadRole::RayHead,
        "ray_worker" => WorkloadRole::RayWorker,
        _ => WorkloadRole::Service,
    }
}

fn check_topology(m: &EngineManifest, req: &Request, out: &mut Vec<Problem>) {
    let Some(resources) = &req.resources else {
        return;
    };
    let roles = &m.spec.topology.roles;
    let problem = |out: &mut Vec<Problem>, message| {
        out.push(Problem::new(
            Cap::NoEngineForHardware.code(),
            Some("/resources"),
            message,
        ));
    };
    for role in &resources.roles {
        if !roles.iter().any(|r| workload(&r.kind) == role.role) {
            problem(
                out,
                format!("{} has no {} role.", m.metadata.display_name, role.role),
            );
        }
    }
    for declared in roles {
        let kind = workload(&declared.kind);
        let role = resources.roles.iter().find(|r| r.role == kind);
        let colocated = declared
            .colocated_with
            .as_ref()
            .and_then(|id| roles.iter().find(|r| &r.id == id));
        let host = role.or_else(|| {
            colocated.and_then(|r| resources.roles.iter().find(|v| v.role == workload(&r.kind)))
        });
        let Some(role) = host else {
            problem(
                out,
                format!(
                    "{} needs resources for its {kind} role.",
                    m.metadata.display_name
                ),
            );
            continue;
        };
        if role.replicas < declared.count || declared.gpus.is_some_and(|n| role.gpu_count < n) {
            problem(
                out,
                format!("This run asks for too few machines or GPUs for the {kind} role."),
            );
        }
        if kind == WorkloadRole::Trainer {
            continue;
        }
        if let Some(machines) = &req.machines {
            let vendor = role.gpu.vendor.map(|v| match v {
                crate::resources::GpuVendor::Nvidia => "nvidia",
                crate::resources::GpuVendor::Amd => "amd",
                crate::resources::GpuVendor::Apple => "apple",
                crate::resources::GpuVendor::Intel => "intel",
            });
            let count = machines
                .iter()
                .filter(|machine| {
                    machine
                        .classes()
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .any(|class| {
                            vendor.is_none_or(|v| v == class.as_str())
                                && (role.gpu_count == 0) == (class == HardwareClass::Cpu)
                                && machine
                                    .fits(
                                        m,
                                        class,
                                        Some(role.gpu_count),
                                        Some(role),
                                        Some(&declared.environment),
                                    )
                                    .is_some()
                        })
                })
                .count();
            if count < role.replicas as usize {
                problem(
                    out,
                    format!("No connected setup has the hardware for the {kind} role."),
                );
            }
        }
    }
}

/// The validation document `constraints[]` run against (Doc 05 §10.6).
fn document(req: &Request, config: &Value, fit: &Fit) -> Value {
    let model = match &req.model {
        Lookup::Found(f) => serde_json::to_value(f).unwrap_or(Value::Null),
        _ => Value::Null,
    };
    let datasets = |from: usize, to: Option<usize>| -> Value {
        req.datasets
            .iter()
            .enumerate()
            .filter(|(i, _)| *i >= from && to.is_none_or(|t| *i < t))
            .find_map(|(_, d)| match d {
                Lookup::Found(f) => serde_json::to_value(f).ok(),
                _ => None,
            })
            .unwrap_or(Value::Null)
    };
    json!({
        "algorithm": req.algorithm,
        "common": config.get("common").cloned().unwrap_or_else(|| json!({})),
        "engine": config.get("engine").cloned().unwrap_or_else(|| json!({})),
        "model": model,
        "datasets": { "train": datasets(0, Some(1)), "eval": datasets(1, None) },
        "signals": {
            "reward": req.reward_attached,
            "rewards": req.reward_attached.then_some(true).into_iter().collect::<Vec<_>>(),
            "rollout": config.get("rollout"),
            "reference": req.model_signals.get("reference").map_or_else(|| model.clone(), signal_facts),
            "critic": req.model_signals.get("critic").map_or_else(|| model.clone(), signal_facts),
            "teacher": req.model_signals.get("teacher").map(signal_facts),
        },
        "hardware": {
            "gpus": fit.class.map_or(req.hardware.gpus, |_| fit.gpus),
            "nodes": req.hardware.nodes,
            "gpu_mem_gib": fit.gpu_mem_gib.or(req.hardware.gpu_mem_gib),
            "compute_capability": fit.compute_capability,
            "accelerator": fit.accelerator,
            "class": fit.class.map(HardwareClass::as_str),
        },
    })
}

fn signal_facts(model: &Lookup<ModelFacts>) -> Value {
    match model {
        Lookup::Found(f) => json!(f),
        _ => Value::Null,
    }
}

/// JSON Pointer escaping for one key.
fn escape(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests;
