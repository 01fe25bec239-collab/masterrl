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
use super::manifest::{EngineManifest, HardwareClass};
use crate::errors::ErrorCode;
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
    if req.config.is_some()
        && algo.requires.signals.teacher == "required"
        && !matches!(text(common, "teacher_model_version_id"), Ok(Some(_)))
    {
        out.push(Problem::new(
            codes::INPUT_MISSING,
            Some("/config/common/teacher_model_version_id"),
            format!(
                "{} learns from a teacher model, but this run names none.",
                algo.display_name
            ),
        ));
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
        if !caps.multi_node || parallel.iter().all(|p| p == "single") {
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
    let picked = hardware(engine, req, &settings, &runnable, &mut out);

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
        let doc = document(req, config, picked);
        for c in m
            .spec
            .constraints
            .iter()
            .filter(|c| c.applies_to.iter().any(|a| a == &req.algorithm))
        {
            let ok = jsonschema::draft202012::new(&c.schema)
                .map(|v| v.is_valid(&doc))
                .unwrap_or(false);
            if !ok {
                out.push(Problem {
                    code: c.error.code,
                    path: None,
                    message: c.error.message.clone(),
                    hint: Some(c.error.hint.clone()),
                });
            }
        }
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
) -> Option<HardwareClass> {
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
        return None;
    }
    if let Some(h) = &m.spec.hardware {
        if fits.iter().all(|c| *c != HardwareClass::Cpu) {
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
        return None;
    }
    match connected
        .iter()
        .copied()
        .find(|c| engine.enabled.contains(c))
    {
        Some(c) => Some(c),
        None => {
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
            None
        }
    }
}

/// The validation document `constraints[]` run against (Doc 05 §10.6).
fn document(req: &Request, config: &Value, class: Option<HardwareClass>) -> Value {
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
        "signals": { "reward": req.reward_attached },
        "hardware": {
            "gpus": req.hardware.gpus,
            "nodes": req.hardware.nodes,
            "gpu_mem_gib": req.hardware.gpu_mem_gib,
            "accelerator": class.map(HardwareClass::as_str),
        },
    })
}

/// JSON Pointer escaping for one key.
fn escape(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests;
