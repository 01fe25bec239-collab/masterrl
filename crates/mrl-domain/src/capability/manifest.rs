//! Engine manifests as the server embeds them: a bundle written by `tools/gen/engines.py`
//! (the merged `mrl-engine.yaml`, plus the config schemas its algorithms name).
//!
//! [`Bundle::parse`] checks a bundle the same way the generator does: the manifest must match
//! `schemas/engine-manifest/v1`, algorithm ids are unique, metric names stay out of the
//! platform's reserved `resource/` prefix, and every schema it names is present and compiles.

use crate::errors::ErrorCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

/// Platform metric names (resource peaks per phase). No engine may emit into this prefix.
pub const RESERVED_METRIC_PREFIX: &str = "resource/";

/// The engine-manifest/v1 schema (P09).
const MANIFEST_SCHEMA: &str = include_str!("../../../../schemas/engine-manifest/v1.schema.json");

/// Why a bundle can't be used. Every message names the field and says what to change.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    /// The bundle is not the JSON the generator writes.
    #[error("the engine bundle can't be read: {0}")]
    Unreadable(String),
    /// The manifest breaks the engine-manifest/v1 rules.
    #[error("the engine manifest has problems: {}", .0.join("; "))]
    Invalid(Vec<String>),
    /// Metric names use the platform's reserved prefix.
    #[error(
        "metric names starting with `{RESERVED_METRIC_PREFIX}` belong to the platform (resource use per phase); rename {}",
        .0.join(", ")
    )]
    ReservedMetrics(Vec<String>),
}

/// What kind of engine a bundle holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleKind {
    /// A training engine (`mrl.dev/engine/v1`).
    Engine,
    /// The built-in utility engine (`mrl.dev/builtin/v1`): job kinds, no training methods.
    Builtin,
}

/// One embedded engine bundle, checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Bundle {
    /// What it holds.
    pub kind: BundleKind,
    /// The adapter folder it came from.
    pub source: String,
    /// Registered only on test and dev servers (the FakeEngine).
    pub dev_only: bool,
    /// The merged manifest, exactly as written.
    pub raw: Value,
    /// Config and UI schemas, by the path the manifest names them with.
    pub schemas: BTreeMap<String, Value>,
    /// The typed manifest (training engines only).
    pub engine: Option<EngineManifest>,
    /// `metadata.name`.
    pub name: String,
    /// `metadata.version`.
    pub version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBundle {
    kind: BundleKind,
    source: String,
    dev_only: bool,
    manifest: Value,
    schemas: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
struct Metadata {
    name: String,
    version: String,
}

#[derive(Deserialize)]
struct Named {
    metadata: Metadata,
}

impl Bundle {
    /// Reads and checks a bundle written by the generator.
    pub fn parse(text: &str) -> Result<Self, ManifestError> {
        let raw: RawBundle =
            serde_json::from_str(text).map_err(|e| ManifestError::Unreadable(e.to_string()))?;
        let named: Named = serde_json::from_value(raw.manifest.clone())
            .map_err(|e| ManifestError::Invalid(vec![format!("/metadata: {e}")]))?;
        let engine = match raw.kind {
            BundleKind::Builtin => {
                let text = |path| {
                    raw.manifest
                        .pointer(path)
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty())
                };
                let kinds = raw
                    .manifest
                    .pointer("/spec/jobKinds")
                    .and_then(Value::as_array);
                if raw.manifest.get("apiVersion") != Some(&Value::from("mrl.dev/builtin/v1"))
                    || raw.manifest.get("kind") != Some(&Value::from("BuiltinManifest"))
                    || ![
                        "/metadata/name",
                        "/metadata/version",
                        "/metadata/displayName",
                        "/spec/adapter/entrypoint",
                        "/spec/adapter/adapterApi",
                        "/spec/adapter/workerProtocol",
                    ]
                    .into_iter()
                    .all(text)
                    || !kinds.is_some_and(|rows| {
                        fn field<'a>(row: &'a Value, key: &str) -> Option<&'a str> {
                            row.get(key)
                                .and_then(Value::as_str)
                                .filter(|s| !s.is_empty())
                        }
                        rows.iter().all(|row| {
                            field(row, "kind").is_some() && field(row, "sandbox").is_some()
                        }) && rows.iter().any(|row| {
                            field(row, "kind") == Some("DATA_PREP")
                                && field(row, "sandbox") == Some("data_prep")
                        })
                    })
                {
                    return Err(ManifestError::Invalid(vec![
                        "the built-in bundle needs mrl.dev/builtin/v1, adapter metadata and the DATA_PREP sandbox".into(),
                    ]));
                }
                None
            }
            BundleKind::Engine => Some(EngineManifest::check(&raw.manifest, &raw.schemas)?),
        };
        Ok(Self {
            kind: raw.kind,
            source: raw.source,
            dev_only: raw.dev_only,
            raw: raw.manifest,
            schemas: raw.schemas,
            engine,
            name: named.metadata.name,
            version: named.metadata.version,
        })
    }
}

fn manifest_schema() -> Result<&'static jsonschema::Validator, ManifestError> {
    static V: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();
    V.get_or_init(|| {
        let schema: Value = serde_json::from_str(MANIFEST_SCHEMA).map_err(|e| e.to_string())?;
        jsonschema::draft202012::new(&schema).map_err(|e| e.to_string())
    })
    .as_ref()
    .map_err(|e| ManifestError::Invalid(vec![format!("the manifest schema can't be used: {e}")]))
}

/// A hardware class: where an engine version is probed and enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareClass {
    /// Any machine's processor, no GPU.
    Cpu,
    /// NVIDIA GPUs (CUDA).
    Nvidia,
    /// AMD GPUs (ROCm).
    Amd,
    /// Apple silicon GPUs (MPS / MLX).
    Apple,
}

impl HardwareClass {
    /// Every class.
    pub const ALL: [Self; 4] = [Self::Cpu, Self::Nvidia, Self::Amd, Self::Apple];

    /// Stored name, like `nvidia`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Nvidia => "nvidia",
            Self::Amd => "amd",
            Self::Apple => "apple",
        }
    }

    /// The class for a stored name.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == s)
    }

    /// Plain words for messages, like `an NVIDIA GPU`.
    pub const fn describe(self) -> &'static str {
        match self {
            Self::Cpu => "the processor only (no GPU)",
            Self::Nvidia => "an NVIDIA GPU",
            Self::Amd => "an AMD GPU",
            Self::Apple => "an Apple silicon GPU",
        }
    }
}

/// The part of a training engine's manifest the control plane decides with.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineManifest {
    /// Name and version.
    pub metadata: EngineMetadata,
    /// The spec.
    pub spec: EngineSpec,
}

/// `metadata`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineMetadata {
    /// Adapter name, like `trl`.
    pub name: String,
    /// Adapter version.
    pub version: String,
    /// Shown in the app.
    pub display_name: String,
    /// Longer text.
    #[serde(default)]
    pub description: Option<String>,
}

/// `spec`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineSpec {
    /// Executable environments.
    pub runtime: Runtime,
    /// Engine-wide capabilities.
    pub capabilities: Capabilities,
    /// Training methods.
    pub algorithms: Vec<Algorithm>,
    /// Rejection rules over the validation document.
    #[serde(default)]
    pub constraints: Vec<Constraint>,
    /// Workload roles and their runtime environments.
    pub topology: Topology,
    /// Where it can run at all.
    #[serde(default)]
    pub hardware: Option<Hardware>,
    /// Engine metric name or prefix -> canonical name.
    #[serde(default)]
    pub metrics: Option<MetricMap>,
}

/// `spec.runtime`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Runtime {
    /// Environments.
    pub environments: Vec<Environment>,
}

/// `spec.runtime.environments[]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Environment {
    /// Environment id.
    pub id: String,
    /// Target OS and CPU architecture.
    pub platform: String,
    /// The accelerator it needs (none: processor only).
    #[serde(default)]
    pub accelerator: Option<Accelerator>,
}

/// `accelerator`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Accelerator {
    /// `nvidia`, `amd`, `apple` or `cpu`.
    pub vendor: HardwareClass,
    /// Accelerator runtime, such as cuda12.
    pub runtime: Option<String>,
    /// Minimum driver version, when specified.
    pub min_driver: Option<String>,
    /// Minimum device compute capability, when specified.
    pub min_compute_capability: Option<String>,
}

/// The launch topology.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Topology {
    /// Declared roles.
    pub roles: Vec<TopologyRole>,
}

/// A declared launch role.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologyRole {
    /// Role identifier.
    pub id: String,
    /// Workload type.
    pub kind: String,
    /// Runtime environment name.
    pub environment: String,
    /// Minimum role count.
    pub count: u32,
    /// Minimum GPU count for the role.
    pub gpus: Option<u32>,
    /// Role sharing a machine with this one, if any.
    pub colocated_with: Option<String>,
}

/// `spec.capabilities` (the fields L0 checks).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// `full`, `lora`, `qlora`, …
    pub update_strategies: Vec<String>,
    /// `bf16`, `fp16`, `fp32`.
    pub precision: Vec<String>,
    /// `bnb-4bit`, `bnb-8bit`.
    #[serde(default)]
    pub quantized_training: Vec<String>,
    /// `single`, `ddp`, …
    pub parallelism: Vec<String>,
    /// Can span machines.
    pub multi_node: bool,
    /// Whether the engine allows models shipped with custom code.
    pub trust_remote_code: String,
}

/// `spec.hardware`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hardware {
    /// Accelerators it runs on at all.
    #[serde(default)]
    pub accelerators: Option<Vec<HardwareClass>>,
    /// Allowed operating systems.
    pub os: Option<Vec<String>>,
    /// Whether WSL2 is supported.
    pub wsl2: Option<bool>,
    /// Whether Apple Silicon is supported.
    #[serde(rename = "apple_silicon")]
    pub apple_silicon: Option<bool>,
    /// Fewest GPUs per machine.
    #[serde(default)]
    pub min_gpus: Option<u32>,
    /// Most GPUs per machine.
    #[serde(default)]
    pub max_gpus: Option<u32>,
}

/// `spec.metrics`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct MetricMap {
    /// Engine name or prefix -> canonical name.
    #[serde(default)]
    pub namespaces: BTreeMap<String, String>,
}

/// One training method (`AlgorithmDefinition`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Algorithm {
    /// Canonical or `x-<engine>.<name>` id.
    pub id: String,
    /// Shown in the app.
    pub display_name: String,
    /// One line.
    pub summary: String,
    /// Upstream trainer.
    pub parent: String,
    /// Fields the adapter pins; users can't change them.
    #[serde(default)]
    pub switch: serde_json::Map<String, Value>,
    /// Recommended values.
    #[serde(default)]
    pub defaults: serde_json::Map<String, Value>,
    /// `stable`, `beta` or `experimental`.
    pub maturity: String,
    /// `full`, `partial` or `approximation`.
    pub fidelity: String,
    /// Why fidelity is not full.
    #[serde(default)]
    pub fidelity_note: Option<String>,
    /// Data, signals and models it needs.
    pub requires: Requires,
    /// Narrowing of the engine's capabilities.
    #[serde(default)]
    pub supports: Supports,
    /// Config form.
    #[serde(default)]
    pub config: Option<ConfigForm>,
    /// Metrics it emits.
    pub metrics: Vec<Metric>,
    /// Can resume.
    pub resume: bool,
    /// Can pause.
    pub pause: bool,
}

/// `requires`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Requires {
    /// Dataset schemas per role.
    pub dataset_schemas: DatasetSchemas,
    /// Signals.
    pub signals: Signals,
    /// Model families it trains (empty: any).
    #[serde(default)]
    pub model_families: Vec<String>,
}

/// `requires.datasetSchemas`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DatasetSchemas {
    /// For training data.
    pub train: Vec<String>,
    /// For evaluation data (none listed: the training schemas).
    #[serde(default)]
    pub eval: Vec<String>,
}

/// `requires.signals` (the ones a submission can supply).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Signals {
    /// `required`, `optional` or `none`.
    pub reward: String,
    /// `required` or `none`.
    pub teacher: String,
    /// `required`, `optional` or `none`.
    pub reference: String,
    /// `required` or `none`.
    pub critic: String,
    /// `required`, `optional` or `none`.
    pub rollout_engine: String,
}

/// `supports`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Supports {
    /// Narrower update strategies.
    #[serde(default)]
    pub update_strategies: Option<Vec<String>>,
    /// Narrower parallelism.
    #[serde(default)]
    pub parallelism: Option<Vec<String>>,
}

/// `config`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigForm {
    /// Path of the JSON Schema for the `engine` block (inlined in the bundle).
    pub schema: String,
    /// Path of the UI hints.
    #[serde(default)]
    pub ui_schema: Option<String>,
    /// Presets (JSON Merge Patches).
    #[serde(default)]
    pub presets: Vec<Value>,
}

/// `metrics[]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Metric {
    /// Canonical metric name.
    pub name: String,
}

/// `constraints[]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Constraint {
    /// Rule id.
    pub id: String,
    /// Methods it applies to.
    pub applies_to: Vec<String>,
    /// JSON Schema over the validation document; failing it rejects.
    pub schema: Value,
    /// What the user sees.
    pub error: ConstraintError,
}

/// `constraints[].error`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ConstraintError {
    /// Catalog code.
    pub code: ErrorCode,
    /// What is wrong.
    pub message: String,
    /// What to do.
    pub hint: String,
}

impl EngineManifest {
    /// Checks a merged manifest and its inlined schemas, then reads the typed part.
    pub fn check(raw: &Value, schemas: &BTreeMap<String, Value>) -> Result<Self, ManifestError> {
        let validator = manifest_schema()?;
        let mut problems: Vec<String> = validator
            .iter_errors(raw)
            .take(10)
            .map(|e| format!("{}: {}", pointer(e.instance_path().as_str()), e.masked()))
            .collect();
        if !problems.is_empty() {
            return Err(ManifestError::Invalid(problems));
        }
        let m: Self = serde_json::from_value(raw.clone())
            .map_err(|e| ManifestError::Invalid(vec![format!("/spec: {e}")]))?;
        let reserved = m.reserved_metrics();
        if !reserved.is_empty() {
            return Err(ManifestError::ReservedMetrics(reserved));
        }
        let mut seen = BTreeSet::new();
        for a in &m.spec.algorithms {
            if !seen.insert(a.id.as_str()) {
                problems.push(format!(
                    "/spec/algorithms: the method `{}` is listed twice",
                    a.id
                ));
            }
            if let Some(form) = &a.config {
                match schemas.get(&form.schema) {
                    None => problems.push(format!(
                        "/spec/algorithms/{}/config/schema: `{}` is not in the bundle",
                        a.id, form.schema
                    )),
                    Some(s) => {
                        if let Err(e) = jsonschema::draft202012::new(s) {
                            problems.push(format!(
                                "/spec/algorithms/{}/config/schema: `{}` is not a usable JSON Schema: {}",
                                a.id, form.schema, e.masked()
                            ));
                        }
                    }
                }
                if let Some(path) = &form.ui_schema {
                    if !schemas.get(path).is_some_and(Value::is_object) {
                        problems.push(format!("/spec/algorithms/{}/config/uiSchema: `{path}` must name an inlined UI object", a.id));
                    }
                }
            }
        }
        for c in &m.spec.constraints {
            if c.error.code.title().is_none() {
                problems.push(format!(
                    "/spec/constraints/{}/error/code: {} is not in the guidance catalog",
                    c.id, c.error.code
                ));
            }
            for method in &c.applies_to {
                if m.algorithm(method).is_none() {
                    problems.push(format!(
                        "/spec/constraints/{}/appliesTo: unknown method `{method}`",
                        c.id
                    ));
                }
            }
            if let Err(e) = jsonschema::draft202012::new(&c.schema) {
                problems.push(format!(
                    "/spec/constraints/{}/schema: not a usable JSON Schema: {}",
                    c.id,
                    e.masked()
                ));
            }
        }
        if problems.is_empty() {
            Ok(m)
        } else {
            Err(ManifestError::Invalid(problems))
        }
    }

    /// Canonical metric names (declared or mapped) in the reserved prefix, sorted.
    pub fn reserved_metrics(&self) -> Vec<String> {
        let declared = self
            .spec
            .algorithms
            .iter()
            .flat_map(|a| a.metrics.iter().map(|m| &m.name));
        // The runtime appends the original metric suffix to the mapping target.
        let mapped = self.spec.metrics.iter().flat_map(|m| m.namespaces.values());
        declared
            .filter(|n| n.starts_with(RESERVED_METRIC_PREFIX))
            .chain(mapped.filter(|n| {
                n.starts_with(RESERVED_METRIC_PREFIX)
                    || RESERVED_METRIC_PREFIX.starts_with(n.as_str())
            }))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// The method with this id.
    pub fn algorithm(&self, id: &str) -> Option<&Algorithm> {
        self.spec.algorithms.iter().find(|a| a.id == id)
    }

    /// The hardware classes some environment can run on, narrowed by `spec.hardware`.
    pub fn hardware_classes(&self) -> BTreeSet<HardwareClass> {
        let envs = self.spec.runtime.environments.iter().map(|e| {
            e.accelerator
                .as_ref()
                .map_or(HardwareClass::Cpu, |a| a.vendor)
        });
        let allowed = self
            .spec
            .hardware
            .as_ref()
            .and_then(|h| h.accelerators.as_ref());
        envs.filter(|c| allowed.is_none_or(|a| a.contains(c)))
            .collect()
    }
}

fn pointer(p: &str) -> &str {
    if p.is_empty() {
        "/"
    } else {
        p
    }
}
