//! Golden rejection catalog: every file in `tests/fixtures/rejections/cases/` is one request
//! against one engine bundle, with the exact problems the matcher must return.

use super::*;
use crate::capability::manifest::{Bundle, ManifestError};
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn bundle(name: &str) -> Bundle {
    let path = match name {
        "golden" => root().join("tests/fixtures/rejections/engines/golden.json"),
        other => root().join(format!(
            "crates/mrl-server/engines/first_party/{other}.json"
        )),
    };
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    Bundle::parse(&text).unwrap_or_else(|e| panic!("{path:?}: {e}"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    about: String,
    engine: String,
    enabled: Vec<HardwareClass>,
    request: CaseRequest,
    expect: Vec<Problem>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseRequest {
    algorithm: String,
    #[serde(default)]
    config: Option<Value>,
    #[serde(default)]
    model: Option<Value>,
    #[serde(default)]
    datasets: Vec<Value>,
    #[serde(default)]
    reward_attached: bool,
    hardware: CaseHardware,
    available: BTreeSet<HardwareClass>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseHardware {
    wanted: String,
    gpus: u32,
    nodes: u32,
}

fn lookup<T: serde::de::DeserializeOwned>(v: Option<&Value>) -> Lookup<T> {
    match v {
        None => Lookup::NotGiven,
        Some(Value::String(s)) if s == "missing" => Lookup::Missing,
        Some(v) => Lookup::Found(serde_json::from_value(v.clone()).expect("facts")),
    }
}

fn request(c: &CaseRequest) -> Request {
    let wanted = match c.hardware.wanted.as_str() {
        "any" => Wanted::Any,
        "any_gpu" => Wanted::AnyGpu,
        other => Wanted::Class(HardwareClass::parse(other).expect("hardware class")),
    };
    Request {
        algorithm: c.algorithm.clone(),
        config: c.config.clone(),
        model: lookup(c.model.as_ref()),
        datasets: c.datasets.iter().map(|d| lookup(Some(d))).collect(),
        reward_attached: c.reward_attached,
        hardware: HardwareRequest {
            wanted,
            gpus: c.hardware.gpus,
            nodes: c.hardware.nodes,
            gpu_mem_gib: None,
        },
        available: c.available.clone(),
        machines: None,
        resources: None,
        model_signals: BTreeMap::new(),
        rollout: false,
    }
}

#[test]
fn golden_rejection_catalog() {
    let dir = root().join("tests/fixtures/rejections/cases");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    assert!(files.len() >= 15, "{} cases", files.len());
    let mut failures = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let case: Case = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file:?}: {e}"));
        let b = bundle(&case.engine);
        let enabled: BTreeSet<_> = case.enabled.iter().copied().collect();
        let engine = Engine {
            manifest: b.engine.as_ref().expect("a training engine"),
            schemas: &b.schemas,
            enabled: &enabled,
        };
        let got = check(engine, &request(&case.request));
        if std::env::var_os("MRL_BLESS").is_some() {
            let mut doc: Value = serde_json::from_str(&text).unwrap();
            doc["expect"] = serde_json::to_value(&got).unwrap();
            std::fs::write(&file, serde_json::to_string_pretty(&doc).unwrap() + "\n").unwrap();
            continue;
        }
        if got != case.expect {
            failures.push(format!(
                "{} ({}):\n  got {}",
                file.file_name().unwrap().to_string_lossy(),
                case.about,
                serde_json::to_string_pretty(&got).unwrap()
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn every_first_party_bundle_reads() {
    let dir = root().join("crates/mrl-server/engines/first_party");
    let mut n = 0;
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "json") {
            let b = Bundle::parse(&std::fs::read_to_string(&p).unwrap())
                .unwrap_or_else(|e| panic!("{p:?}: {e}"));
            assert_eq!(p.file_stem().unwrap().to_string_lossy(), b.name);
            n += 1;
        }
    }
    assert!(n >= 2);
}

/// The same rule the generator applies (tools/gen/engines.py), with the same fixture.
#[test]
fn reserved_metric_prefix_is_refused() {
    let mut raw = bundle("golden").raw;
    raw["spec"]["algorithms"][0]["metrics"][0]["name"] = json!("resource/gpu_peak");
    raw["spec"]["metrics"] = json!({"namespaces": {"mem": "resource/mem_peak"}});
    let schemas = bundle("golden").schemas;
    let err = EngineManifest::check(&raw, &schemas).unwrap_err();
    assert_eq!(
        err,
        ManifestError::ReservedMetrics(vec![
            "resource/gpu_peak".into(),
            "resource/mem_peak".into()
        ])
    );
    let expected = std::fs::read_to_string(
        root().join("tests/fixtures/rejections/manifests/reserved-metric.expected"),
    )
    .unwrap();
    assert!(
        expected.contains(
            "metric names starting with `resource/` belong to the platform (resource use per phase)"
        ),
        "{expected}"
    );
    assert!(err.to_string().starts_with(
        "metric names starting with `resource/` belong to the platform (resource use per phase); rename"
    ));
}

#[test]
fn bundle_problems_are_named() {
    let b = bundle("golden");
    let mut raw = b.raw.clone();
    raw["spec"]["algorithms"][1]["id"] = json!("sft");
    let err = EngineManifest::check(&raw, &b.schemas).unwrap_err();
    assert!(err.to_string().contains("`sft` is listed twice"), "{err}");
    let err = EngineManifest::check(&b.raw, &BTreeMap::new()).unwrap_err();
    assert!(err.to_string().contains("not in the bundle"), "{err}");
    let mut raw = b.raw.clone();
    raw["spec"]["capabilities"]["precision"] = json!(["fp8"]);
    let err = EngineManifest::check(&raw, &b.schemas).unwrap_err();
    assert!(
        err.to_string().contains("/spec/capabilities/precision/0"),
        "{err}"
    );
    assert!(matches!(
        Bundle::parse("{}"),
        Err(ManifestError::Unreadable(_))
    ));
}

#[test]
fn hardware_classes_follow_environments_and_spec_hardware() {
    let b = bundle("golden");
    let m = b.engine.unwrap();
    assert_eq!(
        m.hardware_classes(),
        [HardwareClass::Cpu, HardwareClass::Nvidia].into()
    );
    let fake = bundle("fake").engine.unwrap();
    assert_eq!(fake.hardware_classes(), [HardwareClass::Cpu].into());
}

fn accepted_sft() -> Request {
    let fixture = include_str!(
        "../../../../../tests/fixtures/rejections/cases/90-accepted-lora-on-nvidia.json"
    );
    request(&serde_json::from_str::<Case>(fixture).unwrap().request)
}

fn device(class: HardwareClass, cc: &str) -> Device {
    Device {
        class,
        memory: 24 << 30,
        compute_capability: Some(cc.into()),
        driver: Some("535.10".into()),
    }
}

fn linux(devices: Vec<Device>) -> Machine {
    Machine {
        platform: Some(Platform::Linux),
        arch: "x86_64".into(),
        devices,
    }
}

#[test]
fn a_vendor_match_alone_does_not_make_a_runtime_eligible() {
    let b = bundle("golden");
    let manifest = b.engine.as_ref().unwrap();
    let enabled = [HardwareClass::Nvidia].into();
    let engine = Engine {
        manifest,
        enabled: &enabled,
        schemas: &b.schemas,
    };
    let mut req = accepted_sft();
    req.machines = Some(vec![linux(vec![device(HardwareClass::Nvidia, "7.5")])]);
    assert_eq!(check(engine, &req)[0].code, Cap::NoEngineForHardware.code());

    let mut good = linux(vec![device(HardwareClass::Nvidia, "8.0")]);
    good.arch = "aarch64".into();
    req.machines.as_mut().unwrap().push(good.clone());
    assert_eq!(check(engine, &req)[0].code, Cap::NoEngineForHardware.code());
    good.arch = "x86_64".into();
    req.machines.as_mut().unwrap().push(good);
    assert!(check(engine, &req).is_empty());
}

#[test]
fn constraints_try_other_vendors_and_the_listing_checks_the_gpu_minimum() {
    let b = bundle("golden");
    let mut manifest = b.engine.unwrap();
    let cuda = manifest.spec.runtime.environments[0].clone();
    let mut amd = cuda.clone();
    amd.id = "rocm".into();
    let accelerator = amd.accelerator.as_mut().unwrap();
    accelerator.vendor = HardwareClass::Amd;
    accelerator.runtime = Some("rocm6".into());
    manifest.spec.runtime.environments = vec![cuda, amd];
    let hardware = manifest.spec.hardware.as_mut().unwrap();
    hardware.accelerators = Some(vec![HardwareClass::Nvidia, HardwareClass::Amd]);
    hardware.min_gpus = Some(1);
    manifest.spec.constraints[0].applies_to = vec!["sft".into()];
    manifest.spec.constraints[0].schema = json!({
        "properties": {"hardware": {"properties": {"accelerator": {"const": "amd-rocm6"}}}}
    });
    let enabled = [HardwareClass::Nvidia, HardwareClass::Amd].into();
    let engine = Engine {
        manifest: &manifest,
        enabled: &enabled,
        schemas: &b.schemas,
    };
    let mut req = accepted_sft();
    req.hardware.wanted = Wanted::AnyGpu;
    req.available = enabled.clone();
    let nvidia = linux(vec![device(HardwareClass::Nvidia, "8.0")]);
    req.machines = Some(vec![nvidia.clone()]);
    assert_eq!(check(engine, &req)[0].code, Cap::KtoBatchTooSmall.code());
    req.machines
        .as_mut()
        .unwrap()
        .push(linux(vec![device(HardwareClass::Amd, "8.0")]));
    assert!(check(engine, &req).is_empty());

    req.config = None;
    req.hardware.wanted = Wanted::Any;
    req.hardware.gpus = 0;
    assert!(check(engine, &req).is_empty());
    assert!(offers_qlora(engine, &req));
    manifest.spec.capabilities.quantized_training.clear();
    assert!(!offers_qlora(
        Engine {
            manifest: &manifest,
            enabled: &enabled,
            schemas: &b.schemas,
        },
        &req
    ));
}

#[test]
fn inputs_model_policy_streaming_and_selected_topology_are_checked() {
    let b = bundle("golden");
    let mut manifest = b.engine.unwrap();
    manifest.spec.capabilities.multi_node = true;
    manifest.spec.algorithms[0].requires.signals.rollout_engine = "required".into();
    let enabled = [HardwareClass::Nvidia].into();
    let engine = Engine {
        manifest: &manifest,
        enabled: &enabled,
        schemas: &b.schemas,
    };
    let mut req = accepted_sft();
    req.hardware.nodes = 2;
    req.config = Some(json!({"common": {"parallelism": "single", "update_strategy": "lora"}}));
    if let Lookup::Found(f) = &mut req.model {
        f.requires_remote_code = true;
    }
    if let Lookup::Found(f) = &mut req.datasets[0] {
        f.streaming = true;
    }
    req.model_signals.insert("critic".into(), Lookup::Missing);
    req.model_signals
        .insert("reference".into(), Lookup::Missing);
    let problems = check(engine, &req);
    for code in [
        codes::INPUT_MISSING,
        codes::MODEL_UNKNOWN,
        Cap::ModelBlockedByPolicy.code(),
        Cap::StreamingNeedsStepLimit.code(),
        Cap::SingleMachineOnly.code(),
    ] {
        assert!(
            problems.iter().any(|p| p.code == code),
            "{code}: {problems:?}"
        );
    }
    assert_eq!(
        problems
            .iter()
            .filter(|p| p.code == codes::MODEL_UNKNOWN)
            .count(),
        2
    );
}

#[test]
fn startup_rejects_generated_bundle_mistakes() {
    let b = bundle("golden");
    let mut raw = b.raw.clone();
    raw["spec"]["metrics"] = json!({"namespaces":{"host":"resource"}});
    assert_eq!(
        EngineManifest::check(&raw, &b.schemas).unwrap_err(),
        ManifestError::ReservedMetrics(vec!["resource".into()])
    );
    raw = b.raw.clone();
    raw["spec"]["constraints"][0]["error"]["code"] = json!("MRL-CAP-999");
    raw["spec"]["algorithms"][0]["config"]["uiSchema"] = json!("missing.json");
    let error = EngineManifest::check(&raw, &b.schemas)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("guidance catalog") && error.contains("missing.json"),
        "{error}"
    );
}

#[test]
fn processor_runtimes_run_on_any_allowed_platform_but_gpu_runtimes_need_their_own() {
    // FakeEngine declares one CPU environment, built on linux/amd64.
    let fake = bundle("fake");
    let fake = fake.engine.as_ref().unwrap();
    let mac = Machine {
        platform: Some(Platform::AppleSilicon),
        arch: "aarch64".into(),
        devices: vec![],
    };
    assert!(mac.supports(fake, HardwareClass::Cpu));
    let unknown = Machine {
        platform: None,
        arch: String::new(),
        devices: vec![],
    };
    assert!(unknown.supports(fake, HardwareClass::Cpu));

    // The manifest's OS list still applies to processor runtimes.
    let mut linux_only = fake.clone();
    linux_only.spec.hardware.as_mut().unwrap().os = Some(vec!["linux".into()]);
    assert!(!mac.supports(&linux_only, HardwareClass::Cpu));

    // A GPU runtime still needs its exact platform (golden's CUDA one is linux/amd64).
    let golden = bundle("golden");
    let golden = golden.engine.as_ref().unwrap();
    let mut arm = linux(vec![device(HardwareClass::Nvidia, "8.0")]);
    arm.arch = "aarch64".into();
    assert!(!arm.supports(golden, HardwareClass::Nvidia));
    assert!(arm.supports(golden, HardwareClass::Cpu));
    let unknown_gpu = Machine {
        platform: None,
        ..arm
    };
    assert!(!unknown_gpu.supports(golden, HardwareClass::Nvidia));
}

#[test]
fn a_whole_config_schema_checks_the_config_and_an_engine_schema_the_block() {
    // TRL's schema describes the whole config (`common` + `engine`).
    let b = bundle("trl");
    let enabled = [HardwareClass::Nvidia].into();
    let engine = Engine {
        manifest: b.engine.as_ref().unwrap(),
        enabled: &enabled,
        schemas: &b.schemas,
    };
    let field = |req: &Request| -> Vec<Option<String>> {
        check(engine, req)
            .into_iter()
            .filter(|p| p.code == codes::FIELD_VALUE)
            .map(|p| p.path)
            .collect()
    };
    let mut req = accepted_sft();
    req.config = Some(json!({"common": {"update": {"strategy": "full"}},
        "engine": {"max_steps": 12, "save_steps": 6, "learning_rate": 0.003}}));
    assert!(field(&req).is_empty(), "{:?}", field(&req));
    req.config = Some(json!({"engine": {"max_stepz": 12}}));
    assert_eq!(field(&req), [Some("/config/engine".to_owned())]);
    // The golden bundle's schema is the engine block alone.
    let g = bundle("golden");
    let engine = Engine {
        manifest: g.engine.as_ref().unwrap(),
        enabled: &enabled,
        schemas: &g.schemas,
    };
    req.config = Some(json!({"engine": {"learning_rate": 0.1}}));
    assert!(check(engine, &req)
        .iter()
        .all(|p| p.code != codes::FIELD_VALUE));
}

#[test]
fn a_builtin_bundle_needs_data_prep_and_a_sandbox_for_every_job_kind() {
    let path = root().join("crates/mrl-server/engines/first_party/mrl-builtin.json");
    let good: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert!(Bundle::parse(&good.to_string()).is_ok());
    for kinds in [
        json!([{"kind": "IMPORT", "sandbox": "fetch"}]),
        json!([{"kind": "DATA_PREP", "sandbox": "data_prep"}, {"kind": "IMPORT"}]),
        json!([{"kind": "DATA_PREP", "sandbox": "data_prep"}, {"kind": "", "sandbox": "fetch"}]),
    ] {
        let mut bad = good.clone();
        bad["manifest"]["spec"]["jobKinds"] = kinds.clone();
        assert!(Bundle::parse(&bad.to_string()).is_err(), "{kinds}");
    }
}
