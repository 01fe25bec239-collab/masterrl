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
