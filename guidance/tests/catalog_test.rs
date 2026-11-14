// Tests for the generated Rust loader.
// Run: rustc --edition 2021 --test guidance/tests/catalog_test.rs -o /tmp/guidance_rs_test && /tmp/guidance_rs_test

#[path = "../build/catalog.rs"]
mod catalog;

use catalog::{Value, CATALOG};

#[test]
fn every_error_renders() {
    let errors = CATALOG.get("errors").expect("errors").as_list();
    assert!(!errors.is_empty());
    for entry in errors {
        let code = entry.str("code").expect("code");
        let shown = catalog::explain_error(code);
        assert!(std::ptr::eq(shown, entry), "{code}");
        assert!(shown.str("title").is_some_and(|t| !t.is_empty()), "{code}");
        assert!(shown.str("why").is_some_and(|t| !t.is_empty()), "{code}");
        let steps = shown.get("what_to_do").and_then(|w| w.get("steps")).map_or(&[][..], Value::as_list);
        assert!(!steps.is_empty(), "{code}");
    }
}

#[test]
fn unknown_code_falls_back() {
    assert!(catalog::error("NOT_A_CODE").is_none());
    let fallback = catalog::explain_error("NOT_A_CODE");
    assert_eq!(fallback.str("title"), Some("Something went wrong."));
}

#[test]
fn lookups_find_entries() {
    let oom = catalog::error("OOM/OOM_GPU").expect("OOM/OOM_GPU");
    assert_eq!(oom.str("title"), Some("Training paused: the GPU ran out of memory."));
    assert_eq!(catalog::term("vram").and_then(|t| t.str("term")), Some("VRAM"));
    assert_eq!(catalog::term("Windows Subsystem for Linux").and_then(|t| t.str("term")), Some("WSL2"));
    assert_eq!(catalog::goal("small-like-big").and_then(|g| g.str("reward_type")), Some("teacher"));
    let recipe = catalog::recipe("grpo-verifiable").expect("recipe");
    assert_eq!(recipe.get("estimated").and_then(Value::as_bool), Some(true));
    let profile = catalog::hardware_profile("nvidia-8gb").expect("profile");
    assert_eq!(profile.get("verified").and_then(Value::as_bool), Some(false));
    let memory = profile.get("memory_gb").expect("memory_gb");
    assert_eq!(memory.get("min").and_then(Value::as_f64), Some(8.0));
}

#[test]
fn requirement_rows_have_totals() {
    let row = catalog::requirement("sft-lora", "macos-apple-silicon").expect("row");
    assert_eq!(row.get("supported").and_then(Value::as_bool), Some(true));
    let total = row.get("download_gb").expect("download_gb");
    let (min, max) = (total.get("min").and_then(Value::as_f64), total.get("max").and_then(Value::as_f64));
    assert!(matches!((min, max), (Some(lo), Some(hi)) if lo > 0.0 && lo <= hi));
    let unsupported = catalog::requirement("grpo", "macos-apple-silicon").expect("row");
    assert_eq!(unsupported.get("supported").and_then(Value::as_bool), Some(false));
    assert!(unsupported.get("download_gb").is_none());
}
