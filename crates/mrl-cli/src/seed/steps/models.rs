//! The model seed step (P26): the fixture `tiny-llama` (made by
//! `tools/fixtures/make_fixtures.py`) imported into project `demo` from this machine's folder
//! (`POST /projects/{prj}/models:import`, source `node_path`), then checked against
//! `tests/fixtures/models/GOLDEN.json` once READY. A model that already has a READY or
//! IMPORTING version is left alone, so a second run answers "exists" with the same id.

use super::super::{SeedState, SeedStep};
use crate::api::{all_items, Api, Failure};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Name of the seeded model.
pub const MODEL: &str = "tiny-llama";
/// Folder override (an absolute path on the machine that runs the import).
pub const DIR_ENV: &str = "MRL_SEED_MODEL_DIR";
const GOLDEN: &str = include_str!("../../../../../tests/fixtures/models/GOLDEN.json");
/// How long the step waits for the import job.
const WAIT: Duration = Duration::from_secs(120);

/// The fixture folder: `MRL_SEED_MODEL_DIR`, else the checkout's `tests/fixtures/models/.cache`.
fn fixture_dir() -> Result<String, String> {
    let dir = match std::env::var(DIR_ENV) {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/models/.cache/tiny-llama"
        )),
    };
    let dir = dir.canonicalize().map_err(|_| {
        format!(
            "The fixture model is missing at {}. Run `uv run python tools/fixtures/make_fixtures.py` (or set {DIR_ENV}).",
            dir.display()
        )
    })?;
    dir.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{} is not a UTF-8 path.", dir.display()))
}

fn segment(id: &str) -> Result<&str, String> {
    if !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        Ok(id)
    } else {
        Err(format!(
            "The server gave `{id}` as an id, which can't be used in a request."
        ))
    }
}

/// Returns `(version, created)`: the newest READY or IMPORTING version of model `tiny-llama`
/// in `prj`, or a new import of `dir`; `None` when the server has no model registry (an
/// older server answers its routes with MRL-RES-002).
pub fn ensure_model(
    mut get: impl FnMut(&str) -> Result<Value, Failure>,
    mut post: impl FnMut(&str, &Value, &str) -> Result<Value, Failure>,
    prj: &str,
    dir: impl FnOnce() -> Result<String, String>,
) -> Result<Option<(Value, bool)>, String> {
    let prj = segment(prj)?;
    let list = format!("/projects/{prj}/models");
    match get(&list) {
        Err(f) if f.status == Some(404) && f.message.contains("MRL-RES-002") => return Ok(None),
        Err(f) => return Err(f.into()),
        Ok(_) => {}
    }
    let models = all_items(|p| get(p).map_err(String::from), &list)?;
    let mut previous = "initial".to_owned();
    if let Some(m) = models.iter().find(|m| m["name"] == MODEL) {
        let id = segment(m["id"].as_str().unwrap_or_default())?;
        let versions = all_items(
            |p| get(p).map_err(String::from),
            &format!("/models/{id}/versions"),
        )?;
        if let Some(v) = versions
            .iter()
            .find(|v| v["state"] == "READY" || v["state"] == "IMPORTING")
        {
            return Ok(Some((v.clone(), false)));
        }
        if let Some(id) = versions.first().and_then(|v| v["id"].as_str()) {
            previous = segment(id)?.to_owned();
        }
    }
    let body = json!({"name": MODEL, "source": {"kind": "node_path", "path": dir()?}});
    let key = format!("seed-model-{prj}-{previous}");
    let v = post(&format!("/projects/{prj}/models:import"), &body, &key).map_err(String::from)?;
    Ok(Some((v, true)))
}

/// What a READY version must match: the fixture's golden facts.
pub fn check_golden(v: &Value) -> Result<(), String> {
    let g: Value = serde_json::from_str(GOLDEN).map_err(|e| format!("GOLDEN.json: {e}"))?;
    for (key, want) in [
        ("architecture", &g["architecture"]),
        ("param_count", &g["param_count"]),
        ("context_length", &g["context_length"]),
        ("chat_template_sha256", &g["chat_template_sha256"]),
    ] {
        if &v[key] != want {
            return Err(format!(
                "The imported {MODEL} has {key} {}, but the fixture's GOLDEN.json says {want}.",
                v[key]
            ));
        }
    }
    Ok(())
}

/// Model `tiny-llama` in project `demo`.
pub struct Model;

impl SeedStep for Model {
    fn name(&self) -> &'static str {
        "model"
    }

    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String> {
        let prj = state
            .project
            .clone()
            .ok_or_else(|| "the project step did not run first".to_owned())?;
        let Some((mut v, created)) = ensure_model(
            |p| api.get(p),
            |p, b, key| api.post(p, Some(b), Some(key)),
            &prj,
            fixture_dir,
        )?
        else {
            return Ok("skipped: this server has no model registry".into());
        };
        let id = segment(v["id"].as_str().unwrap_or_default())?.to_owned();
        let started = Instant::now();
        while v["state"] == "IMPORTING" && started.elapsed() < WAIT {
            std::thread::sleep(Duration::from_secs(2));
            v = api
                .get(&format!("/model-versions/{id}"))
                .map_err(String::from)?;
        }
        let verb = if created { "created" } else { "exists " };
        match v["state"].as_str() {
            Some("READY") => {
                check_golden(&v)?;
                Ok(format!("{verb} model `{MODEL}` ({id}, READY)"))
            }
            Some("IMPORTING") => Ok(format!(
                "{verb} model `{MODEL}` ({id}, still importing: is a machine online?)"
            )),
            _ => Err(format!(
                "The {MODEL} import failed: {}",
                v["invalid_reason"]["message"]
                    .as_str()
                    .unwrap_or("no reason given")
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn a_second_run_finds_the_same_version_and_imports_nothing() {
        let posts = RefCell::new(vec![]);
        let get = |p: &str| -> Result<Value, Failure> {
            Ok(match p {
                "/projects/prj_1/models" => json!({"items": [{"id": "mdl_1", "name": MODEL}]}),
                "/models/mdl_1/versions" => json!({"items": [
                    {"id": "mv_2", "state": "INVALID"}, {"id": "mv_1", "state": "READY"}]}),
                other => panic!("unexpected {other}"),
            })
        };
        let post = |p: &str, b: &Value, key: &str| -> Result<Value, Failure> {
            posts
                .borrow_mut()
                .push((p.to_owned(), b.clone(), key.to_owned()));
            Ok(json!({"id": "mv_3", "state": "IMPORTING"}))
        };
        let (v, created) = ensure_model(get, post, "prj_1", || Ok("/fx".into()))
            .unwrap()
            .unwrap();
        assert_eq!((v["id"].as_str(), created), (Some("mv_1"), false));
        assert!(posts.borrow().is_empty());

        // No model yet (or only failed versions): one import of the folder.
        let empty = |_: &str| -> Result<Value, Failure> { Ok(json!({"items": []})) };
        let (v, created) = ensure_model(empty, post, "prj_1", || Ok("/fx".into()))
            .unwrap()
            .unwrap();
        assert_eq!((v["id"].as_str(), created), (Some("mv_3"), true));
        assert_eq!(
            posts.borrow()[0],
            (
                "/projects/prj_1/models:import".to_owned(),
                json!({"name": MODEL, "source": {"kind": "node_path", "path": "/fx"}}),
                "seed-model-prj_1-initial".to_owned()
            )
        );
        assert!(ensure_model(empty, post, "../x", || Ok("/fx".into())).is_err());
        // A server without the model routes: the step is skipped, nothing is posted.
        let old = |_: &str| -> Result<Value, Failure> {
            Err(Failure {
                status: Some(404),
                message: "MRL-RES-002: No endpoint has this path.".into(),
            })
        };
        assert!(ensure_model(old, post, "prj_1", || Ok("/fx".into()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn ready_versions_are_checked_against_the_golden_facts() {
        let g: Value = serde_json::from_str(GOLDEN).unwrap();
        let mut v = json!({
            "architecture": g["architecture"], "param_count": g["param_count"],
            "context_length": g["context_length"], "chat_template_sha256": g["chat_template_sha256"],
        });
        assert!(check_golden(&v).is_ok());
        v["param_count"] = json!(1);
        assert!(check_golden(&v).unwrap_err().contains("param_count"));
    }
}
