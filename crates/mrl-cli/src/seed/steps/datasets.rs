//! Seeds the text and chat fixtures through the resumable uploads and dataset APIs.

use super::super::{SeedState, SeedStep};
use crate::api::{all_items, Api, Failure};
use crate::config_file::CliConfig;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

type Fixture = (&'static str, &'static [u8], &'static str);
const FIXTURES: [Fixture; 2] = [
    (
        "sft.messages.v1",
        include_bytes!("../../../../../tests/fixtures/datasets/sft.messages.v1.jsonl"),
        "0dd3c505b78027b8582c370b209d84e7142c5900959983f4eaf97ab8587a3d74",
    ),
    (
        "pretrain.text.v1",
        include_bytes!("../../../../../tests/fixtures/datasets/pretrain.text.v1.jsonl"),
        "35cd8270a82876fcd6f6d831e593c016b68a053331c3e77c25d5116a7bb0130e",
    ),
];

/// Registers the bundled dataset fixtures, once per digest and project.
pub struct Datasets;

impl SeedStep for Datasets {
    fn name(&self) -> &'static str {
        "datasets"
    }

    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String> {
        let project = state
            .project
            .as_deref()
            .ok_or("The seed project is missing.")?;
        let path = format!("/projects/{}/datasets", crate::commands::encode(project));
        if let Err(e) = api.get(&path) {
            if e.status == Some(404) {
                return Ok("Dataset Studio is unavailable on this server.".into());
            }
            return Err(e.into());
        }
        FIXTURES
            .iter()
            .map(|fixture| ensure(api, project, &path, *fixture))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| items.join(", "))
    }
}

fn id(row: &Value) -> Result<String, String> {
    row["id"]
        .as_str()
        .filter(|id| id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') && !id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "The server did not return a valid ID.".into())
}

fn ensure(api: &Api, project: &str, path: &str, fixture: Fixture) -> Result<String, String> {
    let (schema, bytes, sha) = fixture;
    let name = format!("Dev seed {schema}");
    let lookup = || -> Result<Option<Value>, String> {
        Ok(all_items(|p| api.get(p).map_err(String::from), path)?
            .into_iter()
            .find(|row| row["name"] == name))
    };
    let dataset = match lookup()? {
        Some(row) => row,
        None => match api.post(
            path,
            Some(&json!({"name":name,"license":"MIT"})),
            Some(&format!("dataset-seed-{schema}")),
        ) {
            Ok(row) => row,
            Err(e) if e.status == Some(409) => lookup()?.ok_or(e.message)?,
            Err(e) => return Err(e.into()),
        },
    };
    let versions = format!("/datasets/{}/versions", id(&dataset)?);
    // The fixture's digest, not its request key, identifies it on later runs as well.
    let listed = all_items(|p| api.get(p).map_err(String::from), &versions)?;
    if let Some(row) = listed.iter().find(|row| {
        row["schema_id"] == schema
            && row["source_ref"]["upload_sha256"]
                .as_array()
                .is_some_and(|digests| digests == &[json!(sha)])
    }) {
        return wait_for_ready(api, schema, row.clone());
    }
    let upload = upload(api, project, schema, bytes, sha)?;
    let created = api.post(
        &versions,
        Some(&json!({"source":"upload","schema_id":schema,"uploads":[upload]})),
        Some(&format!("dataset-seed-{sha}")),
    )?;
    wait_for_ready(api, schema, created)
}

fn wait_for_ready(api: &Api, schema: &str, mut row: Value) -> Result<String, String> {
    let version = id(&row)?;
    let path = format!("/dataset-versions/{version}");
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match row["state"].as_str() {
            Some("READY") => return Ok(format!("{schema} READY ({version})")),
            Some("INVALID") => {
                return Err(format!(
                    "{schema} fixture failed validation: {}",
                    row["state_reason"]
                ));
            }
            Some("INGESTING" | "VALIDATING") if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_secs(1));
                row = api.get(&path)?;
            }
            Some("INGESTING" | "VALIDATING") => {
                return Err(format!(
                    "{schema} fixture {version} is queued for validation. Start a worker that can run DATA_PREP and retry the seed."
                ));
            }
            _ => {
                return Err(format!(
                    "{schema} fixture {version} has an unexpected state."
                ))
            }
        }
    }
}

fn upload(
    api: &Api,
    project: &str,
    schema: &str,
    bytes: &[u8],
    sha: &str,
) -> Result<String, String> {
    upload_with(
        |path, body, key| api.post(path, body, key),
        |path| put_part(path, bytes),
        &json!({"project_id":project,"filename":format!("{schema}.jsonl"),
               "size":bytes.len(),"sha256":sha}),
        &format!("dataset-seed-upload-{project}-{sha}"),
    )
}

fn upload_with(
    mut post: impl FnMut(&str, Option<&Value>, Option<&str>) -> Result<Value, Failure>,
    mut put: impl FnMut(&str) -> Result<(), String>,
    body: &Value,
    root_key: &str,
) -> Result<String, String> {
    let mut key = root_key.to_owned();
    let mut closed = std::collections::HashSet::new();
    loop {
        let row = post("/uploads", Some(body), Some(&key))?;
        let upload = id(&row)?;
        let path = format!("/uploads/{upload}");
        match row["state"].as_str() {
            Some("COMPLETED") => return Ok(upload),
            Some("ABORTED" | "EXPIRED") => {}
            Some("OPEN") => {
                match post(&format!("{path}:presign"), None, None) {
                    Ok(presigned) => {
                        if let Some(t) = presigned["tickets"].as_array().and_then(|a| a.first()) {
                            put(t["ticket"]["url"]
                                .as_str()
                                .ok_or("Upload response had no part URL.")?)?;
                        }
                    }
                    // A concurrent seed may already have completed it.
                    Err(e) if upload_closed(&e) => {}
                    Err(e) => return Err(e.into()),
                }
                match post(&format!("{path}:complete"), None, None) {
                    Ok(_) => return Ok(upload),
                    Err(e) if upload_closed(&e) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            _ => return Err("The server returned an unknown upload state.".into()),
        }
        if !closed.insert(upload.clone()) {
            return Err("The server repeatedly returned the same closed upload.".into());
        }
        // The closed upload identifies the next attempt even across restarts.
        key = format!("{root_key}-{upload}");
    }
}

fn upload_closed(error: &Failure) -> bool {
    error.status == Some(409) && error.message.starts_with("MRL-CONFLICT-191 ")
}

fn put_part(path: &str, bytes: &[u8]) -> Result<(), String> {
    // P19 always proxies PUT tickets. Never forward CLI credentials to a different origin.
    let token = path
        .strip_prefix("/api/v1/transfer/")
        .ok_or("Unexpected upload URL.")?;
    if token.is_empty()
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err("Unexpected upload ticket.".into());
    }
    let config = CliConfig::load()?;
    let base = config.server().ok_or("The server address is missing.")?;
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let mut request = client
        .put(format!("{}{path}", base.trim_end_matches('/')))
        .body(bytes.to_vec());
    if let Some(secret) = std::env::var_os("MRL_LAUNCH_SECRET").and_then(|s| s.into_string().ok()) {
        request = request.header("x-mrl-launch-secret", secret);
    }
    let response = request
        .send()
        .map_err(|_| "The upload connection failed.")?;
    if !response.status().is_success() {
        return Err(format!(
            "Unable to upload the seed fixture ({})",
            response.status()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_replays_through_expired_uploads_to_the_replacement() {
        let mut created = false;
        for _ in 0..2 {
            let id = upload_with(
                |path, _, key| match (path, key) {
                    ("/uploads", Some("seed")) => Ok(json!({"id":"up_first","state":"EXPIRED"})),
                    ("/uploads", Some("seed-up_first")) if created => {
                        Ok(json!({"id":"up_next","state":"COMPLETED"}))
                    }
                    ("/uploads", Some("seed-up_first")) => {
                        Ok(json!({"id":"up_next","state":"OPEN"}))
                    }
                    ("/uploads/up_next:presign", None) => Ok(json!({"tickets":[]})),
                    ("/uploads/up_next:complete", None) => {
                        created = true;
                        Ok(json!({}))
                    }
                    _ => panic!("unexpected request {path} {key:?}"),
                },
                |_| panic!("the uploaded part is already present"),
                &json!({}),
                "seed",
            )
            .unwrap();
            assert_eq!(id, "up_next");
        }
    }
}
