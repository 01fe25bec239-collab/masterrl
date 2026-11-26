//! [`AttemptSpec`]: everything the agent needs to run one attempt, built from the
//! `LaunchAttempt` command (plus the `StageArtifacts` items that came before it), or read from a
//! JSON file for `mrl-agent attempt run --spec`.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use mrl_proto::agent::v1::{LaunchAttempt, StageItem};
use mrl_storage::{TicketMethod, TransferTicket};
use serde::{Deserialize, Serialize};

use super::AttemptError;
use crate::exec::Limits;

/// Most workers (replicas over all roles) one attempt may run on a machine.
pub const MAX_WORKERS: u64 = 256;
/// One attempt, ready to run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptSpec {
    /// Job, attempt and fencing epoch.
    pub attempt: AttemptId,
    /// Engine name, manifest folder or `mrl-engine.yaml` the worker loads (`--engine`).
    pub engine: String,
    /// Engine version, for the record.
    #[serde(default)]
    pub engine_version: String,
    /// The resolved job config the worker gets in `Prepare.job_spec_json`.
    pub job_spec: serde_json::Value,
    /// Where each runtime comes from (one entry per `runtime_ref` the roles use).
    pub runtimes: Vec<RuntimeSpec>,
    /// Worker processes to start.
    pub roles: Vec<RoleSpec>,
    /// Inputs to download (or copy) into the read-only inputs folder before launch.
    #[serde(default)]
    pub inputs: Vec<InputSpec>,
    /// Secrets the worker gets as environment variables (values never stored).
    #[serde(default)]
    pub secrets: Vec<SecretGrant>,
    /// CPU, memory and process limits for each worker.
    #[serde(default)]
    pub limits: Limits,
    /// Lease length in seconds; 0 means no lease (local runs).
    #[serde(default)]
    pub lease_s: u32,
    /// Stop at Prepared and wait for a start command.
    #[serde(default)]
    pub barrier: bool,
    /// Prepare only (L2 check).
    #[serde(default)]
    pub dry_run: bool,
    /// Run this many steps and commit nothing (0 = normal run).
    #[serde(default)]
    pub smoke_steps: u32,
    /// Multi-node placement: workers in the job, this node's rank and role endpoints.
    #[serde(default)]
    pub placement: Placement,
    /// Local runs only: copy committed checkpoints here instead of uploading with tickets.
    #[serde(default)]
    pub upload_dir: Option<PathBuf>,
}

/// Where this node sits in a multi-node job (from `LaunchAttempt.allocation`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    /// Workers in the whole job (default 1).
    #[serde(default = "one")]
    pub world_size: u32,
    /// This node's rank.
    #[serde(default)]
    pub node_rank: u32,
    /// Role id -> `host:port` on the job network.
    #[serde(default)]
    pub role_endpoints: BTreeMap<String, String>,
}

impl Default for Placement {
    fn default() -> Self {
        Self {
            world_size: 1,
            node_rank: 0,
            role_endpoints: BTreeMap::new(),
        }
    }
}

/// Which attempt this is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptId {
    /// `job_…`.
    pub job_id: String,
    /// `att_…`: letters, digits, `.`, `_` and `-` only (it names folders).
    pub attempt_id: String,
    /// Fencing epoch from the server.
    #[serde(default)]
    pub fencing_epoch: u64,
}

impl AttemptId {
    /// As the proto `AttemptRef`.
    pub fn to_proto(&self) -> mrl_proto::common::v1::AttemptRef {
        mrl_proto::common::v1::AttemptRef {
            job_id: self.job_id.clone(),
            attempt_id: self.attempt_id.clone(),
            fencing_epoch: self.fencing_epoch,
        }
    }
}

/// Where a runtime's environment comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSpec {
    /// `uv:sha256:<lock hash>` (process mode) or `oci:<image>@sha256:<digest>` (container).
    pub runtime_ref: String,
    /// Process mode: the adapter project holding `pyproject.toml` and `uv.lock`.
    #[serde(default)]
    pub project: Option<PathBuf>,
    /// Process mode: the workspace package to install (`uv sync --package`), if any.
    #[serde(default)]
    pub package: Option<String>,
}

/// The two kinds of runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeKind {
    /// A uv environment built from a lock file with this sha256.
    Process {
        /// Lock file sha256 (lowercase hex).
        lock_sha256: String,
    },
    /// A container image pinned by digest.
    Container {
        /// `name@sha256:<digest>`.
        image: String,
    },
}

impl RuntimeSpec {
    /// Parses the runtime reference.
    pub fn kind(&self) -> Result<RuntimeKind, AttemptError> {
        let r = &self.runtime_ref;
        if let Some(hex) = r.strip_prefix("uv:sha256:") {
            if is_sha256(hex) {
                return Ok(RuntimeKind::Process {
                    lock_sha256: hex.to_owned(),
                });
            }
        } else if let Some(image) = r.strip_prefix("oci:") {
            if let Some((name, digest)) = image.split_once("@sha256:") {
                if !name.is_empty() && is_sha256(digest) {
                    return Ok(RuntimeKind::Container {
                        image: image.to_owned(),
                    });
                }
            }
        }
        Err(AttemptError::Spec(format!(
            "runtime {r:?} must be uv:sha256:<64 hex> or oci:<image>@sha256:<64 hex>; tags and version ranges are never resolved at run time"
        )))
    }

    /// A folder-safe name for this runtime, like `uv-sha256-<hex>`.
    pub fn dir_name(&self) -> String {
        self.runtime_ref
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect()
    }
}

/// One worker role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleSpec {
    /// Role id the worker sends in `Hello`, like `trainer`.
    pub role_id: String,
    /// `trainer`, `rollout_server`, `reward_exec`, ...
    #[serde(default)]
    pub kind: String,
    /// The primary role sends metrics, checkpoints and the authoritative exit report.
    #[serde(default)]
    pub primary: bool,
    /// Worker processes for this role (default 1). With more than one, each gets role id
    /// `<role_id>.<n>` and only `.0` of the primary role is primary.
    #[serde(default = "one")]
    pub count: u32,
    /// GPU indexes this role may use.
    #[serde(default)]
    pub gpus: Vec<u32>,
    /// The runtime this role runs in (one of [`AttemptSpec::runtimes`]).
    pub runtime_ref: String,
}

fn one() -> u32 {
    1
}

/// An input file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    /// Role the worker sees in `Prepare.staged_paths`, like `model` or `dataset.train`.
    pub name: String,
    /// Path inside the attempt's inputs folder.
    pub local_path: String,
    /// Expected sha256.
    pub sha256: String,
    /// Expected size in bytes.
    #[serde(default)]
    pub size: u64,
    /// Download link (presigned or the server's transfer endpoint).
    #[serde(default)]
    pub url: Option<String>,
    /// When the link stops working.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub expires_at: Option<time::OffsetDateTime>,
    /// Local runs only: copy this file instead of downloading.
    #[serde(default)]
    pub file: Option<PathBuf>,
    /// Headers to send with `url` exactly as given (part of a presigned URL's signature).
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

impl InputSpec {
    /// A folder input: its file sits under `<name>/` (one input per file of a model or a
    /// checkpoint), and the worker gets the folder `<inputs>/<name>`. Others are one file.
    pub fn is_folder(&self) -> bool {
        let path = Path::new(&self.local_path);
        path.components().count() > 1 && path.starts_with(&self.name)
    }
}

/// A secret the worker gets as an environment variable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretGrant {
    /// Credential name, like `hf_hub`.
    pub name: String,
    /// Environment variable; default `MRL_CREDENTIAL_<NAME>`.
    #[serde(default)]
    pub env: Option<String>,
}

impl SecretGrant {
    /// The environment variable the worker reads.
    pub fn env_var(&self) -> String {
        self.env
            .clone()
            .unwrap_or_else(|| credential_env(&self.name))
    }
}

pub use mrl_proto::credential_env;

/// A secret value: kept in memory only, never printed.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps a value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    /// The value (only for building the worker's environment).
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(..)")
    }
}

impl AttemptSpec {
    /// Reads a spec from JSON and checks it.
    pub fn from_json(text: &str) -> Result<Self, AttemptError> {
        let spec: Self =
            serde_json::from_str(text).map_err(|e| AttemptError::Spec(e.to_string()))?;
        spec.validate()?;
        Ok(spec)
    }

    /// Builds the spec for a `LaunchAttempt` command. `staged` are the `StageArtifacts` items
    /// received for this attempt; process-mode runtimes come from the installed adapter projects
    /// in `<state>/runtimes/<runtime>/`.
    pub fn from_launch(
        launch: &LaunchAttempt,
        staged: &[StageItem],
        state_dir: &Path,
    ) -> Result<Self, AttemptError> {
        let attempt = launch
            .attempt
            .as_ref()
            .ok_or_else(|| AttemptError::Spec("the launch has no attempt".into()))?;
        let job_spec = serde_json::from_str(&launch.job_spec_json)
            .map_err(|e| AttemptError::Spec(format!("job_spec_json: {e}")))?;
        let mut runtimes: Vec<RuntimeSpec> = Vec::new();
        for role in &launch.roles {
            if runtimes.iter().all(|r| r.runtime_ref != role.runtime_ref) {
                let mut rt = RuntimeSpec {
                    runtime_ref: role.runtime_ref.clone(),
                    project: None,
                    package: None,
                };
                if matches!(rt.kind()?, RuntimeKind::Process { .. }) {
                    rt.project = Some(state_dir.join("runtimes").join(rt.dir_name()));
                }
                runtimes.push(rt);
            }
        }
        let roles = launch
            .roles
            .iter()
            .map(|r| RoleSpec {
                role_id: r.role_id.clone(),
                kind: r.kind.clone(),
                primary: r.primary,
                count: r.count.max(1),
                gpus: if r.gpu_indices.is_empty() {
                    launch
                        .allocation
                        .as_ref()
                        .map(|a| a.gpu_indices.clone())
                        .unwrap_or_default()
                } else {
                    r.gpu_indices.clone()
                },
                runtime_ref: r.runtime_ref.clone(),
            })
            .collect();
        let spec = Self {
            attempt: AttemptId {
                job_id: attempt.job_id.clone(),
                attempt_id: attempt.attempt_id.clone(),
                fencing_epoch: attempt.fencing_epoch,
            },
            engine: launch.engine_name.clone(),
            engine_version: launch.engine_version.clone(),
            job_spec,
            runtimes,
            roles,
            inputs: staged
                .iter()
                .map(input_from_stage)
                .collect::<Result<_, _>>()?,
            secrets: Vec::new(),
            limits: Limits::default(),
            // A launch from the core always runs under a lease.
            lease_s: match launch.lease_duration_s {
                0 => {
                    return Err(AttemptError::Spec(
                        "the launch has no lease (lease_duration_s is 0)".into(),
                    ))
                }
                s => s,
            },
            barrier: launch.barrier,
            dry_run: launch.dry_run,
            smoke_steps: launch.smoke_steps,
            placement: launch
                .allocation
                .as_ref()
                .map(|a| Placement {
                    world_size: a.world_size.max(1),
                    node_rank: a.node_rank,
                    role_endpoints: a
                        .role_endpoints
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                })
                .unwrap_or_default(),
            upload_dir: None,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Checks names, paths and references.
    pub fn validate(&self) -> Result<(), AttemptError> {
        let bad = |m: String| Err(AttemptError::Spec(m));
        if !safe_name(&self.attempt.attempt_id) {
            return bad(format!(
                "attempt id {:?} may use only letters, digits, '.', '_' and '-'",
                self.attempt.attempt_id
            ));
        }
        if self.engine.is_empty() {
            return bad("engine is empty".into());
        }
        if self.roles.is_empty() {
            return bad("the attempt has no roles".into());
        }
        // Checked before anything expands the roles into worker ids.
        let total: u64 = self.roles.iter().map(|r| u64::from(r.count.max(1))).sum();
        if total > MAX_WORKERS {
            return bad(format!(
                "the attempt asks for {total} workers; one machine runs at most {MAX_WORKERS}"
            ));
        }
        if self.roles.iter().filter(|r| r.primary).count() != 1 {
            return bad("exactly one role must be primary".into());
        }
        for rt in &self.runtimes {
            if let RuntimeKind::Process { .. } = rt.kind()? {
                if rt.project.is_none() {
                    return bad(format!("runtime {} has no project folder", rt.runtime_ref));
                }
            }
        }
        let mut ids = std::collections::BTreeSet::new();
        for role in &self.roles {
            if !safe_name(&role.role_id) {
                return bad(format!(
                    "role id {:?} may use only letters, digits, '.', '_' and '-'",
                    role.role_id
                ));
            }
            if !ids.insert(role.role_id.clone()) {
                return bad(format!("role {} is listed twice", role.role_id));
            }
            if self
                .runtimes
                .iter()
                .all(|r| r.runtime_ref != role.runtime_ref)
            {
                return bad(format!(
                    "role {} uses runtime {} which is not in runtimes",
                    role.role_id, role.runtime_ref
                ));
            }
        }
        // Expanded worker ids (`<role>.<n>` for replicas) must be unique too.
        let mut workers = std::collections::BTreeSet::new();
        for (id, _, _) in self.workers() {
            if !workers.insert(id.clone()) {
                return bad(format!(
                    "worker id {id} is used twice (check role names and counts)"
                ));
            }
        }
        // name -> whether it is a folder input (see [`InputSpec::is_folder`]).
        let mut names = std::collections::BTreeMap::new();
        let mut paths: Vec<PathBuf> = Vec::new();
        for input in &self.inputs {
            let p = safe_relative(&input.local_path)?;
            // Two inputs may not share a file, and one may not sit inside another.
            // Compared without case: on a case-insensitive disk `Model.bin` is `model.bin`.
            let p = PathBuf::from(p.to_string_lossy().to_lowercase());
            if let Some(other) = paths.iter().find(|o| o.starts_with(&p) || p.starts_with(o)) {
                return bad(format!(
                    "input {} uses {} which collides with {}",
                    input.name,
                    p.display(),
                    other.display()
                ));
            }
            paths.push(p);
            if !is_sha256(&input.sha256) {
                return bad(format!("input {} has no valid sha256", input.name));
            }
            if input.url.is_none() == input.file.is_none() {
                return bad(format!(
                    "input {} needs exactly one of url or file",
                    input.name
                ));
            }
            // A folder input lists one input per file, all under `<name>/`.
            let folder = input.is_folder();
            let before = names.insert(input.name.clone(), folder);
            if before.is_some() && !(folder && before == Some(true)) {
                return bad(format!("input {} is listed twice", input.name));
            }
        }
        Ok(())
    }

    /// The primary role.
    pub fn primary(&self) -> Option<&RoleSpec> {
        self.roles.iter().find(|r| r.primary)
    }

    /// The runtime of `role`.
    pub fn runtime(&self, role: &RoleSpec) -> Option<&RuntimeSpec> {
        self.runtimes
            .iter()
            .find(|r| r.runtime_ref == role.runtime_ref)
    }

    /// True for training jobs (`job_kind` TRAIN or missing), which must commit a final checkpoint.
    pub fn trains(&self) -> bool {
        self.job_spec
            .get("job_kind")
            .and_then(|k| k.as_str())
            .is_none_or(|k| k.eq_ignore_ascii_case("TRAIN"))
    }

    /// Every worker process to start: `(role id, role, primary)`.
    pub fn workers(&self) -> Vec<(String, &RoleSpec, bool)> {
        let mut out = Vec::new();
        for role in &self.roles {
            for n in 0..role.count.max(1) {
                let id = if role.count <= 1 {
                    role.role_id.clone()
                } else {
                    format!("{}.{n}", role.role_id)
                };
                out.push((id, role, role.primary && n == 0));
            }
        }
        out
    }

    /// `name -> secret` grants by environment variable.
    pub fn secret_envs(&self) -> BTreeMap<String, String> {
        self.secrets
            .iter()
            .map(|s| (s.name.clone(), s.env_var()))
            .collect()
    }
}

fn input_from_stage(item: &StageItem) -> Result<InputSpec, AttemptError> {
    let expires_at = item
        .expires_at
        .as_ref()
        .and_then(|t| time::OffsetDateTime::from_unix_timestamp(t.seconds).ok())
        .ok_or_else(|| {
            AttemptError::Spec(format!("the link for {} has no expiry time", item.name))
        })?;
    Ok(InputSpec {
        name: item.name.clone(),
        local_path: item.local_path.clone(),
        sha256: item.sha256.clone(),
        size: item.size,
        url: Some(item.url.clone()),
        expires_at: Some(expires_at),
        file: None,
        headers: item.headers.clone().into_iter().collect(),
    })
}

impl InputSpec {
    /// The download ticket, when the input has a link.
    pub fn ticket(&self) -> Option<TransferTicket> {
        let url = self.url.as_ref()?;
        Some(TransferTicket {
            method: TicketMethod::Get,
            key: self.name.clone(),
            max_size: self.size,
            expires_at: self.expires_at?,
            url: url.clone(),
            presigned: url.starts_with("http://") || url.starts_with("https://"),
            headers: self.headers.clone(),
        })
    }
}

/// Letters, digits, `.`, `_`, `-`; not empty, not `.` or `..`.
pub fn safe_name(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.len() <= 128
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Checks that `rel` is a plain relative path that stays inside its folder.
pub fn safe_relative(rel: &str) -> Result<PathBuf, AttemptError> {
    let p = Path::new(rel);
    let ok = !rel.is_empty()
        && p.components().all(|c| matches!(c, Component::Normal(_)))
        && !rel.contains('\\');
    if ok {
        Ok(p.to_path_buf())
    } else {
        Err(AttemptError::UnsafePath {
            path: rel.to_owned(),
        })
    }
}

/// 64 lowercase hex characters.
pub fn is_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::agent::v1::RoleLaunch;
    use mrl_proto::common::v1::AttemptRef;

    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn launch() -> LaunchAttempt {
        LaunchAttempt {
            attempt: Some(AttemptRef {
                job_id: "job_1".into(),
                attempt_id: "att_1".into(),
                fencing_epoch: 3,
            }),
            engine_name: "fake".into(),
            job_spec_json: r#"{"job_kind":"TRAIN"}"#.into(),
            roles: vec![RoleLaunch {
                role_id: "trainer".into(),
                kind: "trainer".into(),
                primary: true,
                count: 1,
                gpu_indices: vec![0],
                runtime_ref: format!("uv:sha256:{SHA}"),
            }],
            lease_duration_s: 30,
            ..LaunchAttempt::default()
        }
    }

    #[test]
    fn builds_from_launch_attempt() {
        let item = StageItem {
            name: "model".into(),
            url: "https://store/obj".into(),
            sha256: SHA.into(),
            size: 4,
            local_path: "model/weights.bin".into(),
            expires_at: Some(prost_types::Timestamp {
                seconds: 2_000_000_000,
                nanos: 0,
            }),
            headers: [("if-match".to_owned(), "\"e1\"".to_owned())].into(),
        };
        let spec = AttemptSpec::from_launch(&launch(), &[item], Path::new("/s")).unwrap();
        assert_eq!(spec.attempt.fencing_epoch, 3);
        assert_eq!(spec.lease_s, 30);
        assert!(spec.trains());
        assert_eq!(
            spec.runtimes[0].project.as_deref(),
            Some(Path::new(&format!("/s/runtimes/uv-sha256-{SHA}")))
        );
        let t = spec.inputs[0].ticket().unwrap();
        assert!(t.presigned && t.method == TicketMethod::Get);
        // A presigned URL's signed headers reach the download.
        assert_eq!(
            t.headers.get("if-match").map(String::as_str),
            Some("\"e1\"")
        );
        assert_eq!(spec.workers()[0].0, "trainer");
    }

    #[test]
    fn rejects_bad_specs() {
        let mut l = launch();
        l.roles[0].runtime_ref = "uv:latest".into();
        assert!(matches!(
            AttemptSpec::from_launch(&l, &[], Path::new("/s")),
            Err(AttemptError::Spec(_))
        ));
        let mut l = launch();
        l.attempt.as_mut().unwrap().attempt_id = "../x".into();
        assert!(AttemptSpec::from_launch(&l, &[], Path::new("/s")).is_err());
        let tag = RuntimeSpec {
            runtime_ref: "oci:ghcr.io/x/y:latest".into(),
            project: None,
            package: None,
        };
        assert!(tag.kind().is_err());
        let pinned = RuntimeSpec {
            runtime_ref: format!("oci:ghcr.io/x/y@sha256:{SHA}"),
            ..tag
        };
        assert!(matches!(pinned.kind(), Ok(RuntimeKind::Container { .. })));
        let sha = "b".repeat(64);
        let input = |name: &str, path: &str| InputSpec {
            name: name.into(),
            local_path: path.into(),
            sha256: sha.clone(),
            size: 1,
            url: None,
            expires_at: None,
            file: Some("/f".into()),
            headers: BTreeMap::new(),
        };
        let mut two = AttemptSpec::from_launch(&launch(), &[], Path::new("/s")).unwrap();
        two.inputs = vec![input("model", "m/w.bin"), input("dataset.train", "m/w.bin")];
        assert!(two.validate().is_err(), "same file");
        two.inputs = vec![input("model", "m"), input("dataset.train", "m/w.bin")];
        assert!(two.validate().is_err(), "one inside the other");
        two.inputs = vec![
            input("model", "m/Model.bin"),
            input("dataset.train", "m/model.bin"),
        ];
        assert!(
            two.validate().is_err(),
            "same file on a case-insensitive disk"
        );
        two.inputs = vec![
            input("model", "m/w.bin"),
            input("dataset.train", "d/t.jsonl"),
        ];
        assert!(two.validate().is_ok());
        // A folder input: one input per file, all under `<name>/`.
        two.inputs = vec![
            input("resume_from", "resume_from/trainer_state.json"),
            input("resume_from", "resume_from/weights.bin"),
        ];
        assert!(two.validate().is_ok(), "folder input");
        assert!(two.inputs.iter().all(InputSpec::is_folder));
        two.inputs = vec![input("model", "m/a.bin"), input("model", "m/b.bin")];
        assert!(two.validate().is_err(), "a file input listed twice");
        two.inputs = vec![
            input("resume_from", "resume_from/a.bin"),
            input("resume_from", "b.bin"),
        ];
        assert!(
            two.validate().is_err(),
            "a folder input with a file outside it"
        );
        for bad in ["", "/etc/passwd", "../x", "a/../../b", "a\\b"] {
            assert!(safe_relative(bad).is_err(), "{bad}");
        }
        assert!(safe_relative("model/a.bin").is_ok());
    }

    #[test]
    fn expanded_worker_ids_must_be_unique() {
        let mut l = launch();
        l.roles[0].role_id = "train.main".into();
        l.roles[0].count = 2;
        let spec = AttemptSpec::from_launch(&l, &[], Path::new("/s")).unwrap();
        let ids: Vec<String> = spec.workers().into_iter().map(|(id, _, _)| id).collect();
        assert_eq!(ids, ["train.main.0", "train.main.1"]);
        // An explicit role named like a generated replica id collides.
        let mut extra = l.roles[0].clone();
        extra.role_id = "train.main.1".into();
        extra.primary = false;
        extra.count = 1;
        l.roles.push(extra);
        assert!(AttemptSpec::from_launch(&l, &[], Path::new("/s")).is_err());
    }

    #[test]
    fn multi_worker_roles_and_secrets() {
        let mut l = launch();
        l.roles[0].count = 2;
        let mut spec = AttemptSpec::from_launch(&l, &[], Path::new("/s")).unwrap();
        let w = spec.workers();
        assert_eq!(
            w.iter()
                .map(|(id, _, p)| (id.as_str(), *p))
                .collect::<Vec<_>>(),
            [("trainer.0", true), ("trainer.1", false)]
        );
        spec.secrets.push(SecretGrant {
            name: "hf-hub".into(),
            env: None,
        });
        assert_eq!(spec.secret_envs()["hf-hub"], "MRL_CREDENTIAL_HF_HUB");
        assert_eq!(format!("{:?}", Secret::new("tok")), "Secret(..)");
    }

    #[test]
    fn core_launches_need_a_lease_and_a_bounded_worker_count() {
        let mut l = launch();
        l.lease_duration_s = 0;
        assert!(
            AttemptSpec::from_launch(&l, &[], Path::new("/s")).is_err(),
            "no lease"
        );
        let mut l = launch();
        l.roles[0].count = u32::MAX;
        let e = AttemptSpec::from_launch(&l, &[], Path::new("/s")).unwrap_err();
        assert!(
            e.explain().contains("at most"),
            "refused before expanding: {}",
            e.explain()
        );
    }
}
