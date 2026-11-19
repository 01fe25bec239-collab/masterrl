//! Worker environments on this node (Doc 20 item 15).
//!
//! A runtime is named by its reference: `uv:sha256:<lock hash>` (process mode, the default) or
//! `oci:<image>@sha256:<digest>` (container mode). The agent materializes it once into
//! `<state>/envs/<runtime>`:
//! - process mode: checks the adapter project's `uv.lock` against the hash, keeps a copy of the
//!   lock in the node cache (pinned by the attempt), then runs
//!   `uv sync --frozen --no-dev --no-editable` into the folder. `--frozen` installs exactly what
//!   the lock lists; nothing is resolved at run time.
//! - container mode: `docker pull <image>@sha256:<digest>`, then checks the local image really
//!   has that digest. Tags are refused.
//!
//! A finished environment has a marker file (`.mrl-env.json`); later attempts reuse it (cache
//! hit). Each attempt pins the environments it uses (`.pins/<attempt>`); [`Envs::evict_unused`]
//! removes unpinned ones, least recently used first, when the disk runs low. Downloads resume
//! through uv's and Docker's own caches; their progress lines are passed to the caller.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;

use crate::attempts::spec::{RuntimeKind, RuntimeSpec};
use crate::attempts::AttemptError;
use crate::state::cache::NodeCache;
use crate::AgentError;

/// Marker file of a finished environment.
pub const MARKER: &str = ".mrl-env.json";
const PINS: &str = ".pins";

/// One progress line while an environment is prepared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnvProgress {
    /// The runtime.
    pub runtime_ref: String,
    /// `checking`, `installing`, `verifying` or `ready`.
    pub stage: &'static str,
    /// A line from uv or Docker, or a plain note.
    pub detail: String,
}

/// Receives progress lines.
pub type ProgressFn = dyn Fn(EnvProgress) + Send + Sync;

/// What the worker runs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerCommand {
    /// The `mrl-worker` program inside a uv environment.
    Process {
        /// Path to `mrl-worker`.
        program: PathBuf,
    },
    /// `mrl-worker` inside a container image.
    Container {
        /// `name@sha256:<digest>`.
        image: String,
    },
}

/// A ready environment.
#[derive(Debug, Clone)]
pub struct Materialized {
    /// The runtime reference.
    pub runtime_ref: String,
    /// The environment folder (`.pins/` and the marker live here).
    pub dir: PathBuf,
    /// How to start the worker.
    pub worker: WorkerCommand,
    /// True when the environment was already there.
    pub cache_hit: bool,
    /// Folders the worker must be able to read (the environment and its Python install).
    pub read_paths: Vec<PathBuf>,
}

/// An installed environment, as the doctor lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledEnv {
    /// The runtime reference.
    pub runtime_ref: String,
    /// Workspace package, if one was chosen.
    #[serde(default)]
    pub package: Option<String>,
    /// When it was last used, Unix seconds.
    pub last_used_unix: i64,
    /// Attempts that pin it now.
    #[serde(skip)]
    pub pinned_by: Vec<String>,
    /// The folder.
    #[serde(skip)]
    pub dir: PathBuf,
    /// The environment is complete (its worker program exists).
    #[serde(skip)]
    pub healthy: bool,
}

/// The environments folder.
pub struct Envs {
    root: PathBuf,
    cache: Arc<NodeCache>,
    locks: std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    free_space: Arc<FreeSpaceFn>,
}

/// Free bytes on the disk holding a path (replaceable in tests).
pub type FreeSpaceFn = dyn Fn(&Path) -> Option<u64> + Send + Sync;

/// Folders never copied into an install snapshot (build output, caches, VCS data).
const SNAPSHOT_SKIP: [&str; 8] = [
    ".git",
    "target",
    "node_modules",
    ".venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
];

impl Envs {
    /// Opens `<state>/envs`.
    pub fn open(state_root: &Path, cache: Arc<NodeCache>) -> Result<Self, AttemptError> {
        let root = state_root.join("envs");
        std::fs::create_dir_all(&root).map_err(|e| AgentError::state(&root, e))?;
        Ok(Self {
            root,
            cache,
            locks: std::sync::Mutex::default(),
            free_space: Arc::new(disk_free),
        })
    }

    /// Uses `f` to read free disk space (tests).
    pub fn with_free_space(mut self, f: Arc<FreeSpaceFn>) -> Self {
        self.free_space = f;
        self
    }

    /// Before an install: evicts unused cache entries and unpinned environments (least
    /// recently used first) until the disk keeps `min_free_bytes` free and environments plus
    /// cache fit the cache budget. Fails with MRL-ENV-004 if pinned ones alone don't allow it.
    fn make_room(&self, installing: &Path) -> Result<(), AttemptError> {
        self.cache.reserve(0)?;
        let budget = self.cache.budget();
        let mut envs: Vec<InstalledEnv> = self
            .installed()
            .into_iter()
            .filter(|e| e.dir != installing)
            .collect();
        loop {
            let free = (self.free_space)(&self.root).unwrap_or(u64::MAX);
            let used =
                self.cache.usage().used_bytes + envs.iter().map(|e| dir_size(&e.dir)).sum::<u64>();
            if free >= budget.min_free_bytes && used <= budget.max_bytes {
                return Ok(());
            }
            let Some(i) = envs.iter().position(|e| e.pinned_by.is_empty()) else {
                return Err(AgentError::CacheFull {
                    needed: 0,
                    pinned: used,
                    budget: budget.max_bytes,
                    min_free: budget.min_free_bytes,
                }
                .into());
            };
            let env = envs.remove(i);
            remove_env(&env)?;
        }
    }

    /// The folder for a runtime.
    pub fn dir_for(&self, rt: &RuntimeSpec) -> PathBuf {
        let mut name = rt.dir_name();
        if let Some(p) = &rt.package {
            name.push('-');
            name.extend(p.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-'));
        }
        self.root.join(name)
    }

    /// Makes the runtime ready and pins it (and its lock file) for `holder`.
    pub async fn materialize(
        &self,
        rt: &RuntimeSpec,
        holder: &str,
        progress: &ProgressFn,
    ) -> Result<Materialized, AttemptError> {
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
            Arc::clone(locks.entry(rt.runtime_ref.clone()).or_default())
        };
        let _one_at_a_time = lock.lock().await;
        let say = |stage, detail: String| {
            progress(EnvProgress {
                runtime_ref: rt.runtime_ref.clone(),
                stage,
                detail,
            })
        };
        let dir = self.dir_for(rt);
        std::fs::create_dir_all(&dir).map_err(|e| AgentError::state(&dir, e))?;
        let m = match rt.kind()? {
            RuntimeKind::Process { lock_sha256 } => {
                self.process(rt, &lock_sha256, &dir, holder, &say).await?
            }
            RuntimeKind::Container { image } => {
                container(rt, &image, &dir, &say, || self.make_room(&dir)).await?
            }
        };
        let pin = dir.join(PINS).join(holder);
        std::fs::create_dir_all(dir.join(PINS))
            .and_then(|()| std::fs::write(&pin, b""))
            .map_err(|e| AgentError::state(&pin, e))?;
        write_marker(&dir, rt)?;
        say(
            "ready",
            if m.cache_hit {
                "already installed".into()
            } else {
                "installed".into()
            },
        );
        Ok(m)
    }

    async fn process(
        &self,
        rt: &RuntimeSpec,
        lock_sha256: &str,
        dir: &Path,
        holder: &str,
        say: &(dyn Fn(&'static str, String) + Send + Sync),
    ) -> Result<Materialized, AttemptError> {
        let project = rt.project.as_deref().ok_or_else(|| {
            AttemptError::Spec(format!("runtime {} has no project", rt.runtime_ref))
        })?;
        let lock_path = project.join("uv.lock");
        say("checking", format!("checking {}", lock_path.display()));
        let install_err = |reason: String| AttemptError::EnvInstall {
            runtime_ref: rt.runtime_ref.clone(),
            reason,
        };
        // Read once: these exact bytes are checked, cached and installed from.
        let lock = tokio::fs::read(&lock_path)
            .await
            .map_err(|e| install_err(format!("the lock file could not be read ({e})")))?;
        let actual = mrl_storage::sha256_hex(&lock);
        if actual != lock_sha256 {
            return Err(AttemptError::LockMismatch {
                runtime_ref: rt.runtime_ref.clone(),
                expected: lock_sha256.to_owned(),
                actual,
            });
        }
        // The verified lock is kept in the node cache, pinned while the attempt runs.
        if self.cache.get(lock_sha256)?.is_none() {
            self.cache.reserve(lock.len() as u64)?;
            let tmp = self
                .cache
                .incoming_dir()
                .join(format!("{lock_sha256}.lock"));
            tokio::fs::write(&tmp, &lock)
                .await
                .map_err(|e| AgentError::state(&tmp, e))?;
            self.cache.insert_file(&tmp, lock_sha256).await?;
        }
        self.cache.pin(lock_sha256, holder)?;

        let program = worker_program(dir);
        let hit = marker_matches(dir, rt) && program.is_file();
        if !hit {
            let uv = which("uv").ok_or_else(|| {
                AttemptError::UvMissing("no `uv` program was found on PATH".into())
            })?;
            let plan = LockPlan::parse(&lock, project).map_err(install_err)?;
            self.make_room(dir)?;
            // Install from an agent-owned copy holding the verified lock, never the live
            // project folder.
            let snap = self.root.join(format!(".src-{}", rt.dir_name()));
            let snap_project =
                snapshot_project(project, &plan.local_dirs, &lock, &snap, &self.root)
                    .map_err(|e| install_err(format!("the project could not be copied ({e})")))?;
            plan.check_build_pins(&snap_project, project)
                .map_err(install_err)?;
            let mut cmd = tokio::process::Command::new(uv);
            cmd.args(["sync", "--frozen", "--no-dev", "--no-editable", "--project"])
                .arg(&snap_project)
                .env("UV_PROJECT_ENVIRONMENT", dir)
                .env_remove("VIRTUAL_ENV");
            if let Some(p) = &rt.package {
                cmd.args(["--package", p]);
            }
            // Packages from an index must come as prebuilt wheels: nothing is built from a
            // source archive with unlocked build tools.
            for name in &plan.remote {
                cmd.args(["--no-build-package", name]);
            }
            let synced = run_streaming(cmd, &rt.runtime_ref, "installing", say).await;
            let _ = std::fs::remove_dir_all(&snap);
            synced?;
            if !program.is_file() {
                return Err(install_err(format!(
                    "the environment has no worker program at {} (is mrl-runtime a dependency?)",
                    program.display()
                )));
            }
        }
        let mut read_paths = vec![dir.to_path_buf()];
        if let Some(home) = python_home(dir) {
            read_paths.push(home);
        }
        Ok(Materialized {
            runtime_ref: rt.runtime_ref.clone(),
            dir: dir.to_path_buf(),
            worker: WorkerCommand::Process { program },
            cache_hit: hit,
            read_paths,
        })
    }

    /// Removes `holder`'s pins (environments and cached lock files). Returns how many.
    pub fn unpin_all(&self, holder: &str) -> Result<usize, AttemptError> {
        let mut n = self.cache.unpin_all(holder)?;
        for env in self.installed() {
            let pin = env.dir.join(PINS).join(holder);
            if pin.exists() {
                std::fs::remove_file(&pin).map_err(|e| AgentError::state(&pin, e))?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Every environment with a marker.
    pub fn installed(&self) -> Vec<InstalledEnv> {
        let Ok(rd) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        let mut out: Vec<InstalledEnv> = rd
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let dir = e.path();
                let mut env: InstalledEnv =
                    serde_json::from_slice(&std::fs::read(dir.join(MARKER)).ok()?).ok()?;
                env.pinned_by = std::fs::read_dir(dir.join(PINS))
                    .map(|rd| {
                        rd.filter_map(|p| p.ok())
                            .map(|p| p.file_name().to_string_lossy().into_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                env.healthy = env.runtime_ref.starts_with("oci:") || worker_program(&dir).is_file();
                env.dir = dir;
                Some(env)
            })
            .collect();
        out.sort_by_key(|e| e.last_used_unix);
        out
    }

    /// Removes unpinned environments, least recently used first, until `min_free_bytes` are
    /// free on the disk (or none are left). Returns the removed runtime references.
    pub fn evict_unused(&self, min_free_bytes: u64) -> Result<Vec<String>, AttemptError> {
        let mut removed = Vec::new();
        for env in self.installed() {
            if (self.free_space)(&self.root).is_none_or(|f| f >= min_free_bytes) {
                break;
            }
            if !env.pinned_by.is_empty() {
                continue;
            }
            remove_env(&env)?;
            removed.push(env.runtime_ref);
        }
        Ok(removed)
    }
}

/// Deletes an environment (and its image, in container mode).
fn remove_env(env: &InstalledEnv) -> Result<(), AttemptError> {
    if let Some(image) = env.runtime_ref.strip_prefix("oci:") {
        if let Some(docker) = which("docker") {
            let _ = std::process::Command::new(docker)
                .args(["image", "rm", image])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
    std::fs::remove_dir_all(&env.dir).map_err(|e| AgentError::state(&env.dir, e))?;
    Ok(())
}

/// Bytes used by the files under `dir` (links not followed).
fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(e.path()),
                Ok(t) if t.is_file() => total += e.metadata().map_or(0, |m| m.len()),
                _ => {}
            }
        }
    }
    total
}

/// What a `uv.lock` asks for.
#[derive(Debug, Default, PartialEq, Eq)]
struct LockPlan {
    /// Packages from an index or a URL (must be prebuilt wheels).
    remote: Vec<String>,
    /// Local packages (workspace members, path sources), as folders.
    local_dirs: Vec<PathBuf>,
}

impl LockPlan {
    /// Reads the packages of a lock file whose project is `project`.
    fn parse(lock: &[u8], project: &Path) -> Result<Self, String> {
        let text = std::str::from_utf8(lock).map_err(|_| "the lock file is not text".to_owned())?;
        let doc: toml::Table =
            toml::from_str(text).map_err(|e| format!("the lock file can't be read ({e})"))?;
        let mut plan = Self::default();
        let packages = doc
            .get("package")
            .and_then(|p| p.as_array())
            .cloned()
            .unwrap_or_default();
        for p in packages {
            let name = p
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_owned();
            let Some(source) = p.get("source").and_then(|s| s.as_table()) else {
                continue;
            };
            if source.contains_key("git") {
                return Err(format!(
                    "{name} comes from a git repository; locked environments need released packages"
                ));
            }
            let local = ["editable", "virtual", "directory", "path"]
                .iter()
                .find_map(|k| source.get(*k).and_then(|v| v.as_str()));
            match local {
                Some(rel) => {
                    let p = project.join(rel);
                    plan.local_dirs.push(if p.is_file() {
                        p.parent().map_or(p.clone(), Path::to_path_buf)
                    } else {
                        p
                    });
                }
                None => plan.remote.push(name),
            }
        }
        Ok(plan)
    }

    /// Local packages are built here, so their build tools must be pinned exactly (`==`).
    fn check_build_pins(&self, snap_project: &Path, project: &Path) -> Result<(), String> {
        for dir in &self.local_dirs {
            let rel = dir.strip_prefix(project).unwrap_or(dir);
            let pyproject = snap_project.join(rel).join("pyproject.toml");
            let Ok(text) = std::fs::read_to_string(&pyproject) else {
                continue;
            };
            let doc: toml::Table = toml::from_str(&text)
                .map_err(|e| format!("{} can't be read ({e})", pyproject.display()))?;
            let requires = doc
                .get("build-system")
                .and_then(|b| b.get("requires"))
                .and_then(|r| r.as_array())
                .cloned()
                .unwrap_or_default();
            for r in requires.iter().filter_map(|r| r.as_str()) {
                if !r.contains("==") {
                    return Err(format!(
                        "the build requirement {r:?} in {} is not pinned with ==",
                        pyproject.display()
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Copies the project and its local packages into `snap` (same relative layout, build output
/// and caches skipped, nothing under `exclude`), writes the verified `lock` as its `uv.lock`,
/// and returns the copied project folder.
fn snapshot_project(
    project: &Path,
    local_dirs: &[PathBuf],
    lock: &[u8],
    snap: &Path,
    exclude: &Path,
) -> std::io::Result<PathBuf> {
    let project = std::fs::canonicalize(project)?;
    let exclude = std::fs::canonicalize(exclude).unwrap_or_else(|_| exclude.to_path_buf());
    let mut dirs = vec![project.clone()];
    for d in local_dirs {
        if let Ok(c) = std::fs::canonicalize(d) {
            dirs.push(c);
        }
    }
    let mut root = project.clone();
    for d in &dirs {
        while !d.starts_with(&root) {
            if !root.pop() {
                break;
            }
        }
    }
    let _ = std::fs::remove_dir_all(snap);
    dirs.sort();
    let mut copied: Vec<PathBuf> = Vec::new();
    for d in dirs {
        if copied.iter().any(|c| d.starts_with(c)) {
            continue;
        }
        let rel = d.strip_prefix(&root).unwrap_or(Path::new(""));
        copy_tree(&d, &snap.join(rel), &exclude)?;
        copied.push(d);
    }
    let snap_project = snap.join(project.strip_prefix(&root).unwrap_or(Path::new("")));
    crate::state::atomic_write(&snap_project.join("uv.lock"), lock)?;
    Ok(snap_project)
}

fn copy_tree(from: &Path, to: &Path, exclude: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let src = e.path();
        let name = e.file_name();
        let t = e.file_type()?;
        if t.is_dir() {
            // The state folder may live inside the project: never copy it into itself.
            if SNAPSHOT_SKIP.iter().any(|s| name == *s) || src.starts_with(exclude) {
                continue;
            }
            copy_tree(&src, &to.join(&name), exclude)?;
        } else if t.is_file() {
            std::fs::copy(&src, to.join(&name))?;
        }
    }
    Ok(())
}

async fn container(
    rt: &RuntimeSpec,
    image: &str,
    dir: &Path,
    say: &(dyn Fn(&'static str, String) + Send + Sync),
    make_room: impl FnOnce() -> Result<(), AttemptError>,
) -> Result<Materialized, AttemptError> {
    let docker = which("docker").ok_or_else(|| {
        AttemptError::DockerMissing("no `docker` program was found on PATH".into())
    })?;
    let digest = image.rsplit_once('@').map_or("", |(_, d)| d);
    let has_digest = |docker: &Path| {
        std::process::Command::new(docker)
            .args([
                "image",
                "inspect",
                "--format",
                "{{json .RepoDigests}}",
                image,
            ])
            .stderr(Stdio::null())
            .output()
            .is_ok_and(|o| {
                o.status.success() && String::from_utf8_lossy(&o.stdout).contains(digest)
            })
    };
    let hit = has_digest(&docker);
    if !hit {
        make_room()?;
        let mut cmd = tokio::process::Command::new(&docker);
        cmd.args(["pull", image]);
        run_streaming(cmd, &rt.runtime_ref, "installing", say).await?;
        say("verifying", format!("checking the image digest {digest}"));
        if !has_digest(&docker) {
            return Err(AttemptError::EnvInstall {
                runtime_ref: rt.runtime_ref.clone(),
                reason: "the pulled image does not have the expected digest".into(),
            });
        }
    }
    Ok(Materialized {
        runtime_ref: rt.runtime_ref.clone(),
        dir: dir.to_path_buf(),
        worker: WorkerCommand::Container {
            image: image.to_owned(),
        },
        cache_hit: hit,
        read_paths: Vec::new(),
    })
}

/// Runs `cmd`, passing each output line to `say`. Fails with the last lines on a non-zero exit.
async fn run_streaming(
    mut cmd: tokio::process::Command,
    runtime_ref: &str,
    stage: &'static str,
    say: &(dyn Fn(&'static str, String) + Send + Sync),
) -> Result<(), AttemptError> {
    let fail = |reason: String| AttemptError::EnvInstall {
        runtime_ref: runtime_ref.to_owned(),
        reason,
    };
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| fail(format!("the installer could not start ({e})")))?;
    let (Some(out), Some(err)) = (child.stdout.take(), child.stderr.take()) else {
        return Err(fail("the installer's output could not be read".into()));
    };
    let mut lines = futures::stream::select(
        tokio_stream::wrappers::LinesStream::new(tokio::io::BufReader::new(out).lines()),
        tokio_stream::wrappers::LinesStream::new(tokio::io::BufReader::new(err).lines()),
    );
    let mut tail: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    while let Some(Ok(line)) = futures::StreamExt::next(&mut lines).await {
        if tail.len() == 8 {
            tail.pop_front();
        }
        tail.push_back(line.clone());
        say(stage, line);
    }
    let status = child
        .wait()
        .await
        .map_err(|e| fail(format!("the installer did not finish ({e})")))?;
    if status.success() {
        Ok(())
    } else {
        Err(fail(format!(
            "the installer stopped with {status}: {}",
            Vec::from(tail).join(" | ")
        )))
    }
}

fn marker_matches(dir: &Path, rt: &RuntimeSpec) -> bool {
    std::fs::read(dir.join(MARKER))
        .ok()
        .and_then(|b| serde_json::from_slice::<InstalledEnv>(&b).ok())
        .is_some_and(|m| m.runtime_ref == rt.runtime_ref && m.package == rt.package)
}

fn write_marker(dir: &Path, rt: &RuntimeSpec) -> Result<(), AttemptError> {
    let m = InstalledEnv {
        runtime_ref: rt.runtime_ref.clone(),
        package: rt.package.clone(),
        last_used_unix: time::OffsetDateTime::now_utc().unix_timestamp(),
        pinned_by: Vec::new(),
        dir: PathBuf::new(),
        healthy: true,
    };
    let path = dir.join(MARKER);
    let json = serde_json::to_vec_pretty(&m).map_err(|e| AttemptError::Spec(e.to_string()))?;
    crate::state::atomic_write(&path, &json).map_err(|e| AgentError::state(&path, e))?;
    Ok(())
}

/// `mrl-worker` inside a uv environment.
pub fn worker_program(env_dir: &Path) -> PathBuf {
    if cfg!(windows) {
        env_dir.join("Scripts").join("mrl-worker.exe")
    } else {
        env_dir.join("bin").join("mrl-worker")
    }
}

/// The Python install a uv environment links to (for sandbox read access).
fn python_home(env_dir: &Path) -> Option<PathBuf> {
    let real = std::fs::canonicalize(env_dir.join("bin").join("python")).ok()?;
    let home = real.parent()?.parent()?.to_path_buf();
    (!home.starts_with(env_dir)).then_some(home)
}

/// Finds `name` on `PATH`.
pub fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).find_map(|d| {
        let names: &[String] = if cfg!(windows) {
            &[format!("{name}.exe"), name.to_owned()]
        } else {
            &[name.to_owned()]
        };
        names.iter().map(|n| d.join(n)).find(|p| p.is_file())
    })
}

/// Free bytes on the disk that holds `path`.
pub fn disk_free(path: &Path) -> Option<u64> {
    crate::hw::cpu::volume_of(path).map(|(_, free)| free)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::cache::CacheBudget;

    fn envs(dir: &Path) -> Envs {
        let cache = NodeCache::open(
            dir.join("cache"),
            CacheBudget {
                max_bytes: 1 << 30,
                min_free_bytes: 0,
            },
        )
        .unwrap();
        Envs::open(dir, Arc::new(cache)).unwrap()
    }

    #[tokio::test]
    async fn tampered_lock_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("proj");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("uv.lock"), b"version = 1\n").unwrap();
        let rt = RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", "0".repeat(64)),
            project: Some(project),
            package: None,
        };
        let e = envs(dir.path())
            .materialize(&rt, "att_1", &|_| {})
            .await
            .unwrap_err();
        assert_eq!(e.code(), "MRL-ENV-201");
        assert_eq!(
            e.failure_class(),
            mrl_proto::common::v1::FailureClass::DependencyEnv
        );
    }

    #[tokio::test]
    async fn pins_markers_and_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let e = envs(dir.path());
        let rt = RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", "1".repeat(64)),
            project: None,
            package: Some("mrl-runtime".into()),
        };
        let d = e.dir_for(&rt);
        assert!(d.ends_with(format!("uv-sha256-{}-mrl-runtime", "1".repeat(64))));
        std::fs::create_dir_all(d.join(PINS)).unwrap();
        write_marker(&d, &rt).unwrap();
        std::fs::write(d.join(PINS).join("att_1"), b"").unwrap();
        assert!(marker_matches(&d, &rt));
        let listed = e.installed();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].pinned_by, ["att_1"]);
        assert!(!listed[0].healthy, "no worker program yet");
        // Pinned: never evicted, even when space is short.
        assert!(e.evict_unused(u64::MAX).unwrap().is_empty());
        assert_eq!(e.unpin_all("att_1").unwrap(), 1);
        assert_eq!(
            e.evict_unused(u64::MAX).unwrap(),
            std::slice::from_ref(&rt.runtime_ref)
        );
        assert!(!d.exists());
    }

    #[test]
    fn lock_plan_and_snapshot_use_the_verified_lock() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let project = repo.join("adapters").join("a");
        let runtime = repo.join("runtime");
        for d in [
            project.clone(),
            runtime.join("src"),
            project.join(".venv"),
            project.join("target"),
        ] {
            std::fs::create_dir_all(d).unwrap();
        }
        std::fs::write(
            project.join("pyproject.toml"),
            "[build-system]\nrequires = [\"uv_build==0.11.28\"]\n",
        )
        .unwrap();
        std::fs::write(
            runtime.join("pyproject.toml"),
            "[build-system]\nrequires = [\"uv_build>=0.11\"]\n",
        )
        .unwrap();
        std::fs::write(runtime.join("src").join("m.py"), "x = 1\n").unwrap();
        std::fs::write(project.join("uv.lock"), "changed after the check").unwrap();
        let lock = br#"version = 1
[[package]]
name = "a"
source = { editable = "." }
[[package]]
name = "runtime"
source = { editable = "../../runtime" }
[[package]]
name = "torch"
source = { registry = "https://pypi.org/simple" }
"#;
        let plan = LockPlan::parse(lock, &project).unwrap();
        assert_eq!(plan.remote, ["torch"]);
        assert_eq!(plan.local_dirs.len(), 2);
        let snap = dir.path().join("snap");
        let sp = snapshot_project(
            &project,
            &plan.local_dirs,
            lock,
            &snap,
            &dir.path().join("state"),
        )
        .unwrap();
        assert_eq!(
            std::fs::read(sp.join("uv.lock")).unwrap(),
            lock,
            "the verified bytes"
        );
        assert!(
            snap.join("runtime").join("src").join("m.py").is_file(),
            "layout kept"
        );
        assert!(!sp.join(".venv").exists() && !sp.join("target").exists());
        // The runtime's build tool is a range, not a pin.
        let e = plan.check_build_pins(&sp, &project).unwrap_err();
        assert!(e.contains("uv_build>=0.11"), "{e}");
        let git = b"[[package]]\nname = \"g\"\nsource = { git = \"https://x\" }\n";
        assert!(LockPlan::parse(git, &project).is_err());
    }

    #[tokio::test]
    async fn a_low_disk_evicts_unused_environments_then_refuses() {
        if which("uv").is_none() {
            return; // the uv check comes before the space check
        }
        let dir = tempfile::tempdir().unwrap();
        let cache = NodeCache::open(
            dir.path().join("cache"),
            CacheBudget {
                max_bytes: 1 << 30,
                min_free_bytes: 1 << 20,
            },
        )
        .unwrap();
        let e = Envs::open(dir.path(), Arc::new(cache))
            .unwrap()
            .with_free_space(Arc::new(|_| Some(0)));
        let mk = |n: &str, pinned: bool| {
            let rt = RuntimeSpec {
                runtime_ref: format!("uv:sha256:{}", n.repeat(64)),
                project: None,
                package: None,
            };
            let d = e.dir_for(&rt);
            std::fs::create_dir_all(d.join(PINS)).unwrap();
            write_marker(&d, &rt).unwrap();
            if pinned {
                std::fs::write(d.join(PINS).join("att_live"), b"").unwrap();
            }
            d
        };
        let (old, kept) = (mk("2", false), mk("3", true));
        let project = dir.path().join("proj");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("uv.lock"), b"version = 1\n").unwrap();
        let rt = RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", mrl_storage::sha256_hex(b"version = 1\n")),
            project: Some(project),
            package: None,
        };
        let err = e.materialize(&rt, "att_new", &|_| {}).await.unwrap_err();
        assert_eq!(err.code(), "MRL-ENV-004", "{}", err.explain());
        assert!(!old.exists(), "the unpinned environment was evicted first");
        assert!(kept.exists(), "a pinned one never is");
    }

    #[test]
    fn finds_programs_on_path() {
        assert!(which("definitely-not-a-program-mrl").is_none());
        assert!(disk_free(Path::new(".")).is_some());
    }
}
