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
    free_space: Arc<FreeSpaceFn>,
    /// The state folder's writer lock (see [`lock_state`]); `None` for a read-only view.
    _writer: Option<Arc<std::fs::File>>,
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
    /// Opens `<state>/envs` to install, pin and evict. Takes the state folder's writer lock
    /// ([`lock_state`]): one agent process per state folder.
    pub fn open(state_root: &Path, cache: Arc<NodeCache>) -> Result<Self, AttemptError> {
        let writer = lock_state(state_root)?;
        let root = state_root.join("envs");
        std::fs::create_dir_all(&root).map_err(|e| AgentError::state(&root, e))?;
        reclaim_snapshots(&root);
        Ok(Self {
            root,
            cache,
            free_space: Arc::new(disk_free),
            _writer: Some(writer),
        })
    }

    /// A view for listing only (`mrl-agent doctor` next to a running agent): no lock taken,
    /// nothing reclaimed. Never install or evict through it.
    pub fn open_read_only(state_root: &Path, cache: Arc<NodeCache>) -> Self {
        Self {
            root: state_root.join("envs"),
            cache,
            free_space: Arc::new(disk_free),
            _writer: None,
        }
    }

    /// Uses `f` to read free disk space (tests).
    pub fn with_free_space(mut self, f: Arc<FreeSpaceFn>) -> Self {
        self.free_space = f;
        self
    }

    /// Before writing about `incoming` bytes (a lock file and/or an install): one space
    /// decision for the node cache, the environments (images included) and the new data, all
    /// against the same free-space reading. Unpinned environments go first (least recently
    /// used), then unpinned cache entries, until the disk keeps `min_free_bytes` free after the
    /// write and everything fits the cache budget. Fails with MRL-ENV-004 when only pinned data
    /// is left.
    fn make_room(&self, installing: &Path, incoming: u64) -> Result<(), AttemptError> {
        let budget = self.cache.budget();
        let mut envs = sized(
            self.installed()
                .into_iter()
                .filter(|e| e.dir != installing)
                .collect(),
        );
        loop {
            let free = (self.free_space)(&self.root).unwrap_or(u64::MAX);
            let cache_used = self.cache.usage().used_bytes;
            let used = cache_used + envs.iter().map(|(_, b)| b).sum::<u64>();
            let short = (budget.min_free_bytes + incoming)
                .saturating_sub(free)
                .max((used + incoming).saturating_sub(budget.max_bytes));
            if short == 0 {
                return Ok(());
            }
            if let Some(i) = envs.iter().position(|(e, _)| e.pinned_by.is_empty()) {
                let (mut env, bytes) = envs.remove(i);
                if !remove_env(&env)? {
                    env.pinned_by.push(String::new()); // in use now: counts as pinned
                    envs.push((env, bytes));
                }
                continue;
            }
            // No environment left to remove: free cache entries instead.
            if self.shrink_cache(cache_used.saturating_sub(short)) == 0 {
                return Err(AgentError::CacheFull {
                    needed: incoming,
                    pinned: used,
                    budget: budget.max_bytes,
                    min_free: budget.min_free_bytes,
                }
                .into());
            }
        }
    }

    /// Shrinks environments (images included) plus cache to `limit` bytes (a `Cleanup`
    /// command), removing unpinned environments first, then unpinned cache entries. Only
    /// deletes: a low disk never makes it fail. Pinned data stays.
    pub fn prune_to(&self, limit: u64) -> Result<(), AttemptError> {
        let mut envs = sized(self.installed());
        loop {
            let env_bytes: u64 = envs.iter().map(|(_, b)| b).sum();
            if self.cache.usage().used_bytes + env_bytes <= limit {
                return Ok(());
            }
            if let Some(i) = envs.iter().position(|(e, _)| e.pinned_by.is_empty()) {
                let (mut env, bytes) = envs.remove(i);
                if !remove_env(&env)? {
                    env.pinned_by.push(String::new()); // in use now: counts as pinned
                    envs.push((env, bytes));
                }
                continue;
            }
            self.shrink_cache(limit.saturating_sub(env_bytes));
            return Ok(());
        }
    }

    /// Evicts unpinned cache entries, least recently used first, toward `target` bytes in the
    /// cache. Returns how many were evicted.
    fn shrink_cache(&self, target: u64) -> usize {
        // ponytail: the node cache only offers `reserve(n)` ("make n more bytes fit"), which
        // also applies the disk floor and evicts nothing when it can't meet both. The largest
        // n it accepts frees the most toward the target; bisection finds it in at most 64
        // tries. Ceiling: when all unpinned entries together can't restore the disk floor,
        // every n fails and nothing is evicted. A byte-target eviction on `NodeCache`
        // (state/cache.rs, not this part's file) would replace this.
        let want = self.cache.budget().max_bytes.saturating_sub(target);
        let mut evicted = 0;
        let (mut lo, mut hi) = (0u64, want);
        let mut n = want;
        loop {
            match self.cache.reserve(n) {
                Ok(e) => {
                    evicted += e.len();
                    lo = n.saturating_add(1);
                    if n == u64::MAX {
                        return evicted;
                    }
                }
                Err(_) if n == 0 => return evicted,
                Err(_) => hi = n - 1,
            }
            if lo > hi {
                return evicted;
            }
            n = lo + (hi - lo) / 2;
        }
    }

    /// The install snapshot for the environment in `env_dir`.
    fn snapshot_dir(&self, env_dir: &Path) -> PathBuf {
        let name = env_dir.file_name().unwrap_or_default().to_string_lossy();
        self.root.join(format!(".src-{name}-{}", snapshot_owner()))
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
        let dir = self.dir_for(rt);
        // Held until the pin is written, so eviction (any `Envs` of this agent) never removes
        // it in between.
        let lock = env_lock(&dir);
        let _one_at_a_time = lock.lock().await;
        let say = |stage, detail: String| {
            progress(EnvProgress {
                runtime_ref: rt.runtime_ref.clone(),
                stage,
                detail,
            })
        };
        std::fs::create_dir_all(&dir).map_err(|e| AgentError::state(&dir, e))?;
        let m = match rt.kind()? {
            RuntimeKind::Process { lock_sha256 } => {
                self.process(rt, &lock_sha256, &dir, holder, &say).await?
            }
            RuntimeKind::Container { image } => {
                container(rt, &image, &dir, &say, |bytes| self.make_room(&dir, bytes)).await?
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
        let program = worker_program(dir);
        let hit = marker_matches(dir, rt) && program.is_file();
        let install = if hit {
            None
        } else {
            let uv = which("uv").ok_or_else(|| {
                AttemptError::UvMissing("no `uv` program was found on PATH".into())
            })?;
            Some((uv, LockPlan::parse(&lock, project).map_err(install_err)?))
        };
        // One space decision for everything written next (the lock, when the cache lacks it,
        // and the install), before any of it is written.
        let lock_cached = self.cache.get(lock_sha256)?.is_some();
        if install.is_some() || !lock_cached {
            let incoming = if lock_cached { 0 } else { lock.len() as u64 }
                + install.as_ref().map_or(0, |(_, plan)| plan.install_bytes);
            self.make_room(dir, incoming)?;
        }
        // The verified lock is kept in the node cache, pinned while the attempt runs.
        if !lock_cached {
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

        if let Some((uv, plan)) = install {
            // Install from an agent-owned copy holding the verified lock, never the live
            // project folder. The copy is removed however the install ends (error, cancel);
            // one left by a dead agent is reclaimed by `Envs::open`.
            // Named after the environment folder (runtime and package), so two packages of
            // one lock installing at once never share a copy.
            let snap = ScopedDir(self.snapshot_dir(dir));
            let dirs: Vec<PathBuf> = plan.local.iter().map(|p| p.dir.clone()).collect();
            let copy = snapshot_project(project, &dirs, &lock, &snap.0, &self.root)
                .map_err(|e| install_err(format!("the project could not be copied ({e})")))?;
            let uv_version = uv_version(&uv).await.map_err(install_err)?;
            plan.check_build_backends(&copy, &uv_version)
                .map_err(install_err)?;
            let snap_project = copy.project.clone();
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
            drop(snap);
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
            if remove_env(&env)? {
                removed.push(env.runtime_ref);
            }
        }
        Ok(removed)
    }
}

/// The writer lock of a state folder (`<state>/agent.lock`, exclusive), shared by everything in
/// this process that uses the folder and released when the last user drops it. Environments,
/// pins and the node cache index are only safe with one writing process, so a second process
/// (an `attempt run` next to the agent, or a second agent) is refused.
pub fn lock_state(state_root: &Path) -> Result<Arc<std::fs::File>, AttemptError> {
    static HELD: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<PathBuf, std::sync::Weak<std::fs::File>>>,
    > = std::sync::LazyLock::new(Default::default);
    let mut held = HELD.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(f) = held.get(state_root).and_then(std::sync::Weak::upgrade) {
        return Ok(f);
    }
    std::fs::create_dir_all(state_root).map_err(|e| AgentError::state(state_root, e))?;
    let path = state_root.join("agent.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| AgentError::state(&path, e))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(AgentError::state(
                &path,
                std::io::Error::other(
                    "another mrl-agent is using this state folder; stop it first or use another state_dir",
                ),
            )
            .into());
        }
        Err(std::fs::TryLockError::Error(e)) => return Err(AgentError::state(&path, e).into()),
    }
    let file = Arc::new(file);
    held.insert(state_root.to_path_buf(), Arc::downgrade(&file));
    Ok(file)
}

/// One lock per environment folder for the whole agent process: materializing (up to the pin)
/// and removing hold it.
fn env_lock(dir: &Path) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
    > = std::sync::LazyLock::new(Default::default);
    let mut locks = LOCKS.lock().unwrap_or_else(|p| p.into_inner());
    Arc::clone(locks.entry(dir.to_path_buf()).or_default())
}

/// Deletes an environment (and its image, in container mode) unless it is being prepared or
/// has gained a pin since `env` was read: both are checked under its lock. True if removed.
fn remove_env(env: &InstalledEnv) -> Result<bool, AttemptError> {
    let lock = env_lock(&env.dir);
    let Ok(_held) = lock.try_lock() else {
        return Ok(false); // being prepared or pinned right now
    };
    let pinned = std::fs::read_dir(env.dir.join(PINS)).is_ok_and(|mut rd| rd.next().is_some());
    if pinned {
        return Ok(false);
    }
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
    Ok(true)
}

/// Environments with the disk bytes each holds.
fn sized(envs: Vec<InstalledEnv>) -> Vec<(InstalledEnv, u64)> {
    envs.into_iter()
        .map(|e| {
            let b = env_bytes(&e);
            (e, b)
        })
        .collect()
}

/// Disk bytes an environment holds: its folder, plus its image in container mode.
fn env_bytes(env: &InstalledEnv) -> u64 {
    let image = env.runtime_ref.strip_prefix("oci:").and_then(|image| {
        let out = std::process::Command::new(which("docker")?)
            .args(["image", "inspect", "--format", "{{.Size}}", image])
            .stderr(Stdio::null())
            .output()
            .ok()?;
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<u64>()
            .ok()
    });
    dir_size(&env.dir) + image.unwrap_or(0)
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

/// A package from the source tree.
#[derive(Debug, PartialEq, Eq)]
struct LocalPackage {
    /// Its folder.
    dir: PathBuf,
    /// A virtual project (`source = { virtual = ... }`): uv installs only its dependencies and
    /// never builds it.
    is_virtual: bool,
}

/// What a `uv.lock` asks for.
#[derive(Debug, Default, PartialEq, Eq)]
struct LockPlan {
    /// Packages from an index or a URL (must be prebuilt wheels).
    remote: Vec<String>,
    /// Local packages (workspace members, path sources).
    local: Vec<LocalPackage>,
    /// Estimated installed size: three times the largest wheel of each package.
    install_bytes: u64,
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
                .find_map(|k| source.get(*k).and_then(|v| v.as_str()).map(|v| (*k, v)));
            match local {
                Some((kind, rel)) => {
                    let p = project.join(rel);
                    plan.local.push(LocalPackage {
                        dir: if p.is_file() {
                            p.parent().map_or(p.clone(), Path::to_path_buf)
                        } else {
                            p
                        },
                        is_virtual: kind == "virtual",
                    });
                }
                None => {
                    let wheel = p
                        .get("wheels")
                        .and_then(|w| w.as_array())
                        .into_iter()
                        .flatten()
                        .filter_map(|w| w.get("size").and_then(|s| s.as_integer()))
                        .max()
                        .unwrap_or(0);
                    plan.install_bytes += u64::try_from(wheel).unwrap_or(0) * 3;
                    plan.remote.push(name);
                }
            }
        }
        Ok(plan)
    }

    /// Local packages are built here (all but virtual projects). Their only build backend
    /// allowed is `uv_build` pinned to exactly the installed uv's version
    /// (`uv_build==<uv_version>`): that is the one version uv builds natively, with nothing
    /// resolved. Any other version would be fetched and resolved at install time, outside the
    /// lock. Anything else (no `pyproject.toml` or `[build-system]`, which means uv's legacy
    /// setuptools build; another backend; extra build requirements; a range or wildcard) is
    /// refused.
    fn check_build_backends(&self, copy: &Snapshot, uv_version: &str) -> Result<(), String> {
        for pkg in self.local.iter().filter(|p| !p.is_virtual) {
            let pyproject = copy.copy_of(&pkg.dir).join("pyproject.toml");
            let legacy = || {
                format!(
                    "{} has no [build-system]; it must build with uv_build pinned to one exact version (uv_build==X.Y.Z)",
                    pyproject.display()
                )
            };
            let text = std::fs::read_to_string(&pyproject).map_err(|_| legacy())?;
            let doc: toml::Table = toml::from_str(&text)
                .map_err(|e| format!("{} can't be read ({e})", pyproject.display()))?;
            let build = doc.get("build-system").ok_or_else(legacy)?;
            let backend = build.get("build-backend").and_then(|b| b.as_str());
            let requires: Vec<&str> = build
                .get("requires")
                .and_then(|r| r.as_array())
                .into_iter()
                .flatten()
                .filter_map(|r| r.as_str())
                .collect();
            let wanted = format!("uv_build=={uv_version}");
            if backend != Some("uv_build") || requires != [wanted.as_str()] {
                return Err(format!(
                    "{} must build with {wanted} (the uv installed here builds only that version itself), not {requires:?}",
                    pyproject.display()
                ));
            }
        }
        Ok(())
    }
}

/// The installed uv's version (`uv --version` → `uv 0.11.28 (...)`).
async fn uv_version(uv: &Path) -> Result<String, String> {
    let out = tokio::process::Command::new(uv)
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| format!("uv could not start ({e})"))?;
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .nth(1)
        .filter(|v| out.status.success() && v.bytes().all(|b| b.is_ascii_digit() || b == b'.'))
        .map(str::to_owned)
        .ok_or_else(|| "uv did not report its version".to_owned())
}

/// A folder removed when this value is dropped.
struct ScopedDir(PathBuf);

impl Drop for ScopedDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `<pid>-<start time>` of this agent, naming its install snapshots.
fn snapshot_owner() -> String {
    let pid = std::process::id();
    let start = crate::attempts::record::process_start(pid).unwrap_or(0);
    format!("{pid}-{start}")
}

/// Removes install snapshots (`.src-<name>-<pid>-<start>`) whose agent is no longer running.
fn reclaim_snapshots(root: &Path) {
    let Ok(rd) = std::fs::read_dir(root) else {
        return;
    };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !name.starts_with(".src-") {
            continue;
        }
        let mut parts = name.rsplitn(3, '-');
        let start = parts.next().and_then(|s| s.parse::<u64>().ok());
        let pid = parts.next().and_then(|p| p.parse::<u32>().ok());
        let alive = match (pid, start) {
            (Some(pid), Some(start_time)) => {
                crate::attempts::record::ProcIdentity::Pid { pid, start_time }.alive()
            }
            _ => false,
        };
        if !alive {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

/// An install snapshot: where the source tree's folders were copied.
struct Snapshot {
    /// The copy of the project folder.
    project: PathBuf,
    /// Common ancestor of the project and its local packages (in the source tree).
    src_root: PathBuf,
    /// Where `src_root` was copied.
    copy_root: PathBuf,
}

impl Snapshot {
    /// The copy of a source folder.
    fn copy_of(&self, dir: &Path) -> PathBuf {
        let d = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        self.copy_root
            .join(d.strip_prefix(&self.src_root).unwrap_or(Path::new("")))
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
) -> std::io::Result<Snapshot> {
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
    Ok(Snapshot {
        project: snap_project,
        src_root: root,
        copy_root: snap.to_path_buf(),
    })
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
    make_room: impl FnOnce(u64) -> Result<(), AttemptError>,
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
        make_room(image_bytes(&docker, image))?;
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

/// The image's compressed layer sizes (`docker manifest inspect -v`), times three for the
/// unpacked image; 0 when the registry can't be asked.
fn image_bytes(docker: &Path, image: &str) -> u64 {
    let Ok(out) = std::process::Command::new(docker)
        .args(["manifest", "inspect", "-v", image])
        .stderr(Stdio::null())
        .output()
    else {
        return 0;
    };
    fn sum(v: &serde_json::Value) -> u64 {
        match v {
            serde_json::Value::Object(m) => {
                let here = m.get("layers").and_then(|l| l.as_array()).map_or(0, |l| {
                    l.iter().filter_map(|x| x.get("size")?.as_u64()).sum()
                });
                here + m
                    .iter()
                    .filter(|(k, _)| *k != "layers")
                    .map(|(_, v)| sum(v))
                    .sum::<u64>()
            }
            serde_json::Value::Array(a) => a.iter().map(sum).sum(),
            _ => 0,
        }
    }
    serde_json::from_slice::<serde_json::Value>(&out.stdout).map_or(0, |v| sum(&v) * 3)
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
            "[build-system]\nrequires = [\"uv_build==0.11.28\"]\nbuild-backend = \"uv_build\"\n",
        )
        .unwrap();
        std::fs::write(
            runtime.join("pyproject.toml"),
            "[build-system]\nrequires = [\"uv_build>=0.11\"]\nbuild-backend = \"uv_build\"\n",
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
        assert_eq!(plan.local.len(), 2);
        let dirs: Vec<PathBuf> = plan.local.iter().map(|p| p.dir.clone()).collect();
        let snap = dir.path().join("snap");
        let sp = snapshot_project(&project, &dirs, lock, &snap, &dir.path().join("state")).unwrap();
        assert_eq!(
            std::fs::read(sp.project.join("uv.lock")).unwrap(),
            lock,
            "the verified bytes"
        );
        assert!(
            snap.join("runtime").join("src").join("m.py").is_file(),
            "layout kept"
        );
        assert!(!sp.project.join(".venv").exists() && !sp.project.join("target").exists());
        // The runtime's build tool is a range, not a pin.
        // The runtime outside the project is checked in its copy: a range is refused...
        let e = plan.check_build_backends(&sp, "0.11.28").unwrap_err();
        assert!(e.contains("uv_build>=0.11"), "{e}");
        let runtime_copy = sp.copy_of(&runtime).join("pyproject.toml");
        let set = |text: &str| std::fs::write(&runtime_copy, text).unwrap();
        // ...as are a wildcard and a backend with build dependencies of its own...
        set("[build-system]\nrequires = [\"uv_build==0.11.*\"]\nbuild-backend = \"uv_build\"\n");
        assert!(
            plan.check_build_backends(&sp, "0.11.28").is_err(),
            "wildcard"
        );
        set("[build-system]\nrequires = [\"hatchling==1.27.0\"]\nbuild-backend = \"hatchling.build\"\n");
        assert!(
            plan.check_build_backends(&sp, "0.11.28").is_err(),
            "other backend"
        );
        set("[build-system]\nrequires = [\"uv_build==0.11.28\", \"setuptools==80.0.0\"]\nbuild-backend = \"uv_build\"\n");
        assert!(
            plan.check_build_backends(&sp, "0.11.28").is_err(),
            "extra build requirement"
        );
        // ...a legacy package (no [build-system], or no pyproject.toml at all)...
        set("[project]\nname = \"runtime\"\nversion = \"1\"\n");
        let e = plan.check_build_backends(&sp, "0.11.28").unwrap_err();
        assert!(e.contains("no [build-system]"), "{e}");
        std::fs::remove_file(&runtime_copy).unwrap();
        assert!(
            plan.check_build_backends(&sp, "0.11.28").is_err(),
            "no pyproject.toml"
        );
        // ...an exact pin of another uv_build version (uv would resolve it)...
        set("[build-system]\nrequires = [\"uv_build==0.10.0\"]\nbuild-backend = \"uv_build\"\n");
        let e = plan.check_build_backends(&sp, "0.11.28").unwrap_err();
        assert!(e.contains("uv_build==0.11.28"), "{e}");
        // ...and the installed uv's own version passes.
        set("[build-system]\nrequires = [\"uv_build==0.11.28\"]\nbuild-backend = \"uv_build\"\n");
        assert!(plan.check_build_backends(&sp, "0.11.28").is_ok());
        // Only a virtual project may have no build system: uv never builds it.
        let virt = br#"version = 1
[[package]]
name = "a"
source = { virtual = "." }
"#;
        let vplan = LockPlan::parse(virt, &project).unwrap();
        std::fs::write(
            sp.project.join("pyproject.toml"),
            "[project]\nname = \"a\"\nversion = \"1\"\n",
        )
        .unwrap();
        assert!(vplan.check_build_backends(&sp, "0.11.28").is_ok());
        let git = b"[[package]]\nname = \"g\"\nsource = { git = \"https://x\" }\n";
        assert!(LockPlan::parse(git, &project).is_err());
    }

    #[tokio::test]
    async fn one_space_decision_evicts_unused_environments_first() {
        if which("uv").is_none() {
            return; // the uv check comes before the space check
        }
        // One disk for everything: the node cache and the environments both see it full
        // while the unused environment exists.
        static OLD: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);
        fn disk(_: &Path) -> Option<u64> {
            let full = OLD
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|p: &PathBuf| p.exists());
            Some(if full { 0 } else { 100 << 30 })
        }
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            NodeCache::open_with_free_space(
                dir.path().join("cache"),
                CacheBudget {
                    max_bytes: 1 << 30,
                    min_free_bytes: 1 << 20,
                },
                disk,
            )
            .unwrap(),
        );
        let envs_root = dir.path().join("envs");
        let e = Envs::open(dir.path(), Arc::clone(&cache))
            .unwrap()
            .with_free_space(Arc::new(disk));
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
        *OLD.lock().unwrap() = Some(old.clone());
        let project = dir.path().join("proj");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(project.join("uv.lock"), b"version = 1\n").unwrap();
        let rt = RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", mrl_storage::sha256_hex(b"version = 1\n")),
            project: Some(project),
            package: None,
        };
        // Space is found (the next step, uv on an empty project, is what fails here).
        let err = e.materialize(&rt, "att_new", &|_| {}).await.unwrap_err();
        assert_ne!(err.code(), "MRL-ENV-004", "{}", err.explain());
        assert!(!old.exists(), "the unused environment made room");
        assert!(kept.exists(), "the pinned one survived");
        let lock_sha = mrl_storage::sha256_hex(b"version = 1\n");
        assert!(
            cache.get(&lock_sha).unwrap().is_some(),
            "the lock went into the cache after the room was made"
        );
        // Install copies never outlive the install.
        let left = std::fs::read_dir(&envs_root)
            .unwrap()
            .flatten()
            .filter(|d| d.file_name().to_string_lossy().starts_with(".src-"))
            .count();
        assert_eq!(left, 0);

        // Only pinned data left and still no room: refused.
        let e = e.with_free_space(Arc::new(|_| Some(0)));
        let err = e.materialize(&rt, "att_new", &|_| {}).await.unwrap_err();
        assert_eq!(err.code(), "MRL-ENV-004", "{}", err.explain());
        assert!(kept.exists());
    }

    /// Cold cache: a local package pinned to a uv_build version the installed uv doesn't
    /// build itself is refused before uv runs (uv would resolve that version from an index).
    #[tokio::test]
    async fn a_cold_install_refuses_another_uv_build_version() {
        if which("uv").is_none() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("proj");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::write(
            project.join("pyproject.toml"),
            "[project]\nname = \"p\"\nversion = \"1\"\n[build-system]\nrequires = [\"uv_build==0.10.0\"]\nbuild-backend = \"uv_build\"\n",
        )
        .unwrap();
        let lock = b"version = 1\n[[package]]\nname = \"p\"\nsource = { editable = \".\" }\n";
        std::fs::write(project.join("uv.lock"), lock).unwrap();
        let rt = RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", mrl_storage::sha256_hex(lock)),
            project: Some(project),
            package: None,
        };
        let stages: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::default();
        let seen = Arc::clone(&stages);
        let e = envs(dir.path())
            .materialize(&rt, "att_v", &move |p| seen.lock().unwrap().push(p.stage))
            .await
            .unwrap_err();
        let lines = stages.lock().unwrap().clone();
        assert_eq!(e.code(), "MRL-ENV-202", "{}", e.explain());
        assert!(
            e.explain().contains("builds only that version"),
            "{}",
            e.explain()
        );
        assert!(!lines.contains(&"installing"), "uv sync never ran");
    }

    #[tokio::test]
    async fn a_cold_install_refuses_a_legacy_local_package() {
        if which("uv").is_none() {
            return; // the uv check comes first
        }
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("proj");
        let legacy = project.join("legacy");
        std::fs::create_dir_all(&legacy).unwrap();
        // setup.py only: uv would build it with its default setuptools backend.
        std::fs::write(
            legacy.join("setup.py"),
            "from setuptools import setup\nsetup()\n",
        )
        .unwrap();
        let lock =
            b"version = 1\n[[package]]\nname = \"legacy\"\nsource = { editable = \"legacy\" }\n";
        std::fs::write(project.join("uv.lock"), lock).unwrap();
        let rt = RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", mrl_storage::sha256_hex(lock)),
            project: Some(project),
            package: None,
        };
        let e = envs(dir.path())
            .materialize(&rt, "att_l", &|_| {})
            .await
            .unwrap_err();
        assert_eq!(e.code(), "MRL-ENV-202", "{}", e.explain());
        assert!(e.explain().contains("no [build-system]"), "{}", e.explain());
    }

    #[tokio::test]
    async fn pruning_a_low_disk_deletes_unpinned_data_and_keeps_pinned() {
        // The disk is below its floor the whole time pruning runs (by less than the unpinned
        // data, which is what `NodeCache::reserve` can act on).
        static LOW: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        fn disk(_: &Path) -> Option<u64> {
            Some(if LOW.load(std::sync::atomic::Ordering::SeqCst) {
                0
            } else {
                100 << 30
            })
        }
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            NodeCache::open_with_free_space(
                dir.path().join("cache"),
                CacheBudget {
                    max_bytes: 1 << 30,
                    min_free_bytes: 4,
                },
                disk,
            )
            .unwrap(),
        );
        let mut shas = Vec::new();
        for (i, body) in [b"old entry".as_slice(), b"pinned entry"]
            .iter()
            .enumerate()
        {
            let tmp = cache.incoming_dir().join(format!("e{i}"));
            std::fs::write(&tmp, body).unwrap();
            let sha = mrl_storage::sha256_hex(body);
            cache.insert_file(&tmp, &sha).await.unwrap();
            shas.push(sha);
        }
        cache.pin(&shas[1], "att_live").unwrap();
        let e = Envs::open(dir.path(), Arc::clone(&cache))
            .unwrap()
            .with_free_space(Arc::new(disk));
        let mk = |n: &str, pinned: bool| {
            let rt = RuntimeSpec {
                runtime_ref: format!("uv:sha256:{}", n.repeat(64)),
                project: None,
                package: None,
            };
            let d = e.dir_for(&rt);
            std::fs::create_dir_all(d.join(PINS)).unwrap();
            write_marker(&d, &rt).unwrap();
            std::fs::write(d.join("payload"), vec![0u8; 4096]).unwrap();
            if pinned {
                std::fs::write(d.join(PINS).join("att_live"), b"").unwrap();
            }
            d
        };
        let (old, kept) = (mk("4", false), mk("5", true));
        LOW.store(true, std::sync::atomic::Ordering::SeqCst);
        e.prune_to(0).unwrap();
        assert!(!old.exists() && kept.exists());
        assert!(
            cache.get(&shas[0]).unwrap().is_none(),
            "unpinned entry evicted"
        );
        assert!(cache.get(&shas[1]).unwrap().is_some(), "pinned entry kept");
    }

    /// Eviction from another `Envs` (the janitor's) never removes an environment that is being
    /// prepared or that was pinned after the scan.
    #[tokio::test]
    async fn eviction_respects_a_racing_cache_hit() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (envs(dir.path()), envs(dir.path()));
        let rt = RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", "6".repeat(64)),
            project: None,
            package: None,
        };
        let d = a.dir_for(&rt);
        std::fs::create_dir_all(d.join(PINS)).unwrap();
        write_marker(&d, &rt).unwrap();
        // `a` is materializing it (holds the folder's lock): `b` leaves it.
        let lock = env_lock(&d);
        let held = lock.lock().await;
        assert!(b.evict_unused(u64::MAX).unwrap().is_empty());
        assert!(d.exists());
        // Pinned after `b` read the list: the recheck under the lock keeps it.
        let stale = b.installed().remove(0);
        std::fs::write(d.join(PINS).join("att_hit"), b"").unwrap();
        drop(held);
        assert!(!remove_env(&stale).unwrap());
        assert!(d.exists());
    }

    #[test]
    fn packages_of_one_lock_get_their_own_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let e = envs(dir.path());
        let rt = |p: &str| RuntimeSpec {
            runtime_ref: format!("uv:sha256:{}", "7".repeat(64)),
            project: None,
            package: Some(p.into()),
        };
        let (a, b) = (e.dir_for(&rt("mrl-a")), e.dir_for(&rt("mrl-b")));
        assert_ne!(e.snapshot_dir(&a), e.snapshot_dir(&b));
        assert!(e
            .snapshot_dir(&a)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".src-"));
    }

    #[test]
    fn snapshots_of_dead_agents_are_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("envs");
        let dead = root.join(".src-uv-x-4294967294-1");
        let mine = root.join(format!(".src-uv-y-{}", snapshot_owner()));
        for d in [&dead, &mine] {
            std::fs::create_dir_all(d).unwrap();
        }
        let _ = envs(dir.path());
        assert!(!dead.exists(), "a dead agent's copy is removed");
        assert!(mine.exists(), "a running installer's copy is kept");
    }

    #[test]
    fn finds_programs_on_path() {
        assert!(which("definitely-not-a-program-mrl").is_none());
        assert!(disk_free(Path::new(".")).is_some());
    }
}
