//! Cleaning up after attempts (Doc 20 item 16).
//!
//! - [`release`] runs when an attempt ends: kills any of its workers (and their process groups
//!   or containers) still alive, kills leftover processes whose working folder is inside the
//!   attempt folder, removes the attempt's pins from the node cache and the environments, and
//!   checks that its GPUs gave their memory back.
//! - [`purge`] wipes an ended attempt's scratch (work, staging, inputs, upload snapshots and its socket); the
//!   saved record and logs stay for diagnosis.
//! - [`sweep`] runs every few minutes ([`JanitorService`]): it repeats `release` for ended
//!   attempts whose processes are somehow still alive, purges ended attempts older than
//!   [`KEEP_ENDED`], drops stale partial downloads, and evicts unpinned cache entries and
//!   environments (least recently used first) when the disk gets low.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::attempts::record::{AttemptRecord, ProcIdentity};
use crate::attempts::runner::{signal_identity, socket_path};
use crate::envs::Envs;
use crate::{AgentContext, AgentError, Result};

/// GPU memory still in use after teardown above this is reported (bytes).
pub const GPU_RESIDUE_BYTES: u64 = 512 << 20;
/// Ended attempts' scratch is wiped after this long.
pub const KEEP_ENDED: Duration = Duration::from_secs(3 * 24 * 3600);
/// How often [`JanitorService`] sweeps.
pub const SWEEP_EVERY: Duration = Duration::from_secs(300);

/// What [`release`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CleanupReport {
    /// The attempt.
    pub attempt_id: String,
    /// Processes or containers it had to stop.
    pub killed: usize,
    /// Pins removed.
    pub unpinned: usize,
    /// GPUs that still hold memory, in plain words.
    pub gpu_warnings: Vec<String>,
    /// Problems that did not stop the cleanup.
    pub errors: Vec<String>,
}

/// Releases an ended attempt: stops leftovers, unpins, checks its GPUs.
pub async fn release(
    ctx: &AgentContext,
    envs: &Envs,
    rec: &AttemptRecord,
    gpu_uuids: &HashSet<String>,
) -> CleanupReport {
    let id = rec.spec.attempt.attempt_id.clone();
    let mut report = CleanupReport {
        attempt_id: id.clone(),
        ..CleanupReport::default()
    };
    let token = crate::attempts::runner::attempt_token(&rec.nonce);
    for w in &rec.workers {
        // A container is touched only when it carries this launch's token: an old record
        // never removes a newer launch's container that reused the name.
        if let ProcIdentity::Container { name } = &w.identity {
            let (n, t) = (name.clone(), token.clone());
            let owned = tokio::task::spawn_blocking(move || owns_container(&n, &t))
                .await
                .unwrap_or(false);
            if !owned {
                report
                    .errors
                    .push(format!("container {name} is not this launch's; left alone"));
                continue;
            }
        }
        let identity = w.identity.clone();
        let alive = tokio::task::spawn_blocking(move || identity.alive())
            .await
            .unwrap_or(false);
        if alive {
            signal_identity(&w.identity, true);
            report.killed += 1;
        }
        if let ProcIdentity::Container { name } = &w.identity {
            if let Some(docker) = crate::envs::which("docker") {
                let _ = tokio::process::Command::new(docker)
                    .args(["rm", "-f", name])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .await;
            }
        }
    }
    for cg in rec.workers.iter().filter_map(|w| w.cgroup.as_deref()) {
        match kill_cgroup(cg, &token) {
            Some(n) => report.killed += n,
            None => report.errors.push(format!(
                "cgroup {} is not this launch's; left alone",
                cg.display()
            )),
        }
    }
    let job_dir = ctx.state.job_dir(&id);
    report.killed += kill_leftovers(&job_dir, &token);
    match envs.unpin_all(&id) {
        Ok(n) => report.unpinned = n,
        Err(e) => report.errors.push(e.explain()),
    }
    report.gpu_warnings = gpu_residue(ctx, gpu_uuids).await;
    report
}

/// Processes of one launch that are still alive (not this agent): those whose environment
/// carries `MRL_ATTEMPT_TOKEN=<token>` (inherited by every child, even one that changed folder
/// and session; readable on Linux) or whose working folder is inside `job_dir`. The token is
/// unique per launch, so another agent's processes never match.
pub fn leftovers(job_dir: &Path, token: &str) -> Vec<u32> {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};
    let dir = std::fs::canonicalize(job_dir).ok();
    let mark = format!("{}={token}", crate::attempts::runner::TOKEN_ENV);
    let Ok(mut sys) = System::new() else {
        return Vec::new();
    };
    sys.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing()
            .with_cwd(UpdateKind::Always)
            .with_environ(UpdateKind::Always),
    );
    let me = std::process::id();
    sys.processes()
        .iter()
        .filter(|(pid, p)| {
            pid.as_u32() != me
                && !matches!(p.status(), sysinfo::ProcessStatus::Zombie)
                && (p
                    .environ()
                    .iter()
                    .any(|e| e.as_encoded_bytes() == mark.as_bytes())
                    || dir
                        .as_ref()
                        .is_some_and(|d| p.cwd().is_some_and(|c| c.starts_with(d))))
        })
        .map(|(pid, _)| pid.as_u32())
        .collect()
}

fn kill_leftovers(job_dir: &Path, token: &str) -> usize {
    let found = leftovers(job_dir, token);
    for pid in &found {
        if let Some(identity) = ProcIdentity::of_pid(*pid) {
            signal_identity(&identity, true);
        }
    }
    found.len()
}

/// Linux: the job cgroup (`…/job-<name>`) process `pid` is in, if it is in one.
pub fn job_cgroup_of(pid: u32) -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
        let rel = text.lines().find_map(|l| l.strip_prefix("0::"))?;
        let path = Path::new("/sys/fs/cgroup").join(rel.trim().trim_start_matches('/'));
        let is_job = path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("job-"));
        is_job.then_some(path)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

/// True when container `name` exists and its environment carries this launch's token.
pub fn owns_container(name: &str, token: &str) -> bool {
    let mark = format!("{}={token}", crate::attempts::runner::TOKEN_ENV);
    crate::envs::which("docker").is_some_and(|docker| {
        std::process::Command::new(docker)
            .args([
                "inspect",
                "--format",
                "{{range .Config.Env}}{{println .}}{{end}}",
                name,
            ])
            .stderr(std::process::Stdio::null())
            .output()
            .is_ok_and(|o| {
                o.status.success()
                    && String::from_utf8_lossy(&o.stdout)
                        .lines()
                        .any(|l| l == mark)
            })
    })
}

/// True when process `pid`'s environment carries this launch's token (Linux).
fn has_token(pid: u32, token: &str) -> bool {
    let mark = format!("{}={token}", crate::attempts::runner::TOKEN_ENV);
    std::fs::read(format!("/proc/{pid}/environ"))
        .is_ok_and(|env| env.split(|b| *b == 0).any(|kv| kv == mark.as_bytes()))
}

/// Linux: kills everything in a job cgroup this launch owns and removes it. Ownership: the
/// cgroup's name carries the launch token (`job-<token>-<role>`), or (records from before
/// that naming) one of its processes carries the token. `None`: not ours, left alone. An empty
/// cgroup is just removed.
fn kill_cgroup(cg: &Path, token: &str) -> Option<usize> {
    let procs = |cg: &Path| -> Vec<u32> {
        std::fs::read_to_string(cg.join("cgroup.procs"))
            .map(|t| t.lines().filter_map(|l| l.trim().parse().ok()).collect())
            .unwrap_or_default()
    };
    let found = procs(cg);
    let named = cg
        .file_name()
        .is_some_and(|n| n.to_string_lossy().contains(&token[..16]));
    if !found.is_empty() && !named && !found.iter().any(|p| has_token(*p, token)) {
        return None;
    }
    if !found.is_empty() && std::fs::write(cg.join("cgroup.kill"), "1").is_err() {
        for pid in procs(cg) {
            if let Some(identity) = ProcIdentity::of_pid(pid) {
                signal_identity(&identity, true);
            }
        }
    }
    for _ in 0..50 {
        if procs(cg).is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = std::fs::remove_dir(cg);
    Some(found.len())
}

/// Waits a few seconds for the attempt's GPUs to free their memory; reports the ones that don't.
async fn gpu_residue(ctx: &AgentContext, uuids: &HashSet<String>) -> Vec<String> {
    if uuids.is_empty() {
        return Vec::new();
    }
    let mut warnings = Vec::new();
    for _ in 0..10 {
        let samples: Vec<crate::hw::GpuSample> = ctx
            .hw
            .iter()
            .filter_map(|b| b.sample().ok())
            .flatten()
            .filter(|s| uuids.contains(&s.uuid))
            .collect();
        warnings = samples
            .iter()
            .filter(|s| s.memory_used_bytes > GPU_RESIDUE_BYTES)
            .map(|s| {
                format!(
                    "GPU {} still uses {} MiB after the attempt ended; another program may be using it",
                    s.uuid,
                    s.memory_used_bytes >> 20
                )
            })
            .collect();
        if warnings.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    warnings
}

/// Wipes an ended attempt's scratch: work, staging, inputs, upload snapshots and its socket.
/// True if anything was removed.
pub fn purge(state_root: &Path, job_dir: &Path) -> Result<bool> {
    let mut removed = false;
    for sub in ["work", "staging", "inputs", "uploads"] {
        let d = job_dir.join(sub);
        if d.exists() {
            make_writable(&d);
            std::fs::remove_dir_all(&d).map_err(|e| AgentError::state(&d, e))?;
            removed = true;
        }
    }
    if let Some(name) = job_dir.file_name() {
        let _ = std::fs::remove_file(socket_path(state_root, &name.to_string_lossy()));
    }
    Ok(removed)
}

/// Inputs are read-only; give the owner write access back so they can be deleted.
// Windows only clears the read-only flag (no Unix mode bits are involved).
#[cfg_attr(not(unix), allow(clippy::permissions_set_readonly_false))]
fn make_writable(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            let _ = std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o755));
            if let Ok(rd) = std::fs::read_dir(&d) {
                for e in rd.flatten() {
                    if e.file_type().is_ok_and(|t| t.is_dir()) {
                        stack.push(e.path());
                    }
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        // Windows: inputs carry the read-only flag, which blocks deleting them.
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            if let Ok(rd) = std::fs::read_dir(&d) {
                for e in rd.flatten() {
                    let p = e.path();
                    if e.file_type().is_ok_and(|t| t.is_dir()) {
                        stack.push(p);
                    } else if let Ok(m) = std::fs::metadata(&p) {
                        let mut perms = m.permissions();
                        perms.set_readonly(false);
                        let _ = std::fs::set_permissions(&p, perms);
                    }
                }
            }
        }
    }
}

/// What [`sweep`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SweepReport {
    /// Leftover processes stopped.
    pub killed: usize,
    /// Attempt folders whose scratch was wiped.
    pub purged: Vec<String>,
    /// Stale partial downloads removed.
    pub incoming_removed: usize,
    /// Cache entries evicted.
    pub evicted: usize,
    /// Environments removed.
    pub envs_removed: Vec<String>,
}

/// One janitor pass over the whole node. `live` are attempts running now (left alone).
pub async fn sweep(ctx: &AgentContext, envs: &Envs, live: &HashSet<String>) -> Result<SweepReport> {
    let mut report = SweepReport::default();
    let jobs = ctx.state.root().join("jobs");
    let entries: Vec<PathBuf> = std::fs::read_dir(&jobs)
        .map_err(|e| AgentError::state(&jobs, e))?
        .flatten()
        .map(|e| e.path())
        .collect();
    for dir in entries {
        let name = dir
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        if live.contains(&name) {
            continue;
        }
        let Ok(Some(rec)) = AttemptRecord::load(&dir) else {
            continue;
        };
        if rec.exit.is_none() {
            continue; // not ended: re-adoption decides
        }
        let r = release(ctx, envs, &rec, &HashSet::new()).await;
        report.killed += r.killed;
        let age = std::fs::metadata(dir.join(crate::attempts::record::RECORD_FILE))
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok());
        if age.is_some_and(|a| a > KEEP_ENDED) && purge(ctx.state.root(), &dir)? {
            report.purged.push(name);
        }
    }
    report.incoming_removed = ctx
        .cache
        .expire_incoming(crate::state::cache::INCOMING_TTL)?;
    report.evicted = ctx.cache.reserve(0)?.len();
    report.envs_removed = envs
        .evict_unused(ctx.config.cache.min_free_bytes)
        .map_err(|e| AgentError::exec("the environment cleanup", e.explain()))?;
    Ok(report)
}

/// Runs [`sweep`] every [`SWEEP_EVERY`] (registered in [`crate::registry::services`]).
#[derive(Debug, Default)]
pub struct JanitorService;

#[async_trait::async_trait]
impl crate::registry::AgentService for JanitorService {
    fn name(&self) -> &'static str {
        "janitor"
    }

    async fn run(&self, ctx: Arc<AgentContext>, shutdown: CancellationToken) -> Result<()> {
        let envs = Envs::open(ctx.state.root(), Arc::clone(&ctx.cache))
            .map_err(|e| AgentError::exec("the janitor", e.explain()))?;
        let mut tick = tokio::time::interval(SWEEP_EVERY);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                _ = tick.tick() => {
                    let live = running_attempts(ctx.state.root());
                    let report = sweep(&ctx, &envs, &live).await?;
                    let json = serde_json::to_vec_pretty(&report).unwrap_or_default();
                    ctx.state.write_atomic("janitor.json", &json)?;
                }
            }
        }
    }
}

/// Attempts whose record has no outcome yet (running, or waiting to be re-adopted).
pub fn running_attempts(state_root: &Path) -> HashSet<String> {
    std::fs::read_dir(state_root.join("jobs"))
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    AttemptRecord::load(&e.path())
                        .is_ok_and(|r| r.is_some_and(|r| r.exit.is_none()))
                })
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purge_wipes_scratch_but_keeps_logs() {
        let dir = tempfile::tempdir().unwrap();
        let job = dir.path().join("jobs").join("att_x");
        for sub in ["work", "staging/tmp", "inputs/model", "logs"] {
            std::fs::create_dir_all(job.join(sub)).unwrap();
        }
        std::fs::write(job.join("inputs/model/w.bin"), b"w").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                job.join("inputs/model"),
                std::fs::Permissions::from_mode(0o555),
            )
            .unwrap();
        }
        assert!(purge(dir.path(), &job).unwrap());
        assert!(!job.join("staging").exists() && !job.join("inputs").exists());
        assert!(job.join("logs").exists());
        assert!(!purge(dir.path(), &job).unwrap());
    }

    #[test]
    fn no_leftovers_in_an_empty_folder() {
        let dir = tempfile::tempdir().unwrap();
        assert!(leftovers(dir.path(), "att_none_here").is_empty());
    }
}
