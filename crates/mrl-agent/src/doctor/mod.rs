//! The Environment Doctor (Doc 22 §7–§8): what this machine has, what it can train, and what to
//! install to unlock more.
//!
//! [`Facts::gather`] reads the hardware inventory, the tool versions (`uv`, Python, Docker) and
//! the app environments; each [`DoctorCheck`] from [`checks`] turns the facts into ok / warn /
//! fail with a guidance code (MRL-ENV-001…012 and MRL-ENV-202). [`run`] adds the per-task
//! capability summary from the guidance catalog (P05 requirement rows and hardware profiles),
//! and [`plan`] lists the components a task still needs, with download sizes. Downloads made
//! for a plan go through the environment installer ([`crate::envs`]), which resumes and reports
//! progress.
//!
//! The doctor runs on demand (`mrl-agent doctor`, the `Probe` command), on enrollment (P18 calls
//! [`run`]), and from [`DoctorService`] at agent start and whenever the GPU driver changes.

#[allow(dead_code, missing_docs, clippy::all)]
#[rustfmt::skip]
#[path = "../../../../guidance/build/catalog.rs"]
mod catalog;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mrl_proto::agent::v1::{ProbeCheck, ProbeCheckResult, ProbeOutcome, ProbeResult};
use mrl_proto::common::v1::{Issue, Severity};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::envs::{Envs, InstalledEnv};
use crate::hw::{GpuVendor, Inventory, Platform};
use crate::AgentContext;

const GIB: u64 = 1 << 30;

/// The outcome of one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Fine.
    Ok,
    /// Works, with limits.
    Warn,
    /// Blocks training here.
    Fail,
    /// Does not apply to this machine.
    NotApplicable,
}

/// One check's result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckResult {
    /// Which check.
    pub check: &'static str,
    /// The outcome.
    pub outcome: Outcome,
    /// One plain line, like "NVIDIA driver 580.65 with CUDA 12.9".
    pub summary: String,
    /// Guidance code explaining a warning or failure.
    pub code: Option<&'static str>,
}

/// A registered check.
#[derive(Debug, Clone, Copy)]
pub struct DoctorCheck {
    /// The probe check it answers.
    pub id: ProbeCheck,
    /// Short name, like `nvidia_driver`.
    pub name: &'static str,
    /// The check itself.
    pub run: fn(&Facts) -> (Outcome, String, Option<&'static str>),
}

/// Every check, in report order.
pub fn checks() -> Vec<DoctorCheck> {
    vec![
        // <mrl:register>
        DoctorCheck {
            id: ProbeCheck::Os,
            name: "os",
            run: os,
        },
        DoctorCheck {
            id: ProbeCheck::CpuRamDisk,
            name: "cpu_ram_disk",
            run: cpu_ram_disk,
        },
        DoctorCheck {
            id: ProbeCheck::Gpu,
            name: "gpu",
            run: gpu,
        },
        DoctorCheck {
            id: ProbeCheck::NvidiaDriver,
            name: "nvidia_driver",
            run: nvidia_driver,
        },
        DoctorCheck {
            id: ProbeCheck::Wsl2,
            name: "wsl2",
            run: wsl2,
        },
        DoctorCheck {
            id: ProbeCheck::AppleSilicon,
            name: "apple_silicon",
            run: apple,
        },
        DoctorCheck {
            id: ProbeCheck::Rocm,
            name: "rocm",
            run: rocm,
        },
        DoctorCheck {
            id: ProbeCheck::PythonUv,
            name: "python_uv",
            run: python_uv,
        },
        DoctorCheck {
            id: ProbeCheck::Docker,
            name: "docker",
            run: docker,
        },
        DoctorCheck {
            id: ProbeCheck::Environment,
            name: "environments",
            run: environments,
        },
        // </mrl:register>
    ]
}

/// Tool versions found on this machine (`None`: missing).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Tools {
    /// `uv --version`.
    pub uv: Option<String>,
    /// `python3 --version`.
    pub python: Option<String>,
    /// Docker server version (`None` when Docker is missing or not running).
    pub docker: Option<String>,
}

/// Everything the checks look at.
#[derive(Debug, Clone)]
pub struct Facts {
    /// Hardware inventory.
    pub inventory: Inventory,
    /// Tool versions.
    pub tools: Tools,
    /// App environments.
    pub envs: Vec<InstalledEnv>,
    /// Free bytes on the state disk.
    pub state_disk_free: Option<u64>,
}

async fn version(program: &str, args: &[&str]) -> Option<String> {
    let path = crate::envs::which(program)?;
    let out = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(path)
            .args(args)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    let text = String::from_utf8_lossy(if out.stdout.is_empty() {
        &out.stderr
    } else {
        &out.stdout
    })
    .trim()
    .to_owned();
    (out.status.success() && !text.is_empty()).then_some(text)
}

impl Facts {
    /// Reads the facts of this machine.
    pub async fn gather(ctx: &Arc<AgentContext>, envs: &Envs) -> Self {
        let c = Arc::clone(ctx);
        let inventory = match tokio::task::spawn_blocking(move || c.inventory()).await {
            Ok(inv) => inv,
            Err(_) => crate::hw::collect_inventory(&[]),
        };
        let python = match version("python3", &["--version"]).await {
            Some(v) => Some(v),
            None => version("python", &["--version"]).await,
        };
        Self {
            inventory,
            tools: Tools {
                uv: version("uv", &["--version"]).await,
                python,
                docker: version("docker", &["version", "--format", "{{.Server.Version}}"]).await,
            },
            envs: envs.installed(),
            state_disk_free: crate::envs::disk_free(ctx.state.root()),
        }
    }
}

fn gib(b: u64) -> String {
    format!("{:.0} GiB", b as f64 / GIB as f64)
}

fn os(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    let o = &f.inventory.os;
    let line = format!("{} {} ({}, {})", o.name, o.version, o.kernel, o.arch);
    match f.inventory.platform {
        Platform::Windows => (
            Outcome::Warn,
            format!("{line}: training runs inside WSL2, not on Windows directly"),
            Some("MRL-ENV-007"),
        ),
        Platform::Macos => {
            let major: u32 = o
                .version
                .split('.')
                .next()
                .and_then(|m| m.parse().ok())
                .unwrap_or(0);
            if major < 14 {
                (
                    Outcome::Fail,
                    format!("{line}: training needs macOS 14 or later"),
                    Some("MRL-ENV-001"),
                )
            } else {
                (Outcome::Ok, line, None)
            }
        }
        Platform::Linux | Platform::Wsl2 => (Outcome::Ok, line, None),
    }
}

fn cpu_ram_disk(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    let inv = &f.inventory;
    let ram = inv.memory.total_bytes;
    let disk = f.state_disk_free.unwrap_or(0);
    let line = format!(
        "{} with {} threads, {} memory, {} free disk",
        inv.cpu.model,
        inv.cpu.logical_cores,
        gib(ram),
        gib(disk)
    );
    if ram < 8 * GIB {
        (
            Outcome::Fail,
            format!("{line}: at least 8 GiB of memory is needed"),
            Some("MRL-ENV-003"),
        )
    } else if disk < 20 * GIB {
        (
            Outcome::Fail,
            format!("{line}: at least 20 GiB of free disk is needed"),
            Some("MRL-ENV-004"),
        )
    } else if ram < 16 * GIB {
        (
            Outcome::Warn,
            format!("{line}: 16 GiB or more is better"),
            Some("MRL-ENV-003"),
        )
    } else if disk < 50 * GIB {
        (
            Outcome::Warn,
            format!("{line}: one run can need 10–50 GB"),
            Some("MRL-ENV-004"),
        )
    } else if inv.cpu.logical_cores < 4 {
        (
            Outcome::Warn,
            format!("{line}: data preparation will be slow"),
            Some("MRL-ENV-002"),
        )
    } else {
        (Outcome::Ok, line, None)
    }
}

fn gpu(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    let gpus = &f.inventory.gpus;
    if gpus.is_empty() {
        return (
            Outcome::Warn,
            "No usable GPU was found; only tiny models train here".into(),
            Some("MRL-ENV-005"),
        );
    }
    let list: Vec<String> = gpus
        .iter()
        .map(|g| format!("{} ({})", g.model, gib(g.memory_total_bytes)))
        .collect();
    let biggest = gpus.iter().map(|g| g.memory_total_bytes).max().unwrap_or(0);
    let nvidia = gpus.iter().any(|g| g.vendor == GpuVendor::Nvidia);
    if nvidia && biggest < 6 * GIB {
        (
            Outcome::Warn,
            format!("{}: small models only", list.join(", ")),
            Some("MRL-ENV-005"),
        )
    } else {
        (Outcome::Ok, list.join(", "), None)
    }
}

fn nvidia_driver(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    let Some(g) = f
        .inventory
        .gpus
        .iter()
        .find(|g| g.vendor == GpuVendor::Nvidia)
    else {
        return (Outcome::NotApplicable, "No NVIDIA GPU".into(), None);
    };
    let driver = g.driver_version.clone().unwrap_or_default();
    let cuda = g.cuda_version.clone().unwrap_or_default();
    let num = |s: &str, i: usize| -> u32 {
        s.split('.')
            .nth(i)
            .and_then(|p| p.parse().ok())
            .unwrap_or(0)
    };
    let line = format!("NVIDIA driver {driver} with CUDA {cuda}");
    if num(&driver, 0) < 535 || (num(&cuda, 0), num(&cuda, 1)) < (12, 1) {
        (
            Outcome::Fail,
            format!("{line}: driver 535 or later with CUDA 12.1 or later is needed"),
            Some("MRL-ENV-006"),
        )
    } else {
        (Outcome::Ok, line, None)
    }
}

fn wsl2(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    match (f.inventory.platform, &f.inventory.wsl) {
        (Platform::Windows, _) => (
            Outcome::Fail,
            "WSL2 is not set up; training on Windows runs inside it".into(),
            Some("MRL-ENV-007"),
        ),
        (Platform::Wsl2, Some(w)) => {
            let mem = w.limits.memory_bytes.map_or("default".into(), gib);
            let line = format!(
                "WSL2 {} (memory limit {mem}, networking {})",
                w.distro.clone().unwrap_or_default(),
                w.limits
                    .networking_mode
                    .clone()
                    .unwrap_or_else(|| "nat".into())
            );
            if !w.host_gpu_visible {
                (
                    Outcome::Warn,
                    format!("{line}: the Windows GPU is not visible"),
                    Some("MRL-ENV-007"),
                )
            } else if w.limits.networking_mode.as_deref() != Some("mirrored") {
                (
                    Outcome::Warn,
                    format!("{line}: multi-machine training needs mirrored networking"),
                    Some("MRL-ENV-008"),
                )
            } else {
                (Outcome::Ok, line, None)
            }
        }
        _ => (Outcome::NotApplicable, "Not Windows".into(), None),
    }
}

fn apple(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    let Some(a) = &f.inventory.apple else {
        return (
            Outcome::NotApplicable,
            "Not an Apple Silicon Mac".into(),
            None,
        );
    };
    let line = format!(
        "{} with {} unified memory{}",
        a.chip,
        gib(a.unified_memory_total_bytes),
        if a.mlx_available {
            ", MLX available"
        } else {
            ""
        }
    );
    if a.unified_memory_total_bytes < 16 * GIB || !a.mlx_available {
        (
            Outcome::Warn,
            format!("{line}: only tiny models train here"),
            Some("MRL-ENV-009"),
        )
    } else {
        (Outcome::Ok, line, None)
    }
}

fn rocm(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    if f.inventory.gpus.iter().any(|g| g.vendor == GpuVendor::Amd) {
        (
            Outcome::Warn,
            "AMD GPU found; ROCm support is experimental".into(),
            Some("MRL-ENV-010"),
        )
    } else {
        (Outcome::NotApplicable, "No AMD GPU".into(), None)
    }
}

fn python_uv(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    let py = f.tools.python.clone().map_or_else(
        || "no system Python".into(),
        |p| format!("system {p}, not used for training"),
    );
    match &f.tools.uv {
        Some(uv) => (Outcome::Ok, format!("{uv}; {py}"), None),
        None => (
            Outcome::Fail,
            format!("uv is missing ({py}); the app needs it to install the training tools"),
            Some("MRL-ENV-011"),
        ),
    }
}

fn docker(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    match &f.tools.docker {
        Some(v) => (Outcome::Ok, format!("Docker {v}"), None),
        None => (
            Outcome::Warn,
            "Docker is not installed or not running; container runtimes stay off".into(),
            Some("MRL-ENV-012"),
        ),
    }
}

fn environments(f: &Facts) -> (Outcome, String, Option<&'static str>) {
    if f.envs.is_empty() {
        return (
            Outcome::Ok,
            "No app environments yet; each is installed when a task first needs it".into(),
            None,
        );
    }
    let broken: Vec<&str> = f
        .envs
        .iter()
        .filter(|e| !e.healthy)
        .map(|e| e.runtime_ref.as_str())
        .collect();
    let line = format!("{} app environment(s) installed", f.envs.len());
    if broken.is_empty() {
        (Outcome::Ok, line, None)
    } else {
        (
            Outcome::Warn,
            format!("{line}; incomplete: {}", broken.join(", ")),
            Some("MRL-ENV-202"),
        )
    }
}

/// A task this machine can or can't run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TaskLine {
    /// Task id, like `sft-lora`.
    pub task: String,
    /// Plain name from the recipe, or the id.
    pub name: String,
    /// The catalog note.
    pub note: Option<String>,
    /// "It can't": what stops it here (a failed check or a missing system component).
    pub blocked_by: Option<String>,
    /// "This machine can": download still needed first (app-managed packages), GB (low, high).
    pub download_first_gb: Option<(f64, f64)>,
}

/// Components the user installs on the system; the app installs the rest on demand.
const SYSTEM_COMPONENTS: [&str; 4] = ["nvidia-driver", "cuda", "wsl2", "python-env"];

/// Why `task`'s components can't work here, from the check results (`None`: nothing blocks).
fn blocker(f: &Facts, checks: &[CheckResult], components: &[PlanComponent]) -> Option<String> {
    let failed = |name: &str| {
        checks
            .iter()
            .find(|c| c.check == name && c.outcome == Outcome::Fail)
            .map(|c| c.summary.clone())
    };
    // These block every task.
    for name in ["os", "cpu_ram_disk", "python_uv"] {
        if let Some(why) = failed(name) {
            return Some(why);
        }
    }
    let needs = |id: &str| components.iter().any(|c| c.id == id);
    if needs("nvidia-driver") || needs("cuda") {
        if let Some(why) = failed("nvidia_driver") {
            return Some(why);
        }
    }
    if needs("wsl2") {
        if let Some(why) = failed("wsl2") {
            return Some(why);
        }
    }
    if needs("mlx") && !f.inventory.apple.as_ref().is_some_and(|a| a.mlx_available) {
        return Some("MLX can't run on this Mac".into());
    }
    components
        .iter()
        .find(|c| SYSTEM_COMPONENTS.contains(&c.id.as_str()) && !c.installed)
        .map(|c| format!("{} is not installed", c.name))
}

/// One component of an environment plan.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlanComponent {
    /// Component id, like `pytorch`.
    pub id: String,
    /// Plain name.
    pub name: String,
    /// What it does.
    pub summary: String,
    /// Estimated download, GB (low, high).
    pub download_gb: (f64, f64),
    /// Already on this machine.
    pub installed: bool,
}

/// What a task needs here.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EnvPlan {
    /// Task id.
    pub task: String,
    /// Catalog platform id.
    pub platform: &'static str,
    /// Whether the platform supports the task at all.
    pub supported: bool,
    /// The catalog note.
    pub note: Option<String>,
    /// Components in install order.
    pub components: Vec<PlanComponent>,
    /// Estimated download still needed, GB (low, high).
    pub to_download_gb: (f64, f64),
}

/// The per-task capability summary.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Capabilities {
    /// Catalog platform id, like `linux-nvidia`.
    pub platform: &'static str,
    /// Matching hardware profile id, like `nvidia-24gb`.
    pub hardware_profile: Option<String>,
    /// What models fit, from the profile.
    pub fits: Option<String>,
    /// "This machine can".
    pub can: Vec<TaskLine>,
    /// "It can't".
    pub cannot: Vec<TaskLine>,
    /// "To unlock more, install" (components missing for supported tasks).
    pub to_install: Vec<PlanComponent>,
}

/// The full report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DoctorReport {
    /// Check results in [`checks`] order.
    pub checks: Vec<CheckResult>,
    /// The capability summary.
    pub capabilities: Capabilities,
}

/// The catalog platform id for these facts.
pub fn platform_id(inv: &Inventory) -> &'static str {
    let nvidia = inv.gpus.iter().any(|g| g.vendor == GpuVendor::Nvidia);
    match inv.platform {
        Platform::Wsl2 | Platform::Windows if nvidia => "windows-wsl2-nvidia",
        Platform::Linux if nvidia => "linux-nvidia",
        Platform::Macos if inv.apple.is_some() => "macos-apple-silicon",
        _ => "cpu",
    }
}

fn items(path: &[&str]) -> &'static [catalog::Value] {
    let mut v = &catalog::CATALOG;
    for p in path {
        match v.get(p) {
            Some(next) => v = next,
            None => return &[],
        }
    }
    match *v {
        catalog::Value::List(l) => l,
        _ => &[],
    }
}

fn hardware_profile(inv: &Inventory) -> Option<&'static catalog::Value> {
    let (family, bytes) = if let Some(a) = &inv.apple {
        ("apple-silicon", a.unified_memory_total_bytes)
    } else {
        let max = inv
            .gpus
            .iter()
            .filter(|g| g.vendor == GpuVendor::Nvidia)
            .map(|g| g.memory_total_bytes)
            .max()?;
        ("nvidia", max)
    };
    let have = (bytes as f64 / GIB as f64).round();
    let min_of = |p: &catalog::Value| {
        p.get("memory_gb")
            .and_then(|m| m.get("min"))
            .and_then(|m| m.as_f64())
    };
    items(&["hardware", "profiles"])
        .iter()
        .filter(|p| p.str("family") == Some(family))
        .filter(|p| min_of(p).is_some_and(|m| m <= have))
        .max_by(|a, b| {
            min_of(a)
                .partial_cmp(&min_of(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn installed(component: &str, f: &Facts) -> bool {
    let inv = &f.inventory;
    let in_env = |pkg: &str| {
        f.envs
            .iter()
            .filter(|e| e.healthy)
            .any(|e| has_package(&e.dir, pkg))
    };
    match component {
        "nvidia-driver" => inv
            .gpus
            .iter()
            .any(|g| g.vendor == GpuVendor::Nvidia && g.driver_version.is_some()),
        "cuda" => inv.gpus.iter().any(|g| g.cuda_version.is_some()),
        "wsl2" => inv.wsl.is_some(),
        "python-env" => f.tools.uv.is_some(),
        "pytorch" => in_env("torch"),
        other => in_env(other),
    }
}

/// True when a uv environment has `pkg` in its site-packages.
fn has_package(env_dir: &Path, pkg: &str) -> bool {
    let win = env_dir.join("Lib").join("site-packages").join(pkg);
    if win.is_dir() {
        return true;
    }
    std::fs::read_dir(env_dir.join("lib")).is_ok_and(|rd| {
        rd.flatten()
            .any(|py| py.path().join("site-packages").join(pkg).is_dir())
    })
}

fn component(id: &str, platform: &str, f: &Facts) -> Option<PlanComponent> {
    let c = items(&["requirements", "components"])
        .iter()
        .find(|c| c.str("id") == Some(id))?;
    let size = c.get("download_gb").and_then(|d| d.get(platform));
    let n = |k: &str| {
        size.and_then(|s| s.get(k))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
    };
    Some(PlanComponent {
        id: id.to_owned(),
        name: c.str("name").unwrap_or(id).to_owned(),
        summary: c.str("summary").unwrap_or_default().to_owned(),
        download_gb: (n("min"), n("max")),
        installed: installed(id, f),
    })
}

/// The environment plan for `task` on this machine: components with sizes, marked installed or
/// not. `None` when the catalog has no row for the task here.
pub fn plan(task: &str, f: &Facts) -> Option<EnvPlan> {
    let platform = platform_id(&f.inventory);
    let row = catalog::requirement(task, platform)?;
    let components: Vec<PlanComponent> = match row.get("components") {
        Some(catalog::Value::List(ids)) => ids
            .iter()
            .filter_map(|v| v.as_str())
            .filter_map(|id| component(id, platform, f))
            .collect(),
        _ => Vec::new(),
    };
    let missing = components.iter().filter(|c| !c.installed);
    let to_download_gb = missing.fold((0.0, 0.0), |(a, b), c| {
        (a + c.download_gb.0, b + c.download_gb.1)
    });
    Some(EnvPlan {
        task: task.to_owned(),
        platform,
        supported: row
            .get("supported")
            .and_then(|s| s.as_bool())
            .unwrap_or(false),
        note: row.str("note").map(str::to_owned),
        components,
        to_download_gb,
    })
}

/// Runs every check and builds the capability summary.
pub fn run(f: &Facts) -> DoctorReport {
    let checks: Vec<CheckResult> = checks()
        .iter()
        .map(|c| {
            let (outcome, summary, code) = (c.run)(f);
            CheckResult {
                check: c.name,
                outcome,
                summary,
                code,
            }
        })
        .collect();
    let platform = platform_id(&f.inventory);
    let profile = hardware_profile(&f.inventory);
    let mut caps = Capabilities {
        platform,
        hardware_profile: profile.and_then(|p| p.str("id")).map(str::to_owned),
        fits: profile.and_then(|p| p.str("summary")).map(str::to_owned),
        can: Vec::new(),
        cannot: Vec::new(),
        to_install: Vec::new(),
    };
    for row in items(&["requirements", "rows"])
        .iter()
        .filter(|r| r.str("platform") == Some(platform))
    {
        let Some(task) = row.str("task") else {
            continue;
        };
        let mut line = TaskLine {
            task: task.to_owned(),
            name: catalog::recipe(task)
                .and_then(|r| r.str("name"))
                .unwrap_or(task)
                .to_owned(),
            note: row.str("note").map(str::to_owned),
            blocked_by: None,
            download_first_gb: None,
        };
        if row.get("supported").and_then(|s| s.as_bool()) != Some(true) {
            caps.cannot.push(line);
            continue;
        }
        let task_plan = plan(task, f);
        let components = task_plan
            .as_ref()
            .map(|p| p.components.clone())
            .unwrap_or_default();
        for c in components.iter().filter(|c| !c.installed) {
            if caps.to_install.iter().all(|x| x.id != c.id) {
                caps.to_install.push(c.clone());
            }
        }
        // Supported here only counts when the checks and the system components agree.
        line.blocked_by = blocker(f, &checks, &components);
        if line.blocked_by.is_some() {
            caps.cannot.push(line);
        } else {
            let gb = task_plan.map_or((0.0, 0.0), |p| p.to_download_gb);
            line.download_first_gb = (gb.1 > 0.0).then_some(gb);
            caps.can.push(line);
        }
    }
    DoctorReport {
        checks,
        capabilities: caps,
    }
}

impl DoctorReport {
    /// As a `ProbeResult`, keeping only `only` checks when it is not empty.
    pub fn to_proto(
        &self,
        request_id: &str,
        only: &[i32],
        inventory: &Inventory,
        state_dir: &Path,
    ) -> ProbeResult {
        let all = checks();
        let checks = self
            .checks
            .iter()
            .zip(all.iter())
            .filter(|(_, c)| only.is_empty() || only.contains(&(c.id as i32)))
            .map(|(r, c)| ProbeCheckResult {
                check: c.id as i32,
                outcome: match r.outcome {
                    Outcome::Ok => ProbeOutcome::Ok,
                    Outcome::Warn => ProbeOutcome::Warning,
                    Outcome::Fail => ProbeOutcome::Failed,
                    Outcome::NotApplicable => ProbeOutcome::NotApplicable,
                } as i32,
                summary: r.summary.clone(),
                issues: r
                    .code
                    .map(|code| Issue {
                        code: code.to_owned(),
                        severity: if r.outcome == Outcome::Fail {
                            Severity::Error
                        } else {
                            Severity::Warning
                        } as i32,
                        path: String::new(),
                        message: r.summary.clone(),
                        hint: catalog::error(code)
                            .and_then(|e| e.get("what_to_do"))
                            .and_then(|w| w.get("steps"))
                            .and_then(|s| match *s {
                                catalog::Value::List(l) => l.first().and_then(|v| v.as_str()),
                                _ => None,
                            })
                            .unwrap_or_default()
                            .to_owned(),
                    })
                    .into_iter()
                    .collect(),
                link: None,
            })
            .collect();
        ProbeResult {
            request_id: request_id.to_owned(),
            checks,
            inventory: Some(inventory.to_proto(state_dir)),
        }
    }

    /// The plain three-part text: can, can't, to unlock.
    pub fn text(&self) -> String {
        let mut s = String::new();
        for c in &self.checks {
            let mark = match c.outcome {
                Outcome::Ok => "ok  ",
                Outcome::Warn => "warn",
                Outcome::Fail => "FAIL",
                Outcome::NotApplicable => "n/a ",
            };
            s += &format!("[{mark}] {:<14} {}", c.check, c.summary);
            if let Some(code) = c.code {
                s += &format!("  ({code})");
            }
            s.push('\n');
        }
        let caps = &self.capabilities;
        s += &format!("\nPlatform: {}", caps.platform);
        if let Some(fits) = &caps.fits {
            s += &format!(". {fits}");
        }
        s += "\n\nThis machine can:\n";
        for t in &caps.can {
            s += &format!("  - {}", t.name);
            if let Some(n) = &t.note {
                s += &format!(" ({n})");
            }
            if let Some((lo, hi)) = t.download_first_gb {
                s += &format!(", after a download of about {lo:.1}–{hi:.1} GB");
            }
            s.push('\n');
        }
        s += "It can't:\n";
        for t in &caps.cannot {
            let why = t.blocked_by.as_ref().or(t.note.as_ref());
            s += &format!(
                "  - {}{}\n",
                t.name,
                why.map_or(String::new(), |n| format!(": {n}"))
            );
        }
        if !caps.to_install.is_empty() {
            s += "To unlock more, install:\n";
            for c in &caps.to_install {
                s += &format!(
                    "  - {} ({}): about {}–{} GB\n",
                    c.name,
                    c.summary.trim_end_matches('.'),
                    c.download_gb.0,
                    c.download_gb.1
                );
            }
        }
        s
    }
}

/// `mrl-agent doctor` options.
#[derive(Debug, clap::Args)]
pub struct DoctorArgs {
    /// Print JSON instead of the plain report.
    #[arg(long)]
    pub json: bool,
    /// Show what one task needs here, with download sizes (like `sft-lora` or `grpo`).
    #[arg(long, value_name = "TASK")]
    pub plan: Option<String>,
}

/// Runs `mrl-agent doctor`: prints the report (or a task plan) and saves it in the state
/// folder. Exits with 1 when a check failed.
pub async fn cli(config: Option<&Path>, args: DoctorArgs) -> std::process::ExitCode {
    let result = async {
        let (cfg, _) = crate::config::AgentConfig::load(config)?;
        let ctx = Arc::new(AgentContext::from_config(cfg)?);
        let envs = Envs::open(ctx.state.root(), Arc::clone(&ctx.cache))
            .map_err(|e| crate::AgentError::exec("the environment doctor", e.explain()))?;
        let facts = Facts::gather(&ctx, &envs).await;
        let report = run(&facts);
        ctx.state
            .write_atomic(REPORT_FILE, pretty(&report).as_bytes())?;
        if let Some(task) = &args.plan {
            let Some(p) = plan(task, &facts) else {
                return Err(crate::AgentError::UnknownName {
                    kind: "task",
                    name: task.clone(),
                    known: items(&["requirements", "rows"])
                        .iter()
                        .filter_map(|r| r.str("task"))
                        .collect::<std::collections::BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .join(", "),
                });
            };
            if args.json {
                println!("{}", pretty(&p));
            } else {
                println!("{}", p.text());
            }
        } else if args.json {
            println!("{}", pretty(&report));
        } else {
            print!("{}", report.text());
        }
        Ok(report.checks.iter().any(|c| c.outcome == Outcome::Fail))
    }
    .await;
    match result {
        Ok(false) => std::process::ExitCode::SUCCESS,
        Ok(true) => std::process::ExitCode::FAILURE,
        Err(e) => {
            eprintln!("mrl-agent: {}", e.explain());
            std::process::ExitCode::FAILURE
        }
    }
}

fn pretty<T: Serialize>(v: &T) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

impl EnvPlan {
    /// Plain text: each component with its size, and the total still to download.
    pub fn text(&self) -> String {
        let mut s = format!("{} on {}", self.task, self.platform);
        if !self.supported {
            s += &format!(
                ": not offered here. {}",
                self.note.clone().unwrap_or_default()
            );
            return s;
        }
        if let Some(n) = &self.note {
            s += &format!(" ({n})");
        }
        s.push('\n');
        for c in &self.components {
            s += &format!(
                "  [{}] {} — {}: about {}–{} GB\n",
                if c.installed { "installed" } else { "needed" },
                c.name,
                c.summary.trim_end_matches('.'),
                c.download_gb.0,
                c.download_gb.1
            );
        }
        s += &format!(
            "To download: about {:.1}–{:.1} GB",
            self.to_download_gb.0, self.to_download_gb.1
        );
        s
    }
}

/// File in the state folder with the latest report.
pub const REPORT_FILE: &str = "doctor.json";

/// Runs the doctor at agent start and again whenever a GPU driver version changes.
#[derive(Debug, Default)]
pub struct DoctorService;

#[async_trait::async_trait]
impl crate::registry::AgentService for DoctorService {
    fn name(&self) -> &'static str {
        "doctor"
    }

    async fn run(&self, ctx: Arc<AgentContext>, shutdown: CancellationToken) -> crate::Result<()> {
        let envs = Envs::open(ctx.state.root(), Arc::clone(&ctx.cache))
            .map_err(|e| crate::AgentError::exec("the environment doctor", e.explain()))?;
        let mut last_drivers: Option<Vec<Option<String>>> = None;
        let mut tick = tokio::time::interval(Duration::from_secs(600));
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                _ = tick.tick() => {
                    let facts = Facts::gather(&ctx, &envs).await;
                    let drivers: Vec<Option<String>> =
                        facts.inventory.gpus.iter().map(|g| g.driver_version.clone()).collect();
                    if last_drivers.as_ref() != Some(&drivers) {
                        let report = run(&facts);
                        let json = serde_json::to_vec_pretty(&report).unwrap_or_default();
                        ctx.state.write_atomic(REPORT_FILE, &json)?;
                        last_drivers = Some(drivers);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hw::fake::FakeHw;
    use crate::hw::HwBackend;

    fn facts(gpus: u32, memory: u64) -> Facts {
        let hw: Arc<dyn HwBackend> = Arc::new(FakeHw::with_gpus(gpus, "Fake A100", memory));
        let mut inventory = crate::hw::collect_inventory(&[hw]);
        inventory.platform = Platform::Linux;
        inventory.apple = None;
        inventory.wsl = None;
        inventory.memory.total_bytes = 64 * GIB;
        inventory.cpu.logical_cores = 16;
        for g in &mut inventory.gpus {
            g.vendor = GpuVendor::Nvidia;
            g.driver_version = Some("580.65.06".into());
            g.cuda_version = Some("12.9".into());
        }
        Facts {
            inventory,
            tools: Tools {
                uv: Some("uv 0.11.28".into()),
                python: None,
                docker: None,
            },
            envs: Vec::new(),
            state_disk_free: Some(500 * GIB),
        }
    }

    fn outcome(r: &DoctorReport, name: &str) -> (Outcome, Option<&'static str>) {
        let c = r.checks.iter().find(|c| c.check == name).unwrap();
        (c.outcome, c.code)
    }

    #[test]
    fn nvidia_node_with_fake_gpus() {
        let f = facts(2, 24 * GIB);
        let r = run(&f);
        assert_eq!(outcome(&r, "gpu").0, Outcome::Ok);
        assert_eq!(outcome(&r, "nvidia_driver").0, Outcome::Ok);
        assert_eq!(outcome(&r, "apple_silicon").0, Outcome::NotApplicable);
        assert_eq!(outcome(&r, "docker"), (Outcome::Warn, Some("MRL-ENV-012")));
        assert_eq!(r.capabilities.platform, "linux-nvidia");
        assert_eq!(
            r.capabilities.hardware_profile.as_deref(),
            Some("nvidia-24gb")
        );
        assert!(r.capabilities.can.iter().any(|t| t.task == "grpo"));
        let grpo = r
            .capabilities
            .can
            .iter()
            .find(|t| t.task == "grpo")
            .unwrap();
        assert!(
            grpo.download_first_gb.is_some(),
            "app packages still to download"
        );
        assert!(r.capabilities.cannot.iter().any(|t| t.task == "mlx-sft"));
        // Driver and CUDA are present; PyTorch is not installed in any app environment.
        assert!(r.capabilities.to_install.iter().any(|c| c.id == "pytorch"));
        assert!(!r
            .capabilities
            .to_install
            .iter()
            .any(|c| c.id == "nvidia-driver"));
        let p = plan("grpo", &f).unwrap();
        assert!(p.supported && p.to_download_gb.0 > 0.0);
        assert!(p.components.iter().any(|c| c.id == "vllm" && !c.installed));
        let proto = r.to_proto(
            "req1",
            &[ProbeCheck::NvidiaDriver as i32],
            &f.inventory,
            Path::new("/s"),
        );
        assert_eq!(proto.checks.len(), 1);
        assert!(r.text().contains("This machine can:"));
    }

    #[test]
    fn old_driver_small_gpu_and_missing_uv() {
        let mut f = facts(1, 4 * GIB);
        f.inventory.gpus[0].driver_version = Some("470.1".into());
        f.tools.uv = None;
        f.state_disk_free = Some(10 * GIB);
        let r = run(&f);
        assert_eq!(
            outcome(&r, "nvidia_driver"),
            (Outcome::Fail, Some("MRL-ENV-006"))
        );
        assert_eq!(outcome(&r, "gpu"), (Outcome::Warn, Some("MRL-ENV-005")));
        assert_eq!(
            outcome(&r, "python_uv"),
            (Outcome::Fail, Some("MRL-ENV-011"))
        );
        assert_eq!(
            outcome(&r, "cpu_ram_disk"),
            (Outcome::Fail, Some("MRL-ENV-004"))
        );
        assert!(
            r.capabilities.can.is_empty(),
            "failed prerequisites block every task"
        );
        let proto = r.to_proto("r", &[], &f.inventory, Path::new("/s"));
        let issue = &proto.checks[3].issues[0];
        assert_eq!(issue.code, "MRL-ENV-006");
        assert!(
            !issue.hint.is_empty(),
            "hint comes from the guidance catalog"
        );
        // Only the driver fails: NVIDIA tasks are blocked, with the reason.
        f.tools.uv = Some("uv 0.11.28".into());
        f.state_disk_free = Some(500 * GIB);
        let r = run(&f);
        let grpo = r
            .capabilities
            .cannot
            .iter()
            .find(|t| t.task == "grpo")
            .unwrap();
        assert!(
            grpo.blocked_by.as_deref().unwrap().contains("driver"),
            "{grpo:?}"
        );
        assert!(r.capabilities.can.iter().all(|t| t.task != "sft-lora"));
    }

    #[test]
    fn no_gpu_is_cpu_platform_and_windows_needs_wsl2() {
        let mut f = facts(0, 0);
        let r = run(&f);
        assert_eq!(r.capabilities.platform, "cpu");
        assert_eq!(outcome(&r, "nvidia_driver").0, Outcome::NotApplicable);
        f.inventory.platform = Platform::Windows;
        let r = run(&f);
        assert_eq!(outcome(&r, "wsl2"), (Outcome::Fail, Some("MRL-ENV-007")));
        assert_eq!(outcome(&r, "os").0, Outcome::Warn);
    }
}
