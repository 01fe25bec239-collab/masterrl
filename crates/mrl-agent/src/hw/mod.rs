//! Hardware discovery.
//!
//! GPUs come from [`HwBackend`]s listed in [`registry`] (`nvml`, `mps`; `fake` in tests).
//! CPU, memory, disks and network cards come from [`cpu`], Apple details from [`apple`], and
//! WSL2 details from [`wsl`]. [`collect_inventory`] puts it all together and never fails: a
//! backend that errors is reported as such, and the rest of the inventory is still filled.

pub mod apple;
pub mod cpu;
pub mod fake;
pub mod mps;
pub mod nvml;
pub mod registry;
pub mod wsl;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::Result;

/// A source of GPU devices, like NVML. Register new ones in [`registry`].
pub trait HwBackend: Send + Sync {
    /// Short name, like `nvml`.
    fn name(&self) -> &'static str;
    /// The devices and versions this backend sees, or `None` when its kind of hardware (or
    /// driver) is not on this computer.
    fn inventory(&self) -> Result<Option<BackendInventory>>;
    /// Live readings for each device, in the same order as [`Self::inventory`].
    fn sample(&self) -> Result<Vec<GpuSample>>;
    /// Hardware events (XID errors, out-of-memory, device loss) since the last call.
    fn drain_events(&self) -> Vec<HwEvent>;
}

/// What one backend found.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BackendInventory {
    /// Driver version, like `550.54.15`.
    pub driver_version: Option<String>,
    /// Compute runtime version the driver supports, like CUDA `12.4`.
    pub runtime_version: Option<String>,
    /// True for backends that are not fully supported yet.
    pub experimental: bool,
    /// The devices, with backend-local indexes.
    pub devices: Vec<GpuDevice>,
}

/// GPU maker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GpuVendor {
    /// NVIDIA.
    Nvidia,
    /// AMD.
    Amd,
    /// Apple silicon.
    Apple,
}

impl GpuVendor {
    /// The lowercase name used in protos and JSON.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nvidia => "nvidia",
            Self::Amd => "amd",
            Self::Apple => "apple",
        }
    }
}

/// One GPU (or one Apple GPU).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GpuDevice {
    /// Index across all backends in the inventory (backend-local inside a backend).
    pub index: u32,
    /// The backend that found it.
    pub backend: String,
    /// Maker.
    pub vendor: GpuVendor,
    /// Stable id, like `GPU-5b0e...`.
    pub uuid: String,
    /// Model name.
    pub model: String,
    /// CUDA compute capability, like `8.0`.
    pub compute_capability: Option<String>,
    /// Total memory in bytes (unified memory on Apple).
    pub memory_total_bytes: u64,
    /// Free memory in bytes when inventoried.
    pub memory_free_bytes: u64,
    /// Driver version.
    pub driver_version: Option<String>,
    /// CUDA version the driver supports.
    pub cuda_version: Option<String>,
    /// Whether ECC memory is on (`None` when the GPU has no ECC).
    pub ecc_enabled: Option<bool>,
    /// PCI bus id, like `00000000:07:00.0`.
    pub pci_bus_id: Option<String>,
    /// NVLink peers: uuids of local GPUs, or the PCI bus id of a switch.
    pub nvlink_peers: Vec<String>,
    /// MIG partitioning, when the GPU supports it.
    pub mig: Option<MigInfo>,
    /// True for devices from an experimental backend.
    pub experimental: bool,
}

/// MIG (Multi-Instance GPU) state of one GPU.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigInfo {
    /// MIG mode is on now.
    pub enabled: bool,
    /// MIG mode will be on after the next GPU reset.
    pub pending: bool,
    /// The MIG instances.
    pub instances: Vec<MigInstance>,
}

/// One MIG instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigInstance {
    /// Instance uuid, like `MIG-...`.
    pub uuid: String,
    /// Instance memory in bytes.
    pub memory_total_bytes: u64,
}

/// Live readings for one GPU.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GpuSample {
    /// Index as in the inventory.
    pub index: u32,
    /// Device uuid.
    pub uuid: String,
    /// Memory in use, bytes.
    pub memory_used_bytes: u64,
    /// Memory free, bytes.
    pub memory_free_bytes: u64,
    /// GPU busy percentage.
    pub utilization_percent: Option<u32>,
    /// Temperature in °C.
    pub temperature_c: Option<u32>,
    /// Power draw in watts.
    pub power_watts: Option<f64>,
    /// Uncorrected ECC errors since the driver loaded.
    pub ecc_uncorrected: Option<u64>,
    /// Corrected ECC errors since the driver loaded.
    pub ecc_corrected: Option<u64>,
    /// The GPU is slowing down because of heat.
    pub thermal_throttling: bool,
}

/// What happened in a [`HwEvent`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HwEventKind {
    /// NVIDIA XID error with its code.
    Xid {
        /// The XID number, like 79 (fell off the bus).
        code: u64,
    },
    /// The device ran out of memory.
    OutOfMemory,
    /// The device stopped answering.
    DeviceLost,
}

/// A hardware event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HwEvent {
    /// Device uuid.
    pub uuid: String,
    /// What happened.
    #[serde(flatten)]
    pub kind: HwEventKind,
    /// When, in Unix milliseconds.
    pub at_unix_ms: i64,
}

impl HwEvent {
    /// An event that happened now.
    pub fn now(uuid: impl Into<String>, kind: HwEventKind) -> Self {
        Self {
            uuid: uuid.into(),
            kind,
            at_unix_ms: unix_ms(),
        }
    }
}

pub(crate) fn unix_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

/// Backend health in the inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendStatus {
    /// Working.
    Ok,
    /// Its hardware or driver is not on this computer.
    Absent,
    /// Present but failing; see the message.
    Error,
}

/// One backend's line in the inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendReport {
    /// Backend name.
    pub name: String,
    /// Health.
    pub status: BackendStatus,
    /// The error, with its code, when `status` is `error`.
    pub message: Option<String>,
    /// Driver version.
    pub driver_version: Option<String>,
    /// Runtime version.
    pub runtime_version: Option<String>,
    /// Experimental backend.
    pub experimental: bool,
    /// Number of devices found.
    pub devices: usize,
}

/// The OS the agent runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    /// Linux.
    Linux,
    /// macOS.
    Macos,
    /// Windows.
    Windows,
    /// Linux inside WSL2 on Windows.
    Wsl2,
}

/// A full inventory of this computer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Inventory {
    /// Host name.
    pub hostname: String,
    /// OS family.
    pub platform: Platform,
    /// OS details.
    pub os: cpu::OsInfo,
    /// Processor.
    pub cpu: cpu::CpuInfo,
    /// Main memory.
    pub memory: cpu::MemoryInfo,
    /// Mounted disks.
    pub disks: Vec<cpu::DiskInfo>,
    /// Network cards.
    pub nics: Vec<cpu::NicInfo>,
    /// GPUs from every backend, indexed in order.
    pub gpus: Vec<GpuDevice>,
    /// One line per backend.
    pub backends: Vec<BackendReport>,
    /// Apple silicon details (macOS on Apple chips).
    pub apple: Option<apple::AppleInfo>,
    /// WSL2 details.
    pub wsl: Option<wsl::WslInfo>,
}

/// Builds the inventory from the host and the given backends.
pub fn collect_inventory(backends: &[Arc<dyn HwBackend>]) -> Inventory {
    let host = cpu::host();
    let wsl = wsl::detect();
    let platform = if wsl.is_some() {
        Platform::Wsl2
    } else if cfg!(target_os = "macos") {
        Platform::Macos
    } else if cfg!(windows) {
        Platform::Windows
    } else {
        Platform::Linux
    };
    let mut gpus = Vec::new();
    let mut reports = Vec::new();
    for b in backends {
        let mut report = BackendReport {
            name: b.name().to_owned(),
            status: BackendStatus::Absent,
            message: None,
            driver_version: None,
            runtime_version: None,
            experimental: false,
            devices: 0,
        };
        match b.inventory() {
            Ok(None) => {}
            Ok(Some(inv)) => {
                report.status = BackendStatus::Ok;
                report.driver_version = inv.driver_version;
                report.runtime_version = inv.runtime_version;
                report.experimental = inv.experimental;
                report.devices = inv.devices.len();
                for mut d in inv.devices {
                    d.index = gpus.len() as u32;
                    gpus.push(d);
                }
            }
            Err(e) => {
                report.status = BackendStatus::Error;
                report.message = Some(e.explain());
            }
        }
        reports.push(report);
    }
    Inventory {
        hostname: host.hostname,
        platform,
        os: host.os,
        cpu: host.cpu,
        memory: host.memory,
        disks: host.disks,
        nics: host.nics,
        gpus,
        backends: reports,
        apple: apple::detect(),
        wsl,
    }
}

/// Live readings for the node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Telemetry {
    /// When, in Unix milliseconds.
    pub at_unix_ms: i64,
    /// Per-GPU readings, indexed as in the inventory.
    pub gpus: Vec<GpuSample>,
    /// Main memory in use.
    pub ram_used_bytes: u64,
    /// Swap in use.
    pub swap_used_bytes: u64,
    /// Free space on the state disk.
    pub disk_free_bytes: u64,
    /// Apple memory pressure, 0..1.
    pub memory_pressure: Option<f64>,
    /// Hardware events since the last reading.
    pub events: Vec<HwEvent>,
    /// Backends that failed to sample, with their error.
    pub errors: Vec<String>,
}

/// Reads live values from every backend. GPU indexes follow `inventory` (matched by uuid);
/// devices not in it get indexes after the last one.
pub fn collect_telemetry(
    backends: &[Arc<dyn HwBackend>],
    inventory: &Inventory,
    state_dir: &Path,
) -> Telemetry {
    let by_uuid: HashMap<&str, u32> = inventory
        .gpus
        .iter()
        .map(|g| (g.uuid.as_str(), g.index))
        .collect();
    let mut next = inventory.gpus.len() as u32;
    let mut gpus = Vec::new();
    let mut events = Vec::new();
    let mut errors = Vec::new();
    for b in backends {
        match b.sample() {
            Ok(samples) => {
                for mut s in samples {
                    s.index = by_uuid.get(s.uuid.as_str()).copied().unwrap_or_else(|| {
                        next += 1;
                        next - 1
                    });
                    gpus.push(s);
                }
            }
            Err(e) => errors.push(e.explain()),
        }
        events.extend(b.drain_events());
    }
    let mem = cpu::memory();
    Telemetry {
        at_unix_ms: unix_ms(),
        gpus,
        ram_used_bytes: mem.total_bytes.saturating_sub(mem.available_bytes),
        swap_used_bytes: mem.swap_used_bytes,
        disk_free_bytes: cpu::volume_of(state_dir).map_or(0, |(_, free)| free),
        memory_pressure: apple::memory_pressure(),
        events,
        errors,
    }
}

impl Inventory {
    /// The `mrl.agent.v1.Inventory` message. `state_dir` picks the disk whose free space is
    /// reported.
    pub fn to_proto(&self, state_dir: &Path) -> mrl_proto::agent::v1::Inventory {
        use mrl_proto::agent::v1 as pb;
        let platform = match self.platform {
            Platform::Linux => pb::Platform::Linux,
            Platform::Macos => pb::Platform::Macos,
            Platform::Windows => pb::Platform::Windows,
            Platform::Wsl2 => pb::Platform::Wsl2,
        };
        let cuda = self.gpus.iter().find_map(|g| g.cuda_version.clone());
        pb::Inventory {
            hostname: self.hostname.clone(),
            platform: platform as i32,
            os_version: self.os.version.clone(),
            arch: self.os.arch.clone(),
            cpu_model: self.cpu.model.clone(),
            cpu_cores: self.cpu.physical_cores.unwrap_or(self.cpu.logical_cores) as u32,
            ram_bytes: self.memory.total_bytes,
            disk_free_bytes: cpu::volume_of(state_dir).map_or(0, |(_, free)| free),
            gpus: self
                .gpus
                .iter()
                .map(|g| pb::Gpu {
                    index: g.index,
                    uuid: g.uuid.clone(),
                    vendor: g.vendor.as_str().to_owned(),
                    model: g.model.clone(),
                    memory_bytes: g.memory_total_bytes,
                    compute_capability: g.compute_capability.clone().unwrap_or_default(),
                    driver_version: g.driver_version.clone().unwrap_or_default(),
                })
                .collect(),
            apple_silicon: self.apple.is_some(),
            unified_memory_bytes: self
                .apple
                .as_ref()
                .map_or(0, |a| a.unified_memory_total_bytes),
            cuda_runtime: cuda.unwrap_or_default(),
            wsl2_mirrored_networking: self.wsl.as_ref().is_some_and(wsl::WslInfo::mirrored),
        }
    }
}

impl Telemetry {
    /// The `mrl.agent.v1.NodeTelemetry` message.
    pub fn to_proto(&self) -> mrl_proto::agent::v1::NodeTelemetry {
        use mrl_proto::agent::v1 as pb;
        pb::NodeTelemetry {
            time: Some(prost_types::Timestamp {
                seconds: self.at_unix_ms.div_euclid(1000),
                nanos: (self.at_unix_ms.rem_euclid(1000) * 1_000_000) as i32,
            }),
            gpus: self
                .gpus
                .iter()
                .map(|s| pb::GpuSample {
                    index: s.index,
                    memory_used_bytes: s.memory_used_bytes,
                    utilization_percent: s.utilization_percent.unwrap_or(0),
                    temperature_c: s.temperature_c.unwrap_or(0),
                    thermal_throttling: s.thermal_throttling,
                })
                .collect(),
            ram_used_bytes: self.ram_used_bytes,
            swap_used_bytes: self.swap_used_bytes,
            disk_free_bytes: self.disk_free_bytes,
            memory_pressure: self.memory_pressure.unwrap_or(0.0),
            on_battery: false,
            battery_percent: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hw::fake::{FakeHw, Fault};

    struct Broken;
    impl HwBackend for Broken {
        fn name(&self) -> &'static str {
            "broken"
        }
        fn inventory(&self) -> Result<Option<BackendInventory>> {
            Err(crate::AgentError::Hardware {
                backend: "broken",
                reason: "driver crashed".into(),
            })
        }
        fn sample(&self) -> Result<Vec<GpuSample>> {
            self.inventory().map(|_| Vec::new())
        }
        fn drain_events(&self) -> Vec<HwEvent> {
            Vec::new()
        }
    }

    #[test]
    fn inventory_merges_backends_and_reports_failures() {
        let a = Arc::new(FakeHw::with_gpus(2, "Fake A100", 80 << 30));
        let b = Arc::new(FakeHw::with_gpus(1, "Fake H100", 80 << 30));
        let backends: Vec<Arc<dyn HwBackend>> = vec![a.clone(), Arc::new(Broken), b];
        let inv = collect_inventory(&backends);
        assert_eq!(inv.gpus.len(), 3);
        assert_eq!(
            inv.gpus.iter().map(|g| g.index).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(inv.gpus[2].model, "Fake H100");
        assert_eq!(inv.backends[1].status, BackendStatus::Error);
        assert!(inv.backends[1]
            .message
            .as_deref()
            .unwrap()
            .starts_with("MRL-NODE-124"));
        assert!(inv.cpu.logical_cores > 0 && inv.memory.total_bytes > 0);

        let pb = inv.to_proto(Path::new("."));
        assert_eq!(pb.gpus.len(), 3);
        assert_eq!(pb.gpus[0].vendor, "nvidia");
        assert_eq!(pb.ram_bytes, inv.memory.total_bytes);

        // Device 1 of backend a is lost: telemetry keeps inventory indexes.
        a.inject(Fault::DeviceLost { index: 1 });
        let t = collect_telemetry(&backends, &inv, Path::new("."));
        assert_eq!(t.gpus.iter().map(|s| s.index).collect::<Vec<_>>(), [0, 2]);
        assert_eq!(t.errors.len(), 1);
        assert_eq!(t.events.len(), 1);
        assert_eq!(t.events[0].kind, HwEventKind::DeviceLost);
        let pb = t.to_proto();
        assert_eq!(pb.gpus.len(), 2);
        assert!(pb.time.is_some());
    }

    #[test]
    fn event_json_is_flat() {
        let e = HwEvent {
            uuid: "GPU-1".into(),
            kind: HwEventKind::Xid { code: 79 },
            at_unix_ms: 5,
        };
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            serde_json::json!({"uuid": "GPU-1", "kind": "xid", "code": 79, "at_unix_ms": 5})
        );
    }
}
