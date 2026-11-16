//! NVIDIA GPUs through NVML (`libnvidia-ml`, loaded at run time; no NVIDIA build deps).
//!
//! Reading is split in two so it can be tested without a GPU: [`read_snapshot`] copies raw
//! NVML values into an [`NvmlSnapshot`] (serde, so it can be recorded as JSON with
//! `mrl-agent inventory --nvml-snapshot`), and [`parse_snapshot`] turns a snapshot into
//! devices and samples. XID errors arrive on a background thread (Linux; NVML has no event
//! API elsewhere).

use std::sync::{Arc, Mutex};

use nvml_wrapper::enum_wrappers::device::{EccCounter, MemoryError, TemperatureSensor};
use nvml_wrapper::error::NvmlError;
use nvml_wrapper::Nvml;
use serde::{Deserialize, Serialize};

use super::{
    BackendInventory, GpuDevice, GpuSample, GpuVendor, HwBackend, HwEvent, MigInfo, MigInstance,
};
use crate::{AgentError, Result};

const NAME: &str = "nvml";
/// NVML's limit on NVLink links per GPU.
const MAX_LINKS: u32 = 18;
// Throttle-reason bits that mean "slowed down by heat" (nvml.h).
const THERMAL_BITS: u64 = 0x8 | 0x20 | 0x40;

/// Raw NVML values for the whole node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NvmlSnapshot {
    /// `nvmlSystemGetDriverVersion`.
    pub driver_version: Option<String>,
    /// `nvmlSystemGetCudaDriverVersion`, like 12040.
    pub cuda_driver_version: Option<i32>,
    /// One entry per GPU, in NVML index order.
    pub devices: Vec<RawDevice>,
}

/// Raw NVML values for one GPU. `None` means NVML said "not supported" for this GPU.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawDevice {
    /// NVML index.
    pub index: u32,
    /// `GPU-...` uuid.
    pub uuid: String,
    /// Model name.
    pub name: String,
    /// PCI bus id.
    pub pci_bus_id: Option<String>,
    /// Compute capability (major, minor).
    pub compute_capability: Option<(i32, i32)>,
    /// Memory total, bytes.
    pub memory_total: Option<u64>,
    /// Memory free, bytes.
    pub memory_free: Option<u64>,
    /// Memory used, bytes.
    pub memory_used: Option<u64>,
    /// GPU busy percentage.
    pub utilization_gpu: Option<u32>,
    /// Temperature, °C.
    pub temperature_c: Option<u32>,
    /// Power draw, milliwatts.
    pub power_mw: Option<u32>,
    /// ECC mode on now.
    pub ecc_enabled: Option<bool>,
    /// Volatile uncorrected ECC errors.
    pub ecc_uncorrected: Option<u64>,
    /// Volatile corrected ECC errors.
    pub ecc_corrected: Option<u64>,
    /// Current throttle reason bits.
    pub throttle_reasons: Option<u64>,
    /// PCI bus ids at the far end of active NVLinks.
    pub nvlink_remote_bus_ids: Vec<String>,
    /// MIG mode (current, pending), 1 = on.
    pub mig_mode: Option<(u32, u32)>,
    /// MIG instances: (uuid, memory total).
    pub mig_devices: Vec<(String, u64)>,
}

fn opt<T>(r: std::result::Result<T, NvmlError>) -> Result<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(
            NvmlError::NotSupported
            | NvmlError::FailedToLoadSymbol(_)
            | NvmlError::NoPermission
            | NvmlError::NotFound
            | NvmlError::InvalidArg,
        ) => Ok(None),
        Err(e) => Err(hw_err(e)),
    }
}

fn hw_err(e: NvmlError) -> AgentError {
    AgentError::Hardware {
        backend: NAME,
        reason: e.to_string(),
    }
}

/// Copies every value the agent uses out of NVML.
pub fn read_snapshot(nvml: &Nvml) -> Result<NvmlSnapshot> {
    let count = nvml.device_count().map_err(hw_err)?;
    let mut devices = Vec::with_capacity(count as usize);
    for index in 0..count {
        let d = nvml.device_by_index(index).map_err(hw_err)?;
        let memory = opt(d.memory_info())?;
        let mut nvlink_remote_bus_ids = Vec::new();
        for link in 0..MAX_LINKS {
            let l = d.link_wrapper_for(link);
            if opt(l.is_active())?.unwrap_or(false) {
                if let Some(pci) = opt(l.remote_pci_info())? {
                    nvlink_remote_bus_ids.push(pci.bus_id);
                }
            }
        }
        let mig_mode = opt(d.mig_mode())?.map(|m| (m.current, m.pending));
        let mut mig_devices = Vec::new();
        if mig_mode.is_some_and(|(cur, _)| cur == 1) {
            for i in 0..opt(d.mig_device_count())?.unwrap_or(0) {
                if let Some(m) = opt(d.mig_device_by_index(i))? {
                    let uuid = m.uuid().map_err(hw_err)?;
                    let mem = opt(m.memory_info())?.map_or(0, |mi| mi.total);
                    mig_devices.push((uuid, mem));
                }
            }
        }
        devices.push(RawDevice {
            index,
            uuid: d.uuid().map_err(hw_err)?,
            name: d.name().map_err(hw_err)?,
            pci_bus_id: opt(d.pci_info())?.map(|p| p.bus_id),
            compute_capability: opt(d.cuda_compute_capability())?.map(|c| (c.major, c.minor)),
            memory_total: memory.as_ref().map(|m| m.total),
            memory_free: memory.as_ref().map(|m| m.free),
            memory_used: memory.as_ref().map(|m| m.used),
            utilization_gpu: opt(d.utilization_rates())?.map(|u| u.gpu),
            temperature_c: opt(d.temperature(TemperatureSensor::Gpu))?,
            power_mw: opt(d.power_usage())?,
            ecc_enabled: opt(d.is_ecc_enabled())?.map(|e| e.currently_enabled),
            ecc_uncorrected: opt(
                d.total_ecc_errors(MemoryError::Uncorrected, EccCounter::Volatile)
            )?,
            ecc_corrected: opt(d.total_ecc_errors(MemoryError::Corrected, EccCounter::Volatile))?,
            throttle_reasons: opt(d.current_throttle_reasons())?.map(|t| t.bits()),
            nvlink_remote_bus_ids,
            mig_mode,
            mig_devices,
        });
    }
    Ok(NvmlSnapshot {
        driver_version: opt(nvml.sys_driver_version())?,
        cuda_driver_version: opt(nvml.sys_cuda_driver_version())?,
        devices,
    })
}

/// `12040` -> `12.4`.
pub fn cuda_version_text(v: i32) -> String {
    format!(
        "{}.{}",
        nvml_wrapper::cuda_driver_version_major(v),
        nvml_wrapper::cuda_driver_version_minor(v)
    )
}

/// Turns a snapshot into the backend inventory and the current samples.
pub fn parse_snapshot(s: &NvmlSnapshot) -> (BackendInventory, Vec<GpuSample>) {
    let cuda = s.cuda_driver_version.map(cuda_version_text);
    let bus_to_uuid = |bus: &str| {
        s.devices
            .iter()
            .find(|d| {
                d.pci_bus_id
                    .as_deref()
                    .is_some_and(|b| b.eq_ignore_ascii_case(bus))
            })
            .map_or_else(|| bus.to_owned(), |d| d.uuid.clone())
    };
    let mut devices = Vec::new();
    let mut samples = Vec::new();
    for d in &s.devices {
        let total = d.memory_total.unwrap_or(0);
        let free = d.memory_free.unwrap_or(0);
        let mut peers: Vec<String> = d
            .nvlink_remote_bus_ids
            .iter()
            .map(|b| bus_to_uuid(b))
            .collect();
        peers.sort();
        peers.dedup();
        devices.push(GpuDevice {
            index: d.index,
            backend: NAME.to_owned(),
            vendor: GpuVendor::Nvidia,
            uuid: d.uuid.clone(),
            model: d.name.clone(),
            compute_capability: d.compute_capability.map(|(ma, mi)| format!("{ma}.{mi}")),
            memory_total_bytes: total,
            memory_free_bytes: free,
            driver_version: s.driver_version.clone(),
            cuda_version: cuda.clone(),
            ecc_enabled: d.ecc_enabled,
            pci_bus_id: d.pci_bus_id.clone(),
            nvlink_peers: peers,
            mig: d.mig_mode.map(|(cur, pending)| MigInfo {
                enabled: cur == 1,
                pending: pending == 1,
                instances: d
                    .mig_devices
                    .iter()
                    .map(|(uuid, mem)| MigInstance {
                        uuid: uuid.clone(),
                        memory_total_bytes: *mem,
                    })
                    .collect(),
            }),
            experimental: false,
        });
        samples.push(GpuSample {
            index: d.index,
            uuid: d.uuid.clone(),
            memory_used_bytes: d.memory_used.unwrap_or_else(|| total.saturating_sub(free)),
            memory_free_bytes: free,
            utilization_percent: d.utilization_gpu,
            temperature_c: d.temperature_c,
            power_watts: d.power_mw.map(|mw| f64::from(mw) / 1000.0),
            ecc_uncorrected: d.ecc_uncorrected,
            ecc_corrected: d.ecc_corrected,
            thermal_throttling: d.throttle_reasons.is_some_and(|b| b & THERMAL_BITS != 0),
        });
    }
    let inv = BackendInventory {
        driver_version: s.driver_version.clone(),
        runtime_version: cuda,
        experimental: false,
        devices,
    };
    (inv, samples)
}

/// The NVML backend. NVML is loaded on first use; a computer without the NVIDIA driver
/// reports the backend as absent.
pub struct NvmlBackend {
    nvml: Option<Arc<Nvml>>,
    load_error: Option<String>,
    events: Arc<Mutex<Vec<HwEvent>>>,
}

/// Where WSL2 puts the Windows host's NVML library.
const WSL_LIB: &str = "/usr/lib/wsl/lib/libnvidia-ml.so.1";

impl NvmlBackend {
    /// Loads NVML (from the WSL2 driver folder inside WSL2) and starts XID event watching.
    pub fn new() -> Self {
        let wsl_lib = std::path::Path::new(WSL_LIB);
        let init = if wsl_lib.exists() {
            Nvml::builder().lib_path(wsl_lib.as_os_str()).init()
        } else {
            Nvml::init()
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        match init {
            Ok(nvml) => {
                let nvml = Arc::new(nvml);
                watch_xid(Arc::clone(&nvml), Arc::clone(&events));
                Self {
                    nvml: Some(nvml),
                    load_error: None,
                    events,
                }
            }
            Err(
                NvmlError::LibloadingError(_)
                | NvmlError::DriverNotLoaded
                | NvmlError::LibraryNotFound
                | NvmlError::NoPermission,
            ) => Self {
                nvml: None,
                load_error: None,
                events,
            },
            Err(e) => Self {
                nvml: None,
                load_error: Some(e.to_string()),
                events,
            },
        }
    }

    fn snapshot(&self) -> Result<Option<NvmlSnapshot>> {
        if let Some(e) = &self.load_error {
            return Err(AgentError::Hardware {
                backend: NAME,
                reason: e.clone(),
            });
        }
        self.nvml.as_deref().map(read_snapshot).transpose()
    }
}

impl Default for NvmlBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl HwBackend for NvmlBackend {
    fn name(&self) -> &'static str {
        NAME
    }

    fn inventory(&self) -> Result<Option<BackendInventory>> {
        Ok(self.snapshot()?.map(|s| parse_snapshot(&s).0))
    }

    fn sample(&self) -> Result<Vec<GpuSample>> {
        Ok(self
            .snapshot()?
            .map(|s| parse_snapshot(&s).1)
            .unwrap_or_default())
    }

    fn drain_events(&self) -> Vec<HwEvent> {
        std::mem::take(
            &mut *self
                .events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl NvmlBackend {
    /// The raw snapshot, for recording fixtures. `None` without the NVIDIA driver.
    pub fn raw_snapshot(&self) -> Result<Option<NvmlSnapshot>> {
        self.snapshot()
    }
}

#[cfg(target_os = "linux")]
fn watch_xid(nvml: Arc<Nvml>, events: Arc<Mutex<Vec<HwEvent>>>) {
    use nvml_wrapper::bitmasks::event::EventTypes;
    use nvml_wrapper::enums::event::XidError;

    use super::HwEventKind;
    // ponytail: one thread for the agent's lifetime; it exits when the event set can't be built.
    let spawned = std::thread::Builder::new()
        .name("nvml-xid".into())
        .spawn(move || {
            let Ok(mut set) = nvml.create_event_set() else {
                return;
            };
            let count = nvml.device_count().unwrap_or(0);
            for i in 0..count {
                let Ok(d) = nvml.device_by_index(i) else {
                    continue;
                };
                // A failed registration consumes the set, so stop watching.
                set = match d.register_events(EventTypes::CRITICAL_XID_ERROR, set) {
                    Ok(s) => s,
                    Err(_) => return,
                };
            }
            loop {
                match set.wait(1000) {
                    Ok(ev) => {
                        let uuid = ev.device.uuid().unwrap_or_default();
                        let code = match ev.event_data {
                            Some(XidError::Value(v)) => v,
                            _ => 0,
                        };
                        let kind = if matches!(code, 79 | 74 | 48 | 63 | 64 | 95) {
                            // 79: fell off the bus; the others are fatal memory errors.
                            vec![HwEventKind::Xid { code }, HwEventKind::DeviceLost]
                        } else {
                            vec![HwEventKind::Xid { code }]
                        };
                        let mut q = events
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        q.extend(kind.into_iter().map(|k| HwEvent::now(uuid.clone(), k)));
                    }
                    Err(NvmlError::Timeout) => {}
                    Err(_) => return,
                }
            }
        });
    drop(spawned);
}

#[cfg(not(target_os = "linux"))]
fn watch_xid(_nvml: Arc<Nvml>, _events: Arc<Mutex<Vec<HwEvent>>>) {}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../../tests/fixtures/nvml-2xa100.json");

    #[test]
    fn recorded_snapshot_parses() {
        let snap: NvmlSnapshot = serde_json::from_str(FIXTURE).unwrap();
        let (inv, samples) = parse_snapshot(&snap);
        assert_eq!(inv.driver_version.as_deref(), Some("550.54.15"));
        assert_eq!(inv.runtime_version.as_deref(), Some("12.4"));
        assert_eq!(inv.devices.len(), 2);
        let g0 = &inv.devices[0];
        assert_eq!(g0.model, "NVIDIA A100-SXM4-80GB");
        assert_eq!(g0.compute_capability.as_deref(), Some("8.0"));
        assert_eq!(g0.memory_total_bytes, 85_899_345_920);
        assert_eq!(g0.ecc_enabled, Some(true));
        assert_eq!(g0.cuda_version.as_deref(), Some("12.4"));
        // NVLink peers resolve to the other GPU's uuid; unknown far ends (switches) stay bus ids.
        assert_eq!(g0.nvlink_peers, [inv.devices[1].uuid.clone()]);
        assert_eq!(
            inv.devices[1].nvlink_peers,
            ["00000000:C1:00.0", &g0.uuid[..]]
        );
        let mig = inv.devices[1].mig.as_ref().unwrap();
        assert!(mig.enabled && mig.pending);
        assert_eq!(mig.instances.len(), 2);
        assert_eq!(mig.instances[0].memory_total_bytes, 10_200_547_328);
        assert_eq!(
            g0.mig,
            Some(MigInfo {
                enabled: false,
                pending: false,
                instances: vec![]
            })
        );

        assert_eq!(samples[0].utilization_percent, Some(87));
        assert_eq!(samples[0].power_watts, Some(312.5));
        assert!(!samples[0].thermal_throttling);
        assert!(samples[1].thermal_throttling, "bit 0x40 is a heat slowdown");
        assert_eq!(samples[1].ecc_uncorrected, Some(2));
        // memory_used missing: derived from total - free.
        assert_eq!(
            samples[1].memory_used_bytes,
            85_899_345_920 - 80_000_000_000
        );
        assert_eq!(samples[1].temperature_c, None);
    }

    #[test]
    fn cuda_version_text_formats() {
        assert_eq!(cuda_version_text(12040), "12.4");
        assert_eq!(cuda_version_text(11080), "11.8");
    }

    #[test]
    fn backend_without_driver_is_absent_or_reports() {
        // On CI there is no NVIDIA driver: the backend must say "absent", not fail.
        let b = NvmlBackend::new();
        match b.inventory() {
            Ok(None) => assert!(b.sample().unwrap().is_empty()),
            Ok(Some(inv)) => assert_eq!(inv.devices.len(), b.sample().unwrap().len()),
            Err(e) => assert_eq!(e.code(), "MRL-NODE-124"),
        }
    }
}
