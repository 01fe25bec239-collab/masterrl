//! The Apple GPU as a device (PyTorch MPS / MLX). Inventory only and experimental: macOS has
//! no per-process GPU memory or XID-like events, so samples carry unified-memory use only.

use super::{BackendInventory, GpuDevice, GpuSample, GpuVendor, HwBackend, HwEvent};
use crate::Result;

/// The MPS backend (absent off Apple silicon).
#[derive(Debug, Default)]
pub struct MpsBackend;

impl MpsBackend {
    /// The backend.
    pub fn new() -> Self {
        Self
    }

    fn device(info: &super::apple::AppleInfo) -> GpuDevice {
        GpuDevice {
            index: 0,
            backend: "mps".into(),
            vendor: GpuVendor::Apple,
            uuid: format!("apple-gpu-{}", info.chip.to_lowercase().replace(' ', "-")),
            model: match info.gpu_cores {
                Some(n) => format!("{} ({n}-core GPU)", info.chip),
                None => info.chip.clone(),
            },
            compute_capability: None,
            memory_total_bytes: info.unified_memory_total_bytes,
            memory_free_bytes: info
                .unified_memory_total_bytes
                .saturating_sub(info.unified_memory_used_bytes),
            driver_version: None,
            cuda_version: None,
            ecc_enabled: None,
            pci_bus_id: None,
            nvlink_peers: Vec::new(),
            mig: None,
            experimental: true,
        }
    }
}

impl HwBackend for MpsBackend {
    fn name(&self) -> &'static str {
        "mps"
    }

    fn inventory(&self) -> Result<Option<BackendInventory>> {
        Ok(super::apple::detect().map(|info| BackendInventory {
            driver_version: None,
            runtime_version: None,
            experimental: true,
            devices: vec![Self::device(&info)],
        }))
    }

    fn sample(&self) -> Result<Vec<GpuSample>> {
        Ok(super::apple::detect()
            .map(|info| {
                let d = Self::device(&info);
                vec![GpuSample {
                    index: 0,
                    uuid: d.uuid,
                    memory_used_bytes: info.unified_memory_used_bytes,
                    memory_free_bytes: d.memory_free_bytes,
                    utilization_percent: None,
                    temperature_c: None,
                    power_watts: None,
                    ecc_uncorrected: None,
                    ecc_corrected: None,
                    thermal_throttling: false,
                }]
            })
            .unwrap_or_default())
    }

    fn drain_events(&self) -> Vec<HwEvent> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_experimental_device_on_apple_silicon_only() {
        let inv = MpsBackend::new().inventory().unwrap();
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            let inv = inv.unwrap();
            assert!(inv.experimental && inv.devices[0].experimental);
            assert_eq!(inv.devices[0].vendor, GpuVendor::Apple);
            assert_eq!(MpsBackend::new().sample().unwrap().len(), 1);
        } else {
            assert!(inv.is_none());
            assert!(MpsBackend::new().sample().unwrap().is_empty());
        }
    }
}
