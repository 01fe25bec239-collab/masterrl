//! A scriptable hardware backend for tests: fake NVIDIA-like GPUs with fault injection.
//!
//! ```
//! use mrl_agent::hw::fake::{Fault, FakeHw};
//! use mrl_agent::hw::HwBackend;
//! let hw = FakeHw::with_gpus(2, "Fake A100", 80 << 30);
//! hw.inject(Fault::Oom { index: 0 });
//! assert_eq!(hw.sample().unwrap()[0].memory_free_bytes, 0);
//! assert_eq!(hw.drain_events().len(), 1);
//! ```

use std::sync::{Mutex, MutexGuard};

use super::{BackendInventory, GpuDevice, GpuSample, GpuVendor, HwBackend, HwEvent, HwEventKind};
use crate::{AgentError, Result};

/// A fault to inject into a fake device (`index` is the fake backend's own index).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// The device's memory fills up; an out-of-memory event is raised.
    Oom {
        /// Device index.
        index: u32,
    },
    /// An XID error is raised.
    Xid {
        /// Device index.
        index: u32,
        /// XID number.
        code: u64,
    },
    /// The device disappears from inventory and samples; a device-lost event is raised.
    DeviceLost {
        /// Device index.
        index: u32,
    },
    /// Every call fails with this reason until [`FakeHw::clear_failure`].
    BackendFailure(String),
}

#[derive(Debug)]
struct Slot {
    device: GpuDevice,
    sample: GpuSample,
    lost: bool,
}

#[derive(Debug, Default)]
struct State {
    slots: Vec<Slot>,
    events: Vec<HwEvent>,
    failure: Option<String>,
}

/// The fake backend. Thread-safe; share it with `Arc`.
#[derive(Debug, Default)]
pub struct FakeHw {
    state: Mutex<State>,
}

impl FakeHw {
    /// No devices.
    pub fn new() -> Self {
        Self::default()
    }

    /// `n` identical idle GPUs with `memory` bytes each.
    pub fn with_gpus(n: u32, model: &str, memory: u64) -> Self {
        let hw = Self::new();
        for i in 0..n {
            hw.add_device(GpuDevice {
                index: i,
                backend: "fake".into(),
                vendor: GpuVendor::Nvidia,
                uuid: format!("GPU-fake-{model}-{i}").replace(' ', "-"),
                model: model.to_owned(),
                compute_capability: Some("8.0".into()),
                memory_total_bytes: memory,
                memory_free_bytes: memory,
                driver_version: Some("fake-1.0".into()),
                cuda_version: Some("12.4".into()),
                ecc_enabled: Some(true),
                pci_bus_id: Some(format!("00000000:{:02X}:00.0", i + 1)),
                nvlink_peers: Vec::new(),
                mig: None,
                experimental: false,
            });
        }
        hw
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Adds a device; its index is set to the next free one.
    pub fn add_device(&self, mut device: GpuDevice) {
        let mut s = self.lock();
        device.index = s.slots.len() as u32;
        let sample = GpuSample {
            index: device.index,
            uuid: device.uuid.clone(),
            memory_used_bytes: device.memory_total_bytes - device.memory_free_bytes,
            memory_free_bytes: device.memory_free_bytes,
            utilization_percent: Some(0),
            temperature_c: Some(35),
            power_watts: Some(50.0),
            ecc_uncorrected: Some(0),
            ecc_corrected: Some(0),
            thermal_throttling: false,
        };
        s.slots.push(Slot {
            device,
            sample,
            lost: false,
        });
    }

    /// Changes the live readings of device `index`, like `|s| s.utilization_percent = Some(90)`.
    pub fn update_sample(&self, index: u32, f: impl FnOnce(&mut GpuSample)) {
        if let Some(slot) = self.lock().slots.get_mut(index as usize) {
            f(&mut slot.sample);
        }
    }

    /// Injects a fault.
    pub fn inject(&self, fault: Fault) {
        let mut s = self.lock();
        let event = |s: &mut State, index: u32, kind: HwEventKind| {
            if let Some(slot) = s.slots.get(index as usize) {
                let e = HwEvent::now(slot.device.uuid.clone(), kind);
                s.events.push(e);
            }
        };
        match fault {
            Fault::Oom { index } => {
                if let Some(slot) = s.slots.get_mut(index as usize) {
                    slot.sample.memory_used_bytes = slot.device.memory_total_bytes;
                    slot.sample.memory_free_bytes = 0;
                }
                event(&mut s, index, HwEventKind::OutOfMemory);
            }
            Fault::Xid { index, code } => event(&mut s, index, HwEventKind::Xid { code }),
            Fault::DeviceLost { index } => {
                if let Some(slot) = s.slots.get_mut(index as usize) {
                    slot.lost = true;
                }
                event(&mut s, index, HwEventKind::DeviceLost);
            }
            Fault::BackendFailure(reason) => s.failure = Some(reason),
        }
    }

    /// Ends a [`Fault::BackendFailure`].
    pub fn clear_failure(&self) {
        self.lock().failure = None;
    }

    fn check(s: &State) -> Result<()> {
        match &s.failure {
            Some(reason) => Err(AgentError::Hardware {
                backend: "fake",
                reason: reason.clone(),
            }),
            None => Ok(()),
        }
    }
}

impl HwBackend for FakeHw {
    fn name(&self) -> &'static str {
        "fake"
    }

    fn inventory(&self) -> Result<Option<BackendInventory>> {
        let s = self.lock();
        Self::check(&s)?;
        let devices = s
            .slots
            .iter()
            .filter(|slot| !slot.lost)
            .map(|slot| {
                let mut d = slot.device.clone();
                d.memory_free_bytes = slot.sample.memory_free_bytes;
                d
            })
            .collect();
        Ok(Some(BackendInventory {
            driver_version: Some("fake-1.0".into()),
            runtime_version: Some("12.4".into()),
            experimental: false,
            devices,
        }))
    }

    fn sample(&self) -> Result<Vec<GpuSample>> {
        let s = self.lock();
        Self::check(&s)?;
        Ok(s.slots
            .iter()
            .filter(|slot| !slot.lost)
            .map(|slot| slot.sample.clone())
            .collect())
    }

    fn drain_events(&self) -> Vec<HwEvent> {
        std::mem::take(&mut self.lock().events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn faults_change_readings_and_raise_events() {
        let hw = FakeHw::with_gpus(3, "Fake A100", 1000);
        assert_eq!(hw.inventory().unwrap().unwrap().devices.len(), 3);

        hw.inject(Fault::Oom { index: 0 });
        hw.inject(Fault::Xid { index: 1, code: 79 });
        hw.inject(Fault::DeviceLost { index: 2 });
        hw.inject(Fault::Xid { index: 9, code: 1 }); // no such device: ignored
        let samples = hw.sample().unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].memory_free_bytes, 0);
        assert_eq!(samples[0].memory_used_bytes, 1000);
        let inv = hw.inventory().unwrap().unwrap();
        assert_eq!(inv.devices.len(), 2);
        assert_eq!(inv.devices[0].memory_free_bytes, 0);

        let kinds: Vec<_> = hw.drain_events().into_iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            [
                HwEventKind::OutOfMemory,
                HwEventKind::Xid { code: 79 },
                HwEventKind::DeviceLost
            ]
        );
        assert!(hw.drain_events().is_empty(), "events drain once");

        hw.update_sample(1, |s| s.utilization_percent = Some(90));
        assert_eq!(hw.sample().unwrap()[1].utilization_percent, Some(90));

        hw.inject(Fault::BackendFailure("driver crashed".into()));
        assert_eq!(hw.sample().unwrap_err().code(), "MRL-NODE-124");
        assert!(hw.inventory().is_err());
        hw.clear_failure();
        assert!(hw.sample().is_ok());
    }
}
