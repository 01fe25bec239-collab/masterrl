//! The hardware backends this build registers. Each part adds one line inside the block,
//! like `Arc::new(crate::hw::rocm::RocmBackend::new()),` (P48). A backend whose hardware is
//! not on the computer reports itself as absent, so every backend is listed on every OS.

use std::sync::Arc;

use super::HwBackend;

/// Every registered hardware backend, in inventory order.
pub fn backends() -> Vec<Arc<dyn HwBackend>> {
    vec![
        // <mrl:register>
        Arc::new(super::nvml::NvmlBackend::new()),
        Arc::new(super::mps::MpsBackend::new()),
        // </mrl:register>
    ]
}
