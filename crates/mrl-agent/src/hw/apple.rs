//! Apple silicon details: chip, unified memory, memory pressure, GPU cores and MLX support.
//!
//! Memory comes from `sysinfo`; pressure and GPU core count from the system's own `sysctl` and
//! `ioreg` tools (no unsafe bindings needed). Everything returns `None` off Apple silicon.

use serde::{Deserialize, Serialize};

/// macOS memory pressure, as the system rates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PressureLevel {
    /// Plenty of memory.
    Normal,
    /// Memory is getting tight; the system compresses and swaps.
    Warning,
    /// Memory is nearly gone; apps may be stopped.
    Critical,
}

/// Apple silicon details.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppleInfo {
    /// Chip name, like `Apple M4 Pro`.
    pub chip: String,
    /// Unified memory, bytes.
    pub unified_memory_total_bytes: u64,
    /// Unified memory in use, bytes.
    pub unified_memory_used_bytes: u64,
    /// The system's pressure rating.
    pub memory_pressure: Option<PressureLevel>,
    /// Pressure as a fraction, 0 (all free) to 1 (none free).
    pub memory_pressure_fraction: Option<f64>,
    /// GPU core count.
    pub gpu_cores: Option<u32>,
    /// MLX can run here (Apple silicon on macOS 13.5 or later).
    pub mlx_available: bool,
}

/// `kern.memorystatus_vm_pressure_level` -> level.
pub fn pressure_level(n: u32) -> Option<PressureLevel> {
    match n {
        1 => Some(PressureLevel::Normal),
        2 => Some(PressureLevel::Warning),
        4 => Some(PressureLevel::Critical),
        _ => None,
    }
}

/// Reads `"gpu-core-count" = N` from `ioreg -rc AGXAccelerator -d1` output.
pub fn parse_gpu_cores(ioreg: &str) -> Option<u32> {
    let rest = &ioreg[ioreg.find("\"gpu-core-count\"")? + "\"gpu-core-count\"".len()..];
    let digits: String = rest
        .trim_start_matches([' ', '='])
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// MLX needs macOS 13.5 or later.
pub fn mlx_supported(macos_version: &str) -> bool {
    let mut parts = macos_version
        .split('.')
        .map(|p| p.trim().parse::<u32>().unwrap_or(0));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    (major, minor) >= (13, 5)
}

#[cfg(target_os = "macos")]
fn tool(program: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

#[cfg(target_os = "macos")]
fn sysctl(name: &str) -> Option<String> {
    tool("/usr/sbin/sysctl", &["-n", name])
}

/// Memory pressure as a fraction 0..1 (macOS only): 1 minus the share of memory the system
/// counts as available (`kern.memorystatus_level`).
pub fn memory_pressure() -> Option<f64> {
    #[cfg(target_os = "macos")]
    {
        let free_percent: f64 = sysctl("kern.memorystatus_level")?.parse().ok()?;
        Some((1.0 - free_percent / 100.0).clamp(0.0, 1.0))
    }
    #[cfg(not(target_os = "macos"))]
    None
}

/// Apple silicon details, or `None` on other hardware.
pub fn detect() -> Option<AppleInfo> {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        let mem = super::cpu::memory();
        let chip = sysctl("machdep.cpu.brand_string").unwrap_or_else(|| "Apple silicon".into());
        let os = sysinfo::System::os_version().unwrap_or_default();
        Some(AppleInfo {
            chip,
            unified_memory_total_bytes: mem.total_bytes,
            unified_memory_used_bytes: mem.total_bytes.saturating_sub(mem.available_bytes),
            memory_pressure: sysctl("kern.memorystatus_vm_pressure_level")
                .and_then(|s| s.parse().ok())
                .and_then(pressure_level),
            memory_pressure_fraction: memory_pressure(),
            gpu_cores: tool("/usr/sbin/ioreg", &["-rc", "AGXAccelerator", "-d1"])
                .as_deref()
                .and_then(parse_gpu_cores),
            mlx_available: mlx_supported(&os),
        })
    }
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsers() {
        let ioreg = "+-o AGXAcceleratorG16X  <class AGXAccelerator>\n    {\n      \"gpu-core-count\" = 20\n      \"model\" = \"Apple M4 Pro\"\n    }";
        assert_eq!(parse_gpu_cores(ioreg), Some(20));
        assert_eq!(parse_gpu_cores("nothing"), None);
        assert_eq!(pressure_level(1), Some(PressureLevel::Normal));
        assert_eq!(pressure_level(4), Some(PressureLevel::Critical));
        assert_eq!(pressure_level(3), None);
        assert!(mlx_supported("13.5") && mlx_supported("15.2") && mlx_supported("26.1"));
        assert!(!mlx_supported("13.4.1") && !mlx_supported("12.7") && !mlx_supported(""));
    }

    #[test]
    fn detect_matches_the_hardware() {
        let info = detect();
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            let info = info.expect("Apple silicon is detected");
            assert!(info.unified_memory_total_bytes > 0);
            assert!(info.chip.starts_with("Apple"));
            assert!(info
                .memory_pressure_fraction
                .is_some_and(|p| (0.0..=1.0).contains(&p)));
        } else {
            assert!(info.is_none());
        }
    }
}
