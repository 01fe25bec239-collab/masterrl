//! WSL2 awareness: detect WSL2, read the Windows user's `.wslconfig` limits, and see whether the
//! Windows host's GPU is visible (`/dev/dxg`; NVML then reports it through the WSL driver
//! folder, see [`super::nvml`]).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// WSL2 details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WslInfo {
    /// Distro name (`WSL_DISTRO_NAME`).
    pub distro: Option<String>,
    /// Kernel release, like `5.15.153.1-microsoft-standard-WSL2`.
    pub kernel: String,
    /// The `.wslconfig` that was read, as a Linux path.
    pub wslconfig_path: Option<PathBuf>,
    /// Limits from `.wslconfig`.
    pub limits: WslLimits,
    /// The Windows host GPU is visible here (`/dev/dxg` exists).
    pub host_gpu_visible: bool,
}

impl WslInfo {
    /// `networkingMode=mirrored`, needed for multi-machine jobs from Windows.
    pub fn mirrored(&self) -> bool {
        self.limits
            .networking_mode
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case("mirrored"))
    }
}

/// The `[wsl2]` limits the agent cares about. `None` means "WSL default".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WslLimits {
    /// `memory=`, in bytes.
    pub memory_bytes: Option<u64>,
    /// `processors=`.
    pub processors: Option<u32>,
    /// `swap=`, in bytes.
    pub swap_bytes: Option<u64>,
    /// `networkingMode=`.
    pub networking_mode: Option<String>,
}

/// Parses a WSL size like `8GB`, `512MB` or `1TB` (powers of 1024, as WSL reads them).
pub fn parse_size(text: &str) -> Option<u64> {
    let t = text.trim().to_ascii_uppercase();
    let digits_end = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let n: u64 = t[..digits_end].parse().ok()?;
    let shift = match t[digits_end..].trim() {
        "" | "B" => 0,
        "K" | "KB" => 10,
        "M" | "MB" => 20,
        "G" | "GB" => 30,
        "T" | "TB" => 40,
        _ => return None,
    };
    n.checked_mul(1 << shift)
}

/// Reads the `[wsl2]` section of a `.wslconfig` file.
pub fn parse_wslconfig(text: &str) -> WslLimits {
    let mut limits = WslLimits::default();
    let mut in_wsl2 = false;
    for line in text.lines() {
        let line = line.split(['#', ';']).next().unwrap_or("").trim();
        if line.starts_with('[') {
            in_wsl2 = line.eq_ignore_ascii_case("[wsl2]");
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if !in_wsl2 {
            continue;
        }
        let value = value.trim();
        match key.trim().to_ascii_lowercase().as_str() {
            "memory" => limits.memory_bytes = parse_size(value),
            "processors" => limits.processors = value.parse().ok(),
            "swap" => limits.swap_bytes = parse_size(value),
            "networkingmode" => limits.networking_mode = Some(value.to_owned()),
            _ => {}
        }
    }
    limits
}

/// True when the kernel release names WSL2.
pub fn is_wsl2_kernel(release: &str) -> bool {
    let r = release.to_ascii_lowercase();
    r.contains("microsoft") && r.contains("wsl2")
}

/// Finds the Windows user's `.wslconfig` from inside WSL2, through `cmd.exe` and `wslpath`.
fn find_wslconfig() -> Option<PathBuf> {
    let out = std::process::Command::new("cmd.exe")
        .args(["/C", "echo %USERPROFILE%"])
        .current_dir("/")
        .output()
        .ok()?;
    let profile = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    if !out.status.success() || profile.is_empty() || profile.contains('%') {
        return None;
    }
    let out = std::process::Command::new("wslpath")
        .args(["-u", &profile])
        .output()
        .ok()?;
    let dir = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (out.status.success() && !dir.is_empty()).then(|| Path::new(&dir).join(".wslconfig"))
}

/// WSL2 details, or `None` outside WSL2.
pub fn detect() -> Option<WslInfo> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease").ok()?;
    let kernel = kernel.trim().to_owned();
    if !is_wsl2_kernel(&kernel) {
        return None;
    }
    let wslconfig_path = find_wslconfig();
    let limits = wslconfig_path
        .as_deref()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .map(|t| parse_wslconfig(&t))
        .unwrap_or_default();
    Some(WslInfo {
        distro: std::env::var("WSL_DISTRO_NAME").ok(),
        kernel,
        wslconfig_path,
        limits,
        host_gpu_visible: Path::new("/dev/dxg").exists(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wslconfig_limits_parse() {
        let text = "# comment\n[wsl2]\nmemory=16GB\nprocessors = 8\nswap=512MB ; inline\nnetworkingMode=mirrored\n[experimental]\nmemory=1GB\n";
        let l = parse_wslconfig(text);
        assert_eq!(l.memory_bytes, Some(16 << 30));
        assert_eq!(l.processors, Some(8));
        assert_eq!(l.swap_bytes, Some(512 << 20));
        assert_eq!(l.networking_mode.as_deref(), Some("mirrored"));
        let info = WslInfo {
            distro: None,
            kernel: String::new(),
            wslconfig_path: None,
            limits: l,
            host_gpu_visible: true,
        };
        assert!(info.mirrored());
        assert_eq!(
            parse_wslconfig("memory=4GB"),
            WslLimits::default(),
            "outside [wsl2]"
        );
    }

    #[test]
    fn sizes_and_kernels() {
        assert_eq!(parse_size("4gb"), Some(4 << 30));
        assert_eq!(parse_size("1TB"), Some(1 << 40));
        assert_eq!(parse_size("100"), Some(100));
        assert_eq!(parse_size("lots"), None);
        assert_eq!(parse_size("5PB"), None);
        assert!(is_wsl2_kernel("5.15.153.1-microsoft-standard-WSL2"));
        assert!(!is_wsl2_kernel("6.8.0-45-generic"));
        assert!(!is_wsl2_kernel("4.4.0-19041-Microsoft"), "WSL1 is not WSL2");
    }
}
