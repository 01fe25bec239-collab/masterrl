//! CPU, memory, disk and network inventory (through `sysinfo`).

use std::path::Path;

use serde::{Deserialize, Serialize};
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, Networks, RefreshKind, System};

/// OS details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OsInfo {
    /// OS name, like `Ubuntu` or `Darwin`.
    pub name: String,
    /// OS version, like `24.04` or `15.2`.
    pub version: String,
    /// Kernel version.
    pub kernel: String,
    /// `x86_64` or `aarch64`.
    pub arch: String,
}

/// Processor details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuInfo {
    /// Model name, like `AMD EPYC 7763 64-Core Processor`.
    pub model: String,
    /// CPU maker id, like `GenuineIntel`.
    pub vendor: String,
    /// Physical cores, when the OS reports them.
    pub physical_cores: Option<usize>,
    /// Logical cores (hardware threads).
    pub logical_cores: usize,
    /// Base frequency in MHz.
    pub frequency_mhz: u64,
}

/// Main memory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryInfo {
    /// Total RAM in bytes.
    pub total_bytes: u64,
    /// RAM available to new programs, in bytes.
    pub available_bytes: u64,
    /// Total swap in bytes.
    pub swap_total_bytes: u64,
    /// Swap in use, in bytes.
    pub swap_used_bytes: u64,
}

/// One mounted disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskInfo {
    /// Where it is mounted.
    pub mount_point: String,
    /// Device name.
    pub name: String,
    /// File system, like `ext4` or `apfs`.
    pub file_system: String,
    /// `SSD`, `HDD` or `Unknown(..)`.
    pub kind: String,
    /// Size in bytes.
    pub total_bytes: u64,
    /// Free space in bytes.
    pub free_bytes: u64,
    /// Removable disk.
    pub removable: bool,
    /// Mounted read-only.
    pub read_only: bool,
}

/// One network card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicInfo {
    /// Interface name, like `eth0`.
    pub name: String,
    /// Hardware address.
    pub mac: String,
    /// Addresses with prefix length, like `10.0.0.5/24`.
    pub addresses: Vec<String>,
    /// Largest packet size in bytes.
    pub mtu: u64,
    /// Link speed in bits per second, when known.
    pub link_speed_bps: Option<u64>,
}

/// Everything the host reports, apart from GPUs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Host {
    /// Host name.
    pub hostname: String,
    /// OS.
    pub os: OsInfo,
    /// Processor.
    pub cpu: CpuInfo,
    /// Memory.
    pub memory: MemoryInfo,
    /// Disks.
    pub disks: Vec<DiskInfo>,
    /// Network cards.
    pub nics: Vec<NicInfo>,
}

fn system() -> Option<System> {
    System::new_with_specifics(
        RefreshKind::nothing()
            .with_memory(MemoryRefreshKind::everything())
            .with_cpu(CpuRefreshKind::everything()),
    )
    .ok()
}

/// Reads memory totals.
pub fn memory() -> MemoryInfo {
    let sys = system();
    memory_of(sys.as_ref())
}

fn memory_of(sys: Option<&System>) -> MemoryInfo {
    sys.map_or(
        MemoryInfo {
            total_bytes: 0,
            available_bytes: 0,
            swap_total_bytes: 0,
            swap_used_bytes: 0,
        },
        |s| MemoryInfo {
            total_bytes: s.total_memory(),
            available_bytes: s.available_memory(),
            swap_total_bytes: s.total_swap(),
            swap_used_bytes: s.used_swap(),
        },
    )
}

/// Reads the host inventory.
pub fn host() -> Host {
    let sys = system();
    let cpus = sys.as_ref().map_or(&[][..], System::cpus);
    let first = cpus.first();
    let arch = match System::cpu_arch().as_str() {
        "arm64" => "aarch64".to_owned(),
        other => other.to_owned(),
    };
    Host {
        hostname: System::host_name().unwrap_or_default(),
        os: OsInfo {
            name: System::name().unwrap_or_default(),
            version: System::os_version().unwrap_or_default(),
            kernel: System::kernel_version().unwrap_or_default(),
            arch,
        },
        cpu: CpuInfo {
            model: first
                .map(|c| c.brand().trim().to_owned())
                .unwrap_or_default(),
            vendor: first.map(|c| c.vendor_id().to_owned()).unwrap_or_default(),
            physical_cores: System::physical_core_count().ok(),
            logical_cores: cpus
                .len()
                .max(std::thread::available_parallelism().map_or(0, std::num::NonZero::get)),
            frequency_mhz: first.map_or(0, sysinfo::Cpu::frequency),
        },
        memory: memory_of(sys.as_ref()),
        disks: disks(),
        nics: nics(),
    }
}

fn disks() -> Vec<DiskInfo> {
    let Ok(disks) = Disks::new_with_refreshed_list() else {
        return Vec::new();
    };
    disks
        .iter()
        .map(|d| DiskInfo {
            mount_point: d.mount_point().display().to_string(),
            name: d.name().to_string_lossy().into_owned(),
            file_system: d.file_system().to_string_lossy().into_owned(),
            kind: d.kind().to_string(),
            total_bytes: d.total_space(),
            free_bytes: d.available_space(),
            removable: d.is_removable(),
            read_only: d.is_read_only(),
        })
        .collect()
}

fn nics() -> Vec<NicInfo> {
    let Ok(networks) = Networks::new_with_refreshed_list() else {
        return Vec::new();
    };
    let mut out: Vec<NicInfo> = networks
        .iter()
        .map(|(name, n)| NicInfo {
            name: name.to_string_lossy().into_owned(),
            mac: n.mac_address().to_string(),
            addresses: n
                .ip_networks()
                .iter()
                .map(|ip| format!("{}/{}", ip.addr, ip.prefix))
                .collect(),
            mtu: n.mtu(),
            link_speed_bps: n.receive_link_speed(),
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Total and free bytes of the disk that holds `path` (the longest matching mount point).
pub fn volume_of(path: &Path) -> Option<(u64, u64)> {
    // `canonicalize` would add a `\\?\` prefix on Windows that mount points don't have.
    let path = if cfg!(windows) {
        std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
    } else {
        path.ancestors()
            .find_map(|p| p.canonicalize().ok())
            .unwrap_or_else(|| path.to_path_buf())
    };
    let disks = Disks::new_with_refreshed_list().ok()?;
    disks
        .iter()
        .filter(|d| path.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| (d.total_space(), d.available_space()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_reports_real_cpu_memory_and_disks() {
        let h = host();
        assert!(h.cpu.logical_cores > 0);
        assert!(h.memory.total_bytes > 0);
        assert!(!h.os.arch.is_empty());
        let dir = tempfile::tempdir().unwrap();
        if !h.disks.is_empty() {
            let (total, free) = volume_of(dir.path()).expect("temp dir is on a disk");
            assert!(total > 0 && free <= total);
        }
    }
}
