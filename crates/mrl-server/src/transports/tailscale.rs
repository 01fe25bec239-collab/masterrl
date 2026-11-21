//! Tailscale: TCP to the machine's Tailscale address, plus `tailscale status` for hints.

use super::direct::probe;
use super::{
    codes, split_host_port, tcp_connect, BoxIo, ConnectionReport, Transport, TransportError,
    TransportKind,
};
use serde::Serialize;
use std::path::PathBuf;
use std::time::Duration;

/// TCP to a Tailscale address (`100.x.y.z:port` or `name.tailnet.ts.net:port`).
pub struct Tailscale {
    address: String,
    program: PathBuf,
}

impl Tailscale {
    /// The agent at `address`; `program` is the `tailscale` command used for hints.
    pub fn new(address: &str, program: PathBuf) -> Self {
        Self {
            address: address.to_owned(),
            program,
        }
    }
}

/// What `tailscale status --json` says about this computer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct TailscaleStatus {
    /// The `tailscale` program ran.
    pub installed: bool,
    /// Connected to a tailnet (`BackendState` is `Running`).
    pub running: bool,
    /// The raw `BackendState`, like `Running` or `NeedsLogin`.
    pub state: String,
    /// This computer's Tailscale addresses.
    pub addresses: Vec<String>,
    /// Peer names and addresses (DNS names without the trailing dot, host names, IPs).
    pub peers: Vec<String>,
}

impl TailscaleStatus {
    /// True when `host` is this tailnet's peer (or this computer).
    pub fn knows(&self, host: &str) -> bool {
        let h = host.trim_end_matches('.').to_ascii_lowercase();
        self.addresses.iter().chain(&self.peers).any(|p| {
            let p = p.to_ascii_lowercase();
            p == h || p.split('.').next() == Some(h.as_str())
        })
    }
}

/// Parses `tailscale status --json`.
pub fn parse_status(json: &str) -> Option<TailscaleStatus> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let strings = |x: &serde_json::Value| -> Vec<String> {
        x.as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    };
    let state = v["BackendState"].as_str().unwrap_or_default().to_owned();
    let mut peers = Vec::new();
    if let Some(map) = v["Peer"].as_object() {
        for p in map.values() {
            for key in ["DNSName", "HostName"] {
                if let Some(n) = p[key].as_str().filter(|n| !n.is_empty()) {
                    peers.push(n.trim_end_matches('.').to_owned());
                }
            }
            peers.extend(strings(&p["TailscaleIPs"]));
        }
    }
    Some(TailscaleStatus {
        installed: true,
        running: state == "Running",
        state,
        addresses: strings(&v["Self"]["TailscaleIPs"]),
        peers,
    })
}

/// Runs `tailscale status --json` (5 s limit). Not installed → `installed: false`.
pub async fn detect(program: &std::path::Path) -> TailscaleStatus {
    let run = tokio::process::Command::new(program)
        .args(["status", "--json"])
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(Duration::from_secs(5), run).await {
        // A stopped Tailscale exits non-zero but still prints the JSON.
        Ok(Ok(out)) => {
            parse_status(&String::from_utf8_lossy(&out.stdout)).unwrap_or(TailscaleStatus {
                installed: true,
                ..TailscaleStatus::default()
            })
        }
        _ => TailscaleStatus::default(),
    }
}

#[async_trait::async_trait]
impl Transport for Tailscale {
    fn kind(&self) -> TransportKind {
        TransportKind::Tailscale
    }

    fn target(&self) -> String {
        self.address.clone()
    }

    async fn connect(&self) -> Result<BoxIo, TransportError> {
        Ok(Box::new(tcp_connect(&self.address).await?))
    }

    async fn connection_test(&self) -> ConnectionReport {
        let mut report = ConnectionReport::new(self.kind(), self.target());
        let status = detect(&self.program).await;
        if !status.installed {
            report.fail(
                "tailscale",
                codes::UNREACHABLE,
                "Tailscale is not installed on this computer; install it from tailscale.com and sign in on both machines",
            );
            return report;
        }
        if !status.running {
            report.fail(
                "tailscale",
                codes::UNREACHABLE,
                format!(
                    "Tailscale is installed but not connected (state {:?}); open Tailscale and sign in",
                    status.state
                ),
            );
            return report;
        }
        let host = split_host_port(&self.address)
            .map(|(h, _)| h)
            .unwrap_or_default();
        if status.knows(&host) {
            report.pass("tailscale", format!("{host} is in your tailnet"));
        } else {
            report.fail(
                "tailscale",
                codes::UNREACHABLE,
                format!(
                    "{host} is not one of your Tailscale machines; sign in to the same tailnet on that machine, or use its 100.x address"
                ),
            );
            return report;
        }
        probe(&self.address, &mut report).await;
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS: &str = r#"{
      "BackendState": "Running",
      "Self": {"TailscaleIPs": ["100.64.0.1", "fd7a::1"], "DNSName": "laptop.tail1.ts.net."},
      "Peer": {"nodekey:abc": {"DNSName": "gpu-box.tail1.ts.net.", "HostName": "gpu-box",
                               "TailscaleIPs": ["100.64.0.2"]}}
    }"#;

    #[test]
    fn parses_status_and_finds_peers() {
        let s = parse_status(STATUS).unwrap();
        assert!(s.installed && s.running);
        assert_eq!(s.addresses, ["100.64.0.1", "fd7a::1"]);
        assert!(s.knows("gpu-box"));
        assert!(s.knows("gpu-box.tail1.ts.net"));
        assert!(s.knows("100.64.0.2"));
        assert!(!s.knows("other"));
        let stopped = parse_status(r#"{"BackendState":"NeedsLogin"}"#).unwrap();
        assert!(!stopped.running);
        assert!(parse_status("not json").is_none());
    }

    #[tokio::test]
    async fn missing_program_says_install_tailscale() {
        let t = Tailscale::new(
            "100.64.0.2:7443",
            PathBuf::from("/no/such/tailscale-mrl-test"),
        );
        let r = t.connection_test().await;
        assert!(!r.ok);
        assert!(r.problem().unwrap().contains("not installed"), "{r:?}");
    }
}
