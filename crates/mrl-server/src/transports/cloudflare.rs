//! Cloudflare Tunnel: `cloudflared` on the machine exposes the agent's port under a hostname;
//! the core runs `cloudflared access tcp` locally and connects through it. TLS stays end to end
//! (Cloudflare only carries bytes), so the node certificate still identifies the machine.

use super::{codes, BoxIo, ConnectionReport, Hold, Transport, TransportError, TransportKind};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

/// Through `cloudflared access tcp --hostname <hostname>`.
pub struct CloudflareTunnel {
    hostname: String,
    program: PathBuf,
}

impl CloudflareTunnel {
    /// The tunnel at `hostname`; `program` is the `cloudflared` command.
    pub fn new(hostname: &str, program: PathBuf) -> Self {
        Self {
            hostname: hostname.to_owned(),
            program,
        }
    }
}

/// `cloudflared --version`, or a plain explanation.
pub async fn version(program: &Path) -> Result<String, TransportError> {
    let run = Command::new(program)
        .arg("--version")
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(Duration::from_secs(5), run).await {
        Ok(Ok(out)) if out.status.success() => {
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
        }
        Ok(Ok(_)) => Err(TransportError::Unreachable(format!(
            "{} is not a working cloudflared program",
            program.display()
        ))),
        _ => Err(TransportError::Unreachable(
            "cloudflared is not installed on this computer; install it from Cloudflare's downloads page".to_owned(),
        )),
    }
}

/// Starts `cloudflared access tcp` on a free loopback port and connects to it. The process
/// lives as long as the returned stream.
pub async fn open(program: &Path, hostname: &str) -> Result<BoxIo, TransportError> {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|e| TransportError::Unreachable(format!("no free local port: {e}")))?;
        l.local_addr()
            .map_err(|e| TransportError::Unreachable(format!("no free local port: {e}")))?
            .port()
    };
    let local = format!("127.0.0.1:{port}");
    let mut child: Child = Command::new(program)
        .args(["access", "tcp", "--hostname", hostname, "--url", &local])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| {
            TransportError::Unreachable(
                "cloudflared is not installed on this computer; install it from Cloudflare's downloads page".to_owned(),
            )
        })?;
    let started = Instant::now();
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(TransportError::Unreachable(format!(
                "cloudflared stopped right away ({status}); check that {hostname} is a tunnel you may use and that `cloudflared login` was run"
            )));
        }
        if let Ok(s) = tokio::net::TcpStream::connect(&local).await {
            let _ = s.set_nodelay(true);
            return Ok(Box::new(Hold::new(s, child)));
        }
        if started.elapsed() > super::CONNECT_TIMEOUT {
            return Err(TransportError::Unreachable(format!(
                "cloudflared did not open the tunnel to {hostname} within {} s",
                super::CONNECT_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[async_trait::async_trait]
impl Transport for CloudflareTunnel {
    fn kind(&self) -> TransportKind {
        TransportKind::CloudflareTunnel
    }

    fn target(&self) -> String {
        self.hostname.clone()
    }

    async fn connect(&self) -> Result<BoxIo, TransportError> {
        open(&self.program, &self.hostname).await
    }

    async fn connection_test(&self) -> ConnectionReport {
        let mut report = ConnectionReport::new(self.kind(), self.target());
        match version(&self.program).await {
            Ok(v) => report.pass("cloudflared", v),
            Err(e) => {
                report.fail("cloudflared", codes::UNREACHABLE, e.to_string());
                return report;
            }
        }
        let found = tokio::net::lookup_host((self.hostname.as_str(), 443))
            .await
            .map(|a| a.count())
            .unwrap_or(0);
        match found {
            1.. => report.pass("dns", format!("found {}", self.hostname)),
            0 => {
                report.fail(
                    "dns",
                    codes::UNREACHABLE,
                    format!(
                        "could not find {}; check the tunnel's hostname in the Cloudflare dashboard",
                        self.hostname
                    ),
                );
                return report;
            }
        }
        match open(&self.program, &self.hostname).await {
            Ok(_) => report.pass(
                "tunnel",
                format!("cloudflared opened the tunnel to {}", self.hostname),
            ),
            Err(e) => report.fail("tunnel", codes::UNREACHABLE, e.to_string()),
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn missing_cloudflared_is_explained() {
        let t = CloudflareTunnel::new(
            "agent.example.com",
            PathBuf::from("/no/such/cloudflared-mrl-test"),
        );
        let r = t.connection_test().await;
        assert!(!r.ok);
        assert_eq!(r.steps.len(), 1);
        assert!(
            r.problem()
                .unwrap()
                .contains("cloudflared is not installed"),
            "{r:?}"
        );
        let e = t.connect().await.err().unwrap();
        assert_eq!(e.code(), codes::UNREACHABLE);
    }
}
