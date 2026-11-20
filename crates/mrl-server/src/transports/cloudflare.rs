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

/// A complete TLS record holding an empty ClientHello, which no TLS server accepts: every TLS
/// server answers it (an alert), while a tunnel whose far end is down closes or stays silent.
const PROBE: &[u8] = &[0x16, 0x03, 0x01, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00];

/// Ok when something behind `io` answers: the gateway and the agent are TLS servers, so a reply
/// to [`PROBE`] proves the tunnel reaches one.
pub async fn answers(mut io: BoxIo, wait: Duration) -> Result<(), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let talk = async {
        io.write_all(PROBE).await?;
        io.flush().await?;
        let mut b = [0u8; 1];
        io.read(&mut b).await
    };
    match tokio::time::timeout(wait, talk).await {
        Ok(Ok(n)) if n > 0 => Ok(()),
        Ok(Ok(_)) => Err("the tunnel closed without an answer".to_owned()),
        Ok(Err(e)) => Err(format!("the tunnel failed: {e}")),
        Err(_) => Err(format!("no answer within {} s", wait.as_secs())),
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
            Err(e) => report.fail("tunnel", codes::UNREACHABLE, e.to_string()),
            Ok(io) => match answers(io, Duration::from_secs(10)).await {
                Ok(()) => report.pass(
                    "tunnel",
                    format!("the machine answered through the tunnel to {}", self.hostname),
                ),
                Err(why) => report.fail(
                    "tunnel",
                    codes::UNREACHABLE,
                    format!(
                        "cloudflared runs, but nothing answered behind {} ({why}); check that the machine's tunnel is running and points at the agent's port",
                        self.hostname
                    ),
                ),
            },
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn probe_against(
        serve: impl FnOnce(
                tokio::net::TcpStream,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + 'static,
    ) -> Result<(), String> {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            serve(s).await;
        });
        let io = tokio::net::TcpStream::connect(addr).await.unwrap();
        answers(Box::new(io), Duration::from_secs(2)).await
    }

    #[tokio::test]
    async fn the_tunnel_test_needs_an_answer_from_behind_the_tunnel() {
        // A TLS server behind the tunnel answers the probe.
        let cert = rcgen::generate_simple_self_signed(vec!["x".to_owned()]).unwrap();
        let config = tokio_rustls::rustls::ServerConfig::builder_with_provider(
            std::sync::Arc::new(tokio_rustls::rustls::crypto::ring::default_provider()),
        )
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                cert.signing_key.serialize_der().into(),
            ),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
        let tls = probe_against(move |s| {
            Box::pin(async move {
                let _ = acceptor.accept(s).await;
            })
        })
        .await;
        assert_eq!(tls, Ok(()));
        // The origin is down: cloudflared accepts locally, then closes (or never answers).
        let closed = probe_against(|s| Box::pin(async move { drop(s) })).await;
        assert!(closed.is_err(), "{closed:?}");
        let silent = probe_against(|s| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                drop(s);
            })
        })
        .await;
        assert!(silent.unwrap_err().contains("no answer"));
    }

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
