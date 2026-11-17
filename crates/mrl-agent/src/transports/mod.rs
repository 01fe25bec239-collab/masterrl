//! How the agent reaches the core, or waits for the core to reach it.
//!
//! Every [`Transport`] yields one plain byte stream per connection; mutual TLS runs on top, so
//! the transport never decides who is trusted. Reconnecting (`session`) is the same for all.
//!
//! | Transport | `gateway.transport` | `connect()` |
//! |---|---|---|
//! | Local network, agent dials | `direct` | TCP to `gateway.server` |
//! | Tailscale | `tailscale` | TCP to the core's Tailscale address (with `tailscale status` hints) |
//! | Cloudflare Tunnel | `cloudflare_tunnel` | `cloudflared access tcp` to the core's tunnel hostname |
//! | Core dials (LAN, SSH forward, tunnel to this machine) | `gateway.listen` set | waits for the core on that address |

use crate::session::LinkError;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Longest a transport waits to open its connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A connected byte stream.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
/// A boxed [`Io`].
pub type BoxIo = Box<dyn Io>;

/// One way of reaching the core.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    /// Which transport.
    fn kind(&self) -> TransportKind;
    /// What it connects to, for messages.
    fn target(&self) -> String;
    /// True when the core dials (this side is the TLS server).
    fn waits_for_core(&self) -> bool {
        false
    }
    /// One connection to the core (for [`Listen`], the next one the core opens).
    async fn connect(&self) -> Result<BoxIo, LinkError>;
    /// Explains step by step, in plain words, whether the transport works. Never fails.
    async fn connection_test(&self) -> ConnectionReport;
}

/// `gateway.transport`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    /// Local network.
    #[default]
    Direct,
    /// The core forwards over SSH (the agent listens on loopback).
    Ssh,
    /// Tailscale addresses.
    Tailscale,
    /// Cloudflare Tunnel hostname.
    CloudflareTunnel,
}

impl TransportKind {
    /// For `AgentHello.transport`.
    pub fn to_proto(self) -> mrl_proto::agent::v1::Transport {
        use mrl_proto::agent::v1::Transport as T;
        match self {
            Self::Direct => T::Direct,
            Self::Ssh => T::Ssh,
            Self::Tailscale => T::Tailscale,
            Self::CloudflareTunnel => T::CloudflareTunnel,
        }
    }
}

/// One step of a connection test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Check {
    /// Step name, like `tcp`.
    pub name: String,
    /// Whether it worked.
    pub ok: bool,
    /// What happened.
    pub detail: String,
}

/// The result of [`Transport::connection_test`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionReport {
    /// Transport tested.
    pub transport: TransportKind,
    /// Target.
    pub target: String,
    /// Every step worked.
    pub ok: bool,
    /// Steps; a failing one is last.
    pub steps: Vec<Check>,
    /// Code of the failing step.
    pub code: Option<String>,
}

impl ConnectionReport {
    fn new(transport: TransportKind, target: String) -> Self {
        Self {
            transport,
            target,
            ok: true,
            steps: Vec::new(),
            code: None,
        }
    }

    fn pass(&mut self, name: &str, detail: impl Into<String>) {
        self.steps.push(Check {
            name: name.into(),
            ok: true,
            detail: detail.into(),
        });
    }

    fn fail(&mut self, name: &str, code: &str, detail: impl Into<String>) {
        self.ok = false;
        self.code.get_or_insert_with(|| code.to_owned());
        self.steps.push(Check {
            name: name.into(),
            ok: false,
            detail: detail.into(),
        });
    }

    /// The failing step's detail.
    pub fn problem(&self) -> Option<&str> {
        self.steps.iter().find(|s| !s.ok).map(|s| s.detail.as_str())
    }
}

/// Code when the core can't be reached.
pub const UNREACHABLE: &str = "MRL-LINK-184";
/// A firewall seems to block the port (base catalog).
pub const FIREWALL: &str = "MRL-LINK-004";

fn split(v: &str) -> Option<(String, u16)> {
    if let Ok(a) = v.parse::<SocketAddr>() {
        return Some((a.ip().to_string(), a.port()));
    }
    let (h, p) = v.rsplit_once(':')?;
    let p = p.parse::<u16>().ok().filter(|p| *p != 0)?;
    (!h.is_empty() && !h.contains(':')).then(|| (h.to_owned(), p))
}

async fn tcp(address: &str) -> Result<tokio::net::TcpStream, LinkError> {
    let (host, port) = split(address)
        .ok_or_else(|| LinkError::Unreachable(format!("{address:?} is not a host:port address")))?;
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map(Iterator::collect)
        .unwrap_or_default();
    if addrs.is_empty() {
        return Err(LinkError::Unreachable(format!(
            "could not find {host} on the network; check the name, or use the server's IP address"
        )));
    }
    let mut last = String::new();
    for a in addrs {
        match tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect(a)).await {
            Ok(Ok(s)) => {
                let _ = s.set_nodelay(true);
                return Ok(s);
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                last = format!("{host} answered, but nothing listens on port {port}; is the MasterRL app running there?");
            }
            Ok(Err(e)) => last = format!("could not connect to {a}: {e}"),
            Err(_) => {
                last = format!(
                    "{host} did not answer on port {port} within {} s; a firewall may be blocking the port ({FIREWALL})",
                    CONNECT_TIMEOUT.as_secs()
                );
            }
        }
    }
    Err(LinkError::Unreachable(last))
}

async fn probe(address: &str, r: &mut ConnectionReport) {
    match tcp(address).await {
        Ok(_) => r.pass("tcp", format!("{address} accepts connections")),
        Err(e) => {
            let msg = e.to_string();
            let code = if msg.contains(FIREWALL) {
                FIREWALL
            } else {
                UNREACHABLE
            };
            r.fail("tcp", code, msg);
        }
    }
}

/// TCP to `host:port`.
pub struct Direct {
    address: String,
}

impl Direct {
    /// The core at `address`.
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_owned(),
        }
    }
}

#[async_trait::async_trait]
impl Transport for Direct {
    fn kind(&self) -> TransportKind {
        TransportKind::Direct
    }
    fn target(&self) -> String {
        self.address.clone()
    }
    async fn connect(&self) -> Result<BoxIo, LinkError> {
        Ok(Box::new(tcp(&self.address).await?))
    }
    async fn connection_test(&self) -> ConnectionReport {
        let mut r = ConnectionReport::new(self.kind(), self.target());
        probe(&self.address, &mut r).await;
        r
    }
}

/// What `tailscale status --json` says.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TailscaleStatus {
    /// The program ran.
    pub installed: bool,
    /// Connected (`BackendState` is `Running`).
    pub running: bool,
    /// Raw state.
    pub state: String,
    /// This computer's Tailscale addresses.
    pub addresses: Vec<String>,
}

/// Parses `tailscale status --json`.
pub fn parse_tailscale(json: &str) -> Option<TailscaleStatus> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let state = v["BackendState"].as_str().unwrap_or_default().to_owned();
    Some(TailscaleStatus {
        installed: true,
        running: state == "Running",
        state,
        addresses: v["Self"]["TailscaleIPs"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

/// Runs `tailscale status --json` (5 s limit).
pub async fn tailscale_status(program: &Path) -> TailscaleStatus {
    let run = tokio::process::Command::new(program)
        .args(["status", "--json"])
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(Duration::from_secs(5), run).await {
        Ok(Ok(out)) => {
            parse_tailscale(&String::from_utf8_lossy(&out.stdout)).unwrap_or(TailscaleStatus {
                installed: true,
                ..TailscaleStatus::default()
            })
        }
        _ => TailscaleStatus::default(),
    }
}

/// TCP to the core's Tailscale address.
pub struct Tailscale {
    address: String,
    program: PathBuf,
}

impl Tailscale {
    /// The core at `address`; `program` is `tailscale`.
    pub fn new(address: &str, program: PathBuf) -> Self {
        Self {
            address: address.to_owned(),
            program,
        }
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
    async fn connect(&self) -> Result<BoxIo, LinkError> {
        Ok(Box::new(tcp(&self.address).await?))
    }
    async fn connection_test(&self) -> ConnectionReport {
        let mut r = ConnectionReport::new(self.kind(), self.target());
        let s = tailscale_status(&self.program).await;
        if !s.installed {
            r.fail(
                "tailscale",
                UNREACHABLE,
                "Tailscale is not installed on this machine; install it from tailscale.com and sign in to the same tailnet as the app",
            );
            return r;
        }
        if !s.running {
            r.fail(
                "tailscale",
                UNREACHABLE,
                format!(
                    "Tailscale is installed but not connected (state {:?}); run `tailscale up`",
                    s.state
                ),
            );
            return r;
        }
        r.pass(
            "tailscale",
            format!("connected as {}", s.addresses.join(", ")),
        );
        probe(&self.address, &mut r).await;
        r
    }
}

/// Through `cloudflared access tcp --hostname <hostname>`.
pub struct CloudflareTunnel {
    hostname: String,
    program: PathBuf,
}

impl CloudflareTunnel {
    /// The core's tunnel at `hostname`.
    pub fn new(hostname: &str, program: PathBuf) -> Self {
        Self {
            hostname: hostname.to_owned(),
            program,
        }
    }

    async fn open(&self) -> Result<BoxIo, LinkError> {
        let missing = || {
            LinkError::Unreachable(
                "cloudflared is not installed on this machine; install it from Cloudflare's downloads page".into(),
            )
        };
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| LinkError::Unreachable(format!("no free local port: {e}")))?
            .port();
        let local = format!("127.0.0.1:{port}");
        let mut child = tokio::process::Command::new(&self.program)
            .args([
                "access",
                "tcp",
                "--hostname",
                &self.hostname,
                "--url",
                &local,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| missing())?;
        let start = Instant::now();
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(LinkError::Unreachable(format!(
                    "cloudflared stopped right away ({status}); check the hostname {}",
                    self.hostname
                )));
            }
            if let Ok(s) = tokio::net::TcpStream::connect(&local).await {
                return Ok(Box::new(Hold {
                    io: s,
                    _guard: child,
                }));
            }
            if start.elapsed() > CONNECT_TIMEOUT {
                return Err(LinkError::Unreachable(format!(
                    "cloudflared did not open the tunnel to {} within {} s",
                    self.hostname,
                    CONNECT_TIMEOUT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
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
    async fn connect(&self) -> Result<BoxIo, LinkError> {
        self.open().await
    }
    async fn connection_test(&self) -> ConnectionReport {
        let mut r = ConnectionReport::new(self.kind(), self.target());
        let version = tokio::process::Command::new(&self.program)
            .arg("--version")
            .kill_on_drop(true)
            .output();
        match tokio::time::timeout(Duration::from_secs(5), version).await {
            Ok(Ok(o)) if o.status.success() => {
                r.pass(
                    "cloudflared",
                    String::from_utf8_lossy(&o.stdout).trim().to_owned(),
                );
            }
            _ => {
                r.fail(
                    "cloudflared",
                    UNREACHABLE,
                    "cloudflared is not installed on this machine; install it from Cloudflare's downloads page",
                );
                return r;
            }
        }
        match self.open().await {
            Ok(_) => r.pass(
                "tunnel",
                format!("cloudflared opened the tunnel to {}", self.hostname),
            ),
            Err(e) => r.fail("tunnel", UNREACHABLE, e.to_string()),
        }
        r
    }
}

/// Waits for the core to dial in (LAN, an SSH forward to loopback, or a tunnel to this machine).
pub struct Listen {
    addr: SocketAddr,
    kind: TransportKind,
    listener: tokio::sync::Mutex<Option<tokio::net::TcpListener>>,
}

impl Listen {
    /// Listens on `addr` (bound on first use). `kind` is reported in `AgentHello.transport`.
    pub fn new(addr: SocketAddr, kind: TransportKind) -> Self {
        Self {
            addr,
            kind,
            listener: tokio::sync::Mutex::new(None),
        }
    }

    /// The bound address (after the first `connect`, or `bind`).
    pub async fn bind(&self) -> Result<SocketAddr, LinkError> {
        let mut slot = self.listener.lock().await;
        if slot.is_none() {
            let l = tokio::net::TcpListener::bind(self.addr).await.map_err(|e| {
                LinkError::Unreachable(format!(
                    "could not listen on {}: {e}; another program may use the port, or pick another `gateway.listen`",
                    self.addr
                ))
            })?;
            *slot = Some(l);
        }
        slot.as_ref()
            .and_then(|l| l.local_addr().ok())
            .ok_or_else(|| LinkError::Unreachable("the listener has no address".into()))
    }
}

#[async_trait::async_trait]
impl Transport for Listen {
    fn kind(&self) -> TransportKind {
        self.kind
    }
    fn target(&self) -> String {
        format!("listening on {}", self.addr)
    }
    fn waits_for_core(&self) -> bool {
        true
    }
    async fn connect(&self) -> Result<BoxIo, LinkError> {
        self.bind().await?;
        let slot = self.listener.lock().await;
        let l = slot
            .as_ref()
            .ok_or_else(|| LinkError::Unreachable("the listener is not open".into()))?;
        let (s, _) = l.accept().await.map_err(|e| {
            LinkError::Unreachable(format!("could not accept the core's connection: {e}"))
        })?;
        let _ = s.set_nodelay(true);
        Ok(Box::new(s))
    }
    async fn connection_test(&self) -> ConnectionReport {
        let mut r = ConnectionReport::new(self.kind, self.target());
        match self.bind().await {
            Ok(a) => {
                r.pass("listen", format!("waiting for the core on {a}"));
                if a.ip().is_loopback() && self.kind == TransportKind::Direct {
                    r.pass(
                        "reach",
                        "only this machine can connect (loopback); use 0.0.0.0 for the local network, or the SSH transport",
                    );
                }
            }
            Err(e) => r.fail("listen", UNREACHABLE, e.to_string()),
        }
        r
    }
}

/// A stream that keeps a helper process alive.
struct Hold<T, G> {
    io: T,
    _guard: G,
}

impl<T: AsyncRead + Unpin, G: Unpin> AsyncRead for Hold<T, G> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin, G: Unpin> AsyncWrite for Hold<T, G> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn direct_reports_refused_and_ok() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        assert!(Direct::new(&addr).connection_test().await.ok);
        drop(l);
        // Port 1 (tcpmux) is closed on every test machine; a freed random port may be reused.
        let r = Direct::new("127.0.0.1:1").connection_test().await;
        assert!(!r.ok);
        assert!(r.problem().unwrap().contains("nothing listens"), "{r:?}");
        assert_eq!(r.code.as_deref(), Some(UNREACHABLE));
        let r = Direct::new("no-such-host.invalid:9443")
            .connection_test()
            .await;
        assert!(r.problem().unwrap().contains("could not find"));
    }

    #[tokio::test]
    async fn tailscale_and_cloudflared_missing_are_explained() {
        let t = Tailscale::new("100.64.0.1:9443", PathBuf::from("/no/such/tailscale-mrl"));
        assert!(t
            .connection_test()
            .await
            .problem()
            .unwrap()
            .contains("not installed"));
        let c = CloudflareTunnel::new(
            "core.example.com",
            PathBuf::from("/no/such/cloudflared-mrl"),
        );
        let r = c.connection_test().await;
        assert!(r
            .problem()
            .unwrap()
            .contains("cloudflared is not installed"));
        assert!(c.connect().await.is_err());
        let s =
            parse_tailscale(r#"{"BackendState":"Running","Self":{"TailscaleIPs":["100.64.0.9"]}}"#)
                .unwrap();
        assert!(s.running && s.addresses == ["100.64.0.9"]);
    }

    #[tokio::test]
    async fn listen_accepts_the_core_and_explains_a_taken_port() {
        let l = Listen::new("127.0.0.1:0".parse().unwrap(), TransportKind::Ssh);
        let addr = l.bind().await.unwrap();
        let r = l.connection_test().await;
        assert!(r.ok, "{r:?}");
        let dial = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await });
        assert!(l.connect().await.is_ok());
        assert!(dial.await.unwrap().is_ok());
        let taken = Listen::new(addr, TransportKind::Direct);
        let r = taken.connection_test().await;
        assert!(
            !r.ok && r.problem().unwrap().contains("could not listen"),
            "{r:?}"
        );
    }
}
