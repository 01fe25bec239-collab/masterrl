//! How the core reaches a machine whose agent waits for the core to dial in.
//!
//! Every [`Transport`] returns a plain byte stream; the mutual-TLS gateway session runs on top,
//! so security never depends on the transport (identity is the node certificate). Reconnecting
//! lives in the dialer (`grpc::dialer`), not here, so it is the same for every transport.
//!
//! | Transport | [`Endpoint`] | How |
//! |---|---|---|
//! | Local network | `direct` | TCP to the agent's `gateway.listen` address |
//! | SSH | `ssh` | SSH session (key from the secret store, the SSH agent or `~/.ssh`), then a `direct-tcpip` channel to the agent's loopback port |
//! | Tailscale | `tailscale` | TCP to the machine's Tailscale address, with `tailscale status` hints |
//! | Cloudflare Tunnel | `cloudflare_tunnel` | `cloudflared access tcp` to the machine's tunnel hostname |

pub mod cloudflare;
pub mod direct;
pub mod ssh;
pub mod tailscale;

use crate::seams::SecretStore;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Longest a transport waits to open its connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A connected byte stream.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A boxed [`Io`].
pub type BoxIo = Box<dyn Io>;

/// One way of reaching a machine.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    /// Which transport this is.
    fn kind(&self) -> TransportKind;
    /// What it connects to, for messages, like `ssh gpu-box → 127.0.0.1:7443`.
    fn target(&self) -> String;
    /// Opens a byte stream to the agent.
    async fn connect(&self) -> Result<BoxIo, TransportError>;
    /// Tries to connect and explains, step by step and in plain words, what works and what
    /// doesn't. Never fails: problems are in the report.
    async fn connection_test(&self) -> ConnectionReport;
}

/// The transports, as stored and shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    /// Local network, either side dials.
    Direct,
    /// Through an SSH session the core opens.
    Ssh,
    /// Over Tailscale addresses.
    Tailscale,
    /// Through a Cloudflare Tunnel hostname.
    CloudflareTunnel,
}

impl TransportKind {
    /// The protobuf value sent in `AgentHello.transport`.
    pub fn to_proto(self) -> mrl_proto::agent::v1::Transport {
        use mrl_proto::agent::v1::Transport as T;
        match self {
            Self::Direct => T::Direct,
            Self::Ssh => T::Ssh,
            Self::Tailscale => T::Tailscale,
            Self::CloudflareTunnel => T::CloudflareTunnel,
        }
    }

    /// The protobuf value, back to a kind (`None` when unspecified).
    pub fn from_proto(value: i32) -> Option<Self> {
        use mrl_proto::agent::v1::Transport as T;
        match T::try_from(value).ok()? {
            T::Direct => Some(Self::Direct),
            T::Ssh => Some(Self::Ssh),
            T::Tailscale => Some(Self::Tailscale),
            T::CloudflareTunnel => Some(Self::CloudflareTunnel),
            T::Unspecified => None,
        }
    }

    /// The stored name, like `cloudflare_tunnel`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Ssh => "ssh",
            Self::Tailscale => "tailscale",
            Self::CloudflareTunnel => "cloudflare_tunnel",
        }
    }
}

/// How the core dials one machine (stored in `compute_nodes.endpoint`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Endpoint {
    /// The agent listens on this `host:port` in the local network.
    Direct {
        /// `host:port`.
        address: String,
    },
    /// The core opens SSH to `host` and forwards to the agent's loopback port.
    Ssh {
        /// Host name, address or `~/.ssh/config` alias.
        host: String,
        /// SSH port (default: from `~/.ssh/config`, else 22).
        port: Option<u16>,
        /// SSH user (default: from `~/.ssh/config`, else this account's name).
        user: Option<String>,
        /// The agent's `gateway.listen` port on the machine's loopback.
        agent_port: u16,
        /// Secret-store name of an OpenSSH private key (optional; the SSH agent and
        /// `~/.ssh` keys are tried too).
        key_secret: Option<String>,
    },
    /// The agent listens on this Tailscale `address:port` (100.x or MagicDNS name).
    Tailscale {
        /// `host:port`.
        address: String,
    },
    /// The agent is exposed by `cloudflared` on this hostname.
    CloudflareTunnel {
        /// The tunnel's public hostname.
        hostname: String,
    },
}

impl Endpoint {
    /// The transport for this endpoint.
    pub fn transport(&self, env: &TransportEnv) -> Box<dyn Transport> {
        match self {
            Self::Direct { address } => Box::new(direct::Direct::new(address)),
            Self::Tailscale { address } => Box::new(tailscale::Tailscale::new(
                address,
                env.tailscale_program.clone(),
            )),
            Self::CloudflareTunnel { hostname } => Box::new(cloudflare::CloudflareTunnel::new(
                hostname,
                env.cloudflared_program.clone(),
            )),
            Self::Ssh {
                host,
                port,
                user,
                agent_port,
                key_secret,
            } => Box::new(ssh::Ssh::new(
                ssh::SshTarget::resolve(host, *port, user.as_deref(), env.ssh_config.as_deref()),
                *agent_port,
                key_secret.clone(),
                env.known_hosts.clone(),
                env.secrets.clone(),
            )),
        }
    }

    /// Which transport it uses.
    pub fn kind(&self) -> TransportKind {
        match self {
            Self::Direct { .. } => TransportKind::Direct,
            Self::Ssh { .. } => TransportKind::Ssh,
            Self::Tailscale { .. } => TransportKind::Tailscale,
            Self::CloudflareTunnel { .. } => TransportKind::CloudflareTunnel,
        }
    }

    /// Problems with the values (empty when usable).
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        let host_port = |v: &str, field: &str, out: &mut Vec<String>| {
            if split_host_port(v).is_none() {
                out.push(format!("{field}: {v:?} must look like host:port"));
            }
        };
        match self {
            Self::Direct { address } => host_port(address, "endpoint.address", &mut out),
            Self::Tailscale { address } => host_port(address, "endpoint.address", &mut out),
            Self::CloudflareTunnel { hostname } => {
                if hostname.is_empty() || hostname.contains(['/', ':', ' ']) {
                    out.push(format!(
                        "endpoint.hostname: {hostname:?} must be a plain hostname"
                    ));
                }
            }
            Self::Ssh {
                host, agent_port, ..
            } => {
                if host.is_empty() || host.contains(char::is_whitespace) {
                    out.push("endpoint.host: give the SSH host or alias".to_owned());
                }
                if *agent_port == 0 {
                    out.push("endpoint.agent_port: must be 1–65535".to_owned());
                }
            }
        }
        out
    }
}

/// What transports need from the core: where trusted SSH host keys live, the SSH config, the
/// secret store and the helper programs.
#[derive(Clone)]
pub struct TransportEnv {
    /// The core's own `known_hosts` file (first-use confirmations are written here).
    pub known_hosts: PathBuf,
    /// `~/.ssh/config` (or another file) for host aliases; `None` to skip.
    pub ssh_config: Option<PathBuf>,
    /// Holds SSH private keys named by `key_secret`.
    pub secrets: Arc<dyn SecretStore>,
    /// The `tailscale` program.
    pub tailscale_program: PathBuf,
    /// The `cloudflared` program.
    pub cloudflared_program: PathBuf,
}

/// Why a transport could not connect.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The machine or program could not be reached.
    #[error("{0}")]
    Unreachable(String),
    /// First SSH connection to this host: its key must be confirmed once.
    #[error("this is the first SSH connection to {host}; confirm that its key fingerprint is {fingerprint} before connecting")]
    HostKeyUnknown {
        /// `host:port`.
        host: String,
        /// `SHA256:…`, as `ssh-keygen -l` prints it.
        fingerprint: String,
    },
    /// The SSH host key differs from the confirmed one.
    #[error("the SSH key of {host} changed since you confirmed it (known_hosts line {line}); someone may be intercepting the connection")]
    HostKeyChanged {
        /// `host:port`.
        host: String,
        /// Line in the core's `known_hosts`.
        line: usize,
    },
}

impl TransportError {
    /// The catalogued code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unreachable(_) => codes::UNREACHABLE,
            Self::HostKeyUnknown { .. } => codes::HOST_KEY_UNKNOWN,
            Self::HostKeyChanged { .. } => codes::HOST_KEY_CHANGED,
        }
    }
}

/// Codes used by transports (guidance/fragments/errors/p18.yaml, MRL-LINK-004 from the base).
pub mod codes {
    /// The machine could not be reached over the chosen transport.
    pub const UNREACHABLE: &str = "MRL-LINK-184";
    /// First SSH connection: the host key needs confirming.
    pub const HOST_KEY_UNKNOWN: &str = "MRL-LINK-185";
    /// The SSH host key changed.
    pub const HOST_KEY_CHANGED: &str = "MRL-LINK-186";
    /// A firewall seems to block the port (base catalog).
    pub const FIREWALL: &str = "MRL-LINK-004";
}

/// One step of a connection test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct Check {
    /// Short step name, like `tcp`.
    pub name: String,
    /// Whether the step worked.
    pub ok: bool,
    /// What happened, in plain words.
    pub detail: String,
}

/// The result of [`Transport::connection_test`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct ConnectionReport {
    /// The transport tested.
    pub transport: TransportKind,
    /// What it tried to reach.
    pub target: String,
    /// True when every step worked.
    pub ok: bool,
    /// Steps in order; the first failing one is last.
    pub steps: Vec<Check>,
    /// The error code of the failing step, if any.
    pub code: Option<String>,
}

impl ConnectionReport {
    /// An empty report.
    pub fn new(transport: TransportKind, target: impl Into<String>) -> Self {
        Self {
            transport,
            target: target.into(),
            ok: true,
            steps: Vec::new(),
            code: None,
        }
    }

    /// Records a working step.
    pub fn pass(&mut self, name: &str, detail: impl Into<String>) {
        self.steps.push(Check {
            name: name.to_owned(),
            ok: true,
            detail: detail.into(),
        });
    }

    /// Records a failing step and its code; the report is then not ok.
    pub fn fail(&mut self, name: &str, code: &str, detail: impl Into<String>) {
        self.ok = false;
        self.code.get_or_insert_with(|| code.to_owned());
        self.steps.push(Check {
            name: name.to_owned(),
            ok: false,
            detail: detail.into(),
        });
    }

    /// The failing step's detail, if any.
    pub fn problem(&self) -> Option<&str> {
        self.steps.iter().find(|s| !s.ok).map(|s| s.detail.as_str())
    }
}

/// `host:port` → (host, port). Accepts `[v6]:port`.
pub fn split_host_port(v: &str) -> Option<(String, u16)> {
    if let Ok(a) = v.parse::<SocketAddr>() {
        return Some((a.ip().to_string(), a.port()));
    }
    let (host, port) = v.rsplit_once(':')?;
    let port = port.parse::<u16>().ok().filter(|p| *p != 0)?;
    if host.is_empty() || host.contains(':') {
        return None;
    }
    Some((host.to_owned(), port))
}

/// Opens TCP to `address` with plain-language errors.
pub async fn tcp_connect(address: &str) -> Result<tokio::net::TcpStream, TransportError> {
    let (host, port) = split_host_port(address).ok_or_else(|| {
        TransportError::Unreachable(format!("{address:?} is not a host:port address"))
    })?;
    let addrs: Vec<SocketAddr> = match tokio::net::lookup_host((host.as_str(), port)).await {
        Ok(a) => a.collect(),
        Err(_) => Vec::new(),
    };
    if addrs.is_empty() {
        return Err(TransportError::Unreachable(format!(
            "could not find {host} on the network; check the name, or use the machine's IP address"
        )));
    }
    let mut last = None;
    for addr in addrs {
        match tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(s)) => {
                let _ = s.set_nodelay(true);
                return Ok(s);
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                last = Some(format!(
                    "{host} answered, but nothing listens on port {port}; start the agent there with `gateway.listen` set to this port"
                ));
            }
            Ok(Err(e)) => last = Some(format!("could not connect to {addr}: {e}")),
            Err(_) => {
                last = Some(format!(
                    "{host} did not answer on port {port} within {} s; a firewall may be blocking the port ({})",
                    CONNECT_TIMEOUT.as_secs(),
                    codes::FIREWALL
                ));
            }
        }
    }
    Err(TransportError::Unreachable(last.unwrap_or_else(|| {
        format!("could not connect to {address}")
    })))
}

/// A stream that keeps something alive (an SSH session, a helper process) while it is open.
pub struct Hold<T, G> {
    io: T,
    _guard: G,
}

impl<T, G> Hold<T, G> {
    /// Wraps `io`; `guard` is dropped with the stream.
    pub fn new(io: T, guard: G) -> Self {
        Self { io, _guard: guard }
    }
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
pub(crate) mod tests {
    use super::*;

    /// A secret store with nothing in it.
    pub(crate) struct NoSecrets;

    #[async_trait::async_trait]
    impl SecretStore for NoSecrets {
        async fn get(
            &self,
            _: &str,
        ) -> Result<Option<crate::seams::Secret>, crate::seams::SeamError> {
            Ok(None)
        }
        async fn set(
            &self,
            _: &str,
            _: crate::seams::Secret,
        ) -> Result<(), crate::seams::SeamError> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> Result<(), crate::seams::SeamError> {
            Ok(())
        }
    }

    #[test]
    fn host_port_forms() {
        assert_eq!(split_host_port("gpu:7443"), Some(("gpu".into(), 7443)));
        assert_eq!(split_host_port("[::1]:7443"), Some(("::1".into(), 7443)));
        assert_eq!(split_host_port("gpu"), None);
        assert_eq!(split_host_port("gpu:0"), None);
        assert_eq!(split_host_port(":7443"), None);
    }

    #[test]
    fn endpoint_json_and_problems() {
        let e: Endpoint =
            serde_json::from_str(r#"{"kind":"ssh","host":"box","port":null,"user":null,"agent_port":7443,"key_secret":null}"#)
                .unwrap();
        assert_eq!(e.kind(), TransportKind::Ssh);
        assert!(e.problems().is_empty());
        let bad = Endpoint::Direct {
            address: "nohost".into(),
        };
        assert_eq!(bad.problems().len(), 1);
        assert!(serde_json::from_str::<Endpoint>(r#"{"kind":"carrier_pigeon"}"#).is_err());
    }

    #[test]
    fn report_keeps_first_failure_code() {
        let mut r = ConnectionReport::new(TransportKind::Direct, "x:1");
        r.pass("dns", "found");
        r.fail("tcp", codes::UNREACHABLE, "refused");
        r.fail("tls", codes::FIREWALL, "later");
        assert!(!r.ok);
        assert_eq!(r.code.as_deref(), Some(codes::UNREACHABLE));
        assert_eq!(r.problem(), Some("refused"));
    }
}
