//! The agent gateway: `AgentGatewayService` over mutual TLS (agents dial in) and the dialer
//! for agents the core dials (`AgentEndpointService`).
//!
//! The gateway runs its own TLS listener (`gateway.listen`, default `server.listeners.grpc`)
//! rather than the shared gRPC listener of `app`, because it must see the client certificate
//! of every connection and allow certificate-less connections that may only enroll.
//!
//! - [`tls`]: TLS settings from the internal CA.
//! - [`session`]: one stream (lane) of one node; shared by both directions.
//! - [`dialer`]: keeps sessions to agents that wait for the core (SSH, LAN, Tailscale, tunnel).

pub mod dialer;
pub mod session;
pub mod tls;

use crate::modules::nodes::{Gateway, NodesError};
use crate::transports::TransportKind;
use mrl_proto::agent::v1::agent_gateway_service_server::{
    AgentGatewayService, AgentGatewayServiceServer,
};
use mrl_proto::agent::v1::{AgentMessage, CoreMessage};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The protocol version this server speaks (`AgentHello.protocol_versions` must list it).
pub const PROTOCOL_VERSION: &str = "mrl.agent.v1";
/// Largest protobuf message on a session (Doc 04 E6).
pub const MAX_MESSAGE_BYTES: usize = 4 << 20;
/// HTTP/2 keepalive (Doc 04 E6).
pub const KEEPALIVE: Duration = Duration::from_secs(20);
/// Longest a client may take to finish the TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Who is on the other end of a connection.
#[derive(Debug, Clone, Default)]
pub struct PeerInfo {
    /// Hex SHA-256 of the machine certificate, when the peer has one (else it may only enroll).
    pub cert_sha256: Option<String>,
    /// Remote address (agent-dials only).
    pub remote: Option<SocketAddr>,
    /// The transport, when the core dialed.
    pub transport: Option<TransportKind>,
    /// The node the core meant to reach, when the core dialed.
    pub dialed_node: Option<Uuid>,
}

/// A TLS connection plus who is on the other end.
pub struct Conn {
    io: TlsStream<TcpStream>,
    peer: PeerInfo,
}

impl tonic::transport::server::Connected for Conn {
    type ConnectInfo = PeerInfo;

    fn connect_info(&self) -> PeerInfo {
        self.peer.clone()
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl AsyncWrite for Conn {
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

type CoreStream = Pin<Box<dyn Stream<Item = Result<CoreMessage, tonic::Status>> + Send>>;

/// `AgentGatewayService`: each `Session` call is one lane of one node.
struct GatewaySvc {
    gw: Arc<Gateway>,
    shutdown: CancellationToken,
}

#[tonic::async_trait]
impl AgentGatewayService for GatewaySvc {
    type SessionStream = CoreStream;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<AgentMessage>>,
    ) -> Result<tonic::Response<CoreStream>, tonic::Status> {
        let peer = request
            .extensions()
            .get::<PeerInfo>()
            .cloned()
            .unwrap_or_default();
        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(session::run(
            self.gw.clone(),
            peer,
            Box::pin(inbound),
            tx,
            self.shutdown.child_token(),
        ));
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Serves the gateway on the listener bound at startup until `cancel` fires. Returns at once
/// when this process has no gateway role (nothing was bound).
pub async fn serve(gw: Arc<Gateway>, cancel: CancellationToken) -> Result<(), NodesError> {
    let Some(std_listener) = gw.listener.lock().ok().and_then(|mut l| l.take()) else {
        return Ok(());
    };
    let listener = TcpListener::from_std(std_listener)
        .map_err(|e| NodesError::Protocol(format!("gateway listener: {e}")))?;
    let pki = gw.pki()?;
    let acceptor = TlsAcceptor::from(pki.server_tls.clone());
    let (tx, rx) = mpsc::channel::<Result<Conn, std::io::Error>>(64);
    let accept_cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            let (tcp, remote) = tokio::select! {
                () = accept_cancel.cancelled() => break,
                r = listener.accept() => match r {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error = %e, "gateway could not accept a connection");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                },
            };
            let _ = tcp.set_nodelay(true);
            let (acceptor, tx) = (acceptor.clone(), tx.clone());
            // ponytail: no cap on concurrent handshakes (same as the api listener); add a semaphore if floods matter.
            tokio::spawn(async move {
                match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                    Ok(Ok(io)) => {
                        let peer = PeerInfo {
                            cert_sha256: tls::peer_sha256(io.get_ref().1.peer_certificates()),
                            remote: Some(remote),
                            ..PeerInfo::default()
                        };
                        let _ = tx.send(Ok(Conn { io, peer })).await;
                    }
                    Ok(Err(e)) => {
                        tracing::debug!(error = %e, %remote, "gateway TLS handshake failed")
                    }
                    Err(_) => tracing::debug!(%remote, "gateway TLS handshake timed out"),
                }
            });
        }
    });
    if let Some(a) = gw.addr() {
        tracing::info!(listener = "gateway", scheme = "mtls", addr = %a, "listening");
    }
    let svc = AgentGatewayServiceServer::new(GatewaySvc {
        gw: gw.clone(),
        shutdown: cancel.clone(),
    })
    .max_decoding_message_size(MAX_MESSAGE_BYTES)
    .max_encoding_message_size(MAX_MESSAGE_BYTES);
    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE))
        .http2_keepalive_timeout(Some(KEEPALIVE))
        .add_service(svc)
        .serve_with_incoming_shutdown(ReceiverStream::new(rx), cancel.cancelled())
        .await
        .map_err(|e| NodesError::Protocol(format!("gateway server: {e}")))
}
