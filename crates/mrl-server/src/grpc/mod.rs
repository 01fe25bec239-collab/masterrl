//! The agent gateway: `AgentGatewayService` over mutual TLS (agents dial in) and the dialer
//! for agents the core dials (`AgentEndpointService`).
//!
//! `AgentGatewayService` runs on the shared gRPC listener (`server.listeners.grpc`, 9443): the
//! nodes module hands that listener its mutual TLS (`ServerModule::grpc_tls`), so every request
//! carries the connection's client certificate and id (`ConnectInfo<TlsPeer>`). A connection
//! without a certificate may only enroll.
//!
//! - [`tls`]: TLS settings from the internal CA.
//! - [`session`]: one stream (lane) of one node; shared by both directions.
//! - [`dialer`]: keeps sessions to agents that wait for the core (SSH, LAN, Tailscale, tunnel).

pub mod dialer;
pub mod session;
pub mod tls;

use crate::http::tls::TlsPeer;
use crate::modules::nodes::Gateway;
use crate::transports::TransportKind;
use mrl_proto::agent::v1::agent_gateway_service_server::{
    AgentGatewayService, AgentGatewayServiceServer,
};
use mrl_proto::agent::v1::{AgentMessage, CoreMessage};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use uuid::Uuid;

/// The gRPC service agents call (`package.Service`).
pub const SERVICE_NAME: &str = "mrl.agent.v1.AgentGatewayService";
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
    /// This connection's id in this process ([`PeerInfo::new_connection`]): the streams of one
    /// connection share it, so data streams bind to the CONTROL stream of their own connection.
    pub connection: u64,
}

impl PeerInfo {
    /// A fresh connection id (never 0), from the same counter as the gRPC listener's.
    pub fn new_connection() -> u64 {
        crate::http::tls::next_connection()
    }

    /// The peer of a request on the shared gRPC listener.
    fn of(peer: &TlsPeer) -> Self {
        Self {
            cert_sha256: tls::peer_sha256(Some(&peer.certificates)),
            remote: Some(peer.remote),
            connection: peer.connection,
            ..Self::default()
        }
    }
}

type CoreStream = Pin<Box<dyn Stream<Item = Result<CoreMessage, tonic::Status>> + Send>>;

/// `AgentGatewayService`: each `Session` call is one lane of one node.
struct GatewaySvc {
    gw: Arc<Gateway>,
}

/// The service for the shared gRPC listener (message size limits per Doc 04 E6).
pub fn service(gw: Arc<Gateway>) -> AgentGatewayServiceServer<impl AgentGatewayService> {
    AgentGatewayServiceServer::new(GatewaySvc { gw })
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

#[tonic::async_trait]
impl AgentGatewayService for GatewaySvc {
    type SessionStream = CoreStream;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<AgentMessage>>,
    ) -> Result<tonic::Response<CoreStream>, tonic::Status> {
        // No connection info (not the gateway's TLS): treated as a peer without a certificate.
        let peer = request
            .extensions()
            .get::<axum::extract::ConnectInfo<TlsPeer>>()
            .map(|c| PeerInfo::of(&c.0))
            .unwrap_or_default();
        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(session::run(
            self.gw.clone(),
            peer,
            Box::pin(inbound),
            tx,
            self.gw.stop.child_token(),
        ));
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
