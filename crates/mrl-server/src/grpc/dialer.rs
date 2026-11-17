//! Core-dials: keeps a session to every machine whose agent waits for the core (an `endpoint`
//! is set), and finishes enrollment of machines whose join token has an endpoint.
//!
//! The core opens the transport, runs TLS on top (the core certificate as client certificate),
//! then opens one `AgentEndpointService.Session` per lane. On each stream the core first sends a
//! `CoreHello` with only `lane` set (so the agent knows the lane), then the session runs exactly as
//! when the agent dials (`session::run`). Reconnecting is the same for every transport:
//! exponential backoff 1 s → 60 s with full jitter. Leader-elected, so one replica dials.

use super::session;
use super::{PeerInfo, KEEPALIVE, MAX_MESSAGE_BYTES};
use crate::db::LeaderElection;
use crate::modules::nodes::ca::node_name;
use crate::modules::nodes::{Gateway, NodesError};
use crate::transports::{BoxIo, Endpoint};
use hyper_util::rt::TokioIo;
use mrl_proto::agent::v1::agent_endpoint_service_client::AgentEndpointServiceClient;
use mrl_proto::agent::v1::{core_message, CoreHello, CoreMessage, Lane};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// How often the dialer looks for machines to reach.
pub const SCAN_EVERY: Duration = Duration::from_secs(2);

/// Full-jitter exponential backoff (Doc 04 E6): 100 ms plus a random part of
/// `min(60 s, 1 s × 2^failures)`.
pub fn backoff(failures: u32) -> Duration {
    let cap_ms = (1000u64 << failures.min(6)).min(60_000);
    let r = Uuid::new_v4().as_u128() as u64;
    Duration::from_millis(100 + r % cap_ms)
}

#[derive(Default)]
struct Target {
    busy: bool,
    failures: u32,
    next: Option<Instant>,
}

type Targets = Arc<Mutex<HashMap<Uuid, Target>>>;

/// Runs while this replica leads `nodes.dialer`.
pub async fn run(gw: Arc<Gateway>, cancel: CancellationToken) {
    let election = LeaderElection::new(gw.db.clone(), "nodes.dialer", Duration::from_secs(15));
    election
        .run(cancel, |lead| {
            let gw = gw.clone();
            async move {
                let targets: Targets = Arc::default();
                loop {
                    if let Err(e) = scan(&gw, &targets, &lead).await {
                        tracing::debug!(code = e.code(), error = %e, "dialer scan failed");
                    }
                    tokio::select! {
                        () = lead.cancelled() => return Ok(()),
                        () = tokio::time::sleep(SCAN_EVERY) => {}
                    }
                }
            }
        })
        .await;
}

fn claim(targets: &Targets, id: Uuid) -> bool {
    let Ok(mut t) = targets.lock() else {
        return false;
    };
    let t = t.entry(id).or_default();
    if t.busy || t.next.is_some_and(|n| Instant::now() < n) {
        return false;
    }
    t.busy = true;
    true
}

fn done(targets: &Targets, id: Uuid, ok: bool) {
    if let Ok(mut t) = targets.lock() {
        let t = t.entry(id).or_default();
        t.busy = false;
        t.failures = if ok { 0 } else { t.failures.saturating_add(1) };
        t.next = Some(
            Instant::now()
                + if ok {
                    Duration::from_millis(500)
                } else {
                    backoff(t.failures)
                },
        );
    }
}

async fn scan(
    gw: &Arc<Gateway>,
    targets: &Targets,
    lead: &CancellationToken,
) -> Result<(), NodesError> {
    let stale = f64::from(gw.config.heartbeat_interval_seconds) * 3.0;
    let nodes: Vec<(Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT id, endpoint FROM compute_nodes WHERE endpoint IS NOT NULL AND status <> 'DECOMMISSIONED'
           AND (session_holder IS NULL OR last_heartbeat_at < now() - make_interval(secs => $1))",
    )
    .bind(stale)
    .fetch_all(gw.db.pool())
    .await?;
    for (node, endpoint) in nodes {
        if gw.sessions.has_control(node) || !claim(targets, node) {
            continue;
        }
        let Ok(endpoint) = serde_json::from_value::<Endpoint>(endpoint) else {
            done(targets, node, false);
            continue;
        };
        let (gw, targets, lead) = (gw.clone(), targets.clone(), lead.clone());
        tokio::spawn(async move {
            let ok = match dial_node(&gw, node, &endpoint, &lead).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::info!(node = %node, code = e.code(), error = %e, "could not reach the machine");
                    false
                }
            };
            done(&targets, node, ok);
        });
    }
    let tokens: Vec<(Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT id, endpoint FROM node_enrollment_tokens
         WHERE endpoint IS NOT NULL AND used_at IS NULL AND expires_at > now()",
    )
    .fetch_all(gw.db.pool())
    .await?;
    for (token, endpoint) in tokens {
        if !claim(targets, token) {
            continue;
        }
        let Ok(endpoint) = serde_json::from_value::<Endpoint>(endpoint) else {
            done(targets, token, false);
            continue;
        };
        let (gw, targets, lead) = (gw.clone(), targets.clone(), lead.clone());
        tokio::spawn(async move {
            let ok = match dial_enroll(&gw, &endpoint, &lead).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::debug!(code = e.code(), error = %e, "machine waiting to enroll is not reachable yet");
                    false
                }
            };
            done(&targets, token, ok);
        });
    }
    Ok(())
}

/// An HTTP/2 channel over one already-open stream (no reconnects: the dialer does that).
async fn channel_over<T>(io: T) -> Result<tonic::transport::Channel, NodesError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let slot = Arc::new(Mutex::new(Some(io)));
    let connector = tower::service_fn(move |_: axum::http::Uri| {
        let slot = slot.clone();
        async move {
            slot.lock()
                .ok()
                .and_then(|mut s| s.take())
                .map(TokioIo::new)
                .ok_or_else(|| std::io::Error::other("this connection was already used"))
        }
    });
    tonic::transport::Endpoint::from_static("http://agent.masterrl.internal")
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_while_idle(true)
        .connect_with_connector(connector)
        .await
        .map_err(|e| NodesError::Protocol(format!("HTTP/2 to the agent failed: {e}")))
}

async fn open(gw: &Gateway, endpoint: &Endpoint) -> Result<BoxIo, NodesError> {
    let transport = endpoint.transport(&gw.transport_env());
    Ok(transport.connect().await?)
}

/// Opens one lane: the lane offer first, then the normal session.
async fn lane(
    gw: &Arc<Gateway>,
    client: &mut AgentEndpointServiceClient<tonic::transport::Channel>,
    lane: Lane,
    peer: PeerInfo,
    stop: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>, NodesError> {
    let (tx, rx) = mpsc::channel::<Result<CoreMessage, tonic::Status>>(64);
    let offer = CoreMessage {
        body: Some(core_message::Body::Hello(CoreHello {
            lane: lane as i32,
            ..CoreHello::default()
        })),
        ..CoreMessage::default()
    };
    tx.send(Ok(offer))
        .await
        .map_err(|_| NodesError::Protocol("stream closed".into()))?;
    let outbound = ReceiverStream::new(rx).map_while(Result::ok);
    let inbound = client
        .session(outbound)
        .await
        .map_err(|s| {
            NodesError::Protocol(format!(
                "the agent refused the {lane:?} stream: {}",
                s.message()
            ))
        })?
        .into_inner();
    Ok(tokio::spawn(session::run(
        gw.clone(),
        peer,
        Box::pin(inbound),
        tx,
        stop,
    )))
}

/// Connects to an enrolled node and runs its three lanes until the CONTROL lane ends.
async fn dial_node(
    gw: &Arc<Gateway>,
    node: Uuid,
    endpoint: &Endpoint,
    lead: &CancellationToken,
) -> Result<(), NodesError> {
    let io = open(gw, endpoint).await?;
    let pki = gw.pki()?;
    let name =
        ServerName::try_from(node_name(node)).map_err(|e| NodesError::Protocol(e.to_string()))?;
    let tls = TlsConnector::from(pki.dial_tls.clone())
        .connect(name, io)
        .await
        .map_err(|e| NodesError::Certificate(format!("TLS with the machine failed: {e}")))?;
    let peer = PeerInfo {
        cert_sha256: super::tls::peer_sha256(tls.get_ref().1.peer_certificates()),
        remote: None,
        transport: Some(endpoint.kind()),
        dialed_node: Some(node),
    };
    let channel = channel_over(tls).await?;
    let mut client = AgentEndpointServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let stop = lead.child_token();
    let control = lane(gw, &mut client, Lane::Control, peer.clone(), stop.clone()).await?;
    let telemetry = lane(gw, &mut client, Lane::Telemetry, peer.clone(), stop.clone()).await?;
    let bulk = lane(gw, &mut client, Lane::Bulk, peer, stop.clone()).await?;
    let _ = control.await;
    stop.cancel();
    let _ = tokio::join!(telemetry, bulk);
    Ok(())
}

/// Connects to an agent that waits with `enroll --listen` and lets it enroll.
async fn dial_enroll(
    gw: &Arc<Gateway>,
    endpoint: &Endpoint,
    lead: &CancellationToken,
) -> Result<(), NodesError> {
    let io = open(gw, endpoint).await?;
    let pki = gw.pki()?;
    let name = ServerName::try_from("enroll.masterrl.internal")
        .map_err(|e| NodesError::Protocol(e.to_string()))?;
    let tls = TlsConnector::from(pki.enroll_tls.clone())
        .connect(name, io)
        .await
        .map_err(|e| {
            NodesError::Certificate(format!("TLS with the waiting machine failed: {e}"))
        })?;
    let channel = channel_over(tls).await?;
    let mut client = AgentEndpointServiceClient::new(channel);
    // No machine certificate on this connection: the session accepts only an EnrollRequest.
    let peer = PeerInfo {
        transport: Some(endpoint.kind()),
        ..PeerInfo::default()
    };
    let control = lane(gw, &mut client, Lane::Control, peer, lead.child_token()).await?;
    let _ = control.await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_to_a_minute_with_jitter() {
        for f in 0..20 {
            let d = backoff(f);
            assert!(
                d >= Duration::from_millis(100) && d <= Duration::from_millis(60_100),
                "{f}: {d:?}"
            );
        }
        assert!((0..20).all(|_| backoff(0) <= Duration::from_millis(1100)));
        assert!((0..50).any(|_| backoff(10) > Duration::from_secs(2)));
    }
}
