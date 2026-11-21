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
        // Unused tokens, and used ones whose machine never connected (it may have lost the
        // answer and waits again with the same key) until the token expires.
        "SELECT t.id, t.endpoint FROM node_enrollment_tokens t
         LEFT JOIN compute_nodes n ON n.id = t.used_by_node_id
         WHERE t.endpoint IS NOT NULL AND t.expires_at > now()
           AND (t.used_at IS NULL OR (t.used_key_sha256 IS NOT NULL AND n.status = 'ENROLLING' AND n.session_epoch = 0
                  AND EXISTS (SELECT 1 FROM node_certificates c WHERE c.node_id = n.id AND c.revoked_at IS NULL AND c.not_after > now())
                  AND NOT EXISTS (SELECT 1 FROM node_certificates c WHERE c.node_id = n.id AND c.revoked_at IS NOT NULL)))",
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

/// Runs `step`, a wait on the machine, for at most `limit`; `stop` (leadership lost, shutdown)
/// ends it at once. Every wait on the machine in the dialer goes through this or has a limit of
/// its own, so a machine that stops answering never holds a dial. `what` names the step.
async fn bounded<T>(
    what: &str,
    limit: Duration,
    stop: &CancellationToken,
    step: impl std::future::Future<Output = Result<T, NodesError>>,
) -> Result<T, NodesError> {
    tokio::select! {
        () = stop.cancelled() => Err(NodesError::Protocol(format!("stopped during {what}"))),
        r = tokio::time::timeout(limit, step) => r.unwrap_or_else(|_| {
            Err(NodesError::Protocol(format!(
                "the machine did not finish {what} within {} s",
                limit.as_secs()
            )))
        }),
    }
}

/// An HTTP/2 channel over one already-open stream (no reconnects: the dialer does that), set
/// up within the handshake limit; `stop` ends the setup.
async fn channel_over<T>(
    io: T,
    stop: &CancellationToken,
) -> Result<tonic::transport::Channel, NodesError>
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
    let connect = async {
        tonic::transport::Endpoint::from_static("http://agent.masterrl.internal")
            .http2_keep_alive_interval(KEEPALIVE)
            .keep_alive_while_idle(true)
            .connect_with_connector(connector)
            .await
            .map_err(|e| NodesError::Protocol(format!("HTTP/2 to the agent failed: {e}")))
    };
    bounded("the HTTP/2 setup", super::HANDSHAKE_TIMEOUT, stop, connect).await
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
    let open = async {
        client.session(outbound).await.map_err(|s| {
            NodesError::Protocol(format!(
                "the agent refused the {lane:?} stream: {}",
                s.message()
            ))
        })
    };
    let what = format!("opening the {lane:?} stream");
    let inbound = bounded(&what, super::HANDSHAKE_TIMEOUT, &stop, open)
        .await?
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
    let io = bounded(
        "opening the transport",
        Duration::from_secs(30),
        lead,
        open(gw, endpoint),
    )
    .await?;
    let pki = gw.pki()?;
    let name =
        ServerName::try_from(node_name(node)).map_err(|e| NodesError::Protocol(e.to_string()))?;
    let tls = async {
        TlsConnector::from(pki.dial_tls.clone())
            .connect(name, io)
            .await
            .map_err(|e| NodesError::Certificate(format!("TLS with the machine failed: {e}")))
    };
    let tls = bounded("the TLS handshake", super::HANDSHAKE_TIMEOUT, lead, tls).await?;
    let peer = PeerInfo {
        cert_sha256: super::tls::peer_sha256(tls.get_ref().1.peer_certificates()),
        remote: None,
        transport: Some(endpoint.kind()),
        dialed_node: Some(node),
        connection: PeerInfo::new_connection(),
    };
    let channel = channel_over(tls, lead).await?;
    let client = AgentEndpointServiceClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let stop = lead.child_token();
    let lanes = start_lanes(&stop, |l| {
        let (gw, mut client, peer, stop) = (gw.clone(), client.clone(), peer.clone(), stop.clone());
        async move { lane(&gw, &mut client, l, peer, stop).await }
    })
    .await?;
    // All three streams are required: the first to end ends the connection, and the dialer
    // reconnects them together.
    let (_, _, rest) = futures_util::future::select_all(lanes).await;
    stop.cancel();
    for h in rest {
        let _ = h.await;
    }
    Ok(())
}

/// Starts CONTROL, TELEMETRY and BULK with `start`, in that order. When one fails to start, the
/// lanes already running are stopped and joined before the error is returned, so no lane task
/// outlives its connection.
async fn start_lanes<F, Fut>(
    stop: &CancellationToken,
    mut start: F,
) -> Result<Vec<tokio::task::JoinHandle<()>>, NodesError>
where
    F: FnMut(Lane) -> Fut,
    Fut: std::future::Future<Output = Result<tokio::task::JoinHandle<()>, NodesError>>,
{
    let mut lanes = Vec::new();
    for l in [Lane::Control, Lane::Telemetry, Lane::Bulk] {
        match start(l).await {
            Ok(h) => lanes.push(h),
            Err(e) => {
                stop.cancel();
                for h in lanes {
                    let _ = h.await;
                }
                return Err(e);
            }
        }
    }
    Ok(lanes)
}

/// Connects to an agent that waits with `enroll --listen` and lets it enroll.
async fn dial_enroll(
    gw: &Arc<Gateway>,
    endpoint: &Endpoint,
    lead: &CancellationToken,
) -> Result<(), NodesError> {
    let io = bounded(
        "opening the transport",
        Duration::from_secs(30),
        lead,
        open(gw, endpoint),
    )
    .await?;
    let pki = gw.pki()?;
    let name = ServerName::try_from("enroll.masterrl.internal")
        .map_err(|e| NodesError::Protocol(e.to_string()))?;
    let tls = async {
        TlsConnector::from(pki.enroll_tls.clone())
            .connect(name, io)
            .await
            .map_err(|e| {
                NodesError::Certificate(format!("TLS with the waiting machine failed: {e}"))
            })
    };
    let tls = bounded("the TLS handshake", super::HANDSHAKE_TIMEOUT, lead, tls).await?;
    let channel = channel_over(tls, lead).await?;
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

    #[tokio::test]
    async fn a_lane_that_fails_to_open_stops_the_lanes_already_running() {
        let stop = CancellationToken::new();
        let ended = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r = start_lanes(&stop, |l| {
            let (stop, ended) = (stop.clone(), ended.clone());
            async move {
                if l == Lane::Bulk {
                    return Err(NodesError::Protocol(
                        "the agent refused the Bulk stream".into(),
                    ));
                }
                Ok(tokio::spawn(async move {
                    stop.cancelled().await;
                    ended.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }))
            }
        })
        .await;
        assert!(r.is_err());
        assert!(stop.is_cancelled());
        assert_eq!(ended.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn http2_setup_with_a_machine_that_never_reads_is_bounded_and_stop_ends_it() {
        // The machine's end stays open and never reads: the HTTP/2 preface can't be written.
        let (io, _machine) = tokio::io::duplex(1);
        let stop = CancellationToken::new();
        let started = std::time::Instant::now();
        let limit = super::super::HANDSHAKE_TIMEOUT;
        let r = tokio::time::timeout(limit * 2, channel_over(io, &stop))
            .await
            .expect("the HTTP/2 setup has a time limit");
        assert!(r.is_err());
        assert!(started.elapsed() <= limit + Duration::from_secs(2));
        let (io, _machine) = tokio::io::duplex(1);
        let s = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            s.cancel();
        });
        let started = std::time::Instant::now();
        let r = tokio::time::timeout(limit * 2, channel_over(io, &stop)).await;
        assert!(matches!(r, Ok(Err(_))));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "stop ended the wait"
        );
    }

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
