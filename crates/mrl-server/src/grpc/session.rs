//! One gateway stream (one lane of one node), the same whichever side dialed.
//!
//! 1. The first agent message is `AgentHello` — or `EnrollRequest` on a connection without a
//!    machine certificate, which may carry nothing else.
//! 2. The certificate must be on file, not revoked or expired, belong to `AgentHello.node_id`
//!    (and to the node the core dialed), and the node must not be DECOMMISSIONED.
//! 3. CONTROL: `session_epoch += 1`; `CoreHello.resume_from_seq = control_rx_seq + 1`. Each
//!    message with a new `seq` is handled and `control_rx_seq` raised in one transaction (dedupe on
//!    node, lane, seq), then acked (`CoreMessage{seq: 0, ack_seq}`). Commands go out in `seq`
//!    order; `AgentMessage.ack_seq` marks them RECEIVED; `CommandResult` finishes them.
//!    A CONTROL-class message for the sink is kept in `node_control_inbox` in that transaction
//!    and handed to the sink after it commits (no row lock held while the sink runs).
//! 4. TELEMETRY / BULK: lossy lanes (`lanes`), acked on receipt. CONTROL-class messages on them
//!    are kept the same way, only while the stream's session epoch is still the node's.
//! 5. Every stream re-checks its certificate every `revocation_check_seconds` and closes at
//!    once when this replica revokes the node.

use super::{PROTOCOL_VERSION, PeerInfo};
use crate::modules::nodes::lanes::{LaneItem, LaneKind, LaneMetrics, LossyLane, body_name};
use crate::modules::nodes::store::{self, Accept, CertIdentity};
use crate::modules::nodes::{Gateway, NodesError, commands};
use crate::transports::TransportKind;
use mrl_domain::ids::NodeId;
use mrl_proto::agent::v1::{
    AgentHello, AgentMessage, CoreHello, CoreMessage, EnrollRequest, EnrollResult, Lane,
    LeaseGrant, RenewCertificateRequest, RenewCertificateResult, agent_message, core_message,
};
use mrl_proto::common::v1::{Failure, FailureClass};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{mpsc, watch};
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use uuid::Uuid;

/// Messages from the agent.
pub type Inbound = Pin<Box<dyn Stream<Item = Result<AgentMessage, Status>> + Send>>;
/// Messages to the agent (an `Err` closes the stream with that status).
pub type Outbound = mpsc::Sender<Result<CoreMessage, Status>>;

/// How long the agent has to send its first message.
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(30);
/// How often a CONTROL stream looks for commands without a wake-up.
pub const COMMAND_POLL: Duration = Duration::from_secs(2);

// A seq-zero Drain with this id is a gateway fence on CONTROL. A normal Drain has a
// positive command seq and follows the command handler path.
const DRAIN_FENCE: &str = "gateway-drain-fence";

/// Runs one stream to its end. Errors are sent to the agent as the stream's status.
pub async fn run(
    gw: Arc<Gateway>,
    peer: PeerInfo,
    mut inbound: Inbound,
    out: Outbound,
    shutdown: CancellationToken,
) {
    if let Err(status) = drive(&gw, &peer, &mut inbound, &out, &shutdown).await {
        tracing::info!(code = ?status.code(), message = status.message(), "gateway stream closed");
        // A full channel must not keep the connection alive after its send deadline.
        let _ = out.try_send(Err(status));
    }
}

fn now_ts() -> Option<prost_types::Timestamp> {
    Some(ts(OffsetDateTime::now_utc()))
}

/// A protobuf timestamp.
pub fn ts(t: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.unix_timestamp(),
        nanos: t.nanosecond() as i32,
    }
}

fn denied(e: NodesError) -> Status {
    Status::permission_denied(e.explain())
}

fn unavailable(e: NodesError) -> Status {
    tracing::warn!(code = e.code(), error = %e, "gateway stream failed");
    Status::unavailable(format!(
        "{} the server could not record this message; reconnect and try again",
        e.code()
    ))
}

fn protocol(msg: &str) -> Status {
    Status::invalid_argument(NodesError::Protocol(msg.to_owned()).explain())
}

/// Longest one message may wait for room on a stream (an agent that stopped reading): then the
/// stream closes, so revocation, takeover and shutdown are never held up by a stuck peer.
pub const SEND_TIMEOUT: Duration = Duration::from_secs(30);

async fn put(out: &Outbound, m: CoreMessage) -> Result<(), Status> {
    match tokio::time::timeout(SEND_TIMEOUT, out.send(Ok(m))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(Status::cancelled("the agent closed the stream")),
        Err(_) => Err(Status::deadline_exceeded(format!(
            "the agent stopped reading for {} s",
            SEND_TIMEOUT.as_secs()
        ))),
    }
}

async fn send(
    out: &Outbound,
    body: core_message::Body,
    seq: u64,
    ack_seq: u64,
) -> Result<(), Status> {
    put(
        out,
        CoreMessage {
            seq,
            ack_seq,
            sent_at: now_ts(),
            body: Some(body),
        },
    )
    .await
}

async fn ack(out: &Outbound, ack_seq: u64) -> Result<(), Status> {
    put(
        out,
        CoreMessage {
            seq: 0,
            ack_seq,
            sent_at: now_ts(),
            body: None,
        },
    )
    .await
}

async fn drive(
    gw: &Arc<Gateway>,
    peer: &PeerInfo,
    inbound: &mut Inbound,
    out: &Outbound,
    shutdown: &CancellationToken,
) -> Result<(), Status> {
    let first = tokio::select! {
        () = shutdown.cancelled() => return Ok(()),
        first = tokio::time::timeout(HELLO_TIMEOUT, inbound.next()) => first,
    }
    .map_err(|_| Status::deadline_exceeded("the agent sent no hello within 30 s"))?
    .ok_or_else(|| Status::cancelled("the agent closed the stream before its hello"))??;
    match (&peer.cert_sha256, first.body) {
        (None, Some(agent_message::Body::Enroll(req))) => {
            let result = enroll(gw, &req).await;
            send(out, core_message::Body::EnrollResult(result), 0, 0).await?;
            // Keep the stream open until the agent has the result and closes its side, so
            // the answer is never cut off when the connection is torn down (core-dials).
            tokio::select! {
                () = shutdown.cancelled() => {}
                _ = tokio::time::timeout(Duration::from_secs(10), inbound.next()) => {}
            }
            Ok(())
        }
        (None, _) => Err(Status::unauthenticated(
            NodesError::Certificate(
                "this connection has no machine certificate; enroll the machine first (`mrl-agent enroll`)".into(),
            )
            .explain(),
        )),
        (Some(_), Some(agent_message::Body::Enroll(_))) => Err(Status::failed_precondition(
            NodesError::Protocol("this machine is already enrolled".into()).explain(),
        )),
        (Some(sha), Some(agent_message::Body::Hello(hello))) => {
            let ident = check_identity(gw, peer, sha, &hello).await?;
            let transport = peer.transport.or_else(|| TransportKind::from_proto(hello.transport));
            match Lane::try_from(hello.lane) {
                Ok(Lane::Control) => control(gw, sha, ident, &hello, transport, peer.connection, inbound, out, shutdown).await,
                Ok(Lane::Telemetry | Lane::Bulk) => data(gw, sha, ident, &hello, peer.connection, inbound, out, shutdown).await,
                _ => Err(protocol("AgentHello.lane must be CONTROL, TELEMETRY or BULK")),
            }
        }
        _ => Err(protocol("the first message must be AgentHello")),
    }
}

/// Plain-language failure for an `EnrollResult` / `RenewCertificateResult`.
fn failure(e: &NodesError) -> Failure {
    let (class, retry) = match e {
        NodesError::Token(_) | NodesError::Certificate(_) => (FailureClass::UserConfig, false),
        _ => (FailureClass::InfraTransient, true),
    };
    Failure {
        class: class as i32,
        sub_code: e.code().to_owned(),
        message: e.to_string(),
        retryable_hint: retry,
        ..Failure::default()
    }
}

async fn enroll(gw: &Arc<Gateway>, req: &EnrollRequest) -> EnrollResult {
    let pki = match gw.pki() {
        Ok(p) => p,
        Err(e) => {
            return EnrollResult {
                failure: Some(failure(&e)),
                ..EnrollResult::default()
            };
        }
    };
    match store::enroll(&gw.db, pki.clone(), &gw.config, req).await {
        Ok(done) => {
            tracing::info!(node = %NodeId::from_uuid(done.node), hostname = %req.hostname, "machine enrolled");
            EnrollResult {
                ok: true,
                node_id: NodeId::from_uuid(done.node).to_string(),
                certificate_chain_der: vec![done.cert.der.to_vec(), pki.ca.cert().to_vec()],
                ca_bundle_der: pki.ca.cert().to_vec(),
                // ponytail: no agent-update signing key yet; the agent updater part adds it.
                update_signing_public_key: Vec::new(),
                not_after: Some(ts(done.cert.not_after)),
                failure: None,
            }
        }
        Err(e) => {
            tracing::info!(code = e.code(), error = %e, "enrollment refused");
            EnrollResult {
                failure: Some(failure(&e)),
                ..EnrollResult::default()
            }
        }
    }
}

/// Why `sha` may not be used now, if it may not.
fn refusal(ident: &CertIdentity) -> Option<&'static str> {
    if ident.revoked {
        Some("this certificate was revoked; the machine must enroll again")
    } else if ident.expired {
        Some("this certificate has expired; the machine must enroll again")
    } else if ident.status == "DECOMMISSIONED" {
        Some("this machine was removed from the app")
    } else {
        None
    }
}

async fn check_identity(
    gw: &Gateway,
    peer: &PeerInfo,
    sha: &str,
    hello: &AgentHello,
) -> Result<CertIdentity, Status> {
    let ident = store::cert_identity(&gw.db, sha)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| {
            denied(NodesError::Certificate(
                "this certificate was not issued to a machine of this app".into(),
            ))
        })?;
    if let Some(why) = refusal(&ident) {
        return Err(denied(NodesError::Certificate(why.into())));
    }
    let claimed = NodeId::from_str(&hello.node_id).ok().map(|n| n.as_uuid());
    if claimed != Some(ident.node) || peer.dialed_node.is_some_and(|n| n != ident.node) {
        return Err(denied(NodesError::Certificate(
            "AgentHello.node_id does not match the machine certificate".into(),
        )));
    }
    if !hello.certificate_sha256.is_empty() && !hello.certificate_sha256.eq_ignore_ascii_case(sha) {
        return Err(denied(NodesError::Certificate(
            "AgentHello.certificate_sha256 is not the certificate of this connection".into(),
        )));
    }
    if !hello
        .protocol_versions
        .iter()
        .any(|v| v == PROTOCOL_VERSION)
    {
        return Err(Status::failed_precondition(
            NodesError::Protocol(format!(
                "the agent speaks {:?} but this server speaks {PROTOCOL_VERSION}; update the agent",
                hello.protocol_versions
            ))
            .explain(),
        ));
    }
    Ok(ident)
}

/// Re-reads the certificate; `Err` when it may no longer be used.
async fn still_valid(gw: &Gateway, sha: &str) -> Result<(), Status> {
    verdict(store::cert_identity(&gw.db, sha).await)
}

/// The re-check's decision. A lookup that fails closes the stream too (fail closed): while this
/// replica can't confirm the certificate, it delivers nothing more; the agent reconnects and the
/// handshake checks again.
fn verdict(lookup: Result<Option<CertIdentity>, NodesError>) -> Result<(), Status> {
    match lookup {
        Ok(Some(ident)) => match refusal(&ident) {
            Some(why) => Err(denied(NodesError::Certificate(why.into()))),
            None => Ok(()),
        },
        Ok(None) => Err(denied(NodesError::Certificate(
            "this certificate is no longer on file".into(),
        ))),
        Err(e) => Err(unavailable(e)),
    }
}

fn hello(gw: &Gateway, lane: Lane, resume_from_seq: u64) -> core_message::Body {
    core_message::Body::Hello(CoreHello {
        core_id: gw.core_id.clone(),
        protocol_version: PROTOCOL_VERSION.to_owned(),
        heartbeat_interval_s: gw.config.heartbeat_interval_seconds,
        max_message_bytes: super::MAX_MESSAGE_BYTES as u32,
        resume_from_seq,
        lane: lane as i32,
    })
}

/// Lossy lanes and the telemetry writer of one stream.
struct Lossy {
    metrics: LossyLane,
    logs: LossyLane,
    telemetry: watch::Sender<Option<AgentMessage>>,
}

impl Lossy {
    /// Lanes of one stream bound to the CONTROL session `epoch`; `session` is cancelled when
    /// that session ends or is taken over, and then nothing more is written or delivered.
    fn start(
        gw: &Arc<Gateway>,
        node: Uuid,
        epoch: i64,
        stream: Lane,
        session: &CancellationToken,
    ) -> Self {
        let owner = || crate::modules::nodes::lanes::Owner {
            db: gw.db.clone(),
            node,
            epoch,
        };
        let telemetry_owner = owner();
        let (tx, mut rx) = watch::channel::<Option<AgentMessage>>(None);
        let gw2 = gw.clone();
        let s = session.clone();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() && !s.is_cancelled() {
                let msg = rx.borrow_and_update().clone();
                let Some(mut msg) = msg else { continue };
                let Some(agent_message::Body::Telemetry(t)) = &mut msg.body else {
                    continue;
                };
                let ahead = time::Duration::seconds(
                    i64::try_from(gw2.config.max_clock_skew_seconds).unwrap_or(i64::MAX),
                );
                // The sink gets a reading only when this session saved it (current, newer).
                match store::telemetry(&gw2.db, node, epoch, t, gw2.config.gpu_hot_celsius, ahead)
                    .await
                {
                    Ok(Some(at)) => t.time = Some(ts(at)),
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::debug!(code = e.code(), error = %e, "telemetry not saved");
                        continue;
                    }
                }
                if s.is_cancelled() {
                    return;
                }
                if let Some(sink) = gw2.lanes.sink_for(LaneKind::Telemetry) {
                    let item = LaneItem {
                        node_id: node,
                        session_epoch: epoch,
                        lane: LaneKind::Telemetry,
                        stream,
                        seq: msg.seq,
                        dropped_before: 0,
                        message: msg,
                    };
                    telemetry_owner.deliver(&s, sink.as_ref(), item).await;
                }
            }
        });
        Self {
            metrics: LossyLane::start(
                &gw.lanes,
                LaneKind::Metrics,
                gw.config.lane_buffer,
                session.clone(),
                Some(owner()),
            ),
            logs: LossyLane::start(
                &gw.lanes,
                LaneKind::Logs,
                gw.config.lane_buffer,
                session.clone(),
                Some(owner()),
            ),
            telemetry: tx,
        }
    }

    /// Hands a non-CONTROL message to its lane; never waits.
    fn offer(&self, lane: LaneKind, item: LaneItem) {
        match lane {
            LaneKind::Telemetry => {
                // Latest wins: an unsaved older reading is simply replaced.
                self.telemetry.send_replace(Some(item.message));
            }
            LaneKind::Metrics => self.metrics.offer(item),
            LaneKind::Logs => self.logs.offer(item),
            LaneKind::Control => {}
        }
    }
}

/// Where a CONTROL message goes besides the gateway's own tables.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Route {
    /// Handled by the gateway alone.
    None,
    /// Kept in `node_control_inbox` for the CONTROL sink ([`deliver_kept`] hands it over).
    Inbox,
    /// No sink: journaled to `node_events`.
    Journal,
}

/// The route of a message that is not the gateway's own.
fn route_of(gw: &Gateway, node: Uuid, msg: &AgentMessage) -> Route {
    if own(&msg.body) {
        return Route::None;
    }
    if gw.lanes.sink_for(LaneKind::Control).is_some() {
        return Route::Inbox;
    }
    tracing::warn!(node = %NodeId::from_uuid(node), body = body_name(msg), "no sink for this CONTROL message; journaled to node_events");
    LaneMetrics::get().unrouted(body_name(msg));
    Route::Journal
}

/// Records CONTROL `seq` and its effects in one transaction that holds the node row, so only the
/// node's current session records anything and a takeover waits. A message for the sink is only
/// kept here; it reaches the sink after the row lock is released ([`deliver_kept`]).
async fn record(
    gw: &Gateway,
    node: Uuid,
    epoch: i64,
    msg: &AgentMessage,
    route: Route,
    drain_offered: bool,
) -> Result<Accept, NodesError> {
    let msg = msg.clone();
    let to_db = |e: NodesError| match e {
        NodesError::Db(d) => d,
        o => crate::db::DbError::Query(sqlx::Error::Protocol(o.to_string())),
    };
    Ok(gw
        .db
        .transaction(move |tx| {
            let msg = msg.clone();
            Box::pin(async move {
                let accepted = store::accept(tx, node, epoch, msg.seq)
                    .await
                    .map_err(to_db)?;
                if accepted != Accept::New {
                    return Ok(accepted);
                }
                match &msg.body {
                    Some(agent_message::Body::Heartbeat(hb)) => {
                        store::heartbeat(tx, node, hb.running.len(), hb.time.as_ref())
                            .await
                            .map_err(to_db)?
                    }
                    Some(agent_message::Body::Inventory(inv)) => {
                        store::inventory(tx, node, inv).await.map_err(to_db)?
                    }
                    Some(agent_message::Body::CommandResult(r))
                        if r.command_id == DRAIN_FENCE && drain_offered && r.accepted =>
                    {
                        store::acknowledge_drain(tx, node, epoch)
                            .await
                            .map_err(to_db)?
                    }
                    Some(agent_message::Body::CommandResult(r)) => {
                        store::command_result(tx, node, r).await.map_err(to_db)?
                    }
                    _ => match route {
                        Route::Inbox => store::inbox(tx, node, epoch, Lane::Control, &msg)
                            .await
                            .map_err(to_db)?,
                        Route::Journal => store::unrouted(tx, node, msg.seq, &msg)
                            .await
                            .map_err(to_db)?,
                        Route::None => {}
                    },
                }
                Ok(Accept::New)
            })
        })
        .await?)
}

/// Records a CONTROL-class message that came on a TELEMETRY or BULK stream, only while `epoch`
/// is still the node's session (`Ok(false)`: a newer session took over; nothing was recorded).
async fn record_on_data(
    gw: &Gateway,
    node: Uuid,
    epoch: i64,
    stream: Lane,
    msg: &AgentMessage,
    route: Route,
) -> Result<bool, NodesError> {
    let mut tx = gw.db.begin().await?;
    if !store::holds(&mut tx, node, epoch).await? {
        return Ok(false);
    }
    match route {
        Route::Inbox => store::inbox(&mut tx, node, epoch, stream, msg).await?,
        Route::Journal => store::unrouted(&mut tx, node, msg.seq, msg).await?,
        Route::None => {}
    }
    tx.commit().await.map_err(crate::db::DbError::from)?;
    Ok(true)
}

/// Starts handing the node's kept CONTROL messages to the sink, in a task of its own: the stream
/// (or health pass) that kept them never waits for the sink. A sink error is logged; the messages
/// stay kept and are retried (next message, or the health pass).
pub fn deliver_kept(gw: &Gateway, node: Uuid) {
    let Some(sink) = gw.lanes.sink_for(LaneKind::Control) else {
        return;
    };
    let db = gw.db.clone();
    tokio::spawn(async move {
        if let Err(e) = store::dispatch(&db, sink.as_ref(), node).await {
            tracing::warn!(code = e.code(), error = %e, node = %NodeId::from_uuid(node), "CONTROL messages not delivered yet");
        }
    });
}

/// True for messages the gateway handles itself (never routed to a sink).
fn own(body: &Option<agent_message::Body>) -> bool {
    use agent_message::Body as B;
    matches!(
        body,
        None | Some(
            B::Hello(_)
                | B::Heartbeat(_)
                | B::Inventory(_)
                | B::CommandResult(_)
                | B::Enroll(_)
                | B::RenewCertificate(_)
        )
    )
}

async fn renew(
    gw: &Gateway,
    sha: &str,
    ident: &CertIdentity,
    req: &RenewCertificateRequest,
) -> RenewCertificateResult {
    let pki = match gw.pki() {
        Ok(p) => p,
        Err(e) => {
            return RenewCertificateResult {
                failure: Some(failure(&e)),
                ..Default::default()
            };
        }
    };
    match store::renew(
        &gw.db,
        pki.clone(),
        &gw.config,
        ident.node,
        ident.lifecycle,
        sha,
        &req.csr_der,
    )
    .await
    {
        Ok(cert) => {
            tracing::info!(node = %NodeId::from_uuid(ident.node), "certificate rotated");
            RenewCertificateResult {
                ok: true,
                certificate_chain_der: vec![cert.der.to_vec(), pki.ca.cert().to_vec()],
                not_after: Some(ts(cert.not_after)),
                failure: None,
            }
        }
        Err(e) => RenewCertificateResult {
            failure: Some(failure(&e)),
            ..Default::default()
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn control(
    gw: &Arc<Gateway>,
    sha: &str,
    ident: CertIdentity,
    h: &AgentHello,
    transport: Option<TransportKind>,
    connection: u64,
    inbound: &mut Inbound,
    out: &Outbound,
    shutdown: &CancellationToken,
) -> Result<(), Status> {
    let node = ident.node;
    let (epoch, rx) = store::open_control(
        &gw.db,
        node,
        sha,
        &gw.core_id,
        h,
        transport.map(TransportKind::as_str),
    )
    .await
    .map_err(|e| match e {
        NodesError::Certificate(_) => denied(e),
        e => unavailable(e),
    })?;
    let (superseded, revoked, wake) = gw.sessions.open_control(node, epoch, connection);
    let mut rx_seq = rx.max(0) as u64;
    tracing::info!(node = %NodeId::from_uuid(node), epoch, "machine connected");
    // Every exit from here on (errors included) goes through the cleanup below.
    let run = async {
        if superseded.is_cancelled() {
            return Err(taken_over());
        }
        // This certificate authenticated: certificates issued before it are retired now
        // (rotation keeps the old one valid until the new one is proven to work).
        let retired = store::retire_older(&gw.db, node, sha)
            .await
            .map_err(unavailable)?;
        gw.revoked_locally(&retired);
        send(out, hello(gw, Lane::Control, rx_seq + 1), 0, rx_seq).await?;
        let resume = h.resume_from_seq.max(1);
        if !store::commands_received(&gw.db, node, epoch, resume - 1)
            .await
            .map_err(unavailable)?
        {
            return Err(taken_over());
        }
        let mut sent_upto = resume - 1;
        let mut drain_offered = false;
        let mut acked = 0u64;
        let lossy = Lossy::start(gw, node, epoch, Lane::Control, &superseded);
        let mut poll = tokio::time::interval(COMMAND_POLL);
        let mut recheck =
            tokio::time::interval(Duration::from_secs(gw.config.revocation_check_seconds));
        recheck.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = recheck.tick() => still_valid(gw, sha).await?,
                _ = poll.tick() => {
                    if !drain_offered && store::draining(&gw.db, node, epoch).await.map_err(unavailable)? {
                        let fence = mrl_proto::agent::v1::Command {
                            command_id: DRAIN_FENCE.to_owned(),
                            kind: Some(mrl_proto::agent::v1::command::Kind::Drain(Default::default())),
                        };
                        send(out, core_message::Body::Command(fence), 0, rx_seq).await?;
                        drain_offered = true;
                    }
                    sent_upto = send_commands(gw, node, epoch, sent_upto, rx_seq, out).await?;
                }
                () = wake.notified() => sent_upto = send_commands(gw, node, epoch, sent_upto, rx_seq, out).await?,
                m = inbound.next() => {
                    let Some(Ok(m)) = m else { return Ok(()) };
                    if m.ack_seq > acked {
                        if !store::commands_received(&gw.db, node, epoch, m.ack_seq).await.map_err(unavailable)? {
                            return Err(taken_over());
                        }
                        acked = m.ack_seq;
                    }
                    if m.seq == 0 {
                        continue; // ack only
                    }
                    if m.seq <= rx_seq {
                        ack(out, rx_seq).await?; // a replay
                        continue;
                    }
                    let lane = LaneKind::of(&m);
                    let route = if lane == LaneKind::Control { route_of(gw, node, &m) } else { Route::None };
                    let accepted = record(gw, node, epoch, &m, route, drain_offered).await.map_err(unavailable)?;
                    if accepted == Accept::Stale {
                        return Err(taken_over());
                    }
                    rx_seq = m.seq;
                    if route == Route::Inbox {
                        deliver_kept(gw, node);
                    }
                    if accepted == Accept::New {
                        if lane != LaneKind::Control {
                            lossy.offer(lane, LaneItem { node_id: node, session_epoch: epoch, lane, stream: Lane::Control, seq: m.seq, dropped_before: 0, message: m.clone() });
                        }
                        if let Some(agent_message::Body::RenewCertificate(req)) = &m.body {
                            let result = renew(gw, sha, &ident, req).await;
                            send(out, core_message::Body::RenewCertificateResult(result), 0, rx_seq).await?;
                        }
                    }
                    if let Some(agent_message::Body::Heartbeat(hb)) = &m.body {
                        let leases = store::leases(&gw.db, node, &hb.running).await.map_err(unavailable)?;
                        send(out, core_message::Body::Lease(LeaseGrant { heartbeat_seq: m.seq, leases }), 0, rx_seq).await?;
                    }
                    ack(out, rx_seq).await?;
                }
            }
        }
    };
    let result = tokio::select! {
        biased;
        () = shutdown.cancelled() => Ok(()),
        () = revoked.cancelled() => Err(denied(NodesError::Certificate("this machine's certificates were revoked".into()))),
        () = superseded.cancelled() => Err(taken_over()),
        result = run => result,
    };
    gw.sessions.close_control(node, epoch);
    if let Err(e) = store::close_control(&gw.db, node, epoch).await {
        tracing::debug!(code = e.code(), error = %e, "could not record the disconnect");
    }
    tracing::info!(node = %NodeId::from_uuid(node), epoch, "machine disconnected");
    result
}

/// The CONTROL session a data stream belongs to: the one that came on the same `connection`,
/// when it is also the node's current session in the database. A stream from any other
/// connection (an older one, say) never borrows it. A data stream may open just before its
/// CONTROL stream, so this waits up to 5 s.
async fn bind_to_control(
    gw: &Gateway,
    node: Uuid,
    connection: u64,
) -> Result<(i64, CancellationToken), Status> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let current = store::session_epoch(&gw.db, node)
            .await
            .map_err(unavailable)?;
        if let Some((epoch, token)) = gw.sessions.control_of(node, connection) {
            if epoch == current {
                return Ok((epoch, token));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Status::failed_precondition(
                NodesError::Protocol(
                    "this connection has no CONTROL session for the machine; reconnect all streams"
                        .into(),
                )
                .explain(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The status that closes a stream a newer session replaced.
fn taken_over() -> Status {
    Status::aborted(
        NodesError::Protocol("a newer connection from this machine took over".into()).explain(),
    )
}

/// Claims and sends commands with `seq > after`; returns the last sent seq. Only rows the claim
/// returned are transmitted (SENT is committed first; a row superseded or expired meanwhile is
/// never claimed). A stale session sends nothing and closes.
async fn send_commands(
    gw: &Gateway,
    node: Uuid,
    epoch: i64,
    after: u64,
    rx_seq: u64,
    out: &Outbound,
) -> Result<u64, Status> {
    let claimed = match store::claim_commands(&gw.db, node, epoch, after)
        .await
        .map_err(unavailable)?
    {
        store::Claim::Stale => return Err(taken_over()),
        store::Claim::Send(rows) => rows,
    };
    let mut last = after;
    for (id, seq, payload) in claimed {
        let cmd = commands::decode(&payload).map_err(unavailable)?;
        let size = commands::envelope_len(&cmd);
        if size > super::MAX_MESSAGE_BYTES {
            // Stored before sizes were checked: it can never be sent, so it must not stall the
            // commands after it. The row (and its payload) stays, FAILED, for diagnosis.
            tracing::warn!(node = %NodeId::from_uuid(node), command = %id, size, "a stored command is too large to send; failed");
            store::fail_unsendable(&gw.db, id, size)
                .await
                .map_err(unavailable)?;
            continue;
        }
        send(out, core_message::Body::Command(cmd), seq, rx_seq).await?;
        last = seq;
    }
    Ok(last)
}

#[allow(clippy::too_many_arguments)]
async fn data(
    gw: &Arc<Gateway>,
    sha: &str,
    ident: CertIdentity,
    h: &AgentHello,
    connection: u64,
    inbound: &mut Inbound,
    out: &Outbound,
    shutdown: &CancellationToken,
) -> Result<(), Status> {
    let node = ident.node;
    let lane = Lane::try_from(h.lane).unwrap_or(Lane::Telemetry);
    let revoked = gw.sessions.revoked_token(node);
    send(out, hello(gw, lane, 0), 0, 0).await?;
    // Bound to the CONTROL session of the same connection (on this replica, current in the
    // database): when it ends or is taken over, this stream closes too.
    let (epoch, session) = bind_to_control(gw, node, connection).await?;
    let lossy = Lossy::start(gw, node, epoch, lane, &session);
    let mut recheck =
        tokio::time::interval(Duration::from_secs(gw.config.revocation_check_seconds));
    recheck.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            () = revoked.cancelled() => {
                return Err(denied(NodesError::Certificate("this machine's certificates were revoked".into())));
            }
            () = session.cancelled() => return Err(taken_over()),
            _ = recheck.tick() => still_valid(gw, sha).await?,
            m = inbound.next() => {
                let Some(Ok(m)) = m else { return Ok(()) };
                let kind = LaneKind::of(&m);
                let seq = m.seq;
                if kind == LaneKind::Control {
                    // CONTROL-class messages on a data lane (like guard events) are recorded only
                    // while this stream's session is still the node's, like on CONTROL.
                    let route = route_of(gw, node, &m);
                    if route != Route::None {
                        if !record_on_data(gw, node, epoch, lane, &m, route).await.map_err(unavailable)? {
                            return Err(taken_over());
                        }
                        if route == Route::Inbox {
                            deliver_kept(gw, node);
                        }
                    }
                } else {
                    lossy.offer(kind, LaneItem { node_id: node, session_epoch: epoch, lane: kind, stream: lane, seq, dropped_before: 0, message: m });
                }
                if seq > 0 {
                    ack(out, seq).await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::nodes::store::Lifecycle;

    fn ident(revoked: bool) -> CertIdentity {
        CertIdentity {
            node: Uuid::nil(),
            revoked,
            expired: false,
            status: "ONLINE".into(),
            lifecycle: Lifecycle::Static,
        }
    }

    #[test]
    fn a_failed_revocation_lookup_closes_the_stream() {
        assert!(verdict(Ok(Some(ident(false)))).is_ok());
        assert_eq!(
            verdict(Ok(Some(ident(true)))).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            verdict(Ok(None)).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        let down = NodesError::Db(crate::db::DbError::Unavailable(sqlx::Error::PoolTimedOut));
        assert_eq!(
            verdict(Err(down)).unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }
}
