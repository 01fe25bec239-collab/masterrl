//! Joining a machine to the app: `mrl-agent enroll --server … --token …` (agent dials) or
//! `mrl-agent enroll --listen … --token …` (the core dials, e.g. through SSH).
//!
//! 1. The token (`mrlj_<secret>_<CA fingerprint>`) pins the CA; nothing is sent to a server
//!    whose certificate does not chain to it.
//! 2. A new ECDSA P-256 key is made here; only a certificate request leaves the machine.
//! 3. The certificate, the CA and the key go to `<state>/identity/` (folder 0700, key 0600 on Unix).

pub mod keys;

use crate::session::tls::{self, sha256_hex};
use crate::session::{self as link, LinkError, PROTOCOL_VERSION};
use crate::state::atomic_write;
use crate::transports::TransportKind;
use crate::AgentContext;
use mrl_proto::agent::v1::agent_endpoint_service_server::{
    AgentEndpointService, AgentEndpointServiceServer,
};
use mrl_proto::agent::v1::agent_gateway_service_client::AgentGatewayServiceClient;
use mrl_proto::agent::v1::{
    agent_message, core_message, AgentMessage, CoreMessage, EnrollRequest, EnrollResult,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

/// A parsed join token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinToken {
    /// The whole token, as sent.
    pub token: String,
    /// The pinned CA fingerprint (hex SHA-256).
    pub ca_sha256: String,
}

impl JoinToken {
    /// Reads `mrlj_<64 hex>_<64 hex>`.
    pub fn parse(s: &str) -> Result<Self, LinkError> {
        let s = s.trim();
        let bad = || {
            LinkError::Token(
                "this is not a MasterRL join token; copy the whole `mrlj_…` text again".into(),
            )
        };
        let rest = s.strip_prefix("mrlj_").ok_or_else(bad)?;
        let (secret, pin) = rest.split_once('_').ok_or_else(bad)?;
        let hex64 = |v: &str| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit());
        if !hex64(secret) || !hex64(pin) {
            return Err(bad());
        }
        Ok(Self {
            token: s.to_owned(),
            ca_sha256: pin.to_ascii_lowercase(),
        })
    }
}

/// `identity/link.json`: who this machine is and how it reaches the core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkFile {
    /// `node_…`.
    pub node_id: String,
    /// The core's `host:port` (or tunnel hostname) when the agent dials.
    pub server: Option<String>,
    /// The address the agent listens on when the core dials.
    pub listen: Option<SocketAddr>,
    /// Transport used at enrollment.
    pub transport: TransportKind,
    /// Random id of this installation.
    pub installation_id: String,
    /// Certificate start (Unix seconds, when it was saved).
    pub issued_at: i64,
    /// Certificate end (Unix seconds).
    pub not_after: i64,
}

/// This machine's identity.
#[derive(Clone)]
pub struct Identity {
    /// The link file.
    pub link: LinkFile,
    /// Leaf certificate.
    pub cert: CertificateDer<'static>,
    /// The CA.
    pub ca: CertificateDer<'static>,
    key: Vec<u8>,
    key_ref: Option<keys::KeyRef>,
    /// Where `save` stores a new key: the configured store at enrollment, the key's own store
    /// after `load`.
    pub key_store: keys::KeyStoreChoice,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("link", &self.link)
            .finish_non_exhaustive()
    }
}

/// The identity file (certificates and a reference to the key; the key itself is in the
/// [`keys`] store).
pub const IDENTITY_FILE: &str = "identity.json";

#[derive(Serialize, Deserialize)]
struct IdentityFile {
    link: LinkFile,
    cert: String,
    ca: String,
    key: keys::KeyRef,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// The identity folder inside the state folder.
pub fn identity_dir(state_root: &Path) -> PathBuf {
    state_root.join("identity")
}

fn io_err(path: &Path) -> impl Fn(std::io::Error) -> LinkError + '_ {
    move |e| LinkError::Agent(crate::AgentError::state(path, e))
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

impl Identity {
    /// Loads the identity, or `None` when this machine has not enrolled.
    pub fn load(state_root: &Path) -> Result<Option<Self>, LinkError> {
        let path = identity_dir(state_root).join(IDENTITY_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_err(&path)(e)),
        };
        let damaged = |why: String| {
            LinkError::NotEnrolled(format!(
                "{} is damaged ({why}); enroll again",
                path.display()
            ))
        };
        let f: IdentityFile = serde_json::from_slice(&bytes).map_err(|e| damaged(e.to_string()))?;
        let unhex =
            |name: &str, v: &str| from_hex(v).ok_or_else(|| damaged(format!("{name} is not hex")));
        let key = keys::get(&identity_dir(state_root), &f.key)?;
        Ok(Some(Self {
            link: f.link,
            cert: CertificateDer::from(unhex("cert", &f.cert)?),
            ca: CertificateDer::from(unhex("ca", &f.ca)?),
            key,
            key_store: f.key.store(),
            key_ref: Some(f.key),
        }))
    }

    /// Saves the identity: the key goes to its store first (under a name of its own), then one
    /// atomic replace of `identity.json` publishes the certificate with that key's reference, then
    /// the previous key is removed. A crash leaves the old identity or the new one, never a mix.
    pub fn save(&self, state_root: &Path) -> Result<(), LinkError> {
        let dir = identity_dir(state_root);
        std::fs::create_dir_all(&dir).map_err(io_err(&dir))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .map_err(io_err(&dir))?;
        }
        let name = sha256_hex(&self.cert)[..16].to_owned();
        let previous = Self::load(state_root)
            .ok()
            .flatten()
            .and_then(|p| p.key_ref);
        let key_ref = match &self.key_ref {
            Some(r) => r.clone(),
            None => keys::put(&dir, self.key_store, &name, &self.key)?,
        };
        let f = IdentityFile {
            link: self.link.clone(),
            cert: hex(&self.cert),
            ca: hex(&self.ca),
            key: key_ref.clone(),
        };
        let bytes =
            serde_json::to_vec_pretty(&f).map_err(|e| LinkError::Protocol(e.to_string()))?;
        let path = dir.join(IDENTITY_FILE);
        atomic_write(&path, &bytes).map_err(io_err(&path))?;
        if let Some(old) = previous.filter(|old| *old != key_ref) {
            keys::delete(&dir, &old);
        }
        Ok(())
    }

    /// The private key.
    pub fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key.clone()))
    }

    /// Leaf then CA.
    pub fn chain(&self) -> Vec<CertificateDer<'static>> {
        vec![self.cert.clone(), self.ca.clone()]
    }

    /// Hex SHA-256 of the leaf certificate.
    pub fn cert_sha256(&self) -> String {
        sha256_hex(&self.cert)
    }

    /// When to renew: at 2/3 of the certificate's lifetime.
    pub fn renew_at(&self) -> SystemTime {
        let life = (self.link.not_after - self.link.issued_at).max(0);
        let at = self.link.issued_at + life * 2 / 3;
        UNIX_EPOCH + Duration::from_secs(at.max(0) as u64)
    }

    /// The identity with a renewed certificate and its new key.
    pub fn renewed(
        &self,
        chain: &[Vec<u8>],
        key: &rcgen::KeyPair,
        not_after: Option<&prost_types::Timestamp>,
    ) -> Result<Self, LinkError> {
        let leaf = chain
            .first()
            .ok_or_else(|| LinkError::Protocol("the renewed certificate chain is empty".into()))?;
        let mut next = self.clone();
        next.cert = CertificateDer::from(leaf.clone());
        next.key = key.serialize_der();
        next.key_ref = None; // a new key: stored on save
        next.link.issued_at = unix_now();
        next.link.not_after = not_after.map_or(next.link.issued_at + 86_400, |t| t.seconds);
        Ok(next)
    }
}

/// A new P-256 key and a certificate request for it (DER).
pub fn new_key() -> Result<(rcgen::KeyPair, Vec<u8>), LinkError> {
    let key = rcgen::KeyPair::generate().map_err(|e| LinkError::Certificate(e.to_string()))?;
    let csr = rcgen::CertificateParams::new(Vec::<String>::new())
        .and_then(|p| p.serialize_request(&key))
        .map_err(|e| LinkError::Certificate(e.to_string()))?;
    Ok((key, csr.der().to_vec()))
}

/// A stable fingerprint of this machine's hardware (host name, CPU, GPU UUIDs).
pub fn hardware_fingerprint(ctx: &AgentContext) -> String {
    let inv = ctx.inventory();
    let mut s = format!("{}|{}", inv.hostname, inv.cpu.model);
    for g in &inv.gpus {
        s.push('|');
        s.push_str(&g.uuid);
    }
    sha256_hex(s.as_bytes())
}

fn request(ctx: &AgentContext, token: &JoinToken, csr: Vec<u8>) -> EnrollRequest {
    EnrollRequest {
        join_token: token.token.clone(),
        csr_der: csr,
        hardware_fingerprint: hardware_fingerprint(ctx),
        agent_version: env!("CARGO_PKG_VERSION").to_owned(),
        hostname: ctx.inventory().hostname,
    }
}

/// Turns the core's answer into a saved identity.
fn finish(
    ctx: &AgentContext,
    result: EnrollResult,
    key: &rcgen::KeyPair,
    server: Option<String>,
    listen: Option<SocketAddr>,
    transport: TransportKind,
) -> Result<Identity, LinkError> {
    if !result.ok {
        let f = result.failure.unwrap_or_default();
        return Err(LinkError::Refused {
            code: if f.sub_code.is_empty() {
                "MRL-NODE-180".into()
            } else {
                f.sub_code
            },
            message: f.message,
        });
    }
    let leaf = result
        .certificate_chain_der
        .first()
        .cloned()
        .ok_or_else(|| LinkError::Protocol("the core sent no certificate".into()))?;
    let identity = Identity {
        link: LinkFile {
            node_id: result.node_id,
            server,
            listen,
            transport,
            installation_id: random_id(),
            issued_at: unix_now(),
            not_after: result.not_after.map_or(unix_now() + 86_400, |t| t.seconds),
        },
        cert: CertificateDer::from(leaf),
        ca: CertificateDer::from(result.ca_bundle_der),
        key: key.serialize_der(),
        key_ref: None,
        key_store: ctx.config.gateway.key_store,
    };
    identity.save(ctx.state.root())?;
    Ok(identity)
}

/// A random id (from a fresh key's bytes; the agent has no other random source).
fn random_id() -> String {
    let (k, _) = match new_key() {
        Ok(k) => k,
        Err(_) => return format!("inst-{}", unix_now()),
    };
    sha256_hex(&k.serialize_der())[..32].to_owned()
}

/// Refuses to replace an identity unless `force`.
fn check_not_enrolled(ctx: &AgentContext, force: bool) -> Result<(), LinkError> {
    if force {
        return Ok(());
    }
    if let Some(i) = Identity::load(ctx.state.root())? {
        return Err(LinkError::Token(format!(
            "this machine already joined as {}; run enroll with --force to join again with a new token",
            i.link.node_id
        )));
    }
    Ok(())
}

/// Enrolls by dialing the core at `server` over `transport`.
pub async fn enroll_dial(
    ctx: &AgentContext,
    server: &str,
    token: &str,
    transport: TransportKind,
    force: bool,
) -> Result<Identity, LinkError> {
    check_not_enrolled(ctx, force)?;
    let token = JoinToken::parse(token)?;
    let (key, csr) = new_key()?;
    let t = link::dial_transport(&ctx.config.gateway, transport, server);
    let io = t.connect().await?;
    let tls_io = match link::connect_tls(tls::enroll_client(&token.ca_sha256)?, io, &CancellationToken::new()).await {
        Ok(Some(t)) => t,
        Ok(None) => return Err(LinkError::Unreachable("stopped".into())),
        Err(LinkError::Certificate(e)) => {
            return Err(LinkError::Certificate(format!(
                "the server at {server} is not the app this token came from, or its certificate can't be used ({e})"
            )))
        }
        Err(e) => return Err(e),
    };
    let channel = link::channel_over(tls_io).await?;
    let mut client = AgentGatewayServiceClient::new(channel);
    let (tx, rx) = mpsc::channel(4);
    tx.send(AgentMessage {
        body: Some(agent_message::Body::Enroll(request(ctx, &token, csr))),
        ..AgentMessage::default()
    })
    .await
    .map_err(|_| LinkError::Protocol("stream closed".into()))?;
    let mut inbound = client
        .session(ReceiverStream::new(rx))
        .await
        .map_err(|s| {
            LinkError::Unreachable(format!("the core refused the connection: {}", s.message()))
        })?
        .into_inner();
    let result = wait_result(&mut inbound).await?;
    drop(tx);
    finish(ctx, result, &key, Some(server.to_owned()), None, transport)
}

async fn wait_result<S>(inbound: &mut S) -> Result<EnrollResult, LinkError>
where
    S: tokio_stream::Stream<Item = Result<CoreMessage, tonic::Status>> + Unpin,
{
    loop {
        let m = tokio::time::timeout(Duration::from_secs(30), inbound.next())
            .await
            .map_err(|_| {
                LinkError::Unreachable("the core did not answer the enrollment within 30 s".into())
            })?
            .ok_or_else(|| {
                LinkError::Unreachable("the core closed the connection during enrollment".into())
            })?
            .map_err(|s| {
                LinkError::Unreachable(format!("the core ended the enrollment: {}", s.message()))
            })?;
        if let Some(core_message::Body::EnrollResult(r)) = m.body {
            return Ok(r);
        }
    }
}

type Slot = Arc<Mutex<Option<mpsc::Sender<Result<EnrollResult, LinkError>>>>>;

struct EnrollSvc {
    req: EnrollRequest,
    done: Slot,
}

#[tonic::async_trait]
impl AgentEndpointService for EnrollSvc {
    type SessionStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<AgentMessage, tonic::Status>> + Send>,
    >;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<CoreMessage>>,
    ) -> Result<tonic::Response<Self::SessionStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        let (req, done) = (self.req.clone(), self.done.clone());
        tokio::spawn(async move {
            // The lane offer comes first; enrollment needs no lane.
            let _ = inbound.next().await;
            let _ = tx
                .send(Ok(AgentMessage {
                    body: Some(agent_message::Body::Enroll(req)),
                    ..AgentMessage::default()
                }))
                .await;
            let r = wait_result(&mut inbound).await;
            let sender = done.lock().ok().and_then(|s| s.clone());
            if let Some(s) = sender {
                let _ = s.send(r).await;
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Enrolls by waiting on `listen` until the core dials in (it knows this machine from the
/// token's endpoint). Connections that are not from the pinned core are dropped.
pub async fn enroll_listen(
    ctx: &AgentContext,
    listen: SocketAddr,
    token: &str,
    transport: TransportKind,
    force: bool,
    cancel: CancellationToken,
) -> Result<Identity, LinkError> {
    check_not_enrolled(ctx, force)?;
    let token = JoinToken::parse(token)?;
    let (key, csr) = new_key()?;
    let l = crate::transports::Listen::new(listen, transport);
    let bound = l.bind().await?;
    tracing_line(&format!("waiting for the app to connect on {bound}"));
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls::enroll_server(&token.ca_sha256)?));
    let (done_tx, mut done_rx) = mpsc::channel(1);
    let slot: Slot = Arc::new(Mutex::new(Some(done_tx)));
    loop {
        // Handshakes are bounded and run side by side; peers that aren't the pinned core are dropped.
        let Some(tls_io) =
            link::accept_tls(&l, acceptor.clone(), link::peer_is_core, &cancel).await?
        else {
            return Err(LinkError::Unreachable(
                "stopped before the app connected".into(),
            ));
        };
        let stop = CancellationToken::new();
        let svc = AgentEndpointServiceServer::new(EnrollSvc {
            req: request(ctx, &token, csr.clone()),
            done: slot.clone(),
        });
        let serve = link::serve_one(tls_io, svc, stop.clone());
        tokio::pin!(serve);
        let result = tokio::select! {
            () = cancel.cancelled() => return Err(LinkError::Unreachable("stopped during enrollment".into())),
            _ = &mut serve => None,
            r = done_rx.recv() => r,
            // A core that connects but never enrolls doesn't hold the agent forever.
            () = tokio::time::sleep(Duration::from_secs(60)) => None,
        };
        stop.cancel();
        if let Some(r) = result {
            return finish(ctx, r?, &key, None, Some(bound), transport);
        }
    }
}

fn tracing_line(s: &str) {
    eprintln!("mrl-agent: {s}");
}

/// The protocol version list sent in hellos.
pub fn protocol_versions() -> Vec<String> {
    vec![PROTOCOL_VERSION.to_owned()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_parse_and_bad_ones_are_explained() {
        let t = format!("mrlj_{}_{}", "a".repeat(64), "B".repeat(64));
        let p = JoinToken::parse(&t).unwrap();
        assert_eq!(p.ca_sha256, "b".repeat(64));
        for bad in [
            "",
            "mrlj_",
            "abc",
            &format!("mrlj_{}", "a".repeat(64)),
            &format!("mrlj_{}_{}", "g".repeat(64), "a".repeat(64)),
        ] {
            assert!(
                JoinToken::parse(bad)
                    .unwrap_err()
                    .to_string()
                    .contains("join token"),
                "{bad}"
            );
        }
    }

    #[test]
    fn identity_round_trips_and_renews_at_two_thirds() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Identity::load(dir.path()).unwrap().is_none());
        let (key, _) = new_key().unwrap();
        let id = Identity {
            link: LinkFile {
                node_id: "node_x".into(),
                server: Some("core:9443".into()),
                listen: None,
                transport: TransportKind::Direct,
                installation_id: "i".into(),
                issued_at: 3000,
                not_after: 6000,
            },
            cert: CertificateDer::from(vec![1u8, 2]),
            ca: CertificateDer::from(vec![3u8]),
            key: key.serialize_der(),
            key_ref: None,
            key_store: keys::KeyStoreChoice::File,
        };
        id.save(dir.path()).unwrap();
        let back = Identity::load(dir.path()).unwrap().unwrap();
        assert_eq!(back.link, id.link);
        assert_eq!(back.renew_at(), UNIX_EPOCH + Duration::from_secs(5000));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let json =
                std::fs::read_to_string(identity_dir(dir.path()).join(IDENTITY_FILE)).unwrap();
            assert!(
                !json.contains(&hex(&id.key)),
                "the key is not in identity.json"
            );
            let mode = std::fs::metadata(
                identity_dir(dir.path()).join(format!("keys/{}.key", &id.cert_sha256()[..16])),
            )
            .unwrap()
            .permissions()
            .mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // An interrupted save leaves only a temporary file: the old identity loads whole.
        let idir = identity_dir(dir.path());
        std::fs::write(
            idir.join(".identity.json.tmp-1-0"),
            b"{\"link\": {\"node_id\"",
        )
        .unwrap();
        assert_eq!(
            Identity::load(dir.path()).unwrap().unwrap().cert_sha256(),
            id.cert_sha256()
        );
        // A renewed identity replaces certificate and key together.
        let (key2, _) = new_key().unwrap();
        let next = id.renewed(&[vec![9u8, 9]], &key2, None).unwrap();
        next.save(dir.path()).unwrap();
        let back = Identity::load(dir.path()).unwrap().unwrap();
        assert_eq!(back.cert_sha256(), next.cert_sha256());
        assert_eq!(back.key, key2.serialize_der());
        assert!(
            !identity_dir(dir.path())
                .join(format!("keys/{}.key", &id.cert_sha256()[..16]))
                .exists(),
            "the replaced key is removed"
        );
        std::fs::write(idir.join(IDENTITY_FILE), b"{oops").unwrap();
        assert_eq!(
            Identity::load(dir.path()).unwrap_err().code(),
            "MRL-NODE-182"
        );
    }
}
