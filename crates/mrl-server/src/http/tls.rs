//! HTTPS for team-mode listeners: rustls (ring) with a PEM certificate chain and key.
//!
//! Handshakes run in their own tasks (10 s limit), so one slow client never blocks
//! `accept` for the others. ALPN offers `h2` and `http/1.1`, so gRPC works over TLS too.
//!
//! A module may ask for mutual TLS on the gRPC listener ([`crate::app::GrpcTls`]):
//! [`grpc_acceptor`] then checks client certificates with the module's verifier and serves the
//! module's certificate to clients that ask for its names. Every connection reaches handlers as
//! a [`TlsPeer`] (`axum::extract::ConnectInfo<TlsPeer>` in the request extensions).

use crate::config::TlsConfig;
use crate::error::{ErrorCode, ServerError};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Longest a client may take to finish the TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Who is on the other end of one TLS connection.
#[derive(Debug, Clone)]
pub struct TlsPeer {
    /// Remote address.
    pub remote: SocketAddr,
    /// The client's certificate chain (leaf first) when it sent one; the listener's verifier
    /// accepted it.
    pub certificates: Vec<CertificateDer<'static>>,
    /// This connection's id ([`next_connection`]): every request on it carries the same one.
    pub connection: u64,
}

/// A fresh connection id, unique in this process (never 0). Anything else that names
/// connections (like the gateway's dialer) takes its ids here too.
pub fn next_connection() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, TlsListener>>
    for TlsPeer
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, TlsListener>) -> Self {
        stream.remote_addr().clone()
    }
}

fn load_chain(
    cfg: &TlsConfig,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), ServerError> {
    let fail = |what: &str, e: &dyn std::fmt::Display| {
        // `e` describes the file problem; it never contains key material.
        ServerError::new(ErrorCode::TlsFiles, format!("{what}: {e}"))
    };
    let cert_name = format!("server.tls.cert_path {}", cfg.cert_path.display());
    let key_name = format!("server.tls.key_path {}", cfg.key_path.display());
    let certs = CertificateDer::pem_file_iter(&cfg.cert_path)
        .map_err(|e| fail(&cert_name, &e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fail(&cert_name, &e))?;
    if certs.is_empty() {
        return Err(fail(&cert_name, &"the file has no certificate"));
    }
    let key = PrivateKeyDer::from_pem_file(&cfg.key_path).map_err(|e| fail(&key_name, &e))?;
    Ok((certs, key))
}

/// Loads the certificate chain and key into an acceptor (MRL-VAL-012 on failure).
pub fn acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor, ServerError> {
    let fail = |what: &str, e: &dyn std::fmt::Display| {
        ServerError::new(ErrorCode::TlsFiles, format!("{what}: {e}"))
    };
    let (certs, key) = load_chain(cfg)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| fail("protocol versions", &e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| fail("certificate and key", &e))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Picks the gRPC listener's certificate: the module's for its own names (and for every client
/// when `[server.tls]` is not set), else the operator's.
#[derive(Debug)]
struct PickCert {
    module: Arc<rustls::sign::CertifiedKey>,
    names: Vec<String>,
    operator: Option<Arc<rustls::sign::CertifiedKey>>,
}

impl rustls::server::ResolvesServerCert for PickCert {
    fn resolve(
        &self,
        hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        let own = hello
            .server_name()
            .is_some_and(|n| self.names.iter().any(|m| m.eq_ignore_ascii_case(n)));
        match (&self.operator, own) {
            (Some(op), false) => Some(op.clone()),
            _ => Some(self.module.clone()),
        }
    }
}

/// An acceptor for the gRPC listener with a module's mutual TLS (`tls`) and, in team mode, the
/// operator's `[server.tls]` certificate for every other client (ALPN `h2`).
pub fn grpc_acceptor(
    tls: &crate::app::GrpcTls,
    operator: Option<&TlsConfig>,
) -> Result<TlsAcceptor, ServerError> {
    let fail = |what: &str, e: &dyn std::fmt::Display| {
        ServerError::new(ErrorCode::TlsFiles, format!("{what}: {e}"))
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let operator = match operator {
        Some(cfg) => {
            let (certs, key) = load_chain(cfg)?;
            let signer = provider
                .key_provider
                .load_private_key(key)
                .map_err(|e| fail("server.tls.key_path", &e))?;
            Some(Arc::new(rustls::sign::CertifiedKey::new(certs, signer)))
        }
        None => None,
    };
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| fail("protocol versions", &e))?
        .with_client_cert_verifier(tls.client_verifier.clone())
        .with_cert_resolver(Arc::new(PickCert {
            module: tls.certified_key.clone(),
            names: tls.server_names.clone(),
            operator,
        }));
    config.alpn_protocols = vec![b"h2".to_vec()];
    // Every connection runs the module's verifier: no session resumption, which would reuse a
    // cached client chain without it (a revoked certificate could come back that way).
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A listener that yields finished TLS connections, for `axum::serve`.
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, TlsPeer)>,
    local: SocketAddr,
}

impl TlsListener {
    /// Accepts TCP on `tcp` and runs handshakes on `tracker` until `token` is cancelled.
    pub fn spawn(
        tcp: TcpListener,
        acceptor: TlsAcceptor,
        tracker: &TaskTracker,
        token: CancellationToken,
    ) -> std::io::Result<Self> {
        let local = tcp.local_addr()?;
        let (tx, rx) = mpsc::channel(64);
        let handshakes = tracker.clone();
        tracker.spawn(async move {
            loop {
                let (stream, peer) = tokio::select! {
                    () = token.cancelled() => break,
                    accepted = tcp.accept() => match accepted {
                        Ok(pair) => pair,
                        Err(e) => {
                            // Like axum's own loop: back off briefly (e.g. out of file handles).
                            tracing::warn!(error = %e, "could not accept a connection");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    },
                };
                let (acceptor, tx, token) = (acceptor.clone(), tx.clone(), token.clone());
                // ponytail: no cap on concurrent handshakes; add a semaphore if floods matter.
                handshakes.spawn(async move {
                    tokio::select! {
                        () = token.cancelled() => {}
                        done = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)) => match done {
                            Ok(Ok(tls)) => {
                                let who = TlsPeer {
                                    remote: peer,
                                    certificates: tls
                                        .get_ref()
                                        .1
                                        .peer_certificates()
                                        .map(|c| c.iter().map(|c| c.clone().into_owned()).collect())
                                        .unwrap_or_default(),
                                    connection: next_connection(),
                                };
                                let _ = tx.send((tls, who)).await;
                            }
                            Ok(Err(e)) => tracing::debug!(error = %e, "tls handshake failed"),
                            Err(_) => tracing::debug!("tls handshake timed out"),
                        },
                    }
                });
            }
        });
        Ok(Self { rx, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = TlsPeer;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(conn) => conn,
            // The accept loop stopped, so shutdown is under way; axum stops calling us.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(TlsPeer {
            remote: self.local,
            certificates: Vec::new(),
            connection: 0,
        })
    }
}
