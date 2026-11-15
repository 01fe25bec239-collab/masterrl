//! HTTPS for team-mode listeners: rustls (ring) with a PEM certificate chain and key.
//!
//! Handshakes run in their own tasks (10 s limit), so one slow client never blocks
//! `accept` for the others. ALPN offers `h2` and `http/1.1`, so gRPC works over TLS too.

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

/// Loads the certificate chain and key into an acceptor (MRL-VAL-012 on failure).
pub fn acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor, ServerError> {
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

/// A listener that yields finished TLS connections, for `axum::serve`.
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
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
                                let _ = tx.send((tls, peer)).await;
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
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(conn) => conn,
            // The accept loop stopped, so shutdown is under way; axum stops calling us.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}
