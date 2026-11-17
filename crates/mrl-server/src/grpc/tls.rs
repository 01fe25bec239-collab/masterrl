//! TLS settings made from the internal CA (rustls + ring, TLS 1.2/1.3, ALPN `h2`).
//!
//! - Listener: the core certificate; a client certificate is *optional* at the TLS layer so a
//!   machine without one can enroll, but such a session may only send `EnrollRequest`
//!   (enforced in `session`). Certificates that are presented must chain to the CA.
//! - Dialing an enrolled agent: the core certificate as client certificate; the agent's
//!   certificate must chain to the CA and name the node (`ca::node_name`).
//! - Dialing an agent that waits to enroll: its temporary certificate is not checked; the
//!   agent checks the core's certificate against the CA it pinned, and the join token proves
//!   the machine to the core.

use crate::modules::nodes::ca::Ca;
use crate::modules::nodes::{NodesError, Pki};
use std::sync::Arc;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::{
    ring, verify_tls12_signature, verify_tls13_signature, CryptoProvider,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{
    self, ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
};

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

fn err(e: impl std::fmt::Display) -> NodesError {
    NodesError::Ca(format!("TLS settings: {e}"))
}

/// Builds every TLS setting from the CA (and a fresh core certificate).
pub fn build(ca: Ca) -> Result<Pki, NodesError> {
    let (core_cert, core_key) = ca.issue_core()?;
    let chain = vec![core_cert, ca.cert().clone()];
    let mut roots = RootCertStore::empty();
    roots.add(ca.cert().clone()).map_err(err)?;
    let roots = Arc::new(roots);

    let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider())
        .allow_unauthenticated()
        .build()
        .map_err(err)?;
    let mut server = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain.clone(), PrivateKeyDer::Pkcs8(core_key.clone_key()))
        .map_err(err)?;
    server.alpn_protocols = vec![b"h2".to_vec()];

    let mut dial = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .with_root_certificates(roots)
        .with_client_auth_cert(chain.clone(), PrivateKeyDer::Pkcs8(core_key.clone_key()))
        .map_err(err)?;
    dial.alpn_protocols = vec![b"h2".to_vec()];

    let mut enroll = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyServer(provider())))
        .with_client_auth_cert(chain, PrivateKeyDer::Pkcs8(core_key))
        .map_err(err)?;
    enroll.alpn_protocols = vec![b"h2".to_vec()];

    Ok(Pki {
        ca,
        server_tls: Arc::new(server),
        dial_tls: Arc::new(dial),
        enroll_tls: Arc::new(enroll),
    })
}

/// Accepts the temporary certificate of an agent that waits to enroll (signatures are still
/// checked, so the handshake is a real one). Only used for enrollment dials.
#[derive(Debug)]
struct AnyServer(Arc<CryptoProvider>);

impl ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Hex SHA-256 of the peer's leaf certificate, if it sent one.
pub fn peer_sha256(certs: Option<&[CertificateDer<'_>]>) -> Option<String> {
    certs
        .and_then(|c| c.first())
        .map(|leaf| crate::modules::nodes::ca::sha256_hex(leaf))
}
