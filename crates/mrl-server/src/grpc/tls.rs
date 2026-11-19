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
//! - Revocation is enforced inside both verifiers: a certificate on the revocation list
//!   (`Gateway::refresh_revocations`, reloaded every `revocation_check_seconds` and at once on
//!   local revocation) fails the TLS handshake with `certificate_revoked`.

use crate::modules::nodes::ca::Ca;
use crate::modules::nodes::{NodesError, Pki};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::client::WebPkiServerVerifier;
use tokio_rustls::rustls::crypto::{
    ring, verify_tls12_signature, verify_tls13_signature, CryptoProvider,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{
    self, CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore,
    ServerConfig, SignatureScheme,
};

/// Revoked certificate fingerprints (hex SHA-256 of the DER).
pub type Revoked = Arc<RwLock<HashSet<String>>>;

fn is_revoked(revoked: &Revoked, cert: &CertificateDer<'_>) -> bool {
    let sha = crate::modules::nodes::ca::sha256_hex(cert);
    // A poisoned lock fails closed.
    revoked.read().map_or(true, |set| set.contains(&sha))
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

fn err(e: impl std::fmt::Display) -> NodesError {
    NodesError::Ca(format!("TLS settings: {e}"))
}

/// Builds every TLS setting from the CA (and a fresh core certificate).
pub fn build(ca: Ca, revoked: Revoked) -> Result<Pki, NodesError> {
    let (core_cert, core_key) = ca.issue_core()?;
    let chain = vec![core_cert, ca.cert().clone()];
    let mut roots = RootCertStore::empty();
    roots.add(ca.cert().clone()).map_err(err)?;
    let roots = Arc::new(roots);

    let verifier = Arc::new(CheckedClient {
        inner: WebPkiClientVerifier::builder_with_provider(roots.clone(), provider())
            .allow_unauthenticated()
            .build()
            .map_err(err)?,
        revoked: revoked.clone(),
    });
    let mut server = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain.clone(), PrivateKeyDer::Pkcs8(core_key.clone_key()))
        .map_err(err)?;
    server.alpn_protocols = vec![b"h2".to_vec()];

    let dial_verifier = Arc::new(CheckedServer {
        inner: WebPkiServerVerifier::builder_with_provider(roots, provider())
            .build()
            .map_err(err)?,
        revoked,
    });
    let mut dial = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .dangerous()
        .with_custom_certificate_verifier(dial_verifier)
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

/// The CA check plus the revocation list, for client certificates (agents dialing in).
#[derive(Debug)]
struct CheckedClient {
    inner: Arc<dyn ClientCertVerifier>,
    revoked: Revoked,
}

impl ClientCertVerifier for CheckedClient {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let ok = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        if is_revoked(&self.revoked, end_entity) {
            return Err(rustls::Error::InvalidCertificate(CertificateError::Revoked));
        }
        Ok(ok)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// The CA and name check plus the revocation list, for agents the core dials.
#[derive(Debug)]
struct CheckedServer {
    inner: Arc<WebPkiServerVerifier>,
    revoked: Revoked,
}

impl ServerCertVerifier for CheckedServer {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let ok =
            self.inner
                .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)?;
        if is_revoked(&self.revoked, end_entity) {
            return Err(rustls::Error::InvalidCertificate(CertificateError::Revoked));
        }
        Ok(ok)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
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
