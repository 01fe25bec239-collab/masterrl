//! TLS settings on the agent side (rustls + ring, ALPN `h2`).
//!
//! - Before enrollment the agent knows only the CA fingerprint from its join token. It accepts
//!   the core only if the chain the core presents contains a CA certificate with that
//!   fingerprint, the core's leaf chains to it, and the leaf is named [`CORE_NAME`]. The token is
//!   sent only after that check.
//! - After enrollment: the node certificate is the client certificate (agent dials) or the
//!   server certificate (core dials); the peer must chain to the stored CA and be the core.

use crate::session::LinkError;
use sha2::{Digest, Sha256};
use std::sync::Arc;
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
    self, ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
};

/// The core's TLS name (its certificate names it, whatever address is dialed).
pub const CORE_NAME: &str = "core.masterrl.internal";

/// Hex SHA-256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

fn err(e: impl std::fmt::Display) -> LinkError {
    LinkError::Certificate(format!("TLS settings: {e}"))
}

fn roots(ca: &CertificateDer<'static>) -> Result<Arc<RootCertStore>, LinkError> {
    let mut r = RootCertStore::empty();
    r.add(ca.clone()).map_err(err)?;
    Ok(Arc::new(r))
}

/// The pinned CA from a chain the peer presented.
fn pinned<'a>(chain: &'a [CertificateDer<'a>], pin: &str) -> Option<&'a CertificateDer<'a>> {
    chain
        .iter()
        .find(|c| sha256_hex(c).eq_ignore_ascii_case(pin))
}

/// True when `leaf` is the core's certificate (named [`CORE_NAME`]); the chain was checked already.
pub fn is_core(leaf: &CertificateDer<'_>) -> bool {
    let Ok(name) = ServerName::try_from(CORE_NAME) else {
        return false;
    };
    webpki::EndEntityCert::try_from(leaf)
        .is_ok_and(|ee| ee.verify_is_valid_for_subject_name(&name).is_ok())
}

/// Client TLS for enrolling: trust only a core whose chain has the pinned CA.
pub fn enroll_client(pin: &str) -> Result<ClientConfig, LinkError> {
    let mut c = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCore {
            pin: pin.to_owned(),
            provider: provider(),
        }))
        .with_no_client_auth();
    c.alpn_protocols = vec![b"h2".to_vec()];
    Ok(c)
}

/// Client TLS for an enrolled agent: the node certificate, the stored CA.
pub fn client(
    ca: &CertificateDer<'static>,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<ClientConfig, LinkError> {
    let mut c = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .with_root_certificates(roots(ca)?)
        .with_client_auth_cert(chain, key)
        .map_err(err)?;
    c.alpn_protocols = vec![b"h2".to_vec()];
    Ok(c)
}

/// Server TLS for an enrolled agent the core dials: client certificate required (checked
/// against the CA; [`is_core`] is checked after the handshake).
pub fn server(
    ca: &CertificateDer<'static>,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<ServerConfig, LinkError> {
    let verifier = WebPkiClientVerifier::builder_with_provider(roots(ca)?, provider())
        .build()
        .map_err(err)?;
    let mut s = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .map_err(err)?;
    s.alpn_protocols = vec![b"h2".to_vec()];
    Ok(s)
}

/// Server TLS for an agent waiting to enroll: a throw-away certificate; the core's client
/// certificate must chain to the pinned CA.
pub fn enroll_server(pin: &str) -> Result<ServerConfig, LinkError> {
    let temp = rcgen::generate_simple_self_signed(vec!["enroll.masterrl.internal".to_owned()])
        .map_err(err)?;
    let key = PrivateKeyDer::Pkcs8(temp.signing_key.serialize_der().into());
    let mut s = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(err)?
        .with_client_cert_verifier(Arc::new(PinnedClient {
            pin: pin.to_owned(),
            provider: provider(),
        }))
        .with_single_cert(vec![temp.cert.der().clone()], key)
        .map_err(err)?;
    s.alpn_protocols = vec![b"h2".to_vec()];
    Ok(s)
}

#[derive(Debug)]
struct PinnedCore {
    pin: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedCore {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let ca = pinned(intermediates, &self.pin).ok_or_else(|| {
            rustls::Error::General(
                "the server's certificate authority is not the one in the join token".into(),
            )
        })?;
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.clone().into_owned())
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        let inner =
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots), self.provider.clone())
                .build()
                .map_err(|e| rustls::Error::General(e.to_string()))?;
        let core =
            ServerName::try_from(CORE_NAME).map_err(|e| rustls::Error::General(e.to_string()))?;
        inner.verify_server_cert(end_entity, intermediates, &core, ocsp, now)
    }

    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(m, c, d, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(m, c, d, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
struct PinnedClient {
    pin: String,
    provider: Arc<CryptoProvider>,
}

impl ClientCertVerifier for PinnedClient {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let ca = pinned(intermediates, &self.pin).ok_or_else(|| {
            rustls::Error::General("the connecting core is not the one in the join token".into())
        })?;
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.clone().into_owned())
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        let inner =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), self.provider.clone())
                .build()
                .map_err(|e| rustls::Error::General(e.to_string()))?;
        inner.verify_client_cert(end_entity, intermediates, now)?;
        if !is_core(end_entity) {
            return Err(rustls::Error::General(
                "the connecting peer is not the MasterRL core".into(),
            ));
        }
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(m, c, d, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(m, c, d, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
