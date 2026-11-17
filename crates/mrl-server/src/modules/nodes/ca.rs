//! The internal certificate authority (rcgen, ECDSA P-256).
//!
//! The CA key and certificate live in the `SecretStore` under one name (`gateway.ca_secret`),
//! never in the database or a plain file. The first replica that finds no CA creates one under
//! a PostgreSQL advisory lock, so replicas never race to make two.
//!
//! - Node certificates: SAN `<node uuid>.node.masterrl.internal`, client + server auth (the
//!   agent also serves TLS when the core dials in), 30 days (static) or ≤ 24 h (ephemeral).
//! - The core certificate: SAN [`CORE_NAME`], client + server auth, made fresh at every start.

use super::NodesError;
use crate::db::Db;
use crate::seams::{Secret, SecretStore};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SerialNumber,
};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use tokio_rustls::rustls::pki_types::{
    CertificateDer, CertificateSigningRequestDer, PrivatePkcs8KeyDer,
};
use uuid::Uuid;

/// TLS name of the core in its certificate; agents verify it whatever address they dial.
pub const CORE_NAME: &str = "core.masterrl.internal";
/// How long the core certificate lasts.
pub const CORE_CERT_DAYS: i64 = 397;
/// Advisory lock taken while creating the CA.
const CREATE_LOCK: i64 = 180_183;

/// TLS name of a node in its certificate.
pub fn node_name(node: Uuid) -> String {
    format!("{}.node.masterrl.internal", node.simple())
}

/// Lower-case hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Hex SHA-256 of `bytes` (certificate fingerprints, token hashes).
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// A certificate the CA signed.
#[derive(Debug, Clone)]
pub struct Issued {
    /// DER certificate.
    pub der: CertificateDer<'static>,
    /// Hex SHA-256 of `der`.
    pub sha256: String,
    /// Hex serial number.
    pub serial: String,
    /// End of validity.
    pub not_after: OffsetDateTime,
}

/// The loaded CA.
pub struct Ca {
    cert: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
    sha256: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Stored {
    key_pem: String,
    cert_pem: String,
}

impl Ca {
    /// Loads the CA from `secrets` under `name`, creating it (once, across replicas) if missing.
    pub async fn load_or_create(
        secrets: &dyn SecretStore,
        name: &str,
        db: &Db,
    ) -> Result<Self, NodesError> {
        if let Some(s) = secrets.get(name).await.map_err(ca_err)? {
            return Self::from_secret(&s);
        }
        let mut conn = db.dedicated().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(CREATE_LOCK)
            .execute(&mut conn)
            .await
            .map_err(crate::db::DbError::from)?;
        let result = async {
            if let Some(s) = secrets.get(name).await.map_err(ca_err)? {
                return Self::from_secret(&s);
            }
            let (ca, secret) = Self::generate()?;
            secrets.set(name, secret).await.map_err(ca_err)?;
            tracing::info!(fingerprint = %ca.sha256, "created the internal CA");
            Ok(ca)
        }
        .await;
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(CREATE_LOCK)
            .execute(&mut conn)
            .await;
        result
    }

    /// A new CA and the secret that stores it.
    pub fn generate() -> Result<(Self, Secret), NodesError> {
        let key = KeyPair::generate().map_err(ca_err)?;
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "MasterRL internal CA");
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let now = OffsetDateTime::now_utc();
        params.not_before = now - Duration::hours(1);
        params.not_after = now + Duration::days(3650);
        params.serial_number = Some(serial());
        let cert = params.self_signed(&key).map_err(ca_err)?;
        let stored = Stored {
            key_pem: key.serialize_pem(),
            cert_pem: cert.pem(),
        };
        let json = serde_json::to_string(&stored).map_err(ca_err)?;
        let der = cert.der().clone();
        let issuer = Issuer::new(params, key);
        Ok((
            Self {
                sha256: sha256_hex(&der),
                cert: der,
                issuer,
            },
            Secret::new(json),
        ))
    }

    fn from_secret(s: &Secret) -> Result<Self, NodesError> {
        let stored: Stored = serde_json::from_str(s.expose())
            .map_err(|_| NodesError::Ca("the stored CA could not be read".into()))?;
        let key = KeyPair::from_pem(&stored.key_pem)
            .map_err(|_| NodesError::Ca("the stored CA key could not be read".into()))?;
        let der: CertificateDer<'static> = {
            use tokio_rustls::rustls::pki_types::pem::PemObject;
            CertificateDer::from_pem_slice(stored.cert_pem.as_bytes())
                .map_err(|_| NodesError::Ca("the stored CA certificate could not be read".into()))?
        };
        let issuer = Issuer::from_ca_cert_der(&der, key)
            .map_err(|_| NodesError::Ca("the stored CA certificate could not be read".into()))?;
        Ok(Self {
            sha256: sha256_hex(&der),
            cert: der,
            issuer,
        })
    }

    /// The CA certificate (DER).
    pub fn cert(&self) -> &CertificateDer<'static> {
        &self.cert
    }

    /// Hex SHA-256 of the CA certificate; agents pin it at enrollment.
    pub fn fingerprint(&self) -> &str {
        &self.sha256
    }

    /// Signs a node certificate for the key in `csr_der` (ECDSA P-256 only). Every name in
    /// the request is ignored: the CA sets the names.
    pub fn issue_node(
        &self,
        csr_der: &[u8],
        node: Uuid,
        lifetime: Duration,
    ) -> Result<Issued, NodesError> {
        let csr =
            CertificateSigningRequestParams::from_der(&CertificateSigningRequestDer::from(csr_der))
                .map_err(|_| {
                    NodesError::Certificate(
                        "the certificate request is damaged or its signature is wrong".into(),
                    )
                })?;
        if csr.public_key.algorithm() != &rcgen::PKCS_ECDSA_P256_SHA256 {
            return Err(NodesError::Certificate(
                "the certificate request must use an ECDSA P-256 key".into(),
            ));
        }
        let mut params = leaf_params(&node.to_string(), &node_name(node), lifetime)?;
        params.serial_number = Some(serial());
        let cert = params
            .signed_by(&csr.public_key, &self.issuer)
            .map_err(ca_err)?;
        let der = cert.der().clone();
        Ok(Issued {
            sha256: sha256_hex(&der),
            serial: params
                .serial_number
                .as_ref()
                .map(|s| hex(&s.to_bytes()))
                .unwrap_or_default(),
            not_after: params.not_after,
            der,
        })
    }

    /// A fresh core certificate and its PKCS#8 key.
    pub fn issue_core(
        &self,
    ) -> Result<(CertificateDer<'static>, PrivatePkcs8KeyDer<'static>), NodesError> {
        let key = KeyPair::generate().map_err(ca_err)?;
        let mut params = leaf_params("MasterRL core", CORE_NAME, Duration::days(CORE_CERT_DAYS))?;
        params.serial_number = Some(serial());
        let cert = params.signed_by(&key, &self.issuer).map_err(ca_err)?;
        Ok((
            cert.der().clone(),
            PrivatePkcs8KeyDer::from(key.serialize_der()),
        ))
    }
}

fn leaf_params(cn: &str, dns: &str, lifetime: Duration) -> Result<CertificateParams, NodesError> {
    let mut params = CertificateParams::new(vec![dns.to_owned()]).map_err(ca_err)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    params.distinguished_name = dn;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth,
    ];
    params.use_authority_key_identifier_extension = true;
    let now = OffsetDateTime::now_utc();
    // A few minutes back, so a machine whose clock is slightly behind still accepts it.
    params.not_before = now - Duration::minutes(5);
    params.not_after = now + lifetime;
    Ok(params)
}

fn serial() -> SerialNumber {
    let mut bytes = [0u8; 16];
    bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    bytes[0] &= 0x7f; // positive
    SerialNumber::from_slice(&bytes)
}

fn ca_err(e: impl std::fmt::Display) -> NodesError {
    NodesError::Ca(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn csr(key: &KeyPair) -> Vec<u8> {
        CertificateParams::new(vec!["ignored.example".to_owned()])
            .unwrap()
            .serialize_request(key)
            .unwrap()
            .der()
            .to_vec()
    }

    #[test]
    fn issues_node_and_core_certs_that_verify() {
        let (ca, secret) = Ca::generate().unwrap();
        let again = Ca::from_secret(&secret).unwrap();
        assert_eq!(again.fingerprint(), ca.fingerprint());
        let node = Uuid::now_v7();
        let key = KeyPair::generate().unwrap();
        let issued = again
            .issue_node(&csr(&key), node, Duration::hours(24))
            .unwrap();
        assert_eq!(issued.sha256.len(), 64);
        assert!(issued.not_after <= OffsetDateTime::now_utc() + Duration::hours(24));

        let anchor = webpki::anchor_from_trusted_cert(ca.cert()).unwrap();
        let ee = webpki::EndEntityCert::try_from(&issued.der).unwrap();
        let now = tokio_rustls::rustls::pki_types::UnixTime::now();
        ee.verify_for_usage(
            webpki::ALL_VERIFICATION_ALGS,
            std::slice::from_ref(&anchor),
            &[],
            now,
            webpki::KeyUsage::client_auth(),
            None,
            None,
        )
        .unwrap();
        let name = tokio_rustls::rustls::pki_types::ServerName::try_from(node_name(node)).unwrap();
        ee.verify_is_valid_for_subject_name(&name).unwrap();

        let (core, _) = ca.issue_core().unwrap();
        let core_ee = webpki::EndEntityCert::try_from(&core).unwrap();
        let core_name = tokio_rustls::rustls::pki_types::ServerName::try_from(CORE_NAME).unwrap();
        core_ee
            .verify_is_valid_for_subject_name(&core_name)
            .unwrap();
        assert!(core_ee.verify_is_valid_for_subject_name(&name).is_err());
    }

    #[test]
    fn refuses_damaged_and_non_p256_requests() {
        let (ca, _) = Ca::generate().unwrap();
        let node = Uuid::now_v7();
        assert!(matches!(
            ca.issue_node(b"junk", node, Duration::hours(1)),
            Err(NodesError::Certificate(_))
        ));
        let ed = KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let e = ca
            .issue_node(&csr(&ed), node, Duration::hours(1))
            .unwrap_err();
        assert!(e.to_string().contains("P-256"), "{e}");
    }
}
