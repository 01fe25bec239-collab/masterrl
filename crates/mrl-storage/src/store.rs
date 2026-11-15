//! Backends and the org-scoped [`Store`] handle.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::multipart::MultipartStore;
use object_store::path::Path;
use object_store::signer::{HeaderName, HeaderValue, Method, SignedUrlOptions, Signer};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Result, StorageError};
use crate::hash::sha256_stream;
use crate::keys::OrgScope;
use crate::manifest::ArtifactManifest;
use crate::ticket::{
    expiry, sign_ticket, TicketClaims, TicketKey, TicketMethod, TransferTicket, PROXY_PATH,
};

/// Where objects live.
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendConfig {
    /// A folder on local disk (the desktop default).
    Local {
        /// The root folder. Created if missing.
        root: PathBuf,
    },
    /// Any S3-compatible service (AWS, SeaweedFS, MinIO, R2).
    S3(S3Config),
    /// Google Cloud Storage. Credentials come from the standard `GOOGLE_*` variables.
    #[cfg(feature = "gcs")]
    Gcs {
        /// Bucket name.
        bucket: String,
    },
    /// Azure Blob Storage. Credentials come from the standard `AZURE_*` variables.
    #[cfg(feature = "azure")]
    Azure {
        /// Storage account.
        account: String,
        /// Container name.
        container: String,
    },
}

/// S3 settings. Missing keys fall back to the standard `AWS_*` variables.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    /// Bucket name.
    pub bucket: String,
    /// Region.
    #[serde(default = "default_region")]
    pub region: String,
    /// Custom endpoint, such as `http://127.0.0.1:8333`. Uses path-style requests.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Access key id.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Secret access key.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Allow plain `http://` endpoints (local dev only).
    #[serde(default)]
    pub allow_http: bool,
    /// Send `x-amz-checksum-sha256` so the service verifies each upload. Turn off for
    /// services that do not support it.
    #[serde(default = "yes")]
    pub checksum: bool,
    /// Set when the bucket has Object Lock with a default retention rule. This crate does
    /// not set retention itself; the flag only reports what the bucket enforces.
    #[serde(default)]
    pub object_lock: bool,
}

fn default_region() -> String {
    "us-east-1".into()
}

fn yes() -> bool {
    true
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| ".."),
            )
            .field("allow_http", &self.allow_http)
            .field("checksum", &self.checksum)
            .field("object_lock", &self.object_lock)
            .finish()
    }
}

/// What a backend really supports. Nothing here is emulated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Capabilities {
    /// Hands out presigned GET URLs (size checked, version pinned). PUT tickets, and GET
    /// tickets without it, go through the server proxy.
    pub presign: bool,
    /// Has native multipart uploads. Without it, parts are staged as objects under
    /// `uploads/{id}/parts/` and joined on completion.
    pub multipart: bool,
    /// The bucket enforces write-once retention.
    pub object_lock: bool,
    /// The service verifies a sha256 sent with each upload.
    pub checksum: bool,
}

/// A connected backend. Cheap to clone. Use [`Backend::org`] to get a [`Store`].
#[derive(Clone)]
pub struct Backend {
    pub(crate) objects: Arc<dyn ObjectStore>,
    pub(crate) multipart: Option<Arc<dyn MultipartStore>>,
    signer: Option<Arc<dyn Signer>>,
    caps: Capabilities,
    ticket_key: Option<TicketKey>,
}

impl fmt::Debug for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend")
            .field("objects", &self.objects.to_string())
            .field("caps", &self.caps)
            .finish()
    }
}

impl Backend {
    /// Connects to the configured backend.
    pub fn connect(config: &BackendConfig) -> Result<Self> {
        match config {
            BackendConfig::Local { root } => {
                std::fs::create_dir_all(root).map_err(|e| StorageError::io(root, e))?;
                let fs = object_store::local::LocalFileSystem::new_with_prefix(root)
                    .map_err(StorageError::Backend)?;
                let caps = Capabilities {
                    presign: false,
                    multipart: false,
                    object_lock: false,
                    checksum: false,
                };
                Ok(Self::assemble(Arc::new(fs), None, None, caps))
            }
            BackendConfig::S3(c) => {
                use object_store::aws::{AmazonS3Builder, Checksum};
                let mut b = AmazonS3Builder::from_env()
                    .with_bucket_name(&c.bucket)
                    .with_region(&c.region)
                    .with_allow_http(c.allow_http);
                if let Some(e) = &c.endpoint {
                    b = b.with_endpoint(e).with_virtual_hosted_style_request(false);
                }
                if let Some(k) = &c.access_key_id {
                    b = b.with_access_key_id(k);
                }
                if let Some(s) = &c.secret_access_key {
                    b = b.with_secret_access_key(s);
                }
                if c.checksum {
                    b = b.with_checksum_algorithm(Checksum::SHA256);
                }
                let s3 = Arc::new(b.build().map_err(StorageError::Backend)?);
                let caps = Capabilities {
                    presign: true,
                    multipart: true,
                    object_lock: c.object_lock,
                    checksum: c.checksum,
                };
                Ok(Self::assemble(s3.clone(), Some(s3.clone()), Some(s3), caps))
            }
            #[cfg(feature = "gcs")]
            BackendConfig::Gcs { bucket } => {
                let gcs = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(StorageError::Backend)?;
                let gcs = Arc::new(gcs);
                let caps = Capabilities {
                    presign: true,
                    multipart: true,
                    object_lock: false,
                    checksum: false,
                };
                Ok(Self::assemble(
                    gcs.clone(),
                    Some(gcs.clone()),
                    Some(gcs),
                    caps,
                ))
            }
            #[cfg(feature = "azure")]
            BackendConfig::Azure { account, container } => {
                let az = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_account(account)
                    .with_container_name(container)
                    .build()
                    .map_err(StorageError::Backend)?;
                let az = Arc::new(az);
                // Azure SAS URLs cannot sign headers, so a size limit cannot be bound into
                // them: tickets go through the proxy and presign is reported as missing.
                let caps = Capabilities {
                    presign: false,
                    multipart: true,
                    object_lock: false,
                    checksum: false,
                };
                Ok(Self::assemble(az.clone(), Some(az), None, caps))
            }
        }
    }

    fn assemble(
        objects: Arc<dyn ObjectStore>,
        multipart: Option<Arc<dyn MultipartStore>>,
        signer: Option<Arc<dyn Signer>>,
        caps: Capabilities,
    ) -> Self {
        Self {
            objects,
            multipart,
            signer,
            caps,
            ticket_key: None,
        }
    }

    /// Sets the key that signs tickets for the server-proxied transfer endpoint.
    /// Needed by every backend for PUT tickets, which always use the proxy, and by backends
    /// without `presign` for GET tickets too.
    pub fn with_ticket_key(mut self, key: TicketKey) -> Self {
        self.ticket_key = Some(key);
        self
    }

    /// What this backend supports.
    pub fn capabilities(&self) -> Capabilities {
        self.caps
    }

    /// A handle that can only touch `orgs/{org_id}/`.
    pub fn org(&self, org_id: &str) -> Result<Store> {
        Ok(Store {
            backend: self.clone(),
            scope: OrgScope::new(org_id)?,
        })
    }
}

/// Size and version of a stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    /// Full key.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// Entity tag, if the backend reports one.
    pub etag: Option<String>,
    /// Last change time.
    pub last_modified: OffsetDateTime,
}

/// Storage for one org. Every key it reads, writes, lists or signs is under `orgs/{org}/`.
#[derive(Debug, Clone)]
pub struct Store {
    pub(crate) backend: Backend,
    scope: OrgScope,
}

impl Store {
    /// Key builders for this org.
    pub fn keys(&self) -> &OrgScope {
        &self.scope
    }

    /// What the backend supports.
    pub fn capabilities(&self) -> Capabilities {
        self.backend.caps
    }

    pub(crate) fn path(&self, key: &str) -> Result<Path> {
        let key = self.scope.check(key)?;
        Path::parse(key).map_err(|_| StorageError::BadKey {
            key: key.to_owned(),
            reason: "the object store rejected it",
        })
    }

    /// Writes a whole object.
    pub async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.path(key)?;
        self.backend
            .objects
            .put(&path, PutPayload::from(data))
            .await
            .map_err(|e| StorageError::backend(key, e))?;
        Ok(())
    }

    /// Reads a whole object.
    pub async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.path(key)?;
        let res = self
            .backend
            .objects
            .get(&path)
            .await
            .map_err(|e| StorageError::backend(key, e))?;
        res.bytes().await.map_err(|e| StorageError::backend(key, e))
    }

    /// Reads a byte range.
    pub async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        let path = self.path(key)?;
        self.backend
            .objects
            .get_range(&path, range)
            .await
            .map_err(|e| StorageError::backend(key, e))
    }

    /// Size and version of an object.
    pub async fn head(&self, key: &str) -> Result<ObjectInfo> {
        let path = self.path(key)?;
        let meta = self
            .backend
            .objects
            .head(&path)
            .await
            .map_err(|e| StorageError::backend(key, e))?;
        Ok(info(meta))
    }

    /// Every object under `prefix` (which ends in `/`, or is the org prefix), sorted by key.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let prefix = self.scope.check_prefix(prefix)?;
        let path = Path::parse(prefix.trim_end_matches('/')).map_err(|_| StorageError::BadKey {
            key: prefix.to_owned(),
            reason: "the object store rejected it",
        })?;
        let mut out: Vec<ObjectInfo> = self
            .backend
            .objects
            .list(Some(&path))
            .map_ok(info)
            .try_collect()
            .await
            .map_err(|e| StorageError::backend(prefix, e))?;
        out.retain(|o| self.scope.check(&o.key).is_ok());
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    /// Deletes an object. Deleting a missing object succeeds.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let path = self.path(key)?;
        match self.backend.objects.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(StorageError::Backend(e)),
        }
    }

    /// Streams an object and returns its sha256.
    pub async fn sha256(&self, key: &str) -> Result<String> {
        let path = self.path(key)?;
        let res = self
            .backend
            .objects
            .get(&path)
            .await
            .map_err(|e| StorageError::backend(key, e))?;
        let owned = key.to_owned();
        let stream = res
            .into_stream()
            .map_err(move |e| StorageError::backend(&owned, e));
        Ok(sha256_stream(stream).await?.1)
    }

    /// Writes a manifest as JSON after validating it.
    pub async fn put_manifest(&self, key: &str, manifest: &ArtifactManifest) -> Result<()> {
        manifest.validate()?;
        self.put(key, Bytes::from(manifest.to_json()?)).await
    }

    /// Reads and validates a manifest.
    pub async fn get_manifest(&self, key: &str) -> Result<ArtifactManifest> {
        ArtifactManifest::from_json(&self.get(key).await?)
    }

    /// A ticket for one `method` on one `key`, up to `max_size` bytes, valid for `ttl`
    /// (at most 15 minutes). The size limit is always enforced:
    ///
    /// - GET: the object size is checked now. On a presign backend the result is a presigned
    ///   URL whose signed `if-match` header pins this version, so a bigger object written later
    ///   cannot be read with it.
    /// - PUT, and GET elsewhere: a token for the server-proxied endpoint, which checks it with
    ///   [`crate::verify_ticket`] and [`TicketClaims::check_size`]. Needs
    ///   [`Backend::with_ticket_key`].
    pub async fn transfer_ticket(
        &self,
        key: &str,
        method: TicketMethod,
        max_size: u64,
        ttl: Duration,
    ) -> Result<TransferTicket> {
        let path = self.path(key)?;
        let now = OffsetDateTime::now_utc();
        let expires_at = expiry(now, ttl)?;
        let presigned = match method {
            TicketMethod::Get => {
                let info = self.head(key).await?;
                if info.size > max_size {
                    return Err(StorageError::TicketScope(format!(
                        "the object is {} bytes, more than the {max_size} bytes allowed",
                        info.size
                    )));
                }
                match (&self.backend.signer, info.etag) {
                    (Some(signer), Some(etag)) => {
                        self.presign_get(signer.as_ref(), key, &path, etag, ttl)
                            .await?
                    }
                    _ => None,
                }
            }
            // ponytail: object_store cannot sign content-length, so a presigned PUT cannot cap
            // the size; PUTs go through the proxy. Upgrade: S3 POST policy with content-length-range.
            TicketMethod::Put => None,
        };
        let (url, headers, presigned) = match presigned {
            Some((url, headers)) => (url, headers, true),
            None => {
                let tk = self.backend.ticket_key.as_ref().ok_or_else(|| {
                    StorageError::Config(
                        "this backend needs a ticket key to issue transfer tickets".into(),
                    )
                })?;
                let claims = TicketClaims {
                    key: key.to_owned(),
                    method,
                    max_size,
                    issued_at: now.unix_timestamp(),
                    expires_at: expires_at.unix_timestamp(),
                };
                let url = format!("{PROXY_PATH}{}", sign_ticket(tk, &claims)?);
                (url, BTreeMap::new(), false)
            }
        };
        Ok(TransferTicket {
            method,
            key: key.to_owned(),
            max_size,
            expires_at,
            url,
            presigned,
            headers,
        })
    }

    /// A presigned GET pinned to version `etag`, or `None` if the signer cannot sign headers.
    async fn presign_get(
        &self,
        signer: &dyn Signer,
        key: &str,
        path: &Path,
        etag: String,
        ttl: Duration,
    ) -> Result<Option<(String, BTreeMap<String, String>)>> {
        let Ok(value) = HeaderValue::from_str(&etag) else {
            return Ok(None);
        };
        let opts =
            SignedUrlOptions::new().with_signed_header(HeaderName::from_static("if-match"), value);
        match signer.signed_url_opts(Method::GET, path, ttl, &opts).await {
            Ok(url) => Ok(Some((
                url.to_string(),
                BTreeMap::from([("if-match".to_owned(), etag)]),
            ))),
            Err(object_store::Error::NotSupported { .. }) => Ok(None),
            Err(e) => Err(StorageError::backend(key, e)),
        }
    }
}

fn info(meta: object_store::ObjectMeta) -> ObjectInfo {
    let last_modified = OffsetDateTime::from_unix_timestamp_nanos(i128::from(
        meta.last_modified.timestamp_nanos_opt().unwrap_or(0),
    ))
    .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    ObjectInfo {
        key: meta.location.to_string(),
        size: meta.size,
        etag: meta.e_tag,
        last_modified,
    }
}
