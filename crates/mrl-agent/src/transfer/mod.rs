//! Downloads and uploads by transfer ticket ([`mrl_storage::TransferTicket`]).
//!
//! - Download: into `<dest>.part`, resumed with an HTTP `Range` request after a dropped
//!   connection or an agent restart, checked against the expected sha256, then renamed.
//! - Upload: one `PUT` of the whole file (a PUT ticket covers one object), retried from the
//!   start after a failure. Sends `x-mrl-content-sha256` so the receiving side can check it,
//!   and fails if the file changes during the upload.
//! - Ticket headers are sent exactly as given; the ticket's size limit and expiry are checked
//!   on this side too. Relative ticket URLs (the server-proxied endpoint) use
//!   `transfer.server_url`.
//! - One node-wide speed limit (`transfer.bandwidth_bytes_per_sec`) is shared by every
//!   transfer of a client.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mrl_storage::{StorageError, TicketMethod, TransferTicket};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use crate::config::TransferConfig;
use crate::{AgentError, Result};

/// Header that carries the upload's sha256 (lowercase hex).
pub const SHA256_HEADER: &str = "x-mrl-content-sha256";

/// What a finished transfer did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferReport {
    /// Bytes in the object.
    pub bytes: u64,
    /// sha256 of the content (lowercase hex).
    pub sha256: String,
    /// Bytes that were already on disk from an earlier try (downloads).
    pub resumed_from: u64,
    /// Requests made.
    pub attempts: u32,
}

/// A shared speed limit: each chunk books the next free time slot.
#[derive(Debug)]
struct Throttle {
    bytes_per_sec: u64,
    next: tokio::sync::Mutex<Instant>,
}

impl Throttle {
    async fn take(&self, n: usize) {
        let start = {
            let mut next = self.next.lock().await;
            let start = (*next).max(Instant::now());
            *next = start + Duration::from_secs_f64(n as f64 / self.bytes_per_sec as f64);
            start
        };
        tokio::time::sleep_until(start).await;
    }
}

/// The transfer client. Cheap to clone; clones share the speed limit.
#[derive(Debug, Clone)]
pub struct TransferClient {
    http: reqwest::Client,
    server_url: Option<String>,
    launch_secret: Option<reqwest::header::HeaderValue>,
    retries: u32,
    throttle: Option<Arc<Throttle>>,
}

enum Step {
    Done,
    Retry(String),
}

fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(500u64.saturating_mul(1 << attempt.min(6))).min(Duration::from_secs(30))
}

fn retryable(status: u16) -> bool {
    status == 429 || status >= 500
}

impl TransferClient {
    /// A client with these settings.
    pub fn new(config: &TransferConfig) -> Result<Self> {
        let launch_secret = config
            .server_launch_secret_file
            .as_ref()
            .map(|path| {
                let invalid = |reason: &str| AgentError::Config {
                    path: path.clone(),
                    reason: reason.to_owned(),
                };
                let value = std::fs::read_to_string(path)
                    .map_err(|_| invalid("the Solo launch secret file could not be read"))?;
                let mut value =
                    reqwest::header::HeaderValue::from_str(value.trim_end_matches(['\r', '\n']))
                        .map_err(|_| {
                            invalid("the Solo launch secret is not a valid header value")
                        })?;
                if value.is_empty() {
                    return Err(invalid("the Solo launch secret file is empty"));
                }
                value.set_sensitive(true);
                let server = config
                    .server_url
                    .as_deref()
                    .and_then(|url| reqwest::Url::parse(url).ok())
                    .ok_or_else(|| {
                        invalid("configure transfer.server_url for the Solo launch secret")
                    })?;
                if !matches!(server.scheme(), "http" | "https")
                    || server.host_str().is_none()
                    || server.query().is_some()
                    || server.fragment().is_some()
                    || !server.username().is_empty()
                    || server.password().is_some()
                {
                    return Err(invalid("configure an HTTP or HTTPS Solo transfer base URL"));
                }
                Ok(value)
            })
            .transpose()?;
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs.max(1)))
            .read_timeout(Duration::from_secs(60));
        if launch_secret.is_some() {
            // The private header must never follow redirects to another handler or host.
            builder = builder.redirect(reqwest::redirect::Policy::none());
        }
        let http = builder.build().map_err(|e| AgentError::Config {
            path: PathBuf::from("<transfer settings>"),
            reason: format!("the HTTP client could not start ({e})"),
        })?;
        Ok(Self {
            http,
            server_url: config.server_url.clone(),
            launch_secret,
            retries: config.retries,
            throttle: config.bandwidth_bytes_per_sec.map(|rate| {
                Arc::new(Throttle {
                    bytes_per_sec: rate,
                    next: tokio::sync::Mutex::new(Instant::now()),
                })
            }),
        })
    }

    fn url(&self, t: &TransferTicket) -> Result<String> {
        if t.url.starts_with("http://") || t.url.starts_with("https://") {
            return Ok(t.url.clone());
        }
        let base = self
            .server_url
            .as_deref()
            .ok_or_else(|| AgentError::Transfer {
                key: t.key.clone(),
                reason: "the ticket points at the server, but transfer.server_url is not set"
                    .into(),
            })?;
        Ok(format!("{}{}", base.trim_end_matches('/'), t.url))
    }

    fn check(t: &TransferTicket, method: TicketMethod) -> Result<()> {
        if t.method != method {
            return Err(StorageError::TicketScope(format!(
                "it allows {:?}, not {method:?}",
                t.method
            ))
            .into());
        }
        if time::OffsetDateTime::now_utc() >= t.expires_at {
            return Err(StorageError::TicketExpired.into());
        }
        Ok(())
    }

    fn request(
        &self,
        method: reqwest::Method,
        url: &str,
        t: &TransferTicket,
    ) -> reqwest::RequestBuilder {
        let mut req = self.http.request(method, url);
        for (k, v) in &t.headers {
            req = req.header(k, v);
        }
        let proxied = t.url.strip_prefix("/api/v1/transfer/").filter(|token| {
            !token.is_empty()
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        });
        if let (Some(secret), Some(_)) = (&self.launch_secret, proxied) {
            req = req.header("x-mrl-launch-secret", secret.clone());
        }
        req
    }

    async fn throttle(&self, n: usize) {
        if let Some(t) = &self.throttle {
            t.take(n).await;
        }
    }

    /// Downloads the ticket's object to `dest`, resuming a `<dest>.part` left by an earlier
    /// try. With `expected_sha256`, a mismatch deletes the partial file and fails.
    pub async fn download(
        &self,
        ticket: &TransferTicket,
        dest: &Path,
        expected_sha256: Option<&str>,
    ) -> Result<TransferReport> {
        let url = self.url(ticket)?;
        let mut part = dest.as_os_str().to_owned();
        part.push(".part");
        let part = PathBuf::from(part);
        let resumed_from = tokio::fs::metadata(&part).await.map_or(0, |m| m.len());
        let mut attempts = 0;
        loop {
            Self::check(ticket, TicketMethod::Get)?;
            attempts += 1;
            match self.download_once(ticket, &url, &part).await? {
                Step::Done => break,
                Step::Retry(reason) if attempts > self.retries => {
                    return Err(AgentError::Transfer {
                        key: ticket.key.clone(),
                        reason: format!("{reason} (after {attempts} tries)"),
                    })
                }
                Step::Retry(_) => tokio::time::sleep(backoff(attempts - 1)).await,
            }
        }
        let (bytes, sha256) = mrl_storage::sha256_file(&part).await?;
        if let Some(expected) = expected_sha256 {
            if !expected.eq_ignore_ascii_case(&sha256) {
                let _ = tokio::fs::remove_file(&part).await;
                return Err(StorageError::ChecksumMismatch {
                    key: ticket.key.clone(),
                    expected: expected.to_owned(),
                    actual: sha256,
                }
                .into());
            }
        }
        tokio::fs::rename(&part, dest)
            .await
            .map_err(|e| AgentError::state(dest, e))?;
        Ok(TransferReport {
            bytes,
            sha256,
            resumed_from,
            attempts,
        })
    }

    async fn download_once(&self, t: &TransferTicket, url: &str, part: &Path) -> Result<Step> {
        let io = |e| AgentError::state(part, e);
        let mut offset = tokio::fs::metadata(part).await.map_or(0, |m| m.len());
        if offset > t.max_size {
            offset = 0;
        }
        let mut req = self.request(reqwest::Method::GET, url, t);
        if offset > 0 {
            req = req.header("range", format!("bytes={offset}-"));
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return Ok(Step::Retry(e.without_url().to_string())),
        };
        let status = resp.status().as_u16();
        match status {
            200 => offset = 0,
            206 => {
                // `content-range: bytes START-END/TOTAL` must start where the file ends.
                let start = resp
                    .headers()
                    .get("content-range")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("bytes "))
                    .and_then(|v| v.split('-').next())
                    .and_then(|v| v.parse::<u64>().ok());
                if start != Some(offset) {
                    let _ = tokio::fs::remove_file(part).await;
                    return Ok(Step::Retry("the server resumed at the wrong place".into()));
                }
            }
            // The partial file already holds the whole object; the hash check decides.
            416 if offset > 0 => return Ok(Step::Done),
            401 | 403 => {
                return Err(AgentError::TicketRefused {
                    key: t.key.clone(),
                    status,
                })
            }
            404 => return Err(StorageError::NotFound { key: t.key.clone() }.into()),
            412 => {
                let _ = tokio::fs::remove_file(part).await;
                return Err(AgentError::Transfer {
                    key: t.key.clone(),
                    reason: "the object changed after the ticket was issued".into(),
                });
            }
            s if retryable(s) => return Ok(Step::Retry(format!("HTTP {s}"))),
            s => {
                return Err(AgentError::Transfer {
                    key: t.key.clone(),
                    reason: format!("unexpected HTTP {s}"),
                })
            }
        }
        if let Some(len) = resp.content_length() {
            check_len(t, offset.saturating_add(len))?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(offset == 0)
            .append(offset > 0)
            .open(part)
            .await
            .map_err(io)?;
        let mut total = offset;
        let mut body = resp.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    file.flush().await.map_err(io)?;
                    return Ok(Step::Retry(e.without_url().to_string()));
                }
            };
            total += chunk.len() as u64;
            check_len(t, total)?;
            self.throttle(chunk.len()).await;
            file.write_all(&chunk).await.map_err(io)?;
        }
        file.flush().await.map_err(io)?;
        file.sync_all().await.map_err(io)?;
        Ok(Step::Done)
    }

    /// Uploads `src` with the ticket. Fails with [`StorageError::SourceChanged`] if the file
    /// changes while it is sent.
    pub async fn upload(&self, ticket: &TransferTicket, src: &Path) -> Result<TransferReport> {
        let url = self.url(ticket)?;
        let stamp = |m: std::fs::Metadata| (m.len(), m.modified().ok());
        let before = stamp(
            tokio::fs::metadata(src)
                .await
                .map_err(|e| AgentError::state(src, e))?,
        );
        check_len(ticket, before.0)?;
        let (bytes, sha256) = mrl_storage::sha256_file(src).await?;
        let mut attempts = 0;
        loop {
            Self::check(ticket, TicketMethod::Put)?;
            attempts += 1;
            match self.upload_once(ticket, &url, src, bytes, &sha256).await? {
                Step::Done => break,
                Step::Retry(reason) if attempts > self.retries => {
                    return Err(AgentError::Transfer {
                        key: ticket.key.clone(),
                        reason: format!("{reason} (after {attempts} tries)"),
                    })
                }
                Step::Retry(_) => tokio::time::sleep(backoff(attempts - 1)).await,
            }
        }
        let after = stamp(
            tokio::fs::metadata(src)
                .await
                .map_err(|e| AgentError::state(src, e))?,
        );
        if after != before {
            return Err(StorageError::SourceChanged(src.to_path_buf()).into());
        }
        Ok(TransferReport {
            bytes,
            sha256,
            resumed_from: 0,
            attempts,
        })
    }

    async fn upload_once(
        &self,
        t: &TransferTicket,
        url: &str,
        src: &Path,
        len: u64,
        sha256: &str,
    ) -> Result<Step> {
        let file = tokio::fs::File::open(src)
            .await
            .map_err(|e| AgentError::state(src, e))?;
        let me = self.clone();
        let stream = tokio_util::io::ReaderStream::new(file).then(move |chunk| {
            let me = me.clone();
            async move {
                if let Ok(c) = &chunk {
                    me.throttle(c.len()).await;
                }
                chunk
            }
        });
        let resp = self
            .request(reqwest::Method::PUT, url, t)
            .header("content-length", len)
            .header(SHA256_HEADER, sha256)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => return Ok(Step::Retry(e.without_url().to_string())),
        };
        match resp.status().as_u16() {
            200..=299 => Ok(Step::Done),
            s @ (401 | 403) => Err(AgentError::TicketRefused {
                key: t.key.clone(),
                status: s,
            }),
            413 => Err(StorageError::TicketScope(format!(
                "the server refused {len} bytes as too large"
            ))
            .into()),
            s if retryable(s) => Ok(Step::Retry(format!("HTTP {s}"))),
            s => Err(AgentError::Transfer {
                key: t.key.clone(),
                reason: format!("unexpected HTTP {s}"),
            }),
        }
    }
}

/// Fails when `n` bytes is more than the ticket allows.
fn check_len(t: &TransferTicket, n: u64) -> Result<()> {
    if n > t.max_size {
        return Err(StorageError::TicketScope(format!(
            "{n} bytes is more than the {} bytes this ticket allows",
            t.max_size
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
