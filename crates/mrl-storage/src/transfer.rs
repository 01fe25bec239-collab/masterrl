//! Parallel, resumable file transfer.
//!
//! Large files move in fixed-size parts, several at a time, each retried with backoff.
//! Finished parts are recorded in a ledger file next to the local file
//! (`<name>.mrl-upload.json` / `<name>.mrl-download.json`), written atomically after every
//! part, so a crashed transfer picks up where it stopped.
//!
//! Uploads use native multipart where the backend has it. Otherwise each part is stored as
//! an object under `orgs/{org}/uploads/{upload_id}/parts/` and joined on completion.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path as LocalPath, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::{Result, StorageError};
use crate::hash::{blocking, sha256_file};
use crate::store::Store;

/// Smallest part size. S3 requires 5 MiB for every part but the last.
pub const MIN_PART_SIZE: u64 = 5 << 20;
/// Largest part size. S3 allows at most 5 GiB per part.
pub const MAX_PART_SIZE: u64 = 5 << 30;
const MAX_BACKOFF: Duration = Duration::from_secs(10);
const LEDGER_VERSION: u32 = 1;

/// How a transfer runs.
#[derive(Debug, Clone)]
pub struct TransferOptions {
    /// Bytes per part, from 5 MiB to 5 GiB.
    pub part_size: u64,
    /// Parts in flight at once.
    pub concurrency: usize,
    /// Retries per part after the first try.
    pub retries: u32,
    /// First retry delay; doubles each retry, up to 10 seconds.
    pub backoff: Duration,
}

impl Default for TransferOptions {
    fn default() -> Self {
        Self {
            part_size: 16 << 20,
            concurrency: 4,
            retries: 5,
            backoff: Duration::from_millis(200),
        }
    }
}

impl TransferOptions {
    fn validate(&self) -> Result<()> {
        if !(MIN_PART_SIZE..=MAX_PART_SIZE).contains(&self.part_size) {
            return Err(StorageError::Config(format!(
                "part size must be 5 MiB to 5 GiB, not {} bytes",
                self.part_size
            )));
        }
        if self.concurrency == 0 {
            return Err(StorageError::Config(
                "transfer concurrency must be at least 1".into(),
            ));
        }
        Ok(())
    }

    fn parts(&self, size: u64) -> usize {
        usize::try_from(size.div_ceil(self.part_size)).unwrap_or(usize::MAX)
    }

    fn range(&self, size: u64, idx: usize) -> std::ops::Range<u64> {
        let start = idx as u64 * self.part_size;
        start..(start + self.part_size).min(size)
    }
}

/// What a finished transfer did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferOutcome {
    /// Bytes transferred in total.
    pub size: u64,
    /// sha256 of the local file.
    pub sha256: String,
    /// Number of parts.
    pub parts: usize,
    /// Parts skipped because an earlier, interrupted run already finished them.
    pub resumed_parts: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Session {
    /// One plain put; never written to disk.
    Single,
    Native {
        upload_id: String,
    },
    Staged {
        upload_id: String,
    },
    Download,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ledger {
    version: u32,
    key: String,
    fingerprint: String,
    size: u64,
    part_size: u64,
    session: Session,
    /// Finished part index -> part id (empty for staged parts and downloads).
    done: BTreeMap<usize, String>,
}

impl Ledger {
    fn matches(&self, key: &str, fingerprint: &str, size: u64, part_size: u64) -> bool {
        self.version == LEDGER_VERSION
            && self.key == key
            && self.fingerprint == fingerprint
            && self.size == size
            && self.part_size == part_size
    }
}

fn ledger_path(local: &LocalPath, suffix: &str) -> Result<PathBuf> {
    let name = local
        .file_name()
        .ok_or_else(|| StorageError::Config(format!("{} is not a file path", local.display())))?;
    let mut name = name.to_os_string();
    name.push(suffix);
    Ok(local.with_file_name(name))
}

async fn load_ledger(path: &LocalPath) -> Option<Ledger> {
    let bytes = tokio::fs::read(path).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Writes the ledger to a temp file, syncs it, then renames it over the old one.
async fn save_ledger(path: &LocalPath, ledger: &Ledger) -> Result<()> {
    if ledger.session == Session::Single {
        return Ok(());
    }
    let json =
        serde_json::to_vec(ledger).map_err(|e| StorageError::io(path, std::io::Error::other(e)))?;
    let path = path.to_owned();
    blocking(move || {
        // Unique name: a cancelled part's save can still be running in the background.
        let tmp = path.with_extension(format!("json.{}.tmp", uuid::Uuid::now_v7().simple()));
        let mut f = std::fs::File::create(&tmp).map_err(|e| StorageError::io(&tmp, e))?;
        f.write_all(&json)
            .and_then(|()| f.sync_all())
            .map_err(|e| StorageError::io(&tmp, e))?;
        std::fs::rename(&tmp, &path).map_err(|e| StorageError::io(&path, e))
    })
    .await
}

async fn remove_file(path: &LocalPath) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StorageError::io(path, e)),
    }
}

/// Size and a change fingerprint (size + modification time) of a local file.
async fn local_fingerprint(path: &LocalPath) -> Result<(u64, String)> {
    let meta = tokio::fs::metadata(path)
        .await
        .map_err(|e| StorageError::io(path, e))?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos());
    Ok((meta.len(), format!("{}:{mtime}", meta.len())))
}

fn read_at(path: &LocalPath, range: std::ops::Range<u64>) -> Result<Bytes> {
    let err = |e| StorageError::io(path, e);
    let mut f = std::fs::File::open(path).map_err(err)?;
    f.seek(SeekFrom::Start(range.start)).map_err(err)?;
    let len =
        usize::try_from(range.end - range.start).map_err(|e| err(std::io::Error::other(e)))?;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf).map_err(err)?;
    Ok(Bytes::from(buf))
}

fn write_at(path: &LocalPath, offset: u64, data: &[u8]) -> Result<()> {
    let err = |e| StorageError::io(path, e);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(err)?;
    f.seek(SeekFrom::Start(offset)).map_err(err)?;
    f.write_all(data).map_err(err)?;
    // Data must be on disk before the ledger says the part is done.
    f.sync_data().map_err(err)
}

/// Errors worth another try: network and service hiccups, not "missing" or "forbidden".
fn retryable(e: &object_store::Error) -> bool {
    use object_store::Error as E;
    matches!(e, E::Generic { .. } | E::JoinError { .. })
}

/// Runs `op`, retrying retryable errors with doubling backoff.
async fn retry<T, F, Fut>(
    opts: &TransferOptions,
    mut op: F,
) -> std::result::Result<T, object_store::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, object_store::Error>>,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Err(e) if attempt < opts.retries && retryable(&e) => {
                // ponytail: no jitter; add it if many clients retry against one service at once.
                let delay = opts
                    .backoff
                    .saturating_mul(1 << attempt.min(10))
                    .min(MAX_BACKOFF);
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}

/// An upload in progress. Use [`Store::upload_file`] unless you need part-by-part control.
#[derive(Debug)]
pub struct Upload {
    store: Store,
    local: PathBuf,
    key: String,
    path: Path,
    size: u64,
    fingerprint: String,
    opts: TransferOptions,
    ledger_path: PathBuf,
    ledger: Mutex<Ledger>,
    resumed: usize,
}

impl Store {
    /// Uploads a local file to `key`: in parallel parts, retried, resumable after a crash.
    pub async fn upload_file(
        &self,
        local: impl Into<PathBuf>,
        key: &str,
        opts: TransferOptions,
    ) -> Result<TransferOutcome> {
        self.begin_upload(local, key, opts).await?.finish().await
    }

    /// Starts an upload, or resumes one whose ledger matches the file and key.
    pub async fn begin_upload(
        &self,
        local: impl Into<PathBuf>,
        key: &str,
        opts: TransferOptions,
    ) -> Result<Upload> {
        opts.validate()?;
        let local = local.into();
        let path = self.path(key)?;
        let (size, fingerprint) = local_fingerprint(&local).await?;
        let ledger_path = ledger_path(&local, ".mrl-upload.json")?;
        let fresh = |session| Ledger {
            version: LEDGER_VERSION,
            key: key.to_owned(),
            fingerprint: fingerprint.clone(),
            size,
            part_size: opts.part_size,
            session,
            done: BTreeMap::new(),
        };

        let ledger = if size <= opts.part_size {
            fresh(Session::Single)
        } else {
            let old = load_ledger(&ledger_path).await;
            let native = self.backend.multipart.is_some();
            match old {
                Some(l)
                    if l.matches(key, &fingerprint, size, opts.part_size)
                        && matches!(
                            (&l.session, native),
                            (Session::Native { .. }, true) | (Session::Staged { .. }, false)
                        ) =>
                {
                    self.drop_expired_parts(l).await?
                }
                stale => {
                    if let Some(l) = stale {
                        self.discard(&l).await;
                    }
                    let session = match &self.backend.multipart {
                        Some(mp) => {
                            let upload_id = retry(&opts, || mp.create_multipart(&path))
                                .await
                                .map_err(|e| StorageError::backend(key, e))?;
                            Session::Native { upload_id }
                        }
                        None => Session::Staged {
                            upload_id: uuid::Uuid::now_v7().simple().to_string(),
                        },
                    };
                    let l = fresh(session);
                    save_ledger(&ledger_path, &l).await?;
                    l
                }
            }
        };
        let resumed = ledger.done.len();
        Ok(Upload {
            store: self.clone(),
            local,
            key: key.to_owned(),
            path,
            size,
            fingerprint,
            opts,
            ledger_path,
            ledger: Mutex::new(ledger),
            resumed,
        })
    }

    /// Forgets staged parts that no longer exist (for example after the 24-hour cleanup).
    async fn drop_expired_parts(&self, mut ledger: Ledger) -> Result<Ledger> {
        if let Session::Staged { upload_id } = &ledger.session {
            let stored: std::collections::HashSet<String> = self
                .list(&self.keys().upload_parts(upload_id)?)
                .await?
                .into_iter()
                .map(|o| o.key)
                .collect();
            let keys = self.keys().clone();
            ledger.done.retain(|idx, _| {
                keys.upload_part(upload_id, *idx)
                    .is_ok_and(|k| stored.contains(&k))
            });
        }
        Ok(ledger)
    }

    /// Best effort: frees the storage held by an abandoned upload.
    async fn discard(&self, ledger: &Ledger) {
        let Ok(path) = self.path(&ledger.key) else {
            return;
        };
        match &ledger.session {
            Session::Native { upload_id } => {
                if let Some(mp) = &self.backend.multipart {
                    let _ = mp.abort_multipart(&path, upload_id).await;
                }
            }
            Session::Staged { upload_id } => {
                if let Ok(prefix) = self.keys().upload_parts(upload_id) {
                    if let Ok(parts) = self.list(&prefix).await {
                        for p in parts {
                            let _ = self.delete(&p.key).await;
                        }
                    }
                }
            }
            Session::Single | Session::Download => {}
        }
    }
}

impl Upload {
    /// Number of parts.
    pub fn part_count(&self) -> usize {
        self.opts.parts(self.size).max(1)
    }

    /// Parts not yet uploaded.
    pub async fn pending_parts(&self) -> Vec<usize> {
        let ledger = self.ledger.lock().await;
        (0..self.part_count())
            .filter(|i| !ledger.done.contains_key(i))
            .collect()
    }

    /// Uploads one part (with retries) and records it in the ledger.
    pub async fn put_part(&self, idx: usize) -> Result<()> {
        if idx >= self.part_count() {
            return Err(StorageError::Config(format!(
                "part {idx} is past the last part"
            )));
        }
        let session = {
            let ledger = self.ledger.lock().await;
            if ledger.done.contains_key(&idx) {
                return Ok(());
            }
            ledger.session.clone()
        };
        let range = self.opts.range(self.size, idx);
        let local = self.local.clone();
        let payload = PutPayload::from(blocking(move || read_at(&local, range)).await?);
        let objects = &self.store.backend.objects;
        let part_id = match &session {
            Session::Single | Session::Download => {
                retry(&self.opts, || objects.put(&self.path, payload.clone()))
                    .await
                    .map_err(|e| StorageError::backend(&self.key, e))?;
                String::new()
            }
            Session::Native { upload_id } => {
                let mp = self.store.backend.multipart.as_ref().ok_or_else(|| {
                    StorageError::Config(
                        "this backend lost multipart support during the upload".into(),
                    )
                })?;
                retry(&self.opts, || {
                    mp.put_part(&self.path, upload_id, idx, payload.clone())
                })
                .await
                .map_err(|e| StorageError::backend(&self.key, e))?
                .content_id
            }
            Session::Staged { upload_id } => {
                let part_key = self.store.keys().upload_part(upload_id, idx)?;
                let part_path = self.store.path(&part_key)?;
                retry(&self.opts, || objects.put(&part_path, payload.clone()))
                    .await
                    .map_err(|e| StorageError::backend(&part_key, e))?;
                String::new()
            }
        };
        let mut ledger = self.ledger.lock().await;
        ledger.done.insert(idx, part_id);
        save_ledger(&self.ledger_path, &ledger).await
    }

    /// Uploads every remaining part in parallel, then completes the object.
    pub async fn finish(self) -> Result<TransferOutcome> {
        let ledger_path = self.ledger_path.clone();
        let result = self.finish_inner().await;
        if let Err(StorageError::NotFound { .. }) = &result {
            // The service no longer has this upload (expired or aborted): start fresh next time.
            remove_file(&ledger_path).await?;
        }
        result
    }

    async fn finish_inner(self) -> Result<TransferOutcome> {
        let pending = self.pending_parts().await;
        futures::stream::iter(pending)
            .map(|i| self.put_part(i))
            .buffer_unordered(self.opts.concurrency)
            .try_collect::<Vec<()>>()
            .await?;

        if local_fingerprint(&self.local).await?.1 != self.fingerprint {
            let ledger = self.ledger.lock().await.clone();
            self.store.discard(&ledger).await;
            remove_file(&self.ledger_path).await?;
            return Err(StorageError::SourceChanged(self.local.clone()));
        }

        let ledger = self.ledger.lock().await.clone();
        let objects = &self.store.backend.objects;
        let key = self.key.as_str();
        match &ledger.session {
            Session::Single | Session::Download => {}
            Session::Native { upload_id } => {
                let mp = self.store.backend.multipart.as_ref().ok_or_else(|| {
                    StorageError::Config(
                        "this backend lost multipart support during the upload".into(),
                    )
                })?;
                let parts: Vec<PartId> = ledger
                    .done
                    .values()
                    .map(|id| PartId {
                        content_id: id.clone(),
                    })
                    .collect();
                retry(&self.opts, || {
                    mp.complete_multipart(&self.path, upload_id, parts.clone())
                })
                .await
                .map_err(|e| StorageError::backend(key, e))?;
            }
            Session::Staged { upload_id } => {
                let mut writer = objects
                    .put_multipart(&self.path)
                    .await
                    .map_err(|e| StorageError::backend(key, e))?;
                for idx in 0..self.part_count() {
                    let part_key = self.store.keys().upload_part(upload_id, idx)?;
                    let joined = match self.store.get(&part_key).await {
                        Ok(data) => writer
                            .put_part(PutPayload::from(data))
                            .await
                            .map_err(|e| StorageError::backend(key, e)),
                        Err(e) => Err(e),
                    };
                    if let Err(e) = joined {
                        let _ = writer.abort().await;
                        return Err(e);
                    }
                }
                writer
                    .complete()
                    .await
                    .map_err(|e| StorageError::backend(key, e))?;
                self.store.discard(&ledger).await;
            }
        }
        remove_file(&self.ledger_path).await?;
        let (size, sha256) = sha256_file(&self.local).await?;
        Ok(TransferOutcome {
            size,
            sha256,
            parts: self.part_count(),
            resumed_parts: self.resumed,
        })
    }
}

/// A download in progress. Use [`Store::download_file`] unless you need part-by-part control.
#[derive(Debug)]
pub struct Download {
    store: Store,
    key: String,
    path: Path,
    etag: Option<String>,
    local: PathBuf,
    tmp: PathBuf,
    size: u64,
    opts: TransferOptions,
    ledger_path: PathBuf,
    ledger: Mutex<Ledger>,
    resumed: usize,
}

impl Store {
    /// Downloads `key` to a local file in parallel ranged parts, resumable after a crash.
    /// With `expected_sha256`, a mismatching file is deleted and an error returned.
    pub async fn download_file(
        &self,
        key: &str,
        local: impl Into<PathBuf>,
        expected_sha256: Option<&str>,
        opts: TransferOptions,
    ) -> Result<TransferOutcome> {
        self.begin_download(key, local, opts)
            .await?
            .finish(expected_sha256)
            .await
    }

    /// Starts a download, or resumes one whose ledger matches the object version.
    pub async fn begin_download(
        &self,
        key: &str,
        local: impl Into<PathBuf>,
        opts: TransferOptions,
    ) -> Result<Download> {
        opts.validate()?;
        let local = local.into();
        let path = self.path(key)?;
        let meta = self.head(key).await?;
        let fingerprint = match &meta.etag {
            Some(etag) => format!("etag:{etag}"),
            None => format!(
                "{}:{}",
                meta.size,
                meta.last_modified.unix_timestamp_nanos()
            ),
        };
        let tmp = ledger_path(&local, ".mrl-part")?;
        let ledger_path = ledger_path(&local, ".mrl-download.json")?;
        let tmp_len = tokio::fs::metadata(&tmp).await.map(|m| m.len()).ok();
        let ledger = match load_ledger(&ledger_path).await {
            Some(l)
                if l.session == Session::Download
                    && l.matches(key, &fingerprint, meta.size, opts.part_size)
                    && tmp_len == Some(meta.size) =>
            {
                l
            }
            _ => {
                let (t, size) = (tmp.clone(), meta.size);
                blocking(move || {
                    let f = std::fs::File::create(&t).map_err(|e| StorageError::io(&t, e))?;
                    f.set_len(size).map_err(|e| StorageError::io(&t, e))
                })
                .await?;
                let l = Ledger {
                    version: LEDGER_VERSION,
                    key: key.to_owned(),
                    fingerprint,
                    size: meta.size,
                    part_size: opts.part_size,
                    session: Session::Download,
                    done: BTreeMap::new(),
                };
                save_ledger(&ledger_path, &l).await?;
                l
            }
        };
        let resumed = ledger.done.len();
        Ok(Download {
            store: self.clone(),
            key: key.to_owned(),
            path,
            etag: meta.etag,
            local,
            tmp,
            size: meta.size,
            opts,
            ledger_path,
            ledger: Mutex::new(ledger),
            resumed,
        })
    }
}

impl Download {
    /// Number of parts (0 for an empty object).
    pub fn part_count(&self) -> usize {
        self.opts.parts(self.size)
    }

    /// Fetches one part (with retries), writes it in place and records it in the ledger.
    pub async fn fetch_part(&self, idx: usize) -> Result<()> {
        if idx >= self.part_count() {
            return Err(StorageError::Config(format!(
                "part {idx} is past the last part"
            )));
        }
        if self.ledger.lock().await.done.contains_key(&idx) {
            return Ok(());
        }
        let range = self.opts.range(self.size, idx);
        let objects = &self.store.backend.objects;
        let data = retry(&self.opts, || async {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(range.clone())),
                // Fail instead of mixing parts of two versions if the object is replaced.
                if_match: self.etag.clone(),
                ..GetOptions::default()
            };
            objects.get_opts(&self.path, opts).await?.bytes().await
        })
        .await
        .map_err(|e| StorageError::backend(&self.key, e))?;
        if data.len() as u64 != range.end - range.start {
            return Err(StorageError::Backend(object_store::Error::Generic {
                store: "mrl-storage",
                source: format!(
                    "part {idx} of {} came back with {} bytes",
                    self.key,
                    data.len()
                )
                .into(),
            }));
        }
        let tmp = self.tmp.clone();
        blocking(move || write_at(&tmp, range.start, &data)).await?;
        let mut ledger = self.ledger.lock().await;
        ledger.done.insert(idx, String::new());
        save_ledger(&self.ledger_path, &ledger).await
    }

    /// Fetches every remaining part in parallel, then moves the file into place.
    pub async fn finish(self, expected_sha256: Option<&str>) -> Result<TransferOutcome> {
        let pending: Vec<usize> = {
            let ledger = self.ledger.lock().await;
            (0..self.part_count())
                .filter(|i| !ledger.done.contains_key(i))
                .collect()
        };
        futures::stream::iter(pending)
            .map(|i| self.fetch_part(i))
            .buffer_unordered(self.opts.concurrency)
            .try_collect::<Vec<()>>()
            .await?;
        let (tmp, local) = (self.tmp.clone(), self.local.clone());
        blocking(move || {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&tmp)
                .and_then(|f| f.sync_all())
                .map_err(|e| StorageError::io(&tmp, e))?;
            std::fs::rename(&tmp, &local).map_err(|e| StorageError::io(&local, e))
        })
        .await?;
        remove_file(&self.ledger_path).await?;
        let (size, sha256) = sha256_file(&self.local).await?;
        if let Some(expected) = expected_sha256 {
            if expected != sha256 {
                remove_file(&self.local).await?;
                return Err(StorageError::ChecksumMismatch {
                    key: self.key,
                    expected: expected.to_owned(),
                    actual: sha256,
                });
            }
        }
        Ok(TransferOutcome {
            size,
            sha256,
            parts: self.part_count(),
            resumed_parts: self.resumed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn retries_transient_errors_then_gives_up() {
        let opts = TransferOptions {
            retries: 3,
            backoff: Duration::from_millis(1),
            ..TransferOptions::default()
        };
        let calls = AtomicU32::new(0);
        let flaky = || async {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(object_store::Error::Generic {
                    store: "test",
                    source: "blip".into(),
                })
            } else {
                Ok(7)
            }
        };
        assert_eq!(retry(&opts, flaky).await.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        calls.store(0, Ordering::SeqCst);
        let always = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(object_store::Error::Generic {
                store: "test",
                source: "down".into(),
            })
        };
        assert!(retry(&opts, always).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 4);

        calls.store(0, Ordering::SeqCst);
        let missing = || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(object_store::Error::NotFound {
                path: "x".into(),
                source: "gone".into(),
            })
        };
        assert!(retry(&opts, missing).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "not-found is not retried");
    }

    #[test]
    fn part_math() {
        let o = TransferOptions {
            part_size: MIN_PART_SIZE,
            ..TransferOptions::default()
        };
        assert_eq!(o.parts(0), 0);
        assert_eq!(o.parts(MIN_PART_SIZE), 1);
        assert_eq!(o.parts(MIN_PART_SIZE + 1), 2);
        assert_eq!(
            o.range(MIN_PART_SIZE + 1, 1),
            MIN_PART_SIZE..MIN_PART_SIZE + 1
        );
        assert!(TransferOptions {
            part_size: 1,
            ..o.clone()
        }
        .validate()
        .is_err());
        assert!(TransferOptions {
            concurrency: 0,
            ..o
        }
        .validate()
        .is_err());
    }
}
