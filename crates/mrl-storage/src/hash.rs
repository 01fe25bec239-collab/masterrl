//! Streaming sha256. All hashing of unbounded input runs on the blocking pool.

use std::io::Read;
use std::path::{Path, PathBuf};

use futures::{Stream, TryStreamExt};
use sha2::{Digest, Sha256};

use crate::error::{Result, StorageError};

const READ_BUF: usize = 1 << 20;

/// Lowercase hex sha256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Size and lowercase hex sha256 of a local file, read on the blocking pool.
pub async fn sha256_file(path: impl Into<PathBuf>) -> Result<(u64, String)> {
    let path = path.into();
    blocking(move || hash_reader(&path)).await
}

/// Most chunks gathered before a blocking-pool hash, so tiny chunks still yield regularly.
const MAX_BATCH_CHUNKS: usize = 1024;

/// Size and sha256 of a byte stream, such as an object download.
///
/// Chunks are gathered into batches of about 1 MiB and hashed on the blocking pool, so a big
/// chunk or an always-ready stream never holds an async worker.
pub async fn sha256_stream<S>(stream: S) -> Result<(u64, String)>
where
    S: Stream<Item = Result<bytes::Bytes>>,
{
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;
    let mut stream = std::pin::pin!(stream);
    loop {
        let next = stream.try_next().await?;
        let done = next.is_none();
        if let Some(chunk) = next {
            size += chunk.len() as u64;
            batch_bytes += chunk.len();
            batch.push(chunk);
        }
        if batch_bytes >= READ_BUF || batch.len() >= MAX_BATCH_CHUNKS || (done && !batch.is_empty())
        {
            let chunks = std::mem::take(&mut batch);
            batch_bytes = 0;
            hasher = blocking(move || {
                for c in &chunks {
                    hasher.update(c);
                }
                Ok(hasher)
            })
            .await?;
        }
        if done {
            return Ok((size, hex(&hasher.finalize())));
        }
    }
}

fn hash_reader(path: &Path) -> Result<(u64, String)> {
    let mut file = std::fs::File::open(path).map_err(|e| StorageError::io(path, e))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; READ_BUF];
    let mut size = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(|e| StorageError::io(path, e))?;
        if n == 0 {
            break;
        }
        size += n as u64;
        hasher.update(&buf[..n]);
    }
    Ok((size, hex(&hasher.finalize())))
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 0xf)] as char);
    }
    out
}

/// Runs blocking file work off the async threads.
pub(crate) async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StorageError::io(PathBuf::new(), std::io::Error::other(e)))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn known_digests() {
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(sha256_hex(b""), empty);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"abc").unwrap();
        let abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert_eq!(sha256_file(&path).await.unwrap(), (3, abc.to_owned()));
        let chunks = futures::stream::iter([
            Ok(bytes::Bytes::from_static(b"a")),
            Ok(bytes::Bytes::from_static(b"bc")),
        ]);
        assert_eq!(sha256_stream(chunks).await.unwrap(), (3, abc.to_owned()));
    }

    /// On a single-threaded runtime, another task must keep running while a large,
    /// always-ready stream is hashed.
    #[tokio::test(flavor = "current_thread")]
    async fn stream_hashing_does_not_hold_the_runtime() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let ticks = Arc::new(AtomicU64::new(0));
        let t = ticks.clone();
        let ticker = tokio::spawn(async move {
            loop {
                t.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });
        let chunk = bytes::Bytes::from(vec![7u8; 4 << 20]);
        let data: Vec<u8> = chunk.repeat(16);
        let stream = futures::stream::iter((0..16).map(|_| Ok(chunk.clone())));
        let (size, digest) = sha256_stream(stream).await.unwrap();
        ticker.abort();
        assert_eq!((size, digest), (data.len() as u64, sha256_hex(&data)));
        assert!(
            ticks.load(Ordering::Relaxed) >= 16,
            "the runtime was blocked"
        );
    }
}
