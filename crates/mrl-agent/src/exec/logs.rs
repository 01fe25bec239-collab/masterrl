//! Size-rotated log files: `name.log`, then `name.log.1` (newest old file) up to `name.log.N`.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

/// A log file that rotates when it reaches `max_bytes`, keeping `keep` old files.
#[derive(Debug)]
pub struct RotatingLog {
    path: PathBuf,
    max_bytes: u64,
    keep: u32,
    file: tokio::fs::File,
    written: u64,
}

impl RotatingLog {
    /// Opens (appends to) `path`.
    pub async fn open(path: PathBuf, max_bytes: u64, keep: u32) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        let written = file.metadata().await?.len();
        Ok(Self {
            path,
            max_bytes: max_bytes.max(1),
            keep,
            file,
            written,
        })
    }

    fn numbered(path: &Path, n: u32) -> PathBuf {
        let mut s = path.as_os_str().to_owned();
        s.push(format!(".{n}"));
        PathBuf::from(s)
    }

    async fn rotate(&mut self) -> std::io::Result<()> {
        self.file.flush().await?;
        if self.keep == 0 {
            // Reopen truncated: Windows refuses set_len on an append-only handle.
            self.file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&self.path)
                .await?;
        } else {
            let _ = tokio::fs::remove_file(Self::numbered(&self.path, self.keep)).await;
            for n in (1..self.keep).rev() {
                let from = Self::numbered(&self.path, n);
                if tokio::fs::try_exists(&from).await? {
                    tokio::fs::rename(&from, Self::numbered(&self.path, n + 1)).await?;
                }
            }
            tokio::fs::rename(&self.path, Self::numbered(&self.path, 1)).await?;
            self.file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .await?;
        }
        self.written = 0;
        Ok(())
    }

    /// Writes `buf`, rotating first if it would pass the size limit. A single write larger
    /// than the limit is split across files.
    pub async fn write(&mut self, mut buf: &[u8]) -> std::io::Result<()> {
        while !buf.is_empty() {
            if self.written >= self.max_bytes {
                self.rotate().await?;
            }
            let room = (self.max_bytes - self.written) as usize;
            let (now, rest) = buf.split_at(room.min(buf.len()));
            self.file.write_all(now).await?;
            self.written += now.len() as u64;
            buf = rest;
        }
        Ok(())
    }

    /// Copies `reader` into the log until it ends.
    pub async fn pump(mut self, mut reader: impl AsyncRead + Unpin) -> std::io::Result<()> {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            self.write(&buf[..n]).await?;
        }
        self.file.flush().await?;
        self.file.sync_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rotates_and_keeps_the_newest_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.log");
        let log = RotatingLog::open(path.clone(), 10, 2).await.unwrap();
        log.pump(&b"aaaaaaaaaabbbbbbbbbbccccccccccdd"[..])
            .await
            .unwrap();
        let read = |p: &Path| std::fs::read_to_string(p).unwrap();
        assert_eq!(read(&path), "dd");
        assert_eq!(read(&RotatingLog::numbered(&path, 1)), "cccccccccc");
        assert_eq!(read(&RotatingLog::numbered(&path, 2)), "bbbbbbbbbb");
        assert!(!RotatingLog::numbered(&path, 3).exists());

        // Reopening counts the 2 bytes already there; keep = 0 truncates in place.
        let mut log = RotatingLog::open(path.clone(), 10, 0).await.unwrap();
        log.write(b"eeeeeeeeeeff").await.unwrap();
        log.file.flush().await.unwrap();
        assert_eq!(read(&path), "eeff", "8 e's filled the first file");
    }
}
