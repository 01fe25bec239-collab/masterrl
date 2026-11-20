//! The on-disk CONTROL spool (D-04-05: CONTROL is never dropped).
//!
//! Every durable CONTROL message (command results, attempt status, guard events, …) gets the
//! next CONTROL `seq` and is written to `<dir>/<seq>.pb` (atomic, fsynced) before anyone sends
//! it. A file is removed only when the core acks that seq (`ack_seq`), so after a reconnect —
//! or an agent restart — everything not acked is sent again in seq order. Volatile messages
//! (heartbeat, inventory, certificate renewal) take a seq from the same counter without a file.
//!
//! The counter is saved on every use, so a restarted agent never reuses a seq the core already
//! accepted. Limits: `max_entries` files and `max_bytes` bytes; when full, [`Spool::push`] fails
//! with [`SpoolFull`] and the caller must hold back (nothing is dropped silently).

use crate::state::atomic_write;
use crate::{AgentError, Result};
use mrl_proto::agent::v1::{agent_message::Body, AgentMessage};
use prost::Message;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The spool is at its limit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the control spool is full ({entries} messages, {bytes} bytes waiting for the server)")]
pub struct SpoolFull {
    /// Messages waiting.
    pub entries: usize,
    /// Bytes waiting.
    pub bytes: u64,
}

/// The CONTROL spool.
pub struct Spool {
    dir: PathBuf,
    next: u64,
    entries: BTreeMap<u64, u64>,
    bytes: u64,
    max_entries: usize,
    max_bytes: u64,
}

const COUNTER: &str = "next_seq";

impl Spool {
    /// Opens the spool in `dir` with these limits.
    pub fn open(dir: &Path, max_entries: usize, max_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| AgentError::state(dir, e))?;
        let mut entries = BTreeMap::new();
        let mut bytes = 0;
        for f in std::fs::read_dir(dir).map_err(|e| AgentError::state(dir, e))? {
            let f = f.map_err(|e| AgentError::state(dir, e))?;
            let name = f.file_name().to_string_lossy().into_owned();
            if let Some(seq) = name.strip_suffix(".pb").and_then(|s| s.parse::<u64>().ok()) {
                let len = f.metadata().map(|m| m.len()).unwrap_or(0);
                entries.insert(seq, len);
                bytes += len;
            }
        }
        let saved = std::fs::read_to_string(dir.join(COUNTER))
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(1);
        let next = saved
            .max(entries.keys().next_back().map_or(1, |s| s + 1))
            .max(1);
        Ok(Self {
            dir: dir.to_path_buf(),
            next,
            entries,
            bytes,
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
        })
    }

    fn file(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("{seq:020}.pb"))
    }

    fn take_seq(&mut self) -> Result<u64> {
        let seq = self.next;
        self.next += 1;
        let path = self.dir.join(COUNTER);
        atomic_write(&path, self.next.to_string().as_bytes())
            .map_err(|e| AgentError::state(path, e))?;
        Ok(seq)
    }

    /// Spools a durable message; returns its seq.
    pub fn push(&mut self, body: Body) -> Result<std::result::Result<u64, SpoolFull>> {
        let mut msg = AgentMessage {
            body: Some(body),
            ..AgentMessage::default()
        };
        let len = msg.encoded_len() as u64 + 10;
        if self.entries.len() >= self.max_entries || self.bytes + len > self.max_bytes {
            return Ok(Err(SpoolFull {
                entries: self.entries.len(),
                bytes: self.bytes,
            }));
        }
        let seq = self.take_seq()?;
        msg.seq = seq;
        let bytes = msg.encode_to_vec();
        let path = self.file(seq);
        atomic_write(&path, &bytes).map_err(|e| AgentError::state(path, e))?;
        self.entries.insert(seq, bytes.len() as u64);
        self.bytes += bytes.len() as u64;
        Ok(Ok(seq))
    }

    /// Spooled messages with `seq > after`, in order, plus a fresh seq for a volatile message
    /// sent right after them (`volatile = true`). Taking both under one lock keeps seq order.
    pub fn pending(
        &mut self,
        after: u64,
        volatile: bool,
    ) -> Result<(Vec<AgentMessage>, Option<u64>)> {
        let mut out = Vec::new();
        for (&seq, _) in self.entries.range(after.saturating_add(1)..) {
            let path = self.file(seq);
            let bytes = std::fs::read(&path).map_err(|e| AgentError::state(&path, e))?;
            let msg = AgentMessage::decode(bytes.as_slice()).map_err(|e| AgentError::Config {
                path: path.clone(),
                reason: format!("a spooled message is damaged ({e}); remove the file"),
            })?;
            out.push(msg);
        }
        let seq = if volatile {
            Some(self.take_seq()?)
        } else {
            None
        };
        Ok((out, seq))
    }

    /// Moves the message `seq` out of the spool into `<dir>/oversized/` (kept for diagnosis, never
    /// sent): it can't be sent at all, so it must not block the messages after it.
    pub fn set_aside(&mut self, seq: u64) -> Result<PathBuf> {
        let aside = self.dir.join("oversized");
        std::fs::create_dir_all(&aside).map_err(|e| AgentError::state(&aside, e))?;
        let to = aside.join(format!("{seq:020}.pb"));
        let from = self.file(seq);
        std::fs::rename(&from, &to).map_err(|e| AgentError::state(&from, e))?;
        if let Some(len) = self.entries.remove(&seq) {
            self.bytes -= len;
        }
        Ok(to)
    }

    /// The core durably accepted everything up to `upto`: delete those files.
    pub fn ack(&mut self, upto: u64) -> Result<()> {
        let done: Vec<u64> = self.entries.range(..=upto).map(|(s, _)| *s).collect();
        for seq in done {
            let path = self.file(seq);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(AgentError::state(path, e)),
            }
            if let Some(len) = self.entries.remove(&seq) {
                self.bytes -= len;
            }
        }
        Ok(())
    }

    /// The core expects seqs from `from` on (`CoreHello.resume_from_seq`): never go below it.
    pub fn resume_from(&mut self, from: u64) -> Result<()> {
        self.ack(from.saturating_sub(1))?;
        if from > self.next {
            self.next = from;
            let path = self.dir.join(COUNTER);
            atomic_write(&path, self.next.to_string().as_bytes())
                .map_err(|e| AgentError::state(path, e))?;
        }
        Ok(())
    }

    /// Messages waiting.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when nothing waits.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mrl_proto::agent::v1::CommandResult;

    fn result(id: &str) -> Body {
        Body::CommandResult(CommandResult {
            command_id: id.into(),
            accepted: true,
            failure: None,
        })
    }

    #[test]
    fn survives_restart_and_replays_until_acked() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), 100, 1 << 20).unwrap();
        assert_eq!(s.push(result("a")).unwrap(), Ok(1));
        assert_eq!(s.push(result("b")).unwrap(), Ok(2));
        let (_, hb) = s.pending(2, true).unwrap();
        assert_eq!(hb, Some(3));
        drop(s);
        let mut s = Spool::open(dir.path(), 100, 1 << 20).unwrap();
        let (msgs, _) = s.pending(0, false).unwrap();
        assert_eq!(msgs.iter().map(|m| m.seq).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(
            s.push(result("c")).unwrap(),
            Ok(4),
            "never reuses the volatile seq 3"
        );
        s.ack(2).unwrap();
        assert_eq!(s.len(), 1);
        s.resume_from(10).unwrap();
        assert!(s.is_empty());
        assert_eq!(s.push(result("d")).unwrap(), Ok(10));
    }

    #[test]
    fn full_spool_refuses_instead_of_dropping() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), 2, 1 << 20).unwrap();
        s.push(result("a")).unwrap().unwrap();
        s.push(result("b")).unwrap().unwrap();
        let full = s.push(result("c")).unwrap().unwrap_err();
        assert_eq!(full.entries, 2);
        s.ack(1).unwrap();
        assert!(s.push(result("c")).unwrap().is_ok());
        let mut tiny = Spool::open(&dir.path().join("t"), 100, 8).unwrap();
        assert!(tiny.push(result("x")).unwrap().is_err());
    }
}
