//! The command journal: every command seq the agent accepted, written to disk *before* the
//! agent acks it (RECEIVED), and its outcome once finished.
//!
//! A command whose seq is not above [`Journal::last_seq`] is a repeat and is never run again
//! (the server resends after reconnects; results already sit in the CONTROL spool). Commands
//! still `Received` when the agent starts were interrupted by a restart; [`Journal::interrupted`]
//! lists them so the session reports them FAILED instead of running them twice.

use crate::state::atomic_write;
use crate::{AgentError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Entries kept after they finish (older finished ones are dropped).
pub const KEEP: usize = 1000;

/// Where one command stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    /// Journaled and acked; running.
    Received,
    /// Finished (its `CommandResult` is spooled).
    Finished,
}

/// One journaled command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// `Command.command_id`.
    pub command_id: String,
    /// Where it stands.
    pub step: Step,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    last_seq: u64,
    entries: BTreeMap<u64, Entry>,
}

/// Whether a delivered command is new.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// New: run it.
    New,
    /// Seen before: do nothing.
    Repeat,
}

/// The journal file.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    state: State,
}

impl Journal {
    /// Opens (or starts) the journal at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        let state = match std::fs::read(path) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| AgentError::Config {
                path: path.to_path_buf(),
                reason: format!(
                    "the command journal is damaged ({e}); move it away to start a new one"
                ),
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(e) => return Err(AgentError::state(path, e)),
        };
        Ok(Self {
            path: path.to_path_buf(),
            state,
        })
    }

    /// Highest command seq accepted.
    pub fn last_seq(&self) -> u64 {
        self.state.last_seq
    }

    /// Records a delivered command before it is acked. Repeats change nothing.
    pub fn receive(&mut self, seq: u64, command_id: &str) -> Result<Delivery> {
        if seq <= self.state.last_seq
            || self
                .state
                .entries
                .values()
                .any(|e| e.command_id == command_id)
        {
            return Ok(Delivery::Repeat);
        }
        self.state.last_seq = seq;
        self.state.entries.insert(
            seq,
            Entry {
                command_id: command_id.to_owned(),
                step: Step::Received,
            },
        );
        self.save()?;
        Ok(Delivery::New)
    }

    /// Marks `seq` finished.
    pub fn finish(&mut self, seq: u64) -> Result<()> {
        if let Some(e) = self.state.entries.get_mut(&seq) {
            e.step = Step::Finished;
        }
        let finished: Vec<u64> = self
            .state
            .entries
            .iter()
            .filter(|(_, e)| e.step == Step::Finished)
            .map(|(s, _)| *s)
            .collect();
        if finished.len() > KEEP {
            for s in &finished[..finished.len() - KEEP] {
                self.state.entries.remove(s);
            }
        }
        self.save()
    }

    /// Commands left `Received` (the agent stopped while running them).
    pub fn interrupted(&self) -> Vec<(u64, String)> {
        self.state
            .entries
            .iter()
            .filter(|(_, e)| e.step == Step::Received)
            .map(|(s, e)| (*s, e.command_id.clone()))
            .collect()
    }

    fn save(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.state).map_err(|e| AgentError::exec("journal", e))?;
        atomic_write(&self.path, &bytes).map_err(|e| AgentError::state(&self.path, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeats_are_never_new_even_after_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.json");
        let mut j = Journal::open(&path).unwrap();
        assert_eq!(j.receive(1, "a").unwrap(), Delivery::New);
        assert_eq!(j.receive(1, "a").unwrap(), Delivery::Repeat);
        assert_eq!(j.receive(3, "c").unwrap(), Delivery::New);
        assert_eq!(
            j.receive(2, "b").unwrap(),
            Delivery::Repeat,
            "seq at or below last is a repeat"
        );
        j.finish(1).unwrap();
        let j = Journal::open(&path).unwrap();
        assert_eq!(j.last_seq(), 3);
        assert_eq!(j.interrupted(), vec![(3, "c".to_owned())]);
        std::fs::write(&path, b"{oops").unwrap();
        assert!(Journal::open(&path)
            .unwrap_err()
            .to_string()
            .contains("damaged"));
    }

    #[test]
    fn keeps_a_bounded_history() {
        let dir = tempfile::tempdir().unwrap();
        let mut j = Journal::open(&dir.path().join("j.json")).unwrap();
        for s in 1..=(KEEP as u64 + 5) {
            j.receive(s, &format!("c{s}")).unwrap();
            j.finish(s).unwrap();
        }
        assert_eq!(j.state.entries.len(), KEEP);
        assert_eq!(j.receive(2, "c2").unwrap(), Delivery::Repeat);
    }
}
