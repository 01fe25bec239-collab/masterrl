//! The command journal: every command seq the agent accepted, written to disk *before* the
//! agent acks it (RECEIVED), then its outcome, then whether that outcome reached the CONTROL
//! spool.
//!
//! - A command whose seq is not above [`Journal::last_seq`] is a repeat and never runs again.
//! - Every change is written first and becomes visible only after the write succeeded, so a
//!   failed write never makes an unrun command look like a repeat.
//! - The outcome is kept until it is spooled ([`Journal::unreported`]); a full spool delays the
//!   result, it never loses it, and the handler is not run again.
//! - Commands still `Received` at start were interrupted by a restart ([`Journal::interrupted`]);
//!   the session reports them FAILED instead of running them twice.

use crate::state::atomic_write;
use crate::{AgentError, Result};
use mrl_proto::common::v1::Failure;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Reported entries kept (older ones are dropped).
pub const KEEP: usize = 1000;

/// Where one command stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    /// Journaled and acked; running.
    Received,
    /// Finished; its result is not in the spool yet.
    Done,
    /// Its `CommandResult` is in the spool.
    Reported,
}

/// One journaled command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// `Command.command_id`.
    pub command_id: String,
    /// Where it stands.
    pub step: Step,
    /// Outcome once `Done`: `None` = accepted, else the hex protobuf `Failure`.
    #[serde(default)]
    pub failure: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// A finished command whose result still has to be spooled.
#[derive(Debug, Clone, PartialEq)]
pub struct Unreported {
    /// Command seq.
    pub seq: u64,
    /// `Command.command_id`.
    pub command_id: String,
    /// The outcome.
    pub outcome: std::result::Result<(), Failure>,
}

/// The journal file.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    state: State,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .filter_map(|i| u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok())
        .collect()
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

    /// Writes `next`, then makes it the current state (never the other way round).
    fn commit(&mut self, next: State) -> Result<()> {
        let bytes = serde_json::to_vec(&next).map_err(|e| AgentError::exec("journal", e))?;
        atomic_write(&self.path, &bytes).map_err(|e| AgentError::state(&self.path, e))?;
        self.state = next;
        Ok(())
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
        let mut next = self.state.clone();
        next.last_seq = seq;
        next.entries.insert(
            seq,
            Entry {
                command_id: command_id.to_owned(),
                step: Step::Received,
                failure: None,
            },
        );
        self.commit(next)?;
        Ok(Delivery::New)
    }

    /// Records the outcome of `seq` (`Done`), only when `seq` is still the command `command_id`
    /// and still running: an outcome never lands on another command that reused the seq (after
    /// enrolling again) or on one already finished.
    pub fn complete(
        &mut self,
        seq: u64,
        command_id: &str,
        outcome: &std::result::Result<(), Failure>,
    ) -> Result<()> {
        let mut next = self.state.clone();
        match next.entries.get_mut(&seq) {
            Some(e) if e.command_id == command_id && e.step == Step::Received => {
                e.step = Step::Done;
                e.failure = outcome.as_ref().err().map(|f| hex(&f.encode_to_vec()));
            }
            _ => return Ok(()),
        }
        self.commit(next)
    }

    /// The result of `seq` is in the spool (`Reported`); old reported entries are dropped.
    pub fn reported(&mut self, seq: u64) -> Result<()> {
        let mut next = self.state.clone();
        if let Some(e) = next.entries.get_mut(&seq) {
            e.step = Step::Reported;
            e.failure = None;
        }
        let reported: Vec<u64> = next
            .entries
            .iter()
            .filter(|(_, e)| e.step == Step::Reported)
            .map(|(s, _)| *s)
            .collect();
        if reported.len() > KEEP {
            for s in &reported[..reported.len() - KEEP] {
                next.entries.remove(s);
            }
        }
        self.commit(next)
    }

    /// Finished commands whose results are not spooled yet, oldest first.
    pub fn unreported(&self) -> Vec<Unreported> {
        self.state
            .entries
            .iter()
            .filter(|(_, e)| e.step == Step::Done)
            .map(|(s, e)| Unreported {
                seq: *s,
                command_id: e.command_id.clone(),
                outcome: match &e.failure {
                    None => Ok(()),
                    Some(h) => Err(Failure::decode(unhex(h).as_slice()).unwrap_or_default()),
                },
            })
            .collect()
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
        j.complete(1, "a", &Ok(())).unwrap();
        j.reported(1).unwrap();
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
    fn a_failed_write_never_turns_a_new_command_into_a_repeat() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("gone");
        std::fs::create_dir(&sub).unwrap();
        let mut j = Journal::open(&sub.join("journal.json")).unwrap();
        std::fs::remove_dir(&sub).unwrap();
        assert!(j.receive(1, "a").is_err(), "the write fails");
        assert_eq!(j.last_seq(), 0, "nothing was published");
        std::fs::create_dir(&sub).unwrap();
        assert_eq!(
            j.receive(1, "a").unwrap(),
            Delivery::New,
            "the redelivery runs"
        );
    }

    #[test]
    fn outcomes_wait_until_spooled_and_survive_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("j.json");
        let mut j = Journal::open(&path).unwrap();
        j.receive(1, "a").unwrap();
        j.receive(2, "b").unwrap();
        let f = Failure {
            sub_code: "unsupported_command".into(),
            ..Failure::default()
        };
        j.complete(1, "a", &Ok(())).unwrap();
        j.complete(2, "b", &Err(f.clone())).unwrap();
        let j2 = Journal::open(&path).unwrap();
        let u = j2.unreported();
        assert_eq!(u.len(), 2);
        assert_eq!(u[0].outcome, Ok(()));
        assert_eq!(u[1].outcome, Err(f));
        assert!(j2.interrupted().is_empty());
        j.reported(1).unwrap();
        assert_eq!(j.unreported().len(), 1);
    }

    #[test]
    fn an_outcome_only_lands_on_its_own_command() {
        let dir = tempfile::tempdir().unwrap();
        let mut j = Journal::open(&dir.path().join("j.json")).unwrap();
        j.receive(1, "new").unwrap();
        j.complete(1, "old", &Ok(())).unwrap();
        assert!(
            j.unreported().is_empty(),
            "another command's outcome is ignored"
        );
        assert_eq!(j.interrupted(), vec![(1, "new".to_owned())]);
        j.complete(1, "new", &Ok(())).unwrap();
        j.complete(1, "new", &Err(Failure::default())).unwrap();
        assert_eq!(j.unreported()[0].outcome, Ok(()), "finished once");
    }

    #[test]
    fn keeps_a_bounded_history() {
        let dir = tempfile::tempdir().unwrap();
        let mut j = Journal::open(&dir.path().join("j.json")).unwrap();
        for s in 1..=(KEEP as u64 + 5) {
            j.receive(s, &format!("c{s}")).unwrap();
            j.complete(s, &format!("c{s}"), &Ok(())).unwrap();
            j.reported(s).unwrap();
        }
        assert_eq!(j.state.entries.len(), KEEP);
        assert_eq!(j.receive(2, "c2").unwrap(), Delivery::Repeat);
    }
}
