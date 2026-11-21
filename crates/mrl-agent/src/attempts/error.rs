//! Errors of the attempt runtime, with stable codes (`guidance/fragments/errors/p20.yaml`).

use std::path::PathBuf;

use mrl_proto::common::v1::{ClassifiedBy, Failure, FailureClass};

use crate::AgentError;

/// Something the attempt runtime could not do.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AttemptError {
    /// The attempt settings are missing something or have a value the agent can't use.
    #[error("the attempt settings can't be used: {0}")]
    Spec(String),
    /// A path in the attempt points outside the attempt's own folders.
    #[error("the path {path:?} points outside the attempt folder")]
    UnsafePath {
        /// The path as given.
        path: String,
    },
    /// The environment lock file does not have the hash the attempt names.
    #[error(
        "the environment lock for {runtime_ref} does not match: expected sha256 {expected}, found {actual}"
    )]
    LockMismatch {
        /// The runtime reference.
        runtime_ref: String,
        /// Hash from the runtime reference.
        expected: String,
        /// Hash of the lock file on disk.
        actual: String,
    },
    /// Installing or checking the training environment failed.
    #[error("the training environment {runtime_ref} could not be installed: {reason}")]
    EnvInstall {
        /// The runtime reference.
        runtime_ref: String,
        /// What went wrong.
        reason: String,
    },
    /// `uv` is not installed.
    #[error("uv is not installed or can't run here: {0}")]
    UvMissing(String),
    /// Docker is not installed or not running.
    #[error("Docker is not installed or not running: {0}")]
    DockerMissing(String),
    /// The worker connection (Unix socket) could not be opened.
    #[error("the agent could not open the worker connection at {path}: {reason}")]
    Socket {
        /// The socket path.
        path: PathBuf,
        /// What went wrong.
        reason: String,
    },
    /// The attempt is unknown here, already exists, or can't take this command now.
    #[error("attempt {attempt_id}: {reason}")]
    State {
        /// The attempt.
        attempt_id: String,
        /// Why, in plain words.
        reason: String,
    },
    /// The machine is draining and takes no new attempts.
    #[error("this machine is not taking new attempts: {0}")]
    Draining(String),
    /// A lower-level agent error (state folder, executor, sandbox, cache, transfers).
    #[error(transparent)]
    Agent(#[from] AgentError),
}

/// `text` with every link cut to its host (`https://host/…`) and every proxied transfer token
/// (`/transfer/<token>`) hidden: transfer errors quote the ticket URL, which stays valid for
/// minutes and must not reach logs, statuses or results.
pub fn redact_links(text: &str) -> String {
    let stop = |c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ')' | '>' | ',' | ';');
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find("://") {
        let tail = &rest[i + 3..];
        let end = tail.find(stop).unwrap_or(tail.len());
        let url = &tail[..end];
        let host = url.find(['/', '?', '#']).unwrap_or(url.len());
        out.push_str(&rest[..i + 3]);
        out.push_str(&url[..host]);
        if host < url.len() {
            out.push_str("/…");
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    const PROXIED: &str = "/transfer/";
    let mut done = String::with_capacity(out.len());
    let mut rest = out.as_str();
    while let Some(i) = rest.find(PROXIED) {
        let tail = &rest[i + PROXIED.len()..];
        let end = tail.find(stop).unwrap_or(tail.len());
        done.push_str(&rest[..i + PROXIED.len()]);
        done.push('…');
        rest = &tail[end..];
    }
    done.push_str(rest);
    done
}

impl From<mrl_storage::StorageError> for AttemptError {
    fn from(e: mrl_storage::StorageError) -> Self {
        Self::Agent(AgentError::Storage(e))
    }
}

impl AttemptError {
    /// The stable error code, like `MRL-ENV-201`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Spec(_) => "MRL-VAL-200",
            Self::UnsafePath { .. } => "MRL-VAL-205",
            Self::LockMismatch { .. } => "MRL-ENV-201",
            Self::EnvInstall { .. } => "MRL-ENV-202",
            Self::UvMissing(_) => "MRL-ENV-011",
            Self::DockerMissing(_) => "MRL-ENV-012",
            Self::Socket { .. } => "MRL-NODE-203",
            Self::State { .. } => "MRL-STATE-204",
            Self::Draining(_) => "MRL-NODE-206",
            Self::Agent(e) => e.code(),
        }
    }

    /// The code and the message, for logs and the command line.
    pub fn explain(&self) -> String {
        // Every message that leaves the agent goes through here: links are cut to their host,
        // since a signed ticket's path and query are what grant access.
        redact_links(&format!("{} {self}", self.code()))
    }

    /// The failure class an attempt gets when this error stops it.
    pub fn failure_class(&self) -> FailureClass {
        match self {
            Self::Spec(_) | Self::UnsafePath { .. } => FailureClass::UserConfig,
            Self::LockMismatch { .. }
            | Self::EnvInstall { .. }
            | Self::UvMissing(_)
            | Self::DockerMissing(_) => FailureClass::DependencyEnv,
            Self::Agent(AgentError::CacheFull { .. }) => FailureClass::InfraTransient,
            Self::Agent(AgentError::SandboxUnavailable(_)) => FailureClass::DependencyEnv,
            Self::Agent(
                AgentError::Transfer { .. }
                | AgentError::TicketRefused { .. }
                | AgentError::Storage(_)
                | AgentError::State { .. },
            ) => FailureClass::InfraTransient,
            Self::Socket { .. } | Self::State { .. } | Self::Draining(_) | Self::Agent(_) => {
                FailureClass::HarnessError
            }
        }
    }

    /// This error as an agent-classified failure.
    pub fn to_failure(&self) -> Failure {
        let class = self.failure_class();
        Failure {
            class: class as i32,
            sub_code: self.code().to_owned(),
            message: self.explain(),
            detail_ref: String::new(),
            retryable_hint: matches!(
                class,
                FailureClass::InfraTransient | FailureClass::DependencyEnv
            ),
            suggested_mutations: Vec::new(),
            classified_by: ClassifiedBy::Agent as i32,
        }
    }

    pub(crate) fn state(attempt_id: &str, reason: impl Into<String>) -> Self {
        Self::State {
            attempt_id: attempt_id.to_owned(),
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_are_cut_to_their_host() {
        let t = redact_links(
            "PUT https://bucket.example/obj/key?X-Sig=SECRET failed (http://h:9/api/v1/transfer/TOKEN)",
        );
        assert!(
            !t.contains("SECRET") && !t.contains("TOKEN") && !t.contains("obj/key"),
            "{t}"
        );
        assert!(
            t.contains("https://bucket.example/…") && t.contains("http://h:9/…"),
            "{t}"
        );
        assert_eq!(redact_links("no links here"), "no links here");
        assert!(!redact_links("relative /api/v1/transfer/TOKEN2 x").contains("TOKEN2"));
    }

    #[test]
    fn codes_and_classes() {
        let e = AttemptError::LockMismatch {
            runtime_ref: "uv:sha256:aa".into(),
            expected: "aa".into(),
            actual: "bb".into(),
        };
        assert_eq!(e.code(), "MRL-ENV-201");
        assert_eq!(e.failure_class(), FailureClass::DependencyEnv);
        let f = e.to_failure();
        assert_eq!(f.class, FailureClass::DependencyEnv as i32);
        assert_eq!(f.classified_by, ClassifiedBy::Agent as i32);
        assert!(f.message.starts_with("MRL-ENV-201 "));
        let e = AttemptError::from(mrl_storage::StorageError::TicketExpired);
        assert_eq!(e.code(), "MRL-STORE-008");
        assert_eq!(e.failure_class(), FailureClass::InfraTransient);
    }
}
