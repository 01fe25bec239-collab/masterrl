//! Errors. Every variant has a stable code explained in `guidance/fragments/errors/p08.yaml`.

use std::path::PathBuf;

use crate::manifest::FileProblem;

/// Result alias for this crate.
pub type Result<T, E = StorageError> = std::result::Result<T, E>;

/// Everything that can go wrong in storage.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// A key or prefix is outside the org's area or has a bad shape.
    #[error("storage key {key:?} is not allowed: {reason}")]
    BadKey {
        /// The rejected key.
        key: String,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// An id used to build a key has characters or a shape that is not allowed.
    #[error("id {id:?} is not allowed: {reason}")]
    BadId {
        /// The rejected id.
        id: String,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// The object does not exist.
    #[error("object {key} was not found")]
    NotFound {
        /// The missing key.
        key: String,
    },
    /// The storage service returned an error (after retries).
    #[error("storage service error: {0}")]
    Backend(#[source] object_store::Error),
    /// Reading or writing a local file failed.
    #[error("local file error at {path}: {source}")]
    Io {
        /// The local file.
        path: PathBuf,
        /// The cause.
        #[source]
        source: std::io::Error,
    },
    /// Storage settings are missing or wrong.
    #[error("storage settings problem: {0}")]
    Config(String),
    /// A transfer ticket is malformed or its signature does not match.
    #[error("transfer ticket rejected: {0}")]
    TicketInvalid(&'static str),
    /// A transfer ticket is past its expiry.
    #[error("transfer ticket has expired")]
    TicketExpired,
    /// The request is outside what the ticket allows (method, size or lifetime).
    #[error("transfer ticket does not allow this: {0}")]
    TicketScope(String),
    /// Content does not match its expected sha256.
    #[error("content of {key} does not match: expected sha256 {expected}, got {actual}")]
    ChecksumMismatch {
        /// The object or file checked.
        key: String,
        /// Expected hex digest.
        expected: String,
        /// Actual hex digest.
        actual: String,
    },
    /// Stored files do not match the manifest.
    #[error("{} file(s) do not match the manifest", .0.len())]
    ManifestMismatch(Vec<FileProblem>),
    /// A manifest does not follow artifact-manifest/v1.
    #[error("manifest does not follow artifact-manifest/v1: {0}")]
    ManifestFormat(String),
    /// The local file changed while it was being transferred.
    #[error("local file {0} changed during the transfer")]
    SourceChanged(PathBuf),
}

impl StorageError {
    /// Stable code, explained in the guidance catalog.
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadKey { .. } => "MRL-STORE-001",
            Self::BadId { .. } => "MRL-STORE-002",
            Self::NotFound { .. } => "MRL-STORE-003",
            Self::Backend(_) => "MRL-STORE-004",
            Self::Io { .. } => "MRL-STORE-005",
            Self::Config(_) => "MRL-STORE-006",
            Self::TicketInvalid(_) => "MRL-STORE-007",
            Self::TicketExpired => "MRL-STORE-008",
            Self::TicketScope(_) => "MRL-STORE-009",
            Self::ChecksumMismatch { .. } => "MRL-STORE-010",
            Self::ManifestMismatch(_) => "MRL-STORE-011",
            Self::ManifestFormat(_) => "MRL-STORE-012",
            Self::SourceChanged(_) => "MRL-STORE-013",
        }
    }

    /// Maps an object_store error, turning "not found" into [`StorageError::NotFound`].
    pub(crate) fn backend(key: &str, err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { .. } => Self::NotFound {
                key: key.to_owned(),
            },
            other => Self::Backend(other),
        }
    }

    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_guidance() {
        let fragment = include_str!("../../../guidance/fragments/errors/p08.yaml");
        let all = [
            StorageError::BadKey {
                key: String::new(),
                reason: "",
            },
            StorageError::BadId {
                id: String::new(),
                reason: "",
            },
            StorageError::NotFound { key: String::new() },
            StorageError::Backend(object_store::Error::NotImplemented {
                operation: String::new(),
                implementer: String::new(),
            }),
            StorageError::io("", std::io::Error::other("x")),
            StorageError::Config(String::new()),
            StorageError::TicketInvalid(""),
            StorageError::TicketExpired,
            StorageError::TicketScope(String::new()),
            StorageError::ChecksumMismatch {
                key: String::new(),
                expected: String::new(),
                actual: String::new(),
            },
            StorageError::ManifestMismatch(Vec::new()),
            StorageError::ManifestFormat(String::new()),
            StorageError::SourceChanged(PathBuf::new()),
        ];
        for err in all {
            let code = err.code();
            assert!(
                fragment.contains(&format!("code: {code}\n")),
                "{code} is missing from p08.yaml"
            );
        }
    }
}
