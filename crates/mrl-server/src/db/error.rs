//! Database errors, their catalogued codes and plain-language explanations.

use crate::seams::SeamError;
use std::path::PathBuf;

// The generated catalog is plain static data with no dependencies. It is generated, so rustfmt skips it.
#[allow(clippy::all, dead_code)]
#[rustfmt::skip]
#[path = "../../../../guidance/build/catalog.rs"]
mod catalog;

/// Codes this part adds (guidance/fragments/errors/p11.yaml).
pub mod codes {
    /// Migrations are pending and `database.auto_migrate` is off.
    pub const MIGRATIONS_PENDING: &str = "MRL-INTERNAL-110";
    /// A migration failed.
    pub const MIGRATION_FAILED: &str = "MRL-INTERNAL-111";
    /// The bundled PostgreSQL did not start, or kept stopping.
    pub const BUNDLE_FAILED: &str = "MRL-INTERNAL-112";
    /// A database backup or restore failed.
    pub const BACKUP_FAILED: &str = "MRL-INTERNAL-113";
    /// Moving the data folder to a new PostgreSQL major version failed.
    pub const UPGRADE_FAILED: &str = "MRL-INTERNAL-114";
    /// An event was set aside after too many failed deliveries.
    pub const EVENT_QUARANTINED: &str = "MRL-INTERNAL-115";
    /// The PostgreSQL programs are missing.
    pub const BINARIES_MISSING: &str = "MRL-ENV-110";
    /// Another server already uses this data folder.
    pub const DATA_DIR_LOCKED: &str = "MRL-ENV-111";
    /// The data folder was made by a newer PostgreSQL.
    pub const DATA_DIR_NEWER: &str = "MRL-ENV-112";
    /// A transaction kept conflicting with others.
    pub const CONTENDED: &str = "MRL-CONFLICT-110";
    /// The database did not answer (reused from P07).
    pub const UNAVAILABLE: &str = "MRL-INTERNAL-002";
    /// Anything unexpected (reused from P07).
    pub const INTERNAL: &str = "MRL-INTERNAL-001";
}

/// A database operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// The database could not be reached.
    #[error("the database did not answer: {0}")]
    Unavailable(#[source] sqlx::Error),
    /// A statement failed.
    #[error("a database statement failed: {0}")]
    Query(#[source] sqlx::Error),
    /// A transaction kept failing because other transactions changed the same rows.
    #[error("gave up after {attempts} tries because other changes to the same rows kept getting in the way")]
    Contended {
        /// Tries made.
        attempts: u32,
    },
    /// Migrations are waiting and automatic migration is off.
    #[error("{pending} database update(s) are waiting and `database.auto_migrate` is false; run `mrl-server migrate`")]
    MigrationsPending {
        /// How many.
        pending: usize,
    },
    /// A migration failed.
    #[error("{detail}{}", restored.as_ref().map(|p| format!("; the database was put back from the backup {}", p.display())).unwrap_or_default())]
    MigrationFailed {
        /// What failed.
        detail: String,
        /// The backup restored after the failure, if any.
        restored: Option<PathBuf>,
    },
    /// Backing up before migrating failed.
    #[error("{0}")]
    Bundle(#[from] crate::pgbundle::BundleError),
}

impl DbError {
    /// The catalogued code, like `MRL-INTERNAL-110`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unavailable(_) => codes::UNAVAILABLE,
            Self::Query(_) => codes::INTERNAL,
            Self::Contended { .. } => codes::CONTENDED,
            Self::MigrationsPending { .. } => codes::MIGRATIONS_PENDING,
            Self::MigrationFailed { .. } => codes::MIGRATION_FAILED,
            Self::Bundle(e) => e.code(),
        }
    }

    /// The SQLSTATE of a failed statement, like `40001`.
    pub fn sqlstate(&self) -> Option<String> {
        match self {
            Self::Query(sqlx::Error::Database(e)) => e.code().map(|c| c.into_owned()),
            _ => None,
        }
    }

    /// Code, title, details and steps, for people reading a terminal.
    pub fn explain(&self) -> String {
        explain(self.code(), &self.to_string())
    }
}

impl From<sqlx::Error> for DbError {
    fn from(e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed => Self::Unavailable(e),
            other => Self::Query(other),
        }
    }
}

impl From<DbError> for SeamError {
    fn from(e: DbError) -> Self {
        SeamError::new("database", format!("{}: {e}", e.code()))
    }
}

/// `<code> — <title>`, details and the "what to do" steps from the guidance catalog.
pub fn explain(code: &str, detail: &str) -> String {
    let entry = catalog::explain_error(code);
    let mut out = format!(
        "{code} — {}\nDetails: {detail}\nWhat to do:",
        entry.str("title").unwrap_or("Something went wrong.")
    );
    let steps = entry
        .get("what_to_do")
        .and_then(|w| w.get("steps"))
        .map(|s| s.as_list())
        .unwrap_or_default();
    for step in steps.iter().filter_map(|v| v.as_str()) {
        out.push_str("\n  - ");
        out.push_str(step);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const ALL: [&str; 12] = [
        codes::MIGRATIONS_PENDING,
        codes::MIGRATION_FAILED,
        codes::BUNDLE_FAILED,
        codes::BACKUP_FAILED,
        codes::UPGRADE_FAILED,
        codes::EVENT_QUARANTINED,
        codes::BINARIES_MISSING,
        codes::DATA_DIR_LOCKED,
        codes::DATA_DIR_NEWER,
        codes::CONTENDED,
        codes::UNAVAILABLE,
        codes::INTERNAL,
    ];

    #[test]
    fn every_code_is_registered_and_catalogued() {
        for code in ALL {
            let parsed = mrl_domain::errors::ErrorCode::from_str(code).unwrap();
            assert!(parsed.title().is_some(), "{code} missing from mrl-domain");
            assert!(
                catalog::error(code).is_some(),
                "{code} missing from the guidance catalog"
            );
        }
    }

    #[test]
    fn explain_has_title_detail_and_steps() {
        let e = DbError::MigrationsPending { pending: 2 };
        let text = e.explain();
        assert!(text.starts_with("MRL-INTERNAL-110 — "), "{text}");
        assert!(text.contains("Details: 2 database update(s)"), "{text}");
        assert!(text.contains("\n  - "), "{text}");
    }

    #[test]
    fn connection_errors_are_unavailable() {
        assert_eq!(
            DbError::from(sqlx::Error::PoolTimedOut).code(),
            codes::UNAVAILABLE
        );
        assert_eq!(
            DbError::from(sqlx::Error::RowNotFound).code(),
            codes::INTERNAL
        );
    }
}
