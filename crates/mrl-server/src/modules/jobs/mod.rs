//! Jobs and attempts: the state core every later part drives.
//!
//! - [`JobTransitions`]: the only writer of `jobs.state` and `job_attempts.state`.
//! - [`Chains`]: [`JobValidator`], [`CompletionVerifier`] and [`SubmissionResolver`] members.
//! - [`Projects`]: which org owns a project ([`ProjectDirectory`], registered by P16).
//! - `/jobs` endpoints; the module that serves them is `runs::RunsModule`.

pub mod api;
pub mod chains;
pub mod error;
pub mod model;
pub mod projects;
pub mod scope;
pub mod transitions;

pub use chains::{
    Chains, Completion, CompletionVerifier, JobSpec, JobValidator, ResolveError, SchemaChecks,
    SubmissionResolver, SubmitScope, Validation, Verdict, Verification,
};
pub use error::{codes, DomainProblem, JobsError};
pub use model::{Attempt, AttemptRow, Job, JobRow, Rejection};
pub use projects::{ProjectDirectory, Projects, RehomeLocalRows};
pub use transitions::{AttemptChange, JobChange, JobTransitions, NewJob, RetryRules};
