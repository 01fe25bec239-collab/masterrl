//! MasterRL object storage, shared by the server and the agent.
//!
//! - [`Backend`]: local folder, S3-compatible, and (behind features) GCS and Azure, each with
//!   honest [`Capabilities`].
//! - [`Store`]: a handle scoped to one org; it never touches keys outside `orgs/{org}/`.
//! - [`OrgScope`]: the key layout.
//! - Streaming sha256, parallel resumable transfers, transfer tickets and
//!   `artifact-manifest/v1` helpers.

mod error;
mod hash;
mod keys;
mod manifest;
mod store;
mod ticket;
mod transfer;

pub use error::{Result, StorageError};
pub use hash::{sha256_file, sha256_hex, sha256_stream};
pub use keys::{org_of, Derived, OrgScope, MAX_KEY_LEN, MAX_SEGMENT_LEN};
pub use manifest::{
    ArtifactKind, ArtifactManifest, FileProblem, FileSpec, ManifestFile, SCHEMA_VERSION,
};
pub use store::{Backend, BackendConfig, Capabilities, ObjectInfo, S3Config, Store};
pub use ticket::{
    sign_ticket, verify_ticket, TicketClaims, TicketKey, TicketMethod, TransferTicket,
    MAX_TICKET_TTL, MIN_KEY_LEN, PROXY_PATH,
};
pub use transfer::{
    Download, TransferOptions, TransferOutcome, Upload, MAX_PART_SIZE, MIN_PART_SIZE,
};
