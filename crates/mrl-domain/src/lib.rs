//! Pure domain types for MasterRL: IDs, state machines, failure classes, roles,
//! error codes and resource requests.
//!
//! This crate does no IO. CI rejects tokio, sqlx, tonic and other IO crates here
//! (the optional `sqlx` feature adds `sqlx::Type` for IDs and is off by default).

pub mod errors;
pub mod failure;
pub mod ids;
pub mod rbac;
pub mod resources;
pub mod state;

/// Capability checks. This crate holds only the rejection codes.
pub mod capability {
    pub mod codes;
}

/// The version of this crate, taken from its `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_is_major_minor_patch() {
        let parts: Vec<&str> = VERSION.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "version {VERSION} must be MAJOR.MINOR.PATCH"
        );
        for part in parts {
            assert!(
                part.parse::<u64>().is_ok(),
                "version {VERSION} has a non-numeric part"
            );
        }
    }
}
