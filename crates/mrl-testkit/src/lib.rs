//! Shared test helpers for MasterRL's Rust crates. Used only from tests.
//!
//! - [`seams`]: in-memory and deterministic seam implementations.
//! - [`server`]: start a real `mrl-server` on loopback and talk HTTP to it.

pub mod seams;
pub mod server;

// <mrl:testkit P11> `pub mod pg;` goes right below this line.
pub mod pg;

// <mrl:testkit P12> `pub mod agent;` goes right below this line.

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
