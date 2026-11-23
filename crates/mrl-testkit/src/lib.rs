//! Shared test helpers for MasterRL's Rust crates. Used only from tests.
//!
//! - [`seams`]: in-memory and deterministic seam implementations.
//! - [`server`]: start a real `mrl-server` on loopback and talk HTTP to it.

pub mod seams;
pub mod server;

// <mrl:testkit P11> `pub mod pg;` goes right below this line.
pub mod pg;

// <mrl:testkit P12> `pub mod agent;` goes right below this line.
pub mod agent;

// <mrl:testkit P16> P16 `pub mod` lines go right below this line.
pub mod supabase;

// <mrl:testkit P17> P17 `pub mod` lines go right below this line.

// <mrl:testkit P18> P18 `pub mod` lines go right below this line.

// <mrl:testkit P19> P19 `pub mod` lines go right below this line.
#[cfg(test)]
#[path = "../../mrl-server/src/modules/artifacts/integration_tests.rs"]
mod p19_artifacts;

// <mrl:testkit P20> P20 `pub mod` lines go right below this line.
// <mrl:testkit P21> P21 `pub mod` lines go right below this line.
#[cfg(test)]
#[path = "../../mrl-server/src/modules/scheduler/integration_tests.rs"]
mod p21_scheduler;
// <mrl:testkit P22> P22 `pub mod` lines go right below this line.
#[cfg(test)]
#[path = "../../mrl-server/src/modules/telemetry/integration_tests.rs"]
mod p22_telemetry;
// <mrl:testkit P23> P23 `pub mod` lines go right below this line.
#[cfg(test)]
#[path = "../../mrl-server/src/modules/checkpoints/integration_tests.rs"]
mod p23_checkpoints;
// <mrl:testkit P24> P24 `pub mod` lines go right below this line.

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
