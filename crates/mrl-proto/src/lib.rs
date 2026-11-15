//! Protocol types for MasterRL's internal RPC, generated from `proto/` at build time.
//!
//! One module per proto package: `mrl.worker.v1` is [`worker::v1`], and so on. Each module
//! holds the prost messages plus tonic `*_client` and `*_server` modules for its services.
//! Rules for changing the protos: `docs/dev/contracts.md`.

/// The version of this crate, taken from its `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// Generated code: field docs come from proto comments, which not every field has.
macro_rules! package {
    ($area:ident, $proto:literal, $doc:literal) => {
        #[doc = $doc]
        pub mod $area {
            #[doc = concat!("`", $proto, "`.")]
            #[allow(missing_docs, clippy::all)]
            pub mod v1 {
                tonic::include_proto!($proto);
            }
        }
    };
}

package!(
    common,
    "mrl.common.v1",
    "Shared ids, failure classes and issues."
);
package!(
    agent,
    "mrl.agent.v1",
    "Core <-> mrl-agent gateway (commands, leases, telemetry, enrollment)."
);
package!(
    worker,
    "mrl.worker.v1",
    "Worker Protocol: harness <-> mrl-agent over a Unix socket."
);
package!(
    reward,
    "mrl.reward.v1",
    "RewardService, served by the reward sandbox."
);
package!(
    env,
    "mrl.env.v1",
    "EnvService: multi-turn environments and tools."
);

#[cfg(test)]
mod golden;

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
