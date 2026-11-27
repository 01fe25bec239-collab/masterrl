//! The core's local crash reports: the same redacting panic hook as the agent's (one
//! implementation, kept in `mrl-agent/src/crash.rs`), installed first thing in `main`.

#[path = "../../mrl-agent/src/crash.rs"]
mod report;

pub use report::install;
