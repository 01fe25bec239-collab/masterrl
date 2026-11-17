//! Attempt runtime integration tests: the lifecycle against the real harness (Unix) and the
//! error-code registry check (every OS).

mod codes;
#[cfg(unix)]
mod lifecycle;
