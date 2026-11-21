//! Every code the attempt runtime can return is registered in mrl-domain (and so has guidance).

use mrl_agent::attempts::AttemptError;

#[test]
fn every_attempt_code_is_registered() {
    let errors = [
        AttemptError::Spec(String::new()),
        AttemptError::UnsafePath {
            path: String::new(),
        },
        AttemptError::LockMismatch {
            runtime_ref: String::new(),
            expected: String::new(),
            actual: String::new(),
        },
        AttemptError::EnvInstall {
            runtime_ref: String::new(),
            reason: String::new(),
        },
        AttemptError::UvMissing(String::new()),
        AttemptError::DockerMissing(String::new()),
        AttemptError::Socket {
            path: Default::default(),
            reason: String::new(),
        },
        AttemptError::State {
            attempt_id: String::new(),
            reason: String::new(),
        },
        AttemptError::Draining(String::new()),
        AttemptError::Stopping,
    ];
    for e in errors {
        let code: mrl_domain::errors::ErrorCode = e.code().parse().unwrap();
        assert!(code.title().is_some(), "{} has no registry row", e.code());
    }
}
