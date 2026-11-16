//! Every code the agent can return is registered in mrl-domain (and so has guidance).

use std::path::PathBuf;

use mrl_agent::AgentError;

#[test]
fn every_agent_code_is_registered() {
    let io = || std::io::Error::other("x");
    let errors = [
        AgentError::Config {
            path: PathBuf::new(),
            reason: String::new(),
        },
        AgentError::State {
            path: PathBuf::new(),
            source: io(),
        },
        AgentError::SandboxUnavailable(String::new()),
        AgentError::Exec {
            program: String::new(),
            reason: String::new(),
        },
        AgentError::Hardware {
            backend: "x",
            reason: String::new(),
        },
        AgentError::Service(String::new()),
        AgentError::UnknownName {
            kind: "x",
            name: String::new(),
            known: String::new(),
        },
        AgentError::CacheFull {
            needed: 0,
            pinned: 0,
            budget: 0,
            min_free: 0,
        },
        AgentError::Transfer {
            key: String::new(),
            reason: String::new(),
        },
        AgentError::TicketRefused {
            key: String::new(),
            status: 403,
        },
        AgentError::Storage(mrl_storage::StorageError::TicketExpired),
    ];
    let known: Vec<String> = mrl_domain::errors::all()
        .map(|(c, _)| c.to_string())
        .collect();
    for e in &errors {
        assert!(
            known.iter().any(|k| k == e.code()),
            "{} is not registered",
            e.code()
        );
    }
}
