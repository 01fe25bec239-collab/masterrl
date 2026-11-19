//! SSH: the core opens an SSH session to the machine and forwards the gateway connection
//! through a `direct-tcpip` channel to the agent's loopback port.
//!
//! - Keys: a key from the secret store (`key_secret`), then the SSH agent, then `IdentityFile`
//!   keys from `~/.ssh/config` and the usual `~/.ssh/id_*` files (keys without a passphrase).
//! - Host keys are always checked against the core's own `known_hosts`. An unknown host fails
//!   with its fingerprint; once the user confirms it, [`Ssh::trust_host_key`] records it.
//! - `~/.ssh/config` `Host` aliases give `HostName`, `User`, `Port` and `IdentityFile`.

use super::{codes, BoxIo, ConnectionReport, Hold, Transport, TransportError, TransportKind};
use crate::seams::SecretStore;
use russh::client::{AuthResult, Handle, Handler};
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Where and as whom to connect, after reading `~/.ssh/config`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTarget {
    /// The name the user gave (alias or host).
    pub alias: String,
    /// The host to connect to.
    pub hostname: String,
    /// SSH port.
    pub port: u16,
    /// SSH user.
    pub user: String,
    /// Private key files to try, in order.
    pub identity_files: Vec<PathBuf>,
}

impl SshTarget {
    /// Resolves `host` through the SSH config file (when given and readable); explicit
    /// `port` / `user` win over the file.
    pub fn resolve(
        host: &str,
        port: Option<u16>,
        user: Option<&str>,
        config: Option<&Path>,
    ) -> Self {
        let text = config
            .and_then(|p| std::fs::read_to_string(p).ok())
            .unwrap_or_default();
        let mut t = parse_config(&text, host);
        if let Some(p) = port {
            t.port = p;
        }
        if let Some(u) = user {
            t.user = u.to_owned();
        }
        t
    }
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn expand(path: &str) -> PathBuf {
    match (path.strip_prefix("~/"), home()) {
        (Some(rest), Some(h)) => h.join(rest),
        _ => PathBuf::from(path),
    }
}

/// `*` and `?` glob, as in `Host` lines.
fn glob(pattern: &str, text: &str) -> bool {
    fn go(p: &[u8], t: &[u8]) -> bool {
        match (p.first(), t.first()) {
            (None, None) => true,
            (Some(b'*'), _) => go(&p[1..], t) || (!t.is_empty() && go(p, &t[1..])),
            (Some(b'?'), Some(_)) => go(&p[1..], &t[1..]),
            (Some(a), Some(b)) if a.eq_ignore_ascii_case(b) => go(&p[1..], &t[1..]),
            _ => false,
        }
    }
    go(pattern.as_bytes(), text.as_bytes())
}

/// Reads the settings for `alias` from an OpenSSH client config (first value wins, like `ssh`).
pub fn parse_config(text: &str, alias: &str) -> SshTarget {
    let (mut hostname, mut port, mut user, mut files) = (None, None, None, Vec::new());
    let mut active = true;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = match line.split_once(|c: char| c.is_whitespace() || c == '=') {
            Some((k, v)) => (
                k.to_ascii_lowercase(),
                v.trim().trim_start_matches('=').trim(),
            ),
            None => continue,
        };
        let value = value.trim_matches('"');
        match key.as_str() {
            "host" => {
                let pats: Vec<&str> = value.split_whitespace().collect();
                let negated = pats
                    .iter()
                    .any(|p| p.strip_prefix('!').is_some_and(|p| glob(p, alias)));
                active = !negated && pats.iter().any(|p| !p.starts_with('!') && glob(p, alias));
            }
            "match" => active = false, // ponytail: `Match` blocks are skipped; add when users need them.
            _ if !active => {}
            "hostname" => {
                hostname.get_or_insert_with(|| value.replace("%h", alias));
            }
            "port" => {
                if port.is_none() {
                    port = value.parse::<u16>().ok();
                }
            }
            "user" => {
                user.get_or_insert_with(|| value.to_owned());
            }
            "identityfile" => files.push(expand(value)),
            _ => {}
        }
    }
    if let Some(h) = home() {
        for name in ["id_ed25519", "id_ecdsa", "id_rsa"] {
            let p = h.join(".ssh").join(name);
            if !files.contains(&p) {
                files.push(p);
            }
        }
    }
    SshTarget {
        alias: alias.to_owned(),
        hostname: hostname.unwrap_or_else(|| alias.to_owned()),
        port: port.unwrap_or(22),
        user: user.unwrap_or_else(|| {
            std::env::var("USER")
                .or_else(|_| std::env::var("USERNAME"))
                .unwrap_or_else(|_| "root".to_owned())
        }),
        identity_files: files,
    }
}

/// The SHA-256 fingerprint `ssh-keygen -l` prints, like `SHA256:…`.
pub fn fingerprint(key: &PublicKey) -> String {
    key.fingerprint(HashAlg::Sha256).to_string()
}

/// Decides what [`Ssh::trust_host_key`] saves from one handshake's verdict: `None` when the
/// key is already trusted, the shown key when its fingerprint is `expected`, else an error.
fn key_to_save(
    verdict: Option<HostKey>,
    expected: &str,
    host: &str,
) -> Result<Option<PublicKey>, TransportError> {
    match verdict {
        Some(HostKey::Known) => Ok(None),
        Some(HostKey::Unknown(k)) if fingerprint(&k) == expected => Ok(Some(k)),
        Some(HostKey::Unknown(k)) => Err(TransportError::Unreachable(format!(
            "{host} now shows the key {}, not the confirmed {expected}; nothing was saved",
            fingerprint(&k)
        ))),
        Some(HostKey::Changed(line)) => Err(TransportError::HostKeyChanged {
            host: host.to_owned(),
            line,
        }),
        Some(HostKey::Unreadable(e)) => Err(TransportError::Unreachable(e)),
        None => Err(TransportError::Unreachable(format!(
            "{host} did not show a host key"
        ))),
    }
}

/// What the host key check decided.
#[derive(Debug, Clone)]
enum HostKey {
    Known,
    Unknown(PublicKey),
    Changed(usize),
    Unreadable(String),
}

/// Checks the server key against the core's `known_hosts`; mandatory, never "accept all".
struct Checker {
    known_hosts: PathBuf,
    host: String,
    port: u16,
    seen: Arc<Mutex<Option<HostKey>>>,
}

impl Handler for Checker {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let key = match key {
            PublicKeyOrCertificate::PublicKey { key, .. } => key.clone(),
            PublicKeyOrCertificate::Certificate(c) => PublicKey::new(c.public_key().clone(), ""),
        };
        let verdict = match russh::keys::check_known_hosts_path(
            &self.host,
            self.port,
            &key,
            &self.known_hosts,
        ) {
            Ok(true) => HostKey::Known,
            Ok(false) => HostKey::Unknown(key),
            Err(russh::keys::Error::KeyChanged { line }) => HostKey::Changed(line),
            Err(e) => HostKey::Unreadable(e.to_string()),
        };
        let ok = matches!(verdict, HostKey::Known);
        if let Ok(mut s) = self.seen.lock() {
            *s = Some(verdict);
        }
        Ok(ok)
    }
}

/// The SSH transport.
pub struct Ssh {
    target: SshTarget,
    agent_port: u16,
    key_secret: Option<String>,
    known_hosts: PathBuf,
    secrets: Arc<dyn SecretStore>,
}

impl Ssh {
    /// SSH to `target`, then forward to `127.0.0.1:agent_port` on that machine.
    pub fn new(
        target: SshTarget,
        agent_port: u16,
        key_secret: Option<String>,
        known_hosts: PathBuf,
        secrets: Arc<dyn SecretStore>,
    ) -> Self {
        Self {
            target,
            agent_port,
            key_secret,
            known_hosts,
            secrets,
        }
    }

    fn host_port(&self) -> String {
        format!("{}:{}", self.target.hostname, self.target.port)
    }

    /// Records the host's key as trusted, after the user confirmed `expected` (the fingerprint
    /// from [`TransportError::HostKeyUnknown`]). One handshake: the key that is compared with
    /// `expected` is the key that is saved, so a host that shows another key is never trusted.
    pub async fn trust_host_key(&self, expected: &str) -> Result<(), TransportError> {
        let seen = Arc::new(Mutex::new(None));
        let _ = self.raw_handshake(seen.clone()).await;
        let verdict = seen.lock().ok().and_then(|s| s.clone());
        let Some(key) = key_to_save(verdict, expected, &self.host_port())? else {
            return Ok(()); // already trusted
        };
        russh::keys::known_hosts::learn_known_hosts_path(
            &self.target.hostname,
            self.target.port,
            &key,
            &self.known_hosts,
        )
        .map_err(|e| {
            TransportError::Unreachable(format!(
                "could not save the host key to {}: {e}",
                self.known_hosts.display()
            ))
        })
    }

    async fn raw_handshake(
        &self,
        seen: Arc<Mutex<Option<HostKey>>>,
    ) -> Result<Handle<Checker>, TransportError> {
        let tcp = super::tcp_connect(&self.host_port()).await?;
        let config = Arc::new(russh::client::Config {
            inactivity_timeout: None,
            keepalive_interval: Some(std::time::Duration::from_secs(20)),
            ..Default::default()
        });
        let checker = Checker {
            known_hosts: self.known_hosts.clone(),
            host: self.target.hostname.clone(),
            port: self.target.port,
            seen,
        };
        let connect = russh::client::connect_stream(config, tcp, checker);
        match tokio::time::timeout(super::CONNECT_TIMEOUT, connect).await {
            Ok(Ok(h)) => Ok(h),
            Ok(Err(e)) => Err(TransportError::Unreachable(format!(
                "the SSH handshake with {} failed: {e}",
                self.host_port()
            ))),
            Err(_) => Err(TransportError::Unreachable(format!(
                "{} accepted the connection but did not finish the SSH handshake",
                self.host_port()
            ))),
        }
    }

    /// SSH handshake with the host key check.
    async fn handshake(&self) -> Result<Handle<Checker>, TransportError> {
        let seen = Arc::new(Mutex::new(None));
        let result = self.raw_handshake(seen.clone()).await;
        let verdict = seen.lock().ok().and_then(|s| s.clone());
        match verdict {
            Some(HostKey::Unknown(k)) => Err(TransportError::HostKeyUnknown {
                host: self.host_port(),
                fingerprint: fingerprint(&k),
            }),
            Some(HostKey::Changed(line)) => Err(TransportError::HostKeyChanged {
                host: self.host_port(),
                line,
            }),
            Some(HostKey::Unreadable(e)) => Err(TransportError::Unreachable(format!(
                "could not read {}: {e}",
                self.known_hosts.display()
            ))),
            Some(HostKey::Known) | None => result,
        }
    }

    async fn authenticate(&self, handle: &mut Handle<Checker>) -> Result<(), TransportError> {
        let user = self.target.user.clone();
        let mut keys = Vec::new();
        if let Some(name) = &self.key_secret {
            match self.secrets.get(name).await {
                Ok(Some(s)) => match russh::keys::decode_secret_key(s.expose(), None) {
                    Ok(k) => keys.push(k),
                    Err(_) => {
                        return Err(TransportError::Unreachable(format!(
                            "the SSH key stored as {name:?} could not be read (a key with a passphrase can't be used here)"
                        )))
                    }
                },
                _ => {
                    return Err(TransportError::Unreachable(format!(
                        "no SSH key is stored as {name:?}"
                    )))
                }
            }
        }
        for k in keys {
            let hash = if k.algorithm().is_rsa() {
                handle_rsa_hash(handle).await
            } else {
                None
            };
            let key = PrivateKeyWithHashAlg::new(Arc::new(k), hash);
            if matches!(
                handle.authenticate_publickey(&user, key).await,
                Ok(AuthResult::Success)
            ) {
                return Ok(());
            }
        }
        if agent_auth(handle, &user).await {
            return Ok(());
        }
        for path in &self.target.identity_files {
            let Ok(k) = russh::keys::load_secret_key(path, None) else {
                continue;
            };
            let hash = if k.algorithm().is_rsa() {
                handle_rsa_hash(handle).await
            } else {
                None
            };
            let key = PrivateKeyWithHashAlg::new(Arc::new(k), hash);
            if matches!(
                handle.authenticate_publickey(&user, key).await,
                Ok(AuthResult::Success)
            ) {
                return Ok(());
            }
        }
        Err(TransportError::Unreachable(format!(
            "{} refused every SSH key the app tried for user {user}; add your public key to ~/.ssh/authorized_keys on that machine, or load the key into your SSH agent",
            self.host_port()
        )))
    }

    async fn forward(&self, handle: Handle<Checker>) -> Result<BoxIo, TransportError> {
        let channel = handle
            .channel_open_direct_tcpip("127.0.0.1", u32::from(self.agent_port), "127.0.0.1", 0)
            .await
            .map_err(|e| {
                TransportError::Unreachable(format!(
                    "signed in to {}, but could not reach the agent on its port {} ({e}); start the agent there with `gateway.listen = \"127.0.0.1:{}\"`",
                    self.target.alias, self.agent_port, self.agent_port
                ))
            })?;
        Ok(Box::new(Hold::new(channel.into_stream(), handle)))
    }
}

async fn handle_rsa_hash(handle: &Handle<Checker>) -> Option<HashAlg> {
    handle
        .best_supported_rsa_hash()
        .await
        .ok()
        .flatten()
        .flatten()
}

/// Tries every identity of the running SSH agent.
async fn agent_auth(handle: &mut Handle<Checker>, user: &str) -> bool {
    #[cfg(unix)]
    let agent = russh::keys::agent::client::AgentClient::connect_env().await;
    #[cfg(windows)]
    let agent =
        russh::keys::agent::client::AgentClient::connect_named_pipe(r"\\.\pipe\openssh-ssh-agent")
            .await;
    let Ok(mut agent) = agent else {
        return false;
    };
    let Ok(ids) = agent.request_identities().await else {
        return false;
    };
    for id in ids {
        let key = id.public_key().into_owned();
        let hash = if key.algorithm().is_rsa() {
            handle_rsa_hash(handle).await
        } else {
            None
        };
        if matches!(
            handle
                .authenticate_publickey_with(user, key, hash, &mut agent)
                .await,
            Ok(AuthResult::Success)
        ) {
            return true;
        }
    }
    false
}

#[async_trait::async_trait]
impl Transport for Ssh {
    fn kind(&self) -> TransportKind {
        TransportKind::Ssh
    }

    fn target(&self) -> String {
        format!(
            "ssh {}@{} → 127.0.0.1:{}",
            self.target.user,
            self.host_port(),
            self.agent_port
        )
    }

    async fn connect(&self) -> Result<BoxIo, TransportError> {
        let mut handle = self.handshake().await?;
        self.authenticate(&mut handle).await?;
        self.forward(handle).await
    }

    async fn connection_test(&self) -> ConnectionReport {
        let mut report = ConnectionReport::new(self.kind(), self.target());
        if super::tcp_connect(&self.host_port()).await.is_err() {
            super::direct::probe(&self.host_port(), &mut report).await;
            return report;
        }
        report.pass("tcp", format!("{} accepts connections", self.host_port()));
        let mut handle = match self.handshake().await {
            Ok(h) => {
                report.pass("host_key", "the host key matches the one you confirmed");
                h
            }
            Err(e) => {
                let detail = match &e {
                    TransportError::HostKeyUnknown { .. } => {
                        format!("{e}; compare it with `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub` on the machine")
                    }
                    _ => e.to_string(),
                };
                report.fail("host_key", e.code(), detail);
                return report;
            }
        };
        if let Err(e) = self.authenticate(&mut handle).await {
            report.fail("sign_in", e.code(), e.to_string());
            return report;
        }
        report.pass("sign_in", format!("signed in as {}", self.target.user));
        match self.forward(handle).await {
            Ok(_) => report.pass(
                "forward",
                format!("the agent's port {} answers through SSH", self.agent_port),
            ),
            Err(e) => report.fail("forward", codes::UNREACHABLE, e.to_string()),
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = "
# lab machines
Host gpu-* !gpu-old
    HostName %h.lab.example
    User trainer
    Port 2222
    IdentityFile ~/.ssh/lab_key

Host *
    User fallback
    Port 22
";

    #[test]
    fn config_aliases_resolve() {
        let t = parse_config(CONFIG, "gpu-1");
        assert_eq!(t.hostname, "gpu-1.lab.example");
        assert_eq!((t.port, t.user.as_str()), (2222, "trainer"));
        assert!(t.identity_files[0].ends_with(".ssh/lab_key"));
        let other = parse_config(CONFIG, "gpu-old");
        assert_eq!(other.hostname, "gpu-old");
        assert_eq!((other.port, other.user.as_str()), (22, "fallback"));
        let explicit = SshTarget::resolve("gpu-1", Some(22), Some("me"), None);
        assert_eq!((explicit.port, explicit.user.as_str()), (22, "me"));
        assert!(glob("a*c", "abbc") && glob("a?c", "abc") && !glob("a?c", "ac"));
    }

    fn ssh_to(addr: &str, dir: &Path) -> Ssh {
        let (host, port) = super::super::split_host_port(addr).unwrap();
        Ssh::new(
            SshTarget::resolve(&host, Some(port), Some("nobody"), None),
            7443,
            None,
            dir.join("known_hosts"),
            Arc::new(super::super::tests::NoSecrets),
        )
    }

    #[tokio::test]
    async fn closed_port_is_explained() {
        let dir = tempfile::tempdir().unwrap();
        let r = ssh_to("127.0.0.1:1", dir.path()).connection_test().await;
        assert!(!r.ok);
        assert!(r.problem().unwrap().contains("nothing listens"), "{r:?}");
    }

    #[tokio::test]
    async fn a_server_that_is_not_ssh_fails_the_handshake() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                use tokio::io::AsyncWriteExt;
                let _ = s.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let r = ssh_to(&addr, dir.path()).connection_test().await;
        assert!(!r.ok);
        assert_eq!(r.steps.last().unwrap().name, "host_key");
        assert!(r.problem().unwrap().contains("SSH handshake"), "{r:?}");
    }

    #[test]
    fn only_the_confirmed_key_is_saved() {
        let a = russh::keys::parse_public_key_base64(
            "AAAAC3NzaC1lZDI1NTE5AAAAIFO3B00ltJOm7PCGwKQuYou7uZ/NBr5Uw0M25OC6o7t0",
        )
        .unwrap();
        let b = russh::keys::parse_public_key_base64(
            "AAAAC3NzaC1lZDI1NTE5AAAAIBbJ9jiHIfAuBVlRrQlCgVt0dx07250b7SS/47uxjYjx",
        )
        .unwrap();
        let fa = fingerprint(&a);
        // The user confirmed A, but the handshake now shows B: nothing is saved.
        let e = key_to_save(Some(HostKey::Unknown(b.clone())), &fa, "h:22").unwrap_err();
        assert!(e.to_string().contains("nothing was saved"), "{e}");
        assert_eq!(
            key_to_save(Some(HostKey::Unknown(a.clone())), &fa, "h:22").unwrap(),
            Some(a)
        );
        assert!(key_to_save(Some(HostKey::Known), &fa, "h:22")
            .unwrap()
            .is_none());
        assert!(matches!(
            key_to_save(Some(HostKey::Changed(3)), &fa, "h:22"),
            Err(TransportError::HostKeyChanged { line: 3, .. })
        ));
        assert!(key_to_save(None, &fa, "h:22").is_err());
    }

    /// One named secret.
    struct OneSecret(String, String);

    #[async_trait::async_trait]
    impl SecretStore for OneSecret {
        async fn get(
            &self,
            name: &str,
        ) -> Result<Option<crate::seams::Secret>, crate::seams::SeamError> {
            Ok((name == self.0).then(|| crate::seams::Secret::new(self.1.clone())))
        }
        async fn set(
            &self,
            _: &str,
            _: crate::seams::Secret,
        ) -> Result<(), crate::seams::SeamError> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> Result<(), crate::seams::SeamError> {
            Ok(())
        }
    }

    /// A real SSH server (ci-gateway runs one in a container). Needs `MRL_TEST_SSH_ADDR`
    /// (`host:port`), `MRL_TEST_SSH_USER`, `MRL_TEST_SSH_KEY` (private key file) and
    /// `MRL_TEST_SSH_FORWARD_PORT` (a port listening inside the container). Prints SKIPPED
    /// without them unless `MRL_TEST_SSH_REQUIRED` is set.
    #[tokio::test]
    #[ignore = "needs an SSH server (ci-gateway)"]
    async fn ssh_container_first_use_confirm_forward_and_changed_key() {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let (Some(addr), Some(user), Some(key), Some(fwd)) = (
            env("MRL_TEST_SSH_ADDR"),
            env("MRL_TEST_SSH_USER"),
            env("MRL_TEST_SSH_KEY"),
            env("MRL_TEST_SSH_FORWARD_PORT"),
        ) else {
            assert!(
                env("MRL_TEST_SSH_REQUIRED").is_none(),
                "MRL_TEST_SSH_* must be set"
            );
            eprintln!("SKIPPED: no MRL_TEST_SSH_ADDR");
            return;
        };
        let (host, port) = super::super::split_host_port(&addr).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let known = dir.path().join("known_hosts");
        let ssh = Ssh::new(
            SshTarget::resolve(&host, Some(port), Some(&user), None),
            fwd.parse().unwrap(),
            Some("ssh_key".into()),
            known.clone(),
            Arc::new(OneSecret(
                "ssh_key".into(),
                std::fs::read_to_string(key).unwrap(),
            )),
        );

        // First use: refused with the fingerprint until confirmed.
        let r = ssh.connection_test().await;
        assert_eq!(r.code.as_deref(), Some(codes::HOST_KEY_UNKNOWN), "{r:?}");
        let Err(TransportError::HostKeyUnknown { fingerprint, .. }) = ssh.connect().await else {
            panic!("an unknown host must be refused")
        };
        assert!(fingerprint.starts_with("SHA256:"));
        assert!(ssh.trust_host_key("SHA256:not-the-key").await.is_err());
        assert!(!known.exists() || std::fs::read_to_string(&known).unwrap().is_empty());
        ssh.trust_host_key(&fingerprint).await.unwrap();

        // Confirmed: sign in with the stored key and forward to the port.
        let r = ssh.connection_test().await;
        assert!(r.ok, "{r:?}");
        assert_eq!(
            r.steps.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["tcp", "host_key", "sign_in", "forward"]
        );
        assert!(ssh.connect().await.is_ok());

        // A different key for the same host is refused as a possible attack.
        let host_entry = if port == 22 {
            host.clone()
        } else {
            format!("[{host}]:{port}")
        };
        std::fs::write(
            &known,
            format!("{host_entry} ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFO3B00ltJOm7PCGwKQuYou7uZ/NBr5Uw0M25OC6o7t0\n"),
        )
        .unwrap();
        let r = ssh.connection_test().await;
        assert_eq!(r.code.as_deref(), Some(codes::HOST_KEY_CHANGED), "{r:?}");
        assert!(r.problem().unwrap().contains("intercepting"));
    }
}
