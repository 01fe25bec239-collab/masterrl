//! Local network: plain TCP to the agent's listening address.

use super::{
    codes, split_host_port, tcp_connect, BoxIo, ConnectionReport, Transport, TransportError,
    TransportKind,
};

/// TCP to `host:port` (also used by Tailscale, which only changes the address).
pub struct Direct {
    address: String,
}

impl Direct {
    /// The agent at `address` (`host:port`).
    pub fn new(address: &str) -> Self {
        Self {
            address: address.to_owned(),
        }
    }
}

#[async_trait::async_trait]
impl Transport for Direct {
    fn kind(&self) -> TransportKind {
        TransportKind::Direct
    }

    fn target(&self) -> String {
        self.address.clone()
    }

    async fn connect(&self) -> Result<BoxIo, TransportError> {
        Ok(Box::new(tcp_connect(&self.address).await?))
    }

    async fn connection_test(&self) -> ConnectionReport {
        let mut report = ConnectionReport::new(self.kind(), self.target());
        probe(&self.address, &mut report).await;
        report
    }
}

/// The name lookup and TCP steps shared by direct-style transports.
pub(crate) async fn probe(address: &str, report: &mut ConnectionReport) -> bool {
    let Some((host, port)) = split_host_port(address) else {
        report.fail(
            "address",
            codes::UNREACHABLE,
            format!("{address:?} is not a host:port address, like 192.168.1.20:7443"),
        );
        return false;
    };
    let found = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map(|a| a.count())
        .unwrap_or(0);
    match found {
        1.. => report.pass("dns", format!("found {host}")),
        0 => {
            report.fail(
                "dns",
                codes::UNREACHABLE,
                format!(
                    "could not find {host} on the network; check the name, or use the machine's IP address"
                ),
            );
            return false;
        }
    }
    match tcp_connect(address).await {
        Ok(_) => {
            report.pass("tcp", format!("{host} accepts connections on port {port}"));
            true
        }
        Err(e) => {
            let code = if e.to_string().contains(codes::FIREWALL) {
                codes::FIREWALL
            } else {
                codes::UNREACHABLE
            };
            report.fail("tcp", code, e.to_string());
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn listening_port_passes() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let r = Direct::new(&addr).connection_test().await;
        assert!(r.ok, "{r:?}");
        assert_eq!(r.steps.len(), 2);
        assert!(Direct::new(&addr).connect().await.is_ok());
    }

    #[tokio::test]
    async fn closed_port_says_nothing_listens() {
        // Port 1 (tcpmux) is closed on every test machine; a freed random port may be reused.
        let r = Direct::new("127.0.0.1:1").connection_test().await;
        assert!(!r.ok);
        assert_eq!(r.code.as_deref(), Some(codes::UNREACHABLE));
        let problem = r.problem().unwrap();
        assert!(problem.contains("nothing listens on port"), "{problem}");
    }

    #[tokio::test]
    async fn unknown_host_and_bad_address_are_explained() {
        let r = Direct::new("no-such-host.invalid:7443")
            .connection_test()
            .await;
        assert!(r
            .problem()
            .unwrap()
            .contains("could not find no-such-host.invalid"));
        let r = Direct::new("just-a-name").connection_test().await;
        assert!(r.problem().unwrap().contains("host:port"));
    }
}
