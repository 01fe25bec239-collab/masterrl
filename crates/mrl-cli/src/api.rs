//! A small blocking client for the server's `/api/v1`, with plain-language errors.

use crate::config_file::CliConfig;
use serde_json::Value;
use std::time::Duration;

// The generated catalog is plain static data with no dependencies. It is generated, so rustfmt skips it.
#[allow(clippy::all, dead_code)]
#[rustfmt::skip]
#[path = "../../../guidance/build/catalog.rs"]
mod catalog;

/// No token is saved (guidance `MRL-AUTH-170`).
pub const NO_TOKEN: &str = "MRL-AUTH-170";
/// The server did not answer (guidance `MRL-ENV-170`).
pub const UNREACHABLE: &str = "MRL-ENV-170";
/// The solo launch secret header and its environment variable (set by the desktop app).
const LAUNCH_SECRET_HEADER: &str = "x-mrl-launch-secret";
const LAUNCH_SECRET_ENV: &str = "MRL_LAUNCH_SECRET";

/// `<code> — <title>`, the details, and the catalog's steps.
pub fn explain(code: &str, detail: &str) -> String {
    let entry = catalog::explain_error(code);
    let mut out = format!(
        "{code} — {}\nDetails: {detail}",
        entry.str("title").unwrap_or("Something went wrong.")
    );
    let steps = entry
        .get("what_to_do")
        .and_then(|w| w.get("steps"))
        .map(|s| s.as_list())
        .unwrap_or_default();
    if !steps.is_empty() {
        out.push_str("\nWhat to do:");
        for step in steps.iter().filter_map(|v| v.as_str()) {
            out.push_str("\n  - ");
            out.push_str(step);
        }
    }
    out
}

/// A signed-in connection to one server.
pub struct Api {
    base: String,
    token: Option<String>,
    launch_secret: Option<String>,
    http: reqwest::blocking::Client,
}

impl Api {
    /// Connects with the saved settings and environment overrides.
    pub fn connect(cfg: &CliConfig) -> Result<Self, String> {
        let base = cfg.server().ok_or_else(|| {
            explain(
                UNREACHABLE,
                "No server address is set. Run `mrl config set-server <address>` or set MRL_SERVER.",
            )
        })?;
        let launch_secret = std::env::var(LAUNCH_SECRET_ENV)
            .ok()
            .filter(|s| !s.is_empty());
        let token = cfg.token();
        if token.is_none() && launch_secret.is_none() {
            return Err(explain(
                NO_TOKEN,
                "Run `mrl config set-token` or set MRL_TOKEN.",
            ));
        }
        let http = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| format!("can't set up the connection: {e}"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            token,
            launch_secret,
            http,
        })
    }

    /// `GET /api/v1{path}`.
    pub fn get(&self, path: &str) -> Result<Value, String> {
        self.send(reqwest::Method::GET, path, None, None)
    }

    /// `POST /api/v1{path}` with an optional JSON body and idempotency key.
    pub fn post(
        &self,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
    ) -> Result<Value, String> {
        self.send(reqwest::Method::POST, path, body, key)
    }

    fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
    ) -> Result<Value, String> {
        let url = format!("{}/api/v1{path}", self.base);
        let mut req = self.http.request(method, &url);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        if let Some(s) = &self.launch_secret {
            req = req.header(LAUNCH_SECRET_HEADER, s);
        }
        if let Some(k) = key {
            req = req.header("idempotency-key", k);
        }
        if let Some(b) = body {
            req = req
                .header("content-type", "application/json")
                .body(b.to_string());
        }
        let res = req.send().map_err(|e| {
            let what = if e.is_timeout() {
                "The server took too long to answer"
            } else {
                "Nothing answered"
            };
            explain(UNREACHABLE, &format!("{what} at {}.", self.base))
        })?;
        let status = res.status();
        let text = res
            .text()
            .map_err(|e| explain(UNREACHABLE, &format!("The answer broke off: {e}.")))?;
        let json: Value = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        if status.is_success() {
            return Ok(json);
        }
        Err(problem(status.as_u16(), &json))
    }
}

/// A problem document (or any error answer) as plain text.
pub fn problem(status: u16, json: &Value) -> String {
    let Some(code) = json["code"].as_str() else {
        return format!("the server answered {status}: {json}");
    };
    let mut out = explain(code, json["detail"].as_str().unwrap_or(""));
    if let Some(errors) = json["errors"].as_array() {
        for e in errors {
            out.push_str(&format!(
                "\n  {} {}: {} ({})",
                e["code"].as_str().unwrap_or(""),
                e["path"].as_str().unwrap_or(""),
                e["message"].as_str().unwrap_or(""),
                e["hint"].as_str().unwrap_or(""),
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_uses_the_catalog() {
        let text = explain(NO_TOKEN, "none saved");
        assert!(text.starts_with("MRL-AUTH-170 — "), "{text}");
        assert!(text.contains("Details: none saved"));
        assert!(text.contains("mrl config set-token"));
        assert!(explain(UNREACHABLE, "x").contains("mrl config show"));
    }

    #[test]
    fn problems_show_code_detail_and_field_errors() {
        let p = serde_json::json!({
            "code": "MRL-VAL-004", "detail": "1 field(s) need a change.",
            "errors": [{"code": "MRL-VAL-003", "path": "/name", "message": "too long", "hint": "shorten it"}]
        });
        let text = problem(422, &p);
        assert!(text.contains("MRL-VAL-004"));
        assert!(
            text.contains("MRL-VAL-003 /name: too long (shorten it)"),
            "{text}"
        );
        assert_eq!(problem(502, &Value::Null), "the server answered 502: null");
    }
}
