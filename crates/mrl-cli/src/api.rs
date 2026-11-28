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

/// A failed call: the HTTP status when the server answered, and the plain-language message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The status, or `None` when nothing answered.
    pub status: Option<u16>,
    /// Code, title, details and steps.
    pub message: String,
}

impl From<Failure> for String {
    fn from(f: Failure) -> Self {
        f.message
    }
}

/// How long a dry run may wait: above the longest settings-check budget the server accepts
/// (`[submission] probe_timeout_seconds` ≤ 600 s), so the CLI always gets the server's answer,
/// including its no-probe fallback.
#[allow(dead_code)] // P28's `runs validate` and `--dry-run` use it (Wave 6 lane).
pub const DRY_RUN_DEADLINE: Duration = Duration::from_secs(630);

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
        let http = http_client().map_err(|e| format!("can't set up the connection: {e}"))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            token,
            launch_secret,
            http,
        })
    }

    /// `GET /api/v1{path}`.
    pub fn get(&self, path: &str) -> Result<Value, Failure> {
        self.send(reqwest::Method::GET, path, None, None, None)
    }

    /// `POST /api/v1{path}` with an optional JSON body and idempotency key.
    pub fn post(
        &self,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
    ) -> Result<Value, Failure> {
        self.send(reqwest::Method::POST, path, body, key, None)
    }

    /// Like [`Api::post`], but waits up to `deadline` instead of the usual 90 s (a dry run
    /// that waits for a settings check: [`DRY_RUN_DEADLINE`]).
    #[allow(dead_code)] // P28's dry-run commands use it (Wave 6 lane).
    pub fn post_waiting(
        &self,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
        deadline: Duration,
    ) -> Result<Value, Failure> {
        self.send(reqwest::Method::POST, path, body, key, Some(deadline))
    }

    fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
        key: Option<&str>,
        deadline: Option<Duration>,
    ) -> Result<Value, Failure> {
        let unreachable = |detail: String| Failure {
            status: None,
            message: explain(UNREACHABLE, &detail),
        };
        let url = format!("{}/api/v1{path}", self.base);
        let mut req = self.http.request(method, &url);
        if let Some(d) = deadline {
            req = req.timeout(d);
        }
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
            if e.is_redirect() {
                return unreachable(format!(
                    "{} sent the CLI to another address. Your token is only sent to the configured server.",
                    self.base
                ));
            }
            let what = if e.is_timeout() {
                "The server took too long to answer"
            } else {
                "Nothing answered"
            };
            unreachable(format!("{what} at {}.", self.base))
        })?;
        let status = res.status();
        let text = res
            .text()
            .map_err(|e| unreachable(format!("The answer broke off: {e}.")))?;
        let json: Value = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        if status.is_success() {
            return Ok(json);
        }
        Err(Failure {
            status: Some(status.as_u16()),
            message: problem(status.as_u16(), &json),
        })
    }
}

/// The HTTP client. Redirects are followed only within the server's origin (at most 5), so
/// the token and launch secret never reach another host.
fn http_client() -> reqwest::Result<reqwest::blocking::Client> {
    let policy = reqwest::redirect::Policy::custom(|attempt| {
        let same = attempt
            .previous()
            .first()
            .is_some_and(|first| first.origin() == attempt.url().origin());
        if !same {
            attempt.error("redirect to another origin")
        } else if attempt.previous().len() > 5 {
            attempt.error("too many redirects")
        } else {
            attempt.follow()
        }
    });
    reqwest::blocking::Client::builder()
        // Above the server's default 60 s settings-check budget; longer budgets use
        // `post_waiting` with `DRY_RUN_DEADLINE`.
        .timeout(Duration::from_secs(90))
        .redirect(policy)
        .build()
}

/// Every item of the cursor-paged list `list` (`{items, next_cursor}`), following
/// `next_cursor` to the end. A server that repeats a cursor is an error, not a loop.
pub fn all_items(
    mut get: impl FnMut(&str) -> Result<Value, String>,
    list: &str,
) -> Result<Vec<Value>, String> {
    let mut items = Vec::new();
    let mut path = list.to_owned();
    let mut cursors = std::collections::HashSet::new();
    loop {
        let page = get(&path)?;
        items.extend(page["items"].as_array().cloned().unwrap_or_default());
        match page["next_cursor"].as_str() {
            None => return Ok(items),
            Some(c) if !cursors.insert(c.to_owned()) => {
                return Err(format!("{list} sent the same page cursor twice; stopped"));
            }
            Some(c) => path = format!("{list}?cursor={}", crate::commands::encode(c)),
        }
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

    /// Serves one canned answer per connection and records each request's head.
    fn serve(answers: Vec<String>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for answer in answers {
                let (mut stream, _) = listener.accept().unwrap();
                let mut head = String::new();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    head.push_str(&line.to_ascii_lowercase());
                }
                seen.push(head);
                stream.write_all(answer.as_bytes()).unwrap();
            }
            seen
        });
        (base, handle)
    }

    fn api(base: &str) -> Api {
        Api {
            base: base.to_owned(),
            token: Some("tok".into()),
            launch_secret: Some("secret".into()),
            http: http_client().unwrap(),
        }
    }

    fn redirect(to: &str) -> String {
        format!("HTTP/1.1 302 Found\r\nlocation: {to}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
    }

    #[test]
    fn redirects_never_take_credentials_to_another_origin() {
        let other = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let elsewhere = format!("http://{}/api/v1/x", other.local_addr().unwrap());
        let (base, server) = serve(vec![redirect(&elsewhere)]);
        let err = api(&base).get("/x").unwrap_err();
        assert_eq!(err.status, None);
        assert!(err.message.contains("another address"), "{}", err.message);
        assert_eq!(server.join().unwrap().len(), 1);
        other.set_nonblocking(true).unwrap();
        assert!(other.accept().is_err(), "the other origin was contacted");

        // Same origin: followed, credentials kept.
        let ok = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}";
        let (base, server) = serve(vec![redirect("/api/v1/y"), ok.to_owned()]);
        assert_eq!(api(&base).get("/x").unwrap(), serde_json::json!({}));
        let seen = server.join().unwrap();
        assert!(seen[1].starts_with("get /api/v1/y "), "{seen:?}");
        for head in &seen {
            assert!(head.contains("authorization: bearer tok"), "{head}");
            assert!(head.contains("x-mrl-launch-secret: secret"), "{head}");
        }
    }

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
