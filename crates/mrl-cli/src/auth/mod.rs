//! `mrl login` and `mrl logout`, and the connection to the core that P16's commands use.
//!
//! The server address comes from `--server`, `MRL_SERVER` or the last `mrl login`. The
//! access token comes from `MRL_TOKEN` or the OS keychain (service `masterrl-cli`). In solo
//! mode the first `mrl login` also needs `MRL_LAUNCH_SECRET`, the secret the core printed
//! (or the desktop app holds), because a solo core answers nobody else.

use reqwest::blocking::Client as Http;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

/// Keychain service for the CLI's own secrets.
pub const KEYCHAIN_SERVICE: &str = "masterrl-cli";
const SERVER_KEY: &str = "server";
const LOGIN_WAIT: Duration = Duration::from_secs(300);

/// Where the CLI keeps the server address and its access token.
pub trait Credentials {
    /// Reads `name`.
    fn get(&self, name: &str) -> Result<Option<String>, String>;
    /// Writes `name`.
    fn set(&self, name: &str, value: &str) -> Result<(), String>;
    /// Deletes `name` (no error if absent).
    fn delete(&self, name: &str) -> Result<(), String>;
}

/// The OS keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service).
pub struct Keyring;

fn keychain_error(e: &keyring::Error) -> String {
    format!("MRL-ENV-161: the CLI could not use this computer's keychain ({e})")
}

impl Credentials for Keyring {
    fn get(&self, name: &str) -> Result<Option<String>, String> {
        let entry = keyring::Entry::new(KEYCHAIN_SERVICE, name).map_err(|e| keychain_error(&e))?;
        match entry.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(keychain_error(&e)),
        }
    }

    fn set(&self, name: &str, value: &str) -> Result<(), String> {
        keyring::Entry::new(KEYCHAIN_SERVICE, name)
            .and_then(|e| e.set_password(value))
            .map_err(|e| keychain_error(&e))
    }

    fn delete(&self, name: &str) -> Result<(), String> {
        let entry = keyring::Entry::new(KEYCHAIN_SERVICE, name).map_err(|e| keychain_error(&e))?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(keychain_error(&e)),
        }
    }
}

fn token_key(server: &str) -> String {
    format!("token {server}")
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// A connection to one core.
pub struct Client {
    /// Base address, like `http://127.0.0.1:53111`.
    pub server: String,
    token: Option<String>,
    launch_secret: Option<String>,
    http: Http,
}

impl Client {
    /// The core named by `server`, `MRL_SERVER` or the last login, with the saved credentials.
    pub fn connect(server: Option<&str>, store: &dyn Credentials) -> Result<Self, String> {
        let server = match server.map(str::to_owned).or_else(|| env("MRL_SERVER")) {
            Some(s) => s,
            None => store.get(SERVER_KEY)?.ok_or(
                "tell mrl which server to use: run `mrl login --server http://127.0.0.1:<port>` \
                 (the desktop app shows the address in Settings)",
            )?,
        };
        let server = server.trim_end_matches('/').to_owned();
        if !(server.starts_with("http://") || server.starts_with("https://")) {
            return Err(format!(
                "{server:?} is not a server address; it must start with http:// or https://"
            ));
        }
        let token = match env("MRL_TOKEN") {
            Some(t) => Some(t),
            None => store.get(&token_key(&server)).unwrap_or(None),
        };
        let http = Http::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| format!("could not set up HTTP: {e}"))?;
        Ok(Self {
            server,
            token,
            launch_secret: env("MRL_LAUNCH_SECRET"),
            http,
        })
    }

    fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, String> {
        let mut req = self
            .http
            .request(method, format!("{}/api/v1{path}", self.server));
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        if let Some(s) = &self.launch_secret {
            req = req.header("x-mrl-launch-secret", s);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        let res = req.send().map_err(|e| {
            format!(
                "could not reach the core at {}: {}",
                self.server,
                e.without_url()
            )
        })?;
        let status = res.status();
        let text = res.text().unwrap_or_default();
        if status.is_success() {
            return Ok(serde_json::from_str(&text).unwrap_or(Value::Null));
        }
        Err(problem_text(
            status.as_u16(),
            &text,
            self.token.is_some() || self.launch_secret.is_some(),
        ))
    }

    /// `GET /api/v1{path}`.
    pub fn get(&self, path: &str) -> Result<Value, String> {
        self.send(reqwest::Method::GET, path, None)
    }

    /// `POST /api/v1{path}` with a JSON body.
    pub fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        self.send(reqwest::Method::POST, path, Some(body))
    }
}

/// A plain-language message for an error answer (RFC 9457 problem or anything else).
pub fn problem_text(status: u16, body: &str, had_credentials: bool) -> String {
    let p: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let field = |k: &str| {
        p.get(k)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let mut out = match (field("title"), field("code")) {
        (t, c) if !t.is_empty() => format!("{t} ({c})"),
        _ => format!("the core answered with status {status}"),
    };
    for line in std::iter::once(field("detail"))
        .chain(
            p.get("errors")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|e| {
                    let get = |k: &str| e.get(k).and_then(Value::as_str).unwrap_or_default();
                    format!("{}: {}", get("path"), get("message"))
                }),
        )
        .filter(|l| !l.is_empty())
    {
        out.push_str("\n  ");
        out.push_str(&line);
    }
    if status == 401 && !had_credentials {
        out.push_str("\n  Run `mrl login` first. On a solo core, also set MRL_LAUNCH_SECRET.");
    }
    let trace = field("trace_id");
    if !trace.is_empty() {
        out.push_str(&format!("\n  trace id: {trace}"));
    }
    out
}

/// `mrl login`.
#[derive(Debug, clap::Args)]
pub struct LoginArgs {
    /// Core address, like http://127.0.0.1:53111 (saved for later commands).
    #[arg(long)]
    pub server: Option<String>,
    /// Sign in with `github` or `google`.
    #[arg(long, default_value = "github", value_parser = ["github", "google"])]
    pub provider: String,
    /// Print the sign-in address instead of opening the browser.
    #[arg(long)]
    pub no_browser: bool,
}

/// `mrl logout`.
#[derive(Debug, clap::Args)]
pub struct LogoutArgs {
    /// Core address (default: the last one you signed in to).
    #[arg(long)]
    pub server: Option<String>,
}

/// Opens `url` in the system browser.
pub fn open_browser(url: &str) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    let mut cmd = std::process::Command::new("open");
    #[cfg(target_os = "windows")]
    let mut cmd = {
        // `start` would read the `&` in the address as a command separator.
        let mut c = std::process::Command::new("rundll32");
        c.arg("url.dll,FileProtocolHandler");
        c
    };
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let mut cmd = std::process::Command::new("xdg-open");
    cmd.arg(url)
        .status()
        .map_err(|e| e.to_string())
        .and_then(|s| {
            if s.success() {
                Ok(())
            } else {
                Err(format!("it exited with {s}"))
            }
        })
}

/// Waits for the browser to come back to `listener` with `?flow=…&code=…`.
pub fn wait_for_callback(
    listener: &TcpListener,
    deadline: Instant,
) -> Result<(String, String), String> {
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("could not wait for the browser: {e}"))?;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Some(result) = answer(stream)? {
                    return result;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err("MRL-AUTH-161: the sign-in did not finish within 5 minutes; run `mrl login` again".into());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => return Err(format!("could not wait for the browser: {e}")),
        }
    }
}

type Callback = Result<(String, String), String>;

/// Reads one request. `None` for requests that are not the callback (like a favicon).
fn answer(mut stream: TcpStream) -> Result<Option<Callback>, String> {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut line = String::new();
    BufReader::new(std::io::Read::take(&stream, 8192))
        .read_line(&mut line)
        .map_err(|e| format!("could not read the browser's answer: {e}"))?;
    let target = line.split_whitespace().nth(1).unwrap_or_default();
    let url = reqwest::Url::parse(&format!("http://127.0.0.1{target}")).ok();
    let Some(url) = url.filter(|u| u.path() == "/callback") else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return Ok(None);
    };
    let q: std::collections::HashMap<String, String> = url.query_pairs().into_owned().collect();
    let (page, result) = match (
        q.get("flow"),
        q.get("code"),
        q.get("error_description").or(q.get("error")),
    ) {
        (Some(f), Some(c), None) => (
            "You are signed in. You can close this tab and go back to the terminal.",
            Ok((f.clone(), c.clone())),
        ),
        (_, _, Some(e)) => (
            "The sign-in did not finish. Go back to the terminal for details.",
            Err(format!("MRL-AUTH-161: the sign-in service said: {e}")),
        ),
        _ => (
            "The sign-in did not finish. Go back to the terminal for details.",
            Err("MRL-AUTH-161: the browser came back without a sign-in code".to_owned()),
        ),
    };
    let body =
        format!("<!doctype html><meta charset=\"utf-8\"><title>MasterRL</title><p>{page}</p>");
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    Ok(Some(result))
}

/// `mrl login`: signs in through the browser and keeps an access token in the keychain.
pub fn login(
    args: &LoginArgs,
    store: &dyn Credentials,
    open: &dyn Fn(&str) -> Result<(), String>,
) -> Result<(), String> {
    let client = Client::connect(args.server.as_deref(), store)?;
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("could not open a local port for the sign-in: {e}"))?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let started = client.post(
        "/auth/login",
        &json!({ "provider": args.provider, "redirect_uri": format!("http://127.0.0.1:{port}/callback") }),
    )?;
    let url = started["authorize_url"]
        .as_str()
        .ok_or("the core sent no sign-in address")?;
    let flow_id = started["flow_id"].as_str().unwrap_or_default().to_owned();
    if args.no_browser {
        println!("Open this address in your browser to sign in:\n  {url}");
    } else {
        println!("Opening your browser to sign in. If it does not open, visit:\n  {url}");
        if let Err(e) = open(url) {
            println!("(The browser did not open: {e}.)");
        }
    }
    let (flow, code) = wait_for_callback(&listener, Instant::now() + LOGIN_WAIT)?;
    if flow != flow_id {
        return Err(
            "MRL-AUTH-161: the browser finished a different sign-in; run `mrl login` again".into(),
        );
    }
    let done = client.post(
        "/auth/callback",
        &json!({ "flow_id": flow, "code": code, "issue_token": true, "token_name": "mrl CLI" }),
    )?;
    let token = done["token"]["token"]
        .as_str()
        .ok_or("the core sent no access token")?;
    let who = done["user"]["email"]
        .as_str()
        .or(done["user"]["display_name"].as_str())
        .unwrap_or("your account");
    store.set(SERVER_KEY, &client.server)?;
    if let Err(e) = store.set(&token_key(&client.server), token) {
        println!("Signed in as {who}, but the token could not be saved: {e}.");
        println!("Set this variable to use it (keep it secret):\n  MRL_TOKEN={token}");
        return Ok(());
    }
    println!("Signed in as {who}.");
    Ok(())
}

/// `mrl logout`: ends the sign-in on the core and forgets the token here.
pub fn logout(args: &LogoutArgs, store: &dyn Credentials) -> Result<(), String> {
    let client = Client::connect(args.server.as_deref(), store)?;
    let result = client.post("/auth/logout", &json!({}));
    store.delete(&token_key(&client.server))?;
    match result {
        Ok(_) => println!("Signed out."),
        Err(e) => println!(
            "Forgot the token on this computer. The core did not confirm the sign-out:\n  {e}"
        ),
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// Credentials in memory.
    #[derive(Default)]
    pub struct Memory(pub RefCell<HashMap<String, String>>);

    impl Credentials for Memory {
        fn get(&self, name: &str) -> Result<Option<String>, String> {
            Ok(self.0.borrow().get(name).cloned())
        }
        fn set(&self, name: &str, value: &str) -> Result<(), String> {
            self.0
                .borrow_mut()
                .insert(name.to_owned(), value.to_owned());
            Ok(())
        }
        fn delete(&self, name: &str) -> Result<(), String> {
            self.0.borrow_mut().remove(name);
            Ok(())
        }
    }

    fn browser_hit(port: u16, path: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        write!(s, "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").unwrap();
        let mut out = String::new();
        std::io::Read::read_to_string(&mut s, &mut out).unwrap();
        out
    }

    #[test]
    fn callback_takes_the_code_and_ignores_other_paths() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || {
            let favicon = browser_hit(port, "/favicon.ico");
            let page = browser_hit(port, "/callback?flow=f1&code=c%2B1");
            (favicon, page)
        });
        let got = wait_for_callback(&listener, Instant::now() + Duration::from_secs(10)).unwrap();
        assert_eq!(got, ("f1".to_owned(), "c+1".to_owned()));
        let (favicon, page) = browser.join().unwrap();
        assert!(favicon.starts_with("HTTP/1.1 404"));
        assert!(page.starts_with("HTTP/1.1 200") && page.contains("You are signed in."));
    }

    #[test]
    fn callback_reports_a_refusal_and_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || {
            browser_hit(
                port,
                "/callback?error=access_denied&error_description=User+said+no",
            )
        });
        let err =
            wait_for_callback(&listener, Instant::now() + Duration::from_secs(10)).unwrap_err();
        assert!(
            err.contains("User said no") && err.contains("MRL-AUTH-161"),
            "{err}"
        );
        browser.join().unwrap();
        let err = wait_for_callback(&listener, Instant::now()).unwrap_err();
        assert!(err.contains("within 5 minutes"));
    }

    #[test]
    fn problems_read_as_plain_language() {
        let body = r#"{"title":"Some values in the request need a change.","code":"MRL-VAL-004","detail":"1 field(s) need a change.",
            "errors":[{"path":"/passphrase","message":"MRL-VAL-160: this passphrase does not open the saved setup"}],"trace_id":"abc"}"#;
        let text = problem_text(422, body, true);
        assert!(text.starts_with("Some values in the request need a change. (MRL-VAL-004)"));
        assert!(text.contains("/passphrase: MRL-VAL-160"));
        assert!(text.contains("trace id: abc"));
        assert!(problem_text(401, "", false).contains("mrl login"));
        assert_eq!(
            problem_text(502, "<html>", true),
            "the core answered with status 502"
        );
    }

    #[test]
    fn the_server_comes_from_the_flag_then_the_last_login() {
        let store = Memory::default();
        if std::env::var_os("MRL_SERVER").is_none() {
            let err = Client::connect(None, &store).err().unwrap_or_default();
            assert!(err.contains("mrl login --server"), "{err}");
        }
        assert!(Client::connect(Some("127.0.0.1:5"), &store).is_err());
        store.set(SERVER_KEY, "http://127.0.0.1:7/").unwrap();
        store
            .set(&token_key("http://127.0.0.1:7"), "mrl_pat_x")
            .unwrap();
        if std::env::var_os("MRL_SERVER").is_none() && std::env::var_os("MRL_TOKEN").is_none() {
            let c = Client::connect(None, &store).unwrap();
            assert_eq!(c.server, "http://127.0.0.1:7");
            assert_eq!(c.token.as_deref(), Some("mrl_pat_x"));
        }
        assert_eq!(
            Client::connect(Some("http://h:1"), &store).unwrap().server,
            "http://h:1"
        );
    }
}
