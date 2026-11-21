//! `mrl sync setup|push|restore|status`: end-to-end encrypted setup sync through your account.
//!
//! The passphrase comes from `MRL_SYNC_PASSPHRASE`, one line on stdin (`--passphrase-stdin`),
//! or a hidden prompt.

use crate::auth::{Client, Credentials};
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

/// `mrl sync`.
#[derive(Debug, clap::Args)]
pub struct SyncArgs {
    /// Core address (default: the last one you signed in to).
    #[arg(long, global = true)]
    pub server: Option<String>,
    /// Read the passphrase (or recovery key) from the first line of stdin.
    #[arg(long, global = true)]
    pub passphrase_stdin: bool,
    #[command(subcommand)]
    pub command: SyncCommand,
}

/// What to do.
#[derive(Debug, clap::Subcommand)]
pub enum SyncCommand {
    /// Turn on sync: choose a passphrase and get a recovery key (shown once).
    Setup,
    /// Save this computer's setup to your account (the 5 newest copies are kept).
    Push,
    /// Bring a saved setup onto this computer.
    Restore {
        /// Restore an older copy (see `mrl sync status`).
        #[arg(long)]
        version: Option<i64>,
        /// Use the recovery key instead of the passphrase.
        #[arg(long)]
        recovery_key: bool,
    },
    /// Show whether sync is on, the last push and the saved copies.
    Status,
}

fn secret_input(prompt: &str, from_stdin: bool) -> Result<String, String> {
    if let Some(p) = std::env::var("MRL_SYNC_PASSPHRASE")
        .ok()
        .filter(|p| !p.is_empty())
    {
        return Ok(p);
    }
    let text = if from_stdin {
        let mut line = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut line)
            .map_err(|e| format!("could not read stdin: {e}"))?;
        line
    } else {
        rpassword::prompt_password(prompt)
            .map_err(|e| format!("could not read the passphrase: {e}"))?
    };
    Ok(text.trim_end_matches(['\r', '\n']).to_owned())
}

/// Runs one `mrl sync` command.
pub fn run(args: &SyncArgs, store: &dyn Credentials) -> Result<(), String> {
    let client = Client::connect(args.server.as_deref(), store)?;
    match &args.command {
        SyncCommand::Setup => {
            println!("Choose a sync passphrase (at least 12 characters).");
            println!("If you forget it and lose the recovery key, nobody can restore your setup.");
            let pass = secret_input("Passphrase: ", args.passphrase_stdin)?;
            if !args.passphrase_stdin && std::env::var_os("MRL_SYNC_PASSPHRASE").is_none() {
                let again = secret_input("Passphrase again: ", false)?;
                if again != pass {
                    return Err("the two passphrases differ; run `mrl sync setup` again".into());
                }
            }
            let done = client.post("/sync/setup", &json!({ "passphrase": pass }))?;
            println!("Sync is on. Your recovery key (shown only now, save it somewhere safe):");
            println!("  {}", done["recovery_key"].as_str().unwrap_or_default());
            println!("Run `mrl sync push` to save your setup.");
        }
        SyncCommand::Push => {
            let done = push_pending(&push_state_dir()?, &client.server, &|body| {
                client.post("/sync/push", body)
            })?;
            println!(
                "Saved copy {} of your setup ({} kept).",
                done["version"], done["copies"]
            );
        }
        SyncCommand::Restore {
            version,
            recovery_key,
        } => {
            let mut body = json!({ "version": version });
            if *recovery_key {
                body["recovery_key"] =
                    json!(secret_input("Recovery key: ", args.passphrase_stdin)?);
            } else {
                body["passphrase"] = json!(secret_input("Passphrase: ", args.passphrase_stdin)?);
            }
            let done = client.post("/sync/restore", &body)?;
            println!("{}", restored_text(&done));
        }
        SyncCommand::Status => println!("{}", status_text(&client.get("/sync/status")?)),
    }
    Ok(())
}

fn push_state_dir() -> Result<PathBuf, String> {
    let base = if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        std::env::home_dir().map(|home| home.join("Library/Application Support"))
    } else {
        std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::home_dir().map(|home| home.join(".local/state")))
    };
    base.map(|base| base.join("masterrl"))
        .ok_or_else(|| "could not locate an app folder for pending setup pushes".to_owned())
}

fn push_pending(
    dir: &Path,
    server: &str,
    post: &dyn Fn(&Value) -> Result<Value, String>,
) -> Result<Value, String> {
    let io = |e| format!("could not save the pending setup push: {e}");
    std::fs::create_dir_all(dir).map_err(io)?;
    let lock = std::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join("setup-push.lock"))
        .map_err(io)?;
    // Two CLI invocations must not overwrite the pending retry ID.
    lock.lock().map_err(io)?;
    let path = dir.join("setup-push.json");
    let temp = dir.join("setup-push.json.tmp");
    let mut pending: std::collections::BTreeMap<String, String> = match std::fs::read(&path) {
        Ok(raw) => serde_json::from_slice(&raw)
            .map_err(|_| "the pending setup push could not be read".to_owned())?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Default::default(),
        Err(e) => return Err(io(e)),
    };
    let save = |values: &std::collections::BTreeMap<String, String>| -> Result<(), String> {
        let bytes = serde_json::to_vec(values).map_err(|e| e.to_string())?;
        let mut file = std::fs::File::create(&temp).map_err(io)?;
        file.write_all(&bytes).map_err(io)?;
        file.sync_all().map_err(io)?;
        drop(file);
        std::fs::rename(&temp, &path).map_err(io)
    };
    let id = pending
        .entry(server.to_owned())
        .or_insert_with(|| uuid::Uuid::now_v7().to_string())
        .clone();
    save(&pending)?;
    let done = push_once(&id, post)?;
    pending.remove(server);
    save(&pending)?;
    Ok(done)
}

/// Retry a push with the same operation ID until a well-formed acknowledgement arrives.
fn push_once(id: &str, post: &dyn Fn(&Value) -> Result<Value, String>) -> Result<Value, String> {
    let body = json!({ "op_id": id });
    let transient = |e: &str| {
        e.contains("MRL-LINK-160")
            || e.starts_with("could not reach the core")
            || e.starts_with("could not read the core's reply")
    };
    let mut tries = 0;
    loop {
        let result = post(&body).and_then(|done| {
            if done["version"].as_i64().is_some_and(|v| v > 0)
                && done["copies"]
                    .as_u64()
                    .is_some_and(|n| (1..=5).contains(&n))
            {
                Ok(done)
            } else {
                Err("could not read the core's reply to the setup push".to_owned())
            }
        });
        match result {
            Err(e) if transient(&e) && tries < 2 => {
                tries += 1;
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            other => return other,
        }
    }
}

fn restored_text(done: &Value) -> String {
    let p = &done["profile"];
    let list = |k: &str| p[k].as_array().cloned().unwrap_or_default();
    let mut out = format!(
        "Restored copy {} (saved {}): {} machine(s), {} team(s).",
        done["version"],
        done["at"].as_str().unwrap_or("?"),
        list("machines").len(),
        list("team_memberships").len()
    );
    for m in list("machines") {
        out.push_str(&format!(
            "\n  {} at {} ({})",
            m["name"].as_str().unwrap_or("?"),
            m["address"].as_str().unwrap_or("?"),
            m["transport"]["kind"].as_str().unwrap_or("?")
        ));
    }
    out.push_str("\nAdd the machines' keys again: setup sync never stores secrets.");
    out
}

fn status_text(s: &Value) -> String {
    let mut out = format!(
        "Sync: {}\nSign-in: {}\nLast push: {}",
        if s["set_up"] == json!(true) {
            "on"
        } else {
            "off (run `mrl sync setup`)"
        },
        s["sign_in"].as_str().unwrap_or("?").replace('_', " "),
        match (
            s["last_push_at"].as_str(),
            s["last_pushed_version"].as_i64()
        ) {
            (Some(at), Some(v)) => format!("copy {v} at {at}"),
            _ => "never".to_owned(),
        }
    );
    match s["copies"].as_array() {
        Some(copies) if copies.is_empty() => out.push_str("\nSaved copies: none"),
        Some(copies) => {
            out.push_str("\nSaved copies:");
            for c in copies {
                out.push_str(&format!(
                    "\n  {}  {}",
                    c["version"],
                    c["at"].as_str().unwrap_or("?")
                ));
            }
        }
        None => out.push_str(&format!(
            "\nSaved copies: unknown ({})",
            s["copies_unavailable"].as_str().unwrap_or("not available")
        )),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retried_push_sends_the_same_op_id() {
        let seen = std::cell::RefCell::new(Vec::new());
        let post = |b: &Value| {
            seen.borrow_mut()
                .push(b["op_id"].as_str().unwrap().to_owned());
            if seen.borrow().len() < 3 {
                Err("MRL-LINK-160: the sync service did not answer".to_owned())
            } else {
                Ok(json!({"version": 6, "copies": 5}))
            }
        };
        assert_eq!(push_once("operation", &post).unwrap()["version"], 6);
        let ids = seen.into_inner();
        assert_eq!(ids.len(), 3);
        assert!(ids.iter().all(|i| i == &ids[0]));
        // Other failures are not retried.
        let calls = std::cell::Cell::new(0);
        let refuse = |_: &Value| {
            calls.set(calls.get() + 1);
            Err("MRL-STATE-160: not set up".to_owned())
        };
        assert!(push_once("operation", &refuse).is_err());
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn an_unconfirmed_push_reuses_its_operation_after_reopening_the_journal() {
        let dir = std::env::temp_dir().join(format!("mrl-push-test-{}", uuid::Uuid::new_v4()));
        let first_id = std::cell::RefCell::new(None);
        let first = push_pending(&dir, "http://127.0.0.1:8080", &|body| {
            first_id.replace(body["op_id"].as_str().map(str::to_owned));
            Err("the core did not confirm the push".to_owned())
        });
        assert!(first.is_err());
        push_pending(&dir, "http://127.0.0.1:8080", &|body| {
            assert_eq!(body["op_id"].as_str(), first_id.borrow().as_deref());
            Ok(json!({"version":1,"copies":1}))
        })
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("setup-push.json")).unwrap(),
            "{}"
        );
        std::fs::remove_dir_all(dir).unwrap();
        assert!(push_once("op", &|_| Ok(Value::Null)).is_err());
    }

    #[test]
    fn restore_and_status_read_plainly() {
        let done = json!({"version": 2, "at": "2026-11-16T10:00:00Z", "profile": {
            "machines": [{"name": "gpu-box", "address": "10.0.0.7", "transport": {"kind": "ssh"}}], "team_memberships": []}});
        let text = restored_text(&done);
        assert!(text
            .starts_with("Restored copy 2 (saved 2026-11-16T10:00:00Z): 1 machine(s), 0 team(s)."));
        assert!(text.contains("gpu-box at 10.0.0.7 (ssh)"));
        let s = status_text(
            &json!({"set_up": true, "sign_in": "active", "last_push_at": "t", "last_pushed_version": 3,
            "copies": [{"version": 3, "at": "t"}]}),
        );
        assert!(s.contains("Sync: on") && s.contains("copy 3 at t") && s.contains("\n  3  t"));
        let offline = status_text(
            &json!({"set_up": false, "sign_in": "signed_out", "copies": null, "copies_unavailable": "Sign in first."}),
        );
        assert!(
            offline.contains("off (run")
                && offline.contains("signed out")
                && offline.contains("unknown (Sign in first.)")
        );
    }
}
