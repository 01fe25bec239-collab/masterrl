//! `mrl sync setup|push|restore|status`: end-to-end encrypted setup sync through your account.
//!
//! The passphrase comes from `MRL_SYNC_PASSPHRASE`, one line on stdin (`--passphrase-stdin`),
//! or a hidden prompt.

use crate::auth::{Client, Credentials};
use serde_json::{json, Value};
use std::io::BufRead;

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
            let done = push_once(&|body| client.post("/sync/push", body))?;
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

/// One push, retried up to twice when the core or the sync service did not answer. Every try
/// carries the same `op_id`, so a push the service saved before its reply was lost is not
/// saved a second time (which would push an older copy out of the 5 kept).
fn push_once(post: &dyn Fn(&Value) -> Result<Value, String>) -> Result<Value, String> {
    let body = json!({ "op_id": uuid::Uuid::now_v7().to_string() });
    let transient = |e: &str| e.contains("MRL-LINK-160") || e.contains("could not reach the core");
    let mut tries = 0;
    loop {
        match post(&body) {
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
        assert_eq!(push_once(&post).unwrap()["version"], 6);
        let ids = seen.into_inner();
        assert_eq!(ids.len(), 3);
        assert!(ids.iter().all(|i| i == &ids[0]));
        // Other failures are not retried.
        let calls = std::cell::Cell::new(0);
        let refuse = |_: &Value| {
            calls.set(calls.get() + 1);
            Err("MRL-STATE-160: not set up".to_owned())
        };
        assert!(push_once(&refuse).is_err());
        assert_eq!(calls.get(), 1);
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
