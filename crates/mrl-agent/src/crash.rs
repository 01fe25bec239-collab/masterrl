//! Local crash reports (Doc 22 §14): a panic hook that writes one redacted JSON report per
//! crash to this computer. Nothing is uploaded; the desktop app lists the reports and lets the
//! person copy one or open a GitHub issue with it.
//!
//! The same file serves the agent and the core (`mrl-server/src/crash.rs` includes it).
//!
//! A report keeps the program, version, OS, the panic's source line and the function names of
//! the stack. It never keeps quoted text, paths, addresses, e-mail addresses, URLs or anything
//! shaped like a key or token: [`redact`] replaces them before anything is written.
//!
//! Reports go to `$MRL_CRASH_DIR` (the desktop app sets it), else the per-account app data
//! folder plus `crash-reports`. The newest [`KEEP`] reports per program are kept.

use std::path::{Path, PathBuf};

/// Environment variable naming the report folder.
pub const DIR_ENV: &str = "MRL_CRASH_DIR";
/// Report format tag.
pub const SCHEMA: &str = "mrl-crash/v1";
/// Reports kept per program.
pub const KEEP: usize = 20;
/// Longest message kept, in characters.
const MAX_MESSAGE: usize = 2000;
/// Most stack frames kept.
const MAX_FRAMES: usize = 64;

/// Installs the hook for `program`. The previous hook (the standard message on stderr) still
/// runs afterwards.
pub fn install(program: &'static str, version: &'static str, commit: &'static str) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| (*s).to_owned())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_default();
        let location = info
            .location()
            .map(|l| format!("{}:{}", source_file(l.file()), l.line()))
            .unwrap_or_default();
        let thread = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_owned();
        let frames = frames(&std::backtrace::Backtrace::force_capture().to_string());
        let report = report(
            program, version, commit, &message, &location, &thread, &frames,
        );
        if let Some(dir) = folder() {
            // A crash report must never crash: errors only skip the report.
            let _ = write(&dir, program, &report);
        }
        previous(info);
    }));
}

/// The report folder: `$MRL_CRASH_DIR`, else the app data folder's `crash-reports`.
pub fn folder() -> Option<PathBuf> {
    if let Some(d) = std::env::var_os(DIR_ENV).filter(|v| !v.is_empty()) {
        return Some(PathBuf::from(d));
    }
    let env = |k: &str| {
        std::env::var_os(k)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    let base = if cfg!(windows) {
        env("LOCALAPPDATA").map(|d| d.join("MasterRL"))
    } else if cfg!(target_os = "macos") {
        env("HOME").map(|h| h.join("Library/Application Support/MasterRL"))
    } else {
        env("XDG_STATE_HOME")
            .or_else(|| env("HOME").map(|h| h.join(".local/state")))
            .map(|d| d.join("masterrl"))
    };
    base.map(|b| b.join("crash-reports"))
}

/// A source file as a report shows it: a path inside this workspace as is (it names code, not
/// the computer); anything absolute only by its file name.
fn source_file(file: &str) -> String {
    let relative = !file.starts_with('/')
        && !file.starts_with('\\')
        && !file.contains(':')
        && !file.starts_with('~');
    if relative && file.starts_with("crates/") {
        return file.replace('\\', "/");
    }
    file.rsplit(['/', '\\']).next().unwrap_or("").to_owned()
}

/// Function names from a captured backtrace (no addresses, no file lines).
fn frames(backtrace: &str) -> Vec<String> {
    backtrace
        .lines()
        .filter_map(|l| {
            let t = l.trim();
            let (n, name) = t.split_once(": ")?;
            n.chars().all(|c| c.is_ascii_digit()).then(|| redact(name))
        })
        .take(MAX_FRAMES)
        .collect()
}

/// The report document.
fn report(
    program: &str,
    version: &str,
    commit: &str,
    message: &str,
    location: &str,
    thread: &str,
    frames: &[String],
) -> serde_json::Value {
    let now = time::OffsetDateTime::now_utc();
    serde_json::json!({
        "schema": SCHEMA,
        "program": program,
        "version": version,
        "commit": commit,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "time": now.format(&time::format_description::well_known::Rfc3339).unwrap_or_default(),
        "thread": redact(thread),
        "message": redact(message),
        "location": location,
        "frames": frames,
    })
}

/// Writes `report` as `<program>-<unix ms>-<pid>.json` (private to this account), then keeps
/// only the newest [`KEEP`] of `program`.
fn write(dir: &Path, program: &str, report: &serde_json::Value) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let name = format!("{program}-{ms:013}-{}.json", std::process::id());
    let partial = dir.join(format!(".{name}.partial"));
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut f = options.open(&partial)?;
        std::io::Write::write_all(&mut f, report.to_string().as_bytes())?;
        f.sync_all()?;
    }
    let path = dir.join(&name);
    std::fs::rename(&partial, &path)?;
    let prefix = format!("{program}-");
    let mut mine: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix) && n.ends_with(".json"))
        })
        .collect();
    mine.sort();
    let extra = mine.len().saturating_sub(KEEP);
    for old in mine.into_iter().take(extra) {
        let _ = std::fs::remove_file(old);
    }
    Ok(path)
}

/// Prefixes of keys and tokens (lower case).
const SECRET_PREFIXES: [&str; 12] = [
    "mrl_pat_",
    "mrlj_",
    "hf_",
    "sk-",
    "sk_",
    "ghp_",
    "gho_",
    "github_pat_",
    "xox",
    "akia",
    "eyj",
    "glpat-",
];
/// Words that announce a secret in the next word, or in `name=value`.
const SECRET_WORDS: [&str; 9] = [
    "bearer",
    "basic",
    "password",
    "passphrase",
    "secret",
    "token",
    "apikey",
    "api_key",
    "authorization",
];

/// Replaces everything that could be a person's data with a placeholder: quoted text, paths,
/// URLs, e-mail addresses, keys and tokens, long random-looking strings and PEM blocks. Also
/// cuts the text to 2,000 characters.
pub fn redact(text: &str) -> String {
    let text = match text.find("-----BEGIN") {
        Some(i) => format!("{}<secret>", &text[..i]),
        None => text.to_owned(),
    };
    let unquoted = strip_quotes(&text);
    let mut out = String::with_capacity(unquoted.len());
    let mut announce = false;
    for piece in unquoted.split_inclusive(char::is_whitespace) {
        let word = piece.trim_end();
        let space = &piece[word.len()..];
        let (lead, core, trail) = trim_punct(word);
        let replaced = if core.is_empty() {
            None
        } else if announce {
            Some("<secret>".to_owned())
        } else {
            classify(core)
        };
        let lower = core.to_ascii_lowercase();
        announce = SECRET_WORDS.contains(&lower.trim_end_matches(':'));
        match replaced {
            Some(r) => {
                out.push_str(lead);
                out.push_str(&r);
                out.push_str(trail);
            }
            None => out.push_str(word),
        }
        out.push_str(space);
    }
    if out.chars().count() > MAX_MESSAGE {
        out = out.chars().take(MAX_MESSAGE).collect::<String>() + "…";
    }
    out
}

/// `"…"`, `'…'` and `` `…` `` become `"<text>"`; an unclosed quote hides the rest.
fn strip_quotes(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if matches!(c, '"' | '`') || (c == '\'' && !out.ends_with(char::is_alphanumeric)) {
            let mut escaped = false;
            for d in chars.by_ref() {
                if escaped {
                    escaped = false;
                } else if d == '\\' {
                    escaped = true;
                } else if d == c {
                    break;
                }
            }
            out.push_str("\"<text>\"");
        } else {
            out.push(c);
        }
    }
    out
}

/// Splits leading and trailing punctuation off a word.
fn trim_punct(word: &str) -> (&str, &str, &str) {
    let punct = |c: char| matches!(c, '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';' | '<' | '>');
    let start = word.len() - word.trim_start_matches(punct).len();
    let rest = &word[start..];
    let core = rest.trim_end_matches(|c: char| punct(c) || c == '.' || c == ':');
    (&word[..start], core, &rest[core.len()..])
}

/// The placeholder for one word, or `None` to keep it.
fn classify(word: &str) -> Option<String> {
    if word == "\"<text>\"" {
        return None;
    }
    let lower = word.to_ascii_lowercase();
    if lower.contains("://") {
        return Some("<url>".into());
    }
    if let Some((key, _)) = word.split_once('=') {
        let k = key.to_ascii_lowercase();
        if SECRET_WORDS.iter().any(|w| k.contains(w)) || k.ends_with("key") {
            return Some(format!("{key}=<secret>"));
        }
    }
    if SECRET_PREFIXES.iter().any(|p| lower.starts_with(p)) && word.len() > 6 {
        return Some("<secret>".into());
    }
    if word.contains('/') || word.contains('\\') || word.starts_with('~') {
        return Some("<path>".into());
    }
    let b = word.as_bytes();
    if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' && b.len() > 2 {
        return Some("<path>".into());
    }
    if word.contains('@') && word.contains('.') {
        return Some("<email>".into());
    }
    if lower.starts_with("0x") && word.len() > 6 {
        return Some("<address>".into());
    }
    let random_looking = word.len() >= 20
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '=' | '_' | '-'))
        && word.chars().any(|c| c.is_ascii_digit())
        && !word.contains("::");
    if random_looking {
        return Some("<redacted>".into());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_keeps_the_shape_and_drops_data() {
        let cases = [
            (
                r#"called `Result::unwrap()` on an `Err` value: Io("prompt: tell me a secret")"#,
                "called \"<text>\" on an \"<text>\" value: Io(\"<text>\")",
            ),
            (
                "could not open /Users/ada/data/train.jsonl",
                "could not open <path>",
            ),
            (r"failed C:\Users\ada\models\x.bin", "failed <path>"),
            ("read ~/secret.txt now", "read <path> now"),
            (
                "token mrl_pat_abcdefghijklmnop leaked",
                "token <secret> leaked",
            ),
            (
                "Authorization: Bearer abc.def.ghi",
                "Authorization: <secret> <secret>",
            ),
            (
                "got hf_abcdefABCDEF123 and sk-proj-1234",
                "got <secret> and <secret>",
            ),
            ("mail ada@example.com", "mail <email>"),
            (
                "GET https://hub.example/x?token=1 failed",
                "GET <url> failed",
            ),
            (
                "password=hunter2 api_key=xyz",
                "password=<secret> api_key=<secret>",
            ),
            (
                "id 7f3a9c2b1d4e5f60718293a4b5c6d7e8 bad",
                "id <redacted> bad",
            ),
            ("key -----BEGIN PRIVATE KEY-----\nMIIE", "key <secret>"),
            ("at 0x7ff8a1b2c3d4", "at <address>"),
            (
                "index out of bounds: the len is 3 but the index is 7",
                "index out of bounds: the len is 3 but the index is 7",
            ),
        ];
        for (input, want) in cases {
            assert_eq!(redact(input), want, "input: {input}");
        }
    }

    #[test]
    fn a_panic_writes_one_redacted_report() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        // Only this test sets the variable; the binary's other tests never panic on purpose.
        std::env::set_var(DIR_ENV, dir.path());
        install("mrl-hooktest", "9.9.9", "abc123");
        let crashed = std::thread::Builder::new()
            .name("worker".into())
            .spawn(|| panic!("lost /Users/ada/run.jsonl with hf_secretvalue123"))
            .unwrap_or_else(|e| panic!("{e}"))
            .join();
        let _ = std::panic::take_hook();
        assert!(crashed.is_err());
        let files: Vec<PathBuf> = std::fs::read_dir(dir.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().contains("mrl-hooktest-"))
            .collect();
        assert_eq!(files.len(), 1);
        let text = std::fs::read_to_string(&files[0]).unwrap_or_else(|e| panic!("{e}"));
        let doc: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(doc["message"], "lost <path> with <secret>");
        assert_eq!(
            (doc["program"].as_str(), doc["thread"].as_str()),
            (Some("mrl-hooktest"), Some("worker"))
        );
        assert!(!text.contains("ada") && !text.contains("secretvalue"));
    }

    #[test]
    fn long_messages_are_cut() {
        let long = "word ".repeat(1000);
        assert!(redact(&long).chars().count() <= MAX_MESSAGE + 1);
    }

    #[test]
    fn frames_keep_function_names_only() {
        let bt = "   0: std::backtrace::Backtrace::create\n             at /rustc/abc/library/std/src/backtrace.rs:331:13\n   1: mrl_agent::run\n             at /Users/ada/src/masterrl/crates/mrl-agent/src/main.rs:12:5\n";
        assert_eq!(
            frames(bt),
            ["std::backtrace::Backtrace::create", "mrl_agent::run"]
        );
    }

    #[test]
    fn source_files_name_code_not_the_computer() {
        assert_eq!(
            source_file("crates/mrl-agent/src/main.rs"),
            "crates/mrl-agent/src/main.rs"
        );
        assert_eq!(
            source_file("/Users/ada/.cargo/registry/src/x/tokio-1.0/src/rt.rs"),
            "rt.rs"
        );
        assert_eq!(source_file(r"C:\Users\ada\x\lib.rs"), "lib.rs");
    }

    #[test]
    fn a_report_is_written_privately_and_old_ones_go() {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let doc = report(
            "mrl-test",
            "1.0.0",
            "abc",
            "boom at /home/ada/x with sk-12345678",
            "crates/mrl-agent/src/crash.rs:1",
            "main",
            &[],
        );
        let text = doc.to_string();
        assert!(!text.contains("/home/ada") && !text.contains("sk-12345678"));
        assert_eq!(doc["schema"], SCHEMA);
        let mut last = PathBuf::new();
        for _ in 0..KEEP + 3 {
            last = write(dir.path(), "mrl-test", &doc).unwrap_or_else(|e| panic!("{e}"));
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let left: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap_or_else(|e| panic!("{e}"))
            .filter_map(Result::ok)
            .collect();
        assert_eq!(left.len(), KEEP);
        assert!(last.exists(), "the newest report stays");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&last)
                .unwrap_or_else(|e| panic!("{e}"))
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0, "only this account can read reports");
        }
    }
}
