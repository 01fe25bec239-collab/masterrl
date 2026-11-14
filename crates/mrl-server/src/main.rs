//! The MasterRL core: API, scheduler, registries and events. Run by the desktop app.

use std::process::ExitCode;

const NAME: &str = env!("CARGO_BIN_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
// `--exclude=*` skips all tags, so `--always` yields the commit SHA.
const GIT_SHA: &str = git_version::git_version!(
    args = ["--always", "--abbrev=12", "--dirty=-dirty", "--exclude=*"],
    fallback = "unknown"
);

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args_os()
        .skip(1)
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match reply(&args) {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(text) => {
            eprintln!("{text}");
            ExitCode::from(2)
        }
    }
}

// ponytail: same ~20 lines in each binary; move to a shared crate when a real CLI parser arrives.
fn reply(args: &[&str]) -> Result<String, String> {
    match args {
        ["--version" | "-V"] => Ok(format!("{NAME} {VERSION} ({GIT_SHA})")),
        ["--help" | "-h"] => Ok(usage()),
        [] => Err(usage()),
        _ => Err(format!(
            "{NAME}: '{}' is not an option this program knows.\n\n{}",
            args.join(" "),
            usage()
        )),
    }
}

fn usage() -> String {
    format!("Usage: {NAME} [--version | --help]\n\n  -V, --version  Show the version and git commit\n  -h, --help     Show this help")
}

#[cfg(test)]
mod tests {
    use super::{reply, GIT_SHA, NAME, VERSION};

    #[test]
    fn version_shows_name_version_and_sha() {
        let out = reply(&["--version"]).expect("--version succeeds");
        assert_eq!(out, format!("{NAME} {VERSION} ({GIT_SHA})"));
        assert!(!GIT_SHA.is_empty());
        assert_eq!(reply(&["-V"]), Ok(out));
    }

    #[test]
    fn help_lists_options() {
        let out = reply(&["--help"]).expect("--help succeeds");
        assert!(out.contains("--version") && out.contains("--help"));
    }

    #[test]
    fn unknown_or_missing_options_fail_with_usage() {
        let err = reply(&["--bogus"]).expect_err("unknown option fails");
        assert!(err.contains("'--bogus' is not an option"));
        assert!(err.contains("Usage:"));
        assert!(reply(&[]).is_err());
        assert!(reply(&["--version", "--help"]).is_err());
    }
}
