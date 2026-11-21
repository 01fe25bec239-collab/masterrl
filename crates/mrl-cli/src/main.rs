//! `mrl`, the MasterRL command-line client.
//!
//! Each part writes its subcommands in its own module and adds one line right below each of
//! its markers (module, subcommand, dispatch), so parts never edit the same lines.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

const NAME: &str = env!("CARGO_BIN_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
// `--exclude=*` skips all tags, so `--always` yields the commit SHA.
const GIT_SHA: &str = git_version::git_version!(
    args = ["--always", "--abbrev=12", "--dirty=-dirty", "--exclude=*"],
    fallback = "unknown"
);

// <mrl:cli-mods P16> P16 `mod` lines go right below this line.
mod auth;
mod sync;

// <mrl:cli-mods P17> P17 `mod` lines go right below this line.
mod api;
mod commands;
mod config_file;
mod seed;

/// The MasterRL command-line client.
#[derive(Debug, Parser)]
#[command(name = "mrl", disable_version_flag = true)]
struct Cli {
    /// Show the version and git commit.
    #[arg(short = 'V', long)]
    version: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Show the version and git commit.
    Version,
    // <mrl:cli-commands P16> P16 subcommands go right below this line.
    /// Sign in to your MasterRL account in the browser and save an access token.
    Login(crate::auth::LoginArgs),
    /// Sign out on this computer and forget the access token.
    Logout(crate::auth::LogoutArgs),
    /// Sync your setup through your account, end-to-end encrypted.
    Sync(crate::sync::SyncArgs),
    // <mrl:cli-commands P17> P17 subcommands go right below this line.
    /// Settings: server address, access token, default project.
    #[command(subcommand)]
    Config(commands::ConfigCommand),
    /// Projects.
    #[command(subcommand)]
    Projects(commands::ProjectsCommand),
    /// Experiments (groups of runs).
    #[command(subcommand)]
    Experiments(commands::ExperimentsCommand),
    /// Training and evaluation runs.
    #[command(subcommand)]
    Runs(commands::RunsCommand),
    /// Jobs (the scheduled work behind runs).
    #[command(subcommand)]
    Jobs(commands::JobsCommand),
    /// Developer tools.
    #[command(subcommand)]
    Dev(commands::DevCommand),
}

fn version_line() -> String {
    format!("{NAME} {VERSION} ({GIT_SHA})")
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.version {
        println!("{}", version_line());
        return ExitCode::SUCCESS;
    }
    let Some(command) = cli.command else {
        eprintln!("{NAME}: choose a command; run `{NAME} --help` to see them");
        return ExitCode::from(2);
    };
    match run(command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{NAME}: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Runs one subcommand. An error is a plain-language message for the user.
fn run(command: Command) -> Result<(), String> {
    match command {
        // <mrl:cli-dispatch P16> P16 match arms go right below this line.
        Command::Login(a) => {
            crate::auth::login(&a, &crate::auth::Keyring, &crate::auth::open_browser)
        }
        Command::Logout(a) => crate::auth::logout(&a, &crate::auth::Keyring),
        Command::Sync(a) => crate::sync::run(&a, &crate::auth::Keyring),

        // <mrl:cli-dispatch P17> P17 match arms go right below this line.
        Command::Config(c) => commands::config(c),
        Command::Projects(c) => commands::projects(c),
        Command::Experiments(c) => commands::experiments(c),
        Command::Runs(c) => commands::runs(c),
        Command::Jobs(c) => commands::jobs(c),
        Command::Dev(c) => commands::dev(c),
        Command::Version => {
            println!("{}", version_line());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{version_line, Cli, Command, GIT_SHA, NAME, VERSION};
    use clap::Parser;

    #[test]
    fn version_shows_name_version_and_sha() {
        assert_eq!(version_line(), format!("{NAME} {VERSION} ({GIT_SHA})"));
        assert!(!GIT_SHA.is_empty());
        assert!(Cli::try_parse_from(["mrl", "--version"]).unwrap().version);
        assert!(Cli::try_parse_from(["mrl", "-V"]).unwrap().version);
        let sub = Cli::try_parse_from(["mrl", "version"]).unwrap();
        assert!(matches!(sub.command, Some(Command::Version)));
    }

    #[test]
    fn unknown_options_fail_and_no_command_is_allowed_to_parse() {
        assert!(Cli::try_parse_from(["mrl", "--bogus"]).is_err());
        assert!(Cli::try_parse_from(["mrl"]).unwrap().command.is_none());
    }
}
