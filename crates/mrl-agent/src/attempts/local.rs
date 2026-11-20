//! `mrl-agent attempt run --spec spec.json`: runs one attempt on this machine without a server,
//! for debugging adapters and the attempt runtime.
//!
//! Inputs come from local files (`inputs[].file`) or links; committed checkpoints are copied to
//! `upload_dir`. Events are printed as they happen. Ctrl-C cancels the attempt (the worker gets
//! its grace period); SIGTERM leaves the workers running, and running the same command again
//! re-adopts them (as does killing this process outright). When the attempt ends its scratch is
//! wiped unless `--keep` is given.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use mrl_proto::worker::v1::worker_message::Body;

use super::record::AttemptRecord;
use super::{AttemptError, AttemptEvent, AttemptSpec, Attempts};
use crate::config::AgentConfig;
use crate::AgentContext;

/// `mrl-agent attempt …` subcommands.
#[derive(Debug, clap::Subcommand)]
pub enum AttemptCommand {
    /// Run one attempt here, or re-adopt it if its workers are still running.
    Run {
        /// The attempt spec (JSON). Relative project paths are relative to this file.
        #[arg(long)]
        spec: PathBuf,
        /// Keep the attempt's work, staging and inputs folders when it ends.
        #[arg(long)]
        keep: bool,
        /// Print the outcome as JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Runs an `attempt` subcommand; errors are printed with their code.
pub async fn cli(config: Option<&Path>, cmd: AttemptCommand) -> ExitCode {
    match run_cli(config, cmd).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("mrl-agent: {}", e.explain());
            ExitCode::FAILURE
        }
    }
}

/// Reads a spec file; relative `runtimes[].project`, `inputs[].file` and `upload_dir` are
/// resolved against the file's folder.
pub fn read_spec(path: &Path) -> Result<AttemptSpec, AttemptError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| AttemptError::Spec(format!("{} can't be read ({e})", path.display())))?;
    let mut spec = AttemptSpec::from_json(&text)?;
    let base = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let base = std::fs::canonicalize(&base).unwrap_or(base);
    let abs = |p: &mut PathBuf| {
        if p.is_relative() {
            *p = base.join(&*p);
        }
    };
    for rt in &mut spec.runtimes {
        if let Some(p) = &mut rt.project {
            abs(p);
        }
    }
    for i in &mut spec.inputs {
        if let Some(p) = &mut i.file {
            abs(p);
        }
    }
    if let Some(p) = &mut spec.upload_dir {
        abs(p);
    }
    Ok(spec)
}

fn describe(e: &AttemptEvent) -> Option<String> {
    match e {
        AttemptEvent::Status(s) => {
            let phase =
                mrl_proto::agent::v1::AttemptPhase::try_from(s.phase).map_or("?".into(), |p| {
                    p.as_str_name()
                        .trim_start_matches("ATTEMPT_PHASE_")
                        .to_lowercase()
                });
            Some(format!("phase {phase} (step {})", s.last_step))
        }
        AttemptEvent::Relay { relay, .. } => match relay.message.as_ref()?.body.as_ref()? {
            Body::Prepared(p) => Some(format!("prepared ok={}", p.ok)),
            Body::Progress(p) => Some(format!("step {}/{}", p.step, p.total_steps)),
            Body::CheckpointStaged(c) => Some(format!(
                "checkpoint {} staged at step {}",
                c.checkpoint_id, c.step
            )),
            Body::CheckpointCommit(c) => {
                Some(format!("checkpoint {} commit requested", c.checkpoint_id))
            }
            Body::Exit(r) => Some(format!(
                "worker exit report: {}",
                mrl_proto::worker::v1::ExitKind::try_from(r.kind).map_or("?", |k| k.as_str_name())
            )),
            _ => None,
        },
        AttemptEvent::CommandDone(r) => Some(format!("command {} done", r.command_id)),
        AttemptEvent::Probe(_) => None,
    }
}

async fn run_cli(config: Option<&Path>, cmd: AttemptCommand) -> Result<ExitCode, AttemptError> {
    let AttemptCommand::Run { spec, keep, json } = cmd;
    let spec = read_spec(&spec)?;
    let (cfg, _) = AgentConfig::load(config)?;
    let ctx = Arc::new(AgentContext::from_config(cfg)?);
    let (tx, mut rx) = super::attempt_events(256);
    let printer = tokio::spawn(async move {
        while let Some(e) = rx.recv().await {
            if let Some(line) = describe(&e) {
                eprintln!("mrl-agent: {line}");
            }
        }
    });
    let progress: Arc<crate::envs::ProgressFn> = Arc::new(|p: crate::envs::EnvProgress| {
        eprintln!("mrl-agent: environment {}: {}", p.stage, p.detail);
    });
    let attempts = Attempts::new(Arc::clone(&ctx), tx, Some(progress))?;
    let id = spec.attempt.attempt_id.clone();
    let job_dir = ctx.state.job_dir(&id);

    // Only this attempt: other attempts on the same state folder belong to whoever runs them.
    let readopted = attempts.readopt_where(|a| a == id).await;
    let mine = readopted
        .iter()
        .find(|s| s.attempt.as_ref().is_some_and(|a| a.attempt_id == id));
    match (AttemptRecord::load(&job_dir)?, mine) {
        (Some(rec), _) if rec.exit.is_some() => {
            eprintln!("mrl-agent: attempt {id} already ran here");
            return print_outcome(rec.exit.as_ref(), json);
        }
        (Some(_), Some(_)) => eprintln!("mrl-agent: re-adopted attempt {id}"),
        (Some(_), None) => {
            return Err(AttemptError::state(
                &id,
                "is run by another mrl-agent on this machine; stop that one first",
            ))
        }
        (None, _) => attempts.launch(spec)?,
    }

    // No telemetry service in a local run: feed GPU events to the attempt from here.
    let hw_attempts = Arc::clone(&attempts);
    let hw_ctx = Arc::clone(&ctx);
    let hw = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let events: Vec<crate::hw::HwEvent> =
                hw_ctx.hw.iter().flat_map(|b| b.drain_events()).collect();
            hw_attempts.hw_events(&events);
        }
    });

    let outcome = tokio::select! {
        d = attempts.wait(&id) => d,
        detach = signals(&attempts, &id) => {
            if detach {
                attempts.detach_all().await;
                eprintln!("mrl-agent: left attempt {id} running; run the same command again to re-adopt it");
                hw.abort();
                return Ok(ExitCode::SUCCESS);
            }
            attempts.wait(&id).await
        }
    };
    hw.abort();
    drop(attempts);
    let _ = tokio::time::timeout(Duration::from_secs(2), printer).await;
    if !keep {
        crate::janitor::purge(ctx.state.root(), &job_dir)?;
    }
    print_outcome(outcome.as_ref(), json)
}

/// Waits for Ctrl-C (cancel, then keep waiting) or SIGTERM (returns true: detach).
async fn signals(attempts: &Arc<Attempts>, id: &str) -> bool {
    #[cfg(unix)]
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    loop {
        #[cfg(unix)]
        let got_term = async {
            match term.as_mut() {
                Some(t) => {
                    t.recv().await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        #[cfg(not(unix))]
        let got_term = std::future::pending::<()>();
        tokio::select! {
            () = got_term => return true,
            r = tokio::signal::ctrl_c() => {
                if r.is_err() {
                    return std::future::pending().await;
                }
                eprintln!("mrl-agent: cancelling attempt {id} (press Ctrl-C again to wait)");
                let cmd = mrl_proto::agent::v1::Command {
                    command_id: "local-cancel".into(),
                    kind: Some(mrl_proto::agent::v1::command::Kind::CancelAttempt(
                        mrl_proto::agent::v1::CancelAttempt {
                            attempt: attempts
                                .running()
                                .into_iter()
                                .find(|r| r.attempt_id == id),
                            request_id: "local-cancel".into(),
                            grace_s: 30,
                            reason: "cancelled from the command line".into(),
                        },
                    )),
                };
                let _ = attempts.handle(&cmd).await;
            }
        }
    }
}

fn print_outcome(
    d: Option<&super::exit::ExitDecision>,
    json: bool,
) -> Result<ExitCode, AttemptError> {
    let Some(d) = d else {
        eprintln!("mrl-agent: the attempt stopped without an outcome");
        return Ok(ExitCode::FAILURE);
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(d).map_err(|e| AttemptError::Spec(e.to_string()))?
        );
    } else {
        let kind =
            mrl_proto::worker::v1::ExitKind::try_from(d.exit_kind).map_or("?", |k| k.as_str_name());
        match &d.failure {
            None => println!("Attempt ended: {kind}"),
            Some(f) => println!(
                "Attempt failed: {} {} — {}",
                mrl_proto::common::v1::FailureClass::try_from(f.class)
                    .map_or("?", |c| c.as_str_name()),
                f.sub_code,
                f.message
            ),
        }
    }
    Ok(if d.failure.is_none() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_paths_follow_the_spec_file() {
        let dir = tempfile::tempdir().unwrap();
        let sha = "a".repeat(64);
        let spec = serde_json::json!({
            "attempt": {"job_id": "job_l", "attempt_id": "att_l"},
            "engine": "fake",
            "job_spec": {},
            "runtimes": [{"runtime_ref": format!("uv:sha256:{sha}"), "project": "proj"}],
            "roles": [{"role_id": "trainer", "primary": true, "runtime_ref": format!("uv:sha256:{sha}")}],
            "upload_dir": "out"
        });
        let path = dir.path().join("spec.json");
        std::fs::write(&path, spec.to_string()).unwrap();
        let s = read_spec(&path).unwrap();
        let base = std::fs::canonicalize(dir.path()).unwrap();
        assert_eq!(
            s.runtimes[0].project.as_deref(),
            Some(base.join("proj").as_path())
        );
        assert_eq!(s.upload_dir.as_deref(), Some(base.join("out").as_path()));
    }
}
