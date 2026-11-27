//! P17's subcommands: `config`, `projects`, `experiments`, `runs`, `jobs` and `dev seed`
//! (`runs validate`, `--preset` and `--dry-run` are P28's).

use crate::api::Api;
use crate::config_file::{self, CliConfig};
use clap::{Args, Subcommand};
use serde_json::{json, Value};
use std::io::BufRead;
use std::path::PathBuf;

/// `mrl config …`
#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Save a personal access token (asks for it when not given).
    SetToken {
        /// The token; leave it out to type it in, so it stays out of your shell history.
        token: Option<String>,
    },
    /// Save the server address, like `https://mrl.example.com`.
    SetServer {
        /// The address.
        url: String,
    },
    /// Save the project commands use when `--project` is not given.
    SetProject {
        /// Project id, `prj_…`.
        project: String,
    },
    /// Show the settings in use (the token is hidden).
    Show,
}

/// Output options shared by the commands that print items.
#[derive(Debug, Args)]
pub struct Output {
    /// Print the server's JSON instead of a summary.
    #[arg(long)]
    pub json: bool,
}

/// Paging options.
#[derive(Debug, Args)]
pub struct Paging {
    /// Items per page, 1–200.
    #[arg(long)]
    pub limit: Option<u32>,
    /// `next_cursor` from the previous page.
    #[arg(long)]
    pub cursor: Option<String>,
}

/// `mrl projects …`
#[derive(Debug, Subcommand)]
pub enum ProjectsCommand {
    /// List the projects of a workspace.
    List {
        /// Workspace id, `ws_…`.
        #[arg(long)]
        workspace: String,
        #[command(flatten)]
        paging: Paging,
        #[command(flatten)]
        out: Output,
    },
}

/// `mrl experiments …`
#[derive(Debug, Subcommand)]
pub enum ExperimentsCommand {
    /// List a project's experiments.
    List {
        /// Project id (default: the saved project).
        #[arg(long)]
        project: Option<String>,
        #[command(flatten)]
        paging: Paging,
        #[command(flatten)]
        out: Output,
    },
    /// Create an experiment.
    Create {
        /// Its name, unique in the project.
        name: String,
        /// Project id (default: the saved project).
        #[arg(long)]
        project: Option<String>,
        /// What it is for.
        #[arg(long)]
        description: Option<String>,
    },
}

/// `mrl runs …`
#[derive(Debug, Subcommand)]
pub enum RunsCommand {
    /// Submit a run from a JSON file and/or options.
    Submit(Box<SubmitArgs>),
    /// Check a run without submitting it: settings, a dry run on a connected machine, the
    /// memory estimate and whether it fits (same options as `submit`).
    Validate(Box<SubmitArgs>),
    /// List a project's runs, newest first.
    List {
        /// Project id (default: the saved project).
        #[arg(long)]
        project: Option<String>,
        /// Only runs in this state, like `QUEUED`.
        #[arg(long)]
        state: Option<String>,
        /// Only runs in this experiment, `exp_…`.
        #[arg(long)]
        experiment: Option<String>,
        #[command(flatten)]
        paging: Paging,
        #[command(flatten)]
        out: Output,
    },
    /// Show one run.
    Get {
        /// Run id, `run_…`.
        run: String,
        #[command(flatten)]
        out: Output,
    },
    /// Cancel a run.
    Cancel {
        /// Run id, `run_…`.
        run: String,
        #[command(flatten)]
        out: Output,
    },
}

/// `mrl runs submit` options. Options override the same fields of `--file`.
#[derive(Debug, Args)]
pub struct SubmitArgs {
    /// A JSON file with the run (see the API's `RunSubmission`).
    #[arg(long)]
    pub file: Option<PathBuf>,
    /// Project id (default: the saved project).
    #[arg(long)]
    pub project: Option<String>,
    /// Run name.
    #[arg(long)]
    pub name: Option<String>,
    /// Method id, like `sft`.
    #[arg(long)]
    pub algorithm: Option<String>,
    /// Base model version, `mv_…`.
    #[arg(long)]
    pub model: Option<String>,
    /// Dataset version, `dsv_…` (repeat for more).
    #[arg(long)]
    pub dataset: Vec<String>,
    /// Experiment, `exp_…` (default: the project's `default` experiment).
    #[arg(long)]
    pub experiment: Option<String>,
    /// Request key; reuse one to retry a submit safely (default: a new random key).
    #[arg(long)]
    pub key: Option<String>,
    /// Start from one of the method's presets, like `sft-lora-beginner` (sets `config.preset`).
    #[arg(long)]
    pub preset: Option<String>,
    /// Check the run like `mrl runs validate` and submit nothing.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub out: Output,
}

/// `mrl jobs …`
#[derive(Debug, Subcommand)]
pub enum JobsCommand {
    /// List jobs, newest first.
    List {
        /// Only jobs in this state, like `RUNNING`.
        #[arg(long)]
        state: Option<String>,
        /// Only jobs in this project, `prj_…`.
        #[arg(long)]
        project: Option<String>,
        #[command(flatten)]
        paging: Paging,
        #[command(flatten)]
        out: Output,
    },
}

/// `mrl dev …`
#[derive(Debug, Subcommand)]
pub enum DevCommand {
    /// Set up org `dev`, workspace `default` and project `demo` (safe to run again).
    Seed,
}

fn connect() -> Result<(CliConfig, Api), String> {
    let cfg = CliConfig::load()?;
    let api = Api::connect(&cfg)?;
    Ok((cfg, api))
}

fn project(cfg: &CliConfig, given: Option<String>) -> Result<String, String> {
    given.or_else(|| cfg.project()).ok_or_else(|| {
        "choose a project with --project, or save one with `mrl config set-project` (or `mrl dev seed`)"
            .to_owned()
    })
}

fn query(pairs: &[(&str, Option<String>)]) -> String {
    let parts: Vec<String> = pairs
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| format!("{k}={}", encode(v))))
        .collect();
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

/// Percent-encodes everything but unreserved characters.
pub fn encode(v: &str) -> String {
    v.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn paging(p: &Paging) -> [(&'static str, Option<String>); 2] {
    [
        ("limit", p.limit.map(|l| l.to_string())),
        ("cursor", p.cursor.clone()),
    ]
}

fn print_json(v: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
    );
}

/// Prints a page: one line per item from `cols`, then the next cursor.
fn print_page(page: &Value, out: &Output, cols: &[&str]) {
    if out.json {
        return print_json(page);
    }
    let items = page["items"].as_array().cloned().unwrap_or_default();
    if items.is_empty() {
        println!("(none)");
    }
    for item in &items {
        let line: Vec<String> = cols
            .iter()
            .map(|c| match &item[*c] {
                Value::String(s) => s.clone(),
                Value::Null => "-".to_owned(),
                v => v.to_string(),
            })
            .collect();
        println!("{}", line.join("  "));
    }
    if let Some(c) = page["next_cursor"].as_str() {
        println!("more: --cursor {c}");
    }
}

/// A run in plain words: id, state, and for a rejected run every problem with its hint.
pub fn describe_run(run: &Value) -> String {
    let mut out = format!(
        "{}  {}  {}",
        run["id"].as_str().unwrap_or("?"),
        run["state"].as_str().unwrap_or("?"),
        run["name"].as_str().unwrap_or("")
    );
    for p in run["state_reason"]["problems"]
        .as_array()
        .into_iter()
        .flatten()
    {
        out.push_str(&format!(
            "\n  {}{}: {}\n  Hint: {}",
            p["code"].as_str().unwrap_or(""),
            p["path"]
                .as_str()
                .map(|p| format!(" at {p}"))
                .unwrap_or_default(),
            p["message"].as_str().unwrap_or(""),
            p["hint"].as_str().unwrap_or(""),
        ));
    }
    out
}

fn print_run(run: &Value, out: &Output) {
    if out.json {
        print_json(run);
    } else {
        println!("{}", describe_run(run));
    }
}

/// `mrl config …`
pub fn config(cmd: ConfigCommand) -> Result<(), String> {
    let cfg = CliConfig::load()?;
    match cmd {
        ConfigCommand::SetToken { token } => {
            let token = match token {
                Some(t) => t,
                None => {
                    eprintln!("Paste your personal access token, then press Enter:");
                    let mut line = String::new();
                    std::io::stdin()
                        .lock()
                        .read_line(&mut line)
                        .map_err(|e| format!("can't read the token: {e}"))?;
                    line
                }
            };
            let token = token.trim().to_owned();
            if token.is_empty() {
                return Err("the token is empty; nothing was saved".to_owned());
            }
            let saved = CliConfig::update(|c| c.token = Some(token))?;
            println!("saved the token in {}", saved.display());
        }
        ConfigCommand::SetServer { url } => {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(format!("`{url}` must start with http:// or https://"));
            }
            let saved = CliConfig::update(|c| c.server = Some(url))?;
            println!("saved the server in {}", saved.display());
        }
        ConfigCommand::SetProject { project } => {
            let saved = CliConfig::update(|c| c.project = Some(project))?;
            println!("saved the project in {}", saved.display());
        }
        ConfigCommand::Show => {
            println!("file     {}", config_file::path()?.display());
            println!(
                "server   {}",
                cfg.server().unwrap_or_else(|| "(not set)".into())
            );
            let token = cfg.token().map_or_else(
                || "(not set)".to_owned(),
                |t| format!("{}… (hidden)", t.chars().take(8).collect::<String>()),
            );
            println!("token    {token}");
            println!(
                "project  {}",
                cfg.project().unwrap_or_else(|| "(not set)".into())
            );
        }
    }
    Ok(())
}

/// `mrl projects …`
pub fn projects(cmd: ProjectsCommand) -> Result<(), String> {
    let (_, api) = connect()?;
    match cmd {
        ProjectsCommand::List {
            workspace,
            paging: p,
            out,
        } => {
            let page = api.get(&format!(
                "/workspaces/{}/projects{}",
                encode(&workspace),
                query(&paging(&p))
            ))?;
            print_page(&page, &out, &["id", "slug", "name"]);
        }
    }
    Ok(())
}

/// `mrl experiments …`
pub fn experiments(cmd: ExperimentsCommand) -> Result<(), String> {
    let (cfg, api) = connect()?;
    match cmd {
        ExperimentsCommand::List {
            project: prj,
            paging: p,
            out,
        } => {
            let prj = project(&cfg, prj)?;
            let page = api.get(&format!(
                "/projects/{}/experiments{}",
                encode(&prj),
                query(&paging(&p))
            ))?;
            print_page(&page, &out, &["id", "name", "description"]);
        }
        ExperimentsCommand::Create {
            name,
            project: prj,
            description,
        } => {
            let prj = project(&cfg, prj)?;
            let body = json!({ "name": name, "description": description });
            let made = api.post(
                &format!("/projects/{}/experiments", encode(&prj)),
                Some(&body),
                None,
            )?;
            println!(
                "{}  {}",
                made["id"].as_str().unwrap_or("?"),
                made["name"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

/// Builds the submission body from `--file` and the options.
pub fn submission(args: &SubmitArgs) -> Result<Value, String> {
    let mut body = match &args.file {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("can't read {}: {e}", path.display()))?;
            serde_json::from_str::<Value>(&text)
                .map_err(|e| format!("{} is not JSON: {e}", path.display()))?
        }
        None => json!({}),
    };
    let Some(obj) = body.as_object_mut() else {
        return Err("the run file must hold a JSON object".to_owned());
    };
    let mut set = |k: &str, v: Option<Value>| {
        if let Some(v) = v {
            obj.insert(k.to_owned(), v);
        }
    };
    set("name", args.name.clone().map(Value::from));
    set("algorithm_id", args.algorithm.clone().map(Value::from));
    set("base_model_version_id", args.model.clone().map(Value::from));
    set("experiment_id", args.experiment.clone().map(Value::from));
    if !args.dataset.is_empty() {
        set("dataset_version_ids", Some(json!(args.dataset)));
    }
    if let Some(preset) = &args.preset {
        let config = obj.entry("config").or_insert_with(|| json!({}));
        let Some(config) = config.as_object_mut() else {
            return Err("the run file's `config` must be a JSON object".to_owned());
        };
        config.insert("preset".to_owned(), json!(preset));
    }
    Ok(body)
}

/// A `runs:validate` answer in plain words: the verdict, every issue with its hint, the
/// estimate, and what the preflight changed or suggests.
pub fn describe_validation(v: &Value) -> String {
    let text = |x: &Value| x.as_str().unwrap_or("").to_owned();
    let mut out = if v["ok"] == true {
        "Ready: submitting this would queue the run.".to_owned()
    } else {
        "Not ready: fix the errors below first.".to_owned()
    };
    for i in v["issues"].as_array().into_iter().flatten() {
        out.push_str(&format!(
            "\n  {} {}{}: {}",
            text(&i["severity"]),
            text(&i["code"]),
            i["path"]
                .as_str()
                .map(|p| format!(" at {p}"))
                .unwrap_or_default(),
            text(&i["message"]),
        ));
        if !text(&i["hint"]).is_empty() {
            out.push_str(&format!("\n    Hint: {}", text(&i["hint"])));
        }
    }
    let e = &v["estimate"];
    match e["per_gpu_memory_gib"].as_object() {
        Some(r) => out.push_str(&format!(
            "\nEstimated GPU memory: {:.1}–{:.1} GiB (likely {:.1}), confidence {}.",
            r["lo"].as_f64().unwrap_or(0.0),
            r["hi"].as_f64().unwrap_or(0.0),
            r["expected"].as_f64().unwrap_or(0.0),
            text(&e["confidence"]),
        )),
        None => out.push_str("\nEstimated GPU memory: unknown."),
    }
    let p = &v["preflight"];
    if p.is_object() {
        out.push_str(&format!(
            "\nFit: {}. {}",
            text(&p["verdict"]).replace('_', " "),
            text(&p["message"])
        ));
        for c in p["changes"].as_array().into_iter().flatten() {
            out.push_str(&format!(
                "\n  changed {}: {} -> {} ({})",
                text(&c["path"]),
                c.get("before").unwrap_or(&c["from"]),
                c.get("after").unwrap_or(&c["to"]),
                text(&c["why"])
            ));
        }
        for a in p["alternatives"].as_array().into_iter().flatten() {
            out.push_str(&format!("\n  instead: {}", text(&a["message"])));
        }
    }
    out
}

fn validate_run(api: &Api, prj: &str, body: &Value, out: &Output) -> Result<(), String> {
    let v = api.post(
        &format!("/projects/{}/runs:validate", encode(prj)),
        Some(body),
        None,
    )?;
    if out.json {
        print_json(&v);
    } else {
        println!("{}", describe_validation(&v));
    }
    if v["ok"] == true {
        Ok(())
    } else {
        Err("the run is not ready to submit".to_owned())
    }
}

/// `mrl runs …`
pub fn runs(cmd: RunsCommand) -> Result<(), String> {
    let (cfg, api) = connect()?;
    match cmd {
        RunsCommand::Validate(args) => {
            let prj = project(&cfg, args.project.clone())?;
            validate_run(&api, &prj, &submission(&args)?, &args.out)?;
        }
        RunsCommand::Submit(args) => {
            let prj = project(&cfg, args.project.clone())?;
            let body = submission(&args)?;
            if args.dry_run {
                return validate_run(&api, &prj, &body, &args.out);
            }
            let key = args
                .key
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let run = api.post(
                &format!("/projects/{}/runs", encode(&prj)),
                Some(&body),
                Some(&key),
            )?;
            print_run(&run, &args.out);
        }
        RunsCommand::List {
            project: prj,
            state,
            experiment,
            paging: p,
            out,
        } => {
            let prj = project(&cfg, prj)?;
            let [l, c] = paging(&p);
            let q = query(&[("state", state), ("experiment", experiment), l, c]);
            let page = api.get(&format!("/projects/{}/runs{q}", encode(&prj)))?;
            print_page(&page, &out, &["id", "state", "algorithm_id", "name"]);
        }
        RunsCommand::Get { run, out } => {
            print_run(&api.get(&format!("/runs/{}", encode(&run)))?, &out)
        }
        RunsCommand::Cancel { run, out } => print_run(
            &api.post(&format!("/runs/{}:cancel", encode(&run)), None, None)?,
            &out,
        ),
    }
    Ok(())
}

/// `mrl jobs …`
pub fn jobs(cmd: JobsCommand) -> Result<(), String> {
    let (_, api) = connect()?;
    match cmd {
        JobsCommand::List {
            state,
            project,
            paging: p,
            out,
        } => {
            let [l, c] = paging(&p);
            let page = api.get(&format!(
                "/jobs{}",
                query(&[("state", state), ("project", project), l, c])
            ))?;
            print_page(&page, &out, &["id", "state", "kind", "priority", "run_id"]);
        }
    }
    Ok(())
}

/// `mrl dev …`
pub fn dev(cmd: DevCommand) -> Result<(), String> {
    match cmd {
        DevCommand::Seed => {
            let (_, api) = connect()?;
            let state = crate::seed::run(&api)?;
            if let Some(prj) = state.project {
                CliConfig::update(|c| c.project = Some(prj.clone()))?;
                println!("saved {prj} as the default project");
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(file: Option<PathBuf>) -> SubmitArgs {
        SubmitArgs {
            file,
            project: None,
            name: Some("n".into()),
            algorithm: None,
            model: Some("mv_1".into()),
            dataset: vec!["dsv_1".into(), "dsv_2".into()],
            experiment: None,
            key: None,
            preset: None,
            dry_run: false,
            out: Output { json: false },
        }
    }

    #[test]
    fn presets_go_into_the_config_and_validation_reads_plainly() {
        let mut a = args(None);
        a.preset = Some("sft-lora-beginner".into());
        let body = submission(&a).unwrap();
        assert_eq!(body["config"]["preset"], "sft-lora-beginner");
        let v = json!({"ok": false, "issues": [
            {"severity": "error", "code": "MRL-CAP-280", "path": "/resources",
             "message": "Too big.", "hint": "Use QLoRA."}],
            "estimate": {"per_gpu_memory_gib": {"lo": 6.0, "expected": 7.5, "hi": 9.0}, "confidence": "low"},
            "preflight": {"verdict": "does_not_fit", "message": "It won't start.",
              "changes": [{"path": "/engine/gradient_checkpointing", "before": false, "after": true,
                "why": "Uses less memory."}], "alternatives": [{"message": "Use QLoRA."}]}});
        let text = describe_validation(&v);
        assert!(text.starts_with("Not ready"), "{text}");
        assert!(text.contains("error MRL-CAP-280 at /resources: Too big."));
        assert!(text.contains("Hint: Use QLoRA."));
        assert!(text.contains("6.0–9.0 GiB (likely 7.5), confidence low"));
        assert!(text.contains("Fit: does not fit. It won't start."));
        assert!(text.contains("instead: Use QLoRA."));
        assert!(text
            .contains("changed /engine/gradient_checkpointing: false -> true (Uses less memory.)"));
        let unknown = describe_validation(&json!({"ok": true, "issues": [], "estimate": {}}));
        assert!(unknown.starts_with("Ready") && unknown.contains("unknown"));
    }

    #[test]
    fn options_override_the_file() {
        let dir = std::env::temp_dir().join(format!("mrl-cli-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("run.json");
        std::fs::write(
            &f,
            r#"{"name": "old", "algorithm_id": "sft", "config": {"a": 1}}"#,
        )
        .unwrap();
        let body = submission(&args(Some(f))).unwrap();
        assert_eq!(body["name"], "n");
        assert_eq!(body["algorithm_id"], "sft");
        assert_eq!(body["config"]["a"], 1);
        assert_eq!(body["dataset_version_ids"], json!(["dsv_1", "dsv_2"]));
        assert!(body.get("experiment_id").is_none());
        std::fs::write(dir.join("bad.json"), "[1]").unwrap();
        assert!(submission(&args(Some(dir.join("bad.json")))).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn queries_are_encoded_and_runs_described() {
        assert_eq!(query(&[("a", None)]), "");
        assert_eq!(
            query(&[("state", Some("QUEUED".into())), ("c", Some("x y&".into()))]),
            "?state=QUEUED&c=x%20y%26"
        );
        let run = json!({"id": "run_1", "state": "REJECTED", "name": "n", "state_reason": {"problems": [
            {"code": "MRL-CAP-001", "path": "/algorithm_id", "message": "No engine.", "hint": "Pick another method."}]}});
        let text = describe_run(&run);
        assert!(text.starts_with("run_1  REJECTED  n"));
        assert!(text.contains("MRL-CAP-001 at /algorithm_id: No engine."));
        assert!(text.contains("Hint: Pick another method."));
    }
}
