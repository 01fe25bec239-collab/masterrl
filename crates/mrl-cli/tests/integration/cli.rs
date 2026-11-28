//! The `mrl` binary against a real team-mode test server with PostgreSQL.
//! The tenancy endpoints come from [`TenancyDouble`] until a real tenancy API exists.

use crate::support::{call, runs_server, test_user, TenancyDouble};
use axum::http::Method;
use mrl_domain::ids::{DatasetVersionId, ModelVersionId, OrgId};
use mrl_domain::rbac::Role;
use mrl_server::modules::jobs::Chains;
use mrl_testkit::pg::TestDb;
use std::path::PathBuf;
use std::process::{Command, Output};

struct Cli {
    server: String,
    config: PathBuf,
}

impl Cli {
    fn run(&self, token: Option<&str>, args: &[&str]) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mrl"));
        cmd.args(args)
            .env("MRL_CONFIG", &self.config)
            .env("MRL_SERVER", &self.server)
            .env_remove("MRL_PROJECT")
            .env_remove("MRL_LAUNCH_SECRET");
        match token {
            Some(t) => cmd.env("MRL_TOKEN", t),
            None => cmd.env_remove("MRL_TOKEN"),
        };
        cmd.output().expect("run mrl")
    }

    fn ok(&self, args: &[&str]) -> String {
        let out = self.run(Some("tok"), args);
        let (stdout, stderr) = text(&out);
        assert!(
            out.status.success(),
            "mrl {args:?} failed:\n{stdout}\n{stderr}"
        );
        stdout
    }

    fn fails(&self, token: Option<&str>, args: &[&str]) -> String {
        let out = self.run(token, args);
        assert!(!out.status.success(), "mrl {args:?} should fail");
        text(&out).1
    }
}

fn text(out: &Output) -> (String, String) {
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
#[ignore = "needs DATABASE_URL"]
fn seed_then_submit_returns_a_rejected_run_with_code_and_hint() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    // One item per page, with another org first: the seed must page to find `dev` again.
    let tenancy = TenancyDouble::new(1);
    tenancy.add_org("other");
    let (db, srv) = rt.block_on(async {
        let db = TestDb::new().await;
        let users = [test_user("tok", "usr_admin", OrgId::new(), Role::Owner)];
        let chains = Chains::registered(db.db(), &mrl_server::Config::default());
        let srv = runs_server(&db, chains, &users, vec![Box::new(tenancy.clone())]).await;
        (db, srv)
    });
    let dir = std::env::temp_dir().join(format!("mrl-cli-test-{}", uuid::Uuid::new_v4()));
    let cli = Cli {
        server: format!("http://{}", srv.api()),
        config: dir.join("cli.toml"),
    };

    // Plain-language failures before anything is set up.
    assert!(cli.fails(None, &["runs", "list"]).contains("MRL-AUTH-170"));
    let down = Cli {
        server: "http://127.0.0.1:9".into(),
        config: cli.config.clone(),
    };
    assert!(down
        .fails(Some("tok"), &["jobs", "list"])
        .contains("MRL-ENV-170"));
    assert!(cli
        .fails(Some("tok"), &["runs", "list"])
        .contains("--project"));

    // Settings.
    let saved = cli.run(None, &["config", "set-token", "tok-secret-value"]);
    assert!(saved.status.success());
    let show = cli.run(None, &["config", "show"]);
    let shown = text(&show).0;
    assert!(shown.contains("tok-secr… (hidden)"), "{shown}");
    assert!(!shown.contains("tok-secret-value"));
    assert!(cli
        .fails(None, &["config", "set-server", "ftp://x"])
        .contains("http://"));

    // Seed twice: the second run finds everything.
    let first = cli.ok(&["dev", "seed"]);
    assert!(first.contains("created org `dev`"), "{first}");
    assert!(first.contains("created project `demo`"), "{first}");
    let again = cli.ok(&["dev", "seed"]);
    assert!(again.contains("exists  project `demo`"), "{again}");
    assert_eq!(
        tenancy.counts(),
        (2, 1, 1),
        "the second seed created nothing"
    );
    assert!(again.contains("signed in as usr_admin (user)"));
    // Like the real API, the fake refuses a second `dev` with 409 (the seed reads that as "already there").
    let body = serde_json::json!({"slug": "dev", "name": "dev"});
    let dup = rt.block_on(call(
        &srv,
        Method::POST,
        "/api/v1/orgs",
        "tok",
        None,
        Some(&body),
    ));
    assert_eq!(dup.status, 409);

    // Submit: the default chains reject it with MRL-CAP-001 and a hint.
    let model = ModelVersionId::new().to_string();
    let dataset = DatasetVersionId::new().to_string();
    let submitted = cli.ok(&[
        "runs",
        "submit",
        "--name",
        "demo-run",
        "--algorithm",
        "sft",
        "--model",
        &model,
        "--dataset",
        &dataset,
    ]);
    assert!(submitted.contains("REJECTED"), "{submitted}");
    assert!(
        submitted.contains("MRL-CAP-001 at /algorithm_id"),
        "{submitted}"
    );
    assert!(
        submitted.contains("Hint: Pick another method."),
        "{submitted}"
    );
    let run = submitted.split_whitespace().next().unwrap().to_owned();
    assert!(run.starts_with("run_"));

    let listed = cli.ok(&["runs", "list"]);
    assert!(
        listed.contains(&run) && listed.contains("REJECTED"),
        "{listed}"
    );
    let got: serde_json::Value =
        serde_json::from_str(&cli.ok(&["runs", "get", &run, "--json"])).unwrap();
    assert_eq!(got["state_reason"]["code"], "MRL-CAP-001");
    let cancel = cli.fails(Some("tok"), &["runs", "cancel", &run]);
    assert!(cancel.contains("MRL-STATE-170"), "{cancel}");
    assert!(cli
        .ok(&["jobs", "list", "--state", "REJECTED"])
        .contains("TRAIN"));

    // Experiments and projects.
    assert!(cli
        .ok(&["experiments", "create", "sweep"])
        .contains("sweep"));
    let exps = cli.ok(&["experiments", "list"]);
    assert!(exps.contains("sweep") && exps.contains("default"), "{exps}");
    let dup = cli.fails(Some("tok"), &["experiments", "create", "sweep"]);
    assert!(dup.contains("MRL-CONFLICT-171"), "{dup}");
    let config = std::fs::read_to_string(&cli.config).unwrap();
    let ws_line = first
        .lines()
        .find(|l| l.contains("workspace `default`"))
        .unwrap();
    let ws = ws_line.split('(').nth(1).unwrap().trim_end_matches(')');
    assert!(cli
        .ok(&["projects", "list", "--workspace", ws])
        .contains("demo"));
    assert!(config.contains("project = \"prj_"), "{config}");

    rt.block_on(async { srv.shutdown().await.unwrap() });
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

/// A silent worker: the server's settings check waits out a 100 s budget, then answers with
/// its L0 fallback (MRL-CAP-282). Both dry-run commands wait past the CLI's usual 90 s.
#[test]
#[ignore = "waits out the CLI's 90 s default timeout (just test-integration)"]
fn both_dry_runs_get_the_l0_fallback_after_a_silent_worker_times_out() {
    use axum::routing::post;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let addr = rt.block_on({
        let seen = seen.clone();
        async move {
            let app = axum::Router::new().route(
                "/api/v1/projects/{prj}/runs:validate",
                post(move || {
                    let seen = seen.clone();
                    async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_secs(92)).await;
                        axum::Json(serde_json::json!({
                            "ok": true,
                            "checked": "l0",
                            "issues": [{"severity": "info", "code": "MRL-CAP-282", "path": null,
                                "message": "No machine answered within 100 seconds, so the settings were checked by the app alone and the memory is not estimated.",
                                "hint": ""}],
                            "estimate": {"verdict": "unknown", "confidence": "low", "assumptions": []},
                            "resolved_config": {},
                            "preflight": null
                        }))
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            addr
        }
    });
    let dir = std::env::temp_dir().join(format!("mrl-cli-test-{}", uuid::Uuid::new_v4()));
    let cli = Arc::new(Cli {
        server: format!("http://{addr}"),
        config: dir.join("cli.toml"),
    });
    let base = [
        "--project",
        "prj_01M8GDJY74EQK82MRRQB9A0RNF",
        "--name",
        "n",
        "--algorithm",
        "sft",
        "--model",
        "mv_01M8GDH3ZDE4QTWWSTF6SEWCGK",
        "--dataset",
        "dsv_01M8GDH3ZDE4QTWWSTF6SEWCGK",
    ];
    let runs: Vec<_> = [
        vec!["runs", "validate"],
        vec!["runs", "submit", "--dry-run"],
    ]
    .into_iter()
    .map(|mut args| {
        args.extend(base);
        let cli = cli.clone();
        std::thread::spawn(move || {
            let args: Vec<&str> = args.to_vec();
            (args.join(" "), cli.ok(&args))
        })
    })
    .collect();
    for run in runs {
        let (args, out) = run.join().unwrap();
        assert!(out.contains("MRL-CAP-282"), "mrl {args}: {out}");
        assert!(out.contains("Ready"), "mrl {args}: {out}");
    }
    assert_eq!(
        seen.load(Ordering::SeqCst),
        2,
        "one dry run each, nothing submitted"
    );
}
