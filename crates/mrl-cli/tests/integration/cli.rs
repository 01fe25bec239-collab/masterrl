//! The `mrl` binary against a real team-mode test server with PostgreSQL.
//! The tenancy endpoints come from [`TenancyDouble`] until a real tenancy API exists.

use crate::support::{runs_server, test_user, TenancyDouble};
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
        let chains = Chains::registered(db.db());
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
