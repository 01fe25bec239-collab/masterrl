//! Linux integration tests: cgroup limits, whole-tree kill and the bubblewrap sandbox.
//!
//! `#[ignore]`d; run with `cargo test -p mrl-agent -- --ignored` (or `just test-integration`).
//! - Executor tests need a writable, empty cgroup v2 folder in `MRL_TEST_CGROUP_ROOT`
//!   (for example `sudo mkdir /sys/fs/cgroup/mrl-test && sudo chown $USER -R /sys/fs/cgroup/mrl-test`).
//! - Sandbox tests need `bwrap` and unprivileged user namespaces.
//!
//! Without them the tests print SKIPPED and pass, unless `MRL_TEST_CGROUP_REQUIRED=1` or
//! `MRL_TEST_SANDBOX_REQUIRED=1`, which make them fail instead.
//! CI (`.github/workflows/ci-agent.yml`) sets up both and sets the REQUIRED flags.
#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::time::Duration;

use mrl_agent::config::{ExecConfig, SandboxChoice, SandboxConfig};
use mrl_agent::exec::process::ProcessExecutor;
use mrl_agent::exec::{ExecSpec, Executor, Limits};
use mrl_agent::sandbox;

fn required(var: &str) -> bool {
    std::env::var(var).as_deref() == Ok("1")
}

fn skip(var: &str, why: &str) {
    assert!(!required(var), "{why}");
    eprintln!("SKIPPED: {why}");
}

fn cgroup_executor() -> Option<ProcessExecutor> {
    let Some(root) = std::env::var_os("MRL_TEST_CGROUP_ROOT").map(PathBuf::from) else {
        skip(
            "MRL_TEST_CGROUP_REQUIRED",
            "MRL_TEST_CGROUP_ROOT is not set",
        );
        return None;
    };
    let exec = ProcessExecutor::new(&ExecConfig {
        cgroup_root: Some(root),
        ..ExecConfig::default()
    })
    .unwrap();
    let s = exec.limit_support();
    if !(s.memory && s.pids && s.oom_detection) {
        skip(
            "MRL_TEST_CGROUP_REQUIRED",
            &format!("cgroup limits are off: {:?}", s.note),
        );
        return None;
    }
    Some(exec)
}

fn bwrap() -> Option<Box<dyn sandbox::SandboxBackend>> {
    match sandbox::select(&SandboxConfig {
        backend: SandboxChoice::Bubblewrap,
        bwrap_path: None,
    }) {
        Ok(b) => Some(b),
        Err(e) => {
            skip("MRL_TEST_SANDBOX_REQUIRED", &e.explain());
            None
        }
    }
}

fn pid_gone(pid: i32) -> bool {
    std::fs::metadata(format!("/proc/{pid}")).is_err()
        || std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .is_ok_and(|s| s.split_whitespace().nth(2) == Some("Z"))
}

#[tokio::test]
#[ignore = "needs a delegated cgroup v2 root"]
async fn memory_limit_is_enforced_and_oom_is_detected() {
    let Some(exec) = cgroup_executor() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    // `tail` keeps a whole "line" of zeros in memory: 256 MiB against a 32 MiB limit.
    let mut spec = ExecSpec::new(
        "oom",
        "/bin/sh",
        &["-c", "head -c 268435456 /dev/zero | tail > /dev/null"],
        dir.path(),
    );
    spec.limits = Limits {
        memory_bytes: Some(32 << 20),
        pids: Some(64),
        cpu_millis: Some(500),
    };
    let report = exec.spawn(spec).await.unwrap().wait().await.unwrap();
    assert!(report.oom_killed, "{report:?}");
    assert!(!report.success());
    assert!(report.limits.memory && report.limits.cpu && report.limits.pids);

    // Under the limit it succeeds and no OOM is reported.
    let mut spec = ExecSpec::new(
        "fits",
        "/bin/sh",
        &["-c", "head -c 1048576 /dev/zero | tail > /dev/null"],
        dir.path(),
    );
    spec.limits.memory_bytes = Some(32 << 20);
    let report = exec.spawn(spec).await.unwrap().wait().await.unwrap();
    assert!(report.success() && !report.oom_killed, "{report:?}");
}

#[tokio::test]
#[ignore = "needs a delegated cgroup v2 root"]
async fn kill_ends_the_whole_tree_even_after_setsid() {
    let Some(exec) = cgroup_executor() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let pids = dir.path().join("pids");
    // One child in the group, one that escapes it with setsid.
    let script = format!(
        "sleep 300 & echo $! >> {p}; setsid sleep 300 & echo $! >> {p}; wait",
        p = pids.display()
    );
    let mut run = exec
        .spawn(ExecSpec::new(
            "tree",
            "/bin/sh",
            &["-c", &script],
            dir.path(),
        ))
        .await
        .unwrap();
    let mut children = Vec::new();
    for _ in 0..300 {
        children = std::fs::read_to_string(&pids)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| l.trim().parse::<i32>().ok())
            .collect();
        if children.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(children.len(), 2);
    run.kill().await.unwrap();
    let report = run.wait().await.unwrap();
    assert_eq!(report.signal, Some(9), "{report:?}");
    for pid in children {
        let mut gone = false;
        for _ in 0..200 {
            if pid_gone(pid) {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(gone, "process {pid} survived");
    }
}

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces"]
async fn data_prep_blocks_network_and_writes_outside_staging() {
    let Some(backend) = bwrap() else { return };
    let exec = ProcessExecutor::new(&ExecConfig::default()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let allowed = dir.path().join("datasets");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::write(allowed.join("rows.jsonl"), "{}\n").unwrap();
    let outside = dir.path().join("outside.txt");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let script = format!(
        r#"
        if (echo hi > /dev/tcp/127.0.0.1/{port}) 2>/dev/null; then echo net=open; else echo net=blocked; fi
        echo ok > "$MRL_STAGING_DIR/out.txt" && echo staging=written
        if echo x > {outside} 2>/dev/null; then echo outside=written; else echo outside=refused; fi
        if echo x > {allowed}/new.txt 2>/dev/null; then echo allowed=written; else echo allowed=refused; fi
        cat {allowed}/rows.jsonl >/dev/null && echo allowed=readable
        if echo x > ./work.txt 2>/dev/null; then echo work=written; else echo work=refused; fi
        if echo x > /root-file 2>/dev/null; then echo root=written; else echo root=refused; fi
        if ls "$HOME" >/dev/null 2>&1 && [ "$HOME" != / ]; then echo home=visible; else echo home=hidden; fi
        "#,
        outside = outside.display(),
        allowed = allowed.display(),
    );
    let run_script = |name: &str, sandboxed: bool| {
        let spec = ExecSpec::new(name, "/bin/bash", &["-c", &script], &dir.path().join(name));
        let spec = if sandboxed {
            backend
                .wrap(&sandbox::data_prep(), spec, std::slice::from_ref(&allowed))
                .unwrap()
        } else {
            spec
        };
        (
            spec.staging_dir.clone(),
            spec.log_dir.join("stdout.log"),
            spec,
        )
    };

    // Control run without the sandbox: the network is reachable.
    let (_, log, spec) = run_script("open", false);
    assert!(exec
        .spawn(spec)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap()
        .success());
    assert!(std::fs::read_to_string(log).unwrap().contains("net=open"));
    // The control run wrote these; clear them so the sandboxed run is judged alone.
    std::fs::remove_file(&outside).unwrap();
    std::fs::remove_file(allowed.join("new.txt")).unwrap();

    let (staging, log, spec) = run_script("boxed", true);
    let report = exec.spawn(spec).await.unwrap().wait().await.unwrap();
    let out = std::fs::read_to_string(log).unwrap();
    assert!(report.success(), "{report:?}\n{out}");
    for line in [
        "net=blocked",
        "staging=written",
        "allowed=refused",
        "allowed=readable",
        "work=refused",
        "root=refused",
        "home=hidden",
    ] {
        assert!(out.contains(line), "missing {line} in:\n{out}");
    }
    assert_eq!(
        std::fs::read_to_string(staging.join("out.txt")).unwrap(),
        "ok\n"
    );
    // Whatever happened inside, nothing outside staging reached the host disk.
    assert!(
        !outside.exists(),
        "a write outside staging reached the host"
    );
    assert!(!allowed.join("new.txt").exists());
    drop(listener);
}
