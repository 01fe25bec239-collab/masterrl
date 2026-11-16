use std::sync::atomic::{AtomicBool, Ordering};

use mrl_agent::hw::HwEventKind;

use super::*;

struct Watcher {
    started: Arc<AtomicBool>,
    fail: bool,
}

#[async_trait::async_trait]
impl AgentService for Watcher {
    fn name(&self) -> &'static str {
        "watcher"
    }
    async fn run(
        &self,
        ctx: Arc<AgentContext>,
        shutdown: CancellationToken,
    ) -> Result<(), AgentError> {
        assert!(ctx.state.root().join("inventory.json").is_file());
        self.started.store(true, Ordering::SeqCst);
        if self.fail {
            return Err(AgentError::Service("watcher gave up".into()));
        }
        shutdown.cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn fake_agent_reports_fake_gpus_and_faults() {
    let agent = FakeAgent::start(2).unwrap();
    let inv = agent.inventory();
    assert_eq!(inv.gpus.len(), 2);
    assert_eq!(inv.gpus[1].model, FAKE_GPU_MODEL);
    assert!(inv.memory.total_bytes > 0, "host values are real");

    agent.inject(Fault::Oom { index: 0 });
    agent.inject(Fault::DeviceLost { index: 1 });
    let t = agent.telemetry();
    assert_eq!(t.gpus.len(), 1);
    assert_eq!(t.gpus[0].memory_free_bytes, 0);
    let kinds: Vec<_> = t.events.iter().map(|e| e.kind.clone()).collect();
    assert_eq!(kinds, [HwEventKind::OutOfMemory, HwEventKind::DeviceLost]);
    assert_eq!(agent.inventory().gpus.len(), 1);
    assert!(agent.dir().join("state/cache").is_dir());
}

#[tokio::test]
async fn main_loop_runs_services_until_stopped_or_failed() {
    let agent = FakeAgent::start(1).unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let running = agent.serve(vec![Box::new(Watcher {
        started: started.clone(),
        fail: false,
    })]);
    while !started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    let pid_file = agent.ctx.state.root().join(mrl_agent::PID_FILE);
    assert!(pid_file.is_file());
    running.stop().await.unwrap();
    assert!(!pid_file.exists(), "the pid file goes away on shutdown");

    // One failing service stops the others and its error comes back.
    let other = Arc::new(AtomicBool::new(false));
    let running = agent.serve(vec![
        Box::new(Watcher {
            started: other.clone(),
            fail: false,
        }),
        Box::new(Watcher {
            started: Arc::new(AtomicBool::new(false)),
            fail: true,
        }),
    ]);
    let e = running.join().await.unwrap_err();
    assert_eq!(e.code(), "MRL-NODE-125");
}

#[cfg(unix)]
#[tokio::test]
async fn fake_agent_runs_programs() {
    let agent = FakeAgent::start(1).unwrap();
    let mut spec = agent.spec("hello", "/bin/sh", &["-c", "echo $CUDA_VISIBLE_DEVICES"]);
    spec.gpus = vec![agent.inventory().gpus[0].uuid.clone()];
    let log = spec.log_dir.join("stdout.log");
    let report = agent
        .ctx
        .executor
        .spawn(spec)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(report.success());
    let out = std::fs::read_to_string(log).unwrap();
    assert!(out.starts_with("GPU-fake-"), "{out}");
}
