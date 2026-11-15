//! Dispatcher integration tests (`just test-integration`; needs `DATABASE_URL`).

use super::*;
use async_trait::async_trait;
use mrl_server::db::Tx;
use mrl_server::modules::events::{
    sweep, ConsumerError, Dispatcher, DispatcherOptions, Event, EventConsumer, Quarantined,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

const EFFECTS: &str = "CREATE TABLE effects (
    n bigserial PRIMARY KEY, consumer text NOT NULL, event_id uuid NOT NULL,
    aggregate uuid NOT NULL, seq bigint NOT NULL)";

/// Writes one `effects` row per event in the delivery transaction.
struct Recorder {
    name: &'static str,
    calls: AtomicUsize,
    /// The handler call (1-based) that blocks forever, to kill the dispatcher mid-batch.
    hang_at: Option<usize>,
    /// Events with this seq always fail.
    poison_seq: Option<i64>,
}

impl Recorder {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            calls: AtomicUsize::new(0),
            hang_at: None,
            poison_seq: None,
        }
    }
}

#[async_trait]
impl EventConsumer for Recorder {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn handle(&self, tx: &mut Tx, e: &Event) -> Result<(), ConsumerError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        sqlx::query(
            "INSERT INTO effects (consumer, event_id, aggregate, seq) VALUES ($1, $2, $3, $4)",
        )
        .bind(self.name)
        .bind(e.event_id)
        .bind(e.aggregate_id)
        .bind(e.aggregate_seq)
        .execute(&mut **tx)
        .await?;
        if self.hang_at == Some(call) {
            std::future::pending::<()>().await;
        }
        if self.poison_seq == Some(e.aggregate_seq) {
            return Err(format!("cannot handle seq {}", e.aggregate_seq).into());
        }
        Ok(())
    }
}

fn fast() -> DispatcherOptions {
    DispatcherOptions {
        batch_size: 7,
        poll_interval: Duration::from_millis(50),
        max_attempts: 3,
        retry_base: Duration::from_millis(20),
        retry_max: Duration::from_millis(100),
        lease: Duration::from_secs(3),
    }
}

fn start(
    d: Dispatcher,
) -> (
    Arc<Dispatcher>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let d = Arc::new(d);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(d.clone().run(cancel.clone()));
    (d, cancel, task)
}

async fn wait_rows(t: &TestDb, sql: &'static str, want: i64, within: Duration) {
    let start = Instant::now();
    loop {
        let n: i64 = sqlx::query_scalar(sql).fetch_one(t.pool()).await.unwrap();
        if n >= want {
            return;
        }
        assert!(
            start.elapsed() < within,
            "{sql}: {n} of {want} after {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// `(aggregate, seq)` in the order effects were committed.
async fn effects(t: &TestDb, consumer: &str) -> Vec<(Uuid, i64)> {
    sqlx::query_as("SELECT aggregate, seq FROM effects WHERE consumer = $1 ORDER BY n")
        .bind(consumer)
        .fetch_all(t.pool())
        .await
        .unwrap()
}

fn assert_in_order(rows: &[(Uuid, i64)]) {
    let mut last: std::collections::HashMap<Uuid, i64> = Default::default();
    for (agg, seq) in rows {
        let prev = last.insert(*agg, *seq).unwrap_or(0);
        assert!(*seq > prev, "aggregate {agg}: seq {seq} after {prev}");
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn killed_mid_batch_every_event_is_delivered_once_in_order() {
    let t = TestDb::new().await;
    t.seed(EFFECTS).await;
    let aggs: Vec<Uuid> = (0..5).map(|_| Uuid::now_v7()).collect();
    for seq in 1..=10 {
        for a in &aggs {
            t.write_events(&[event("job", *a, seq)]).await;
        }
    }
    let first = Arc::new(Recorder {
        hang_at: Some(20),
        ..Recorder::new("recorder")
    });
    let (_, _, task) =
        start(Dispatcher::new(t.replica(), vec![first.clone()]).with_options(fast()));
    let start_wait = Instant::now();
    while first.calls.load(Ordering::SeqCst) < 20 {
        assert!(
            start_wait.elapsed() < Duration::from_secs(20),
            "dispatcher stuck"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(t.count("effects").await, 19, "the 20th is still open");
    task.abort(); // killed mid-batch, inside a delivery
    let _ = task.await;

    let second = Arc::new(Recorder::new("recorder"));
    let (_, cancel, task) =
        start(Dispatcher::new(t.replica(), vec![second.clone()]).with_options(fast()));
    wait_rows(
        &t,
        "SELECT count(*) FROM effects",
        50,
        Duration::from_secs(20),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    task.await.unwrap();

    let rows = effects(&t, "recorder").await;
    assert_eq!(rows.len(), 50, "side effects exactly once");
    let distinct: std::collections::HashSet<_> = rows.iter().collect();
    assert_eq!(distinct.len(), 50);
    assert_in_order(&rows);
    let handled = first.calls.load(Ordering::SeqCst) + second.calls.load(Ordering::SeqCst);
    assert!(
        handled >= 50,
        "every event delivered at least once ({handled})"
    );
    assert_eq!(t.count("processed_events").await, 50);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_lower_id_committed_later_is_never_skipped() {
    let t = TestDb::new().await;
    t.seed(EFFECTS).await;
    let (x, y) = (Uuid::now_v7(), Uuid::now_v7());
    // T1 takes its transaction id first; T2 second.
    let mut t1 = t.db().begin().await.unwrap();
    let mut t2 = t.db().begin().await.unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *t1)
        .await
        .unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *t2)
        .await
        .unwrap();
    // T2 writes first (lower id), T1 second (higher id); T1 commits, T2 stays open.
    OutboxWriter::write(&mut t2, &event("job", x, 1))
        .await
        .unwrap();
    OutboxWriter::write(&mut t1, &event("job", y, 1))
        .await
        .unwrap();
    t1.commit().await.unwrap();

    let rec = Arc::new(Recorder::new("late"));
    let (d, cancel, task) =
        start(Dispatcher::new(t.replica(), vec![rec.clone()]).with_options(fast()));
    wait_rows(
        &t,
        "SELECT count(*) FROM effects",
        1,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(effects(&t, "late").await, vec![(y, 1)]);

    t2.commit().await.unwrap();
    wait_rows(
        &t,
        "SELECT count(*) FROM effects",
        2,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(effects(&t, "late").await, vec![(y, 1), (x, 1)]);
    let caught_up = Instant::now();
    while d.lag().await.unwrap() > 0.0 {
        assert!(
            caught_up.elapsed() < Duration::from_secs(5),
            "lag stays above zero"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn aggregate_order_holds_when_transaction_ids_run_backwards() {
    let t = TestDb::new().await;
    t.seed(EFFECTS).await;
    let agg = Uuid::now_v7();
    // Transaction ids: T2 < hold < T1. T1 writes seq 1 and commits, then T2 writes seq 2.
    let mut t2 = t.db().begin().await.unwrap();
    let mut hold = t.db().begin().await.unwrap();
    let mut t1 = t.db().begin().await.unwrap();
    for tx in [&mut t2, &mut hold, &mut t1] {
        sqlx::query("SELECT pg_current_xact_id()")
            .execute(&mut **tx)
            .await
            .unwrap();
    }
    OutboxWriter::write(&mut t1, &event("job", agg, 1))
        .await
        .unwrap();
    t1.commit().await.unwrap();
    OutboxWriter::write(&mut t2, &event("job", agg, 2))
        .await
        .unwrap();
    t2.commit().await.unwrap();
    // `hold` keeps xmin between them: seq 2 is past the xmin rule, seq 1 is not yet.

    let rec = Arc::new(Recorder::new("ordered"));
    let (_, cancel, task) =
        start(Dispatcher::new(t.replica(), vec![rec.clone()]).with_options(fast()));
    wait_rows(
        &t,
        "SELECT count(*) FROM effects",
        2,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(effects(&t, "ordered").await, vec![(agg, 1), (agg, 2)]);
    cancel.cancel();
    task.await.unwrap();

    // Two weeks pass with the cursor still before seq 1 (it waits on `hold`). Retention may
    // drop seq 2's dedup row (the cursor passed it) but must keep seq 1's.
    t.seed("UPDATE processed_events SET processed_at = now() - interval '15 days'")
        .await;
    assert_eq!(sweep(t.db(), &["ordered"]).await.unwrap().processed, 1);
    assert_eq!(t.count("processed_events").await, 1);

    hold.commit().await.unwrap();
    let rec = Arc::new(Recorder::new("ordered"));
    let (d, cancel, task) =
        start(Dispatcher::new(t.replica(), vec![rec.clone()]).with_options(fast()));
    let caught_up = Instant::now();
    while d.lag().await.unwrap() > 0.0 {
        assert!(
            caught_up.elapsed() < Duration::from_secs(10),
            "did not catch up"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    task.await.unwrap();
    assert_eq!(rec.calls.load(Ordering::SeqCst), 0, "seq 1 ran again");
    assert_eq!(effects(&t, "ordered").await.len(), 2, "no second delivery");
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_poison_event_is_quarantined_and_the_rest_flow() {
    let t = TestDb::new().await;
    t.seed(EFFECTS).await;
    let agg = Uuid::now_v7();
    for seq in 1..=3 {
        t.write_events(&[event("job", agg, seq)]).await;
    }
    let alerts: Arc<Mutex<Vec<Quarantined>>> = Arc::default();
    let hook = {
        let alerts = alerts.clone();
        Arc::new(move |q: &Quarantined| alerts.lock().unwrap().push(q.clone()))
    };
    let rec = Arc::new(Recorder {
        poison_seq: Some(2),
        ..Recorder::new("picky")
    });
    let other = Arc::new(Recorder::new("relaxed"));
    let d = Dispatcher::new(t.replica(), vec![rec.clone(), other.clone()])
        .with_options(fast())
        .with_alert_hook(hook);
    let (_, cancel, task) = start(d);
    wait_rows(
        &t,
        "SELECT count(*) FROM effects WHERE consumer = 'picky'",
        2,
        Duration::from_secs(10),
    )
    .await;
    wait_rows(
        &t,
        "SELECT count(*) FROM effects WHERE consumer = 'relaxed'",
        3,
        Duration::from_secs(10),
    )
    .await;
    cancel.cancel();
    task.await.unwrap();

    assert_eq!(effects(&t, "picky").await, vec![(agg, 1), (agg, 3)]);
    let alerts = alerts.lock().unwrap().clone();
    assert_eq!(alerts.len(), 1);
    assert_eq!(
        (alerts[0].consumer.as_str(), alerts[0].attempts),
        ("picky", 3)
    );
    assert!(alerts[0].error.contains("seq 2"), "{:?}", alerts[0]);
    let (attempts, quarantined): (i32, bool) = sqlx::query_as(
        "SELECT attempts, quarantined_at IS NOT NULL FROM outbox_failures WHERE consumer = 'picky'",
    )
    .fetch_one(t.pool())
    .await
    .unwrap();
    assert_eq!((attempts, quarantined), (3, true));
    assert_eq!(
        rec.calls.load(Ordering::SeqCst),
        2 + 3,
        "seq 2 tried three times"
    );
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn with_no_consumers_the_dispatcher_idles_at_zero_lag() {
    let t = TestDb::new().await;
    t.write_events(&[event("job", Uuid::now_v7(), 1)]).await;
    let (d, cancel, task) = start(Dispatcher::new(t.replica(), Vec::new()).with_options(fast()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(d.lag().await.unwrap(), 0.0);
    cancel.cancel();
    task.await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn consumers_skip_types_they_do_not_accept() {
    struct OnlyJobs(Recorder);
    #[async_trait]
    impl EventConsumer for OnlyJobs {
        fn name(&self) -> &'static str {
            self.0.name
        }
        fn accepts(&self, event_type: &str) -> bool {
            event_type.starts_with("job.")
        }
        async fn handle(&self, tx: &mut Tx, e: &Event) -> Result<(), ConsumerError> {
            self.0.handle(tx, e).await
        }
    }
    let t = TestDb::new().await;
    t.seed(EFFECTS).await;
    t.write_events(&[
        event("node", Uuid::now_v7(), 1),
        event("job", Uuid::now_v7(), 1),
    ])
    .await;
    let c = Arc::new(OnlyJobs(Recorder::new("jobs-only")));
    let (d, cancel, task) = start(Dispatcher::new(t.replica(), vec![c]).with_options(fast()));
    wait_rows(
        &t,
        "SELECT count(*) FROM effects",
        1,
        Duration::from_secs(10),
    )
    .await;
    let start_wait = Instant::now();
    while d.lag().await.unwrap() > 0.0 {
        assert!(
            start_wait.elapsed() < Duration::from_secs(5),
            "cursor did not pass the skipped event"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cancel.cancel();
    task.await.unwrap();
    assert_eq!(t.count("effects").await, 1);
    assert_eq!(t.count("processed_events").await, 1);
}
