//! Database integration tests (`just test-integration`; needs `DATABASE_URL`).

use super::*;
use crate::server::{TestModule, TestServer, TEST_SECRET};
use axum::http::Method;
use mrl_server::config::Config;
use mrl_server::db::{
    codes, migrate, set_org, DbReady, Isolation, LeaderElection, Migrations, PgIdempotencyStore,
};
use mrl_server::modules::events::{sweep, EventsModule, CHANNEL};
use mrl_server::seams::{
    IdempotencyBegin, IdempotencyStore, ReadinessCheck, StartupStep, StoredResponse,
    LAUNCH_SECRET_HEADER,
};
use mrl_server::Mode;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgListener;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn migrations_apply_from_empty_and_from_the_previous_one() {
    let all = db::migrator();
    let n = all.iter().count();
    assert!(n >= 2);

    let fresh = TestDb::empty().await;
    assert_eq!(migrate::pending(fresh.db(), &all).await.unwrap().len(), n);
    assert_eq!(migrate::apply(fresh.db()).await.unwrap().len(), n);
    assert!(migrate::pending(fresh.db(), &all).await.unwrap().is_empty());
    assert!(
        migrate::apply(fresh.db()).await.unwrap().is_empty(),
        "second run is a no-op"
    );

    let older = TestDb::empty().await;
    let previous = Migrator::with_migrations(all.iter().take(n - 1).cloned().collect());
    migrate::apply_with(older.db(), &previous).await.unwrap();
    let left = migrate::pending(older.db(), &all).await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(
        migrate::apply(older.db()).await.unwrap(),
        vec![left[0].version]
    );
    for table in [
        "outbox_events",
        "processed_events",
        "consumer_cursors",
        "idempotency_keys",
        "outbox_failures",
    ] {
        assert_eq!(older.count(table).await, 0, "{table}");
    }
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn pending_migrations_block_startup_when_auto_migrate_is_off() {
    let t = TestDb::empty().await;
    let manual = t.replica_with(&DatabaseConfig {
        auto_migrate: false,
        ..test_config()
    });
    let err = Migrations(manual.clone()).run().await.unwrap_err();
    assert!(err.detail.starts_with(codes::MIGRATIONS_PENDING), "{err}");
    assert!(DbReady(manual.clone()).check().await.is_err());
    Migrations(t.db().clone()).run().await.unwrap();
    Migrations(manual.clone()).run().await.unwrap();
    DbReady(manual).check().await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn serialization_failures_are_retried() {
    let t = TestDb::new().await;
    t.seed("CREATE TABLE skew (v int)").await;
    let tries = Arc::new(AtomicU32::new(0));
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let run = |v: i32| {
        let (db, tries, gate) = (t.db().clone(), tries.clone(), gate.clone());
        async move {
            let first = Arc::new(AtomicBool::new(true));
            db.transaction_with(Isolation::Serializable, |tx| {
                let (tries, gate, first) = (tries.clone(), gate.clone(), first.clone());
                Box::pin(async move {
                    tries.fetch_add(1, Ordering::SeqCst);
                    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM skew")
                        .fetch_one(&mut **tx)
                        .await?;
                    if first.swap(false, Ordering::SeqCst) {
                        gate.wait().await;
                    }
                    sqlx::query("INSERT INTO skew VALUES ($1)")
                        .bind(v)
                        .execute(&mut **tx)
                        .await?;
                    Ok(())
                })
            })
            .await
        }
    };
    let (a, b) = tokio::join!(run(1), run(2));
    a.unwrap();
    b.unwrap();
    assert_eq!(t.count("skew").await, 2);
    assert!(tries.load(Ordering::SeqCst) >= 3, "one side retried");
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn org_setting_lasts_for_the_transaction_only() {
    let t = TestDb::new().await;
    let org = Uuid::now_v7();
    let seen = t
        .db()
        .transaction(|tx| {
            Box::pin(async move {
                set_org(tx, org).await?;
                let v: Option<String> =
                    sqlx::query_scalar("SELECT current_setting('mrl.org_id', true)")
                        .fetch_one(&mut **tx)
                        .await?;
                Ok(v)
            })
        })
        .await
        .unwrap();
    assert_eq!(seen, Some(org.to_string()));
    let after: Option<String> = sqlx::query_scalar("SELECT current_setting('mrl.org_id', true)")
        .fetch_one(t.pool())
        .await
        .unwrap();
    assert!(after.unwrap_or_default().is_empty());
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn idempotency_keys_replay_refuse_and_expire() {
    let t = TestDb::new().await;
    let store = PgIdempotencyStore::new(t.db().clone());
    let (a, b) = ([1u8; 32], [2u8; 32]);
    assert_eq!(
        store.begin("usr_1", "k", a).await.unwrap(),
        IdempotencyBegin::Started
    );
    assert_eq!(
        store.begin("usr_1", "k", a).await.unwrap(),
        IdempotencyBegin::InProgress
    );
    assert_eq!(
        store.begin("usr_1", "k", b).await.unwrap(),
        IdempotencyBegin::Mismatch
    );
    assert_eq!(
        store.begin("usr_2", "k", b).await.unwrap(),
        IdempotencyBegin::Started,
        "keys are per scope"
    );
    let stored = StoredResponse {
        status: 201,
        headers: vec![("content-type".into(), "application/json".into())],
        body: b"{\"id\":1}".to_vec(),
    };
    store.complete("usr_1", "k", stored.clone()).await.unwrap();
    assert_eq!(
        store.begin("usr_1", "k", a).await.unwrap(),
        IdempotencyBegin::Replay(stored.clone())
    );
    assert_eq!(
        store.begin("usr_1", "k", b).await.unwrap(),
        IdempotencyBegin::Mismatch
    );
    store.release("usr_1", "k").await.unwrap();
    assert_eq!(
        store.begin("usr_1", "k", a).await.unwrap(),
        IdempotencyBegin::Replay(stored),
        "release keeps finished keys"
    );

    store.release("usr_2", "k").await.unwrap();
    assert_eq!(
        store.begin("usr_2", "k", a).await.unwrap(),
        IdempotencyBegin::Started,
        "released keys start over"
    );

    // A key a crashed server left running is taken over after 10 minutes.
    t.seed("UPDATE idempotency_keys SET created_at = now() - interval '11 minutes' WHERE scope = 'usr_2'").await;
    assert_eq!(
        store.begin("usr_2", "k", b).await.unwrap(),
        IdempotencyBegin::Started
    );

    // Expired keys are free again and swept.
    t.seed("UPDATE idempotency_keys SET expires_at = now() - interval '1 second' WHERE scope = 'usr_1'").await;
    assert_eq!(
        store.begin("usr_1", "k", b).await.unwrap(),
        IdempotencyBegin::Started
    );
    t.seed("UPDATE idempotency_keys SET expires_at = now() - interval '1 second'")
        .await;
    assert_eq!(db::sweep_idempotency_keys(t.db()).await.unwrap(), 2);
    assert_eq!(t.count("idempotency_keys").await, 0);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn server_with_events_module_serves_idempotent_posts_and_is_ready() {
    let t = TestDb::new().await;
    let seams = crate::seams::build(
        crate::seams::solo(TEST_SECRET).idempotency(PgIdempotencyStore::new(t.db().clone())),
        Mode::Solo,
    );
    let modules: Vec<Box<dyn mrl_server::ServerModule>> = vec![
        Box::new(EventsModule::new(t.db().clone(), Vec::new())),
        Box::new(TestModule::default()),
    ];
    let srv = TestServer::start(Config::default(), seams, modules)
        .await
        .unwrap();
    let post = |key: &'static str, body: &'static str| {
        let srv = &srv;
        async move {
            srv.request(
                Method::POST,
                "/api/v1/items",
                &[
                    (LAUNCH_SECRET_HEADER, TEST_SECRET),
                    ("idempotency-key", key),
                    ("content-type", "application/json"),
                ],
                body,
            )
            .await
        }
    };
    let first = post("k1", "{\"a\":1}").await;
    assert_eq!(first.status, 201);
    let again = post("k1", "{\"a\":1}").await;
    assert_eq!((again.status, &again.body), (201, &first.body));
    assert_eq!(again.headers["idempotent-replayed"], "true");
    let other = post("k1", "{\"a\":2}").await;
    assert_eq!(other.status, 422);
    assert_eq!(other.json()["code"], "MRL-IDEM-002");
    assert_eq!(srv.get_authed("/readyz").await.status, 200);
    srv.shutdown().await.unwrap();
}

/// Starts an election that marks `leading` while it leads.
fn campaign(
    db: Db,
    lease: Duration,
    leading: Arc<AtomicBool>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        LeaderElection::new(db, "testkit-election", lease)
            .run(cancel, |lead| {
                let leading = leading.clone();
                async move {
                    leading.store(true, Ordering::SeqCst);
                    lead.cancelled().await;
                    leading.store(false, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;
    })
}

async fn wait_for(flag: &AtomicBool, within: Duration) -> Duration {
    let start = Instant::now();
    while !flag.load(Ordering::SeqCst) {
        assert!(start.elapsed() < within, "no leader within {within:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    start.elapsed()
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn a_second_replica_takes_over_within_the_lease() {
    let t = TestDb::new().await;
    let lease = Duration::from_secs(3);
    let (a, b) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let a_task = campaign(t.replica(), lease, a.clone(), CancellationToken::new());
    wait_for(&a, lease).await;
    let b_cancel = CancellationToken::new();
    let b_task = campaign(t.replica(), lease, b.clone(), b_cancel.clone());
    tokio::time::sleep(lease).await;
    assert!(!b.load(Ordering::SeqCst), "only one leader");

    // The leader process dies: its lock connection goes with it.
    a_task.abort();
    let took = wait_for(&b, lease * 2).await;
    assert!(took <= lease, "took over in {took:?}");

    // The leader loses its connection (network cut): it steps down, another takes over.
    let c = Arc::new(AtomicBool::new(false));
    let c_cancel = CancellationToken::new();
    let c_task = campaign(t.replica(), lease, c.clone(), c_cancel.clone());
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_locks
         WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .execute(t.pool())
    .await
    .unwrap();
    let start = Instant::now();
    while b.load(Ordering::SeqCst) {
        assert!(start.elapsed() < lease, "old leader did not step down");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let start = Instant::now();
    while !(c.load(Ordering::SeqCst) || b.load(Ordering::SeqCst)) {
        assert!(start.elapsed() < lease * 2, "nobody took over");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !(c.load(Ordering::SeqCst) && b.load(Ordering::SeqCst)),
        "two leaders"
    );
    b_cancel.cancel();
    c_cancel.cancel();
    b_task.await.unwrap();
    c_task.await.unwrap();
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn one_transaction_wakes_listeners_once_without_a_payload() {
    let t = TestDb::new().await;
    let mut listener = PgListener::connect_with(t.pool()).await.unwrap();
    listener.listen(CHANNEL).await.unwrap();
    let agg = Uuid::now_v7();
    let ids = t
        .write_events(&[
            event("job", agg, 1),
            event("job", agg, 2),
            event("job", agg, 3),
        ])
        .await;
    assert_eq!(ids.len(), 3);
    let n = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n.payload(), "");
    let more = tokio::time::timeout(Duration::from_millis(300), listener.recv()).await;
    assert!(more.is_err(), "a second notification arrived");
    assert_eq!(t.count("outbox_events").await, 3);
}

#[tokio::test]
#[ignore = "needs DATABASE_URL"]
async fn retention_waits_for_every_consumer() {
    let t = TestDb::new().await;
    let agg = Uuid::now_v7();
    t.write_events(&[event("job", agg, 1), event("job", agg, 2)])
        .await;
    t.write_events(&[event("job", agg, 3)]).await;
    t.seed(
        "UPDATE outbox_events SET created_at = now() - interval '8 days' WHERE aggregate_seq < 3",
    )
    .await;
    // `a` is past everything; `b` has no cursor yet, so nothing may go.
    t.seed(
        "INSERT INTO consumer_cursors (consumer, last_id, last_xid, updated_at)
         SELECT 'a', max(id), max((xid::text)::bigint), now() FROM outbox_events",
    )
    .await;
    assert_eq!(sweep(t.db(), &["a", "b"]).await.unwrap().outbox, 0);
    t.seed(
        "INSERT INTO consumer_cursors (consumer, last_id, last_xid, updated_at)
         SELECT 'b', max(id), max((xid::text)::bigint), now() FROM outbox_events WHERE aggregate_seq < 3",
    )
    .await;
    assert_eq!(sweep(t.db(), &["a", "b"]).await.unwrap().outbox, 2);
    assert_eq!(t.count("outbox_events").await, 1, "the recent event stays");

    t.seed(
        "INSERT INTO processed_events (consumer, event_id, processed_at) VALUES
           ('a', gen_random_uuid(), now() - interval '15 days'), ('a', gen_random_uuid(), now())",
    )
    .await;
    assert_eq!(sweep(t.db(), &["a"]).await.unwrap().processed, 1);
    assert_eq!(t.count("processed_events").await, 1);
}
