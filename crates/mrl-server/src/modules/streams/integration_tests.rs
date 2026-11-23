//! P22 stream tests: resume with `Last-Event-ID`, topic authorization, limits, resync.
//! Part of the telemetry integration tests (`super` holds the world and seeding helpers).

use super::{points, relay, seed_run, send_log, send_metrics, Seeded, Who, World};
use axum::body::Body;
use axum::http::{Method, Request};
use mrl_proto::worker::v1::worker_message::Body as WorkerBody;
use mrl_proto::worker::v1::{LogStream, Progress};
use mrl_server::modules::alerts::{AlertScope, AlertSink, NewAlert, Severity};
use mrl_server::modules::nodes::lanes::LaneKind;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio_stream::StreamExt;
use uuid::Uuid;

/// One server-sent event.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Sse {
    pub(super) event: String,
    pub(super) id: Option<String>,
    pub(super) data: Value,
}

/// An open stream, read event by event.
pub(super) struct Client {
    body: axum::body::BodyDataStream,
    buf: String,
}

/// Opens `GET /streams?topics=…` as `who`; the status, and the stream when it is 200.
async fn open(
    w: &World,
    who: Who,
    topics: &str,
    last: Option<&str>,
) -> (u16, Option<Client>, String) {
    open_at(w.srv.api(), who, topics, last).await
}

pub(super) async fn open_at(
    addr: std::net::SocketAddr,
    who: Who,
    topics: &str,
    last: Option<&str>,
) -> (u16, Option<Client>, String) {
    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    let (mut send, conn) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tcp))
        .await
        .unwrap_or_else(|e| panic!("handshake: {e}"));
    tokio::spawn(conn);
    let (k, v) = World::auth(who);
    let mut req = Request::builder()
        .method(Method::GET)
        .uri(format!("/api/v1/streams?topics={topics}"))
        .header("host", addr.to_string())
        .header(k, v);
    if let Some(l) = last {
        req = req.header("last-event-id", l);
    }
    let req = req.body(Body::empty()).unwrap_or_else(|e| panic!("{e}"));
    let res = send
        .send_request(req)
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));
    let status = res.status().as_u16();
    let body = Body::new(res.into_body());
    if status != 200 {
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .unwrap_or_default();
        return (status, None, String::from_utf8_lossy(&bytes).into_owned());
    }
    (
        status,
        Some(Client {
            body: body.into_data_stream(),
            buf: String::new(),
        }),
        String::new(),
    )
}

impl Client {
    /// The next event, or `None` after `wait`.
    async fn next(&mut self, wait: Duration) -> Option<Sse> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if let Some(end) = self.buf.find("\n\n") {
                let block: String = self.buf.drain(..end + 2).collect();
                let (mut event, mut id, mut data) = (String::from("message"), None, String::new());
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        event = v.trim().to_owned();
                    } else if let Some(v) = line.strip_prefix("id:") {
                        id = Some(v.trim().to_owned());
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push_str(v.trim_start());
                    }
                }
                return Some(Sse {
                    event,
                    id,
                    data: serde_json::from_str(&data).unwrap_or(Value::Null),
                });
            }
            let chunk = tokio::time::timeout_at(deadline, self.body.next())
                .await
                .ok()??
                .ok()?;
            self.buf.push_str(&String::from_utf8_lossy(&chunk));
        }
    }

    /// Events (heartbeats left out) until `done(events)` or 15 s.
    pub(super) async fn until(&mut self, done: impl Fn(&[Sse]) -> bool) -> Vec<Sse> {
        let mut got = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while !done(&got) {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await {
                Some(e) if e.event == "heartbeat" => {}
                Some(e) => got.push(e),
                None => break,
            }
        }
        got
    }
}

fn logs_of(events: &[Sse]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e.event == "log")
        .map(|e| e.data["text"].as_str().unwrap_or_default().to_owned())
        .collect()
}

fn steps_of(events: &[Sse]) -> Vec<i64> {
    events
        .iter()
        .filter(|e| e.event == "metrics")
        .flat_map(|e| {
            e.data["runs"][0]["series"][0]["points"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .map(|p| p[0].as_i64().unwrap_or(-1))
        .collect()
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn a_reconnect_with_last_event_id_has_no_gap_and_no_duplicate() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let topics = format!("run:{0}:metrics,run:{0}:logs,run:{0}:state", s.run_id());
    let (status, c, body) = open(&w, Who::Solo, &topics, None).await;
    assert_eq!(status, 200, "{body}");
    let mut c = c.unwrap_or_else(|| panic!("stream"));
    for i in 0..6u64 {
        send_log(
            &w,
            &s,
            i + 1,
            0,
            LogStream::Stdout,
            i * 10,
            format!("log {i}\n").as_bytes(),
        )
        .await;
    }
    send_metrics(&w, &s, 100, points("loss", 0..5, |x| x as f64)).await;
    w.db.write_events(&[crate::pg::event("job", s.job, 1)])
        .await;
    let first = c.until(|e| logs_of(e).len() >= 3).await;
    // Disconnect after the third log line; everything else of the first session is unseen.
    let cut = first
        .iter()
        .position(|e| e.event == "log" && logs_of(std::slice::from_ref(e)) == ["log 2\n"])
        .unwrap_or_else(|| panic!("{first:?}"));
    let seen = &first[..=cut];
    let last = seen[cut]
        .id
        .clone()
        .unwrap_or_else(|| panic!("events carry ids"));
    drop(c);

    for i in 6..9u64 {
        send_log(
            &w,
            &s,
            i + 1,
            0,
            LogStream::Stdout,
            i * 10,
            format!("log {i}\n").as_bytes(),
        )
        .await;
    }
    send_metrics(&w, &s, 101, points("loss", 5..8, |x| x as f64)).await;
    w.db.write_events(&[crate::pg::event("job", s.job, 2)])
        .await;
    let (_, c, _) = open(&w, Who::Solo, &topics, Some(&last)).await;
    let mut c = c.unwrap_or_else(|| panic!("stream"));
    let rest = c
        .until(|e| {
            let all: Vec<Sse> = seen.iter().chain(e.iter()).cloned().collect();
            logs_of(&all).len() >= 9
                && steps_of(&all).len() >= 8
                && all.iter().filter(|x| x.event == "state").count() >= 2
        })
        .await;
    let all: Vec<Sse> = seen.iter().chain(rest.iter()).cloned().collect();
    let expected: Vec<String> = (0..9).map(|i| format!("log {i}\n")).collect();
    assert_eq!(logs_of(&all), expected, "every log line once, in order");
    assert_eq!(
        steps_of(&all),
        (0..8).collect::<Vec<_>>(),
        "every metric point once"
    );
    let seqs: Vec<i64> = all
        .iter()
        .filter(|e| e.event == "state")
        .map(|e| e.data["aggregate_seq"].as_i64().unwrap_or(-1))
        .collect();
    assert_eq!(seqs, vec![1, 2], "every job event once");
    assert!(all.iter().all(|e| e.id.is_some()));
}

/// Records `s.project` as a project of `s.org` (P16's tables), so project topics can find it.
async fn register_project(w: &World, s: &Seeded) {
    let p = w.db.pool();
    let (org, ws) = (s.org, Uuid::now_v7());
    sqlx::query("INSERT INTO organizations (id, slug, name, kind) VALUES ($1, $2, 'o', 'team') ON CONFLICT DO NOTHING")
        .bind(org).bind(org.to_string()).execute(p).await.unwrap_or_else(|e| panic!("{e}"));
    sqlx::query("INSERT INTO workspaces (id, org_id, slug, name) VALUES ($1, $2, $3, 'w')")
        .bind(ws)
        .bind(org)
        .bind(ws.to_string())
        .execute(p)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    sqlx::query(
        "INSERT INTO projects (id, org_id, workspace_id, slug, name) VALUES ($1, $2, $3, $4, 'p')",
    )
    .bind(s.project)
    .bind(org)
    .bind(ws)
    .bind(s.project.to_string())
    .execute(p)
    .await
    .unwrap_or_else(|e| panic!("{e}"));
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn topics_are_authorized_when_the_stream_opens() {
    let w = World::team().await;
    let a = seed_run(&w.db, w.org(Who::A), json!({})).await;
    let b = seed_run(&w.db, w.org(Who::B), json!({})).await;
    register_project(&w, &a).await;
    let mine = format!("run:{}:metrics", a.run_id());
    let jobs = format!("project:{}:jobs", a.project_id());
    for topics in [&mine, &jobs] {
        let (status, _, body) = open(&w, Who::A, topics, None).await;
        assert_eq!(status, 200, "{topics}: {body}");
    }
    // Another org's run or project, alone or next to the caller's own, and a missing run.
    for topics in [
        mine.clone(),
        format!("run:{}:logs,run:{}:state", b.run_id(), a.run_id()),
        jobs.clone(),
        format!("run:{}:state", mrl_domain::ids::RunId::new()),
    ] {
        let (status, _, body) = open(&w, Who::B, &topics, None).await;
        assert_eq!(status, 403, "{topics}: {body}");
    }
    let (status, _, body) = open(&w, Who::A, "run:nope:metrics", None).await;
    assert_eq!(status, 422, "{body}");
    let many = vec![mine.as_str(); 51].join(",");
    let (status, _, body) = open(&w, Who::A, &many, None).await;
    assert_eq!(status, 429, "{body}");
    assert!(body.contains("MRL-QUOTA-220"), "{body}");

    // Ten streams per person; the eleventh is refused until one closes.
    let mut streams = Vec::new();
    for _ in 0..10 {
        let (status, c, _) = open(&w, Who::A, &mine, None).await;
        assert_eq!(status, 200);
        streams.push(c);
    }
    let (status, _, _) = open(&w, Who::A, &mine, None).await;
    assert_eq!(status, 429);
    let (other, _, _) = open(&w, Who::B, &format!("run:{}:metrics", b.run_id()), None).await;
    assert_eq!(other, 200, "the limit is per person");
    streams.pop();
    let mut reopened = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if open(&w, Who::A, &mine, None).await.0 == 200 {
            reopened = true;
            break;
        }
    }
    assert!(reopened, "a closed stream frees its place");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn project_topics_show_the_projects_job_events() {
    let w = World::team().await;
    let a = seed_run(&w.db, w.org(Who::A), json!({})).await;
    let other = seed_run(&w.db, w.org(Who::A), json!({})).await;
    register_project(&w, &a).await;
    let (status, c, body) = open(
        &w,
        Who::A,
        &format!("project:{}:jobs", a.project_id()),
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let mut c = c.unwrap_or_else(|| panic!("stream"));
    w.db.write_events(&[
        crate::pg::event("job", other.job, 1),
        crate::pg::event("job", a.job, 1),
    ])
    .await;
    let got = c.until(|e| !e.is_empty()).await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(
        got[0].data["aggregate_id"],
        mrl_domain::ids::JobId::from_uuid(a.job).to_string()
    );
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn progress_alerts_heartbeats_and_resync_arrive() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let topics = format!("run:{}:progress,alerts", s.run_id());
    let (status, c, body) = open(&w, Who::Solo, &topics, Some("not-a-cursor")).await;
    assert_eq!(status, 200, "{body}");
    let mut c = c.unwrap_or_else(|| panic!("stream"));
    let first = c
        .next(Duration::from_secs(5))
        .await
        .unwrap_or_else(|| panic!("resync"));
    assert_eq!(
        (first.event.as_str(), first.data["reason"].as_str()),
        ("resync", Some("bad_cursor"))
    );
    let hb = c
        .next(Duration::from_secs(5))
        .await
        .unwrap_or_else(|| panic!("heartbeat"));
    assert_eq!(hb.event, "heartbeat");
    assert!(hb.id.is_some(), "heartbeats carry the cursor");

    let p = WorkerBody::Progress(Progress {
        step: 12,
        total_steps: 100,
        ..Progress::default()
    });
    w.deliver(s.node, LaneKind::Control, 1, 0, relay(&s, 1, p))
        .await;
    fire(&w, &s, "metric.loss_spike").await;
    let got = c
        .until(|e| e.iter().any(|x| x.event == "progress") && e.iter().any(|x| x.event == "alert"))
        .await;
    let prog = got
        .iter()
        .find(|e| e.event == "progress")
        .unwrap_or_else(|| panic!("{got:?}"));
    assert_eq!(prog.data["step"], 12);
    let alert = got
        .iter()
        .find(|e| e.event == "alert")
        .unwrap_or_else(|| panic!("{got:?}"));
    assert_eq!(alert.data["code"], "metric.loss_spike");
    assert_eq!(alert.data["state"], "OPEN");
}

async fn fire(w: &World, s: &Seeded, code: &str) -> Uuid {
    let mut tx = w.db.db().begin().await.unwrap_or_else(|e| panic!("{e}"));
    let id = AlertSink::fire(
        &mut tx,
        &NewAlert {
            org: s.org,
            project: Some(s.project),
            scope: AlertScope::Run(s.run),
            source: "metric",
            code: code.into(),
            severity: Severity::Warning,
            title: "The loss jumped.".into(),
            details: json!({}),
            dedup_key: format!("run:{}:{code}", s.run_id()),
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    tx.commit().await.unwrap_or_else(|e| panic!("{e}"));
    id
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn a_cursor_behind_sealed_logs_asks_for_a_resync() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let topics = format!("run:{}:logs", s.run_id());
    let (_, c, _) = open(&w, Who::Solo, &topics, None).await;
    let mut c = c.unwrap_or_else(|| panic!("stream"));
    send_log(&w, &s, 1, 0, LogStream::Stdout, 0, b"one\n").await;
    let got = c.until(|e| !logs_of(e).is_empty()).await;
    assert_eq!(logs_of(&got), ["one\n"]);
    let heartbeat = c.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(heartbeat.event, "heartbeat");
    let last = heartbeat.id.unwrap();
    drop(c);
    // Keep xmin behind the new chunk, as concurrent integration tests can do.
    let mut older = w.db.db().begin().await.unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *older)
        .await
        .unwrap();
    send_log(&w, &s, 2, 0, LogStream::Stdout, 10, b"two\n").await;
    mrl_server::modules::telemetry::logs::seal(&w.tel, Duration::ZERO)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let (_, c, _) = open(&w, Who::Solo, &topics, Some(&last)).await;
    let mut c = c.unwrap_or_else(|| panic!("stream"));
    let heartbeat = c.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(heartbeat.event, "heartbeat");
    assert_eq!(heartbeat.id.as_deref(), Some(last.as_str()));
    older.commit().await.unwrap();

    let events = c.until(|events| !events.is_empty()).await;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].event, "resync");
    assert_eq!(events[0].data["reason"], "logs_sealed");
    // The REST route has the line the stream can no longer show.
    let page = w
        .get(Who::Solo, &format!("/api/v1/runs/{}/logs", s.run_id()))
        .await
        .json();
    let texts: BTreeSet<&str> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["text"].as_str().unwrap())
        .collect();
    assert!(texts.contains("two\n"));
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn reconnect_keeps_large_batches_and_each_progress_and_alert_revision() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let topics = format!("run:{0}:metrics,run:{0}:progress,alerts", s.run_id());
    let (_, c, _) = open(&w, Who::Solo, &topics, None).await;
    let mut c = c.unwrap();
    let initial = c.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(initial.event, "heartbeat");
    drop(c);
    send_metrics(&w, &s, 1, points("loss", 0..501, |x| x as f64)).await;
    for seq in 1..=2 {
        let p = WorkerBody::Progress(Progress {
            step: seq,
            ..Default::default()
        });
        w.deliver(s.node, LaneKind::Control, seq, 0, relay(&s, seq, p))
            .await;
        fire(&w, &s, "metric.test").await;
    }
    let (_, c, _) = open(&w, Who::Solo, &topics, initial.id.as_deref()).await;
    let mut c = c.unwrap();
    let got = c.until(|e| e.len() >= 5).await;
    assert_eq!(steps_of(&got), (0..501).collect::<Vec<_>>());
    let values = |kind: &str, field: &str| {
        got.iter()
            .filter(|e| e.event == kind)
            .map(|e| e.data[field].as_i64().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(values("progress", "step"), [1, 2]);
    assert_eq!(values("alert", "fire_count"), [1, 2]);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn an_open_stream_resyncs_if_a_chunk_is_pruned_before_the_next_poll() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let topic = format!("run:{}:logs", s.run_id());
    let (_, client, _) = open(&w, Who::Solo, &topic, None).await;
    let mut client = client.unwrap();
    assert_eq!(
        client.next(Duration::from_secs(5)).await.unwrap().event,
        "heartbeat"
    );
    let mut tx = w.db.db().begin().await.unwrap();
    // Publish the pruning mark without ever making the chunk visible to a poll.
    let id: i64 = sqlx::query_scalar(
        r#"INSERT INTO log_chunks
        (org_id, run_id, attempt_id, rank, stream, "offset", data)
        VALUES ($1, $2, $3, 0, 'stdout', 0, 'line') RETURNING id"#,
    )
    .bind(s.org)
    .bind(s.run)
    .bind(s.attempt)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    sqlx::query("DELETE FROM log_chunks WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let events = client.until(|e| !e.is_empty()).await;
    assert_eq!(events[0].event, "resync");
    assert_eq!(events[0].data["reason"], "logs_sealed");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn a_metric_poll_resyncs_when_its_partition_is_retired_concurrently() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    sqlx::query(
        "CREATE TABLE p22_retiring_week PARTITION OF metric_points
                 FOR VALUES FROM ('2000-01-03 00:00:00+00') TO ('2000-01-10 00:00:00+00')",
    )
    .execute(w.db.pool())
    .await
    .unwrap();
    let topic = format!("run:{}:metrics", s.run_id());
    let (_, client, _) = open(&w, Who::Solo, &topic, None).await;
    let mut client = client.unwrap();
    let heartbeat = client.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(heartbeat.event, "heartbeat");
    drop(client);

    send_metrics(&w, &s, 1, points("loss", 0..1, |_| 1.0)).await;
    sqlx::query("UPDATE metric_points SET ts = '2000-01-04 00:00:00+00'")
        .execute(w.db.pool())
        .await
        .unwrap();
    let mut retirement = w.db.db().begin().await.unwrap();
    sqlx::query("LOCK TABLE ONLY metric_points IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *retirement)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO telemetry_pruned (source, scope_id, xid, id)
                 SELECT 'm', run_id, xid, id FROM metric_batches WHERE run_id = $1",
    )
    .bind(s.run)
    .execute(&mut *retirement)
    .await
    .unwrap();

    let (_, resumed, _) = open(&w, Who::Solo, &topic, heartbeat.id.as_deref()).await;
    let mut resumed = resumed.unwrap();
    // The poll must reach the lock before retirement commits, on either revision.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_locks
                    WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
                      AND relation = 'metric_points'::regclass AND NOT granted)",
            )
            .fetch_one(w.db.pool())
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the metric poll should wait for the partition lock");
    sqlx::query("DROP TABLE p22_retiring_week")
        .execute(&mut *retirement)
        .await
        .unwrap();
    retirement.commit().await.unwrap();

    let events = resumed.until(|events| !events.is_empty()).await;
    let event = events
        .first()
        .expect("retiring an unread partition must send resync");
    assert_eq!(event.event, "resync");
    assert_eq!(event.data["reason"], "too_old");
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn revisions_follow_write_order_when_a_transaction_gets_its_xid_early() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let topic = format!("alerts,run:{}:progress", s.run_id());
    let (_, client, _) = open(&w, Who::Solo, &topic, None).await;
    let mut client = client.unwrap();
    let mut early = w.db.db().begin().await.unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *early)
        .await
        .unwrap();
    let id = fire(&w, &s, "metric.test").await;
    let p = WorkerBody::Progress(Progress {
        step: 1,
        ..Default::default()
    });
    w.deliver(s.node, LaneKind::Control, 1, 0, relay(&s, 1, p))
        .await;
    sqlx::query("UPDATE alerts SET fire_count = 2, xid = pg_current_xact_id() WHERE id = $1")
        .bind(id)
        .execute(&mut *early)
        .await
        .unwrap();
    sqlx::query("UPDATE run_progress SET step = 2, xid = pg_current_xact_id() WHERE run_id = $1")
        .bind(s.run)
        .execute(&mut *early)
        .await
        .unwrap();
    early.commit().await.unwrap();
    let events = client.until(|events| events.len() >= 4).await;
    let values = |kind: &str, field: &str| {
        events
            .iter()
            .filter(|e| e.event == kind)
            .map(|e| e.data[field].as_i64().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(values("alert", "fire_count"), [1, 2]);
    assert_eq!(values("progress", "step"), [1, 2]);
}
