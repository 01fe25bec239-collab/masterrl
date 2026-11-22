//! P22 alert tests: lifecycle, access and node health alerts.
//! Part of the telemetry integration tests (`super` holds the world and seeding helpers).

use super::{seed_run, Seeded, Who, World};
use mrl_domain::ids::{AlertId, NodeId};
use mrl_server::modules::alerts::{AlertScope, AlertSink, NewAlert, NodeHealthAlerts, Severity};
use mrl_server::modules::events::{Event, EventConsumer};
use serde_json::json;
use uuid::Uuid;

async fn fire(w: &World, s: &Seeded, code: &str, severity: Severity) -> Uuid {
    let mut tx = w.db.db().begin().await.unwrap_or_else(|e| panic!("{e}"));
    let id = AlertSink::fire(
        &mut tx,
        &NewAlert {
            org: s.org,
            project: Some(s.project),
            scope: AlertScope::Run(s.run),
            source: "safety",
            code: code.into(),
            severity,
            title: "The reward stopped rising.".into(),
            details: json!({"window": 50}),
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
async fn an_alert_fires_once_per_key_and_is_acknowledged_then_resolved() {
    let w = World::solo().await;
    let s = seed_run(&w.db, Uuid::nil(), json!({})).await;
    let first = fire(&w, &s, "safety.plateau", Severity::Warning).await;
    let again = fire(&w, &s, "safety.plateau", Severity::Error).await;
    assert_eq!(first, again, "a live alert is refreshed, not duplicated");
    let id = AlertId::from_uuid(first).to_string();

    let list = w.get(Who::Solo, "/api/v1/alerts?state=OPEN").await;
    assert_eq!(list.status, 200, "{}", list.text());
    let a = &list.json()["items"][0];
    assert_eq!(
        (
            a["id"].as_str(),
            a["fire_count"].as_i64(),
            a["severity"].as_str()
        ),
        (Some(id.as_str()), Some(2), Some("error"))
    );
    assert_eq!(a["scope_id"], s.run_id());

    let ack = w
        .post(Who::Solo, &format!("/api/v1/alerts/{id}:ack"), json!({}))
        .await;
    assert_eq!(ack.status, 200, "{}", ack.text());
    assert_eq!(ack.json()["state"], "ACKED");
    assert_eq!(ack.json()["acked_by"], "local-owner");
    let twice = w
        .post(Who::Solo, &format!("/api/v1/alerts/{id}:ack"), json!({}))
        .await;
    assert_eq!(
        (twice.status, twice.json()["state"].as_str()),
        (200, Some("ACKED"))
    );
    let done = w
        .post(
            Who::Solo,
            &format!("/api/v1/alerts/{id}:resolve"),
            json!({}),
        )
        .await;
    assert_eq!(
        (done.status, done.json()["state"].as_str()),
        (200, Some("RESOLVED"))
    );
    let late = w
        .post(Who::Solo, &format!("/api/v1/alerts/{id}:ack"), json!({}))
        .await;
    assert_eq!(late.status, 409, "{}", late.text());
    assert_eq!(late.json()["code"], "MRL-STATE-220");
    let bad = w
        .post(Who::Solo, &format!("/api/v1/alerts/{id}:snooze"), json!({}))
        .await;
    assert_eq!(bad.status, 404);

    // Once resolved, the next fire opens a new alert.
    let next = fire(&w, &s, "safety.plateau", Severity::Warning).await;
    assert_ne!(next, first);
    let open = w.get(Who::Solo, "/api/v1/alerts?state=OPEN").await.json();
    assert_eq!(open["items"].as_array().unwrap().len(), 1);
    let all = w.get(Who::Solo, "/api/v1/alerts?limit=1").await.json();
    assert_eq!(
        all["items"][0]["id"],
        AlertId::from_uuid(next).to_string(),
        "newest first"
    );
    let cursor = all["next_cursor"]
        .as_str()
        .unwrap_or_else(|| panic!("a second page"));
    let older = w
        .get(
            Who::Solo,
            &format!("/api/v1/alerts?limit=1&cursor={cursor}"),
        )
        .await
        .json();
    assert_eq!(older["items"][0]["id"], id);
    assert_eq!(
        w.get(Who::Solo, "/api/v1/alerts?state=SNOOZED")
            .await
            .status,
        422
    );

    // One run's alerts only; an unknown run is 404.
    let other = seed_run(&w.db, Uuid::nil(), json!({})).await;
    fire(&w, &other, "safety.other", Severity::Info).await;
    let one = w
        .get(Who::Solo, &format!("/api/v1/alerts?run={}", s.run_id()))
        .await;
    assert_eq!(one.status, 200, "{}", one.text());
    let body = one.json();
    let codes: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["code"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        codes,
        vec!["safety.plateau", "safety.plateau"],
        "both of this run's alerts, not the other run's"
    );
    let missing = w
        .get(
            Who::Solo,
            &format!("/api/v1/alerts?run={}", mrl_domain::ids::RunId::new()),
        )
        .await;
    assert_eq!(missing.status, 404);
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn another_orgs_alerts_are_hidden() {
    let w = World::team().await;
    let a = seed_run(&w.db, w.org(Who::A), json!({})).await;
    let id = AlertId::from_uuid(fire(&w, &a, "safety.nan", Severity::Critical).await).to_string();
    let mine = w.get(Who::A, "/api/v1/alerts").await.json();
    assert_eq!(mine["items"].as_array().unwrap().len(), 1);
    let theirs = w.get(Who::B, "/api/v1/alerts").await.json();
    assert!(theirs["items"].as_array().unwrap().is_empty());
    let ack = w
        .post(Who::B, &format!("/api/v1/alerts/{id}:ack"), json!({}))
        .await;
    assert_eq!(ack.status, 404, "{}", ack.text());
    let state: String = sqlx::query_scalar("SELECT state FROM alerts")
        .fetch_one(w.db.pool())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(state, "OPEN");
}

fn node_event(node: Uuid, kind: &str, reason: &str, seq: i64) -> Event {
    Event {
        id: seq,
        event_id: Uuid::now_v7(),
        org_id: None,
        aggregate_kind: "node".into(),
        aggregate_id: node,
        aggregate_seq: seq,
        event_type: kind.into(),
        schema_version: 1,
        payload: json!({ "node_id": NodeId::from_uuid(node).to_string(), "reason": reason }),
        trace_context: None,
        created_at: time::OffsetDateTime::now_utc(),
        xid: 0,
    }
}

async fn live_codes(w: &World) -> Vec<String> {
    sqlx::query_scalar("SELECT code FROM alerts WHERE state = 'OPEN' ORDER BY code")
        .fetch_all(w.db.pool())
        .await
        .unwrap_or_else(|e| panic!("{e}"))
}

#[tokio::test]
#[ignore = "needs PostgreSQL (just dev)"]
async fn node_health_events_open_and_resolve_node_alerts() {
    let w = World::solo().await;
    let node = Uuid::now_v7();
    let c = NodeHealthAlerts;
    let mut seq = 0;
    let mut deliver = |kind: &'static str, reason: &'static str| {
        seq += 1;
        node_event(node, kind, reason, seq)
    };
    for (event, want) in [
        (
            deliver("node.degraded", "health:disk_low,gpu_hot"),
            vec!["node.disk_low", "node.gpu_hot"],
        ),
        (
            deliver("node.degraded", "health:gpu_hot"),
            vec!["node.gpu_hot"],
        ),
        (
            deliver("node.disconnected", ""),
            vec!["node.gpu_hot", "node.offline"],
        ),
        (deliver("node.reconnected", ""), vec!["node.gpu_hot"]),
        (deliver("node.recovered", ""), vec![]),
        (
            deliver("node.degraded", "heartbeat_late"),
            vec!["node.heartbeat_late"],
        ),
        (deliver("node.decommissioned", ""), vec![]),
    ] {
        let mut tx = w.db.db().begin().await.unwrap_or_else(|e| panic!("{e}"));
        c.handle(&mut tx, &event)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        tx.commit().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(live_codes(&w).await, want, "after {}", event.event_type);
    }
    let list = w
        .get(Who::Solo, "/api/v1/alerts?scope_kind=node")
        .await
        .json();
    let first = &list["items"]
        .as_array()
        .unwrap()
        .last()
        .cloned()
        .unwrap_or_default();
    assert_eq!(first["scope_id"], NodeId::from_uuid(node).to_string());
    assert_eq!(first["state"], "RESOLVED");
    assert_eq!(first["resolved_by"], "system");
}
