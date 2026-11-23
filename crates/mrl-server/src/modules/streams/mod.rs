//! Live streams: `GET /streams?topics=…` (server-sent events, Doc 09 §6.4, D-04-07).
//!
//! Topics (comma-separated, at most `live.max_topics_per_stream`):
//!
//! | Topic | Event | Source | Needs |
//! |---|---|---|---|
//! | `run:<run>:state` | `state` | job and attempt events in `outbox_events` | `runs.read` |
//! | `run:<run>:metrics` | `metrics` | `metric_batches` → their points | `metrics_logs.read` |
//! | `run:<run>:logs` | `log` | `log_chunks` | `metrics_logs.read` |
//! | `run:<run>:progress` | `progress` | `run_progress` | `runs.read` |
//! | `project:<prj>:jobs` | `state` | job events of the project | `runs.read` |
//! | `alerts` | `alert` | `alerts` rows of your org you may see | `streams.subscribe` |
//!
//! Every topic also needs `streams.subscribe`. Each topic is authorized when the stream opens;
//! any topic refused, hidden or missing answers 403 for the whole request.
//!
//! Every source is tailed in `(xid, id)` order below the snapshot `xmin`, like the outbox
//! dispatcher, so a row committed late is never skipped. Each event's `id` is the composite
//! cursor of all five sources (`v2~o.<xid>.<id>~m.…~l.…~p.…~a.…`);
//! reconnecting with `Last-Event-ID` resumes each source exactly: no gap, no duplicate.
//! Heartbeats carry the cursor too. A `metrics` event holds every new point of one poll
//! (polls are at least 500 ms apart, so a series updates at most twice a second). `resync` asks the client to
//! re-read through the REST routes: an unusable cursor, logs sealed or events removed since
//! the cursor, a client too slow to keep up (the stream then ends), or the server stopping.

use crate::app::{AppState, ServerModule};
use crate::db::Db;
use crate::error::{ApiError, ErrorCode};
use crate::modules::jobs::scope::{self, org_text};
use crate::modules::jobs::{JobsError, Projects};
use crate::modules::runs::api::find_run;
use crate::modules::telemetry::{codes, field_error, LiveConfig};
use crate::seams::{Decision, RequestContext, Resource};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use futures_util::StreamExt;
use mrl_domain::ids::{AttemptId, JobId, ProjectId, RunId};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{PgConnection, Row};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use utoipa::OpenApi;
use uuid::Uuid;

/// Rows read per source per poll.
const BATCH: i64 = 200;
/// Target points per poll. A single larger batch is sent whole.
const TARGET_POINTS: i64 = 5000;
/// Polls buffered behind the HTTP response.
const BUFFERED_POLLS: usize = 2;

/// One topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topic {
    /// `run:<run>:<what>`.
    Run(Uuid, RunTopic),
    /// `project:<prj>:jobs`.
    ProjectJobs(Uuid),
    /// `alerts`.
    Alerts,
}

/// What a run topic shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunTopic {
    /// Job and attempt events.
    State,
    /// New metric points.
    Metrics,
    /// New log chunks.
    Logs,
    /// Progress reports.
    Progress,
}

impl Topic {
    /// Parses `run:run_…:state`, `project:prj_…:jobs` or `alerts`.
    pub fn parse(s: &str) -> Option<Self> {
        if s == "alerts" {
            return Some(Self::Alerts);
        }
        let mut parts = s.split(':');
        let (kind, id, what) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() {
            return None;
        }
        match kind {
            "run" => {
                let run = id.parse::<RunId>().ok()?.as_uuid();
                let what = match what {
                    "state" => RunTopic::State,
                    "metrics" => RunTopic::Metrics,
                    "logs" => RunTopic::Logs,
                    "progress" => RunTopic::Progress,
                    _ => return None,
                };
                Some(Self::Run(run, what))
            }
            "project" if what == "jobs" => {
                Some(Self::ProjectJobs(id.parse::<ProjectId>().ok()?.as_uuid()))
            }
            _ => None,
        }
    }
}

/// Where a `(xid, id)`-ordered source has been read to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Pos {
    xid: i64,
    id: i64,
}

/// The read position of every source; an event's `id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    o: Pos,
    m: Pos,
    l: Pos,
    p: Pos,
    a: Pos,
}

impl Cursor {
    /// Every source at now, including transactions still in flight.
    fn start(xmin: i64) -> Self {
        let p = Pos { xid: xmin, id: 0 };
        Self {
            o: p,
            m: p,
            l: p,
            p,
            a: p,
        }
    }

    /// The `id:` text.
    pub fn encode(&self) -> String {
        let mut text = String::from("v2");
        for (key, pos) in [
            ("o", self.o),
            ("m", self.m),
            ("l", self.l),
            ("p", self.p),
            ("a", self.a),
        ] {
            use std::fmt::Write as _;
            let _ = write!(text, "~{key}.{}.{}", pos.xid, pos.id);
        }
        text
    }

    /// Reads a current cursor. Older formats require a fresh REST snapshot.
    pub fn parse(s: &str) -> Option<Self> {
        let mut parts = s.split('~');
        if parts.next()? != "v2" {
            return None;
        }
        let mut pos = |key: &str| {
            let mut f = parts.next()?.split('.');
            if f.next()? != key {
                return None;
            }
            let xid = f.next()?.parse::<i64>().ok()?;
            let id = f.next()?.parse::<i64>().ok()?;
            (xid >= 0 && id >= 0 && f.next().is_none()).then_some(Pos { xid, id })
        };
        let cursor = Self {
            o: pos("o")?,
            m: pos("m")?,
            l: pos("l")?,
            p: pos("p")?,
            a: pos("a")?,
        };
        parts.next().is_none().then_some(cursor)
    }

    fn after_snapshot(&self, xmax: i64) -> bool {
        [self.o, self.m, self.l, self.p, self.a]
            .iter()
            .any(|p| p.xid > xmax || (p.xid == xmax && p.id != 0))
    }
}

/// The authorized subscription.
#[derive(Debug, Default, Clone)]
struct Sub {
    /// `run → job` for state topics.
    state_jobs: Vec<Uuid>,
    projects: Vec<Uuid>,
    metrics: Vec<Uuid>,
    logs: Vec<Uuid>,
    progress: Vec<Uuid>,
    /// The org whose alerts are shown.
    alerts: Option<Uuid>,
}

/// Open streams per principal on this server.
struct Hub {
    db: Db,
    config: LiveConfig,
    projects: Projects,
    open: Mutex<HashMap<String, u32>>,
}

/// A stream's place in [`Hub::open`]; freed when the stream ends.
struct Slot {
    hub: Arc<Hub>,
    who: String,
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Ok(mut open) = self.hub.open.lock() {
            if let Some(n) = open.get_mut(&self.who) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    open.remove(&self.who);
                }
            }
        }
    }
}

fn too_many(detail: String) -> JobsError {
    JobsError::domain(
        codes::TOO_MANY_STREAMS,
        StatusCode::TOO_MANY_REQUESTS,
        detail,
    )
}

impl Hub {
    fn take_slot(self: &Arc<Self>, who: &str) -> Result<Slot, JobsError> {
        let mut open = self.open.lock().map_err(|_| {
            JobsError::Api(ApiError::new(
                ErrorCode::Internal,
                "stream limits are unavailable",
            ))
        })?;
        let n = open.entry(who.to_owned()).or_insert(0);
        if *n >= self.config.max_streams_per_principal {
            return Err(too_many(format!(
                "You already have {n} live views open; the limit is {}.",
                self.config.max_streams_per_principal
            )));
        }
        *n += 1;
        Ok(Slot {
            hub: self.clone(),
            who: who.to_owned(),
        })
    }
}

fn refused(topic: &str) -> JobsError {
    JobsError::Api(ApiError::new(
        ErrorCode::Forbidden,
        format!("You may not watch {topic}."),
    ))
}

/// Allow, or 403 for Deny and Hide alike (a hidden topic must not differ from a refused one).
async fn allow(
    state: &AppState,
    ctx: &RequestContext,
    topic: &str,
    actions: &[&str],
    res: &Resource,
) -> Result<(), JobsError> {
    for action in actions {
        if state.seams.authorizer.authorize(ctx, action, res).await? != Decision::Allow {
            return Err(refused(topic));
        }
    }
    Ok(())
}

async fn authorize(
    hub: &Hub,
    state: &AppState,
    ctx: &RequestContext,
    topics: &[(String, Topic)],
) -> Result<Sub, JobsError> {
    let mut sub = Sub::default();
    for (text, topic) in topics {
        match *topic {
            Topic::Run(run, what) => {
                let row = find_run(&hub.db, run).await?.ok_or_else(|| refused(text))?;
                let res = scope::resource(
                    "run",
                    Some(RunId::from_uuid(run).to_string()),
                    row.org_id,
                    row.project_id,
                );
                let action = match what {
                    RunTopic::State | RunTopic::Progress => "runs.read",
                    RunTopic::Metrics | RunTopic::Logs => "metrics_logs.read",
                };
                allow(state, ctx, text, &["streams.subscribe", action], &res).await?;
                match what {
                    RunTopic::State => sub.state_jobs.push(row.job_id),
                    RunTopic::Metrics => sub.metrics.push(run),
                    RunTopic::Logs => sub.logs.push(run),
                    RunTopic::Progress => sub.progress.push(run),
                }
            }
            Topic::ProjectJobs(project) => {
                let id = ProjectId::from_uuid(project);
                let org = match hub.projects.org_for(ctx, id).await {
                    Ok(org) => org,
                    Err(e @ JobsError::Seam(_)) => return Err(e),
                    Err(_) => return Err(refused(text)),
                };
                let res = scope::resource("project", Some(id.to_string()), org, project);
                allow(state, ctx, text, &["streams.subscribe", "runs.read"], &res).await?;
                sub.projects.push(project);
            }
            Topic::Alerts => {
                let org = scope::tenant(ctx)?;
                let res = Resource {
                    kind: "alert".into(),
                    id: None,
                    org: org_text(org),
                    project: None,
                };
                allow(state, ctx, text, &["streams.subscribe"], &res).await?;
                sub.alerts = Some(org);
            }
        }
    }
    Ok(sub)
}

/// Query parameters of `GET /streams`.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct StreamParams {
    /// Comma-separated topics, like `run:run_…:metrics,alerts`.
    pub topics: Option<String>,
}

/// `GET /streams`: live events for the topics (server-sent events).
#[utoipa::path(get, path = "/streams", tag = "streams", operation_id = "open_stream",
    params(StreamParams,
        ("Last-Event-ID" = Option<String>, Header, description = "The last event id received, to resume")),
    responses((status = 200, description = "An event stream", content_type = "text/event-stream"),
        (status = 403, description = "A topic is not allowed, hidden or missing"),
        (status = 422, description = "A topic is not understood"),
        (status = 429, description = "Too many streams or topics (MRL-QUOTA-220)")))]
async fn open(
    State(state): State<AppState>,
    Extension(hub): Extension<Arc<Hub>>,
    ctx: RequestContext,
    headers: HeaderMap,
    Query(q): Query<StreamParams>,
) -> Result<axum::response::Response, JobsError> {
    let texts: Vec<String> = q
        .topics
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
        .collect();
    if texts.is_empty() {
        return Err(field_error(
            "topics",
            "Name at least one topic.",
            "Pass topics=run:<run id>:metrics, for example.",
        ));
    }
    let max = usize::try_from(hub.config.max_topics_per_stream).unwrap_or(usize::MAX);
    if texts.len() > max {
        return Err(too_many(format!(
            "{} topics asked for; one live view may watch {max}.",
            texts.len()
        )));
    }
    let mut topics = Vec::with_capacity(texts.len());
    for t in texts {
        let topic = Topic::parse(&t).ok_or_else(|| {
            field_error(
                "topics",
                &format!("{t:?} is not a topic the app knows."),
                "Use run:<run id>:state|metrics|logs|progress, project:<project id>:jobs or alerts.",
            )
        })?;
        topics.push((t, topic));
    }
    let sub = authorize(&hub, &state, &ctx, &topics).await?;
    let slot = hub.take_slot(&ctx.principal.id)?;
    let resume = headers
        .get("last-event-id")
        .map(|v| v.to_str().unwrap_or(""));
    let mut live = Live::begin(hub.clone(), state, ctx, sub, resume).await?;
    let poll = Duration::from_millis(hub.config.stream_poll_millis);
    let (tx, rx) = tokio::sync::mpsc::channel(BUFFERED_POLLS);
    let (end_tx, end_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        loop {
            let events = if let Some(first) = live.first.take() {
                first
            } else {
                tokio::select! {
                    () = tx.closed() => return,
                    () = tokio::time::sleep(poll) => {}
                }
                tokio::select! {
                    () = tx.closed() => return,
                    events = live.step() => events,
                }
            };
            if !events.is_empty() {
                match tx.try_send(events) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        let _ = end_tx.send(event("resync", None, &json!({"reason": "too_slow"})));
                        return;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return,
                }
            }
            if live.done {
                return;
            }
        }
    });
    let events = tokio_stream::wrappers::ReceiverStream::new(rx)
        .flat_map(futures_util::stream::iter)
        .chain(futures_util::stream::once(end_rx).filter_map(|e| async { e.ok() }))
        .map(move |e| {
            let _ = &slot; // The body owns the connection slot until it closes.
            Ok::<_, Infallible>(e)
        });
    Ok(Sse::new(events).into_response())
}

/// One open stream.
struct Live {
    hub: Arc<Hub>,
    state: AppState,
    ctx: RequestContext,
    sub: Sub,
    cursor: Cursor,
    /// Events to send at once (a resync after the cursor check).
    first: Option<Vec<Event>>,
    last_sent: Instant,
    done: bool,
}

fn event(kind: &str, cursor: Option<&Cursor>, data: &Value) -> Event {
    let e = Event::default().event(kind).data(data.to_string());
    match cursor {
        Some(c) => e.id(c.encode()),
        None => e,
    }
}

impl Live {
    async fn begin(
        hub: Arc<Hub>,
        state: AppState,
        ctx: RequestContext,
        sub: Sub,
        resume: Option<&str>,
    ) -> Result<Self, JobsError> {
        let (xmin, xmax): (i64, i64) = {
            let r = sqlx::query(
                "SELECT pg_snapshot_xmin(s)::text::bigint AS lo, pg_snapshot_xmax(s)::text::bigint AS hi
                   FROM pg_current_snapshot() s",
            )
            .fetch_one(hub.db.pool())
            .await?;
            (r.try_get("lo")?, r.try_get("hi")?)
        };
        let fresh = Cursor::start(xmin);
        let mut first = Vec::new();
        let cursor = match resume.map(Cursor::parse) {
            None => fresh,
            Some(None) => {
                first.push(event(
                    "resync",
                    Some(&fresh),
                    &json!({"reason": "bad_cursor"}),
                ));
                fresh
            }
            Some(Some(c)) if c.after_snapshot(xmax) => {
                first.push(event(
                    "resync",
                    Some(&fresh),
                    &json!({"reason": "bad_cursor"}),
                ));
                fresh
            }
            Some(Some(c)) => c,
        };
        Ok(Self {
            hub,
            state,
            ctx,
            sub,
            cursor,
            first: Some(first),
            last_sent: Instant::now(),
            done: false,
        })
    }

    /// One poll: new events, a heartbeat when idle, or a closing resync.
    async fn step(&mut self) -> Vec<Event> {
        if self.state.is_draining() {
            self.done = true;
            return vec![event(
                "resync",
                Some(&self.cursor),
                &json!({"reason": "server_stopping"}),
            )];
        }
        let mut out = Vec::new();
        if let Err(e) = self.poll(&mut out).await {
            tracing::warn!(code = %e.code(), error = %e, "stream poll failed; trying again");
        }
        if !out.is_empty() {
            self.last_sent = Instant::now();
        } else if self.last_sent.elapsed()
            >= Duration::from_secs(self.hub.config.stream_heartbeat_seconds)
        {
            self.last_sent = Instant::now();
            out.push(event("heartbeat", Some(&self.cursor), &json!({})));
        }
        out
    }

    /// Read the prune markers and retained rows in the same database snapshot.
    async fn poll(&mut self, out: &mut Vec<Event>) -> Result<(), JobsError> {
        let mut tx = self.hub.db.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        if !self.sub.metrics.is_empty() {
            // Partition drops can hide raw points from a snapshot that predates their prune marker.
            // A retirement holds the table while it archives a week; give up after a short wait
            // so the stream keeps sending heartbeats and polls again.
            sqlx::query("SET LOCAL lock_timeout = '2s'")
                .execute(&mut *tx)
                .await?;
            sqlx::query("LOCK TABLE ONLY metric_points IN ACCESS SHARE MODE")
                .execute(&mut *tx)
                .await?;
        }
        let xmin: i64 =
            sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint")
                .fetch_one(&mut *tx)
                .await?;
        if let Some((pruned_xid, reason)) = too_old(&mut tx, &self.sub, &self.cursor).await? {
            // Wait until the safe tail can move past the deleted rows. Resetting to an
            // older point would send another resync every poll while a transaction is open.
            if pruned_xid >= xmin {
                return Ok(());
            }
            self.cursor = Cursor::start(xmin);
            out.push(event(
                "resync",
                Some(&self.cursor),
                &json!({"reason": reason}),
            ));
            return Ok(());
        }
        let end = Pos {
            xid: xmin - 1,
            id: i64::MAX,
        };
        if !self.sub.state_jobs.is_empty() || !self.sub.projects.is_empty() {
            let rows = sqlx::query(
                "SELECT o.id, o.xid::text::bigint AS x, o.type, o.aggregate_kind, o.aggregate_id,
                        o.aggregate_seq, o.payload, o.created_at
                   FROM outbox_events o
                  WHERE (o.xid, o.id) > (($1::bigint)::text::xid8, $2)
                    AND o.xid < ($3::bigint)::text::xid8
                    AND ((o.aggregate_kind = 'job' AND (o.aggregate_id = ANY($4)
                           OR o.aggregate_id IN (SELECT id FROM jobs WHERE project_id = ANY($5))))
                      OR (o.aggregate_kind = 'attempt'
                           AND o.aggregate_id IN (SELECT id FROM job_attempts WHERE job_id = ANY($4))))
                  ORDER BY o.xid, o.id LIMIT $6",
            )
            .bind(self.cursor.o.xid)
            .bind(self.cursor.o.id)
            .bind(xmin)
            .bind(&self.sub.state_jobs)
            .bind(&self.sub.projects)
            .bind(BATCH)
            .fetch_all(&mut *tx)
            .await?;
            for r in &rows {
                self.cursor.o = Pos {
                    xid: r.try_get("x")?,
                    id: r.try_get("id")?,
                };
                let kind: String = r.try_get("aggregate_kind")?;
                let agg: Uuid = r.try_get("aggregate_id")?;
                let shown = match kind.as_str() {
                    "attempt" => AttemptId::from_uuid(agg).to_string(),
                    _ => JobId::from_uuid(agg).to_string(),
                };
                let at: time::OffsetDateTime = r.try_get("created_at")?;
                let data = json!({
                    "type": r.try_get::<String, _>("type")?,
                    "aggregate_kind": kind,
                    "aggregate_id": shown,
                    "aggregate_seq": r.try_get::<i64, _>("aggregate_seq")?,
                    "payload": r.try_get::<Value, _>("payload")?,
                    "at": at.format(&time::format_description::well_known::Rfc3339).unwrap_or_default(),
                });
                out.push(event("state", Some(&self.cursor), &data));
            }
            if i64::try_from(rows.len()).unwrap_or(0) < BATCH {
                self.cursor.o = self.cursor.o.max(end);
            }
        }
        if !self.sub.metrics.is_empty() {
            self.metrics(&mut tx, xmin, end, out).await?;
        }
        if !self.sub.logs.is_empty() {
            self.logs(&mut tx, xmin, end, out).await?;
        }
        if !self.sub.progress.is_empty() {
            let rows = sqlx::query(
                "SELECT id, run_id, payload, xid::text::bigint AS x
                   FROM telemetry_progress_events
                  WHERE run_id = ANY($1) AND (xid, id) > (($2::bigint)::text::xid8, $3)
                    AND xid < ($4::bigint)::text::xid8
                  ORDER BY xid, id LIMIT $5",
            )
            .bind(&self.sub.progress)
            .bind(self.cursor.p.xid)
            .bind(self.cursor.p.id)
            .bind(xmin)
            .bind(BATCH)
            .fetch_all(&mut *tx)
            .await?;
            for r in &rows {
                let mut data: Value = r.try_get("payload")?;
                let attempt = data["attempt_id"]
                    .as_str()
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .ok_or_else(|| sqlx::Error::Protocol("progress event has no attempt".into()))?;
                let data = json!({
                    "run_id": RunId::from_uuid(r.try_get("run_id")?).to_string(),
                    "attempt_id": AttemptId::from_uuid(attempt).to_string(),
                    "step": data["step"].take(),
                    "total_steps": data["total_steps"].take(),
                    "epoch": data["epoch"].take(),
                    "phase": data["phase"].take(),
                    "tokens_seen": data["tokens_seen"].take(),
                    "eta_seconds": data["eta_seconds"].take(),
                    "updated_at": data["updated_at"].take(),
                });
                self.cursor.p = row_pos(r)?;
                out.push(event("progress", Some(&self.cursor), &data));
            }
            if i64::try_from(rows.len()).unwrap_or(0) < BATCH {
                self.cursor.p = self.cursor.p.max(end);
            }
        }
        if let Some(org) = self.sub.alerts {
            let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT {}, a.xid::text::bigint AS x, a.event_id AS tail_id FROM telemetry_alert_events a
                   LEFT JOIN runs r ON a.scope_kind = 'run' AND r.id = a.scope_id
                  WHERE a.org_id = $1 AND (a.xid, a.event_id) > (($2::bigint)::text::xid8, $3)
                    AND a.xid < ($4::bigint)::text::xid8
                  ORDER BY a.xid, a.event_id LIMIT $5",
                crate::modules::alerts::ALERT_COLS
            )))
            .bind(org)
            .bind(self.cursor.a.xid)
            .bind(self.cursor.a.id)
            .bind(xmin)
            .bind(BATCH)
            .fetch_all(&mut *tx)
            .await?;
            // P16 authorization uses the pool too, so release this connection first.
            tx.commit().await?;
            for r in &rows {
                let row = crate::modules::alerts::row_of(r)?;
                let visible = crate::modules::alerts::visible(&self.state, &self.ctx, &row).await?;
                self.cursor.a = Pos {
                    xid: r.try_get("x")?,
                    id: r.try_get("tail_id")?,
                };
                if visible {
                    let data = serde_json::to_value(&row.view).unwrap_or(Value::Null);
                    out.push(event("alert", Some(&self.cursor), &data));
                }
            }
            if i64::try_from(rows.len()).unwrap_or(0) < BATCH {
                self.cursor.a = self.cursor.a.max(end);
            }
        } else {
            tx.commit().await?;
        }
        Ok(())
    }

    /// New metric batches of the subscribed runs as one `metrics` event.
    async fn metrics(
        &mut self,
        db: &mut PgConnection,
        xmin: i64,
        end: Pos,
        out: &mut Vec<Event>,
    ) -> Result<(), JobsError> {
        let batches = sqlx::query(
            "WITH candidates AS (
                 SELECT id, xid, point_count FROM metric_batches
                  WHERE run_id = ANY($1) AND (xid, id) > (($2::bigint)::text::xid8, $3)
                    AND xid < ($4::bigint)::text::xid8
                  ORDER BY xid, id LIMIT $5
             ), sized AS (
                 SELECT *, sum(point_count) OVER (ORDER BY xid, id) AS total FROM candidates
             )
             SELECT id, xid::text::bigint AS x, total
               FROM sized ORDER BY xid, id",
        )
        .bind(&self.sub.metrics)
        .bind(self.cursor.m.xid)
        .bind(self.cursor.m.id)
        .bind(xmin)
        .bind(BATCH)
        .fetch_all(&mut *db)
        .await?;
        let mut included = batches.len().min(1);
        while let Some(batch) = batches.get(included) {
            if batch.try_get::<i64, _>("total")? > TARGET_POINTS {
                break;
            }
            included += 1;
        }
        let full = included < batches.len() || i64::try_from(batches.len()).unwrap_or(0) >= BATCH;
        let batches = &batches[..included];
        if let Some(last) = batches.last() {
            let ids: Vec<i64> = batches
                .iter()
                .map(|b| b.try_get("id"))
                .collect::<Result<_, _>>()?;
            let points = sqlx::query(
                "SELECT s.run_id, p.series_id, s.name, s.split, p.step, p.value,
                        floor(extract(epoch FROM p.ts) * 1000)::bigint AS ms
                   FROM metric_points p JOIN metric_series s ON s.id = p.series_id
                  WHERE p.batch_id = ANY($1) ORDER BY p.series_id, p.batch_id, p.step",
            )
            .bind(&ids)
            .fetch_all(&mut *db)
            .await?;
            // run → series → (name, split, points)
            type Line = (String, String, Vec<(i64, f64, i64)>);
            let mut runs: BTreeMap<Uuid, BTreeMap<i64, Line>> = BTreeMap::new();
            for p in &points {
                let line = runs
                    .entry(p.try_get("run_id")?)
                    .or_default()
                    .entry(p.try_get("series_id")?)
                    .or_insert_with(|| (String::new(), String::new(), Vec::new()));
                if line.0.is_empty() {
                    line.0 = p.try_get("name")?;
                    line.1 = p.try_get("split")?;
                }
                line.2
                    .push((p.try_get("step")?, p.try_get("value")?, p.try_get("ms")?));
            }
            self.cursor.m = Pos {
                xid: last.try_get("x")?,
                id: last.try_get("id")?,
            };
            if !runs.is_empty() {
                let data = json!({ "runs": runs.into_iter().map(|(run, series)| json!({
                    "run_id": RunId::from_uuid(run).to_string(),
                    "series": series.into_iter().map(|(id, (name, split, pts))| {
                        json!({"series_id": id, "name": name, "split": split, "truncated": false,
                               "points": pts.iter().map(|(s, v, t)| json!([s, v, t])).collect::<Vec<_>>()})
                    }).collect::<Vec<_>>(),
                })).collect::<Vec<_>>() });
                out.push(event("metrics", Some(&self.cursor), &data));
            }
        }
        if !full {
            self.cursor.m = self.cursor.m.max(end);
        }
        Ok(())
    }

    /// New log chunks of the subscribed runs, one `log` event each.
    async fn logs(
        &mut self,
        db: &mut PgConnection,
        xmin: i64,
        end: Pos,
        out: &mut Vec<Event>,
    ) -> Result<(), JobsError> {
        let rows = sqlx::query(
            "SELECT id, xid::text::bigint AS x, run_id, attempt_id, rank, stream, \"offset\", data
               FROM log_chunks
              WHERE run_id = ANY($1) AND (xid, id) > (($2::bigint)::text::xid8, $3)
                AND xid < ($4::bigint)::text::xid8
              ORDER BY xid, id LIMIT $5",
        )
        .bind(&self.sub.logs)
        .bind(self.cursor.l.xid)
        .bind(self.cursor.l.id)
        .bind(xmin)
        .bind(BATCH)
        .fetch_all(&mut *db)
        .await?;
        for r in &rows {
            self.cursor.l = Pos {
                xid: r.try_get("x")?,
                id: r.try_get("id")?,
            };
            let data: Vec<u8> = r.try_get("data")?;
            let body = json!({
                "run_id": RunId::from_uuid(r.try_get("run_id")?).to_string(),
                "id": self.cursor.l.id,
                "attempt_id": AttemptId::from_uuid(r.try_get("attempt_id")?).to_string(),
                "rank": r.try_get::<i32, _>("rank")?,
                "stream": r.try_get::<String, _>("stream")?,
                "offset": r.try_get::<i64, _>("offset")?,
                "text": String::from_utf8_lossy(&data),
            });
            out.push(event("log", Some(&self.cursor), &body));
        }
        let full = i64::try_from(rows.len()).unwrap_or(0) >= BATCH;
        if !full {
            self.cursor.l = self.cursor.l.max(end);
        }
        Ok(())
    }
}

fn row_pos(row: &sqlx::postgres::PgRow) -> Result<Pos, sqlx::Error> {
    Ok(Pos {
        xid: row.try_get("x")?,
        id: row.try_get("id")?,
    })
}

async fn too_old(
    db: &mut PgConnection,
    sub: &Sub,
    c: &Cursor,
) -> Result<Option<(i64, &'static str)>, JobsError> {
    let trimmed = sqlx::query_as::<_, (i64, String)>(
        "WITH scopes(source, scope, x, i) AS (
             SELECT 'l', unnest($1::uuid[]), $2::bigint, $3::bigint
             UNION ALL SELECT 'm', unnest($4::uuid[]), $5, $6
             UNION ALL SELECT 'p', unnest($7::uuid[]), $8, $9
             UNION ALL SELECT 'a', $10::uuid, $11, $12
             UNION ALL SELECT 'o', id, $13, $14 FROM jobs
                  WHERE id = ANY($15) OR project_id = ANY($16)
             UNION ALL SELECT 'o', id, $13, $14 FROM job_attempts WHERE job_id = ANY($15)
         )
         SELECT t.xid::text::bigint, t.source FROM scopes s JOIN telemetry_pruned t
              ON t.source = s.source AND t.scope_id = s.scope
            WHERE (t.xid, t.id) > (s.x::text::xid8, s.i)
            ORDER BY t.xid DESC, (t.source = 'l') DESC LIMIT 1",
    )
    .bind(&sub.logs)
    .bind(c.l.xid)
    .bind(c.l.id)
    .bind(&sub.metrics)
    .bind(c.m.xid)
    .bind(c.m.id)
    .bind(&sub.progress)
    .bind(c.p.xid)
    .bind(c.p.id)
    .bind(sub.alerts)
    .bind(c.a.xid)
    .bind(c.a.id)
    .bind(c.o.xid)
    .bind(c.o.id)
    .bind(&sub.state_jobs)
    .bind(&sub.projects)
    .fetch_optional(db)
    .await?;
    Ok(trimmed.map(|(xid, source)| {
        (
            xid,
            if source == "l" {
                "logs_sealed"
            } else {
                "too_old"
            },
        )
    }))
}

/// The streams module.
pub struct StreamsModule {
    hub: Arc<Hub>,
}

impl StreamsModule {
    /// The module with the registered project directory.
    pub fn new(db: Db, config: LiveConfig) -> Self {
        let projects = Projects::registered(&db);
        Self::with_projects(db, config, projects)
    }

    /// The module with this project directory (tests).
    pub fn with_projects(db: Db, config: LiveConfig, projects: Projects) -> Self {
        Self {
            hub: Arc::new(Hub {
                db,
                config,
                projects,
                open: Mutex::new(HashMap::new()),
            }),
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(open),
    tags((name = "streams", description = "Live server-sent event streams."))
)]
struct Doc;

impl ServerModule for StreamsModule {
    fn name(&self) -> &'static str {
        "streams"
    }

    fn routes(&self) -> Router<AppState> {
        Router::new()
            .route("/streams", get(open))
            .route_layer(axum::middleware::from_fn(
                crate::modules::jobs::error::render,
            ))
            .layer(Extension(self.hub.clone()))
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        Doc::openapi()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_parse_strictly() {
        let run = RunId::new();
        let prj = ProjectId::new();
        assert_eq!(
            Topic::parse(&format!("run:{run}:metrics")),
            Some(Topic::Run(run.as_uuid(), RunTopic::Metrics))
        );
        assert_eq!(
            Topic::parse(&format!("project:{prj}:jobs")),
            Some(Topic::ProjectJobs(prj.as_uuid()))
        );
        assert_eq!(Topic::parse("alerts"), Some(Topic::Alerts));
        for bad in [
            format!("run:{run}:secrets"),
            format!("run:{prj}:state"),
            format!("run:{run}:state:x"),
            "node:node_x:telemetry".to_owned(),
            String::new(),
        ] {
            assert_eq!(Topic::parse(&bad), None, "{bad}");
        }
    }

    #[test]
    fn cursors_round_trip_and_refuse_garbage() {
        let mut c = Cursor::start(100);
        c.m = Pos { xid: 120, id: 7 };
        c.a = Pos { xid: 130, id: 8 };
        assert_eq!(Cursor::parse(&c.encode()), Some(c));
        assert!(!Cursor::start(100).after_snapshot(100));
        for bad in [
            "",
            "o.1.2",
            "o.1.2~m.1.2~l.1.2~p.1~a.1.x",
            "v2~o.1.2~m.1.2~m.1.2~p.1.2~a.1.2",
            "v2~o.1.2~m.-1.2~l.1.2~p.1.2~a.1.2",
            "v2~o.1.2~m.1.2~l.1.2~p.1.2~a.1.-1",
        ] {
            assert_eq!(Cursor::parse(bad), None, "{bad}");
        }
    }
}
