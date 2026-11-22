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
//! cursor of all sources (`o.<xid>.<id>~m.<xid>.<id>~l.<xid>.<id>~p.<xid>~a.<xid>.<uuid>`);
//! reconnecting with `Last-Event-ID` resumes each source exactly: no gap, no duplicate.
//! Heartbeats carry the cursor too. A `metrics` event holds every new point of one poll
//! (polls are at least 500 ms apart, so a series updates at most twice a second; at most the
//! latest 500 points per series, `truncated` says when more came). `resync` asks the client to
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
use sqlx::Row;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use utoipa::OpenApi;
use uuid::Uuid;

/// Rows read per source per poll.
const BATCH: i64 = 200;
/// Points per series in one `metrics` event.
const POINTS_PER_SERIES: usize = 500;
/// Polls in a row with a full batch before a client counts as too slow.
const SLOW_POLLS: u32 = 20;

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
    p: i64,
    a: (i64, Uuid),
}

impl Cursor {
    /// Every source at "now": rows from transactions at or after `xmin`.
    fn start(xmin: i64) -> Self {
        let p = Pos { xid: xmin, id: 0 };
        Self {
            o: p,
            m: p,
            l: p,
            p: xmin - 1,
            a: (xmin, Uuid::nil()),
        }
    }

    /// The `id:` text.
    pub fn encode(&self) -> String {
        format!(
            "o.{}.{}~m.{}.{}~l.{}.{}~p.{}~a.{}.{}",
            self.o.xid,
            self.o.id,
            self.m.xid,
            self.m.id,
            self.l.xid,
            self.l.id,
            self.p,
            self.a.0,
            self.a.1
        )
    }

    /// Reads [`Cursor::encode`] text; anything else is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        let mut c = Self::start(0);
        let mut seen = 0;
        for part in s.split('~') {
            let mut f = part.split('.');
            let (key, a, b) = (f.next()?, f.next()?, f.next());
            if f.next().is_some() {
                return None;
            }
            let pos = |b: Option<&str>| -> Option<Pos> {
                Some(Pos {
                    xid: a.parse().ok()?,
                    id: b?.parse().ok()?,
                })
            };
            match key {
                "o" => c.o = pos(b)?,
                "m" => c.m = pos(b)?,
                "l" => c.l = pos(b)?,
                "p" if b.is_none() => c.p = a.parse().ok()?,
                "a" => c.a = (a.parse().ok()?, b?.parse().ok()?),
                _ => return None,
            }
            seen += 1;
        }
        (seen == 5).then_some(c)
    }

    fn xids(&self) -> [i64; 5] {
        [self.o.xid, self.m.xid, self.l.xid, self.p, self.a.0]
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
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let live = Live::begin(hub.clone(), state, ctx, sub, slot, resume.as_deref()).await?;
    let poll = Duration::from_millis(hub.config.stream_poll_millis);
    let events = futures_util::stream::unfold(live, move |mut live| async move {
        if live.done {
            return None;
        }
        let events = if let Some(first) = live.first.take() {
            first
        } else {
            tokio::time::sleep(poll).await;
            live.step().await
        };
        Some((events, live))
    })
    .flat_map(|events| futures_util::stream::iter(events.into_iter().map(Ok::<_, Infallible>)));
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
    slow: u32,
    done: bool,
    _slot: Slot,
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
        slot: Slot,
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
            Some(Some(c)) if c.xids().iter().any(|x| *x >= xmax) => {
                first.push(event(
                    "resync",
                    Some(&fresh),
                    &json!({"reason": "bad_cursor"}),
                ));
                fresh
            }
            Some(Some(c)) => match too_old(&hub.db, &sub, &c).await? {
                Some(reason) => {
                    first.push(event("resync", Some(&fresh), &json!({"reason": reason})));
                    fresh
                }
                None => c,
            },
        };
        Ok(Self {
            hub,
            state,
            ctx,
            sub,
            cursor,
            first: Some(first),
            last_sent: Instant::now(),
            slow: 0,
            done: false,
            _slot: slot,
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
        match self.poll(&mut out).await {
            Ok(full) => {
                self.slow = if full { self.slow + 1 } else { 0 };
                if self.slow >= SLOW_POLLS {
                    self.done = true;
                    out.push(event(
                        "resync",
                        Some(&self.cursor),
                        &json!({"reason": "too_slow"}),
                    ));
                }
            }
            Err(e) => {
                tracing::warn!(code = %e.code(), error = %e, "stream poll failed; trying again")
            }
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

    /// Reads every source once; true if any source had more than one batch waiting.
    async fn poll(&mut self, out: &mut Vec<Event>) -> Result<bool, JobsError> {
        let db = self.hub.db.clone();
        let xmin: i64 =
            sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint")
                .fetch_one(db.pool())
                .await?;
        // Every row below xmin is visible now: a source read to its end may skip to it.
        let end = Pos {
            xid: xmin - 1,
            id: i64::MAX,
        };
        let mut full = false;
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
            .fetch_all(db.pool())
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
            full |= i64::try_from(rows.len()).unwrap_or(0) >= BATCH;
            if i64::try_from(rows.len()).unwrap_or(0) < BATCH {
                self.cursor.o = self.cursor.o.max(end);
            }
        }
        if !self.sub.metrics.is_empty() {
            full |= self.metrics(&db, xmin, end, out).await?;
        }
        if !self.sub.logs.is_empty() {
            full |= self.logs(&db, xmin, end, out).await?;
        }
        if !self.sub.progress.is_empty() {
            let rows = sqlx::query(
                "SELECT run_id, attempt_id, step, total_steps, epoch, phase, tokens_seen, eta_seconds,
                        updated_at, xid::text::bigint AS x
                   FROM run_progress
                  WHERE run_id = ANY($1) AND xid > ($2::bigint)::text::xid8
                    AND xid < ($3::bigint)::text::xid8
                  ORDER BY xid",
            )
            .bind(&self.sub.progress)
            .bind(self.cursor.p)
            .bind(xmin)
            .fetch_all(db.pool())
            .await?;
            for r in &rows {
                self.cursor.p = r.try_get("x")?;
                let at: time::OffsetDateTime = r.try_get("updated_at")?;
                let data = json!({
                    "run_id": RunId::from_uuid(r.try_get("run_id")?).to_string(),
                    "attempt_id": AttemptId::from_uuid(r.try_get("attempt_id")?).to_string(),
                    "step": r.try_get::<i64, _>("step")?,
                    "total_steps": r.try_get::<i64, _>("total_steps")?,
                    "epoch": r.try_get::<f64, _>("epoch")?,
                    "phase": r.try_get::<String, _>("phase")?,
                    "tokens_seen": r.try_get::<i64, _>("tokens_seen")?,
                    "eta_seconds": r.try_get::<f64, _>("eta_seconds")?,
                    "updated_at": at.format(&time::format_description::well_known::Rfc3339).unwrap_or_default(),
                });
                out.push(event("progress", Some(&self.cursor), &data));
            }
            self.cursor.p = self.cursor.p.max(end.xid);
        }
        if let Some(org) = self.sub.alerts {
            let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT {}, a.xid::text::bigint AS x FROM alerts a
                   LEFT JOIN runs r ON a.scope_kind = 'run' AND r.id = a.scope_id
                  WHERE a.org_id = $1 AND (a.xid, a.id) > (($2::bigint)::text::xid8, $3)
                    AND a.xid < ($4::bigint)::text::xid8
                  ORDER BY a.xid, a.id LIMIT $5",
                crate::modules::alerts::ALERT_COLS
            )))
            .bind(org)
            .bind(self.cursor.a.0)
            .bind(self.cursor.a.1)
            .bind(xmin)
            .bind(BATCH)
            .fetch_all(db.pool())
            .await?;
            for r in &rows {
                let row = crate::modules::alerts::row_of(r)?;
                self.cursor.a = (r.try_get("x")?, row.id);
                if crate::modules::alerts::visible(&self.state, &self.ctx, &row).await? {
                    let data = serde_json::to_value(&row.view).unwrap_or(Value::Null);
                    out.push(event("alert", Some(&self.cursor), &data));
                }
            }
            full |= i64::try_from(rows.len()).unwrap_or(0) >= BATCH;
            if i64::try_from(rows.len()).unwrap_or(0) < BATCH {
                self.cursor.a = self.cursor.a.max((end.xid, Uuid::from_u128(u128::MAX)));
            }
        }
        Ok(full)
    }

    /// New metric batches of the subscribed runs as one `metrics` event.
    async fn metrics(
        &mut self,
        db: &Db,
        xmin: i64,
        end: Pos,
        out: &mut Vec<Event>,
    ) -> Result<bool, JobsError> {
        let batches = sqlx::query(
            "SELECT id, xid::text::bigint AS x FROM metric_batches
              WHERE run_id = ANY($1) AND (xid, id) > (($2::bigint)::text::xid8, $3)
                AND xid < ($4::bigint)::text::xid8
              ORDER BY xid, id LIMIT $5",
        )
        .bind(&self.sub.metrics)
        .bind(self.cursor.m.xid)
        .bind(self.cursor.m.id)
        .bind(xmin)
        .bind(BATCH)
        .fetch_all(db.pool())
        .await?;
        let full = i64::try_from(batches.len()).unwrap_or(0) >= BATCH;
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
            .fetch_all(db.pool())
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
                        let truncated = pts.len() > POINTS_PER_SERIES;
                        let keep = &pts[pts.len().saturating_sub(POINTS_PER_SERIES)..];
                        json!({"series_id": id, "name": name, "split": split, "truncated": truncated,
                               "points": keep.iter().map(|(s, v, t)| json!([s, v, t])).collect::<Vec<_>>()})
                    }).collect::<Vec<_>>(),
                })).collect::<Vec<_>>() });
                out.push(event("metrics", Some(&self.cursor), &data));
            }
        }
        if !full {
            self.cursor.m = self.cursor.m.max(end);
        }
        Ok(full)
    }

    /// New log chunks of the subscribed runs, one `log` event each.
    async fn logs(
        &mut self,
        db: &Db,
        xmin: i64,
        end: Pos,
        out: &mut Vec<Event>,
    ) -> Result<bool, JobsError> {
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
        .fetch_all(db.pool())
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
        Ok(full)
    }
}

/// Why a resumed cursor can't continue exactly, if it can't.
async fn too_old(db: &Db, sub: &Sub, c: &Cursor) -> Result<Option<&'static str>, JobsError> {
    if !sub.logs.is_empty() {
        // Chunks after the cursor were sealed (moved out of the hot table).
        let sealed: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM log_index WHERE run_id = ANY($1) AND id_hi > $2)",
        )
        .bind(&sub.logs)
        .bind(c.l.id)
        .fetch_one(db.pool())
        .await?;
        if sealed {
            return Ok(Some("logs_sealed"));
        }
    }
    // Retention removed rows the cursor had not reached.
    for (table, pos, used) in [
        (
            "outbox_events",
            c.o,
            !sub.state_jobs.is_empty() || !sub.projects.is_empty(),
        ),
        ("metric_batches", c.m, !sub.metrics.is_empty()),
    ] {
        if !used || pos.id == i64::MAX {
            continue;
        }
        let min: Option<i64> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT min(id) FROM {table}")))
                .fetch_one(db.pool())
                .await?;
        if min.is_some_and(|m| m > pos.id.saturating_add(1)) {
            return Ok(Some("too_old"));
        }
    }
    Ok(None)
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
        c.a = (130, Uuid::now_v7());
        assert_eq!(Cursor::parse(&c.encode()), Some(c));
        for bad in [
            "",
            "o.1.2",
            "o.1.2~m.1.2~l.1.2~p.1~a.1.x",
            "o.1.2~m.1.2~l.1.2~p.1.2~a.1.00000000-0000-0000-0000-000000000000",
        ] {
            assert_eq!(Cursor::parse(bad), None, "{bad}");
        }
    }
}
