//! Probes: is this engine version usable on this kind of machine?
//!
//! The leader ([`Prober::run`]) looks every `probe_interval_seconds` for live engine versions
//! with a hardware class not yet enabled, picks an ONLINE machine of that class, writes a
//! PENDING `capability_evidence` row and sends a `Probe` (its `request_id` is the row id).
//! [`ProbeSink`] takes the machine's `ProbeResult` (CONTROL kind `probe_result`): only the
//! machine that was asked can answer, only once (duplicates and replays change nothing). A
//! probe passes when every check it asked for came back and none failed. A probe with no
//! answer is sent again after `probe_timeout_seconds`; after a failure the class waits
//! `probe_retry_seconds`.

use super::{store, EnginesConfig, EnginesError};
use crate::db::{Db, LeaderElection};
use crate::modules::nodes::commands::{CommandKind, CommandSender, NewCommand};
use crate::modules::nodes::lanes::{LaneItem, LaneKind, LaneSink, SinkError};
use crate::modules::nodes::{Gateway, NodesError};
use mrl_domain::capability::manifest::{EngineManifest, HardwareClass};
use mrl_domain::ids::NodeId;
use mrl_proto::agent::v1::{agent_message::Body, command::Kind, Probe, ProbeCheck, ProbeOutcome};
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The only command kind this part sends.
static KINDS: &[CommandKind] = &[CommandKind::Probe];

/// Longest probe summary kept per check (agent text).
const SUMMARY_CHARS: usize = 300;

/// The checks a probe asks for on `class`.
pub fn checks_for(class: HardwareClass) -> Vec<ProbeCheck> {
    let mut c = vec![ProbeCheck::Os, ProbeCheck::CpuRamDisk, ProbeCheck::PythonUv];
    c.extend(match class {
        HardwareClass::Cpu => vec![],
        HardwareClass::Nvidia => vec![ProbeCheck::Gpu, ProbeCheck::NvidiaDriver],
        HardwareClass::Amd => vec![ProbeCheck::Gpu, ProbeCheck::Rocm],
        HardwareClass::Apple => vec![ProbeCheck::AppleSilicon],
    });
    c
}

/// Sends probes (leader only).
pub struct Prober {
    db: Db,
    config: EnginesConfig,
    sender: CommandSender,
}

impl Prober {
    /// A prober that sends through `gateway`.
    pub fn new(db: Db, config: EnginesConfig, gateway: &Arc<Gateway>) -> Self {
        Self {
            db,
            config,
            sender: CommandSender::new(gateway, KINDS),
        }
    }

    /// Plans probes while this process leads `engines.probes`, until `cancel`.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let election =
            LeaderElection::new(self.db.clone(), "engines.probes", Duration::from_secs(15));
        election
            .run(cancel, |lead| {
                let me = self.clone();
                async move {
                    let every = Duration::from_secs(me.config.probe_interval_seconds);
                    loop {
                        match me.plan_once().await {
                            Ok(n) if n > 0 => tracing::info!(sent = n, "engine probes sent"),
                            Ok(_) => {}
                            Err(e) => tracing::warn!(code = e.code(), error = %e, "engine probe planning failed"),
                        }
                        tokio::select! {
                            () = lead.cancelled() => return Ok(()),
                            () = tokio::time::sleep(every) => {}
                        }
                    }
                }
            })
            .await;
    }

    /// One planning pass; returns how many probes it sent.
    pub async fn plan_once(&self) -> Result<usize, EnginesError> {
        let rows = sqlx::query(
            "SELECT ev.id, te.kind, ev.manifest, ev.probe_results
             FROM engine_versions ev JOIN training_engines te ON te.id = ev.engine_id
             WHERE te.listed AND ev.status IN ('PROBING', 'ENABLED') ORDER BY ev.id",
        )
        .fetch_all(self.db.pool())
        .await?;
        let mut sent = 0;
        for r in rows {
            let id: Uuid = r.try_get("id")?;
            let kind: String = r.try_get("kind")?;
            let manifest = if kind == "builtin" {
                None
            } else {
                let m = serde_json::from_value::<EngineManifest>(r.try_get("manifest")?);
                let Ok(m) = m else { continue };
                Some(m)
            };
            let classes: BTreeSet<_> = manifest.as_ref().map_or_else(
                || [HardwareClass::Cpu].into(),
                EngineManifest::hardware_classes,
            );
            let enabled = store::enabled_classes(&r.try_get::<Value, _>("probe_results")?);
            for class in classes.difference(&enabled) {
                if self.probe(id, *class, manifest.as_ref()).await? {
                    sent += 1;
                }
            }
        }
        Ok(sent)
    }

    /// Sends one probe for `(version, class)` unless one is waiting or failed recently.
    async fn probe(
        &self,
        version: Uuid,
        class: HardwareClass,
        manifest: Option<&EngineManifest>,
    ) -> Result<bool, EnginesError> {
        let mut tx = self.db.begin().await?;
        let Some((status, _, results)) = store::lock_version(&mut tx, version).await? else {
            return Ok(false);
        };
        if !matches!(
            status,
            mrl_domain::state::EngineVersionState::Probing
                | mrl_domain::state::EngineVersionState::Enabled
        ) || store::enabled_classes(&results).contains(&class)
        {
            return Ok(false);
        }
        sqlx::query(
            "UPDATE capability_evidence SET result = 'FAILED', decided_at = now(),
                    detail = detail || jsonb_build_object('timed_out', true)
             WHERE engine_version_id = $1 AND hardware_class = $2 AND source = 'probe'
               AND result = 'PENDING'
               AND at + make_interval(secs => COALESCE((detail->>'timeout_seconds')::float8, 300)) <= now()",
        ).bind(version).bind(class.as_str()).execute(&mut *tx).await?;
        let busy: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM capability_evidence
               WHERE engine_version_id = $1 AND hardware_class = $2 AND source = 'probe'
                 AND result = 'PENDING')",
        )
        .bind(version)
        .bind(class.as_str())
        .fetch_one(&mut *tx)
        .await?;
        if busy {
            tx.commit().await?;
            return Ok(false);
        }
        let rows = sqlx::query(
            "SELECT n.id, n.inventory, COALESCE((
                SELECT jsonb_agg(jsonb_build_object('vendor', g.vendor, 'memory', g.vram_total_bytes,
                                                'index', g.index_on_node))
                FROM gpu_devices g WHERE g.node_id = n.id AND g.present AND g.health = 'OK'
             ), '[]') AS devices
             FROM compute_nodes n LEFT JOIN LATERAL (
                SELECT max(at) AS last_tried, max(decided_at) FILTER (
                    WHERE detail ? 'results'
                ) AS last_failed
                FROM capability_evidence
                WHERE engine_version_id = $2 AND hardware_class = $1 AND source = 'probe'
                  AND node_id = n.id AND result = 'FAILED'
             ) prior ON true
             WHERE n.status = 'ONLINE'
               AND ($1 = 'cpu' OR EXISTS (SELECT 1 FROM gpu_devices g WHERE g.node_id = n.id
                     AND g.present AND g.health = 'OK' AND g.vendor = $1))
               AND (prior.last_failed IS NULL OR prior.last_failed <= now() - make_interval(secs => $3))
             ORDER BY prior.last_tried ASC NULLS FIRST, n.last_heartbeat_at DESC NULLS LAST, n.id",
        ).bind(class.as_str()).bind(version).bind(self.config.probe_retry_seconds as f64)
         .fetch_all(&mut *tx).await?;
        let mut node = None;
        for row in rows {
            if manifest
                .is_none_or(|m| store::machine_from_row(&row).is_ok_and(|i| i.supports(m, class)))
            {
                node = Some(row.try_get::<Uuid, _>("id")?);
                break;
            }
        }
        let Some(node) = node else {
            tx.commit().await?;
            return Ok(false);
        };
        let checks: Vec<i32> = checks_for(class).into_iter().map(|c| c as i32).collect();
        let evidence = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO capability_evidence (id, engine_version_id, hardware_class, source, node_id, result, detail)
             VALUES ($1, $2, $3, 'probe', $4, 'PENDING', $5)",
        ).bind(evidence).bind(version).bind(class.as_str()).bind(node)
         .bind(json!({ "checks": checks, "timeout_seconds": self.config.probe_timeout_seconds }))
         .execute(&mut *tx).await?;
        tx.commit().await?;
        let probe = Kind::Probe(Probe {
            request_id: evidence.to_string(),
            checks,
            peer_node_id: String::new(),
        });
        let command =
            NewCommand::new(probe).deadline(Duration::from_secs(self.config.probe_timeout_seconds));
        match self.sender.send(NodeId::from_uuid(node), command).await {
            Ok(c) => {
                sqlx::query("UPDATE capability_evidence SET command_id = $2 WHERE id = $1")
                    .bind(evidence)
                    .bind(c.id)
                    .execute(self.db.pool())
                    .await?;
                Ok(true)
            }
            Err(e @ NodesError::CommandRefused(_)) => {
                sqlx::query(
                    "UPDATE capability_evidence SET result = 'FAILED', decided_at = now()
                     WHERE id = $1 AND result = 'PENDING'",
                )
                .bind(evidence)
                .execute(self.db.pool())
                .await?;
                tracing::info!(code = e.code(), error = %e, "probe refused");
                Ok(false)
            }
            // A database failure can lose the response to a committed send.
            Err(e) => Err(e.into()),
        }
    }
}

/// Takes `probe_result` CONTROL messages.
pub struct ProbeSink {
    db: Db,
}

impl ProbeSink {
    /// A sink writing to `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    async fn take(&self, item: &LaneItem) -> Result<(), EnginesError> {
        let Some(Body::ProbeResult(p)) = &item.message.body else {
            return Ok(());
        };
        let Ok(evidence) = Uuid::parse_str(&p.request_id) else {
            // Probes this part did not send (like a doctor run from the CLI).
            return Ok(());
        };
        let mut tx = self.db.begin().await?;
        let version: Option<Uuid> = sqlx::query_scalar(
            "SELECT engine_version_id FROM capability_evidence WHERE id = $1 AND node_id = $2 AND result = 'PENDING'",
        ).bind(evidence).bind(item.node_id).fetch_optional(&mut *tx).await?;
        let Some(version) = version else {
            return Ok(());
        };
        let Some(_) = store::lock_version(&mut tx, version).await? else {
            return Ok(());
        };
        let row = sqlx::query(
            "SELECT engine_version_id, hardware_class, detail FROM capability_evidence
             WHERE id = $1 AND node_id = $2 AND source = 'probe' AND result = 'PENDING'
               AND at + make_interval(secs => COALESCE((detail->>'timeout_seconds')::float8, 300)) > now()
             FOR UPDATE",
        )
        .bind(evidence)
        .bind(item.node_id)
        .fetch_optional(&mut *tx)
        .await?;
        // Unknown, asked of another machine, or already decided: nothing to do.
        let Some(row) = row else { return Ok(()) };
        let version: Uuid = row.try_get("engine_version_id")?;
        let class: String = row.try_get("hardware_class")?;
        let Some(class) = HardwareClass::parse(&class) else {
            return Ok(());
        };
        let detail: Value = row.try_get("detail")?;
        let asked: Vec<i64> = detail
            .get("checks")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_i64).collect())
            .unwrap_or_default();
        let ok = |o: i32| {
            [ProbeOutcome::Ok, ProbeOutcome::Warning]
                .iter()
                .any(|x| *x as i32 == o)
        };
        let passed = !asked.is_empty()
            && p.checks.len() == asked.len()
            && p.checks.iter().all(|c| ok(c.outcome))
            && asked
                .iter()
                .all(|a| p.checks.iter().any(|c| i64::from(c.check) == *a));
        let results: Vec<Value> = p
            .checks
            .iter()
            .map(|c| {
                json!({
                    "check": c.check,
                    "outcome": c.outcome,
                    "summary": c.summary.chars().take(SUMMARY_CHARS).collect::<String>(),
                })
            })
            .collect();
        let result = if passed { "PASSED" } else { "FAILED" };
        sqlx::query(
            "UPDATE capability_evidence SET result = $2, detail = detail || $3, decided_at = now()
             WHERE id = $1",
        )
        .bind(evidence)
        .bind(result)
        .bind(json!({ "results": results }))
        .execute(&mut *tx)
        .await?;
        let entry = json!({
            "status": if passed { "ENABLED" } else { "FAILED" },
            "node_id": NodeId::from_uuid(item.node_id).to_string(),
            "evidence_id": evidence.to_string(),
        });
        store::record_probe(&mut tx, version, class, passed, entry).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl LaneSink for ProbeSink {
    fn name(&self) -> &'static str {
        "engines.probes"
    }

    fn lanes(&self) -> &'static [LaneKind] {
        &[LaneKind::Control]
    }

    fn control_kinds(&self) -> &'static [&'static str] {
        &["probe_result"]
    }

    async fn deliver(&self, item: LaneItem) -> Result<(), SinkError> {
        self.take(&item).await.map_err(|e| SinkError {
            sink: "engines.probes",
            detail: format!("{}: {e}", e.code()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpu_classes_ask_for_their_driver_checks() {
        assert_eq!(checks_for(HardwareClass::Cpu).len(), 3);
        assert!(checks_for(HardwareClass::Nvidia).contains(&ProbeCheck::NvidiaDriver));
        assert!(checks_for(HardwareClass::Amd).contains(&ProbeCheck::Rocm));
        assert!(checks_for(HardwareClass::Apple).contains(&ProbeCheck::AppleSilicon));
    }
}
