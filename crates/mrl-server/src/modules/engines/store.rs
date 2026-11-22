//! Registry storage: registering bundles, reading engine versions and the hardware the
//! connected machines offer.

use super::EnginesError;
use crate::db::{Db, Tx};
use crate::modules::events::{NewEvent, OutboxWriter};
use mrl_domain::capability::manifest::{Bundle, BundleKind, EngineManifest, HardwareClass};
use mrl_domain::state::{EngineVersion, EngineVersionEvent, EngineVersionState, StateMachine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use uuid::Uuid;

/// The sha256 of a manifest as stored.
pub fn manifest_sha256(raw: &Value) -> String {
    let bytes = serde_json::to_vec(raw).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// The lock hash (or image) of the trainer's environment: what the run will execute.
fn runtime_ref(raw: &Value) -> Option<String> {
    let spec = raw.get("spec")?;
    let env_id = spec
        .get("runtimes")?
        .as_array()?
        .iter()
        .find(|r| r.get("role") == Some(&Value::from("trainer")))?
        .get("runtime_ref")?
        .as_str()?;
    let env = spec
        .get("runtime")?
        .get("environments")?
        .as_array()?
        .iter()
        .find(|e| e.get("id") == Some(&Value::from(env_id)))?;
    env.get("image")
        .or_else(|| env.get("lockSha256"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn event(
    version: Uuid,
    seq: i64,
    event_type: &str,
    payload: Value,
) -> Result<NewEvent, EnginesError> {
    NewEvent::new(event_type, EngineVersion::AGGREGATE, version, seq, payload)
        .map_err(|e| EnginesError::Event(e.to_string()))
}

/// Upserts `bundles` (one transaction, one registration at a time across replicas) and
/// lists dev-only engines only when `dev_engines` is on.
pub async fn register(db: &Db, bundles: &[&Bundle], dev_engines: bool) -> Result<(), EnginesError> {
    let mut tx = db.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('mrl.engines.register'))")
        .execute(&mut *tx)
        .await?;
    for b in bundles {
        register_one(&mut tx, b).await?;
    }
    sqlx::query("UPDATE training_engines SET listed = (NOT dev_only OR $1) WHERE listed <> (NOT dev_only OR $1)")
        .bind(dev_engines)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn register_one(tx: &mut Tx, b: &Bundle) -> Result<(), EnginesError> {
    let kind = match b.kind {
        BundleKind::Engine => "engine",
        BundleKind::Builtin => "builtin",
    };
    sqlx::query(
        "INSERT INTO training_engines (id, name, kind, origin, dev_only)
         VALUES ($1, $2, $3, 'first_party', $4) ON CONFLICT (name) DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(&b.name)
    .bind(kind)
    .bind(b.dev_only)
    .execute(&mut **tx)
    .await?;
    let engine: Uuid = sqlx::query_scalar("SELECT id FROM training_engines WHERE name = $1")
        .bind(&b.name)
        .fetch_one(&mut **tx)
        .await?;
    let sha = manifest_sha256(&b.raw);
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT manifest_sha256 FROM engine_versions WHERE engine_id = $1 AND version = $2",
    )
    .bind(engine)
    .bind(&b.version)
    .fetch_optional(&mut **tx)
    .await?;
    match stored {
        Some(s) if s == sha => return Ok(()),
        Some(_) => {
            return Err(EnginesError::Changed {
                name: b.name.clone(),
                version: b.version.clone(),
            })
        }
        None => {}
    }
    let id = Uuid::now_v7();
    let status = EngineVersion::state_name(EngineVersion::initial_state(true));
    let worker = b
        .raw
        .pointer("/spec/adapter/workerProtocol")
        .and_then(Value::as_str);
    sqlx::query(
        "INSERT INTO engine_versions (id, engine_id, version, manifest, schemas, manifest_sha256,
           runtime_ref, worker_protocol, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(id)
    .bind(engine)
    .bind(&b.version)
    .bind(&b.raw)
    .bind(serde_json::to_value(&b.schemas).unwrap_or_else(|_| json!({})))
    .bind(&sha)
    .bind(runtime_ref(&b.raw))
    .bind(worker)
    .bind(status)
    .execute(&mut **tx)
    .await?;
    if let Some(m) = &b.engine {
        for a in &m.spec.algorithms {
            let form = a.config.as_ref();
            let schema = form.and_then(|f| b.schemas.get(&f.schema));
            let ui = form
                .and_then(|f| f.ui_schema.as_ref())
                .and_then(|p| b.schemas.get(p));
            let schemas: Vec<String> = a
                .requires
                .dataset_schemas
                .train
                .iter()
                .chain(&a.requires.dataset_schemas.eval)
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let raw_algo = b
                .raw
                .pointer("/spec/algorithms")
                .and_then(Value::as_array)
                .and_then(|l| {
                    l.iter()
                        .find(|x| x.get("id") == Some(&Value::from(a.id.as_str())))
                });
            let signals: Vec<String> = raw_algo
                .and_then(|x| x.pointer("/requires/signals"))
                .and_then(Value::as_object)
                .map(|s| {
                    s.iter()
                        .filter(|(_, v)| v.as_str() == Some("required"))
                        .map(|(k, _)| k.clone())
                        .collect()
                })
                .unwrap_or_default();
            let metrics = raw_algo
                .and_then(|x| x.get("metrics"))
                .cloned()
                .unwrap_or_else(|| json!([]));
            sqlx::query(
                "INSERT INTO algorithm_definitions (engine_version_id, algorithm_id, display_name,
                   summary, parent_trainer, switch, defaults, maturity, fidelity, fidelity_note,
                   required_schemas, required_signals, config_schema, ui_schema, presets, emitted_metrics)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            )
            .bind(id)
            .bind(&a.id)
            .bind(&a.display_name)
            .bind(&a.summary)
            .bind(&a.parent)
            .bind(Value::Object(a.switch.clone()))
            .bind(Value::Object(a.defaults.clone()))
            .bind(&a.maturity)
            .bind(&a.fidelity)
            .bind(&a.fidelity_note)
            .bind(&schemas)
            .bind(&signals)
            .bind(schema)
            .bind(ui)
            .bind(Value::Array(form.map(|f| f.presets.clone()).unwrap_or_default()))
            .bind(&metrics)
            .execute(&mut **tx)
            .await?;
        }
    }
    let e = event(
        id,
        0,
        EngineVersion::CREATED_EVENT,
        json!({ "engine": b.name, "version": b.version, "status": status }),
    )?;
    OutboxWriter::write(tx, &e).await?;
    Ok(())
}

/// One stored engine version a job could run on.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// `engine_versions.id`.
    pub id: Uuid,
    /// Engine name.
    pub name: String,
    /// Version.
    pub version: String,
    /// The typed manifest.
    pub manifest: EngineManifest,
    /// Inlined schemas.
    pub schemas: BTreeMap<String, Value>,
    /// Hardware classes with a passed probe.
    pub enabled: BTreeSet<HardwareClass>,
}

/// Hardware classes marked ENABLED in `probe_results`.
pub fn enabled_classes(probe_results: &Value) -> BTreeSet<HardwareClass> {
    probe_results
        .as_object()
        .map(|o| {
            o.iter()
                .filter(|(_, v)| v.get("status") == Some(&Value::from("ENABLED")))
                .filter_map(|(k, _)| HardwareClass::parse(k))
                .collect()
        })
        .unwrap_or_default()
}

/// Live training-engine versions that declare `algorithm` (any method when `None`), oldest
/// engine name first.
pub async fn candidates(db: &Db, algorithm: Option<&str>) -> Result<Vec<Candidate>, EnginesError> {
    // Live: listed engines whose version is PROBING or ENABLED.
    let rows = sqlx::query(
        "SELECT ev.id, te.name, ev.version, ev.manifest, ev.schemas, ev.probe_results
         FROM engine_versions ev JOIN training_engines te ON te.id = ev.engine_id
         WHERE te.listed AND ev.status IN ('PROBING', 'ENABLED') AND te.kind = 'engine'
           AND ($1::text IS NULL OR EXISTS (SELECT 1 FROM algorithm_definitions ad
                 WHERE ad.engine_version_id = ev.id AND ad.algorithm_id = $1))
         ORDER BY te.name, ev.id",
    )
    .bind(algorithm)
    .fetch_all(db.pool())
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let name: String = r.try_get("name")?;
        let version: String = r.try_get("version")?;
        let raw: Value = r.try_get("manifest")?;
        // Stored only after its checks passed, so it always reads; skip it loudly if not.
        let manifest = match serde_json::from_value::<EngineManifest>(raw) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(engine = %name, version = %version, error = %e, "a stored engine manifest no longer reads; skipped");
                continue;
            }
        };
        let schemas: Value = r.try_get("schemas")?;
        let probes: Value = r.try_get("probe_results")?;
        out.push(Candidate {
            id: r.try_get("id")?,
            name,
            version,
            manifest,
            schemas: serde_json::from_value(schemas).unwrap_or_default(),
            enabled: enabled_classes(&probes),
        });
    }
    Ok(out)
}

/// Hardware classes of the machines that could take work: ONLINE or DEGRADED machines in
/// platform pools and pools of `org` (platform pools only when `org` is `None`), only `pool`
/// when given.
/// Every machine offers its processor; machines with healthy GPUs add their maker.
pub async fn available_classes(
    db: &Db,
    org: Option<Uuid>,
    pool: Option<Uuid>,
) -> Result<BTreeSet<HardwareClass>, EnginesError> {
    let vendors: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT COALESCE(g.vendor, 'cpu')
         FROM compute_nodes n JOIN compute_pools p ON p.id = n.pool_id
         LEFT JOIN gpu_devices g ON g.node_id = n.id AND g.present AND g.health = 'OK'
         WHERE n.status IN ('ONLINE', 'DEGRADED')
           AND (p.org_id IS NULL OR p.org_id = $1)
           AND ($2::uuid IS NULL OR p.id = $2)",
    )
    .bind(org)
    .bind(pool)
    .fetch_all(db.pool())
    .await?;
    let mut out: BTreeSet<HardwareClass> = vendors
        .iter()
        .filter_map(|v| HardwareClass::parse(v))
        .collect();
    if !vendors.is_empty() {
        out.insert(HardwareClass::Cpu);
    }
    Ok(out)
}

/// A version's status, version counter and probe results, locked for update.
pub async fn lock_version(
    tx: &mut Tx,
    id: Uuid,
) -> Result<Option<(EngineVersionState, i64, Value)>, EnginesError> {
    let row = sqlx::query(
        "SELECT status, state_version, probe_results FROM engine_versions WHERE id = $1 FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(r) = row else { return Ok(None) };
    let status: String = r.try_get("status")?;
    let status =
        EngineVersionState::from_str(&status).map_err(|e| EnginesError::Event(e.to_string()))?;
    Ok(Some((
        status,
        r.try_get("state_version")?,
        r.try_get("probe_results")?,
    )))
}

/// Records a probe outcome for `class` on version `id` (locking it) and enables the
/// version on its first passed probe. A class once ENABLED stays so.
pub async fn record_probe(
    tx: &mut Tx,
    id: Uuid,
    class: HardwareClass,
    passed: bool,
    entry: Value,
) -> Result<(), EnginesError> {
    let Some((status, seq, mut results)) = lock_version(tx, id).await? else {
        return Ok(());
    };
    if enabled_classes(&results).contains(&class) {
        return Ok(());
    }
    if let Some(o) = results.as_object_mut() {
        o.insert(class.as_str().to_owned(), entry);
    }
    sqlx::query("UPDATE engine_versions SET probe_results = $2 WHERE id = $1")
        .bind(id)
        .bind(&results)
        .execute(&mut **tx)
        .await?;
    if !passed {
        return Ok(());
    }
    sqlx::query(
        "UPDATE algorithm_definitions SET verified_hw = array_append(verified_hw, $2)
         WHERE engine_version_id = $1 AND NOT ($2 = ANY (verified_hw))",
    )
    .bind(id)
    .bind(class.as_str())
    .execute(&mut **tx)
    .await?;
    if let Ok(to) = EngineVersion::can_transition(status, EngineVersionEvent::Enable) {
        if status == EngineVersionState::Probing {
            sqlx::query("UPDATE engine_versions SET status = $2, state_version = $3 WHERE id = $1")
                .bind(id)
                .bind(to.as_str())
                .bind(seq + 1)
                .execute(&mut **tx)
                .await?;
            let e = event(
                id,
                seq + 1,
                EngineVersion::event_type(EngineVersionEvent::Enable),
                json!({ "from": status.as_str(), "to": to.as_str(), "hardware_class": class.as_str() }),
            )?;
            OutboxWriter::write(tx, &e).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_ref_is_the_trainer_environment_lock_and_classes_read() {
        let raw = json!({"spec": {
            "runtimes": [{"role": "trainer", "runtime_ref": "a"}],
            "runtime": {"environments": [{"id": "b", "lockSha256": "x"}, {"id": "a", "lockSha256": "y"}]}}});
        assert_eq!(runtime_ref(&raw).as_deref(), Some("y"));
        let probes = json!({"cpu": {"status": "ENABLED"}, "nvidia": {"status": "FAILED"}, "tpu": {"status": "ENABLED"}});
        assert_eq!(enabled_classes(&probes), [HardwareClass::Cpu].into());
        assert_eq!(manifest_sha256(&json!({})).len(), 64);
    }
}
