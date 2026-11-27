//! Config resolution, the same for submit and `runs:validate`:
//!
//! 1. `config.preset` (a preset id of the method, like `sft-lora-beginner`) is laid down first,
//! 2. the submitted settings go on top of it (JSON Merge Patch: a `null` removes a value),
//! 3. every setting still missing gets its default from the method's config schema,
//! 4. `common.engine` is pinned to the `name@version` whose schema and presets were used, so
//!    the capability check (P24) picks that same engine version.
//!
//! `preset` itself is not part of the resolved config (engine schemas refuse unknown keys);
//! the submitted config keeps it.

use crate::db::Db;
use crate::error::FieldError;
use crate::modules::jobs::{error::hint_for, ResolveError, SubmissionResolver};
use crate::seams::SeamError;
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

/// The same bytes P17 hashes for `run_configs.resolved_sha256`.
pub fn sha256(v: &Value) -> Vec<u8> {
    Sha256::digest(serde_json::to_vec(v).unwrap_or_default()).to_vec()
}

/// `s`, or what its local `$ref` (`#/…`) points at.
pub fn deref<'a>(root: &'a Value, mut s: &'a Value) -> &'a Value {
    for _ in 0..8 {
        match s
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|r| r.strip_prefix('#'))
            .and_then(|p| root.pointer(p))
        {
            Some(next) => s = next,
            None => break,
        }
    }
    s
}

/// RFC 7386 JSON Merge Patch.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(p) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = json!({});
    }
    if let Value::Object(t) = target {
        for (k, v) in p {
            if v.is_null() {
                t.remove(k);
            } else {
                merge_patch(t.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
    }
}

/// Fills every missing property of `value` that `schema` gives a default, at every depth.
pub fn fill_defaults(root: &Value, schema: &Value, value: &mut Value) {
    fill(root, schema, value, 0);
}

fn fill(root: &Value, schema: &Value, value: &mut Value, depth: u32) {
    if depth > 32 {
        return;
    }
    let s = deref(root, schema);
    let (Some(props), Some(obj)) = (
        s.get("properties").and_then(Value::as_object),
        value.as_object_mut(),
    ) else {
        return;
    };
    for (key, prop) in props {
        if !obj.contains_key(key) {
            // A default next to a `$ref` wins over the referenced schema's own.
            let default = prop
                .get("default")
                .or_else(|| deref(root, prop).get("default"));
            match default {
                Some(d) => {
                    obj.insert(key.clone(), d.clone());
                }
                None => continue,
            }
        }
        if let Some(child) = obj.get_mut(key) {
            if child.is_object() {
                fill(root, prop, child, depth + 1);
            }
        }
    }
}

/// Defaults < preset < submitted settings. `schema` is the method's config schema: for the
/// whole config (with `common` and `engine` properties, like TRL's) or the engine block.
///
/// Only the `engine` block (the method's own settings) gets defaults. A `common` setting left
/// out means the platform's automatic choice (precision, seed, update strategy), which the
/// capability check reads as such; writing the engine's default there would change it.
pub fn layer(submitted: &Value, preset: Option<&Value>, schema: Option<&Value>) -> Value {
    let mut out = json!({});
    if let Some(p) = preset {
        merge_patch(&mut out, p);
    }
    merge_patch(&mut out, submitted);
    if let (Some(s), Some(o)) = (schema, out.as_object_mut()) {
        let block = match s.pointer("/properties/engine") {
            Some(e) => e,
            None if s.pointer("/properties/common").is_some() => return out,
            None => s,
        };
        let engine = o.entry("engine").or_insert_with(|| json!({}));
        if let Some(d) = block
            .get("default")
            .filter(|_| engine.as_object().is_some_and(|m| m.is_empty()))
        {
            *engine = d.clone();
        }
        fill_defaults(s, block, engine);
    }
    out
}

/// A method of one live engine version.
#[derive(Debug, Clone, PartialEq)]
pub struct Method {
    /// `engine_versions.id`.
    pub engine_version_id: Uuid,
    /// Engine name, like `trl`.
    pub engine: String,
    /// Engine version.
    pub version: String,
    /// A dev-only engine (the FakeEngine).
    pub dev_only: bool,
    /// Method id.
    pub algorithm_id: String,
    /// `stable`, `beta` or `experimental`.
    pub maturity: String,
    /// The method's config schema.
    pub schema: Option<Value>,
    /// The method's presets (`[{id, title, patch, …}]`).
    pub presets: Value,
    /// The stored manifest.
    pub manifest: Value,
}

impl Method {
    /// `name@version`.
    pub fn engine_ref(&self) -> String {
        format!("{}@{}", self.engine, self.version)
    }

    /// The engine checks settings on a machine before a run (manifest `validate.l1`).
    pub fn has_l1(&self) -> bool {
        self.manifest
            .pointer("/spec/validate/l1")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    /// The method can train with QLoRA.
    pub fn supports_qlora(&self) -> bool {
        let algo = self.manifest["spec"]["algorithms"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|a| a["id"] == self.algorithm_id.as_str());
        algo.and_then(|a| a.pointer("/supports/updateStrategies"))
            .or_else(|| self.manifest.pointer("/spec/capabilities/updateStrategies"))
            .and_then(Value::as_array)
            .is_some_and(|s| s.iter().any(|v| v == "qlora"))
    }

    fn preset(&self, id: &str) -> Option<&Value> {
        self.presets
            .as_array()?
            .iter()
            .find(|p| p["id"] == id)
            .and_then(|p| p.get("patch"))
    }
}

/// `common.engine` as `(name, version)`, when it is a usable string.
pub fn engine_choice(config: &Value) -> Option<(String, Option<String>)> {
    let s = config.pointer("/common/engine")?.as_str()?;
    match s.split_once('@') {
        Some((n, v)) if !n.is_empty() && !v.is_empty() => Some((n.to_owned(), Some(v.to_owned()))),
        Some(_) => None,
        None if !s.is_empty() => Some((s.to_owned(), None)),
        None => None,
    }
}

/// Live methods (listed engine, version PROBING or ENABLED), narrowed by method, engine and
/// preset, in the capability check's order (engine name, then registration).
pub async fn methods(
    db: &Db,
    algorithm: Option<&str>,
    engine: Option<&(String, Option<String>)>,
    preset: Option<&str>,
) -> Result<Vec<Method>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT ev.id, te.name, ev.version, te.dev_only, ad.algorithm_id, ad.maturity,
                ad.config_schema, ad.presets, ev.manifest
           FROM algorithm_definitions ad
           JOIN engine_versions ev ON ev.id = ad.engine_version_id
           JOIN training_engines te ON te.id = ev.engine_id
          WHERE te.listed AND te.kind = 'engine' AND ev.status IN ('PROBING', 'ENABLED')
            AND ($1::text IS NULL OR ad.algorithm_id = $1)
            AND ($2::text IS NULL OR te.name = $2)
            AND ($3::text IS NULL OR ev.version = $3)
            AND ($4::text IS NULL OR ad.presets @> jsonb_build_array(jsonb_build_object('id', $4::text)))
          ORDER BY te.name, ev.id, ad.algorithm_id",
    )
    .bind(algorithm)
    .bind(engine.map(|e| e.0.as_str()))
    .bind(engine.and_then(|e| e.1.as_deref()))
    .bind(preset)
    .fetch_all(db.pool())
    .await?;
    rows.iter()
        .map(|r| {
            Ok(Method {
                engine_version_id: r.try_get("id")?,
                engine: r.try_get("name")?,
                version: r.try_get("version")?,
                dev_only: r.try_get("dev_only")?,
                algorithm_id: r.try_get("algorithm_id")?,
                maturity: r.try_get("maturity")?,
                schema: r.try_get("config_schema")?,
                presets: r.try_get("presets")?,
                manifest: r.try_get("manifest")?,
            })
        })
        .collect()
}

/// A resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolved {
    /// The resolved config.
    pub config: Value,
    /// The method whose schema and presets were used (none: kept as submitted).
    pub method: Option<Method>,
    /// The preset applied.
    pub preset: Option<String>,
}

fn invalid(path: &str, message: String) -> ResolveError {
    let code = super::codes::UNKNOWN_PRESET.to_string();
    ResolveError::Invalid(vec![FieldError {
        path: path.to_owned(),
        hint: hint_for(&code),
        code,
        message,
    }])
}

/// Resolves `config` for `algorithm`. Without the method (the submit chain does not know it)
/// the preset names it; with neither, the config is kept as submitted.
pub async fn resolve(
    db: &Db,
    algorithm: Option<&str>,
    config: &Value,
) -> Result<Resolved, ResolveError> {
    let kept = |c: &Value| Resolved {
        config: c.clone(),
        method: None,
        preset: None,
    };
    let Some(obj) = config.as_object() else {
        // The schema check rejects it.
        return Ok(kept(config));
    };
    let preset = match obj.get("preset") {
        None => None,
        Some(Value::String(p)) if (1..=100).contains(&p.chars().count()) => Some(p.clone()),
        Some(_) => {
            return Err(invalid(
                "/config/preset",
                "The preset must be a preset id, like `sft-lora-beginner`.".into(),
            ))
        }
    };
    let submitted: Value = Value::Object(
        obj.iter()
            .filter(|(k, _)| k.as_str() != "preset")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Map<_, _>>(),
    );
    if algorithm.is_none() && preset.is_none() {
        return Ok(kept(&submitted));
    }
    let choice = engine_choice(&submitted);
    if submitted.pointer("/common/engine").is_some() && choice.is_none() {
        // Malformed: the capability check names the field.
        return Ok(kept(&submitted));
    }
    let found = methods(db, algorithm, choice.as_ref(), preset.as_deref())
        .await
        .map_err(|e| ResolveError::Seam(SeamError::from(crate::db::DbError::from(e))))?;
    let Some(method) = found.first().cloned() else {
        return match &preset {
            Some(p) => Err(invalid(
                "/config/preset",
                match algorithm {
                    Some(a) => {
                        format!("No engine here offers the preset `{p}` for the method `{a}`.")
                    }
                    None => format!("No engine here offers the preset `{p}`."),
                },
            )),
            // No engine offers the method: the validator chain explains (MRL-CAP-001).
            None => Ok(kept(&submitted)),
        };
    };
    if algorithm.is_none()
        && found
            .iter()
            .any(|m| (&m.engine, &m.algorithm_id) != (&method.engine, &method.algorithm_id))
    {
        return Err(invalid(
            "/config/preset",
            format!(
                "Several methods offer the preset `{}`; name the engine in `common.engine`.",
                preset.as_deref().unwrap_or_default()
            ),
        ));
    }
    let patch = preset.as_deref().and_then(|p| method.preset(p));
    let mut out = layer(&submitted, patch, method.schema.as_ref());
    if let Some(o) = out.as_object_mut() {
        let common = o.entry("common").or_insert_with(|| json!({}));
        if let Some(c) = common.as_object_mut() {
            c.insert("engine".into(), json!(method.engine_ref()));
        }
    }
    Ok(Resolved {
        config: out,
        method: Some(method),
        preset,
    })
}

/// The submit chain's resolver (`// <mrl:submission-resolvers>`): presets and defaults.
pub struct Presets {
    db: Db,
}

impl Presets {
    /// Reads engine methods from `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

#[async_trait]
impl SubmissionResolver for Presets {
    fn name(&self) -> &'static str {
        "presets"
    }

    async fn resolve(&self, _submitted: &Value, resolved: Value) -> Result<Value, ResolveError> {
        // ponytail: the chain passes only the config, so at submit the method comes from the
        // preset; a config without one is kept (the CLI and UI submit what `runs:validate`
        // resolved). Passing the method to resolvers is an item for A1.
        Ok(resolve(&self.db, None, &resolved).await?.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Value {
        json!({
            "type": "object", "additionalProperties": false,
            "properties": {
                "common": {"$ref": "#/$defs/Common", "default": {"precision": "auto", "update": {"strategy": "lora"}}},
                "engine": {"$ref": "#/$defs/Engine", "default": {}}
            },
            "$defs": {
                "Common": {"properties": {
                    "precision": {"default": "auto"}, "seed": {"default": 0},
                    "update": {"$ref": "#/$defs/Update"}}},
                "Update": {"properties": {"strategy": {"default": "full"}, "lora": {"$ref": "#/$defs/Lora"}}},
                "Lora": {"properties": {"r": {"default": 16}, "alpha": {"default": 32}}},
                "Engine": {"properties": {
                    "learning_rate": {"default": 0.0001}, "num_train_epochs": {"default": 1},
                    "max_steps": {"default": null}}}
            }
        })
    }

    #[test]
    fn precedence_is_defaults_then_preset_then_submitted() {
        let preset = json!({"common": {"update": {"strategy": "lora", "lora": {"r": 8}}},
                            "engine": {"learning_rate": 0.0002, "num_train_epochs": 3}});
        let submitted = json!({"engine": {"num_train_epochs": 2}});
        let s = schema();
        let out = layer(&submitted, Some(&preset), Some(&s));
        assert_eq!(
            out["engine"]["num_train_epochs"], 2,
            "submitted beats preset"
        );
        assert_eq!(
            out["engine"]["learning_rate"], 0.0002,
            "preset beats default"
        );
        assert_eq!(
            out["engine"]["max_steps"],
            Value::Null,
            "null defaults are kept"
        );
        assert_eq!(out["common"]["update"]["lora"]["r"], 8);
        assert!(
            out["common"].get("precision").is_none()
                && out["common"]["update"]["lora"].get("alpha").is_none(),
            "common settings left out stay the platform's choice: {out}"
        );
        // No preset: the schema's defaults, then the submitted values.
        let out = layer(&submitted, None, Some(&s));
        assert!(out.get("common").is_none());
        assert_eq!(out["engine"]["learning_rate"], 0.0001);
        // A submitted null removes the preset's value; the default comes back.
        let out = layer(
            &json!({"engine": {"learning_rate": null}}),
            Some(&preset),
            Some(&s),
        );
        assert_eq!(out["engine"]["learning_rate"], 0.0001);
        // No schema: preset + submitted only.
        let out = layer(&submitted, Some(&preset), None);
        assert_eq!(
            out["engine"],
            json!({"learning_rate": 0.0002, "num_train_epochs": 2})
        );
    }

    #[test]
    fn engine_block_schemas_fill_the_engine_block() {
        let s = json!({"properties": {"lr": {"default": 0.1}, "steps": {"default": 5}}});
        let out = layer(&json!({"engine": {"steps": 9}}), None, Some(&s));
        assert_eq!(out, json!({"engine": {"lr": 0.1, "steps": 9}}));
    }

    #[test]
    fn merge_patch_and_refs() {
        let mut t = json!({"a": {"b": 1, "c": 2}, "d": [1]});
        merge_patch(&mut t, &json!({"a": {"b": null, "e": 3}, "d": [2, 3]}));
        assert_eq!(t, json!({"a": {"c": 2, "e": 3}, "d": [2, 3]}));
        let mut t = json!(1);
        merge_patch(&mut t, &json!({"x": 1}));
        assert_eq!(t, json!({"x": 1}));
        let root = json!({"$defs": {"A": {"$ref": "#/$defs/B"}, "B": {"x": 1}}});
        assert_eq!(
            deref(&root, &json!({"$ref": "#/$defs/A"})),
            &json!({"x": 1})
        );
        let loops = json!({"$defs": {"A": {"$ref": "#/$defs/A"}}});
        assert_eq!(
            deref(&loops, &json!({"$ref": "#/$defs/A"}))["$ref"],
            "#/$defs/A"
        );
    }

    #[test]
    fn engine_choices() {
        let c = |s: &str| engine_choice(&json!({"common": {"engine": s}}));
        assert_eq!(c("trl"), Some(("trl".into(), None)));
        assert_eq!(c("trl@0.1.0"), Some(("trl".into(), Some("0.1.0".into()))));
        assert_eq!(c("trl@"), None);
        assert_eq!(c(""), None);
        assert_eq!(engine_choice(&json!({})), None);
    }

    #[test]
    fn hashes_match_p17() {
        let v = json!({"b": 1, "a": {"y": 2, "x": 1}});
        let expected = Sha256::digest(br#"{"a":{"x":1,"y":2},"b":1}"#).to_vec();
        assert_eq!(sha256(&v), expected, "compact JSON, sorted keys");
    }
}
