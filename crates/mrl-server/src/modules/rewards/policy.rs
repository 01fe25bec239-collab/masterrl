//! Checks a reward version before it is stored: the policy against `reward-policy/v1`, every
//! component reference against the component list, params against each component's schema,
//! and the test cases' shape. The server never runs reward code; the REWARD_PREVIEW job runs
//! the version in the sandbox and decides whether its tests pass.
//!
//! Learned reward models (`*/model.*`) and judge models (`judge/…`, `*/judge.*`) are refused
//! with MRL-RWD-290 until they exist (P41). `org/` and `hub/` components need a code bundle;
//! the sandbox resolves them when it loads the bundle.

use super::codes;
use super::hash::ComponentDigest;
use super::Catalog;
use crate::error::FieldError;
use mrl_domain::ids::ModelVersionId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::OnceLock;

/// The policy schema, byte-identical to `schemas/reward-policy/v1.schema.json`.
const SCHEMA: &str = include_str!("../../../../../schemas/reward-policy/v1.schema.json");

/// Largest policy document, in bytes of compact JSON.
pub const MAX_POLICY_BYTES: usize = 1 << 20;
/// Longest prompt or completion in a test case.
pub const MAX_CASE_TEXT: usize = 64 * 1024;
/// Largest `reference` of a test case, in bytes of compact JSON.
pub const MAX_CASE_REFERENCE: usize = 256 * 1024;

/// What a checked policy needs for its hash and its record.
#[derive(Debug, Clone, PartialEq)]
pub struct Checked {
    /// Every distinct component reference with its code digest, sorted.
    pub components: Vec<ComponentDigest>,
    /// `standard` (Tier 1 only) or `code_exec` (some component runs model-written code).
    pub sandbox_tier: &'static str,
    /// Model versions named in params (`mv_…`), sorted.
    pub model_refs: Vec<String>,
    /// Component ids in policy order.
    pub component_ids: Vec<String>,
}

fn problem(
    path: &str,
    code: mrl_domain::errors::ErrorCode,
    message: String,
    hint: &str,
) -> FieldError {
    FieldError {
        path: path.to_owned(),
        code: code.to_string(),
        message,
        hint: hint.to_owned(),
    }
}

fn validator() -> Result<&'static jsonschema::Validator, String> {
    static V: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();
    V.get_or_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA).map_err(|e| e.to_string())?;
        jsonschema::draft202012::new(&schema).map_err(|e| e.to_string())
    })
    .as_ref()
    .map_err(Clone::clone)
}

/// Why a reference names something that doesn't exist yet, if it does.
fn learned(reference: &str) -> Option<&'static str> {
    let (ns, rest) = reference.split_once('/')?;
    let name = rest.split('@').next().unwrap_or(rest);
    if ns == "judge" || name.starts_with("judge.") {
        Some("judge models")
    } else if name.starts_with("model.") {
        Some("learned reward models")
    } else {
        None
    }
}

fn model_ids(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::String(s) if s.starts_with("mv_") && s.parse::<ModelVersionId>().is_ok() => {
            out.insert(s.clone());
        }
        Value::Array(items) => items.iter().for_each(|i| model_ids(i, out)),
        Value::Object(map) => map.values().for_each(|i| model_ids(i, out)),
        _ => {}
    }
}

/// Checks `policy`. `code_bundle` is the bundle hash when the version has code.
pub fn check(
    policy: &Value,
    catalog: &Catalog,
    code_bundle: Option<&str>,
) -> Result<Checked, Vec<FieldError>> {
    let size = serde_json::to_vec(policy)
        .map(|b| b.len())
        .unwrap_or(usize::MAX);
    if size > MAX_POLICY_BYTES {
        return Err(vec![problem(
            "/policy",
            codes::POLICY_INVALID,
            format!("The policy is {size} bytes; the limit is {MAX_POLICY_BYTES} bytes."),
            "Make the policy smaller, for example by moving long text into the code bundle.",
        )]);
    }
    let v = validator().map_err(|e| {
        vec![problem(
            "/policy",
            codes::POLICY_INVALID,
            format!("The policy schema can't be used: {e}"),
            "Report this problem; the server's policy schema is damaged.",
        )]
    })?;
    let schema_problems: Vec<FieldError> = v
        .iter_errors(policy)
        .take(20)
        .map(|e| {
            problem(
                &format!("/policy{}", e.instance_path().as_str()),
                codes::POLICY_INVALID,
                format!(
                    "This part of the policy doesn't fit RewardPolicy v1: {}.",
                    e.masked()
                ),
                "Fix this field; see the reward policy reference for the format.",
            )
        })
        .collect();
    if !schema_problems.is_empty() {
        return Err(schema_problems);
    }
    let mut out = Vec::new();
    if policy.pointer("/metadata/signal").and_then(Value::as_str) == Some("scalar.step") {
        out.push(problem(
            "/policy/metadata/signal",
            codes::STEP_UNSUPPORTED,
            "Step-by-step rewards (scalar.step) aren't available yet.".into(),
            "Use scalar.sequence: one reward per answer.",
        ));
    }
    let comps = policy
        .pointer("/spec/components")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut ids = BTreeSet::new();
    let mut component_ids = Vec::new();
    let mut digests = BTreeSet::new();
    let mut tier2 = false;
    let mut models = BTreeSet::new();
    for (i, c) in comps.iter().enumerate() {
        let at = format!("/policy/spec/components/{i}");
        let id = c.get("id").and_then(Value::as_str).unwrap_or_default();
        if !ids.insert(id.to_owned()) {
            out.push(problem(
                &format!("{at}/id"),
                codes::POLICY_INVALID,
                format!("Two components have the id `{id}`."),
                "Give every component its own id.",
            ));
        }
        component_ids.push(id.to_owned());
        let reference = c.get("use").and_then(Value::as_str).unwrap_or_default();
        let params = c
            .get("params")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        model_ids(&params, &mut models);
        if let Some(what) = learned(reference) {
            out.push(problem(
                &format!("{at}/use"),
                codes::LEARNED_UNAVAILABLE,
                format!("`{reference}` uses {what}, which aren't available yet."),
                "Use built-in components or your own code for now.",
            ));
            continue;
        }
        if reference.starts_with("builtin/") {
            let Some(row) = catalog.get(reference) else {
                out.push(problem(
                    &format!("{at}/use"),
                    codes::COMPONENT_UNKNOWN,
                    format!("No component `{reference}` is installed."),
                    "Pick a component from GET /reward-components.",
                ));
                continue;
            };
            tier2 |= row.tier == 2;
            digests.insert(ComponentDigest {
                reference: reference.to_owned(),
                code_sha256: row.code_sha256.clone(),
            });
            match jsonschema::draft202012::new(&row.param_schema) {
                Ok(pv) => out.extend(pv.iter_errors(&params).take(10).map(|e| {
                    problem(
                        &format!("{at}/params{}", e.instance_path().as_str()),
                        codes::PARAMS_INVALID,
                        format!("This setting doesn't fit `{reference}`: {}.", e.masked()),
                        "Check the component's settings in GET /reward-components.",
                    )
                })),
                Err(e) => out.push(problem(
                    &format!("{at}/use"),
                    codes::COMPONENT_UNKNOWN,
                    format!("`{reference}` has a settings schema the server can't read: {e}."),
                    "Report this problem; the component list is damaged.",
                )),
            }
        } else {
            match code_bundle {
                Some(bundle) => {
                    digests.insert(ComponentDigest {
                        reference: reference.to_owned(),
                        code_sha256: bundle.to_owned(),
                    });
                }
                None => out.push(problem(
                    &format!("{at}/use"),
                    codes::COMPONENT_UNKNOWN,
                    format!("`{reference}` is your own code, but this version has no code bundle."),
                    "Attach the code bundle with code_artifact_id, or use a built-in component.",
                )),
            }
        }
    }
    if !out.is_empty() {
        return Err(out);
    }
    Ok(Checked {
        components: digests.into_iter().collect(),
        sandbox_tier: if tier2 { "code_exec" } else { "standard" },
        model_refs: models.into_iter().collect(),
        component_ids,
    })
}

/// One expected number: exact, `{approx, tol}`, `{min, max}`, or `null` (no value: masked
/// or not applicable).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Expected {
    /// Exactly this value.
    Exact(f64),
    /// Within `tol` of `approx`.
    Approx {
        /// The value.
        approx: f64,
        /// Allowed difference (≥ 0).
        tol: f64,
    },
    /// Inside the bounds (either may be left out).
    Range {
        /// Lowest allowed.
        #[serde(default)]
        min: Option<f64>,
        /// Highest allowed.
        #[serde(default)]
        max: Option<f64>,
    },
}

/// What a test case expects (Doc 06 §1.4.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RewardCaseExpect)]
pub struct Expect {
    /// The sample's total; `null` expects no total (masked).
    #[serde(
        default,
        deserialize_with = "some_value",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(value_type = Option<Object>)]
    pub total: Option<Option<Expected>>,
    /// Per component id.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub components: std::collections::BTreeMap<String, Option<Expected>>,
    /// Flags the sample must carry.
    #[serde(default)]
    pub flags: Vec<String>,
    /// Flags it must not carry.
    #[serde(default)]
    pub absent_flags: Vec<String>,
    /// `SCORED`, `MASKED` or `FALLBACK_APPLIED`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
}

fn some_value<'de, D>(d: D) -> Result<Option<Option<Expected>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<Expected>::deserialize(d).map(Some)
}

/// The sample a test case scores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RewardCaseSample)]
pub struct CaseSample {
    /// The prompt as the model saw it.
    pub prompt: String,
    /// The model's answer.
    pub completion: String,
    /// What the answer is checked against (any JSON: a string, or `{answer, …}`).
    #[serde(default)]
    #[schema(value_type = Object)]
    pub reference: Value,
    /// Tokens in the completion (length components read it).
    #[serde(default)]
    pub completion_token_count: u32,
    /// `stop`, `length`, `tool_calls` or `abort`.
    #[serde(default = "stop")]
    pub finish_reason: String,
}

fn stop() -> String {
    "stop".into()
}

/// One test case as submitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = RewardTestCaseInput)]
pub struct TestCase {
    /// Short name, unique in the version.
    pub name: String,
    /// The sample.
    pub sample: CaseSample,
    /// What must come out.
    pub expect: Expect,
}

fn finite(e: &Expected) -> bool {
    match e {
        Expected::Exact(v) => v.is_finite(),
        Expected::Approx { approx, tol } => approx.is_finite() && tol.is_finite() && *tol >= 0.0,
        Expected::Range { min, max } => {
            min.is_none_or(f64::is_finite)
                && max.is_none_or(f64::is_finite)
                && (min.is_some() || max.is_some())
                && min.zip(*max).is_none_or(|(lo, hi)| lo <= hi)
        }
    }
}

/// Reads the test cases (raw JSON, so each problem names its case) against the policy's
/// component ids.
pub fn parse_cases(
    raw: &[Value],
    component_ids: &[String],
    max: usize,
) -> Result<Vec<TestCase>, Vec<FieldError>> {
    let bad = |i: usize, field: &str, message: String| {
        problem(
            &format!("/test_cases/{i}{field}"),
            codes::CASE_INVALID,
            message,
            "Fix this test case; see the reward test case format.",
        )
    };
    if raw.len() > max {
        return Err(vec![bad(
            max,
            "",
            format!(
                "A version can have at most {max} test cases (got {}).",
                raw.len()
            ),
        )]);
    }
    let mut out = Vec::new();
    let mut problems = Vec::new();
    let mut names = BTreeSet::new();
    for (i, v) in raw.iter().enumerate() {
        let case: TestCase = match serde_json::from_value(v.clone()) {
            Ok(c) => c,
            Err(e) => {
                problems.push(bad(i, "", format!("This test case can't be read: {e}.")));
                continue;
            }
        };
        let name = case.name.trim();
        if name.is_empty() || name.chars().count() > 200 {
            problems.push(bad(
                i,
                "/name",
                "Give the case a name of 1–200 characters.".into(),
            ));
        } else if !names.insert(name.to_owned()) {
            problems.push(bad(i, "/name", format!("Two cases are named `{name}`.")));
        }
        for (field, text) in [
            ("prompt", &case.sample.prompt),
            ("completion", &case.sample.completion),
        ] {
            if text.len() > MAX_CASE_TEXT {
                problems.push(bad(
                    i,
                    &format!("/sample/{field}"),
                    format!("The {field} is longer than {MAX_CASE_TEXT} bytes."),
                ));
            }
        }
        let reference_size =
            serde_json::to_vec(&case.sample.reference).map_or(usize::MAX, |b| b.len());
        if reference_size > MAX_CASE_REFERENCE {
            problems.push(bad(
                i,
                "/sample/reference",
                format!("The reference is larger than {MAX_CASE_REFERENCE} bytes."),
            ));
        }
        if !["stop", "length", "tool_calls", "abort"].contains(&case.sample.finish_reason.as_str())
        {
            problems.push(bad(
                i,
                "/sample/finish_reason",
                "finish_reason must be stop, length, tool_calls or abort.".into(),
            ));
        }
        let e = &case.expect;
        if e.total.is_none()
            && e.components.is_empty()
            && e.flags.is_empty()
            && e.absent_flags.is_empty()
            && e.resolution.is_none()
        {
            problems.push(bad(
                i,
                "/expect",
                "Say what must come out: total, components, flags or resolution.".into(),
            ));
        }
        if let Some(Some(t)) = &e.total {
            if !finite(t) {
                problems.push(bad(
                    i,
                    "/expect/total",
                    "Use finite numbers (and tol ≥ 0, min ≤ max).".into(),
                ));
            }
        }
        for (id, want) in &e.components {
            if !component_ids.contains(id) {
                problems.push(bad(
                    i,
                    &format!("/expect/components/{id}"),
                    format!("The policy has no component `{id}`."),
                ));
            } else if want.as_ref().is_some_and(|w| !finite(w)) {
                problems.push(bad(
                    i,
                    &format!("/expect/components/{id}"),
                    "Use finite numbers (and tol ≥ 0, min ≤ max).".into(),
                ));
            }
        }
        if let Some(r) = &e.resolution {
            if !["SCORED", "MASKED", "FALLBACK_APPLIED"].contains(&r.as_str()) {
                problems.push(bad(
                    i,
                    "/expect/resolution",
                    "resolution must be SCORED, MASKED or FALLBACK_APPLIED.".into(),
                ));
            }
        }
        if [&case.name, &case.sample.prompt, &case.sample.completion]
            .iter()
            .any(|t| t.contains('\0'))
        {
            problems.push(bad(
                i,
                "",
                "Text can't contain the NUL character (\\u0000).".into(),
            ));
        }
        out.push(case);
    }
    if problems.is_empty() {
        Ok(out)
    } else {
        Err(problems)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn catalog() -> Catalog {
        Catalog::embedded().unwrap()
    }

    pub(crate) fn math_policy() -> Value {
        json!({
            "apiVersion": "mrl.dev/reward/v1",
            "kind": "RewardPolicy",
            "metadata": {"name": "math", "signal": "scalar.sequence"},
            "spec": {
                "components": [
                    {"id": "correct", "use": "builtin/math.verify@1", "weight": 1.0},
                    {"id": "format", "use": "builtin/format.think_answer_tags@1", "weight": 0.2}
                ],
                "aggregation": {"method": "weighted_sum", "where": "platform"},
                "failure": {"on_timeout": {"action": "mask"}, "on_error": {"action": "mask"},
                            "max_failure_rate": 0.2},
                "tests": "tests.yaml"
            }
        })
    }

    #[test]
    fn a_builtin_policy_checks_and_lists_its_digests() {
        let c = check(&math_policy(), &catalog(), None).unwrap();
        assert_eq!(c.sandbox_tier, "standard");
        assert_eq!(c.components.len(), 2);
        assert!(c.components.iter().all(|d| d.code_sha256.len() == 64));
        assert_eq!(c.component_ids, ["correct", "format"]);
    }

    #[test]
    fn learned_reward_models_and_judges_are_refused_with_rwd_290() {
        for r in [
            "judge/helpfulness@1",
            "builtin/model.reward@1",
            "hub/judge.pairwise@2",
        ] {
            let mut p = math_policy();
            p["spec"]["components"][0]["use"] = json!(r);
            let e = check(&p, &catalog(), Some(&"a".repeat(64))).unwrap_err();
            assert_eq!(e[0].code, "MRL-RWD-290", "{r}: {e:?}");
            assert_eq!(e[0].path, "/policy/spec/components/0/use");
        }
    }

    #[test]
    fn unknown_builtins_bad_params_schema_and_user_code_without_bundle() {
        let mut p = math_policy();
        p["spec"]["components"][0]["use"] = json!("builtin/nope@1");
        assert_eq!(
            check(&p, &catalog(), None).unwrap_err()[0].code,
            "MRL-RWD-142"
        );
        let mut p = math_policy();
        p["spec"]["components"][0]["params"] = json!({"no_such_setting": 1});
        assert_eq!(
            check(&p, &catalog(), None).unwrap_err()[0].code,
            "MRL-RWD-143"
        );
        let mut p = math_policy();
        p["spec"]["components"][0]["use"] = json!("org/mine@1");
        assert_eq!(
            check(&p, &catalog(), None).unwrap_err()[0].code,
            "MRL-RWD-142"
        );
        let ok = check(&p, &catalog(), Some(&"b".repeat(64))).unwrap();
        assert!(ok.components.iter().any(|d| d.reference == "org/mine@1"));
        let mut p = math_policy();
        p["kind"] = json!("Other");
        assert_eq!(
            check(&p, &catalog(), None).unwrap_err()[0].code,
            "MRL-RWD-140"
        );
        let mut p = math_policy();
        p["metadata"]["signal"] = json!("scalar.step");
        assert_eq!(
            check(&p, &catalog(), None).unwrap_err()[0].code,
            "MRL-RWD-148"
        );
    }

    #[test]
    fn code_components_make_the_version_tier2() {
        let mut p = math_policy();
        p["spec"]["components"][0]["use"] = json!("builtin/code.unit_tests@1");
        assert_eq!(
            check(&p, &catalog(), None).unwrap().sandbox_tier,
            "code_exec"
        );
    }

    #[test]
    fn test_cases_are_read_and_problems_name_the_case() {
        let ids = vec!["correct".to_owned()];
        let good = json!({"name": "right", "sample": {"prompt": "1+1?", "completion": "2",
            "reference": "2"}, "expect": {"total": {"approx": 1.0, "tol": 0.01},
            "components": {"correct": 1}}});
        let cases = parse_cases(std::slice::from_ref(&good), &ids, 10).unwrap();
        assert_eq!(cases[0].sample.finish_reason, "stop");
        assert_eq!(
            cases[0].expect.total,
            Some(Some(Expected::Approx {
                approx: 1.0,
                tol: 0.01
            }))
        );
        let masked = json!({"name": "m", "sample": {"prompt": "p", "completion": "c"},
            "expect": {"total": null}});
        assert_eq!(
            parse_cases(&[masked], &ids, 10).unwrap()[0].expect.total,
            Some(None)
        );
        let bad = [
            json!({"name": "x", "sample": {"prompt": "p", "completion": "c"}, "expect": {}}),
            json!({"name": "y", "sample": {"prompt": "p", "completion": "c"},
                   "expect": {"components": {"nope": 1}}}),
            json!({"name": "z", "sample": {"prompt": "p"}, "expect": {"total": 1}}),
        ];
        let e = parse_cases(&bad, &ids, 10).unwrap_err();
        assert_eq!(e.len(), 3);
        assert!(e.iter().all(|p| p.code == "MRL-VAL-290"));
        assert_eq!(e[1].path, "/test_cases/1/expect/components/nope");
        let dup = parse_cases(&[good.clone(), good], &ids, 10).unwrap_err();
        assert_eq!(dup[0].path, "/test_cases/1/name");
        assert!(parse_cases(&[json!({})], &ids, 0).is_err());
    }
}
