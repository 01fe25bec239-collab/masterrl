//! The content hash of a reward version (Doc 06 §1.4.1): sha256 over the RFC 8785 canonical
//! JSON of everything that decides how the reward scores.
//!
//! [`HashInputs`] covers the policy, each component reference with its code digest, the code
//! bundle, the executor environment, referenced model versions, judge templates (none until
//! P41) and the SDK contract. Two versions with the same inputs have the same hash, whatever
//! key order or number spelling the request used.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Format tag of the hash inputs (bumped if their meaning changes).
pub const CONTENT_FORMAT: &str = "mrl-reward-content/1";

/// The bundle line format the executor hashes code files with (P14's `mrl-reward-bundle/1`).
pub const BUNDLE_FILE_LINE: &str = "file";

/// One component reference and the digest of the code behind it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ComponentDigest {
    /// `builtin/math.verify@1`, `org/my.reward@1`, …
    #[serde(rename = "ref")]
    pub reference: String,
    /// sha256 of the built-in's source file, or the code bundle hash for `org/` and `hub/`.
    pub code_sha256: String,
}

/// Everything the content hash covers.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HashInputs {
    /// [`CONTENT_FORMAT`].
    pub format: String,
    /// The reward SDK contract the executor speaks (`mrl-rewards/1`).
    pub sdk_contract: String,
    /// The policy document.
    pub policy: Value,
    /// Every distinct component reference, sorted.
    pub components: Vec<ComponentDigest>,
    /// The code bundle, if any.
    pub code_bundle_sha256: Option<String>,
    /// The executor environment (mrl-rewards version, its pinned dependencies and built-ins).
    pub executor_sha256: String,
    /// Model versions the policy reads (`mv_…`), sorted.
    pub model_refs: Vec<String>,
    /// Judge prompt templates (none until learned judges exist).
    pub judge_templates: Vec<String>,
}

impl HashInputs {
    /// The inputs as JSON (stored next to the hash so a reader can see what it covers).
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// sha256 (lowercase hex) of the canonical JSON of the inputs.
    pub fn content_sha256(&self) -> String {
        sha256_hex(canonical(&self.to_value()).as_bytes())
    }
}

/// Lowercase hex sha256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let d = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in d {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// The code bundle hash: sha256 over `file <path> <sha256>` lines sorted by path, the same
/// lines the executor adds to its bundle hash for the files under `code/`.
pub fn bundle_sha256<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> String {
    let mut files: Vec<(&str, &str)> = files.into_iter().collect();
    files.sort_unstable();
    let mut text = String::new();
    for (path, sha) in files {
        let _ = writeln!(text, "{BUNDLE_FILE_LINE} {path} {sha}");
    }
    sha256_hex(text.as_bytes())
}

/// RFC 8785 (JCS) canonical JSON of `v`: object keys sorted by UTF-16 code units, no
/// whitespace, numbers as ECMAScript prints doubles, strings escaped like `JSON.stringify`.
pub fn canonical(v: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, v);
    out
}

fn write_value(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&number(n.as_f64().unwrap_or(0.0))),
        Value::String(s) => write_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(out, k);
                out.push(':');
                if let Some(item) = map.get(k) {
                    write_value(out, item);
                }
            }
            out.push('}');
        }
    }
}

fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{09}' => out.push_str("\\t"),
            '\u{0a}' => out.push_str("\\n"),
            '\u{0c}' => out.push_str("\\f"),
            '\u{0d}' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// ECMAScript `Number.prototype.toString` for a finite double (JSON has no NaN or infinity).
fn number(v: f64) -> String {
    if v == 0.0 || !v.is_finite() {
        return "0".to_owned();
    }
    let sign = if v < 0.0 { "-" } else { "" };
    // `{:e}` prints the shortest digits that read back as the same double.
    let sci = format!("{:e}", v.abs());
    let (mantissa, exp) = sci.split_once('e').unwrap_or((sci.as_str(), "0"));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let k = digits.len() as i32;
    let n = exp.parse::<i32>().unwrap_or(0) + 1;
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", &digits[..n as usize], &digits[n as usize..])
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let e = n - 1;
        let e = if e >= 0 {
            format!("+{e}")
        } else {
            e.to_string()
        };
        if k == 1 {
            format!("{digits}e{e}")
        } else {
            format!("{}.{}e{e}", &digits[..1], &digits[1..])
        }
    };
    format!("{sign}{body}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_print_like_ecmascript() {
        // Values from RFC 8785 Appendix B and the ECMAScript spec.
        let cases = [
            (0.0, "0"),
            (-0.0, "0"),
            (1.0, "1"),
            (-1.5, "-1.5"),
            (1e21, "1e+21"),
            (1e20, "100000000000000000000"),
            (123456789012345680000.0, "123456789012345680000"),
            (0.000001, "0.000001"),
            (0.0000001, "1e-7"),
            (1.2345e-7, "1.2345e-7"),
            (5e-324, "5e-324"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (333333333.3333333, "333333333.3333333"),
            (0.1 + 0.2, "0.30000000000000004"),
            (9007199254740992.0, "9007199254740992"),
        ];
        for (v, want) in cases {
            assert_eq!(number(v), want, "{v:e}");
        }
    }

    #[test]
    fn canonical_sorts_by_utf16_and_escapes_like_json_stringify() {
        // RFC 8785 §3.2.3: "\u{20ac}" (U+20AC) sorts before "\u{1f600}" (a surrogate pair,
        // 0xD83D) by UTF-16 units, although its UTF-8 bytes compare the other way.
        let v = json!({"\u{1f600}": 1, "\u{20ac}": 2, "a": [true, null, "x\n\u{1}\"/"], "B": 1.50});
        assert_eq!(
            canonical(&v),
            "{\"B\":1.5,\"a\":[true,null,\"x\\n\\u0001\\\"/\"],\"\u{20ac}\":2,\"\u{1f600}\":1}"
        );
    }

    fn inputs(policy: Value) -> HashInputs {
        HashInputs {
            format: CONTENT_FORMAT.into(),
            sdk_contract: "mrl-rewards/1".into(),
            policy,
            components: vec![ComponentDigest {
                reference: "builtin/math.verify@1".into(),
                code_sha256: "a".repeat(64),
            }],
            code_bundle_sha256: None,
            executor_sha256: "e".repeat(64),
            model_refs: vec![],
            judge_templates: vec![],
        }
    }

    /// RW-3: same spec → same hash; each input that changes the reward changes the hash.
    #[test]
    fn rw3_same_spec_same_hash_and_every_input_counts() {
        let a = inputs(json!({"spec": {"w": 1.0, "b": [1, 2]}, "kind": "RewardPolicy"}));
        let reordered = inputs(
            serde_json::from_str(r#"{"kind":"RewardPolicy","spec":{"b":[1,2.0],"w":1}}"#).unwrap(),
        );
        assert_eq!(a.content_sha256(), reordered.content_sha256());
        let base = a.content_sha256();
        let mut code = a.clone();
        code.components[0].code_sha256 = "b".repeat(64);
        let mut env = a.clone();
        env.executor_sha256 = "f".repeat(64);
        let mut model = a.clone();
        model.model_refs = vec!["mv_01".into()];
        let mut bundle = a.clone();
        bundle.code_bundle_sha256 = Some("c".repeat(64));
        let mut policy = a.clone();
        policy.policy["spec"]["w"] = json!(2.0);
        for (what, changed) in [
            ("code digest", code),
            ("executor env", env),
            ("model ref", model),
            ("code bundle", bundle),
            ("policy", policy),
        ] {
            assert_ne!(
                changed.content_sha256(),
                base,
                "{what} must change the hash"
            );
        }
    }

    #[test]
    fn bundle_hash_is_order_free_and_matches_the_executor_lines() {
        let one = bundle_sha256([("b.py", "22"), ("components.yaml", "11")]);
        let two = bundle_sha256([("components.yaml", "11"), ("b.py", "22")]);
        assert_eq!(one, two);
        assert_eq!(one, sha256_hex(b"file b.py 22\nfile components.yaml 11\n"));
    }
}
