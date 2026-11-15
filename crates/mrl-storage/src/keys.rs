//! Key layout. Every key lives under `orgs/{org_id}/`, and an [`OrgScope`] never builds
//! or accepts a key outside its own org.

use crate::error::{Result, StorageError};

/// Longest key accepted. S3 allows 1024 bytes.
pub const MAX_KEY_LEN: usize = 1024;
/// Longest single id or path segment.
pub const MAX_SEGMENT_LEN: usize = 255;

/// Derived files stored next to a dataset version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Derived {
    /// `stats.json`
    Stats,
    /// `token_hist.json`
    TokenHist,
    /// `validation_report.json`
    ValidationReport,
    /// `preview.parquet`
    Preview,
}

impl Derived {
    fn file_name(self) -> &'static str {
        match self {
            Self::Stats => "stats.json",
            Self::TokenHist => "token_hist.json",
            Self::ValidationReport => "validation_report.json",
            Self::Preview => "preview.parquet",
        }
    }
}

/// The key area of one org: `orgs/{org_id}/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgScope {
    org_id: String,
    prefix: String,
}

impl OrgScope {
    /// Creates the scope for `org_id`.
    pub fn new(org_id: &str) -> Result<Self> {
        id(org_id)?;
        Ok(Self {
            org_id: org_id.to_owned(),
            prefix: format!("orgs/{org_id}/"),
        })
    }

    /// The org id.
    pub fn org_id(&self) -> &str {
        &self.org_id
    }

    /// `orgs/{org_id}/`
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Accepts a full object key only if it is inside this org and well formed.
    pub fn check<'k>(&self, key: &'k str) -> Result<&'k str> {
        let bad = |reason| StorageError::BadKey {
            key: key.to_owned(),
            reason,
        };
        let rest = key
            .strip_prefix(&self.prefix)
            .ok_or_else(|| bad("it is outside this org"))?;
        if key.len() > MAX_KEY_LEN {
            return Err(bad("it is longer than 1024 bytes"));
        }
        if rest.is_empty() {
            return Err(bad("it names the org itself, not an object"));
        }
        for part in rest.split('/') {
            segment(part).map_err(|_| {
                bad("a part of it is empty, `.`, `..` or has disallowed characters")
            })?;
        }
        Ok(key)
    }

    /// Accepts a listing prefix inside this org: the org prefix itself, or a key ending in `/`.
    pub fn check_prefix<'k>(&self, prefix: &'k str) -> Result<&'k str> {
        if prefix == self.prefix {
            return Ok(prefix);
        }
        match prefix.strip_suffix('/') {
            Some(inner) => self.check(inner).map(|_| prefix),
            None => Err(StorageError::BadKey {
                key: prefix.to_owned(),
                reason: "a listing prefix must end with `/`",
            }),
        }
    }

    fn key(&self, parts: &[&str]) -> Result<String> {
        for p in parts {
            id(p)?;
        }
        let key = format!("{}{}", self.prefix, parts.join("/"));
        self.check(&key)?;
        Ok(key)
    }

    /// Joins `base` (a prefix ending in `/`) and a relative path such as a manifest entry.
    pub fn join(&self, base: &str, relative: &str) -> Result<String> {
        self.check_prefix(base)?;
        relative_path(relative)?;
        let key = format!("{base}{relative}");
        self.check(&key)?;
        Ok(key)
    }

    /// `blobs/sha256/{aa}/{bb}/{sha256}`
    pub fn blob(&self, sha256: &str) -> Result<String> {
        sha256_hex(sha256)?;
        self.key(&["blobs", "sha256", &sha256[..2], &sha256[2..4], sha256])
    }

    /// `datasets/{ds}/versions/{dsv}/manifest.json`
    pub fn dataset_manifest(&self, ds: &str, dsv: &str) -> Result<String> {
        self.key(&["datasets", ds, "versions", dsv, "manifest.json"])
    }

    /// `datasets/{ds}/versions/{dsv}/derived/{file}`
    pub fn dataset_derived(&self, ds: &str, dsv: &str, file: Derived) -> Result<String> {
        self.key(&["datasets", ds, "versions", dsv, "derived", file.file_name()])
    }

    /// `models/{mdl}/versions/{mv}/manifest.json`
    pub fn model_manifest(&self, mdl: &str, mv: &str) -> Result<String> {
        self.key(&["models", mdl, "versions", mv, "manifest.json"])
    }

    /// `runs/{run}/config/resolved.yaml`
    pub fn run_config(&self, run: &str) -> Result<String> {
        self.key(&["runs", run, "config", "resolved.yaml"])
    }

    /// `runs/{run}/checkpoints/{ckpt}/manifest.json`
    pub fn checkpoint_manifest(&self, run: &str, ckpt: &str) -> Result<String> {
        self.key(&["runs", run, "checkpoints", ckpt, "manifest.json"])
    }

    /// `runs/{run}/checkpoints/{ckpt}/_staging/attempt-{attempt}/` (a prefix).
    ///
    /// Each attempt writes only under its own prefix, so a fenced-off old attempt can never
    /// overwrite the files of the attempt that replaced it.
    pub fn checkpoint_staging(&self, run: &str, ckpt: &str, attempt: &str) -> Result<String> {
        id(attempt)?;
        let dir = format!("attempt-{attempt}");
        Ok(format!(
            "{}/",
            self.key(&["runs", run, "checkpoints", ckpt, "_staging", &dir])?
        ))
    }

    /// `runs/{run}/logs/{attempt}/{rank}/{seq}.log.zst`
    pub fn run_log(&self, run: &str, attempt: &str, rank: u32, seq: u64) -> Result<String> {
        self.key(&[
            "runs",
            run,
            "logs",
            attempt,
            &rank.to_string(),
            &format!("{seq}.log.zst"),
        ])
    }

    /// `runs/{run}/samples/{step}.jsonl.zst`
    pub fn run_samples(&self, run: &str, step: u64) -> Result<String> {
        self.key(&["runs", run, "samples", &format!("{step}.jsonl.zst")])
    }

    /// `runs/{run}/rollouts/{policy_version}/{batch}.parquet`
    pub fn rollout(&self, run: &str, policy_version: &str, batch: &str) -> Result<String> {
        id(batch)?;
        self.key(&[
            "runs",
            run,
            "rollouts",
            policy_version,
            &format!("{batch}.parquet"),
        ])
    }

    /// `evals/{evr}/results.json`
    pub fn eval_results(&self, evr: &str) -> Result<String> {
        self.key(&["evals", evr, "results.json"])
    }

    /// `evals/{evr}/per_sample.parquet`
    pub fn eval_per_sample(&self, evr: &str) -> Result<String> {
        self.key(&["evals", evr, "per_sample.parquet"])
    }

    /// `exports/{export_id}/` (a prefix).
    pub fn export(&self, export_id: &str) -> Result<String> {
        Ok(format!("{}/", self.key(&["exports", export_id])?))
    }

    /// `uploads/{upload_id}/parts/` (a prefix). Staged parts here expire after 24 hours.
    pub fn upload_parts(&self, upload_id: &str) -> Result<String> {
        Ok(format!("{}/", self.key(&["uploads", upload_id, "parts"])?))
    }

    /// `uploads/{upload_id}/parts/{index}` with a zero-padded index.
    pub fn upload_part(&self, upload_id: &str, index: usize) -> Result<String> {
        self.key(&["uploads", upload_id, "parts", &format!("{index:06}")])
    }
}

/// The org id of a full key `orgs/{org}/...`, if it has that shape.
pub fn org_of(key: &str) -> Option<&str> {
    key.strip_prefix("orgs/")?
        .split_once('/')
        .map(|(org, _)| org)
}

/// One path segment: 1–255 bytes of `A-Z a-z 0-9 . _ - = + @ ,`, never `.` or `..`.
fn segment(s: &str) -> std::result::Result<(), &'static str> {
    if s.is_empty() || s.len() > MAX_SEGMENT_LEN {
        return Err("it must be 1 to 255 characters long");
    }
    if s == "." || s == ".." {
        return Err("it cannot be `.` or `..`");
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._-=+@,".contains(&b))
    {
        return Err("only letters, digits and . _ - = + @ , are allowed");
    }
    Ok(())
}

/// An id that becomes one key segment. Ids also may not start with `.`.
fn id(s: &str) -> Result<()> {
    let bad = |reason| StorageError::BadId {
        id: s.to_owned(),
        reason,
    };
    segment(s).map_err(bad)?;
    if s.starts_with('.') {
        return Err(bad("it cannot start with `.`"));
    }
    Ok(())
}

/// A relative path made of valid segments, such as `weights/model.safetensors`.
pub(crate) fn relative_path(s: &str) -> Result<()> {
    let bad = |reason| StorageError::BadKey {
        key: s.to_owned(),
        reason,
    };
    if s.is_empty() || s.len() > MAX_KEY_LEN {
        return Err(bad("a relative path must be 1 to 1024 characters long"));
    }
    for part in s.split('/') {
        segment(part).map_err(bad)?;
    }
    Ok(())
}

/// A lowercase hex sha256 digest.
pub(crate) fn sha256_hex(s: &str) -> Result<()> {
    if s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        Ok(())
    } else {
        Err(StorageError::BadId {
            id: s.to_owned(),
            reason: "a sha256 must be 64 lowercase hex characters",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn scope() -> OrgScope {
        OrgScope::new("org1").unwrap()
    }

    #[test]
    fn layout_matches_spec() {
        let s = scope();
        let sha = "ab".repeat(32);
        assert_eq!(
            s.blob(&sha).unwrap(),
            format!("orgs/org1/blobs/sha256/ab/ab/{sha}")
        );
        assert_eq!(
            s.dataset_manifest("d", "v").unwrap(),
            "orgs/org1/datasets/d/versions/v/manifest.json"
        );
        assert_eq!(
            s.dataset_derived("d", "v", Derived::TokenHist).unwrap(),
            "orgs/org1/datasets/d/versions/v/derived/token_hist.json"
        );
        assert_eq!(
            s.model_manifest("m", "1").unwrap(),
            "orgs/org1/models/m/versions/1/manifest.json"
        );
        assert_eq!(
            s.run_config("r").unwrap(),
            "orgs/org1/runs/r/config/resolved.yaml"
        );
        assert_eq!(
            s.checkpoint_manifest("r", "c").unwrap(),
            "orgs/org1/runs/r/checkpoints/c/manifest.json"
        );
        assert_eq!(
            s.checkpoint_staging("r", "c", "a7").unwrap(),
            "orgs/org1/runs/r/checkpoints/c/_staging/attempt-a7/"
        );
        assert_eq!(
            s.run_log("r", "a", 3, 9).unwrap(),
            "orgs/org1/runs/r/logs/a/3/9.log.zst"
        );
        assert_eq!(
            s.run_samples("r", 100).unwrap(),
            "orgs/org1/runs/r/samples/100.jsonl.zst"
        );
        assert_eq!(
            s.rollout("r", "p2", "b1").unwrap(),
            "orgs/org1/runs/r/rollouts/p2/b1.parquet"
        );
        assert_eq!(
            s.eval_results("e").unwrap(),
            "orgs/org1/evals/e/results.json"
        );
        assert_eq!(
            s.eval_per_sample("e").unwrap(),
            "orgs/org1/evals/e/per_sample.parquet"
        );
        assert_eq!(s.export("x").unwrap(), "orgs/org1/exports/x/");
        assert_eq!(
            s.upload_part("u", 7).unwrap(),
            "orgs/org1/uploads/u/parts/000007"
        );
    }

    #[test]
    fn rejects_escapes() {
        let s = scope();
        for key in [
            "orgs/org2/runs/r",
            "/orgs/org1/runs/r",
            "orgs/org1/../org2/x",
            "orgs/org1/runs/../../org2/x",
            "orgs/org1/runs//x",
            "orgs/org1/runs/./x",
            "orgs/org1/",
            "orgs/org1",
            "orgs/org10/x",
            "orgs/org1/a\\b",
            "runs/r",
        ] {
            assert!(s.check(key).is_err(), "{key} should be rejected");
        }
        assert!(s.dataset_manifest("..", "v").is_err());
        assert!(s.run_config("a/b").is_err());
        assert!(s.join("orgs/org1/x/", "../y").is_err());
        assert!(s.join("orgs/org2/x/", "y").is_err());
        assert!(OrgScope::new("../x").is_err());
    }

    fn assert_in_scope(s: &OrgScope, key: &str) {
        assert!(key.starts_with("orgs/org1/"), "{key} escaped the org");
        let rest = key["orgs/org1/".len()..].trim_end_matches('/');
        assert!(
            rest.split('/')
                .all(|p| !p.is_empty() && p != "." && p != ".."),
            "{key}"
        );
        assert_eq!(org_of(key), Some("org1"));
        assert!(s.check(key.trim_end_matches('/')).is_ok());
    }

    proptest! {
        #[test]
        fn no_built_key_escapes(a in ".{0,40}", b in ".{0,40}", c in "[./a-z]{0,12}", n in any::<u64>()) {
            let s = scope();
            let built = [
                s.dataset_manifest(&a, &b),
                s.dataset_derived(&a, &c, Derived::Preview),
                s.model_manifest(&c, &a),
                s.run_config(&a),
                s.checkpoint_manifest(&a, &c),
                s.checkpoint_staging(&a, &b, &c),
                s.run_log(&a, &c, n as u32, n),
                s.run_samples(&c, n),
                s.rollout(&a, &b, &c),
                s.eval_results(&c),
                s.export(&a),
                s.upload_part(&c, n as usize),
                s.join("orgs/org1/x/", &format!("{a}/{c}")),
                s.blob(&a),
            ];
            for key in built.into_iter().flatten() {
                assert_in_scope(&s, &key);
            }
        }

        #[test]
        fn checked_keys_stay_in_scope(key in "(orgs/(org1|org2|\\.\\.)/)?[a-z./\\\\]{0,30}") {
            let s = scope();
            if let Ok(k) = s.check(&key) {
                assert_in_scope(&s, k);
            }
        }
    }
}
