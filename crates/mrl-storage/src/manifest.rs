//! `artifact-manifest/v1`: the list of files in a model, dataset, checkpoint, eval or export.

use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{Result, StorageError};
use crate::hash::sha256_file;
use crate::keys::{relative_path, sha256_hex};
use crate::store::Store;

/// The only schema version this crate reads and writes.
pub const SCHEMA_VERSION: &str = "artifact-manifest/v1";

/// What the artifact is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ArtifactKind {
    /// A model version.
    Model,
    /// A dataset version.
    Dataset,
    /// A training checkpoint.
    Checkpoint,
    /// Evaluation results.
    Eval,
    /// An export bundle.
    Export,
    /// Anything else.
    Other,
}

/// One file in the artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestFile {
    /// Path relative to the artifact, using `/`.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Lowercase hex sha256.
    pub sha256: String,
    /// What the file is for, such as `weights` or `tokenizer`.
    pub role: String,
    /// File format, such as `safetensors`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// An `artifact-manifest/v1` document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactManifest {
    /// Always [`SCHEMA_VERSION`].
    pub schema_version: String,
    /// What the artifact is.
    pub kind: ArtifactKind,
    /// When the manifest was built.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// The files.
    pub files: Vec<ManifestFile>,
    /// Where the artifact came from (free-form).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<serde_json::Map<String, serde_json::Value>>,
}

/// A local file to put in a manifest.
#[derive(Debug, Clone)]
pub struct FileSpec {
    /// Path relative to the artifact root, using `/`.
    pub path: String,
    /// What the file is for.
    pub role: String,
    /// File format, if known.
    pub format: Option<String>,
}

/// Why a stored file does not match its manifest entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "problem", rename_all = "snake_case")]
pub enum FileProblem {
    /// No object at the expected key.
    Missing {
        /// Manifest path.
        path: String,
    },
    /// The object has a different size.
    SizeMismatch {
        /// Manifest path.
        path: String,
        /// Size in the manifest.
        expected: u64,
        /// Size in storage.
        actual: u64,
    },
    /// The object content hashes differently.
    HashMismatch {
        /// Manifest path.
        path: String,
        /// sha256 in the manifest.
        expected: String,
        /// sha256 of the stored object.
        actual: String,
    },
}

impl ArtifactManifest {
    /// Builds a manifest by hashing local files under `root`.
    pub async fn build(
        kind: ArtifactKind,
        root: &Path,
        files: Vec<FileSpec>,
        source: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<Self> {
        let mut out = Vec::with_capacity(files.len());
        for spec in files {
            relative_path(&spec.path).map_err(|e| StorageError::ManifestFormat(e.to_string()))?;
            let (size, sha256) = sha256_file(root.join(&spec.path)).await?;
            out.push(ManifestFile {
                path: spec.path,
                size,
                sha256,
                role: spec.role,
                format: spec.format,
            });
        }
        let manifest = Self {
            schema_version: SCHEMA_VERSION.to_owned(),
            kind,
            created_at: OffsetDateTime::now_utc(),
            files: out,
            source,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Parses and validates manifest JSON.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|e| StorageError::ManifestFormat(e.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Pretty JSON.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|e| StorageError::ManifestFormat(e.to_string()))
    }

    /// Checks fields the type system cannot: version, paths, digests, roles, duplicates.
    pub fn validate(&self) -> Result<()> {
        let bad = |msg: String| Err(StorageError::ManifestFormat(msg));
        if self.schema_version != SCHEMA_VERSION {
            return bad(format!(
                "schema_version is {:?}, expected {SCHEMA_VERSION:?}",
                self.schema_version
            ));
        }
        let mut seen = HashSet::new();
        for f in &self.files {
            if relative_path(&f.path).is_err() {
                return bad(format!(
                    "file path {:?} is not a safe relative path",
                    f.path
                ));
            }
            if sha256_hex(&f.sha256).is_err() {
                return bad(format!(
                    "file {:?} has a sha256 that is not 64 lowercase hex characters",
                    f.path
                ));
            }
            if f.role.is_empty() {
                return bad(format!("file {:?} has an empty role", f.path));
            }
            if !seen.insert(f.path.as_str()) {
                return bad(format!("file {:?} is listed twice", f.path));
            }
        }
        Ok(())
    }

    /// Checks every file exists at `key_of(file)` with the listed size; with `rehash`, also
    /// streams each object and compares its sha256.
    pub async fn verify<F>(&self, store: &Store, rehash: bool, key_of: F) -> Result<()>
    where
        F: Fn(&ManifestFile) -> Result<String>,
    {
        self.validate()?;
        let mut problems = Vec::new();
        for f in &self.files {
            let key = key_of(f)?;
            let path = f.path.clone();
            let info = match store.head(&key).await {
                Ok(info) => info,
                Err(StorageError::NotFound { .. }) => {
                    problems.push(FileProblem::Missing { path });
                    continue;
                }
                Err(e) => return Err(e),
            };
            if info.size != f.size {
                problems.push(FileProblem::SizeMismatch {
                    path,
                    expected: f.size,
                    actual: info.size,
                });
                continue;
            }
            if rehash {
                let actual = store.sha256(&key).await?;
                if actual != f.sha256 {
                    problems.push(FileProblem::HashMismatch {
                        path,
                        expected: f.sha256.clone(),
                        actual,
                    });
                }
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(StorageError::ManifestMismatch(problems))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn exact_json_shape() {
        let json = format!(
            r#"{{"schema_version":"artifact-manifest/v1","kind":"checkpoint","created_at":"2026-11-15T10:00:00Z",
            "files":[{{"path":"w/model.safetensors","size":0,"sha256":"{SHA}","role":"weights","format":"safetensors"}},
                     {{"path":"notes.txt","size":0,"sha256":"{SHA}","role":"notes"}}],
            "source":{{"run":"r1"}}}}"#
        );
        let m = ArtifactManifest::from_json(json.as_bytes()).unwrap();
        assert_eq!(m.kind, ArtifactKind::Checkpoint);
        let back: serde_json::Value = serde_json::from_slice(&m.to_json().unwrap()).unwrap();
        let orig: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(back, orig);
    }

    #[test]
    fn rejects_bad_manifests() {
        let ok = ArtifactManifest {
            schema_version: SCHEMA_VERSION.into(),
            kind: ArtifactKind::Model,
            created_at: OffsetDateTime::UNIX_EPOCH,
            files: vec![ManifestFile {
                path: "a".into(),
                size: 0,
                sha256: SHA.into(),
                role: "r".into(),
                format: None,
            }],
            source: None,
        };
        assert!(ok.validate().is_ok());
        let mut cases = Vec::new();
        cases.push(ArtifactManifest {
            schema_version: "artifact-manifest/v2".into(),
            ..ok.clone()
        });
        for (path, sha, role) in [
            ("../a", SHA, "r"),
            ("/a", SHA, "r"),
            ("a", &SHA[1..], "r"),
            ("a", &SHA.to_uppercase(), "r"),
            ("a", SHA, ""),
        ] {
            let file = ManifestFile {
                path: path.into(),
                size: 0,
                sha256: sha.into(),
                role: role.into(),
                format: None,
            };
            cases.push(ArtifactManifest {
                files: vec![file],
                ..ok.clone()
            });
        }
        cases.push(ArtifactManifest {
            files: vec![ok.files[0].clone(), ok.files[0].clone()],
            ..ok.clone()
        });
        for m in cases {
            assert_eq!(m.validate().unwrap_err().code(), "MRL-STORE-012", "{m:?}");
        }
        let extra = r#"{"schema_version":"artifact-manifest/v1","kind":"other","created_at":"2026-11-15T10:00:00Z","files":[],"extra":1}"#;
        assert!(ArtifactManifest::from_json(extra.as_bytes()).is_err());
    }
}
