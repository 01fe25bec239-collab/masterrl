//! Built-in jobs can install their pinned runtime on a node with no prior project checkout.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::attempts::AttemptError;
use crate::AgentError;

include!(concat!(env!("OUT_DIR"), "/builtin_runtime.rs"));

const LOCK: &[u8] = include_bytes!("../../../../python/builtin-env/uv.lock");
const PREFIX: &str = ".mrl-builtin-";

fn digest() -> &'static str {
    static DIGEST: OnceLock<String> = OnceLock::new();
    DIGEST.get_or_init(|| {
        let mut manifest = String::new();
        for (path, bytes) in FILES {
            manifest.push_str(&mrl_storage::sha256_hex(path.as_bytes()));
            manifest.push_str(&mrl_storage::sha256_hex(bytes));
        }
        mrl_storage::sha256_hex(manifest.as_bytes())
    })
}

pub(crate) fn project(state: &Path, lock_sha256: &str) -> Result<Option<PathBuf>, AttemptError> {
    if lock_sha256 != mrl_storage::sha256_hex(LOCK) {
        return Ok(None);
    }
    let root = state.join("runtimes").join(format!("{PREFIX}{}", digest()));
    let ready = root.join(".ready");
    if !ready.is_file() {
        for (path, bytes) in FILES {
            let destination = root.join(path);
            if let Some(dir) = destination.parent() {
                std::fs::create_dir_all(dir).map_err(|e| AgentError::state(dir, e))?;
            }
            crate::state::atomic_write(&destination, bytes)
                .map_err(|e| AgentError::state(&destination, e))?;
        }
        crate::state::atomic_write(&ready, b"").map_err(|e| AgentError::state(&ready, e))?;
    }
    Ok(Some(root.join("builtin-env")))
}

pub(crate) fn cache_suffix(project: &Path) -> Option<&str> {
    if project.file_name()?.to_str()? != "builtin-env" {
        return None;
    }
    let hash = project
        .parent()?
        .file_name()?
        .to_str()?
        .strip_prefix(PREFIX)?;
    (hash.len() == 64 && hash.bytes().all(|c| c.is_ascii_hexdigit())).then_some(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_builtin_runtime_is_available_on_a_new_node() {
        let state = tempfile::tempdir().unwrap();
        let hash = mrl_storage::sha256_hex(LOCK);
        assert!(project(state.path(), &"0".repeat(64)).unwrap().is_none());
        let env = project(state.path(), &hash).unwrap().unwrap();
        assert_eq!(std::fs::read(env.join("uv.lock")).unwrap(), LOCK);
        assert!(env
            .join("../mrl-runtime/src/mrl_runtime/builtin/import_model.py")
            .is_file());
        assert_eq!(cache_suffix(&env), Some(digest()));
        assert_eq!(project(state.path(), &hash).unwrap(), Some(env));
    }
}
