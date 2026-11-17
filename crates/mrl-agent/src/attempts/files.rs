//! Moving files in and out of an attempt: inputs into the read-only inputs folder (through the
//! node cache), staged outputs out with tickets (or into a folder for local runs), always
//! checked against their sha256.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mrl_storage::{StorageError, TransferTicket};

use super::spec::{safe_relative, AttemptSpec};
use super::AttemptError;
use crate::{AgentContext, AgentError};

/// Where one staged file goes.
#[derive(Debug, Clone, PartialEq)]
pub enum Destination {
    /// Upload with this ticket.
    Ticket(TransferTicket),
    /// Local runs: copy into this folder, at the file's staging path.
    Dir(PathBuf),
}

/// Downloads (or copies) every input into `inputs_dir` through the node cache, pinned for
/// `holder`, and makes the files read-only. Returns `name -> absolute path` for `Prepare`.
pub async fn stage_inputs(
    ctx: &AgentContext,
    spec: &AttemptSpec,
    inputs_dir: &Path,
    holder: &str,
) -> Result<BTreeMap<String, String>, AttemptError> {
    std::fs::create_dir_all(inputs_dir).map_err(|e| AgentError::state(inputs_dir, e))?;
    let mut staged = BTreeMap::new();
    for input in &spec.inputs {
        let dest = inputs_dir.join(safe_relative(&input.local_path)?);
        let object = match ctx.cache.get(&input.sha256)? {
            Some(p) => p,
            None => {
                ctx.cache.reserve(input.size)?;
                // Named by hash, so an interrupted download resumes from `<tmp>.part`.
                let tmp = ctx
                    .cache
                    .incoming_dir()
                    .join(format!("{}.in", input.sha256));
                if let Some(ticket) = input.ticket() {
                    ctx.transfer
                        .download(&ticket, &tmp, Some(&input.sha256))
                        .await?;
                } else if let Some(file) = &input.file {
                    tokio::fs::copy(file, &tmp)
                        .await
                        .map_err(|e| AgentError::state(file, e))?;
                }
                ctx.cache.insert_file(&tmp, &input.sha256).await?
            }
        };
        ctx.cache.pin(&input.sha256, holder)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| AgentError::state(parent, e))?;
        }
        if !dest.exists() && std::fs::hard_link(&object, &dest).is_err() {
            std::fs::copy(&object, &dest).map_err(|e| AgentError::state(&dest, e))?;
        }
        read_only(&dest)?;
        staged.insert(input.name.clone(), dest.display().to_string());
    }
    Ok(staged)
}

#[cfg(unix)]
fn read_only(path: &Path) -> Result<(), AttemptError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))
        .map_err(|e| AgentError::state(path, e).into())
}

#[cfg(not(unix))]
fn read_only(path: &Path) -> Result<(), AttemptError> {
    // Windows can't delete read-only files without clearing the flag; the cache must stay
    // evictable, so inputs are left writable there.
    let _ = path;
    Ok(())
}

/// Checks a staged file against its expected size and sha256. `Err` holds the plain reason.
pub async fn verify(staging: &Path, rel: &str, size: u64, sha256: &str) -> Result<PathBuf, String> {
    let rel_path = safe_relative(rel).map_err(|e| e.to_string())?;
    let path = staging.join(rel_path);
    let (actual_size, actual) = mrl_storage::sha256_file(&path)
        .await
        .map_err(|e| format!("{rel} can't be read ({e})"))?;
    if actual_size != size || actual != sha256 {
        return Err(format!("{rel} does not match what the worker reported"));
    }
    Ok(path)
}

/// Sends one checked file to its destination.
pub async fn send(
    ctx: &AgentContext,
    staging: &Path,
    rel: &str,
    size: u64,
    sha256: &str,
    dest: &Destination,
) -> Result<(), AttemptError> {
    let src = verify(staging, rel, size, sha256).await.map_err(|actual| {
        AttemptError::from(StorageError::ChecksumMismatch {
            key: rel.to_owned(),
            expected: sha256.to_owned(),
            actual,
        })
    })?;
    match dest {
        Destination::Ticket(t) => {
            ctx.transfer.upload(t, &src).await?;
        }
        Destination::Dir(dir) => {
            let to = dir.join(safe_relative(rel)?);
            if let Some(p) = to.parent() {
                std::fs::create_dir_all(p).map_err(|e| AgentError::state(p, e))?;
            }
            tokio::fs::copy(&src, &to)
                .await
                .map_err(|e| AgentError::state(&to, e))?;
            let (_, copied) = mrl_storage::sha256_file(&to).await?;
            if copied != sha256 {
                return Err(StorageError::ChecksumMismatch {
                    key: to.display().to_string(),
                    expected: sha256.to_owned(),
                    actual: copied,
                }
                .into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn verify_and_copy() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging");
        std::fs::create_dir_all(staging.join("ckpt")).unwrap();
        std::fs::write(staging.join("ckpt/w.bin"), b"weights").unwrap();
        let sha = mrl_storage::sha256_hex(b"weights");
        assert!(verify(&staging, "ckpt/w.bin", 7, &sha).await.is_ok());
        assert!(verify(&staging, "ckpt/w.bin", 7, &"0".repeat(64))
            .await
            .is_err());
        assert!(verify(&staging, "../etc/passwd", 7, &sha).await.is_err());
    }
}
