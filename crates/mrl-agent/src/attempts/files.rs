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
        if !dest.exists() {
            link_or_copy(&object, &dest)?;
        }
        read_only(&dest)?;
        staged.insert(input.name.clone(), dest.display().to_string());
    }
    Ok(staged)
}

/// Unix: a hard link to the (read-only) cache object.
#[cfg(unix)]
fn link_or_copy(object: &Path, dest: &Path) -> Result<(), AttemptError> {
    if std::fs::hard_link(object, dest).is_err() {
        std::fs::copy(object, dest).map_err(|e| AgentError::state(dest, e))?;
    }
    Ok(())
}

/// Windows: always a private copy, so a worker can never change the shared cache object
/// through a writable link.
#[cfg(not(unix))]
fn link_or_copy(object: &Path, dest: &Path) -> Result<(), AttemptError> {
    std::fs::copy(object, dest).map_err(|e| AgentError::state(dest, e))?;
    Ok(())
}

#[cfg(unix)]
fn read_only(path: &Path) -> Result<(), AttemptError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))
        .map_err(|e| AgentError::state(path, e).into())
}

#[cfg(not(unix))]
fn read_only(path: &Path) -> Result<(), AttemptError> {
    let mut perms = std::fs::metadata(path)
        .map_err(|e| AgentError::state(path, e))?
        .permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(path, perms).map_err(|e| AgentError::state(path, e).into())
}

/// Opens `rel` beneath `staging` without following any symlink (Unix: one component at a
/// time with `O_NOFOLLOW`), so a worker can't point the agent at a file outside staging.
#[cfg(unix)]
fn open_beneath(staging: &Path, rel: &Path) -> std::io::Result<std::fs::File> {
    use rustix::fs::{openat, Mode, OFlags, CWD};
    let dir_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut dir = openat(CWD, staging, dir_flags, Mode::empty())?;
    let parts: Vec<_> = rel.components().collect();
    let Some((last, dirs)) = parts.split_last() else {
        return Err(std::io::Error::other("empty path"));
    };
    for c in dirs {
        dir = openat(&dir, c.as_os_str(), dir_flags, Mode::empty())?;
    }
    let fd = openat(
        &dir,
        last.as_os_str(),
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok(std::fs::File::from(fd))
}

#[cfg(not(unix))]
fn open_beneath(staging: &Path, rel: &Path) -> std::io::Result<std::fs::File> {
    let mut p = staging.to_path_buf();
    for c in rel.components() {
        p.push(c);
        if std::fs::symlink_metadata(&p)?.file_type().is_symlink() {
            return Err(std::io::Error::other("the path contains a link"));
        }
    }
    std::fs::File::open(p)
}

/// Copies a staged file into an agent-owned snapshot (`<attempt>/uploads/`, outside staging)
/// and checks the snapshot's size and sha256. The snapshot is what gets uploaded, so the bytes
/// sent are exactly the bytes checked. `Err` holds the plain reason.
pub async fn snapshot(
    staging: &Path,
    rel: &str,
    size: u64,
    sha256: &str,
) -> Result<PathBuf, String> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let rel_path = safe_relative(rel).map_err(|e| e.to_string())?;
    let dir = staging.parent().unwrap_or(staging).join("uploads");
    super::record::private_dir(&dir)
        .map_err(|e| format!("{} can't be created ({e})", dir.display()))?;
    let snap = dir.join(format!(
        "{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let (staging_c, snap_c) = (staging.to_path_buf(), snap.clone());
    let copied = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let mut src = open_beneath(&staging_c, &rel_path)?;
        if !src.metadata()?.is_file() {
            return Err(std::io::Error::other("not a regular file"));
        }
        let mut dst = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&snap_c)?;
        std::io::copy(&mut src, &mut dst)?;
        dst.sync_all()
    })
    .await;
    let fail = |why: String| {
        let _ = std::fs::remove_file(&snap);
        Err(why)
    };
    match copied {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return fail(format!("{rel} can't be read safely ({e})")),
        Err(e) => return fail(format!("{rel} can't be read ({e})")),
    }
    match mrl_storage::sha256_file(&snap).await {
        Ok((n, actual)) if n == size && actual == sha256 => Ok(snap),
        Ok(_) => fail(format!("{rel} does not match what the worker reported")),
        Err(e) => fail(format!("{rel} can't be read ({e})")),
    }
}

/// Sends one staged file to its destination: snapshot, check, then upload the snapshot.
pub async fn send(
    ctx: &AgentContext,
    staging: &Path,
    rel: &str,
    size: u64,
    sha256: &str,
    dest: &Destination,
) -> Result<(), AttemptError> {
    let src = snapshot(staging, rel, size, sha256)
        .await
        .map_err(|actual| {
            AttemptError::from(StorageError::ChecksumMismatch {
                key: rel.to_owned(),
                expected: sha256.to_owned(),
                actual,
            })
        })?;
    let sent = deliver(ctx, &src, rel, sha256, dest).await;
    let _ = std::fs::remove_file(&src);
    sent
}

async fn deliver(
    ctx: &AgentContext,
    src: &Path,
    rel: &str,
    sha256: &str,
    dest: &Destination,
) -> Result<(), AttemptError> {
    match dest {
        Destination::Ticket(t) => {
            ctx.transfer.upload(t, src).await?;
        }
        Destination::Dir(dir) => {
            let to = dir.join(safe_relative(rel)?);
            if let Some(p) = to.parent() {
                std::fs::create_dir_all(p).map_err(|e| AgentError::state(p, e))?;
            }
            tokio::fs::copy(src, &to)
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
    async fn snapshots_are_checked_and_never_follow_links() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging");
        std::fs::create_dir_all(staging.join("ckpt")).unwrap();
        std::fs::write(staging.join("ckpt/w.bin"), b"weights").unwrap();
        let sha = mrl_storage::sha256_hex(b"weights");
        let snap = snapshot(&staging, "ckpt/w.bin", 7, &sha).await.unwrap();
        assert!(
            snap.starts_with(dir.path().join("uploads")),
            "outside staging"
        );
        assert_eq!(std::fs::read(&snap).unwrap(), b"weights");
        assert!(snapshot(&staging, "ckpt/w.bin", 7, &"0".repeat(64))
            .await
            .is_err());
        assert!(snapshot(&staging, "../etc/passwd", 7, &sha).await.is_err());
        #[cfg(unix)]
        {
            // A link to a file outside staging, as the last part or in the middle.
            let secret = dir.path().join("secret");
            std::fs::write(&secret, b"weights").unwrap();
            std::os::unix::fs::symlink(&secret, staging.join("ckpt/link.bin")).unwrap();
            assert!(snapshot(&staging, "ckpt/link.bin", 7, &sha).await.is_err());
            std::fs::create_dir_all(dir.path().join("out")).unwrap();
            std::fs::write(dir.path().join("out/w.bin"), b"weights").unwrap();
            std::os::unix::fs::symlink(dir.path().join("out"), staging.join("dirlink")).unwrap();
            assert!(snapshot(&staging, "dirlink/w.bin", 7, &sha).await.is_err());
        }
    }
}
