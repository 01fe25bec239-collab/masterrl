//! Moving files in and out of an attempt: inputs into the read-only inputs folder (through the
//! node cache), staged outputs out with tickets (or into a folder for local runs), always
//! checked against their sha256.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
        // One fill per hash at a time (across attempts); the cache is checked again once this
        // attempt owns the fill, and the object is pinned before the fill is released.
        let fill = {
            let mut fills = FILLS.lock().unwrap_or_else(|p| p.into_inner());
            Arc::clone(fills.entry(input.sha256.clone()).or_default())
        };
        let _filling = fill.lock().await;
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
        drop(_filling);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| AgentError::state(parent, e))?;
        }
        if std::fs::symlink_metadata(&dest).is_ok() {
            // Already there: only a retry of this same input may reuse it (for example a
            // differently cased name on a case-insensitive disk is another input's file).
            let same = mrl_storage::sha256_file(&dest)
                .await
                .is_ok_and(|(n, sha)| n == input.size && sha == input.sha256);
            if !same {
                return Err(AttemptError::Spec(format!(
                    "input {} would overwrite another input at {}",
                    input.name,
                    dest.display()
                )));
            }
        } else {
            link_or_copy(&object, &dest)?;
        }
        read_only(&dest)?;
        staged.insert(input.name.clone(), dest.display().to_string());
    }
    Ok(staged)
}

/// Cache fills in progress, by sha256.
static FILLS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
> = std::sync::LazyLock::new(Default::default);

/// Copies `from` to a new file `to` (fails if `to` exists).
fn copy_new(from: &Path, to: &Path) -> Result<(), AttemptError> {
    let mut src = std::fs::File::open(from).map_err(|e| AgentError::state(from, e))?;
    let mut dst = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(to)
        .map_err(|e| AgentError::state(to, e))?;
    std::io::copy(&mut src, &mut dst).map_err(|e| AgentError::state(to, e))?;
    Ok(())
}

/// Unix: a hard link to the (read-only) cache object.
#[cfg(unix)]
fn link_or_copy(object: &Path, dest: &Path) -> Result<(), AttemptError> {
    match std::fs::hard_link(object, dest) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(AgentError::state(dest, e).into())
        }
        Err(_) => copy_new(object, dest),
    }
}

/// Windows: always a private copy, so a worker can never change the shared cache object
/// through a writable link.
#[cfg(not(unix))]
fn link_or_copy(object: &Path, dest: &Path) -> Result<(), AttemptError> {
    copy_new(object, dest)
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

/// Windows: walks the path holding a handle on every folder, opened without following
/// reparse points (junctions, links) and shared for reading only, so nothing on the path can be
/// renamed, replaced or deleted until the file itself is open; every handle is checked to be a
/// plain folder (then file), not a reparse point. The copy then reads from that file handle.
#[cfg(windows)]
fn open_beneath(staging: &Path, rel: &Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    const FILE_SHARE_READ: u32 = 0x1;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    let open = |p: &Path, dir: bool| -> std::io::Result<std::fs::File> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(
                FILE_FLAG_OPEN_REPARSE_POINT | if dir { FILE_FLAG_BACKUP_SEMANTICS } else { 0 },
            )
            .open(p)?;
        let m = f.metadata()?;
        if m.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 || m.is_dir() != dir {
            return Err(std::io::Error::other(
                "the path contains a link or junction",
            ));
        }
        Ok(f)
    };
    let parts: Vec<_> = rel.components().collect();
    let Some((last, dirs)) = parts.split_last() else {
        return Err(std::io::Error::other("empty path"));
    };
    let mut p = staging.to_path_buf();
    let mut held = vec![open(&p, true)?];
    for c in dirs {
        p.push(c);
        held.push(open(&p, true)?);
    }
    p.push(last);
    let file = open(&p, false)?;
    drop(held);
    Ok(file)
}

#[cfg(not(any(unix, windows)))]
fn open_beneath(staging: &Path, rel: &Path) -> std::io::Result<std::fs::File> {
    let _ = (staging, rel);
    Err(std::io::Error::other(
        "uploads are not supported on this system",
    ))
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
        #[cfg(windows)]
        {
            // A junction to a folder outside staging.
            std::fs::create_dir_all(dir.path().join("out")).unwrap();
            std::fs::write(dir.path().join("out").join("w.bin"), b"weights").unwrap();
            let made = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(staging.join("junction"))
                .arg(dir.path().join("out"))
                .status()
                .unwrap();
            assert!(made.success());
            assert!(snapshot(&staging, "junction/w.bin", 7, &sha).await.is_err());
            // While a snapshot reads, the folders on the path can't be replaced.
            assert!(snapshot(&staging, "ckpt/w.bin", 7, &sha).await.is_ok());
        }
    }
}
