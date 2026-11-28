//! Moving files in and out of an attempt: inputs into the read-only inputs folder (through the
//! node cache), staged outputs out with tickets (or into a folder for local runs), always
//! checked against their sha256.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mrl_storage::{ArtifactManifest, StorageError, TransferTicket};

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
    // A hard link shares bytes with the cache object, so it is safe only where the worker
    // can't undo the read-only bit: every worker in a container, with inputs mounted `:ro`.
    // A process-mode worker runs as the agent's user and could chmod and rewrite it.
    let shared_ok = spec
        .runtimes
        .iter()
        .all(|r| matches!(r.kind(), Ok(super::spec::RuntimeKind::Container { .. })));
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
        let object = match ctx.cache.get_pinned(&input.sha256, holder)? {
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
                    copy_stoppable(file, &tmp, false).await?;
                }
                ctx.cache.insert_pinned(&tmp, &input.sha256, holder).await?
            }
        };
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
            if !(shared_ok && hard_link(&object, &dest)?) {
                copy_stoppable(&object, &dest, true).await?;
            }
        }
        read_only(&dest)?;
        let shown = if input.is_folder() {
            inputs_dir.join(&input.name)
        } else {
            dest
        };
        staged.insert(input.name.clone(), shown.display().to_string());
    }
    Ok(staged)
}

/// Cache fills in progress, by sha256.
static FILLS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
> = std::sync::LazyLock::new(Default::default);

/// A flag that is set when this value is dropped. Blocking work started for an async task
/// checks it, so it stops soon after the task is cancelled (lease expiry, cancel) instead of
/// running on in the background.
pub struct StopOnDrop(Arc<std::sync::atomic::AtomicBool>);

impl StopOnDrop {
    /// A new, unset flag.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Arc::default())
    }

    /// The flag, for the blocking side.
    pub fn flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.0)
    }
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Copies `from` to `to` off the async runtime, 1 MiB at a time, stopping (and removing the
/// partial copy) as soon as the calling task is cancelled. `create_new`: fail if `to` exists.
async fn copy_stoppable(from: &Path, to: &Path, create_new: bool) -> Result<(), AttemptError> {
    let input = tokio::fs::File::open(from)
        .await
        .map_err(|e| AgentError::state(from, e))?;
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .create_new(create_new)
        .truncate(true)
        .open(to)
        .map_err(|e| AgentError::state(to, e))?;
    let cleanup = RemoveFile(to.to_path_buf());
    let mut output = tokio::fs::File::from_std(output);
    let mut input = tokio::io::BufReader::with_capacity(1 << 20, input);
    let copied = tokio::io::copy_buf(&mut input, &mut output).await;
    let result = match copied {
        Ok(_) => output.sync_all().await,
        Err(e) => Err(e),
    };
    drop(output);
    if result.is_ok() {
        cleanup.keep();
    }
    result.map_err(|e| AgentError::state(to, e).into())
}

struct RemoveFile(PathBuf);

impl RemoveFile {
    fn keep(mut self) {
        self.0.clear();
    }
}

impl Drop for RemoveFile {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

/// Unix: a hard link to the (read-only) cache object, for container workers only. False when
/// linking isn't possible here (another disk, or not Unix): the caller copies.
#[cfg(unix)]
fn hard_link(object: &Path, dest: &Path) -> Result<bool, AttemptError> {
    match std::fs::hard_link(object, dest) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(AgentError::state(dest, e).into())
        }
        Err(_) => Ok(false),
    }
}

/// Windows: never a link, so a worker can never change the shared cache object.
#[cfg(not(unix))]
fn hard_link(_object: &Path, _dest: &Path) -> Result<bool, AttemptError> {
    Ok(false)
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
    // Non-blocking: a named pipe placed by the worker opens at once (and is then refused as
    // not a regular file) instead of hanging this thread; regular files read normally.
    let fd = openat(
        &dir,
        last.as_os_str(),
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
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
    open_beneath_with(staging, rel, &mut || {})
}

/// [`open_beneath`], calling `between` once every folder is held and before the file opens
/// (tests race a replacement there).
#[cfg(windows)]
fn open_beneath_with(
    staging: &Path,
    rel: &Path,
    between: &mut dyn FnMut(),
) -> std::io::Result<std::fs::File> {
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
    between();
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
    let result = async {
        use tokio::io::AsyncReadExt;
        let src = open_beneath(staging, &rel_path)?;
        let meta = src.metadata()?;
        if !meta.is_file() || meta.len() != size {
            return Err(std::io::Error::other(
                "not a regular file of the authorized size",
            ));
        }
        // Create synchronously so cancellation cannot orphan a file before its guard exists.
        let dst = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&snap)?;
        let cleanup = RemoveFile(snap.clone());
        let mut dst = tokio::fs::File::from_std(dst);
        let src = tokio::io::BufReader::with_capacity(1 << 20, tokio::fs::File::from_std(src));
        tokio::io::copy_buf(&mut src.take(size.saturating_add(1)), &mut dst).await?;
        dst.sync_all().await?;
        Ok::<_, std::io::Error>(cleanup)
    }
    .await
    .map_err(|e| format!("{rel} can't be read safely ({e})"))?;
    match mrl_storage::sha256_file(&snap).await {
        Ok((n, actual)) if n == size && actual == sha256 => {
            result.keep();
            Ok(snap)
        }
        Ok(_) => Err(format!("{rel} does not match what the worker reported")),
        Err(e) => Err(format!("{rel} can't be read ({e})")),
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
    let src = RemoveFile(src);
    deliver(ctx, &src.0, rel, sha256, dest).await
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
            let file_name = src.file_name().unwrap_or_default().to_string_lossy();
            let temp = to.with_file_name(format!(".mrl-upload-{file_name}"));
            copy_stoppable(src, &temp, true).await?;
            let temp = RemoveFile(temp);
            let (_, actual) = mrl_storage::sha256_file(&temp.0).await?;
            if actual != sha256 {
                return Err(StorageError::ChecksumMismatch {
                    key: rel.to_owned(),
                    expected: sha256.to_owned(),
                    actual,
                }
                .into());
            }
            tokio::fs::rename(&temp.0, &to)
                .await
                .map_err(|e| AgentError::state(&to, e))?;
        }
    }
    Ok(())
}

/// Resolves an export destination through the node's allowlist before writing anything.
pub(super) fn local_export_path(
    cfg: &crate::config::AgentConfig,
    path: &Path,
) -> Result<PathBuf, AttemptError> {
    let denied = || {
        AttemptError::Spec("Choose a new export directory inside the node's allowed paths.".into())
    };
    if !path.is_absolute()
        || path
            .components()
            .any(|part| part == std::path::Component::ParentDir)
    {
        return Err(denied());
    }
    let parent = path.parent().ok_or_else(denied)?;
    let target = path.file_name().ok_or_else(denied)?;
    if !cfg.path_allowed(parent) {
        return Err(denied());
    }
    Ok(parent
        .canonicalize()
        .map_err(|e| AgentError::state(parent, e))?
        .join(target))
}

/// Publishes checked model files as one directory. Repeating a completed copy verifies it.
pub(super) async fn publish_local_export(
    staging: &Path,
    destination: &Path,
) -> Result<(), AttemptError> {
    use std::io::Read;
    let manifest_file = Path::new("outputs/artifact-manifest.json");
    let manifest =
        open_beneath(staging, manifest_file).map_err(|e| AgentError::state(manifest_file, e))?;
    const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
    let mut bytes = Vec::new();
    manifest
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| AgentError::state(manifest_file, e))?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(AttemptError::Spec("The export has too many files.".into()));
    }
    let files = ArtifactManifest::from_json(&bytes)?.files;
    let files: Vec<_> = files
        .iter()
        .filter_map(|f| f.path.strip_prefix("export/").map(|rel| (rel, f)))
        .collect();
    if files.is_empty() {
        return Err(AttemptError::Spec(
            "The worker produced no model files to export.".into(),
        ));
    }

    let parent = destination
        .parent()
        .ok_or_else(|| AttemptError::Spec("The export has no parent directory.".into()))?;
    let temporary = tempfile::Builder::new()
        .prefix(".mrl-export-")
        .tempdir_in(parent)
        .map_err(|e| AgentError::state(parent, e))?;
    for (rel, f) in &files {
        let copied = snapshot(staging, &format!("outputs/{}", f.path), f.size, &f.sha256)
            .await
            .map_err(|e| AttemptError::Spec(format!("The export could not be copied: {e}")))?;
        let copied = RemoveFile(copied);
        let target = temporary.path().join(safe_relative(rel)?);
        if let Some(dir) = target.parent() {
            tokio::fs::create_dir_all(dir)
                .await
                .map_err(|e| AgentError::state(dir, e))?;
        }
        tokio::fs::copy(&copied.0, &target)
            .await
            .map_err(|e| AgentError::state(&target, e))?;
        tokio::fs::OpenOptions::new()
            .write(true)
            .open(&target)
            .await
            .map_err(|e| AgentError::state(&target, e))?
            .sync_all()
            .await
            .map_err(|e| AgentError::state(&target, e))?;
    }

    match tokio::fs::symlink_metadata(destination).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            match tokio::fs::rename(temporary.path(), destination).await {
                Ok(()) => return Ok(()),
                Err(e) if !destination.is_dir() => {
                    return Err(AgentError::state(destination, e).into())
                }
                Err(_) => {}
            }
        }
        Ok(_) => {}
        Err(e) => return Err(AgentError::state(destination, e).into()),
    }
    for (rel, f) in files {
        let file = open_beneath(destination, &safe_relative(rel)?).map_err(|_| {
            AttemptError::Spec("The export directory already exists with other files.".into())
        })?;
        use futures::TryStreamExt;
        let stream = tokio_util::io::ReaderStream::new(tokio::fs::File::from_std(file));
        let (size, hash) = mrl_storage::sha256_stream(stream.map_err(|source| StorageError::Io {
            path: destination.join(rel),
            source,
        }))
        .await?;
        if size != f.size || hash != f.sha256 {
            return Err(AttemptError::Spec(
                "The export directory already exists with other files.".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_local_model_export_is_atomic_checked_and_never_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let allowed = dir.path().join("allowed");
        std::fs::create_dir(&allowed).unwrap();
        let cfg = crate::config::AgentConfig {
            allowed_paths: vec![allowed.clone()],
            ..Default::default()
        };
        assert!(local_export_path(&cfg, &dir.path().join("foreign")).is_err());
        let target = local_export_path(&cfg, &allowed.join("download")).unwrap();
        let staging = dir.path().join("staging");
        let outputs = staging.join("outputs");
        std::fs::create_dir_all(outputs.join("export")).unwrap();
        let weights = outputs.join("export/model.safetensors");
        std::fs::write(&weights, b"wrong!").unwrap();
        let manifest = serde_json::json!({
            "schema_version": "artifact-manifest/v1", "kind": "export",
            "created_at": "2026-11-27T00:00:00Z",
            "files": [{"path": "export/model.safetensors", "size": 6,
                       "sha256": mrl_storage::sha256_hex(b"actual"), "role": "weights"}],
        });
        std::fs::write(outputs.join("artifact-manifest.json"), manifest.to_string()).unwrap();
        assert!(publish_local_export(&staging, &target).await.is_err());
        assert!(!target.exists());
        std::fs::write(&weights, b"actual").unwrap();
        publish_local_export(&staging, &target).await.unwrap();
        publish_local_export(&staging, &target).await.unwrap();
        let existing = target.join("model.safetensors");
        assert_eq!(std::fs::read(&existing).unwrap(), b"actual");
        std::fs::write(&existing, b"other").unwrap();
        assert!(publish_local_export(&staging, &target).await.is_err());
        assert_eq!(std::fs::read(existing).unwrap(), b"other");
    }

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
        // A file bigger than authorized is refused without a snapshot left behind.
        std::fs::write(staging.join("ckpt/big.bin"), vec![0u8; 4096]).unwrap();
        assert!(snapshot(&staging, "ckpt/big.bin", 7, &sha).await.is_err());
        assert_eq!(
            std::fs::read_dir(dir.path().join("uploads"))
                .unwrap()
                .count(),
            1,
            "only the good snapshot"
        );
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
            assert!(snapshot(&staging, "ckpt/w.bin", 7, &sha).await.is_ok());
        }
    }

    /// A named pipe staged in place of a file is refused at once (opening it never waits for
    /// a writer).
    #[cfg(unix)]
    #[tokio::test]
    async fn a_named_pipe_is_refused_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging");
        std::fs::create_dir_all(&staging).unwrap();
        let made = std::process::Command::new("mkfifo")
            .arg(staging.join("pipe"))
            .status()
            .unwrap();
        assert!(made.success());
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            snapshot(&staging, "pipe", 7, &"0".repeat(64)),
        )
        .await
        .expect("returned without waiting for a writer");
        assert!(r.is_err());
    }

    /// A copy whose task is cancelled stops and removes its partial file.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_copy_stops_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let big = dir.path().join("big");
        // Sparse: costs no disk, takes a while to copy.
        std::fs::File::create(&big)
            .unwrap()
            .set_len(2 << 30)
            .unwrap();
        let dest = dir.path().join("copy");
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            copy_stoppable(&big, &dest, true),
        )
        .await;
        assert!(cancelled.is_err(), "still copying when cancelled");
        for _ in 0..100 {
            if !dest.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(!dest.exists(), "the partial copy was removed");
    }

    /// Windows: a replacement raced into the walk fails. While the folders are held, the
    /// folder can't be renamed away (to swap in one holding other bytes); once the file is
    /// open it can't be replaced; and the bytes read are the original ones.
    #[cfg(windows)]
    #[test]
    fn held_folders_and_file_cannot_be_replaced() {
        use std::io::Read;
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging");
        let ckpt = staging.join("ckpt");
        std::fs::create_dir_all(&ckpt).unwrap();
        std::fs::write(ckpt.join("w.bin"), b"weights").unwrap();
        let other = dir.path().join("other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("w.bin"), b"secret!").unwrap();
        let mut raced = false;
        let mut f = open_beneath_with(&staging, &Path::new("ckpt").join("w.bin"), &mut || {
            let moved = std::fs::rename(&ckpt, dir.path().join("moved"));
            assert!(moved.is_err(), "a held folder can't be renamed away");
            raced = true;
        })
        .unwrap();
        assert!(raced);
        assert!(
            std::fs::rename(other.join("w.bin"), ckpt.join("w.bin")).is_err(),
            "the open file can't be replaced"
        );
        let mut got = Vec::new();
        f.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"weights");
    }
}
