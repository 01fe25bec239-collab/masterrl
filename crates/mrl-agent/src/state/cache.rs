//! The node cache: a content-addressed store (by sha256) under the state folder.
//!
//! Entries are pinned while a live attempt or deployment uses them (one pin per holder id).
//! Before each new entry, least-recently-used unpinned entries are evicted until the cache fits
//! its budget and the disk keeps `min_free_bytes` free. A new entry is refused with
//! [`AgentError::CacheFull`] only if pinned entries alone leave no room (Doc 20 item 16).
//!
//! Layout: `objects/<first 2 hex>/<sha256>`, `incoming/` for downloads in progress, and
//! `index.json` (sizes, LRU clock, pins), written atomically after every change.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

use crate::state::atomic_write;
use crate::{AgentError, Result};

/// How much the cache may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CacheBudget {
    /// Largest total size of entries.
    pub max_bytes: u64,
    /// Free space always left on the cache disk.
    pub min_free_bytes: u64,
}

/// What the eviction planner needs to know about one entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryView {
    /// The entry's sha256.
    pub sha256: String,
    /// Size in bytes.
    pub size: u64,
    /// LRU clock value of the last use (higher is newer).
    pub last_used: u64,
    /// True while any holder pins it.
    pub pinned: bool,
}

/// Picks which entries to evict so `incoming` more bytes fit: oldest unpinned first.
/// `disk_free` is the free space on the cache disk now.
pub fn plan_eviction(
    entries: &[EntryView],
    incoming: u64,
    budget: CacheBudget,
    disk_free: u64,
) -> Result<Vec<String>> {
    let used: u64 = entries.iter().map(|e| e.size).sum();
    let pinned: u64 = entries.iter().filter(|e| e.pinned).map(|e| e.size).sum();
    let over_budget = used
        .saturating_add(incoming)
        .saturating_sub(budget.max_bytes);
    let short_on_disk = budget
        .min_free_bytes
        .saturating_add(incoming)
        .saturating_sub(disk_free);
    let need = over_budget.max(short_on_disk);
    let full = || AgentError::CacheFull {
        needed: incoming,
        pinned,
        budget: budget.max_bytes,
        min_free: budget.min_free_bytes,
    };
    if pinned.saturating_add(incoming) > budget.max_bytes {
        return Err(full());
    }
    let mut candidates: Vec<&EntryView> = entries.iter().filter(|e| !e.pinned).collect();
    candidates.sort_by(|a, b| (a.last_used, &a.sha256).cmp(&(b.last_used, &b.sha256)));
    let mut freed = 0u64;
    let mut out = Vec::new();
    for e in candidates {
        if freed >= need {
            break;
        }
        freed += e.size;
        out.push(e.sha256.clone());
    }
    if freed < need {
        return Err(full());
    }
    Ok(out)
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Index {
    clock: u64,
    entries: BTreeMap<String, Entry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    size: u64,
    last_used: u64,
    pins: BTreeSet<String>,
}

/// Totals for status reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheUsage {
    /// Number of entries.
    pub entries: usize,
    /// Bytes used by all entries.
    pub used_bytes: u64,
    /// Bytes used by pinned entries.
    pub pinned_bytes: u64,
    /// The budget in force.
    pub budget: CacheBudget,
}

/// The content-addressed node cache.
#[derive(Debug)]
pub struct NodeCache {
    root: PathBuf,
    budget: CacheBudget,
    index: Mutex<Index>,
}

fn check_sha(sha: &str) -> Result<()> {
    if sha.len() == 64 && sha.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        Ok(())
    } else {
        Err(AgentError::UnknownName {
            kind: "cache key",
            name: sha.to_owned(),
            known: "64 lowercase hex characters (a sha256)".into(),
        })
    }
}

impl NodeCache {
    /// Opens the cache in `root`. Drops index entries whose files are gone and files the
    /// index does not list (left by a crash), and clears `incoming/` leftovers older than the
    /// process (all of them, since only this process writes there).
    pub fn open(root: PathBuf, budget: CacheBudget) -> Result<Self> {
        for sub in ["objects", "incoming"] {
            let d = root.join(sub);
            std::fs::create_dir_all(&d).map_err(|e| AgentError::state(&d, e))?;
        }
        let index_path = root.join("index.json");
        let mut index: Index = match std::fs::read(&index_path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Index::default(),
            Err(e) => return Err(AgentError::state(&index_path, e)),
        };
        let cache = Self {
            root,
            budget,
            index: Mutex::new(Index::default()),
        };
        index
            .entries
            .retain(|sha, _| check_sha(sha).is_ok() && cache.object_path(sha).is_file());
        cache.remove_unlisted(&index)?;
        *cache.lock() = index;
        cache.save(&cache.lock())?;
        Ok(cache)
    }

    fn remove_unlisted(&self, index: &Index) -> Result<()> {
        let objects = self.root.join("objects");
        let read = |d: &Path| std::fs::read_dir(d).map_err(|e| AgentError::state(d, e));
        for shard in read(&objects)?.flatten() {
            for file in read(&shard.path())?.flatten() {
                let name = file.file_name().to_string_lossy().into_owned();
                if !index.entries.contains_key(&name) {
                    let p = file.path();
                    std::fs::remove_file(&p).map_err(|e| AgentError::state(&p, e))?;
                }
            }
        }
        let incoming = self.incoming_dir();
        for file in read(&incoming)?.flatten() {
            let p = file.path();
            std::fs::remove_file(&p).map_err(|e| AgentError::state(&p, e))?;
        }
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, Index> {
        // A panic while holding the lock leaves the index as consistent as its last save.
        self.index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn save(&self, index: &Index) -> Result<()> {
        let path = self.root.join("index.json");
        let bytes = serde_json::to_vec(index).map_err(|e| {
            AgentError::state(
                &path,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;
        atomic_write(&path, &bytes).map_err(|e| AgentError::state(path, e))
    }

    /// Where an entry's file lives (it may not exist).
    pub fn object_path(&self, sha256: &str) -> PathBuf {
        let shard = sha256.get(..2).unwrap_or("__");
        self.root.join("objects").join(shard).join(sha256)
    }

    /// A folder on the cache disk for downloads in progress; files there are not entries.
    pub fn incoming_dir(&self) -> PathBuf {
        self.root.join("incoming")
    }

    /// The budget in force.
    pub fn budget(&self) -> CacheBudget {
        self.budget
    }

    /// The entry's path if it is cached; marks it as just used.
    pub fn get(&self, sha256: &str) -> Result<Option<PathBuf>> {
        check_sha(sha256)?;
        let mut index = self.lock();
        index.clock += 1;
        let clock = index.clock;
        let Some(entry) = index.entries.get_mut(sha256) else {
            return Ok(None);
        };
        entry.last_used = clock;
        self.save(&index)?;
        Ok(Some(self.object_path(sha256)))
    }

    /// Evicts unpinned entries until `incoming` more bytes fit. Call before a download into
    /// [`Self::incoming_dir`]. Returns the evicted sha256s.
    pub fn reserve(&self, incoming: u64) -> Result<Vec<String>> {
        let mut index = self.lock();
        self.make_room(&mut index, incoming)
    }

    fn make_room(&self, index: &mut Index, incoming: u64) -> Result<Vec<String>> {
        let views: Vec<EntryView> = index
            .entries
            .iter()
            .map(|(sha, e)| EntryView {
                sha256: sha.clone(),
                size: e.size,
                last_used: e.last_used,
                pinned: !e.pins.is_empty(),
            })
            .collect();
        let free = crate::hw::cpu::volume_of(&self.root).map_or(u64::MAX, |(_, free)| free);
        let evict = plan_eviction(&views, incoming, self.budget, free)?;
        for sha in &evict {
            let p = self.object_path(sha);
            match std::fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(AgentError::state(p, e)),
            }
            index.entries.remove(sha);
        }
        if !evict.is_empty() {
            self.save(index)?;
        }
        Ok(evict)
    }

    /// Moves the file at `src` into the cache as entry `sha256`, after checking its content
    /// matches. Evicts first if needed. If the entry already exists, `src` is removed.
    /// Returns the entry's path.
    pub async fn insert_file(&self, src: &Path, sha256: &str) -> Result<PathBuf> {
        check_sha(sha256)?;
        let (size, actual) = mrl_storage::sha256_file(src).await?;
        if actual != sha256 {
            return Err(mrl_storage::StorageError::ChecksumMismatch {
                key: src.display().to_string(),
                expected: sha256.to_owned(),
                actual,
            }
            .into());
        }
        let dest = self.object_path(sha256);
        let mut index = self.lock();
        index.clock += 1;
        let clock = index.clock;
        if let Some(entry) = index.entries.get_mut(sha256) {
            entry.last_used = clock;
            self.save(&index)?;
            std::fs::remove_file(src).map_err(|e| AgentError::state(src, e))?;
            return Ok(dest);
        }
        self.make_room(&mut index, size)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| AgentError::state(parent, e))?;
        }
        if std::fs::rename(src, &dest).is_err() {
            // Different disk: copy, then remove the source.
            std::fs::copy(src, &dest).map_err(|e| AgentError::state(&dest, e))?;
            std::fs::remove_file(src).map_err(|e| AgentError::state(src, e))?;
        }
        index.entries.insert(
            sha256.to_owned(),
            Entry {
                size,
                last_used: clock,
                pins: BTreeSet::new(),
            },
        );
        self.save(&index)?;
        Ok(dest)
    }

    /// Pins an entry for `holder` (an attempt or deployment id). Pinned entries are never
    /// evicted. Pinning twice for the same holder is a no-op.
    pub fn pin(&self, sha256: &str, holder: &str) -> Result<()> {
        check_sha(sha256)?;
        let mut index = self.lock();
        let entry = index
            .entries
            .get_mut(sha256)
            .ok_or_else(|| AgentError::UnknownName {
                kind: "cache entry",
                name: sha256.to_owned(),
                known: "entries already in the node cache".into(),
            })?;
        entry.pins.insert(holder.to_owned());
        self.save(&index)
    }

    /// Removes `holder`'s pin. Returns false if there was none.
    pub fn unpin(&self, sha256: &str, holder: &str) -> Result<bool> {
        let mut index = self.lock();
        let removed = index
            .entries
            .get_mut(sha256)
            .is_some_and(|e| e.pins.remove(holder));
        if removed {
            self.save(&index)?;
        }
        Ok(removed)
    }

    /// Removes every pin `holder` has (for example when its attempt ends).
    pub fn unpin_all(&self, holder: &str) -> Result<usize> {
        let mut index = self.lock();
        let n = index
            .entries
            .values_mut()
            .map(|e| e.pins.remove(holder))
            .filter(|removed| *removed)
            .count();
        if n > 0 {
            self.save(&index)?;
        }
        Ok(n)
    }

    /// Current totals.
    pub fn usage(&self) -> CacheUsage {
        let index = self.lock();
        CacheUsage {
            entries: index.entries.len(),
            used_bytes: index.entries.values().map(|e| e.size).sum(),
            pinned_bytes: index
                .entries
                .values()
                .filter(|e| !e.pins.is_empty())
                .map(|e| e.size)
                .sum(),
            budget: self.budget,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn sha(data: &[u8]) -> String {
        mrl_storage::sha256_hex(data)
    }

    async fn put(cache: &NodeCache, dir: &Path, data: &[u8]) -> String {
        let s = sha(data);
        let src = dir.join(&s);
        std::fs::write(&src, data).unwrap();
        cache.insert_file(&src, &s).await.unwrap();
        s
    }

    fn budget(max: u64) -> CacheBudget {
        CacheBudget {
            max_bytes: max,
            min_free_bytes: 0,
        }
    }

    #[tokio::test]
    async fn lru_unpinned_entries_are_evicted_first() {
        let dir = tempfile::tempdir().unwrap();
        let cache = NodeCache::open(dir.path().join("c"), budget(30)).unwrap();
        let a = put(&cache, dir.path(), &[1; 10]).await;
        let b = put(&cache, dir.path(), &[2; 10]).await;
        let c = put(&cache, dir.path(), &[3; 10]).await;
        cache.get(&a).unwrap(); // a is now newest; b is oldest
        cache.pin(&b, "attempt-1").unwrap();
        let d = put(&cache, dir.path(), &[4; 10]).await;
        // b is pinned, so c (the oldest unpinned) goes.
        assert!(cache.get(&c).unwrap().is_none());
        assert!(cache.get(&b).unwrap().is_some());
        assert!(cache.get(&a).unwrap().is_some() && cache.get(&d).unwrap().is_some());
        assert_eq!(cache.usage().used_bytes, 30);
        assert_eq!(cache.usage().pinned_bytes, 10);

        // Pins alone leave no room for 25 bytes.
        cache.pin(&a, "attempt-1").unwrap();
        cache.pin(&d, "attempt-2").unwrap();
        let e = cache.reserve(25).unwrap_err();
        assert_eq!(e.code(), "MRL-ENV-004");
        assert_eq!(cache.unpin_all("attempt-1").unwrap(), 2);
        assert!(!cache.unpin(&a, "attempt-1").unwrap());
        assert_eq!(cache.reserve(15).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn content_is_checked_and_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("c");
        let cache = NodeCache::open(root.clone(), budget(100)).unwrap();
        let src = dir.path().join("f");
        std::fs::write(&src, b"hello").unwrap();
        let wrong = sha(b"other");
        let e = cache.insert_file(&src, &wrong).await.unwrap_err();
        assert_eq!(e.code(), "MRL-STORE-010");
        assert_eq!(cache.get("not-a-sha").unwrap_err().code(), "MRL-VAL-126");
        let s = put(&cache, dir.path(), b"hello").await;
        cache.pin(&s, "dep-1").unwrap();
        // A crash leftover: an unlisted object and an incoming file.
        let stray = cache.object_path(&sha(b"stray"));
        std::fs::create_dir_all(stray.parent().unwrap()).unwrap();
        std::fs::write(&stray, b"stray").unwrap();
        std::fs::write(cache.incoming_dir().join("x.part"), b"x").unwrap();
        drop(cache);

        let cache = NodeCache::open(root, budget(100)).unwrap();
        assert_eq!(cache.usage().pinned_bytes, 5);
        assert!(!stray.exists());
        assert_eq!(std::fs::read_dir(cache.incoming_dir()).unwrap().count(), 0);
        let p = cache.get(&s).unwrap().unwrap();
        assert_eq!(std::fs::read(p).unwrap(), b"hello");
        assert_eq!(
            cache.pin(&sha(b"nope"), "x").unwrap_err().code(),
            "MRL-VAL-126"
        );
    }

    fn entries() -> impl Strategy<Value = Vec<EntryView>> {
        prop::collection::vec((1u64..1000, 0u64..50, any::<bool>()), 0..20).prop_map(|v| {
            v.into_iter()
                .enumerate()
                .map(|(i, (size, last_used, pinned))| EntryView {
                    sha256: format!("{i:064}"),
                    size,
                    last_used,
                    pinned,
                })
                .collect()
        })
    }

    proptest! {
        #[test]
        fn eviction_fits_the_budget_and_never_touches_pins(
            entries in entries(),
            incoming in 0u64..3000,
            max in 0u64..20_000,
            min_free in 0u64..5000,
            disk_free in 0u64..10_000,
        ) {
            let b = CacheBudget { max_bytes: max, min_free_bytes: min_free };
            let used: u64 = entries.iter().map(|e| e.size).sum();
            let pinned: u64 = entries.iter().filter(|e| e.pinned).map(|e| e.size).sum();
            match plan_eviction(&entries, incoming, b, disk_free) {
                Ok(evicted) => {
                    let gone: u64 = entries.iter()
                        .filter(|e| evicted.contains(&e.sha256)).map(|e| e.size).sum();
                    prop_assert!(used - gone + incoming <= max);
                    prop_assert!(disk_free + gone >= min_free + incoming);
                    for e in &entries {
                        if evicted.contains(&e.sha256) {
                            prop_assert!(!e.pinned);
                            // Everything kept and unpinned is at least as new as what went.
                            for k in entries.iter().filter(|k| !k.pinned && !evicted.contains(&k.sha256)) {
                                prop_assert!((k.last_used, &k.sha256) > (e.last_used, &e.sha256));
                            }
                        }
                    }
                }
                Err(e) => {
                    prop_assert_eq!(e.code(), "MRL-ENV-004");
                    let unpinned = used - pinned;
                    prop_assert!(
                        pinned + incoming > max || disk_free + unpinned < min_free + incoming
                    );
                }
            }
        }
    }
}
