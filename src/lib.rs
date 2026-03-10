//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

//! RoughDB — an embedded key-value store written in Rust, porting
//! [LevelDB](https://github.com/google/leveldb) to Rust.
//!
//! RoughDB is an LSM-tree key-value store with a LevelDB-compatible on-disk format.
//! It supports persistent (WAL + MANIFEST + SSTables) and in-memory operation,
//! multi-level compaction, snapshots, and bidirectional iteration.
//!
//! # Getting started
//!
//! ## Opening a database
//!
//! ```no_run
//! use roughdb::{Db, Options};
//!
//! let mut opts = Options::default();
//! opts.create_if_missing = true;
//!
//! let db = Db::open("/tmp/my_db", opts)?;
//! # Ok::<(), roughdb::Error>(())
//! ```
//!
//! Use [`Db::default`] for a lightweight in-memory database (no WAL, no flush):
//!
//! ```
//! let db = roughdb::Db::default();
//! ```
//!
//! ## Reads and writes
//!
//! ```
//! # let db = roughdb::Db::default();
//! db.put(b"hello", b"world")?;
//!
//! match db.get(b"hello") {
//!     Ok(value) => println!("got: {}", String::from_utf8_lossy(&value)),
//!     Err(e) if e.is_not_found() => println!("not found"),
//!     Err(e) => return Err(e),
//! }
//!
//! db.delete(b"hello")?;
//! # Ok::<(), roughdb::Error>(())
//! ```
//!
//! ## Atomic batch writes
//!
//! ```
//! # let db = roughdb::Db::default();
//! use roughdb::{WriteBatch, WriteOptions};
//!
//! let mut batch = WriteBatch::new();
//! batch.put(b"key1", b"val1");
//! batch.put(b"key2", b"val2");
//! batch.delete(b"old_key");
//!
//! db.write(&WriteOptions::default(), &batch)?;
//! # Ok::<(), roughdb::Error>(())
//! ```
//!
//! ## Iterating over keys
//!
//! Iterators start unpositioned — call [`DbIter::seek_to_first`], [`DbIter::seek_to_last`],
//! or [`DbIter::seek`] before reading.  Both forward and backward traversal are supported;
//! direction switches are handled transparently.
//!
//! ```
//! # let db = roughdb::Db::default();
//! # db.put(b"a", b"1").unwrap();
//! use roughdb::ReadOptions;
//!
//! let mut it = db.new_iterator(&ReadOptions::default())?;
//!
//! it.seek_to_first();
//! while it.valid() {
//!     println!("{:?} = {:?}", it.key(), it.value());
//!     it.next();
//! }
//! # Ok::<(), roughdb::Error>(())
//! ```
//!
//! ## Snapshots
//!
//! A snapshot pins the database to a specific point in time; reads through it
//! see only writes that preceded the snapshot.
//!
//! ```
//! # let db = roughdb::Db::default();
//! use roughdb::ReadOptions;
//!
//! db.put(b"k", b"v1")?;
//! let snap = db.get_snapshot();
//! db.put(b"k", b"v2")?;
//!
//! let opts = ReadOptions { snapshot: Some(&snap), ..ReadOptions::default() };
//! assert_eq!(db.get_with_options(&opts, b"k")?, b"v1");
//!
//! // snap releases automatically here when it goes out of scope.
//! # Ok::<(), roughdb::Error>(())
//! ```

use crate::db::version_edit::{FileMetaData, VersionEdit};
use crate::db::version_set::VersionSet;
use crate::log::reader::Reader as LogReader;
use crate::log::writer::Writer as LogWriter;
use crate::memtable::{ArcMemTableIter, Memtable, MemtableResult};
use crate::table::builder::TableBuilder;
use crate::table::reader::{LookupResult, Table};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub mod error;
pub use error::Error;
pub mod filter;
pub use filter::BloomFilterPolicy;
pub mod options;
pub use options::{CompressionType, Options, WriteOptions};
pub(crate) mod coding;
pub(crate) mod db;
pub(crate) mod iter;
pub(crate) mod log;
pub(crate) mod memtable;
pub(crate) mod table;
pub mod write_batch;
pub use write_batch::{Handler, WriteBatch};

/// An immutable snapshot of the database state at a particular sequence number.
///
/// Obtained via [`Db::get_snapshot`] and passed via [`ReadOptions::snapshot`] to pin reads to a
/// specific point in time.  The snapshot is released automatically when it is dropped; compaction
/// is then free to discard entries that were only retained because of it.
///
/// See `include/leveldb/db.h: DB::GetSnapshot`.
pub struct Snapshot<'db> {
  db: &'db Db,
  pub(crate) seq: u64,
}

impl Drop for Snapshot<'_> {
  fn drop(&mut self) {
    self.db.state.lock().unwrap().snapshots.remove(&self.seq);
  }
}

impl std::fmt::Debug for Snapshot<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Snapshot").field("seq", &self.seq).finish()
  }
}

/// Options that control read operations.
///
/// See `include/leveldb/options.h`.
#[derive(Debug, Clone, Copy)]
pub struct ReadOptions<'snap> {
  /// Verify block checksums on every read in this operation.
  ///
  /// When set, every SSTable data block read for this `get` or iterator call has its CRC32c
  /// checksum verified.  Any mismatch returns [`Error::Corruption`].
  ///
  /// For database-wide verification without setting this on every `ReadOptions`, use
  /// [`Options::paranoid_checks`] instead.
  ///
  /// Default: false.
  pub verify_checksums: bool,

  /// Cache blocks read during this operation in the block cache.
  /// Set to `false` for bulk scans to avoid polluting the cache.
  ///
  /// **Not yet implemented.** Accepted but ignored — there is no block cache yet, so all block
  /// reads bypass caching unconditionally.
  ///
  /// Default: true.
  pub fill_cache: bool,

  /// Read as of this snapshot's sequence number.
  /// `None` means "use an implicit snapshot of the current state".
  pub snapshot: Option<&'snap Snapshot<'snap>>,
}

impl Default for ReadOptions<'_> {
  fn default() -> Self {
    ReadOptions {
      verify_checksums: false,
      fill_cache: true,
      snapshot: None,
    }
  }
}

// ── Shared inserter used by both Db::write and Db::recover_wal ───────────────

struct Inserter<'a> {
  mem: &'a Memtable,
  seq: u64,
}

impl Handler for Inserter<'_> {
  fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
    self.mem.add(self.seq, key, value);
    self.seq += 1;
    Ok(())
  }

  fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
    self.mem.delete(self.seq, key);
    self.seq += 1;
    Ok(())
  }
}

// ── DbState: lives inside a Mutex ────────────────────────────────────────────
//
// Matching LevelDB's single `mutex_`: the lock is taken briefly to snapshot
// Arc refs (`mem`, `imm`, current `Version`) and then released before any
// I/O (memtable reads are lock-free; SSTable reads and WAL writes are
// disk I/O).  Flush I/O is entirely lock-free — `begin_flush` rotates
// mem→imm and creates the new WAL file under the lock, then drops it;
// `finish_flush` re-acquires only to install the new Version and swap `log`.

struct DbState {
  /// Sequence number of the last committed write.
  last_sequence: u64,
  /// `None` for in-memory / test databases (no WAL).
  log: Option<LogWriter>,
  /// Active memtable.  `Arc` allows cloning a handle for iterators without
  /// holding the DB lock.
  mem: Arc<Memtable>,
  /// Sealed memtable currently being flushed to disk.  Reads must check this
  /// after `mem` and before the current `Version`.  `None` when no flush is
  /// in progress.
  imm: Option<Arc<Memtable>>,
  /// Tracks the set of live SSTable files and the MANIFEST.
  /// `None` for in-memory databases (`Db::default()`).
  version_set: Option<VersionSet>,
  /// Sequence numbers pinned by live snapshots (acquired via `get_snapshot`).
  /// Compaction uses the minimum value here as the visibility cutoff so it
  /// does not drop entries still observable through an active snapshot.
  snapshots: std::collections::BTreeSet<u64>,
}

// ── FlushPrep / FlushResult ───────────────────────────────────────────────────

/// Produced by `begin_flush` (under the write lock): everything needed to run
/// `write_flush` and `finish_flush` without holding the lock.
struct FlushPrep {
  sst_number: u64,
  sst_path: std::path::PathBuf,
  old_mem: Arc<Memtable>,
  /// File number of the new WAL created under the write lock.
  new_log_number: u64,
  /// Open writer for the new WAL (empty at this point).
  new_log: LogWriter,
  /// Path of the old WAL to delete after `log_and_apply` succeeds.
  old_log_path: std::path::PathBuf,
}

/// Return value of `write_flush`: everything `finish_flush` needs to install
/// the new SSTable into the `VersionSet` and complete WAL rotation.
struct FlushResult {
  file_number: u64,
  file_size: u64,
  smallest: Vec<u8>,
  largest: Vec<u8>,
  table: Arc<Table>,
  new_log_number: u64,
  new_log: LogWriter,
  old_log_path: std::path::PathBuf,
}

// ── DbIter ────────────────────────────────────────────────────────────────────

/// User-facing iterator over a consistent snapshot of the database.
///
/// Applies snapshot filtering, tombstone handling, and version merging across
/// the active memtable, any in-progress flush memtable, and all SSTable files.
///
/// Obtain via [`Db::new_iterator`].  The iterator starts **unpositioned**;
/// call [`seek_to_first`], [`seek_to_last`], or [`seek`] before reading keys
/// or values.  Supports both forward ([`next`]) and backward ([`prev`])
/// traversal; direction switches are handled transparently.
///
/// [`seek_to_first`]: DbIter::seek_to_first
/// [`seek_to_last`]: DbIter::seek_to_last
/// [`seek`]: DbIter::seek
/// [`next`]: DbIter::next
/// [`prev`]: DbIter::prev
pub struct DbIter {
  inner: db::db_iter::DbIterator,
}

impl DbIter {
  /// Returns `true` if the iterator is positioned at a valid entry.
  pub fn valid(&self) -> bool {
    self.inner.valid()
  }

  /// Position at the first user-visible entry.
  pub fn seek_to_first(&mut self) {
    self.inner.seek_to_first();
  }

  /// Position at the last user-visible entry.
  pub fn seek_to_last(&mut self) {
    self.inner.seek_to_last();
  }

  /// Position at the first user-visible entry with `key >= target`.
  pub fn seek(&mut self, key: &[u8]) {
    self.inner.seek(key);
  }

  /// Advance to the next user-visible entry.
  ///
  /// # Panics
  /// Panics (debug) if `valid()` is false.
  pub fn next(&mut self) {
    self.inner.next();
  }

  /// Move to the previous user-visible entry.
  ///
  /// # Panics
  /// Panics (debug) if `valid()` is false.
  pub fn prev(&mut self) {
    self.inner.prev();
  }

  /// Current user key.  Only valid when `valid()` is true.
  pub fn key(&self) -> &[u8] {
    self.inner.key()
  }

  /// Current value.  Only valid when `valid()` is true.
  pub fn value(&self) -> &[u8] {
    self.inner.value()
  }

  /// Returns a sticky error if the iterator encountered corruption.
  pub fn status(&self) -> Option<&Error> {
    self.inner.status()
  }
}

// ── LOCK file ────────────────────────────────────────────────────────────────

/// Acquire an exclusive, non-blocking `flock` on `file`.
///
/// Returns `Err(Error::IoError(...))` immediately if another process holds the
/// lock (`EWOULDBLOCK`), rather than blocking indefinitely.
///
/// # Safety
/// `flock(2)` is async-signal-safe and only modifies kernel state associated
/// with the file descriptor.
fn acquire_lock(file: &std::fs::File) -> Result<(), Error> {
  use std::os::unix::io::AsRawFd;
  // SAFETY: the fd is valid for the lifetime of `file`; flock does not alias memory.
  let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
  if ret == 0 {
    Ok(())
  } else {
    Err(Error::IoError(std::io::Error::last_os_error()))
  }
}

// ── Db ───────────────────────────────────────────────────────────────────────

pub struct Db {
  state: Mutex<DbState>,
  options: Options,
  /// `Some((dir, lock))` for persistent databases; `None` for in-memory (`Db::default()`).
  /// The `lock` file is held open to maintain the exclusive `flock` for the database lifetime.
  persistent: Option<(PathBuf, std::fs::File)>,
}

impl Default for Db {
  /// In-memory database with no WAL and no flush trigger.
  /// Intended for tests and ephemeral use.
  fn default() -> Self {
    Self {
      state: Mutex::new(DbState {
        last_sequence: 0,
        log: None,
        mem: Arc::new(Memtable::default()),
        imm: None,
        version_set: None,
        snapshots: std::collections::BTreeSet::new(),
      }),
      options: Options::default(),
      persistent: None,
    }
  }
}

impl Db {
  /// Open (or create) a persistent database at `path`.
  ///
  /// ## New database (no `CURRENT` file)
  /// - Returns [`Error::InvalidArgument`] if `options.create_if_missing` is `false`.
  /// - Creates a `MANIFEST-000002` with the initial `VersionEdit`.
  /// - Creates `CURRENT` pointing to the MANIFEST.
  /// - Creates `000001.log` as the WAL.
  ///
  /// ## Existing database (`CURRENT` file present)
  /// - Returns [`Error::InvalidArgument`] if `options.error_if_exists` is `true`.
  /// - Recovers the `VersionSet` from the MANIFEST (loads live SSTable files).
  /// - Replays WAL records whose sequence number exceeds the last sequence
  ///   already reflected in the MANIFEST (avoids double-inserting flushed data).
  ///
  /// In both cases an exclusive POSIX `flock` is acquired on `<path>/LOCK`.
  /// If another process already holds the lock, an [`Error::IoError`] is returned
  /// immediately rather than blocking.  The lock is released when the `Db` is dropped.
  ///
  /// `Db::default()` is retained for in-memory / test use (no WAL, no flush).
  pub fn open<P: AsRef<std::path::Path>>(path: P, options: Options) -> Result<Self, Error> {
    use std::fs::OpenOptions;

    let path = path.as_ref();
    let current_path = path.join("CURRENT");
    let db_exists = current_path.exists();

    if db_exists && options.error_if_exists {
      return Err(Error::InvalidArgument(format!(
        "database already exists at {}",
        path.display()
      )));
    }
    if !db_exists && !options.create_if_missing {
      return Err(Error::InvalidArgument(format!(
        "database does not exist at {}",
        path.display()
      )));
    }

    // Create the directory only when we are going to create a new database.
    if !db_exists {
      std::fs::create_dir_all(path)?;
    }

    // Acquire an exclusive non-blocking flock on LOCK before any other I/O.
    // This prevents two processes from corrupting the same database concurrently.
    let lock_file = OpenOptions::new()
      .create(true)
      .truncate(true)
      .read(true)
      .write(true)
      .open(path.join("LOCK"))?;
    acquire_lock(&lock_file)?;

    let (version_set, mem, last_sequence) = if db_exists {
      // ── Existing database: MANIFEST-driven recovery ──────────────────────
      let mut vs =
        VersionSet::recover(path, options.paranoid_checks, options.filter_policy.clone())?;
      let manifest_last_seq = vs.last_sequence();
      let mem = Arc::new(Memtable::default());

      // Replay WAL records newer than what is already in the SSTables.
      // Use the log number recorded in the MANIFEST (set by WAL rotation).
      let log_path = path.join(format!("{:06}.log", vs.log_number()));
      let actual_last_seq = if log_path.exists() {
        let file = std::fs::File::open(&log_path)?;
        let file_len = file.metadata()?.len();
        if file_len > 0 {
          Self::recover_wal(file, &mem, manifest_last_seq, options.paranoid_checks)?
        } else {
          manifest_last_seq
        }
      } else {
        manifest_last_seq
      };

      if actual_last_seq > manifest_last_seq {
        vs.set_last_sequence(actual_last_seq);
      }
      let last_seq = vs.last_sequence();
      (Some(vs), mem, last_seq)
    } else {
      // ── New database: create MANIFEST and WAL ────────────────────────────
      let vs = VersionSet::create(path)?;
      // Initial WAL is always 000001.log (log_number = 1 from VersionSet::create).
      std::fs::File::create(path.join("000001.log"))?;
      (Some(vs), Arc::new(Memtable::default()), 0)
    };

    // Open (or reopen) the current WAL for subsequent writes.
    let log_number = version_set.as_ref().map_or(1, |vs| vs.log_number());
    let log_path = path.join(format!("{log_number:06}.log"));
    let log_file = OpenOptions::new()
      .read(true)
      .append(true)
      .create(true)
      .open(&log_path)?;
    let file_len = log_file.metadata()?.len();
    let log_writer = LogWriter::new(log_file, file_len);

    Ok(Self {
      state: Mutex::new(DbState {
        last_sequence,
        log: Some(log_writer),
        mem,
        imm: None,
        version_set,
        snapshots: std::collections::BTreeSet::new(),
      }),
      options,
      persistent: Some((path.to_path_buf(), lock_file)),
    })
  }

  /// Replay WAL records from `file` into `mem`, skipping any whose sequence
  /// number is entirely covered by `min_sequence` (already in an SSTable).
  ///
  /// Returns the highest sequence number replayed (or `min_sequence` if
  /// nothing was replayed).
  fn recover_wal(
    file: std::fs::File,
    mem: &Memtable,
    min_sequence: u64,
    paranoid_checks: bool,
  ) -> Result<u64, Error> {
    let mut reader = LogReader::new(file, None, paranoid_checks, 0);
    let mut max_sequence: u64 = min_sequence;

    while let Some(record) = reader.read_record() {
      let batch = WriteBatch::from_contents(record)?;
      let start_seq = batch.sequence();
      // Skip batches fully covered by data already in SSTables.
      if batch.count() > 0 {
        let end_seq = start_seq + batch.count() as u64 - 1;
        if end_seq <= min_sequence {
          continue;
        }
      }
      batch.iterate(&mut Inserter {
        mem,
        seq: start_seq,
      })?;
      if batch.count() > 0 {
        let end_seq = start_seq + batch.count() as u64 - 1;
        if end_seq > max_sequence {
          max_sequence = end_seq;
        }
      }
    }

    Ok(max_sequence)
  }

  pub fn get<K>(&self, key: K) -> Result<Vec<u8>, Error>
  where
    K: AsRef<[u8]>,
  {
    self.get_with_options(&ReadOptions::default(), key)
  }

  pub fn get_with_options<K>(&self, opts: &ReadOptions, key: K) -> Result<Vec<u8>, Error>
  where
    K: AsRef<[u8]>,
  {
    let key = key.as_ref();

    // Mirror LevelDB's DBImpl::Get: take the lock only long enough to snapshot
    // the current memtable refs and sequence number, then release before doing
    // any I/O (memtable reads are lock-free; SSTable reads go to disk).
    let (sequence, mem, imm, version) = {
      let state = self.state.lock().unwrap();
      (
        opts.snapshot.map(|s| s.seq).unwrap_or(state.last_sequence),
        Arc::clone(&state.mem),
        state.imm.as_ref().map(Arc::clone),
        state.version_set.as_ref().map(|vs| vs.current()),
      )
    };

    let verify_checksums = opts.verify_checksums || self.options.paranoid_checks;

    match mem.get(key, sequence) {
      MemtableResult::Hit(v) => return Ok(v),
      MemtableResult::Deleted => return Err(Error::NotFound),
      MemtableResult::Miss => {}
    }

    if let Some(imm) = imm {
      match imm.get(key, sequence) {
        MemtableResult::Hit(v) => return Ok(v),
        MemtableResult::Deleted => return Err(Error::NotFound),
        MemtableResult::Miss => {}
      }
    }

    if let Some(version) = version {
      match version.get(key, sequence, verify_checksums)? {
        LookupResult::Value(v) => return Ok(v),
        LookupResult::Deleted => return Err(Error::NotFound),
        LookupResult::NotInTable => {}
      }
    }

    Err(Error::NotFound)
  }

  /// Return an immutable snapshot of the current database state.
  ///
  /// Pass a reference to the returned [`Snapshot`] via [`ReadOptions::snapshot`] to pin reads
  /// (via [`Db::get_with_options`] or [`Db::new_iterator`]) to this point in time.
  ///
  /// The snapshot is released automatically when it is dropped.  Pinned snapshots prevent
  /// compaction from discarding entries still visible through them, so avoid holding snapshots
  /// longer than necessary.
  ///
  /// See `include/leveldb/db.h: DB::GetSnapshot`.
  pub fn get_snapshot(&self) -> Snapshot<'_> {
    let mut state = self.state.lock().unwrap();
    let seq = state.last_sequence;
    state.snapshots.insert(seq);
    Snapshot { db: self, seq }
  }

  /// Return a property value for a named `property`, or `None` if the property
  /// is unknown.
  ///
  /// Supported property names:
  ///
  /// | Property | Description |
  /// |---|---|
  /// | `"leveldb.num-files-at-level<N>"` | File count at level N (0–6) |
  /// | `"leveldb.stats"` | Per-level file count and size table |
  /// | `"leveldb.sstables"` | One line per SSTable across all levels |
  /// | `"leveldb.approximate-memory-usage"` | Memtable bytes as a decimal string |
  ///
  /// See `include/leveldb/db.h: DB::GetProperty`.
  pub fn get_property(&self, property: &str) -> Option<String> {
    let prop = property.strip_prefix("leveldb.")?;

    // Snapshot what we need under the lock, then release before formatting.
    let (version, mem_usage, imm_usage) = {
      let state = self.state.lock().unwrap();
      let version = state.version_set.as_ref().map(|vs| vs.current());
      let mem_usage = state.mem.approximate_memory_usage();
      let imm_usage = state
        .imm
        .as_ref()
        .map_or(0, |i| i.approximate_memory_usage());
      (version, mem_usage, imm_usage)
    };

    if let Some(rest) = prop.strip_prefix("num-files-at-level") {
      let level: usize = rest.parse().ok()?;
      if level >= crate::db::version::NUM_LEVELS {
        return None;
      }
      let count = version.as_ref().map_or(0, |v| v.num_files(level));
      return Some(count.to_string());
    }

    match prop {
      "stats" => {
        let mut out = String::from(
          "                               Compactions\n\
           Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n\
           --------------------------------------------------\n",
        );
        for level in 0..crate::db::version::NUM_LEVELS {
          let files = version.as_ref().map_or(0, |v| v.num_files(level));
          let bytes = version.as_ref().map_or(0, |v| v.level_bytes(level));
          if files > 0 {
            out.push_str(&format!(
              "{level:3} {files:8} {mb:8.0}       0.0      0.0       0.0\n",
              mb = bytes as f64 / 1_048_576.0,
            ));
          }
        }
        Some(out)
      }
      "sstables" => Some(
        version
          .as_ref()
          .map_or_else(String::new, |v| v.debug_string()),
      ),
      "approximate-memory-usage" => Some((mem_usage + imm_usage).to_string()),
      _ => None,
    }
  }

  /// Return approximate on-disk byte sizes for each `(start, limit)` key range.
  ///
  /// Each element of the returned `Vec` corresponds to the same-indexed range
  /// in `ranges`.  The sizes are estimates only and do not account for
  /// compression or index-block overhead.  In-memory (unflushed) data is not
  /// counted.
  ///
  /// An open-ended range can be expressed with `start = b""` or `limit = b""`,
  /// which naturally compare as the logical minimum/maximum key respectively
  /// in internal-key order.
  ///
  /// See `include/leveldb/db.h: DB::GetApproximateSizes` and
  /// `db/db_impl.cc: DBImpl::GetApproximateSizes`.
  pub fn get_approximate_sizes(&self, ranges: &[(&[u8], &[u8])]) -> Vec<u64> {
    use crate::table::format::make_internal_key;

    // Snapshot the current version under the lock, then release.
    let version = {
      let state = self.state.lock().unwrap();
      state.version_set.as_ref().map(|vs| vs.current())
    };

    ranges
      .iter()
      .map(|&(start, limit)| {
        let Some(ref v) = version else { return 0 };
        // Convert user keys to lookup internal keys (seq=u64::MAX>>8, vtype=1)
        // so they sort before all real entries for the same user key — the same
        // sentinel used in Table::get.
        let istart = make_internal_key(start, u64::MAX >> 8, 1);
        let ilimit = make_internal_key(limit, u64::MAX >> 8, 1);
        let start_off = v.approximate_offset_of(&istart);
        let limit_off = v.approximate_offset_of(&ilimit);
        limit_off.saturating_sub(start_off)
      })
      .collect()
  }

  pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let mut batch = WriteBatch::new();
    batch.put(key.as_ref(), value.as_ref());
    self.write(&WriteOptions::default(), &batch)
  }

  pub fn delete<K>(&self, key: K) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
  {
    let mut batch = WriteBatch::new();
    batch.delete(key.as_ref());
    self.write(&WriteOptions::default(), &batch)
  }

  /// Create a forward iterator over a consistent snapshot of the database.
  ///
  /// If `opts.snapshot` is set, the iterator is pinned to that sequence number;
  /// otherwise it reads as of the current `last_sequence`.
  ///
  /// The memtable(s) and all SSTable file handles are pinned for the lifetime
  /// of the returned `DbIter`; reads and writes proceed concurrently.
  pub fn new_iterator(&self, opts: &ReadOptions<'_>) -> Result<DbIter, Error> {
    use crate::db::db_iter::DbIterator;
    use crate::db::merge_iter::MergingIterator;

    // Snapshot Arc refs under the lock, then release before constructing the
    // iterator (matching LevelDB's NewInternalIterator pattern).
    let (sequence, mem, imm, version) = {
      let state = self.state.lock().unwrap();
      (
        opts.snapshot.map(|s| s.seq).unwrap_or(state.last_sequence),
        Arc::clone(&state.mem),
        state.imm.as_ref().map(Arc::clone),
        state.version_set.as_ref().map(|vs| vs.current()),
      )
    };

    let verify_checksums = opts.verify_checksums || self.options.paranoid_checks;

    let mut children: Vec<Box<dyn crate::iter::InternalIterator>> = Vec::new();

    // Active memtable (newest writes, scanned first).
    children.push(Box::new(ArcMemTableIter::new(mem)));

    // Sealed memtable being flushed to disk (if any).
    if let Some(imm) = imm {
      children.push(Box::new(ArcMemTableIter::new(imm)));
    }

    // SSTable files from the current Version, level by level (L0 first).
    if let Some(version) = version {
      for level_files in &version.files {
        for meta in level_files {
          if let Some(table) = &meta.table {
            children.push(Box::new(table.new_iterator(verify_checksums)?));
          }
        }
      }
    }

    let inner = DbIterator::new(Box::new(MergingIterator::new(children)), sequence);
    Ok(DbIter { inner })
  }

  /// Compact all SSTable files that overlap `[begin, end]` (user-key range)
  /// down toward the deepest level, matching LevelDB's `CompactRange`.
  ///
  /// Both bounds are inclusive and optional (`None` means "no bound").
  ///
  /// The compaction is synchronous and runs level-by-level from L0 to the
  /// deepest level that currently has overlapping files.  After each level's
  /// compaction the updated version is used for the next level, so newly
  /// created files are eligible for further compaction in the same call.
  ///
  /// No-op for in-memory databases (`Db::default()`).
  /// Destroy the database at `path`, deleting all of its files and the directory.
  ///
  /// Acquires the `LOCK` file before removing anything to ensure no other process
  /// has the database open.  Returns [`Error::IoError`] immediately (without blocking)
  /// if the lock cannot be acquired.
  ///
  /// All recognised database files (`CURRENT`, `MANIFEST-*`, `*.log`, `*.ldb`, `LOCK`) are
  /// removed.  Unrecognised files are left in place; the directory is removed only if it is
  /// empty afterwards.
  ///
  /// Returns `Ok(())` if `path` does not exist.
  ///
  /// See `db/db_impl.cc: DestroyDB`.
  pub fn destroy<P: AsRef<std::path::Path>>(path: P) -> Result<(), Error> {
    let path = path.as_ref();

    if !path.exists() {
      return Ok(());
    }

    // Acquire the lock to confirm no other process has the database open.
    let lock_path = path.join("LOCK");
    let lock_file = std::fs::OpenOptions::new()
      .create(true)
      .truncate(true)
      .read(true)
      .write(true)
      .open(&lock_path)?;
    acquire_lock(&lock_file)?;

    // Enumerate and remove every recognised database file (except LOCK — last).
    for entry in std::fs::read_dir(path)? {
      let entry = entry?;
      let name = entry.file_name();
      let name = name.to_string_lossy();
      if name == "LOCK" {
        continue; // removed after we release the flock below
      }
      if parse_db_filename(&name).is_some() {
        let _ = std::fs::remove_file(entry.path());
      }
    }

    // Release the flock by dropping the file, then remove the LOCK file itself.
    drop(lock_file);
    let _ = std::fs::remove_file(&lock_path);

    // Remove the directory if it is now empty; ignore the error if it is not.
    let _ = std::fs::remove_dir(path);

    Ok(())
  }

  pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<(), Error> {
    use crate::db::version::NUM_LEVELS;

    let path = match self.path() {
      Some(p) => p.to_path_buf(),
      None => return Ok(()),
    };

    // Find the deepest level that currently has files overlapping [begin, end].
    let max_level = {
      let g = self.state.lock().unwrap();
      let vs = match &g.version_set {
        Some(vs) => vs,
        None => return Ok(()),
      };
      let version = vs.current();
      let mut max = 0usize;
      for level in 0..NUM_LEVELS {
        let has = version.files[level].iter().any(|m| {
          file_overlaps_range(
            ikey_user_key(&m.smallest),
            ikey_user_key(&m.largest),
            begin,
            end,
          )
        });
        if has {
          max = level;
        }
      }
      max
    };

    // Compact every level from 0 up to (but not including) max_level, pushing
    // data toward max_level.  If max_level is already the deepest level (6),
    // cap at NUM_LEVELS-1 since there is no level above it to compact into.
    let stop = max_level.min(NUM_LEVELS - 1);
    for level in 0..stop {
      compact_level_range(&path, &self.state, &self.options, level, begin, end)?;
    }

    Ok(())
  }

  pub fn write(&self, opts: &WriteOptions, batch: &WriteBatch) -> Result<(), Error> {
    let mut state = self.state.lock().unwrap();
    let start_seq = state.last_sequence + 1;

    // Clone and stamp so the WAL record carries the correct embedded sequence.
    let mut stamped = batch.clone();
    stamped.set_sequence(start_seq);

    if let Some(log) = state.log.as_mut() {
      log.add_record(stamped.contents())?;
      if opts.sync {
        log.sync()?;
      }
    }

    stamped.iterate(&mut Inserter {
      mem: &state.mem,
      seq: start_seq,
    })?;
    state.last_sequence += batch.count() as u64;

    // Trigger an L0 flush when the memtable exceeds the size limit.
    // Only applies to persistent databases (path.is_some()).
    if let Some(path) = self.path() {
      if state.mem.approximate_memory_usage() >= self.options.write_buffer_size {
        // Phase 1: rotate mem → imm, allocate new WAL file (write lock held).
        let prep = begin_flush(path, &mut state)?;
        // Release the write lock before doing any I/O.
        drop(state);
        // Phase 2: write the SSTable (no lock held — reads proceed freely).
        let result = write_flush(prep, &self.options)?;
        // Phase 3: install the new Version, swap WAL, clear imm (lock held).
        let mut state = self.state.lock().unwrap();
        finish_flush(&mut state, result)?;
        drop(state);
        // Phase 4: delete obsolete files (lock released; best-effort).
        delete_obsolete_files(path, &self.state);
        // Phase 5: compact L0 → L1 if we've hit the trigger.  Loop so that a
        // burst of flushes that accumulates more than one compaction's worth of
        // L0 files still gets drained before returning to the caller.
        for _ in 0..32 {
          let l0 = {
            let g = self.state.lock().unwrap();
            g.version_set
              .as_ref()
              .map_or(0, |vs| vs.current().files[0].len())
          };
          if l0 < L0_COMPACTION_TRIGGER {
            break;
          }
          // Slow-write throttle: yield briefly when L0 is piling up but hasn't
          // hit the stop threshold.  This matches LevelDB's MakeRoomForWrite.
          if l0 >= L0_SLOWDOWN_WRITES_TRIGGER {
            std::thread::sleep(std::time::Duration::from_millis(1));
          }
          maybe_compact(path, &self.state, &self.options);
        }
      }
    }

    Ok(())
  }

  /// Returns the on-disk database directory, or `None` for in-memory databases.
  fn path(&self) -> Option<&std::path::Path> {
    self.persistent.as_ref().map(|(p, _)| p.as_path())
  }
}

// ── flush helpers ─────────────────────────────────────────────────────────────
//
// Flush is split into three phases so the mutex is not held during I/O:
//
//   begin_flush  — under lock: rotate mem→imm, allocate new WAL file
//   write_flush  — no lock: write SSTable to disk, open it for reading
//   finish_flush — under lock: install new Version, swap WAL, clear imm

/// Phase 1 (under write lock): seal `mem` as `imm`, install a fresh memtable,
/// allocate the next SSTable file number, and create the new WAL file.
///
/// The new WAL is created here — under the lock — so that any writes between
/// now and `finish_flush` still go to the *old* WAL (via `state.log`).  The
/// new WAL becomes active only once `finish_flush` swaps `state.log`.
fn begin_flush(path: &std::path::Path, state: &mut DbState) -> Result<FlushPrep, Error> {
  let vs = state
    .version_set
    .as_mut()
    .expect("begin_flush: no VersionSet");
  let old_log_number = vs.log_number();
  let sst_number = vs.next_file_number();
  let new_log_number = vs.next_file_number();
  let new_log_file = std::fs::File::create(path.join(format!("{new_log_number:06}.log")))?;
  let new_log = LogWriter::new(new_log_file, 0);
  let old_mem = std::mem::take(&mut state.mem);
  state.imm = None; // should already be None; be explicit
  Ok(FlushPrep {
    sst_number,
    sst_path: path.join(format!("{sst_number:06}.ldb")),
    old_mem,
    new_log_number,
    new_log,
    old_log_path: path.join(format!("{old_log_number:06}.log")),
  })
}

/// Phase 2 (no lock held): iterate `old_mem`, write an SSTable, and pass WAL
/// rotation state through to `finish_flush`.  On error `old_mem` is dropped;
/// data remains safe in the old WAL (not yet rotated away).
fn write_flush(prep: FlushPrep, opts: &Options) -> Result<FlushResult, Error> {
  use std::fs::OpenOptions;
  let FlushPrep {
    sst_number,
    sst_path,
    old_mem,
    new_log_number,
    new_log,
    old_log_path,
  } = prep;
  let file = OpenOptions::new()
    .write(true)
    .create_new(true)
    .open(&sst_path)?;
  let mut builder = TableBuilder::new(
    file,
    opts.block_size,
    opts.block_restart_interval,
    opts.filter_policy.clone(),
    opts.compression,
  );
  let mut smallest = Vec::new();
  let mut largest = Vec::new();
  {
    let mut it = old_mem.iter();
    it.seek_to_first();
    let mut first = true;
    while it.valid() {
      let ikey = it.key().to_vec(); // SSTable internal key (user_key || tag)
      if first {
        smallest = ikey.clone();
        first = false;
      }
      largest = ikey;
      builder.add(it.key(), it.value())?;
      it.advance();
    }
  }
  let file_size = builder.finish()?;
  drop(old_mem);
  let read_file = std::fs::File::open(&sst_path)?;
  let table = Arc::new(Table::open(
    read_file,
    file_size,
    opts.filter_policy.clone(),
  )?);
  Ok(FlushResult {
    file_number: sst_number,
    file_size,
    smallest,
    largest,
    table,
    new_log_number,
    new_log,
    old_log_path,
  })
}

/// Phase 3 (under write lock): atomically record the new SST *and* new log
/// number in the MANIFEST (`log_and_apply`), swap `state.log` to the new WAL,
/// clear `imm`, and delete the old WAL.
///
/// `set_log_number` is called *before* `log_and_apply` so the MANIFEST record
/// carries the correct log number — matching LevelDB's `MakeRoomForWrite`.
fn finish_flush(state: &mut DbState, result: FlushResult) -> Result<(), Error> {
  let meta = FileMetaData::with_table(
    result.file_number,
    result.file_size,
    result.smallest,
    result.largest,
    result.table,
  );
  let mut edit = VersionEdit::new();
  edit.new_files.push((0, meta));
  let vs = state
    .version_set
    .as_mut()
    .expect("finish_flush: no VersionSet");
  vs.set_last_sequence(state.last_sequence);
  vs.set_log_number(result.new_log_number);
  vs.log_and_apply(&mut edit)?;
  // Switch to the new WAL; the MANIFEST now points past the old one.
  state.log = Some(result.new_log);
  state.imm = None;
  // Best-effort delete — ignore errors (e.g. the path never existed on new DB).
  let _ = std::fs::remove_file(&result.old_log_path);
  Ok(())
}

// ── Compaction ────────────────────────────────────────────────────────────────
//
// L0→L1 compaction is triggered when L0 reaches L0_COMPACTION_TRIGGER files.
// It runs synchronously after each flush using the same lock-snapshot pattern
// as flush: snapshot state under lock, I/O without lock, install under lock.
//
// Shadow-key pruning: for each user key, only the newest version is kept.
// Tombstone elision: deletion markers are dropped when there is definitely no
// data for that key below the output level (L2+).

const L0_COMPACTION_TRIGGER: usize = 4;
const L0_SLOWDOWN_WRITES_TRIGGER: usize = 8;

/// Extract the user-key prefix from an SSTable internal key.
/// Internal key format: `user_key || (seq<<8|vtype).to_le_bytes()` (8-byte tag).
fn ikey_user_key(ikey: &[u8]) -> &[u8] {
  if ikey.len() >= 8 {
    &ikey[..ikey.len() - 8]
  } else {
    ikey
  }
}

/// True if the user-key ranges [a_s..a_l] and [b_s..b_l] overlap.
fn key_ranges_overlap(a_s: &[u8], a_l: &[u8], b_s: &[u8], b_l: &[u8]) -> bool {
  ikey_user_key(a_s) <= ikey_user_key(b_l) && ikey_user_key(b_s) <= ikey_user_key(a_l)
}

/// True if `user_key` is definitely absent from all files at level ≥ `from_level`.
///
/// A conservative check: returns `false` if any file's key range spans the user
/// key, even if the key isn't actually present in the file.  Safe to use for
/// tombstone elision: we only drop a tombstone when this returns `true`.
fn is_base_level(version: &crate::db::version::Version, uk: &[u8], from_level: usize) -> bool {
  use crate::db::version::NUM_LEVELS;
  for level in from_level..NUM_LEVELS {
    for meta in &version.files[level] {
      if uk >= ikey_user_key(&meta.smallest) && uk <= ikey_user_key(&meta.largest) {
        return false;
      }
    }
  }
  true
}

/// Select files for the next L0→L1 compaction.
///
/// Returns `None` if L0 has fewer than `L0_COMPACTION_TRIGGER` files.  Otherwise
/// returns `(l0_inputs, l1_inputs)` where `l1_inputs` are the L1 files whose key
/// range overlaps with the union key range of all L0 files.
type CompactionInputs = (Vec<Arc<FileMetaData>>, Vec<Arc<FileMetaData>>);

fn pick_compaction(version: &crate::db::version::Version) -> Option<CompactionInputs> {
  if version.files[0].len() < L0_COMPACTION_TRIGGER {
    return None;
  }
  let l0_inputs: Vec<Arc<FileMetaData>> = version.files[0].to_vec();

  // Union key range of all L0 files.
  let mut range_small = l0_inputs[0].smallest.clone();
  let mut range_large = l0_inputs[0].largest.clone();
  for m in l0_inputs.iter().skip(1) {
    if ikey_user_key(&m.smallest) < ikey_user_key(&range_small) {
      range_small = m.smallest.clone();
    }
    if ikey_user_key(&m.largest) > ikey_user_key(&range_large) {
      range_large = m.largest.clone();
    }
  }

  let l1_inputs: Vec<Arc<FileMetaData>> = version.files[1]
    .iter()
    .filter(|m| key_ranges_overlap(&m.smallest, &m.largest, &range_small, &range_large))
    .cloned()
    .collect();

  Some((l0_inputs, l1_inputs))
}

/// A completed compaction output SSTable.
struct CompactionOutput {
  file_number: u64,
  file_size: u64,
  smallest: Vec<u8>,
  largest: Vec<u8>,
  table: Arc<Table>,
}

/// In-progress output SSTable being built during compaction.
struct CompactionOutputFile {
  file_number: u64,
  path: std::path::PathBuf,
  builder: TableBuilder,
  smallest: Vec<u8>,
}

/// Finalise `cur`, open the file for reading, and push to `outputs`.
fn finish_compaction_output(
  cur: CompactionOutputFile,
  largest: Vec<u8>,
  outputs: &mut Vec<CompactionOutput>,
  filter_policy: Option<Arc<dyn crate::filter::FilterPolicy>>,
) -> Result<(), Error> {
  let file_size = cur.builder.finish()?;
  let read_file = std::fs::File::open(&cur.path)?;
  let table = Arc::new(Table::open(read_file, file_size, filter_policy)?);
  outputs.push(CompactionOutput {
    file_number: cur.file_number,
    file_size,
    smallest: cur.smallest,
    largest,
    table,
  });
  Ok(())
}

/// Phase 2 (no lock): merge input files, apply pruning, write output SSTables.
///
/// Shadow-key pruning: for each user key, only the first (newest) version
/// encountered in the merge order is kept.
/// Tombstone elision: a deletion marker is dropped when there is provably no
/// data for that key at levels > `output_level`.
///
/// `inputs` should be ordered so that newer files (by file number) appear
/// before older ones for the same level, allowing `MergingIterator` to resolve
/// same-key ties in favour of the newer version via the lower child index.
fn do_compaction(
  path: &std::path::Path,
  state: &Mutex<DbState>,
  version: &crate::db::version::Version,
  inputs: &[&FileMetaData],
  oldest_snapshot: u64,
  output_level: usize,
  opts: &Options,
) -> Result<Vec<CompactionOutput>, Error> {
  use crate::db::merge_iter::MergingIterator;
  use crate::iter::InternalIterator;

  let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
  for meta in inputs {
    let table = meta
      .table
      .as_ref()
      .expect("compaction input must have an open table");
    children.push(Box::new(table.new_iterator(opts.paranoid_checks)?));
  }

  let mut merger = MergingIterator::new(children);
  merger.seek_to_first();

  let mut outputs: Vec<CompactionOutput> = Vec::new();
  let mut current: Option<CompactionOutputFile> = None;
  let mut current_largest: Vec<u8> = Vec::new();

  // Deduplication / tombstone-elision state.
  let mut current_user_key: Vec<u8> = Vec::new();
  let mut has_current_user_key = false;
  let mut last_sequence_for_key: u64 = u64::MAX;

  while merger.valid() {
    let ikey = merger.key().to_vec();
    let value = merger.value().to_vec();
    merger.next();

    let (uk, seq, vtype) = match crate::table::format::parse_internal_key(&ikey) {
      Some(parts) => parts,
      None => continue, // corrupt key — skip silently
    };

    // Track first occurrence of this user key.
    let first_occurrence = !has_current_user_key || uk != current_user_key.as_slice();
    if first_occurrence {
      current_user_key = uk.to_vec();
      has_current_user_key = true;
      last_sequence_for_key = u64::MAX;
    }

    // Determine whether to drop this entry.
    let drop = if last_sequence_for_key <= oldest_snapshot {
      // A newer version for this user key was already emitted and is visible
      // to even the oldest active snapshot — this older version is invisible.
      true
    } else if vtype == 0 && seq <= oldest_snapshot && is_base_level(version, uk, output_level + 1) {
      // Tombstone that no snapshot can see below this level — safe to elide.
      true
    } else {
      false
    };

    last_sequence_for_key = seq;

    if !drop {
      // Rotate to a new output file if the current one is at the size limit.
      if let Some(ref cur) = current {
        if cur.builder.file_size() >= opts.max_file_size as u64 {
          let finished = current.take().unwrap();
          finish_compaction_output(
            finished,
            std::mem::take(&mut current_largest),
            &mut outputs,
            opts.filter_policy.clone(),
          )?;
        }
      }

      // Open a new output file (allocating its number under the lock).
      if current.is_none() {
        let file_number = {
          let mut g = state.lock().unwrap();
          g.version_set.as_mut().unwrap().next_file_number()
        };
        let sst_path = path.join(format!("{file_number:06}.ldb"));
        let file = std::fs::OpenOptions::new()
          .write(true)
          .create_new(true)
          .open(&sst_path)?;
        let builder = TableBuilder::new(
          file,
          opts.block_size,
          opts.block_restart_interval,
          opts.filter_policy.clone(),
          opts.compression,
        );
        current = Some(CompactionOutputFile {
          file_number,
          path: sst_path,
          builder,
          smallest: ikey.clone(),
        });
      }

      let cur = current.as_mut().unwrap();
      cur.builder.add(&ikey, &value)?;
      current_largest = ikey;
    }
  }

  // Finalise the last output file (if any).
  if let Some(cur) = current {
    finish_compaction_output(
      cur,
      current_largest,
      &mut outputs,
      opts.filter_policy.clone(),
    )?;
  }

  Ok(outputs)
}

/// Phase 3 (under lock): record deleted input files and new output files in a
/// single `VersionEdit`, then call `log_and_apply`.
///
/// `deleted` is a list of `(level, file_number)` pairs for all input files.
/// `output_level` is the level the output SSTables are placed at.
fn install_compaction(
  state: &mut DbState,
  deleted: &[(i32, u64)],
  outputs: Vec<CompactionOutput>,
  output_level: i32,
) -> Result<(), Error> {
  let vs = state
    .version_set
    .as_mut()
    .expect("install_compaction: no VersionSet");
  let mut edit = VersionEdit::new();
  for &(level, number) in deleted {
    edit.deleted_files.push((level, number));
  }
  for out in outputs {
    let meta = FileMetaData::with_table(
      out.file_number,
      out.file_size,
      out.smallest,
      out.largest,
      out.table,
    );
    edit.new_files.push((output_level, meta));
  }
  vs.set_last_sequence(state.last_sequence);
  vs.log_and_apply(&mut edit)?;
  Ok(())
}

/// True if a file whose user-key range is `[file_small, file_large]` overlaps
/// the user-key range `[begin, end]` (either bound may be `None` = open).
fn file_overlaps_range(
  file_small: &[u8],
  file_large: &[u8],
  begin: Option<&[u8]>,
  end: Option<&[u8]>,
) -> bool {
  begin.is_none_or(|b| file_large >= b) && end.is_none_or(|e| file_small <= e)
}

/// Select files for a range-based compaction of `level` → `level + 1`.
///
/// Returns `(level_inputs, next_level_inputs)` for files at `level` that
/// overlap `[begin, end]` and the `level+1` files that overlap their union
/// range.  Returns `None` if there is nothing to compact.
fn pick_range_compaction(
  version: &crate::db::version::Version,
  level: usize,
  begin: Option<&[u8]>,
  end: Option<&[u8]>,
) -> Option<CompactionInputs> {
  use crate::db::version::NUM_LEVELS;

  if level + 1 >= NUM_LEVELS {
    return None;
  }

  let level_inputs: Vec<Arc<FileMetaData>> = version.files[level]
    .iter()
    .filter(|m| {
      file_overlaps_range(
        ikey_user_key(&m.smallest),
        ikey_user_key(&m.largest),
        begin,
        end,
      )
    })
    .cloned()
    .collect();

  if level_inputs.is_empty() {
    return None;
  }

  // Union user-key range of the selected level files.
  let range_small = level_inputs
    .iter()
    .min_by_key(|m| ikey_user_key(&m.smallest))
    .unwrap()
    .smallest
    .clone();
  let range_large = level_inputs
    .iter()
    .max_by_key(|m| ikey_user_key(&m.largest))
    .unwrap()
    .largest
    .clone();

  let next_inputs: Vec<Arc<FileMetaData>> = version.files[level + 1]
    .iter()
    .filter(|m| key_ranges_overlap(&m.smallest, &m.largest, &range_small, &range_large))
    .cloned()
    .collect();

  Some((level_inputs, next_inputs))
}

/// Run a single-level range compaction (`level` → `level + 1`) synchronously.
///
/// Uses the same three-phase lock protocol as `maybe_compact`.
/// Returns `Ok(true)` if a compaction ran, `Ok(false)` if nothing to compact.
fn compact_level_range(
  path: &std::path::Path,
  state: &Mutex<DbState>,
  opts: &Options,
  level: usize,
  begin: Option<&[u8]>,
  end: Option<&[u8]>,
) -> Result<bool, Error> {
  // Phase 1: snapshot.
  let (version, oldest_snapshot) = {
    let g = state.lock().unwrap();
    let vs = match &g.version_set {
      Some(vs) => vs,
      None => return Ok(false),
    };
    let oldest = g
      .snapshots
      .iter()
      .next()
      .copied()
      .unwrap_or(g.last_sequence);
    (vs.current(), oldest)
  };

  let (level_inputs, next_inputs) = match pick_range_compaction(&version, level, begin, end) {
    Some(p) => p,
    None => return Ok(false),
  };

  let output_level = level + 1;
  let inputs: Vec<&FileMetaData> = level_inputs
    .iter()
    .chain(next_inputs.iter())
    .map(Arc::as_ref)
    .collect();
  let deleted: Vec<(i32, u64)> = level_inputs
    .iter()
    .map(|m| (level as i32, m.number))
    .chain(next_inputs.iter().map(|m| (output_level as i32, m.number)))
    .collect();

  // Phase 2: I/O (no lock).
  let outputs = do_compaction(
    path,
    state,
    version.as_ref(),
    inputs.as_slice(),
    oldest_snapshot,
    output_level,
    opts,
  )?;

  // Phase 3: install.
  {
    let mut g = state.lock().unwrap();
    install_compaction(&mut g, &deleted, outputs, output_level as i32)?;
  }

  delete_obsolete_files(path, state);
  Ok(true)
}

/// Run a synchronous L0→L1 compaction if L0 has reached the trigger.
///
/// Uses the same three-phase lock protocol as flush:
///   1. Snapshot current version under lock.
///   2. Run I/O (MergingIterator + SSTable writes) without the lock.
///   3. Install the result (VersionEdit + log_and_apply) under the lock.
///
/// Errors are silently ignored — a failed compaction does not affect
/// correctness; L0 will simply accumulate more files and be retried.
fn maybe_compact(path: &std::path::Path, state: &Mutex<DbState>, opts: &Options) {
  // Phase 1: snapshot.
  let (version, oldest_snapshot) = {
    let g = state.lock().unwrap();
    let vs = match &g.version_set {
      Some(vs) => vs,
      None => return,
    };
    let oldest = g
      .snapshots
      .iter()
      .next()
      .copied()
      .unwrap_or(g.last_sequence);
    (vs.current(), oldest)
  };

  let (l0_inputs, l1_inputs) = match pick_compaction(&version) {
    Some(p) => p,
    None => return,
  };

  // Build the flat input list (L0 newest-first, then L1) and deleted pairs.
  let inputs: Vec<&FileMetaData> = l0_inputs
    .iter()
    .chain(l1_inputs.iter())
    .map(Arc::as_ref)
    .collect();
  let deleted: Vec<(i32, u64)> = l0_inputs
    .iter()
    .map(|m| (0i32, m.number))
    .chain(l1_inputs.iter().map(|m| (1i32, m.number)))
    .collect();

  // Phase 2: I/O (no lock).
  let outputs = match do_compaction(
    path,
    state,
    version.as_ref(),
    inputs.as_slice(),
    oldest_snapshot,
    1,
    opts,
  ) {
    Ok(o) => o,
    Err(_) => return,
  };

  // Phase 3: install.
  {
    let mut g = state.lock().unwrap();
    if install_compaction(&mut g, &deleted, outputs, 1).is_err() {
      return;
    }
  }

  delete_obsolete_files(path, state);
}

// ── DeleteObsoleteFiles ───────────────────────────────────────────────────────

/// Recognised kinds of database files, parsed from their filenames.
enum FileKind {
  Log,
  Manifest,
  Table,
  Current,
  Lock,
}

/// Parse a database filename into `(file_number, kind)`.
///
/// Returns `None` for filenames that don't match any known pattern (e.g.
/// temporary editor files, `.` / `..`).
///
/// Mirrors LevelDB's `ParseFileName` in `db/filename.h/cc`.
fn parse_db_filename(name: &str) -> Option<(u64, FileKind)> {
  if name == "CURRENT" {
    return Some((0, FileKind::Current));
  }
  if name == "LOCK" {
    return Some((0, FileKind::Lock));
  }
  if let Some(rest) = name.strip_prefix("MANIFEST-") {
    let n = rest.parse().ok()?;
    return Some((n, FileKind::Manifest));
  }
  if let Some(stem) = name.strip_suffix(".log") {
    let n = stem.parse().ok()?;
    return Some((n, FileKind::Log));
  }
  if let Some(stem) = name.strip_suffix(".ldb") {
    let n = stem.parse().ok()?;
    return Some((n, FileKind::Table));
  }
  None
}

/// Delete database files that are no longer referenced by any live `Version`.
///
/// Matches LevelDB's `DBImpl::RemoveObsoleteFiles`:
///   1. Take the lock briefly to collect the live file set and watermarks.
///   2. Release the lock.
///   3. Enumerate the directory and delete any obsolete files.
///
/// Errors (missing dir, unlink failures) are silently ignored — GC is
/// best-effort and failure does not affect correctness.
fn delete_obsolete_files(path: &std::path::Path, state: &Mutex<DbState>) {
  use std::collections::HashSet;

  // Step 1: snapshot live-file info under the lock, then release it.
  let (live_tables, log_number, manifest_number) = {
    let state = state.lock().unwrap();
    let vs = match &state.version_set {
      Some(vs) => vs,
      None => return, // in-memory DB — nothing to clean up
    };
    let mut live = HashSet::new();
    vs.add_live_files(&mut live);
    (live, vs.log_number(), vs.manifest_number())
  };

  // Step 2: enumerate the directory and delete obsolete files.
  let entries = match std::fs::read_dir(path) {
    Ok(e) => e,
    Err(_) => return,
  };
  for entry in entries.flatten() {
    let name = entry.file_name();
    let name = name.to_string_lossy();
    if let Some((number, kind)) = parse_db_filename(&name) {
      let keep = match kind {
        FileKind::Log => number >= log_number,
        FileKind::Manifest => number >= manifest_number,
        FileKind::Table => live_tables.contains(&number),
        FileKind::Current | FileKind::Lock => true,
      };
      if !keep {
        let _ = std::fs::remove_file(entry.path());
      }
    }
  }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use crate::{Db, Error, Options, ReadOptions, WriteOptions};

  #[test]
  fn in_memory_round_trip() {
    let db = Db::default();
    assert!(db.get(b"42").unwrap_err().is_not_found());
    db.put(b"42", b"An answer to some question").unwrap();
    assert_eq!(db.get(b"42").unwrap(), b"An answer to some question");
    db.delete(b"42").unwrap();
    assert!(db.get(b"42").unwrap_err().is_not_found());
  }

  #[test]
  fn open_creates_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), create_options()).unwrap();
      db.put(b"key", b"value").unwrap();
      db.put(b"foo", b"bar").unwrap();
      db.delete(b"foo").unwrap();
    }
    // Reopen: recovery must restore state from the WAL.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"key").unwrap(), b"value");
    assert!(db.get(b"foo").unwrap_err().is_not_found());
  }

  #[test]
  fn open_empty_db_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), create_options()).unwrap();
    assert!(db.get(b"anything").unwrap_err().is_not_found());
  }

  #[test]
  fn open_fails_when_missing_and_create_if_missing_false() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options::default(); // create_if_missing = false
    let err = Db::open(dir.path(), opts).err().unwrap();
    assert!(
      matches!(err, Error::InvalidArgument(_)),
      "expected InvalidArgument"
    );
  }

  #[test]
  fn open_succeeds_when_missing_and_create_if_missing_true() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), create_options()).unwrap();
    db.put(b"k", b"v").unwrap();
    assert_eq!(db.get(b"k").unwrap(), b"v");
  }

  #[test]
  fn open_fails_when_exists_and_error_if_exists_true() {
    let dir = tempfile::tempdir().unwrap();
    // Create the database first.
    Db::open(dir.path(), create_options()).unwrap();
    // Second open with error_if_exists must fail.
    let opts = Options {
      error_if_exists: true,
      ..Options::default()
    };
    let err = Db::open(dir.path(), opts).err().unwrap();
    assert!(
      matches!(err, Error::InvalidArgument(_)),
      "expected InvalidArgument"
    );
  }

  #[test]
  fn open_succeeds_when_exists_and_error_if_exists_false() {
    let dir = tempfile::tempdir().unwrap();
    Db::open(dir.path(), create_options()).unwrap();
    // Default error_if_exists = false — reopening must succeed.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    // DB is empty but opens without error.
    assert!(db.get(b"k").unwrap_err().is_not_found());
  }

  #[test]
  fn create_if_missing_does_not_create_directory_on_failure() {
    // When create_if_missing = false, Db::open must not create the directory.
    let base = tempfile::tempdir().unwrap();
    let db_path = base.path().join("nonexistent");
    assert!(!db_path.exists());
    let _ = Db::open(&db_path, Options::default());
    assert!(
      !db_path.exists(),
      "Db::open must not create the directory when create_if_missing = false"
    );
  }

  #[test]
  fn lock_prevents_concurrent_open() {
    let dir = tempfile::tempdir().unwrap();
    let _db = Db::open(dir.path(), create_options()).unwrap();
    // Second open while the first is still alive must fail with IoError (EWOULDBLOCK).
    let err = Db::open(dir.path(), Options::default()).err().unwrap();
    assert!(
      matches!(err, Error::IoError(_)),
      "expected IoError from flock, got {err:?}"
    );
  }

  #[test]
  fn lock_released_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), create_options()).unwrap();
    db.put(b"k", b"v").unwrap();
    drop(db); // releases the flock
              // Reopening after drop must succeed.
    let db2 = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db2.get(b"k").unwrap(), b"v");
  }

  // ── Db::destroy tests ──────────────────────────────────────────────────────

  #[test]
  fn destroy_nonexistent_path_is_ok() {
    let base = tempfile::tempdir().unwrap();
    assert!(Db::destroy(base.path().join("no_such_db")).is_ok());
  }

  #[test]
  fn destroy_removes_all_db_files_and_directory() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), create_options()).unwrap();
      // Write enough to trigger a flush so we get at least one .ldb file.
      for i in 0..200u32 {
        db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
      }
    } // db dropped here — releases lock

    Db::destroy(dir.path()).unwrap();
    assert!(
      !dir.path().exists(),
      "directory should be removed after destroy"
    );
  }

  #[test]
  fn destroy_fails_if_db_is_open() {
    let dir = tempfile::tempdir().unwrap();
    let _db = Db::open(dir.path(), create_options()).unwrap();
    let err = Db::destroy(dir.path()).err().unwrap();
    assert!(
      matches!(err, Error::IoError(_)),
      "expected IoError from flock, got {err:?}"
    );
  }

  #[test]
  fn destroy_leaves_unrecognised_files() {
    let dir = tempfile::tempdir().unwrap();
    Db::open(dir.path(), create_options()).unwrap();
    // Plant an unrecognised file.
    std::fs::write(dir.path().join("extra.txt"), b"keep me").unwrap();

    Db::destroy(dir.path()).unwrap();

    // The directory must still exist (not empty) and contain only the extra file.
    assert!(dir.path().exists());
    let remaining: Vec<_> = std::fs::read_dir(dir.path())
      .unwrap()
      .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
      .collect();
    assert_eq!(remaining, vec!["extra.txt"]);
  }

  #[test]
  fn write_batch_atomic_recovery() {
    use crate::WriteBatch;
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), create_options()).unwrap();
      let mut batch = WriteBatch::new();
      batch.put("a", "1");
      batch.put("b", "2");
      batch.put("c", "3");
      db.write(&WriteOptions::default(), &batch).unwrap();
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get("a").unwrap(), b"1");
    assert_eq!(db.get("b").unwrap(), b"2");
    assert_eq!(db.get("c").unwrap(), b"3");
  }

  #[test]
  fn sequence_advances_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), create_options()).unwrap();
      db.put(b"k", b"first").unwrap();
    }
    {
      let db = Db::open(dir.path(), Options::default()).unwrap();
      db.put(b"k", b"second").unwrap();
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), b"second");
  }

  // ── L0 flush + disk read tests ─────────────────────────────────────────────

  fn create_options() -> Options {
    Options {
      create_if_missing: true,
      ..Options::default()
    }
  }

  fn small_options() -> Options {
    // write_buffer_size small enough to flush after a handful of entries.
    Options {
      create_if_missing: true,
      write_buffer_size: 512,
      ..Options::default()
    }
  }

  #[test]
  fn flush_creates_ldb_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();

    // Write enough data to exceed 512 bytes and trigger a flush.
    for i in 0u32..20 {
      db.put(
        format!("key{i:04}").as_bytes(),
        format!("value{i:04}").as_bytes(),
      )
      .unwrap();
    }

    // At least one .ldb file must exist.
    let ldb_count = std::fs::read_dir(dir.path())
      .unwrap()
      .filter(|e| {
        e.as_ref()
          .unwrap()
          .file_name()
          .to_string_lossy()
          .ends_with(".ldb")
      })
      .count();
    assert!(
      ldb_count >= 1,
      "expected at least one .ldb file, found {ldb_count}"
    );
  }

  #[test]
  fn flushed_data_readable_from_l0() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();

    // Write enough to trigger one or more flushes.
    for i in 0u32..20 {
      db.put(
        format!("key{i:04}").as_bytes(),
        format!("val{i:04}").as_bytes(),
      )
      .unwrap();
    }

    // All keys must be readable regardless of whether they ended up in the
    // memtable or in an L0 file.
    for i in 0u32..20 {
      let expected = format!("val{i:04}");
      assert_eq!(
        db.get(format!("key{i:04}").as_bytes()).unwrap(),
        expected.as_bytes()
      );
    }
  }

  #[test]
  fn delete_tombstone_hides_flushed_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();

    // Write a key and force a flush so it ends up in L0.
    for i in 0u32..20 {
      db.put(format!("fk{i:04}").as_bytes(), b"v").unwrap();
    }
    // Delete the first key after the flush; the tombstone lives in mem.
    db.delete(b"fk0000").unwrap();

    // The tombstone in mem must shadow the value in L0.
    assert!(db.get(b"fk0000").unwrap_err().is_not_found());
    // Other keys are still readable.
    assert_eq!(db.get(b"fk0001").unwrap(), b"v");
  }

  #[test]
  fn flush_file_numbers_do_not_collide_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    // First session: flush some data.
    {
      let db = Db::open(dir.path(), small_options()).unwrap();
      for i in 0u32..20 {
        db.put(format!("k{i:04}").as_bytes(), b"v1").unwrap();
      }
    }
    let first_ldb_count = std::fs::read_dir(dir.path())
      .unwrap()
      .filter(|e| {
        e.as_ref()
          .unwrap()
          .file_name()
          .to_string_lossy()
          .ends_with(".ldb")
      })
      .count();

    // Second session: flush more data — must not overwrite the first session's files.
    {
      let db = Db::open(dir.path(), small_options()).unwrap();
      for i in 0u32..20 {
        db.put(format!("k{i:04}").as_bytes(), b"v2").unwrap();
      }
    }
    let second_ldb_count = std::fs::read_dir(dir.path())
      .unwrap()
      .filter(|e| {
        e.as_ref()
          .unwrap()
          .file_name()
          .to_string_lossy()
          .ends_with(".ldb")
      })
      .count();
    assert!(
      second_ldb_count >= first_ldb_count,
      "second session wrote fewer .ldb files ({second_ldb_count}) than first ({first_ldb_count})"
    );
  }

  #[test]
  fn wal_rotation_after_flush() {
    // After a flush, the old WAL (000001.log) must be deleted and a new one
    // must exist under the log number recorded in the MANIFEST.
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), small_options()).unwrap();
      // Trigger at least one flush.
      for i in 0u32..20 {
        db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
      }
    }

    // The original WAL (000001.log) should have been deleted.
    assert!(
      !dir.path().join("000001.log").exists(),
      "000001.log should be deleted after WAL rotation"
    );

    // A new .log file with a higher number must exist.
    let log_count = std::fs::read_dir(dir.path())
      .unwrap()
      .filter(|e| {
        e.as_ref()
          .unwrap()
          .file_name()
          .to_string_lossy()
          .ends_with(".log")
      })
      .count();
    assert!(
      log_count >= 1,
      "expected a new .log file after WAL rotation"
    );

    // Reopen must succeed and all keys must be readable.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    for i in 0u32..20 {
      assert_eq!(db.get(format!("k{i:04}").as_bytes()).unwrap(), b"v");
    }
  }

  #[test]
  fn open_reopen_reads_l0() {
    // Data that was flushed to an SSTable must be readable after a reopen that
    // uses MANIFEST-driven recovery (not WAL replay).
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), small_options()).unwrap();
      for i in 0u32..20 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
          .unwrap();
      }
      // Ensure at least one flush happened.
      let ldb_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
          e.as_ref()
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".ldb")
        })
        .count();
      assert!(ldb_count >= 1, "expected a flush to have occurred");
    }
    // Reopen: flushed keys come from the SSTable via VersionSet::recover;
    // unflushed keys come from WAL replay.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    for i in 0u32..20 {
      let expected = format!("v{i:04}");
      assert_eq!(
        db.get(format!("k{i:04}").as_bytes()).unwrap(),
        expected.as_bytes(),
        "key k{i:04} not found after reopen"
      );
    }
  }

  // ── new_iterator tests ─────────────────────────────────────────────────────

  #[test]
  fn iterator_in_memory_db() {
    use crate::ReadOptions;
    let db = Db::default();
    db.put(b"b", b"B").unwrap();
    db.put(b"a", b"A").unwrap();
    db.put(b"c", b"C").unwrap();

    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_first();
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while it.valid() {
      pairs.push((it.key().to_vec(), it.value().to_vec()));
      it.next();
    }
    assert_eq!(
      pairs,
      vec![
        (b"a".to_vec(), b"A".to_vec()),
        (b"b".to_vec(), b"B".to_vec()),
        (b"c".to_vec(), b"C".to_vec()),
      ]
    );
    assert!(it.status().is_none());
  }

  #[test]
  fn iterator_seek() {
    use crate::ReadOptions;
    let db = Db::default();
    db.put(b"aa", b"1").unwrap();
    db.put(b"bb", b"2").unwrap();
    db.put(b"cc", b"3").unwrap();

    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek(b"bb");
    assert!(it.valid());
    assert_eq!(it.key(), b"bb");
    assert_eq!(it.value(), b"2");
    it.next();
    assert_eq!(it.key(), b"cc");
    it.next();
    assert!(!it.valid());
  }

  #[test]
  fn iterator_skips_tombstones() {
    use crate::ReadOptions;
    let db = Db::default();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.delete(b"a").unwrap();

    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.key(), b"b");
    it.next();
    assert!(!it.valid());
  }

  #[test]
  fn iterator_spans_memtable_and_l0() {
    // Keys flushed to L0 and keys in the memtable must appear together in order.
    use crate::ReadOptions;
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();

    // Write enough to trigger one flush (keys go to L0).
    for i in 0u32..20 {
      db.put(format!("k{i:02}").as_bytes(), format!("v{i:02}").as_bytes())
        .unwrap();
    }

    // Collect all keys via the iterator.
    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_first();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(it.key().to_vec());
      it.next();
    }

    // All 20 keys must appear exactly once, in sorted order.
    assert_eq!(keys.len(), 20);
    for i in 0u32..20 {
      assert_eq!(keys[i as usize], format!("k{i:02}").as_bytes());
    }
  }

  #[test]
  fn iterator_after_reopen_reads_l0_and_mem() {
    // After a reopen: flushed keys come from L0 (MANIFEST recovery),
    // unflushed keys come from WAL replay into the memtable. The iterator
    // must see both.
    use crate::ReadOptions;
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), small_options()).unwrap();
      for i in 0u32..20 {
        db.put(format!("k{i:02}").as_bytes(), format!("v{i:02}").as_bytes())
          .unwrap();
      }
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_first();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(it.key().to_vec());
      it.next();
    }
    assert_eq!(keys.len(), 20);
  }

  // ── Backward iteration tests ───────────────────────────────────────────────

  #[test]
  fn iterator_seek_to_last_in_memory() {
    use crate::ReadOptions;
    let db = Db::default();
    db.put(b"a", b"1").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"b", b"2").unwrap();
    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), b"c");
    assert_eq!(it.value(), b"3");
  }

  #[test]
  fn iterator_prev_in_memory() {
    use crate::ReadOptions;
    let db = Db::default();
    for &k in &[b"a" as &[u8], b"b", b"c", b"d"] {
      db.put(k, k).unwrap();
    }
    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_last();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(it.key().to_vec());
      it.prev();
    }
    assert_eq!(
      keys,
      vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );
  }

  #[test]
  fn iterator_prev_skips_deleted_keys() {
    use crate::ReadOptions;
    let db = Db::default();
    db.put(b"a", b"a").unwrap();
    db.put(b"b", b"b").unwrap();
    db.put(b"c", b"c").unwrap();
    db.delete(b"b").unwrap();
    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_last();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(it.key().to_vec());
      it.prev();
    }
    assert_eq!(keys, vec![b"c".to_vec(), b"a".to_vec()]);
  }

  #[test]
  fn iterator_prev_spans_l0_and_memtable() {
    use crate::ReadOptions;
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();
    // Force a flush.
    for i in 0u32..20 {
      db.put(format!("k{i:02}").as_bytes(), b"v").unwrap();
    }
    // A few more keys in the current memtable.
    db.put(b"z1", b"v").unwrap();
    db.put(b"z2", b"v").unwrap();

    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), b"z2");

    // Collect all keys backward and verify they are in descending order.
    let mut prev_key: Vec<u8> = it.key().to_vec();
    it.prev();
    while it.valid() {
      assert!(it.key() < prev_key.as_slice());
      prev_key = it.key().to_vec();
      it.prev();
    }
  }

  // ── delete_obsolete_files tests ────────────────────────────────────────────

  /// Count files with the given extension in `dir`.
  fn count_files(dir: &std::path::Path, ext: &str) -> usize {
    std::fs::read_dir(dir)
      .unwrap()
      .filter(|e| {
        e.as_ref()
          .unwrap()
          .file_name()
          .to_string_lossy()
          .ends_with(ext)
      })
      .count()
  }

  #[test]
  fn obsolete_ldb_files_are_deleted() {
    // Multiple flushes create multiple .ldb files.  After each flush the old
    // .ldb files that are superseded by subsequent flushes remain live in the
    // current Version and must NOT be deleted.  Verify the count stays sane.
    //
    // Each flush produces one new .ldb file; with 3 flushes there should be
    // 3 .ldb files total — none deleted, because all are in the current Version.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();
    let mut flush_count = 0usize;
    let mut i = 0u32;
    while flush_count < 3 {
      db.put(format!("key{i:04}").as_bytes(), b"value").unwrap();
      i += 1;
      let new_count = count_files(dir.path(), ".ldb");
      if new_count > flush_count {
        flush_count = new_count;
      }
    }
    // Every flushed file is still live — none should be deleted.
    assert_eq!(count_files(dir.path(), ".ldb"), 3);
    // And data is still fully readable.
    for j in 0..i {
      assert!(db.get(format!("key{j:04}").as_bytes()).is_ok());
    }
  }

  #[test]
  fn obsolete_ldb_file_deleted_after_reopen_with_no_reference() {
    // Manually place an .ldb file with a number that is NOT in any Version,
    // then call Db::open.  The file should be deleted by delete_obsolete_files.
    //
    // We can't easily trigger deletion of an orphan .ldb during normal writes
    // (every flushed file lands in the current Version), so we simulate an
    // orphan by writing a dummy file before opening.
    let dir = tempfile::tempdir().unwrap();
    // First session: produce a real DB (which gets an SST at file 3).
    {
      let db = Db::open(dir.path(), small_options()).unwrap();
      for i in 0u32..20 {
        db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
      }
    }
    // Place a fake orphan .ldb file that no Version references.
    let orphan = dir.path().join("999999.ldb");
    std::fs::write(&orphan, b"garbage").unwrap();
    assert!(orphan.exists());

    // Reopen: delete_obsolete_files is NOT called on open (only on flush).
    // We trigger a flush to force GC.
    let db = Db::open(dir.path(), small_options()).unwrap();
    for i in 0u32..20 {
      db.put(format!("k{i:04}").as_bytes(), b"v2").unwrap();
    }
    // After a flush, the orphan must be gone.
    assert!(
      !orphan.exists(),
      "orphan .ldb file should have been deleted"
    );
  }

  #[test]
  fn current_and_manifest_always_kept() {
    // After flushes, CURRENT and the active MANIFEST must always be present.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();
    for i in 0u32..20 {
      db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
    }
    assert!(dir.path().join("CURRENT").exists());
    let current = std::fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    let manifest_name = current.trim();
    assert!(dir.path().join(manifest_name).exists());
  }

  // ── compaction tests ───────────────────────────────────────────────────────

  /// Options that produce many small flushes so L0 fills quickly.
  fn tiny_options() -> Options {
    Options {
      create_if_missing: true,
      write_buffer_size: 128,
      ..Options::default()
    }
  }

  #[test]
  fn compaction_reduces_l0_file_count() {
    // Write enough data to produce ≥ L0_COMPACTION_TRIGGER flushes, which
    // should trigger a compaction that moves files from L0 to L1.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();

    for i in 0u32..200 {
      db.put(
        format!("key{i:05}").as_bytes(),
        format!("val{i:05}").as_bytes(),
      )
      .unwrap();
    }

    // After enough writes (many flushes → multiple compactions), L0 should
    // be below the compaction trigger.
    let l0_count = {
      let g = db.state.lock().unwrap();
      g.version_set.as_ref().unwrap().current().files[0].len()
    };
    assert!(
      l0_count < crate::L0_COMPACTION_TRIGGER,
      "L0 should have been compacted; found {l0_count} files"
    );
  }

  #[test]
  fn compacted_data_readable() {
    // All keys written before compaction must still be readable after it.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();

    for i in 0u32..100 {
      db.put(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
        .unwrap();
    }

    // Force enough writes to guarantee multiple compactions have run.
    for i in 0u32..100 {
      let expected = format!("v{i:04}");
      assert_eq!(
        db.get(format!("k{i:04}").as_bytes()).unwrap(),
        expected.as_bytes(),
        "key k{i:04} not readable after compaction"
      );
    }
  }

  #[test]
  fn compaction_tombstone_hides_l1_value() {
    // Write a value for a key, flush it to L0/L1 via enough extra writes,
    // then write a tombstone.  The tombstone must shadow the compacted value.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();

    db.put(b"target", b"old-value").unwrap();
    // Write filler to flush "target" to disk.
    for i in 0u32..100 {
      db.put(format!("filler{i:05}").as_bytes(), b"x").unwrap();
    }
    // Now delete "target".
    db.delete(b"target").unwrap();
    // More filler so the tombstone also flushes.
    for i in 100u32..200 {
      db.put(format!("filler{i:05}").as_bytes(), b"x").unwrap();
    }

    assert!(
      db.get(b"target").unwrap_err().is_not_found(),
      "tombstone must shadow value in L1"
    );
  }

  #[test]
  fn compacted_data_readable_after_reopen() {
    // Data compacted to L1 must survive a reopen (MANIFEST recovery).
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), tiny_options()).unwrap();
      for i in 0u32..100 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
          .unwrap();
      }
    }
    // Reopen: data must come from L1 (via MANIFEST) not the WAL.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    for i in 0u32..100 {
      let expected = format!("v{i:04}");
      assert_eq!(
        db.get(format!("k{i:04}").as_bytes()).unwrap(),
        expected.as_bytes(),
        "key k{i:04} not found after reopen"
      );
    }
  }

  #[test]
  fn l1_files_present_after_compaction() {
    // After compaction, at least one L1 file must exist.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();
    for i in 0u32..200 {
      db.put(format!("k{i:05}").as_bytes(), b"v").unwrap();
    }
    let l1_count = {
      let g = db.state.lock().unwrap();
      g.version_set.as_ref().unwrap().current().files[1].len()
    };
    assert!(
      l1_count >= 1,
      "expected at least one L1 file after compaction"
    );
  }

  // ── compact_range tests ────────────────────────────────────────────────────

  #[test]
  fn compact_range_all_keys_readable_after() {
    // compact_range over the full key space must not lose any data.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();
    for i in 0u32..100 {
      db.put(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
        .unwrap();
    }
    db.compact_range(None, None).unwrap();
    for i in 0u32..100 {
      assert_eq!(
        db.get(format!("k{i:04}").as_bytes()).unwrap(),
        format!("v{i:04}").as_bytes(),
        "key k{i:04} missing after compact_range"
      );
    }
  }

  #[test]
  fn compact_range_subrange_preserves_keys_outside() {
    // Compacting a sub-range must not disturb keys outside it.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();
    for i in 0u32..100 {
      db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
    }
    // Compact only the middle third.
    db.compact_range(Some(b"k0033"), Some(b"k0066")).unwrap();
    // All keys must still be readable.
    for i in 0u32..100 {
      assert!(
        db.get(format!("k{i:04}").as_bytes()).is_ok(),
        "key k{i:04} missing after partial compact_range"
      );
    }
  }

  #[test]
  fn compact_range_moves_data_to_deeper_level() {
    // After a full compact_range, all data should be below L0.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();
    for i in 0u32..200 {
      db.put(format!("k{i:05}").as_bytes(), b"v").unwrap();
    }
    db.compact_range(None, None).unwrap();
    let l0_count = {
      let g = db.state.lock().unwrap();
      g.version_set.as_ref().unwrap().current().files[0].len()
    };
    assert_eq!(l0_count, 0, "L0 should be empty after full compact_range");
  }

  #[test]
  fn compact_range_tombstone_elided_with_no_data_below() {
    // After compact_range pushes a tombstone to the deepest level, the key
    // must still be gone (tombstone correctly shadows the value).
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();
    db.put(b"gone", b"value").unwrap();
    for i in 0u32..100 {
      db.put(format!("filler{i:05}").as_bytes(), b"x").unwrap();
    }
    db.delete(b"gone").unwrap();
    for i in 100u32..200 {
      db.put(format!("filler{i:05}").as_bytes(), b"x").unwrap();
    }
    db.compact_range(None, None).unwrap();
    assert!(
      db.get(b"gone").unwrap_err().is_not_found(),
      "deleted key must not reappear after compact_range"
    );
  }

  #[test]
  fn compact_range_noop_on_inmemory_db() {
    let db = Db::default();
    db.put(b"k", b"v").unwrap();
    db.compact_range(None, None).unwrap(); // must not panic
    assert_eq!(db.get(b"k").unwrap(), b"v");
  }

  #[test]
  fn compact_range_data_readable_after_reopen() {
    // Data compacted via compact_range must survive a reopen.
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), tiny_options()).unwrap();
      for i in 0u32..100 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
          .unwrap();
      }
      db.compact_range(None, None).unwrap();
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    for i in 0u32..100 {
      assert_eq!(
        db.get(format!("k{i:04}").as_bytes()).unwrap(),
        format!("v{i:04}").as_bytes()
      );
    }
  }

  // ── paranoid_checks / verify_checksums tests ───────────────────────────────

  #[test]
  fn paranoid_checks_does_not_break_normal_operation() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
      create_if_missing: true,
      paranoid_checks: true,
      ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    db.put(b"hello", b"world").unwrap();
    assert_eq!(db.get(b"hello").unwrap(), b"world");
  }

  #[test]
  fn paranoid_checks_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), create_options()).unwrap();
      for i in 0..200u32 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i:04}").as_bytes())
          .unwrap();
      }
    }
    // Reopen with paranoid_checks — recovery and reads must both succeed.
    let opts = Options {
      paranoid_checks: true,
      ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.get(b"k0000").unwrap(), b"v0000");
    let mut it = db
      .new_iterator(&ReadOptions {
        verify_checksums: true,
        ..ReadOptions::default()
      })
      .unwrap();
    it.seek_to_first();
    assert!(it.valid());
  }

  #[test]
  fn corrupted_block_detected_with_verify_checksums() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), small_options()).unwrap();
      // Write enough to force at least one .ldb flush (small_options write_buffer_size = 512).
      for i in 0..200u32 {
        db.put(format!("k{i:04}").as_bytes(), b"v").unwrap();
      }
    } // db dropped — flushes and releases lock

    // Corrupt the middle of the first .ldb file.
    let ldb: Vec<_> = std::fs::read_dir(dir.path())
      .unwrap()
      .filter_map(|e| {
        let e = e.unwrap();
        let n = e.file_name().to_string_lossy().into_owned();
        if n.ends_with(".ldb") {
          Some(e.path())
        } else {
          None
        }
      })
      .collect();
    assert!(!ldb.is_empty(), "expected at least one .ldb file");
    let mut data = std::fs::read(&ldb[0]).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xff;
    std::fs::write(&ldb[0], &data).unwrap();

    // Reopen without paranoid — get may or may not fail depending on which block is hit.
    // Reopen with verify_checksums — must detect the corruption.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let read_opts = ReadOptions {
      verify_checksums: true,
      ..ReadOptions::default()
    };
    // At least one key should produce a Corruption error when we scan everything.
    let mut it = db.new_iterator(&read_opts).unwrap();
    it.seek_to_first();
    let mut saw_corruption = false;
    while it.valid() {
      it.next();
    }
    if let Some(e) = it.status() {
      if matches!(e, Error::Corruption(_)) {
        saw_corruption = true;
      }
    }
    // Also try a direct get into a corrupted block.
    for i in 0..200u32 {
      if let Err(Error::Corruption(_)) =
        db.get_with_options(&read_opts, format!("k{i:04}").as_bytes())
      {
        saw_corruption = true;
        break;
      }
    }
    assert!(
      saw_corruption,
      "expected at least one Corruption error with verify_checksums=true"
    );
  }

  // ── Snapshot tests ─────────────────────────────────────────────────────────

  #[test]
  fn snapshot_pins_sequence_number() {
    let db = Db::default();
    db.put(b"k", b"v1").unwrap();
    let snap = db.get_snapshot();
    db.put(b"k", b"v2").unwrap();

    // Without a snapshot we see the latest value.
    assert_eq!(db.get(b"k").unwrap(), b"v2");

    // With the snapshot we see the value as of when it was taken.
    let opts = ReadOptions {
      snapshot: Some(&snap),
      ..ReadOptions::default()
    };
    assert_eq!(db.get_with_options(&opts, b"k").unwrap(), b"v1");

    drop(snap);
    // After release, reads still see the latest value.
    assert_eq!(db.get(b"k").unwrap(), b"v2");
  }

  #[test]
  fn snapshot_sees_key_not_yet_deleted() {
    let db = Db::default();
    db.put(b"x", b"alive").unwrap();
    let snap = db.get_snapshot();
    db.delete(b"x").unwrap();

    // Snapshot was taken before the deletion — key should be visible.
    let opts = ReadOptions {
      snapshot: Some(&snap),
      ..ReadOptions::default()
    };
    assert_eq!(db.get_with_options(&opts, b"x").unwrap(), b"alive");

    // Current view sees the deletion.
    assert!(db.get(b"x").unwrap_err().is_not_found());
  }

  #[test]
  fn snapshot_iterator_sees_point_in_time() {
    let db = Db::default();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    let snap = db.get_snapshot();
    db.put(b"c", b"3").unwrap();
    db.delete(b"a").unwrap();

    let opts = ReadOptions {
      snapshot: Some(&snap),
      ..ReadOptions::default()
    };
    let mut it = db.new_iterator(&opts).unwrap();
    it.seek_to_first();

    // Should see a=1, b=2 but not c=3 (added after snap) and not a=deleted.
    assert!(it.valid());
    assert_eq!(it.key(), b"a");
    assert_eq!(it.value(), b"1");
    it.next();
    assert!(it.valid());
    assert_eq!(it.key(), b"b");
    assert_eq!(it.value(), b"2");
    it.next();
    assert!(!it.valid());
  }

  #[test]
  fn multiple_snapshots_independent() {
    let db = Db::default();
    db.put(b"k", b"v1").unwrap();
    let snap1 = db.get_snapshot();
    db.put(b"k", b"v2").unwrap();
    let snap2 = db.get_snapshot();
    db.put(b"k", b"v3").unwrap();

    let opts1 = ReadOptions {
      snapshot: Some(&snap1),
      ..ReadOptions::default()
    };
    let opts2 = ReadOptions {
      snapshot: Some(&snap2),
      ..ReadOptions::default()
    };
    assert_eq!(db.get_with_options(&opts1, b"k").unwrap(), b"v1");
    assert_eq!(db.get_with_options(&opts2, b"k").unwrap(), b"v2");
    assert_eq!(db.get(b"k").unwrap(), b"v3");
  }

  // ── Bloom filter tests ───────────────────────────────────────────────────────

  fn bloom_options() -> Options {
    Options {
      create_if_missing: true,
      write_buffer_size: 512,
      filter_policy: Some(std::sync::Arc::new(crate::BloomFilterPolicy::new(10))),
      ..Options::default()
    }
  }

  #[test]
  fn bloom_filter_inserted_key_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), bloom_options()).unwrap();
    for i in 0u32..50 {
      db.put(format!("key{i:04}").as_bytes(), b"val").unwrap();
    }
    // All keys must be readable after flush.
    for i in 0u32..50 {
      assert_eq!(db.get(format!("key{i:04}").as_bytes()).unwrap(), b"val");
    }
  }

  #[test]
  fn bloom_filter_absent_key_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), bloom_options()).unwrap();
    for i in 0u32..50 {
      db.put(format!("key{i:04}").as_bytes(), b"val").unwrap();
    }
    // Absent keys must return NotFound (the filter must not cause false negatives).
    assert!(db.get(b"absent").is_err());
    assert!(db.get(b"nope").is_err());
  }

  #[test]
  fn bloom_filter_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), bloom_options()).unwrap();
      for i in 0u32..50 {
        db.put(format!("key{i:04}").as_bytes(), b"val").unwrap();
      }
    }
    // Reopen with the same filter policy — filter blocks should be read back.
    let db2 = Db::open(dir.path(), bloom_options()).unwrap();
    for i in 0u32..50 {
      assert_eq!(db2.get(format!("key{i:04}").as_bytes()).unwrap(), b"val");
    }
    assert!(db2.get(b"absent").is_err());
  }

  #[test]
  fn bloom_filter_reopen_without_policy_still_works() {
    // An SSTable written *with* a filter policy must still be readable when
    // reopened *without* one (the filter is simply ignored).
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), bloom_options()).unwrap();
      for i in 0u32..50 {
        db.put(format!("key{i:04}").as_bytes(), b"val").unwrap();
      }
    }
    let opts_no_filter = Options {
      create_if_missing: false,
      ..Options::default()
    };
    let db2 = Db::open(dir.path(), opts_no_filter).unwrap();
    for i in 0u32..50 {
      assert_eq!(db2.get(format!("key{i:04}").as_bytes()).unwrap(), b"val");
    }
  }

  // ── get_property ─────────────────────────────────────────────────────────

  /// Helper: open a DB, write enough data to trigger at least one L0 flush,
  /// and return the open database.
  fn db_with_l0_files() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
      create_if_missing: true,
      write_buffer_size: 4096, // tiny buffer → fast flush
      ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    // Write enough to trigger multiple flushes (200-byte values × 100 keys >> 4 KiB buffer).
    let val = vec![b'x'; 200];
    for i in 0u32..100 {
      db.put(format!("key{i:04}").as_bytes(), &val).unwrap();
    }
    (dir, db)
  }

  #[test]
  fn get_property_unknown_returns_none() {
    let (_dir, db) = db_with_l0_files();
    assert!(db.get_property("leveldb.unknown").is_none());
    assert!(db.get_property("unknown").is_none());
    assert!(db.get_property("leveldb.num-files-at-level99").is_none());
  }

  #[test]
  fn get_property_num_files_at_level() {
    let (_dir, db) = db_with_l0_files();
    // At least one file must exist across all levels (data may have compacted to L1).
    let total: usize = (0..7)
      .map(|l| {
        db.get_property(&format!("leveldb.num-files-at-level{l}"))
          .unwrap()
          .parse::<usize>()
          .unwrap()
      })
      .sum();
    assert!(
      total >= 1,
      "expected ≥1 file across all levels, got {total}"
    );
    // Level 6 should be empty given the small dataset.
    let l6: usize = db
      .get_property("leveldb.num-files-at-level6")
      .unwrap()
      .parse()
      .unwrap();
    assert_eq!(l6, 0);
  }

  #[test]
  fn get_property_approximate_memory_usage() {
    let (_dir, db) = db_with_l0_files();
    let usage: usize = db
      .get_property("leveldb.approximate-memory-usage")
      .unwrap()
      .parse()
      .unwrap();
    // Should be nonzero (at minimum, the current memtable uses some memory).
    assert!(usage > 0, "expected nonzero memory usage");
  }

  #[test]
  fn get_property_stats_contains_header() {
    let (_dir, db) = db_with_l0_files();
    let stats = db.get_property("leveldb.stats").unwrap();
    assert!(stats.contains("Level"), "stats missing header: {stats}");
    assert!(
      stats.contains("Files"),
      "stats missing Files column: {stats}"
    );
  }

  #[test]
  fn get_property_sstables_lists_files() {
    let (_dir, db) = db_with_l0_files();
    let tables = db.get_property("leveldb.sstables").unwrap();
    // All seven level headers must appear.
    for level in 0..7 {
      assert!(
        tables.contains(&format!("--- level {level} ---")),
        "sstables missing level {level} header: {tables}"
      );
    }
    // At least one file entry (number:size[...]) should appear.
    assert!(
      tables.contains('['),
      "sstables missing file entries: {tables}"
    );
  }

  #[test]
  fn get_property_in_memory_db_returns_sensible_values() {
    let db = Db::default();
    db.put(b"k", b"v").unwrap();
    // In-memory DB has no VersionSet, so file counts are all 0.
    let count: usize = db
      .get_property("leveldb.num-files-at-level0")
      .unwrap()
      .parse()
      .unwrap();
    assert_eq!(count, 0);
    // But memory usage should reflect the memtable contents.
    let usage: usize = db
      .get_property("leveldb.approximate-memory-usage")
      .unwrap()
      .parse()
      .unwrap();
    assert!(usage > 0);
  }

  // ── get_approximate_sizes ─────────────────────────────────────────────────

  #[test]
  fn get_approximate_sizes_empty_ranges() {
    let (_dir, db) = db_with_l0_files();
    let sizes = db.get_approximate_sizes(&[]);
    assert!(sizes.is_empty());
  }

  #[test]
  fn get_approximate_sizes_in_memory_db_returns_zeros() {
    let db = Db::default();
    db.put(b"a", b"v").unwrap();
    let sizes = db.get_approximate_sizes(&[(b"a".as_ref(), b"z".as_ref())]);
    assert_eq!(sizes, vec![0]);
  }

  #[test]
  fn get_approximate_sizes_wide_range_covers_data() {
    let (_dir, db) = db_with_l0_files();
    // A range that spans all written keys should return a non-zero size.
    let sizes = db.get_approximate_sizes(&[(b"\x00".as_ref(), b"\xff".as_ref())]);
    assert_eq!(sizes.len(), 1);
    assert!(
      sizes[0] > 0,
      "expected non-zero size for wide range, got {}",
      sizes[0]
    );
  }

  #[test]
  fn get_approximate_sizes_multiple_ranges() {
    let (_dir, db) = db_with_l0_files();
    let sizes = db.get_approximate_sizes(&[
      (b"\x00".as_ref(), b"\x80".as_ref()),
      (b"\x80".as_ref(), b"\xff".as_ref()),
    ]);
    assert_eq!(sizes.len(), 2);
    // Both halves may be 0 if all keys land in one half, but their sum
    // should equal the total across the full range (approximately).
    let total_sizes = db.get_approximate_sizes(&[(b"\x00".as_ref(), b"\xff".as_ref())]);
    // Individual sizes should be <= total (saturating arithmetic).
    assert!(sizes[0] + sizes[1] <= total_sizes[0] + 4096 * 8);
  }

  #[test]
  fn get_approximate_sizes_empty_range_returns_zero() {
    let (_dir, db) = db_with_l0_files();
    // start == limit → empty range; approximate_offset_of returns the same value.
    let sizes = db.get_approximate_sizes(&[(b"key".as_ref(), b"key".as_ref())]);
    assert_eq!(sizes, vec![0]);
  }
}
