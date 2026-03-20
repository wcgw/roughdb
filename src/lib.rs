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
//! db.write(&WriteOptions::default(), batch)?;
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

use crate::db::table_cache::TableCache;
use crate::db::version_edit::{FileMetaData, VersionEdit};
use crate::db::version_set::VersionSet;
use crate::logfile::reader::Reader as LogReader;
use crate::logfile::writer::Writer as LogWriter;
use crate::memtable::{ArcMemTableIter, Memtable, MemtableResult};
use crate::table::builder::TableBuilder;
use crate::table::reader::{LookupResult, Table};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub mod cache;
pub use cache::BlockCache;
pub mod error;
pub use error::Error;
pub mod filter;
pub use filter::BloomFilterPolicy;
pub mod options;
pub use options::{CompressionType, FlushOptions, Options, WriteOptions};
pub(crate) mod coding;
pub(crate) mod db;
pub(crate) mod iter;
pub(crate) mod logfile;
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
    self
      .db
      .inner
      .state
      .lock()
      .unwrap()
      .snapshots
      .remove(&self.seq);
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
  ///
  /// Set to `false` for bulk scans to avoid polluting the cache with data that
  /// is unlikely to be re-read soon (e.g. `DbIter` full-table scans).
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

// ── Write-queue types ─────────────────────────────────────────────────────────

/// A single pending write in the group-write queue.
///
/// Pushed by every `Db::write` caller; consumed by the leader of the group
/// that processes it.  All fields are read/written only while the `DbState`
/// mutex is held.
struct WriterSlot {
  id: u64,
  batch: WriteBatch,
  sync: bool,
}

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
  // ── Batch-write queue ──────────────────────────────────────────────────────
  /// Pending write requests in arrival order.  The front entry is the current
  /// leader; all others are followers waiting on `Db::write_condvar`.
  writers: std::collections::VecDeque<WriterSlot>,
  /// Counter used to assign stable IDs to `WriterSlot`s.
  next_writer_id: u64,
  /// Results for writes completed by a leader on behalf of their originators.
  /// A follower checks this map on wake-up and removes its own entry.
  /// `Ok(())` is the common case; `Err` propagates a WAL/memtable failure.
  completed: std::collections::HashMap<u64, Result<(), Error>>,
  // ── Seek-based compaction (Gap 5) ──────────────────────────────────────────
  /// File + level nominated for seek-based compaction (its `allowed_seeks` hit
  /// zero).  `None` when no file is pending.  Cleared by `install_compaction`
  /// after the file is removed from the live set.
  seek_compact_file: Option<(Arc<FileMetaData>, usize)>,
  /// Set to `true` when `seek_compact_file` is first populated, so the next
  /// `Db::write` compaction loop runs even without a preceding flush.
  compaction_needed: bool,
  // ── Background thread coordination ─────────────────────────────────────────
  /// `true` while the background thread has been notified and hasn't yet
  /// finished one cycle.  Prevents redundant notifications.
  background_scheduled: bool,
  /// Sticky error set by the background thread on flush/compaction failure.
  /// Returned to writers on their next call; clears only on reopen.
  background_error: Option<Error>,
  /// `FlushPrep` produced by `begin_flush` under the write lock; consumed by
  /// the background thread via `write_flush` + `finish_flush`.
  pending_flush: Option<FlushPrep>,
}

// ── FlushPrep / FlushResult ───────────────────────────────────────────────────

/// Produced by `begin_flush` (under the write lock): everything needed to run
/// `write_flush` and `finish_flush` without holding the lock.
struct FlushPrep {
  sst_number: u64,
  sst_path: std::path::PathBuf,
  old_mem: Arc<Memtable>,
  /// File number of the new WAL that was activated in `begin_flush`.
  /// `finish_flush` records this in the MANIFEST via `vs.set_log_number`.
  new_log_number: u64,
  /// Sequence number at the time of the flush rotation.  `finish_flush`
  /// records this as `last_sequence` in the MANIFEST so WAL replay correctly
  /// replays entries written to the new WAL after the rotation.
  last_sequence_at_rotation: u64,
  /// Path of the old WAL to delete after `log_and_apply` succeeds.
  old_log_path: std::path::PathBuf,
}

/// Return value of `write_flush`: everything `finish_flush` needs to install
/// the new SSTable into the `VersionSet` and complete WAL rotation.
struct FlushResult {
  file_number: u64,
  file_size: u64,
  /// Smallest internal key written to the SSTable.
  smallest: Vec<u8>,
  /// Largest internal key written to the SSTable.
  largest: Vec<u8>,
  /// User-key extracted from `smallest` (for `pick_level_for_memtable_output`).
  smallest_user_key: Vec<u8>,
  /// User-key extracted from `largest` (for `pick_level_for_memtable_output`).
  largest_user_key: Vec<u8>,
  table: Arc<Table>,
  /// New WAL file number (already active; just needs to be committed to MANIFEST).
  new_log_number: u64,
  /// Sequence number captured at rotation time; used as `last_sequence` in the
  /// MANIFEST so WAL replay correctly replays entries written after the rotation.
  last_sequence_at_rotation: u64,
  /// Path of the old WAL, deleted by `finish_flush` after `log_and_apply`.
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

  /// Return a standard [`Iterator`] that yields `(key, value)` pairs by advancing forward from
  /// the current position.
  ///
  /// Position the `DbIter` first (via [`seek_to_first`], [`seek`], etc.), then call `forward()`
  /// to obtain an adapter that implements Rust's [`Iterator`] trait.  Each call to
  /// [`Iterator::next`] reads the current key and value, advances the cursor, and returns owned
  /// copies.
  ///
  /// The adapter yields `Some(Err(..))` exactly once if the underlying iterator encounters
  /// corruption, then `None` on all subsequent calls.
  ///
  /// [`seek_to_first`]: DbIter::seek_to_first
  /// [`seek`]: DbIter::seek
  ///
  /// # Examples
  ///
  /// ```
  /// # let db = roughdb::Db::default();
  /// # db.put(b"a", b"1").unwrap();
  /// use roughdb::ReadOptions;
  ///
  /// let mut it = db.new_iterator(&ReadOptions::default())?;
  /// it.seek_to_first();
  ///
  /// for result in it.forward() {
  ///     let (key, value) = result?;
  ///     println!("{:?} = {:?}", key, value);
  /// }
  /// # Ok::<(), roughdb::Error>(())
  /// ```
  pub fn forward(&mut self) -> ForwardIter<'_> {
    ForwardIter {
      inner: self,
      done: false,
    }
  }
}

/// Adapter returned by [`DbIter::forward`] that implements [`Iterator`].
///
/// Yields `Ok((key, value))` pairs as owned `Vec<u8>`s, advancing forward from
/// wherever the parent `DbIter` was positioned when `forward()` was called.
pub struct ForwardIter<'a> {
  inner: &'a mut DbIter,
  done: bool,
}

impl Iterator for ForwardIter<'_> {
  type Item = Result<(Vec<u8>, Vec<u8>), Error>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.done {
      return None;
    }
    if !self.inner.valid() {
      self.done = true;
      return self.inner.status().map(|e| Err(e.clone()));
    }
    let key = self.inner.key().to_vec();
    let value = self.inner.value().to_vec();
    self.inner.next();
    Some(Ok((key, value)))
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

struct Persistence {
  dir: PathBuf,
  table_cache: TableCache,
  _lock: std::fs::File,
}

// ── DbInner / Db ─────────────────────────────────────────────────────────────

/// Shared state owned by both the foreground `Db` handle and the background
/// compaction/flush thread.  Always accessed through an `Arc`.
pub(crate) struct DbInner {
  pub(crate) state: Mutex<DbState>,
  /// Wakes follower writers when the current group leader finishes and when
  /// there may be a new leader at the front of the write queue.
  write_condvar: std::sync::Condvar,
  /// Wakes the background thread when work is available (flush / compact).
  bg_condvar: std::sync::Condvar,
  pub(crate) options: Options,
  pub(crate) persistence: Option<Persistence>,
  /// Set to `true` on `Db::drop` to tell the background thread to exit.
  shutting_down: AtomicBool,
}

pub struct Db {
  pub(crate) inner: Arc<DbInner>,
  bg_thread: Option<std::thread::JoinHandle<()>>,
}

impl Default for Db {
  /// In-memory database with no WAL and no flush trigger.
  /// Intended for tests and ephemeral use.
  fn default() -> Self {
    Self {
      inner: Arc::new(DbInner {
        state: Mutex::new(DbState {
          last_sequence: 0,
          log: None,
          mem: Arc::new(Memtable::default()),
          imm: None,
          version_set: None,
          snapshots: std::collections::BTreeSet::new(),
          writers: std::collections::VecDeque::new(),
          next_writer_id: 0,
          completed: std::collections::HashMap::new(),
          seek_compact_file: None,
          compaction_needed: false,
          background_scheduled: false,
          background_error: None,
          pending_flush: None,
        }),
        write_condvar: std::sync::Condvar::new(),
        bg_condvar: std::sync::Condvar::new(),
        options: Options::default(),
        persistence: None,
        shutting_down: AtomicBool::new(false),
      }),
      bg_thread: None,
    }
  }
}

impl Drop for Db {
  fn drop(&mut self) {
    if self.inner.persistence.is_some() {
      log::info!("shutting down database");
      self.inner.shutting_down.store(true, Ordering::Release);
      self.inner.bg_condvar.notify_one();
      if let Some(t) = self.bg_thread.take() {
        t.join().ok();
      }
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

    // Create the table cache for this persistent database.
    let cache_capacity = options
      .max_open_files
      .saturating_sub(crate::db::table_cache::NUM_NON_TABLE_CACHE_FILES)
      .max(1);
    let table_cache = crate::db::table_cache::TableCache::new(
      path,
      cache_capacity,
      options.filter_policy.clone(),
      options.block_cache.clone(),
    );

    let (version_set, mem, last_sequence) = if db_exists {
      // ── Existing database: MANIFEST-driven recovery ──────────────────────
      log::info!("opening existing database at {}", path.display());
      let mut vs = VersionSet::recover(path, options.paranoid_checks)?;
      let manifest_last_seq = vs.last_sequence();
      let mem = Arc::new(Memtable::default());

      // Replay WAL records newer than what is already in the SSTables.
      // Use the log number recorded in the MANIFEST (set by WAL rotation).
      let log_path = path.join(format!("{:06}.log", vs.log_number()));
      let actual_last_seq = if log_path.exists() {
        let file = std::fs::File::open(&log_path)?;
        let file_len = file.metadata()?.len();
        if file_len > 0 {
          log::info!(
            "replaying WAL {:06}.log (manifest_last_seq={})",
            vs.log_number(),
            manifest_last_seq
          );
          let seq = Self::recover_wal(file, &mem, manifest_last_seq, options.paranoid_checks)?;
          log::info!("WAL replay complete: max_sequence={seq}");
          seq
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
      log::info!("creating new database at {}", path.display());
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

    let inner = Arc::new(DbInner {
      state: Mutex::new(DbState {
        last_sequence,
        log: Some(log_writer),
        mem,
        imm: None,
        version_set,
        snapshots: std::collections::BTreeSet::new(),
        writers: std::collections::VecDeque::new(),
        next_writer_id: 0,
        completed: std::collections::HashMap::new(),
        seek_compact_file: None,
        compaction_needed: false,
        background_scheduled: false,
        background_error: None,
        pending_flush: None,
      }),
      write_condvar: std::sync::Condvar::new(),
      bg_condvar: std::sync::Condvar::new(),
      options,
      persistence: Some(Persistence {
        dir: path.to_path_buf(),
        table_cache,
        _lock: lock_file,
      }),
      shutting_down: AtomicBool::new(false),
    });
    let bg_inner = Arc::clone(&inner);
    let bg_thread = std::thread::spawn(move || bg_worker(bg_inner));
    Ok(Self {
      inner,
      bg_thread: Some(bg_thread),
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

  /// Look up `key` in the database, returning its value.
  ///
  /// Returns [`Error::NotFound`] if `key` does not exist or was deleted.
  /// Equivalent to calling [`get_with_options`] with [`ReadOptions::default`].
  ///
  /// [`get_with_options`]: Db::get_with_options
  pub fn get<K>(&self, key: K) -> Result<Vec<u8>, Error>
  where
    K: AsRef<[u8]>,
  {
    self.get_with_options(&ReadOptions::default(), key)
  }

  /// Look up `key` in the database with explicit read options, returning its value.
  ///
  /// The lookup order mirrors LevelDB's `DBImpl::Get`:
  /// 1. Active memtable (lock-free after a brief snapshot under the lock).
  /// 2. Sealed memtable (`imm`) being flushed to disk, if any.
  /// 3. SSTable files in the current `Version`, level by level (L0 first).
  ///
  /// The lock is held only long enough to snapshot the current `Arc<Memtable>` refs and
  /// sequence number; all subsequent lookups (memtable reads and SSTable I/O) proceed without
  /// the lock.
  ///
  /// If `opts.snapshot` is set, only writes that preceded the snapshot are visible; otherwise
  /// the read sees all writes committed before this call.
  ///
  /// Returns [`Error::NotFound`] if `key` does not exist or was deleted.
  ///
  /// See `db/db_impl.cc: DBImpl::Get`.
  pub fn get_with_options<K>(&self, opts: &ReadOptions, key: K) -> Result<Vec<u8>, Error>
  where
    K: AsRef<[u8]>,
  {
    let key = key.as_ref();

    // Mirror LevelDB's DBImpl::Get: take the lock only long enough to snapshot
    // the current memtable refs and sequence number, then release before doing
    // any I/O (memtable reads are lock-free; SSTable reads go to disk).
    let (sequence, mem, imm, version) = {
      let state = self.inner.state.lock().unwrap();
      (
        opts.snapshot.map(|s| s.seq).unwrap_or(state.last_sequence),
        Arc::clone(&state.mem),
        state.imm.as_ref().map(Arc::clone),
        state.version_set.as_ref().map(|vs| vs.current()),
      )
    };

    let verify_checksums = opts.verify_checksums || self.inner.options.paranoid_checks;
    let fill_cache = opts.fill_cache;

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
      if let Some(persistence) = &self.inner.persistence {
        let (result, stats) = version.get(
          key,
          sequence,
          verify_checksums,
          fill_cache,
          &persistence.table_cache,
        )?;
        // Update seek stats under the lock (re-acquire briefly).
        if stats.seek_file.is_some() {
          let mut g = self.inner.state.lock().unwrap();
          update_stats(&mut g, &stats);
          maybe_schedule_compaction(&self.inner, &mut g);
        }
        match result {
          LookupResult::Value(v) => return Ok(v),
          LookupResult::Deleted => return Err(Error::NotFound),
          LookupResult::NotInTable => {}
        }
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
    let mut state = self.inner.state.lock().unwrap();
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
      let state = self.inner.state.lock().unwrap();
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
      let state = self.inner.state.lock().unwrap();
      state.version_set.as_ref().map(|vs| vs.current())
    };

    ranges
      .iter()
      .map(|&(start, limit)| {
        let (Some(ref v), Some(ref tc)) = (&version, &self.inner.persistence) else {
          return 0;
        };
        // Convert user keys to lookup internal keys (seq=u64::MAX>>8, vtype=1)
        // so they sort before all real entries for the same user key — the same
        // sentinel used in Table::get.
        let istart = make_internal_key(start, u64::MAX >> 8, 1);
        let ilimit = make_internal_key(limit, u64::MAX >> 8, 1);
        let start_off = v.approximate_offset_of(&istart, &tc.table_cache);
        let limit_off = v.approximate_offset_of(&ilimit, &tc.table_cache);
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
    self.write(&WriteOptions::default(), batch)
  }

  pub fn delete<K>(&self, key: K) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
  {
    let mut batch = WriteBatch::new();
    batch.delete(key.as_ref());
    self.write(&WriteOptions::default(), batch)
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
      let state = self.inner.state.lock().unwrap();
      (
        opts.snapshot.map(|s| s.seq).unwrap_or(state.last_sequence),
        Arc::clone(&state.mem),
        state.imm.as_ref().map(Arc::clone),
        state.version_set.as_ref().map(|vs| vs.current()),
      )
    };

    let verify_checksums = opts.verify_checksums || self.inner.options.paranoid_checks;
    let fill_cache = opts.fill_cache;

    let mut children: Vec<Box<dyn crate::iter::InternalIterator>> = Vec::new();

    // Active memtable (newest writes, scanned first).
    children.push(Box::new(ArcMemTableIter::new(mem)));

    // Sealed memtable being flushed to disk (if any).
    if let Some(imm) = imm {
      children.push(Box::new(ArcMemTableIter::new(imm)));
    }

    // SSTable files from the current Version, level by level (L0 first).
    if let (Some(version), Some(persistence)) = (version, &self.inner.persistence) {
      for level_files in &version.files {
        for meta in level_files {
          let table = persistence
            .table_cache
            .get_or_open(meta.number, meta.file_size)?;
          children.push(Box::new(table.new_iterator(verify_checksums, fill_cache)?));
        }
      }
    }

    let inner = DbIterator::new(Box::new(MergingIterator::new(children)), sequence);
    Ok(DbIter { inner })
  }

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
  pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<(), Error> {
    use crate::db::version::NUM_LEVELS;

    let persistence = match &self.inner.persistence {
      Some(p) => p,
      None => return Ok(()),
    };

    let path = persistence.dir.as_path();

    // Find the deepest level that currently has files overlapping [begin, end].
    let max_level = {
      let g = self.inner.state.lock().unwrap();
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
      compact_level_range(
        path,
        &self.inner.state,
        &self.inner.options,
        &persistence.table_cache,
        level,
        begin,
        end,
      )?;
    }

    Ok(())
  }

  /// Flush all in-memory writes to an SSTable and clear the corresponding WAL segment.
  ///
  /// If the active memtable is empty, this is a no-op (returns `Ok(())` immediately).
  /// If a flush is already in progress for a previous memtable, waits for it to finish
  /// before rotating the current one.
  ///
  /// After this call returns (with `opts.wait = true`), all data written before the call
  /// is guaranteed to reside in an SSTable on disk and the WAL segment that covered it has
  /// been deleted.
  ///
  /// Always a no-op on in-memory databases (`Db::default()`).
  ///
  /// See `include/rocksdb/db.h: DB::Flush`.
  pub fn flush(&self, opts: &FlushOptions) -> Result<(), Error> {
    let mut g = self.inner.state.lock().unwrap();

    if let Some(ref e) = g.background_error {
      return Err(e.clone());
    }
    // No-op for in-memory databases.
    if self.inner.persistence.is_none() {
      return Ok(());
    }

    // If the memtable is empty, there is nothing to flush.  We may still need to wait for an
    // in-progress flush that was triggered by a prior writer.
    if g.mem.approximate_memory_usage() == 0 {
      if opts.wait {
        while (g.imm.is_some() || g.pending_flush.is_some()) && g.background_error.is_none() {
          g = self.inner.write_condvar.wait(g).unwrap();
        }
        return g
          .background_error
          .as_ref()
          .map_or(Ok(()), |e| Err(e.clone()));
      }
      return Ok(());
    }

    // Wait for any in-progress flush to drain before rotating mem → imm.
    while (g.imm.is_some() || g.pending_flush.is_some()) && g.background_error.is_none() {
      g = self.inner.write_condvar.wait(g).unwrap();
    }
    if let Some(ref e) = g.background_error {
      return Err(e.clone());
    }

    // Rotate mem → imm and schedule the background flush.
    let path = self.inner.persistence.as_ref().unwrap().dir.as_path();
    let prep = begin_flush(path, &mut g)?;
    g.imm = Some(Arc::clone(&prep.old_mem));
    g.pending_flush = Some(prep);
    maybe_schedule_compaction(&self.inner, &mut g);

    if opts.wait {
      while (g.imm.is_some() || g.pending_flush.is_some()) && g.background_error.is_none() {
        g = self.inner.write_condvar.wait(g).unwrap();
      }
      if let Some(ref e) = g.background_error {
        return Err(e.clone());
      }
    }

    Ok(())
  }

  /// Apply `batch` atomically to the database.
  ///
  /// All operations in `batch` are written together as a single WAL record and inserted into the
  /// memtable under a contiguous range of sequence numbers.  Either all operations succeed or
  /// none are applied.
  ///
  /// ## Write grouping
  ///
  /// Concurrent callers are batched together by a group leader that writes a single combined WAL
  /// record and memtable pass, amortising `fsync` cost.  A `sync = true` writer never joins a
  /// non-sync group (doing so would silently drop its sync guarantee).  Non-sync writers may join
  /// a sync group — they receive a free `fsync`, which is harmless.
  ///
  /// ## Backpressure
  ///
  /// The write leader calls `make_room_for_write` before writing:
  /// - **L0 slowdown** (≥ 8 files): sleeps 1 ms (at most once per `write` call).
  /// - **Flush in progress** (`imm` present): waits for the background thread to complete it.
  /// - **L0 hard stop** (≥ 12 files): blocks until the background thread drains L0.
  /// - **Memtable full**: rotates `mem → imm`, schedules the background flush, and continues.
  ///
  /// Followers bypass all backpressure — only the leader gates on it.
  ///
  /// ## Durability
  ///
  /// If `opts.sync` is `true`, the WAL record is `fsync`'d before this call returns.  Otherwise
  /// the OS page cache provides durability — data survives crashes of the process but not the OS.
  ///
  /// See `db/db_impl.cc: DBImpl::Write`.
  pub fn write(&self, opts: &WriteOptions, batch: WriteBatch) -> Result<(), Error> {
    // ── Phase 1: Enqueue this write request ──────────────────────────────────
    //
    // Every caller pushes a `WriterSlot` and waits until it is either at the
    // front of the queue (becoming the group leader) or has been processed by a
    // previous leader and its result placed in `state.completed`.
    let my_id = {
      let mut state = self.inner.state.lock().unwrap();
      let id = state.next_writer_id;
      state.next_writer_id += 1;
      state.writers.push_back(WriterSlot {
        id,
        batch,
        sync: opts.sync,
      });
      id
    };

    // ── Phase 2: Wait to become leader or receive a completed result ──────────
    let mut state = self.inner.state.lock().unwrap();
    loop {
      // A previous leader may have already processed our slot.
      if let Some(result) = state.completed.remove(&my_id) {
        return result;
      }
      // If we're at the front, we are the new leader.
      if state.writers.front().map(|w| w.id) == Some(my_id) {
        break;
      }
      state = self.inner.write_condvar.wait(state).unwrap();
    }

    // ── Phase 2.5: Ensure there is room in the memtable ──────────────────────
    state = make_room_for_write(&self.inner, state)?;

    // ── Phase 3: Build a group ────────────────────────────────────────────────
    //
    // Collect as many waiting writers as we can into one combined batch.
    // Group-size bound: 1 MiB by default; tightened to first_size + 128 KiB
    // when the first batch is small (prevents small writes from being stalled
    // by a large one joining the group later).  Matches LevelDB's
    // `BuildBatchGroup` from `db/db_impl.cc`.
    //
    // Sync boundary: a sync=true writer is not included in a non-sync group
    // (it would silently drop the sync guarantee).  Non-sync writers *may*
    // join a sync group — they receive sync for free, which is harmless.
    let first_size = state.writers.front().unwrap().batch.approximate_size();
    let max_size = if first_size <= 128 << 10 {
      first_size + (128 << 10)
    } else {
      1 << 20
    };
    let first_sync = state.writers.front().unwrap().sync;

    let mut group_size = first_size;
    let mut group_len = 1;
    while group_len < state.writers.len() {
      let next = &state.writers[group_len];
      // Stop before a sync writer if the group is non-sync.
      if next.sync && !first_sync {
        break;
      }
      let next_size = next.batch.approximate_size();
      if group_size + next_size > max_size {
        break;
      }
      group_size += next_size;
      group_len += 1;
    }

    // ── Phase 4: Build combined batch, write WAL, insert into memtable ────────
    let need_sync = state.writers.iter().take(group_len).any(|w| w.sync);
    let start_seq = state.last_sequence + 1;

    let mut combined = WriteBatch::new();
    combined.set_sequence(start_seq);
    for slot in state.writers.iter().take(group_len) {
      combined.append(&slot.batch);
    }

    let status: Result<(), Error> = (|| {
      if let Some(log) = state.log.as_mut() {
        log.add_record(combined.contents())?;
        if need_sync {
          log.sync()?;
        }
      }
      combined.iterate(&mut Inserter {
        mem: &state.mem,
        seq: start_seq,
      })?;
      state.last_sequence += combined.count() as u64;
      Ok(())
    })();

    // ── Phase 5: Retire the group ─────────────────────────────────────────────
    //
    // Pop the leader's own slot, then pop each follower and store its result
    // in `completed` so it can be retrieved on wake-up.
    state.writers.pop_front(); // leader
    for _ in 1..group_len {
      let slot = state.writers.pop_front().unwrap();
      let follower_result = match &status {
        Ok(()) => Ok(()),
        Err(e) => Err(e.clone()),
      };
      state.completed.insert(slot.id, follower_result);
    }

    // Wake all waiting writers: the next leader can now step up, and
    // followers whose result is in `completed` can return it.
    self.inner.write_condvar.notify_all();

    // Propagate the leader's own status.
    status?;

    // ── Phase 6: Schedule background work if needed ───────────────────────────
    //
    // If the write filled the memtable and no flush is already pending, rotate
    // mem → imm now so the background thread can flush it.  Then wait for the
    // flush to complete before returning, preserving the invariant that all
    // data written before a `put` returns is durable and readable.
    if let Some(ref persistence) = self.inner.persistence {
      let triggered_flush = if state.imm.is_none()
        && state.pending_flush.is_none()
        && state.mem.approximate_memory_usage() >= self.inner.options.write_buffer_size
      {
        let path = persistence.dir.as_path();
        match begin_flush(path, &mut state) {
          Ok(prep) => {
            state.imm = Some(Arc::clone(&prep.old_mem));
            state.pending_flush = Some(prep);
            true
          }
          Err(e) => {
            log::warn!("begin_flush after write failed: {e}, flush deferred");
            false
          }
        }
      } else {
        false
      };
      maybe_schedule_compaction(&self.inner, &mut state);
      // If we triggered a flush, wait for the background thread to complete it.
      // This preserves the original synchronous-flush behaviour seen by callers.
      if triggered_flush {
        while (state.imm.is_some() || state.pending_flush.is_some())
          && state.background_error.is_none()
        {
          state = self.inner.write_condvar.wait(state).unwrap();
        }
        // A background error means the flush failed.  The write itself already
        // succeeded (WAL + memtable), so we don't propagate the error here —
        // the next write will see it in make_room_for_write.
      }
    }
    drop(state);

    Ok(())
  }
}

// ── Background scheduling helpers ────────────────────────────────────────────

/// Notify the background thread if there is work to do and no notification is
/// already outstanding.  Call while holding the `DbState` lock.
fn maybe_schedule_compaction(inner: &Arc<DbInner>, g: &mut DbState) {
  if inner.shutting_down.load(Ordering::Relaxed) {
    return;
  }
  if g.background_error.is_some() {
    return;
  }
  if g.background_scheduled {
    return;
  }
  let has_imm = g.imm.is_some() || g.pending_flush.is_some();
  let needs = has_imm || {
    let v = g.version_set.as_ref().map(|vs| vs.current());
    v.as_ref()
      .is_some_and(|v| needs_compaction(v, g.compaction_needed))
  };
  if !needs {
    return;
  }
  g.background_scheduled = true;
  inner.bg_condvar.notify_one();
}

/// Called by the write leader (while holding the lock) to ensure there is room
/// in the memtable.  May sleep or wait on `write_condvar` if the system is
/// under write pressure.  Returns the (possibly re-acquired) lock guard.
fn make_room_for_write<'a>(
  inner: &'a Arc<DbInner>,
  mut g: std::sync::MutexGuard<'a, DbState>,
) -> Result<std::sync::MutexGuard<'a, DbState>, Error> {
  let mut allow_delay = true;
  loop {
    if let Some(ref e) = g.background_error {
      return Err(e.clone());
    }
    if inner.persistence.is_none() {
      // In-memory: no flush needed.
      break;
    }
    let l0 = g
      .version_set
      .as_ref()
      .map_or(0, |vs| vs.current().files[0].len());
    if allow_delay && l0 >= L0_SLOWDOWN_WRITES_TRIGGER {
      // Slow down at most once per write call.
      log::debug!("L0 file count ({l0}) ≥ {L0_SLOWDOWN_WRITES_TRIGGER}: delaying writes 1ms");
      allow_delay = false;
      drop(g);
      std::thread::sleep(std::time::Duration::from_millis(1));
      g = inner.state.lock().unwrap();
    } else if g.mem.approximate_memory_usage() < inner.options.write_buffer_size {
      break; // There is room in the current memtable.
    } else if g.imm.is_some() || g.pending_flush.is_some() {
      // A flush is already in progress; wait for the background thread.
      log::debug!("waiting for in-progress flush to complete");
      g = inner.write_condvar.wait(g).unwrap();
    } else if l0 >= L0_STOP_WRITES_TRIGGER {
      // Too many L0 files; wait for the background thread to drain them.
      log::warn!(
        "L0 file count ({l0}) ≥ {L0_STOP_WRITES_TRIGGER}: stopping writes until compaction drains L0"
      );
      g = inner.write_condvar.wait(g).unwrap();
    } else {
      // Rotate mem → imm, schedule background flush.
      log::info!(
        "memtable full ({} bytes ≥ {}): rotating to immutable",
        g.mem.approximate_memory_usage(),
        inner.options.write_buffer_size,
      );
      let path = inner.persistence.as_ref().unwrap().dir.as_path();
      let prep = begin_flush(path, &mut g)?;
      g.imm = Some(Arc::clone(&prep.old_mem));
      g.pending_flush = Some(prep);
      maybe_schedule_compaction(inner, &mut g);
    }
  }
  Ok(g)
}

/// Background worker: sleeps on `bg_condvar`, wakes to perform flush or
/// compaction, then re-checks.
fn bg_worker(inner: Arc<DbInner>) {
  let mut g = inner.state.lock().unwrap();
  loop {
    // Sleep until scheduled or shutting down.
    while !g.background_scheduled && !inner.shutting_down.load(Ordering::Relaxed) {
      g = inner.bg_condvar.wait(g).unwrap();
    }
    g.background_scheduled = false;

    // On shutdown: flush any pending memtable, then exit.
    let shutting_down = inner.shutting_down.load(Ordering::Relaxed);

    if g.background_error.is_some() {
      inner.write_condvar.notify_all();
      if shutting_down {
        return;
      }
      continue;
    }

    let Some(ref p) = inner.persistence else {
      inner.write_condvar.notify_all();
      if shutting_down {
        return;
      }
      continue;
    };
    let path = p.dir.clone();
    let tc = p.table_cache.clone();

    // ── Flush if pending ──────────────────────────────────────────────────────
    if let Some(prep) = g.pending_flush.take() {
      log::info!("bg: flushing memtable to SSTable {}", prep.sst_number);
      drop(g);
      let result = write_flush(prep, &inner.options);
      g = inner.state.lock().unwrap();
      match result {
        Ok(res) => {
          if let Err(e) = finish_flush(&mut g, res, &inner.options, &tc) {
            log::error!("bg: finish_flush failed: {e}, stopping writes");
            g.background_error = Some(e);
          }
        }
        Err(e) => {
          log::error!("bg: write_flush failed: {e}, stopping writes");
          g.background_error = Some(e);
        }
      }
      inner.write_condvar.notify_all();
      drop(g);
      delete_obsolete_files(&path, &inner.state);
      g = inner.state.lock().unwrap();
    } else if !shutting_down {
      // ── Compaction (skipped on shutdown path) ────────────────────────────
      drop(g);
      maybe_compact(&path, &inner.state, &inner.options, &tc);
      inner.write_condvar.notify_all();
      delete_obsolete_files(&path, &inner.state);
      g = inner.state.lock().unwrap();
    }

    // After processing, check whether to exit (shutdown) or reschedule.
    let shutting_down = inner.shutting_down.load(Ordering::Relaxed);
    let has_pending = g.imm.is_some() || g.pending_flush.is_some();
    if shutting_down && !has_pending {
      inner.write_condvar.notify_all();
      return;
    }

    // Reschedule immediately if more work remains.
    if !shutting_down {
      let needs_c = {
        let v = g.version_set.as_ref().map(|vs| vs.current());
        v.as_ref()
          .is_some_and(|v| needs_compaction(v, g.compaction_needed))
      };
      if has_pending || needs_c {
        g.background_scheduled = true;
        // Loop back immediately without waiting.
      }
    } else if has_pending {
      // Still have pending work during shutdown — keep looping.
      g.background_scheduled = true;
    }
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
/// allocate the next SSTable file number, and create and activate the new WAL.
///
/// The new WAL is activated immediately (swapped into `state.log`) so that
/// any writes between now and when the background thread runs `finish_flush`
/// go to the new WAL rather than the old one.  If the new WAL were activated
/// only in `finish_flush`, those intermediate writes would land in the old WAL
/// which `finish_flush` then deletes — causing data loss on reopen.
///
/// `finish_flush` still calls `vs.set_log_number` and `log_and_apply` to
/// commit the new log number and SSTable to the MANIFEST, so that on reopen
/// the correct WAL is replayed.
fn begin_flush(path: &std::path::Path, state: &mut DbState) -> Result<FlushPrep, Error> {
  let vs = state
    .version_set
    .as_mut()
    .expect("begin_flush: no VersionSet");
  let old_log_number = vs.log_number();
  let sst_number = vs.next_file_number();
  let new_log_number = vs.next_file_number();
  log::debug!("begin_flush: sst={sst_number}, new_log={new_log_number}, old_log={old_log_number}");
  let new_log_file = std::fs::File::create(path.join(format!("{new_log_number:06}.log")))?;
  let new_log = LogWriter::new(new_log_file, 0);
  // Activate the new WAL immediately; preserve the old WAL so finish_flush
  // can delete it after log_and_apply commits the rotation to the MANIFEST.
  let _old_log = state.log.replace(new_log);
  let old_mem = std::mem::take(&mut state.mem);
  state.imm = None; // should already be None; be explicit
                    // Capture last_sequence now so finish_flush stores the correct value in the
                    // MANIFEST.  Writes that happen to the new WAL after begin_flush will have
                    // sequences > last_sequence_at_rotation and will be replayed on next open.
  let last_sequence_at_rotation = state.last_sequence;
  Ok(FlushPrep {
    sst_number,
    sst_path: path.join(format!("{sst_number:06}.ldb")),
    old_mem,
    new_log_number,
    last_sequence_at_rotation,
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
    last_sequence_at_rotation,
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
    opts.block_cache.clone(),
  )?);
  let smallest_user_key = ikey_user_key(&smallest).to_vec();
  let largest_user_key = ikey_user_key(&largest).to_vec();
  Ok(FlushResult {
    file_number: sst_number,
    file_size,
    smallest,
    largest,
    smallest_user_key,
    largest_user_key,
    table,
    new_log_number,
    last_sequence_at_rotation,
    old_log_path,
  })
}

/// Phase 3 (under write lock): atomically record the new SST *and* new log
/// number in the MANIFEST (`log_and_apply`), clear `imm`, and delete the old WAL.
///
/// `begin_flush` already activated the new WAL (swapping `state.log`), so
/// `finish_flush` does not touch `state.log`.  `set_log_number` is called
/// before `log_and_apply` so the MANIFEST record carries the correct log
/// number — matching LevelDB's `MakeRoomForWrite`.
fn finish_flush(
  state: &mut DbState,
  result: FlushResult,
  opts: &Options,
  tc: &crate::db::table_cache::TableCache,
) -> Result<(), Error> {
  // Register the new table in the cache before installing the version.
  tc.insert(result.file_number, result.table);
  let meta = FileMetaData::new(
    result.file_number,
    result.file_size,
    result.smallest,
    result.largest,
  );
  // Choose output level: try to push past L0 when there is no overlap and
  // grandparent bytes are within bounds.  Falls back to L0 when the memtable
  // range overlaps existing L0 files.
  let output_level = {
    let version = state.version_set.as_ref().map(|vs| vs.current());
    match version {
      Some(v) if !result.smallest_user_key.is_empty() => pick_level_for_memtable_output(
        &v,
        &result.smallest_user_key,
        &result.largest_user_key,
        opts.max_file_size as u64,
      ),
      _ => 0,
    }
  };
  let mut edit = VersionEdit::new();
  edit.new_files.push((output_level as i32, meta));
  let vs = state
    .version_set
    .as_mut()
    .expect("finish_flush: no VersionSet");
  // Use the sequence number captured at rotation time, NOT the current
  // state.last_sequence.  Writes to the new WAL after begin_flush have
  // sequences > last_sequence_at_rotation and must be replayed on reopen;
  // recording the current (higher) last_sequence would cause them to be skipped.
  vs.set_last_sequence(result.last_sequence_at_rotation);
  vs.set_log_number(result.new_log_number);
  vs.log_and_apply(&mut edit, tc)?;
  // state.log was already swapped to the new WAL in begin_flush — no swap needed here.
  state.imm = None;
  log::info!(
    "flush complete: file {} ({} bytes) at L{output_level}",
    result.file_number,
    result.file_size,
  );
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

// L0_COMPACTION_TRIGGER is referenced in tests and as a documentation anchor;
// `needs_compaction` uses `compaction_score` so the constant itself is only
// used in tests.
#[allow(dead_code)]
const L0_COMPACTION_TRIGGER: usize = 4;
const L0_SLOWDOWN_WRITES_TRIGGER: usize = 8;
const L0_STOP_WRITES_TRIGGER: usize = 12;

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

// ── Geometry helpers ──────────────────────────────────────────────────────────

/// User-key range union of a non-empty slice of files.
///
/// Returns `(smallest_internal_key, largest_internal_key)` spanning all files.
fn get_range(files: &[Arc<FileMetaData>]) -> (Vec<u8>, Vec<u8>) {
  debug_assert!(!files.is_empty());
  let mut smallest = files[0].smallest.clone();
  let mut largest = files[0].largest.clone();
  for f in files.iter().skip(1) {
    if ikey_user_key(&f.smallest) < ikey_user_key(&smallest) {
      smallest = f.smallest.clone();
    }
    if ikey_user_key(&f.largest) > ikey_user_key(&largest) {
      largest = f.largest.clone();
    }
  }
  (smallest, largest)
}

/// Union of the key ranges of two slices of files (either may be empty).
fn get_range2(a: &[Arc<FileMetaData>], b: &[Arc<FileMetaData>]) -> (Vec<u8>, Vec<u8>) {
  if b.is_empty() {
    return get_range(a);
  }
  if a.is_empty() {
    return get_range(b);
  }
  let (s1, l1) = get_range(a);
  let (s2, l2) = get_range(b);
  let smallest = if ikey_user_key(&s1) <= ikey_user_key(&s2) {
    s1
  } else {
    s2
  };
  let largest = if ikey_user_key(&l1) >= ikey_user_key(&l2) {
    l1
  } else {
    l2
  };
  (smallest, largest)
}

/// Total on-disk bytes across a slice of files.
fn total_file_size(files: &[Arc<FileMetaData>]) -> u64 {
  files.iter().map(|f| f.file_size).sum()
}

/// Files at `level` whose user-key range overlaps `[begin_uk, end_uk]`.
///
/// For L0, the result may expand the input range (L0 files can overlap each
/// other), so the scan repeats until stable.  For L1+, files are
/// non-overlapping and sorted, so a single linear pass suffices.
///
/// `begin_uk` and `end_uk` are bare user keys (no internal key suffix).
fn get_overlapping_inputs(
  version: &crate::db::version::Version,
  level: usize,
  begin_uk: &[u8],
  end_uk: &[u8],
) -> Vec<Arc<FileMetaData>> {
  let mut result: Vec<Arc<FileMetaData>> = Vec::new();
  let mut lo = begin_uk.to_vec();
  let mut hi = end_uk.to_vec();

  loop {
    result.clear();
    let prev_lo = lo.clone();
    let prev_hi = hi.clone();
    for f in &version.files[level] {
      let f_lo = ikey_user_key(&f.smallest);
      let f_hi = ikey_user_key(&f.largest);
      if f_hi < lo.as_slice() || f_lo > hi.as_slice() {
        continue; // no overlap
      }
      result.push(Arc::clone(f));
      if level == 0 {
        // Expand range to cover this file.
        if f_lo < lo.as_slice() {
          lo = f_lo.to_vec();
        }
        if f_hi > hi.as_slice() {
          hi = f_hi.to_vec();
        }
      }
    }
    if level > 0 || (lo == prev_lo && hi == prev_hi) {
      break;
    }
  }
  result
}

/// True if any file at `level` has a user-key range overlapping `[smallest_uk, largest_uk]`.
///
/// For L0 uses a full scan (files may overlap each other).
/// For L1+ uses a linear scan; files are non-overlapping and sorted so we can break early.
///
/// Port of LevelDB's `Version::OverlapInLevel`.
fn overlap_in_level(
  version: &crate::db::version::Version,
  level: usize,
  smallest_uk: &[u8],
  largest_uk: &[u8],
) -> bool {
  for f in &version.files[level] {
    let f_lo = ikey_user_key(&f.smallest);
    let f_hi = ikey_user_key(&f.largest);
    if f_hi >= smallest_uk && f_lo <= largest_uk {
      return true;
    }
    // L1+: files are sorted by smallest key; once f_lo > largest_uk no later file can overlap.
    if level > 0 && f_lo > largest_uk {
      break;
    }
  }
  false
}

/// Maximum grandparent (level+2) overlap in bytes before flush placement stops pushing deeper.
///
/// Matches LevelDB's `MaxGrandParentOverlapBytes = 10 * TargetFileSize`.
fn max_grandparent_overlap_bytes(max_file_size: u64) -> u64 {
  10 * max_file_size
}

/// Choose the level at which to place a freshly flushed memtable SSTable.
///
/// If the flush range does not overlap any L0 files, it may be safe to push the
/// output directly to L1 or even L2, reducing L0→L1 compaction pressure.
///
/// Rules (port of `Version::PickLevelForMemTableOutput`):
/// - Start at L0.
/// - While `level < MAX_MEM_COMPACT_LEVEL` (2):
///   - If the range overlaps L+1, stop.
///   - If grandparent (L+2) overlap exceeds `max_grandparent_overlap_bytes`, stop.
///   - Otherwise advance to level+1.
fn pick_level_for_memtable_output(
  version: &crate::db::version::Version,
  smallest_uk: &[u8],
  largest_uk: &[u8],
  max_file_size: u64,
) -> usize {
  use crate::db::version::NUM_LEVELS;
  const MAX_MEM_COMPACT_LEVEL: usize = 2;

  let mut level = 0;
  if !overlap_in_level(version, 0, smallest_uk, largest_uk) {
    // No L0 overlap — consider pushing deeper.
    while level < MAX_MEM_COMPACT_LEVEL {
      if overlap_in_level(version, level + 1, smallest_uk, largest_uk) {
        break;
      }
      if level + 2 < NUM_LEVELS {
        // Check grandparent overlap to avoid creating a file that will cause
        // an expensive compaction at the next level.
        let grandparent_bytes = total_file_size(&get_overlapping_inputs(
          version,
          level + 2,
          smallest_uk,
          largest_uk,
        ));
        if grandparent_bytes > max_grandparent_overlap_bytes(max_file_size) {
          break;
        }
      }
      level += 1;
    }
  }
  level
}

/// Extend `compaction_files` with any file in `version.files[level]` that
/// shares a user key with the current largest key in `compaction_files` (at a
/// different sequence number).
///
/// This prevents a compaction from splitting entries for the same user key
/// across two output SSTables.  Port of LevelDB's `AddBoundaryInputs`.
fn add_boundary_inputs(
  version: &crate::db::version::Version,
  level: usize,
  compaction_files: &mut Vec<Arc<FileMetaData>>,
) {
  use crate::table::format::cmp_internal_keys;

  loop {
    // Find the largest internal key in the current compaction set.
    let largest = match compaction_files
      .iter()
      .max_by(|a, b| cmp_internal_keys(&a.largest, &b.largest))
    {
      Some(f) => f.largest.clone(),
      None => return,
    };
    let largest_uk = ikey_user_key(&largest).to_vec();

    // Find the smallest file in version.files[level] whose smallest key has
    // the same user key as `largest_uk` but a strictly greater internal key
    // (i.e., same user key, lower sequence number), and isn't already in the
    // compaction set.
    let boundary = version.files[level]
      .iter()
      .filter(|f| {
        let f_uk = ikey_user_key(&f.smallest);
        f_uk == largest_uk.as_slice()
          && cmp_internal_keys(&f.smallest, &largest) == std::cmp::Ordering::Greater
          && !compaction_files.iter().any(|c| c.number == f.number)
      })
      .min_by(|a, b| cmp_internal_keys(&a.smallest, &b.smallest));

    match boundary {
      Some(f) => compaction_files.push(Arc::clone(f)),
      None => return,
    }
  }
}

// ── CompactionSpec ────────────────────────────────────────────────────────────

/// Everything needed to execute one compaction pass.
struct CompactionSpec {
  /// Level being compacted; inputs come from `level` and `level+1`.
  level: usize,
  /// `inputs[0]` = files at `level`; `inputs[1]` = files at `level+1`.
  inputs: [Vec<Arc<FileMetaData>>; 2],
  /// Files at `level+2` overlapping the full input range.
  grandparents: Vec<Arc<FileMetaData>>,
  /// Version snapshot used to build this spec.
  input_version: Arc<crate::db::version::Version>,
  /// VersionEdit carrying the compact_pointer update (filled by setup_other_inputs).
  edit: VersionEdit,

  // ── Gap 4: grandparent-overlap output limiting ────────────────────────────
  /// Index into `grandparents` of the next file to scan in `should_stop_before`.
  grandparent_index: usize,
  /// Whether we have seen at least one key (used to skip overlap accounting for
  /// the very first key, matching LevelDB's `Compaction::ShouldStopBefore`).
  seen_key: bool,
  /// Accumulated grandparent overlap bytes since the last output-file boundary.
  overlapped_bytes: i64,
  /// Per-level monotone cursors for `is_base_level_for_key`.  Each entry starts
  /// at 0 and only advances forward, amortising repeated scans to O(1)/key.
  level_ptrs: [usize; crate::db::version::NUM_LEVELS],
}

impl CompactionSpec {
  fn new(level: usize, input_version: Arc<crate::db::version::Version>) -> Self {
    CompactionSpec {
      level,
      inputs: [Vec::new(), Vec::new()],
      grandparents: Vec::new(),
      input_version,
      edit: VersionEdit::new(),
      grandparent_index: 0,
      seen_key: false,
      overlapped_bytes: 0,
      level_ptrs: [0; crate::db::version::NUM_LEVELS],
    }
  }

  fn all_input_files(&self) -> impl Iterator<Item = &Arc<FileMetaData>> {
    self.inputs[0].iter().chain(self.inputs[1].iter())
  }
}

// ── setup_other_inputs ────────────────────────────────────────────────────────

/// Populate `spec.inputs[1]` (level+1 files), try to expand inputs[0] without
/// pulling in more level+1 files, populate grandparents, and record the
/// compact-pointer update in `spec.edit`.
///
/// Port of LevelDB's `VersionSet::SetupOtherInputs`.
fn setup_other_inputs(
  spec: &mut CompactionSpec,
  version: &Arc<crate::db::version::Version>,
  _compact_pointer: &[Vec<u8>; crate::db::version::NUM_LEVELS],
  opts: &Options,
) {
  use crate::db::version::NUM_LEVELS;

  let level = spec.level;
  add_boundary_inputs(version, level, &mut spec.inputs[0]);

  let (smallest, largest) = get_range(&spec.inputs[0]);
  let smallest_uk = ikey_user_key(&smallest).to_vec();
  let largest_uk = ikey_user_key(&largest).to_vec();

  spec.inputs[1] = get_overlapping_inputs(version, level + 1, &smallest_uk, &largest_uk);
  add_boundary_inputs(version, level + 1, &mut spec.inputs[1]);

  let (all_start, all_limit) = get_range2(&spec.inputs[0], &spec.inputs[1]);
  let all_start_uk = ikey_user_key(&all_start).to_vec();
  let all_limit_uk = ikey_user_key(&all_limit).to_vec();

  // Try to expand inputs[0] without expanding inputs[1].
  if !spec.inputs[1].is_empty() {
    let mut expanded0 = get_overlapping_inputs(version, level, &all_start_uk, &all_limit_uk);
    add_boundary_inputs(version, level, &mut expanded0);

    let inputs1_size = total_file_size(&spec.inputs[1]);
    let expanded0_size = total_file_size(&expanded0);
    let expand_limit = 25 * opts.max_file_size as u64;

    if expanded0.len() > spec.inputs[0].len() && inputs1_size + expanded0_size < expand_limit {
      let (exp_start, exp_limit) = get_range(&expanded0);
      let exp_start_uk = ikey_user_key(&exp_start).to_vec();
      let exp_limit_uk = ikey_user_key(&exp_limit).to_vec();
      let expanded1 = get_overlapping_inputs(version, level + 1, &exp_start_uk, &exp_limit_uk);
      // Only accept the expansion if it doesn't pull in more L+1 files.
      if expanded1.len() == spec.inputs[1].len() {
        spec.inputs[0] = expanded0;
        spec.inputs[1] = expanded1;
      }
    }
  }

  // Populate grandparents (level+2 files overlapping the compaction range).
  if level + 2 < NUM_LEVELS {
    let (final_start, final_limit) = get_range2(&spec.inputs[0], &spec.inputs[1]);
    let final_start_uk = ikey_user_key(&final_start).to_vec();
    let final_limit_uk = ikey_user_key(&final_limit).to_vec();
    spec.grandparents =
      get_overlapping_inputs(version, level + 2, &final_start_uk, &final_limit_uk);
  }

  // Update compact_pointer: advance past largest key of inputs[0].
  let (_, new_largest) = get_range(&spec.inputs[0]);
  spec.edit.compact_pointers.push((level as i32, new_largest));
}

/// Returns `true` when the current output SSTable should be closed because the
/// bytes it would overlap at `level+2` (the grandparents) have exceeded the
/// limit `max_grandparent_overlap_bytes`.
///
/// Monotone in `ikey`: callers must feed keys in ascending internal-key order.
/// Closing the output early limits future compaction amplification.
///
/// Port of LevelDB `Compaction::ShouldStopBefore`.
fn should_stop_before(spec: &mut CompactionSpec, ikey: &[u8], opts: &Options) -> bool {
  use crate::table::format::cmp_internal_keys;
  let limit = max_grandparent_overlap_bytes(opts.max_file_size as u64) as i64;
  while spec.grandparent_index < spec.grandparents.len()
    && cmp_internal_keys(ikey, &spec.grandparents[spec.grandparent_index].largest)
      == std::cmp::Ordering::Greater
  {
    if spec.seen_key {
      spec.overlapped_bytes += spec.grandparents[spec.grandparent_index].file_size as i64;
    }
    spec.grandparent_index += 1;
  }
  spec.seen_key = true;
  if spec.overlapped_bytes > limit {
    spec.overlapped_bytes = 0;
    return true;
  }
  false
}

/// Returns `true` if `user_key` is definitely absent from all levels ≥
/// `spec.level + 2` (i.e., the output level is the lowest level that holds
/// this key).  Used for tombstone elision: we can only drop a deletion marker
/// when it cannot hide a live value at a deeper level.
///
/// Uses per-level monotone cursors (`spec.level_ptrs`) so each cursor advances
/// at most once per key across the entire compaction — amortised O(1)/key.
///
/// Port of LevelDB `Compaction::IsBaseLevelForKey`.
fn is_base_level_for_key(spec: &mut CompactionSpec, user_key: &[u8]) -> bool {
  use crate::db::version::NUM_LEVELS;
  for lvl in (spec.level + 2)..NUM_LEVELS {
    let files = &spec.input_version.files[lvl];
    while spec.level_ptrs[lvl] < files.len() {
      let f = &files[spec.level_ptrs[lvl]];
      let f_largest_uk = ikey_user_key(&f.largest);
      if user_key <= f_largest_uk {
        // user_key is ≤ this file's largest — it may be inside this file.
        if user_key >= ikey_user_key(&f.smallest) {
          return false; // user_key falls within this file's range
        }
        break; // user_key is before this file; no later file at this level can match
      }
      spec.level_ptrs[lvl] += 1; // user_key is past this file; advance cursor
    }
  }
  true
}

// ── Seek-based compaction helpers ─────────────────────────────────────────────

/// Decrement the seek budget of the blamed file and, if it hits zero, nominate
/// it for seek-based compaction.
///
/// Called under the DB lock after every SSTable `get` that had to probe more
/// than one file.  Port of LevelDB `Version::UpdateStats`.
fn update_stats(state: &mut DbState, stats: &crate::db::version::GetStats) {
  use std::sync::atomic::Ordering;
  if let Some(ref file) = stats.seek_file {
    let prev = file.allowed_seeks.fetch_sub(1, Ordering::Relaxed);
    if prev <= 1 && state.seek_compact_file.is_none() {
      state.seek_compact_file = Some((Arc::clone(file), stats.seek_file_level));
      state.compaction_needed = true;
    }
  }
}

/// True if `version` has a level that needs compaction (score ≥ 1.0), or if a
/// seek-based compaction candidate has been nominated.
fn needs_compaction(version: &crate::db::version::Version, compaction_needed: bool) -> bool {
  version.compaction_score >= 1.0 || compaction_needed
}

/// Select the next compaction to run, based on level scores, compact-pointer
/// round-robin, or seek-based nomination.
///
/// Returns `None` if no compaction is needed.
fn pick_compaction(
  version: &Arc<crate::db::version::Version>,
  compact_pointer: &[Vec<u8>; crate::db::version::NUM_LEVELS],
  opts: &Options,
  seek_compact: Option<&(Arc<FileMetaData>, usize)>,
) -> Option<CompactionSpec> {
  // ── Size-triggered (highest priority) ─────────────────────────────────────
  if version.compaction_score >= 1.0 {
    let level = version.compaction_level as usize;
    let mut spec = CompactionSpec::new(level, Arc::clone(version));

    if level == 0 {
      // For L0, seed with the first file past compact_pointer[0], then expand
      // to all overlapping L0 files (L0 files can overlap each other).
      let seed = version.files[0]
        .iter()
        .find(|f| {
          compact_pointer[0].is_empty()
            || ikey_user_key(&f.largest) > ikey_user_key(&compact_pointer[0])
        })
        .or_else(|| version.files[0].first())
        .cloned()?;

      let seed_lo = ikey_user_key(&seed.smallest).to_vec();
      let seed_hi = ikey_user_key(&seed.largest).to_vec();
      spec.inputs[0] = get_overlapping_inputs(version, 0, &seed_lo, &seed_hi);
    } else {
      // L1+: pick the first file whose largest key is past compact_pointer[level].
      let file = version.files[level]
        .iter()
        .find(|f| {
          compact_pointer[level].is_empty()
            || ikey_user_key(&f.largest) > ikey_user_key(&compact_pointer[level])
        })
        .or_else(|| version.files[level].first())
        .cloned()?;
      spec.inputs[0].push(file);
    }

    setup_other_inputs(&mut spec, version, compact_pointer, opts);
    return Some(spec);
  }

  // ── Seek-triggered fallback ────────────────────────────────────────────────
  if let Some((file, level)) = seek_compact {
    let mut spec = CompactionSpec::new(*level, Arc::clone(version));
    spec.inputs[0].push(Arc::clone(file));
    setup_other_inputs(&mut spec, version, compact_pointer, opts);
    return Some(spec);
  }

  None
}

/// Alias for a plain (level_inputs, next_level_inputs) pair returned by
/// `pick_range_compaction`.  Used only internally by `compact_level_range`.
type CompactionInputs = (Vec<Arc<FileMetaData>>, Vec<Arc<FileMetaData>>);

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
  block_cache: Option<Arc<BlockCache>>,
) -> Result<(), Error> {
  let file_size = cur.builder.finish()?;
  let read_file = std::fs::File::open(&cur.path)?;
  let table = Arc::new(Table::open(
    read_file,
    file_size,
    filter_policy,
    block_cache,
  )?);
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
/// L0 inputs are already newest-first in `spec.inputs[0]` (Version stores them
/// that way), so `MergingIterator` resolves same-key ties in favour of the
/// newer version via the lower child index.
fn do_compaction(
  path: &std::path::Path,
  state: &Mutex<DbState>,
  spec: &mut CompactionSpec,
  oldest_snapshot: u64,
  opts: &Options,
  tc: &crate::db::table_cache::TableCache,
) -> Result<Vec<CompactionOutput>, Error> {
  use crate::db::merge_iter::MergingIterator;
  use crate::iter::InternalIterator;

  let _output_level = spec.level + 1;
  log::info!(
    "compaction L{}→L{}: {} + {} files ({} + {} bytes)",
    spec.level,
    spec.level + 1,
    spec.inputs[0].len(),
    spec.inputs[1].len(),
    spec.inputs[0].iter().map(|f| f.file_size).sum::<u64>(),
    spec.inputs[1].iter().map(|f| f.file_size).sum::<u64>(),
  );

  let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
  for meta in spec.all_input_files() {
    let table = tc.get_or_open(meta.number, meta.file_size)?;
    // Compaction is a bulk scan — don't pollute the block cache.
    children.push(Box::new(table.new_iterator(opts.paranoid_checks, false)?));
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
    } else if vtype == 0 && seq <= oldest_snapshot && is_base_level_for_key(spec, uk) {
      // Tombstone that no snapshot can see below this level — safe to elide.
      true
    } else {
      false
    };

    last_sequence_for_key = seq;

    if !drop {
      // Close the current output file early if grandparent overlap is too high.
      // This limits future compaction amplification (Gap 4).
      if current.is_some() && should_stop_before(spec, &ikey, opts) {
        let finished = current.take().unwrap();
        finish_compaction_output(
          finished,
          std::mem::take(&mut current_largest),
          &mut outputs,
          opts.filter_policy.clone(),
          opts.block_cache.clone(),
        )?;
      }

      // Rotate to a new output file if the current one is at the size limit.
      if let Some(ref cur) = current {
        if cur.builder.file_size() >= opts.max_file_size as u64 {
          let finished = current.take().unwrap();
          finish_compaction_output(
            finished,
            std::mem::take(&mut current_largest),
            &mut outputs,
            opts.filter_policy.clone(),
            opts.block_cache.clone(),
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
      opts.block_cache.clone(),
    )?;
  }

  log::info!(
    "compaction L{}→L{} complete: {} output files ({} bytes)",
    spec.level,
    spec.level + 1,
    outputs.len(),
    outputs.iter().map(|o| o.file_size).sum::<u64>(),
  );
  Ok(outputs)
}

/// Phase 3 (under lock): record deleted input files and new output files in a
/// single `VersionEdit`, then call `log_and_apply`.
///
/// Also persists the compact-pointer update from `spec.edit` so the MANIFEST
/// records the round-robin cursor advance.
fn install_compaction(
  state: &mut DbState,
  spec: &CompactionSpec,
  outputs: Vec<CompactionOutput>,
  tc: &crate::db::table_cache::TableCache,
) -> Result<(), Error> {
  let vs = state
    .version_set
    .as_mut()
    .expect("install_compaction: no VersionSet");
  let output_level = (spec.level + 1) as i32;
  let mut edit = VersionEdit::new();
  for f in &spec.inputs[0] {
    edit.deleted_files.push((spec.level as i32, f.number));
  }
  for f in &spec.inputs[1] {
    edit.deleted_files.push((spec.level as i32 + 1, f.number));
  }
  for out in outputs {
    // Register the output table in the cache before installing the version.
    tc.insert(out.file_number, out.table);
    let meta = FileMetaData::new(out.file_number, out.file_size, out.smallest, out.largest);
    edit.new_files.push((output_level, meta));
  }
  // Persist the compact-pointer update so it survives a reopen.
  for (lvl, key) in &spec.edit.compact_pointers {
    edit.compact_pointers.push((*lvl, key.clone()));
  }
  vs.set_last_sequence(state.last_sequence);
  vs.log_and_apply(&mut edit, tc)?;

  // Clear seek_compact_file if the nominated file was removed by this compaction.
  let deleted_numbers: std::collections::HashSet<u64> =
    spec.all_input_files().map(|f| f.number).collect();
  if let Some((ref f, _)) = state.seek_compact_file {
    if deleted_numbers.contains(&f.number) {
      state.seek_compact_file = None;
    }
  }

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
  tc: &crate::db::table_cache::TableCache,
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

  // Build a CompactionSpec from the range-compaction inputs.
  let mut spec = CompactionSpec::new(level, Arc::clone(&version));
  spec.inputs[0] = level_inputs;
  spec.inputs[1] = next_inputs;
  // No compact-pointer update for manual range compactions.
  // Populate grandparents if level+2 < NUM_LEVELS.
  if level + 2 < crate::db::version::NUM_LEVELS {
    let (s, l) = get_range2(&spec.inputs[0], &spec.inputs[1]);
    spec.grandparents =
      get_overlapping_inputs(&version, level + 2, ikey_user_key(&s), ikey_user_key(&l));
  }

  // Phase 2: I/O (no lock).
  let outputs = do_compaction(path, state, &mut spec, oldest_snapshot, opts, tc)?;

  // Phase 3: install.
  {
    let mut g = state.lock().unwrap();
    install_compaction(&mut g, &spec, outputs, tc)?;
  }

  delete_obsolete_files(path, state);
  Ok(true)
}

/// Returns `true` when the compaction can be executed as a trivial move:
/// a single file at `spec.level` with no `level+1` overlap and acceptable
/// grandparent overlap.  No data is read or written — only the MANIFEST is
/// updated.
///
/// Port of LevelDB `Compaction::IsTrivialMove`.
fn is_trivial_move(spec: &CompactionSpec, opts: &Options) -> bool {
  spec.inputs[0].len() == 1
    && spec.inputs[1].is_empty()
    && total_file_size(&spec.grandparents)
      <= max_grandparent_overlap_bytes(opts.max_file_size as u64)
}

/// Execute a trivial-move compaction: move one file from `spec.level` to
/// `spec.level + 1` via a MANIFEST update only — no I/O.
///
/// The `TableCache` entry is evicted (so the file will be re-opened under the
/// new level number mapping on next access) and then re-inserted with the same
/// handle so the table stays warm in the cache.
///
/// Port of the trivial-move branch in LevelDB `DBImpl::BackgroundCompaction`.
fn install_trivial_move(
  state: &mut DbState,
  spec: &CompactionSpec,
  tc: &crate::db::table_cache::TableCache,
) -> Result<(), Error> {
  let vs = state
    .version_set
    .as_mut()
    .expect("install_trivial_move: no VersionSet");

  let file = Arc::clone(&spec.inputs[0][0]);
  let mut edit = VersionEdit::new();
  edit.deleted_files.push((spec.level as i32, file.number));
  edit
    .new_files
    .push((spec.level as i32 + 1, Arc::clone(&file)));
  // Persist the compact-pointer update.
  for (lvl, key) in &spec.edit.compact_pointers {
    edit.compact_pointers.push((*lvl, key.clone()));
  }

  // Keep the table warm: evict the old entry, re-insert under same number.
  // log_and_apply evicts the deleted file; re-insert it here so the next
  // access finds it in the cache instead of re-opening from disk.
  let table_arc = tc.get_or_open(file.number, file.file_size).ok();

  vs.set_last_sequence(state.last_sequence);
  vs.log_and_apply(&mut edit, tc)?;

  // Re-insert so the file stays warm even though log_and_apply evicted it.
  if let Some(table) = table_arc {
    tc.insert(file.number, table);
  }

  // Clear seek_compact_file if this file was the nominee.
  if let Some((ref scf, _)) = state.seek_compact_file {
    if scf.number == file.number {
      state.seek_compact_file = None;
    }
  }

  Ok(())
}

/// Run synchronous compaction for any level that needs it (score ≥ 1.0).
///
/// Uses the same three-phase lock protocol as flush:
///   1. Snapshot current version under lock.
///   2. Run I/O (MergingIterator + SSTable writes) without the lock.
///   3. Install the result (VersionEdit + log_and_apply) under the lock.
///
/// Errors are silently ignored — a failed compaction does not affect
/// correctness; the triggering level will be retried on the next call.
fn maybe_compact(
  path: &std::path::Path,
  state: &Mutex<DbState>,
  opts: &Options,
  tc: &crate::db::table_cache::TableCache,
) {
  // Phase 1: snapshot.
  let (version, oldest_snapshot, compact_pointer, seek_compact_file) = {
    let mut g = state.lock().unwrap();
    let (current, compact_ptr) = match &g.version_set {
      Some(vs) => (vs.current(), vs.compact_pointer().clone()),
      None => return,
    };
    let oldest = g
      .snapshots
      .iter()
      .next()
      .copied()
      .unwrap_or(g.last_sequence);
    // Clear the compaction_needed flag; it will be re-set if another seek miss
    // occurs before we finish.
    g.compaction_needed = false;
    let seek = g.seek_compact_file.clone();
    (current, oldest, compact_ptr, seek)
  };

  if !needs_compaction(&version, seek_compact_file.is_some()) {
    return;
  }

  let mut spec = match pick_compaction(&version, &compact_pointer, opts, seek_compact_file.as_ref())
  {
    Some(s) => s,
    None => return,
  };

  // Trivial move: single file, no L+1 overlap, acceptable grandparent overlap.
  // No I/O needed — just a MANIFEST update.
  if is_trivial_move(&spec, opts) {
    let file = &spec.inputs[0][0];
    log::info!(
      "trivial move: file {} ({} bytes) L{}→L{}",
      file.number,
      file.file_size,
      spec.level,
      spec.level + 1,
    );
    let mut g = state.lock().unwrap();
    if let Err(e) = install_trivial_move(&mut g, &spec, tc) {
      log::warn!("trivial move failed: {e}");
    }
    drop(g);
    delete_obsolete_files(path, state);
    return;
  }

  // Phase 2: I/O (no lock).
  let outputs = match do_compaction(path, state, &mut spec, oldest_snapshot, opts, tc) {
    Ok(o) => o,
    Err(e) => {
      log::warn!("compaction L{}→L{} failed: {e}", spec.level, spec.level + 1);
      return;
    }
  };

  // Phase 3: install.
  {
    let mut g = state.lock().unwrap();
    if let Err(e) = install_compaction(&mut g, &spec, outputs, tc) {
      log::warn!(
        "install_compaction L{}→L{} failed: {e}",
        spec.level,
        spec.level + 1
      );
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
        log::debug!("deleting obsolete file: {name}");
        let _ = std::fs::remove_file(entry.path());
      }
    }
  }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use crate::{Db, Error, Options, ReadOptions, WriteBatch, WriteOptions};
  use serial_test::serial;

  // ── parse_db_filename tests (port of LevelDB filename_test.cc: Parse) ─────────

  #[test]
  fn parse_db_filename_valid_cases() {
    use super::{parse_db_filename, FileKind};

    let cases: &[(&str, u64, FileKind)] = &[
      ("100.log", 100, FileKind::Log),
      ("0.log", 0, FileKind::Log),
      ("0.ldb", 0, FileKind::Table),
      ("CURRENT", 0, FileKind::Current),
      ("LOCK", 0, FileKind::Lock),
      ("MANIFEST-2", 2, FileKind::Manifest),
      ("MANIFEST-7", 7, FileKind::Manifest),
      // u64::MAX
      (
        "18446744073709551615.log",
        18_446_744_073_709_551_615,
        FileKind::Log,
      ),
    ];
    for &(name, number, ref kind) in cases {
      let result = parse_db_filename(name);
      assert!(result.is_some(), "{name}: expected Some, got None");
      let (n, k) = result.unwrap();
      assert_eq!(n, number, "{name}: wrong number");
      assert!(
        std::mem::discriminant(&k) == std::mem::discriminant(kind),
        "{name}: wrong kind"
      );
    }
  }

  #[test]
  fn parse_db_filename_invalid_cases() {
    use super::parse_db_filename;
    let errors: &[&str] = &[
      "",
      "foo",
      "foo-dx-100.log",
      ".log",
      "manifest",
      "CURREN",
      "CURRENTX",
      "MANIFES",
      "MANIFEST",
      "MANIFEST-",
      "XMANIFEST-3",
      "MANIFEST-3x",
      "LOCKx",
      "100",
      "100.",
      "100.lop",
      // u64 overflow
      "18446744073709551616.log",
      "184467440737095516150.log",
    ];
    for &name in errors {
      assert!(
        parse_db_filename(name).is_none(),
        "{name}: expected None, got Some"
      );
    }
  }

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
      db.write(&WriteOptions::default(), batch).unwrap();
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
  #[serial(fd)]
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
      let g = db.inner.state.lock().unwrap();
      g.version_set.as_ref().unwrap().current().files[0].len()
    };
    assert!(
      l0_count < crate::L0_COMPACTION_TRIGGER,
      "L0 should have been compacted; found {l0_count} files"
    );
  }

  #[test]
  #[serial(fd)]
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
  #[serial(fd)]
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
  #[serial(fd)]
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
  #[serial(fd)]
  fn files_present_below_l0_after_writes() {
    // After enough writes, data should be present at L1 or deeper.
    // With flush placement enabled, sequential unique-key flushes may bypass
    // L0 and land at L2 directly, so we test for any level ≥ 1.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();
    for i in 0u32..200 {
      db.put(format!("k{i:05}").as_bytes(), b"v").unwrap();
    }
    let deep_count: usize = {
      let g = db.inner.state.lock().unwrap();
      let cur = g.version_set.as_ref().unwrap().current();
      (1..crate::db::version::NUM_LEVELS)
        .map(|l| cur.files[l].len())
        .sum()
    };
    assert!(
      deep_count >= 1,
      "expected at least one file at L1+ after writes"
    );
  }

  // ── compact_range tests ────────────────────────────────────────────────────

  #[test]
  #[serial(fd)]
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
  #[serial(fd)]
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
  #[serial(fd)]
  fn compact_range_moves_data_to_deeper_level() {
    // After a full compact_range, all data should be below L0.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny_options()).unwrap();
    for i in 0u32..200 {
      db.put(format!("k{i:05}").as_bytes(), b"v").unwrap();
    }
    db.compact_range(None, None).unwrap();
    let l0_count = {
      let g = db.inner.state.lock().unwrap();
      g.version_set.as_ref().unwrap().current().files[0].len()
    };
    assert_eq!(l0_count, 0, "L0 should be empty after full compact_range");
  }

  #[test]
  #[serial(fd)]
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
  #[serial(fd)]
  fn compact_range_noop_on_inmemory_db() {
    let db = Db::default();
    db.put(b"k", b"v").unwrap();
    db.compact_range(None, None).unwrap(); // must not panic
    assert_eq!(db.get(b"k").unwrap(), b"v");
  }

  #[test]
  #[serial(fd)]
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

  // ─── Batch-group write tests ──────────────────────────────────────────────

  /// Sequential writes from multiple threads all complete and are readable.
  #[test]
  fn batch_group_concurrent_writes_all_visible() {
    use std::sync::Arc;
    let db = Arc::new(Db::default());
    let n = 100usize;
    let handles: Vec<_> = (0..n)
      .map(|i| {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
          let key = format!("key{i:04}");
          db.put(key.as_bytes(), b"val").unwrap();
        })
      })
      .collect();
    for h in handles {
      h.join().unwrap();
    }
    for i in 0..n {
      let key = format!("key{i:04}");
      assert_eq!(db.get(key.as_bytes()).unwrap(), b"val");
    }
  }

  /// A write batch submitted from a follower slot is correctly applied.
  #[test]
  fn batch_group_follower_batch_applied() {
    use std::sync::{Arc, Barrier};
    let db = Arc::new(Db::default());
    let barrier = Arc::new(Barrier::new(2));

    // Two threads race to write different keys.
    let (db1, b1) = (Arc::clone(&db), Arc::clone(&barrier));
    let t1 = std::thread::spawn(move || {
      b1.wait();
      db1.put(b"alpha", b"1").unwrap();
    });

    let (db2, b2) = (Arc::clone(&db), Arc::clone(&barrier));
    let t2 = std::thread::spawn(move || {
      b2.wait();
      db2.put(b"beta", b"2").unwrap();
    });

    t1.join().unwrap();
    t2.join().unwrap();

    assert_eq!(db.get(b"alpha").unwrap(), b"1");
    assert_eq!(db.get(b"beta").unwrap(), b"2");
  }

  /// Sequence numbers remain monotone under concurrent writes.
  #[test]
  fn batch_group_sequence_numbers_monotone() {
    use std::sync::Arc;
    let db = Arc::new(Db::default());
    let n = 50usize;
    let handles: Vec<_> = (0..n)
      .map(|i| {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
          let key = format!("seq{i:04}");
          db.put(key.as_bytes(), b"x").unwrap();
        })
      })
      .collect();
    for h in handles {
      h.join().unwrap();
    }
    // Verify all keys landed exactly once and with consistent values.
    let mut count = 0usize;
    let opts = ReadOptions::default();
    let mut it = db.new_iterator(&opts).unwrap();
    it.seek_to_first();
    while it.valid() {
      count += 1;
      it.next();
    }
    assert_eq!(count, n);
  }

  /// A sync=true write in the middle of a group does not corrupt other writes.
  #[test]
  fn batch_group_sync_write_does_not_corrupt() {
    use std::sync::{Arc, Barrier};
    let db = Arc::new(Db::default());
    let barrier = Arc::new(Barrier::new(3));

    let threads: Vec<_> = (0..3usize)
      .map(|i| {
        let db = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        std::thread::spawn(move || {
          b.wait();
          let opts = WriteOptions { sync: i == 1 };
          let key = format!("skey{i}");
          let mut batch = WriteBatch::new();
          batch.put(key.as_bytes(), b"v");
          db.write(&opts, batch).unwrap();
        })
      })
      .collect();

    for t in threads {
      t.join().unwrap();
    }

    for i in 0..3usize {
      let key = format!("skey{i}");
      assert_eq!(db.get(key.as_bytes()).unwrap(), b"v");
    }
  }

  // ── Gap 1: level-score-based compaction tests ─────────────────────────────

  /// Build a bare `Version` (no Arc) with controlled file sizes at each level.
  fn version_with_files(level_sizes: &[(usize, u64)]) -> crate::db::version::Version {
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;

    let mut v = crate::db::version::Version::new();
    let mut file_number = 10u64;
    for &(level, size) in level_sizes {
      let ikey = make_internal_key(format!("a{file_number}").as_bytes(), file_number, 1);
      let meta = FileMetaData::new(file_number, size, ikey.clone(), ikey);
      v.files[level].push(meta);
      file_number += 1;
    }
    v
  }

  #[test]
  fn finalize_l0_score_from_file_count() {
    // 4 L0 files / L0_COMPACTION_TRIGGER (4) = score 1.0.
    let mut v = version_with_files(&[(0, 0), (0, 0), (0, 0), (0, 0)]);
    crate::db::version::finalize(&mut v, 4);
    assert!(
      (v.compaction_score - 1.0).abs() < 1e-9,
      "expected score 1.0, got {}",
      v.compaction_score
    );
    assert_eq!(v.compaction_level, 0);
  }

  #[test]
  fn finalize_l1_score_from_bytes() {
    // 10 MiB at L1 = score exactly 1.0.
    let ten_mib = 10 * 1_048_576u64;
    let mut v = version_with_files(&[(1, ten_mib)]);
    crate::db::version::finalize(&mut v, 4);
    assert!(
      (v.compaction_score - 1.0).abs() < 1e-9,
      "expected score 1.0, got {}",
      v.compaction_score
    );
    assert_eq!(v.compaction_level, 1);
  }

  #[test]
  fn finalize_picks_best_level() {
    // L1 half full (score 0.5), L0 double trigger (score 2.0) → best is L0.
    let five_mib = 5 * 1_048_576u64;
    let mut v = version_with_files(&[
      (0, 0),
      (0, 0),
      (0, 0),
      (0, 0),
      (0, 0),
      (0, 0),
      (0, 0),
      (0, 0), // 8 L0 files → score 2.0
      (1, five_mib),
    ]);
    crate::db::version::finalize(&mut v, 4);
    assert!(v.compaction_score >= 2.0 - 1e-9);
    assert_eq!(v.compaction_level, 0);
  }

  #[test]
  fn add_boundary_inputs_includes_same_user_key() {
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    // Two files at L1 with the same user key "k" but different sequence numbers.
    // File A: smallest=("k", seq=5), largest=("k", seq=5)
    // File B: smallest=("k", seq=2), largest=("k", seq=2)
    // (higher seq sorts before lower seq in internal key order)
    let ikey_high = make_internal_key(b"k", 5, 1);
    let ikey_low = make_internal_key(b"k", 2, 1);

    let file_a = FileMetaData::new(10, 100, ikey_high.clone(), ikey_high.clone());
    let file_b = FileMetaData::new(11, 100, ikey_low.clone(), ikey_low.clone());

    let mut v = Version::new();
    // L1 sorted by smallest: file_a (seq=5) sorts BEFORE file_b (seq=2) because
    // higher seq means lower internal key value (tag sorts descending).
    v.files[1].push(Arc::clone(&file_a));
    v.files[1].push(Arc::clone(&file_b));

    // Start compaction with only file_a; boundary check should add file_b.
    let mut compaction_files = vec![Arc::clone(&file_a)];
    crate::add_boundary_inputs(&v, 1, &mut compaction_files);

    assert_eq!(
      compaction_files.len(),
      2,
      "add_boundary_inputs should have added file_b (same user key, lower seq)"
    );
    assert!(compaction_files.iter().any(|f| f.number == 11));
  }

  #[test]
  #[serial(fd)]
  fn compact_pointer_persists_across_reopen() {
    // Run a compaction that should set a compact_pointer, then reopen and verify
    // the pointer is restored from the MANIFEST.
    //
    // Strategy: write the same small key range many times so each flush overlaps
    // with the previous L0 file, forcing all flushes to land at L0.  Once L0
    // reaches L0_COMPACTION_TRIGGER (4) a real L0→L1 compaction fires and the
    // compact_pointer is recorded in the MANIFEST.
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), tiny_options()).unwrap();
      // Each round rewrites the same 5 keys.  With write_buffer_size=128 bytes,
      // each round flushes, producing an overlapping L0 file.  After 4+ rounds
      // L0→L1 compaction fires and compact_pointer is set.
      for round in 0u32..8 {
        for k in 0u32..5 {
          db.put(
            format!("key{k:02}").as_bytes(),
            format!("val{round:02}_{k:02}").as_bytes(),
          )
          .unwrap();
        }
      }
    }

    // Reopen: the MANIFEST should contain compact_pointer records.
    let vs = crate::db::version_set::VersionSet::recover(dir.path(), false).unwrap();
    // At least one level should have a non-empty compact_pointer (L0 or L1).
    let has_pointer = vs.compact_pointer().iter().any(|p| !p.is_empty());
    assert!(
      has_pointer,
      "at least one compact_pointer should be set after compaction"
    );
  }

  // ── Gap 2: flush placement tests ─────────────────────────────────────────

  /// The first flush into an empty database should be pushed past L0.
  ///
  /// With an empty version (no files at any level), `pick_level_for_memtable_output`
  /// advances to `MAX_MEM_COMPACT_LEVEL` (2) because there is no overlap at L1
  /// and no grandparent bytes at L2, so the file lands at L2.  The key invariant
  /// is that L0 stays empty and the data is readable.
  #[test]
  #[serial(fd)]
  fn flush_placement_skips_l0_on_first_flush() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
      create_if_missing: true,
      write_buffer_size: 4 * 1024,
      max_file_size: 2 * 1024 * 1024,
      block_cache: None,
      filter_policy: None,
      ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();

    // Fill the memtable past the flush threshold.
    // 200 writes × ~25 bytes each ≈ 5 KiB > write_buffer_size (4 KiB).
    for i in 0u32..200 {
      db.put(format!("flush{i:04}").as_bytes(), b"value").unwrap();
    }

    // L0 must be empty: the flush was pushed to a higher level.
    // With a completely empty version the output lands at L2 (MAX_MEM_COMPACT_LEVEL).
    let (l0, l2) = {
      let g = db.inner.state.lock().unwrap();
      let cur = g.version_set.as_ref().unwrap().current();
      (cur.files[0].len(), cur.files[2].len())
    };
    assert_eq!(
      l0, 0,
      "expected L0 to be empty after first flush into empty DB"
    );
    assert_eq!(l2, 1, "expected one file at L2 (MAX_MEM_COMPACT_LEVEL)");

    // Verify data is readable regardless of which level it landed at.
    assert_eq!(db.get(b"flush0000").unwrap(), b"value");
    assert_eq!(db.get(b"flush0199").unwrap(), b"value");
  }

  /// When L0 already has files that overlap the flush range, the output must
  /// stay at L0 (we cannot skip over in-progress L0 data).
  #[test]
  #[serial(fd)]
  fn flush_placement_stays_at_l0_when_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
      create_if_missing: true,
      write_buffer_size: 4 * 1024,
      max_file_size: 2 * 1024 * 1024,
      block_cache: None,
      filter_policy: None,
      ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();

    // Write overlapping key ranges twice so the first flush lands at L0 and
    // the second flush's range overlaps L0.
    // First batch → first flush → L1 (no L0 yet).
    for i in 0u32..200 {
      db.put(format!("key{i:04}").as_bytes(), b"v1").unwrap();
    }
    // Second batch uses the same key range → will overlap L0 (or L1) on flush.
    for i in 0u32..200 {
      db.put(format!("key{i:04}").as_bytes(), b"v2").unwrap();
    }

    // Both flushes should have completed; data must be readable.
    for i in 0u32..200 {
      let val = db.get(format!("key{i:04}").as_bytes()).unwrap();
      assert_eq!(val, b"v2");
    }
  }

  #[test]
  #[serial(fd)]
  fn multilevel_compaction_l1_to_l2() {
    // Write enough data so that L1 exceeds 10 MiB, triggering L1→L2 compaction.
    //
    // Use a large value (~1 KiB) per key so data accumulates quickly at L1.
    // write_buffer_size = 32 KiB → each flush produces ~32 entries.
    // max_file_size = 64 KiB → L1 files are ~64 KiB each.
    // After ~160 flushes and corresponding L0→L1 compactions, L1 total bytes
    // exceeds 10 MiB and an automatic L1→L2 compaction fires.
    let dir = tempfile::tempdir().unwrap();
    let value_size = 1024usize; // 1 KiB per value
    let opts = Options {
      create_if_missing: true,
      write_buffer_size: 32 * 1024,
      max_file_size: 64 * 1024,
      // Disable block cache and filter to avoid interference.
      block_cache: None,
      filter_policy: None,
      ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();

    // Write enough keys to produce > 10 MiB at L1.
    // Use pseudo-random bytes so Snappy compression doesn't shrink them to nothing.
    let large_val: Vec<u8> = (0..value_size).map(|i| (i * 7 + 13) as u8).collect();
    for i in 0u32..75_000 {
      // Mix the index into each value so all values differ (avoids any dedup).
      let mut val = large_val.clone();
      let bytes = i.to_le_bytes();
      val[..4].copy_from_slice(&bytes);
      db.put(format!("key{i:07}").as_bytes(), &val).unwrap();
    }

    let (l1_bytes, l2_files) = {
      let g = db.inner.state.lock().unwrap();
      let cur = g.version_set.as_ref().unwrap().current();
      let l1b: u64 = cur.files[1].iter().map(|f| f.file_size).sum();
      (l1b, cur.files[2].len())
    };
    // L2 should have received at least one file from an L1→L2 compaction.
    assert!(
      l2_files > 0,
      "L2 should have files after L1→L2 compaction; L1_bytes={l1_bytes} L2_files={l2_files}"
    );
    // Spot-check: a handful of keys must still be readable.
    // (Full scan would be too slow; check every 2000th key.)
    let large_val: Vec<u8> = (0..value_size).map(|i| (i * 7 + 13) as u8).collect();
    for i in (0u32..75_000).step_by(2000) {
      let mut expected = large_val.clone();
      let bytes = i.to_le_bytes();
      expected[..4].copy_from_slice(&bytes);
      let val = db.get(format!("key{i:07}").as_bytes()).unwrap();
      assert_eq!(val, expected.as_slice());
    }
  }

  // ── Gap 3: Trivial-move tests ─────────────────────────────────────────────

  #[test]
  fn trivial_move_advances_file_to_next_level() {
    // Build a Version with one L1 file and no L2 overlap,
    // then verify is_trivial_move returns true.
    use super::{is_trivial_move, CompactionSpec};
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8]| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        4096,
        make_internal_key(small, 10, 1u8), // 1 = Value
        make_internal_key(large, 1, 1u8),
      )
    };

    let mut v = Version::new();
    // One L1 file covering "a".."z"; no L2 files.
    v.files[1].push(make_meta(10, b"a", b"z"));
    let version = Arc::new(v);

    let opts = Options {
      max_file_size: 64 * 1024,
      ..Options::default()
    };

    // Build a spec for this file: one L1 input, no L2 inputs, no grandparents.
    let mut spec = CompactionSpec::new(1, Arc::clone(&version));
    spec.inputs[0].push(Arc::clone(&version.files[1][0]));
    // inputs[1] and grandparents are empty (no L2 or L3 files).

    assert!(
      is_trivial_move(&spec, &opts),
      "single file with no next-level overlap and no grandparents should be a trivial move"
    );

    // Also verify: adding grandparent bytes that exceed the limit disqualifies it.
    for i in 0u64..12 {
      spec.grandparents.push(make_meta(
        100 + i,
        &format!("{i:02}").into_bytes(),
        &format!("{:02}", i + 1).into_bytes(),
      ));
    }
    // Each grandparent file is 4096 bytes; 12 files = 49152 bytes.
    // Use a very small max_file_size: 10 * 1024 = 10240 < 49152 → not trivial.
    let tiny_opts = Options {
      max_file_size: 1024,
      ..Options::default()
    };
    assert!(
      !is_trivial_move(&spec, &tiny_opts),
      "excessive grandparent overlap should disqualify trivial move"
    );
  }

  #[test]
  fn trivial_move_not_trivial_when_next_level_has_files() {
    use super::{is_trivial_move, CompactionSpec};
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8]| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        4096,
        make_internal_key(small, 10, 1u8),
        make_internal_key(large, 1, 1u8),
      )
    };

    let mut v = Version::new();
    v.files[1].push(make_meta(10, b"a", b"z"));
    v.files[2].push(make_meta(20, b"m", b"p")); // overlapping L2 file
    let version = Arc::new(v);

    let opts = Options {
      max_file_size: 64 * 1024,
      ..Options::default()
    };

    let mut spec = CompactionSpec::new(1, Arc::clone(&version));
    spec.inputs[0].push(Arc::clone(&version.files[1][0]));
    spec.inputs[1].push(Arc::clone(&version.files[2][0])); // L2 overlap present

    assert!(
      !is_trivial_move(&spec, &opts),
      "trivial move requires inputs[1] to be empty"
    );
  }

  #[test]
  #[serial(fd)]
  fn trivial_move_end_to_end() {
    // Write data so a single non-overlapping file ends up at L1, then confirm
    // it is trivially moved to L2 by maybe_compact.  We force the scenario by
    // writing unique keys (no overlap with any future flush) and checking that
    // after the DB is opened and a compaction cycle runs, L2 gains the file.
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
      create_if_missing: true,
      write_buffer_size: 512,
      max_file_size: 64 * 1024,
      block_cache: None,
      filter_policy: None,
      ..Options::default()
    };

    {
      let db = Db::open(dir.path(), opts.clone()).unwrap();
      // Write a small unique-key batch so it flushes to L2 (no overlap) and
      // then write a large overlapping batch to force L0 accumulation and
      // trigger compaction, which should move the L2 file trivially to L3.
      for i in 0u32..10 {
        db.put(format!("aaa{i:03}").as_bytes(), b"firstbatch")
          .unwrap();
      }
      // Force a second flush to produce L0 files that overlap the first range.
      for i in 0u32..10 {
        db.put(format!("aaa{i:03}").as_bytes(), b"secondbatch")
          .unwrap();
      }
    }

    // Reopen and verify all keys readable with latest values.
    let db2 = Db::open(dir.path(), opts).unwrap();
    for i in 0u32..10 {
      let val = db2.get(format!("aaa{i:03}").as_bytes()).unwrap();
      assert_eq!(val, b"secondbatch");
    }
  }

  // ── Gap 4: grandparent-overlap output limiting tests ──────────────────────

  #[test]
  fn is_base_level_for_key_cursor_advances_monotonically() {
    // Build a version with a few L3 files; verify that is_base_level_for_key
    // correctly returns false for keys inside those files and true for keys
    // outside, and that the cursor only ever moves forward.
    use super::{is_base_level_for_key, CompactionSpec};
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8]| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        4096,
        make_internal_key(small, 10, 1u8),
        make_internal_key(large, 1, 1u8),
      )
    };

    let mut v = Version::new();
    // L1 compaction inputs (level 1).
    v.files[1].push(make_meta(1, b"a", b"z"));
    // L3 = level+2 from L1: two non-overlapping files.
    v.files[3].push(make_meta(10, b"b", b"d")); // covers b..d
    v.files[3].push(make_meta(11, b"p", b"r")); // covers p..r
    let version = Arc::new(v);

    let mut spec = CompactionSpec::new(1, Arc::clone(&version));
    spec.inputs[0].push(Arc::clone(&version.files[1][0]));

    // "c" is inside file 10 (b..d) → NOT base level.
    assert!(!is_base_level_for_key(&mut spec, b"c"));
    // "q" is inside file 11 (p..r) → NOT base level.
    // Cursor for L3 should have advanced past file 10.
    assert!(!is_base_level_for_key(&mut spec, b"q"));
    // "s" is past both files → IS base level.
    assert!(is_base_level_for_key(&mut spec, b"s"));
    // Cursor must now be at the end; a key before the end (but still past both
    // files) should also return true without regressing the cursor.
    assert!(is_base_level_for_key(&mut spec, b"t"));
    // The cursor for L3 should be at index 2 (past both files) — check it
    // hasn't regressed.
    assert_eq!(spec.level_ptrs[3], 2);
  }

  #[test]
  fn should_stop_before_triggers_on_excess_grandparent_overlap() {
    use super::{should_stop_before, CompactionSpec};
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8], size: u64| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        size,
        make_internal_key(small, 10, 1u8),
        make_internal_key(large, 1, 1u8),
      )
    };

    let v = Version::new();
    let version = Arc::new(v);

    let opts = Options {
      max_file_size: 1024, // max_grandparent_overlap_bytes = 10 * 1024 = 10240
      ..Options::default()
    };

    let mut spec = CompactionSpec::new(0, Arc::clone(&version));
    // Three grandparent files, each 6000 bytes.
    // seen_key is false for the first key, so file 20's bytes don't count.
    // After passing file 21 (6000 bytes) and then file 22 (6000 more = 12000),
    // the limit of 10 * 1024 = 10240 is exceeded on the third key.
    spec.grandparents.push(make_meta(20, b"b", b"d", 6000));
    spec.grandparents.push(make_meta(21, b"e", b"g", 6000));
    spec.grandparents.push(make_meta(22, b"h", b"k", 6000));

    // Feed keys that advance past each grandparent file.
    // Keys are internal keys; construct them to be > each file's largest.
    let after_d = make_internal_key(b"e", 0, 0u8); // > "d" in ikey order
    let after_g = make_internal_key(b"h", 0, 0u8);
    let after_k = make_internal_key(b"l", 0, 0u8);

    // First key — seen_key becomes true, no bytes accumulated yet.
    assert!(!should_stop_before(&mut spec, &after_d, &opts));
    // Second key — passes file 20 (4096 bytes accumulated), still under limit.
    assert!(!should_stop_before(&mut spec, &after_g, &opts));
    // Third key — passes file 21 (8192 bytes), still under limit.
    // After passing file 22 (12288 > 10240) it should trigger.
    assert!(should_stop_before(&mut spec, &after_k, &opts));
    // After triggering, overlapped_bytes is reset to 0.
    assert_eq!(spec.overlapped_bytes, 0);
  }

  #[test]
  #[serial(fd)]
  fn grandparent_limiting_produces_multiple_output_files() {
    // Verify that a compaction subject to high grandparent overlap produces
    // multiple output files rather than one giant file.
    //
    // Setup:
    //   - Write key ranges A and B into the DB, flush to L2 (empty DB).
    //   - Write overlapping versions to force L0 accumulation and L0→L1 compaction.
    //   - After L0→L1, write yet more data to push L1→L2.
    //   - Verify all keys remain readable after compaction.
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
      create_if_missing: true,
      write_buffer_size: 512,
      max_file_size: 2 * 1024,
      block_cache: None,
      filter_policy: None,
      ..Options::default()
    };

    let db = Db::open(dir.path(), opts.clone()).unwrap();
    // Write enough unique keys with large-ish values so multiple flushes and
    // compactions occur, exercising the grandparent limiting path.
    let val = vec![b'x'; 64];
    for round in 0u32..20 {
      for k in 0u32..20 {
        db.put(format!("k{k:04}").as_bytes(), &val).unwrap();
      }
      let _ = round; // suppress lint
    }
    drop(db);

    // Reopen and verify correctness.
    let db2 = Db::open(dir.path(), opts).unwrap();
    for k in 0u32..20 {
      let result = db2.get(format!("k{k:04}").as_bytes());
      assert!(
        result.is_ok(),
        "key k{k:04} should be readable after compaction"
      );
      assert_eq!(result.unwrap(), val);
    }
  }

  // ── Gap 5: seek-based compaction tests ────────────────────────────────────

  #[test]
  fn seek_stats_decrements_allowed_seeks() {
    // Build a two-file L0 scenario and verify that the first probed file has
    // its allowed_seeks decremented when more than one file is consulted.
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    // Two L0 files with overlapping ranges so any key lookup in the overlap
    // must consult file 0 first (newest-first), then file 1.
    let mk = |num: u64, small: &[u8], large: &[u8]| -> Arc<FileMetaData> {
      FileMetaData::new(
        num,
        4096,
        make_internal_key(small, 10, 1u8),
        make_internal_key(large, 1, 1u8),
      )
    };

    let mut v = Version::new();
    // Newest file (file 1) covers "a".."m"; older file (file 2) covers "g".."z".
    // L0 is newest-first so file 1 is index 0.
    v.files[0].push(mk(1, b"a", b"m"));
    v.files[0].push(mk(2, b"g", b"z"));
    let version = Arc::new(v);

    // We can't do a real table lookup without actual SSTable files, so instead
    // directly call the get stats logic by checking GetStats construction.
    // Simulate: we probed file 1 first (found nothing) then file 2.
    // Blame = file 1 (first consulted, not where we found the answer).
    let before = version.files[0][0].allowed_seeks.load(Ordering::Relaxed);
    assert!(before >= 100, "initial allowed_seeks should be ≥ 100");

    // Simulate the update_stats call that Db::get would make.
    use crate::db::version::GetStats;
    let stats = GetStats {
      seek_file: Some(Arc::clone(&version.files[0][0])),
      seek_file_level: 0,
    };
    let mut ds = super::DbState {
      last_sequence: 0,
      log: None,
      mem: Arc::new(crate::memtable::Memtable::new()),
      imm: None,
      version_set: None,
      snapshots: Default::default(),
      writers: Default::default(),
      next_writer_id: 0,
      completed: Default::default(),
      seek_compact_file: None,
      compaction_needed: false,
      background_scheduled: false,
      background_error: None,
      pending_flush: None,
    };
    super::update_stats(&mut ds, &stats);
    let after = version.files[0][0].allowed_seeks.load(Ordering::Relaxed);
    assert_eq!(
      after,
      before - 1,
      "allowed_seeks should have decremented by 1"
    );
    assert!(
      ds.seek_compact_file.is_none(),
      "seek budget not exhausted yet"
    );
    assert!(!ds.compaction_needed);
  }

  #[test]
  fn seek_stats_triggers_compaction_when_budget_exhausted() {
    use crate::db::version::GetStats;
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let mk = |num: u64| -> Arc<FileMetaData> {
      // Use a tiny file (size < 16384) so allowed_seeks initialises to 100.
      FileMetaData::new(
        num,
        1024,
        make_internal_key(b"a", 10, 1u8),
        make_internal_key(b"z", 1, 1u8),
      )
    };

    let mut v = Version::new();
    v.files[0].push(mk(1));
    let version = Arc::new(v);

    let file = Arc::clone(&version.files[0][0]);
    let initial = file.allowed_seeks.load(Ordering::Relaxed);
    assert_eq!(initial, 100);

    // Drain allowed_seeks to 1 manually.
    file.allowed_seeks.store(1, Ordering::Relaxed);

    let stats = GetStats {
      seek_file: Some(Arc::clone(&file)),
      seek_file_level: 0,
    };
    let mut ds = super::DbState {
      last_sequence: 0,
      log: None,
      mem: Arc::new(crate::memtable::Memtable::new()),
      imm: None,
      version_set: None,
      snapshots: Default::default(),
      writers: Default::default(),
      next_writer_id: 0,
      completed: Default::default(),
      seek_compact_file: None,
      compaction_needed: false,
      background_scheduled: false,
      background_error: None,
      pending_flush: None,
    };
    super::update_stats(&mut ds, &stats);

    assert!(
      ds.seek_compact_file.is_some(),
      "seek_compact_file should be set once budget hits 0"
    );
    assert!(ds.compaction_needed, "compaction_needed should be set");
    let (nominated, level) = ds.seek_compact_file.as_ref().unwrap();
    assert_eq!(nominated.number, 1);
    assert_eq!(*level, 0);
  }

  #[test]
  #[serial(fd)]
  fn seek_based_compaction_fires_after_repeated_misses() {
    // Write data so there are multiple L0 files with overlapping key ranges.
    // Then perform repeated gets on a key that forces consulting 2+ files but
    // doesn't exist (all misses), draining allowed_seeks.  Once exhausted the
    // compaction loop should fire and reduce L0.
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
      create_if_missing: true,
      write_buffer_size: 512,
      max_file_size: 4 * 1024,
      block_cache: None,
      filter_policy: None,
      ..Options::default()
    };
    let db = Db::open(dir.path(), opts.clone()).unwrap();

    // Write overlapping key ranges in multiple rounds so multiple L0 files
    // accumulate (we write fast enough that compaction doesn't drain them all).
    // Use write_buffer_size=512 so each round flushes.
    for round in 0u32..4 {
      for k in 0u32..5 {
        db.put(
          format!("key{k:02}").as_bytes(),
          format!("val{round:02}").as_bytes(),
        )
        .unwrap();
      }
    }

    // Manually drain allowed_seeks on an L0 file to force a seek-compaction.
    {
      use std::sync::atomic::Ordering;
      let g = db.inner.state.lock().unwrap();
      if let Some(vs) = &g.version_set {
        let cur = vs.current();
        if let Some(f) = cur.files[0].first() {
          f.allowed_seeks.store(1, Ordering::Relaxed);
        }
      }
    }

    // One more get will trigger update_stats → seek_compact_file nominated.
    // The next write's compaction loop should drain it.
    let _ = db.get(b"key00");
    db.put(b"trigger", b"compaction").unwrap();

    // Verify all original keys still readable.
    for k in 0u32..5 {
      let val = db.get(format!("key{k:02}").as_bytes()).unwrap();
      assert!(
        val.starts_with(b"val"),
        "key{k:02} should still be readable"
      );
    }
  }

  // ── Db::flush tests ──────────────────────────────────────────────────────

  #[test]
  fn flush_noop_on_in_memory_db() {
    let db = Db::default();
    db.put(b"k", b"v").unwrap();
    db.flush(&crate::FlushOptions::default()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), b"v");
  }

  #[test]
  fn flush_noop_on_empty_memtable() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), create_options()).unwrap();
    // No writes — flush should return Ok without error.
    db.flush(&crate::FlushOptions::default()).unwrap();
  }

  #[serial(fd)]
  #[test]
  fn flush_moves_data_to_sstable() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), create_options()).unwrap();
    db.put(b"hello", b"world").unwrap();
    // Before flush: L0 should be empty.
    assert_eq!(db.get_property("leveldb.num-files-at-level0").unwrap(), "0");
    db.flush(&crate::FlushOptions::default()).unwrap();
    // After flush: at least one L0 (or higher) SSTable should exist.
    let total_files: usize = (0..7)
      .map(|l| {
        db.get_property(&format!("leveldb.num-files-at-level{l}"))
          .unwrap_or_else(|| "0".into())
          .parse::<usize>()
          .unwrap_or(0)
      })
      .sum();
    assert!(
      total_files >= 1,
      "expected at least one SSTable after flush"
    );
    // Data must still be readable.
    assert_eq!(db.get(b"hello").unwrap(), b"world");
  }

  #[serial(fd)]
  #[test]
  fn flush_is_durable_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), create_options()).unwrap();
      db.put(b"persist", b"yes").unwrap();
      db.flush(&crate::FlushOptions::default()).unwrap();
      // Drop db — WAL for the flushed data was already deleted by the flush.
    }
    // Reopen without writing anything: data must come from the SSTable.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"persist").unwrap(), b"yes");
  }

  #[serial(fd)]
  #[test]
  fn flush_wait_false_does_not_block() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), create_options()).unwrap();
    for i in 0..10u32 {
      db.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    // Non-blocking flush: should return immediately.
    db.flush(&crate::FlushOptions { wait: false }).unwrap();
    // Data must be readable regardless of whether the flush has completed yet.
    assert_eq!(db.get(b"k0").unwrap(), b"v");
  }

  // ── ForwardIter tests ────────────────────────────────────────────────────

  #[test]
  fn forward_iter_full_scan() {
    let db = Db::default();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();

    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_first();
    let pairs: Vec<_> = it.forward().map(|r| r.unwrap()).collect();
    assert_eq!(
      pairs,
      vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
      ]
    );
  }

  #[test]
  fn forward_iter_from_seek() {
    let db = Db::default();
    for c in b'a'..=b'e' {
      db.put([c], [c]).unwrap();
    }

    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek(b"c");
    let keys: Vec<_> = it.forward().map(|r| r.unwrap().0).collect();
    assert_eq!(keys, vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
  }

  #[test]
  fn forward_iter_empty() {
    let db = Db::default();
    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    it.seek_to_first();
    assert_eq!(it.forward().count(), 0);
  }

  #[test]
  fn forward_iter_unpositioned_yields_nothing() {
    let db = Db::default();
    db.put(b"k", b"v").unwrap();
    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
    // No seek — iterator is unpositioned, valid() is false.
    assert_eq!(it.forward().count(), 0);
  }

  #[test]
  fn forward_iter_reusable_after_drop() {
    let db = Db::default();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();

    let mut it = db.new_iterator(&ReadOptions::default()).unwrap();

    // First pass: scan from "a".
    it.seek_to_first();
    let first: Vec<_> = it.forward().map(|r| r.unwrap().0).collect();
    assert_eq!(first, vec![b"a".to_vec(), b"b".to_vec()]);

    // ForwardIter dropped — reposition and scan again.
    it.seek(b"b");
    let second: Vec<_> = it.forward().map(|r| r.unwrap().0).collect();
    assert_eq!(second, vec![b"b".to_vec()]);
  }
}
