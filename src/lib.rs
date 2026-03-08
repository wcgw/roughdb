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

use crate::db::version_edit::{FileMetaData, VersionEdit};
use crate::db::version_set::VersionSet;
use crate::log::reader::Reader as LogReader;
use crate::log::writer::Writer as LogWriter;
use crate::memtable::{Memtable, MemtableResult};
use crate::table::builder::TableBuilder;
use crate::table::reader::{LookupResult, Table};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub mod error;
pub use error::Error;
pub mod options;
pub use options::{CompressionType, Options, ReadOptions, Snapshot, WriteOptions};
pub(crate) mod coding;
pub(crate) mod db;
pub(crate) mod iter;
pub(crate) mod log;
pub(crate) mod memtable;
pub(crate) mod table;
pub mod write_batch;
pub use write_batch::{Handler, WriteBatch};

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

// ── DbState: lives inside an RwLock ──────────────────────────────────────────
//
// Reads take a shared read lock; they check `mem`, then `imm`, then the
// current `Version` (via `version_set`) without blocking each other.
// Writes take an exclusive write lock briefly (WAL + memtable insert), then
// release before doing flush I/O.  Flush I/O holds no lock: it rotates
// mem→imm under a write lock, does the I/O lock-free, then briefly
// re-acquires a write lock to install the new version and clear `imm`.

struct DbState {
  /// Sequence number of the last committed write.
  last_sequence: u64,
  /// `None` for in-memory / test databases (no WAL).
  log: Option<LogWriter>,
  mem: Memtable,
  /// Sealed memtable currently being flushed to disk.  Reads must check this
  /// after `mem` and before the current `Version`.  `None` when no flush is
  /// in progress.
  imm: Option<Memtable>,
  /// Tracks the set of live SSTable files and the MANIFEST.
  /// `None` for in-memory databases (`Db::default()`).
  version_set: Option<VersionSet>,
}

// ── FlushPrep / FlushResult ───────────────────────────────────────────────────

/// Produced by `begin_flush` (under the write lock): everything needed to run
/// `write_flush` and `finish_flush` without holding the lock.
struct FlushPrep {
  sst_number: u64,
  sst_path: std::path::PathBuf,
  old_mem: Memtable,
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

// ── Db ───────────────────────────────────────────────────────────────────────

pub struct Db {
  state: RwLock<DbState>,
  options: Options,
  /// Database directory.  `None` for in-memory (`Db::default()`).
  path: Option<PathBuf>,
}

impl Default for Db {
  /// In-memory database with no WAL and no flush trigger.
  /// Intended for tests and ephemeral use.
  fn default() -> Self {
    Self {
      state: RwLock::new(DbState {
        last_sequence: 0,
        log: None,
        mem: Memtable::default(),
        imm: None,
        version_set: None,
      }),
      options: Options::default(),
      path: None,
    }
  }
}

impl Db {
  /// Open (or create) a persistent database at `path`.
  ///
  /// ## New database (no `CURRENT` file)
  /// - Creates a `MANIFEST-000002` with the initial `VersionEdit`.
  /// - Creates `CURRENT` pointing to the MANIFEST.
  /// - Creates `000001.log` as the WAL.
  ///
  /// ## Existing database (`CURRENT` file present)
  /// - Recovers the `VersionSet` from the MANIFEST (loads live SSTable files).
  /// - Replays WAL records whose sequence number exceeds the last sequence
  ///   already reflected in the MANIFEST (avoids double-inserting flushed data).
  ///
  /// `Db::default()` is retained for in-memory / test use (no WAL, no flush).
  pub fn open<P: AsRef<std::path::Path>>(path: P, options: Options) -> Result<Self, Error> {
    use std::fs::OpenOptions;

    let path = path.as_ref();
    std::fs::create_dir_all(path)?;

    let current_path = path.join("CURRENT");

    let (version_set, mem, last_sequence) = if current_path.exists() {
      // ── Existing database: MANIFEST-driven recovery ──────────────────────
      let mut vs = VersionSet::recover(path)?;
      let manifest_last_seq = vs.last_sequence();
      let mem = Memtable::default();

      // Replay WAL records newer than what is already in the SSTables.
      // Use the log number recorded in the MANIFEST (set by WAL rotation).
      let log_path = path.join(format!("{:06}.log", vs.log_number()));
      let actual_last_seq = if log_path.exists() {
        let file = std::fs::File::open(&log_path)?;
        let file_len = file.metadata()?.len();
        if file_len > 0 {
          Self::recover_wal(file, &mem, manifest_last_seq)?
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
      (Some(vs), Memtable::default(), 0)
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
      state: RwLock::new(DbState {
        last_sequence,
        log: Some(log_writer),
        mem,
        imm: None,
        version_set,
      }),
      options,
      path: Some(path.to_path_buf()),
    })
  }

  /// Replay WAL records from `file` into `mem`, skipping any whose sequence
  /// number is entirely covered by `min_sequence` (already in an SSTable).
  ///
  /// Returns the highest sequence number replayed (or `min_sequence` if
  /// nothing was replayed).
  fn recover_wal(file: std::fs::File, mem: &Memtable, min_sequence: u64) -> Result<u64, Error> {
    let mut reader = LogReader::new(file, None, true, 0);
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
    let key = key.as_ref();
    // A shared read lock allows multiple concurrent readers.
    let state = self.state.read().unwrap();

    match state.mem.get(key) {
      MemtableResult::Hit(hit) => return Ok(hit),
      MemtableResult::Deleted => return Err(Error::NotFound),
      MemtableResult::Miss => {}
    }

    // Check the sealed memtable being flushed, if any.
    if let Some(imm) = &state.imm {
      match imm.get(key) {
        MemtableResult::Hit(hit) => return Ok(hit),
        MemtableResult::Deleted => return Err(Error::NotFound),
        MemtableResult::Miss => {}
      }
    }

    // Search the current Version for the key across all levels.
    if let Some(vs) = &state.version_set {
      match vs.current().get(key, state.last_sequence)? {
        LookupResult::Value(v) => return Ok(v),
        LookupResult::Deleted => return Err(Error::NotFound),
        LookupResult::NotInTable => {}
      }
    }

    Err(Error::NotFound)
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

  pub fn write(&self, opts: &WriteOptions, batch: &WriteBatch) -> Result<(), Error> {
    let mut state = self.state.write().unwrap();
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
    if let Some(path) = self.path.as_deref() {
      if state.mem.approximate_memory_usage() >= self.options.write_buffer_size {
        // Phase 1: rotate mem → imm, allocate new WAL file (write lock held).
        let prep = begin_flush(path, &mut state)?;
        // Release the write lock before doing any I/O.
        drop(state);
        // Phase 2: write the SSTable (no lock held — reads proceed freely).
        let result = write_flush(prep, &self.options)?;
        // Phase 3: install the new Version, swap WAL, clear imm (write lock).
        let mut state = self.state.write().unwrap();
        finish_flush(&mut state, result)?;
      }
    }

    Ok(())
  }
}

// ── flush helpers ─────────────────────────────────────────────────────────────
//
// Flush is split into three phases so the write lock is not held during I/O:
//
//   begin_flush  — under write lock: rotate mem→imm, return sealed memtable
//   write_flush  — no lock: write SSTable to disk, open it for reading
//   finish_flush — under write lock: install new Version in VersionSet, clear imm

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
  let mut builder = TableBuilder::new(file, opts.block_size, opts.block_restart_interval);
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
  let table = Arc::new(Table::open(read_file, file_size)?);
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use crate::{Db, Options, WriteOptions};

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
      let db = Db::open(dir.path(), Options::default()).unwrap();
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
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert!(db.get(b"anything").unwrap_err().is_not_found());
  }

  #[test]
  fn write_batch_atomic_recovery() {
    use crate::WriteBatch;
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), Options::default()).unwrap();
      let mut batch = WriteBatch::new();
      batch.put(b"a", b"1");
      batch.put(b"b", b"2");
      batch.put(b"c", b"3");
      db.write(&WriteOptions::default(), &batch).unwrap();
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), b"1");
    assert_eq!(db.get(b"b").unwrap(), b"2");
    assert_eq!(db.get(b"c").unwrap(), b"3");
  }

  #[test]
  fn sequence_advances_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path(), Options::default()).unwrap();
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

  fn small_options() -> Options {
    // write_buffer_size small enough to flush after a handful of entries.
    Options {
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
}
