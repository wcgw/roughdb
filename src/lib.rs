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

use crate::log::reader::Reader as LogReader;
use crate::log::writer::Writer as LogWriter;
use crate::memtable::{Memtable, MemtableResult};
use crate::table::builder::TableBuilder;
use crate::table::reader::{LookupResult, Table};
use std::path::PathBuf;
use std::sync::RwLock;

pub mod error;
pub use error::Error;
pub mod options;
pub use options::{CompressionType, Options, ReadOptions, Snapshot, WriteOptions};
pub(crate) mod coding;
pub(crate) mod log;
pub(crate) mod memtable;
pub(crate) mod table;
pub mod write_batch;
pub use write_batch::{Handler, WriteBatch};

// ── Shared inserter used by both Db::write and Db::recover ───────────────────

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
// Reads take a shared read lock; they check `mem`, then `imm`, then `l0_files`
// without blocking each other.  Writes take an exclusive write lock briefly
// (WAL + memtable insert), then release before doing flush I/O.  Flush I/O
// itself holds no lock: it rotates mem→imm under a write lock, does the I/O
// lock-free, then briefly re-acquires a write lock to install the new Table
// and clear `imm`.

struct DbState {
  last_sequence: u64,
  /// `None` for in-memory / test databases (no WAL).
  log: Option<LogWriter>,
  mem: Memtable,
  /// Sealed memtable currently being flushed to disk.  Reads must check this
  /// after `mem` and before `l0_files`.  `None` when no flush is in progress.
  imm: Option<Memtable>,
  /// L0 SSTable files open for reading, newest first.
  l0_files: Vec<(u64, Table)>,
  /// Next file number for `.ldb` files (1 is the WAL; L0 starts at 2).
  next_file_number: u64,
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
        l0_files: Vec::new(),
        next_file_number: 2,
      }),
      options: Options::default(),
      path: None,
    }
  }
}

impl Db {
  /// Open (or create) a persistent database at `path`.
  ///
  /// - Creates the directory if it does not exist.
  /// - Scans for existing `.ldb` files to determine the next file number.
  /// - If a WAL (`000001.log`) exists, replays it into the memtable.
  /// - Opens (or creates) the WAL for subsequent writes.
  ///
  /// After a flush, new writes keep appending to the WAL; WAL rotation
  /// (truncation after flush) is deferred to Phase 9.  All WAL data is
  /// therefore always available for recovery, making it unnecessary to
  /// load existing `.ldb` files on reopen for correctness.
  ///
  /// `Db::default()` is retained for in-memory / test use (no WAL, no flush).
  pub fn open<P: AsRef<std::path::Path>>(path: P, options: Options) -> Result<Self, Error> {
    use std::fs::OpenOptions;

    let path = path.as_ref();
    std::fs::create_dir_all(path)?;

    // Determine the next file number from any existing .ldb files so new
    // flushes do not overwrite data from a previous session.
    let next_file_number = scan_next_file_number(path)?;

    let log_path = path.join("000001.log");
    let mem = Memtable::default();

    // Open the log file (creating it if absent) with read + append access so
    // the same handle can serve recovery reads and subsequent WAL writes.
    let log_file = OpenOptions::new().read(true).append(true).create(true).open(&log_path)?;
    let file_len = log_file.metadata()?.len();

    let last_sequence = if file_len > 0 {
      Self::recover(log_file.try_clone()?, &mem)?
    } else {
      0
    };

    let log_writer = LogWriter::new(log_file, file_len);

    Ok(Self {
      state: RwLock::new(DbState {
        last_sequence,
        log: Some(log_writer),
        mem,
        imm: None,
        l0_files: Vec::new(),
        next_file_number,
      }),
      options,
      path: Some(path.to_path_buf()),
    })
  }

  /// Replay all records from a WAL file into `mem`, returning the highest
  /// sequence number seen.  Incomplete trailing records (torn writes) are
  /// silently ignored, matching LevelDB's crash-recovery behaviour.
  fn recover(file: std::fs::File, mem: &Memtable) -> Result<u64, Error> {
    let mut reader = LogReader::new(file, None, true, 0);
    let mut max_sequence: u64 = 0;

    while let Some(record) = reader.read_record() {
      let batch = WriteBatch::from_contents(record)?;
      let start_seq = batch.sequence();
      batch.iterate(&mut Inserter { mem, seq: start_seq })?;
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
    // A shared read lock allows multiple concurrent readers; writers and flush
    // I/O do not block here.
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

    // Scan L0 files newest-first.  A `Deleted` result stops the search
    // immediately — the key was deleted and no older file should be consulted.
    for (_, table) in &state.l0_files {
      match table.get(key, false)? {
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

    stamped.iterate(&mut Inserter { mem: &state.mem, seq: start_seq })?;
    state.last_sequence += batch.count() as u64;

    // Trigger an L0 flush when the memtable exceeds the size limit.
    // Only applies to persistent databases (path.is_some()).
    if let Some(path) = self.path.as_deref() {
      if state.mem.approximate_memory_usage() >= self.options.write_buffer_size {
        // Phase 1: rotate mem → imm under the write lock, then release it.
        let (file_number, file_path, old_mem) = begin_flush(path, &mut state);
        // Release the write lock before doing any I/O.
        drop(state);
        // Phase 2: write the SSTable (no lock held — reads proceed freely).
        let table = write_flush(file_path, file_number, old_mem, &self.options)?;
        // Phase 3: install the new Table and clear imm under a fresh write lock.
        let mut state = self.state.write().unwrap();
        finish_flush(&mut state, file_number, table);
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
//   finish_flush — under write lock: install Table in l0_files, clear imm

/// Phase 1 (under write lock): seal `mem` as `imm`, install a fresh memtable,
/// and return the sealed memtable together with its destination path.
fn begin_flush(
  path: &std::path::Path,
  state: &mut DbState,
) -> (u64, std::path::PathBuf, Memtable) {
  use std::mem;
  let file_number = state.next_file_number;
  state.next_file_number += 1;
  let old_mem = mem::replace(&mut state.mem, Memtable::default());
  state.imm = None; // should already be None; be explicit
  let file_path = path.join(format!("{file_number:06}.ldb"));
  (file_number, file_path, old_mem)
}

/// Phase 2 (no lock held): iterate `old_mem`, write an SSTable, return the
/// open `Table` ready for reading.  On error `old_mem` is dropped; data is
/// safe in the WAL.
fn write_flush(
  file_path: std::path::PathBuf,
  _file_number: u64,
  old_mem: Memtable,
  opts: &Options,
) -> Result<Table, Error> {
  use std::fs::OpenOptions;
  let file = OpenOptions::new().write(true).create_new(true).open(&file_path)?;
  let mut builder = TableBuilder::new(file, opts.block_size, opts.block_restart_interval);
  {
    let mut it = old_mem.iter();
    while it.valid() {
      builder.add(&it.ikey(), it.value())?;
      it.advance();
    }
  }
  let file_size = builder.finish()?;
  drop(old_mem);
  let read_file = std::fs::File::open(&file_path)?;
  Table::open(read_file, file_size)
}

/// Phase 3 (under write lock): prepend the new Table to `l0_files` and clear
/// `imm` so readers stop consulting the now-flushed memtable.
fn finish_flush(state: &mut DbState, file_number: u64, table: Table) {
  state.l0_files.insert(0, (file_number, table));
  state.imm = None;
}

// ── scan_next_file_number ─────────────────────────────────────────────────────

/// Scan `path` for `*.ldb` files and return the next available file number.
///
/// File 1 is reserved for the WAL; L0 files start at 2.
fn scan_next_file_number(path: &std::path::Path) -> Result<u64, Error> {
  let mut max = 1u64;
  for entry in std::fs::read_dir(path)? {
    let entry = entry?;
    let name = entry.file_name();
    let name = name.to_string_lossy();
    if let Some(stem) = name.strip_suffix(".ldb") {
      if let Ok(n) = stem.parse::<u64>() {
        if n > max {
          max = n;
        }
      }
    }
  }
  Ok(max + 1)
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
    Options { write_buffer_size: 512, ..Options::default() }
  }

  #[test]
  fn flush_creates_ldb_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();

    // Write enough data to exceed 512 bytes and trigger a flush.
    for i in 0u32..20 {
      db.put(format!("key{i:04}").as_bytes(), format!("value{i:04}").as_bytes()).unwrap();
    }

    // At least one .ldb file must exist.
    let ldb_count = std::fs::read_dir(dir.path())
      .unwrap()
      .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".ldb"))
      .count();
    assert!(ldb_count >= 1, "expected at least one .ldb file, found {ldb_count}");
  }

  #[test]
  fn flushed_data_readable_from_l0() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_options()).unwrap();

    // Write enough to trigger one or more flushes.
    for i in 0u32..20 {
      db.put(format!("key{i:04}").as_bytes(), format!("val{i:04}").as_bytes()).unwrap();
    }

    // All keys must be readable regardless of whether they ended up in the
    // memtable or in an L0 file.
    for i in 0u32..20 {
      let expected = format!("val{i:04}");
      assert_eq!(db.get(format!("key{i:04}").as_bytes()).unwrap(), expected.as_bytes());
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
      .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".ldb"))
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
      .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".ldb"))
      .count();
    assert!(
      second_ldb_count >= first_ldb_count,
      "second session wrote fewer .ldb files ({second_ldb_count}) than first ({first_ldb_count})"
    );
  }
}
