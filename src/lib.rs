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
use std::sync::Mutex;

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

// ── WriteState: lives inside the write mutex ─────────────────────────────────

struct WriteState {
  last_sequence: u64,
  /// `None` for in-memory / test databases (no WAL).
  log: Option<LogWriter>,
}

// ── Db ───────────────────────────────────────────────────────────────────────

pub struct Db {
  /// Guards all SkipList mutations. Payload carries the last committed
  /// sequence number and the optional WAL writer.
  write_lock: Mutex<WriteState>,
  mem: Memtable,
  imm: Option<Memtable>,
  // disk: (later phases)
}

impl Default for Db {
  /// In-memory database with no WAL. Intended for tests and ephemeral use.
  fn default() -> Self {
    Self {
      write_lock: Mutex::new(WriteState { last_sequence: 0, log: None }),
      mem: Memtable::default(),
      imm: None,
    }
  }
}

impl Db {
  /// Open (or create) a persistent database at `path`.
  ///
  /// - Creates the directory if it does not exist.
  /// - If a WAL (`000001.log`) exists, replays it to reconstruct the memtable
  ///   and restore `last_sequence`.
  /// - Opens (or creates) the WAL for subsequent writes.
  ///
  /// `Db::default()` is retained for in-memory / test use (no WAL).
  /// Full MANIFEST-driven log-number tracking is deferred to Phase 9.
  pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, Error> {
    use std::fs::OpenOptions;

    let path = path.as_ref();
    std::fs::create_dir_all(path)?;

    let log_path = path.join("000001.log");
    let mem = Memtable::default();

    // Open the log file (creating it if absent) with read + append access so
    // the same handle can serve recovery reads and subsequent WAL writes.
    // O_APPEND ensures every write lands at the end of the file regardless of
    // the read position left by recovery.
    let log_file = OpenOptions::new().read(true).append(true).create(true).open(&log_path)?;
    let file_len = log_file.metadata()?.len();

    let last_sequence = if file_len > 0 {
      Self::recover(log_file.try_clone()?, &mem)?
    } else {
      0
    };

    let log_writer = LogWriter::new(log_file, file_len);

    Ok(Self {
      write_lock: Mutex::new(WriteState { last_sequence, log: Some(log_writer) }),
      mem,
      imm: None,
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

    match self.mem.get(key) {
      MemtableResult::Hit(hit) => return Ok(hit),
      MemtableResult::Deleted => return Err(Error::NotFound),
      MemtableResult::Miss => {}
    }

    if let Some(imm) = self.imm.as_ref() {
      match imm.get(key) {
        MemtableResult::Hit(hit) => return Ok(hit),
        MemtableResult::Deleted => return Err(Error::NotFound),
        MemtableResult::Miss => {}
      }
    }

    // return self.disk
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
    let mut state = self.write_lock.lock().unwrap();
    let start_seq = state.last_sequence + 1;

    // Clone and stamp so the WAL record carries the correct embedded sequence
    // for recovery replay.
    let mut stamped = batch.clone();
    stamped.set_sequence(start_seq);

    if let Some(log) = state.log.as_mut() {
      log.add_record(stamped.contents())?;
      if opts.sync {
        log.sync()?;
      }
    }

    stamped.iterate(&mut Inserter { mem: &self.mem, seq: start_seq })?;
    state.last_sequence += batch.count() as u64;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::{Db, WriteOptions};

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
      let db = Db::open(dir.path()).unwrap();
      db.put(b"key", b"value").unwrap();
      db.put(b"foo", b"bar").unwrap();
      db.delete(b"foo").unwrap();
    }
    // Reopen: recovery must restore state from the WAL.
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"key").unwrap(), b"value");
    assert!(db.get(b"foo").unwrap_err().is_not_found());
  }

  #[test]
  fn open_empty_db_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    assert!(db.get(b"anything").unwrap_err().is_not_found());
  }

  #[test]
  fn write_batch_atomic_recovery() {
    use crate::WriteBatch;
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path()).unwrap();
      let mut batch = WriteBatch::new();
      batch.put(b"a", b"1");
      batch.put(b"b", b"2");
      batch.put(b"c", b"3");
      db.write(&WriteOptions::default(), &batch).unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), b"1");
    assert_eq!(db.get(b"b").unwrap(), b"2");
    assert_eq!(db.get(b"c").unwrap(), b"3");
  }

  #[test]
  fn sequence_advances_across_reopen() {
    // Sequence numbers must be monotonically increasing across reopens so that
    // entries written after recovery always shadow entries written before it.
    let dir = tempfile::tempdir().unwrap();
    {
      let db = Db::open(dir.path()).unwrap();
      db.put(b"k", b"first").unwrap();
    }
    {
      let db = Db::open(dir.path()).unwrap();
      db.put(b"k", b"second").unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), b"second");
  }
}
