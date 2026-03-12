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

use crate::cache::BlockCache;
use crate::error::Error;
use crate::filter::FilterPolicy;
use crate::table::reader::Table;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Files reserved for non-SSTable uses (WAL, MANIFEST, CURRENT, LOCK, etc.).
///
/// Matches `kNumNonTableCacheFiles` in LevelDB's `db/db_impl.cc`.
pub(crate) const NUM_NON_TABLE_CACHE_FILES: usize = 10;

struct Inner {
  path: PathBuf,
  filter_policy: Option<Arc<dyn FilterPolicy>>,
  block_cache: Option<Arc<BlockCache>>,
  /// Maximum number of open `Table` handles the cache will hold at once.
  capacity: usize,
  /// LRU order: front = least-recently used, back = most-recently used.
  order: VecDeque<u64>,
  /// Maps file number → open Table handle.
  map: HashMap<u64, Arc<Table>>,
}

impl Inner {
  /// Return the open `Table` for `number`, opening (and possibly evicting) as needed.
  fn get_or_open(&mut self, number: u64, file_size: u64) -> Result<Arc<Table>, Error> {
    if let Some(t) = self.map.get(&number) {
      // Cache hit: promote to MRU position.
      if let Some(pos) = self.order.iter().position(|&n| n == number) {
        self.order.remove(pos);
      }
      self.order.push_back(number);
      return Ok(Arc::clone(t));
    }

    // Cache miss: evict LRU entry if at capacity.
    if self.map.len() >= self.capacity {
      if let Some(evict) = self.order.pop_front() {
        self.map.remove(&evict);
      }
    }

    // Open the SSTable file and parse the footer + index block.
    let sst_path = self.path.join(format!("{number:06}.ldb"));
    let file = File::open(&sst_path)
      .map_err(|e| Error::Corruption(format!("cannot open SSTable {number:06}.ldb: {e}")))?;
    let table = Arc::new(Table::open(
      file,
      file_size,
      self.filter_policy.clone(),
      self.block_cache.clone(),
    )?);

    self.order.push_back(number);
    self.map.insert(number, Arc::clone(&table));
    Ok(table)
  }

  /// Insert a `Table` that was just built (no need to open from disk).
  ///
  /// If `number` is already present, the existing entry is replaced.
  /// Evicts LRU if at capacity before inserting a new entry.
  fn insert(&mut self, number: u64, table: Arc<Table>) {
    if !self.map.contains_key(&number) && self.map.len() >= self.capacity {
      if let Some(evict) = self.order.pop_front() {
        self.map.remove(&evict);
      }
    }
    // Promote / insert.
    if let Some(pos) = self.order.iter().position(|&n| n == number) {
      self.order.remove(pos);
    }
    self.order.push_back(number);
    self.map.insert(number, table);
  }

  /// Remove the entry for `number` (called when the file is deleted by compaction).
  fn evict(&mut self, number: u64) {
    if self.map.remove(&number).is_some() {
      if let Some(pos) = self.order.iter().position(|&n| n == number) {
        self.order.remove(pos);
      }
    }
  }
}

/// A bounded LRU cache of open SSTable file handles, keyed by file number.
///
/// Capacity = `Options::max_open_files - NUM_NON_TABLE_CACHE_FILES`.  When the
/// cache is full and a new file must be opened, the least-recently-used entry is
/// evicted (its `Arc<Table>` is dropped, closing the file once no iterator holds
/// a reference).
///
/// `TableCache` is `Clone` (backed by an `Arc<Mutex<_>>`) so it can be shared
/// between the state-locked path and lock-free I/O paths without a separate
/// synchronisation level.
///
/// Matches LevelDB's `TableCache` (`db/table_cache.h/cc`).
#[derive(Clone)]
pub(crate) struct TableCache(Arc<Mutex<Inner>>);

impl TableCache {
  /// Create a new cache backed by the SSTable files in `path`.
  ///
  /// `capacity` is the maximum number of simultaneously open `Table` handles.
  /// If `capacity` is 0 it is silently raised to 1.
  pub(crate) fn new(
    path: &Path,
    capacity: usize,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
    block_cache: Option<Arc<BlockCache>>,
  ) -> Self {
    TableCache(Arc::new(Mutex::new(Inner {
      path: path.to_owned(),
      filter_policy,
      block_cache,
      capacity: capacity.max(1),
      order: VecDeque::new(),
      map: HashMap::new(),
    })))
  }

  /// Return (or lazily open) the `Table` for `number`.
  pub(crate) fn get_or_open(&self, number: u64, file_size: u64) -> Result<Arc<Table>, Error> {
    self.0.lock().unwrap().get_or_open(number, file_size)
  }

  /// Insert a `Table` that was just created (flush or compaction output).
  ///
  /// Avoids a redundant file-open when we already have the handle in memory.
  pub(crate) fn insert(&self, number: u64, table: Arc<Table>) {
    self.0.lock().unwrap().insert(number, table);
  }

  /// Evict the entry for `number` (called when the file is garbage-collected).
  pub(crate) fn evict(&self, number: u64) {
    self.0.lock().unwrap().evict(number);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::options::CompressionType;
  use crate::table::builder::TableBuilder;
  use crate::table::format::make_internal_key;

  fn write_sst(dir: &Path, number: u64, entries: &[(&[u8], &[u8])]) -> u64 {
    let path = dir.join(format!("{number:06}.ldb"));
    let file = File::create(&path).unwrap();
    let mut b = TableBuilder::new(file, 4096, 16, None, CompressionType::NoCompression);
    for &(k, v) in entries {
      b.add(&make_internal_key(k, 1, 1), v).unwrap();
    }
    b.finish().unwrap()
  }

  #[test]
  fn get_or_open_opens_sst() {
    let dir = tempfile::tempdir().unwrap();
    let size = write_sst(dir.path(), 3, &[(b"hello", b"world")]);
    let tc = TableCache::new(dir.path(), 10, None, None);
    let table = tc.get_or_open(3, size).unwrap();
    use crate::table::reader::LookupResult;
    assert!(
      matches!(table.get(b"hello", false, true).unwrap(), LookupResult::Value(v) if v == b"world")
    );
  }

  #[test]
  fn insert_bypasses_disk_open() {
    let dir = tempfile::tempdir().unwrap();
    let size = write_sst(dir.path(), 3, &[(b"k", b"v")]);
    // Open manually, then insert — cache should return the same Arc.
    let file = File::open(dir.path().join("000003.ldb")).unwrap();
    let table = Arc::new(Table::open(file, size, None, None).unwrap());
    let tc = TableCache::new(dir.path(), 10, None, None);
    tc.insert(3, Arc::clone(&table));
    let got = tc.get_or_open(3, size).unwrap();
    // Same underlying pointer.
    assert!(Arc::ptr_eq(&table, &got));
  }

  #[test]
  fn evict_removes_entry() {
    let dir = tempfile::tempdir().unwrap();
    let size = write_sst(dir.path(), 3, &[(b"k", b"v")]);
    let tc = TableCache::new(dir.path(), 10, None, None);
    tc.get_or_open(3, size).unwrap();
    tc.evict(3);
    // After eviction, get_or_open re-opens from disk (still works).
    tc.get_or_open(3, size).unwrap();
  }

  #[test]
  fn lru_evicts_least_recently_used() {
    let dir = tempfile::tempdir().unwrap();
    // Create 3 SSTables but set capacity = 2.
    let s3 = write_sst(dir.path(), 3, &[(b"a", b"1")]);
    let s4 = write_sst(dir.path(), 4, &[(b"b", b"2")]);
    let s5 = write_sst(dir.path(), 5, &[(b"c", b"3")]);
    let tc = TableCache::new(dir.path(), 2, None, None);
    tc.get_or_open(3, s3).unwrap(); // cache: [3]
    tc.get_or_open(4, s4).unwrap(); // cache: [3, 4]
    tc.get_or_open(5, s5).unwrap(); // capacity exceeded → evict 3; cache: [4, 5]
                                    // 3 was evicted — the Arc count inside the cache is 0.
                                    // Opening it again must succeed (re-open from disk).
    let inner = tc.0.lock().unwrap();
    assert!(!inner.map.contains_key(&3));
    assert!(inner.map.contains_key(&4));
    assert!(inner.map.contains_key(&5));
  }
}
