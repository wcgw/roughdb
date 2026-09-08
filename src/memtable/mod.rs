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

mod arena;
mod entry;
mod skiplist;

use arena::Arena;
use entry::Entry;
use skiplist::SkipList;
use std::sync::Arc;

use crate::comparator::{BytewiseComparator, Comparator};

#[derive(Debug, PartialEq)]
pub enum MemtableResult<T> {
  Miss,
  Deleted,
  Hit(T),
}

#[cfg(test)]
impl<T> MemtableResult<T> {
  pub fn unwrap_value(self) -> T {
    match self {
      MemtableResult::Hit(val) => val,
      MemtableResult::Deleted => {
        panic!("called `MemtableResult::unwrap_value()` on a `Deleted` value!")
      }
      MemtableResult::Miss => {
        panic!("called `MemtableResult::unwrap_value()` on a `Missed` value!")
      }
    }
  }
}

/// In-memory write buffer: a skip list of entry-encoded key/value versions.
///
/// # Thread safety
///
/// `Memtable` is `Sync` (the skip list's reads are lock-free), but writes are
/// *not* internally synchronised: [`Memtable::add`] and [`Memtable::delete`]
/// are `unsafe fn` and require the caller to serialise writers.  `Db` does so
/// by inserting only while holding the `DbState` mutex, matching LevelDB.
pub struct Memtable {
  table: SkipList,
  comparator: Arc<dyn Comparator>,
}

impl Memtable {
  pub fn new(comparator: Arc<dyn Comparator>) -> Self {
    Self {
      table: SkipList::new(Arena::default(), Arc::clone(&comparator)),
      comparator,
    }
  }

  /// Insert a value for `key` at sequence number `seq`.
  ///
  /// # Safety
  /// No other `add` or `delete` may run concurrently on this memtable; the
  /// caller must serialise writers (in `Db`, the write leader holds the
  /// `DbState` mutex).  Concurrent `get` and iterators are fine.
  pub unsafe fn add(&self, seq: u64, key: &[u8], value: &[u8]) {
    let size = Entry::encoded_value_size(seq, key, value);
    // SAFETY: writer serialisation is forwarded from this function's
    // contract; `write_value_to` fills exactly `size` bytes.
    unsafe {
      self
        .table
        .alloc_and_insert(size, |buf| Entry::write_value_to(buf, seq, key, value))
    };
  }

  /// Look up `key` at the given `sequence` number.
  ///
  /// Returns `Hit(value)` if the newest version with `seq <= sequence` is a
  /// Put, `Deleted` if it is a tombstone, or `Miss` if no visible version
  /// exists.  Pass `u64::MAX` (or use [`Memtable::get_latest`]) to read the
  /// absolute latest version.
  pub fn get<K: AsRef<[u8]>>(&self, key: K, sequence: u64) -> MemtableResult<Vec<u8>> {
    let key = key.as_ref();
    // In skip-list order (user_key ASC, seq DESC) this lands on the newest
    // version of `key` with seq ≤ `sequence`, or on a later key.
    match self.table.find_first_at_or_after(key, sequence) {
      Some(e) if self.comparator.compare(e.key, key) == std::cmp::Ordering::Equal => {
        match e.value {
          None => MemtableResult::Deleted,
          Some(val) => MemtableResult::Hit(val.to_vec()),
        }
      }
      _ => MemtableResult::Miss,
    }
  }

  /// Insert a deletion tombstone for `key` at sequence number `seq`.
  ///
  /// # Safety
  /// Same contract as [`Memtable::add`]: no concurrent `add`/`delete`.
  pub unsafe fn delete(&self, seq: u64, key: &[u8]) {
    let size = Entry::encoded_deletion_size(seq, key);
    // SAFETY: writer serialisation is forwarded from this function's
    // contract; `write_deletion_to` fills exactly `size` bytes.
    unsafe {
      self
        .table
        .alloc_and_insert(size, |buf| Entry::write_deletion_to(buf, seq, key))
    };
  }

  /// Return a forward iterator over all entries in internal-key order.
  ///
  /// The returned iterator starts in an invalid (unpositioned) state; the
  /// caller must invoke `seek_to_first()` or `seek()` before reading entries.
  pub fn iter(&self) -> MemTableIterator<'_> {
    MemTableIterator {
      inner: self.table.iter(),
      cached_key: Vec::new(),
      cached_value: &[],
    }
  }

  /// Approximate number of bytes used by this memtable (arena allocations).
  ///
  /// Used to decide when to flush to L0.
  pub fn approximate_memory_usage(&self) -> usize {
    self.table.arena_memory_usage()
  }
}

impl Default for Memtable {
  fn default() -> Self {
    Self::new(Arc::new(BytewiseComparator))
  }
}

// ── MemTableIterator ──────────────────────────────────────────────────────────

/// Forward iterator over a [`Memtable`] that emits entries in SSTable internal-key
/// order (user key ASC, sequence DESC).
///
/// Starts unpositioned; call `seek_to_first()` or `seek()` before reading.
/// Implements `InternalIterator` for use with `MergingIterator`.
pub struct MemTableIterator<'a> {
  inner: skiplist::SkipListIter<'a>,
  /// Cached SSTable internal key for the current position.  Cleared when
  /// the iterator becomes invalid.
  cached_key: Vec<u8>,
  /// Value of the current entry (empty for tombstones), borrowed from the
  /// arena.  Decoded once per position, alongside `cached_key`.
  cached_value: &'a [u8],
}

impl<'a> MemTableIterator<'a> {
  /// Recompute `cached_key` and `cached_value` from the current position.
  fn update_cached(&mut self) {
    if self.inner.valid() {
      let e = self.inner.entry();
      let vtype: u8 = if e.value.is_some() { 1 } else { 0 };
      // Encode in place so `cached_key`'s allocation is reused across steps
      // instead of allocating a fresh `Vec` per entry.
      crate::table::format::encode_internal_key_into(&mut self.cached_key, e.key, e.seq, vtype);
      self.cached_value = e.value.unwrap_or(&[]);
    } else {
      self.cached_key.clear();
      self.cached_value = &[];
    }
  }

  pub fn valid(&self) -> bool {
    self.inner.valid()
  }

  /// Position at the first entry.
  pub fn seek_to_first(&mut self) {
    self.inner.seek_to_first();
    self.update_cached();
  }

  /// Position at the last entry.
  pub fn seek_to_last(&mut self) {
    self.inner.seek_to_last();
    self.update_cached();
  }

  /// Position at the first entry whose SSTable internal key is ≥ `target`.
  ///
  /// A malformed `target` (shorter than the 8-byte tag) leaves the iterator
  /// invalid.
  pub fn seek(&mut self, target: &[u8]) {
    match crate::table::format::parse_internal_key(target) {
      Some((user_key, seq, _)) => self.inner.seek(user_key, seq),
      None => self.inner.seek(&[], 0),
    }
    self.update_cached();
  }

  /// SSTable internal key for the current entry (owned).
  #[cfg(test)]
  pub(crate) fn ikey(&self) -> Vec<u8> {
    self.cached_key.clone()
  }

  /// Current SSTable internal key as a slice.
  pub fn key(&self) -> &[u8] {
    debug_assert!(self.valid());
    &self.cached_key
  }

  /// Value bytes for the current entry; empty slice for tombstones.
  pub fn value(&self) -> &[u8] {
    debug_assert!(self.valid());
    self.cached_value
  }

  /// Advance to the next entry.
  pub fn advance(&mut self) {
    debug_assert!(self.valid());
    self.inner.advance();
    self.update_cached();
  }

  /// Move to the previous entry.
  pub fn prev(&mut self) {
    debug_assert!(self.valid());
    self.inner.prev();
    self.update_cached();
  }
}

impl crate::iter::InternalIterator for MemTableIterator<'_> {
  fn valid(&self) -> bool {
    self.inner.valid()
  }

  fn seek_to_first(&mut self) {
    self.seek_to_first();
  }

  fn seek_to_last(&mut self) {
    self.seek_to_last();
  }

  fn seek(&mut self, target: &[u8]) {
    self.seek(target);
  }

  fn next(&mut self) {
    self.advance();
  }

  fn prev(&mut self) {
    self.prev();
  }

  fn key(&self) -> &[u8] {
    self.key()
  }

  fn value(&self) -> &[u8] {
    self.value()
  }

  fn status(&self) -> Option<&crate::error::Error> {
    None
  }
}

// ── ArcMemTableIter ───────────────────────────────────────────────────────────

/// Owned memtable iterator that keeps the `Arc<Memtable>` alive.
///
/// `MemTableIterator<'a>` borrows the `Memtable`'s arena via raw pointers; the
/// `'a` parameter is a `PhantomData` marker that prevents the iterator from
/// outliving the `Memtable`.  This wrapper stores the `Arc<Memtable>` alongside
/// the iterator so the iterator is `'static` and can be boxed as
/// `Box<dyn InternalIterator>`.
///
/// The `'static` lifetime is a lie the borrow checker cannot see through:
/// `iter` really borrows the memtable owned by `_owner`.  It is sound because
/// `_owner` lives as long as this struct (and is declared after `iter`, so it
/// is dropped after it), and every slice handed out via `key()`/`value()` is
/// bounded by `&self`, so nothing can outlive the `Arc`.
pub(crate) struct ArcMemTableIter {
  iter: MemTableIterator<'static>,
  _owner: Arc<Memtable>,
}

impl ArcMemTableIter {
  pub(crate) fn new(mem: Arc<Memtable>) -> Self {
    // SAFETY: `raw` points to the `Memtable` kept alive by `mem`, which is
    // stored in `_owner` below and therefore outlives the iterator.
    // Dereferencing the raw pointer yields a borrow of unbounded lifetime; we
    // pin it to `'static`, which is sound for the reasons given on the struct.
    let raw: *const Memtable = Arc::as_ptr(&mem);
    let iter: MemTableIterator<'static> = unsafe { (*raw).iter() };
    ArcMemTableIter { iter, _owner: mem }
  }
}

impl crate::iter::InternalIterator for ArcMemTableIter {
  fn valid(&self) -> bool {
    self.iter.valid()
  }

  fn seek_to_first(&mut self) {
    self.iter.seek_to_first();
  }

  fn seek_to_last(&mut self) {
    self.iter.seek_to_last();
  }

  fn seek(&mut self, target: &[u8]) {
    self.iter.seek(target);
  }

  fn next(&mut self) {
    self.iter.advance();
  }

  fn prev(&mut self) {
    self.iter.prev();
  }

  fn key(&self) -> &[u8] {
    self.iter.key()
  }

  fn value(&self) -> &[u8] {
    self.iter.value()
  }

  fn status(&self) -> Option<&crate::error::Error> {
    None
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::str::from_utf8;

  fn add(mem: &Memtable, seq: u64, key: &[u8], value: &[u8]) {
    // SAFETY: tests write from a single thread, so no concurrent writer exists.
    unsafe { Memtable::add(mem, seq, key, value) }
  }

  fn delete(mem: &Memtable, seq: u64, key: &[u8]) {
    // SAFETY: as in `add`.
    unsafe { Memtable::delete(mem, seq, key) }
  }

  #[test]
  fn creates_memtable() {
    let table = Memtable::default();
    assert_eq!(0, table.table.len());
  }

  #[test]
  fn insert_get() {
    let table = Memtable::default();
    add(&table, 0, b"foo", b"bar");
    assert_eq!(
      b"bar",
      table.get(b"foo", u64::MAX).unwrap_value().as_slice()
    );
  }

  #[test]
  fn replace_get() {
    let table = Memtable::default();
    add(&table, 0, b"foo", b"foo");
    assert_eq!(
      b"foo",
      table.get(b"foo", u64::MAX).unwrap_value().as_slice()
    );
    add(&table, 1, b"foo", b"bar");
    assert_eq!(
      b"bar",
      table.get(b"foo", u64::MAX).unwrap_value().as_slice()
    );
  }

  #[test]
  fn miss_get() {
    let table = Memtable::default();
    add(&table, 0, b"foo", b"bar");
    assert_eq!(table.get(b"bar", u64::MAX), MemtableResult::Miss);
  }

  #[test]
  fn miss_empty() {
    let table = Memtable::default();
    assert_eq!(table.get(b"foo", u64::MAX), MemtableResult::Miss);
  }

  #[test]
  fn hit_deleted() {
    let table = Memtable::default();
    add(&table, 0, b"foo", b"bar");
    delete(&table, 1, b"foo");
    assert_eq!(table.get(b"foo", u64::MAX), MemtableResult::Deleted);
  }

  #[test]
  fn lifecycle() {
    let table = Memtable::default();
    {
      let foo = String::from("foo");
      add(&table, 0, foo.as_bytes(), foo.as_bytes());
      let value = table.get(b"foo", u64::MAX).unwrap_value();
      assert_eq!("foo", from_utf8(value.as_ref()).unwrap());
    }
    {
      let sparkle_heart = String::from("💖");
      add(&table, 1, b"foo", sparkle_heart.as_bytes());
    }
    let value = table.get(b"foo", u64::MAX).unwrap_value();
    assert_eq!("💖", from_utf8(value.as_ref()).unwrap());
    delete(&table, 2, b"foo");
    assert_eq!(3, table.table.len());
  }

  // ── MemTableIterator tests ────────────────────────────────────────────────

  use crate::table::format::parse_internal_key;

  #[test]
  fn iter_empty_memtable() {
    let mem = Memtable::default();
    let it = mem.iter();
    assert!(!it.valid());
  }

  #[test]
  fn iter_single_value() {
    let mem = Memtable::default();
    add(&mem, 7, b"key", b"val");
    let mut it = mem.iter();
    it.seek_to_first();
    assert!(it.valid());
    let ikey = it.ikey();
    let (uk, seq, vtype) = parse_internal_key(&ikey).unwrap();
    assert_eq!(uk, b"key");
    assert_eq!(seq, 7);
    assert_eq!(vtype, 1); // Value
    assert_eq!(it.value(), b"val");
    it.advance();
    assert!(!it.valid());
  }

  #[test]
  fn iter_tombstone() {
    let mem = Memtable::default();
    delete(&mem, 3, b"gone");
    let mut it = mem.iter();
    it.seek_to_first();
    assert!(it.valid());
    let ikey = it.ikey();
    let (uk, seq, vtype) = parse_internal_key(&ikey).unwrap();
    assert_eq!(uk, b"gone");
    assert_eq!(seq, 3);
    assert_eq!(vtype, 0); // Deletion
    assert_eq!(it.value(), b"");
    it.advance();
    assert!(!it.valid());
  }

  #[test]
  fn iter_ordering_user_key_asc_seq_desc() {
    let mem = Memtable::default();
    // Insert in non-sequential order; iterator must yield in sorted order.
    add(&mem, 1, b"b", b"B1");
    add(&mem, 2, b"a", b"A2");
    add(&mem, 3, b"a", b"A3");
    add(&mem, 4, b"c", b"C4");

    let mut it = mem.iter();
    it.seek_to_first();
    let mut keys: Vec<(Vec<u8>, u64)> = Vec::new();
    while it.valid() {
      let ikey = it.ikey();
      let (uk, seq, _vtype) = parse_internal_key(&ikey).unwrap();
      keys.push((uk.to_vec(), seq));
      it.advance();
    }
    // Expected: a@3, a@2, b@1, c@4 (user key ASC, seq DESC within same key)
    assert_eq!(keys[0], (b"a".to_vec(), 3));
    assert_eq!(keys[1], (b"a".to_vec(), 2));
    assert_eq!(keys[2], (b"b".to_vec(), 1));
    assert_eq!(keys[3], (b"c".to_vec(), 4));
  }

  #[test]
  fn iter_ordering_follows_custom_comparator() {
    // Guards the intent that memtable ordering is driven by the pluggable
    // comparator, not by a byte-wise ordering of the encoded entry.  Under a
    // reverse comparator, user keys must iterate DESCending — a naive
    // `Entry: Ord` on raw bytes would (wrongly) still yield ascending order.
    struct ReverseComparator;
    impl Comparator for ReverseComparator {
      fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
      }
      fn name(&self) -> &str {
        "test.ReverseComparator"
      }
      fn find_shortest_separator(&self, _start: &mut Vec<u8>, _limit: &[u8]) {}
      fn find_short_successor(&self, _key: &mut Vec<u8>) {}
    }

    let mem = Memtable::new(Arc::new(ReverseComparator));
    add(&mem, 1, b"a", b"A");
    add(&mem, 2, b"b", b"B");
    add(&mem, 3, b"c", b"C");

    let mut it = mem.iter();
    it.seek_to_first();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      let ikey = it.ikey();
      let (uk, _, _) = parse_internal_key(&ikey).unwrap();
      keys.push(uk.to_vec());
      it.advance();
    }
    assert_eq!(keys, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
  }

  #[test]
  fn approximate_memory_usage_grows() {
    let mem = Memtable::default();
    let before = mem.approximate_memory_usage();
    add(&mem, 0, b"k", b"v");
    let after = mem.approximate_memory_usage();
    assert!(after > before);
  }
}
