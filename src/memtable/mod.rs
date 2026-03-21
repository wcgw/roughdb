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
use std::cell::UnsafeCell;
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

pub struct Memtable {
  table: UnsafeCell<SkipList>,
  comparator: Arc<dyn Comparator>,
}

// SAFETY: all mutations are serialised by the DB-level write mutex in `Db`;
// reads are lock-free via the skip-list's acquire/release atomics.
unsafe impl Sync for Memtable {}

impl Memtable {
  pub fn new(comparator: Arc<dyn Comparator>) -> Self {
    Self {
      table: UnsafeCell::new(SkipList::new(Arena::default(), Arc::clone(&comparator))),
      comparator,
    }
  }

  pub fn add(&self, seq: u64, key: &[u8], value: &[u8]) {
    let size = Entry::encoded_value_size(seq, key, value);
    // SAFETY: caller holds the DB write mutex, serialising all mutations.
    let table = unsafe { &mut *self.table.get() };
    table.alloc_and_insert(size, |buf| Entry::write_value_to(buf, seq, key, value));
  }

  /// Look up `key` at the given `sequence` number.
  ///
  /// Returns `Hit(value)` if the newest version with `seq <= sequence` is a
  /// Put, `Deleted` if it is a tombstone, or `Miss` if no visible version
  /// exists.  Pass `u64::MAX` (or use [`Memtable::get_latest`]) to read the
  /// absolute latest version.
  pub fn get<K: AsRef<[u8]>>(&self, key: K, sequence: u64) -> MemtableResult<Vec<u8>> {
    let key = key.as_ref();
    // Seek to (key, sequence): in skip-list order (user_key ASC, seq DESC),
    // this positions at the first entry with the same user key and seq ≤ sequence.
    let ssize = Entry::seek_key_size(key, sequence);
    let mut sbuf = vec![0u8; ssize];
    Entry::write_seek_key_to(&mut sbuf, key, sequence);
    // SAFETY: SkipList reads are lock-free via acquire/release atomics.
    let table = unsafe { &*self.table.get() };
    match table.find_first_at_or_after(&sbuf) {
      Some(payload) => {
        let e = Entry::from_slice(payload);
        if self.comparator.compare(e.key(), key) == std::cmp::Ordering::Equal {
          match e.value().map(Vec::from) {
            None => MemtableResult::Deleted,
            Some(val) => MemtableResult::Hit(val),
          }
        } else {
          MemtableResult::Miss
        }
      }
      None => MemtableResult::Miss,
    }
  }

  pub fn delete(&self, seq: u64, key: &[u8]) {
    let size = Entry::encoded_deletion_size(seq, key);
    // SAFETY: caller holds the DB write mutex, serialising all mutations.
    let table = unsafe { &mut *self.table.get() };
    table.alloc_and_insert(size, |buf| Entry::write_deletion_to(buf, seq, key));
  }

  /// Return a forward iterator over all entries in internal-key order.
  ///
  /// The returned iterator starts in an invalid (unpositioned) state; the
  /// caller must invoke `seek_to_first()` or `seek()` before reading entries.
  pub(crate) fn iter(&self) -> MemTableIterator<'_> {
    // SAFETY: SkipList reads are lock-free via acquire/release atomics.
    let table = unsafe { &*self.table.get() };
    MemTableIterator {
      inner: table.iter(),
      cached_key: Vec::new(),
    }
  }

  /// Approximate number of bytes used by this memtable (arena allocations).
  ///
  /// Used to decide when to flush to L0.
  pub(crate) fn approximate_memory_usage(&self) -> usize {
    // SAFETY: accessing Arena (which is Send+Sync) from a shared ref is fine.
    let table = unsafe { &*self.table.get() };
    table.arena_memory_usage()
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
/// Implements [`InternalIterator`] for use with `MergingIterator`.
pub(crate) struct MemTableIterator<'a> {
  inner: skiplist::SkipListIter<'a>,
  /// Cached SSTable internal key for the current position.  Cleared when
  /// the iterator becomes invalid.
  cached_key: Vec<u8>,
}

impl<'a> MemTableIterator<'a> {
  /// Recompute `cached_key` from the current skip-list position.
  fn update_cached_key(&mut self) {
    if self.inner.valid() {
      let e = Entry::from_slice(self.inner.payload());
      let vtype: u8 = if e.value().is_some() { 1 } else { 0 };
      self.cached_key = crate::table::format::make_internal_key(e.key(), e.sequence_id(), vtype);
    } else {
      self.cached_key.clear();
    }
  }

  pub(crate) fn valid(&self) -> bool {
    self.inner.valid()
  }

  /// Position at the first entry.
  pub(crate) fn seek_to_first(&mut self) {
    self.inner.seek_to_first();
    self.update_cached_key();
  }

  /// Position at the last entry.
  pub(crate) fn seek_to_last(&mut self) {
    self.inner.seek_to_last();
    self.update_cached_key();
  }

  /// Position at the first entry whose SSTable internal key is ≥ `target`.
  pub(crate) fn seek(&mut self, target: &[u8]) {
    if let Some((user_key, seq, _)) = crate::table::format::parse_internal_key(target) {
      let size = Entry::seek_key_size(user_key, seq);
      let mut buf = vec![0u8; size];
      Entry::write_seek_key_to(&mut buf, user_key, seq);
      self.inner.seek(&buf);
    }
    self.update_cached_key();
  }

  /// SSTable internal key for the current entry (owned).
  #[cfg(test)]
  pub(crate) fn ikey(&self) -> Vec<u8> {
    self.cached_key.clone()
  }

  /// Current SSTable internal key as a slice.
  pub(crate) fn key(&self) -> &[u8] {
    debug_assert!(self.valid());
    &self.cached_key
  }

  /// Value bytes for the current entry; empty slice for tombstones.
  pub(crate) fn value(&self) -> &[u8] {
    debug_assert!(self.valid());
    Entry::from_slice(self.inner.payload())
      .value()
      .unwrap_or(&[])
  }

  /// Advance to the next entry.
  pub(crate) fn advance(&mut self) {
    debug_assert!(self.valid());
    self.inner.advance();
    self.update_cached_key();
  }

  /// Move to the previous entry.
  pub(crate) fn prev(&mut self) {
    debug_assert!(self.valid());
    self.inner.prev();
    self.update_cached_key();
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
/// # Safety
///
/// The transmute from `MemTableIterator<'_>` to `MemTableIterator<'static>` is
/// sound because `_owner` keeps the `Memtable` (and its arena) alive for the
/// entire lifetime of this struct.  All slices returned from `key()` and
/// `value()` through the `InternalIterator` trait are bounded by `&self`'s
/// lifetime, so they cannot escape beyond the iterator.
pub(crate) struct ArcMemTableIter {
  _owner: Arc<Memtable>,
  iter: MemTableIterator<'static>,
}

impl ArcMemTableIter {
  pub(crate) fn new(mem: Arc<Memtable>) -> Self {
    // SAFETY: `raw` points to the Memtable kept alive by `mem` (stored in
    // `_owner` below).  `iter()` returns a `MemTableIterator` whose raw
    // pointers reference the arena inside that Memtable.  We transmute the
    // `'_` lifetime to `'static`; this is safe because `_owner` guarantees
    // the arena outlives `self`.
    let raw: *const Memtable = Arc::as_ptr(&mem);
    let iter: MemTableIterator<'_> = unsafe { (*raw).iter() };
    let iter: MemTableIterator<'static> = unsafe { std::mem::transmute(iter) };
    ArcMemTableIter { _owner: mem, iter }
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

  #[test]
  fn creates_memtable() {
    let table = Memtable::default();
    assert_eq!(0, unsafe { &*table.table.get() }.len());
  }

  #[test]
  fn insert_get() {
    let table = Memtable::default();
    table.add(0, b"foo", b"bar");
    assert_eq!(
      b"bar",
      table.get(b"foo", u64::MAX).unwrap_value().as_slice()
    );
  }

  #[test]
  fn replace_get() {
    let table = Memtable::default();
    table.add(0, b"foo", b"foo");
    assert_eq!(
      b"foo",
      table.get(b"foo", u64::MAX).unwrap_value().as_slice()
    );
    table.add(1, b"foo", b"bar");
    assert_eq!(
      b"bar",
      table.get(b"foo", u64::MAX).unwrap_value().as_slice()
    );
  }

  #[test]
  fn miss_get() {
    let table = Memtable::default();
    table.add(0, b"foo", b"bar");
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
    table.add(0, b"foo", b"bar");
    table.delete(1, b"foo");
    assert_eq!(table.get(b"foo", u64::MAX), MemtableResult::Deleted);
  }

  #[test]
  fn lifecycle() {
    let table = Memtable::default();
    {
      let foo = String::from("foo");
      table.add(0, foo.as_bytes(), foo.as_bytes());
      let value = table.get(b"foo", u64::MAX).unwrap_value();
      assert_eq!("foo", from_utf8(value.as_ref()).unwrap());
    }
    {
      let sparkle_heart = String::from("💖");
      table.add(1, b"foo", sparkle_heart.as_bytes());
    }
    let value = table.get(b"foo", u64::MAX).unwrap_value();
    assert_eq!("💖", from_utf8(value.as_ref()).unwrap());
    table.delete(2, b"foo");
    assert_eq!(3, unsafe { &*table.table.get() }.len());
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
    mem.add(7, b"key", b"val");
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
    mem.delete(3, b"gone");
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
    mem.add(1, b"b", b"B1");
    mem.add(2, b"a", b"A2");
    mem.add(3, b"a", b"A3");
    mem.add(4, b"c", b"C4");

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
  fn approximate_memory_usage_grows() {
    let mem = Memtable::default();
    let before = mem.approximate_memory_usage();
    mem.add(0, b"k", b"v");
    let after = mem.approximate_memory_usage();
    assert!(after > before);
  }
}
