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
}

// SAFETY: all mutations are serialised by the DB-level write mutex in `Db`;
// reads are lock-free via the skip-list's acquire/release atomics.
unsafe impl Sync for Memtable {}

impl Memtable {
  pub fn new() -> Self {
    Self { table: UnsafeCell::new(SkipList::new(Arena::default())) }
  }

  pub fn add(&self, seq: u64, key: &[u8], value: &[u8]) {
    let size = Entry::encoded_value_size(seq, key, value);
    // SAFETY: caller holds the DB write mutex, serialising all mutations.
    let table = unsafe { &mut *self.table.get() };
    table.alloc_and_insert(size, |buf| Entry::write_value_to(buf, seq, key, value));
  }

  pub fn get<K: AsRef<[u8]>>(&self, key: K) -> MemtableResult<Vec<u8>> {
    let key = key.as_ref();
    let lsize = Entry::lookup_size(key);
    let mut lbuf = vec![0u8; lsize];
    Entry::write_lookup_to(&mut lbuf, key);
    // SAFETY: SkipList reads are lock-free via acquire/release atomics.
    let table = unsafe { &*self.table.get() };
    match table.find_first_at_or_after(&lbuf) {
      Some(payload) => {
        let e = Entry::from_slice(payload);
        if e.key() == key {
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
  /// The returned iterator borrows from `self`; no mutation may occur while
  /// it is live.  During flush the caller holds the DB write mutex, which
  /// prevents concurrent mutations.
  pub(crate) fn iter(&self) -> MemTableIterator<'_> {
    // SAFETY: SkipList reads are lock-free via acquire/release atomics.
    let table = unsafe { &*self.table.get() };
    MemTableIterator { inner: table.iter() }
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
    Self::new()
  }
}

// ── MemTableIterator ──────────────────────────────────────────────────────────

/// Forward iterator over a [`Memtable`] that emits entries in SSTable internal-key
/// order (user key ASC, sequence DESC).
///
/// Each step yields:
/// - `ikey()` — an owned SSTable internal key: `user_key || (seq << 8 | vtype).to_le_bytes()`
/// - `value()` — the value bytes, or an empty slice for tombstones.
///
/// Intended for use by the L0 flush path; the caller must hold the DB write
/// mutex to prevent concurrent mutations.
pub(crate) struct MemTableIterator<'a> {
  inner: skiplist::SkipListIter<'a>,
}

impl<'a> MemTableIterator<'a> {
  pub(crate) fn valid(&self) -> bool {
    self.inner.valid()
  }

  /// SSTable internal key for the current entry.
  ///
  /// Format: `user_key || tag` where `tag = (seq << 8 | vtype as u64).to_le_bytes()`.
  pub(crate) fn ikey(&self) -> Vec<u8> {
    let e = Entry::from_slice(self.inner.payload());
    let vtype: u8 = if e.value().is_some() { 1 } else { 0 };
    crate::table::format::make_internal_key(e.key(), e.sequence_id(), vtype)
  }

  /// Value bytes for the current entry; empty slice for tombstones.
  pub(crate) fn value(&self) -> &'a [u8] {
    Entry::from_slice(self.inner.payload()).value().unwrap_or(&[])
  }

  /// Advance to the next entry.
  pub(crate) fn advance(&mut self) {
    self.inner.advance();
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::str::from_utf8;

  #[test]
  fn creates_memtable() {
    let table = Memtable::new();
    assert_eq!(0, unsafe { &*table.table.get() }.len());
  }

  #[test]
  fn insert_get() {
    let table = Memtable::new();
    table.add(0, b"foo", b"bar");
    assert_eq!(b"bar", table.get(b"foo").unwrap_value().as_slice());
  }

  #[test]
  fn replace_get() {
    let table = Memtable::new();
    table.add(0, b"foo", b"foo");
    assert_eq!(b"foo", table.get(b"foo").unwrap_value().as_slice());
    table.add(1, b"foo", b"bar");
    assert_eq!(b"bar", table.get(b"foo").unwrap_value().as_slice());
  }

  #[test]
  fn miss_get() {
    let table = Memtable::new();
    table.add(0, b"foo", b"bar");
    assert_eq!(table.get(b"bar"), MemtableResult::Miss);
  }

  #[test]
  fn miss_empty() {
    let table = Memtable::new();
    assert_eq!(table.get(b"foo"), MemtableResult::Miss);
  }

  #[test]
  fn hit_deleted() {
    let table = Memtable::new();
    table.add(0, b"foo", b"bar");
    table.delete(1, b"foo");
    assert_eq!(table.get(b"foo"), MemtableResult::Deleted);
  }

  #[test]
  fn lifecycle() {
    let table = Memtable::new();
    {
      let foo = String::from("foo");
      table.add(0, foo.as_bytes(), foo.as_bytes());
      let value = table.get(b"foo").unwrap_value();
      assert_eq!("foo", from_utf8(value.as_ref()).unwrap());
    }
    {
      let sparkle_heart = String::from("💖");
      table.add(1, b"foo", sparkle_heart.as_bytes());
    }
    let value = table.get(b"foo").unwrap_value();
    assert_eq!("💖", from_utf8(value.as_ref()).unwrap());
    table.delete(2, b"foo");
    assert_eq!(3, unsafe { &*table.table.get() }.len());
  }

  // ── MemTableIterator tests ────────────────────────────────────────────────

  use crate::table::format::parse_internal_key;

  #[test]
  fn iter_empty_memtable() {
    let mem = Memtable::new();
    let it = mem.iter();
    assert!(!it.valid());
  }

  #[test]
  fn iter_single_value() {
    let mem = Memtable::new();
    mem.add(7, b"key", b"val");
    let mut it = mem.iter();
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
    let mem = Memtable::new();
    mem.delete(3, b"gone");
    let mut it = mem.iter();
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
    let mem = Memtable::new();
    // Insert in non-sequential order; iterator must yield in sorted order.
    mem.add(1, b"b", b"B1");
    mem.add(2, b"a", b"A2");
    mem.add(3, b"a", b"A3");
    mem.add(4, b"c", b"C4");

    let mut it = mem.iter();
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
    let mem = Memtable::new();
    let before = mem.approximate_memory_usage();
    mem.add(0, b"k", b"v");
    let after = mem.approximate_memory_usage();
    assert!(after > before);
  }
}
