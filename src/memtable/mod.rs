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
}

impl Default for Memtable {
  fn default() -> Self {
    Self::new()
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
}
