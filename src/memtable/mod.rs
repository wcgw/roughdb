//    Copyright (c) 2023 The RoughDB Authors
//
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

use crate::memtable::arena::Arena;
use entry::Entry;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

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

pub struct Memtable<'a> {
  table: RwLock<BTreeSet<Entry<'a>>>,
  sequence: AtomicU64,
  arena: Arena,
}

impl<'a> Memtable<'a> {
  pub fn new() -> Self {
    Self {
      table: RwLock::new(BTreeSet::new()),
      sequence: AtomicU64::default(),
      arena: Arena::default(),
    }
  }

  pub fn add<K: AsRef<[u8]>, V: AsRef<[u8]>>(&'a self, key: K, value: V) {
    let entry = Entry::new_value(&self.arena, self.next_seq(), key.as_ref(), value.as_ref());
    let mut table = self.table.write().unwrap();
    table.insert(entry);
  }

  pub fn get<K: AsRef<[u8]>>(&'a self, key: K) -> MemtableResult<Vec<u8>> {
    let table = self.table.read().unwrap();
    let key = key.as_ref();
    let mut buffer = vec![0u8; 20 + key.len()];
    match table
      .range(&Entry::new_lookup_key(buffer.as_mut_slice(), key)..)
      .next()
    {
      Some(entry) if entry.key() == key => match entry.value().map(Vec::from) {
        None => MemtableResult::Deleted,
        Some(val) => MemtableResult::Hit(val),
      },
      _ => MemtableResult::Miss,
    }
  }

  pub fn delete(&'a self, key: &[u8]) {
    let mut table = self.table.write().unwrap();
    table.insert(Entry::new_deletion(&self.arena, self.next_seq(), key));
  }

  fn next_seq(&self) -> u64 {
    self.sequence.fetch_add(1, Ordering::Relaxed)
  }
}

impl Default for Memtable<'_> {
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
    assert_eq!(0, table.table.read().unwrap().len());
  }

  #[test]
  fn insert_get() {
    let table = Memtable::new();
    table.add(b"foo", b"bar");
    assert_eq!(b"bar", table.get(b"foo").unwrap_value().as_slice());
  }

  #[test]
  fn replace_get() {
    let table = Memtable::new();
    table.add(b"foo", b"foo");
    assert_eq!(b"foo", table.get(b"foo").unwrap_value().as_slice());
    table.add(b"foo", b"bar");
    assert_eq!(b"bar", table.get(b"foo").unwrap_value().as_slice());
  }

  #[test]
  fn miss_get() {
    let table = Memtable::new();
    table.add(b"foo", b"bar");
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
    table.add(b"foo", b"bar");
    table.delete(b"foo");
    assert_eq!(table.get(b"foo"), MemtableResult::Deleted);
  }

  #[test]
  fn lifecycle() {
    let table = Memtable::new();
    {
      let foo = String::from("foo");
      table.add(foo.as_bytes(), foo.as_bytes());
      let value = table.get(b"foo").unwrap_value();
      assert_eq!("foo", from_utf8(value.as_ref()).unwrap());
    }
    {
      let sparkle_heart = String::from("💖");
      table.add(b"foo", sparkle_heart.as_bytes());
    }
    let value = table.get(b"foo").unwrap_value();
    assert_eq!("💖", from_utf8(value.as_ref()).unwrap());
    table.delete(b"foo");
    assert_eq!(3, table.table.read().unwrap().len());
  }
}
