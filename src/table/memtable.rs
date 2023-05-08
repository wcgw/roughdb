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
//

use crate::table::entry::Entry;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Memtable {
  table: BTreeSet<Entry>,
  sequence: AtomicU64,
}

impl Memtable {
  pub fn new() -> Memtable {
    Memtable {
      table: BTreeSet::new(),
      sequence: AtomicU64::default(),
    }
  }

  pub fn add<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
    let entry = Entry::new_value(self.next_seq(), key.as_ref(), value.as_ref());
    self.table.insert(entry);
  }

  pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Vec<u8>> {
    match self
      .table
      .range(&Entry::new_value(u64::MAX, key.as_ref(), b"")..)
      .next()
    {
      Some(entry) if entry.key() == key.as_ref() => entry.value().map(Vec::from),
      _ => None,
    }
  }

  pub fn delete(&mut self, key: &[u8]) {
    self.table.insert(Entry::new_deletion(self.next_seq(), key));
  }

  fn next_seq(&self) -> u64 {
    self.sequence.fetch_add(1, Ordering::SeqCst)
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
    assert_eq!(0, table.table.len());
  }

  #[test]
  fn insert_get() {
    let mut table = Memtable::new();
    table.add(b"foo", b"bar");
    assert_eq!(b"bar", table.get(b"foo").unwrap().as_slice());
  }

  #[test]
  fn replace_get() {
    let mut table = Memtable::new();
    table.add(b"foo", b"foo");
    assert_eq!(b"foo", table.get(b"foo").unwrap().as_slice());
    table.add(b"foo", b"bar");
    assert_eq!(b"bar", table.get(b"foo").unwrap().as_slice());
  }

  #[test]
  fn miss_get() {
    let mut table = Memtable::new();
    table.add(b"foo", b"bar");
    assert!(table.get(b"bar").is_none());
  }

  #[test]
  fn miss_deleted() {
    let mut table = Memtable::new();
    table.add(b"foo", b"bar");
    table.delete(b"foo");
    assert!(table.get(b"foo").is_none());
  }

  #[test]
  fn lifecycle() {
    let mut table = Memtable::new();
    {
      let foo = String::from("foo");
      table.add(foo.as_bytes(), foo.as_bytes());
      let value = table.get(b"foo").unwrap();
      assert_eq!("foo", from_utf8(value.as_ref()).unwrap());
    }
    {
      let sparkle_heart = String::from("💖");
      table.add(b"foo", sparkle_heart.as_bytes());
    }
    let value = table.get(b"foo").unwrap();
    assert_eq!("💖", from_utf8(value.as_ref()).unwrap());
    table.delete(b"foo");
    assert_eq!(3, table.table.len());
  }
}
