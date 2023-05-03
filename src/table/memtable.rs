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

use std::collections::BTreeSet;
use table::entry::Entry;
use crate::table::entry::Entry;

pub struct Memtable {
  table: BTreeSet<Entry>,
}

impl Memtable {
  pub fn new() -> Memtable {
    Memtable {
      table: BTreeSet::new(),
    }
  }

  pub fn add(&mut self, key: &[u8], value: &[u8]) {
    self.table.replace(Entry::new_value(key, value));
  }

  pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
    match self.table.get(&Entry::new_value(key, b"")) {
      None => None,
      Some(entry) => entry.value(),
    }
  }

  pub fn delete(&mut self, key: &[u8]) {
    self.table.replace(Entry::new_deletion(key));
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
    assert_eq!(b"bar", table.get(b"foo").unwrap());
  }

  #[test]
  fn replace_get() {
    let mut table = Memtable::new();
    table.add(b"foo", b"foo");
    table.add(b"foo", b"bar");
    assert_eq!(b"bar", table.get(b"foo").unwrap());
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
    }
    {
      let foo = String::from("foo");
      let sparkle_heart = String::from("💖");
      table.add(foo.as_bytes(), sparkle_heart.as_bytes());
    }
    let value = table.get(b"foo").unwrap();
    assert_eq!("💖", from_utf8(value).unwrap());
  }
}
