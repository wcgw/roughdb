use std::collections::BTreeSet;
use table::entry::Entry;

pub struct Memtable {
  table: BTreeSet<Entry>,
}

impl Memtable {
  pub fn new() -> Memtable {
    Memtable { table: BTreeSet::new() }
  }

  pub fn len(&self) -> usize {
    self.table.len()
  }

  pub fn insert(&mut self, key: &str, value: &str) -> bool {
    self.table.replace(Entry::new_value(key, value)).is_none()
  }

  pub fn get(&self, key: &str) -> Option<&str> {
    match self.table.get(&Entry::new_value(key, "")) {
      None => None,
      Some(e) => if e.deleted() {
        None
      } else {
        Some(e.value())
      }
    }
  }

  pub fn delete(&mut self, key: &str) -> bool {
    self.table.replace(Entry::new_deletion(key)).is_none()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn creates_memtable() {
    let table = Memtable::new();
    assert_eq!(0, table.len());
  }

  #[test]
  fn insert_get() {
    let mut table = Memtable::new();
    assert!(table.insert(&"foo", &"bar"));
    assert_eq!("bar", table.get(&"foo").unwrap());
  }

  #[test]
  fn replace_get() {
    let mut table = Memtable::new();
    assert!(table.insert(&"foo", &"foo"));
    assert!(!table.insert(&"foo", &"bar"));
    assert_eq!("bar", table.get(&"foo").unwrap());
  }

  #[test]
  fn miss_get() {
    let mut table = Memtable::new();
    table.insert(&"foo", &"bar");
    assert!(table.get(&"bar").is_none());
  }

  #[test]
  fn miss_deleted() {
    let mut table = Memtable::new();
    table.insert(&"foo", &"bar");
    table.delete(&"foo");
    assert!(table.get(&"foo").is_none());
  }
}
