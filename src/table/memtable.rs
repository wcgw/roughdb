use std::collections::BTreeSet;
use table::entry::Entry;

pub struct Memtable {
  table: BTreeSet<Entry>,
}

impl Memtable {
  pub fn new() -> Memtable {
    Memtable { table: BTreeSet::new() }
  }

  pub fn add(&mut self, key: &str, value: &str) {
    self.table.replace(Entry::new_value(key, value));
  }

  pub fn get(&self, key: &str) -> Option<&str> {
    match self.table.get(&Entry::new_value(key, "")) {
      None => None,
      Some(e) => e.value(),
    }
  }

  pub fn delete(&mut self, key: &str) {
    self.table.replace(Entry::new_deletion(key));
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn creates_memtable() {
    let table = Memtable::new();
    assert_eq!(0, table.table.len());
  }

  #[test]
  fn insert_get() {
    let mut table = Memtable::new();
    table.add(&"foo", &"bar");
    assert_eq!("bar", table.get(&"foo").unwrap());
  }

  #[test]
  fn replace_get() {
    let mut table = Memtable::new();
    table.add(&"foo", &"foo");
    table.add(&"foo", &"bar");
    assert_eq!("bar", table.get(&"foo").unwrap());
  }

  #[test]
  fn miss_get() {
    let mut table = Memtable::new();
    table.add(&"foo", &"bar");
    assert!(table.get(&"bar").is_none());
  }

  #[test]
  fn miss_deleted() {
    let mut table = Memtable::new();
    table.add(&"foo", &"bar");
    table.delete(&"foo");
    assert!(table.get(&"foo").is_none());
  }

  #[test]
  fn lifecycle() {
    let mut table = Memtable::new();
    {
      let foo = String::from("foo");
      table.add(&foo, &foo);
    }
    {
      let foo = String::from("foo");
      let bar = String::from("bar");
      table.add(&foo, &bar);
    }
    assert_eq!("bar", table.get("foo").unwrap());
  }
}
