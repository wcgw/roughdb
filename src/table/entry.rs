use options::ValueType;

use std::cmp::Ord;
use std::cmp::Ordering;
use std::cmp::PartialEq;
use std::cmp::PartialOrd;
use std::cmp::Eq;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::str::from_utf8;
use std::vec::Vec;

pub struct Entry {
  // todo inline vtype & klen in the data!
  vtype: ValueType,
  klen: usize,
  data: Vec<u8>,
}

impl Entry {
  pub fn new_deletion(key: &str) -> Entry {
    let klen = key.len();
    let mut vec = Vec::with_capacity(klen);
    vec.extend(key.as_bytes());
    Entry {
      vtype: ValueType::Deletion,
      klen: klen,
      data: vec,
    }
  }

  pub fn new_value(key: &str, value: &str) -> Entry {
    // todo Add klen as varints
    let klen = key.len();
    let mut vec = Vec::with_capacity(klen + value.len());
    vec.extend(key.as_bytes());
    vec.extend(value.as_bytes());
    Entry {
      vtype: ValueType::Value,
      klen: klen,
      data: vec,
    }
  }
}

impl Entry {
  fn key(&self) -> &str {
    from_utf8(&self.data[..self.klen]).unwrap()
  }

  pub fn deleted(&self) -> bool {
    match self.vtype {
      ValueType::Deletion => true,
      _ => false,
    }
  }

  pub fn value(&self) -> &str {
    from_utf8(&self.data[self.klen..]).unwrap()
  }
}

impl Ord for Entry {
  fn cmp(&self, other: &Self) -> Ordering {
    self.key().cmp(&other.key())
  }
}

impl PartialOrd for Entry {
  fn partial_cmp(&self, other: &Entry) -> Option<Ordering> {
    Some(self.cmp(&other))
  }
}

impl Eq for Entry {}

impl PartialEq for Entry {
  fn eq(&self, other: &Entry) -> bool {
    self.key() == other.key()
  }
}

impl Debug for Entry {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "table::Entry {{ key: {} }}", self.key())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::mem::size_of;

  #[test]
  fn value_not_deleted() {
    let entry = Entry::new_value(&"Foo", &"Bar");
    assert!(!entry.deleted());
  }

  #[test]
  fn deletion_is_deleted() {
    let entry = Entry::new_deletion(&"Foo");
    assert!(entry.deleted());
  }

  #[test]
  fn saves_key() {
    let entry = Entry::new_value(&"Foo", &"Bar");
    assert_eq!("Foo", entry.key());
  }

  #[test]
  fn saves_value() {
    let entry = Entry::new_value(&"Foo", &"Bar");
    assert_eq!("Bar", entry.value());
  }

  #[test]
  fn key_supports_love() {
    let entry = Entry::new_value(&"💖", &"Bar");
    assert_eq!("💖", entry.key());
  }

  #[test]
  fn value_supports_love() {
    let entry = Entry::new_value(&"Bar", &"💖");
    assert_eq!("💖", entry.value());
  }

  #[test]
  fn entries_with_same_key_are_equal() {
    let entry = Entry::new_value(&"💖", &"Bar");
    let other = Entry::new_value(&"💖", &"Foo");
    assert_eq!(entry, other);
  }

  #[test]
  fn entries_with_different_keys_are_not_equal() {
    let entry = Entry::new_value(&"💖", &"Bar");
    let noway = Entry::new_value(&"💀", &"Bar");
    assert_ne!(noway, entry);
  }

  #[test]
  fn size() {
    assert_eq!(40, size_of::<Entry>()); // todo this should be 24, i.e size_of::<Vec<u8>>()
  }
}
