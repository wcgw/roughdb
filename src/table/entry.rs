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
  // todo inline klen in the data?
  klen: usize,
  data: Vec<u8>,
}

impl Entry {
  pub fn new(key: &str, value: &str) -> Entry {
    // todo Add klen as varints
    let klen = key.len();
    let mut vec = Vec::with_capacity(klen + value.len());
    vec.extend(key.as_bytes());
    vec.extend(value.as_bytes());
    Entry {
      klen: klen,
      data: vec,
    }
  }
}

impl Entry {
  fn key(&self) -> &str {
    from_utf8(&self.data[..self.klen]).unwrap()
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

  #[test]
  fn saves_key() {
    let entry = Entry::new(&"Foo", &"Bar");
    assert_eq!("Foo", entry.key());
  }

  #[test]
  fn saves_value() {
    let entry = Entry::new(&"Foo", &"Bar");
    assert_eq!("Bar", entry.value());
  }

  #[test]
  fn key_supports_love() {
    let entry = Entry::new(&"💖", &"Bar");
    assert_eq!("💖", entry.key());
  }

  #[test]
  fn value_supports_love() {
    let entry = Entry::new(&"Bar", &"💖");
    assert_eq!("💖", entry.value());
  }

  #[test]
  fn entries_with_same_key_are_equal() {
    let entry = Entry::new(&"💖", &"Bar");
    let other = Entry::new(&"💖", &"Foo");
    assert_eq!(entry, other);
  }

  #[test]
  fn entries_with_different_keys_are_not_equal() {
    let entry = Entry::new(&"💖", &"Bar");
    let noway = Entry::new(&"💀", &"Bar");
    assert_ne!(noway, entry);
  }
}
