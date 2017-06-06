use std::cmp::Ord;
use std::cmp::Ordering;
use std::cmp::PartialEq;
use std::cmp::PartialOrd;
use std::cmp::Eq;

pub struct Entry {
  key: String,
  value: String,
}

impl Entry {
  pub fn new(key: String, value: String) -> Entry {
    Entry {
      key: key,
      value: value,
    }
  }
}

impl Ord for Entry {
  fn cmp(&self, other: &Self) -> Ordering {
    self.key.cmp(&other.key)
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
    self.key == other.key
  }
}
