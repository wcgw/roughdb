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
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn creates_memtable() {
    let table = Memtable::new();
    assert_eq!(0, table.len());
  }
}
