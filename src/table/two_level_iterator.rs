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

use crate::error::Error;
use crate::iter::InternalIterator;

/// Given the value bytes of an index entry (an encoded `BlockHandle`), open
/// the corresponding data block and return an iterator over it.
pub(crate) type BlockFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn InternalIterator>, Error>>;

/// Composes an index iterator with a block-opener function to provide a
/// transparent, seek-able view over all entries in an SSTable.
///
/// The index iterator yields one entry per data block; its value is an encoded
/// `BlockHandle`.  `block_fn` maps that handle to a `BlockIter` over the
/// corresponding data block.  `TwoLevelIterator` advances through data blocks
/// automatically, so callers see a flat stream of (internal-key, value) pairs.
///
/// See `table/two_level_iterator.h/cc`.
pub(crate) struct TwoLevelIterator {
  index_iter: Box<dyn InternalIterator>,
  data_iter: Option<Box<dyn InternalIterator>>,
  block_fn: BlockFn,
  /// Encoded `BlockHandle` of the currently open data block.  Used to skip
  /// redundant re-opens when consecutive seeks land in the same block.
  data_block_handle: Vec<u8>,
  /// Sticky I/O or corruption error.
  status: Option<Error>,
}

impl TwoLevelIterator {
  pub(crate) fn new(index_iter: Box<dyn InternalIterator>, block_fn: BlockFn) -> Self {
    TwoLevelIterator {
      index_iter,
      data_iter: None,
      block_fn,
      data_block_handle: Vec::new(),
      status: None,
    }
  }

  /// Open (or reuse) the data block that the index iterator currently points at.
  fn init_data_block(&mut self) {
    if !self.index_iter.valid() {
      self.data_iter = None;
      return;
    }
    // Clone the handle value before mutating self (borrow checker).
    let handle_value = self.index_iter.value().to_vec();
    if self.data_iter.is_some() && handle_value == self.data_block_handle {
      return; // same block — reuse the existing data iterator
    }
    self.data_block_handle = handle_value.clone();
    match (self.block_fn)(&handle_value) {
      Ok(iter) => self.data_iter = Some(iter),
      Err(e) => {
        self.data_iter = None;
        self.data_block_handle.clear();
        if self.status.is_none() {
          self.status = Some(e);
        }
      }
    }
  }

  /// Advance past any empty data blocks (rare in practice; handles edge cases
  /// such as a failed block open leaving `data_iter` as `None`).
  fn skip_empty_data_blocks_forward(&mut self) {
    loop {
      if self.data_iter.as_ref().is_some_and(|it| it.valid()) {
        break;
      }
      if !self.index_iter.valid() {
        self.data_iter = None;
        return;
      }
      self.index_iter.next();
      self.init_data_block();
      if let Some(ref mut it) = self.data_iter {
        it.seek_to_first();
      }
    }
  }

  /// Move backward past any empty or exhausted data blocks.
  fn skip_empty_data_blocks_backward(&mut self) {
    loop {
      if self.data_iter.as_ref().is_some_and(|it| it.valid()) {
        break;
      }
      if !self.index_iter.valid() {
        self.data_iter = None;
        return;
      }
      self.index_iter.prev();
      if !self.index_iter.valid() {
        self.data_iter = None;
        return;
      }
      self.init_data_block();
      if let Some(ref mut it) = self.data_iter {
        it.seek_to_last();
      }
    }
  }
}

impl InternalIterator for TwoLevelIterator {
  fn valid(&self) -> bool {
    self.data_iter.as_ref().is_some_and(|it| it.valid())
  }

  fn seek_to_first(&mut self) {
    self.index_iter.seek_to_first();
    self.init_data_block();
    if let Some(ref mut it) = self.data_iter {
      it.seek_to_first();
    }
    self.skip_empty_data_blocks_forward();
  }

  fn seek_to_last(&mut self) {
    self.index_iter.seek_to_last();
    self.init_data_block();
    if let Some(ref mut it) = self.data_iter {
      it.seek_to_last();
    }
    self.skip_empty_data_blocks_backward();
  }

  fn seek(&mut self, target: &[u8]) {
    self.index_iter.seek(target);
    self.init_data_block();
    if let Some(ref mut it) = self.data_iter {
      it.seek(target);
    }
    self.skip_empty_data_blocks_forward();
  }

  fn next(&mut self) {
    debug_assert!(self.valid());
    self.data_iter.as_mut().unwrap().next();
    self.skip_empty_data_blocks_forward();
  }

  fn prev(&mut self) {
    debug_assert!(self.valid());
    self.data_iter.as_mut().unwrap().prev();
    self.skip_empty_data_blocks_backward();
  }

  fn key(&self) -> &[u8] {
    debug_assert!(self.valid());
    self.data_iter.as_ref().unwrap().key()
  }

  fn value(&self) -> &[u8] {
    debug_assert!(self.valid());
    self.data_iter.as_ref().unwrap().value()
  }

  fn status(&self) -> Option<&Error> {
    if let Some(ref e) = self.status {
      return Some(e);
    }
    self.index_iter.status()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::table::builder::TableBuilder;
  use crate::table::format::{make_internal_key, parse_internal_key};
  use crate::table::reader::Table;

  fn build_table(pairs: &[(&[u8], &[u8])], block_size: usize) -> (tempfile::NamedTempFile, u64) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut builder = TableBuilder::new(file, block_size, 16);
    for (seq, &(k, v)) in pairs.iter().enumerate() {
      let ikey = make_internal_key(k, seq as u64 + 1, 1);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    (tmp, size)
  }

  fn user_key(ikey: &[u8]) -> &[u8] {
    parse_internal_key(ikey)
      .map(|(uk, _, _)| uk)
      .expect("valid internal key")
  }

  #[test]
  fn empty_table_not_valid() {
    let (tmp, size) = build_table(&[], 4096);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    let mut it = table.new_iterator().unwrap();
    it.seek_to_first();
    assert!(!it.valid());
  }

  #[test]
  fn single_block_iterate_all() {
    let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let (tmp, size) = build_table(pairs, 4096);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    let mut it = table.new_iterator().unwrap();
    it.seek_to_first();
    for (k, v) in pairs {
      assert!(it.valid());
      assert_eq!(user_key(it.key()), *k);
      assert_eq!(it.value(), *v);
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn seek_to_existing_key() {
    let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let (tmp, size) = build_table(pairs, 4096);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    let mut it = table.new_iterator().unwrap();
    // Lookup key with max sequence number seeks to the newest version.
    let target = make_internal_key(b"b", u64::MAX >> 8, 1);
    it.seek(&target);
    assert!(it.valid());
    assert_eq!(user_key(it.key()), b"b");
    assert_eq!(it.value(), b"2");
  }

  #[test]
  fn seek_past_last_key() {
    let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2")];
    let (tmp, size) = build_table(pairs, 4096);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    let mut it = table.new_iterator().unwrap();
    let target = make_internal_key(b"z", u64::MAX >> 8, 1);
    it.seek(&target);
    assert!(!it.valid());
  }

  #[test]
  fn multiple_blocks_iterate_all() {
    // Tiny block_size forces many data blocks.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0u32..50)
      .map(|i| {
        (
          format!("key{i:05}").into_bytes(),
          format!("val{i:05}").into_bytes(),
        )
      })
      .collect();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut builder = TableBuilder::new(file, 64, 4);
    for (seq, (k, v)) in pairs.iter().enumerate() {
      let ikey = make_internal_key(k, seq as u64 + 1, 1);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    let mut it = table.new_iterator().unwrap();
    it.seek_to_first();
    for (k, v) in &pairs {
      assert!(it.valid(), "expected valid iterator for key {:?}", k);
      assert_eq!(user_key(it.key()), k.as_slice());
      assert_eq!(it.value(), v.as_slice());
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn multiple_blocks_seek() {
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0u32..50)
      .map(|i| {
        (
          format!("key{i:05}").into_bytes(),
          format!("val{i:05}").into_bytes(),
        )
      })
      .collect();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut builder = TableBuilder::new(file, 64, 4);
    for (seq, (k, v)) in pairs.iter().enumerate() {
      let ikey = make_internal_key(k, seq as u64 + 1, 1);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    let mut it = table.new_iterator().unwrap();
    // Seek to the middle; verify we land at the right key and can iterate forward.
    let target = make_internal_key(b"key00025", u64::MAX >> 8, 1);
    it.seek(&target);
    assert!(it.valid());
    assert_eq!(user_key(it.key()), b"key00025");
    assert_eq!(it.value(), b"val00025");
    it.next();
    assert!(it.valid());
    assert_eq!(user_key(it.key()), b"key00026");
  }
}
