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

use crate::coding::write_varu64;

/// Builds a data block or index block from sorted key-value pairs.
///
/// Each entry is stored as:
/// `[shared_len: varint][unshared_len: varint][value_len: varint][key_suffix][value]`
///
/// Every `restart_interval`-th key is a "restart point" — its `shared_len` is 0
/// and its absolute offset is appended to a trailing restart array, enabling
/// binary search during reads.
///
/// Call `finish()` to obtain the complete block bytes, then `reset()` to reuse
/// the builder for a new block.  See `table/block_builder.h/cc`.
pub(crate) struct BlockBuilder {
  buf: Vec<u8>,
  restarts: Vec<u32>,
  last_key: Vec<u8>,
  counter: usize,
  restart_interval: usize,
  finished: bool,
}

impl BlockBuilder {
  pub(crate) fn new(restart_interval: usize) -> Self {
    assert!(restart_interval >= 1);
    Self {
      buf: Vec::new(),
      restarts: vec![0], // offset 0 is always a restart point
      last_key: Vec::new(),
      counter: 0,
      restart_interval,
      finished: false,
    }
  }

  /// Append a key-value pair.  `key` must be ≥ the last key added (sorted order).
  pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) {
    debug_assert!(!self.finished, "add called after finish");

    // Compute shared prefix length with the last key.
    let shared = if self.counter % self.restart_interval == 0 {
      // Restart point: no sharing.
      self.restarts.push(self.buf.len() as u32);
      0
    } else {
      shared_prefix_len(&self.last_key, key)
    };

    let unshared = key.len() - shared;

    // Encode the three varint lengths.
    let mut tmp = [0u8; 10];
    let n = write_varu64(&mut tmp, shared as u64);
    self.buf.extend_from_slice(&tmp[..n]);
    let n = write_varu64(&mut tmp, unshared as u64);
    self.buf.extend_from_slice(&tmp[..n]);
    let n = write_varu64(&mut tmp, value.len() as u64);
    self.buf.extend_from_slice(&tmp[..n]);

    // Key suffix (unshared portion) and value.
    self.buf.extend_from_slice(&key[shared..]);
    self.buf.extend_from_slice(value);

    self.last_key.clear();
    self.last_key.extend_from_slice(key);
    self.counter += 1;
  }

  /// Finish the block: append the restart array + count, return the bytes.
  ///
  /// Do not call `add` after `finish`.  Call `reset` to start a new block.
  pub(crate) fn finish(&mut self) -> &[u8] {
    for &r in &self.restarts {
      self.buf.extend_from_slice(&r.to_le_bytes());
    }
    let n = self.restarts.len() as u32;
    self.buf.extend_from_slice(&n.to_le_bytes());
    self.finished = true;
    &self.buf
  }

  /// Reset the builder, discarding all accumulated data.
  pub(crate) fn reset(&mut self) {
    self.buf.clear();
    self.restarts.clear();
    self.restarts.push(0);
    self.last_key.clear();
    self.counter = 0;
    self.finished = false;
  }

  /// Estimate of the current unfinished block size in bytes (excludes restart array footer).
  pub(crate) fn current_size_estimate(&self) -> usize {
    self.buf.len() + self.restarts.len() * 4 + 4
  }

  pub(crate) fn is_empty(&self) -> bool {
    self.buf.is_empty()
  }
}

fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
  a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::iter::InternalIterator;
  use crate::table::block::Block;

  fn build_block(pairs: &[(&[u8], &[u8])], interval: usize) -> Vec<u8> {
    let mut bb = BlockBuilder::new(interval);
    for &(k, v) in pairs {
      bb.add(k, v);
    }
    bb.finish().to_vec()
  }

  #[test]
  fn empty_block_finish() {
    let mut bb = BlockBuilder::new(16);
    let data = bb.finish().to_vec();
    // Should have at least the restart count (4 bytes) + one restart offset (4 bytes) = 8.
    assert!(data.len() >= 8);
    let block = Block::new(data);
    let it = block.iter();
    assert!(!it.valid());
  }

  #[test]
  fn single_entry() {
    let data = build_block(&[(b"key", b"value")], 16);
    let block = Block::new(data);
    let mut it = block.iter();
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.key(), b"key");
    assert_eq!(it.value(), b"value");
    it.next();
    assert!(!it.valid());
  }

  #[test]
  fn multiple_entries_prefix_compression() {
    let pairs: Vec<(&[u8], &[u8])> =
      vec![(b"aa", b"1"), (b"ab", b"2"), (b"ac", b"3"), (b"ba", b"4")];
    let data = build_block(&pairs, 16);
    let block = Block::new(data);
    let mut it = block.iter();
    it.seek_to_first();
    for (expected_k, expected_v) in pairs {
      assert!(it.valid());
      assert_eq!(it.key(), expected_k);
      assert_eq!(it.value(), expected_v);
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn restart_interval_creates_restart_points() {
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0u8..6).map(|i| (vec![i], vec![i + 10])).collect();
    let pairs_ref: Vec<(&[u8], &[u8])> = pairs
      .iter()
      .map(|(k, v)| (k.as_slice(), v.as_slice()))
      .collect();
    let data = build_block(&pairs_ref, 3);
    let block = Block::new(data.clone());
    // Verify all 6 entries readable in order.
    let mut it = block.iter();
    it.seek_to_first();
    for (ek, _ev) in &pairs {
      assert!(it.valid());
      assert_eq!(it.key(), ek.as_slice());
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn seek_finds_entry() {
    let pairs: Vec<(&[u8], &[u8])> = vec![
      (b"a", b"1"),
      (b"b", b"2"),
      (b"c", b"3"),
      (b"d", b"4"),
      (b"e", b"5"),
    ];
    let data = build_block(&pairs, 2);
    let block = Block::new(data);
    let mut it = block.iter();
    it.seek(b"c");
    assert!(it.valid());
    assert_eq!(it.key(), b"c");
    assert_eq!(it.value(), b"3");
  }

  #[test]
  fn reset_allows_reuse() {
    let mut bb = BlockBuilder::new(16);
    bb.add(b"old", b"data");
    bb.finish();
    bb.reset();
    bb.add(b"new", b"entry");
    let data = bb.finish().to_vec();
    let block = Block::new(data);
    let mut it = block.iter();
    it.seek_to_first();
    assert_eq!(it.key(), b"new");
    it.next();
    assert!(!it.valid());
  }
}
