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

use crate::coding::read_varu64;
use crate::comparator::Comparator;
use crate::error::Error;
use crate::iter::InternalIterator;
use crate::table::format::cmp_internal_keys;
use std::sync::Arc;

/// An immutable, decoded SSTable block.
///
/// The raw `data` slice holds the serialised entries followed by the restart
/// point array and a 4-byte count.  `Block::iter()` returns a `BlockIter`
/// that decodes entries on the fly.  See `table/block.h/cc`.
#[derive(Clone)]
pub(crate) struct Block {
  /// Block data shared with iterators via `Arc` (zero-copy `iter()`).
  data: Arc<Vec<u8>>,
  /// Byte offset of the first restart-point u32 within `data`.
  restarts_offset: usize,
  /// Number of restart points.
  num_restarts: usize,
  /// Comparator for user-key ordering.
  comparator: Arc<dyn Comparator>,
}

impl Block {
  /// Wrap raw block bytes (as returned by `read_block`, minus trailer).
  ///
  /// Panics if `data` is too short to hold a valid restart count.
  pub(crate) fn new(data: Vec<u8>, comparator: Arc<dyn Comparator>) -> Self {
    assert!(data.len() >= 4, "block too short: {} bytes", data.len());
    let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
    let restarts_size = num_restarts * 4 + 4; // offsets + count field
    assert!(
      data.len() >= restarts_size,
      "block data ({} bytes) too short for {num_restarts} restart points",
      data.len()
    );
    let restarts_offset = data.len() - restarts_size;
    Block {
      data: Arc::new(data),
      restarts_offset,
      num_restarts,
      comparator,
    }
  }

  /// Return the raw block data bytes (used by the block cache for charge accounting).
  pub(crate) fn data(&self) -> &[u8] {
    &self.data
  }

  /// Return an iterator over this block's entries.
  ///
  /// The iterator clones the `Arc` — no data is copied.
  pub(crate) fn iter(&self) -> BlockIter {
    BlockIter {
      data: Arc::clone(&self.data),
      restarts_offset: self.restarts_offset,
      num_restarts: self.num_restarts,
      current: self.restarts_offset, // starts invalid
      key: Vec::new(),
      value_start: 0,
      value_len: 0,
      comparator: Arc::clone(&self.comparator),
    }
  }
}

// ── BlockIter ─────────────────────────────────────────────────────────────────

/// Forward iterator over a `Block`.
///
/// Owns an `Arc` reference to the block data, so it can be stored anywhere
/// without a lifetime parameter.  Reconstructs full keys from delta-encoded
/// (shared_len, unshared_len) pairs and supports `seek` via binary search
/// over restart points.  See `table/block.cc`.
pub(crate) struct BlockIter {
  data: Arc<Vec<u8>>,
  restarts_offset: usize,
  num_restarts: usize,
  /// Current position in `data` (points at next entry's shared_len varint,
  /// or `>= restarts_offset` when invalid).
  current: usize,
  /// Fully reconstructed key at `current`.
  key: Vec<u8>,
  /// Value slice start within `data`.
  value_start: usize,
  /// Value slice length.
  value_len: usize,
  /// Comparator for user-key ordering.
  comparator: Arc<dyn Comparator>,
}

impl BlockIter {
  fn restart_point(&self, index: usize) -> usize {
    let off = self.restarts_offset + index * 4;
    u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap()) as usize
  }

  /// Decode the entry at `self.current`, updating `key`, `value_start`, `value_len`.
  ///
  /// Assumes `self.key` already holds the full key from the previous entry
  /// (or is empty at a restart point).
  fn decode_entry(&mut self) {
    let data = &*self.data;
    let mut pos = self.current;

    let (shared, n) = read_varu64(&data[pos..]);
    pos += n;
    let (unshared, n) = read_varu64(&data[pos..]);
    pos += n;
    let (vlen, n) = read_varu64(&data[pos..]);
    pos += n;

    let shared = shared as usize;
    let unshared = unshared as usize;
    let vlen = vlen as usize;

    self.key.truncate(shared);
    self.key.extend_from_slice(&data[pos..pos + unshared]);
    pos += unshared;

    self.value_start = pos;
    self.value_len = vlen;
  }

  /// Return the key stored at restart point `index` without modifying `self`.
  fn key_at_restart(&self, index: usize) -> Vec<u8> {
    let data = &*self.data;
    let mut pos = self.restart_point(index);
    // At a restart point shared_len is always 0.
    let (_shared, n) = read_varu64(&data[pos..]);
    pos += n;
    let (unshared, n) = read_varu64(&data[pos..]);
    pos += n;
    let (_vlen, n) = read_varu64(&data[pos..]);
    pos += n;
    data[pos..pos + unshared as usize].to_vec()
  }
}

impl InternalIterator for BlockIter {
  fn valid(&self) -> bool {
    self.current < self.restarts_offset
  }

  fn seek_to_first(&mut self) {
    if self.restarts_offset == 0 {
      return; // empty block
    }
    self.current = 0;
    self.key.clear();
    self.decode_entry();
  }

  /// Position at the last entry.
  ///
  /// Starts from the last restart point and scans forward to the final entry.
  fn seek_to_last(&mut self) {
    if self.num_restarts == 0 || self.restarts_offset == 0 {
      return; // empty block stays invalid
    }
    let last_restart = self.num_restarts - 1;
    self.current = self.restart_point(last_restart);
    self.key.clear();
    self.decode_entry();
    // Advance until no more entries remain before the restart array.
    while self.value_start + self.value_len < self.restarts_offset {
      self.current = self.value_start + self.value_len;
      self.decode_entry();
    }
  }

  /// Position at the first entry with `key >= target`.
  ///
  /// Uses binary search over restart points, then linear scan within the
  /// selected region.
  fn seek(&mut self, target: &[u8]) {
    let mut lo = 0usize;
    let mut hi = self.num_restarts; // exclusive
    while lo + 1 < hi {
      let mid = lo + (hi - lo) / 2;
      let restart_key = self.key_at_restart(mid);
      match cmp_internal_keys(restart_key.as_slice(), target, &*self.comparator) {
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal => lo = mid,
        std::cmp::Ordering::Greater => hi = mid,
      }
    }
    self.current = self.restart_point(lo);
    self.key.clear();
    self.decode_entry();
    while self.valid()
      && cmp_internal_keys(self.key(), target, &*self.comparator) == std::cmp::Ordering::Less
    {
      self.next();
    }
  }

  fn next(&mut self) {
    debug_assert!(self.valid());
    self.current = self.value_start + self.value_len;
    if self.current < self.restarts_offset {
      self.decode_entry();
    }
  }

  /// Move to the entry immediately before the current one.
  ///
  /// Uses restart points to binary-search for the restart block that
  /// contains the predecessor, then forward-scans within that block.
  /// O(restart_interval) forward steps after an O(log(num_restarts)) search.
  fn prev(&mut self) {
    debug_assert!(self.valid());
    let current_pos = self.current;

    // If we are at the very first entry (restart point 0 is always offset 0),
    // there is no predecessor — become invalid.
    if current_pos == 0 {
      self.current = self.restarts_offset; // past restarts → invalid
      self.key.clear();
      return;
    }

    // Find the largest restart index whose offset is strictly < current_pos.
    // Restart indices are sorted ascending, so we scan linearly (num_restarts
    // is typically small, e.g. block_size / restart_interval ≈ 16).
    let mut lo = 0usize;
    for i in 1..self.num_restarts {
      if self.restart_point(i) < current_pos {
        lo = i;
      } else {
        break;
      }
    }

    // Scan forward from restart_point(lo) until the *next* entry is at
    // current_pos — that makes the current entry the predecessor.
    self.current = self.restart_point(lo);
    self.key.clear();
    self.decode_entry();
    while self.value_start + self.value_len < current_pos {
      self.current = self.value_start + self.value_len;
      self.decode_entry();
    }
  }

  fn key(&self) -> &[u8] {
    debug_assert!(self.valid());
    &self.key
  }

  fn value(&self) -> &[u8] {
    debug_assert!(self.valid());
    &self.data[self.value_start..self.value_start + self.value_len]
  }

  fn status(&self) -> Option<&Error> {
    None // BlockIter is pure memory; no I/O errors possible.
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::table::block_builder::BlockBuilder;

  fn make_block(pairs: &[(&[u8], &[u8])], interval: usize) -> Block {
    let mut bb = BlockBuilder::new(interval);
    for &(k, v) in pairs {
      bb.add(k, v);
    }
    Block::new(
      bb.finish().to_vec(),
      Arc::new(crate::comparator::BytewiseComparator),
    )
  }

  #[test]
  fn empty_block_not_valid() {
    let mut bb = BlockBuilder::new(16);
    let block = Block::new(
      bb.finish().to_vec(),
      Arc::new(crate::comparator::BytewiseComparator),
    );
    assert!(!block.iter().valid());
  }

  #[test]
  fn single_entry_round_trip() {
    let block = make_block(&[(b"mykey", b"myval")], 16);
    let mut it = block.iter();
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.key(), b"mykey");
    assert_eq!(it.value(), b"myval");
    it.next();
    assert!(!it.valid());
  }

  #[test]
  fn multiple_entries_in_order() {
    let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let block = make_block(pairs, 16);
    let mut it = block.iter();
    it.seek_to_first();
    for (k, v) in pairs {
      assert!(it.valid());
      assert_eq!(it.key(), *k);
      assert_eq!(it.value(), *v);
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn seek_exact_match() {
    let block = make_block(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")], 16);
    let mut it = block.iter();
    it.seek(b"b");
    assert!(it.valid());
    assert_eq!(it.key(), b"b");
    assert_eq!(it.value(), b"2");
  }

  #[test]
  fn seek_between_keys() {
    let block = make_block(&[(b"a", b"1"), (b"c", b"3")], 16);
    let mut it = block.iter();
    it.seek(b"b");
    assert!(it.valid());
    assert_eq!(it.key(), b"c");
  }

  #[test]
  fn seek_past_last_key() {
    let block = make_block(&[(b"a", b"1"), (b"b", b"2")], 16);
    let mut it = block.iter();
    it.seek(b"z");
    assert!(!it.valid());
  }

  #[test]
  fn seek_across_restart_boundaries() {
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (b'a'..=b'i').map(|c| (vec![c], vec![c + 1])).collect();
    let pairs_ref: Vec<(&[u8], &[u8])> = pairs
      .iter()
      .map(|(k, v)| (k.as_slice(), v.as_slice()))
      .collect();
    let block = make_block(&pairs_ref, 3);
    let mut it = block.iter();
    it.seek(b"f");
    assert!(it.valid());
    assert_eq!(it.key(), b"f");
    assert_eq!(it.value(), &[b'f' + 1]);
  }

  // ── Backward iteration tests ──────────────────────────────────────────────

  #[test]
  fn seek_to_last_empty_block() {
    let mut bb = BlockBuilder::new(16);
    let block = Block::new(
      bb.finish().to_vec(),
      Arc::new(crate::comparator::BytewiseComparator),
    );
    let mut it = block.iter();
    it.seek_to_last();
    assert!(!it.valid());
  }

  #[test]
  fn seek_to_last_single_entry() {
    let block = make_block(&[(b"only", b"v")], 16);
    let mut it = block.iter();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), b"only");
    assert_eq!(it.value(), b"v");
    it.prev();
    assert!(!it.valid());
  }

  #[test]
  fn seek_to_last_multiple_entries() {
    let block = make_block(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")], 16);
    let mut it = block.iter();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), b"c");
    assert_eq!(it.value(), b"3");
  }

  #[test]
  fn prev_iterates_all_entries_backward() {
    let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let block = make_block(pairs, 16);
    let mut it = block.iter();
    it.seek_to_last();
    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while it.valid() {
      result.push((it.key().to_vec(), it.value().to_vec()));
      it.prev();
    }
    assert_eq!(
      result,
      vec![
        (b"c".to_vec(), b"3".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"a".to_vec(), b"1".to_vec()),
      ]
    );
  }

  #[test]
  fn prev_across_restart_boundaries() {
    // restart_interval=3: restarts at entries 0, 3, 6, …
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (b'a'..=b'i').map(|c| (vec![c], vec![c + 1])).collect();
    let pairs_ref: Vec<(&[u8], &[u8])> = pairs
      .iter()
      .map(|(k, v)| (k.as_slice(), v.as_slice()))
      .collect();
    let block = make_block(&pairs_ref, 3);
    let mut it = block.iter();
    it.seek_to_last();
    let mut keys: Vec<u8> = Vec::new();
    while it.valid() {
      assert_eq!(it.key().len(), 1);
      keys.push(it.key()[0]);
      it.prev();
    }
    let expected: Vec<u8> = (b'a'..=b'i').rev().collect();
    assert_eq!(keys, expected);
  }
}
