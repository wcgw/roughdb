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
use crate::table::format::cmp_internal_keys;

/// An immutable, decoded SSTable block.
///
/// The raw `data` slice holds the serialised entries followed by the restart
/// point array and a 4-byte count.  `Block::iter()` returns a forward-only
/// `BlockIterator` that decodes entries on the fly.  See `table/block.h/cc`.
pub(crate) struct Block {
  data: Vec<u8>,
  /// Byte offset of the first restart-point u32 within `data`.
  restarts_offset: usize,
  /// Number of restart points.
  num_restarts: usize,
}

impl Block {
  /// Wrap raw block bytes (as returned by `read_block`, minus trailer).
  ///
  /// Panics if `data` is too short to hold a valid restart count.
  pub(crate) fn new(data: Vec<u8>) -> Self {
    assert!(data.len() >= 4, "block too short: {} bytes", data.len());
    let num_restarts =
      u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
    let restarts_size = num_restarts * 4 + 4; // offsets + count field
    assert!(
      data.len() >= restarts_size,
      "block data ({} bytes) too short for {num_restarts} restart points",
      data.len()
    );
    let restarts_offset = data.len() - restarts_size;
    Block { data, restarts_offset, num_restarts }
  }

  /// Return a forward iterator over this block's entries.
  pub(crate) fn iter(&self) -> BlockIterator<'_> {
    BlockIterator::new(self)
  }

  fn restart_point(&self, index: usize) -> usize {
    let off = self.restarts_offset + index * 4;
    u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap()) as usize
  }
}

// ── BlockIterator ─────────────────────────────────────────────────────────────

/// Forward-only iterator over a `Block`.
///
/// Advances entry by entry, reconstructing the full key from the delta-encoded
/// (shared_len, unshared_len) pair.  Supports `seek` via binary search over the
/// restart point array.  See `table/block.cc`.
pub(crate) struct BlockIterator<'a> {
  block: &'a Block,
  /// Current position in `block.data` (points at the next entry's shared_len varint,
  /// or `>= restarts_offset` when invalid).
  current: usize,
  /// Fully reconstructed key at `current`.
  key: Vec<u8>,
  /// Value slice start within `block.data`.
  value_start: usize,
  /// Value slice length.
  value_len: usize,
}

impl<'a> BlockIterator<'a> {
  fn new(block: &'a Block) -> Self {
    BlockIterator {
      block,
      current: block.restarts_offset, // starts invalid
      key: Vec::new(),
      value_start: 0,
      value_len: 0,
    }
  }

  pub(crate) fn valid(&self) -> bool {
    self.current < self.block.restarts_offset
  }

  pub(crate) fn key(&self) -> &[u8] {
    debug_assert!(self.valid());
    &self.key
  }

  pub(crate) fn value(&self) -> &[u8] {
    debug_assert!(self.valid());
    &self.block.data[self.value_start..self.value_start + self.value_len]
  }

  pub(crate) fn seek_to_first(&mut self) {
    if self.block.restarts_offset == 0 {
      return; // empty block
    }
    self.current = 0;
    self.key.clear();
    self.decode_entry();
  }

  /// Position at the first entry with `key >= target`.
  ///
  /// Uses binary search over restart points, then linear scan within the
  /// selected region.  After the call, `valid()` is true iff such an entry exists.
  pub(crate) fn seek(&mut self, target: &[u8]) {
    // Binary search restart points for the largest restart whose key <= target.
    let mut lo = 0usize;
    let mut hi = self.block.num_restarts; // exclusive
    while lo + 1 < hi {
      let mid = lo + (hi - lo) / 2;
      let restart_key = self.key_at_restart(mid);
      match cmp_internal_keys(restart_key.as_slice(), target) {
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal => lo = mid,
        std::cmp::Ordering::Greater => hi = mid,
      }
    }
    // Linear scan from restart point `lo`.
    self.current = self.block.restart_point(lo);
    self.key.clear();
    self.decode_entry();
    while self.valid() && cmp_internal_keys(self.key(), target) == std::cmp::Ordering::Less {
      self.next();
    }
  }

  pub(crate) fn next(&mut self) {
    debug_assert!(self.valid());
    self.current = self.value_start + self.value_len;
    if self.current < self.block.restarts_offset {
      self.decode_entry();
    }
  }

  /// Decode the entry at `self.current`, updating `self.key`, `value_start`, `value_len`.
  ///
  /// Assumes `self.key` already holds the full key from the previous entry
  /// (or is empty at a restart point).
  fn decode_entry(&mut self) {
    let data = &self.block.data;
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

    // Reconstruct key: keep shared prefix, append unshared suffix.
    self.key.truncate(shared);
    self.key.extend_from_slice(&data[pos..pos + unshared]);
    pos += unshared;

    self.value_start = pos;
    self.value_len = vlen;
  }

  /// Return the key stored at restart point `index` without modifying `self`.
  fn key_at_restart(&self, index: usize) -> Vec<u8> {
    let data = &self.block.data;
    let mut pos = self.block.restart_point(index);
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::table::block_builder::BlockBuilder;

  fn make_block(pairs: &[(&[u8], &[u8])], interval: usize) -> Block {
    let mut bb = BlockBuilder::new(interval);
    for &(k, v) in pairs {
      bb.add(k, v);
    }
    Block::new(bb.finish().to_vec())
  }

  #[test]
  fn empty_block_not_valid() {
    let mut bb = BlockBuilder::new(16);
    let block = Block::new(bb.finish().to_vec());
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
    // Target doesn't exist; iterator lands on the next key.
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
    // 9 entries, restart every 3 — forces binary search across multiple restart points.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> =
      (b'a'..=b'i').map(|c| (vec![c], vec![c + 1])).collect();
    let pairs_ref: Vec<(&[u8], &[u8])> = pairs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    let block = make_block(&pairs_ref, 3);
    let mut it = block.iter();
    it.seek(b"f");
    assert!(it.valid());
    assert_eq!(it.key(), b"f");
    assert_eq!(it.value(), &[b'f' + 1]);
  }
}
