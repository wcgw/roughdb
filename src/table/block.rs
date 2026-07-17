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
  /// Returns `Error::Corruption` if `data` is too short to hold a valid restart
  /// array, rather than panicking — the bytes may come from a corrupt SSTable.
  pub(crate) fn new(data: Vec<u8>, comparator: Arc<dyn Comparator>) -> Result<Self, Error> {
    if data.len() < 4 {
      return Err(Error::Corruption(format!(
        "block too short: {} bytes",
        data.len()
      )));
    }
    let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
    // `num_restarts` comes straight off disk; guard the multiply against overflow.
    let restarts_size = num_restarts
      .checked_mul(4)
      .and_then(|v| v.checked_add(4)) // offsets + count field
      .ok_or_else(|| Error::Corruption("block restart count overflow".to_owned()))?;
    if data.len() < restarts_size {
      return Err(Error::Corruption(format!(
        "block data ({} bytes) too short for {num_restarts} restart points",
        data.len()
      )));
    }
    let restarts_offset = data.len() - restarts_size;
    Ok(Block {
      data: Arc::new(data),
      restarts_offset,
      num_restarts,
      comparator,
    })
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
      status: None,
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
  /// Sticky corruption error: set the first time a malformed entry is decoded.
  /// Once set, the iterator is invalidated and stays that way.
  status: Option<Error>,
}

/// Read a varint starting at `pos`, refusing to read past `limit` (the start of
/// the restart array).  Returns `(value, new_pos)` or `None` on truncation.
fn read_varu64_at(data: &[u8], pos: usize, limit: usize) -> Option<(u64, usize)> {
  if pos > limit {
    return None;
  }
  let (value, n) = read_varu64(&data[pos..limit]);
  if n == 0 {
    return None; // malformed or truncated varint
  }
  Some((value, pos + n))
}

impl BlockIter {
  fn restart_point(&self, index: usize) -> usize {
    let off = self.restarts_offset + index * 4;
    u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap()) as usize
  }

  /// Decode the entry at `self.current`, updating `key`, `value_start`, `value_len`.
  ///
  /// Assumes `self.key` already holds the full key from the previous entry
  /// (or is empty at a restart point).  A corrupt entry (bad varint, or a
  /// length that runs past the restart array) invalidates the iterator and
  /// records a sticky `status`, rather than panicking on an out-of-bounds slice.
  fn decode_entry(&mut self) {
    if self.try_decode_entry().is_none() {
      self.mark_corrupt();
    }
  }

  /// Fallible core of `decode_entry`; returns `None` on any malformed field.
  fn try_decode_entry(&mut self) -> Option<()> {
    let data = &*self.data;
    let limit = self.restarts_offset;

    let (shared, pos) = read_varu64_at(data, self.current, limit)?;
    let (unshared, pos) = read_varu64_at(data, pos, limit)?;
    let (vlen, pos) = read_varu64_at(data, pos, limit)?;

    let shared = shared as usize;
    let unshared = unshared as usize;
    let vlen = vlen as usize;

    // `shared` can only reference bytes already in the reconstructed key.
    if shared > self.key.len() {
      return None;
    }
    // The unshared key suffix and the value must both fit before the restarts.
    let key_end = pos.checked_add(unshared)?;
    let value_end = key_end.checked_add(vlen)?;
    if value_end > limit {
      return None;
    }

    self.key.truncate(shared);
    self.key.extend_from_slice(&data[pos..key_end]);
    self.value_start = key_end;
    self.value_len = vlen;
    Some(())
  }

  /// Invalidate the iterator and record a sticky corruption error.
  fn mark_corrupt(&mut self) {
    self.current = self.restarts_offset;
    // Make `value_start + value_len` land exactly at the restart array so the
    // forward-scan loops in `seek_to_last`/`prev` terminate immediately.
    self.value_start = self.restarts_offset;
    self.value_len = 0;
    self.key.clear();
    if self.status.is_none() {
      self.status = Some(Error::Corruption("corrupt block entry".to_owned()));
    }
  }

  /// Return the key stored at restart point `index`, or `None` if the entry
  /// there is malformed.  Does not modify `self`.
  ///
  /// At a restart point the key is stored contiguously (shared prefix length
  /// is always 0), so it can be borrowed from block memory — no allocation.
  fn key_at_restart(&self, index: usize) -> Option<&[u8]> {
    let data = &*self.data;
    let limit = self.restarts_offset;
    // At a restart point shared_len is always 0, so the unshared suffix is the
    // full key.
    let (_shared, pos) = read_varu64_at(data, self.restart_point(index), limit)?;
    let (unshared, pos) = read_varu64_at(data, pos, limit)?;
    let (_vlen, pos) = read_varu64_at(data, pos, limit)?;
    let key_end = pos.checked_add(unshared as usize)?;
    if key_end > limit {
      return None;
    }
    Some(&data[pos..key_end])
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
      let restart_key = match self.key_at_restart(mid) {
        Some(k) => k,
        None => {
          self.mark_corrupt();
          return;
        }
      };
      match cmp_internal_keys(restart_key, target, &*self.comparator) {
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
    self.status.as_ref()
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
    .unwrap()
  }

  #[test]
  fn empty_block_not_valid() {
    let mut bb = BlockBuilder::new(16);
    let block = Block::new(
      bb.finish().to_vec(),
      Arc::new(crate::comparator::BytewiseComparator),
    )
    .unwrap();
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
    )
    .unwrap();
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

  // ── Corruption handling ───────────────────────────────────────────────────
  //
  // A corrupt data block must never panic on an out-of-bounds slice; the
  // iterator should go invalid and expose a sticky `Corruption` status.

  fn bytewise() -> Arc<dyn Comparator> {
    Arc::new(crate::comparator::BytewiseComparator)
  }

  /// Raw bytes of a valid single-entry block `("k" -> "v")`, restart_interval 16.
  ///
  /// Layout: `[shared=0][unshared=1][vlen=1]['k']['v'] [restart u32 = 0][count u32 = 1]`.
  /// So byte 0 = shared, 1 = unshared, 2 = vlen, 3 = key, 4 = value;
  /// `restarts_offset` = 5.
  fn single_entry_bytes() -> Vec<u8> {
    let mut bb = BlockBuilder::new(16);
    bb.add(b"k", b"v");
    bb.finish().to_vec()
  }

  #[test]
  fn block_new_rejects_too_short() {
    // Fewer than 4 bytes cannot even hold the restart count.
    assert!(matches!(
      Block::new(vec![0u8; 3], bytewise()),
      Err(Error::Corruption(_))
    ));
  }

  #[test]
  fn block_new_rejects_oversized_restart_count() {
    // 8 bytes: a (bogus) restart offset followed by a restart count of
    // 0xFFFF_FFFF, which would require far more data than is present.
    let mut data = vec![0u8; 4];
    data.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
      Block::new(data, bytewise()),
      Err(Error::Corruption(_))
    ));
  }

  #[test]
  fn corrupt_unshared_length_marks_invalid() {
    let mut data = single_entry_bytes();
    data[1] = 0x7f; // unshared = 127, runs past the 5-byte entry region
    let block = Block::new(data, bytewise()).unwrap();
    let mut it = block.iter();
    it.seek_to_first();
    assert!(!it.valid());
    assert!(matches!(it.status(), Some(Error::Corruption(_))));
  }

  #[test]
  fn corrupt_value_length_marks_invalid() {
    let mut data = single_entry_bytes();
    data[2] = 0x7f; // vlen = 127, runs past the end of the block
    let block = Block::new(data, bytewise()).unwrap();
    let mut it = block.iter();
    it.seek_to_first();
    assert!(!it.valid());
    assert!(matches!(it.status(), Some(Error::Corruption(_))));
  }

  #[test]
  fn corrupt_shared_length_marks_invalid() {
    let mut data = single_entry_bytes();
    data[0] = 0x05; // shared = 5 at a restart point (must be 0; exceeds key len)
    let block = Block::new(data, bytewise()).unwrap();
    let mut it = block.iter();
    it.seek_to_first();
    assert!(!it.valid());
    assert!(matches!(it.status(), Some(Error::Corruption(_))));
  }

  #[test]
  fn truncated_varint_marks_invalid() {
    let mut data = single_entry_bytes();
    // Fill the whole entry region with continuation bytes: the varint never
    // terminates before the restart array.
    for b in data.iter_mut().take(5) {
      *b = 0x80;
    }
    let block = Block::new(data, bytewise()).unwrap();
    let mut it = block.iter();
    it.seek_to_first();
    assert!(!it.valid());
    assert!(matches!(it.status(), Some(Error::Corruption(_))));
  }

  #[test]
  fn corrupt_restart_key_in_seek_marks_invalid() {
    // restart_interval=1 makes every entry a restart point, so `seek`'s binary
    // search calls `key_at_restart` on an interior entry.  Each entry is
    // [shared=0][unshared=1][vlen=1][key][val] = 5 bytes; entry 1 starts at
    // offset 5, its unshared-length byte is at offset 6.
    let mut bb = BlockBuilder::new(1);
    bb.add(b"a", b"1");
    bb.add(b"b", b"2");
    bb.add(b"c", b"3");
    let mut data = bb.finish().to_vec();
    data[6] = 0x7f; // corrupt entry 1's unshared length
    let block = Block::new(data, bytewise()).unwrap();
    let mut it = block.iter();
    it.seek(b"b");
    assert!(!it.valid());
    assert!(matches!(it.status(), Some(Error::Corruption(_))));
  }
}
