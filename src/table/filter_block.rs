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

use crate::filter::FilterPolicy;
use std::sync::Arc;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Log₂ of the filter interval in bytes (`1 << FILTER_BASE_LG` = 2 KiB).
/// One filter is generated per 2 KiB segment of data block offsets.
/// See `table/filter_block.cc: kFilterBaseLg`.
const FILTER_BASE_LG: u8 = 11;
const FILTER_BASE: u64 = 1 << FILTER_BASE_LG;

// ── FilterBlockWriter ─────────────────────────────────────────────────────────

/// Builds a filter block for an SSTable.
///
/// Usage (matches `TableBuilder`'s call pattern):
/// 1. Call `start_block(block_offset)` before each data block.
/// 2. Call `add_key(key)` for every key written into that data block.
/// 3. Call `finish()` after the last data block to get the filter block bytes.
///
/// See `table/filter_block.h/cc: FilterBlockBuilder`.
pub(crate) struct FilterBlockWriter {
  policy: Arc<dyn FilterPolicy>,
  /// Concatenated raw key bytes.
  keys: Vec<u8>,
  /// Start index within `keys` for each accumulated key.
  key_starts: Vec<usize>,
  /// Growing filter block: concatenated filter bytes for all generated filters.
  result: Vec<u8>,
  /// Offset within `result` where each generated filter starts.
  filter_offsets: Vec<u32>,
}

impl FilterBlockWriter {
  pub(crate) fn new(policy: Arc<dyn FilterPolicy>) -> Self {
    Self {
      policy,
      keys: Vec::new(),
      key_starts: Vec::new(),
      result: Vec::new(),
      filter_offsets: Vec::new(),
    }
  }

  /// Notify the writer that a new data block starting at `block_offset` is
  /// about to be written.  Generates any filters for completed intervals.
  pub(crate) fn start_block(&mut self, block_offset: u64) {
    let filter_index = block_offset / FILTER_BASE;
    // Generate one empty filter for every interval that ended before this block.
    while filter_index > self.filter_offsets.len() as u64 {
      self.generate_filter();
    }
  }

  /// Add `key` to the set of keys for the current filter interval.
  pub(crate) fn add_key(&mut self, key: &[u8]) {
    self.key_starts.push(self.keys.len());
    self.keys.extend_from_slice(key);
  }

  /// Finalise the filter block and return its bytes.
  ///
  /// Layout:
  /// ```text
  /// [filter_0 data] … [filter_N data]
  /// [u32 LE start_0] … [u32 LE start_N]   ← offset array (N+1 entries)
  /// [u32 LE array_offset]                  ← start of offset array
  /// [u8  FILTER_BASE_LG]
  /// ```
  ///
  /// `array_offset` also serves as the implicit "one-past-the-end" offset for
  /// the last filter's data, so `num_filters = (total - 5 - array_offset) / 4`.
  pub(crate) fn finish(mut self) -> Vec<u8> {
    // Flush any keys that haven't been built into a filter yet.
    if !self.key_starts.is_empty() {
      self.generate_filter();
    }

    let array_offset = self.result.len() as u32;

    // Append per-filter start offsets.
    for &off in &self.filter_offsets {
      self.result.extend_from_slice(&off.to_le_bytes());
    }
    // Append the array start offset and the base_lg encoding parameter.
    self.result.extend_from_slice(&array_offset.to_le_bytes());
    self.result.push(FILTER_BASE_LG);

    self.result
  }

  fn generate_filter(&mut self) {
    // Record where this filter starts in `result`.
    self.filter_offsets.push(self.result.len() as u32);

    if self.key_starts.is_empty() {
      // No keys — record an empty (zero-length) filter and return.
      return;
    }

    // Build a slice view over the accumulated keys.
    let num_keys = self.key_starts.len();
    let mut key_slices: Vec<&[u8]> = Vec::with_capacity(num_keys);
    for i in 0..num_keys {
      let start = self.key_starts[i];
      let end = if i + 1 < num_keys {
        self.key_starts[i + 1]
      } else {
        self.keys.len()
      };
      key_slices.push(&self.keys[start..end]);
    }

    let filter_bytes = self.policy.create_filter(&key_slices);
    self.result.extend_from_slice(&filter_bytes);

    // Reset key accumulator for the next interval.
    self.keys.clear();
    self.key_starts.clear();
  }
}

// ── FilterBlockReader ─────────────────────────────────────────────────────────

/// Reads filter data from a pre-parsed filter block.
///
/// See `table/filter_block.h/cc: FilterBlockReader`.
pub(crate) struct FilterBlockReader {
  policy: Arc<dyn FilterPolicy>,
  /// The raw filter block bytes (verbatim from disk).
  data: Vec<u8>,
  /// Byte offset within `data` where the filter offset array begins.
  /// Equivalently, this is the total size of all concatenated filter data.
  array_offset: usize,
  /// Number of filters stored in the block.
  num: usize,
  /// Log₂ of the filter interval (== `FILTER_BASE_LG` for well-formed blocks).
  base_lg: u8,
}

impl FilterBlockReader {
  /// Parse a filter block.
  ///
  /// Returns `None` if `data` is too short or otherwise malformed.
  pub(crate) fn new(policy: Arc<dyn FilterPolicy>, data: Vec<u8>) -> Option<Self> {
    let n = data.len();
    if n < 5 {
      return None; // need at least [u32 array_offset][u8 base_lg]
    }
    let base_lg = data[n - 1];
    let array_offset = u32::from_le_bytes(data[n - 5..n - 1].try_into().ok()?) as usize;
    if array_offset > n - 5 {
      return None; // array_offset points past valid data
    }
    let num = (n - 5 - array_offset) / 4;
    Some(FilterBlockReader {
      policy,
      data,
      array_offset,
      num,
      base_lg,
    })
  }

  /// Return `true` if `key` might be present in the data block at `block_offset`.
  ///
  /// Returns `true` conservatively when the filter index is out of range or the
  /// filter block is malformed, to avoid false negatives.
  pub(crate) fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool {
    let index = (block_offset >> self.base_lg) as usize;
    if index >= self.num {
      return true; // out-of-range — treat as potential match
    }

    let off = self.array_offset + index * 4;
    // Decode the start and "limit" (one-past-end) offsets for this filter.
    let start = u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap()) as usize;
    // The next u32 is either the next filter's start, or `array_offset` for
    // the very last filter — both give the exclusive end of this filter.
    let limit = u32::from_le_bytes(self.data[off + 4..off + 8].try_into().unwrap()) as usize;

    if start == limit {
      return false; // explicitly empty filter — no keys were added
    }
    if start > limit || limit > self.array_offset {
      return true; // malformed — be conservative
    }
    self.policy.key_may_match(key, &self.data[start..limit])
  }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;
  use crate::filter::BloomFilterPolicy;

  fn bloom() -> Arc<dyn FilterPolicy> {
    Arc::new(BloomFilterPolicy::new(10))
  }

  #[test]
  fn round_trip_single_block() {
    let policy = bloom();
    let mut w = FilterBlockWriter::new(Arc::clone(&policy));
    w.start_block(0);
    w.add_key(b"hello");
    w.add_key(b"world");
    let data = w.finish();

    let r = FilterBlockReader::new(Arc::clone(&policy), data).expect("parse");
    assert!(r.key_may_match(0, b"hello"));
    assert!(r.key_may_match(0, b"world"));
    // Keys not in the filter should return false with high probability.
    // (Occasionally a false positive can occur — we just smoke-test here.)
    // Absent key *might* match but very rarely with 10 bits/key.
  }

  #[test]
  fn keys_in_correct_interval() {
    let policy = bloom();
    let mut w = FilterBlockWriter::new(Arc::clone(&policy));

    // Block 0: offset 0.  Add "aaa".
    w.start_block(0);
    w.add_key(b"aaa");

    // Block 1: offset FILTER_BASE (2048).  Add "bbb".
    w.start_block(FILTER_BASE);
    w.add_key(b"bbb");

    let data = w.finish();
    let r = FilterBlockReader::new(Arc::clone(&policy), data).expect("parse");

    // "aaa" is in filter 0 (block offset 0), "bbb" is in filter 1 (offset 2048).
    assert!(r.key_may_match(0, b"aaa"));
    assert!(r.key_may_match(FILTER_BASE, b"bbb"));

    // "aaa" should NOT be in filter 1 (different interval).
    assert!(!r.key_may_match(FILTER_BASE, b"aaa"));
  }

  #[test]
  fn empty_writer_parses() {
    let policy = bloom();
    let w = FilterBlockWriter::new(Arc::clone(&policy));
    let data = w.finish();
    // No filters — should parse cleanly and return true for all (safe default).
    let r = FilterBlockReader::new(Arc::clone(&policy), data).expect("parse");
    // num == 0, so key_may_match always returns true.
    assert!(r.key_may_match(0, b"anything"));
  }

  #[test]
  fn gap_between_blocks_produces_empty_filter() {
    let policy = bloom();
    let mut w = FilterBlockWriter::new(Arc::clone(&policy));
    w.start_block(0);
    w.add_key(b"k");
    // Jump 3 intervals ahead — two empty filters should be generated.
    w.start_block(3 * FILTER_BASE);
    w.add_key(b"z");
    let data = w.finish();

    let r = FilterBlockReader::new(Arc::clone(&policy), data).expect("parse");
    // Interval 0 has "k".
    assert!(r.key_may_match(0, b"k"));
    // Intervals 1 and 2 are empty — should return false.
    assert!(!r.key_may_match(FILTER_BASE, b"k"));
    assert!(!r.key_may_match(2 * FILTER_BASE, b"k"));
    // Interval 3 has "z".
    assert!(r.key_may_match(3 * FILTER_BASE, b"z"));
  }
}
