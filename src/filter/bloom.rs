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

// ── Hash ─────────────────────────────────────────────────────────────────────

/// LevelDB's internal hash function (from `util/hash.cc`).
fn leveldb_hash(data: &[u8], seed: u32) -> u32 {
  const M: u32 = 0xc6a4a793;
  const R: u32 = 24;
  let n = data.len();
  let mut h = seed ^ ((n as u32).wrapping_mul(M));

  let mut i = 0;
  while i + 4 <= n {
    let w = u32::from_le_bytes(data[i..i + 4].try_into().unwrap());
    h = h.wrapping_add(w);
    h = h.wrapping_mul(M);
    h ^= h >> 16;
    i += 4;
  }

  // Remaining 1–3 bytes (fall-through matches C switch).
  match n - i {
    3 => {
      h = h.wrapping_add((data[i + 2] as u32) << 16);
      h = h.wrapping_add((data[i + 1] as u32) << 8);
      h = h.wrapping_add(data[i] as u32);
      h = h.wrapping_mul(M);
      h ^= h >> R;
    }
    2 => {
      h = h.wrapping_add((data[i + 1] as u32) << 8);
      h = h.wrapping_add(data[i] as u32);
      h = h.wrapping_mul(M);
      h ^= h >> R;
    }
    1 => {
      h = h.wrapping_add(data[i] as u32);
      h = h.wrapping_mul(M);
      h ^= h >> R;
    }
    _ => {}
  }
  h
}

/// Hash function used by `BloomFilterPolicy` (from `util/bloom.cc: BloomHash`).
fn bloom_hash(key: &[u8]) -> u32 {
  leveldb_hash(key, 0xbc9f1d34)
}

// ── BloomFilterPolicy ─────────────────────────────────────────────────────────

/// LevelDB-compatible Bloom filter policy (Kirsch-Mitzenmacher double hashing).
///
/// `bits_per_key` trades memory for false-positive rate:
///
/// | bits/key | false-positive rate |
/// |----------|---------------------|
/// | 10       | ~1 %                |
/// | 14       | ~0.1 %              |
/// | 20       | ~0.01 %             |
///
/// The default in LevelDB itself is 10 bits/key.
///
/// # Examples
///
/// ```
/// use roughdb::{Options, BloomFilterPolicy};
/// use std::sync::Arc;
///
/// let mut opts = Options::default();
/// opts.filter_policy = Some(Arc::new(BloomFilterPolicy::new(10)));
/// ```
///
/// See `util/bloom.cc` and `include/leveldb/filter_policy.h`.
pub struct BloomFilterPolicy {
  bits_per_key: usize,
  /// Number of hash functions: `round(bits_per_key * ln(2))`, clamped to `[1, 30]`.
  k: usize,
}

impl BloomFilterPolicy {
  /// Create a Bloom filter policy with `bits_per_key` bits allocated per key.
  ///
  /// Typical value: `10` (≈ 1 % false-positive rate).
  pub fn new(bits_per_key: usize) -> Self {
    // k = ln(2) * bits_per_key ≈ 0.69 * bits_per_key, rounded, clamped to [1, 30].
    let k = ((bits_per_key as f64 * 0.69) as usize).clamp(1, 30);
    BloomFilterPolicy { bits_per_key, k }
  }
}

impl FilterPolicy for BloomFilterPolicy {
  fn name(&self) -> &str {
    "leveldb.BuiltinBloomFilter2"
  }

  /// Create a Bloom filter for `keys`.
  ///
  /// The returned bytes have a minimum length of 2 (a 1-byte all-zeros filter
  /// plus the probe-count trailer), even for an empty key set.  This avoids a
  /// special case in `key_may_match` and matches LevelDB's behaviour.
  fn create_filter(&self, keys: &[&[u8]]) -> Vec<u8> {
    // Compute filter size (minimum 64 bits to avoid degenerate filters).
    let bits = (keys.len() * self.bits_per_key).max(64);
    let bytes = bits.div_ceil(8);
    let bits = bytes * 8; // round up to a full byte boundary

    // Allocate: `bytes` zero bytes for the bit array + 1 byte for k.
    let mut filter = vec![0u8; bytes + 1];
    filter[bytes] = self.k as u8; // record probe count in the final byte

    for &key in keys {
      let mut h = bloom_hash(key);
      // Kirsch-Mitzenmacher: rotate right 17 bits gives a near-independent h2.
      let delta = h.rotate_left(15);
      for _ in 0..self.k {
        let bitpos = (h % bits as u32) as usize;
        filter[bitpos / 8] |= 1 << (bitpos % 8);
        h = h.wrapping_add(delta);
      }
    }

    filter
  }

  fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
    let len = filter.len();
    if len < 2 {
      return false;
    }
    let bits = (len - 1) * 8;
    let k = filter[len - 1] as usize;
    if k > 30 {
      // New or unknown encoding format — be conservative.
      return true;
    }

    let mut h = bloom_hash(key);
    let delta = h.rotate_left(15);
    for _ in 0..k {
      let bitpos = (h % bits as u32) as usize;
      if (filter[bitpos / 8] & (1 << (bitpos % 8))) == 0 {
        return false;
      }
      h = h.wrapping_add(delta);
    }
    true
  }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;

  fn policy() -> BloomFilterPolicy {
    BloomFilterPolicy::new(10)
  }

  #[test]
  fn empty_filter_no_match() {
    let p = policy();
    let filter = p.create_filter(&[]);
    // An empty-key filter should not match arbitrary keys.
    assert!(!p.key_may_match(b"hello", &filter));
  }

  #[test]
  fn inserted_keys_match() {
    let p = policy();
    let keys: Vec<Vec<u8>> = (0u32..100).map(|i| i.to_le_bytes().to_vec()).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let filter = p.create_filter(&key_refs);
    for k in &key_refs {
      assert!(p.key_may_match(k, &filter), "key {k:?} not found in filter");
    }
  }

  #[test]
  fn false_positive_rate_reasonable() {
    // Generate a filter over 10_000 keys and probe 10_000 absent keys.
    // With 10 bits/key the expected FPR is ~1 %; we allow up to 2 % here.
    let p = policy();
    let keys: Vec<Vec<u8>> = (0u32..10_000).map(|i| i.to_le_bytes().to_vec()).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let filter = p.create_filter(&key_refs);

    let mut false_positives = 0u32;
    for i in 10_000u32..20_000 {
      let probe = i.to_le_bytes();
      if p.key_may_match(&probe, &filter) {
        false_positives += 1;
      }
    }
    let fpr = false_positives as f64 / 10_000.0;
    assert!(fpr < 0.02, "false positive rate {fpr:.3} exceeds 2 %");
  }

  #[test]
  fn short_filter_returns_false() {
    let p = policy();
    // A filter shorter than 2 bytes should never match.
    assert!(!p.key_may_match(b"key", &[]));
    assert!(!p.key_may_match(b"key", &[0u8]));
  }

  #[test]
  fn name_matches_leveldb() {
    assert_eq!(policy().name(), "leveldb.BuiltinBloomFilter2");
  }
}
