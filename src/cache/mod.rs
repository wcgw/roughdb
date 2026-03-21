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

//! Block cache — an LRU cache of decompressed SSTable data blocks.
//!
//! LevelDB uses a generic sharded LRU cache with custom deleters and opaque
//! handles (`util/cache.cc`).  In Rust, `Arc<Block>` replaces the handle +
//! deleter pattern: dropping the cache's copy is enough; any `BlockIter` that
//! already cloned the `Arc<Vec<u8>>` remains valid independently.
//!
//! The key is `(cache_id, block_offset)`.  Each `Table` gets a unique `cache_id`
//! from [`BlockCache::new_id`] so blocks from different files never collide even
//! when file numbers are reused after compaction.
//!
//! Default capacity: 8 MiB (matching LevelDB's `Options::block_cache` default).

use crate::table::block::Block;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Default block cache capacity in bytes.
pub const DEFAULT_BLOCK_CACHE_CAPACITY: usize = 8 * 1024 * 1024;

struct CacheInner {
  /// Maximum total bytes of block data held in the cache.
  capacity: usize,
  /// Current total bytes charged (sum of `block.data().len()` for each entry).
  usage: usize,
  /// LRU order: front = least-recently used, back = most-recently used.
  order: VecDeque<(u64, u64)>,
  /// Maps `(cache_id, block_offset) → Block`.
  map: HashMap<(u64, u64), Block>,
}

impl CacheInner {
  fn get(&mut self, cache_id: u64, offset: u64) -> Option<Block> {
    let key = (cache_id, offset);
    let block = self.map.get(&key)?.clone();
    // Promote to MRU.
    if let Some(pos) = self.order.iter().position(|&k| k == key) {
      self.order.remove(pos);
    }
    self.order.push_back(key);
    Some(block)
  }

  fn insert(&mut self, cache_id: u64, offset: u64, block: Block) {
    let key = (cache_id, offset);
    let charge = block.data().len();

    // If already present, replace and update usage.
    if let Some(old) = self.map.remove(&key) {
      self.usage = self.usage.saturating_sub(old.data().len());
      if let Some(pos) = self.order.iter().position(|&k| k == key) {
        self.order.remove(pos);
      }
    }

    // Evict LRU entries until we have room (or only one entry remains).
    while self.usage + charge > self.capacity && !self.order.is_empty() {
      if let Some(evict_key) = self.order.pop_front() {
        if let Some(evicted) = self.map.remove(&evict_key) {
          self.usage = self.usage.saturating_sub(evicted.data().len());
        }
      }
    }

    self.order.push_back(key);
    self.map.insert(key, block);
    self.usage += charge;
  }
}

/// An LRU cache of decompressed SSTable data blocks, keyed by `(cache_id, block_offset)`.
///
/// `BlockCache` is cheaply cloneable (`Arc`-backed) and safe to share across threads.
///
/// See `include/leveldb/cache.h` and `util/cache.cc`.
#[derive(Clone)]
pub struct BlockCache {
  next_id: Arc<AtomicU64>,
  inner: Arc<Mutex<CacheInner>>,
}

impl BlockCache {
  /// Create a new block cache with the given byte capacity.
  pub fn new(capacity: usize) -> Self {
    BlockCache {
      next_id: Arc::new(AtomicU64::new(1)),
      inner: Arc::new(Mutex::new(CacheInner {
        capacity,
        usage: 0,
        order: VecDeque::new(),
        map: HashMap::new(),
      })),
    }
  }

  /// Allocate a unique cache ID for a new `Table`.
  ///
  /// Each `Table` calls this once at open time so its blocks never collide with
  /// another table's blocks even when file numbers are reused after compaction.
  pub(crate) fn new_id(&self) -> u64 {
    self.next_id.fetch_add(1, Ordering::Relaxed)
  }

  /// Look up a block by `(cache_id, block_offset)`.
  ///
  /// Returns a clone of the cached `Block` on hit (cheap — `Block` wraps `Arc<Vec<u8>>`).
  pub(crate) fn get(&self, cache_id: u64, offset: u64) -> Option<Block> {
    self.inner.lock().unwrap().get(cache_id, offset)
  }

  /// Insert `block` at `(cache_id, block_offset)`, evicting LRU entries as needed.
  pub(crate) fn insert(&self, cache_id: u64, offset: u64, block: Block) {
    self.inner.lock().unwrap().insert(cache_id, offset, block);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn make_block(size: usize) -> Block {
    // The block content here is not valid for iteration — it's only used
    // for cache charge accounting in tests.  The last 4 bytes are treated as
    // the restart count (0 = zero restarts, which is valid).
    Block::new(
      vec![0u8; size],
      std::sync::Arc::new(crate::comparator::BytewiseComparator),
    )
  }

  #[test]
  fn hit_after_insert() {
    let cache = BlockCache::new(1024);
    let id = cache.new_id();
    cache.insert(id, 0, make_block(100));
    assert!(cache.get(id, 0).is_some());
  }

  #[test]
  fn miss_on_absent_key() {
    let cache = BlockCache::new(1024);
    assert!(cache.get(1, 0).is_none());
  }

  #[test]
  fn evicts_lru_when_over_capacity() {
    // Capacity = 200 bytes; insert two 100-byte blocks, then a third — first must be evicted.
    let cache = BlockCache::new(200);
    let id = cache.new_id();
    cache.insert(id, 0, make_block(100)); // block A
    cache.insert(id, 100, make_block(100)); // block B — now at capacity
    cache.insert(id, 200, make_block(100)); // block C — A must be evicted
    assert!(cache.get(id, 0).is_none(), "A should have been evicted");
    assert!(cache.get(id, 100).is_some(), "B should still be present");
    assert!(cache.get(id, 200).is_some(), "C should be present");
  }

  #[test]
  fn lru_promotes_on_get() {
    // Insert A then B; access A so it becomes MRU; insert C — B should be evicted.
    let cache = BlockCache::new(200);
    let id = cache.new_id();
    cache.insert(id, 0, make_block(100)); // A (LRU)
    cache.insert(id, 100, make_block(100)); // B (MRU); capacity reached
    cache.get(id, 0); // Promote A → now B is LRU
    cache.insert(id, 200, make_block(100)); // C — B should be evicted
    assert!(
      cache.get(id, 0).is_some(),
      "A should still be present (was promoted)"
    );
    assert!(cache.get(id, 100).is_none(), "B should have been evicted");
    assert!(cache.get(id, 200).is_some(), "C should be present");
  }

  #[test]
  fn unique_ids_per_new_id_call() {
    let cache = BlockCache::new(1024);
    let id1 = cache.new_id();
    let id2 = cache.new_id();
    assert_ne!(id1, id2);
  }

  #[test]
  fn clone_shares_state() {
    let cache = BlockCache::new(1024);
    let cache2 = cache.clone();
    let id = cache.new_id();
    cache.insert(id, 0, make_block(64));
    // The clone should see the same entry.
    assert!(cache2.get(id, 0).is_some());
  }
}
