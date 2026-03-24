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

//! User-supplied compaction filters for dropping or transforming entries during
//! compaction.
//!
//! A [`CompactionFilter`] is invoked for the newest visible version of every
//! key during compaction.  It can keep, remove, or replace the value — enabling
//! TTL expiry, value transformations, and selective deletion without a separate
//! scan-and-delete pass.
//!
//! Filters are created per compaction via a [`CompactionFilterFactory`] stored
//! in [`Options`](crate::Options).
//!
//! See `include/rocksdb/compaction_filter.h`.

/// Decision returned by a [`CompactionFilter`] for each key.
#[derive(Debug)]
pub enum CompactionDecision {
  /// Keep the key-value pair unchanged.
  Keep,
  /// Remove the key-value pair entirely.
  Remove,
  /// Replace the value with a new one (key and sequence number are preserved).
  ChangeValue(Vec<u8>),
}

/// User-supplied filter invoked for every key during compaction.
///
/// Only the **newest visible version** of each key is presented — older shadow
/// versions are already pruned before the filter runs.  Entries still visible
/// to a live snapshot are never filtered.
///
/// Tombstones (`value_type == 0`) are presented so the filter can suppress
/// elision (return [`Keep`](CompactionDecision::Keep)) if needed.
///
/// # Thread safety
///
/// A filter instance is owned by a single compaction and is never shared.
/// The factory that creates it must be `Send + Sync` (shared via `Arc` in
/// `Options`), but the filter itself only needs `Send`.
///
/// See `include/rocksdb/compaction_filter.h: CompactionFilter`.
pub trait CompactionFilter: Send {
  /// Decide whether to keep, remove, or modify this entry.
  ///
  /// - `level`: the output level of this compaction.
  /// - `key`: the user key (no internal sequence/type suffix).
  /// - `value`: the current value bytes (empty for tombstones).
  /// - `value_type`: `1` = Value, `0` = Deletion tombstone.
  fn filter(
    &mut self,
    level: usize,
    key: &[u8],
    value: &[u8],
    value_type: u8,
  ) -> CompactionDecision;

  /// Human-readable name for logging.
  fn name(&self) -> &str;
}

/// Creates a fresh [`CompactionFilter`] for each compaction run.
///
/// Using a factory rather than a bare filter allows the filter to hold
/// per-compaction mutable state (e.g. a snapshot of the current time for TTL
/// checks) without shared mutable state between concurrent compactions.
///
/// See `include/rocksdb/compaction_filter.h: CompactionFilterFactory`.
pub trait CompactionFilterFactory: Send + Sync {
  /// Create a new filter for one compaction run.
  fn create_compaction_filter(&self) -> Box<dyn CompactionFilter>;

  /// Human-readable name for logging.
  fn name(&self) -> &str;
}
