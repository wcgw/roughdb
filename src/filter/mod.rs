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

/// A policy for constructing and querying a key filter (e.g. Bloom filter).
///
/// Implemented by [`BloomFilterPolicy`].  Pass an instance via
/// [`Options::filter_policy`](crate::Options::filter_policy) to have SSTables
/// write filter blocks that short-circuit `Table::get` on definite-miss lookups.
///
/// See `include/leveldb/filter_policy.h`.
pub trait FilterPolicy: Send + Sync {
  /// Short name identifying this filter policy.
  ///
  /// Stored as the metaindex key `"filter.<name>"` in every SSTable written
  /// with this policy.  If the name changes, existing SSTables become
  /// unreadable through the new policy.  The built-in Bloom filter uses
  /// `"leveldb.BuiltinBloomFilter2"`.
  fn name(&self) -> &str;

  /// Build a filter from a set of raw key slices.
  ///
  /// The returned bytes are stored verbatim in the SSTable filter block.
  /// Called once per filter interval (every ~2 KiB of data block offsets).
  fn create_filter(&self, keys: &[&[u8]]) -> Vec<u8>;

  /// Return `true` if `key` **might** be in the set that created `filter`.
  ///
  /// A return value of `true` is inconclusive (may be a false positive).
  /// A return value of `false` guarantees the key is absent.
  fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

pub mod bloom;
pub use bloom::BloomFilterPolicy;
