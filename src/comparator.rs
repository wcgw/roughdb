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

/// A comparator defines a total order over user keys.
///
/// The same comparator must be used for all operations on a database — opening a database with a
/// different comparator than the one used to create it is an error.
///
/// Port of LevelDB's `include/leveldb/comparator.h`.
pub trait Comparator: Send + Sync {
  /// Three-way comparison. Returns `Ordering::Less`, `Equal`, or `Greater`.
  fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

  /// The name of this comparator, stored in the MANIFEST so that opening a database with an
  /// incompatible comparator is detected.
  fn name(&self) -> &str;

  /// If `*start < limit`, change `*start` to a short string in `[start, limit)`.
  ///
  /// Used to shorten index-block separators. Simple implementations may leave `start` unchanged.
  fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]);

  /// Change `*key` to a short string `>= *key`.
  ///
  /// Used for the last entry in an index block. Simple implementations may leave `key` unchanged.
  fn find_short_successor(&self, key: &mut Vec<u8>);
}

/// Default comparator that orders keys lexicographically (byte-by-byte).
///
/// This is the comparator used by LevelDB when no custom comparator is specified.
#[derive(Debug, Clone, Copy)]
pub struct BytewiseComparator;

impl Comparator for BytewiseComparator {
  fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.cmp(b)
  }

  fn name(&self) -> &str {
    "leveldb.BytewiseComparator"
  }

  fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
    // Find the length of the common prefix.
    let min_len = start.len().min(limit.len());
    let mut diff_index = 0;
    while diff_index < min_len && start[diff_index] == limit[diff_index] {
      diff_index += 1;
    }
    if diff_index < min_len {
      let diff_byte = start[diff_index];
      // If we can increment the differing byte and it's still less than limit's byte,
      // truncate start to diff_index+1.
      if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
        start[diff_index] += 1;
        start.truncate(diff_index + 1);
        debug_assert!(self.compare(start, limit) == std::cmp::Ordering::Less);
      }
    }
  }

  fn find_short_successor(&self, key: &mut Vec<u8>) {
    // Find first byte that can be incremented.
    for i in 0..key.len() {
      if key[i] != 0xff {
        key[i] += 1;
        key.truncate(i + 1);
        return;
      }
    }
    // key is all 0xff — leave unchanged.
  }
}
