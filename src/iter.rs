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

use crate::error::Error;

/// Common interface for all internal iterators: block, SSTable, memtable, merged.
///
/// All iterators start invalid; callers must invoke `seek_to_first()` or `seek()`
/// before reading `key()` / `value()`.  Keys are internal keys
/// (`user_key || 8-byte LE tag`); values are raw value bytes.
pub(crate) trait InternalIterator {
  fn valid(&self) -> bool;
  fn seek_to_first(&mut self);
  /// Position at the last entry.
  fn seek_to_last(&mut self);
  /// Position at the first entry whose key ≥ `target`.
  fn seek(&mut self, target: &[u8]);
  fn next(&mut self);
  /// Move to the previous entry.  Only call when `valid()` is true.
  fn prev(&mut self);
  /// Current internal key.  Only valid when `valid()` is true.
  fn key(&self) -> &[u8];
  /// Current value.  Only valid when `valid()` is true.
  fn value(&self) -> &[u8];
  /// Sticky I/O or corruption error encountered during iteration, if any.
  fn status(&self) -> Option<&Error>;
}
