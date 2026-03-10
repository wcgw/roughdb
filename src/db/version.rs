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

use crate::db::version_edit::FileMetaData;
use crate::error::Error;
use crate::table::reader::LookupResult;
use std::sync::Arc;

/// Number of levels in the LSM tree.
pub(crate) const NUM_LEVELS: usize = 7;

/// A snapshot of the LSM structure: per-level lists of live SSTable files.
///
/// L0 files are stored newest-first (highest file number first).
/// L1–L6 files are stored sorted by smallest key (for binary search in Phase 6).
///
/// `Version` is reference-counted; it lives until all iterators opened on it
/// are dropped.
#[derive(Clone)]
pub(crate) struct Version {
  pub files: [Vec<Arc<FileMetaData>>; NUM_LEVELS],
}

impl Version {
  pub(crate) fn new() -> Self {
    Version {
      files: std::array::from_fn(|_| Vec::new()),
    }
  }

  /// Look up `user_key` across all levels, returning the first match.
  ///
  /// L0 is scanned newest-first.  L1–L6 are scanned linearly (binary search
  /// deferred to a later phase).
  ///
  /// `verify_checksums` is forwarded to every `Table::get` call; set it to
  /// `true` when `ReadOptions::verify_checksums` or `Options::paranoid_checks`
  /// is active.
  pub(crate) fn get(
    &self,
    user_key: &[u8],
    _sequence: u64,
    verify_checksums: bool,
  ) -> Result<LookupResult, Error> {
    // L0: newest-first scan.
    for meta in &self.files[0] {
      let table = meta
        .table
        .as_ref()
        .expect("FileMetaData in Version must have an open table");
      match table.get(user_key, verify_checksums)? {
        LookupResult::Value(v) => return Ok(LookupResult::Value(v)),
        LookupResult::Deleted => return Ok(LookupResult::Deleted),
        LookupResult::NotInTable => {}
      }
    }

    // L1–L6: linear scan (binary search deferred to a later phase).
    for level in 1..NUM_LEVELS {
      for meta in &self.files[level] {
        let table = meta
          .table
          .as_ref()
          .expect("FileMetaData in Version must have an open table");
        match table.get(user_key, verify_checksums)? {
          LookupResult::Value(v) => return Ok(LookupResult::Value(v)),
          LookupResult::Deleted => return Ok(LookupResult::Deleted),
          LookupResult::NotInTable => {}
        }
      }
    }

    Ok(LookupResult::NotInTable)
  }
}
