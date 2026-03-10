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

use crate::db::table_cache::TableCache;
use crate::db::version_edit::FileMetaData;
use crate::error::Error;
use crate::table::format::parse_internal_key;
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
/// are dropped.  Open file handles are managed by the [`TableCache`], not here.
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
  /// Tables are opened on demand via `tc`; `verify_checksums` is forwarded to
  /// every `Table::get` call.
  pub(crate) fn get(
    &self,
    user_key: &[u8],
    _sequence: u64,
    verify_checksums: bool,
    tc: &TableCache,
  ) -> Result<LookupResult, Error> {
    // L0: newest-first scan.
    for meta in &self.files[0] {
      let table = tc.get_or_open(meta.number, meta.file_size)?;
      match table.get(user_key, verify_checksums)? {
        LookupResult::Value(v) => return Ok(LookupResult::Value(v)),
        LookupResult::Deleted => return Ok(LookupResult::Deleted),
        LookupResult::NotInTable => {}
      }
    }

    // L1–L6: linear scan (binary search deferred to a later phase).
    for level in 1..NUM_LEVELS {
      for meta in &self.files[level] {
        let table = tc.get_or_open(meta.number, meta.file_size)?;
        match table.get(user_key, verify_checksums)? {
          LookupResult::Value(v) => return Ok(LookupResult::Value(v)),
          LookupResult::Deleted => return Ok(LookupResult::Deleted),
          LookupResult::NotInTable => {}
        }
      }
    }

    Ok(LookupResult::NotInTable)
  }

  /// Estimate the cumulative on-disk byte offset of `ikey` across all levels.
  ///
  /// For each level, files whose `largest` key is entirely before `ikey` have
  /// their full `file_size` added.  For the file that straddles the boundary
  /// (if any), `Table::approximate_offset_of` is used.  Files entirely after
  /// `ikey` are skipped (and for L1+, the scan stops early because files are
  /// sorted by smallest key).
  ///
  /// See `db/version_set.cc: VersionSet::ApproximateOffsetOf`.
  pub(crate) fn approximate_offset_of(&self, ikey: &[u8], tc: &TableCache) -> u64 {
    use crate::table::format::cmp_internal_keys;
    let mut result = 0u64;
    for level in 0..NUM_LEVELS {
      for meta in &self.files[level] {
        if cmp_internal_keys(&meta.largest, ikey) <= std::cmp::Ordering::Equal {
          // Entire file is before ikey.
          result += meta.file_size;
        } else if cmp_internal_keys(&meta.smallest, ikey) > std::cmp::Ordering::Equal {
          // Entire file is after ikey; L1+ files are sorted so no later file
          // in this level can contain ikey.
          if level > 0 {
            break;
          }
        } else {
          // ikey falls within this file's range.
          if let Ok(table) = tc.get_or_open(meta.number, meta.file_size) {
            result += table.approximate_offset_of(ikey);
          }
        }
      }
    }
    result
  }

  /// Number of live SSTable files at `level`.
  pub(crate) fn num_files(&self, level: usize) -> usize {
    self.files[level].len()
  }

  /// Total on-disk size (bytes) of all SSTable files at `level`.
  pub(crate) fn level_bytes(&self, level: usize) -> u64 {
    self.files[level].iter().map(|f| f.file_size).sum()
  }

  /// Human-readable listing of all levels and their files.
  ///
  /// Format (matching LevelDB's `Version::DebugString`):
  /// ```text
  /// --- level 0 ---
  ///  3:4096[key1 .. key2]
  /// --- level 1 ---
  /// ```
  /// Internal key bytes are rendered as escaped ASCII (printable bytes shown
  /// literally; non-printable bytes shown as `\xHH`).  The sequence/type
  /// suffix is stripped so only the user key is shown.
  pub(crate) fn debug_string(&self) -> String {
    let mut out = String::new();
    for level in 0..NUM_LEVELS {
      out.push_str(&format!("--- level {level} ---\n"));
      for f in &self.files[level] {
        let smallest = format_user_key(&f.smallest);
        let largest = format_user_key(&f.largest);
        out.push_str(&format!(
          " {}:{}[{} .. {}]\n",
          f.number, f.file_size, smallest, largest
        ));
      }
    }
    out
  }
}

/// Render an internal key as an escaped user-key string.
fn format_user_key(ikey: &[u8]) -> String {
  let user_key = parse_internal_key(ikey)
    .map(|(uk, _, _)| uk)
    .unwrap_or(ikey);
  let mut s = String::new();
  for &b in user_key {
    if b.is_ascii_graphic() || b == b' ' {
      s.push(b as char);
    } else {
      s.push_str(&format!("\\x{b:02x}"));
    }
  }
  s
}
