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

use crate::comparator::Comparator;
use crate::db::table_cache::TableCache;
use crate::db::version_edit::FileMetaData;
use crate::error::Error;
use crate::table::format::{parse_internal_key, user_key};
use crate::table::reader::LookupResult;
use std::sync::Arc;

/// Statistics returned by [`Version::get`] for seek-based compaction tracking.
///
/// `seek_file` is set to the first file that was probed (opened) for this
/// lookup when more than one file had to be consulted.  The rationale: if we
/// only needed one file, no seek was "wasted".  If we needed two or more, the
/// first file is charged — it forced an unnecessary I/O probe.
///
/// Port of LevelDB `Version::GetStats`.
pub(crate) struct GetStats {
  /// The first file charged with a wasted seek, if any.
  pub seek_file: Option<Arc<FileMetaData>>,
  /// Level of `seek_file`.
  pub seek_file_level: usize,
}

/// Number of levels in the LSM tree.
pub(crate) const NUM_LEVELS: usize = 7;

/// L0→L1 compaction is triggered when L0 reaches this many files.
///
/// Lives here (rather than in `lib.rs`) so `finalize` and the `Version` query
/// methods can reference it without a back-dependency on the crate root.
pub(crate) const L0_COMPACTION_TRIGGER: usize = 4;

/// A snapshot of the LSM structure: per-level lists of live SSTable files.
///
/// L0 files are stored newest-first (highest file number first).
/// L1–L6 files are stored sorted by smallest key (for binary search in Phase 6).
///
/// `Version` is reference-counted; it lives until all iterators opened on it
/// are dropped.  Open file handles are managed by the [`TableCache`], not here.
#[derive(Clone)]
pub(crate) struct Version {
  /// Per-level live SSTable files.  Private: all overlap/range queries go
  /// through the methods below, and construction goes through `new`/
  /// `from_files`, so callers cannot depend on the raw representation.
  files: [Vec<Arc<FileMetaData>>; NUM_LEVELS],
  /// User-key comparator.  A `Version` is inherently ordered by a comparator,
  /// so it owns one — overlap/range queries need not take it as a parameter.
  comparator: Arc<dyn Comparator>,
  /// Best compaction score across all levels.  -1.0 before `finalize` is called.
  /// Score ≥ 1.0 means this level should be compacted.
  pub compaction_score: f64,
  /// Level with the highest compaction score.  -1 before `finalize` is called.
  pub compaction_level: i32,
}

impl Version {
  /// Create an empty `Version` (no files at any level).
  pub(crate) fn new(comparator: Arc<dyn Comparator>) -> Self {
    Self::from_files(std::array::from_fn(|_| Vec::new()), comparator)
  }

  /// Create a `Version` from pre-assembled per-level file lists.  Compaction
  /// scores are left unset (`finalize` computes them).
  pub(crate) fn from_files(
    files: [Vec<Arc<FileMetaData>>; NUM_LEVELS],
    comparator: Arc<dyn Comparator>,
  ) -> Self {
    Version {
      files,
      comparator,
      compaction_score: -1.0,
      compaction_level: -1,
    }
  }

  /// Read-only view of the files at `level`.
  ///
  /// L0 is newest-first; L1–L6 are sorted by smallest user key.
  pub(crate) fn files_at(&self, level: usize) -> &[Arc<FileMetaData>] {
    &self.files[level]
  }

  /// Append a file to `level` without re-sorting.  Test-only construction helper.
  #[cfg(test)]
  pub(crate) fn push_file_for_test(&mut self, level: usize, meta: Arc<FileMetaData>) {
    self.files[level].push(meta);
  }

  /// Look up `user_key` across all levels, returning the first match and seek
  /// statistics for compaction accounting.
  ///
  /// L0 is scanned newest-first.  L1–L6 are scanned linearly (binary search
  /// deferred to a later phase).
  ///
  /// Tables are opened on demand via `tc`; `verify_checksums` is forwarded to
  /// every `Table::get` call.
  ///
  /// The returned [`GetStats`] blames the first file consulted when the lookup
  /// required probing more than one file (matching LevelDB `Version::Get`).
  pub(crate) fn get(
    &self,
    user_key: &[u8],
    sequence: u64,
    verify_checksums: bool,
    fill_cache: bool,
    tc: &TableCache,
  ) -> Result<(LookupResult, GetStats), Error> {
    let mut stats = GetStats {
      seek_file: None,
      seek_file_level: 0,
    };
    let mut last_file_read: Option<Arc<FileMetaData>> = None;
    let mut last_file_read_level: usize = 0;

    // Helper: record blame on the previous file before moving to a new one.
    macro_rules! charge_prev {
      ($meta:expr, $level:expr) => {
        if last_file_read.is_some() && stats.seek_file.is_none() {
          stats.seek_file = last_file_read.clone();
          stats.seek_file_level = last_file_read_level;
        }
        last_file_read = Some(Arc::clone($meta));
        last_file_read_level = $level;
      };
    }

    // L0: newest-first scan.
    for meta in &self.files[0] {
      charge_prev!(meta, 0);
      let table = tc.get_or_open(meta.number, meta.file_size)?;
      match table.get(user_key, sequence, verify_checksums, fill_cache)? {
        LookupResult::Value(v) => return Ok((LookupResult::Value(v), stats)),
        LookupResult::Deleted => return Ok((LookupResult::Deleted, stats)),
        LookupResult::NotInTable => {}
      }
    }

    // L1–L6: linear scan (binary search deferred to a later phase).
    for level in 1..NUM_LEVELS {
      for meta in &self.files[level] {
        charge_prev!(meta, level);
        let table = tc.get_or_open(meta.number, meta.file_size)?;
        match table.get(user_key, sequence, verify_checksums, fill_cache)? {
          LookupResult::Value(v) => return Ok((LookupResult::Value(v), stats)),
          LookupResult::Deleted => return Ok((LookupResult::Deleted, stats)),
          LookupResult::NotInTable => {}
        }
      }
    }

    Ok((LookupResult::NotInTable, stats))
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
    let cmp = &*tc.comparator();
    let mut result = 0u64;
    for level in 0..NUM_LEVELS {
      for meta in &self.files[level] {
        if cmp_internal_keys(&meta.largest, ikey, cmp) <= std::cmp::Ordering::Equal {
          // Entire file is before ikey.
          result += meta.file_size;
        } else if cmp_internal_keys(&meta.smallest, ikey, cmp) > std::cmp::Ordering::Equal {
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

  /// Files at `level` whose user-key range overlaps `[begin_uk, end_uk]`.
  ///
  /// For L0, the result may expand the query range (L0 files can overlap each
  /// other), so the scan repeats until stable.  For L1+, files are
  /// non-overlapping and sorted, so a single linear pass suffices.
  ///
  /// `begin_uk` and `end_uk` are bare user keys (no internal-key suffix).
  /// Port of LevelDB's `Version::GetOverlappingInputs`.
  pub(crate) fn overlapping_inputs(
    &self,
    level: usize,
    begin_uk: &[u8],
    end_uk: &[u8],
  ) -> Vec<Arc<FileMetaData>> {
    let cmp = &*self.comparator;
    let mut result: Vec<Arc<FileMetaData>> = Vec::new();
    let mut lo = begin_uk.to_vec();
    let mut hi = end_uk.to_vec();

    loop {
      result.clear();
      let prev_lo = lo.clone();
      let prev_hi = hi.clone();
      for f in &self.files[level] {
        let f_lo = user_key(&f.smallest);
        let f_hi = user_key(&f.largest);
        if cmp.compare(f_hi, lo.as_slice()).is_lt() || cmp.compare(f_lo, hi.as_slice()).is_gt() {
          continue; // no overlap
        }
        result.push(Arc::clone(f));
        if level == 0 {
          // Expand range to cover this file, then rescan.
          if cmp.compare(f_lo, lo.as_slice()).is_lt() {
            lo = f_lo.to_vec();
          }
          if cmp.compare(f_hi, hi.as_slice()).is_gt() {
            hi = f_hi.to_vec();
          }
        }
      }
      if level > 0 || (lo == prev_lo && hi == prev_hi) {
        break;
      }
    }
    result
  }

  /// True if any file at `level` has a user-key range overlapping
  /// `[smallest_uk, largest_uk]`.  Port of LevelDB's `Version::OverlapInLevel`.
  pub(crate) fn overlaps_level(&self, level: usize, smallest_uk: &[u8], largest_uk: &[u8]) -> bool {
    let cmp = &*self.comparator;
    for f in &self.files[level] {
      let f_lo = user_key(&f.smallest);
      let f_hi = user_key(&f.largest);
      if cmp.compare(f_hi, smallest_uk).is_ge() && cmp.compare(f_lo, largest_uk).is_le() {
        return true;
      }
      // L1+: files are sorted by smallest key; once f_lo > largest_uk no later
      // file can overlap.
      if level > 0 && cmp.compare(f_lo, largest_uk).is_gt() {
        break;
      }
    }
    false
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

// ── Compaction score helpers ──────────────────────────────────────────────────

/// Maximum total bytes allowed at a given level (L1+).
///
/// L1 = 10 MiB, L2 = 100 MiB, L3 = 1 GiB, …  (×10 per level).
/// L0 uses a file-count threshold, not byte size.
pub(crate) fn max_bytes_for_level(level: usize) -> f64 {
  let mut result = 10.0 * 1_048_576.0_f64; // 10 MiB
  let mut l = level;
  while l > 1 {
    result *= 10.0;
    l -= 1;
  }
  result
}

/// Compute and store the highest compaction score across all levels in `version`.
///
/// Must be called on an owned `Version` before it is wrapped in `Arc`.  Uses
/// [`L0_COMPACTION_TRIGGER`] as the L0 file-count threshold.
pub(crate) fn finalize(version: &mut Version) {
  let mut best_level = -1i32;
  let mut best_score = -1.0f64;

  for level in 0..(NUM_LEVELS - 1) {
    let score = if level == 0 {
      version.files[0].len() as f64 / L0_COMPACTION_TRIGGER as f64
    } else {
      let bytes: u64 = version.files[level].iter().map(|f| f.file_size).sum();
      bytes as f64 / max_bytes_for_level(level)
    };
    if score > best_score {
      best_score = score;
      best_level = level as i32;
    }
  }

  version.compaction_score = best_score;
  version.compaction_level = best_level;
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::comparator::BytewiseComparator;
  use crate::table::format::make_internal_key;

  /// Build a file covering user-key range `[lo, hi]`.  These overlap/range
  /// queries can now be tested against a hand-built `Version` — no `Db`, no
  /// disk, no background threads.
  fn meta(number: u64, lo: &[u8], hi: &[u8]) -> Arc<FileMetaData> {
    FileMetaData::new(
      number,
      1000,
      make_internal_key(lo, 1, 1),
      make_internal_key(hi, 1, 1),
    )
  }

  fn version_l1(ranges: &[(u64, &[u8], &[u8])]) -> Version {
    let mut v = Version::new(Arc::new(BytewiseComparator));
    for &(n, lo, hi) in ranges {
      v.push_file_for_test(1, meta(n, lo, hi));
    }
    v
  }

  #[test]
  fn overlaps_level_l1() {
    let v = version_l1(&[(1, b"a", b"c"), (2, b"m", b"p")]);
    assert!(v.overlaps_level(1, b"b", b"b")); // inside [a,c]
    assert!(v.overlaps_level(1, b"c", b"n")); // spans the gap into [m,p]
    assert!(!v.overlaps_level(1, b"d", b"l")); // strictly in the gap
    assert!(!v.overlaps_level(1, b"q", b"z")); // past every file
  }

  #[test]
  fn overlapping_inputs_l1_selects_touched_files() {
    let v = version_l1(&[(1, b"a", b"c"), (2, b"m", b"p"), (3, b"x", b"z")]);
    let nums: Vec<u64> = v
      .overlapping_inputs(1, b"b", b"n")
      .iter()
      .map(|f| f.number)
      .collect();
    assert_eq!(nums, vec![1, 2]); // touches [a,c] and [m,p], not [x,z]
  }

  #[test]
  fn overlapping_inputs_l0_expands_transitively() {
    // L0 files can overlap each other, so a query that touches one pulls in
    // its transitively-overlapping neighbours.
    let mut v = Version::new(Arc::new(BytewiseComparator));
    v.push_file_for_test(0, meta(1, b"a", b"e"));
    v.push_file_for_test(0, meta(2, b"d", b"h")); // overlaps file 1 (d..e)
    v.push_file_for_test(0, meta(3, b"t", b"z")); // disjoint
    let mut nums: Vec<u64> = v
      .overlapping_inputs(0, b"a", b"b")
      .iter()
      .map(|f| f.number)
      .collect();
    nums.sort();
    assert_eq!(nums, vec![1, 2]); // expanded to include file 2, excluded file 3
  }

  #[test]
  fn overlapping_inputs_respects_custom_comparator() {
    // Under a reverse comparator, "ranges" run the other way; a Version built
    // with it must answer overlap queries in that order, not byte order.
    struct Reverse;
    impl Comparator for Reverse {
      fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
      }
      fn name(&self) -> &str {
        "test.Reverse"
      }
      fn find_shortest_separator(&self, _: &mut Vec<u8>, _: &[u8]) {}
      fn find_short_successor(&self, _: &mut Vec<u8>) {}
    }
    // With the reverse comparator, a file "covers" [hi, lo] in reverse order.
    // File smallest=z largest=a means the descending span from z down to a.
    let mut v = Version::new(Arc::new(Reverse));
    v.push_file_for_test(1, meta(1, b"z", b"a"));
    // Query span from m down to b overlaps the file under reverse ordering.
    assert!(v.overlaps_level(1, b"m", b"b"));
  }
}
