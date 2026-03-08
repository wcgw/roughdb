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
use crate::iter::InternalIterator;
use crate::table::format::{make_internal_key, parse_internal_key};

/// User-facing forward iterator over a merged view of the database.
///
/// Wraps an [`InternalIterator`] (typically a [`MergingIterator`]) and applies:
///
/// - **Snapshot filtering**: entries with `seq > sequence` are invisible.
/// - **Tombstone handling**: a deletion marker hides all older versions of
///   that key, including keys written earlier within the same sequence range.
/// - **Version merging**: only the newest visible version of each user key is
///   returned; older versions are silently skipped.
///
/// In LevelDB's design the iterator is always in one of two directions.  We
/// implement forward-only for now (`Prev` / `SeekToLast` are deferred to
/// post-parity backward iteration).
///
/// When `valid()` is true the internal iterator is positioned at the exact
/// entry yielded by `key()` / `value()`, so those methods borrow directly from
/// the inner iterator without copying.
///
/// See `db/db_iter.h/cc` in LevelDB.
///
/// [`MergingIterator`]: crate::db::merge_iter::MergingIterator
pub(crate) struct DbIterator {
  /// Underlying merged internal iterator.
  iter: Box<dyn InternalIterator>,
  /// Snapshot sequence number: only entries with `seq <= sequence` are visible.
  sequence: u64,
  /// Sticky error from a corrupt internal key.
  status: Option<Error>,
  /// Whether the iterator is positioned at a valid user entry.
  valid: bool,
}

impl DbIterator {
  pub(crate) fn new(iter: Box<dyn InternalIterator>, sequence: u64) -> Self {
    DbIterator {
      iter,
      sequence,
      status: None,
      valid: false,
    }
  }

  // ── Core seek helpers ───────────────────────────────────────────────────

  /// Advance through the internal iterator until we land on an acceptable
  /// user-visible entry, or exhaust the iterator.
  ///
  /// Starting from the current internal position (which must be valid):
  ///
  /// - Entries with `seq > snapshot` are silently skipped.
  /// - A deletion marker for key K causes all subsequent entries with
  ///   `user_key ≤ K` to be skipped (`skipping = true`, `skip = K`).
  /// - The first `Value` entry with `user_key > skip` (or any entry when
  ///   `skipping = false`) sets `valid = true` and returns.
  ///
  /// On exhaustion `valid` is set to `false`.
  ///
  /// `skip` is a mutable caller-owned buffer.  When `skipping` is `false` on
  /// entry it is used as scratch storage (matching LevelDB's idiom where
  /// `saved_key_` is passed as temporary storage for `FindNextUserEntry`).
  fn find_next_user_entry(&mut self, mut skipping: bool, skip: &mut Vec<u8>) {
    loop {
      if !self.iter.valid() {
        self.valid = false;
        return;
      }

      let ikey = self.iter.key();
      match parse_internal_key(ikey) {
        Some((user_key, seq, vtype)) if seq <= self.sequence => match vtype {
          0 => {
            // Deletion: arrange to skip all upcoming entries for this key.
            skip.clear();
            skip.extend_from_slice(user_key);
            skipping = true;
          }
          1 => {
            // Value: accept unless it is hidden by a prior tombstone.
            if !skipping || user_key > skip.as_slice() {
              self.valid = true;
              return;
            }
          }
          _ => {
            // Unknown value type — treat as corruption and record it.
            self.status = Some(Error::Corruption(format!(
              "unknown value type {} in internal key",
              vtype
            )));
          }
        },
        None => {
          // Corrupt internal key — record and skip.
          self.status = Some(Error::Corruption(
            "corrupted internal key in DbIterator".to_string(),
          ));
        }
        _ => {} // seq > snapshot: invisible
      }

      self.iter.next();
    }
  }

  // ── Public iterator interface ───────────────────────────────────────────

  pub(crate) fn valid(&self) -> bool {
    self.valid
  }

  /// Position at the first user-visible entry.
  pub(crate) fn seek_to_first(&mut self) {
    self.iter.seek_to_first();
    if self.iter.valid() {
      let mut skip = Vec::new();
      self.find_next_user_entry(false, &mut skip);
    } else {
      self.valid = false;
    }
  }

  /// Position at the first user-visible entry with `user_key >= target`.
  pub(crate) fn seek(&mut self, user_key: &[u8]) {
    // Seek the internal iterator to `(user_key, snapshot_seq, Value)`, the
    // largest internal key that sorts at or before any real entry for this
    // user key at the snapshot.
    let ikey = make_internal_key(user_key, self.sequence, 1);
    self.iter.seek(&ikey);
    if self.iter.valid() {
      let mut skip = Vec::new();
      self.find_next_user_entry(false, &mut skip);
    } else {
      self.valid = false;
    }
  }

  /// Advance to the next user-visible entry.
  pub(crate) fn next(&mut self) {
    debug_assert!(self.valid);
    // Copy the current user key so we can skip all remaining internal entries
    // for it (older versions + duplicate writes).
    let ikey = self.iter.key().to_vec();
    let mut skip = parse_internal_key(&ikey)
      .map(|(uk, _, _)| uk.to_vec())
      .unwrap_or_default();
    // Advance the internal iterator past the current entry, then search.
    self.iter.next();
    if !self.iter.valid() {
      self.valid = false;
      return;
    }
    self.find_next_user_entry(true, &mut skip);
  }

  /// Current user key.  Only valid when `valid()` is true.
  ///
  /// The internal iterator is positioned at this entry, so the slice borrows
  /// from the inner iterator without copying.
  pub(crate) fn key(&self) -> &[u8] {
    debug_assert!(self.valid);
    let ikey = self.iter.key();
    // User key occupies all bytes except the trailing 8-byte tag.
    parse_internal_key(ikey)
      .map(|(uk, _, _)| uk)
      .unwrap_or(ikey)
  }

  /// Current value.  Only valid when `valid()` is true.
  pub(crate) fn value(&self) -> &[u8] {
    debug_assert!(self.valid);
    self.iter.value()
  }

  pub(crate) fn status(&self) -> Option<&Error> {
    if let Some(ref e) = self.status {
      return Some(e);
    }
    self.iter.status()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::db::merge_iter::MergingIterator;
  use crate::table::builder::TableBuilder;
  use crate::table::format::make_internal_key;
  use crate::table::reader::Table;

  // ── helpers ────────────────────────────────────────────────────────────────

  /// Build an SSTable from internal-key / value pairs and return a boxed
  /// `TwoLevelIterator` over it.
  fn table_iter(pairs: &[(&[u8], u64, u8, &[u8])]) -> Box<dyn InternalIterator> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut builder = TableBuilder::new(file, 4096, 16);
    for &(k, seq, vtype, v) in pairs {
      let ikey = make_internal_key(k, seq, vtype);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    Box::new(table.new_iterator().unwrap())
  }

  fn make_db_iter(children: Vec<Box<dyn InternalIterator>>, seq: u64) -> DbIterator {
    DbIterator::new(Box::new(MergingIterator::new(children)), seq)
  }

  // ── tests ──────────────────────────────────────────────────────────────────

  #[test]
  fn empty_iterator_not_valid() {
    let mut it = make_db_iter(vec![], 100);
    it.seek_to_first();
    assert!(!it.valid());
  }

  #[test]
  fn single_value_visible() {
    // seq=1 entry, snapshot=10 → visible.
    let child = table_iter(&[(b"foo", 1, 1, b"bar")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.key(), b"foo");
    assert_eq!(it.value(), b"bar");
    it.next();
    assert!(!it.valid());
  }

  #[test]
  fn entry_above_snapshot_invisible() {
    // Entry written at seq=5 is not visible at snapshot=4.
    let child = table_iter(&[(b"foo", 5, 1, b"bar")]);
    let mut it = make_db_iter(vec![child], 4);
    it.seek_to_first();
    assert!(!it.valid());
  }

  #[test]
  fn tombstone_hides_older_value() {
    // Higher-seq entries must come first in the SSTable (internal key: seq DESC).
    // seq=2: delete "foo" (comes first); seq=1: put "foo"="bar" (comes second).
    // At snapshot=10 "foo" is deleted.
    let child = table_iter(&[(b"foo", 2, 0, b""), (b"foo", 1, 1, b"bar")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_first();
    assert!(!it.valid(), "tombstone should hide the older value");
  }

  #[test]
  fn tombstone_below_snapshot_hides_value() {
    // Tombstone at seq=3 (comes first), value at seq=1.  Snapshot=5 sees both → key deleted.
    let child = table_iter(&[(b"a", 3, 0, b""), (b"a", 1, 1, b"v1")]);
    let mut it = make_db_iter(vec![child], 5);
    it.seek_to_first();
    assert!(!it.valid());
  }

  #[test]
  fn tombstone_above_snapshot_does_not_hide_value() {
    // Tombstone at seq=5 (comes first in SSTable), value at seq=1.
    // Snapshot=3: tombstone seq=5 > snapshot → invisible; value seq=1 ≤ snapshot → visible.
    let child = table_iter(&[(b"a", 5, 0, b""), (b"a", 1, 1, b"v1")]);
    let mut it = make_db_iter(vec![child], 3);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.key(), b"a");
    assert_eq!(it.value(), b"v1");
  }

  #[test]
  fn newest_version_wins() {
    // Two versions of "foo".  Higher seq (3) comes first in SSTable.
    // Snapshot sees both; newest (seq=3) is returned; older (seq=1) is skipped.
    let child = table_iter(&[(b"foo", 3, 1, b"new"), (b"foo", 1, 1, b"old")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.key(), b"foo");
    assert_eq!(it.value(), b"new");
    it.next();
    assert!(!it.valid());
  }

  #[test]
  fn iterate_multiple_keys() {
    let child = table_iter(&[(b"a", 1, 1, b"1"), (b"b", 2, 1, b"2"), (b"c", 3, 1, b"3")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_first();
    for (k, v) in [(b"a" as &[u8], b"1" as &[u8]), (b"b", b"2"), (b"c", b"3")] {
      assert!(it.valid());
      assert_eq!(it.key(), k);
      assert_eq!(it.value(), v);
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn seek_positions_at_correct_key() {
    let child = table_iter(&[
      (b"apple", 1, 1, b"a"),
      (b"banana", 2, 1, b"b"),
      (b"cherry", 3, 1, b"c"),
    ]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek(b"banana");
    assert!(it.valid());
    assert_eq!(it.key(), b"banana");
    assert_eq!(it.value(), b"b");
    it.next();
    assert!(it.valid());
    assert_eq!(it.key(), b"cherry");
  }

  #[test]
  fn seek_skips_to_next_visible_key() {
    // "b" is deleted; seeking to "b" should yield "c".
    let child = table_iter(&[
      (b"a", 1, 1, b"a"),
      (b"b", 2, 0, b""), // tombstone
      (b"c", 3, 1, b"c"),
    ]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek(b"b");
    assert!(it.valid());
    assert_eq!(it.key(), b"c");
  }

  #[test]
  fn seek_past_all_keys_not_valid() {
    let child = table_iter(&[(b"a", 1, 1, b"1")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek(b"z");
    assert!(!it.valid());
  }

  #[test]
  fn merged_sources_snapshot_and_tombstones() {
    // Source 0 (newer, L0): put a@5, delete b@4, put c@3
    // Source 1 (older, L0): put a@2, put b@1, put d@1
    // Snapshot = 10.  Expected visible keys: a@5=v2, c@3=vc, d@1=vd (b deleted).
    let c0 = table_iter(&[
      (b"a", 5, 1, b"v2"),
      (b"b", 4, 0, b""), // tombstone
      (b"c", 3, 1, b"vc"),
    ]);
    let c1 = table_iter(&[
      (b"a", 2, 1, b"v1"),
      (b"b", 1, 1, b"vb"),
      (b"d", 1, 1, b"vd"),
    ]);
    let mut it = make_db_iter(vec![c0, c1], 10);
    it.seek_to_first();

    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while it.valid() {
      result.push((it.key().to_vec(), it.value().to_vec()));
      it.next();
    }
    assert_eq!(
      result,
      vec![
        (b"a".to_vec(), b"v2".to_vec()),
        (b"c".to_vec(), b"vc".to_vec()),
        (b"d".to_vec(), b"vd".to_vec()),
      ]
    );
  }
}
