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

/// Direction of the last positioning or movement operation.
#[derive(Debug, PartialEq)]
enum Direction {
  Forward,
  Reverse,
}

/// User-facing iterator over a merged view of the database.
///
/// Wraps an [`InternalIterator`] (typically a [`MergingIterator`]) and applies:
///
/// - **Snapshot filtering**: entries with `seq > sequence` are invisible.
/// - **Tombstone handling**: a deletion marker hides all older versions of
///   that key, including keys written earlier within the same sequence range.
/// - **Version merging**: only the newest visible version of each user key is
///   returned; older versions are silently skipped.
///
/// Supports both forward (`seek_to_first`, `seek`, `next`) and backward
/// (`seek_to_last`, `prev`) iteration.  Switching directions is handled
/// transparently.
///
/// **Direction semantics** (matching LevelDB's `DBIter`):
/// - Forward: `iter` is positioned at the current entry; `key()` / `value()`
///   borrow directly from `iter` without copying.
/// - Reverse: `iter` is positioned at the entry *just before* the current one
///   in key order; `key()` / `value()` return `saved_key` / `saved_value`.
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
  /// Current traversal direction.
  direction: Direction,
  /// In Reverse direction: user key of the current entry.
  saved_key: Vec<u8>,
  /// In Reverse direction: value bytes of the current entry.
  saved_value: Vec<u8>,
}

impl DbIterator {
  pub(crate) fn new(iter: Box<dyn InternalIterator>, sequence: u64) -> Self {
    DbIterator {
      iter,
      sequence,
      status: None,
      valid: false,
      direction: Direction::Forward,
      saved_key: Vec::new(),
      saved_value: Vec::new(),
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

  /// Scan backward through the internal iterator to find the previous
  /// user-visible entry, storing it in `saved_key` / `saved_value`.
  ///
  /// Must be called with `direction == Reverse`.  `iter` should be positioned
  /// at or before the entry just before the one we want to present:
  /// - On entry, `saved_key` holds the user key that we must scan *past*
  ///   (i.e. we want a user key strictly less than it).
  /// - On return, if a valid entry is found, `self.valid = true` and
  ///   `saved_key` / `saved_value` hold the new current user entry.
  ///   `iter` is left positioned at the entry *before* the new current one.
  /// - If no valid entry exists, `self.valid = false` and direction is reset
  ///   to Forward (matching LevelDB).
  ///
  /// See `db/db_iter.cc: DBIter::FindPrevUserEntry`.
  fn find_prev_user_entry(&mut self) {
    debug_assert_eq!(self.direction, Direction::Reverse);
    // `value_type` tracks whether we have saved a Value entry in this call.
    // Starts as 0 (Deletion) so the break condition does not fire on the
    // first visible entry.
    let mut value_type: u8 = 0; // 0 = Deletion, 1 = Value

    while self.iter.valid() {
      let ikey = self.iter.key();
      if let Some((user_key, seq, vtype)) = parse_internal_key(ikey) {
        if seq <= self.sequence {
          // Stop when we encounter a value entry for a user key that is
          // strictly smaller than the one we saved (we've found our answer).
          if value_type != 0 && user_key < self.saved_key.as_slice() {
            break;
          }
          value_type = vtype;
          if vtype == 0 {
            // Deletion marker: clear the saved entry (this key is deleted).
            self.saved_key.clear();
            self.saved_value.clear();
          } else {
            // Value: update saved entry with the most recent version seen.
            let raw_value = self.iter.value();
            self.saved_value.clear();
            self.saved_value.extend_from_slice(raw_value);
            self.saved_key.clear();
            self.saved_key.extend_from_slice(user_key);
          }
        }
        // seq > snapshot: invisible; fall through to iter.prev()
      } else {
        self.status = Some(Error::Corruption(
          "corrupted internal key in DbIterator (reverse)".to_string(),
        ));
      }
      self.iter.prev();
    }

    if value_type == 0 {
      // Reached the beginning without finding a visible entry.
      self.valid = false;
      self.saved_key.clear();
      self.saved_value.clear();
      self.direction = Direction::Forward;
    } else {
      self.valid = true;
    }
  }

  // ── Public iterator interface ───────────────────────────────────────────

  pub(crate) fn valid(&self) -> bool {
    self.valid
  }

  /// Position at the first user-visible entry.
  pub(crate) fn seek_to_first(&mut self) {
    self.direction = Direction::Forward;
    self.iter.seek_to_first();
    if self.iter.valid() {
      let mut skip = Vec::new();
      self.find_next_user_entry(false, &mut skip);
    } else {
      self.valid = false;
    }
  }

  /// Position at the last user-visible entry.
  ///
  /// After this call the iterator is in Reverse direction; `key()` and
  /// `value()` return `saved_key` / `saved_value`.
  pub(crate) fn seek_to_last(&mut self) {
    self.direction = Direction::Reverse;
    self.saved_value.clear();
    self.iter.seek_to_last();
    self.find_prev_user_entry();
  }

  /// Position at the first user-visible entry with `user_key >= target`.
  pub(crate) fn seek(&mut self, user_key: &[u8]) {
    self.direction = Direction::Forward;
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

    let mut skip: Vec<u8>;

    if self.direction == Direction::Reverse {
      // Switching from Reverse to Forward.
      // `iter` is positioned at an entry with user_key < saved_key (the
      // entry that caused `find_prev_user_entry` to stop), or it is invalid
      // (we were at the very first entry).
      self.direction = Direction::Forward;
      if !self.iter.valid() {
        self.iter.seek_to_first();
      } else {
        self.iter.next();
      }
      if !self.iter.valid() {
        self.valid = false;
        self.saved_key.clear();
        return;
      }
      // Use saved_key as the key to skip (advance past the entry we were
      // presenting before the direction switch).
      skip = std::mem::take(&mut self.saved_key);
    } else {
      // Forward → Forward: copy current key and advance.
      let ikey = self.iter.key().to_vec();
      skip = parse_internal_key(&ikey)
        .map(|(uk, _, _)| uk.to_vec())
        .unwrap_or_default();
      self.iter.next();
      if !self.iter.valid() {
        self.valid = false;
        return;
      }
    }

    self.find_next_user_entry(true, &mut skip);
  }

  /// Move to the previous user-visible entry.
  ///
  /// If currently in Forward direction, switches to Reverse by scanning
  /// backward past the current entry.
  pub(crate) fn prev(&mut self) {
    debug_assert!(self.valid);

    if self.direction == Direction::Forward {
      // Switching from Forward to Reverse.
      // `iter` is positioned AT the current entry.  Save the current user
      // key, then scan backward until we see a different (smaller) user key.
      debug_assert!(self.iter.valid());
      let ikey = self.iter.key().to_vec();
      self.saved_key.clear();
      if let Some((uk, _, _)) = parse_internal_key(&ikey) {
        self.saved_key.extend_from_slice(uk);
      }
      loop {
        self.iter.prev();
        if !self.iter.valid() {
          self.valid = false;
          self.saved_key.clear();
          self.saved_value.clear();
          return;
        }
        if let Some((uk, _, _)) = parse_internal_key(self.iter.key()) {
          if uk < self.saved_key.as_slice() {
            break;
          }
        }
      }
      self.direction = Direction::Reverse;
    }

    self.find_prev_user_entry();
  }

  /// Current user key.  Only valid when `valid()` is true.
  ///
  /// In Forward direction the slice borrows from the inner iterator (no copy).
  /// In Reverse direction returns `saved_key`.
  pub(crate) fn key(&self) -> &[u8] {
    debug_assert!(self.valid);
    if self.direction == Direction::Forward {
      let ikey = self.iter.key();
      parse_internal_key(ikey)
        .map(|(uk, _, _)| uk)
        .unwrap_or(ikey)
    } else {
      &self.saved_key
    }
  }

  /// Current value.  Only valid when `valid()` is true.
  pub(crate) fn value(&self) -> &[u8] {
    debug_assert!(self.valid);
    if self.direction == Direction::Forward {
      self.iter.value()
    } else {
      &self.saved_value
    }
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
    let mut builder = TableBuilder::new(
      file,
      4096,
      16,
      None,
      crate::options::CompressionType::NoCompression,
    );
    for &(k, seq, vtype, v) in pairs {
      let ikey = make_internal_key(k, seq, vtype);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    let table = Table::open(tmp.reopen().unwrap(), size, None).unwrap();
    Box::new(table.new_iterator(false).unwrap())
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

  // ── Backward iteration tests ──────────────────────────────────────────────

  #[test]
  fn seek_to_last_empty_not_valid() {
    let mut it = make_db_iter(vec![], 100);
    it.seek_to_last();
    assert!(!it.valid());
  }

  #[test]
  fn seek_to_last_basic() {
    let child = table_iter(&[(b"a", 1, 1, b"1"), (b"b", 2, 1, b"2"), (b"c", 3, 1, b"3")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), b"c");
    assert_eq!(it.value(), b"3");
  }

  #[test]
  fn prev_iterates_all_backward() {
    let child = table_iter(&[(b"a", 1, 1, b"1"), (b"b", 2, 1, b"2"), (b"c", 3, 1, b"3")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_last();
    let mut result: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while it.valid() {
      result.push((it.key().to_vec(), it.value().to_vec()));
      it.prev();
    }
    assert_eq!(
      result,
      vec![
        (b"c".to_vec(), b"3".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"a".to_vec(), b"1".to_vec()),
      ]
    );
  }

  #[test]
  fn prev_tombstone_hides_deleted_key() {
    // del b@3 (first in SSTable for "b"), put b@1; snapshot=10 → "b" deleted.
    let child = table_iter(&[
      (b"a", 1, 1, b"va"),
      (b"b", 3, 0, b""), // tombstone
      (b"b", 1, 1, b"vb"),
      (b"c", 2, 1, b"vc"),
    ]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_last();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(it.key().to_vec());
      it.prev();
    }
    assert_eq!(keys, vec![b"c".to_vec(), b"a".to_vec()]);
  }

  #[test]
  fn prev_snapshot_filters_future_entries() {
    // "c" written at seq=5 is invisible at snapshot=3.
    let child = table_iter(&[(b"a", 1, 1, b"a"), (b"b", 2, 1, b"b"), (b"c", 5, 1, b"c")]);
    let mut it = make_db_iter(vec![child], 3);
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.key(), b"b");
    it.prev();
    assert!(it.valid());
    assert_eq!(it.key(), b"a");
    it.prev();
    assert!(!it.valid());
  }

  #[test]
  fn prev_after_forward_seek() {
    let child = table_iter(&[(b"a", 1, 1, b"a"), (b"b", 2, 1, b"b"), (b"c", 3, 1, b"c")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek(b"b");
    assert_eq!(it.key(), b"b");
    it.prev();
    assert!(it.valid());
    assert_eq!(it.key(), b"a");
    it.prev();
    assert!(!it.valid());
  }

  #[test]
  fn next_after_prev_switches_direction() {
    let child = table_iter(&[(b"a", 1, 1, b"a"), (b"b", 2, 1, b"b"), (b"c", 3, 1, b"c")]);
    let mut it = make_db_iter(vec![child], 10);
    it.seek_to_last();
    assert_eq!(it.key(), b"c");
    it.prev();
    assert_eq!(it.key(), b"b");
    // Switch back to forward.
    it.next();
    assert!(it.valid());
    assert_eq!(it.key(), b"c");
  }
}
