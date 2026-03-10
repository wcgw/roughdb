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
use crate::table::format::cmp_internal_keys;

/// N-way merge iterator over a set of sorted [`InternalIterator`]s.
///
/// At each step the iterator yields the smallest key across all children,
/// breaking ties by the child's position in the `children` slice (lower index
/// wins, corresponding to "newer" sources in the merge order).  The current
/// child is tracked by index; a linear scan is used to find the minimum — this
/// is O(N) but N is small in practice (memtable + a handful of L0 files).
///
/// See `table/merger.h/cc` in LevelDB.
pub(crate) struct MergingIterator {
  children: Vec<Box<dyn InternalIterator>>,
  /// Index into `children` of the child currently positioned at the smallest
  /// key.  `None` when no child is valid (iterator exhausted or unpositioned).
  current: Option<usize>,
}

impl MergingIterator {
  pub(crate) fn new(children: Vec<Box<dyn InternalIterator>>) -> Self {
    MergingIterator {
      children,
      current: None,
    }
  }

  /// Scan all valid children and set `current` to the one with the smallest
  /// key.  Among equal keys the child with the lowest index wins.
  fn find_smallest(&mut self) {
    self.current = self
      .children
      .iter()
      .enumerate()
      .filter(|(_, c)| c.valid())
      .min_by(|(i, a), (j, b)| {
        let ord = cmp_internal_keys(a.key(), b.key());
        // For equal keys, prefer the lower index (newer source).
        if ord == std::cmp::Ordering::Equal {
          i.cmp(j)
        } else {
          ord
        }
      })
      .map(|(i, _)| i);
  }

  /// Scan all valid children and set `current` to the one with the largest
  /// key.  Among equal keys the child with the lowest index wins (same
  /// tiebreaking as forward direction).
  fn find_largest(&mut self) {
    self.current = self
      .children
      .iter()
      .enumerate()
      .filter(|(_, c)| c.valid())
      .max_by(|(i, a), (j, b)| {
        let ord = cmp_internal_keys(a.key(), b.key());
        // For equal keys, lower index wins: reverse-compare indices so that
        // max_by selects the lower index (it returns the "larger" element).
        if ord == std::cmp::Ordering::Equal {
          j.cmp(i)
        } else {
          ord
        }
      })
      .map(|(i, _)| i);
  }
}

impl InternalIterator for MergingIterator {
  fn valid(&self) -> bool {
    self.current.is_some()
  }

  fn seek_to_first(&mut self) {
    for child in &mut self.children {
      child.seek_to_first();
    }
    self.find_smallest();
  }

  fn seek_to_last(&mut self) {
    for child in &mut self.children {
      child.seek_to_last();
    }
    self.find_largest();
  }

  fn seek(&mut self, target: &[u8]) {
    for child in &mut self.children {
      child.seek(target);
    }
    self.find_smallest();
  }

  fn next(&mut self) {
    debug_assert!(self.valid());
    let cur = self.current.unwrap();
    self.children[cur].next();
    self.find_smallest();
  }

  fn prev(&mut self) {
    debug_assert!(self.valid());
    let cur = self.current.unwrap();
    self.children[cur].prev();
    self.find_largest();
  }

  fn key(&self) -> &[u8] {
    debug_assert!(self.valid());
    self.children[self.current.unwrap()].key()
  }

  fn value(&self) -> &[u8] {
    debug_assert!(self.valid());
    self.children[self.current.unwrap()].value()
  }

  fn status(&self) -> Option<&Error> {
    for child in &self.children {
      if let Some(e) = child.status() {
        return Some(e);
      }
    }
    None
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::table::builder::TableBuilder;
  use crate::table::format::{make_internal_key, parse_internal_key};
  use crate::table::reader::Table;

  // ── helpers ────────────────────────────────────────────────────────────────

  fn user_key(ikey: &[u8]) -> &[u8] {
    parse_internal_key(ikey)
      .map(|(uk, _, _)| uk)
      .expect("valid internal key")
  }

  /// Build an in-memory SSTable from `pairs` and return a `TwoLevelIterator`
  /// over it.
  fn table_iter(pairs: &[(&[u8], &[u8])]) -> Box<dyn InternalIterator> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut builder = TableBuilder::new(
      file,
      4096,
      16,
      None,
      crate::options::CompressionType::NoCompression,
    );
    for (seq, &(k, v)) in pairs.iter().enumerate() {
      let ikey = make_internal_key(k, seq as u64 + 1, 1);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    let table = Table::open(tmp.reopen().unwrap(), size, None).unwrap();
    Box::new(table.new_iterator(false).unwrap())
  }

  /// Build a simple vec iterator from (ikey, value) pairs.
  struct VecIter {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
  }

  impl VecIter {
    fn new(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
      VecIter {
        entries: pairs,
        pos: usize::MAX,
      }
    }
  }

  impl InternalIterator for VecIter {
    fn valid(&self) -> bool {
      self.pos < self.entries.len()
    }

    fn seek_to_first(&mut self) {
      self.pos = if self.entries.is_empty() {
        usize::MAX
      } else {
        0
      };
    }

    fn seek_to_last(&mut self) {
      self.pos = if self.entries.is_empty() {
        usize::MAX
      } else {
        self.entries.len() - 1
      };
    }

    fn seek(&mut self, target: &[u8]) {
      self.pos = self
        .entries
        .iter()
        .position(|(k, _)| cmp_internal_keys(k, target) != std::cmp::Ordering::Less)
        .unwrap_or(usize::MAX);
    }

    fn next(&mut self) {
      debug_assert!(self.valid());
      self.pos += 1;
      if self.pos >= self.entries.len() {
        self.pos = usize::MAX;
      }
    }

    fn prev(&mut self) {
      debug_assert!(self.valid());
      if self.pos == 0 {
        self.pos = usize::MAX;
      } else {
        self.pos -= 1;
      }
    }

    fn key(&self) -> &[u8] {
      &self.entries[self.pos].0
    }

    fn value(&self) -> &[u8] {
      &self.entries[self.pos].1
    }

    fn status(&self) -> Option<&Error> {
      None
    }
  }

  fn make_ikey(key: &[u8], seq: u64) -> Vec<u8> {
    make_internal_key(key, seq, 1)
  }

  fn vec_iter(pairs: &[(&[u8], u64, &[u8])]) -> Box<dyn InternalIterator> {
    let entries = pairs
      .iter()
      .map(|&(k, seq, v)| (make_ikey(k, seq), v.to_vec()))
      .collect();
    Box::new(VecIter::new(entries))
  }

  // ── tests ──────────────────────────────────────────────────────────────────

  #[test]
  fn empty_no_children() {
    let mut it = MergingIterator::new(vec![]);
    it.seek_to_first();
    assert!(!it.valid());
  }

  #[test]
  fn single_child_iterate_all() {
    let pairs = &[
      (b"a" as &[u8], 1, b"1" as &[u8]),
      (b"b", 2, b"2"),
      (b"c", 3, b"3"),
    ];
    let mut it = MergingIterator::new(vec![vec_iter(pairs)]);
    it.seek_to_first();
    for &(k, _, v) in pairs {
      assert!(it.valid());
      assert_eq!(user_key(it.key()), k);
      assert_eq!(it.value(), v);
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn two_disjoint_ranges_merged_in_order() {
    // child 0: a, c, e — child 1: b, d, f
    let c0 = vec_iter(&[(b"a", 1, b"a0"), (b"c", 3, b"c0"), (b"e", 5, b"e0")]);
    let c1 = vec_iter(&[(b"b", 2, b"b1"), (b"d", 4, b"d1"), (b"f", 6, b"f1")]);
    let mut it = MergingIterator::new(vec![c0, c1]);
    it.seek_to_first();
    let expected: &[(&[u8], &[u8])] = &[
      (b"a", b"a0"),
      (b"b", b"b1"),
      (b"c", b"c0"),
      (b"d", b"d1"),
      (b"e", b"e0"),
      (b"f", b"f1"),
    ];
    for &(k, v) in expected {
      assert!(it.valid(), "expected key {:?}", k);
      assert_eq!(user_key(it.key()), k);
      assert_eq!(it.value(), v);
      it.next();
    }
    assert!(!it.valid());
  }

  #[test]
  fn duplicate_key_lower_index_child_wins() {
    // Both children have key "a" — child 0 (index 0) should win.
    let c0 = vec_iter(&[(b"a", 2, b"newer")]);
    let c1 = vec_iter(&[(b"a", 1, b"older")]);
    let mut it = MergingIterator::new(vec![c0, c1]);
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(it.value(), b"newer");
  }

  #[test]
  fn seek_positions_correctly() {
    let c0 = vec_iter(&[(b"a", 1, b"1"), (b"c", 3, b"3"), (b"e", 5, b"5")]);
    let c1 = vec_iter(&[(b"b", 2, b"2"), (b"d", 4, b"4"), (b"f", 6, b"6")]);
    let mut it = MergingIterator::new(vec![c0, c1]);
    let target = make_ikey(b"c", u64::MAX >> 8);
    it.seek(&target);
    assert!(it.valid());
    assert_eq!(user_key(it.key()), b"c");
    assert_eq!(it.value(), b"3");
    it.next();
    assert!(it.valid());
    assert_eq!(user_key(it.key()), b"d");
  }

  #[test]
  fn seek_past_all_keys() {
    let c0 = vec_iter(&[(b"a", 1, b"1")]);
    let mut it = MergingIterator::new(vec![c0]);
    let target = make_ikey(b"z", u64::MAX >> 8);
    it.seek(&target);
    assert!(!it.valid());
  }

  #[test]
  fn with_sstable_children() {
    let t0 = table_iter(&[(b"alpha", b"A"), (b"gamma", b"G")]);
    let t1 = table_iter(&[(b"beta", b"B"), (b"delta", b"D")]);
    let mut it = MergingIterator::new(vec![t0, t1]);
    it.seek_to_first();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(user_key(it.key()).to_vec());
      it.next();
    }
    assert_eq!(
      keys,
      vec![
        b"alpha".to_vec(),
        b"beta".to_vec(),
        b"delta".to_vec(),
        b"gamma".to_vec()
      ]
    );
  }

  // ── Backward iteration tests ──────────────────────────────────────────────

  #[test]
  fn seek_to_last_empty() {
    let mut it = MergingIterator::new(vec![]);
    it.seek_to_last();
    assert!(!it.valid());
  }

  #[test]
  fn seek_to_last_single_child() {
    let c0 = vec_iter(&[(b"a", 1, b"1"), (b"b", 2, b"2"), (b"c", 3, b"3")]);
    let mut it = MergingIterator::new(vec![c0]);
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(user_key(it.key()), b"c");
    assert_eq!(it.value(), b"3");
  }

  #[test]
  fn prev_iterates_backward_two_children() {
    let c0 = vec_iter(&[(b"a", 1, b"a0"), (b"c", 3, b"c0"), (b"e", 5, b"e0")]);
    let c1 = vec_iter(&[(b"b", 2, b"b1"), (b"d", 4, b"d1"), (b"f", 6, b"f1")]);
    let mut it = MergingIterator::new(vec![c0, c1]);
    it.seek_to_last();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(user_key(it.key()).to_vec());
      it.prev();
    }
    assert_eq!(
      keys,
      vec![
        b"f".to_vec(),
        b"e".to_vec(),
        b"d".to_vec(),
        b"c".to_vec(),
        b"b".to_vec(),
        b"a".to_vec(),
      ]
    );
  }

  #[test]
  fn seek_to_last_with_multi_version_key() {
    // c0 has z@seq=2, c1 has z@seq=1.  Internal key ordering: z@2 < z@1
    // (higher seq sorts first = is "smaller").  find_largest picks z@1 (c1).
    let c0 = vec_iter(&[(b"z", 2, b"newer")]);
    let c1 = vec_iter(&[(b"z", 1, b"older")]);
    let mut it = MergingIterator::new(vec![c0, c1]);
    it.seek_to_last();
    assert!(it.valid());
    // z@1 is the larger internal key → child 1 wins at the MergingIterator level.
    assert_eq!(it.value(), b"older");
    // The DbIterator (on top) would then scan backward and surface "newer".
  }

  #[test]
  fn backward_with_sstable_children() {
    let t0 = table_iter(&[(b"alpha", b"A"), (b"gamma", b"G")]);
    let t1 = table_iter(&[(b"beta", b"B"), (b"delta", b"D")]);
    let mut it = MergingIterator::new(vec![t0, t1]);
    it.seek_to_last();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while it.valid() {
      keys.push(user_key(it.key()).to_vec());
      it.prev();
    }
    assert_eq!(
      keys,
      vec![
        b"gamma".to_vec(),
        b"delta".to_vec(),
        b"beta".to_vec(),
        b"alpha".to_vec(),
      ]
    );
  }
}
