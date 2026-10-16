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

use crate::coding::{
  read_u32_le, read_u64_le, read_varu64, write_u32_le, write_u64_le, write_varu64,
};
use crate::table::format::ValueType;
use crate::Error;

const HEADER_SIZE: usize = 12;

/// One operation of a [`WriteBatch`], borrowed from the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOp<'a> {
  Put { key: &'a [u8], value: &'a [u8] },
  Delete { key: &'a [u8] },
}

#[derive(Clone)]
pub struct WriteBatch {
  rep: Vec<u8>,
}

impl WriteBatch {
  pub fn new() -> Self {
    WriteBatch {
      rep: vec![0u8; HEADER_SIZE],
    }
  }

  pub fn put<K, V>(&mut self, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let (key, value) = (key.as_ref(), value.as_ref());
    self.rep.reserve(key.len() + value.len() + 21);
    self.rep.push(ValueType::Value as u8);
    let mut tmp = [0u8; 10];
    let n = write_varu64(&mut tmp, key.len() as u64);
    self.rep.extend_from_slice(&tmp[..n]);
    self.rep.extend_from_slice(key);
    let n = write_varu64(&mut tmp, value.len() as u64);
    self.rep.extend_from_slice(&tmp[..n]);
    self.rep.extend_from_slice(value);
    self.set_count(self.count() + 1);
  }

  pub fn delete<K>(&mut self, key: K)
  where
    K: AsRef<[u8]>,
  {
    let key = key.as_ref();
    self.rep.reserve(key.len() + 11);
    self.rep.push(ValueType::Deletion as u8);
    let mut tmp = [0u8; 10];
    let n = write_varu64(&mut tmp, key.len() as u64);
    self.rep.extend_from_slice(&tmp[..n]);
    self.rep.extend_from_slice(key);
    self.set_count(self.count() + 1);
  }

  pub fn clear(&mut self) {
    self.rep.clear();
    self.rep.resize(HEADER_SIZE, 0);
  }

  pub fn approximate_size(&self) -> usize {
    self.rep.len()
  }

  /// The batch's operations, in the order they were added.
  ///
  /// A batch built with [`put`](Self::put) and [`delete`](Self::delete) is
  /// always well-formed.  One read back from a damaged WAL may not be: the
  /// iterator then yields a single [`Error::Corruption`] (a truncated or
  /// unknown record, or a record count that disagrees with the header) and
  /// ends, after the operations before the damage.
  pub fn iter(&self) -> Iter<'_> {
    Iter {
      rep: &self.rep,
      pos: HEADER_SIZE,
      seen: 0,
      expected: self.count(),
      done: false,
    }
  }

  pub fn append(&mut self, src: &WriteBatch) {
    let combined = self.count() + src.count();
    self.rep.extend_from_slice(&src.rep[HEADER_SIZE..]);
    self.set_count(combined);
  }

  // ── pub(crate) internals ────────────────────────────────────────────────────

  pub(crate) fn sequence(&self) -> u64 {
    read_u64_le(self.rep[0..8].try_into().unwrap())
  }

  pub(crate) fn set_sequence(&mut self, seq: u64) {
    write_u64_le((&mut self.rep[0..8]).try_into().unwrap(), seq);
  }

  pub(crate) fn count(&self) -> u32 {
    read_u32_le(self.rep[8..12].try_into().unwrap())
  }

  pub(crate) fn contents(&self) -> &[u8] {
    &self.rep
  }

  /// Wrap raw bytes (as read from the WAL) into a `WriteBatch` for replay.
  pub(crate) fn from_contents(data: Vec<u8>) -> Result<Self, Error> {
    if data.len() < HEADER_SIZE {
      return Err(Error::Corruption("write batch record too small".to_owned()));
    }
    Ok(Self { rep: data })
  }

  fn set_count(&mut self, n: u32) {
    write_u32_le((&mut self.rep[8..12]).try_into().unwrap(), n);
  }
}

impl Default for WriteBatch {
  fn default() -> Self {
    Self::new()
  }
}

impl<'a> IntoIterator for &'a WriteBatch {
  type Item = Result<BatchOp<'a>, Error>;
  type IntoIter = Iter<'a>;

  fn into_iter(self) -> Iter<'a> {
    self.iter()
  }
}

/// Iterator over the operations of a [`WriteBatch`]; see [`WriteBatch::iter`].
#[derive(Debug, Clone)]
pub struct Iter<'a> {
  rep: &'a [u8],
  pos: usize,
  /// Records decoded so far, checked against the header's count at the end.
  seen: u32,
  expected: u32,
  /// Set once the iterator has ended, after the last record or an error.
  done: bool,
}

impl<'a> Iterator for Iter<'a> {
  type Item = Result<BatchOp<'a>, Error>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.done {
      return None;
    }
    if self.pos >= self.rep.len() {
      self.done = true;
      return (self.seen != self.expected).then(|| {
        Err(Error::Corruption(format!(
          "WriteBatch count mismatch: header says {}, found {}",
          self.expected, self.seen
        )))
      });
    }
    let op = self.decode_record();
    match op {
      Ok(_) => self.seen += 1,
      Err(_) => self.done = true,
    }
    Some(op)
  }
}

impl std::iter::FusedIterator for Iter<'_> {}

impl<'a> Iter<'a> {
  fn decode_record(&mut self) -> Result<BatchOp<'a>, Error> {
    let tag = self.rep[self.pos];
    self.pos += 1;
    match ValueType::from_byte(tag) {
      Some(ValueType::Value) => {
        let key = self.decode_slice("key")?;
        let value = self.decode_slice("value")?;
        Ok(BatchOp::Put { key, value })
      }
      Some(ValueType::Deletion) => Ok(BatchOp::Delete {
        key: self.decode_slice("key")?,
      }),
      None => Err(Error::Corruption(format!("unknown WriteBatch tag: {tag}"))),
    }
  }

  /// Decode a length-prefixed slice (the record's `what`).
  fn decode_slice(&mut self, what: &str) -> Result<&'a [u8], Error> {
    let (len, n) = read_varu64(&self.rep[self.pos..]);
    if n == 0 {
      return Err(Error::Corruption(format!(
        "bad varint in WriteBatch {what} length"
      )));
    }
    self.pos += n;
    let remaining = &self.rep[self.pos..];
    // `len` comes from the record: compare before converting, so a huge one
    // cannot overflow the end offset.
    if len > remaining.len() as u64 {
      return Err(Error::Corruption(format!("WriteBatch {what} truncated")));
    }
    let slice = &remaining[..len as usize];
    self.pos += slice.len();
    Ok(slice)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Db, ReadOptions, WriteOptions};

  fn ops(b: &WriteBatch) -> Result<Vec<BatchOp<'_>>, Error> {
    b.iter().collect()
  }

  fn put<'a>(key: &'a [u8], value: &'a [u8]) -> BatchOp<'a> {
    BatchOp::Put { key, value }
  }

  fn delete(key: &[u8]) -> BatchOp<'_> {
    BatchOp::Delete { key }
  }

  #[test]
  fn empty_batch() {
    let b = WriteBatch::new();
    assert_eq!(b.approximate_size(), 12);
    assert_eq!(b.count(), 0);
    assert!(ops(&b).unwrap().is_empty());
  }

  #[test]
  fn put_and_iterate() {
    let mut b = WriteBatch::new();
    b.put("hello", "world");
    assert_eq!(ops(&b).unwrap(), vec![put(b"hello", b"world")]);
  }

  #[test]
  fn delete_and_iterate() {
    let mut b = WriteBatch::new();
    b.delete(b"gone");
    assert_eq!(ops(&b).unwrap(), vec![delete(b"gone")]);
  }

  #[test]
  fn mixed_iterate() {
    let mut b = WriteBatch::new();
    b.put(b"a", b"1");
    b.delete(b"b");
    b.put(b"c", b"3");
    assert_eq!(
      ops(&b).unwrap(),
      vec![put(b"a", b"1"), delete(b"b"), put(b"c", b"3"),]
    );
  }

  #[test]
  fn approximate_size_grows() {
    let mut b = WriteBatch::new();
    let s0 = b.approximate_size();
    b.put(b"k", b"v");
    let s1 = b.approximate_size();
    b.delete(b"k");
    let s2 = b.approximate_size();
    assert!(s1 > s0);
    assert!(s2 > s1);
  }

  #[test]
  fn clear_resets() {
    let mut b = WriteBatch::new();
    b.put(b"x", b"y");
    b.clear();
    assert_eq!(b.approximate_size(), 12);
    assert_eq!(b.count(), 0);
    assert!(ops(&b).unwrap().is_empty());
  }

  #[test]
  fn append_merges() {
    let mut a = WriteBatch::new();
    a.put(b"a", b"1");
    let mut b = WriteBatch::new();
    b.put(b"b", b"2");
    b.delete(b"c");
    a.append(&b);
    assert_eq!(a.count(), 3);
    assert_eq!(
      ops(&a).unwrap(),
      vec![put(b"a", b"1"), put(b"b", b"2"), delete(b"c"),]
    );
  }

  #[test]
  fn corrupt_truncated() {
    let mut b = WriteBatch::new();
    b.put(b"key", b"value");
    // truncate mid-record
    b.rep.truncate(b.rep.len() - 2);
    assert!(matches!(ops(&b), Err(Error::Corruption(_))));
  }

  #[test]
  fn corrupt_bad_tag() {
    let mut b = WriteBatch::new();
    b.put(b"k", b"v");
    // overwrite the tag byte with an invalid value
    b.rep[HEADER_SIZE] = 0xFF;
    assert!(matches!(ops(&b), Err(Error::Corruption(_))));
  }

  /// A length read from a WAL record is untrusted: one that runs past the end
  /// of the record must be reported as corruption, however large.
  #[test]
  fn corrupt_length_past_the_end() {
    let mut rep = vec![0u8; HEADER_SIZE];
    write_u32_le((&mut rep[8..12]).try_into().unwrap(), 1);
    rep.push(ValueType::Value as u8);
    let mut tmp = [0u8; 10];
    let n = write_varu64(&mut tmp, u64::MAX);
    rep.extend_from_slice(&tmp[..n]);
    let b = WriteBatch::from_contents(rep).unwrap();
    assert!(matches!(ops(&b), Err(Error::Corruption(_))));
  }

  // Port of LevelDB write_batch_test.cc: Append
  //
  // Verifies sequential appends: empty-to-empty stays empty, each subsequent
  // append accumulates records from the source batch, and count is maintained.
  #[test]
  fn append_multiple_steps() {
    let mut b1 = WriteBatch::new();
    let mut b2 = WriteBatch::new();
    b1.set_sequence(200);
    b2.set_sequence(300);

    // Append empty b2 to empty b1 → count stays 0.
    b1.append(&b2);
    assert_eq!(b1.count(), 0);
    assert!(ops(&b1).unwrap().is_empty());

    // b2 gets one entry; appending to b1 brings count to 1.
    b2.put(b"a", b"va");
    b1.append(&b2);
    assert_eq!(b1.count(), 1);
    assert_eq!(ops(&b1).unwrap(), vec![put(b"a", b"va")]);

    // b2 cleared, gets one new entry; b1 count reaches 2.
    b2.clear();
    b2.put(b"b", b"vb");
    b1.append(&b2);
    assert_eq!(b1.count(), 2);

    // b2 gains a delete; appending both entries brings b1 count to 4.
    b2.delete(b"foo");
    b1.append(&b2);
    assert_eq!(b1.count(), 4);
    assert_eq!(
      ops(&b1).unwrap(),
      vec![
        put(b"a", b"va"),
        put(b"b", b"vb"),
        put(b"b", b"vb"),
        delete(b"foo"),
      ]
    );
  }

  #[test]
  fn db_write_round_trip() {
    let db = Db::default();
    let mut batch = WriteBatch::new();
    batch.put(b"foo", b"bar");
    batch.put(b"baz", b"qux");
    batch.delete(b"foo");
    db.write(&WriteOptions::default(), batch).unwrap();
    assert!(db
      .get_with_options(&ReadOptions::default(), b"foo")
      .unwrap()
      .is_none());
    assert_eq!(
      db.get_with_options(&ReadOptions::default(), b"baz")
        .unwrap()
        .unwrap(),
      b"qux"
    );
  }
}
