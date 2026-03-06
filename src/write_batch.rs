//    Copyright (c) 2023 The RoughDB Authors
//
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

use crate::coding::{read_u32_le, read_u64_le, read_varu64, write_u32_le, write_u64_le, write_varu64};
use crate::Error;

const HEADER_SIZE: usize = 12;
const TAG_VALUE: u8 = 0x01;
const TAG_DELETE: u8 = 0x00;

pub trait Handler {
  fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
  fn delete(&mut self, key: &[u8]) -> Result<(), Error>;
}

pub struct WriteBatch {
  rep: Vec<u8>,
}

impl WriteBatch {
  pub fn new() -> Self {
    WriteBatch { rep: vec![0u8; HEADER_SIZE] }
  }

  pub fn put(&mut self, key: &[u8], value: &[u8]) {
    self.rep.push(TAG_VALUE);
    let mut tmp = [0u8; 10];
    let n = write_varu64(&mut tmp, key.len() as u64);
    self.rep.extend_from_slice(&tmp[..n]);
    self.rep.extend_from_slice(key);
    let n = write_varu64(&mut tmp, value.len() as u64);
    self.rep.extend_from_slice(&tmp[..n]);
    self.rep.extend_from_slice(value);
    self.set_count(self.count() + 1);
  }

  pub fn delete(&mut self, key: &[u8]) {
    self.rep.push(TAG_DELETE);
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

  pub fn byte_size(&self) -> usize {
    self.rep.len()
  }

  pub fn iterate(&self, handler: &mut dyn Handler) -> Result<(), Error> {
    if self.rep.len() < HEADER_SIZE {
      return Err(Error::Corruption("WriteBatch too small".to_string()));
    }
    let expected = self.count();
    let mut pos = HEADER_SIZE;
    let mut seen: u32 = 0;
    while pos < self.rep.len() {
      let tag = self.rep[pos];
      pos += 1;
      match tag {
        TAG_VALUE => {
          let (klen, ksize) = read_varu64(&self.rep[pos..]);
          if ksize == 0 {
            return Err(Error::Corruption("bad varint in WriteBatch key length".to_string()));
          }
          pos += ksize;
          let kend = pos + klen as usize;
          if kend > self.rep.len() {
            return Err(Error::Corruption("WriteBatch key truncated".to_string()));
          }
          let key = &self.rep[pos..kend];
          pos = kend;
          let (vlen, vsize) = read_varu64(&self.rep[pos..]);
          if vsize == 0 {
            return Err(Error::Corruption("bad varint in WriteBatch value length".to_string()));
          }
          pos += vsize;
          let vend = pos + vlen as usize;
          if vend > self.rep.len() {
            return Err(Error::Corruption("WriteBatch value truncated".to_string()));
          }
          let value = &self.rep[pos..vend];
          pos = vend;
          handler.put(key, value)?;
        }
        TAG_DELETE => {
          let (klen, ksize) = read_varu64(&self.rep[pos..]);
          if ksize == 0 {
            return Err(Error::Corruption("bad varint in WriteBatch key length".to_string()));
          }
          pos += ksize;
          let kend = pos + klen as usize;
          if kend > self.rep.len() {
            return Err(Error::Corruption("WriteBatch key truncated".to_string()));
          }
          let key = &self.rep[pos..kend];
          pos = kend;
          handler.delete(key)?;
        }
        _ => {
          return Err(Error::Corruption(format!("unknown WriteBatch tag: {tag}")));
        }
      }
      seen += 1;
    }
    if seen != expected {
      return Err(Error::Corruption(format!(
        "WriteBatch count mismatch: header says {expected}, found {seen}"
      )));
    }
    Ok(())
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

  fn set_count(&mut self, n: u32) {
    write_u32_le((&mut self.rep[8..12]).try_into().unwrap(), n);
  }
}

impl Default for WriteBatch {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Db;

  struct Recording {
    ops: Vec<Op>,
  }

  #[derive(Debug, PartialEq)]
  enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
  }

  impl Handler for Recording {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
      self.ops.push(Op::Put(key.to_vec(), value.to_vec()));
      Ok(())
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
      self.ops.push(Op::Delete(key.to_vec()));
      Ok(())
    }
  }

  #[test]
  fn empty_batch() {
    let b = WriteBatch::new();
    assert_eq!(b.byte_size(), 12);
    assert_eq!(b.count(), 0);
    let mut r = Recording { ops: vec![] };
    assert!(b.iterate(&mut r).is_ok());
    assert!(r.ops.is_empty());
  }

  #[test]
  fn put_and_iterate() {
    let mut b = WriteBatch::new();
    b.put(b"hello", b"world");
    let mut r = Recording { ops: vec![] };
    b.iterate(&mut r).unwrap();
    assert_eq!(r.ops, vec![Op::Put(b"hello".to_vec(), b"world".to_vec())]);
  }

  #[test]
  fn delete_and_iterate() {
    let mut b = WriteBatch::new();
    b.delete(b"gone");
    let mut r = Recording { ops: vec![] };
    b.iterate(&mut r).unwrap();
    assert_eq!(r.ops, vec![Op::Delete(b"gone".to_vec())]);
  }

  #[test]
  fn mixed_iterate() {
    let mut b = WriteBatch::new();
    b.put(b"a", b"1");
    b.delete(b"b");
    b.put(b"c", b"3");
    let mut r = Recording { ops: vec![] };
    b.iterate(&mut r).unwrap();
    assert_eq!(
      r.ops,
      vec![
        Op::Put(b"a".to_vec(), b"1".to_vec()),
        Op::Delete(b"b".to_vec()),
        Op::Put(b"c".to_vec(), b"3".to_vec()),
      ]
    );
  }

  #[test]
  fn byte_size_grows() {
    let mut b = WriteBatch::new();
    let s0 = b.byte_size();
    b.put(b"k", b"v");
    let s1 = b.byte_size();
    b.delete(b"k");
    let s2 = b.byte_size();
    assert!(s1 > s0);
    assert!(s2 > s1);
  }

  #[test]
  fn clear_resets() {
    let mut b = WriteBatch::new();
    b.put(b"x", b"y");
    b.clear();
    assert_eq!(b.byte_size(), 12);
    assert_eq!(b.count(), 0);
    let mut r = Recording { ops: vec![] };
    b.iterate(&mut r).unwrap();
    assert!(r.ops.is_empty());
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
    let mut r = Recording { ops: vec![] };
    a.iterate(&mut r).unwrap();
    assert_eq!(
      r.ops,
      vec![
        Op::Put(b"a".to_vec(), b"1".to_vec()),
        Op::Put(b"b".to_vec(), b"2".to_vec()),
        Op::Delete(b"c".to_vec()),
      ]
    );
  }

  #[test]
  fn corrupt_truncated() {
    let mut b = WriteBatch::new();
    b.put(b"key", b"value");
    // truncate mid-record
    b.rep.truncate(b.rep.len() - 2);
    let mut r = Recording { ops: vec![] };
    assert!(matches!(b.iterate(&mut r), Err(Error::Corruption(_))));
  }

  #[test]
  fn corrupt_bad_tag() {
    let mut b = WriteBatch::new();
    b.put(b"k", b"v");
    // overwrite the tag byte with an invalid value
    b.rep[HEADER_SIZE] = 0xFF;
    let mut r = Recording { ops: vec![] };
    assert!(matches!(b.iterate(&mut r), Err(Error::Corruption(_))));
  }

  #[test]
  fn db_write_round_trip() {
    let db = Db::default();
    let mut batch = WriteBatch::new();
    batch.put(b"foo", b"bar");
    batch.put(b"baz", b"qux");
    batch.delete(b"foo");
    db.write(&batch).unwrap();
    assert!(db.get(b"foo").unwrap_err().is_not_found());
    assert_eq!(db.get(b"baz").unwrap(), b"qux");
  }
}
