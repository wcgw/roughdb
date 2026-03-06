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

use crate::coding::{read_varu64, write_varu64};
use std::cmp::Eq;
use std::cmp::Ord;
use std::cmp::Ordering;
use std::cmp::PartialEq;
use std::cmp::PartialOrd;
use std::fmt::Debug;
use std::fmt::Formatter;

#[derive(Debug, PartialEq)]
enum ValueType {
  Deletion,
  Value,
}

impl TryFrom<u8> for ValueType {
  type Error = ();

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(ValueType::Deletion),
      1 => Ok(ValueType::Value),
      _ => Err(()),
    }
  }
}

pub struct Entry<'a> {
  data: &'a [u8],
}

impl<'a> Entry<'a> {
  // ── Allocation-free encoding helpers (used by the SkipList path) ────────────

  /// Create an [`Entry`] view over an already-encoded byte slice.
  /// The caller must ensure the slice contains a valid encoding.
  pub(crate) fn from_slice(data: &[u8]) -> Entry<'_> {
    Entry { data }
  }

  /// Byte count of the encoded form of a value entry.
  pub(crate) fn encoded_value_size(seq: u64, key: &[u8], value: &[u8]) -> usize {
    let mut tmp = [0u8; 10];
    let ksize = write_varu64(&mut tmp, key.len() as u64);
    let ssize = write_varu64(&mut tmp, seq);
    let vsize = write_varu64(&mut tmp, value.len() as u64);
    ksize + key.len() + ssize + 1 + vsize + value.len()
  }

  /// Encode a value entry into `buf` (which must be exactly
  /// [`encoded_value_size`] bytes long).
  pub(crate) fn write_value_to(buf: &mut [u8], seq: u64, key: &[u8], value: &[u8]) {
    let mut kd = [0u8; 10];
    let ks = write_varu64(&mut kd, key.len() as u64);
    let mut sd = [0u8; 10];
    let ss = write_varu64(&mut sd, seq);
    let mut vd = [0u8; 10];
    let vs = write_varu64(&mut vd, value.len() as u64);

    let mut pos = 0;
    buf[pos..pos + ks].copy_from_slice(&kd[..ks]);
    pos += ks;
    buf[pos..pos + key.len()].copy_from_slice(key);
    pos += key.len();
    buf[pos..pos + ss].copy_from_slice(&sd[..ss]);
    pos += ss;
    buf[pos] = ValueType::Value as u8;
    pos += 1;
    buf[pos..pos + vs].copy_from_slice(&vd[..vs]);
    pos += vs;
    buf[pos..pos + value.len()].copy_from_slice(value);
  }

  /// Byte count of the encoded form of a deletion tombstone.
  pub(crate) fn encoded_deletion_size(seq: u64, key: &[u8]) -> usize {
    let mut tmp = [0u8; 10];
    let ksize = write_varu64(&mut tmp, key.len() as u64);
    let ssize = write_varu64(&mut tmp, seq);
    ksize + key.len() + ssize + 1
  }

  /// Encode a deletion tombstone into `buf` (which must be exactly
  /// [`encoded_deletion_size`] bytes long).
  pub(crate) fn write_deletion_to(buf: &mut [u8], seq: u64, key: &[u8]) {
    let mut kd = [0u8; 10];
    let ks = write_varu64(&mut kd, key.len() as u64);
    let mut sd = [0u8; 10];
    let ss = write_varu64(&mut sd, seq);

    let mut pos = 0;
    buf[pos..pos + ks].copy_from_slice(&kd[..ks]);
    pos += ks;
    buf[pos..pos + key.len()].copy_from_slice(key);
    pos += key.len();
    buf[pos..pos + ss].copy_from_slice(&sd[..ss]);
    pos += ss;
    buf[pos] = ValueType::Deletion as u8;
  }

  /// Byte count of a lookup key for `key` (seq = [`u64::MAX`], no value).
  pub(crate) fn lookup_size(key: &[u8]) -> usize {
    let mut tmp = [0u8; 10];
    let ksize = write_varu64(&mut tmp, key.len() as u64);
    let ssize = write_varu64(&mut tmp, u64::MAX);
    ksize + key.len() + ssize
  }

  /// Encode a lookup key into `buf` (which must be at least
  /// [`lookup_size`] bytes long).  A lookup key encodes only
  /// `[klen | key | seq=MAX]`; it has no value-type or value bytes.
  pub(crate) fn write_lookup_to(buf: &mut [u8], key: &[u8]) {
    let mut kd = [0u8; 10];
    let ks = write_varu64(&mut kd, key.len() as u64);
    let mut sd = [0u8; 10];
    let ss = write_varu64(&mut sd, u64::MAX);

    let mut pos = 0;
    buf[pos..pos + ks].copy_from_slice(&kd[..ks]);
    pos += ks;
    buf[pos..pos + key.len()].copy_from_slice(key);
    pos += key.len();
    buf[pos..pos + ss].copy_from_slice(&sd[..ss]);
  }

  pub fn sequence_id(&self) -> u64 {
    let (klen, ksize) = read_varu64(self.data);
    let pos = ksize + klen as usize;
    let (seq, _length) = read_varu64(&self.data[pos..]);
    seq
  }

  pub fn key(&self) -> &[u8] {
    let (len, pos) = read_varu64(self.data);
    &self.data[pos..pos + len as usize]
  }

  pub fn value(&self) -> Option<&[u8]> {
    let (klen, ksize) = read_varu64(self.data);
    let mut pos = ksize + klen as usize;
    let (_seq, seq_size) = read_varu64(&self.data[pos..]);
    pos += seq_size;
    let vtype = match <u8 as TryInto<ValueType>>::try_into(self.data[pos]) {
      Ok(value_type) => value_type,
      Err(_) => panic!("Corruption! This needs handling... eventually!"),
    };
    pos += 1;
    match vtype {
      ValueType::Deletion => None,
      _ => {
        let (val_len, val_size) = read_varu64(&self.data[pos..]);
        pos += val_size;
        let end = pos + val_len as usize;
        assert_eq!(self.data.len(), end);
        Some(&self.data[pos..end])
      }
    }
  }
}


impl Ord for Entry<'_> {
  fn cmp(&self, other: &Self) -> Ordering {
    let ordering = self.key().cmp(other.key());
    match ordering {
      Ordering::Equal => other.sequence_id().cmp(&self.sequence_id()),
      _ => ordering,
    }
  }
}

impl PartialOrd for Entry<'_> {
  fn partial_cmp(&self, other: &Entry) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Eq for Entry<'_> {}

impl PartialEq for Entry<'_> {
  fn eq(&self, other: &Entry) -> bool {
    self.key() == other.key()
  }
}

impl Debug for Entry<'_> {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    write!(f, "memtable::Entry {{ key: {:?} }}", self.key())
  }
}
