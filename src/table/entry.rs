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

use crate::options::ValueType;
use std::cmp::Eq;
use std::cmp::Ord;
use std::cmp::Ordering;
use std::cmp::PartialEq;
use std::cmp::PartialOrd;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::vec::Vec;

pub struct Entry {
  data: Vec<u8>,
}

impl Entry {
  pub fn new_value(seq: u64, key: &[u8], value: &[u8]) -> Self {
    let mut kdata = [0; 20];
    let seq_size = write_varu64(&mut kdata, seq);
    let klen = key.len();
    let header_size = write_varu64(&mut kdata[seq_size..], klen as u64) + seq_size;

    let mut vec = Vec::with_capacity(header_size + klen + value.len() + 1);
    vec.push(ValueType::Value as u8);
    for b in kdata.iter().take(header_size) {
      vec.push(*b);
    }
    vec.extend(key);
    vec.extend(value);
    Entry { data: vec }
  }

  pub fn new_deletion(seq: u64, key: &[u8]) -> Self {
    let mut kdata = [0; 20];
    let seq_size = write_varu64(&mut kdata, seq);
    let klen = key.len();
    let header_size = write_varu64(&mut kdata[seq_size..], klen as u64) + seq_size;

    let mut vec = Vec::with_capacity(header_size + klen + 1);
    vec.push(ValueType::Deletion as u8);
    for b in kdata.iter().take(header_size) {
      vec.push(*b);
    }
    vec.extend(key);
    Entry { data: vec }
  }

  pub(crate) fn new_lookup_key(key: &[u8]) -> Self {
    Self::new_value(u64::MAX, key, b"")
  }

  pub fn vtype(&self) -> ValueType {
    let value = self.data[0];
    match <u8 as TryInto<ValueType>>::try_into(value) {
      Ok(value_type) => value_type,
      Err(_) => panic!("Corruption! This needs handling... eventually!"),
    }
  }

  pub fn sequence_id(&self) -> u64 {
    let (seq, _length) = read_varu64(&self.data[1..]);
    seq
  }

  pub fn key(&self) -> &[u8] {
    let (_, seq_size) = read_varu64(&self.data[1..]);
    let (klen, ksize) = read_varu64(&self.data[1 + seq_size..]);
    let header = 1 + seq_size + ksize;
    &self.data[header..((klen as usize) + header)]
  }

  pub fn value(&self) -> Option<&[u8]> {
    if let ValueType::Deletion = self.vtype() {
      return None;
    }
    let (_, seq_size) = read_varu64(&self.data[1..]);
    let (klen, ksize) = read_varu64(&self.data[1 + seq_size..]);
    let header = 1 + seq_size + ksize;
    Some(&self.data[(header + (klen as usize))..])
  }

  pub fn key_value(&self) -> (&[u8], Option<&[u8]>) {
    let vtype = self.vtype();
    let (_, seq_size) = read_varu64(&self.data[1..]);
    let (klen, ksize) = read_varu64(&self.data[1 + seq_size..]);
    let header = ksize + seq_size + 1;
    let key = &self.data[header..((klen as usize) + header)];
    let value = match vtype {
      ValueType::Deletion => None,
      _ => Some(&self.data[(header + (klen as usize))..]),
    };
    (key, value)
  }

  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }
}

pub fn write_varu64(data: &mut [u8], mut n: u64) -> usize {
  let mut i = 0;
  while n >= 0b1000_0000 {
    data[i] = (n as u8) | 0b1000_0000;
    n >>= 7;
    i += 1;
  }
  data[i] = n as u8;
  i + 1
}

pub fn read_varu64(data: &[u8]) -> (u64, usize) {
  let mut n: u64 = 0;
  let mut shift: u32 = 0;
  for (i, &b) in data.iter().enumerate() {
    if b < 0b1000_0000 {
      return match (b as u64).checked_shl(shift) {
        None => (0, 0),
        Some(b) => (n | b, i + 1),
      };
    }
    match ((b as u64) & 0b0111_1111).checked_shl(shift) {
      None => return (0, 0),
      Some(b) => n |= b,
    }
    shift += 7;
  }
  (0, 0)
}

impl Ord for Entry {
  fn cmp(&self, other: &Self) -> Ordering {
    let ordering = self.key().cmp(other.key());
    match ordering {
      Ordering::Equal => other.sequence_id().cmp(&self.sequence_id()),
      _ => ordering,
    }
  }
}

impl PartialOrd for Entry {
  fn partial_cmp(&self, other: &Entry) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Eq for Entry {}

impl PartialEq for Entry {
  fn eq(&self, other: &Entry) -> bool {
    self.key() == other.key()
  }
}

impl Debug for Entry {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "table::Entry {{ key: {:?} }}", self.key())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::mem::size_of;

  #[test]
  fn new_value_is_value() {
    let entry = Entry::new_value(u64::MAX, b"Foo", b"Bar");
    assert_eq!(ValueType::Value as u8, entry.vtype() as u8);
  }

  #[test]
  fn saves_key() {
    let entry = Entry::new_value(u64::MIN, b"Foo", b"Bar");
    assert_eq!(b"Foo", entry.key());
  }

  #[test]
  fn key_value() {
    let entry = Entry::new_value(u64::MAX, b"Foo", b"Bar");
    let (key, value) = entry.key_value();
    assert_eq!(b"Foo", key);
    assert_eq!(b"Bar", value.unwrap());
  }

  #[test]
  fn saves_value() {
    let entry = Entry::new_value(2, b"Foo", b"Bar");
    assert_eq!(b"Bar", entry.value().unwrap());
  }

  #[test]
  fn new_deletion_is_deletion() {
    let entry = Entry::new_deletion(3, b"Foo");
    assert_eq!(ValueType::Deletion as u8, entry.vtype() as u8);
  }

  #[test]
  fn deletion_value_is_none() {
    let entry = Entry::new_deletion(4, b"Foo");
    assert!(entry.value().is_none());
  }

  #[test]
  fn entries_with_same_key_are_equal() {
    let entry = Entry::new_value(5, b"fizz", b"Bar");
    let other = Entry::new_value(6, b"fizz", b"Foo");
    assert_eq!(entry, other);
  }

  #[test]
  fn entries_with_different_keys_are_not_equal() {
    let entry = Entry::new_value(7, b"fizz", b"Bar");
    let noway = Entry::new_value(8, b"buzz", b"Bar");
    assert_ne!(noway, entry);
  }

  #[test]
  fn size() {
    assert_eq!(size_of::<Vec<u8>>(), size_of::<Entry>());
  }

  #[test]
  fn value_len() {
    let key = b"This is a very long key, that will be > 127 and klen will then be 2 bytes long\
         This is a very long key, that will be > 127 and klen will then be 2 bytes long\
         ";
    assert!(key.len() > 127);
    let value = b"fizz";
    let entry = Entry::new_value(42, key, value);
    assert_eq!(1 + 1 + 2 + key.len() + value.len(), entry.len());
  }

  #[test]
  fn deletion_len() {
    let key = b"Bar";
    let entry = Entry::new_deletion(42, key);
    assert_eq!(1 + 1 + 1 + key.len(), entry.len());
  }

  #[test]
  fn u64var_encoding() {
    let mut data = [0; 18];
    assert_eq!(write_varu64(&mut data, 0), 1);
    assert_eq!(read_varu64(&data), (0, 1));
    assert_eq!(write_varu64(&mut data, 127), 1);
    assert_eq!(read_varu64(&data), (127, 1));
    assert_eq!(write_varu64(&mut data, 128), 2);
    assert_eq!(read_varu64(&data), (128, 2));
    assert_eq!(write_varu64(&mut data, u64::MAX), 10);
    assert_eq!(read_varu64(&data), (u64::MAX, 10));
  }
}
