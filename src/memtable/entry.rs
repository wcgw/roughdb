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

#[cfg(test)]
use crate::memtable::arena::Arena;
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
  // ── Arena-allocating constructors (used by tests) ───────────────────────────

  #[cfg(test)]
  pub fn new_value(arena: &'a Arena, seq: u64, key: &[u8], value: &[u8]) -> Self {
    let size = Self::encoded_value_size(seq, key, value);
    let buffer = arena.allocate(size).expect("Allocation failed");
    Self::write_value_to(buffer, seq, key, value);
    Entry { data: buffer }
  }

  #[cfg(test)]
  pub fn new_deletion(arena: &'a Arena, seq: u64, key: &[u8]) -> Self {
    let size = Self::encoded_deletion_size(seq, key);
    let buffer = arena.allocate(size).expect("Allocation failed");
    Self::write_deletion_to(buffer, seq, key);
    Entry { data: buffer }
  }

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

  #[cfg(test)]
  fn vtype(&self) -> ValueType {
    let (klen, ksize) = read_varu64(self.data);
    let mut pos = ksize + klen as usize;
    let (_seq, seq_size) = read_varu64(&self.data[pos..]);
    pos += seq_size;
    match <u8 as TryInto<ValueType>>::try_into(self.data[pos]) {
      Ok(value_type) => value_type,
      Err(_) => panic!("Corruption! This needs handling... eventually!"),
    }
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

  #[cfg(test)]
  pub fn key_value(&self) -> (&[u8], Option<&[u8]>) {
    let (klen, mut pos) = read_varu64(self.data);
    let key = &self.data[pos..pos + klen as usize];
    pos += klen as usize;
    let (_seq, seq_size) = read_varu64(&self.data[pos..]);
    pos += seq_size;
    let vtype = match <u8 as TryInto<ValueType>>::try_into(self.data[pos]) {
      Ok(value_type) => value_type,
      Err(_) => panic!("Corruption! This needs handling... eventually!"),
    };
    pos += 1;
    let value = match vtype {
      ValueType::Deletion => None,
      _ => {
        let (val_len, val_size) = read_varu64(&self.data[pos..]);
        pos += val_size;
        let end = pos + val_len as usize;
        assert_eq!(self.data.len(), end);
        Some(&self.data[pos..end])
      }
    };
    (key, value)
  }

  #[cfg(test)]
  fn len(&self) -> usize {
    self.data.len()
  }
}

fn write_varu64(data: &mut [u8], mut n: u64) -> usize {
  let mut i = 0;
  while n >= 0b1000_0000 {
    data[i] = (n as u8) | 0b1000_0000;
    n >>= 7;
    i += 1;
  }
  data[i] = n as u8;
  i + 1
}

fn read_varu64(data: &[u8]) -> (u64, usize) {
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

#[cfg(test)]
mod tests {
  use super::*;
  use std::mem::size_of;

  #[test]
  fn new_value_is_value() {
    let arena = Arena::default();
    let entry = Entry::new_value(&arena, u64::MAX, b"Foo", b"Bar");
    let (klen, ksize) = read_varu64(entry.data);
    let (_seq, ssize) = read_varu64(&entry.data[ksize + klen as usize..]);
    let value = entry.data[ksize + klen as usize + ssize];
    assert_eq!(
      ValueType::Value as u8,
      (match <u8 as TryInto<ValueType>>::try_into(value) {
        Ok(value_type) => value_type,
        Err(_) => panic!("Corruption! This needs handling... eventually!"),
      }) as u8
    );
  }

  #[test]
  fn sequence_id() {
    let arena = Arena::default();
    let entry = Entry::new_value(&arena, u64::MIN, b"Foo", b"Bar");
    assert_eq!(u64::MIN, entry.sequence_id());
    let entry = Entry::new_value(&arena, u64::MAX, b"Foo", b"Bar");
    assert_eq!(u64::MAX, entry.sequence_id());
  }

  #[test]
  fn saves_key() {
    let arena = Arena::default();
    let entry = Entry::new_value(&arena, u64::MIN, b"Foo", b"Bar");
    assert_eq!(b"Foo", entry.key());
  }

  #[test]
  fn key_value() {
    let arena = Arena::default();
    let entry = Entry::new_value(&arena, u64::MAX, b"Foo", b"Bar");
    let (key, value) = entry.key_value();
    assert_eq!(b"Foo", key);
    assert_eq!(b"Bar", value.unwrap());
  }

  #[test]
  fn saves_value() {
    let arena = Arena::default();
    let entry = Entry::new_value(&arena, 2, b"Foo", b"Bar");
    assert_eq!(b"Bar", entry.value().unwrap());
  }

  #[test]
  fn new_deletion_is_deletion() {
    let arena = Arena::default();
    let entry = Entry::new_deletion(&arena, 3, b"Foo");
    assert_eq!(ValueType::Deletion, entry.vtype());
  }

  #[test]
  fn deletion_value_is_none() {
    let arena = Arena::default();
    let entry = Entry::new_deletion(&arena, 4, b"Foo");
    assert!(entry.value().is_none());
  }

  #[test]
  fn entries_with_same_key_are_equal() {
    let arena = Arena::default();
    let entry = Entry::new_value(&arena, 5, b"fizz", b"Bar");
    let other = Entry::new_value(&arena, 5, b"fizz", b"Foo");
    assert_eq!(entry, other);
  }

  #[test]
  fn entries_with_different_keys_are_not_equal() {
    let arena = Arena::default();
    let entry = Entry::new_value(&arena, 7, b"fizz", b"Bar");
    let noway = Entry::new_value(&arena, 8, b"buzz", b"Bar");
    assert_ne!(noway, entry);
  }

  #[test]
  fn size_of_entry() {
    assert_eq!(size_of::<&[u8]>(), size_of::<Entry>());
  }

  #[test]
  fn value_len() {
    let arena = Arena::default();
    {
      let key = b"bar";
      let value = b"fizz";
      let entry = Entry::new_value(&arena, 42, key, value);
      assert_eq!(1 + 3 + 1 + 1 + 1 + 4, entry.len());
      assert_eq!(entry.len(), entry.data.len());
    }

    {
      let key = b"This is a very long key, that will be > 127 and klen will then be 2 bytes long\
         This is a very long key, that will be > 127 and klen will then be 2 bytes long\
         ";
      assert!(key.len() > 127);
      let value = b"fizz";
      let entry = Entry::new_value(&arena, 42, key, value);
      assert_eq!(key, entry.key());
      assert_eq!(value, entry.value().unwrap());
      assert_eq!(42, entry.sequence_id());
      assert_eq!(2 + key.len() + 1 + 1 + 1 + value.len(), entry.len());
      assert_eq!(entry.len(), entry.data.len());
    }
  }

  #[test]
  fn deletion_len() {
    let arena = Arena::default();
    let key = b"Bar";
    let entry = Entry::new_deletion(&arena, 42, key);
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

  #[test]
  fn as_byte() {
    assert_eq!(0, ValueType::Deletion as u8);
    assert_eq!(1, ValueType::Value as u8);
  }

  #[test]
  fn from_byte() {
    assert_eq!(Ok(ValueType::Deletion), 0u8.try_into());
    assert_eq!(Ok(ValueType::Value), 1u8.try_into());
    assert!(<u8 as TryInto<ValueType>>::try_into(2u8).is_err());
  }
  #[test]
  fn size_of_vtype() {
    assert_eq!(1, size_of::<ValueType>());
  }
}
