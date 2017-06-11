use options::ValueType;

use std::cmp::Ord;
use std::cmp::Ordering;
use std::cmp::PartialEq;
use std::cmp::PartialOrd;
use std::cmp::Eq;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::str::from_utf8;
use std::vec::Vec;

pub struct Entry {
  data: Vec<u8>,
}

impl Entry {
  pub fn new_value(key: &str, value: &str) -> Entry {
    let klen = key.len();
    let mut kdata = [0; 9];
    let ksize = write_varu64(&mut kdata, klen as u64);
    let mut vec = Vec::with_capacity(1 + ksize + klen + value.len());
    vec.push(ValueType::Value as u8);
    for x in 0..ksize {
      vec.push(kdata[x])
    }
    vec.extend(key.as_bytes());
    vec.extend(value.as_bytes());
    Entry {
      data: vec,
    }
  }

  pub fn new_deletion(key: &str) -> Entry {
    let klen = key.len();
    let mut kdata = [0; 9];
    let ksize = write_varu64(&mut kdata, klen as u64);
    let mut vec = Vec::with_capacity(1 + ksize + klen);
    vec.push(ValueType::Deletion as u8);
    for x in 0..ksize {
      vec.push(kdata[x])
    }
    vec.extend(key.as_bytes());
    Entry {
      data: vec,
    }
  }
}

impl Entry {
  fn vtype(&self) -> ValueType {
    ValueType::from_raw(self.data[0])
  }

  fn key(&self) -> &str {
    let (klen, ksize) = read_varu64(&self.data[1..]);
    let header = ksize + 1;
    from_utf8(&self.data[header..((klen as usize) + header)]).unwrap()
  }

  pub fn value(&self) -> Option<&str> {
    if let ValueType::Deletion = self.vtype() {
      return Option::None;
    }
    let (klen, ksize) = read_varu64(&self.data[1..]);
    let header = ksize + 1;
    Option::Some(from_utf8(&self.data[(header + (klen as usize))..]).unwrap())
  }

  pub fn key_value(&self) -> (&str, Option<&str>) {
    let vtype = self.vtype();
    let (klen, ksize) = read_varu64(&self.data[1..]);
    let header = ksize + 1;
    let key = from_utf8(&self.data[header..((klen as usize) + header)]).unwrap();
    let value = match vtype {
      ValueType::Deletion => Option::None,
      _ => Option::Some(from_utf8(&self.data[(header + (klen as usize))..]).unwrap())
    };
    (key, value)
  }

  pub fn len(&self) -> usize {
    self.data.len()
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
    self.key().cmp(&other.key())
  }
}

impl PartialOrd for Entry {
  fn partial_cmp(&self, other: &Entry) -> Option<Ordering> {
    Some(self.cmp(&other))
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
    write!(f, "table::Entry {{ key: {} }}", self.key())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::mem::size_of;

  #[test]
  fn new_value_is_value() {
    let entry = Entry::new_value(&"Foo", &"Bar");
    assert_eq!(ValueType::Value as u8, entry.vtype() as u8);
  }

  #[test]
  fn saves_key() {
    let entry = Entry::new_value(&"Foo", &"Bar");
    assert_eq!("Foo", entry.key());
  }

  #[test]
  fn key_value() {
    let entry = Entry::new_value(&"Foo", &"Bar");
    let (key, value) = entry.key_value();
    assert_eq!("Foo", key);
    assert_eq!("Bar", value.unwrap());
  }


  #[test]
  fn saves_value() {
    let entry = Entry::new_value(&"Foo", &"Bar");
    assert_eq!("Bar", entry.value().unwrap());
  }

  #[test]
  fn new_deletion_is_deletion() {
    let entry = Entry::new_deletion(&"Foo");
    assert_eq!(ValueType::Deletion as u8, entry.vtype() as u8);
  }

  #[test]
  fn deletion_value_is_none() {
    let entry = Entry::new_deletion(&"Foo");
    assert!(entry.value().is_none());
  }

  #[test]
  fn key_supports_love() {
    let entry = Entry::new_value(&"💖", &"Bar");
    assert_eq!("💖", entry.key());
  }

  #[test]
  fn value_supports_love() {
    let entry = Entry::new_value(&"Bar", &"💖");
    assert_eq!("💖", entry.value().unwrap());
  }

  #[test]
  fn entries_with_same_key_are_equal() {
    let entry = Entry::new_value(&"💖", &"Bar");
    let other = Entry::new_value(&"💖", &"Foo");
    assert_eq!(entry, other);
  }

  #[test]
  fn entries_with_different_keys_are_not_equal() {
    let entry = Entry::new_value(&"💖", &"Bar");
    let noway = Entry::new_value(&"💀", &"Bar");
    assert_ne!(noway, entry);
  }

  #[test]
  fn size() {
    assert_eq!(size_of::<Vec<u8>>(), size_of::<Entry>());
  }

  #[test]
  fn value_len() {
    let key = &"This is a very long key, that will be > 127 and klen will then be 2 bytes long\
         This is a very long key, that will be > 127 and klen will then be 2 bytes long\
         ";
    assert!(key.len() > 127);
    let value = &"💖";
    let entry = Entry::new_value(key, value);
    assert_eq!(1 + 2 + key.len() + value.len(), entry.len());
  }

  #[test]
  fn deletion_len() {
    let key = &"Bar";
    let entry = Entry::new_deletion(key);
    assert_eq!(1 + 1 + key.len(), entry.len());
  }
}
