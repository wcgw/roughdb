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

//! Memtable entry encoding.
//!
//! ```text
//! [klen: varint][key: klen bytes][seq: varint][vtype: u8][vlen: varint][value: vlen bytes]
//! ```
//!
//! A deletion tombstone stops after `vtype`.  Entries are written straight
//! into arena memory by the `write_*` helpers and read back by the raw
//! decoders, which trust the encoding (the memtable never holds bytes that
//! did not come from these writers).

use crate::coding::{read_varu64, write_varu64};
use std::fmt::Debug;
use std::fmt::Formatter;
use std::slice;

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

/// A decoded entry: borrowed views into the arena bytes it was read from.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DecodedEntry<'a> {
  pub key: &'a [u8],
  pub seq: u64,
  /// `None` for a deletion tombstone.
  pub value: Option<&'a [u8]>,
}

pub struct Entry<'a> {
  data: &'a [u8],
}

impl<'a> Entry<'a> {
  // ── Allocation-free encoding helpers (used by the SkipList path) ────────────

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

  // ── Raw decoders (the skip-list hot path) ───────────────────────────────────
  //
  // Nodes do not record their payload length (matching RocksDB's
  // InlineSkipList), so these decode straight from a pointer, trusting the
  // encoding produced by the writers above.

  /// Decode only the user key of the entry at `p`, returning it together
  /// with a pointer to the encoded sequence number that follows it — for
  /// [`Entry::seq_raw`], so a comparison pays for the sequence number only
  /// when the user keys tie.
  ///
  /// # Safety
  /// `p` must point to the first byte of a complete entry written by
  /// [`Entry::write_value_to`] or [`Entry::write_deletion_to`], every byte of
  /// which is initialised and stays valid (unmodified, not freed) for `'b`.
  #[inline]
  pub(crate) unsafe fn key_raw<'b>(p: *const u8) -> (&'b [u8], *const u8) {
    // SAFETY: the entry starts with a complete `klen` varint (caller
    // contract).
    let (klen, ks) = unsafe { read_varu64_raw(p) };
    let klen = klen as usize;
    // SAFETY: `klen` key bytes follow the varint, and the entry continues
    // after them (caller contract), so both pointers stay inside it.
    unsafe { (slice::from_raw_parts(p.add(ks), klen), p.add(ks + klen)) }
  }

  /// Decode the sequence number at `p`.
  ///
  /// # Safety
  /// `p` must be the second value returned by [`Entry::key_raw`] for an
  /// entry that still satisfies that function's contract.
  #[inline]
  pub(crate) unsafe fn seq_raw(p: *const u8) -> u64 {
    // SAFETY: a complete `seq` varint follows the key (caller contract).
    unsafe { read_varu64_raw(p).0 }
  }

  /// Decode the user key and sequence number of the entry at `p`.
  ///
  /// # Safety
  /// Same contract as [`Entry::key_raw`].
  #[inline]
  pub(crate) unsafe fn decode_key_raw<'b>(p: *const u8) -> (&'b [u8], u64) {
    // SAFETY: forwarded from this function's contract; `q` is the pointer
    // `key_raw` hands out for `seq_raw`.
    unsafe {
      let (key, q) = Self::key_raw(p);
      (key, Self::seq_raw(q))
    }
  }

  /// Decode the whole entry at `p`.
  ///
  /// # Safety
  /// Same contract as [`Entry::decode_key_raw`].
  #[inline]
  pub(crate) unsafe fn decode_raw<'b>(p: *const u8) -> DecodedEntry<'b> {
    // SAFETY: every read below stays inside the entry the caller vouches
    // for: `klen` varint, `klen` key bytes, `seq` varint, one `vtype` byte,
    // and — for a value entry only — a `vlen` varint followed by `vlen`
    // value bytes.  The encoding is fixed by `write_value_to` /
    // `write_deletion_to`.
    unsafe {
      let (klen, ks) = read_varu64_raw(p);
      let klen = klen as usize;
      let key = slice::from_raw_parts(p.add(ks), klen);
      let mut q = p.add(ks + klen);
      let (seq, ss) = read_varu64_raw(q);
      q = q.add(ss);
      let vtype = *q;
      q = q.add(1);
      let value = if vtype == ValueType::Deletion as u8 {
        None
      } else {
        debug_assert_eq!(vtype, ValueType::Value as u8, "corrupt memtable entry");
        let (vlen, vs) = read_varu64_raw(q);
        Some(slice::from_raw_parts(q.add(vs), vlen as usize))
      };
      DecodedEntry { key, seq, value }
    }
  }

  // ── Slice-based reference decoder (tests only) ──────────────────────────────

  /// Create an [`Entry`] view over an already-encoded byte slice.
  #[cfg(test)]
  pub(crate) fn from_slice(data: &[u8]) -> Entry<'_> {
    Entry { data }
  }

  #[cfg(test)]
  pub fn sequence_id(&self) -> u64 {
    let (klen, ksize) = read_varu64(self.data);
    let pos = ksize + klen as usize;
    let (seq, _length) = read_varu64(&self.data[pos..]);
    seq
  }

  #[cfg(test)]
  pub fn key(&self) -> &'a [u8] {
    let (len, pos) = read_varu64(self.data);
    &self.data[pos..pos + len as usize]
  }

  #[cfg(test)]
  pub fn value(&self) -> Option<&'a [u8]> {
    let (klen, ksize) = read_varu64(self.data);
    let mut pos = ksize + klen as usize;
    let (_seq, seq_size) = read_varu64(&self.data[pos..]);
    pos += seq_size;
    let vtype = ValueType::try_from(self.data[pos]).expect("corrupt entry");
    pos += 1;
    match vtype {
      ValueType::Deletion => None,
      ValueType::Value => {
        let (val_len, val_size) = read_varu64(&self.data[pos..]);
        pos += val_size;
        let end = pos + val_len as usize;
        assert_eq!(self.data.len(), end);
        Some(&self.data[pos..end])
      }
    }
  }
}

/// Read a LEB128 `u64` starting at `p`, returning the value and the number of
/// bytes consumed.  One-byte values (the common case for key lengths) take the
/// fast path.
///
/// # Safety
/// `p` must point to a complete varint — at most 10 bytes, terminated by a
/// byte with the high bit clear — all of whose bytes are initialised.  The
/// writers in this module never produce anything else.
#[inline]
unsafe fn read_varu64_raw(p: *const u8) -> (u64, usize) {
  // SAFETY: the first byte of a varint always exists (caller contract).
  let b = unsafe { *p };
  if b < 0x80 {
    return (b as u64, 1);
  }
  let mut n = (b & 0x7f) as u64;
  let mut shift = 7;
  let mut i = 1;
  loop {
    debug_assert!(i < 10, "varint longer than 10 bytes");
    // SAFETY: the previous byte had its continuation bit set, so the varint
    // extends at least to byte `i` (caller contract).
    let b = unsafe { *p.add(i) };
    n |= ((b & 0x7f) as u64) << shift;
    i += 1;
    if b < 0x80 {
      return (n, i);
    }
    shift += 7;
  }
}

// NOTE: `Entry` deliberately has no `Ord`/`PartialOrd`/`Eq`/`PartialEq` impls.
// Memtable ordering is defined by the skip list, which compares user keys with
// the pluggable `Options::comparator` (see `SkipList::compare_node`), then
// sequence numbers descending.  A byte-wise `Ord` on `Entry` would silently
// disagree with a custom comparator, so ordering lives solely in the skip list.

impl Debug for Entry<'_> {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    let (len, pos) = read_varu64(self.data);
    write!(
      f,
      "memtable::Entry {{ key: {:?} }}",
      &self.data[pos..pos + len as usize]
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Cross-check the raw decoders against the bounds-checked slice decoder
  /// for a value entry.
  fn check_value(seq: u64, key: &[u8], value: &[u8]) {
    let size = Entry::encoded_value_size(seq, key, value);
    let mut buf = vec![0xAAu8; size];
    Entry::write_value_to(&mut buf, seq, key, value);

    let e = Entry::from_slice(&buf);
    assert_eq!(e.key(), key);
    assert_eq!(e.sequence_id(), seq);
    assert_eq!(e.value(), Some(value));

    // SAFETY: `buf` holds a complete entry written just above.
    let (k, s) = unsafe { Entry::decode_key_raw(buf.as_ptr()) };
    assert_eq!(k, key);
    assert_eq!(s, seq);
    // SAFETY: as above.
    let d = unsafe { Entry::decode_raw(buf.as_ptr()) };
    assert_eq!(d.key, key);
    assert_eq!(d.seq, seq);
    assert_eq!(d.value, Some(value));
  }

  fn check_deletion(seq: u64, key: &[u8]) {
    let size = Entry::encoded_deletion_size(seq, key);
    let mut buf = vec![0xAAu8; size];
    Entry::write_deletion_to(&mut buf, seq, key);

    let e = Entry::from_slice(&buf);
    assert_eq!(e.key(), key);
    assert_eq!(e.sequence_id(), seq);
    assert_eq!(e.value(), None);

    // SAFETY: `buf` holds a complete entry written just above.
    let d = unsafe { Entry::decode_raw(buf.as_ptr()) };
    assert_eq!(d.key, key);
    assert_eq!(d.seq, seq);
    assert_eq!(d.value, None);
  }

  #[test]
  fn raw_decoders_match_reference_decoder() {
    let long_key = vec![b'k'; 300]; // 2-byte klen varint
    let long_value = vec![b'v'; 20_000]; // 3-byte vlen varint
    for &seq in &[
      0u64,
      1,
      127,
      128,
      16_383,
      16_384,
      1 << 32,
      u64::MAX >> 8,
      u64::MAX,
    ] {
      check_value(seq, b"", b"");
      check_value(seq, b"k", b"");
      check_value(seq, b"key", b"value");
      check_value(seq, &long_key, b"v");
      check_value(seq, b"k", &long_value);
      check_value(seq, &long_key, &long_value);
      check_deletion(seq, b"");
      check_deletion(seq, b"gone");
      check_deletion(seq, &long_key);
    }
  }

  #[test]
  fn raw_varint_covers_all_lengths() {
    for shift in 0..64 {
      let v = 1u64 << shift;
      for n in [v - 1, v, v + 1] {
        let mut buf = [0u8; 10];
        let len = write_varu64(&mut buf, n);
        // SAFETY: `buf` holds a complete varint of `len` bytes.
        let (got, used) = unsafe { read_varu64_raw(buf.as_ptr()) };
        assert_eq!((got, used), (n, len), "n = {n}");
      }
    }
  }
}
