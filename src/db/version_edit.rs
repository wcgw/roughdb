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
use crate::error::Error;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

// LevelDB-compatible MANIFEST tag constants.
const TAG_COMPARATOR: u64 = 1;
const TAG_LOG_NUMBER: u64 = 2;
const TAG_NEXT_FILE_NUMBER: u64 = 3;
const TAG_LAST_SEQUENCE: u64 = 4;
const TAG_COMPACT_POINTER: u64 = 5;
const TAG_DELETED_FILE: u64 = 6;
const TAG_NEW_FILE: u64 = 7;
const TAG_PREV_LOG_NUMBER: u64 = 9;

/// Metadata for one SSTable file.
///
/// Open `Table` handles are managed by the [`TableCache`]; this struct carries
/// only the durable metadata that is encoded in the MANIFEST.
///
/// [`TableCache`]: super::table_cache::TableCache
pub(crate) struct FileMetaData {
  pub number: u64,
  pub file_size: u64,
  /// Smallest internal key stored in this file.
  pub smallest: Vec<u8>,
  /// Largest internal key stored in this file.
  pub largest: Vec<u8>,
  /// Remaining seek budget before this file is nominated for compaction.
  ///
  /// In-memory only — not encoded in the MANIFEST.  Initialised to
  /// `max(100, file_size / 16384)` matching LevelDB's heuristic (one seek
  /// costs ~16 KiB of I/O; after enough misses the overhead outweighs a
  /// compaction).  Decremented atomically on each `get` that probes the file
  /// without finding the target key; when it reaches ≤ 0 the file is
  /// flagged for seek-based compaction.
  pub allowed_seeks: AtomicI32,
}

impl FileMetaData {
  pub(crate) fn new(number: u64, file_size: u64, smallest: Vec<u8>, largest: Vec<u8>) -> Arc<Self> {
    let allowed_seeks = (file_size / 16384).max(100) as i32;
    Arc::new(Self {
      number,
      file_size,
      smallest,
      largest,
      allowed_seeks: AtomicI32::new(allowed_seeks),
    })
  }
}

/// An atomic delta to the set of live SSTable files.
///
/// Written to the MANIFEST log for crash-safe recovery. LevelDB-compatible
/// tag-based binary encoding (see `encode` / `decode`).
pub(crate) struct VersionEdit {
  pub log_number: Option<u64>,
  pub prev_log_number: Option<u64>,
  pub next_file_number: Option<u64>,
  pub last_sequence: Option<u64>,
  /// Files added: `(level, metadata)`.
  pub new_files: Vec<(i32, Arc<FileMetaData>)>,
  /// Files removed: `(level, file_number)`.
  pub deleted_files: Vec<(i32, u64)>,
  /// Compact pointers: `(level, largest_internal_key)` — round-robin compaction cursor.
  pub compact_pointers: Vec<(i32, Vec<u8>)>,
}

impl VersionEdit {
  pub(crate) fn new() -> Self {
    VersionEdit {
      log_number: None,
      prev_log_number: None,
      next_file_number: None,
      last_sequence: None,
      new_files: Vec::new(),
      deleted_files: Vec::new(),
      compact_pointers: Vec::new(),
    }
  }

  /// Serialise the edit to bytes for storage in the MANIFEST log.
  ///
  /// The `table` field of each `FileMetaData` is intentionally skipped;
  /// tables are re-opened from disk during recovery.
  pub(crate) fn encode(&self) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 10]; // maximum varint64 width

    macro_rules! push_varint {
      ($n:expr) => {{
        let len = write_varu64(&mut tmp, $n);
        buf.extend_from_slice(&tmp[..len]);
      }};
    }

    if let Some(v) = self.log_number {
      push_varint!(TAG_LOG_NUMBER);
      push_varint!(v);
    }
    if let Some(v) = self.prev_log_number {
      push_varint!(TAG_PREV_LOG_NUMBER);
      push_varint!(v);
    }
    if let Some(v) = self.next_file_number {
      push_varint!(TAG_NEXT_FILE_NUMBER);
      push_varint!(v);
    }
    if let Some(v) = self.last_sequence {
      push_varint!(TAG_LAST_SEQUENCE);
      push_varint!(v);
    }
    for (level, key) in &self.compact_pointers {
      push_varint!(TAG_COMPACT_POINTER);
      push_varint!(*level as u64);
      encode_bytes(&mut buf, key);
    }
    for &(level, number) in &self.deleted_files {
      push_varint!(TAG_DELETED_FILE);
      push_varint!(level as u64);
      push_varint!(number);
    }
    for (level, meta) in &self.new_files {
      push_varint!(TAG_NEW_FILE);
      push_varint!(*level as u64);
      push_varint!(meta.number);
      push_varint!(meta.file_size);
      encode_bytes(&mut buf, &meta.smallest);
      encode_bytes(&mut buf, &meta.largest);
    }
    buf
  }

  /// Parse a `VersionEdit` from `data`.
  ///
  /// Unknown tags are silently ignored (forward compatibility). Returns a
  /// `Corruption` error if a known field is truncated or otherwise malformed.
  pub(crate) fn decode(data: &[u8]) -> Result<Self, Error> {
    let mut edit = VersionEdit::new();
    let mut pos = 0;

    while pos < data.len() {
      let (tag, n) = read_varu64(&data[pos..]);
      if n == 0 {
        return Err(Error::Corruption(
          "VersionEdit: truncated tag varint".to_owned(),
        ));
      }
      pos += n;

      match tag {
        TAG_LOG_NUMBER => {
          let (v, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("log_number"));
          }
          pos += n;
          edit.log_number = Some(v);
        }
        TAG_PREV_LOG_NUMBER => {
          let (v, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("prev_log_number"));
          }
          pos += n;
          edit.prev_log_number = Some(v);
        }
        TAG_NEXT_FILE_NUMBER => {
          let (v, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("next_file_number"));
          }
          pos += n;
          edit.next_file_number = Some(v);
        }
        TAG_LAST_SEQUENCE => {
          let (v, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("last_sequence"));
          }
          pos += n;
          edit.last_sequence = Some(v);
        }
        TAG_DELETED_FILE => {
          let (level, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("deleted_file level"));
          }
          pos += n;
          let (number, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("deleted_file number"));
          }
          pos += n;
          edit.deleted_files.push((level as i32, number));
        }
        TAG_NEW_FILE => {
          let (level, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("new_file level"));
          }
          pos += n;
          let (number, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("new_file number"));
          }
          pos += n;
          let (file_size, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("new_file size"));
          }
          pos += n;
          let (smallest, n) = decode_bytes(&data[pos..])?;
          pos += n;
          let (largest, n) = decode_bytes(&data[pos..])?;
          pos += n;
          edit.new_files.push((
            level as i32,
            FileMetaData::new(number, file_size, smallest, largest),
          ));
        }
        TAG_COMPACT_POINTER => {
          let (level, n) = read_varu64(&data[pos..]);
          if n == 0 {
            return Err(trunc("compact_pointer level"));
          }
          pos += n;
          let (key, n) = decode_bytes(&data[pos..])?;
          pos += n;
          edit.compact_pointers.push((level as i32, key));
        }
        TAG_COMPARATOR => {
          // Skip comparator name (length-prefixed bytes).
          let (_, n) = decode_bytes(&data[pos..])?;
          pos += n;
        }
        _ => {
          // Unknown tag: stop parsing (can't skip without knowing field size).
          // Treat as end of meaningful data for forward compatibility.
          break;
        }
      }
    }

    Ok(edit)
  }
}

// ── encoding helpers ──────────────────────────────────────────────────────────

fn encode_bytes(buf: &mut Vec<u8>, data: &[u8]) {
  let mut tmp = [0u8; 10];
  let n = write_varu64(&mut tmp, data.len() as u64);
  buf.extend_from_slice(&tmp[..n]);
  buf.extend_from_slice(data);
}

fn decode_bytes(data: &[u8]) -> Result<(Vec<u8>, usize), Error> {
  let (len, n) = read_varu64(data);
  if n == 0 {
    return Err(Error::Corruption(
      "VersionEdit: truncated byte-length varint".to_owned(),
    ));
  }
  let end = n + len as usize;
  if end > data.len() {
    return Err(Error::Corruption(
      "VersionEdit: truncated byte-data".to_owned(),
    ));
  }
  Ok((data[n..end].to_vec(), end))
}

fn trunc(field: &str) -> Error {
  Error::Corruption(format!("VersionEdit: truncated {field}"))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn encode_decode_roundtrip() {
    let mut edit = VersionEdit::new();
    edit.log_number = Some(42);
    edit.prev_log_number = Some(41);
    edit.next_file_number = Some(100);
    edit.last_sequence = Some(999);
    edit.deleted_files.push((0, 5));
    edit.deleted_files.push((1, 10));
    edit.new_files.push((
      0,
      FileMetaData::new(
        7,
        1024,
        b"aaa\x01\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
        b"zzz\x01\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
      ),
    ));

    let encoded = edit.encode();
    let decoded = VersionEdit::decode(&encoded).unwrap();

    assert_eq!(decoded.log_number, Some(42));
    assert_eq!(decoded.prev_log_number, Some(41));
    assert_eq!(decoded.next_file_number, Some(100));
    assert_eq!(decoded.last_sequence, Some(999));
    assert_eq!(decoded.deleted_files, vec![(0, 5), (1, 10)]);
    assert_eq!(decoded.new_files.len(), 1);
    let (level, meta) = &decoded.new_files[0];
    assert_eq!(*level, 0);
    assert_eq!(meta.number, 7);
    assert_eq!(meta.file_size, 1024);
    assert_eq!(meta.smallest, b"aaa\x01\x00\x00\x00\x00\x00\x00\x00\x00");
    assert_eq!(meta.largest, b"zzz\x01\x00\x00\x00\x00\x00\x00\x00\x00");
  }

  #[test]
  fn encode_decode_empty() {
    let edit = VersionEdit::new();
    let encoded = edit.encode();
    assert!(encoded.is_empty());
    let decoded = VersionEdit::decode(&encoded).unwrap();
    assert!(decoded.log_number.is_none());
    assert!(decoded.new_files.is_empty());
    assert!(decoded.deleted_files.is_empty());
  }

  #[test]
  fn decode_ignores_unknown_tags() {
    // Build a valid edit, then append an unknown tag byte at the end.
    let mut edit = VersionEdit::new();
    edit.last_sequence = Some(5);
    let mut encoded = edit.encode();
    encoded.push(99); // unknown tag — no value follows; break stops parsing

    let decoded = VersionEdit::decode(&encoded).unwrap();
    assert_eq!(decoded.last_sequence, Some(5));
  }

  #[test]
  fn decode_truncated_returns_err() {
    // A tag byte for last_sequence followed by an incomplete varint.
    let bad: &[u8] = &[TAG_LAST_SEQUENCE as u8, 0x80]; // 0x80 = continuation bit set, no next byte
    let result = VersionEdit::decode(bad);
    assert!(result.is_err());
  }

  #[test]
  fn decode_truncated_new_file_bytes_returns_err() {
    // TAG_NEW_FILE followed by valid level + number + size, then a byte-length
    // varint that claims more bytes than are available.
    // Build the test payload manually (split write+slice to avoid borrow conflict).
    let mut buf = Vec::new();
    let mut tmp = [0u8; 10];
    macro_rules! push {
      ($n:expr) => {{
        let len = write_varu64(&mut tmp, $n);
        buf.extend_from_slice(&tmp[..len]);
      }};
    }
    push!(TAG_NEW_FILE);
    push!(0); // level
    push!(3); // number
    push!(100); // file_size
    push!(10); // smallest len = 10 (but only 5 bytes follow)
    buf.extend_from_slice(b"short"); // only 5 bytes, not 10
    let result = VersionEdit::decode(&buf);
    assert!(result.is_err());
  }
}
