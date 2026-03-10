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

use crate::coding::{crc32c, crc32c_extend, mask_crc, read_varu64, unmask_crc, write_varu64};
use crate::error::Error;
use crate::options::CompressionType;
use std::fs::File;
use std::os::unix::fs::FileExt;

// ── Constants ────────────────────────────────────────────────────────────────

/// Magic number written at the end of every SSTable footer.
/// See `table/format.h: kTableMagicNumber`.
pub(crate) const TABLE_MAGIC_NUMBER: u64 = 0xdb4775248b80fb57;

/// Block trailer: 1-byte compression type + 4-byte masked CRC32c.
pub(crate) const BLOCK_TRAILER_SIZE: usize = 5;

/// Maximum encoded size of a `BlockHandle` (two varint64s, up to 10 bytes each).
const MAX_ENCODED_HANDLE_LENGTH: usize = 20;

/// Encoded footer length (two handles padded to `MAX_ENCODED_HANDLE_LENGTH` each + 8-byte magic).
pub(crate) const FOOTER_ENCODED_LENGTH: usize = 2 * MAX_ENCODED_HANDLE_LENGTH + 8;

// ── Internal key helpers ─────────────────────────────────────────────────────

/// Construct an SSTable internal key: `user_key || tag` where
/// `tag = (seq << 8 | vtype as u64) as u64 LE`.
///
/// `vtype`: `1` = Value, `0` = Deletion — matches `memtable/entry.rs: ValueType`.
pub(crate) fn make_internal_key(user_key: &[u8], seq: u64, vtype: u8) -> Vec<u8> {
  let mut out = Vec::with_capacity(user_key.len() + 8);
  out.extend_from_slice(user_key);
  let tag = (seq << 8) | vtype as u64;
  out.extend_from_slice(&tag.to_le_bytes());
  out
}

/// Extract `(user_key, seq, vtype)` from an SSTable internal key.
///
/// Returns `None` if `ikey` is shorter than 8 bytes.
pub(crate) fn parse_internal_key(ikey: &[u8]) -> Option<(&[u8], u64, u8)> {
  if ikey.len() < 8 {
    return None;
  }
  let (user_key, tag_bytes) = ikey.split_at(ikey.len() - 8);
  let tag = u64::from_le_bytes(tag_bytes.try_into().unwrap());
  let seq = tag >> 8;
  let vtype = (tag & 0xff) as u8;
  Some((user_key, seq, vtype))
}

/// Compare two SSTable internal keys: user_key ASC, then sequence DESC
/// (higher sequence number sorts first, so a lookup finds the newest version).
pub(crate) fn cmp_internal_keys(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
  if a.len() < 8 || b.len() < 8 {
    // Malformed keys; fall back to byte comparison.
    return a.cmp(b);
  }
  let a_user = &a[..a.len() - 8];
  let b_user = &b[..b.len() - 8];
  let user_cmp = a_user.cmp(b_user);
  if user_cmp != std::cmp::Ordering::Equal {
    return user_cmp;
  }
  // Same user key — compare tags descending (higher tag = newer = "less" in sort order).
  let a_tag = u64::from_le_bytes(a[a.len() - 8..].try_into().unwrap());
  let b_tag = u64::from_le_bytes(b[b.len() - 8..].try_into().unwrap());
  b_tag.cmp(&a_tag)
}

// ── BlockHandle ───────────────────────────────────────────────────────────────

/// An (offset, size) pointer into an SSTable file, encoded as two varint64s.
/// See `table/format.h: BlockHandle`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BlockHandle {
  pub(crate) offset: u64,
  pub(crate) size: u64,
}

impl BlockHandle {
  /// Encode into `buf`, returning the number of bytes written (≤ 20).
  pub(crate) fn encode_to(&self, buf: &mut [u8]) -> usize {
    let mut tmp = [0u8; 10];
    let n1 = write_varu64(&mut tmp, self.offset);
    buf[..n1].copy_from_slice(&tmp[..n1]);
    let n2 = write_varu64(&mut tmp, self.size);
    buf[n1..n1 + n2].copy_from_slice(&tmp[..n2]);
    n1 + n2
  }

  /// Decode from `data`, returning `(handle, bytes_consumed)`.
  ///
  /// Returns `Err` if the varints are malformed.
  pub(crate) fn decode_from(data: &[u8]) -> Result<(Self, usize), Error> {
    let (offset, n1) = read_varu64(data);
    if n1 == 0 {
      return Err(Error::Corruption(
        "bad varint in BlockHandle offset".to_owned(),
      ));
    }
    let (size, n2) = read_varu64(&data[n1..]);
    if n2 == 0 {
      return Err(Error::Corruption(
        "bad varint in BlockHandle size".to_owned(),
      ));
    }
    Ok((BlockHandle { offset, size }, n1 + n2))
  }
}

// ── Footer ────────────────────────────────────────────────────────────────────

/// SSTable footer: metaindex handle + index handle + padding + magic.
/// Fixed encoded length: 48 bytes. See `table/format.h: Footer`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Footer {
  pub(crate) metaindex_handle: BlockHandle,
  pub(crate) index_handle: BlockHandle,
}

impl Footer {
  /// Encode to a 48-byte array.
  pub(crate) fn encode(&self) -> [u8; FOOTER_ENCODED_LENGTH] {
    let mut buf = [0u8; FOOTER_ENCODED_LENGTH];
    let n1 = self.metaindex_handle.encode_to(&mut buf);
    let n2 = self.index_handle.encode_to(&mut buf[n1..]);
    // Padding bytes [n1+n2 .. 40] remain zero.
    let magic_start = FOOTER_ENCODED_LENGTH - 8;
    buf[magic_start..].copy_from_slice(&TABLE_MAGIC_NUMBER.to_le_bytes());
    let _ = n2; // n1+n2 ≤ 40; padding fills the rest
    buf
  }

  /// Decode from a 48-byte slice.
  pub(crate) fn decode(data: &[u8; FOOTER_ENCODED_LENGTH]) -> Result<Self, Error> {
    let magic_start = FOOTER_ENCODED_LENGTH - 8;
    let magic = u64::from_le_bytes(data[magic_start..].try_into().unwrap());
    if magic != TABLE_MAGIC_NUMBER {
      return Err(Error::Corruption(format!(
        "SSTable magic mismatch: {magic:#018x}"
      )));
    }
    let (metaindex_handle, n1) = BlockHandle::decode_from(data)?;
    let (index_handle, _) = BlockHandle::decode_from(&data[n1..])?;
    Ok(Footer {
      metaindex_handle,
      index_handle,
    })
  }
}

// ── BlockContents ─────────────────────────────────────────────────────────────

/// The raw bytes of a block, minus the 5-byte trailer.
pub(crate) struct BlockContents {
  pub(crate) data: Vec<u8>,
}

// ── read_block ────────────────────────────────────────────────────────────────

/// Read and (optionally) verify a block from `file` at `handle`.
///
/// `verify_checksums`: when `true`, computes CRC32c over `[data + type_byte]`
/// and compares against the masked CRC stored in the trailer.
pub(crate) fn read_block(
  file: &File,
  handle: &BlockHandle,
  verify_checksums: bool,
) -> Result<BlockContents, Error> {
  let n = handle.size as usize;
  let mut buf = vec![0u8; n + BLOCK_TRAILER_SIZE];
  file.read_exact_at(&mut buf, handle.offset)?;

  if verify_checksums {
    let stored_masked = u32::from_le_bytes(buf[n + 1..n + 5].try_into().unwrap());
    let stored = unmask_crc(stored_masked);
    // CRC covers data bytes + type byte.
    let computed = crc32c_extend(crc32c(&buf[..n]), &buf[n..n + 1]);
    if computed != stored {
      return Err(Error::Corruption("block CRC mismatch".to_owned()));
    }
  }

  let compression_type = buf[n];
  match compression_type {
    0x00 => {
      // NoCompression — data is already in buf[..n].
      buf.truncate(n);
      Ok(BlockContents { data: buf })
    }
    0x01 => {
      // Snappy
      let decompressed = snap::raw::decompress_len(&buf[..n])
        .map_err(|e| Error::Corruption(format!("snappy length decode: {e}")))?;
      let mut out = vec![0u8; decompressed];
      snap::raw::Decoder::new()
        .decompress(&buf[..n], &mut out)
        .map_err(|e| Error::Corruption(format!("snappy decompress: {e}")))?;
      Ok(BlockContents { data: out })
    }
    0x02 => {
      // Zstd
      let out = zstd::bulk::decompress(&buf[..n], 16 * 1024 * 1024)
        .map_err(|e| Error::Corruption(format!("zstd decompress: {e}")))?;
      Ok(BlockContents { data: out })
    }
    _ => Err(Error::NotSupported(format!(
      "SSTable compression type {compression_type} not supported"
    ))),
  }
}

/// Write a block to `dest` (appends data + trailer), returning the `BlockHandle`.
///
/// `compression`: the algorithm to apply.  If the compressed output is not at least 12.5% smaller
/// than the raw data, the block is written uncompressed (matching LevelDB's heuristic).
///
/// The trailer is `[type: u8][masked_crc: u32 LE]`.
pub(crate) fn write_raw_block(
  dest: &mut impl std::io::Write,
  data: &[u8],
  offset: u64,
  compression: CompressionType,
  zstd_level: i32,
) -> Result<BlockHandle, Error> {
  let (block_data, compression_type_byte): (std::borrow::Cow<[u8]>, u8) = match compression {
    CompressionType::NoCompression => (std::borrow::Cow::Borrowed(data), 0x00),
    CompressionType::Snappy => {
      let mut enc = snap::raw::Encoder::new();
      let max_len = snap::raw::max_compress_len(data.len());
      let mut compressed = vec![0u8; max_len];
      let n = enc
        .compress(data, &mut compressed)
        .map_err(|e| Error::from(std::io::Error::other(e.to_string())))?;
      compressed.truncate(n);
      // Only use compressed form if it saves at least 12.5%.
      if n < data.len() - data.len() / 8 {
        (std::borrow::Cow::Owned(compressed), 0x01)
      } else {
        (std::borrow::Cow::Borrowed(data), 0x00)
      }
    }
    CompressionType::Zstd => {
      let range = zstd::compression_level_range();
      let level = zstd_level.clamp(*range.start(), *range.end());
      let compressed = zstd::bulk::compress(data, level)
        .map_err(|e| Error::from(std::io::Error::other(e.to_string())))?;
      if compressed.len() < data.len() - data.len() / 8 {
        (std::borrow::Cow::Owned(compressed), 0x02)
      } else {
        (std::borrow::Cow::Borrowed(data), 0x00)
      }
    }
  };

  let crc = mask_crc(crc32c_extend(crc32c(&block_data), &[compression_type_byte]));
  let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
  trailer[0] = compression_type_byte;
  trailer[1..].copy_from_slice(&crc.to_le_bytes());
  dest.write_all(&block_data)?;
  dest.write_all(&trailer)?;
  Ok(BlockHandle {
    offset,
    size: block_data.len() as u64,
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn internal_key_round_trip() {
    let key = b"hello";
    let seq = 42u64;
    let vtype = 1u8;
    let ikey = make_internal_key(key, seq, vtype);
    let (uk, s, vt) = parse_internal_key(&ikey).unwrap();
    assert_eq!(uk, key);
    assert_eq!(s, seq);
    assert_eq!(vt, vtype);
  }

  #[test]
  fn cmp_internal_keys_user_key_order() {
    let a = make_internal_key(b"apple", 1, 1);
    let b = make_internal_key(b"banana", 1, 1);
    assert_eq!(cmp_internal_keys(&a, &b), std::cmp::Ordering::Less);
  }

  #[test]
  fn cmp_internal_keys_seq_desc() {
    // Same user key: higher sequence sorts first (Less).
    let newer = make_internal_key(b"key", 10, 1);
    let older = make_internal_key(b"key", 5, 1);
    assert_eq!(cmp_internal_keys(&newer, &older), std::cmp::Ordering::Less);
  }

  #[test]
  fn block_handle_encode_decode() {
    let h = BlockHandle {
      offset: 12345,
      size: 678,
    };
    let mut buf = [0u8; 20];
    let n = h.encode_to(&mut buf);
    let (decoded, consumed) = BlockHandle::decode_from(&buf[..n]).unwrap();
    assert_eq!(consumed, n);
    assert_eq!(decoded.offset, h.offset);
    assert_eq!(decoded.size, h.size);
  }

  #[test]
  fn footer_encode_decode() {
    let f = Footer {
      metaindex_handle: BlockHandle { offset: 0, size: 0 },
      index_handle: BlockHandle {
        offset: 1024,
        size: 200,
      },
    };
    let encoded = f.encode();
    assert_eq!(encoded.len(), FOOTER_ENCODED_LENGTH);
    let decoded = Footer::decode(&encoded).unwrap();
    assert_eq!(decoded.index_handle.offset, 1024);
    assert_eq!(decoded.index_handle.size, 200);
  }

  #[test]
  fn footer_bad_magic() {
    let mut buf = [0u8; FOOTER_ENCODED_LENGTH];
    buf[40..48].copy_from_slice(&0xdeadbeef_u64.to_le_bytes());
    assert!(matches!(Footer::decode(&buf), Err(Error::Corruption(_))));
  }

  // ── Compression round-trip tests ─────────────────────────────────────────

  fn roundtrip_block(compression: crate::options::CompressionType) {
    // Use compressible data (repetitive string) so Snappy/Zstd actually compress it.
    let data: Vec<u8> = b"hello world! "
      .iter()
      .cycle()
      .take(1024)
      .copied()
      .collect();
    let mut buf: Vec<u8> = Vec::new();
    let handle = write_raw_block(&mut buf, &data, 0, compression, 1).unwrap();

    // Read it back using a temporary file (read_block requires a File).
    let mut tmp = tempfile::tempfile().unwrap();
    std::io::Write::write_all(&mut tmp, &buf).unwrap();
    let contents = read_block(&tmp, &handle, true).unwrap();
    assert_eq!(contents.data, data);
  }

  #[test]
  fn no_compression_roundtrip() {
    roundtrip_block(crate::options::CompressionType::NoCompression);
  }

  #[test]
  fn snappy_compression_roundtrip() {
    roundtrip_block(crate::options::CompressionType::Snappy);
  }

  #[test]
  fn zstd_compression_roundtrip() {
    roundtrip_block(crate::options::CompressionType::Zstd);
  }

  #[test]
  fn snappy_incompressible_falls_back_to_no_compression() {
    // Random-looking data: Snappy won't achieve 12.5% savings, so block is stored raw.
    let data: Vec<u8> = (0u8..=255).cycle().take(256).collect();
    let mut buf: Vec<u8> = Vec::new();
    let _ = write_raw_block(
      &mut buf,
      &data,
      0,
      crate::options::CompressionType::Snappy,
      1,
    )
    .unwrap();
    // Trailer byte should be 0x00 (NoCompression) because the data didn't compress well.
    let trailer_type = buf[buf.len() - BLOCK_TRAILER_SIZE];
    assert_eq!(trailer_type, 0x00, "expected fallback to NoCompression");
  }
}
