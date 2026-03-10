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

pub(crate) fn write_varu64(data: &mut [u8], mut n: u64) -> usize {
  let mut i = 0;
  while n >= 0b1000_0000 {
    data[i] = (n as u8) | 0b1000_0000;
    n >>= 7;
    i += 1;
  }
  data[i] = n as u8;
  i + 1
}

pub(crate) fn read_varu64(data: &[u8]) -> (u64, usize) {
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

pub(crate) fn write_u64_le(buf: &mut [u8; 8], n: u64) {
  buf.copy_from_slice(&n.to_le_bytes());
}

pub(crate) fn read_u64_le(buf: &[u8; 8]) -> u64 {
  u64::from_le_bytes(*buf)
}

pub(crate) fn write_u32_le(buf: &mut [u8; 4], n: u32) {
  buf.copy_from_slice(&n.to_le_bytes());
}

pub(crate) fn read_u32_le(buf: &[u8; 4]) -> u32 {
  u32::from_le_bytes(*buf)
}

// ── CRC32c helpers ───────────────────────────────────────────────────────────
//
// LevelDB masks stored CRCs to guard against the possibility of a CRC value
// being misinterpreted as a length or type field if data is shifted.  The
// mask rotates the bits and adds a constant, making the transformation
// reversible but distinct from the identity.  See `util/crc32c.h`.

const CRC_MASK_DELTA: u32 = 0xa282_ead8;

/// Compute CRC32c over `data`, extending an initial value of 0.
pub(crate) fn crc32c(data: &[u8]) -> u32 {
  crc32c::crc32c(data)
}

/// Extend an existing CRC32c value with additional `data`.
pub(crate) fn crc32c_extend(crc: u32, data: &[u8]) -> u32 {
  crc32c::crc32c_append(crc, data)
}

/// Apply LevelDB's CRC mask before storing a checksum on disk.
pub(crate) fn mask_crc(crc: u32) -> u32 {
  (crc.rotate_right(15)).wrapping_add(CRC_MASK_DELTA)
}

/// Reverse [`mask_crc`] when verifying a stored checksum.
pub(crate) fn unmask_crc(masked: u32) -> u32 {
  masked.wrapping_sub(CRC_MASK_DELTA).rotate_left(15)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;

  // Port of LevelDB coding_test.cc: Fixed32
  #[test]
  fn fixed_u32_le_roundtrip() {
    let mut buf = Vec::<u8>::new();
    for v in 0u32..100_000 {
      let mut tmp = [0u8; 4];
      write_u32_le(&mut tmp, v);
      buf.extend_from_slice(&tmp);
    }
    for (i, chunk) in buf.chunks_exact(4).enumerate() {
      let actual = read_u32_le(chunk.try_into().unwrap());
      assert_eq!(actual, i as u32);
    }
  }

  // Port of LevelDB coding_test.cc: Fixed64
  #[test]
  fn fixed_u64_le_roundtrip() {
    let mut values: Vec<u64> = Vec::new();
    for power in 0..=63u32 {
      let v = 1u64 << power;
      values.push(v.wrapping_sub(1));
      values.push(v);
      values.push(v.wrapping_add(1));
    }

    let mut buf = Vec::<u8>::new();
    for &v in &values {
      let mut tmp = [0u8; 8];
      write_u64_le(&mut tmp, v);
      buf.extend_from_slice(&tmp);
    }
    for (i, chunk) in buf.chunks_exact(8).enumerate() {
      let actual = read_u64_le(chunk.try_into().unwrap());
      assert_eq!(actual, values[i]);
    }
  }

  // Port of LevelDB coding_test.cc: EncodingOutput — verifies little-endian layout
  #[test]
  fn encoding_output_is_little_endian() {
    let mut tmp = [0u8; 4];
    write_u32_le(&mut tmp, 0x04030201);
    assert_eq!(tmp, [0x01, 0x02, 0x03, 0x04]);

    let mut tmp = [0u8; 8];
    write_u64_le(&mut tmp, 0x0807060504030201);
    assert_eq!(tmp, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
  }

  // Port of LevelDB coding_test.cc: Varint32 — all (value-group, shift) combinations
  #[test]
  fn varu64_roundtrip_bit_combinations() {
    let mut buf = Vec::<u8>::new();
    let mut expected = Vec::<u64>::new();
    let mut tmp = [0u8; 10];

    for i in 0u32..(32 * 32) {
      let v = ((i / 32) as u64) << (i % 32);
      expected.push(v);
      let n = write_varu64(&mut tmp, v);
      buf.extend_from_slice(&tmp[..n]);
    }

    let mut pos = 0;
    for &exp in &expected {
      let (actual, n) = read_varu64(&buf[pos..]);
      assert_ne!(n, 0, "truncated at pos {pos}");
      assert_eq!(actual, exp);
      pos += n;
    }
    assert_eq!(pos, buf.len());
  }

  // Port of LevelDB coding_test.cc: Varint64 — 0, u64::MAX, and power-of-two edges
  #[test]
  fn varu64_roundtrip_special_values() {
    let mut values: Vec<u64> = vec![0, 100, u64::MAX, u64::MAX - 1];
    for k in 0..64u32 {
      let power = 1u64 << k;
      values.push(power);
      values.push(power.saturating_sub(1));
      values.push(power.saturating_add(1));
    }

    let mut buf = Vec::<u8>::new();
    let mut tmp = [0u8; 10];
    for &v in &values {
      let n = write_varu64(&mut tmp, v);
      buf.extend_from_slice(&tmp[..n]);
    }

    let mut pos = 0;
    for &exp in &values {
      let (actual, n) = read_varu64(&buf[pos..]);
      assert_ne!(n, 0, "failed at value {exp}");
      assert_eq!(actual, exp);
      pos += n;
    }
    assert_eq!(pos, buf.len());
  }

  // Port of LevelDB coding_test.cc: Varint64Overflow
  #[test]
  fn varu64_overflow_returns_zero() {
    // 10 bytes with continuation bit set, followed by a terminal byte.
    // The 10th continuation byte causes a shift of 70 >= 64 → overflow.
    let input: &[u8] = &[
      0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11,
    ];
    let (_, n) = read_varu64(input);
    assert_eq!(n, 0, "expected overflow to return n=0");
  }

  // Port of LevelDB coding_test.cc: Varint64Truncation
  #[test]
  fn varu64_truncation_fails_on_prefix() {
    let large = (1u64 << 63) | 100;
    let mut tmp = [0u8; 10];
    let len = write_varu64(&mut tmp, large);

    // Every strict prefix must fail.
    for prefix_len in 0..len {
      let (_, n) = read_varu64(&tmp[..prefix_len]);
      assert_eq!(n, 0, "prefix of len {prefix_len} should fail");
    }

    // The full encoding must succeed.
    let (v, n) = read_varu64(&tmp[..len]);
    assert_ne!(n, 0);
    assert_eq!(v, large);
  }

  // Port of LevelDB coding_test.cc: Strings — length-prefixed byte sequences
  #[test]
  fn length_prefixed_bytes_roundtrip() {
    // Encode several byte strings as [varint length][bytes].
    let strings: &[&[u8]] = &[b"", b"foo", b"bar", &[b'x'; 200]];
    let mut buf = Vec::<u8>::new();
    let mut tmp = [0u8; 10];
    for &s in strings {
      let n = write_varu64(&mut tmp, s.len() as u64);
      buf.extend_from_slice(&tmp[..n]);
      buf.extend_from_slice(s);
    }

    let mut pos = 0;
    for &expected in strings {
      let (len, n) = read_varu64(&buf[pos..]);
      assert_ne!(n, 0);
      pos += n;
      let decoded = &buf[pos..pos + len as usize];
      assert_eq!(decoded, expected);
      pos += len as usize;
    }
    assert_eq!(pos, buf.len());
  }

  // Bonus: mask_crc / unmask_crc are inverses for all sampled values.
  #[test]
  fn crc_mask_roundtrip() {
    for v in [0u32, 1, 0x1234_5678, 0xffff_ffff, 0xa5a5_a5a5] {
      assert_eq!(unmask_crc(mask_crc(v)), v);
    }
  }
}
