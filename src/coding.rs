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
