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

/// Log file block size: 32 KiB. See `db/log_format.h: kBlockSize`.
pub(crate) const BLOCK_SIZE: usize = 32768;

/// Record header size in bytes: 4 (crc32c) + 2 (length) + 1 (type).
/// See `db/log_format.h: kHeaderSize`.
pub(crate) const HEADER_SIZE: usize = 7;

/// Fragment type tag stored in the one-byte type field of every record header.
///
/// Layout of a record on disk:
/// ```text
/// [crc32c: u32 LE][length: u16 LE][type: u8][data: u8 * length]
/// ```
/// The CRC covers the type byte and all data bytes.
///
/// A record never starts in the last six bytes of a block; those bytes are
/// zero-padded and skipped by the reader. See `db/log_format.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RecordType {
  /// Reserved for pre-allocated / zero-padded trailer bytes.
  Zero = 0,
  /// Record fits entirely within one block.
  Full = 1,
  /// First fragment of a record that spans multiple blocks.
  First = 2,
  /// Interior fragment of a multi-block record.
  Middle = 3,
  /// Final fragment of a multi-block record.
  Last = 4,
}

impl TryFrom<u8> for RecordType {
  type Error = ();

  fn try_from(b: u8) -> Result<Self, ()> {
    match b {
      0 => Ok(Self::Zero),
      1 => Ok(Self::Full),
      2 => Ok(Self::First),
      3 => Ok(Self::Middle),
      4 => Ok(Self::Last),
      _ => Err(()),
    }
  }
}
