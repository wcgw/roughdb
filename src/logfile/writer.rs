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

use crate::coding::{crc32c, crc32c_extend, mask_crc};
use crate::error::Error;
use crate::logfile::format::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use std::fs::File;
use std::io::{BufWriter, Write};

/// WAL log writer. Wraps a `BufWriter<File>` and fragments records into 32 KiB
/// blocks, each with a 7-byte header: `[crc32c: u32 LE][len: u16 LE][type: u8]`.
///
/// See `db/log_writer.h/cc`.
pub(crate) struct Writer {
  dest: BufWriter<File>,
  /// Byte offset within the current block (0 .. BLOCK_SIZE).
  block_offset: usize,
  /// Pre-computed CRC32c of each record-type byte (index = RecordType value).
  /// Avoids recomputing the type contribution for every record emitted.
  type_crc: [u32; RecordType::Last as usize + 1],
}

impl Writer {
  /// Create a writer that appends to `file`.
  ///
  /// `dest_length` is the current byte length of the file; it is used to
  /// resume appending to an existing log without re-reading it.  Pass `0`
  /// for a freshly created file.
  pub(crate) fn new(file: File, dest_length: u64) -> Self {
    let mut type_crc = [0u32; RecordType::Last as usize + 1];
    for (i, slot) in type_crc.iter_mut().enumerate() {
      *slot = crc32c(&[i as u8]);
    }
    Self {
      dest: BufWriter::new(file),
      block_offset: (dest_length % BLOCK_SIZE as u64) as usize,
      type_crc,
    }
  }

  /// Append a record to the log, fragmenting it across block boundaries as
  /// needed.  An empty slice emits a single zero-length `Full` record.
  pub(crate) fn add_record(&mut self, data: &[u8]) -> Result<(), Error> {
    let mut remaining = data;
    let mut begin = true;

    loop {
      let leftover = BLOCK_SIZE - self.block_offset;

      // A header never starts in the last six bytes of a block; pad with zeros
      // and switch to the next block.
      if leftover < HEADER_SIZE {
        if leftover > 0 {
          self.dest.write_all(&[0u8; HEADER_SIZE - 1][..leftover])?;
        }
        self.block_offset = 0;
      }

      let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
      let fragment_len = remaining.len().min(avail);
      let end = fragment_len == remaining.len();

      let record_type = match (begin, end) {
        (true, true) => RecordType::Full,
        (true, false) => RecordType::First,
        (false, false) => RecordType::Middle,
        (false, true) => RecordType::Last,
      };

      self.emit_physical_record(record_type, &remaining[..fragment_len])?;
      remaining = &remaining[fragment_len..];
      begin = false;

      if end {
        return Ok(());
      }
    }
  }

  /// Flush OS buffers and fsync the underlying file.  Called when
  /// `WriteOptions::sync` is set.
  pub(crate) fn sync(&mut self) -> Result<(), Error> {
    self.dest.flush()?;
    self.dest.get_ref().sync_all().map_err(Error::from)
  }

  /// Write one physical record (header + payload) to the underlying file and
  /// flush the `BufWriter`.  Advances `block_offset` by `HEADER_SIZE + length`.
  fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<(), Error> {
    let length = data.len();
    debug_assert!(length <= 0xffff, "fragment too large for u16 length field");
    debug_assert!(self.block_offset + HEADER_SIZE + length <= BLOCK_SIZE);

    // CRC covers the type byte (pre-computed) extended with the payload.
    let crc = mask_crc(crc32c_extend(self.type_crc[record_type as usize], data));

    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&crc.to_le_bytes());
    header[4] = (length & 0xff) as u8;
    header[5] = (length >> 8) as u8;
    header[6] = record_type as u8;

    self.dest.write_all(&header)?;
    self.dest.write_all(data)?;
    self.dest.flush()?;

    self.block_offset += HEADER_SIZE + length;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::logfile::format::{RecordType, BLOCK_SIZE, HEADER_SIZE};
  use std::io::{Read, Seek, SeekFrom};

  /// Write records to a temp file via Writer, then read back the raw bytes for
  /// inspection.
  fn write_and_read(records: &[&[u8]]) -> Vec<u8> {
    let file = tempfile::tempfile().unwrap();
    let mut writer = Writer::new(file.try_clone().unwrap(), 0);
    for rec in records {
      writer.add_record(rec).unwrap();
    }
    drop(writer);
    let mut f = file;
    f.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    buf
  }

  fn parse_header(buf: &[u8]) -> (u32, usize, RecordType) {
    let crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let len = buf[4] as usize | ((buf[5] as usize) << 8);
    let rtype = RecordType::try_from(buf[6]).unwrap();
    (crc, len, rtype)
  }

  #[test]
  fn empty_record() {
    let buf = write_and_read(&[b""]);
    assert_eq!(buf.len(), HEADER_SIZE);
    let (_, len, rtype) = parse_header(&buf);
    assert_eq!(len, 0);
    assert_eq!(rtype, RecordType::Full);
  }

  #[test]
  fn small_record() {
    let payload = b"hello";
    let buf = write_and_read(&[payload]);
    assert_eq!(buf.len(), HEADER_SIZE + payload.len());
    let (_, len, rtype) = parse_header(&buf);
    assert_eq!(len, payload.len());
    assert_eq!(rtype, RecordType::Full);
    assert_eq!(&buf[HEADER_SIZE..], payload);
  }

  #[test]
  fn crc_covers_type_and_data() {
    use crate::coding::{crc32c, crc32c_extend, mask_crc};
    let payload = b"crc-check";
    let buf = write_and_read(&[payload]);
    let (stored_crc, _, _) = parse_header(&buf);
    let expected = mask_crc(crc32c_extend(crc32c(&[RecordType::Full as u8]), payload));
    assert_eq!(stored_crc, expected);
  }

  #[test]
  fn record_spanning_blocks() {
    // A payload that exactly fills the rest of the first block (minus header)
    // plus one byte forces a First + Last split.
    let first_avail = BLOCK_SIZE - HEADER_SIZE;
    let payload = vec![0xabu8; first_avail + 1];
    let buf = write_and_read(&[&payload]);

    // First fragment: Full block — header at 0, data fills first_avail bytes.
    let (_, len0, type0) = parse_header(&buf[0..]);
    assert_eq!(type0, RecordType::First);
    assert_eq!(len0, first_avail);

    // Second fragment: starts at BLOCK_SIZE, one byte of data.
    let (_, len1, type1) = parse_header(&buf[BLOCK_SIZE..]);
    assert_eq!(type1, RecordType::Last);
    assert_eq!(len1, 1);
  }

  #[test]
  fn trailer_padding_when_six_bytes_remain() {
    // Leave exactly 6 bytes in the first block (< HEADER_SIZE), forcing a pad
    // then a Full record in the next block.
    let first_avail = BLOCK_SIZE - HEADER_SIZE - 6; // fills block up to 6-byte trailer
    let payload_a = vec![0u8; first_avail];
    let payload_b = b"next";
    let buf = write_and_read(&[&payload_a, payload_b]);

    // The 6-byte trailer must be zeros.
    let trailer_start = HEADER_SIZE + first_avail;
    assert_eq!(&buf[trailer_start..trailer_start + 6], &[0u8; 6]);

    // payload_b lives as a Full record at the start of the next block.
    let (_, len, rtype) = parse_header(&buf[BLOCK_SIZE..]);
    assert_eq!(rtype, RecordType::Full);
    assert_eq!(len, payload_b.len());
  }

  #[test]
  fn resume_existing_file() {
    // Write one record, then resume from the file's current length and write another.
    let file = tempfile::tempfile().unwrap();
    let payload_a = b"first";
    {
      let mut w = Writer::new(file.try_clone().unwrap(), 0);
      w.add_record(payload_a).unwrap();
    }
    let offset = (HEADER_SIZE + payload_a.len()) as u64;
    {
      let mut w = Writer::new(file.try_clone().unwrap(), offset);
      w.add_record(b"second").unwrap();
    }
    let mut f = file;
    f.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();

    // Both records must be readable at their expected offsets.
    let (_, len0, type0) = parse_header(&buf);
    assert_eq!(type0, RecordType::Full);
    assert_eq!(len0, payload_a.len());

    let (_, len1, type1) = parse_header(&buf[offset as usize..]);
    assert_eq!(type1, RecordType::Full);
    assert_eq!(len1, b"second".len());
  }
}
