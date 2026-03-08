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

use crate::coding::{crc32c, crc32c_extend, unmask_crc};
use crate::log::format::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// Receives corruption notifications from the [`Reader`].
///
/// See `db/log_reader.h: Reader::Reporter`.
pub(crate) trait Reporter {
  fn corruption(&mut self, bytes: u64, reason: &str);
}

/// WAL log reader. Reads sequentially from a `File`, reassembling fragmented
/// records and optionally verifying CRC32c checksums.
///
/// See `db/log_reader.h/cc`.
pub(crate) struct Reader {
  file: File,
  reporter: Option<Box<dyn Reporter>>,
  checksum: bool,
  /// Raw block buffer; data is valid in `backing[buf_start..buf_end]`.
  backing: Box<[u8; BLOCK_SIZE]>,
  buf_start: usize,
  buf_end: usize,
  eof: bool,
  /// File offset of the byte just past `backing[..buf_end]`.
  end_of_buffer_offset: u64,
  /// File offset of the last record returned by `read_record`.
  last_record_offset: u64,
  initial_offset: u64,
  /// True while skipping Middle/Last fragments after an initial-offset seek.
  resyncing: bool,
}

/// Internal result of reading one physical record from the block buffer.
enum PhysKind {
  Rec(RecordType),
  Eof,
  Bad,
}

struct PhysRecord {
  kind: PhysKind,
  /// Index into `self.backing` where the payload starts.
  data_start: usize,
  data_len: usize,
}

impl Reader {
  /// Create a reader over `file`.
  ///
  /// - `checksum`: verify CRC32c on every record.
  /// - `initial_offset`: skip all records whose physical start is before this
  ///   file offset.  Pass `0` to read from the beginning.
  pub(crate) fn new(
    file: File,
    reporter: Option<Box<dyn Reporter>>,
    checksum: bool,
    initial_offset: u64,
  ) -> Self {
    Self {
      file,
      reporter,
      checksum,
      backing: Box::new([0u8; BLOCK_SIZE]),
      buf_start: 0,
      buf_end: 0,
      eof: false,
      end_of_buffer_offset: 0,
      last_record_offset: 0,
      initial_offset,
      resyncing: initial_offset > 0,
    }
  }

  /// File offset of the last record returned by `read_record`.
  /// Undefined before the first successful call.
  #[cfg(test)]
  pub(crate) fn last_record_offset(&self) -> u64 {
    self.last_record_offset
  }

  /// Read the next logical record, reassembling fragments as needed.
  ///
  /// Returns `None` at end-of-file or on an unrecoverable error.  Incomplete
  /// trailing records caused by a crash are silently treated as EOF.
  pub(crate) fn read_record(&mut self) -> Option<Vec<u8>> {
    if self.last_record_offset < self.initial_offset && !self.skip_to_initial_block() {
      return None;
    }

    let mut scratch: Vec<u8> = Vec::new();
    let mut in_fragmented = false;
    let mut prospective_record_offset: u64 = 0;

    loop {
      let pr = self.read_physical_record();

      // Physical offset of this record's header: end of buffer minus
      // remaining bytes minus the header and payload we just consumed.
      let physical_record_offset = self.end_of_buffer_offset
        - (self.buf_end - self.buf_start) as u64
        - HEADER_SIZE as u64
        - pr.data_len as u64;

      // While resyncing after an initial-offset seek, skip Middle/Last
      // fragments that belong to a record that started before the seek point.
      if self.resyncing {
        match &pr.kind {
          PhysKind::Rec(RecordType::Middle) => continue,
          PhysKind::Rec(RecordType::Last) => {
            self.resyncing = false;
            continue;
          }
          _ => self.resyncing = false,
        }
      }

      match pr.kind {
        PhysKind::Rec(RecordType::Full) => {
          if in_fragmented && !scratch.is_empty() {
            self.report_corruption(scratch.len() as u64, "partial record without end(1)");
          }
          self.last_record_offset = physical_record_offset;
          return Some(self.backing[pr.data_start..pr.data_start + pr.data_len].to_vec());
        }

        PhysKind::Rec(RecordType::First) => {
          if in_fragmented && !scratch.is_empty() {
            self.report_corruption(scratch.len() as u64, "partial record without end(2)");
          }
          prospective_record_offset = physical_record_offset;
          scratch.clear();
          scratch.extend_from_slice(&self.backing[pr.data_start..pr.data_start + pr.data_len]);
          in_fragmented = true;
        }

        PhysKind::Rec(RecordType::Middle) => {
          if !in_fragmented {
            self.report_corruption(pr.data_len as u64, "missing start of fragmented record(1)");
          } else {
            scratch.extend_from_slice(&self.backing[pr.data_start..pr.data_start + pr.data_len]);
          }
        }

        PhysKind::Rec(RecordType::Last) => {
          if !in_fragmented {
            self.report_corruption(pr.data_len as u64, "missing start of fragmented record(2)");
          } else {
            scratch.extend_from_slice(&self.backing[pr.data_start..pr.data_start + pr.data_len]);
            self.last_record_offset = prospective_record_offset;
            return Some(scratch);
          }
        }

        PhysKind::Rec(RecordType::Zero) => {
          // Zero records are already filtered in read_physical_record.
        }

        PhysKind::Eof => {
          // An incomplete trailing record is a torn write, not corruption.
          scratch.clear();
          return None;
        }

        PhysKind::Bad => {
          if in_fragmented {
            self.report_corruption(scratch.len() as u64, "error in middle of record");
            in_fragmented = false;
            scratch.clear();
          }
        }
      }
    }
  }

  /// Seek the file to the first block that could contain `initial_offset`.
  fn skip_to_initial_block(&mut self) -> bool {
    let offset_in_block = (self.initial_offset % BLOCK_SIZE as u64) as usize;
    let mut block_start = self.initial_offset - offset_in_block as u64;
    // If the offset falls in the 6-byte trailer zone, skip to the next block.
    if offset_in_block > BLOCK_SIZE - 6 {
      block_start += BLOCK_SIZE as u64;
    }
    self.end_of_buffer_offset = block_start;
    if block_start > 0 {
      if let Err(e) = self.file.seek(SeekFrom::Start(block_start)) {
        self.report_corruption(block_start, &format!("initial seek failed: {e}"));
        return false;
      }
    }
    true
  }

  /// Read one physical record from the buffered block stream.
  ///
  /// Refills the block buffer from the file as needed.  Returns a
  /// `PhysRecord` describing the type, payload location in `self.backing`,
  /// and payload length.  `PhysKind::Bad` is used for records to skip
  /// without terminating iteration; `PhysKind::Eof` terminates it.
  fn read_physical_record(&mut self) -> PhysRecord {
    let bad = PhysRecord {
      kind: PhysKind::Bad,
      data_start: 0,
      data_len: 0,
    };
    let eof = PhysRecord {
      kind: PhysKind::Eof,
      data_start: 0,
      data_len: 0,
    };

    loop {
      if self.buf_end - self.buf_start < HEADER_SIZE {
        if self.eof {
          // Truncated header at EOF: torn write, not corruption.
          self.buf_start = self.buf_end;
          return eof;
        }
        // Refill: read the next block (loop to handle short reads).
        self.buf_start = 0;
        self.buf_end = 0;
        loop {
          match self.file.read(&mut self.backing[self.buf_end..BLOCK_SIZE]) {
            Ok(0) => {
              self.eof = true;
              break;
            }
            Ok(n) => {
              self.buf_end += n;
              self.end_of_buffer_offset += n as u64;
              if self.buf_end == BLOCK_SIZE {
                break;
              }
              // Partial read — try to fill the remainder.
            }
            Err(e) => {
              self.buf_start = self.buf_end;
              self.report_corruption(BLOCK_SIZE as u64, &format!("read error: {e}"));
              self.eof = true;
              return eof;
            }
          }
        }
        continue;
      }

      // Parse the 7-byte header.
      let hdr_start = self.buf_start;
      let hdr = &self.backing[hdr_start..hdr_start + HEADER_SIZE];
      let length = hdr[4] as usize | ((hdr[5] as usize) << 8);
      let rtype_byte = hdr[6];

      if HEADER_SIZE + length > self.buf_end - self.buf_start {
        let drop = (self.buf_end - self.buf_start) as u64;
        self.buf_start = self.buf_end;
        if self.eof {
          // Payload truncated at EOF: torn write.
          return eof;
        }
        self.report_corruption(drop, "bad record length");
        return bad;
      }

      // Silently skip zero-length Zero records (pre-allocated file regions).
      if rtype_byte == RecordType::Zero as u8 && length == 0 {
        self.buf_start = self.buf_end;
        return bad;
      }

      // CRC verification.
      if self.checksum {
        let stored = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let expected = unmask_crc(stored);
        let actual = crc32c_extend(
          crc32c(&[rtype_byte]),
          &self.backing[hdr_start + HEADER_SIZE..hdr_start + HEADER_SIZE + length],
        );
        if actual != expected {
          let drop = (self.buf_end - self.buf_start) as u64;
          self.buf_start = self.buf_end;
          self.report_corruption(drop, "checksum mismatch");
          return bad;
        }
      }

      let data_start = hdr_start + HEADER_SIZE;
      self.buf_start = hdr_start + HEADER_SIZE + length;

      // Skip physical records that started before initial_offset.
      let record_file_offset = self.end_of_buffer_offset
        - (self.buf_end - self.buf_start) as u64
        - HEADER_SIZE as u64
        - length as u64;
      if record_file_offset < self.initial_offset {
        return bad;
      }

      match RecordType::try_from(rtype_byte) {
        Ok(rtype) => {
          return PhysRecord {
            kind: PhysKind::Rec(rtype),
            data_start,
            data_len: length,
          }
        }
        Err(_) => {
          self.report_corruption(
            (HEADER_SIZE + length) as u64,
            &format!("unknown record type {rtype_byte}"),
          );
          return bad;
        }
      }
    }
  }

  fn report_corruption(&mut self, bytes: u64, reason: &str) {
    // Mirror LevelDB: only report if the drop starts at or after initial_offset.
    let buf_remaining = (self.buf_end - self.buf_start) as u64;
    let drop_start = self
      .end_of_buffer_offset
      .saturating_sub(buf_remaining)
      .saturating_sub(bytes);
    if drop_start >= self.initial_offset {
      if let Some(r) = self.reporter.as_mut() {
        r.corruption(bytes, reason);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::log::writer::Writer;
  use std::io::{Seek, SeekFrom, Write};

  struct CorruptionLog(Vec<(u64, String)>);

  impl Reporter for CorruptionLog {
    fn corruption(&mut self, bytes: u64, reason: &str) {
      self.0.push((bytes, reason.to_owned()));
    }
  }

  /// Write `records` to a temp file and return the file rewound to the start.
  fn write_records(records: &[&[u8]]) -> File {
    let file = tempfile::tempfile().unwrap();
    let mut writer = Writer::new(file.try_clone().unwrap(), 0);
    for r in records {
      writer.add_record(r).unwrap();
    }
    drop(writer);
    let mut f = file;
    f.seek(SeekFrom::Start(0)).unwrap();
    f
  }

  fn make_reader(file: File, checksum: bool, initial_offset: u64) -> Reader {
    Reader::new(file, None, checksum, initial_offset)
  }

  fn make_reader_with_reporter(
    file: File,
    checksum: bool,
    initial_offset: u64,
  ) -> (Reader, *mut CorruptionLog) {
    let log = Box::new(CorruptionLog(Vec::new()));
    let ptr = Box::as_ref(&log) as *const _ as *mut CorruptionLog;
    let reader = Reader::new(file, Some(log), checksum, initial_offset);
    (reader, ptr)
  }

  #[test]
  fn empty_record_round_trip() {
    let f = write_records(&[b""]);
    let mut r = make_reader(f, true, 0);
    assert_eq!(r.read_record().unwrap(), b"");
    assert!(r.read_record().is_none());
  }

  #[test]
  fn small_record_round_trip() {
    let f = write_records(&[b"hello"]);
    let mut r = make_reader(f, true, 0);
    assert_eq!(r.read_record().unwrap(), b"hello");
    assert!(r.read_record().is_none());
  }

  #[test]
  fn multiple_records_in_order() {
    let f = write_records(&[b"first", b"second", b"third"]);
    let mut r = make_reader(f, true, 0);
    assert_eq!(r.read_record().unwrap(), b"first");
    assert_eq!(r.read_record().unwrap(), b"second");
    assert_eq!(r.read_record().unwrap(), b"third");
    assert!(r.read_record().is_none());
  }

  #[test]
  fn fragmented_record_round_trip() {
    // Payload larger than one block forces fragmentation.
    let payload: Vec<u8> = (0u8..=255).cycle().take(BLOCK_SIZE * 2 + 100).collect();
    let f = write_records(&[&payload]);
    let mut r = make_reader(f, true, 0);
    assert_eq!(r.read_record().unwrap(), payload);
    assert!(r.read_record().is_none());
  }

  #[test]
  fn crc_mismatch_reported_and_block_dropped() {
    // On a CRC mismatch the reader drops the rest of the current block (the
    // `length` field itself may be corrupted, making any further parsing unsafe).
    let file = tempfile::tempfile().unwrap();
    {
      let mut w = Writer::new(file.try_clone().unwrap(), 0);
      w.add_record(b"good").unwrap();
      w.add_record(b"corrupted").unwrap();
    }
    // Corrupt a payload byte of the second record.
    // Offset: (HEADER_SIZE + 4) + HEADER_SIZE = 18.
    let mut f = file.try_clone().unwrap();
    f.seek(SeekFrom::Start(18)).unwrap();
    f.write_all(&[0xff]).unwrap();

    f.seek(SeekFrom::Start(0)).unwrap();
    let (mut r, log_ptr) = make_reader_with_reporter(f, true, 0);

    assert_eq!(r.read_record().unwrap(), b"good");
    // The corrupted record drops the rest of block 0; only EOF remains.
    assert!(r.read_record().is_none());

    // SAFETY: log_ptr is valid for the duration of this test.
    let log = unsafe { &*log_ptr };
    assert!(!log.0.is_empty(), "corruption should have been reported");
    assert!(log.0[0].1.contains("checksum mismatch"));
  }

  #[test]
  fn no_checksum_no_report_on_corrupt_data() {
    let file = tempfile::tempfile().unwrap();
    {
      let mut w = Writer::new(file.try_clone().unwrap(), 0);
      w.add_record(b"data").unwrap();
    }
    // Corrupt the payload.
    let mut f = file.try_clone().unwrap();
    f.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
    f.write_all(&[0xff, 0xff, 0xff, 0xff]).unwrap();

    f.seek(SeekFrom::Start(0)).unwrap();
    let (mut r, log_ptr) = make_reader_with_reporter(f, false, 0);
    // With checksum disabled the corrupted bytes are returned as-is.
    let rec = r.read_record().unwrap();
    assert_eq!(rec.len(), b"data".len());

    let log = unsafe { &*log_ptr };
    assert!(log.0.is_empty(), "no report expected when checksum=false");
  }

  #[test]
  fn torn_write_at_eof_returns_none() {
    let file = tempfile::tempfile().unwrap();
    {
      let mut w = Writer::new(file.try_clone().unwrap(), 0);
      w.add_record(b"complete").unwrap();
    }
    // Append a truncated header (< 7 bytes) to simulate a torn write.
    let mut f = file.try_clone().unwrap();
    f.seek(SeekFrom::End(0)).unwrap();
    f.write_all(&[0x00, 0x00, 0x00]).unwrap(); // 3 bytes — too short

    f.seek(SeekFrom::Start(0)).unwrap();
    let mut r = make_reader(f, true, 0);
    assert_eq!(r.read_record().unwrap(), b"complete");
    // The torn header must be silently ignored.
    assert!(r.read_record().is_none());
  }

  #[test]
  fn last_record_offset_tracks_correctly() {
    let f = write_records(&[b"a", b"bb", b"ccc"]);
    let mut r = make_reader(f, false, 0);
    r.read_record();
    let off0 = r.last_record_offset();
    r.read_record();
    let off1 = r.last_record_offset();
    r.read_record();
    let off2 = r.last_record_offset();
    // Offsets must be strictly increasing.
    assert!(off0 < off1, "{off0} < {off1}");
    assert!(off1 < off2, "{off1} < {off2}");
  }
}
