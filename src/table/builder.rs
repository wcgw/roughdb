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

use crate::error::Error;
use crate::filter::FilterPolicy;
use crate::table::block_builder::BlockBuilder;
use crate::table::filter_block::FilterBlockWriter;
use crate::table::format::{write_raw_block, BlockHandle, Footer, FOOTER_ENCODED_LENGTH};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

/// Builds an SSTable file from sorted key-value pairs.
///
/// Typical usage:
/// 1. Construct `TableBuilder::new(file, block_size, restart_interval, filter_policy)`.
/// 2. Call `add(key, value)` for each entry in key order.
/// 3. Call `finish()` to write index block and footer, or `abandon()` to discard.
///
/// See `include/leveldb/table_builder.h` and `table/table_builder.cc`.
pub(crate) struct TableBuilder {
  dest: BufWriter<File>,
  data_block: BlockBuilder,
  index_block: BlockBuilder,
  /// Offset of the next byte to be written (= current file size).
  offset: u64,
  num_entries: u64,
  block_size: usize,
  /// Key of the last entry added; used to construct index block separator keys.
  last_key: Vec<u8>,
  /// Handle of the most recently flushed data block (to be written into index block).
  pending_handle: Option<BlockHandle>,
  closed: bool,
  /// Filter writer — `Some` when a filter policy was configured.
  filter_writer: Option<FilterBlockWriter>,
  /// Name of the filter policy, for the metaindex key `"filter.<name>"`.
  filter_policy_name: Option<String>,
}

impl TableBuilder {
  /// Create a new builder wrapping `file`.
  ///
  /// - `block_size`: target uncompressed data block size in bytes (e.g. 4096).
  /// - `restart_interval`: restart points every N keys for data blocks.
  /// - `filter_policy`: optional filter policy; when `Some`, a filter block is
  ///   written and referenced in the metaindex block.
  pub(crate) fn new(
    file: File,
    block_size: usize,
    restart_interval: usize,
    filter_policy: Option<&Arc<dyn FilterPolicy>>,
  ) -> Self {
    let (filter_writer, filter_policy_name) = match filter_policy {
      Some(policy) => (
        Some(FilterBlockWriter::new(Arc::clone(policy))),
        Some(format!("filter.{}", policy.name())),
      ),
      None => (None, None),
    };
    Self {
      dest: BufWriter::new(file),
      // Data blocks use the configured restart interval.
      data_block: BlockBuilder::new(restart_interval),
      // Index block uses interval=1 so every key is a restart point (easy binary search).
      index_block: BlockBuilder::new(1),
      offset: 0,
      num_entries: 0,
      block_size,
      last_key: Vec::new(),
      pending_handle: None,
      closed: false,
      filter_writer,
      filter_policy_name,
    }
  }

  /// Append a key-value pair.  Keys must be supplied in ascending order.
  pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
    debug_assert!(!self.closed);

    // If the previous data block was flushed, emit its index entry now.
    // The separator key is just `last_key` (no FindShortestSeparator for Phase 3).
    if let Some(handle) = self.pending_handle.take() {
      let mut handle_enc = [0u8; 20];
      let n = handle.encode_to(&mut handle_enc);
      self.index_block.add(&self.last_key, &handle_enc[..n]);
    }

    // Add the USER KEY to the filter (not the full internal key).
    // LevelDB wraps the user's FilterPolicy with InternalFilterPolicy, which
    // strips the 8-byte sequence+type suffix before hashing.  We replicate
    // that behaviour here: the filter always stores and queries user keys.
    if let Some(fw) = self.filter_writer.as_mut() {
      let user_key = if key.len() >= 8 {
        &key[..key.len() - 8]
      } else {
        key
      };
      fw.add_key(user_key);
    }

    self.data_block.add(key, value);
    self.last_key.clear();
    self.last_key.extend_from_slice(key);
    self.num_entries += 1;

    // Flush the data block when it exceeds the target size.
    if self.data_block.current_size_estimate() >= self.block_size {
      self.flush_data_block()?;
    }

    Ok(())
  }

  /// Complete the SSTable: flush any remaining data, write the filter block
  /// (if configured), index block, metaindex block, and footer.
  /// Returns the total file size.
  pub(crate) fn finish(mut self) -> Result<u64, Error> {
    self.closed = true;

    // Flush the last (possibly partial) data block.
    if !self.data_block.is_empty() {
      self.flush_data_block()?;
    }

    // Emit the index entry for the last data block.
    if let Some(handle) = self.pending_handle.take() {
      let mut handle_enc = [0u8; 20];
      let n = handle.encode_to(&mut handle_enc);
      self.index_block.add(&self.last_key, &handle_enc[..n]);
    }

    // Write the filter block (if a policy was configured) and build the
    // metaindex block that points to it.  Otherwise write an empty metaindex.
    let metaindex_handle = if let (Some(fw), Some(filter_key)) =
      (self.filter_writer.take(), self.filter_policy_name.take())
    {
      // Finalise and write the filter block.
      let filter_data = fw.finish();
      let filter_handle = write_raw_block(&mut self.dest, &filter_data, self.offset)?;
      self.offset += filter_handle.size + 5;

      // Write the metaindex block with one entry: "filter.<name>" → BlockHandle.
      let mut meta = BlockBuilder::new(1);
      let mut handle_enc = [0u8; 20];
      let n = filter_handle.encode_to(&mut handle_enc);
      meta.add(filter_key.as_bytes(), &handle_enc[..n]);
      let meta_data = meta.finish().to_vec();
      let mh = write_raw_block(&mut self.dest, &meta_data, self.offset)?;
      self.offset += mh.size + 5;
      mh
    } else {
      // No filter — write an empty metaindex block.
      let mut empty_meta = BlockBuilder::new(1);
      let meta_data = empty_meta.finish().to_vec();
      let mh = write_raw_block(&mut self.dest, &meta_data, self.offset)?;
      self.offset += mh.size + 5;
      mh
    };

    // Write index block.
    let index_data = self.index_block.finish().to_vec();
    let index_handle = write_raw_block(&mut self.dest, &index_data, self.offset)?;
    self.offset += index_handle.size + 5;

    // Write footer.
    let footer = Footer {
      metaindex_handle,
      index_handle,
    };
    let footer_bytes = footer.encode();
    self.dest.write_all(&footer_bytes)?;
    self.offset += FOOTER_ENCODED_LENGTH as u64;

    self.dest.flush()?;
    Ok(self.offset)
  }

  /// Discard the in-progress SSTable without writing a footer.  The caller is
  /// responsible for deleting the underlying file.
  #[allow(dead_code)]
  pub(crate) fn abandon(mut self) {
    self.closed = true;
    // Intentionally does not flush — leave the file incomplete/unusable.
  }

  #[allow(dead_code)]
  pub(crate) fn num_entries(&self) -> u64 {
    self.num_entries
  }

  /// Current file size (bytes written so far).
  #[allow(dead_code)]
  pub(crate) fn file_size(&self) -> u64 {
    self.offset
  }

  // ── private ──────────────────────────────────────────────────────────────

  /// Flush `data_block` to disk, record its `BlockHandle` in `pending_handle`.
  fn flush_data_block(&mut self) -> Result<(), Error> {
    debug_assert!(!self.data_block.is_empty());
    let block_data = self.data_block.finish().to_vec();
    let handle = write_raw_block(&mut self.dest, &block_data, self.offset)?;
    self.offset += handle.size + 5; // data bytes + 5-byte trailer
    self.data_block.reset();
    self.pending_handle = Some(handle);
    // Notify the filter writer that a new data block boundary has been reached.
    // The next block will start at `self.offset`, so the filter for the current
    // interval is finalised at that boundary.
    if let Some(fw) = self.filter_writer.as_mut() {
      fw.start_block(self.offset);
    }
    self.dest.flush()?;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::table::format::make_internal_key;
  use crate::table::reader::{LookupResult as L, Table};

  /// Build a table from (user_key, value) pairs using internal keys with ascending sequence numbers.
  fn build_table(pairs: &[(&[u8], &[u8])]) -> (tempfile::NamedTempFile, u64) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut builder = TableBuilder::new(file, 4096, 16, None);
    for (seq, &(k, v)) in pairs.iter().enumerate() {
      let ikey = make_internal_key(k, seq as u64 + 1, 1);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    (tmp, size)
  }

  #[test]
  fn empty_table_finish() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let builder = TableBuilder::new(file, 4096, 16, None);
    let size = builder.finish().unwrap();
    // Should at least have metaindex block + trailer + index block + trailer + footer.
    assert!(size >= FOOTER_ENCODED_LENGTH as u64);
  }

  #[test]
  fn single_entry_round_trip() {
    let (tmp, size) = build_table(&[(b"hello", b"world")]);
    let file = tmp.reopen().unwrap();
    let table = Table::open(file, size, None).unwrap();
    assert!(matches!(table.get(b"hello", false).unwrap(), L::Value(v) if v == b"world"));
    assert!(matches!(
      table.get(b"missing", false).unwrap(),
      L::NotInTable
    ));
  }

  #[test]
  fn multiple_entries_round_trip() {
    let pairs: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")];
    let (tmp, size) = build_table(&pairs);
    let file = tmp.reopen().unwrap();
    let table = Table::open(file, size, None).unwrap();
    for (k, v) in &pairs {
      assert!(matches!(table.get(k, false).unwrap(), L::Value(ref val) if val == v));
    }
  }

  #[test]
  fn many_entries_spanning_blocks() {
    // Use tiny block_size to force multiple data blocks.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0u32..200)
      .map(|i| {
        (
          format!("key{i:05}").into_bytes(),
          format!("val{i:05}").into_bytes(),
        )
      })
      .collect();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut builder = TableBuilder::new(file, 64, 4, None);
    for (seq, (k, v)) in pairs.iter().enumerate() {
      let ikey = make_internal_key(k, seq as u64 + 1, 1);
      builder.add(&ikey, v).unwrap();
    }
    let size = builder.finish().unwrap();
    let file = tmp.reopen().unwrap();
    let table = Table::open(file, size, None).unwrap();
    for (k, v) in &pairs {
      assert!(matches!(table.get(k, false).unwrap(), L::Value(ref val) if val == v));
    }
  }
}
