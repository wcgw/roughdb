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

use crate::cache::BlockCache;
use crate::comparator::Comparator;
use crate::error::Error;
use crate::filter::FilterPolicy;
use crate::iter::InternalIterator;
use crate::table::block::Block;
use crate::table::filter_block::FilterBlockReader;
use crate::table::format::{read_block, BlockHandle, Footer, FOOTER_ENCODED_LENGTH};
use crate::table::two_level_iterator::TwoLevelIterator;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

/// Three-way result of a `Table::get` lookup.
///
/// Distinguishing tombstones from genuine misses is required so that `Db::get`
/// can stop searching older L0 files once it finds a deletion record.
pub(crate) enum LookupResult {
  /// The key was found and has this value.
  Value(Vec<u8>),
  /// The key has a deletion tombstone in this table.  The caller should
  /// return `NotFound` without consulting older files.
  Deleted,
  /// The key is not present in this table at all.  The caller may check
  /// older (lower-numbered) L0 files.
  NotInTable,
}

/// A random-access SSTable reader.
///
/// `Table::open` reads the footer, index block, and (optionally) the filter
/// block once; subsequent `get` calls use the in-memory index and filter to
/// locate data blocks, reading them from disk via `pread` (no seeking —
/// takes `&self`).  See `table/table.h/cc`.
pub(crate) struct Table {
  file: File,
  index_block: Block,
  /// Offset of the metaindex block within the file.  Used as the "end of data
  /// blocks" sentinel in `approximate_offset_of` — the same heuristic that
  /// LevelDB uses in `Table::ApproximateOffsetOf`.
  metaindex_offset: u64,
  /// Parsed filter block, present when the SSTable was written with a filter
  /// policy whose name matches the one in the metaindex.
  filter: Option<FilterBlockReader>,
  /// Unique ID assigned by the block cache; used as the high half of the cache key.
  cache_id: u64,
  /// Shared block cache, if configured via `Options::block_cache`.
  block_cache: Option<Arc<BlockCache>>,
  /// Comparator for user-key ordering.
  comparator: Arc<dyn Comparator>,
}

impl Table {
  /// Open an SSTable file of `file_size` bytes.
  ///
  /// Reads and validates the footer, then reads the index block and (if
  /// `filter_policy` is `Some` and the metaindex contains a matching filter
  /// block) the filter block into memory.
  pub(crate) fn open(
    file: File,
    file_size: u64,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
    block_cache: Option<Arc<BlockCache>>,
    comparator: Arc<dyn Comparator>,
  ) -> Result<Self, Error> {
    if file_size < FOOTER_ENCODED_LENGTH as u64 {
      return Err(Error::Corruption("SSTable file too small".to_owned()));
    }

    // Read and decode the footer.
    let footer_offset = file_size - FOOTER_ENCODED_LENGTH as u64;
    let mut footer_buf = [0u8; FOOTER_ENCODED_LENGTH];
    file.read_exact_at(&mut footer_buf, footer_offset)?;
    let footer = Footer::decode(&footer_buf)?;

    // Read the index block.
    let index_contents = read_block(&file, &footer.index_handle, false)?;
    let index_block = Block::new(index_contents.data, Arc::clone(&comparator));

    // Optionally read the filter block from the metaindex.
    let filter = filter_policy.and_then(|policy| {
      read_filter_block(&file, &footer.metaindex_handle, policy)
        .ok()
        .flatten()
    });

    // Claim a unique cache ID from the block cache (0 = no cache).
    let cache_id = block_cache.as_ref().map(|c| c.new_id()).unwrap_or(0);

    Ok(Table {
      file,
      index_block,
      metaindex_offset: footer.metaindex_handle.offset,
      filter,
      cache_id,
      block_cache,
      comparator,
    })
  }

  /// Read (or retrieve from cache) the data block at `handle`.
  ///
  /// - Cache hit: returns a clone of the cached `Block` without any I/O.
  /// - Cache miss: reads from disk, then inserts into the cache when `fill_cache` is `true`.
  /// - No cache: reads from disk unconditionally.
  fn read_data_block(
    &self,
    handle: &BlockHandle,
    verify: bool,
    fill_cache: bool,
  ) -> Result<Block, Error> {
    // Check the block cache first.
    if let Some(cache) = &self.block_cache {
      if let Some(block) = cache.get(self.cache_id, handle.offset) {
        return Ok(block);
      }
    }

    // Cache miss (or no cache): read from disk.
    let contents = read_block(&self.file, handle, verify)?;
    let block = Block::new(contents.data, Arc::clone(&self.comparator));

    // Insert into the cache unless the caller asked us not to (e.g. bulk scan).
    if fill_cache {
      if let Some(cache) = &self.block_cache {
        cache.insert(self.cache_id, handle.offset, block.clone());
      }
    }

    Ok(block)
  }

  /// Look up `user_key` in the table, returning a [`LookupResult`].
  ///
  /// `verify_checksums`: when `true`, CRC32c is verified on every block read.
  /// `fill_cache`: when `false`, a cache miss is not inserted into the block cache.
  pub(crate) fn get(
    &self,
    user_key: &[u8],
    sequence: u64,
    verify_checksums: bool,
    fill_cache: bool,
  ) -> Result<LookupResult, Error> {
    use crate::table::format::{make_internal_key, parse_internal_key};

    // Construct a lookup internal key: user_key + tag(sequence, vtype=kValueTypeForSeek).
    // In internal-key order (seq DESC), this sorts before all entries for `user_key`
    // with seq <= `sequence`, so `seek(lookup_key)` lands at the newest visible version.
    let lookup_key = make_internal_key(user_key, sequence, 1);

    // Search the index block for the first data block whose largest key >= lookup_key.
    let mut idx = self.index_block.iter();
    idx.seek(&lookup_key);
    if !idx.valid() {
      return Ok(LookupResult::NotInTable);
    }

    // Decode the BlockHandle from the index entry's value.
    let (handle, _) = BlockHandle::decode_from(idx.value())?;

    // Consult the filter (if present) before doing any data-block I/O.
    // A definite-negative skips the read entirely; false positives proceed normally.
    if let Some(filter) = &self.filter {
      if !filter.key_may_match(handle.offset, user_key) {
        return Ok(LookupResult::NotInTable);
      }
    }

    // Read (or fetch from cache) the data block.
    let data_block = self.read_data_block(&handle, verify_checksums, fill_cache)?;
    let mut it = data_block.iter();
    it.seek(&lookup_key);

    if it.valid() {
      let ikey = it.key();
      match parse_internal_key(ikey) {
        None => {
          return Err(Error::Corruption(
            "invalid internal key in data block".to_owned(),
          ))
        }
        Some((found_user_key, _seq, vtype)) => {
          if self.comparator.compare(found_user_key, user_key) != std::cmp::Ordering::Equal {
            return Ok(LookupResult::NotInTable);
          }
          // we did find the key, we can ignore `_seq`, as it will be <= `sequence` by definition
          // no need to recheck for that
          match vtype {
            1 => return Ok(LookupResult::Value(it.value().to_vec())),
            0 => return Ok(LookupResult::Deleted),
            _ => return Err(Error::Corruption(format!("unknown vtype {vtype}"))),
          }
        }
      }
    }
    Ok(LookupResult::NotInTable)
  }

  /// Estimate the byte offset within the file of the data that would contain
  /// `ikey` (an SSTable internal key).
  ///
  /// - Files (data blocks) that are entirely before `ikey` contribute their
  ///   full block offset.
  /// - If `ikey` falls within a block, the block's start offset is returned.
  /// - If `ikey` is past the last key, `metaindex_offset` is returned as a
  ///   proxy for "end of data blocks" (matching LevelDB's heuristic).
  ///
  /// See `table/table.cc: Table::ApproximateOffsetOf`.
  pub(crate) fn approximate_offset_of(&self, ikey: &[u8]) -> u64 {
    let mut idx = self.index_block.iter();
    idx.seek(ikey);
    if idx.valid() {
      BlockHandle::decode_from(idx.value())
        .map(|(h, _)| h.offset)
        .unwrap_or(self.metaindex_offset)
    } else {
      self.metaindex_offset
    }
  }

  /// Return a `TwoLevelIterator` over all entries in this table.
  ///
  /// The iterator clones the file handle (cheap `dup(2)`) so it can read data
  /// blocks on demand without holding a reference to `self`.
  ///
  /// `fill_cache`: when `false`, data blocks read by this iterator are not inserted
  /// into the block cache (useful for bulk scans such as compaction).
  pub(crate) fn new_iterator(
    &self,
    verify_checksums: bool,
    fill_cache: bool,
  ) -> Result<TwoLevelIterator, Error> {
    use crate::table::two_level_iterator::BlockFn;
    let file = self.file.try_clone()?;
    let block_cache = self.block_cache.clone();
    let cache_id = self.cache_id;
    let comparator = Arc::clone(&self.comparator);
    let index_iter: Box<dyn InternalIterator> = Box::new(self.index_block.iter());
    let block_fn: BlockFn = Box::new(move |handle_value: &[u8]| {
      let (handle, _) = BlockHandle::decode_from(handle_value)?;

      // Check block cache first.
      if let Some(cache) = &block_cache {
        if let Some(block) = cache.get(cache_id, handle.offset) {
          return Ok(Box::new(block.iter()) as Box<dyn InternalIterator>);
        }
      }

      // Read from disk.
      let contents = read_block(&file, &handle, verify_checksums)?;
      let block = Block::new(contents.data, Arc::clone(&comparator));

      if fill_cache {
        if let Some(cache) = &block_cache {
          cache.insert(cache_id, handle.offset, block.clone());
        }
      }

      Ok(Box::new(block.iter()) as Box<dyn InternalIterator>)
    });
    Ok(TwoLevelIterator::new(index_iter, block_fn))
  }

  /// Iterate over the entire table, calling `f(internal_key, value)` for each entry.
  ///
  /// Used by the L0 flush verifier in tests.  Entries are yielded in internal-key order.
  #[cfg(test)]
  #[allow(dead_code)]
  pub(crate) fn for_each<F>(&self, mut f: F) -> Result<(), Error>
  where
    F: FnMut(&[u8], &[u8]) -> Result<(), Error>,
  {
    let mut idx = self.index_block.iter();
    idx.seek_to_first();
    while idx.valid() {
      let (handle, _) = BlockHandle::decode_from(idx.value())?;
      let contents = read_block(&self.file, &handle, false)?;
      let data_block = Block::new(contents.data, Arc::clone(&self.comparator));
      let mut it = data_block.iter();
      it.seek_to_first();
      while it.valid() {
        f(it.key(), it.value())?;
        it.next();
      }
      idx.next();
    }
    Ok(())
  }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Read the filter block from a table's metaindex, if the metaindex contains an
/// entry for the key `"filter.<policy.name()>"`.
///
/// Returns `Ok(Some(_))` when a matching filter block is found and parsed
/// successfully.  Returns `Ok(None)` when no matching entry exists (e.g. the
/// table was written without a filter policy, or the policy name differs).
/// Returns `Ok(None)` (rather than an error) on soft failures such as a
/// truncated or unrecognised metaindex — reading filter blocks is best-effort.
fn read_filter_block(
  file: &File,
  metaindex_handle: &BlockHandle,
  policy: Arc<dyn FilterPolicy>,
) -> Result<Option<FilterBlockReader>, Error> {
  // Read the metaindex block (always uncompressed; checksums not required here).
  // The metaindex uses raw string keys ("filter.<name>"), so BytewiseComparator is correct.
  let meta_contents = read_block(file, metaindex_handle, false)?;
  let meta_block = Block::new(
    meta_contents.data,
    Arc::new(crate::comparator::BytewiseComparator),
  );

  // Seek the metaindex for the key "filter.<policy_name>".
  let filter_key = format!("filter.{}", policy.name());
  let filter_key_bytes = filter_key.as_bytes();
  let mut it = meta_block.iter();
  it.seek(filter_key_bytes);
  if !it.valid() || it.key() != filter_key_bytes {
    // No matching filter entry — table was written without this filter policy.
    return Ok(None);
  }

  // Decode the BlockHandle to the filter block.
  let (filter_handle, _) = BlockHandle::decode_from(it.value())?;

  // Read the raw filter block bytes (uncompressed; checksums skipped here as
  // LevelDB does in `ReadFilter` — the filter block is verified at build time).
  let filter_contents = read_block(file, &filter_handle, false)?;

  // Parse and return the FilterBlockReader.
  Ok(FilterBlockReader::new(policy, filter_contents.data))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::table::builder::TableBuilder;
  use crate::table::format::make_internal_key;

  /// Build a table with internal keys (as TableBuilder receives from the flusher).
  fn write_table_internal(pairs: &[(&[u8], u64, u8, &[u8])]) -> (tempfile::NamedTempFile, u64) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut b = TableBuilder::new(
      file,
      4096,
      16,
      None,
      crate::options::CompressionType::NoCompression,
      Arc::new(crate::comparator::BytewiseComparator),
    );
    for &(uk, seq, vt, val) in pairs {
      let ikey = make_internal_key(uk, seq, vt);
      b.add(&ikey, val).unwrap();
    }
    let size = b.finish().unwrap();
    (tmp, size)
  }

  #[test]
  fn open_and_get_user_key() {
    let (tmp, size) = write_table_internal(&[(b"hello", 1, 1, b"world")]);
    let table = Table::open(
      tmp.reopen().unwrap(),
      size,
      None,
      None,
      Arc::new(crate::comparator::BytewiseComparator),
    )
    .unwrap();
    assert!(
      matches!(table.get(b"hello", u64::MAX, false, true).unwrap(), LookupResult::Value(v) if v == b"world")
    );
  }

  #[test]
  fn get_missing_key() {
    let (tmp, size) = write_table_internal(&[(b"a", 1, 1, b"1")]);
    let table = Table::open(
      tmp.reopen().unwrap(),
      size,
      None,
      None,
      Arc::new(crate::comparator::BytewiseComparator),
    )
    .unwrap();
    assert!(matches!(
      table.get(b"z", u64::MAX, false, true).unwrap(),
      LookupResult::NotInTable
    ));
  }

  #[test]
  fn get_tombstone_returns_deleted() {
    // vtype=0 is a deletion tombstone — must return Deleted, not NotInTable.
    let (tmp, size) = write_table_internal(&[(b"gone", 1, 0, b"")]);
    let table = Table::open(
      tmp.reopen().unwrap(),
      size,
      None,
      None,
      Arc::new(crate::comparator::BytewiseComparator),
    )
    .unwrap();
    assert!(matches!(
      table.get(b"gone", u64::MAX, false, true).unwrap(),
      LookupResult::Deleted
    ));
  }

  #[test]
  fn get_newest_version_wins() {
    let (tmp, size) = write_table_internal(&[(b"key", 10, 1, b"new"), (b"key", 5, 1, b"old")]);
    let table = Table::open(
      tmp.reopen().unwrap(),
      size,
      None,
      None,
      Arc::new(crate::comparator::BytewiseComparator),
    )
    .unwrap();
    assert!(
      matches!(table.get(b"key", u64::MAX, false, true).unwrap(), LookupResult::Value(v) if v == b"new")
    );
  }

  #[test]
  fn get_respects_sequence_number() {
    // Two versions of "key": seq=10 ("new") and seq=5 ("old").
    let (tmp, size) = write_table_internal(&[(b"key", 10, 1, b"new"), (b"key", 5, 1, b"old")]);
    let table = Table::open(
      tmp.reopen().unwrap(),
      size,
      None,
      None,
      Arc::new(crate::comparator::BytewiseComparator),
    )
    .unwrap();
    // With sequence=u64::MAX, we see the newest (seq=10).
    assert!(
      matches!(table.get(b"key", u64::MAX, false, true).unwrap(), LookupResult::Value(v) if v == b"new")
    );
    // With sequence=7, seq=10 is invisible — we see seq=5.
    assert!(
      matches!(table.get(b"key", 7, false, true).unwrap(), LookupResult::Value(v) if v == b"old")
    );
    // With sequence=3, both versions are invisible.
    assert!(matches!(
      table.get(b"key", 3, false, true).unwrap(),
      LookupResult::NotInTable
    ));
  }

  #[test]
  fn verify_checksums_passes_on_valid_block() {
    let (tmp, size) = write_table_internal(&[(b"k", 1, 1, b"v")]);
    let table = Table::open(
      tmp.reopen().unwrap(),
      size,
      None,
      None,
      Arc::new(crate::comparator::BytewiseComparator),
    )
    .unwrap();
    assert!(matches!(
      table.get(b"k", u64::MAX, true, true).unwrap(),
      LookupResult::Value(_)
    ));
  }

  #[test]
  fn file_too_small_returns_error() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut tmp.reopen().unwrap(), &[0u8; 10]).unwrap();
    assert!(matches!(
      Table::open(
        tmp.reopen().unwrap(),
        10,
        None,
        None,
        Arc::new(crate::comparator::BytewiseComparator),
      ),
      Err(Error::Corruption(_))
    ));
  }
}
