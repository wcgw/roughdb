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
use crate::table::block::Block;
use crate::table::format::{read_block, BlockHandle, Footer, FOOTER_ENCODED_LENGTH};
use std::fs::File;
use std::os::unix::fs::FileExt;

/// A random-access SSTable reader.
///
/// `Table::open` reads the footer and index block once; subsequent `get` calls
/// use the in-memory index to locate data blocks, reading them from disk via
/// `pread` (no seeking — takes `&self`).  See `table/table.h/cc`.
pub(crate) struct Table {
  file: File,
  index_block: Block,
}

impl Table {
  /// Open an SSTable file of `file_size` bytes.
  ///
  /// Reads and validates the footer, then reads the index block into memory.
  pub(crate) fn open(file: File, file_size: u64) -> Result<Self, Error> {
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
    let index_block = Block::new(index_contents.data);

    Ok(Table { file, index_block })
  }

  /// Look up `user_key` in the table.
  ///
  /// Returns:
  /// - `Ok(Some(value))` — key found, returns its value.
  /// - `Ok(None)` — key not present (no hit in this table; caller should check next file).
  /// - `Err(_)` — I/O or corruption error.
  ///
  /// `verify_checksums`: when `true`, CRC32c is verified on every block read.
  ///
  /// Note: this is a user-key lookup, not an internal-key lookup.  The table
  /// stores internal keys (user_key + tag), but `Table::get` seeks using a
  /// lookup internal key with `seq = u64::MAX` so it matches the newest version
  /// of any entry with the given user key.
  pub(crate) fn get(&self, user_key: &[u8], verify_checksums: bool) -> Result<Option<Vec<u8>>, Error> {
    use crate::table::format::{make_internal_key, parse_internal_key};

    // Construct a lookup internal key: user_key + tag(seq=u64::MAX >> 8, vtype=1).
    // seq=u64::MAX >> 8 is the largest sequence, so this sorts before all real entries
    // for `user_key` and lands the seek at the right spot.
    let lookup_key = make_internal_key(user_key, u64::MAX >> 8, 1);

    // Search the index block for the first data block whose largest key >= lookup_key.
    let mut idx = self.index_block.iter();
    idx.seek(&lookup_key);
    if !idx.valid() {
      return Ok(None);
    }

    // Decode the BlockHandle from the index entry's value.
    let (handle, _) = BlockHandle::decode_from(idx.value())?;

    // Read and scan the data block.
    let contents = read_block(&self.file, &handle, verify_checksums)?;
    let data_block = Block::new(contents.data);
    let mut it = data_block.iter();
    it.seek(&lookup_key);

    while it.valid() {
      let ikey = it.key();
      match parse_internal_key(ikey) {
        None => return Err(Error::Corruption("invalid internal key in data block".to_owned())),
        Some((found_user_key, _seq, vtype)) => {
          if found_user_key != user_key {
            // Moved past all entries for this user key.
            return Ok(None);
          }
          match vtype {
            1 => return Ok(Some(it.value().to_vec())), // Value
            0 => return Ok(None),                      // Deletion tombstone
            _ => return Err(Error::Corruption(format!("unknown vtype {vtype}"))),
          }
        }
      }
    }
    Ok(None)
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
    use crate::table::format::BlockHandle;

    let mut idx = self.index_block.iter();
    idx.seek_to_first();
    while idx.valid() {
      let (handle, _) = BlockHandle::decode_from(idx.value())?;
      let contents = read_block(&self.file, &handle, false)?;
      let data_block = Block::new(contents.data);
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::table::builder::TableBuilder;
  use crate::table::format::make_internal_key;


  /// Build a table with internal keys (as TableBuilder receives from the flusher).
  fn write_table_internal(pairs: &[(&[u8], u64, u8, &[u8])]) -> (tempfile::NamedTempFile, u64) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.reopen().unwrap();
    let mut b = TableBuilder::new(file, 4096, 16);
    for &(uk, seq, vt, val) in pairs {
      let ikey = make_internal_key(uk, seq, vt);
      b.add(&ikey, val).unwrap();
    }
    let size = b.finish().unwrap();
    (tmp, size)
  }

  #[test]
  fn open_and_get_user_key() {
    // Table stores internal keys; Table::get receives user keys.
    let (tmp, size) = write_table_internal(&[(b"hello", 1, 1, b"world")]);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    assert_eq!(table.get(b"hello", false).unwrap(), Some(b"world".to_vec()));
  }

  #[test]
  fn get_missing_key() {
    let (tmp, size) = write_table_internal(&[(b"a", 1, 1, b"1")]);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    assert!(table.get(b"z", false).unwrap().is_none());
  }

  #[test]
  fn get_tombstone_returns_none() {
    // vtype=0 is a deletion tombstone.
    let (tmp, size) = write_table_internal(&[(b"gone", 1, 0, b"")]);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    assert!(table.get(b"gone", false).unwrap().is_none());
  }

  #[test]
  fn get_newest_version_wins() {
    // Newer seq sorts first (seq DESC in internal key order).
    // Table::get with seq=u64::MAX>>8 finds seq=10 before seq=5.
    let (tmp, size) =
      write_table_internal(&[(b"key", 10, 1, b"new"), (b"key", 5, 1, b"old")]);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    assert_eq!(table.get(b"key", false).unwrap().as_deref(), Some(b"new".as_ref()));
  }

  #[test]
  fn verify_checksums_passes_on_valid_block() {
    let (tmp, size) = write_table_internal(&[(b"k", 1, 1, b"v")]);
    let table = Table::open(tmp.reopen().unwrap(), size).unwrap();
    assert!(table.get(b"k", true).unwrap().is_some());
  }

  #[test]
  fn file_too_small_returns_error() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    // Write just 10 bytes — too small for a footer.
    std::io::Write::write_all(&mut tmp.reopen().unwrap(), &[0u8; 10]).unwrap();
    assert!(matches!(Table::open(tmp.reopen().unwrap(), 10), Err(Error::Corruption(_))));
  }
}
