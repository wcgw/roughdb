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

use crate::db::table_cache::TableCache;
use crate::db::version::Version;
use crate::db::version_edit::{FileMetaData, VersionEdit};
use crate::error::Error;
use crate::log::reader::Reader as LogReader;
use crate::log::writer::Writer as LogWriter;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

// ── MANIFEST file-name helpers ────────────────────────────────────────────────

pub(crate) fn manifest_filename(number: u64) -> String {
  format!("MANIFEST-{number:06}")
}

fn write_current_file(path: &Path, manifest_number: u64) -> Result<(), Error> {
  let content = format!("{}\n", manifest_filename(manifest_number));
  std::fs::write(path.join("CURRENT"), content)?;
  Ok(())
}

fn read_current_file(path: &Path) -> Result<String, Error> {
  let content = std::fs::read_to_string(path.join("CURRENT"))?;
  let trimmed = content.trim_end_matches('\n');
  if trimmed.is_empty() {
    return Err(Error::Corruption("CURRENT file is empty".to_owned()));
  }
  Ok(trimmed.to_owned())
}

// ── VersionSet ────────────────────────────────────────────────────────────────

/// Manages the set of live SSTable files across all levels.
///
/// Writes every change (flush, compaction) to the MANIFEST log so the database
/// can be recovered without replaying the entire WAL.
pub(crate) struct VersionSet {
  current: Arc<Version>,
  /// Next file number to allocate.  Incremented by `next_file_number()`.
  next_file_number: u64,
  /// Sequence number of the last write reflected in the current `Version`.
  last_sequence: u64,
  /// File number of the current WAL.
  log_number: u64,
  /// Open writer for the MANIFEST log.
  manifest_log: LogWriter,
  /// File number of the MANIFEST file (used to update CURRENT on rotation in Phase 6).
  #[allow(dead_code)]
  pub(crate) manifest_number: u64,
  /// Round-robin compaction cursor per level: the largest internal key last compacted.
  /// Empty vec = not yet set.  Persisted to MANIFEST via TAG_COMPACT_POINTER.
  compact_pointer: [Vec<u8>; crate::db::version::NUM_LEVELS],
}

impl VersionSet {
  /// Create a brand-new `VersionSet` for a database that has never been opened.
  ///
  /// - Creates `MANIFEST-000002` and writes the initial `VersionEdit`.
  /// - Writes `CURRENT` pointing to that file.
  ///
  /// File numbering: 1 = WAL, 2 = MANIFEST, 3+ = SSTables.
  pub(crate) fn create(path: &Path) -> Result<Self, Error> {
    let manifest_number = 2u64;
    let manifest_path = path.join(manifest_filename(manifest_number));

    let file = File::create(&manifest_path)?;
    let mut manifest_log = LogWriter::new(file, 0);

    // Write the initial VersionEdit so recovery always finds a valid sequence.
    let mut edit = VersionEdit::new();
    edit.next_file_number = Some(3); // SSTables start at 3
    edit.last_sequence = Some(0);
    edit.log_number = Some(1);
    manifest_log.add_record(&edit.encode())?;

    write_current_file(path, manifest_number)?;

    Ok(VersionSet {
      current: Arc::new(Version::new()),
      next_file_number: 3,
      last_sequence: 0,
      log_number: 1,
      manifest_log,
      manifest_number,
      compact_pointer: std::array::from_fn(|_| Vec::new()),
    })
  }

  /// Recover a `VersionSet` from an existing MANIFEST.
  ///
  /// Reads `CURRENT`, replays each `VersionEdit` in the MANIFEST log, and
  /// assembles the initial `Version` (metadata only — tables are opened lazily
  /// by the `TableCache` on first access).  Returns a `VersionSet` ready for
  /// `log_and_apply`.
  pub(crate) fn recover(path: &Path, paranoid_checks: bool) -> Result<Self, Error> {
    let manifest_name = read_current_file(path)?;
    let manifest_path = path.join(&manifest_name);

    // Extract file number from the manifest name ("MANIFEST-000002" → 2).
    let manifest_number = parse_manifest_number(&manifest_name)?;

    // Replay all VersionEdits from the MANIFEST.
    let manifest_file_for_read = File::open(&manifest_path)?;
    let mut reader = LogReader::new(manifest_file_for_read, None, paranoid_checks, 0);

    let mut builder = Builder::new();
    while let Some(record) = reader.read_record() {
      let edit = VersionEdit::decode(&record)?;
      builder.apply(&edit);
    }

    // Extract scalar fields before `build` consumes the builder.
    let next_file_number = builder.next_file_number;
    let last_sequence = builder.last_sequence;
    let log_number = builder.log_number;
    // Clone compact_pointer before builder is consumed by build().
    let compact_pointer: [Vec<u8>; crate::db::version::NUM_LEVELS] =
      std::array::from_fn(|i| builder.compact_pointer[i].clone());

    let mut files = builder.build();
    // Compute compaction scores on the recovered version.
    crate::db::version::finalize(&mut files, 4); // 4 = L0_COMPACTION_TRIGGER

    // Re-open the MANIFEST for appending (continue after the last record).
    let manifest_file_for_write = OpenOptions::new().append(true).open(&manifest_path)?;
    let manifest_len = manifest_file_for_write.metadata()?.len();
    let manifest_log = LogWriter::new(manifest_file_for_write, manifest_len);

    Ok(VersionSet {
      current: Arc::new(files),
      next_file_number,
      last_sequence,
      log_number,
      manifest_log,
      manifest_number,
      compact_pointer,
    })
  }

  /// Apply `edit` to the current `Version` and append it to the MANIFEST.
  ///
  /// Fills in `next_file_number`, `last_sequence`, and `log_number` from the
  /// current `VersionSet` state before encoding.  Deleted files are evicted
  /// from `tc` so their file handles are released.
  pub(crate) fn log_and_apply(
    &mut self,
    edit: &mut VersionEdit,
    tc: &TableCache,
  ) -> Result<(), Error> {
    edit.next_file_number = Some(self.next_file_number);
    edit.last_sequence = Some(self.last_sequence);
    edit.log_number = Some(self.log_number);

    self.manifest_log.add_record(&edit.encode())?;

    // Build new Version by cloning current and applying additions/deletions.
    let mut new_files: [Vec<Arc<FileMetaData>>; 7] =
      std::array::from_fn(|i| self.current.files[i].clone());

    for &(level, number) in &edit.deleted_files {
      let level = level as usize;
      new_files[level].retain(|m| m.number != number);
      tc.evict(number);
    }
    for (level, meta) in &edit.new_files {
      let level = *level as usize;
      if level == 0 {
        // L0: newest-first — prepend.
        new_files[0].insert(0, Arc::clone(meta));
      } else {
        // L1+: append; sorted below.
        new_files[level].push(Arc::clone(meta));
      }
    }

    // Keep L1–L6 sorted by smallest user key so binary search and overlap
    // checks work correctly.  L0 files are intentionally newest-first.
    for level_files in new_files.iter_mut().skip(1) {
      level_files.sort_by(|a, b| {
        let ak = if a.smallest.len() >= 8 {
          &a.smallest[..a.smallest.len() - 8]
        } else {
          &a.smallest
        };
        let bk = if b.smallest.len() >= 8 {
          &b.smallest[..b.smallest.len() - 8]
        } else {
          &b.smallest
        };
        ak.cmp(bk)
      });
    }

    // Apply compact-pointer updates from this edit.
    for (level, key) in &edit.compact_pointers {
      self.compact_pointer[*level as usize] = key.clone();
    }

    let mut v = Version {
      files: new_files,
      compaction_score: -1.0,
      compaction_level: -1,
    };
    crate::db::version::finalize(&mut v, 4); // 4 = L0_COMPACTION_TRIGGER
    self.current = Arc::new(v);
    Ok(())
  }

  // ── Accessors ───────────────────────────────────────────────────────────────

  /// Allocate the next file number and return it (pre-increment).
  pub(crate) fn next_file_number(&mut self) -> u64 {
    let n = self.next_file_number;
    self.next_file_number += 1;
    n
  }

  pub(crate) fn last_sequence(&self) -> u64 {
    self.last_sequence
  }

  pub(crate) fn set_last_sequence(&mut self, seq: u64) {
    self.last_sequence = seq;
  }

  pub(crate) fn log_number(&self) -> u64 {
    self.log_number
  }

  pub(crate) fn set_log_number(&mut self, n: u64) {
    self.log_number = n;
  }

  pub(crate) fn current(&self) -> Arc<Version> {
    Arc::clone(&self.current)
  }

  /// Read-only access to the compact-pointer array (one entry per level).
  pub(crate) fn compact_pointer(&self) -> &[Vec<u8>; crate::db::version::NUM_LEVELS] {
    &self.compact_pointer
  }

  pub(crate) fn manifest_number(&self) -> u64 {
    self.manifest_number
  }

  /// Add the file numbers of every live SSTable across all levels to `live`.
  ///
  /// Called by `delete_obsolete_files` to compute the set of files that must
  /// not be deleted.  Matches LevelDB's `VersionSet::AddLiveFiles`.
  pub(crate) fn add_live_files(&self, live: &mut HashSet<u64>) {
    for level_files in &self.current.files {
      for meta in level_files {
        live.insert(meta.number);
      }
    }
  }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Accumulates `VersionEdit` deltas during MANIFEST replay.
///
/// After all records have been applied, `build` opens the live SSTable files
/// and constructs the initial `Version`.
struct Builder {
  /// Live files keyed by `(level, file_number)`.
  added: HashMap<(i32, u64), Arc<FileMetaData>>,
  /// Files that have been explicitly deleted (supersedes any prior addition).
  deleted: HashSet<(i32, u64)>,
  next_file_number: u64,
  last_sequence: u64,
  log_number: u64,
  /// Round-robin compaction cursor accumulated from compact-pointer edits.
  compact_pointer: [Vec<u8>; crate::db::version::NUM_LEVELS],
}

impl Builder {
  fn new() -> Self {
    Builder {
      added: HashMap::new(),
      deleted: HashSet::new(),
      next_file_number: 3,
      last_sequence: 0,
      log_number: 1,
      compact_pointer: std::array::from_fn(|_| Vec::new()),
    }
  }

  fn apply(&mut self, edit: &VersionEdit) {
    if let Some(v) = edit.log_number {
      self.log_number = v;
    }
    if let Some(v) = edit.next_file_number {
      self.next_file_number = v;
    }
    if let Some(v) = edit.last_sequence {
      self.last_sequence = v;
    }
    for &(level, number) in &edit.deleted_files {
      self.deleted.insert((level, number));
      self.added.remove(&(level, number));
    }
    for (level, meta) in &edit.new_files {
      let key = (*level, meta.number);
      self.deleted.remove(&key);
      self.added.insert(key, Arc::clone(meta));
    }
    for (level, key) in &edit.compact_pointers {
      self.compact_pointer[*level as usize] = key.clone();
    }
  }

  /// Assemble the initial `Version` from replayed MANIFEST edits.
  ///
  /// Tables are opened lazily by the `TableCache` on first access — no I/O
  /// here, matching LevelDB's separation between `VersionSet` and
  /// `TableCache`.
  fn build(self) -> Version {
    let mut files: [Vec<Arc<FileMetaData>>; 7] = std::array::from_fn(|_| Vec::new());

    // Collect live files per level (stable insertion order for L0).
    let mut by_level: Vec<Vec<(u64, Arc<FileMetaData>)>> = vec![Vec::new(); 7];
    for ((level, number), meta) in self.added {
      by_level[level as usize].push((number, meta));
    }

    for (level, mut level_files) in by_level.into_iter().enumerate() {
      if level == 0 {
        // L0: newest-first (sort by file number descending).
        level_files.sort_by(|a, b| b.0.cmp(&a.0));
      } else {
        // L1+: sort by smallest key.
        level_files.sort_by(|a, b| a.1.smallest.cmp(&b.1.smallest));
      }
      for (_, meta) in level_files {
        files[level].push(meta);
      }
    }

    Version {
      files,
      compaction_score: -1.0,
      compaction_level: -1,
    }
  }
}

// ── File-number parsing ───────────────────────────────────────────────────────

fn parse_manifest_number(name: &str) -> Result<u64, Error> {
  name
    .strip_prefix("MANIFEST-")
    .and_then(|s| s.parse::<u64>().ok())
    .ok_or_else(|| Error::Corruption(format!("CURRENT points to invalid MANIFEST name: {name}")))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;
  use crate::db::table_cache::{TableCache, NUM_NON_TABLE_CACHE_FILES};
  use crate::options::Options;
  use crate::table::builder::TableBuilder;
  use crate::table::format::make_internal_key;

  fn make_tc(dir: &Path) -> TableCache {
    let opts = Options::default();
    TableCache::new(
      dir,
      opts.max_open_files - NUM_NON_TABLE_CACHE_FILES,
      None,
      None,
    )
  }

  /// Write a minimal SSTable at `path/NNNNNN.ldb` and return (file_number, file_size).
  fn write_sst(dir: &Path, file_number: u64, entries: &[(&[u8], u64, u8, &[u8])]) -> (u64, u64) {
    let sst_path = dir.join(format!("{file_number:06}.ldb"));
    let file = std::fs::OpenOptions::new()
      .write(true)
      .create_new(true)
      .open(&sst_path)
      .unwrap();
    let opts = Options::default();
    let mut builder = TableBuilder::new(
      file,
      opts.block_size,
      opts.block_restart_interval,
      opts.filter_policy.clone(),
      opts.compression,
    );
    for &(uk, seq, vt, val) in entries {
      builder.add(&make_internal_key(uk, seq, vt), val).unwrap();
    }
    let size = builder.finish().unwrap();
    (file_number, size)
  }

  #[test]
  fn create_writes_current_file() {
    let dir = tempfile::tempdir().unwrap();
    VersionSet::create(dir.path()).unwrap();

    let content = std::fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    assert_eq!(content, "MANIFEST-000002\n");
    assert!(dir.path().join("MANIFEST-000002").exists());
  }

  #[test]
  fn log_and_apply_persists() {
    let dir = tempfile::tempdir().unwrap();
    let mut vs = VersionSet::create(dir.path()).unwrap();
    let tc = make_tc(dir.path());

    // Simulate flushing file 3.
    let (fnum, fsize) = write_sst(dir.path(), 3, &[(b"hello", 1, 1, b"world")]);
    let meta = FileMetaData::new(fnum, fsize, b"hello".to_vec(), b"hello".to_vec());
    let mut edit = VersionEdit::new();
    edit.new_files.push((0, meta));
    vs.log_and_apply(&mut edit, &tc).unwrap();

    // Current version must have file 3 in L0.
    let cur = vs.current();
    assert_eq!(cur.files[0].len(), 1);
    assert_eq!(cur.files[0][0].number, 3);

    // Drop and recover — the file must still appear.
    drop(vs);
    let vs2 = VersionSet::recover(dir.path(), false).unwrap();
    let cur2 = vs2.current();
    assert_eq!(cur2.files[0].len(), 1);
    assert_eq!(cur2.files[0][0].number, 3);
  }

  #[test]
  fn recover_reopens_tables() {
    let dir = tempfile::tempdir().unwrap();
    let mut vs = VersionSet::create(dir.path()).unwrap();
    let tc = make_tc(dir.path());

    let (fnum, fsize) = write_sst(dir.path(), 3, &[(b"key", 1, 1, b"val")]);
    let meta = FileMetaData::new(fnum, fsize, b"key".to_vec(), b"key".to_vec());
    let mut edit = VersionEdit::new();
    edit.new_files.push((0, meta));
    vs.log_and_apply(&mut edit, &tc).unwrap();
    drop(vs);

    let vs2 = VersionSet::recover(dir.path(), false).unwrap();
    let tc2 = make_tc(dir.path());
    let cur = vs2.current();
    use crate::table::reader::LookupResult;
    assert!(matches!(
      cur.get(b"key", 0, false, true, &tc2).unwrap(),
      LookupResult::Value(v) if v == b"val"
    ));
  }

  #[test]
  fn sequence_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut vs = VersionSet::create(dir.path()).unwrap();
    let tc = make_tc(dir.path());

    vs.set_last_sequence(42);
    let mut edit = VersionEdit::new();
    // log_and_apply persists last_sequence even with no file changes.
    vs.log_and_apply(&mut edit, &tc).unwrap();
    drop(vs);

    let vs2 = VersionSet::recover(dir.path(), false).unwrap();
    assert_eq!(vs2.last_sequence(), 42);
  }
}
