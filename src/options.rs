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

/// Compression algorithm applied to SSTable data blocks.
///
/// Values are fixed on disk; do not renumber existing variants.
/// See `include/leveldb/options.h: CompressionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
  NoCompression = 0x0,
  #[default]
  Snappy = 0x1,
  Zstd = 0x2,
}

/// Options that control the overall behaviour of a database.
///
/// Pass to `Db::open` (Phase 9). Fields that depend on types defined in later
/// phases are added as those phases are implemented:
/// - `comparator`  — Phase 1 (hardcoded lexicographic ordering for now)
/// - `env`         — Phase 2 (file I/O abstraction)
/// - `info_log`    — Phase 2
/// - `block_cache` — Phase 5
/// - `filter_policy` — Phase 4
///
/// See `include/leveldb/options.h`.
#[derive(Debug, Clone)]
pub struct Options {
  // ── Behaviour ───────────────────────────────────────────────────────────
  /// Create the database if it does not already exist.
  pub create_if_missing: bool,

  /// Return an error if the database already exists.
  pub error_if_exists: bool,

  /// Aggressively check data integrity; stop early on any detected error.
  pub paranoid_checks: bool,

  // ── Performance ─────────────────────────────────────────────────────────
  /// Bytes of key-value data to accumulate in the memtable before flushing
  /// to an L0 SSTable.  Larger values improve bulk-load throughput at the
  /// cost of higher memory use and longer recovery time.
  ///
  /// Default: 4 MiB. Active from Phase 9 (memtable flush).
  pub write_buffer_size: usize,

  /// Maximum number of simultaneously open file descriptors.
  ///
  /// Default: 1 000. Active from Phase 8 (table cache).
  pub max_open_files: usize,

  /// Uncompressed byte size of each SSTable data block.
  ///
  /// Default: 4 KiB. Active from Phase 3 (SSTable format).
  pub block_size: usize,

  /// Number of keys between delta-encoding restart points within a block.
  ///
  /// Default: 16. Active from Phase 3.
  pub block_restart_interval: usize,

  /// Maximum size of an individual SSTable file before a new one is started.
  ///
  /// Default: 2 MiB. Active from Phase 9 (compaction).
  pub max_file_size: usize,

  /// Compression algorithm applied to SSTable data blocks.
  ///
  /// Default: `Snappy`. Active from Phase 3.
  pub compression: CompressionType,

  /// Compression level used when `compression` is `Zstd`. Range: `[-5, 22]`.
  ///
  /// Default: 1. Active from Phase 3.
  pub zstd_compression_level: i32,

  /// Reuse existing MANIFEST and log files on open (experimental).
  ///
  /// Default: false. Active from Phase 9.
  pub reuse_logs: bool,
}

impl Default for Options {
  fn default() -> Self {
    Options {
      create_if_missing: false,
      error_if_exists: false,
      paranoid_checks: false,
      write_buffer_size: 4 * 1024 * 1024,
      max_open_files: 1_000,
      block_size: 4 * 1024,
      block_restart_interval: 16,
      max_file_size: 2 * 1024 * 1024,
      compression: CompressionType::Snappy,
      zstd_compression_level: 1,
      reuse_logs: false,
    }
  }
}

/// Options that control read operations.
///
/// See `include/leveldb/options.h`.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
  /// Verify block checksums on every read.
  ///
  /// Default: false. Active from Phase 3 (SSTable reader).
  pub verify_checksums: bool,

  /// Cache blocks read during this operation in the block cache.
  /// Set to `false` for bulk scans to avoid polluting the cache.
  ///
  /// Default: true. Active from Phase 5 (block cache).
  pub fill_cache: bool,

  /// Read as of this snapshot's sequence number.
  /// `None` means "use an implicit snapshot of the current state".
  ///
  /// Active from Phase 9 (snapshot support).
  pub snapshot: Option<Snapshot>,
}

impl ReadOptions {
  pub fn new() -> Self {
    ReadOptions {
      fill_cache: true,
      ..Default::default()
    }
  }
}

/// Options that control write operations.
///
/// See `include/leveldb/options.h`.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
  /// Flush the OS buffer cache (fsync) before acknowledging the write.
  /// Slower but durable across process *and* machine crashes.
  /// When `false`, writes survive process crashes but may be lost on a
  /// hard reboot.
  ///
  /// Default: false. Active from Phase 2 (WAL).
  pub sync: bool,
}

/// An immutable snapshot of the database state at a particular sequence number.
///
/// Snapshots are handed out by `Db::get_snapshot` (Phase 9) and passed via
/// [`ReadOptions::snapshot`] to pin reads to a specific point in time.
/// The inner sequence number is not part of the public API.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot(pub(crate) u64);
