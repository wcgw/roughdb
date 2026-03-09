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
/// See `include/leveldb/options.h`.
#[derive(Debug, Clone)]
pub struct Options {
  // ── Behaviour ───────────────────────────────────────────────────────────
  /// Create the database if it does not already exist.
  ///
  /// Default: false.
  pub create_if_missing: bool,

  /// Return an error if the database already exists.
  ///
  /// Default: false.
  pub error_if_exists: bool,

  /// Aggressively check data integrity; stop early on any detected error.
  ///
  /// **Not yet implemented.** The flag is accepted but currently has no effect: WAL and SSTable
  /// block reads do not perform checksum verification even when this is set.
  ///
  /// Default: false.
  pub paranoid_checks: bool,

  // ── Performance ─────────────────────────────────────────────────────────
  /// Bytes of key-value data to accumulate in the memtable before flushing to an L0 SSTable.
  /// Larger values improve bulk-load throughput at the cost of higher memory use and longer
  /// recovery time.
  ///
  /// Default: 4 MiB.
  pub write_buffer_size: usize,

  /// Maximum number of simultaneously open file descriptors.
  ///
  /// **Not yet implemented.** Accepted but ignored — there is no table cache yet, so all SSTable
  /// file handles stay open for the lifetime of the database regardless of this value.
  ///
  /// Default: 1 000.
  pub max_open_files: usize,

  /// Uncompressed byte size of each SSTable data block.
  ///
  /// Default: 4 KiB.
  pub block_size: usize,

  /// Number of keys between delta-encoding restart points within a block.
  ///
  /// Default: 16.
  pub block_restart_interval: usize,

  /// Maximum size of an individual SSTable file before a new one is started during compaction.
  ///
  /// Default: 2 MiB.
  pub max_file_size: usize,

  /// Compression algorithm applied to SSTable data blocks.
  ///
  /// **Not yet implemented.** Accepted but ignored — all blocks are currently written
  /// uncompressed regardless of this setting. Reading compressed blocks written by LevelDB or
  /// other implementations is also not yet supported.
  ///
  /// Default: `Snappy`.
  pub compression: CompressionType,

  /// Compression level used when `compression` is `Zstd`. Range: `[-5, 22]`.
  ///
  /// **Not yet implemented.** Has no effect until `compression` is implemented.
  ///
  /// Default: 1.
  pub zstd_compression_level: i32,

  /// Reuse existing MANIFEST and log files on open rather than creating new ones (experimental).
  ///
  /// **Not yet implemented.** Accepted but ignored.
  ///
  /// Default: false.
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
  /// **Partially implemented.** `Table::get` and `read_block` respect this flag, but
  /// `Version::get` (the SSTable lookup path used by `Db::get`) currently hardcodes `false`,
  /// so point lookups never verify checksums even when this is set. Iterator block reads are
  /// also unaffected. Setting this to `true` has no effect until the flag is fully threaded
  /// through.
  ///
  /// Default: false.
  pub verify_checksums: bool,

  /// Cache blocks read during this operation in the block cache.
  /// Set to `false` for bulk scans to avoid polluting the cache.
  ///
  /// **Not yet implemented.** Accepted but ignored — there is no block cache yet, so all block
  /// reads bypass caching unconditionally.
  ///
  /// Default: true.
  pub fill_cache: bool,

  /// Read as of this snapshot's sequence number.
  /// `None` means "use an implicit snapshot of the current state".
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
