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
#[derive(Clone)]
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
  /// When set, acts as a database-wide `verify_checksums = true`: every SSTable block read
  /// (both point lookups via [`Db::get`] and iterator block reads) verifies its CRC32c checksum,
  /// and WAL and MANIFEST records are checksum-verified during recovery.  Any mismatch returns
  /// [`Error::Corruption`].
  ///
  /// This flag is OR-ed with [`ReadOptions::verify_checksums`] on each individual read, so
  /// per-read verification can also be enabled independently.
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

  // ── Filters ─────────────────────────────────────────────────────────────
  /// Filter policy applied to SSTable data blocks.
  ///
  /// When set, each SSTable written by this database includes a filter block.
  /// Before reading any data block, `Table::get` consults the filter; a
  /// definite-negative answer skips all data-block I/O entirely, making
  /// point lookups on absent keys much cheaper.
  ///
  /// [`BloomFilterPolicy::new(10)`](crate::BloomFilterPolicy::new) gives
  /// roughly 1 % false positives at ~10 bits per key — the same default used
  /// by LevelDB.  Pass `None` to disable filtering (useful for write-heavy
  /// workloads where all reads are expected to hit).
  ///
  /// The filter is also applied when reading SSTables on reopen, provided the
  /// policy name stored in the metaindex matches the configured policy.
  ///
  /// Default: `None` (no filter).
  pub filter_policy: Option<std::sync::Arc<dyn crate::filter::FilterPolicy>>,
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
      filter_policy: None,
    }
  }
}

impl std::fmt::Debug for Options {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Options")
      .field("create_if_missing", &self.create_if_missing)
      .field("error_if_exists", &self.error_if_exists)
      .field("paranoid_checks", &self.paranoid_checks)
      .field("write_buffer_size", &self.write_buffer_size)
      .field("max_open_files", &self.max_open_files)
      .field("block_size", &self.block_size)
      .field("block_restart_interval", &self.block_restart_interval)
      .field("max_file_size", &self.max_file_size)
      .field("compression", &self.compression)
      .field("zstd_compression_level", &self.zstd_compression_level)
      .field("reuse_logs", &self.reuse_logs)
      .field(
        "filter_policy",
        &self.filter_policy.as_ref().map(|p| p.name()),
      )
      .finish()
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
  /// Default: false.
  pub sync: bool,
}
