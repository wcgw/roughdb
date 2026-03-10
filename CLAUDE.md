# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

RoughDB is an embedded key-value store written in Rust, porting LevelDB to Rust. The LevelDB C++ source lives at
`../leveldb/` (primary reference). RocksDB source lives at `../rocksdb/` and should only be consulted for improvements
to existing LevelDB features — not for porting RocksDB-specific features.

## Commands

```bash
cargo test                        # run all tests
cargo test memtable               # run tests matching a path/name
cargo test -- --nocapture         # show println! output
cargo bench                       # run Criterion benchmarks (benches/db.rs)
cargo clippy                      # lint
cargo fmt                         # format (2-space indent, see rustfmt.toml)
```

## Architecture

RoughDB is an LSM-tree key-value store. The logical write path is:

```
Db::put/delete/write
  → WriteBatch (encode ops)
  → WAL (durability: written before ack)
  → Memtable (in-memory skip list, most recent writes)
      ↓ (when full, seal as imm and flush)
  → SSTable L0 (sorted, immutable file)
      ↓ (background compaction)
  → SSTable L1 … L6
```

The read path checks `mem` → `imm` → each level of SSTables (newest to oldest), stopping at the first hit.

### Implemented

**Memtable** (`src/memtable/`)
- Skip list with RocksDB's `InlineSkipList` memory layout: every node is a single arena allocation containing the
  level-0 link, higher-level links prefixed before it (negative indexing), a `u32` payload length, and the
  varint-encoded entry inline. This keeps the hot level-0 link and payload in the same cache line.
- `Splice` caches `prev`/`next` from the last insert for O(1) amortised sequential inserts.
- Lock-free reads (Acquire/Release atomics); writes serialised by the DB-level write mutex in `Db` (no lock inside
  `Memtable` itself — `UnsafeCell<SkipList>` + `unsafe impl Sync`).

**Entry encoding** (`src/memtable/entry.rs`)
- Internal key format: `[klen: varint][key][seq: varint][vtype: u8][vlen: varint][value]`
- `vtype`: `0` = Deletion, `1` = Value.
- Ordering: key ASC, seq DESC — so a lookup key with `seq = u64::MAX` always seeks to the newest version of a user key.
- Allocation-free helpers (`write_value_to`, `write_lookup_to`, etc.) encode directly into arena memory.

**Arena** (`src/memtable/arena/bump.rs`)
- Thin wrapper around `bumpalo::Bump`; `allocate_aligned(size, align)` panics on OOM (matching LevelDB: memtable OOM
  is unrecoverable).
- Unlike LevelDB/RocksDB, which manage explicit 4 KB slabs and bypass them for large allocations (> slab/4), we
  delegate slab management entirely to bumpalo. The large-allocation bypass is bumpalo's internal policy rather than
  ours, but the outcome is equivalent.
- **`memory_usage()`**: `Arena` tracks exact bytes used via an explicit `AtomicUsize` counter (bumpalo's
  `allocated_bytes()` returns chunk *capacity* not bytes used). `Memtable::approximate_memory_usage()` delegates to
  this; `Db::write` compares against `options.write_buffer_size` to trigger L0 flush.
- **Hardcoded 10 MB cap**: The `Arena::default()` cap of 10 MB is a safety ceiling only; the actual flush threshold
  is `Options::write_buffer_size` (default 4 MB). The cap should be removed or raised in Phase 9.

---

## Roadmap

Features are listed in dependency order. Each phase must be complete before the next begins.

### Phase 1 — Core types

*Prerequisite for everything.*

- [x] **`Error`** (`src/error.rs`): `Result`-based error type with variants `NotFound`, `Corruption`, `InvalidArgument`,
  `NotSupported`, `IoError`. `Db::get/put/delete` now return `Result<_, Error>`. See `include/leveldb/status.h`.
- [x] **`WriteBatch`**: Serialises a sequence of Put/Delete operations into a byte buffer. Batch format: `[seq:
  u64][count: u32][records…]` where each record is `[vtype: u8][klen: varint][key][vlen: varint][value]`. Exposes `put`,
  `delete`, `iterate(Handler)`, `approximate_size`. `Clone`-able. `Db::put`/`delete` are thin wrappers over `Db::write`.
  See `include/leveldb/write_batch.h` and `db/write_batch.cc`.
- [x] **`Options` / `ReadOptions` / `WriteOptions`**: Configuration structs. `Options` carries the write-buffer size,
  block size, compression type, filter policy, block cache handle. `WriteOptions` adds a sync flag. `ReadOptions` adds a
  snapshot handle and checksum-verify flag. See `include/leveldb/options.h`.

### Phase 2 — Write-Ahead Log

*Enables crash durability and the correct sequence-number model. Must precede full `Db::Open`.*

**Step 1 — Prerequisites**

- [x] **`crc32c` crate**: Add `crc32c = "..."` to `Cargo.toml`. Used for record header checksums.
- [x] **Sequence number ownership**: Move `last_sequence` from `Memtable` to `Db`. Carried as part of `WriteState`
  (alongside the optional `LogWriter`) inside `write_lock: Mutex<WriteState>` — the type system enforces that both
  are inaccessible without holding the write lock, matching LevelDB's plain `uint64_t` under `mutex_`.
  `Memtable::add`/`delete` gain an explicit `seq: u64` parameter (internal `next_seq` counter removed). `Db::write`
  reads `start_seq = state.last_sequence + 1`, clones and stamps the batch, writes to WAL, iterates passing
  incrementing sequence numbers per record, then advances `state.last_sequence += batch.count()` after all inserts.

**Step 2 — Log primitives**

- [x] **Format** (`src/log/format.rs`): 32 KB blocks. Record header: `[crc32c: u32 LE][len: u16 LE][type: u8]` (7
  bytes). Fragment types: `Zero=0` (pad), `Full=1`, `First=2`, `Middle=3`, `Last=4`. Constants: `BLOCK_SIZE = 32768`,
  `HEADER_SIZE = 7`. See `db/log_format.h`.
- [x] **`log::Writer`** (`src/log/writer.rs`): Wraps `BufWriter<File>` directly — no `Env` abstraction yet (deferred to
  Phase 9). `add_record(&[u8])` fragments the payload across block boundaries; pads trailing fewer-than-7-byte remnants
  with zeros; pre-computes per-type CRCs on construction. Constructor: `new(file: File, dest_length: u64)` where
  `dest_length` allows resuming an existing file. See `db/log_writer.h/cc`.
- [x] **`log::Reader`** (`src/log/reader.rs`): Wraps `File`. `read_record() -> Option<Vec<u8>>` reassembles
  multi-fragment records into a scratch buffer. Optional CRC verification (`checksum: bool`). Reports corruption via a
  `Reporter` trait (one method: `corruption(bytes: u64, reason: &str)`). Resynchronisation: when `initial_offset > 0`,
  skips `Middle`/`Last` fragments until a `Full` or `First` is found. See `db/log_reader.h/cc`.

**Step 3 — Integration**

- [x] **`Db::open(path)`** (`src/lib.rs` or `src/db/mod.rs`): Creates the directory if absent; opens or creates
  `<path>/000001.log`; constructs `Db` with a live `log::Writer`. Returns `Result<Db, Error>`. `Db::default()` retained
  for in-memory/test use (no WAL). Full MANIFEST-driven log-number tracking deferred to Phase 9.
- [x] **`Db::write` wired to WAL**: Change signature to `fn write(&self, opts: &WriteOptions, batch: &WriteBatch) ->
  Result<(), Error>`. Under the write lock: read `start_seq`, call `batch.set_sequence(start_seq)`, call
  `log_writer.add_record(batch.contents())`, sync if `opts.sync`, iterate into memtable, advance `last_sequence`.
  The stamp must precede the log write so WAL records carry the correct embedded sequence for recovery replay.
  `Db::put`/`delete` updated to forward a default `WriteOptions`.
- [x] **Recovery** (`Db::open` calls this when WAL exists): Construct a `log::Reader` from offset 0; for each record
  decode it as a `WriteBatch` and replay via `batch.iterate(&mut inserter)` using the batch's embedded sequence; after
  all records restore `last_sequence` to the highest sequence seen. Incomplete trailing records (torn write on crash)
  are silently ignored.

### Phase 3 — SSTable format + L0 flush + disk reads

*The on-disk immutable file format, wired end-to-end: memtable flushes to L0 SSTables and `Db::get` falls through to
disk on in-memory misses. Mirrors the approach taken in Phase 2, where the WAL was integrated rather than deferred.*

**SSTable primitives**

- [x] **`Block`** (`src/table/block.rs`): Delta-encoded key-value pairs with restart points every N keys (default 16).
  Each entry: `[shared_len: varint][unshared_len: varint][value_len: varint][key_suffix][value]`. Restart point array at
  block tail enables binary search. See `table/block.h/cc`.
- [x] **`BlockIterator`** (`src/table/block.rs`): Decodes delta-encoded entries on the fly; jumps to restart points for
  seeks using `cmp_internal_keys` (user_key ASC, seq DESC). Needed by `Table::get` to decode data blocks — pulled
  forward from Phase 6. See `table/block.cc`.
- [x] **`BlockBuilder`** (`src/table/block_builder.rs`): Accumulates entries into a `Block`, tracking the last key for
  delta encoding. See `table/block_builder.h/cc`.
- [x] **`BlockHandle` / `Footer`** (`src/table/format.rs`): `BlockHandle` is an (offset, size) pair encoded as two
  varints. Footer is 48 bytes: metaindex handle + index handle + 8-byte magic (`0xdb4775248b80fb57`). Also contains
  `make_internal_key`, `parse_internal_key`, `cmp_internal_keys`. See `table/format.h`.
- [x] **`TableBuilder`** (`src/table/builder.rs`): Sequentially appends sorted key-value pairs; writes data blocks,
  index block, empty metaindex block, and footer. NoCompression only (Phase 3). No filter blocks. Index block uses
  `restart_interval=1`. See `include/leveldb/table_builder.h` and `table/table_builder.cc`.
- [x] **`Table`** (`src/table/reader.rs`): Random-access reader; reads footer + index block on `open`; `get(user_key)`
  searches index block, reads data block via `pread` (`FileExt::read_at`), returns value/tombstone/None.
  See `table/table.cc`.

**Memtable iteration** *(pulled forward from Phase 6 — required for flush)*

- [x] **`SkipList::iter()`**: Forward-only iteration over the skip list (backward deferred to Phase 6). Yields entries
  in internal-key order (user key ASC, sequence DESC) via `SkipListIter<'a>` (walks level-0 links with Acquire loads).
- [x] **`MemTableIterator`**: Thin forward iterator over `SkipList::iter()`, decoding each entry via `Entry::from_slice`
  to expose `ikey()` (SSTable internal key: `user_key || tag`) and `value()` to the flusher. `Entry::key()`/`value()`
  lifetimes refined to `&'a [u8]` so values borrow from the arena, not from the short-lived `Entry` view.

**L0 flush + disk read wiring**

- [x] **`Arena::memory_usage()`**: Tracks bytes via an explicit `AtomicUsize` counter (bumpalo's `allocated_bytes()`
  returns chunk *capacity*, not used bytes). `Memtable::approximate_memory_usage()` delegates to this counter so `Db`
  can compare against `Options::write_buffer_size`.
- [x] **`Db::open` takes `Options`**: Pass `write_buffer_size` (and future options) through to the database.
- [x] **L0 flush** (`Db`): When `mem` exceeds `write_buffer_size` after a write, `std::mem::replace` swaps in a fresh
  `Memtable`, the old one is iterated via `MemTableIterator` and flushed to `<path>/<number>.ldb` via `TableBuilder`,
  then the opened `Table` is prepended to `l0_files: Vec<(u64, Table)>`. `scan_next_file_number` prevents file-number
  collisions across sessions. All mutable state lives in `Mutex<DbState>`.
- [x] **`Db::get` disk fallback**: On miss in `mem`, scan `l0_files` newest-first, calling `Table::get` on each.
  `LookupResult::{Value, Deleted, NotInTable}` lets tombstones stop the search without continuing to older files.
  No multi-level routing yet — deferred to Phase 9.

### Phase 4 — Iterators

*Required by `Db::NewIterator` and internally by compaction. Forward `SkipList` iteration, `MemTableIterator`, and
`BlockIterator` were pulled into Phase 3; this phase completes the forward iterator stack. Backward iteration is
deferred to post-parity as it is not needed for compaction or the core read path.*

- [x] **`TwoLevelIterator`** (`src/table/two_level_iterator.rs`): Composes an index iterator and a block-opener
  function, yielding a transparent view over all blocks in an SSTable. `BlockFn` closure captures a cloned `File`
  handle and opens data blocks on demand. `BlockIter` (renamed from `BlockIterator<'a>`) now owns an `Arc<Vec<u8>>`
  so it can be stored without a lifetime parameter. `InternalIterator` trait in `src/iter.rs` provides the shared
  interface used by `TwoLevelIterator`, `MergingIterator`, and `DbIterator`. See `table/two_level_iterator.h/cc`.
- [x] **`MergingIterator`** (`src/db/merge_iter.rs`): N-way merge of sorted iterators using a heap or linear scan
  (LevelDB uses linear for small N). See `table/merger.h/cc`.
- [x] **`DbIterator`** (`src/db/db_iter.rs`): Wraps `MergingIterator`; applies snapshot sequence-number filtering,
  merges value versions, skips tombstones for the user. Forward-only for now. See `db/db_iter.h/cc`.

### Phase 5 — Version management

*Tracks the set of live SSTable files across levels, enables MANIFEST-based recovery, and gates compaction. Open
`Table` handles are stored as `Arc<Table>` directly in `FileMetaData` — no LRU table cache needed for correctness;
that is a post-parity optimisation.*

- [x] **`FileMetaData`** (`src/db/version_edit.rs`): File number, size, smallest/largest internal key bytes,
  `Option<Arc<Table>>` for the open reader (`Some` for all files in a live `Version`; `None` only during MANIFEST
  replay). `FileMetaData::with_table` is the canonical constructor. See `db/version_edit.h`.
- [x] **`VersionEdit`** (`src/db/version_edit.rs`): Atomic delta — files added/removed per level, log number, next
  file number, last sequence. LevelDB-compatible tag-based binary encoding; `table` field is in-memory only and not
  encoded. `encode`/`decode` with forward-compatible unknown-tag handling. See `db/version_edit.h/cc`.
- [x] **`Version`** (`src/db/version.rs`): Snapshot of the LSM structure — `[Vec<Arc<FileMetaData>>; 7]`.
  L0 newest-first; L1–L6 sorted by smallest key. `get(user_key, seq)` scans L0 then L1–L6 linearly (binary
  search deferred to Phase 6). Reference-counted via `Arc<Version>`. See `db/version_set.h/cc`.
- [x] **`VersionSet`** (`src/db/version_set.rs`): Maintains current `Arc<Version>`, MANIFEST log, and
  file-number/sequence state. `create` writes `MANIFEST-000002` + `CURRENT`; `recover` replays MANIFEST via a
  `Builder` that opens Tables for all live files; `log_and_apply` appends to MANIFEST and installs a new `Version`.
  `DbState` replaces `l0_files`/`next_file_number` with `version_set: Option<VersionSet>`; `Db::open` uses
  MANIFEST-driven recovery with WAL replay filtered to `seq > manifest_last_sequence`. See `db/version_set.h/cc`.

### Phase 6 — Full database

*Completes the user-facing API to match LevelDB's `include/leveldb/db.h` and wires all prior phases into a
crash-safe database with multi-level compaction. MANIFEST-driven recovery and level routing are done (Phase 5);
what remains is compaction, the full Iterator/Snapshot API, and operational hygiene (WAL rotation, file GC).*

**Operational correctness**

- [x] **WAL rotation after flush**: `begin_flush` allocates a new WAL file number and creates the new log file
  under the write lock; `finish_flush` calls `vs.set_log_number(new)` before `log_and_apply` so the MANIFEST
  atomically records the new SST and the new log number; then swaps `state.log` and deletes the old WAL.
  Prevents unbounded WAL growth. See `db/db_impl.cc: DBImpl::MakeRoomForWrite`.
- [x] **`DeleteObsoleteFiles`**: After every flush, `delete_obsolete_files` takes the lock briefly to snapshot the
  live `.ldb` set (`VersionSet::add_live_files`), `log_number`, and `manifest_number`, then releases the lock and
  scans the directory. Keep rules: `.log` ≥ `log_number`, `MANIFEST-*` ≥ `manifest_number`, `.ldb` in live set,
  `CURRENT`/`LOCK` always kept. `parse_db_filename` mirrors LevelDB's `ParseFileName`. Called in `Db::write` after
  `finish_flush` drops its lock. See `db/db_impl.cc: DBImpl::RemoveObsoleteFiles`.

**Compaction**

- [x] **L0→L1 compaction**: Triggered when L0 file count ≥ 4 (`L0_COMPACTION_TRIGGER`). `pick_compaction` selects
  all L0 files plus any L1 files whose user-key range overlaps the union L0 range. `do_compaction` (no lock) builds
  a `MergingIterator` over all inputs (L0 newest-first so ties resolve to newer versions), applies shadow-key
  pruning (only the newest version of each user key is kept) and tombstone elision (deletion markers are dropped
  when no data exists at L2+). Writes output SSTables to L1 (new file per `max_file_size`). `install_compaction`
  (under lock) records a single `VersionEdit` deleting all inputs and adding all outputs, then calls
  `log_and_apply`. `log_and_apply` now sorts L1–L6 files by smallest user key after each edit. `maybe_compact`
  orchestrates the three phases; called from `Db::write` in a loop (up to 32×) after every flush. Slow-write
  throttle: sleeps 1 ms per iteration when L0 ≥ `L0_SLOWDOWN_WRITES_TRIGGER` (8). No background thread — runs
  synchronously on the writing goroutine. `compact_pointer` round-robin not implemented (all L0 files always
  selected). See `db/db_impl.cc: DBImpl::BackgroundCompaction`, `DoCompactionWork`, `MakeRoomForWrite`.
- [x] **`Db::compact_range(begin, end)`**: Manual compaction of a user-key range (`Option<&[u8]>` for open bounds).
  Finds the deepest level with files overlapping `[begin, end]`, then calls `compact_level_range` for each level
  from 0 to `max_level - 1`.  Each call uses the three-phase lock protocol; newly compacted files are visible to
  subsequent levels' compactions via a fresh version snapshot.  `pick_range_compaction` selects files at `level`
  that overlap `[begin, end]` plus next-level files that overlap the union range; reuses `do_compaction` and
  `install_compaction`.  No-op on in-memory databases.  See `db/db_impl.cc: DBImpl::CompactRange`.

**Iterator and snapshot API** *(all required by `include/leveldb/db.h`)*

- [x] **Backward iteration**: `SkipListIter::seek_to_last()` / `prev()` (scan level-0 from head; no back-pointers),
  `BlockIter::seek_to_last()` (last restart + scan forward) / `prev()` (binary-search restart points, scan to
  predecessor), `TwoLevelIterator::prev()` + `skip_empty_data_blocks_backward()`, `MergingIterator::prev()` /
  `find_largest()` (symmetric to `find_smallest`), `DbIterator::prev()` / `seek_to_last()` / `find_prev_user_entry()`
  (LevelDB-style direction field; Reverse direction stores entry in `saved_key`/`saved_value`; direction switch in
  `next()` handled). `InternalIterator` trait extended with `seek_to_last` + `prev`. Public `DbIter` exposes both.
  26 new unit tests across all layers; 161 tests total. See `table/iterator.h` and `db/db_iter.cc`.
- [x] **`Db::new_iterator(read_opts)`**: Returns `Result<DbIter, Error>`. Pins `Arc<Memtable>` for `mem` and `imm`
  via `ArcMemTableIter` (owned wrapper using `unsafe` transmute of `'a` lifetime, backed by the Arc). Creates
  `TwoLevelIterator` per SSTable from the current `Version`. Builds `MergingIterator` → `DbIterator`. Exposes:
  `valid`, `seek_to_first`, `seek_to_last`, `seek`, `next`, `prev`, `key`, `value`, `status`. `DbState.mem` and
  `.imm` changed to `Arc<Memtable>` to enable Arc cloning for the iterator. See `db/db_impl.cc`.
- [x] **`Db::GetSnapshot` / `ReleaseSnapshot`**: `get_snapshot()` locks, reads `last_sequence`, inserts into
  `DbState::snapshots: BTreeSet<u64>`, returns `Snapshot(seq)`. `release_snapshot(snap)` removes from the set.
  Compaction (`do_compaction`, `maybe_compact`, `compact_level_range`) uses `oldest_snapshot = snapshots.min()
  .unwrap_or(last_sequence)` as the visibility cutoff so entries observable through any live snapshot are not
  pruned. See `db/snapshot.h`.
- [x] **`Db::get` snapshot support**: `Db::get` now takes `&ReadOptions` as first argument. Snapshot seq comes
  from `opts.snapshot.map(|s| s.0).unwrap_or(last_sequence)`. `Memtable::get` changed to take `seq: u64`;
  uses `write_seek_key_to(key, seq)` instead of `write_lookup_to(key)` so reads are snapshot-aware.
  `lookup_size`/`write_lookup_to` gated with `#[cfg(test)]`. 4 new snapshot tests; 165 total.
  See `db/db_impl.cc: DBImpl::Get`.

**Remaining user-facing API surface** *(in `include/leveldb/db.h`)*

- [x] **`Db::GetProperty(property)`**: `get_property(&self, property: &str) -> Option<String>`. Supported
  properties: `"leveldb.num-files-at-level<N>"` (file count at level N), `"leveldb.stats"` (per-level file
  count and size; time/read/write columns emit `0` since compaction stats are not tracked),
  `"leveldb.sstables"` (one line per SSTable with file number, size, and escaped user-key range),
  `"leveldb.approximate-memory-usage"` (memtable bytes). Returns `None` for unknown properties or invalid level
  numbers. Helpers `num_files`, `level_bytes`, `debug_string` added to `Version`.
  See `db/db_impl.cc: DBImpl::GetProperty`.
- [x] **`Db::GetApproximateSizes(ranges)`**: Given a slice of `(start_key, end_key)` ranges, return approximate
  on-disk byte count for each range by walking the index blocks of overlapping SSTables. Used by tools to estimate
  data distribution. `Version::approximate_offset_of` accumulates `file_size` for files entirely before the key
  and delegates to `Table::approximate_offset_of` (index-block seek, returns block start offset or `metaindex_offset`
  as end-of-data proxy) for straddling files. Returns `0` for in-memory databases.
  See `db/db_impl.cc: DBImpl::GetApproximateSizes`.
- [x] **`Db::destroy(path)`**: Associated function (no open instance needed). Acquires `LOCK` non-blocking —
  returns `IoError` immediately if another process holds it. Enumerates the directory via `parse_db_filename`,
  deletes every recognised file except `LOCK`, then drops the flock and deletes `LOCK`. Calls `remove_dir`
  (not `remove_dir_all`) — silently ignores failure so unrecognised files are left in place.
  Returns `Ok(())` if `path` does not exist. See `db/db_impl.cc: DestroyDB`.

**`LOCK` file** *(prerequisite for the above free functions and safe multi-process use)*

- [x] **`LOCK` file**: On `Db::open`, acquire an exclusive `flock` on `<path>/LOCK` to prevent two processes from
  opening the same database simultaneously. Release on `Db::drop`. `acquire_lock` uses `libc::flock(LOCK_EX |
  LOCK_NB)` — returns `IoError` immediately rather than blocking if another process holds the lock. The `File`
  handle is stored as `lock_file: Option<File>` in `Db` (suppressed `dead_code` warning; held for Drop side-effect).
  See `util/env_posix.cc: LockFile`.

**Compression**

- [x] **Bloom filters**: Kirsch-Mitzenmacher double-hashing; `k = round(0.69 * bits_per_key)` hash functions; one filter
  per ~2 KB of data blocks stored in the SSTable filter block. The filter block is written uncompressed and read at
  `Table::open`; `Table::get` checks it after the index-block lookup and before reading or decompressing any data
  block, so a definite-negative skips all I/O and decompression entirely. A `FilterPolicy` trait allows custom
  filters. `Options::filter_policy` wires the chosen policy into flush and compaction.
  See `util/bloom.cc` and `include/leveldb/filter_policy.h`.
- [x] **Block compression**: Wire `Options::compression` and `Options::zstd_compression_level` through `TableBuilder`
  and `Table`.  `write_raw_block` in `src/table/format.rs` compresses data blocks via `snap` (Snappy) or `zstd`
  crates; falls back to NoCompression when compressed output is not at least 12.5% smaller (LevelDB's
  `kCompressionRatio` heuristic).  `read_block` decompresses based on the trailer byte (`0x00` = none, `0x01` =
  Snappy, `0x02` = Zstd).  Filter, metaindex, and index blocks are always written uncompressed (matching LevelDB).
  `TableBuilder::new` accepts `compression: CompressionType` and `zstd_level: i32`; both are passed from flush and
  compaction via `Options`.  Default remains `Snappy` (matching LevelDB).

**Options wiring**

- [x] **`ReadOptions::verify_checksums` + `Options::paranoid_checks`**: Both are fully wired.
  `verify_checksums = opts.verify_checksums || self.options.paranoid_checks` is computed in `Db::get_with_options`
  and `Db::new_iterator` and forwarded to `Version::get(verify_checksums)` and `Table::new_iterator(verify_checksums)`
  (which threads it into the `BlockFn` closure).  `Options::paranoid_checks` is also passed to `VersionSet::recover`
  and `recover_wal` as the `checksum` argument to `log::Reader::new`, so MANIFEST and WAL records are verified during
  recovery.  See `db/db_impl.cc: DBImpl::Get`, `table/table.cc: Table::InternalGet`.

---

### Post-parity improvements

*Optimisations and secondary features to add once RoughDB has a fully functional database. None are prerequisites for
the phases above.*

- **Batch-grouped writes**: Multiple concurrent `Db::write` callers are grouped by a leader that writes a single
  combined WAL record and memtable batch, amortising `fsync` cost over many writers. Transparent to callers; the
  current single-writer-at-a-time path is correct. See `db/db_impl.cc: DBImpl::Write`.
- **Block cache**: Sharded LRU (`src/cache/lru.rs`) with two lists per shard (in-use / evictable), custom deleters,
  capacity-based eviction. Default 8 MB. Required by `Table` reader to cache hot blocks. See `util/cache.cc`.
- **`ReadOptions::fill_cache`**: Currently ignored — no block cache exists. Once the block cache is implemented,
  `fill_cache = false` should skip inserting blocks into the cache on reads (useful for bulk scans to avoid
  evicting hot data). Wire into `read_block` and the `TwoLevelIterator` block-opener closure.
- **Table cache**: LRU of `(file_number → Table)` entries replacing the `Arc<Table>` in `FileMetaData`. Bounds open
  file descriptors and avoids repeated footer parses. See `db/table_cache.h/cc`.
- **`Options::max_open_files`**: Currently ignored — all SSTable `File` handles stay open unconditionally via
  `Arc<Table>` in `FileMetaData`. Once the table cache is implemented, pass
  `max_open_files - kNumNonTableCacheFiles` as its capacity. See `db/db_impl.cc: DBImpl::Open`.
- **`Options::reuse_logs`**: Experimental LevelDB flag — when set, the existing WAL and MANIFEST files are reused
  on `Db::open` rather than creating new ones after recovery, saving an `fsync` of `CURRENT`. Requires MANIFEST
  recovery to detect the reuse case and skip writing a new `CURRENT`. Low priority.
  See `db/db_impl.cc: DBImpl::Recover`.
- **Custom comparator**: Wire `Options::comparator` through to all key-comparison sites (skip list, block seek,
  compaction). Currently hardcoded bytewise. Needed to use RoughDB as a sorted map on non-lexicographic keys.
  See `include/leveldb/comparator.h`.
- **`RepairDB(path, options)`**: Scans the database directory, recovers as many SSTables as possible from a corrupt
  or partial MANIFEST, and rebuilds a valid MANIFEST. See `db/repair.cc`.
- **`Db::flush_wal(sync: bool)` / `Db::sync_wal()`**: RocksDB-style explicit WAL flush. `flush_wal` pushes buffered
  data from the `BufWriter` to the OS page cache; `sync = true` also `fsync`s. Useful for amortising `fsync` cost
  over many `sync=false` writes. Currently `Db::drop` relies on `BufWriter`'s implicit flush-on-drop, which silences
  I/O errors. See `DB::FlushWAL` / `DB::SyncWAL` in `include/rocksdb/db.h`.
- **WAL file recycling** (RocksDB): Reuse old log files from a pool rather than deleting and recreating them, avoiding
  inode churn on slow filesystems. Uses a 12-byte header (standard 7 bytes + `log_number: u32`) so the reader can
  distinguish recycled files. See `db/log_writer.cc` in RocksDB.
- **Ribbon filters** (RocksDB): Drop-in replacement for Bloom filters offering the same false-positive rate in ~30%
  fewer bits. See `util/ribbon_impl.h` in RocksDB.

---

## Key conventions

- `rustfmt.toml` sets `tab_spaces = 2`.
- `unsafe` blocks must carry a `// SAFETY:` comment; `unsafe fn` must carry a `# Safety` doc section.
- `find_splice_for_level` and `recompute_splice_levels` are free functions (not `SkipList` methods) to avoid a
  simultaneous `&self` / `&mut self.splice` borrow conflict — a pattern to follow whenever a method needs both `&self`
  and `&mut self.field`.
- Methods or functions only used in tests are gated with `#[cfg(test)]`.
- Prefer encoding directly into arena/pre-allocated memory (see `Entry` helpers) over intermediate `Vec` allocations on
  hot paths.
- Run `cargo fmt` on changesets
- Have text and other files hardwrap at 120 character lines
