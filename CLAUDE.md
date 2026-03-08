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

- [ ] **`FileMetaData`**: File number, size, smallest/largest `InternalKey`, `Arc<Table>` for the open reader.
  See `db/version_edit.h`.
- [ ] **`VersionEdit`** (`src/db/version_edit.rs`): Atomic delta — files added/removed per level, log number, next
  sequence, compact pointers. Serialised to the MANIFEST. See `db/version_edit.h/cc`.
- [ ] **`Version`** (`src/db/version.rs`): Snapshot of the LSM structure — per-level `Vec<Arc<FileMetaData>>`.
  Reference-counted; lives until all iterators opened on it are dropped. See `db/version_set.h/cc`.
- [ ] **`VersionSet`** (`src/db/version_set.rs`): Maintains the linked list of `Version`s, the current version, the
  MANIFEST log, and compact-pointer state. Applies `VersionEdit`s atomically. See `db/version_set.h/cc`.

### Phase 6 — Full database

*Ties all prior phases into a working, persistent, crash-safe database with multi-level compaction.*

- [ ] **`Db::Open`** (full): MANIFEST-driven recovery; replays the WAL from the sequence in the MANIFEST, rebuilds
  `VersionSet`, re-opens all live SSTable files. Replaces the Phase 3 flat `Vec<Table>` with proper level routing.
  Rotates the WAL after each memtable flush (truncates old log). See `db/db_impl.cc: DBImpl::Recover`.
- [ ] **`Db::Write`** (batch-grouped): Multiple concurrent writers are grouped; only the leader writes to the WAL and
  inserts into the memtable. See `db/db_impl.cc: DBImpl::Write`.
- [ ] **Background compaction**: Triggered when L0 file count ≥ 4 (slow writes at 8, stop at 12). Selects input
  files, runs a `MergingIterator`, drops shadowed versions and obsolete tombstones, writes output files to L+1.
  Managed in `db/db_impl.cc: BackgroundCompaction`.
- [ ] **`Db::NewIterator`**: Returns a `DbIterator` over a merged view of mem + imm + all version files, pinned to
  the current sequence number.
- [ ] **`Db::GetSnapshot` / `ReleaseSnapshot`**: Snapshots are sequence numbers in a doubly-linked list; they
  prevent compaction from dropping versions still visible to a live snapshot.
- [ ] **`Db::CompactRange`**: Manual compaction of a user-specified key range.

---

### Post-parity improvements

*Optimisations and secondary features to add once RoughDB has a fully functional database. None are prerequisites for
the phases above.*

- **Bloom filters**: Kirsch-Mitzenmacher double-hashing; `k = round(0.69 * bits_per_key)` hash functions; one filter
  per ~2 KB of data blocks stored in the SSTable filter block. Avoids unnecessary block reads for missing keys.
  See `util/bloom.cc` and `include/leveldb/filter_policy.h`. A `FilterPolicy` trait allows custom filters.
- **Block cache**: Sharded LRU (`src/cache/lru.rs`) with two lists per shard (in-use / evictable), custom deleters,
  capacity-based eviction. Default 8 MB. Required by `Table` reader to cache hot blocks. See `util/cache.cc`.
- **Table cache**: LRU of `(file_number → Table)` entries replacing the `Arc<Table>` in `FileMetaData`. Bounds open
  file descriptors and avoids repeated footer parses. See `db/table_cache.h/cc`.
- **Backward iteration**: `SkipList::iter()` reverse, `MemTableIterator` backward, `DbIterator` backward seek/prev.
  Required for `Db::NewIterator` to support full bidirectional scans.
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
