# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

RoughDB is an embedded key-value store written in Rust, porting LevelDB to Rust. The LevelDB C++ source lives at `../leveldb/` (primary reference). RocksDB source lives at `../rocksdb/` and should only be consulted for improvements to existing LevelDB features — not for porting RocksDB-specific features.

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
- Skip list with RocksDB's `InlineSkipList` memory layout: every node is a single arena allocation containing the level-0 link, higher-level links prefixed before it (negative indexing), a `u32` payload length, and the varint-encoded entry inline. This keeps the hot level-0 link and payload in the same cache line.
- `Splice` caches `prev`/`next` from the last insert for O(1) amortised sequential inserts.
- Lock-free reads (Acquire/Release atomics); writes serialised by `RwLock`.

**Entry encoding** (`src/memtable/entry.rs`)
- Internal key format: `[klen: varint][key][seq: varint][vtype: u8][vlen: varint][value]`
- `vtype`: `0` = Deletion, `1` = Value.
- Ordering: key ASC, seq DESC — so a lookup key with `seq = u64::MAX` always seeks to the newest version of a user key.
- Allocation-free helpers (`write_value_to`, `write_lookup_to`, etc.) encode directly into arena memory.

**Arena** (`src/memtable/arena/bump.rs`)
- Thin wrapper around `bumpalo::Bump`, default 10 MB capacity.
- `allocate_aligned` panics on OOM (matching LevelDB: memtable OOM is unrecoverable).

---

## Roadmap

Features are listed in dependency order. Each phase must be complete before the next begins.

### Phase 1 — Core types

*Prerequisite for everything.*

- [x] **`Error`** (`src/error.rs`): `Result`-based error type with variants `NotFound`, `Corruption`, `InvalidArgument`, `NotSupported`, `IoError`. `Db::get/put/delete` now return `Result<_, Error>`. See `include/leveldb/status.h`.
- [x] **`WriteBatch`**: Serialises a sequence of Put/Delete operations into a byte buffer. Batch format: `[seq: u64][count: u32][records…]` where each record is `[vtype: u8][klen: varint][key][vlen: varint][value]`. Exposes `put`, `delete`, `iterate(Handler)`, `byte_size`. See `include/leveldb/write_batch.h` and `db/write_batch.cc`.
- [x] **`Options` / `ReadOptions` / `WriteOptions`**: Configuration structs. `Options` carries the write-buffer size, block size, compression type, filter policy, block cache handle. `WriteOptions` adds a sync flag. `ReadOptions` adds a snapshot handle and checksum-verify flag. See `include/leveldb/options.h`.

### Phase 2 — Write-Ahead Log

*Enables crash durability. Must precede full `Db::Open`.*

- [ ] **Format** (`src/log/format.rs`): 32 KB blocks, each record `[crc32c: 4][len: 2][type: 1][data]`. Records spanning block boundaries are split into FIRST / MIDDLE / LAST fragments. See `db/log_format.h`.
- [ ] **`log::Writer`** (`src/log/writer.rs`): Appends records, handles block-boundary padding. See `db/log_writer.h/cc`.
- [ ] **`log::Reader`** (`src/log/reader.rs`): Reads and reassembles fragmented records; reports corruption via a callback. See `db/log_reader.h/cc`.

> **RocksDB improvement worth adopting**: WAL file recycling — pre-allocate a pool of log files and reuse them rather than creating/deleting, avoiding inode churn on filesystems that are slow at that. See `db/log_writer.cc` in RocksDB.

### Phase 3 — SSTable format

*The on-disk immutable file format. Required by flush and compaction.*

- [ ] **`Block`** (`src/table/block.rs`): Delta-encoded key-value pairs with restart points every N keys (default 16). Each entry: `[shared_len: varint][unshared_len: varint][value_len: varint][key_suffix][value]`. Restart point array at block tail enables binary search. See `table/block.h/cc`.
- [ ] **`BlockBuilder`** (`src/table/block_builder.rs`): Accumulates entries into a `Block`, tracking the last key for delta encoding. See `table/block_builder.h/cc`.
- [ ] **`BlockHandle` / `Footer`** (`src/table/format.rs`): `BlockHandle` is an (offset, size) pair encoded as two varints. Footer is 48 bytes: metaindex handle + index handle + 8-byte magic (`0xdb4775248b80fb57`). See `table/format.h`.
- [ ] **`TableBuilder`** (`src/table/builder.rs`): Sequentially appends sorted key-value pairs; writes data blocks, index block, optional filter block, and footer. See `include/leveldb/table_builder.h` and `table/table_builder.cc`.
- [ ] **`Table`** (`src/table/reader.rs`): Random-access reader; uses the index block to locate data blocks, optionally the filter block to skip misses. See `table/table.cc`.

### Phase 4 — Bloom filters

*Used by the SSTable filter block to avoid unnecessary block reads. Can be implemented alongside Phase 3.*

- [ ] **`BloomFilter`** (`src/filter/bloom.rs`): Kirsch-Mitzenmacher double-hashing; `k = round(0.69 * bits_per_key)` hash functions; one filter per ~2 KB of data blocks stored in the SSTable filter block. See `util/bloom.cc` and `include/leveldb/filter_policy.h`.
- [ ] **`FilterPolicy` trait**: Abstract interface so users can supply custom filters.

> **RocksDB improvement (optional, later)**: Ribbon filters offer the same false-positive rate in ~30% fewer bits. See `util/ribbon_impl.h` in RocksDB.

### Phase 5 — Block cache

*Required by the `Table` reader to cache hot blocks in memory.*

- [ ] **`LruCache`** (`src/cache/lru.rs`): Sharded LRU with two lists per shard (in-use / evictable), custom deleters, capacity-based eviction. Reference-counted handles prevent eviction of live entries. See `util/cache.cc` and `include/leveldb/cache.h`.
- [ ] Default capacity: 8 MB. Each shard is mutex-protected independently.

### Phase 6 — Iterators

*Required by `Db::NewIterator` and internally by compaction.*

- [ ] **`SkipList::iter()`**: Add forward and backward iteration to the existing skip list (currently supports seek/insert only).
- [ ] **`MemTableIterator`**: Thin wrapper over `SkipList::iter()` exposing the standard `Iterator` trait.
- [ ] **`BlockIterator`** (`src/table/block.rs`): Decodes delta-encoded entries on the fly, jumps to restart points for seeks. See `table/block.cc`.
- [ ] **`TwoLevelIterator`** (`src/table/two_level_iterator.rs`): Composes an index iterator and a block-opener function, yielding a transparent view over all blocks in an SSTable. See `table/two_level_iterator.h/cc`.
- [ ] **`MergingIterator`** (`src/db/merge_iter.rs`): N-way merge of sorted iterators using a heap or linear scan (LevelDB uses linear for small N). See `table/merger.h/cc`.
- [ ] **`DbIterator`** (`src/db/db_iter.rs`): Wraps `MergingIterator`; applies snapshot sequence-number filtering, merges value versions, skips tombstones for the user. See `db/db_iter.h/cc`.

### Phase 7 — Version management

*Tracks the set of live SSTable files and enables safe concurrent reads during compaction.*

- [ ] **`FileMetaData`**: File number, size, smallest/largest `InternalKey`. See `db/version_edit.h`.
- [ ] **`VersionEdit`** (`src/db/version_edit.rs`): Atomic delta describing a state change — files added/removed per level, log number, next sequence, compact pointers. Serialised to the MANIFEST. See `db/version_edit.h/cc`.
- [ ] **`Version`** (`src/db/version.rs`): Snapshot of the LSM structure — per-level list of `FileMetaData`. Reference-counted; lives until all iterators opened on it are dropped. See `db/version_set.h/cc`.
- [ ] **`VersionSet`** (`src/db/version_set.rs`): Maintains the linked list of `Version`s, the current version, the MANIFEST log, and compact-pointer state. Applies `VersionEdit`s to produce new versions. See `db/version_set.h/cc`.

### Phase 8 — Table cache

*Caches open `Table` file handles to avoid repeated file opens and footer parses.*

- [ ] **`TableCache`** (`src/db/table_cache.rs`): LRU of `(file_number → Table)` entries. Also the entry point for creating SSTable iterators during reads and compaction. See `db/table_cache.h/cc`.

### Phase 9 — Full database

*Ties all prior phases into a working, persistent, crash-safe database.*

- [ ] **`Db::Open`**: Create or recover an existing database. Recovery replays the WAL from the sequence number recorded in the MANIFEST to reconstruct the last memtable. See `db/db_impl.cc: DBImpl::Recover`.
- [ ] **`Db::Write`** (batch-grouped): Multiple concurrent writers are grouped; only the leader writes to the WAL and inserts into the memtable. See `db/db_impl.cc: DBImpl::Write`.
- [ ] **Memtable flush**: When `mem` exceeds `write_buffer_size` (default 4 MB), it is sealed as `imm` and a background task writes it to a new L0 SSTable via `TableBuilder`.
- [ ] **Background compaction**: Triggered when L0 file count ≥ 4 (slow writes at 8, stop at 12). Selects input files, runs a `MergingIterator` over them, drops shadowed versions and obsolete tombstones, and writes output files to L+1. Managed in `db/db_impl.cc: BackgroundCompaction`.
- [ ] **`Db::NewIterator`**: Returns a `DbIterator` over a merged view of mem + imm + all version files, pinned to the current sequence.
- [ ] **`Db::GetSnapshot` / `ReleaseSnapshot`**: Snapshots are sequence numbers stored in a doubly-linked list; they prevent compaction from dropping key versions still visible to a snapshot.
- [ ] **`Db::CompactRange`**: Manual compaction of a user-specified key range.

---

## Key conventions

- `rustfmt.toml` sets `tab_spaces = 2`.
- `unsafe` blocks must carry a `// SAFETY:` comment; `unsafe fn` must carry a `# Safety` doc section.
- `find_splice_for_level` and `recompute_splice_levels` are free functions (not `SkipList` methods) to avoid a simultaneous `&self` / `&mut self.splice` borrow conflict — a pattern to follow whenever a method needs both `&self` and `&mut self.field`.
- Methods or functions only used in tests are gated with `#[cfg(test)]`.
- Prefer encoding directly into arena/pre-allocated memory (see `Entry` helpers) over intermediate `Vec` allocations on hot paths.
