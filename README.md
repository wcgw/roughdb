# RoughDB

Embedded key-value storage written in Rust — a port of
[LevelDB](https://github.com/google/leveldb) to Rust.

[![Crates.io Version](https://img.shields.io/crates/v/roughdb)](https://crates.io/crates/roughdb)
[![docs.rs](https://img.shields.io/docsrs/roughdb)](https://docs.rs/roughdb/latest/roughdb/)

RoughDB is an LSM-tree key-value store with a LevelDB-compatible on-disk
format. It supports persistent (WAL + MANIFEST + SSTables) and in-memory
operation, multi-level compaction, and bidirectional iteration.

> [!WARNING]
> While I started this all "manually", many years ago... 
> This is an experiment of sort. While I'm relatively serious about this, I also used it more recently to experiment
> with different things, including generative AI. I'm reviewing all changes manually and carefully. 

## Getting started

```bash
cargo add roughdb
```

### Opening a database

```rust
use roughdb::{Db, Options};

let mut opts = Options::default();
opts.create_if_missing = true;

let db = Db::open("/tmp/my_db", opts)?;
```

Use `Db::default()` for a lightweight in-memory database (no WAL, no flush):

```rust
let db = Db::default();
```

### Basic reads and writes

```rust
use roughdb::{ReadOptions, WriteOptions};

// Write
db.put(b"hello", b"world")?;
db.delete(b"hello")?;

// Read
match db.get(b"hello") {
    Ok(value) => println!("got: {}", String::from_utf8_lossy(&value)),
    Err(e) if e.is_not_found() => println!("not found"),
    Err(e) => return Err(e),
}
```

### Atomic batch writes

```rust
use roughdb::{WriteBatch, WriteOptions};

let mut batch = WriteBatch::new();
batch.put(b"key1", b"val1");
batch.put(b"key2", b"val2");
batch.delete(b"old_key");

db.write(&WriteOptions::default(), &batch)?;
```

### Iterating over keys

Iterators start unpositioned — call `seek_to_first`, `seek_to_last`, or
`seek` before reading.

```rust
use roughdb::ReadOptions;

let mut it = db.new_iterator(&ReadOptions::default())?;

// Forward
it.seek_to_first();
while it.valid() {
    println!("{:?} = {:?}", it.key(), it.value());
    it.next();
}

// Backward
it.seek_to_last();
while it.valid() {
    println!("{:?}", it.key());
    it.prev();
}

// Range scan from "b" onward
it.seek(b"b");
while it.valid() {
    println!("{:?}", it.key());
    it.next();
}
```

### Snapshots

A snapshot pins the database to a specific point in time; reads through it see
only writes that preceded the snapshot.

```rust
use roughdb::ReadOptions;

let snap = db.get_snapshot();

// Reads with this snapshot see the state at the moment it was taken.
let opts = ReadOptions { snapshot: Some(&snap), ..ReadOptions::default() };
let value = db.get_with_options(&opts, b"key")?;
let mut it = db.new_iterator(&opts)?;

// snap releases automatically when it goes out of scope.
```


### Manual compaction

```rust
// Compact everything
db.compact_range(None, None)?;

// Compact a specific key range
db.compact_range(Some(b"aaa"), Some(b"zzz"))?;
```

## Building and testing

```bash
cargo build
cargo test
cargo clippy
cargo bench      # Criterion benchmarks (benches/db.rs)
```

## Status

RoughDB is in active development. The on-disk format is LevelDB-compatible.

**Implemented:**

- WAL + MANIFEST + SSTable flush and crash-safe recovery
- L0→L1 compaction with snapshot-aware pruning and tombstone elision; manual `compact_range`
- Bidirectional iteration (`seek_to_first`, `seek_to_last`, `seek`, `next`, `prev`)
- Snapshots (`get_snapshot` / `release_snapshot`)
- Bloom filter support (`Options::filter_policy`)
- Block compression: Snappy (default) and Zstd (`Options::compression`)
- `get_property` — `leveldb.num-files-at-level<N>`, `leveldb.stats`, `leveldb.sstables`,
  `leveldb.approximate-memory-usage`
- `get_approximate_sizes` — byte-range estimation via index-block seeks
- `destroy` — safely removes a database directory
- `LOCK` file — prevents concurrent opens by multiple processes
- Table cache — LRU open-file-handle cache bounded by `Options::max_open_files`
- Block cache — LRU byte-capacity cache with per-table IDs; `ReadOptions::fill_cache`
- Batch-grouped writes — concurrent writers are grouped under one WAL record, amortising fsync cost

**Partial / known limitations:**

- **Compaction is L0→L1 only.** LevelDB uses a level-score system (L1 target 10 MB, 10× per level) to drive
  multi-level compaction. RoughDB only compacts when L0 reaches 4 files; L1–L6 can grow unbounded after L0 pressure
  subsides. `compact_range` works across all levels but automatic background compaction does not.
- **Compaction runs synchronously** on the writing thread. LevelDB runs a background thread so writes are never
  blocked waiting for compaction I/O. Large compactions will cause P99 write-latency spikes.
- **No seek-based compaction.** LevelDB tracks per-file seek counts and triggers compaction on hot files regardless
  of size; this read-sampling path (`RecordReadSample`) is not implemented.
- **No trivial-move optimisation.** When a single file at level N has no overlap with level N+2, LevelDB moves it
  without merging. RoughDB always merges all selected input files.
- **No grandparent-overlap limiting.** LevelDB stops growing a compaction output file early when it would overlap
  too many level-(N+2) files, reducing future compaction amplification. RoughDB sizes output files by
  `max_file_size` only.
- **All memtable flushes go to L0.** LevelDB's `PickLevelForMemTableOutput` can place a flush directly into L1 or
  L2 when there is no overlap, reducing L0 pressure. Not implemented.
- **`Options::reuse_logs`** is accepted but ignored.

**Not yet implemented:**

- **Custom comparators** — keys are always compared bytewise; `Options::comparator` does not exist. Applications
  that require a custom sort order (e.g. reverse, integer, prefix) cannot use RoughDB.
- **`Env` abstraction** — file I/O is hardcoded to the local POSIX filesystem. There is no way to inject a custom
  storage backend (in-memory, encrypted, cloud, etc.).
- **`RepairDB`** — recovery from a corrupt or partial MANIFEST by scanning surviving SSTables.
- **Info logging** — LevelDB writes compaction progress, recovery details, and errors to an `info_log`. RoughDB
  produces no log output.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
