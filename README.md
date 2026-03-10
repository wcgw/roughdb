# RoughDB

Embedded key-value storage written in Rust — a port of
[LevelDB](https://github.com/google/leveldb) to Rust.

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
- Multi-level compaction (L0→L1, `compact_range`) with snapshot-aware pruning and tombstone elision
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

**Not yet implemented:**

- Block cache (`ReadOptions::fill_cache` is accepted but ignored)
- Batch-grouped writes (writes are serialised one at a time)
- Custom comparators

## License

Apache License 2.0 — see [LICENSE](LICENSE).
