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

The public API surface is just `Db` in `src/lib.rs`, exposing `put`, `get`, and `delete`. `Db` holds a `Memtable` (`mem`) and an optional immutable memtable (`imm`); the disk layer is not yet implemented.

### Memtable (`src/memtable/`)

The memtable is backed by a custom skip list (`skiplist.rs`) that mirrors RocksDB's `InlineSkipList` memory layout. Every node is a single arena allocation:

```
raw ──►  [ AtomicPtr<Node> ] level h−1 ┐
         ...                           │ (height−1) prefix words
         [ AtomicPtr<Node> ] level   1 ┘
node ──► [ AtomicPtr<Node> ] level   0   ← Node struct
         [ u32 data_len ]
         [ varint-encoded Entry bytes ]
```

Higher-level next pointers live *before* the `Node` struct and are reached by subtracting from the level-0 pointer. This keeps the hot level-0 link and the payload in the same cache line.

Writes require `&mut SkipList` (the `Memtable`'s `RwLock` write guard provides this). Reads are lock-free using Acquire loads / Release stores on the next pointers.

The `Splice` struct caches `prev`/`next` from the last insert for O(1) amortised sequential inserts.

### Entry encoding (`src/memtable/entry.rs`)

All keys and values are encoded into a single byte string:

```
[klen: varint][key bytes][seq: varint][vtype: u8][vlen: varint][value bytes]
```

`vtype` is `0` (Deletion) or `1` (Value). Lookup keys omit the vtype/value suffix and use `seq = u64::MAX`, which sorts *before* all real entries for the same user key under the `Entry` ordering (key ASC, seq DESC), so a seek to a lookup key always lands on the newest version.

The arena-allocating constructors (`new_value`, `new_deletion`) are `#[cfg(test)]` only. The production path uses the static helpers (`encoded_value_size`, `write_value_to`, `write_lookup_to`, etc.) to encode directly into arena-allocated node memory with no intermediate allocation.

### Arena (`src/memtable/arena/bump.rs`)

Thin wrapper around `bumpalo::Bump` with a 10 MB default capacity. `allocate_aligned` (used by the skip list) panics on OOM, matching LevelDB's behaviour. `allocate` (zero-initialised) is `#[cfg(test)]` only.

## Key conventions

- `rustfmt.toml` sets `tab_spaces = 2`.
- `unsafe` blocks must carry a `// SAFETY:` comment; `unsafe fn` must carry a `# Safety` doc section.
- `find_splice_for_level` and `recompute_splice_levels` are free functions (not `SkipList` methods) to avoid a simultaneous `&self` / `&mut self.splice` borrow conflict.
- Methods or functions only used in tests are gated with `#[cfg(test)]`.
