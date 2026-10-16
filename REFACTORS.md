# Refactors towards more idiomatic Rust

Worked through top to bottom; each item lands in its own commit and is ticked off here when it does.  Items 1–5
change no public API.  Items 6–10 do, and are acceptable as a 0.x minor bump.

## Internal (no public API change)

- [x] **1. `lib.rs` free functions become methods on `DbInner` / `DbState`.**  About a dozen free functions take
  `&Mutex<DbState>` or `&mut DbState` plus `path`, `opts`, `tc` or `fs`, all of which are fields of `DbInner` or
  `Persistence`: `compact_level_range(path, state, opts, tc, level, begin, end)`, `maybe_compact(path, state, opts,
  tc)`, `delete_obsolete_files(path, state, fs)`, `begin_flush(path, state, comparator, fs)`, …  As methods, the
  pass-through parameters disappear.
  *Done:* scheduling, the background loop (`run_background_work`), flush phases 1 and 3, compaction and file GC are
  `DbInner` methods; lock-held transitions (`take_group_batch`, `update_stats`, `install_*`, and a new
  `oldest_snapshot` replacing two copies) are `DbState` methods; flush phase 2 is `FlushPrep::write_table`.
  `begin_flush` also hands the sealed memtable to the background thread, which its three callers each did by hand.

- [x] **2. Typed internal keys.**  `parse_internal_key` returns `Option<(&[u8], u64, u8)>`, and `ValueType` is
  private to `memtable/entry.rs`, so other modules compare against bare literals (`vtype == 0` in
  `db/compaction.rs` and `db/db_iter.rs`, seek keys built with a literal `1`).  One crate-wide `ValueType` and a
  `ParsedInternalKey { user_key, sequence, value_type }` (as in LevelDB) make every match exhaustive.
  *Done:* `ValueType` and `ParsedInternalKey` live in `table/format.rs`; memtable entries and `WriteBatch` records use
  the same `ValueType` instead of their own copies.  `parse_internal_key` rejects unknown type bytes, as LevelDB's
  `ParseInternalKey` does.  The public `CompactionFilter::filter` still takes a raw `u8`: see item 11.

- [ ] **3. Impossible `DbState` combinations become unrepresentable.**  `imm` and `pending_flush` always change
  together (seven sites test `imm.is_some() || pending_flush.is_some()`): one flush-state enum.  `Option<VersionSet>`
  and `Option<Persistence>` both encode "is this database persistent?" (21 unwraps, 4 `expect`s): an in-memory
  database gets an empty `VersionSet` instead, leaving `Option<Persistence>` the one place that fact lives.

- [ ] **4. One LRU, O(1).**  `TableCache` and `BlockCache` each carry a copy of a `VecDeque` + `HashMap` LRU whose
  every hit runs `order.iter().position(..)`: a linear scan under a global mutex on the read path (the 8 MiB
  default block cache holds ~2,000 4 KiB blocks).  One internal generic LRU with O(1) operations, measured with
  `benches/db.rs` before and after.

- [ ] **5. Named types instead of tuples and mixed level integers.**  `VersionEdit` carries
  `Vec<(i32, Arc<FileMetaData>)>`, `Vec<(i32, u64)>` and `Vec<(i32, Vec<u8>)>`; `seek_compact_file` is an
  `Option<(Arc<FileMetaData>, usize)>`; `pick_range_compaction` returns a bare pair of input lists.  Levels are
  `i32` in edits but `usize` everywhere else (7 `as i32` casts in `lib.rs`): `i32` only in the encoder and decoder.

## Public API (breaking)

- [ ] **6. `get` returns `Result<Option<Vec<u8>>, Error>`; `Error::NotFound` goes.**  A missing key is not a
  failure, yet every caller writes `Err(e) if e.is_not_found()` (README, crate docs, `examples/cli.rs`, benches).
  `NotFound` is LevelDB's C++ `Status` showing through.

- [ ] **7. Error type polish.**  `Display` as lowercase one-liners (today `"NotFound"`, `"Corruption: …"`);
  `#[non_exhaustive]` (used nowhere in the crate); `IoError(Arc<io::Error>)`, so that the `Clone` the sticky
  background error needs keeps the original error instead of rebuilding it from `kind()` and `to_string()`.

- [ ] **8. Rust naming and ranges.**  No `get_` prefixes: `get_snapshot` → `snapshot`, `get_property` →
  `property`, `get_approximate_sizes` → `approximate_sizes`.  Ranges as Rust ranges: `db.compact_range(..)` rather
  than two `Option<&[u8]>`, and `&[Range<&[u8]>]` rather than `&[(&[u8], &[u8])]`.

- [ ] **9. Open mode as an enum.**  `Options::create_if_missing` and `error_if_exists` admit "may not create, must
  not exist", which can only fail.  One `OpenMode` (open existing / open or create / create new, like
  `std::fs::OpenOptions`) replaces both; clippy's `struct_excessive_bools` flags `Options` today.

- [ ] **10. `WriteBatch` iteration as an iterator.**  `iterate(&mut dyn Handler)` is a visitor callback; an
  iterator of `BatchOp`s is the idiomatic shape.  `Inserter` becomes a plain loop and the public `Handler` trait
  goes.

- [ ] **11. `CompactionFilter::filter` takes a value-type enum.**  Its `value_type: u8` argument is documented as
  "`1` = Value, `0` = Deletion"; a public enum (the crate's `ValueType`, or one of its own) says that in the type.
  Found while doing item 2.

## Small items

- [ ] `Db`, `DbIter` and `WriteBatch` implement `Debug`.
- [ ] A test pins `Db: Send + Sync`.
- [ ] `Snapshot` is `#[must_use]` (`let _ = db.get_snapshot()` releases it immediately).
- [ ] `Drop for Db` reports a background-thread panic instead of discarding it (`t.join().ok()`).
- [ ] Public functions document `# Errors` and `# Panics` (34 and 8 missing under clippy's pedantic lints); the 4
  wildcard matches on crate enums become exhaustive.
- [ ] `Cargo.toml` declares a `rust-version`.
