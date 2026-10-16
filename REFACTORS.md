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

- [x] **3. Impossible `DbState` combinations become unrepresentable.**  `imm` and `pending_flush` always change
  together (seven sites test `imm.is_some() || pending_flush.is_some()`): one flush-state enum.  `Option<VersionSet>`
  and `Option<Persistence>` both encode "is this database persistent?" (21 unwraps, 4 `expect`s): an in-memory
  database gets an empty `VersionSet` instead, leaving `Option<Persistence>` the one place that fact lives.
  *Done:* `FlushState::{Idle, Pending(FlushPrep), Writing(imm)}` replaces the pair; `begin_flush` asserts it starts
  from `Idle`.  `VersionSet::in_memory` backs `Db::default()` with an empty `Version` and a MANIFEST that panics if
  written; `DbState::version_set` is no longer an `Option`, and `DbState::new` replaces five struct literals.  One
  visible effect: `get_property("leveldb.sstables")` on an in-memory database lists the seven empty levels, as on an
  empty persistent one, instead of returning `""`.

- [x] **4. One LRU, O(1).**  `TableCache` and `BlockCache` each carry a copy of a `VecDeque` + `HashMap` LRU whose
  every hit runs `order.iter().position(..)`: a linear scan under a global mutex on the read path (the 8 MiB
  default block cache holds ~2,000 4 KiB blocks).  One internal generic LRU with O(1) operations, measured with
  `benches/db.rs` before and after.
  *Done:* `cache::lru::Lru` (a slab threaded with an index-linked recency list, bounded by total charge) backs both
  caches.  `disk_read/random` went from 8.29 ms to 6.37 ms (−23%) per 10,000 gets; `disk_read/missing`, which takes
  no block-cache hits, is unchanged.

- [x] **5. Named types instead of tuples and mixed level integers.**  `VersionEdit` carries
  `Vec<(i32, Arc<FileMetaData>)>`, `Vec<(i32, u64)>` and `Vec<(i32, Vec<u8>)>`; `seek_compact_file` is an
  `Option<(Arc<FileMetaData>, usize)>`; `pick_range_compaction` returns a bare pair of input lists.  Levels are
  `i32` in edits but `usize` everywhere else (7 `as i32` casts in `lib.rs`): `i32` only in the encoder and decoder.
  *Done:* `VersionEdit` holds `NewFile`, `DeletedFile` and `CompactPointer` entries (built with `add_file` /
  `delete_file`), all with `usize` levels; `Version::compaction_level` is a `usize` too.  The seek nominee and
  `GetStats` carry a `LevelFile`; `Compaction` carries an `Option<CompactPointer>` instead of a whole `VersionEdit`;
  `pick_range_compaction` returns a complete `Compaction`, grandparents included.  Found on the way: `decode` did not
  bound levels, so a corrupt MANIFEST level panicked recovery with an out-of-bounds index; it is now `Corruption`.

## Public API (breaking)

- [x] **6. `get` returns `Result<Option<Vec<u8>>, Error>`; `Error::NotFound` goes.**  A missing key is not a
  failure, yet every caller writes `Err(e) if e.is_not_found()` (README, crate docs, `examples/cli.rs`, benches).
  `NotFound` is LevelDB's C++ `Status` showing through.
  *Done:* `get` and `get_with_options` return `Ok(None)` for an absent or deleted key; `Error::NotFound` and
  `Error::is_not_found` are gone.  README, crate docs, `examples/cli.rs` and the benches match; the benches now
  assert hits are present instead of only that the call succeeded.

- [x] **7. Error type polish.**  `Display` as lowercase one-liners (today `"NotFound"`, `"Corruption: …"`);
  `#[non_exhaustive]` (used nowhere in the crate); `IoError(Arc<io::Error>)`, so that the `Clone` the sticky
  background error needs keeps the original error instead of rebuilding it from `kind()` and `to_string()`.
  *Done:* `Display` reads `corruption: …`, `invalid argument: …`, `not supported: …`, `I/O error: …`; `Error` is
  `#[non_exhaustive]`, derives `Clone`, and documents each variant; a test pins `Send + Sync + 'static`.

- [x] **8. Rust naming and ranges.**  No `get_` prefixes: `get_snapshot` → `snapshot`, `get_property` →
  `property`, `get_approximate_sizes` → `approximate_sizes`.  Ranges as Rust ranges: `db.compact_range(..)` rather
  than two `Option<&[u8]>`, and `&[Range<&[u8]>]` rather than `&[(&[u8], &[u8])]`.
  *Done:* `snapshot`, `property`, `approximate_sizes`; `compact_range` takes `impl RangeBounds<&[u8]>` (`..`, or
  slices such as `start..=end`; literal keys need `.as_slice()`).  The old `get_approximate_sizes` docs claimed an
  empty `limit` meant "to the end": it sorts first, so such a range measured 0 bytes; the claim is gone.

- [x] **9. Open mode as an enum.**  `Options::create_if_missing` and `error_if_exists` admit "may not create, must
  not exist", which can only fail.  One `OpenMode` (open existing / open or create / create new, like
  `std::fs::OpenOptions`) replaces both; clippy's `struct_excessive_bools` flags `Options` today.
  *Done:* `Options::open_mode: OpenMode::{Existing (default), CreateIfMissing, CreateNew}`, `#[non_exhaustive]` and
  re-exported at the crate root; `struct_excessive_bools` no longer fires.

- [x] **10. `WriteBatch` iteration as an iterator.**  `iterate(&mut dyn Handler)` is a visitor callback; an
  iterator of `BatchOp`s is the idiomatic shape.  `Inserter` becomes a plain loop and the public `Handler` trait
  goes.
  *Done:* `WriteBatch::iter()` (and `&WriteBatch: IntoIterator`) yields `Result<BatchOp, Error>`; `Handler` is gone.
  `Inserter`, a safe type whose soundness rested on its callers, is now `unsafe fn insert_batch` with a `# Safety`
  section.  Found on the way: a record length near `u64::MAX` overflowed the end offset and panicked (in release
  too, as a bad slice) while replaying a damaged WAL; such a length is now `Corruption`.

- [x] **11. `CompactionFilter::filter` takes a value-type enum.**  Its `value_type: u8` argument is documented as
  "`1` = Value, `0` = Deletion"; a public enum (the crate's `ValueType`, or one of its own) says that in the type.
  Found while doing item 2.
  *Done:* `filter(level, key, value: CompactionValue)`, where `CompactionValue` is `Value(&[u8])` or `Deletion`, so a
  tombstone can no longer come with a value.  `ChangeValue` on a deletion is documented as, and now is, a no-op (its
  bytes used to be stored as the tombstone's unread value).  Not covered by a test: a tombstone only reaches the
  filter when the key has data below the output level, which `compact_range` alone cannot set up.

## Small items

- [ ] `Db`, `DbIter` and `WriteBatch` implement `Debug`.
- [ ] A test pins `Db: Send + Sync`.
- [ ] `Snapshot` is `#[must_use]` (`let _ = db.get_snapshot()` releases it immediately).
- [ ] `Drop for Db` reports a background-thread panic instead of discarding it (`t.join().ok()`).
- [ ] Public functions document `# Errors` and `# Panics` (34 and 8 missing under clippy's pedantic lints); the 4
  wildcard matches on crate enums become exhaustive.
- [ ] `Cargo.toml` declares a `rust-version`.
