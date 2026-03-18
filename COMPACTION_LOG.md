# Compaction Implementation Log

Tracks the plan and progress for closing the compaction gap between RoughDB and LevelDB.

## Status

| Gap | Description | Status |
|-----|-------------|--------|
| 1 | Level-score-based compaction | ✅ Done |
| 2 | Flush placement (`PickLevelForMemTableOutput`) | ✅ Done |
| 3 | Trivial-move optimisation | ✅ Done |
| 4 | Grandparent-overlap output limiting | ✅ Done |
| 5 | Seek-based compaction | ✅ Done |

---

## Dependency order

```
Gap 1 (level-score)
  ├── Gap 2 (flush placement)
  ├── Gap 3 (trivial move)
  ├── Gap 4 (grandparent limiting)
  └── Gap 5 (seek-based)
```

Gap 1 is the prerequisite for all others and the most important correctness fix: without it L1–L6
grow unbounded, since RoughDB currently only compacts when L0 reaches 4 files.

---

## Key constants

| Constant | Value | Used by |
|----------|-------|---------|
| `L0_COMPACTION_TRIGGER` | 4 | Score denominator at L0 |
| `MAX_BYTES_FOR_LEVEL[1]` | 10 MiB (×10 per level) | `finalize`, score computation |
| `MAX_MEM_COMPACT_LEVEL` | 2 | Flush placement cap |
| `EXPANDED_COMPACTION_BYTE_SIZE_LIMIT` | 25 × `max_file_size` | Input expansion in `setup_other_inputs` |
| `MAX_GRANDPARENT_OVERLAP_BYTES` | 10 × `max_file_size` | `should_stop_before`, trivial-move, flush placement |
| `READ_BYTES_PERIOD` | 1 MiB | Seek sampling interval in `DbIterator` |
| `ALLOWED_SEEKS_MIN` | 100 | Minimum seek budget for new files |
| `ALLOWED_SEEKS_DIV` | 16384 | Bytes-per-seek divisor: `allowed_seeks = max(100, file_size / 16384)` |

---

## Gap 1 — Level-score-based compaction

**Why:** Without this, L1–L6 grow without bound. Only L0→L1 compaction ever runs automatically.

**LevelDB reference:** `db/version_set.cc` — `Finalize`, `PickCompaction`, `SetupOtherInputs`,
`AddBoundaryInputs`, `GetOverlappingInputs`; `db/db_impl.cc` — `BackgroundCompaction`,
`DoCompactionWork`.

### New data structures

**`CompactionSpec`** (new struct, replaces bare inputs tuple):
- `level: usize` — level being compacted; inputs come from `level` and `level+1`
- `inputs: [Vec<Arc<FileMetaData>>; 2]` — `inputs[0]` at `level`, `inputs[1]` at `level+1`
- `grandparents: Vec<Arc<FileMetaData>>` — files at `level+2` overlapping the full input range
- `input_version: Arc<Version>` — version snapshot used to pick inputs
- `edit: VersionEdit` — carries the `compact_pointer` update for the MANIFEST
- Mutable state for Gap 4: `grandparent_index: usize`, `seen_key: bool`,
  `overlapped_bytes: i64`, `level_ptrs: [usize; NUM_LEVELS]`

**`Version` (new fields):**
- `compaction_score: f64` — initialised to `-1.0`; written once by `finalize()` before `Arc` wrap
- `compaction_level: i32` — initialised to `-1`; written once by `finalize()`

**`VersionSet` (new field):**
- `compact_pointer: [Vec<u8>; NUM_LEVELS]` — largest internal key last compacted per level;
  empty = not yet set; persisted to MANIFEST via tag 5

**`VersionEdit` (new field):**
- `compact_pointers: Vec<(i32, Vec<u8>)>` — `(level, encoded_internal_key)` pairs
- Tag constant: `TAG_COMPACT_POINTER = 5`
- Encode: emit tag 5, varint level, length-prefixed key bytes for each pair
- Decode: parse tag 5 records into `compact_pointers`

### New functions

**`max_bytes_for_level(level: usize) -> f64`**
```
// L1 = 10 MiB, L2 = 100 MiB, L3 = 1 GiB, …
let mut result = 10.0 * 1_048_576.0_f64;
let mut l = level;
while l > 1 { result *= 10.0; l -= 1; }
result
```

**`finalize(version: &mut Version, opts: &Options)`**

Called inside `log_and_apply` (and `recover`) on the owned `Version` before `Arc::new`. For each
level 0..NUM_LEVELS-1 compute:
- L0: `score = file_count as f64 / L0_COMPACTION_TRIGGER as f64`
- L1+: `score = total_bytes as f64 / max_bytes_for_level(level)`

Store the highest `(score, level)` pair in `version.compaction_score` / `version.compaction_level`.

**`needs_compaction(version: &Version, dbstate: &DbState) -> bool`**
```
version.compaction_score >= 1.0 || dbstate.seek_compact_file.is_some()
```
(The `seek_compact_file` field is added in Gap 5; for now only score matters.)

**`get_overlapping_inputs(version, level, begin: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Arc<FileMetaData>>`**

Port of `Version::GetOverlappingInputs`. `begin`/`end` are user-key bytes.
- L0: iterate all files; if a file overlaps, add it; if it expands the range, restart the scan.
- L1+: linear scan; skip files entirely before `begin` or entirely after `end`.

**`add_boundary_inputs(version, level, files: &mut Vec<Arc<FileMetaData>>)`**

Port of `AddBoundaryInputs`. Correctness-critical — prevents split-key bugs.

Find the largest key in `files`. Search `version.files[level]` for any file `b` where
`user_key(b.smallest) == user_key(largest)` and `b.smallest > largest` in internal key order
(i.e., same user key, lower sequence number). Add the smallest such file not already in `files`,
then repeat with the updated largest. Stop when no new boundary file is found.

**`setup_other_inputs(spec: &mut CompactionSpec, version: &Arc<Version>, vs: &mut VersionSet, opts: &Options)`**

Port of `VersionSet::SetupOtherInputs`:
1. `add_boundary_inputs(version, level, &mut spec.inputs[0])`
2. `(smallest, largest)` = range union of `inputs[0]`
3. `spec.inputs[1]` = `get_overlapping_inputs(version, level+1, smallest_user_key, largest_user_key)`
4. `add_boundary_inputs(version, level+1, &mut spec.inputs[1])`
5. `(all_start, all_limit)` = union range of both input sets
6. Input expansion (only if `inputs[1]` non-empty):
   - `expanded0` = `get_overlapping_inputs(version, level, all_start, all_limit)` + `add_boundary_inputs`
   - Accept if `expanded0.len() > inputs[0].len()` AND
     `total_size(inputs[1]) + total_size(expanded0) < 25 * max_file_size` AND
     `get_overlapping_inputs(level+1, expanded0_range).len() == inputs[1].len()`
7. If `level+2 < NUM_LEVELS`: `spec.grandparents` = `get_overlapping_inputs(version, level+2, all_start, all_limit)`
8. `vs.compact_pointer[level] = largest.clone()`
9. `spec.edit.compact_pointers.push((level as i32, largest))`

**`pick_compaction(version: &Arc<Version>, vs: &VersionSet, dbstate: &DbState, opts: &Options) -> Option<CompactionSpec>`**

Replaces current implementation:
1. Size-triggered (`version.compaction_score >= 1.0`):
   - `level = version.compaction_level as usize`
   - Find first file in `version.files[level]` whose `largest` > `vs.compact_pointer[level]` (internal
     key comparison); wrap to `files[level][0]` if none found
   - For L0: expand seed to all overlapping L0 files via `get_overlapping_inputs`
   - Call `setup_other_inputs`
2. Seek-triggered (Gap 5): handled after size-triggered (size takes priority)
3. Return `None` if neither branch applies

**`needs_compaction` check in `Db::write` loop:**

Replace `l0 < L0_COMPACTION_TRIGGER` with `!needs_compaction(version, &state)`.

### Changes to existing code

| Location | Change |
|----------|--------|
| `VersionEdit::encode/decode` | Add tag 5 for compact pointers |
| `VersionSet::recover` / `Builder` | Restore `compact_pointer[]` from MANIFEST |
| `VersionSet::log_and_apply` | Call `finalize`; apply `edit.compact_pointers` to `self.compact_pointer` |
| `do_compaction` | Accept `&mut CompactionSpec`; extract inputs from spec |
| `install_compaction` | Accept `&mut CompactionSpec`; extract delete/add pairs from spec |
| `maybe_compact` | Loop until `needs_compaction` false; dispatch `pick_compaction` |
| `Db::write` | Replace fixed L0 check with `needs_compaction` |

### Implementation order

1. `VersionEdit` tag 5 encoding/decoding + `compact_pointers` field
2. `VersionSet::compact_pointer` + `Builder` recovery + `log_and_apply` application
3. `max_bytes_for_level`, `finalize`, `needs_compaction`
4. Geometry helpers: `get_range`, `get_range2`, `total_file_size`
5. `get_overlapping_inputs`
6. `add_boundary_inputs`
7. `setup_other_inputs`
8. `CompactionSpec` struct
9. New `pick_compaction`
10. Update `do_compaction` + `install_compaction`
11. New `maybe_compact` + `Db::write` loop

### Tests

- Score computation: build a `Version` with controlled file sizes at L1; call `finalize`; assert
  `compaction_score` and `compaction_level` match the formula.
- Multi-level trigger: write enough data to push L1 beyond 10 MiB; verify automatic L1→L2
  compaction without calling `compact_range`.
- Round-robin: write keys across a wide range; run several compactions; verify `compact_pointer`
  advances and different key-space regions are visited.
- `add_boundary_inputs`: two L1 files sharing a user key at different sequence numbers; verify
  both are included in the compaction input.
- Compact-pointer persistence: run a compaction, close and reopen the DB; verify `compact_pointer`
  is restored from MANIFEST.

---

## Gap 2 — Flush placement (`PickLevelForMemTableOutput`)

**Why:** All flushes currently go to L0, increasing L0→L1 pressure. When L0 is empty and there is
no overlap, the flush output can go directly to L1 or L2.

**LevelDB reference:** `db/version_set.cc` — `Version::PickLevelForMemTableOutput`,
`Version::OverlapInLevel`.

**Prerequisite:** Gap 1 (needs `get_overlapping_inputs`, `MAX_GRANDPARENT_OVERLAP_BYTES`).

### New functions

**`overlap_in_level(version, level, smallest_user_key, largest_user_key) -> bool`**

- L0: same expand-restart scan as `get_overlapping_inputs` (L0 files may overlap each other).
- L1+: linear scan; return `true` on first overlapping file.

**`pick_level_for_memtable_output(version, smallest_user_key, largest_user_key, opts) -> usize`**
```
let mut level = 0;
if !overlap_in_level(version, 0, smallest, largest) {
    while level < MAX_MEM_COMPACT_LEVEL {    // MAX_MEM_COMPACT_LEVEL = 2
        if overlap_in_level(version, level + 1, smallest, largest) { break; }
        if level + 2 < NUM_LEVELS {
            let grandparent_bytes = total_file_size(
                &get_overlapping_inputs(version, level + 2, smallest_ikey, largest_ikey));
            if grandparent_bytes > MAX_GRANDPARENT_OVERLAP_BYTES { break; }
        }
        level += 1;
    }
}
level
```

Internal keys for the grandparent check:
- `smallest_ikey = make_internal_key(smallest_user_key, u64::MAX, ValueType::Value)`
- `largest_ikey  = make_internal_key(largest_user_key, 0, ValueType::Deletion)`

### Integration

- `FlushPrep` / `FlushResult` carries the flushed key range:
  `smallest_user_key: Vec<u8>`, `largest_user_key: Vec<u8>` (extracted from the first and last
  internal keys written by `TableBuilder`).
- `finish_flush` calls `pick_level_for_memtable_output` with the current version and passes the
  result as `output_level` to `install_compaction` (currently hardcoded `0`).

### Tests

- Push-to-L1: flush when L0 is empty and no L1 overlap → output lands at L1.
- No push when L0 has overlapping files → output stays at L0.
- Grandparent block: large L2 overlap → flush capped at L1 instead of L2.

---

## Gap 3 — Trivial-move optimisation

**Why:** When a single file at level L has no L+1 overlap and small grandparent overlap, it can be
moved to L+1 with a MANIFEST update only — no I/O, no merge.

**LevelDB reference:** `db/db_impl.cc` — `Compaction::IsTrivialMove`.

**Prerequisite:** Gap 1 (`spec.grandparents` populated by `setup_other_inputs`).

### New functions

**`is_trivial_move(spec: &CompactionSpec, opts: &Options) -> bool`**
```
spec.inputs[0].len() == 1
    && spec.inputs[1].is_empty()
    && total_file_size(&spec.grandparents) <= MAX_GRANDPARENT_OVERLAP_BYTES
```

**`install_trivial_move(state, spec, tc) -> Result<(), Error>`**

No file I/O. Builds a `VersionEdit` with one delete at `spec.level` and one add at `spec.level + 1`
for the same file number, plus the compact-pointer update from `spec.edit`. Calls `log_and_apply`.
TableCache: `evict` is called by `log_and_apply` for the deleted entry; the file will be re-opened
on next access under the new level.

### Integration

In `maybe_compact`, after `pick_compaction` returns a spec:
```rust
if is_trivial_move(&spec, opts) {
    install_trivial_move(&mut state.lock(), &spec, tc)?;
    continue;
}
```

### Tests

- Move test: single L1 file with no L2 overlap → MANIFEST has one delete at L1, one add at L2
  with the same file number; no data blocks read.
- Grandparent block: excessive L3 overlap → trivial move rejected.

---

## Gap 4 — Grandparent-overlap output limiting

**Why:** Without this, a compaction can produce a single giant output file that will require a very
expensive merge at the next level. Closing the output early when grandparent overlap is high limits
future compaction amplification.

**LevelDB reference:** `db/db_impl.cc` — `Compaction::ShouldStopBefore`,
`Compaction::IsBaseLevelForKey`.

**Prerequisite:** Gap 1 (`CompactionSpec` with grandparent + `level_ptrs` fields).

### New functions

**`should_stop_before(spec: &mut CompactionSpec, internal_key: &[u8], opts: &Options) -> bool`**

Port of `Compaction::ShouldStopBefore`:
```
while spec.grandparent_index < spec.grandparents.len()
    && internal_key > spec.grandparents[spec.grandparent_index].largest {
    if spec.seen_key {
        spec.overlapped_bytes += spec.grandparents[spec.grandparent_index].file_size as i64;
    }
    spec.grandparent_index += 1;
}
spec.seen_key = true;
if spec.overlapped_bytes > MAX_GRANDPARENT_OVERLAP_BYTES {
    spec.overlapped_bytes = 0;
    return true;
}
false
```
Uses full internal key comparison for the `>` check.

**`is_base_level_for_key(spec: &mut CompactionSpec, user_key: &[u8]) -> bool`**

Port of `Compaction::IsBaseLevelForKey`. Uses `spec.level_ptrs[lvl]` as a monotone cursor (never
resets) for each level `>= spec.level + 2`, amortising scans to O(1) per key:
```
for lvl in (spec.level + 2)..NUM_LEVELS {
    let files = &spec.input_version.files[lvl];
    while spec.level_ptrs[lvl] < files.len() {
        let f = &files[spec.level_ptrs[lvl]];
        if user_key <= user_key_of(f.largest) {
            if user_key >= user_key_of(f.smallest) { return false; }
            break;
        }
        spec.level_ptrs[lvl] += 1;
    }
}
true
```

### Integration into `do_compaction`

- Replace `is_base_level(...)` call with `is_base_level_for_key(spec, user_key)`.
- After each key is processed and before potentially opening a new output file, check:
  ```rust
  if should_stop_before(spec, &current_ikey, opts) {
      if let Some(cur) = current_output.take() {
          finish_compaction_output(cur, ...);
      }
  }
  ```

### Tests

- Output splitting: set up a compaction where grandparent overlap would normally produce one large
  file; verify multiple smaller output files are produced.
- `IsBaseLevelForKey` cursor: verify the cursor only advances forward and never re-scans.

---

## Gap 5 — Seek-based compaction

**Why:** Files that are repeatedly consulted but don't contain the sought key waste I/O on every
read. After enough misses, compacting the file eliminates the redundant seeks.

**LevelDB reference:** `db/version_set.cc` — `Version::UpdateStats`, `Version::RecordReadSample`;
`db/db_impl.cc` — `DBImpl::Get` (calls `UpdateStats`).

**Prerequisite:** Gap 1 (needs `needs_compaction` to incorporate seek branch;
`pick_compaction` needs seek branch).

### New data

**`FileMetaData` (new field):**
- `allowed_seeks: AtomicI32` — in-memory only, not encoded. Initialised in `log_and_apply`
  when a file is added: `allowed_seeks = max(100, file_size / 16384) as i32`.

**`DbState` (new fields):**
- `seek_compact_file: Option<(Arc<FileMetaData>, usize)>` — the file and level that exhausted
  its seek budget. Cleared after a compaction that removes the file.
- `compaction_needed: bool` — set when a file's seek budget is exhausted; causes the next
  `Db::write` compaction loop to run even without a flush.

**`GetStats` (new struct):**
- `seek_file: Option<Arc<FileMetaData>>`
- `seek_file_level: usize`

### New functions

**`Version::get` change:** Returns `(LookupResult, GetStats)`.
Track the first file whose range contains `user_key` (the "blamed" file for requiring a level probe).

**`update_stats(dbstate: &mut DbState, stats: &GetStats)`**

Decrement `stats.seek_file.allowed_seeks` by 1 (Relaxed). If it reaches ≤ 0 and
`dbstate.seek_compact_file` is `None`, set `dbstate.seek_compact_file` and `dbstate.compaction_needed`.

**`Version::record_read_sample(internal_key: &[u8], dbstate: &mut DbState)`**

Called from `DbIterator` every `READ_BYTES_PERIOD = 1 MiB` of key+value bytes read. Parses the
internal key to extract the user key, counts how many files' ranges it overlaps. If ≥ 2, calls
`update_stats`.

### Integration

- `Db::get`: after `version.get(...)`, re-acquire lock and call `update_stats`.
- `Db::write` compaction loop: also runs when `dbstate.compaction_needed` is true (not just after a flush).
- `DbIterator`: tracks `bytes_until_sampling: usize` initialised to `READ_BYTES_PERIOD`. After
  each `next()`/`prev()`, subtract `key.len() + value.len()`. When it reaches 0, call
  `record_read_sample` and reset.
- `pick_compaction`: after the size-triggered branch, check `dbstate.seek_compact_file` as a
  fallback input (size compaction takes priority).
- `needs_compaction`: include `|| dbstate.seek_compact_file.is_some()`.

### Tests

- Seek decrement: call `get` on a key that requires probing two L0 files; verify `allowed_seeks`
  decrements on the first file.
- Seek trigger: decrement `allowed_seeks` to zero via repeated gets; verify `compaction_needed`
  becomes true and the next write drains it.
- `record_read_sample`: iterate through a range spanning multiple L0 files; verify the compaction
  eventually fires.

---

## Changelog

| Date | Event |
|------|-------|
| 2026-03-12 | Plan written |
| 2026-03-12 | Gap 1 implemented: level-score-based multi-level compaction. 245 tests passing. |
| 2026-03-18 | Gap 2 implemented: flush placement (`pick_level_for_memtable_output`). 247 tests passing. |
| 2026-03-18 | Gap 3 implemented: trivial-move optimisation (`is_trivial_move`, `install_trivial_move`). 250 tests passing. |
| 2026-03-18 | Gap 4 implemented: grandparent-overlap output limiting (`should_stop_before`, `is_base_level_for_key`). 253 tests passing. |
| 2026-03-18 | Gap 5 implemented: seek-based compaction (`allowed_seeks`, `update_stats`, `GetStats`). 256 tests passing. |
