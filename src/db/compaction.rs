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

//! Compaction planning and execution.
//!
//! The compaction protocol has three phases; this module owns the first two:
//!
//! 1. **Planning** (no lock, no I/O): [`pick_compaction`] / [`pick_range_compaction`]
//!    query a [`Version`](crate::db::version::Version) snapshot and produce a
//!    [`Compaction`] plan — the input
//!    files at `level` and `level + 1`, the grandparent files used for output
//!    limiting, and the compact-pointer advance to persist.
//! 2. **Execution** (I/O, no lock): [`do_compaction`] merges the plan's inputs,
//!    applies shadow-key pruning, tombstone elision, and the optional
//!    [`CompactionFilter`](crate::compaction_filter::CompactionFilter), and writes
//!    output SSTables.  Output file numbers are allocated through an injected
//!    callback so this module never touches the DB lock.
//! 3. **Installation** (under lock): stays with `Db` in `lib.rs`
//!    (`install_compaction` / `install_trivial_move`), which records the edit in
//!    the MANIFEST via `VersionSet::log_and_apply`.
//!
//! Port of the compaction logic in LevelDB's `db/version_set.cc`
//! (`PickCompaction`, `SetupOtherInputs`, `Compaction`) and `db/db_impl.cc`
//! (`DoCompactionWork`).

use crate::cache::BlockCache;
use crate::db::version_edit::{FileMetaData, VersionEdit};
use crate::error::Error;
use crate::options::Options;
use crate::table::builder::TableBuilder;
use crate::table::format::user_key as ikey_user_key;
use crate::table::reader::Table;
use std::sync::Arc;

/// True if the user-key ranges [a_s..a_l] and [b_s..b_l] overlap.
fn key_ranges_overlap(
  a_s: &[u8],
  a_l: &[u8],
  b_s: &[u8],
  b_l: &[u8],
  cmp: &dyn crate::comparator::Comparator,
) -> bool {
  cmp.compare(ikey_user_key(a_s), ikey_user_key(b_l)).is_le()
    && cmp.compare(ikey_user_key(b_s), ikey_user_key(a_l)).is_le()
}

// ── Geometry helpers ──────────────────────────────────────────────────────────

/// User-key range union of a non-empty slice of files.
///
/// Returns `(smallest_internal_key, largest_internal_key)` spanning all files.
fn get_range(
  files: &[Arc<FileMetaData>],
  cmp: &dyn crate::comparator::Comparator,
) -> (Vec<u8>, Vec<u8>) {
  debug_assert!(!files.is_empty());
  let mut smallest = files[0].smallest.clone();
  let mut largest = files[0].largest.clone();
  for f in files.iter().skip(1) {
    if cmp
      .compare(ikey_user_key(&f.smallest), ikey_user_key(&smallest))
      .is_lt()
    {
      smallest = f.smallest.clone();
    }
    if cmp
      .compare(ikey_user_key(&f.largest), ikey_user_key(&largest))
      .is_gt()
    {
      largest = f.largest.clone();
    }
  }
  (smallest, largest)
}

/// Union of the key ranges of two slices of files (either may be empty).
pub(crate) fn get_range2(
  a: &[Arc<FileMetaData>],
  b: &[Arc<FileMetaData>],
  cmp: &dyn crate::comparator::Comparator,
) -> (Vec<u8>, Vec<u8>) {
  if b.is_empty() {
    return get_range(a, cmp);
  }
  if a.is_empty() {
    return get_range(b, cmp);
  }
  let (s1, l1) = get_range(a, cmp);
  let (s2, l2) = get_range(b, cmp);
  let smallest = if cmp.compare(ikey_user_key(&s1), ikey_user_key(&s2)).is_le() {
    s1
  } else {
    s2
  };
  let largest = if cmp.compare(ikey_user_key(&l1), ikey_user_key(&l2)).is_ge() {
    l1
  } else {
    l2
  };
  (smallest, largest)
}

/// Total on-disk bytes across a slice of files.
fn total_file_size(files: &[Arc<FileMetaData>]) -> u64 {
  files.iter().map(|f| f.file_size).sum()
}

/// Maximum grandparent (level+2) overlap in bytes before flush placement stops pushing deeper.
///
/// Matches LevelDB's `MaxGrandParentOverlapBytes = 10 * TargetFileSize`.
fn max_grandparent_overlap_bytes(max_file_size: u64) -> u64 {
  10 * max_file_size
}

/// Choose the level at which to place a freshly flushed memtable SSTable.
///
/// If the flush range does not overlap any L0 files, it may be safe to push the
/// output directly to L1 or even L2, reducing L0→L1 compaction pressure.
///
/// Rules (port of `Version::PickLevelForMemTableOutput`):
/// - Start at L0.
/// - While `level < MAX_MEM_COMPACT_LEVEL` (2):
///   - If the range overlaps L+1, stop.
///   - If grandparent (L+2) overlap exceeds `max_grandparent_overlap_bytes`, stop.
///   - Otherwise advance to level+1.
pub(crate) fn pick_level_for_memtable_output(
  version: &crate::db::version::Version,
  smallest_uk: &[u8],
  largest_uk: &[u8],
  max_file_size: u64,
) -> usize {
  use crate::db::version::NUM_LEVELS;
  const MAX_MEM_COMPACT_LEVEL: usize = 2;

  let mut level = 0;
  if !version.overlaps_level(0, smallest_uk, largest_uk) {
    // No L0 overlap — consider pushing deeper.
    while level < MAX_MEM_COMPACT_LEVEL {
      if version.overlaps_level(level + 1, smallest_uk, largest_uk) {
        break;
      }
      if level + 2 < NUM_LEVELS {
        // Check grandparent overlap to avoid creating a file that will cause
        // an expensive compaction at the next level.
        let grandparent_bytes =
          total_file_size(&version.overlapping_inputs(level + 2, smallest_uk, largest_uk));
        if grandparent_bytes > max_grandparent_overlap_bytes(max_file_size) {
          break;
        }
      }
      level += 1;
    }
  }
  level
}

/// Extend `compaction_files` with any file in `version.files[level]` that
/// shares a user key with the current largest key in `compaction_files` (at a
/// different sequence number).
///
/// This prevents a compaction from splitting entries for the same user key
/// across two output SSTables.  Port of LevelDB's `AddBoundaryInputs`.
fn add_boundary_inputs(
  version: &crate::db::version::Version,
  level: usize,
  compaction_files: &mut Vec<Arc<FileMetaData>>,
  cmp: &dyn crate::comparator::Comparator,
) {
  use crate::table::format::cmp_internal_keys;

  loop {
    // Find the largest internal key in the current compaction set.
    let largest = match compaction_files
      .iter()
      .max_by(|a, b| cmp_internal_keys(&a.largest, &b.largest, cmp))
    {
      Some(f) => f.largest.clone(),
      None => return,
    };
    let largest_uk = ikey_user_key(&largest).to_vec();

    // Find the smallest file in version.files[level] whose smallest key has
    // the same user key as `largest_uk` but a strictly greater internal key
    // (i.e., same user key, lower sequence number), and isn't already in the
    // compaction set.
    let boundary = version
      .files_at(level)
      .iter()
      .filter(|f| {
        let f_uk = ikey_user_key(&f.smallest);
        cmp.compare(f_uk, largest_uk.as_slice()).is_eq()
          && cmp_internal_keys(&f.smallest, &largest, cmp) == std::cmp::Ordering::Greater
          && !compaction_files.iter().any(|c| c.number == f.number)
      })
      .min_by(|a, b| cmp_internal_keys(&a.smallest, &b.smallest, cmp));

    match boundary {
      Some(f) => compaction_files.push(Arc::clone(f)),
      None => return,
    }
  }
}

// ── Compaction ────────────────────────────────────────────────────────────

/// A compaction plan: everything needed to execute one compaction pass.
///
/// Produced by [`pick_compaction`] / [`pick_range_compaction`] (planning),
/// consumed by [`do_compaction`] (execution) and by `install_compaction` /
/// `install_trivial_move` in `lib.rs` (installation).
pub(crate) struct Compaction {
  /// Level being compacted; inputs come from `level` and `level+1`.
  pub(crate) level: usize,
  /// `inputs[0]` = files at `level`; `inputs[1]` = files at `level+1`.
  pub(crate) inputs: [Vec<Arc<FileMetaData>>; 2],
  /// Files at `level+2` overlapping the full input range.
  pub(crate) grandparents: Vec<Arc<FileMetaData>>,
  /// Version snapshot used to build this plan.
  input_version: Arc<crate::db::version::Version>,
  /// VersionEdit carrying the compact_pointer update (filled by setup_other_inputs).
  pub(crate) edit: VersionEdit,

  // ── Gap 4: grandparent-overlap output limiting ────────────────────────────
  /// Index into `grandparents` of the next file to scan in `should_stop_before`.
  grandparent_index: usize,
  /// Whether we have seen at least one key (used to skip overlap accounting for
  /// the very first key, matching LevelDB's `Compaction::ShouldStopBefore`).
  seen_key: bool,
  /// Accumulated grandparent overlap bytes since the last output-file boundary.
  overlapped_bytes: i64,
  /// Per-level monotone cursors for `is_base_level_for_key`.  Each entry starts
  /// at 0 and only advances forward, amortising repeated scans to O(1)/key.
  level_ptrs: [usize; crate::db::version::NUM_LEVELS],
}

impl Compaction {
  pub(crate) fn new(level: usize, input_version: Arc<crate::db::version::Version>) -> Self {
    Compaction {
      level,
      inputs: [Vec::new(), Vec::new()],
      grandparents: Vec::new(),
      input_version,
      edit: VersionEdit::new(),
      grandparent_index: 0,
      seen_key: false,
      overlapped_bytes: 0,
      level_ptrs: [0; crate::db::version::NUM_LEVELS],
    }
  }

  pub(crate) fn all_input_files(&self) -> impl Iterator<Item = &Arc<FileMetaData>> {
    self.inputs[0].iter().chain(self.inputs[1].iter())
  }
}

// ── setup_other_inputs ────────────────────────────────────────────────────────

/// Populate `spec.inputs[1]` (level+1 files), try to expand inputs[0] without
/// pulling in more level+1 files, populate grandparents, and record the
/// compact-pointer update in `spec.edit`.
///
/// Port of LevelDB's `VersionSet::SetupOtherInputs`.
fn setup_other_inputs(
  spec: &mut Compaction,
  version: &Arc<crate::db::version::Version>,
  _compact_pointer: &[Vec<u8>; crate::db::version::NUM_LEVELS],
  opts: &Options,
) {
  use crate::db::version::NUM_LEVELS;
  let cmp = &*opts.comparator;

  let level = spec.level;
  add_boundary_inputs(version, level, &mut spec.inputs[0], cmp);

  let (smallest, largest) = get_range(&spec.inputs[0], cmp);
  let smallest_uk = ikey_user_key(&smallest).to_vec();
  let largest_uk = ikey_user_key(&largest).to_vec();

  spec.inputs[1] = version.overlapping_inputs(level + 1, &smallest_uk, &largest_uk);
  add_boundary_inputs(version, level + 1, &mut spec.inputs[1], cmp);

  let (all_start, all_limit) = get_range2(&spec.inputs[0], &spec.inputs[1], cmp);
  let all_start_uk = ikey_user_key(&all_start).to_vec();
  let all_limit_uk = ikey_user_key(&all_limit).to_vec();

  // Try to expand inputs[0] without expanding inputs[1].
  if !spec.inputs[1].is_empty() {
    let mut expanded0 = version.overlapping_inputs(level, &all_start_uk, &all_limit_uk);
    add_boundary_inputs(version, level, &mut expanded0, cmp);

    let inputs1_size = total_file_size(&spec.inputs[1]);
    let expanded0_size = total_file_size(&expanded0);
    let expand_limit = 25 * opts.max_file_size as u64;

    if expanded0.len() > spec.inputs[0].len() && inputs1_size + expanded0_size < expand_limit {
      let (exp_start, exp_limit) = get_range(&expanded0, cmp);
      let exp_start_uk = ikey_user_key(&exp_start).to_vec();
      let exp_limit_uk = ikey_user_key(&exp_limit).to_vec();
      let expanded1 = version.overlapping_inputs(level + 1, &exp_start_uk, &exp_limit_uk);
      // Only accept the expansion if it doesn't pull in more L+1 files.
      if expanded1.len() == spec.inputs[1].len() {
        spec.inputs[0] = expanded0;
        spec.inputs[1] = expanded1;
      }
    }
  }

  // Populate grandparents (level+2 files overlapping the compaction range).
  if level + 2 < NUM_LEVELS {
    let (final_start, final_limit) = get_range2(&spec.inputs[0], &spec.inputs[1], cmp);
    let final_start_uk = ikey_user_key(&final_start).to_vec();
    let final_limit_uk = ikey_user_key(&final_limit).to_vec();
    spec.grandparents = version.overlapping_inputs(level + 2, &final_start_uk, &final_limit_uk);
  }

  // Update compact_pointer: advance past largest key of inputs[0].
  let (_, new_largest) = get_range(&spec.inputs[0], cmp);
  spec.edit.compact_pointers.push((level as i32, new_largest));
}

/// Returns `true` when the current output SSTable should be closed because the
/// bytes it would overlap at `level+2` (the grandparents) have exceeded the
/// limit `max_grandparent_overlap_bytes`.
///
/// Monotone in `ikey`: callers must feed keys in ascending internal-key order.
/// Closing the output early limits future compaction amplification.
///
/// Port of LevelDB `Compaction::ShouldStopBefore`.
fn should_stop_before(spec: &mut Compaction, ikey: &[u8], opts: &Options) -> bool {
  use crate::table::format::cmp_internal_keys;
  let cmp = &*opts.comparator;
  let limit = max_grandparent_overlap_bytes(opts.max_file_size as u64) as i64;
  while spec.grandparent_index < spec.grandparents.len()
    && cmp_internal_keys(
      ikey,
      &spec.grandparents[spec.grandparent_index].largest,
      cmp,
    ) == std::cmp::Ordering::Greater
  {
    if spec.seen_key {
      spec.overlapped_bytes += spec.grandparents[spec.grandparent_index].file_size as i64;
    }
    spec.grandparent_index += 1;
  }
  spec.seen_key = true;
  if spec.overlapped_bytes > limit {
    spec.overlapped_bytes = 0;
    return true;
  }
  false
}

/// Returns `true` if `user_key` is definitely absent from all levels ≥
/// `spec.level + 2` (i.e., the output level is the lowest level that holds
/// this key).  Used for tombstone elision: we can only drop a deletion marker
/// when it cannot hide a live value at a deeper level.
///
/// Uses per-level monotone cursors (`spec.level_ptrs`) so each cursor advances
/// at most once per key across the entire compaction — amortised O(1)/key.
///
/// Port of LevelDB `Compaction::IsBaseLevelForKey`.
fn is_base_level_for_key(
  spec: &mut Compaction,
  user_key: &[u8],
  cmp: &dyn crate::comparator::Comparator,
) -> bool {
  use crate::db::version::NUM_LEVELS;
  for lvl in (spec.level + 2)..NUM_LEVELS {
    let files = spec.input_version.files_at(lvl);
    while spec.level_ptrs[lvl] < files.len() {
      let f = &files[spec.level_ptrs[lvl]];
      let f_largest_uk = ikey_user_key(&f.largest);
      if cmp.compare(user_key, f_largest_uk).is_le() {
        // user_key is ≤ this file's largest — it may be inside this file.
        if cmp.compare(user_key, ikey_user_key(&f.smallest)).is_ge() {
          return false; // user_key falls within this file's range
        }
        break; // user_key is before this file; no later file at this level can match
      }
      spec.level_ptrs[lvl] += 1; // user_key is past this file; advance cursor
    }
  }
  true
}

/// True if `version` has a level that needs compaction (score ≥ 1.0), or if a
/// seek-based compaction candidate has been nominated.
pub(crate) fn needs_compaction(
  version: &crate::db::version::Version,
  compaction_needed: bool,
) -> bool {
  version.compaction_score >= 1.0 || compaction_needed
}

/// Select the next compaction to run, based on level scores, compact-pointer
/// round-robin, or seek-based nomination.
///
/// Returns `None` if no compaction is needed.
pub(crate) fn pick_compaction(
  version: &Arc<crate::db::version::Version>,
  compact_pointer: &[Vec<u8>; crate::db::version::NUM_LEVELS],
  opts: &Options,
  seek_compact: Option<&(Arc<FileMetaData>, usize)>,
) -> Option<Compaction> {
  let cmp = &*opts.comparator;
  // ── Size-triggered (highest priority) ─────────────────────────────────────
  if version.compaction_score >= 1.0 {
    let level = version.compaction_level as usize;
    let mut spec = Compaction::new(level, Arc::clone(version));

    if level == 0 {
      // For L0, seed with the first file past compact_pointer[0], then expand
      // to all overlapping L0 files (L0 files can overlap each other).
      let seed = version
        .files_at(0)
        .iter()
        .find(|f| {
          compact_pointer[0].is_empty()
            || cmp
              .compare(
                ikey_user_key(&f.largest),
                ikey_user_key(&compact_pointer[0]),
              )
              .is_gt()
        })
        .or_else(|| version.files_at(0).first())
        .cloned()?;

      let seed_lo = ikey_user_key(&seed.smallest).to_vec();
      let seed_hi = ikey_user_key(&seed.largest).to_vec();
      spec.inputs[0] = version.overlapping_inputs(0, &seed_lo, &seed_hi);
    } else {
      // L1+: pick the first file whose largest key is past compact_pointer[level].
      let file = version
        .files_at(level)
        .iter()
        .find(|f| {
          compact_pointer[level].is_empty()
            || cmp
              .compare(
                ikey_user_key(&f.largest),
                ikey_user_key(&compact_pointer[level]),
              )
              .is_gt()
        })
        .or_else(|| version.files_at(level).first())
        .cloned()?;
      spec.inputs[0].push(file);
    }

    setup_other_inputs(&mut spec, version, compact_pointer, opts);
    return Some(spec);
  }

  // ── Seek-triggered fallback ────────────────────────────────────────────────
  if let Some((file, level)) = seek_compact {
    let mut spec = Compaction::new(*level, Arc::clone(version));
    spec.inputs[0].push(Arc::clone(file));
    setup_other_inputs(&mut spec, version, compact_pointer, opts);
    return Some(spec);
  }

  None
}
/// Alias for a plain (level_inputs, next_level_inputs) pair returned by
/// `pick_range_compaction`.  Used only internally by `compact_level_range`.
type CompactionInputs = (Vec<Arc<FileMetaData>>, Vec<Arc<FileMetaData>>);

/// A completed compaction output SSTable.
pub(crate) struct CompactionOutput {
  pub(crate) file_number: u64,
  pub(crate) file_size: u64,
  pub(crate) smallest: Vec<u8>,
  pub(crate) largest: Vec<u8>,
  pub(crate) table: Arc<Table>,
}

/// In-progress output SSTable being built during compaction.
struct CompactionOutputFile {
  file_number: u64,
  path: std::path::PathBuf,
  builder: TableBuilder,
  smallest: Vec<u8>,
}

/// Finalise `cur`, open the file for reading, and push to `outputs`.
fn finish_compaction_output(
  cur: CompactionOutputFile,
  largest: Vec<u8>,
  outputs: &mut Vec<CompactionOutput>,
  filter_policy: Option<Arc<dyn crate::filter::FilterPolicy>>,
  block_cache: Option<Arc<BlockCache>>,
  comparator: Arc<dyn crate::comparator::Comparator>,
  fs: &dyn crate::env::FileSystem,
) -> Result<(), Error> {
  let file_size = cur.builder.finish()?;
  let read_file = fs.open_random_access(&cur.path)?;
  let table = Arc::new(Table::open(
    read_file,
    file_size,
    filter_policy,
    block_cache,
    comparator,
  )?);
  outputs.push(CompactionOutput {
    file_number: cur.file_number,
    file_size,
    smallest: cur.smallest,
    largest,
    table,
  });
  Ok(())
}

/// Phase 2 (no lock): merge input files, apply pruning, write output SSTables.
///
/// Shadow-key pruning: for each user key, only the first (newest) version
/// encountered in the merge order is kept.
/// Tombstone elision: a deletion marker is dropped when there is provably no
/// data for that key at levels > `output_level`.
///
/// L0 inputs are already newest-first in `spec.inputs[0]` (Version stores them
/// that way), so `MergingIterator` resolves same-key ties in favour of the
/// newer version via the lower child index.
///
/// `next_file_number` allocates output file numbers.  Callers that hold shared
/// state (e.g. `Db`) pass a closure that briefly takes their lock; this module
/// itself never touches a lock, which is what keeps it executable against a
/// bare `Version` in tests.
pub(crate) fn do_compaction(
  path: &std::path::Path,
  next_file_number: &mut dyn FnMut() -> u64,
  spec: &mut Compaction,
  oldest_snapshot: u64,
  opts: &Options,
  tc: &crate::db::table_cache::TableCache,
) -> Result<Vec<CompactionOutput>, Error> {
  use crate::db::merge_iter::MergingIterator;
  use crate::iter::InternalIterator;

  let output_level = spec.level + 1;

  // Create a compaction filter for this run (if configured).
  let mut compaction_filter: Option<Box<dyn crate::compaction_filter::CompactionFilter>> = opts
    .compaction_filter_factory
    .as_ref()
    .map(|f| f.create_compaction_filter());

  log::info!(
    "compaction L{}→L{}: {} + {} files ({} + {} bytes)",
    spec.level,
    spec.level + 1,
    spec.inputs[0].len(),
    spec.inputs[1].len(),
    spec.inputs[0].iter().map(|f| f.file_size).sum::<u64>(),
    spec.inputs[1].iter().map(|f| f.file_size).sum::<u64>(),
  );

  let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
  for meta in spec.all_input_files() {
    let table = tc.get_or_open(meta.number, meta.file_size)?;
    // Compaction is a bulk scan — don't pollute the block cache.
    children.push(Box::new(table.new_iterator(opts.paranoid_checks, false)?));
  }

  let mut merger = MergingIterator::new(children, Arc::clone(&opts.comparator));
  merger.seek_to_first();

  let mut outputs: Vec<CompactionOutput> = Vec::new();
  let mut current: Option<CompactionOutputFile> = None;
  let mut current_largest: Vec<u8> = Vec::new();

  // Deduplication / tombstone-elision state.
  let mut current_user_key: Vec<u8> = Vec::new();
  let mut has_current_user_key = false;
  let mut last_sequence_for_key: u64 = u64::MAX;

  while merger.valid() {
    let ikey = merger.key().to_vec();
    let mut value = merger.value().to_vec();
    merger.next();

    let (uk, seq, vtype) = match crate::table::format::parse_internal_key(&ikey) {
      Some(parts) => parts,
      None => continue, // corrupt key — skip silently
    };

    // Track first occurrence of this user key.
    let first_occurrence = !has_current_user_key
      || opts
        .comparator
        .compare(uk, current_user_key.as_slice())
        .is_ne();
    if first_occurrence {
      current_user_key = uk.to_vec();
      has_current_user_key = true;
      last_sequence_for_key = u64::MAX;
    }

    // Determine whether to drop this entry.
    let drop = if last_sequence_for_key <= oldest_snapshot {
      // A newer version for this user key was already emitted and is visible
      // to even the oldest active snapshot — this older version is invisible.
      true
    } else if vtype == 0
      && seq <= oldest_snapshot
      && is_base_level_for_key(spec, uk, &*opts.comparator)
    {
      // Tombstone that no snapshot can see below this level — safe to elide.
      true
    } else {
      false
    };

    last_sequence_for_key = seq;

    if !drop {
      // Apply the compaction filter to the newest visible version of each key.
      // Entries still visible to a live snapshot are never filtered (seq > oldest_snapshot).
      if let Some(ref mut filter) = compaction_filter {
        if first_occurrence && seq <= oldest_snapshot {
          match filter.filter(output_level, uk, &value, vtype) {
            crate::compaction_filter::CompactionDecision::Keep => {}
            crate::compaction_filter::CompactionDecision::Remove => continue,
            crate::compaction_filter::CompactionDecision::ChangeValue(new_val) => {
              value = new_val;
            }
          }
        }
      }

      // Close the current output file early if grandparent overlap is too high.
      // This limits future compaction amplification (Gap 4).
      if current.is_some() && should_stop_before(spec, &ikey, opts) {
        let finished = current.take().unwrap();
        finish_compaction_output(
          finished,
          std::mem::take(&mut current_largest),
          &mut outputs,
          opts.filter_policy.clone(),
          opts.block_cache.clone(),
          Arc::clone(&opts.comparator),
          &*opts.file_system,
        )?;
      }

      // Rotate to a new output file if the current one is at the size limit.
      if let Some(ref cur) = current {
        if cur.builder.file_size() >= opts.max_file_size as u64 {
          let finished = current.take().unwrap();
          finish_compaction_output(
            finished,
            std::mem::take(&mut current_largest),
            &mut outputs,
            opts.filter_policy.clone(),
            opts.block_cache.clone(),
            Arc::clone(&opts.comparator),
            &*opts.file_system,
          )?;
        }
      }

      // Open a new output file (the allocator briefly takes the DB lock).
      if current.is_none() {
        let file_number = next_file_number();
        let sst_path = path.join(format!("{file_number:06}.ldb"));
        let file = opts.file_system.create_writable(&sst_path)?;
        let builder = TableBuilder::new(
          file,
          opts.block_size,
          opts.block_restart_interval,
          opts.filter_policy.clone(),
          opts.compression,
          Arc::clone(&opts.comparator),
        );
        current = Some(CompactionOutputFile {
          file_number,
          path: sst_path,
          builder,
          smallest: ikey.clone(),
        });
      }

      let cur = current.as_mut().unwrap();
      cur.builder.add(&ikey, &value)?;
      current_largest = ikey;
    }
  }

  // Finalise the last output file (if any).
  if let Some(cur) = current {
    finish_compaction_output(
      cur,
      current_largest,
      &mut outputs,
      opts.filter_policy.clone(),
      opts.block_cache.clone(),
      Arc::clone(&opts.comparator),
      &*opts.file_system,
    )?;
  }

  // Each output file was fsync'd by TableBuilder::finish; one directory sync
  // persists all their directory entries before install_compaction records
  // them in the MANIFEST and the inputs become deletable.
  if !outputs.is_empty() {
    opts.file_system.sync_dir(path)?;
  }

  log::info!(
    "compaction L{}→L{} complete: {} output files ({} bytes)",
    spec.level,
    spec.level + 1,
    outputs.len(),
    outputs.iter().map(|o| o.file_size).sum::<u64>(),
  );
  Ok(outputs)
}
/// True if a file whose user-key range is `[file_small, file_large]` overlaps
/// the user-key range `[begin, end]` (either bound may be `None` = open).
pub(crate) fn file_overlaps_range(
  file_small: &[u8],
  file_large: &[u8],
  begin: Option<&[u8]>,
  end: Option<&[u8]>,
  cmp: &dyn crate::comparator::Comparator,
) -> bool {
  begin.is_none_or(|b| cmp.compare(file_large, b).is_ge())
    && end.is_none_or(|e| cmp.compare(file_small, e).is_le())
}

/// Select files for a range-based compaction of `level` → `level + 1`.
///
/// Returns `(level_inputs, next_level_inputs)` for files at `level` that
/// overlap `[begin, end]` and the `level+1` files that overlap their union
/// range.  Returns `None` if there is nothing to compact.
pub(crate) fn pick_range_compaction(
  version: &crate::db::version::Version,
  level: usize,
  begin: Option<&[u8]>,
  end: Option<&[u8]>,
  cmp: &dyn crate::comparator::Comparator,
) -> Option<CompactionInputs> {
  use crate::db::version::NUM_LEVELS;

  if level + 1 >= NUM_LEVELS {
    return None;
  }

  let level_inputs: Vec<Arc<FileMetaData>> = version
    .files_at(level)
    .iter()
    .filter(|m| {
      file_overlaps_range(
        ikey_user_key(&m.smallest),
        ikey_user_key(&m.largest),
        begin,
        end,
        cmp,
      )
    })
    .cloned()
    .collect();

  if level_inputs.is_empty() {
    return None;
  }

  // Union user-key range of the selected level files.
  let range_small = level_inputs
    .iter()
    .min_by(|a, b| cmp.compare(ikey_user_key(&a.smallest), ikey_user_key(&b.smallest)))
    .unwrap()
    .smallest
    .clone();
  let range_large = level_inputs
    .iter()
    .max_by(|a, b| cmp.compare(ikey_user_key(&a.largest), ikey_user_key(&b.largest)))
    .unwrap()
    .largest
    .clone();

  let next_inputs: Vec<Arc<FileMetaData>> = version
    .files_at(level + 1)
    .iter()
    .filter(|m| key_ranges_overlap(&m.smallest, &m.largest, &range_small, &range_large, cmp))
    .cloned()
    .collect();

  Some((level_inputs, next_inputs))
}
/// Returns `true` when the compaction can be executed as a trivial move:
/// a single file at `spec.level` with no `level+1` overlap and acceptable
/// grandparent overlap.  No data is read or written — only the MANIFEST is
/// updated.
///
/// Port of LevelDB `Compaction::IsTrivialMove`.
pub(crate) fn is_trivial_move(spec: &Compaction, opts: &Options) -> bool {
  spec.inputs[0].len() == 1
    && spec.inputs[1].is_empty()
    && total_file_size(&spec.grandparents)
      <= max_grandparent_overlap_bytes(opts.max_file_size as u64)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn add_boundary_inputs_includes_same_user_key() {
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    // Two files at L1 with the same user key "k" but different sequence numbers.
    // File A: smallest=("k", seq=5), largest=("k", seq=5)
    // File B: smallest=("k", seq=2), largest=("k", seq=2)
    // (higher seq sorts before lower seq in internal key order)
    let ikey_high = make_internal_key(b"k", 5, 1);
    let ikey_low = make_internal_key(b"k", 2, 1);

    let file_a = FileMetaData::new(10, 100, ikey_high.clone(), ikey_high.clone());
    let file_b = FileMetaData::new(11, 100, ikey_low.clone(), ikey_low.clone());

    let mut v = Version::new(std::sync::Arc::new(crate::comparator::BytewiseComparator));
    // L1 sorted by smallest: file_a (seq=5) sorts BEFORE file_b (seq=2) because
    // higher seq means lower internal key value (tag sorts descending).
    v.push_file_for_test(1, Arc::clone(&file_a));
    v.push_file_for_test(1, Arc::clone(&file_b));

    // Start compaction with only file_a; boundary check should add file_b.
    let mut compaction_files = vec![Arc::clone(&file_a)];
    add_boundary_inputs(&v, 1, &mut compaction_files, &crate::BytewiseComparator);

    assert_eq!(
      compaction_files.len(),
      2,
      "add_boundary_inputs should have added file_b (same user key, lower seq)"
    );
    assert!(compaction_files.iter().any(|f| f.number == 11));
  }

  #[test]
  fn trivial_move_advances_file_to_next_level() {
    // Build a Version with one L1 file and no L2 overlap,
    // then verify is_trivial_move returns true.
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8]| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        4096,
        make_internal_key(small, 10, 1u8), // 1 = Value
        make_internal_key(large, 1, 1u8),
      )
    };

    let mut v = Version::new(std::sync::Arc::new(crate::comparator::BytewiseComparator));
    // One L1 file covering "a".."z"; no L2 files.
    v.push_file_for_test(1, make_meta(10, b"a", b"z"));
    let version = Arc::new(v);

    let opts = Options {
      max_file_size: 64 * 1024,
      ..Options::default()
    };

    // Build a spec for this file: one L1 input, no L2 inputs, no grandparents.
    let mut spec = Compaction::new(1, Arc::clone(&version));
    spec.inputs[0].push(Arc::clone(&version.files_at(1)[0]));
    // inputs[1] and grandparents are empty (no L2 or L3 files).

    assert!(
      is_trivial_move(&spec, &opts),
      "single file with no next-level overlap and no grandparents should be a trivial move"
    );

    // Also verify: adding grandparent bytes that exceed the limit disqualifies it.
    for i in 0u64..12 {
      spec.grandparents.push(make_meta(
        100 + i,
        &format!("{i:02}").into_bytes(),
        &format!("{:02}", i + 1).into_bytes(),
      ));
    }
    // Each grandparent file is 4096 bytes; 12 files = 49152 bytes.
    // Use a very small max_file_size: 10 * 1024 = 10240 < 49152 → not trivial.
    let tiny_opts = Options {
      max_file_size: 1024,
      ..Options::default()
    };
    assert!(
      !is_trivial_move(&spec, &tiny_opts),
      "excessive grandparent overlap should disqualify trivial move"
    );
  }

  #[test]
  fn trivial_move_not_trivial_when_next_level_has_files() {
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8]| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        4096,
        make_internal_key(small, 10, 1u8),
        make_internal_key(large, 1, 1u8),
      )
    };

    let mut v = Version::new(std::sync::Arc::new(crate::comparator::BytewiseComparator));
    v.push_file_for_test(1, make_meta(10, b"a", b"z"));
    v.push_file_for_test(2, make_meta(20, b"m", b"p")); // overlapping L2 file
    let version = Arc::new(v);

    let opts = Options {
      max_file_size: 64 * 1024,
      ..Options::default()
    };

    let mut spec = Compaction::new(1, Arc::clone(&version));
    spec.inputs[0].push(Arc::clone(&version.files_at(1)[0]));
    spec.inputs[1].push(Arc::clone(&version.files_at(2)[0])); // L2 overlap present

    assert!(
      !is_trivial_move(&spec, &opts),
      "trivial move requires inputs[1] to be empty"
    );
  }

  #[test]
  fn is_base_level_for_key_cursor_advances_monotonically() {
    // Build a version with a few L3 files; verify that is_base_level_for_key
    // correctly returns false for keys inside those files and true for keys
    // outside, and that the cursor only ever moves forward.
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8]| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        4096,
        make_internal_key(small, 10, 1u8),
        make_internal_key(large, 1, 1u8),
      )
    };

    let mut v = Version::new(std::sync::Arc::new(crate::comparator::BytewiseComparator));
    // L1 compaction inputs (level 1).
    v.push_file_for_test(1, make_meta(1, b"a", b"z"));
    // L3 = level+2 from L1: two non-overlapping files.
    v.push_file_for_test(3, make_meta(10, b"b", b"d")); // covers b..d
    v.push_file_for_test(3, make_meta(11, b"p", b"r")); // covers p..r
    let version = Arc::new(v);

    let mut spec = Compaction::new(1, Arc::clone(&version));
    spec.inputs[0].push(Arc::clone(&version.files_at(1)[0]));

    let cmp = &crate::BytewiseComparator;
    // "c" is inside file 10 (b..d) → NOT base level.
    assert!(!is_base_level_for_key(&mut spec, b"c", cmp));
    // "q" is inside file 11 (p..r) → NOT base level.
    // Cursor for L3 should have advanced past file 10.
    assert!(!is_base_level_for_key(&mut spec, b"q", cmp));
    // "s" is past both files → IS base level.
    assert!(is_base_level_for_key(&mut spec, b"s", cmp));
    // Cursor must now be at the end; a key before the end (but still past both
    // files) should also return true without regressing the cursor.
    assert!(is_base_level_for_key(&mut spec, b"t", cmp));
    // The cursor for L3 should be at index 2 (past both files) — check it
    // hasn't regressed.
    assert_eq!(spec.level_ptrs[3], 2);
  }

  #[test]
  fn should_stop_before_triggers_on_excess_grandparent_overlap() {
    use crate::db::version::Version;
    use crate::db::version_edit::FileMetaData;
    use crate::table::format::make_internal_key;
    use std::sync::Arc;

    let make_meta = |number: u64, small: &[u8], large: &[u8], size: u64| -> Arc<FileMetaData> {
      FileMetaData::new(
        number,
        size,
        make_internal_key(small, 10, 1u8),
        make_internal_key(large, 1, 1u8),
      )
    };

    let v = Version::new(std::sync::Arc::new(crate::comparator::BytewiseComparator));
    let version = Arc::new(v);

    let opts = Options {
      max_file_size: 1024, // max_grandparent_overlap_bytes = 10 * 1024 = 10240
      ..Options::default()
    };

    let mut spec = Compaction::new(0, Arc::clone(&version));
    // Three grandparent files, each 6000 bytes.
    // seen_key is false for the first key, so file 20's bytes don't count.
    // After passing file 21 (6000 bytes) and then file 22 (6000 more = 12000),
    // the limit of 10 * 1024 = 10240 is exceeded on the third key.
    spec.grandparents.push(make_meta(20, b"b", b"d", 6000));
    spec.grandparents.push(make_meta(21, b"e", b"g", 6000));
    spec.grandparents.push(make_meta(22, b"h", b"k", 6000));

    // Feed keys that advance past each grandparent file.
    // Keys are internal keys; construct them to be > each file's largest.
    let after_d = make_internal_key(b"e", 0, 0u8); // > "d" in ikey order
    let after_g = make_internal_key(b"h", 0, 0u8);
    let after_k = make_internal_key(b"l", 0, 0u8);

    // First key — seen_key becomes true, no bytes accumulated yet.
    assert!(!should_stop_before(&mut spec, &after_d, &opts));
    // Second key — passes file 20 (4096 bytes accumulated), still under limit.
    assert!(!should_stop_before(&mut spec, &after_g, &opts));
    // Third key — passes file 21 (8192 bytes), still under limit.
    // After passing file 22 (12288 > 10240) it should trigger.
    assert!(should_stop_before(&mut spec, &after_k, &opts));
    // After triggering, overlapped_bytes is reset to 0.
    assert_eq!(spec.overlapped_bytes, 0);
  }
}
