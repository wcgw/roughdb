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

//! Concurrent-read, externally-synchronised-write skip list.
//!
//! # Memory layout
//!
//! Every node is a single arena allocation laid out as:
//!
//! ```text
//!  raw ──►  [ AtomicPtr<Node> ]  level h−1  ┐
//!           [ AtomicPtr<Node> ]  level h−2  │  (height − 1) prefix words
//!                  …                        │
//!           [ AtomicPtr<Node> ]  level   1  ┘
//! node ──►  [ AtomicPtr<Node> ]  level   0  ←── Node struct begins here
//!           [ payload bytes … ]              ←── varint-encoded Entry
//! ```
//!
//! The `*mut Node` pointer always points to the level-0 link.  Higher levels
//! are accessed by walking *backwards* in memory (`node.slot(n)` subtracts `n`
//! pointer widths from the level-0 address).  This is RocksDB's
//! `InlineSkipList` layout: the hot level-0 link and the start of the payload
//! share a cache line, and nothing else is stored per node — neither the
//! height (see the level invariant below) nor the payload length (the entry
//! encoding is self-delimiting, see `entry.rs`).
//!
//! # Level invariant
//!
//! A node's height is not stored.  Instead, every access to level `l` of a
//! node relies on this invariant, which the whole module upholds:
//!
//! > A node is linked into level `l` (some level-`l` link points at it) only
//! > if it was allocated with height `> l`.  Therefore any node reached by
//! > following a level-`l` link is valid for every level `≤ l`, and the
//! > sentinel `head` (allocated at `MAX_HEIGHT`) is valid for every level
//! > `< MAX_HEIGHT`.
//!
//! Traversals start at `head` and only ever *descend*, so the level they
//! access at a node is never above the level of the link that reached it.
//! `alloc_and_insert` links a new node at exactly the levels
//! `0..height` it was allocated with.
//!
//! # Thread safety
//!
//! Writes must be serialised by the caller: [`SkipList::alloc_and_insert`] is
//! an `unsafe fn` whose contract is that no other insert runs concurrently.
//! In the database the write leader inserts while holding the `DbState`
//! mutex, matching LevelDB's model.  Reads are lock-free and may run
//! concurrently with the writer: node links are published with Release
//! stores and consumed with Acquire loads, so a reader that reaches a node
//! through a link observes its fully-written payload.  Nodes are never freed
//! before the arena is dropped together with the list.

use super::arena::Arena;
use super::entry::{DecodedEntry, Entry};
use crate::comparator::Comparator;
use std::cell::UnsafeCell;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::mem;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Maximum number of levels.  Matches LevelDB.
const MAX_HEIGHT: usize = 12;

/// Level-promotion probability denominator: a node is promoted with
/// probability 1/BRANCHING.  Matches LevelDB.
const BRANCHING: u32 = 4;

// Layout constants (computed from the Node struct at compile time).
const NODE_SIZE: usize = mem::size_of::<AtomicPtr<Node>>();
const NODE_ALIGN: usize = mem::align_of::<AtomicPtr<Node>>();

// ─── Node ────────────────────────────────────────────────────────────────────

/// A skip-list node.  The struct occupies exactly one pointer-sized word
/// (the level-0 next link); higher levels and the inline payload are stored
/// outside the struct — see the module-level memory layout diagram.
#[repr(transparent)]
struct Node {
  next: AtomicPtr<Node>,
}

impl Node {
  /// Raw pointer to the atomic slot for `level`.
  ///
  /// * Level 0 lives inside the struct (`&self.next`).
  /// * Level n lives n pointer-widths *before* the struct.
  ///
  /// # Safety
  /// `self` must have been allocated by `alloc_node_raw` with a height
  /// strictly greater than `level` (see the module-level *level invariant*).
  /// Violating this walks off the start of the allocation.
  #[inline]
  unsafe fn slot(&self, level: usize) -> *mut AtomicPtr<Node> {
    // SAFETY: the caller guarantees `level < height`, and `alloc_node_raw`
    // placed `height - 1` link slots immediately before the struct, so the
    // resulting pointer stays inside the same allocation.
    unsafe { (&self.next as *const AtomicPtr<Node> as *mut AtomicPtr<Node>).sub(level) }
  }

  /// Acquire-load of the level-`level` link.  Safe to call from concurrent
  /// readers: it synchronises with the Release store that published the
  /// returned node, so the node's payload and lower links are visible.
  ///
  /// # Safety
  /// Same requirement as [`Node::slot`]: `level` must be below the height
  /// this node was allocated with.
  #[inline]
  unsafe fn load_next(&self, level: usize) -> *mut Node {
    // SAFETY: `slot` requires `level < height`, forwarded from this
    // function's contract.  The slot was initialised (to null) by
    // `alloc_node_raw`, so the atomic is valid to load.
    unsafe { (*self.slot(level)).load(Ordering::Acquire) }
  }

  /// Release-store of the level-`level` link.  Publishes `ptr` (a fully
  /// initialised node) to concurrent readers that follow this link.
  ///
  /// # Safety
  /// Same requirement as [`Node::slot`]: `level` must be below the height
  /// this node was allocated with.
  #[inline]
  unsafe fn store_next(&self, level: usize, ptr: *mut Node) {
    // SAFETY: `slot` requires `level < height`, forwarded from this
    // function's contract; the slot is an initialised atomic.
    unsafe { (*self.slot(level)).store(ptr, Ordering::Release) }
  }

  /// Relaxed load of the level-`level` link.
  ///
  /// # Safety
  /// Same requirement as [`Node::slot`]: `level` must be below the height
  /// this node was allocated with.  In addition the caller must be the
  /// (sole) writer: without Acquire ordering the returned node's contents
  /// are only guaranteed visible to the thread that linked it.
  #[inline]
  unsafe fn relaxed_next(&self, level: usize) -> *mut Node {
    // SAFETY: `slot` requires `level < height`, forwarded from this
    // function's contract; the slot is an initialised atomic.
    unsafe { (*self.slot(level)).load(Ordering::Relaxed) }
  }

  /// Relaxed store of the level-`level` link.  Used to initialise a new
  /// node's links before it is published via a Release store on its
  /// predecessor, which then acts as the fence that makes these stores
  /// visible to readers.
  ///
  /// # Safety
  /// Same requirement as [`Node::slot`]: `level` must be below the height
  /// this node was allocated with.  The node must not yet be reachable by
  /// readers (otherwise a Release store is required).
  #[inline]
  unsafe fn relaxed_set_next(&self, level: usize, ptr: *mut Node) {
    // SAFETY: `slot` requires `level < height`, forwarded from this
    // function's contract; the slot lies inside the node's allocation.
    unsafe { (*self.slot(level)).store(ptr, Ordering::Relaxed) }
  }

  // ── Inline-payload accessors ────────────────────────────────────────────

  /// Pointer to the first byte of the inline payload, which immediately
  /// follows the Node struct in the allocation.
  ///
  /// # Safety
  /// `node` must be a pointer produced by `alloc_node_raw`.  The returned
  /// pointer is `NODE_SIZE` bytes past `node`, which is within the
  /// allocation; reading from it is only valid for the `data_size` bytes
  /// specified at allocation time.
  #[inline]
  unsafe fn data_ptr(node: *const Node) -> *const u8 {
    // SAFETY: `node` comes from `alloc_node_raw` (caller contract), which
    // reserved the payload bytes starting at this offset.
    unsafe { (node as *const u8).add(NODE_SIZE) }
  }

  /// The user key and sequence number of the entry stored in `node` — the
  /// two fields a comparison needs.
  ///
  /// # Safety
  /// `node` must be a pointer produced by `alloc_node_raw` whose payload
  /// has been fully written with an `Entry` encoding.  For a node obtained
  /// through a link this holds because `alloc_and_insert` writes the payload
  /// before publishing the node with a Release store, and links are read
  /// with Acquire loads.  The returned lifetime `'a` must not outlive the
  /// `Arena` that owns the allocation.
  #[inline]
  unsafe fn key_and_seq<'a>(node: *const Node) -> (&'a [u8], u64) {
    // SAFETY: `data_ptr` requires `node` from `alloc_node_raw`, and
    // `decode_key_raw` requires a complete, initialised entry at that
    // address — both forwarded from this function's contract.
    unsafe { Entry::decode_key_raw(Self::data_ptr(node)) }
  }

  /// The whole entry stored in `node`.
  ///
  /// # Safety
  /// Same contract as [`Node::key_and_seq`].
  #[inline]
  unsafe fn entry<'a>(node: *const Node) -> DecodedEntry<'a> {
    // SAFETY: forwarded from this function's contract, as in `key_and_seq`.
    unsafe { Entry::decode_raw(Self::data_ptr(node)) }
  }
}

// ─── Prefetch ────────────────────────────────────────────────────────────────

/// Ask the CPU to start pulling in the cache line at `p` — a node's level-0
/// link and the first payload bytes — so that if the traversal moves on to
/// it, its key is already (partly) in cache.  Mirrors RocksDB's `PREFETCH`
/// in `FindGreaterOrEqual` / `FindSpliceForLevel`.  Purely a hint: it never
/// faults, so a null or stale pointer is harmless.
#[inline(always)]
fn prefetch(p: *const Node) {
  #[cfg(target_arch = "x86_64")]
  {
    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
    // SAFETY: `sse` is part of the x86_64 baseline, so the target feature the
    // intrinsic requires is always present; and `prefetch` is a hint that
    // never faults, so the pointer's validity does not matter.
    unsafe { _mm_prefetch(p as *const i8, _MM_HINT_T0) };
  }
  #[cfg(not(target_arch = "x86_64"))]
  let _ = p;
}

// ─── Ordering helpers ────────────────────────────────────────────────────────
//
// Entries are ordered by user key ascending (via the pluggable comparator),
// then by sequence number *descending*, so the newest version of a key comes
// first and a search for `(key, seq)` lands on the newest version with a
// sequence number ≤ `seq`.

/// Three-way comparison of the entry stored in `n` against the search key
/// `(key, seq)`: `Less` means the node sorts before the key.
///
/// # Safety
/// `n` must be non-null and a fully written node: one reached through a link
/// (Acquire load) or held in the writer's splice.
#[inline]
unsafe fn compare_node(
  cmp: &dyn Comparator,
  n: *const Node,
  key: &[u8],
  seq: u64,
) -> std::cmp::Ordering {
  // SAFETY: `data_ptr` requires a node from `alloc_node_raw` and `key_raw`
  // a complete entry at that address — both forwarded from this function's
  // contract.
  let (nkey, seq_ptr) = unsafe { Entry::key_raw(Node::data_ptr(n)) };
  match cmp.compare(nkey, key) {
    // Same user key: the higher sequence number sorts first.  Only now is
    // the node's sequence number decoded.
    // SAFETY: `seq_ptr` came from `key_raw` on this very entry.
    Equal => seq.cmp(&unsafe { Entry::seq_raw(seq_ptr) }),
    o => o,
  }
}

/// Returns `true` if `(key, seq)` sorts strictly after node `n`.  A null `n`
/// is treated as +∞.
///
/// # Safety
/// `n` must be null or a fully written node, as for [`compare_node`].
#[inline]
unsafe fn key_after_node(cmp: &dyn Comparator, key: &[u8], seq: u64, n: *const Node) -> bool {
  // SAFETY: null is handled here; otherwise forwarded from this function's
  // contract.
  !n.is_null() && unsafe { compare_node(cmp, n, key, seq) } == Less
}

// ─── Splice ──────────────────────────────────────────────────────────────────

/// Cached insertion context.
///
/// After each insert the Splice records the predecessor (`prev`) and
/// successor (`next`) pointers at every level.  On the next insert — when
/// keys arrive in sequential order — those cached pointers let us skip the
/// O(log N) top-down traversal and start the search from the bottom, giving
/// amortised O(1) insertion for the sequential hot path.
///
/// The invariant is: `prev[i].key ≤ last_inserted.key < next[i].key` for all
/// `i < height`, and `prev[i]` / `next[i]` were reached via level-`i` links
/// (so they are valid for level `i` — see the module-level level invariant).
///
/// `Splice` is only reachable through [`SkipList`], whose `Send`/`Sync`
/// impls cover it; it deliberately has no impls of its own.
struct Splice {
  /// Number of valid levels in `prev`/`next`.
  height: usize,
  prev: [*mut Node; MAX_HEIGHT + 1],
  next: [*mut Node; MAX_HEIGHT + 1],
}

impl Splice {
  fn new() -> Self {
    Splice {
      height: 0,
      prev: [ptr::null_mut(); MAX_HEIGHT + 1],
      next: [ptr::null_mut(); MAX_HEIGHT + 1],
    }
  }
}

// ─── PRNG ────────────────────────────────────────────────────────────────────

/// Linear-congruential PRNG matching LevelDB's `util/random.h` exactly.
///
/// Period = 2³¹ − 1.  Used only for height selection during insertion.
struct Rng {
  seed: u32,
}

impl Rng {
  /// Mersenne prime modulus (2³¹ − 1).
  const M: u32 = 2_147_483_647;
  /// Primitive root of M.
  const A: u64 = 16_807;

  fn new(seed: u32) -> Self {
    let s = seed & Self::M;
    Rng {
      seed: if s == 0 { 1 } else { s },
    }
  }

  fn next_u32(&mut self) -> u32 {
    let product = (self.seed as u64) * Self::A;
    let mut s = ((product >> 31) + (product & Self::M as u64)) as u32;
    if s > Self::M {
      s -= Self::M;
    }
    self.seed = s;
    s
  }
}

/// Generate a random height in `[1, MAX_HEIGHT]`.
///
/// Height increases with probability 1/[`BRANCHING`] per level, giving a
/// geometric distribution identical to LevelDB's.
fn random_height(rng: &mut Rng) -> usize {
  let mut h = 1;
  while h < MAX_HEIGHT && rng.next_u32().is_multiple_of(BRANCHING) {
    h += 1;
  }
  h
}

// ─── SkipList ────────────────────────────────────────────────────────────────

/// State that only the writer touches.  Kept behind an `UnsafeCell` so that
/// inserts go through `&SkipList` like reads do: the list is shared between
/// the writer and lock-free readers, and a `&mut SkipList` would (wrongly)
/// assert exclusive access to the whole struct while readers hold `&SkipList`.
struct WriterState {
  rng: Rng,
  /// Cached splice from the last insert, accelerates sequential writes.
  splice: Splice,
}

/// An ordered skip list that stores entry-encoded payloads inline in each
/// node, backed by an owned [`Arena`].
///
/// # Thread safety
///
/// Reads are lock-free.  Writes must be serialised externally — see
/// [`SkipList::alloc_and_insert`] — which the DB write mutex in `Db`
/// provides.
pub(crate) struct SkipList {
  arena: Arena,
  head: *mut Node,
  max_height: AtomicUsize,
  /// Number of entries (excludes the sentinel head).
  len: AtomicUsize,
  writer: UnsafeCell<WriterState>,
  /// User-key comparator; sequence numbers break ties (descending).
  comparator: Arc<dyn Comparator>,
}

// SAFETY (Send): the list owns its arena; `head` and every pointer in
// `writer.splice` point into that arena, so moving the list to another
// thread moves everything they reference along with it.  `Arena` (bumpalo)
// is `Send`, and `Arc<dyn Comparator>` is `Send` because `Comparator: Send +
// Sync`.
//
// SAFETY (Sync): a shared `&SkipList` is used by any number of lock-free
// readers and by at most one writer at a time — `alloc_and_insert` is an
// `unsafe fn` whose contract demands that.  Readers only touch:
//   • `head` (never written after `new`) and `comparator` (`Sync`);
//   • `max_height` / `len` (atomics);
//   • node memory reached through links, always via Acquire loads that pair
//     with the writer's Release stores, so a reachable node's payload and
//     links are fully written before a reader can see it.
// The writer alone touches `arena` (bumpalo's `Bump` is `!Sync`; readers
// never call into it) and `writer` (the `UnsafeCell`).  Nodes are never
// freed until the arena drops with the list, so no pointer dangles while
// any `&SkipList` exists.
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

impl SkipList {
  pub fn new(arena: Arena, comparator: Arc<dyn Comparator>) -> Self {
    // SAFETY: `height = MAX_HEIGHT` is within `[1, MAX_HEIGHT]`, and
    // `data_size = 0` because the sentinel head carries no payload.  The
    // returned pointer is valid for the lifetime of `arena`, which moves into
    // the `SkipList` below and therefore outlives every use of `head`.
    let head = unsafe { alloc_node_raw(&arena, 0, MAX_HEIGHT) };
    SkipList {
      arena,
      head,
      max_height: AtomicUsize::new(1),
      len: AtomicUsize::new(0),
      writer: UnsafeCell::new(WriterState {
        rng: Rng::new(0xdead_beef),
        splice: Splice::new(),
      }),
      comparator,
    }
  }

  #[cfg(test)]
  pub fn len(&self) -> usize {
    self.len.load(Ordering::Relaxed)
  }

  /// Bytes allocated from the underlying arena.
  pub(crate) fn arena_memory_usage(&self) -> usize {
    self.arena.memory_usage()
  }

  /// Return an iterator positioned before the first entry.
  ///
  /// The caller must position it (`seek_to_first()`, `seek_to_last()` or
  /// `seek()`) before reading `entry()`.  Lock-free; safe for concurrent
  /// reads.
  pub(crate) fn iter(&self) -> SkipListIter<'_> {
    SkipListIter {
      list: self,
      current: ptr::null(),
    }
  }

  // ── Internal traversals ──────────────────────────────────────────────────

  #[inline]
  fn max_height(&self) -> usize {
    self.max_height.load(Ordering::Relaxed)
  }

  /// The first node whose entry sorts ≥ `(key, seq)`, or null.
  fn find_greater_or_equal(&self, key: &[u8], seq: u64) -> *mut Node {
    let cmp = &*self.comparator;
    let mut x = self.head;
    let mut level = self.max_height() - 1;
    // The node one level up that made us descend: it sorts after the key,
    // so when the level below leads to the same node we skip re-comparing.
    let mut last_bigger: *mut Node = ptr::null_mut();
    loop {
      // SAFETY: `x` is `head` (valid for every level < MAX_HEIGHT) or a node
      // reached by following a level-`level` link, and `level` only ever
      // decreases — so by the level invariant `level` is below `x`'s height.
      // `x` is never null: it only advances to a non-null `next`.  `next` is
      // null or a published node, as `compare_node` requires.
      let (next, ord) = unsafe {
        let next = (*x).load_next(level);
        let ord = if next.is_null() || ptr::eq(next, last_bigger) {
          Greater
        } else {
          // `next` was reached via a level-`level` link, so that level is
          // valid for it: fetch the node after it while we compare.
          prefetch((*next).load_next(level));
          compare_node(cmp, next, key, seq)
        };
        (next, ord)
      };
      match ord {
        Equal => return next,
        Less => x = next,
        Greater if level == 0 => return next,
        Greater => {
          last_bigger = next;
          level -= 1;
        }
      }
    }
  }

  /// The last node whose entry sorts < `(key, seq)`, or `head` if none.
  fn find_less_than(&self, key: &[u8], seq: u64) -> *mut Node {
    let cmp = &*self.comparator;
    let mut x = self.head;
    let mut level = self.max_height() - 1;
    // The node one level up that made us descend: the key is known not to
    // sort after it, so when the level below leads to the same node we skip
    // re-comparing.
    let mut last_not_after: *mut Node = ptr::null_mut();
    loop {
      // SAFETY: as in `find_greater_or_equal`: `level` is below `x`'s height
      // by the level invariant, `x` is non-null, and `next` is null or a
      // published node as `key_after_node` requires.
      let (next, after) = unsafe {
        let next = (*x).load_next(level);
        let after = if next.is_null() || ptr::eq(next, last_not_after) {
          false
        } else {
          // Level `level` is valid for `next` (reached via that level).
          prefetch((*next).load_next(level));
          key_after_node(cmp, key, seq, next)
        };
        (next, after)
      };
      if after {
        x = next;
      } else if level == 0 {
        return x;
      } else {
        last_not_after = next;
        level -= 1;
      }
    }
  }

  /// The last node in the list, or `head` if the list is empty.
  fn find_last(&self) -> *mut Node {
    let mut x = self.head;
    let mut level = self.max_height() - 1;
    loop {
      // SAFETY: as in `find_greater_or_equal`: `level` is below `x`'s height
      // by the level invariant and `x` is non-null.
      let next = unsafe { (*x).load_next(level) };
      if !next.is_null() {
        x = next;
      } else if level == 0 {
        return x;
      } else {
        level -= 1;
      }
    }
  }

  // ── Public operations ────────────────────────────────────────────────────

  /// The first entry that sorts ≥ `(key, seq)` in internal order, i.e. the
  /// newest version of `key` with a sequence number ≤ `seq`, or — if there
  /// is none — the first entry of a later key.
  ///
  /// Returns `None` past the end.  The entry borrows from the arena for the
  /// lifetime of `&self`.
  pub fn find_first_at_or_after(&self, key: &[u8], seq: u64) -> Option<DecodedEntry<'_>> {
    let node = self.find_greater_or_equal(key, seq);
    if node.is_null() {
      None
    } else {
      // SAFETY: `node` is non-null and was reached through an Acquire load
      // of a link, so it is a fully initialised node (payload written before
      // the Release store that linked it).  The returned lifetime is that of
      // `&self`, which owns the arena.
      Some(unsafe { Node::entry(node) })
    }
  }

  /// Allocate a node whose inline payload will hold `data_size` bytes, then
  /// call `write_fn` to fill that payload, and finally link the node into the
  /// skip list.
  ///
  /// This two-phase approach lets the caller encode directly into the
  /// arena-allocated inline area, avoiding any intermediate heap allocation.
  ///
  /// # Safety
  /// No other call to `alloc_and_insert` on this list may run concurrently:
  /// the caller must serialise writers (in `Db`, the write leader holds the
  /// `DbState` mutex).  Concurrent readers (`find_first_at_or_after`,
  /// iterators) are fine.  `write_fn` must fill every byte of the slice it
  /// is given with a valid `Entry` encoding whose `(key, seq)` is not already
  /// present in the list.
  pub unsafe fn alloc_and_insert<F>(&self, data_size: usize, write_fn: F)
  where
    F: FnOnce(&mut [u8]),
  {
    let cmp = &*self.comparator;
    // SAFETY: the caller guarantees this is the only writer, and readers
    // never touch `writer`, so this is the sole live reference to it.
    let w = unsafe { &mut *self.writer.get() };
    let height = random_height(&mut w.rng);

    let cur_max = self.max_height();
    if height > cur_max {
      // Relaxed is enough: a reader that observes the new max_height will
      // follow either a null link from head (and drop a level) or, once
      // linked, the new node — both correct.  Readers that observe the old
      // value are unaffected.  The splice levels `cur_max..height` are filled
      // in by the full recompute below (`splice.height < effective_max`).
      self.max_height.store(height, Ordering::Relaxed);
    }

    // Allocate and fill the node *before* computing the insertion position,
    // so the comparator can read the inline payload during traversal.
    // SAFETY: `self.arena` is owned by `self` and therefore lives at least as
    // long as the returned `node` pointer.  `height` is in `[1, MAX_HEIGHT]`
    // (guaranteed by `random_height`), satisfying `alloc_node_raw`'s contract.
    let node = unsafe { alloc_node_raw(&self.arena, data_size, height) };
    // SAFETY: `node` was just allocated with `data_size` payload bytes and has
    // not yet been linked into the list, so no other reference to this memory
    // region exists.  The slice is valid for `data_size` bytes of writes.
    let payload_buf =
      unsafe { slice::from_raw_parts_mut(Node::data_ptr(node) as *mut u8, data_size) };
    write_fn(payload_buf);
    // SAFETY: `node` is from `alloc_node_raw`; `write_fn` has just written a
    // complete entry into its payload (caller contract).
    let (key, seq) = unsafe { Node::key_and_seq(node) };

    let effective_max = cur_max.max(height);
    let splice = &mut w.splice;

    // ── Determine how many levels need recomputing ──────────────────────
    //
    // Walk up the cached splice until we find a level that still brackets
    // the new key.  Use the pessimistic strategy (recompute everything if
    // the key is outside the bracket at any level), which is simpler and
    // still gives amortised O(1) for the sequential hot path.
    let recompute_height = if splice.height < effective_max {
      // Splice was never used, or effective_max just grew.
      splice.prev[effective_max] = self.head;
      splice.next[effective_max] = ptr::null_mut();
      splice.height = effective_max;
      effective_max
    } else {
      let mut h = 0;
      while h < effective_max {
        let pn = splice.prev[h];
        let nn = splice.next[h];
        // SAFETY: `splice.prev[h]` is `head` or a node that was reached (and
        // linked) at level `h`, so `h` is below its height; we are the sole
        // writer (caller contract), which `relaxed_next` requires.  `pn` and
        // `nn` are `head`, null, or published nodes, as `key_after_node`
        // requires — and `pn` is only compared when it is not `head`.
        let (tight, before_prev, after_next) = unsafe {
          let tight = (*pn).relaxed_next(h) == nn;
          if !tight {
            (false, false, false)
          } else {
            let before_prev = pn != self.head && !key_after_node(cmp, key, seq, pn);
            let after_next = !before_prev && key_after_node(cmp, key, seq, nn);
            (true, before_prev, after_next)
          }
        };
        if !tight {
          // Stale at this level: move up.
          h += 1;
        } else if before_prev || after_next {
          // Key falls outside the cached bracket: start over.
          h = effective_max;
        } else {
          break; // This level brackets the new key — done.
        }
      }
      h
    };

    if recompute_height > 0 {
      recompute_splice_levels(cmp, key, seq, splice, recompute_height);
    }

    // The new entry must sort strictly between its level-0 neighbours:
    // duplicate `(key, seq)` pairs are a caller bug.
    // SAFETY: `splice.prev[0]` is `head` or a published node (only compared
    // when not `head`); `splice.next[0]` is null or a published node.
    debug_assert!(unsafe {
      (splice.prev[0] == self.head || compare_node(cmp, splice.prev[0], key, seq) == Less)
        && (splice.next[0].is_null() || compare_node(cmp, splice.next[0], key, seq) == Greater)
    });

    // ── Link the node into every level ──────────────────────────────────
    for i in 0..height {
      // SAFETY: `node` was allocated with `height` levels and `i < height`.
      // It is not yet reachable by readers, so a Relaxed store suffices: the
      // Release store on the predecessor below publishes it.
      unsafe { (*node).relaxed_set_next(i, splice.next[i]) };
      // SAFETY: `splice.prev[i]` is `head` or a node reached via level-`i`
      // links, so `i` is below its height.  The Release ordering ensures that
      // once a reader traverses this link to reach `node`, the node's payload
      // (written by `write_fn`) and its lower-level links (set by the relaxed
      // stores above) are already visible to that reader.
      unsafe { (*splice.prev[i]).store_next(i, node) };
      // Update splice so the next sequential insert can reuse it.
      splice.prev[i] = node;
    }

    self.len.fetch_add(1, Ordering::Relaxed);
  }
}

// ─── Free traversal helpers ──────────────────────────────────────────────────
//
// These functions don't need access to `SkipList` fields — extracting them
// keeps the hot insert path free of any `&self` / `&mut splice` entanglement.

/// Find the tightest `(prev, next)` bracket for `(key, seq)` at `level`,
/// starting from `before` (which must precede the key) and stopping no later
/// than `after` (which must follow the key, or null).
#[inline]
fn find_splice_for_level(
  cmp: &dyn Comparator,
  key: &[u8],
  seq: u64,
  mut before: *mut Node,
  after: *mut Node,
  level: usize,
) -> (*mut Node, *mut Node) {
  loop {
    // SAFETY: `before` is either `head` or a node reached through a
    // level-`level` link (the initial `before` comes from the splice one level
    // up, whose nodes are valid at that higher level and hence at `level`;
    // later ones are read from level-`level` links in this loop).  By the
    // level invariant `level` is below `before`'s height.  `next` is null or
    // a published node, as `key_after_node` requires.
    let (next, after_next) = unsafe {
      let next = (*before).load_next(level);
      let after_next = if next.is_null() || ptr::eq(next, after) {
        false
      } else {
        // Level `level` is valid for `next` (reached via that level), and so
        // is `level - 1`: fetch the next candidates at this level and at
        // the level below, which the recompute visits next.
        prefetch((*next).load_next(level));
        if level > 0 {
          prefetch((*next).load_next(level - 1));
        }
        key_after_node(cmp, key, seq, next)
      };
      (next, after_next)
    };
    if !after_next {
      return (before, next);
    }
    before = next;
  }
}

/// Recompute splice levels `[0, recompute_height)` top-down, using the
/// already-valid bracket at `splice.prev/next[recompute_height]`.
fn recompute_splice_levels(
  cmp: &dyn Comparator,
  key: &[u8],
  seq: u64,
  splice: &mut Splice,
  recompute_height: usize,
) {
  for i in (0..recompute_height).rev() {
    let (p, n) = find_splice_for_level(cmp, key, seq, splice.prev[i + 1], splice.next[i + 1], i);
    splice.prev[i] = p;
    splice.next[i] = n;
  }
}

// ─── Arena allocation helper ─────────────────────────────────────────────────

/// Allocate a raw node from `arena` with the given `height` and `data_size`.
///
/// All next pointers are set to null (relaxed).  The payload bytes are *not*
/// initialised; the caller is responsible for filling them before the node
/// is made visible to readers.
///
/// # Safety
/// * `height` must be in `[1, MAX_HEIGHT]`.
/// * The returned pointer is valid only for the lifetime of `arena`.  The
///   caller must ensure `arena` outlives every use of the returned pointer.
unsafe fn alloc_node_raw(arena: &Arena, data_size: usize, height: usize) -> *mut Node {
  debug_assert!((1..=MAX_HEIGHT).contains(&height));

  // Layout: prefix of (height−1) higher-level AtomicPtrs, then the Node
  // struct (the level-0 AtomicPtr), then the payload.
  let prefix = NODE_SIZE * (height - 1);
  let total = prefix + NODE_SIZE + data_size;

  // `raw` points to `total` bytes aligned to `NODE_ALIGN`.  The node struct
  // begins at `raw + prefix`; the prefix area holds the higher-level link
  // slots that `slot(i)` for i > 0 reaches by subtracting from the node ptr.
  let raw = arena.allocate_aligned(total, NODE_ALIGN);
  // SAFETY: `prefix < total`, so the offset stays inside the allocation, and
  // `prefix` is a multiple of `NODE_ALIGN`, so the result is aligned for
  // `AtomicPtr<Node>`.
  let node = unsafe { raw.add(prefix) as *mut Node };

  // Null-initialise every next pointer.  For level 0, `slot(0)` returns
  // `&node.next` (within the struct).  For level i > 0, `slot(i)` steps back
  // `i` pointer-widths, landing within the prefix area — all within the
  // bounds of the `total`-byte allocation.
  for i in 0..height {
    // SAFETY: `i < height`, the height this node is being allocated with; the
    // node is not reachable by anyone yet, so a Relaxed store is fine.
    unsafe { (*node).relaxed_set_next(i, ptr::null_mut()) };
  }

  node
}

// ─── SkipListIter ────────────────────────────────────────────────────────────

/// Bidirectional iterator over a [`SkipList`].
///
/// Starts in an invalid (unpositioned) state; the caller must invoke
/// [`seek_to_first`], [`seek_to_last`] or [`seek`] before reading
/// [`entry`].  Lock-free reads via Acquire loads.
///
/// [`seek_to_first`]: SkipListIter::seek_to_first
/// [`seek_to_last`]: SkipListIter::seek_to_last
/// [`seek`]: SkipListIter::seek
/// [`entry`]: SkipListIter::entry
pub(crate) struct SkipListIter<'a> {
  /// The list being iterated; keeps the arena alive for `'a`.
  list: &'a SkipList,
  /// Current position (`null` when unpositioned or exhausted).
  current: *const Node,
}

// SAFETY: apart from `current`, the iterator is a `&'a SkipList`, which is
// `Send + Sync` because `SkipList: Sync`.  `current` is a position inside
// `list`'s arena (kept alive for `'a`) and is only ever dereferenced through
// the same Acquire-load protocol readers use, so moving or sharing the
// iterator across threads adds nothing beyond sharing `&SkipList`.
unsafe impl Send for SkipListIter<'_> {}
unsafe impl Sync for SkipListIter<'_> {}

impl<'a> SkipListIter<'a> {
  pub(crate) fn valid(&self) -> bool {
    !self.current.is_null()
  }

  /// Position at the first entry.  Leaves the iterator invalid on an empty
  /// list.
  pub(crate) fn seek_to_first(&mut self) {
    // SAFETY: `head` is valid for every level below MAX_HEIGHT, and 0 is.
    self.current = unsafe { (*self.list.head).load_next(0) };
  }

  /// Position at the last entry.  Leaves the iterator invalid on an empty
  /// list.
  pub(crate) fn seek_to_last(&mut self) {
    let last = self.list.find_last();
    self.current = if ptr::eq(last, self.list.head) {
      ptr::null()
    } else {
      last
    };
  }

  /// Move to the predecessor of the current entry; becomes invalid at the
  /// first entry.
  ///
  /// There are no back-pointers: like LevelDB, we search for the last node
  /// that sorts before the current key, which is O(log N).
  ///
  /// # Panics
  /// Panics (debug) if `valid()` is false.
  pub(crate) fn prev(&mut self) {
    debug_assert!(self.valid(), "prev() called on exhausted iterator");
    // SAFETY: `current` is non-null (checked by `valid()`) and was reached
    // through an Acquire load of a link, so it is a fully initialised node.
    let (key, seq) = unsafe { Node::key_and_seq(self.current) };
    let p = self.list.find_less_than(key, seq);
    self.current = if ptr::eq(p, self.list.head) {
      ptr::null()
    } else {
      p
    };
  }

  /// Position at the first entry that sorts ≥ `(key, seq)` — see
  /// [`SkipList::find_first_at_or_after`].
  pub(crate) fn seek(&mut self, key: &[u8], seq: u64) {
    self.current = self.list.find_greater_or_equal(key, seq);
  }

  /// The current entry.
  ///
  /// # Panics
  /// Panics (debug) if `valid()` is false.
  pub(crate) fn entry(&self) -> DecodedEntry<'a> {
    debug_assert!(self.valid(), "entry() called on exhausted iterator");
    // SAFETY: `current` is non-null (checked by `valid()`), was reached
    // through an Acquire load of a link and is therefore a fully initialised
    // node.  The lifetime `'a` is that of the borrowed list, which owns the
    // arena.
    unsafe { Node::entry(self.current) }
  }

  /// Advance to the next entry.
  ///
  /// # Panics
  /// Panics (debug) if `valid()` is false.
  pub(crate) fn advance(&mut self) {
    debug_assert!(self.valid(), "advance() called on exhausted iterator");
    // SAFETY: `current` is a valid node (checked by `valid()`); every node is
    // valid for level 0.
    self.current = unsafe { (*self.current).load_next(0) };
  }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;
  use crate::comparator::BytewiseComparator;

  fn make_list() -> SkipList {
    SkipList::new(Arena::default(), Arc::new(BytewiseComparator))
  }

  fn insert_key(list: &SkipList, key: &[u8], seq: u64, value: &[u8]) {
    let size = Entry::encoded_value_size(seq, key, value);
    // SAFETY: tests insert from a single thread, so no concurrent writer
    // exists; `write_value_to` fills exactly `size` bytes.
    unsafe { list.alloc_and_insert(size, |buf| Entry::write_value_to(buf, seq, key, value)) };
  }

  fn insert_tombstone(list: &SkipList, key: &[u8], seq: u64) {
    let size = Entry::encoded_deletion_size(seq, key);
    // SAFETY: as in `insert_key`.
    unsafe { list.alloc_and_insert(size, |buf| Entry::write_deletion_to(buf, seq, key)) };
  }

  /// Latest value of `key`, or `None` if absent or a tombstone.
  fn seek(list: &SkipList, key: &[u8]) -> Option<Vec<u8>> {
    list
      .find_first_at_or_after(key, u64::MAX)
      .filter(|e| e.key == key)
      .and_then(|e| e.value.map(|v| v.to_vec()))
  }

  /// All `(key, seq)` pairs in forward iteration order.
  fn forward_keys(list: &SkipList) -> Vec<(Vec<u8>, u64)> {
    let mut it = list.iter();
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
      let e = it.entry();
      out.push((e.key.to_vec(), e.seq));
      it.advance();
    }
    out
  }

  /// All `(key, seq)` pairs in backward iteration order.
  fn backward_keys(list: &SkipList) -> Vec<(Vec<u8>, u64)> {
    let mut it = list.iter();
    it.seek_to_last();
    let mut out = Vec::new();
    while it.valid() {
      let e = it.entry();
      out.push((e.key.to_vec(), e.seq));
      it.prev();
    }
    out
  }

  /// Deterministic xorshift for shuffles.
  fn shuffled(n: u64, mut rng: u64) -> Vec<u64> {
    let mut v: Vec<u64> = (0..n).collect();
    for i in (1..n as usize).rev() {
      rng ^= rng << 13;
      rng ^= rng >> 7;
      rng ^= rng << 17;
      v.swap(i, rng as usize % (i + 1));
    }
    v
  }

  #[test]
  fn empty_miss() {
    let list = make_list();
    assert!(seek(&list, b"foo").is_none());
    assert!(list.find_first_at_or_after(b"", u64::MAX).is_none());
  }

  #[test]
  fn insert_and_find() {
    let list = make_list();
    insert_key(&list, b"foo", 1, b"bar");
    assert_eq!(seek(&list, b"foo"), Some(b"bar".to_vec()));
  }

  #[test]
  fn newer_version_wins() {
    let list = make_list();
    insert_key(&list, b"foo", 1, b"v1");
    insert_key(&list, b"foo", 2, b"v2");
    assert_eq!(seek(&list, b"foo"), Some(b"v2".to_vec()));
  }

  #[test]
  fn tombstone_hides_value() {
    let list = make_list();
    insert_key(&list, b"foo", 1, b"bar");
    insert_tombstone(&list, b"foo", 2);
    let e = list.find_first_at_or_after(b"foo", u64::MAX).unwrap();
    assert_eq!(e.key, b"foo");
    assert_eq!(e.seq, 2);
    assert!(e.value.is_none()); // tombstone
  }

  #[test]
  fn seek_with_sequence_number_skips_newer_versions() {
    let list = make_list();
    for seq in 1..=5 {
      insert_key(&list, b"k", seq, format!("v{seq}").as_bytes());
    }
    insert_key(&list, b"z", 9, b"Z");
    // Exact hit on a version.
    let e = list.find_first_at_or_after(b"k", 3).unwrap();
    assert_eq!((e.key, e.seq, e.value), (&b"k"[..], 3, Some(&b"v3"[..])));
    // Snapshot newer than every version: newest.
    let e = list.find_first_at_or_after(b"k", 100).unwrap();
    assert_eq!(e.seq, 5);
    // Snapshot older than every version: falls through to the next key.
    let e = list.find_first_at_or_after(b"k", 0).unwrap();
    assert_eq!((e.key, e.seq), (&b"z"[..], 9));
    // Key between existing keys.
    let e = list.find_first_at_or_after(b"m", u64::MAX).unwrap();
    assert_eq!(e.key, b"z");
  }

  #[test]
  fn sequential_inserts() {
    let list = make_list();
    for i in 0u64..1000 {
      let key = format!("{i:016}");
      insert_key(&list, key.as_bytes(), i, b"v");
    }
    assert_eq!(list.len(), 1000);
    // Spot-check a few
    assert!(seek(&list, b"0000000000000042").is_some());
    assert!(seek(&list, b"0000000000000999").is_some());
    assert!(seek(&list, b"0000000000001000").is_none());
  }

  #[test]
  fn ordering_preserved() {
    let list = make_list();
    insert_key(&list, b"bbb", 1, b"B");
    insert_key(&list, b"aaa", 2, b"A");
    insert_key(&list, b"ccc", 3, b"C");
    assert_eq!(seek(&list, b"aaa"), Some(b"A".to_vec()));
    assert_eq!(seek(&list, b"bbb"), Some(b"B".to_vec()));
    assert_eq!(seek(&list, b"ccc"), Some(b"C".to_vec()));
  }

  #[test]
  fn random_inserts_iterate_in_internal_order_both_ways() {
    // Random insertion order, several versions per key, keys of varying
    // length; forward and backward iteration must agree with a sorted model.
    let list = make_list();
    let mut expected: Vec<(Vec<u8>, u64)> = Vec::new();
    for (seq, i) in shuffled(4000, 0x9e37_79b9_7f4a_7c15)
      .into_iter()
      .enumerate()
    {
      let key = format!("{:0width$}", i % 700, width = 3 + (i % 5) as usize).into_bytes();
      insert_key(&list, &key, seq as u64, b"v");
      expected.push((key, seq as u64));
    }
    // Internal order: key ascending, then sequence descending.
    expected.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

    assert_eq!(list.len(), expected.len());
    assert_eq!(forward_keys(&list), expected);
    expected.reverse();
    assert_eq!(backward_keys(&list), expected);
  }

  #[test]
  fn insert_hint_survives_jumps_before_and_after_the_splice() {
    // Alternate between two distant regions, and go backwards within one,
    // so the cached splice is repeatedly stale, too far left and too far
    // right.
    let list = make_list();
    let mut expected = Vec::new();
    for i in 0u64..500 {
      for key in [
        format!("a{:05}", 1000 - i),
        format!("z{i:05}"),
        format!("m{:05}", i * 7 % 500),
      ] {
        insert_key(&list, key.as_bytes(), i * 3, b"v");
        expected.push((key.into_bytes(), i * 3));
      }
    }
    expected.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    assert_eq!(forward_keys(&list), expected);
  }

  // ── Backward iteration tests ──────────────────────────────────────────────

  #[test]
  fn seek_to_last_empty_list() {
    let list = make_list();
    let mut it = list.iter();
    it.seek_to_last();
    assert!(!it.valid());
  }

  #[test]
  fn seek_to_last_single_entry() {
    let list = make_list();
    insert_key(&list, b"only", 1, b"v");
    let mut it = list.iter();
    it.seek_to_last();
    assert!(it.valid());
    assert_eq!(it.entry().key, b"only");
    it.prev();
    assert!(!it.valid());
  }

  #[test]
  fn seek_to_last_returns_lexicographically_last() {
    let list = make_list();
    insert_key(&list, b"aaa", 1, b"A");
    insert_key(&list, b"ccc", 2, b"C");
    insert_key(&list, b"bbb", 3, b"B");
    let mut it = list.iter();
    it.seek_to_last();
    assert!(it.valid());
    // The skip list stores entries in Entry order (key ASC, seq DESC).
    // "ccc" is lexicographically last.
    assert_eq!(it.entry().key, b"ccc");
  }

  #[test]
  fn prev_traverses_all_entries_backward() {
    let list = make_list();
    insert_key(&list, b"a", 1, b"A");
    insert_key(&list, b"b", 2, b"B");
    insert_key(&list, b"c", 3, b"C");
    assert_eq!(
      backward_keys(&list),
      vec![(b"c".to_vec(), 3), (b"b".to_vec(), 2), (b"a".to_vec(), 1)]
    );
  }

  #[test]
  fn prev_at_first_entry_becomes_invalid() {
    let list = make_list();
    insert_key(&list, b"x", 1, b"v");
    let mut it = list.iter();
    it.seek_to_first();
    assert!(it.valid());
    it.prev();
    assert!(!it.valid());
  }

  #[test]
  fn prev_from_a_seek_in_the_middle_and_across_versions() {
    let list = make_list();
    for (key, seq) in [(b"a", 1u64), (b"b", 2), (b"b", 3), (b"b", 4), (b"c", 5)] {
      insert_key(&list, key, seq, b"v");
    }
    let mut it = list.iter();
    // Seek lands on b@3 (newest version with seq ≤ 3).
    it.seek(b"b", 3);
    assert_eq!((it.entry().key, it.entry().seq), (&b"b"[..], 3));
    it.prev();
    assert_eq!((it.entry().key, it.entry().seq), (&b"b"[..], 4));
    it.prev();
    assert_eq!((it.entry().key, it.entry().seq), (&b"a"[..], 1));
    it.prev();
    assert!(!it.valid());
    // Seek past the end, then walk back from the last entry.
    it.seek(b"zzz", u64::MAX);
    assert!(!it.valid());
    it.seek_to_last();
    assert_eq!(it.entry().key, b"c");
    it.prev();
    assert_eq!((it.entry().key, it.entry().seq), (&b"b"[..], 2));
  }

  // ── Concurrency ───────────────────────────────────────────────────────────

  #[test]
  fn concurrent_readers_only_ever_see_sorted_published_entries() {
    // One writer inserts keys in random order while readers scan and seek
    // without any locking.  Every reader must observe a sorted sequence
    // whose entries are all complete (key == value), and seeks must land on
    // an entry ≥ the target.
    use std::sync::atomic::AtomicBool;

    const N: u64 = 20_000;
    let list = make_list();
    let done = AtomicBool::new(false);
    let key_of = |i: u64| format!("{i:08}").into_bytes();

    std::thread::scope(|s| {
      for r in 0..3 {
        let list = &list;
        let done = &done;
        s.spawn(move || {
          let mut scans = 0u64;
          let mut rng = 0xc0ff_ee00_u64 + r;
          while !done.load(Ordering::Acquire) || scans < 2 {
            // Full forward scan: strictly increasing, fully written entries.
            let mut it = list.iter();
            it.seek_to_first();
            let mut prev: Option<Vec<u8>> = None;
            while it.valid() {
              let e = it.entry();
              assert_eq!(e.value, Some(e.key), "torn entry observed");
              if let Some(p) = &prev {
                assert!(p.as_slice() < e.key, "out of order: {p:?} !< {:?}", e.key);
              }
              prev = Some(e.key.to_vec());
              it.advance();
            }
            // Random seeks.
            for _ in 0..64 {
              rng ^= rng << 13;
              rng ^= rng >> 7;
              rng ^= rng << 17;
              let target = key_of(rng % N);
              if let Some(e) = list.find_first_at_or_after(&target, u64::MAX) {
                assert!(e.key >= target.as_slice());
                assert_eq!(e.value, Some(e.key));
              }
            }
            scans += 1;
          }
        });
      }

      for (seq, i) in shuffled(N, 0x1234_5678_9abc_def0).into_iter().enumerate() {
        let key = key_of(i);
        insert_key(&list, &key, seq as u64, &key);
      }
      done.store(true, Ordering::Release);
    });

    assert_eq!(list.len() as u64, N);
    let keys = forward_keys(&list);
    assert_eq!(keys.len() as u64, N);
    assert!(keys.windows(2).all(|w| w[0].0 < w[1].0));
  }

  // ── Memory ────────────────────────────────────────────────────────────────

  #[test]
  fn per_entry_overhead_is_bounded() {
    // Per node the arena holds the payload plus `height` link words, with
    // E[height] = 1/(1 - 1/BRANCHING) = 4/3, i.e. ≈ 10.7 bytes on 64-bit,
    // plus up to `NODE_ALIGN - 1` = 7 bytes of alignment padding.  Nothing
    // else is stored per node; guard that with a 12 + 7 byte budget.
    let list = make_list();
    let before = list.arena_memory_usage();
    let n = 2000u64;
    let mut payload = 0usize;
    for i in 0..n {
      let key = format!("{i:016}");
      let value = [b'v'; 100];
      payload += Entry::encoded_value_size(i, key.as_bytes(), &value);
      insert_key(&list, key.as_bytes(), i, &value);
    }
    let overhead = list.arena_memory_usage() - before - payload;
    assert!(
      overhead <= (12 + NODE_ALIGN - 1) * n as usize,
      "{:.1} bytes of overhead per entry",
      overhead as f64 / n as f64
    );
  }
}
