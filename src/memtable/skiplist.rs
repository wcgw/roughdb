//    Copyright (c) 2023 The RoughDB Authors
//
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
//!           [ u32, little-endian ]           ←── byte length of inline payload
//!           [ payload bytes … ]              ←── varint-encoded Entry
//! ```
//!
//! The `*mut Node` pointer always points to the level-0 link.  Higher levels
//! are accessed by walking *backwards* in memory (`node.slot(n)` subtracts `n`
//! pointer widths from the level-0 address).  This mirrors RocksDB's
//! `InlineSkipList` layout, which keeps both the hot level-0 link and the
//! payload in the same or adjacent cache lines during sequential traversal.
//!
//! # Thread safety
//!
//! Writes must be serialised by the caller (the `RwLock` in `Memtable`).
//! Reads are lock-free: node links are published with release stores and
//! consumed with acquire loads, matching LevelDB's model.

use super::arena::Arena;
use super::entry::Entry;
use std::mem;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Maximum number of levels.  Matches LevelDB.
const MAX_HEIGHT: usize = 12;

/// Level-promotion probability denominator: a node is promoted with
/// probability 1/BRANCHING.  Matches LevelDB.
const BRANCHING: u32 = 4;

// Layout constants (computed from the Node struct at compile time).
const NODE_SIZE: usize = mem::size_of::<AtomicPtr<Node>>();
const NODE_ALIGN: usize = mem::align_of::<AtomicPtr<Node>>();
const DATA_LEN_SIZE: usize = mem::size_of::<u32>();

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
  /// `level` must be strictly less than the height this node was allocated
  /// with.  Violating this walks off the start of the allocation.
  #[inline]
  unsafe fn slot(&self, level: usize) -> *mut AtomicPtr<Node> {
    (&self.next as *const AtomicPtr<Node> as *mut AtomicPtr<Node>).sub(level)
  }

  /// Acquire-load: safe to call from concurrent readers.
  #[inline]
  pub fn load_next(&self, level: usize) -> *mut Node {
    // SAFETY: `slot(level)` is valid as long as `level < height_of_node`.
    // All callers in this module traverse only levels that were initialised
    // by `alloc_node_raw`, and never exceed the node's allocated height.
    unsafe { (*self.slot(level)).load(Ordering::Acquire) }
  }

  /// Release-store: makes the fully-initialised successor node visible to
  /// concurrent readers that follow this link.
  #[inline]
  pub fn store_next(&self, level: usize, ptr: *mut Node) {
    // SAFETY: same precondition as `load_next` — `level < height_of_node`.
    // The Release ordering ensures that the pointed-to node's payload and
    // lower-level links are visible to any reader that follows this pointer.
    unsafe { (*self.slot(level)).store(ptr, Ordering::Release) }
  }

  /// Relaxed load — only safe when the caller holds the write lock.
  #[inline]
  pub fn relaxed_next(&self, level: usize) -> *mut Node {
    // SAFETY: same precondition as `load_next` — `level < height_of_node`.
    // Relaxed ordering is acceptable here only when no concurrent writer can
    // be racing; the `RwLock` write guard in `Memtable` provides that.
    unsafe { (*self.slot(level)).load(Ordering::Relaxed) }
  }

  /// Relaxed store.  Used when initialising a new node's links before
  /// it is published to readers via a subsequent release store.
  #[inline]
  pub fn relaxed_set_next(&self, level: usize, ptr: *mut Node) {
    // SAFETY: same precondition as `load_next` — `level < height_of_node`.
    // Relaxed ordering is safe because the node has not yet been published:
    // a subsequent `store_next` (Release) on the predecessor will act as the
    // memory fence that makes these initialised links visible to readers.
    unsafe { (*self.slot(level)).store(ptr, Ordering::Relaxed) }
  }

  // ── Inline-payload accessors ────────────────────────────────────────────

  /// Pointer to the `u32` data-length field that immediately follows the
  /// Node struct in the allocation.
  ///
  /// # Safety
  /// `node` must be a pointer produced by `alloc_node_raw`.  That function
  /// allocates `NODE_SIZE + DATA_LEN_SIZE + data_size` bytes starting at
  /// `node`, so the returned pointer is always within the allocation and
  /// properly aligned for a `u32` read or write.
  #[inline]
  unsafe fn data_len_ptr(node: *const Node) -> *mut u32 {
    (node as *mut u8).add(NODE_SIZE) as *mut u32
  }

  /// Pointer to the first byte of the inline payload.
  ///
  /// # Safety
  /// `node` must be a pointer produced by `alloc_node_raw`.  The returned
  /// pointer is `NODE_SIZE + DATA_LEN_SIZE` bytes past `node`, which is
  /// within the allocation.  Reading from it is only valid for the
  /// `data_size` bytes specified at allocation time.
  #[inline]
  unsafe fn data_ptr(node: *const Node) -> *const u8 {
    (node as *const u8).add(NODE_SIZE + DATA_LEN_SIZE)
  }

  /// Returns the inline payload as a byte slice.
  ///
  /// # Safety
  /// `node` must be a pointer produced by `alloc_node_raw` whose payload
  /// has been fully written before this call.  In `alloc_and_insert` this is
  /// guaranteed by the `write_fn` call that precedes any use of `payload`.
  /// The returned lifetime `'a` must not outlive the `Arena` that owns the
  /// allocation.
  #[inline]
  pub unsafe fn payload<'a>(node: *const Node) -> &'a [u8] {
    // SAFETY: `data_len_ptr` requires `node` from `alloc_node_raw`; the u32
    // at that offset was written by `alloc_node_raw` and is properly aligned.
    let len = ptr::read(Self::data_len_ptr(node)) as usize;
    // SAFETY: `data_ptr` is within the allocation; `len` bytes starting there
    // were initialised by the caller before this node was made visible to
    // readers (upheld by the caller's contract documented above).
    slice::from_raw_parts(Self::data_ptr(node), len)
  }
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
/// `i < height`.
struct Splice {
  /// Number of valid levels in `prev`/`next`.
  height: usize,
  prev: [*mut Node; MAX_HEIGHT + 1],
  next: [*mut Node; MAX_HEIGHT + 1],
}

// SAFETY: `Splice` contains raw node pointers, so the compiler does not
// derive `Send`/`Sync` automatically.  It is safe to share because `Splice`
// is only ever accessed while the `Memtable` write lock is held, preventing
// concurrent access from other threads.
unsafe impl Send for Splice {}
unsafe impl Sync for Splice {}

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

// ─── SkipList ────────────────────────────────────────────────────────────────

/// An ordered skip list that stores entry-encoded payloads inline in each
/// node, backed by an owned [`Arena`].
///
/// # Thread safety
///
/// Writes must be serialised externally (the `Memtable`'s `RwLock` provides
/// this guarantee).  Reads are lock-free.
pub(crate) struct SkipList {
  arena: Arena,
  head: *mut Node,
  max_height: AtomicUsize,
  /// Number of entries (excludes the sentinel head).
  len: usize,
  rng: Rng,
  /// Cached splice from the last insert, accelerates sequential writes.
  splice: Splice,
}

// SAFETY: `SkipList` contains raw node pointers (in `head` and inside
// `Splice`), so the compiler does not derive `Send`/`Sync` automatically.
// It is safe to share because:
//   • All pointer-following reads use Acquire loads, which synchronise with
//     the Release stores used when nodes are linked; this makes the payload
//     bytes visible to any reader that reaches a node through its links.
//   • Writes (`alloc_and_insert`) are serialised by the `RwLock` write guard
//     in `Memtable`, so no two writers race on the mutable fields.
//   • Nodes are never freed until the `Arena` is dropped (with the
//     `SkipList`), so pointers remain valid for the lifetime of `self`.
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

impl SkipList {
  pub fn new(arena: Arena) -> Self {
    // SAFETY: `arena` is valid for the duration of this call.  `data_size = 0`
    // because the sentinel head carries no payload.  `height = MAX_HEIGHT` so
    // every level slot is initialised.  The returned pointer is valid for the
    // lifetime of `arena`, which moves into `SkipList` on the next line and
    // therefore lives as long as `head` is ever dereferenced.
    let head = unsafe { alloc_node_raw(&arena, 0, MAX_HEIGHT) };
    SkipList {
      arena,
      head,
      max_height: AtomicUsize::new(1),
      len: 0,
      rng: Rng::new(0xdead_beef),
      splice: Splice::new(),
    }
  }

  #[cfg(test)]
  pub fn len(&self) -> usize {
    self.len
  }

  // ── Internal helpers ─────────────────────────────────────────────────────

  #[inline]
  fn max_height(&self) -> usize {
    self.max_height.load(Ordering::Relaxed)
  }

  /// Generate a random height in `[1, MAX_HEIGHT]`.
  ///
  /// Height increases with probability 1/[`BRANCHING`] per level, giving a
  /// geometric distribution identical to LevelDB's.
  fn random_height(&mut self) -> usize {
    let mut h = 1;
    while h < MAX_HEIGHT && self.rng.next_u32().is_multiple_of(BRANCHING) {
      h += 1;
    }
    h
  }

  /// Returns `true` if `key_data` sorts strictly after the payload stored in
  /// node `n`.  A null `n` is treated as +∞.
  ///
  /// Only ever called with `n != head` (head has no payload).
  #[inline]
  fn key_after_node(key_data: &[u8], n: *const Node) -> bool {
    if n.is_null() {
      return false;
    }
    // SAFETY: `n` is non-null (checked above).  Every non-null node pointer
    // in the list was produced by `alloc_node_raw` and had its payload written
    // by `write_fn` in `alloc_and_insert` before being published via a Release
    // store.  We therefore observe a fully initialised payload.
    let node_payload = unsafe { Node::payload(n) };
    let ke = Entry::from_slice(key_data);
    let ne = Entry::from_slice(node_payload);
    ke.cmp(&ne) == std::cmp::Ordering::Greater
  }

  // ── Public operations ────────────────────────────────────────────────────

  /// Seek to the first node whose payload sorts ≥ `key_data`.
  ///
  /// Returns `None` if no such node exists.  The returned slice is borrowed
  /// from the `Arena` and valid for the lifetime of `&self`.
  pub fn find_first_at_or_after<'s>(&'s self, key_data: &[u8]) -> Option<&'s [u8]> {
    let mut x = self.head;
    let mut level = self.max_height() - 1;
    loop {
      // SAFETY: `x` is either `self.head` (always valid) or a node that
      // `key_after_node` advanced us to; `key_after_node` only returns `true`
      // for non-null pointers, so `x` is never null.  `level` starts at
      // `max_height - 1` and only decrements, so it never exceeds the
      // allocated height of any node we visit (all nodes have height ≥ 1,
      // and the head is allocated at `MAX_HEIGHT`).
      let next = unsafe { (*x).load_next(level) };
      if Self::key_after_node(key_data, next) {
        x = next;
      } else if level == 0 {
        return if next.is_null() {
          None
        } else {
          // SAFETY: `next` is non-null (checked above) and is a fully
          // initialised node — its payload was written before the Release
          // store that linked it in, and we consumed that store via the
          // Acquire load above.
          Some(unsafe { Node::payload(next) })
        };
      } else {
        level -= 1;
      }
    }
  }

  /// Allocate a node whose inline payload will hold `data_size` bytes, then
  /// call `write_fn` to fill that payload, and finally link the node into the
  /// skip list.
  ///
  /// This two-phase approach lets the caller encode directly into the
  /// arena-allocated inline area, avoiding any intermediate heap allocation.
  pub fn alloc_and_insert<F>(&mut self, data_size: usize, write_fn: F)
  where
    F: FnOnce(&mut [u8]),
  {
    let height = self.random_height();

    // Extend the splice if the new node reaches a previously unused level.
    let cur_max = self.max_height();
    if height > cur_max {
      for i in cur_max..height {
        self.splice.prev[i] = self.head;
        self.splice.next[i] = ptr::null_mut();
      }
      // Relaxed: readers that observe the old max_height will follow null
      // pointers from head, which is safe.  Readers that observe the new
      // max_height will either find the new node or null — both correct.
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
    // SAFETY: `node` is from `alloc_node_raw`; `write_fn` has just initialised
    // every payload byte through `payload_buf` above.
    let key_data = unsafe { Node::payload(node) };

    let effective_max = cur_max.max(height);

    // ── Determine how many levels need recomputing ──────────────────────
    //
    // Walk up the cached splice until we find a level that still brackets
    // the new key.  Use the pessimistic strategy (recompute everything if
    // the key is outside the bracket at any level), which is simpler and
    // still gives amortised O(1) for the sequential hot path.
    let recompute_height = if self.splice.height < effective_max {
      // Splice was never used, or effective_max just grew.
      self.splice.prev[effective_max] = self.head;
      self.splice.next[effective_max] = ptr::null_mut();
      self.splice.height = effective_max;
      effective_max
    } else {
      let mut h = 0;
      while h < effective_max {
        let pn = self.splice.prev[h];
        let nn = self.splice.next[h];
        // Is the level-h splice still tight?
        // SAFETY: `pn` is `self.splice.prev[h]`, which is either `self.head`
        // or a node that was previously linked into the list at level `h`.
        // Both remain valid for the arena's lifetime.  The caller holds the
        // write lock, so the Relaxed ordering is safe.
        let tight = unsafe { (*pn).relaxed_next(h) == nn };
        if !tight {
          h += 1;
        } else if pn != self.head && !Self::key_after_node(key_data, pn) {
          // Key falls before the cached predecessor: start over.
          h = effective_max;
        } else if Self::key_after_node(key_data, nn) {
          // Key falls after the cached successor: start over.
          h = effective_max;
        } else {
          break; // This level brackets the new key — done.
        }
      }
      h
    };

    if recompute_height > 0 {
      recompute_splice_levels(key_data, &mut self.splice, recompute_height);
    }

    // ── Link the node into every level ──────────────────────────────────
    for i in 0..height {
      // SAFETY: `node` was just allocated and has not been linked yet; we
      // have exclusive write access (`&mut self`).  Relaxed ordering is safe
      // here because the subsequent Release store on the predecessor (below)
      // acts as the publish fence — readers cannot reach `node` until they
      // follow that Release store, at which point these relaxed-stored links
      // are already visible.
      unsafe { (*node).relaxed_set_next(i, self.splice.next[i]) };
      // SAFETY: `self.splice.prev[i]` is `self.head` or a previously linked
      // node, both valid for the arena's lifetime.  The Release ordering
      // ensures that once a reader traverses this link to reach `node`, the
      // node's payload (written by `write_fn`) and all its lower-level links
      // (set by the relaxed stores above) are already visible to that reader.
      unsafe { (*self.splice.prev[i]).store_next(i, node) };
      // Update splice so the next sequential insert can reuse it.
      self.splice.prev[i] = node;
    }

    self.len += 1;
  }
}

// ─── Free traversal helpers ──────────────────────────────────────────────────
//
// These functions don't need access to `SkipList` fields — extracting them
// avoids the simultaneous `&self` / `&mut self.splice` borrow in the hot
// insert path.

/// Find the tightest `(prev, next)` bracket for `key_data` at `level`,
/// starting from `before` (which must precede the key) and stopping no later
/// than `after` (which must follow the key, or null).
#[inline]
fn find_splice_for_level(
  key_data: &[u8],
  mut before: *mut Node,
  after: *mut Node,
  level: usize,
) -> (*mut Node, *mut Node) {
  loop {
    // SAFETY: `before` is either `head` or a linked node returned by a prior
    // iteration of this loop.  In either case it is a valid pointer produced
    // by `alloc_node_raw`.  `level` is always below the node's allocated
    // height: the splice is built for levels ≤ `effective_max`, and nodes are
    // linked at every level up to their own height which is ≥ the level they
    // appear at in the list.
    let next = unsafe { (*before).load_next(level) };
    if next == after || !SkipList::key_after_node(key_data, next) {
      return (before, next);
    }
    before = next;
  }
}

/// Recompute splice levels `[0, recompute_height)` top-down, using the
/// already-valid bracket at `splice.prev/next[recompute_height]`.
fn recompute_splice_levels(key_data: &[u8], splice: &mut Splice, recompute_height: usize) {
  for i in (0..recompute_height).rev() {
    let (p, n) = find_splice_for_level(key_data, splice.prev[i + 1], splice.next[i + 1], i);
    splice.prev[i] = p;
    splice.next[i] = n;
  }
}

// ─── Arena allocation helper ─────────────────────────────────────────────────

/// Allocate a raw node from `arena` with the given `height` and `data_size`.
///
/// All next pointers are set to null (relaxed).  The `data_len` field is
/// written.  The payload bytes are *not* initialised; the caller is
/// responsible for filling them before the node is made visible to readers.
///
/// # Safety
/// * `height` must be in `[1, MAX_HEIGHT]`.
/// * The returned pointer is valid only for the lifetime of `arena`.  The
///   caller must ensure `arena` outlives every use of the returned pointer.
unsafe fn alloc_node_raw(arena: &Arena, data_size: usize, height: usize) -> *mut Node {
  debug_assert!((1..=MAX_HEIGHT).contains(&height));

  // Layout: prefix of (height−1) higher-level AtomicPtrs, then the Node
  // struct (the level-0 AtomicPtr), then the u32 data length, then the data.
  let prefix = NODE_SIZE * (height - 1);
  let total = prefix + NODE_SIZE + DATA_LEN_SIZE + data_size;

  // `raw` points to `total` bytes aligned to `NODE_ALIGN`.  The node struct
  // begins at `raw + prefix`; the prefix area holds the higher-level link
  // slots that `slot(i)` for i > 0 reaches by subtracting from the node ptr.
  let raw = arena.allocate_aligned(total, NODE_ALIGN);
  let node = raw.add(prefix) as *mut Node;

  // Null-initialise every next pointer.  For level 0, `slot(0)` returns
  // `&node.next` (within the struct).  For level i > 0, `slot(i)` steps back
  // `i` pointer-widths, landing within the prefix area — all within the
  // bounds of the `total`-byte allocation.
  for i in 0..height {
    (*node).relaxed_set_next(i, ptr::null_mut());
  }

  // `data_len_ptr(node)` returns `node as *mut u8 + NODE_SIZE`, which is
  // within the allocation and aligned to `DATA_LEN_SIZE` (u32 = 4 bytes;
  // the allocation is aligned to `NODE_ALIGN` = pointer size ≥ 4 bytes).
  ptr::write(Node::data_len_ptr(node), data_size as u32);

  node
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;

  fn make_list() -> SkipList {
    SkipList::new(Arena::default())
  }

  fn insert_key(list: &mut SkipList, key: &[u8], seq: u64, value: &[u8]) {
    let size = Entry::encoded_value_size(seq, key, value);
    list.alloc_and_insert(size, |buf| Entry::write_value_to(buf, seq, key, value));
  }

  fn insert_tombstone(list: &mut SkipList, key: &[u8], seq: u64) {
    let size = Entry::encoded_deletion_size(seq, key);
    list.alloc_and_insert(size, |buf| Entry::write_deletion_to(buf, seq, key));
  }

  fn seek(list: &SkipList, key: &[u8]) -> Option<Vec<u8>> {
    let lsize = Entry::lookup_size(key);
    let mut lbuf = vec![0u8; lsize];
    Entry::write_lookup_to(&mut lbuf, key);
    list.find_first_at_or_after(&lbuf).and_then(|payload| {
      let e = Entry::from_slice(payload);
      if e.key() == key {
        e.value().map(|v| v.to_vec())
      } else {
        None
      }
    })
  }

  #[test]
  fn empty_miss() {
    let list = make_list();
    assert!(seek(&list, b"foo").is_none());
  }

  #[test]
  fn insert_and_find() {
    let mut list = make_list();
    insert_key(&mut list, b"foo", 1, b"bar");
    assert_eq!(seek(&list, b"foo"), Some(b"bar".to_vec()));
  }

  #[test]
  fn newer_version_wins() {
    let mut list = make_list();
    insert_key(&mut list, b"foo", 1, b"v1");
    insert_key(&mut list, b"foo", 2, b"v2");
    assert_eq!(seek(&list, b"foo"), Some(b"v2".to_vec()));
  }

  #[test]
  fn tombstone_hides_value() {
    let mut list = make_list();
    insert_key(&mut list, b"foo", 1, b"bar");
    insert_tombstone(&mut list, b"foo", 2);
    let lsize = Entry::lookup_size(b"foo");
    let mut lbuf = vec![0u8; lsize];
    Entry::write_lookup_to(&mut lbuf, b"foo");
    let payload = list.find_first_at_or_after(&lbuf).unwrap();
    let e = Entry::from_slice(payload);
    assert_eq!(e.key(), b"foo");
    assert!(e.value().is_none()); // tombstone
  }

  #[test]
  fn sequential_inserts() {
    let mut list = make_list();
    for i in 0u64..1000 {
      let key = format!("{i:016}");
      insert_key(&mut list, key.as_bytes(), i, b"v");
    }
    assert_eq!(list.len(), 1000);
    // Spot-check a few
    assert!(seek(&list, b"0000000000000042").is_some());
    assert!(seek(&list, b"0000000000000999").is_some());
    assert!(seek(&list, b"0000000000001000").is_none());
  }

  #[test]
  fn ordering_preserved() {
    let mut list = make_list();
    insert_key(&mut list, b"bbb", 1, b"B");
    insert_key(&mut list, b"aaa", 2, b"A");
    insert_key(&mut list, b"ccc", 3, b"C");
    assert_eq!(seek(&list, b"aaa"), Some(b"A".to_vec()));
    assert_eq!(seek(&list, b"bbb"), Some(b"B".to_vec()));
    assert_eq!(seek(&list, b"ccc"), Some(b"C".to_vec()));
  }
}
