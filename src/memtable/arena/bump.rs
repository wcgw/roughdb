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

use bumpalo::Bump;
use std::alloc::Layout;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Arena {
  arena: Bump,
  /// Running total of bytes requested via `allocate_aligned`.
  ///
  /// Bumpalo's `allocated_bytes()` returns total chunk *capacity*, which
  /// doesn't grow for small allocations within a pre-allocated chunk.
  /// This counter tracks actual usage instead, matching LevelDB's `MemoryUsage()`.
  bytes_used: AtomicUsize,
}

impl Arena {
  pub fn new(capacity: usize) -> Self {
    let bump = Bump::new();
    bump.set_allocation_limit(Some(capacity));
    Arena {
      arena: bump,
      bytes_used: AtomicUsize::new(0),
    }
  }

  /// Bytes of memory allocated from the arena so far (exact, not an estimate).
  ///
  /// Used by `Db` to decide when to flush the memtable to L0.
  pub fn memory_usage(&self) -> usize {
    self.bytes_used.load(Ordering::Relaxed)
  }

  /// Allocate `size` bytes with the given power-of-two `align`.
  ///
  /// The allocation is *not* zero-initialised; callers must initialise every
  /// byte before use.  Panics on OOM — matching LevelDB's behaviour, since a
  /// memtable allocation failure is unrecoverable.
  pub fn allocate_aligned(&self, size: usize, align: usize) -> *mut u8 {
    let layout = Layout::from_size_align(size, align).expect("invalid layout");
    match self.arena.try_alloc_layout(layout) {
      Ok(ptr) => {
        self.bytes_used.fetch_add(size, Ordering::Relaxed);
        ptr.as_ptr()
      }
      Err(_) => panic!("Arena OOM: failed to allocate {size} bytes (align {align})"),
    }
  }
}

impl Default for Arena {
  fn default() -> Self {
    Self::new(10 << 20) // 10 MB
  }
}
