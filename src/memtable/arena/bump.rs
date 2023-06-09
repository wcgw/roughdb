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

use bumpalo::Bump;
use std::alloc::Layout;
use std::ptr;

pub struct Arena {
  arena: Bump,
}

impl Arena {
  pub fn new(capacity: usize) -> Self {
    let bump = Bump::new();
    bump.set_allocation_limit(Some(capacity));
    Arena { arena: bump }
  }

  pub fn allocate(&self, size: usize) -> Option<&mut [u8]> {
    match Layout::array::<u8>(size) {
      Ok(layout) => match self.arena.try_alloc_layout(layout) {
        Ok(ptr) => unsafe {
          let data = ptr.as_ptr();
          ptr::write_bytes(data, 0, size);
          let slice = std::slice::from_raw_parts_mut(data, size);
          Some(slice)
        },
        Err(_) => None,
      },
      Err(_) => None,
    }
  }
}

impl Default for Arena {
  fn default() -> Self {
    Self::new(10 << 20) // 10 MB
  }
}

#[cfg(test)]
mod tests {
  use crate::memtable::arena::Arena;

  #[test]
  fn initializes_slice_to_zeros() {
    let arena = Arena::new(64);
    let slice = arena.allocate(32).expect("This should succeed");
    for byte in slice {
      assert_eq!(0, *byte);
    }
  }

  #[test]
  fn returns_none_on_oom() {
    let arena = Arena::new(32);
    let some_mem = arena.allocate(64);
    assert!(some_mem.is_none());
  }
}
