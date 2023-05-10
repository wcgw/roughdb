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

pub struct Arena {
  arena: Bump,
}

impl Arena {
  pub fn new(capacity: usize) -> Self {
    Arena {
      arena: Bump::with_capacity(capacity),
    }
  }

  pub fn allocate(&self, size: usize) -> &mut [u8] {
    self.arena.alloc_slice_fill_clone(size, &0u8)
  }
}

impl Default for Arena {
  fn default() -> Self {
    Self::new(10 << 20) // 10 MB
  }
}
