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

use crate::memtable::{Memtable, MemtableResult};

pub(crate) mod memtable;

#[derive(Default)]
pub struct Db<'a> {
  mem: Memtable<'a>,
  imm: Option<Memtable<'a>>,
  // disk: ,
}

impl<'a> Db<'a> {
  pub fn get<K>(&'a self, key: K) -> Option<Vec<u8>>
  where
    K: AsRef<[u8]>,
  {
    let key = key.as_ref();

    match self.mem.get(key) {
      MemtableResult::Hit(hit) => hit,
      MemtableResult::Miss => {
        if let Some(imm) = self.imm.as_ref() {
          if let MemtableResult::Hit(hit) = imm.get(key) {
            return hit;
          }
        }
        // return self.disk
        None
      }
    }
  }

  pub fn put<K, V>(&'a self, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let key = key.as_ref();
    let val = value.as_ref();
    self.mem.add(key, val);
  }

  pub fn delete<K>(&'a self, key: K)
  where
    K: AsRef<[u8]>,
  {
    let key = key.as_ref();
    self.mem.delete(key);
  }
}

#[cfg(test)]
mod tests {
  use crate::Db;

  #[test]
  fn it_works() {
    let db = Db::default();
    assert_eq!(db.get(b"42"), None);
    db.put(b"42", b"An answer to some question");
    assert_eq!(db.get(b"42"), Some(b"An answer to some question".to_vec()));
    db.delete(b"42");
    assert_eq!(db.get(b"42"), None);
  }
}
