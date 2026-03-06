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

pub mod error;
pub use error::Error;
pub(crate) mod coding;
pub(crate) mod memtable;
pub mod write_batch;
pub use write_batch::{Handler, WriteBatch};

#[derive(Default)]
pub struct Db {
  mem: Memtable,
  imm: Option<Memtable>,
  // disk: ,
}

impl Db {
  pub fn get<K>(&self, key: K) -> Result<Vec<u8>, Error>
  where
    K: AsRef<[u8]>,
  {
    let key = key.as_ref();

    match self.mem.get(key) {
      MemtableResult::Hit(hit) => return Ok(hit),
      MemtableResult::Deleted => return Err(Error::NotFound),
      MemtableResult::Miss => {}
    }

    if let Some(imm) = self.imm.as_ref() {
      match imm.get(key) {
        MemtableResult::Hit(hit) => return Ok(hit),
        MemtableResult::Deleted => return Err(Error::NotFound),
        MemtableResult::Miss => {}
      }
    }

    // return self.disk
    Err(Error::NotFound)
  }

  pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let key = key.as_ref();
    let val = value.as_ref();
    self.mem.add(key, val);
    Ok(())
  }

  pub fn delete<K>(&self, key: K) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
  {
    let key = key.as_ref();
    self.mem.delete(key);
    Ok(())
  }

  pub fn write(&self, batch: &WriteBatch) -> Result<(), Error> {
    struct Inserter<'a>(&'a Memtable);
    impl Handler for Inserter<'_> {
      fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.0.add(key, value);
        Ok(())
      }
      fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.0.delete(key);
        Ok(())
      }
    }
    batch.iterate(&mut Inserter(&self.mem))
  }
}

#[cfg(test)]
mod tests {
  use crate::Db;

  #[test]
  fn it_works() {
    let db = Db::default();
    assert!(db.get(b"42").unwrap_err().is_not_found());
    db.put(b"42", b"An answer to some question").unwrap();
    assert_eq!(db.get(b"42").unwrap(), b"An answer to some question");
    db.delete(b"42").unwrap();
    assert!(db.get(b"42").unwrap_err().is_not_found());
  }
}
