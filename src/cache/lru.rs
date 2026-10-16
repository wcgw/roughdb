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

//! A least-recently-used map with O(1) lookups, inserts and removals, shared
//! by the block cache and the table cache.

use std::collections::HashMap;
use std::hash::Hash;

/// Marks the end of the recency list.
const NIL: usize = usize::MAX;

struct Node<K, V> {
  key: K,
  value: V,
  charge: usize,
  /// Neighbour towards the least recently used end, or `NIL`.
  older: usize,
  /// Neighbour towards the most recently used end, or `NIL`.
  newer: usize,
}

/// A map that holds entries up to a total *charge* (bytes for blocks, one per
/// entry for open tables) and evicts the least recently used entries to stay
/// within it.  Looking an entry up makes it the most recently used.
///
/// The entry just inserted is never evicted, even when its charge alone
/// exceeds the capacity: it then evicts everything else.
///
/// Not synchronised: callers wrap it in a `Mutex`.
pub(crate) struct Lru<K, V> {
  capacity: usize,
  usage: usize,
  index: HashMap<K, usize>,
  /// Slots holding the entries; `None` slots are listed in `free`.
  slots: Vec<Option<Node<K, V>>>,
  free: Vec<usize>,
  /// Least recently used entry, or `NIL` when empty.
  oldest: usize,
  /// Most recently used entry, or `NIL` when empty.
  newest: usize,
}

impl<K: Hash + Eq + Clone, V> Lru<K, V> {
  /// An empty map holding entries up to a total charge of `capacity`.
  pub(crate) fn new(capacity: usize) -> Self {
    Lru {
      capacity,
      usage: 0,
      index: HashMap::new(),
      slots: Vec::new(),
      free: Vec::new(),
      oldest: NIL,
      newest: NIL,
    }
  }

  /// The value for `key`, which becomes the most recently used entry; `None`
  /// if absent.
  pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
    let slot = *self.index.get(key)?;
    self.unlink(slot);
    self.link_newest(slot);
    Some(&self.node(slot).value)
  }

  /// Insert `value` for `key` with the given charge, replacing any previous
  /// value, as the most recently used entry.  Then evicts least recently
  /// used entries, other than this one, until the total charge fits the
  /// capacity.
  pub(crate) fn insert(&mut self, key: K, value: V, charge: usize) {
    self.remove(&key);
    let node = Node {
      key: key.clone(),
      value,
      charge,
      older: NIL,
      newer: NIL,
    };
    let slot = match self.free.pop() {
      Some(slot) => {
        self.slots[slot] = Some(node);
        slot
      }
      None => {
        self.slots.push(Some(node));
        self.slots.len() - 1
      }
    };
    self.index.insert(key, slot);
    self.link_newest(slot);
    self.usage += charge;
    while self.usage > self.capacity && self.oldest != slot {
      let oldest = self.oldest;
      self.remove_slot(oldest);
    }
  }

  /// Remove the entry for `key`, returning its value; `None` if absent.
  pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
    let slot = *self.index.get(key)?;
    Some(self.remove_slot(slot))
  }

  #[cfg(test)]
  pub(crate) fn len(&self) -> usize {
    self.index.len()
  }

  /// Whether `key` has an entry, without making it the most recently used.
  #[cfg(test)]
  pub(crate) fn contains_key(&self, key: &K) -> bool {
    self.index.contains_key(key)
  }

  fn node(&self, slot: usize) -> &Node<K, V> {
    self.slots[slot]
      .as_ref()
      .expect("indexed LRU slot holds an entry")
  }

  fn node_mut(&mut self, slot: usize) -> &mut Node<K, V> {
    self.slots[slot]
      .as_mut()
      .expect("linked LRU slot holds an entry")
  }

  fn remove_slot(&mut self, slot: usize) -> V {
    self.unlink(slot);
    let node = self.slots[slot]
      .take()
      .expect("indexed LRU slot holds an entry");
    self.free.push(slot);
    self.index.remove(&node.key);
    self.usage -= node.charge;
    node.value
  }

  /// Detach `slot` from the recency list.
  fn unlink(&mut self, slot: usize) {
    let (older, newer) = {
      let node = self.node(slot);
      (node.older, node.newer)
    };
    match older {
      NIL => self.oldest = newer,
      o => self.node_mut(o).newer = newer,
    }
    match newer {
      NIL => self.newest = older,
      n => self.node_mut(n).older = older,
    }
  }

  /// Attach a detached `slot` as the most recently used entry.
  fn link_newest(&mut self, slot: usize) {
    let previous_newest = self.newest;
    {
      let node = self.node_mut(slot);
      node.older = previous_newest;
      node.newer = NIL;
    }
    match previous_newest {
      NIL => self.oldest = slot,
      p => self.node_mut(p).newer = slot,
    }
    self.newest = slot;
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Keys from least to most recently used.
  fn keys_by_recency(lru: &Lru<u32, &str>) -> Vec<u32> {
    let mut keys = Vec::new();
    let mut slot = lru.oldest;
    while slot != NIL {
      let node = lru.node(slot);
      keys.push(node.key);
      slot = node.newer;
    }
    keys
  }

  #[test]
  fn get_returns_inserted_value_and_promotes_it() {
    let mut lru = Lru::new(10);
    lru.insert(1, "a", 1);
    lru.insert(2, "b", 1);
    assert_eq!(lru.get(&1), Some(&"a"));
    assert_eq!(keys_by_recency(&lru), [2, 1]);
    assert_eq!(lru.get(&3), None);
  }

  #[test]
  fn evicts_least_recently_used_by_charge() {
    let mut lru = Lru::new(3);
    lru.insert(1, "a", 1);
    lru.insert(2, "b", 1);
    lru.insert(3, "c", 1);
    lru.get(&1);
    lru.insert(4, "d", 2);
    // 2 and 3 were the least recently used; evicting both makes room.
    assert_eq!(keys_by_recency(&lru), [1, 4]);
    assert_eq!(lru.usage, 3);
  }

  #[test]
  fn replacing_a_key_updates_its_charge() {
    let mut lru = Lru::new(10);
    lru.insert(1, "a", 4);
    lru.insert(1, "b", 6);
    assert_eq!(lru.len(), 1);
    assert_eq!(lru.usage, 6);
    assert_eq!(lru.get(&1), Some(&"b"));
  }

  #[test]
  fn an_entry_larger_than_the_capacity_is_kept_alone() {
    let mut lru = Lru::new(2);
    lru.insert(1, "a", 1);
    lru.insert(2, "big", 5);
    assert_eq!(keys_by_recency(&lru), [2]);
    lru.insert(3, "c", 1);
    assert_eq!(keys_by_recency(&lru), [3]);
  }

  #[test]
  fn remove_returns_the_value_and_frees_its_slot() {
    let mut lru = Lru::new(10);
    lru.insert(1, "a", 1);
    lru.insert(2, "b", 1);
    lru.insert(3, "c", 1);
    assert_eq!(lru.remove(&2), Some("b"));
    assert_eq!(lru.remove(&2), None);
    assert_eq!(keys_by_recency(&lru), [1, 3]);
    lru.insert(4, "d", 1);
    assert_eq!(lru.slots.len(), 3, "the freed slot is reused");
    assert_eq!(keys_by_recency(&lru), [1, 3, 4]);
    assert_eq!(lru.usage, 3);
  }

  #[test]
  fn emptied_map_can_be_refilled() {
    let mut lru = Lru::new(10);
    lru.insert(1, "a", 1);
    lru.remove(&1);
    assert_eq!(keys_by_recency(&lru), []);
    lru.insert(2, "b", 1);
    assert_eq!(keys_by_recency(&lru), [2]);
  }
}
