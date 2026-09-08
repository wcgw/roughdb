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

//! Skip-list / memtable micro-benchmarks.
//!
//! Drives `Memtable` directly (no WAL, no `Db` mutex, no write grouping) so the
//! numbers isolate the skip list, entry encoding and arena.  Build with
//! `cargo bench --features bench --bench skiplist`.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use roughdb::bench::Memtable;

/// Entries per benchmark.  ~6.5 MiB of 16-byte keys and 100-byte values — the
/// size of a full default (4 MiB) write buffer and a bit more, so the list is
/// deep enough (≈ 8 levels) for traversal costs to dominate.
const N: u64 = 50_000;

/// Entries for the reverse-scan benchmark.  Deliberately small: it is the
/// regression guard for `prev()`, and must stay tractable even if `prev()`
/// degrades to a linear scan per step.
const N_REVERSE: u64 = 5_000;

/// Versions written per key in the multi-version benchmarks.
const VERSIONS: u64 = 4;

/// 100-byte value, matching LevelDB's default benchmark value size.
const VALUE: [u8; 100] = [b'v'; 100];

/// Zero-padded 16-byte decimal key, matching LevelDB's db_bench format.
fn make_key(i: u64) -> [u8; 16] {
  let mut key = [0u8; 16];
  let s = format!("{i:016}");
  key.copy_from_slice(s.as_bytes());
  key
}

/// Fisher-Yates shuffle of [0, n) using xorshift64 for reproducibility.
fn shuffled(n: u64) -> Vec<u64> {
  let mut v: Vec<u64> = (0..n).collect();
  let mut rng: u64 = 0xdeadbeef_cafebabe;
  for i in (1..n as usize).rev() {
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    let j = rng as usize % (i + 1);
    v.swap(i, j);
  }
  v
}

/// SSTable internal key `user_key || (seq << 8 | kTypeValue)` — the form
/// `MemTableIterator::seek` accepts.
fn internal_key(key: &[u8], seq: u64) -> Vec<u8> {
  let mut ik = Vec::with_capacity(key.len() + 8);
  ik.extend_from_slice(key);
  ik.extend_from_slice(&((seq << 8) | 1).to_le_bytes());
  ik
}

/// Single-threaded insert: the benchmark never writes from more than one
/// thread, which is the contract `Memtable::add` requires.
fn add(mem: &Memtable, seq: u64, key: &[u8], value: &[u8]) {
  // SAFETY: all inserts in this benchmark happen on the calling thread.
  unsafe { Memtable::add(mem, seq, key, value) }
}

fn fill_sequential(n: u64) -> Memtable {
  let mem = Memtable::default();
  for i in 0..n {
    add(&mem, i, &make_key(i), &VALUE);
  }
  mem
}

/// `versions` versions of each of `n` keys, newest last (seq = v * n + i).
fn fill_versioned(n: u64, versions: u64) -> Memtable {
  let mem = Memtable::default();
  for v in 0..versions {
    for i in 0..n {
      add(&mem, v * n + i, &make_key(i), &VALUE);
    }
  }
  mem
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

fn insert_benchmarks(c: &mut Criterion) {
  let order = shuffled(N);
  let mut group = c.benchmark_group("skiplist/insert");
  group.throughput(Throughput::Elements(N));

  // Memory footprint is reported once, outside the timed loops.
  let mem = fill_sequential(N);
  eprintln!(
    "skiplist: {} arena bytes/entry (alignment padding included) for {N} entries (16-byte key, 100-byte value)",
    mem.approximate_memory_usage() as u64 / N
  );
  drop(mem);

  // Keys arrive in ascending order: the splice fast path.
  group.bench_function("sequential", |b| {
    b.iter(|| black_box(fill_sequential(N)));
  });

  // Keys arrive in random order: a full top-down search per insert.
  group.bench_function("random", |b| {
    b.iter(|| {
      let mem = Memtable::default();
      for &i in &order {
        add(&mem, i, &make_key(i), &VALUE);
      }
      black_box(mem);
    });
  });

  // Every key rewritten with a higher sequence number: the new version lands
  // immediately before its predecessor, exercising the equal-user-key path.
  group.bench_function("overwrite", |b| {
    b.iter(|| {
      let mem = fill_sequential(N);
      for &i in &order {
        add(&mem, N + i, &make_key(i), &VALUE);
      }
      black_box(mem);
    });
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Point lookup
// ---------------------------------------------------------------------------

fn lookup_benchmarks(c: &mut Criterion) {
  let order = shuffled(N);
  let mut group = c.benchmark_group("skiplist/lookup");
  group.throughput(Throughput::Elements(N));

  let mem = fill_sequential(N);

  group.bench_function("sequential", |b| {
    b.iter(|| {
      for i in 0..N {
        black_box(mem.get(make_key(i), u64::MAX));
      }
    });
  });

  group.bench_function("random", |b| {
    b.iter(|| {
      for &i in &order {
        black_box(mem.get(make_key(i), u64::MAX));
      }
    });
  });

  // Absent keys that fall *between* existing ones (last digit replaced by a
  // letter), so the search descends all the way rather than falling off the
  // end of the list.
  let missing: Vec<[u8; 16]> = order
    .iter()
    .map(|&i| {
      let mut k = make_key(i);
      k[15] = b'a';
      k
    })
    .collect();
  group.bench_function("missing", |b| {
    b.iter(|| {
      for k in &missing {
        black_box(mem.get(k, u64::MAX));
      }
    });
  });

  // Multiple versions per key: the search has to compare sequence numbers
  // once user keys tie, and lands on the newest version.
  let versioned = fill_versioned(N, VERSIONS);
  group.bench_function("versioned_latest", |b| {
    b.iter(|| {
      for &i in &order {
        black_box(versioned.get(make_key(i), u64::MAX));
      }
    });
  });

  // Snapshot read at the oldest version: the seek must step past every
  // newer version of the key.
  group.bench_function("versioned_snapshot", |b| {
    b.iter(|| {
      for &i in &order {
        black_box(versioned.get(make_key(i), i));
      }
    });
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Iteration
// ---------------------------------------------------------------------------

fn scan_benchmarks(c: &mut Criterion) {
  let order = shuffled(N);
  let mut group = c.benchmark_group("skiplist/scan");

  let mem = fill_sequential(N);

  group.throughput(Throughput::Elements(N));
  group.bench_function("forward", |b| {
    b.iter(|| {
      let mut it = mem.iter();
      it.seek_to_first();
      let mut n = 0u64;
      while it.valid() {
        black_box((it.key(), it.value()));
        it.advance();
        n += 1;
      }
      assert_eq!(n, N);
    });
  });

  // Random iterator seeks (the `MergingIterator` / `DbIterator::seek` path).
  let targets: Vec<Vec<u8>> = order
    .iter()
    .map(|&i| internal_key(&make_key(i), u64::MAX >> 8))
    .collect();
  group.bench_function("seek", |b| {
    b.iter(|| {
      let mut it = mem.iter();
      for t in &targets {
        it.seek(t);
        black_box(it.key());
      }
    });
  });

  let small = fill_sequential(N_REVERSE);
  group.throughput(Throughput::Elements(N_REVERSE));
  group.bench_function("backward", |b| {
    b.iter(|| {
      let mut it = small.iter();
      it.seek_to_last();
      let mut n = 0u64;
      while it.valid() {
        black_box((it.key(), it.value()));
        it.prev();
        n += 1;
      }
      assert_eq!(n, N_REVERSE);
    });
  });

  group.finish();
}

criterion_group!(
  benches,
  insert_benchmarks,
  lookup_benchmarks,
  scan_benchmarks
);
criterion_main!(benches);
