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

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use roughdb::Db;

/// Number of entries per benchmark iteration — sized to stay within a
/// realistic memtable budget (LevelDB default: 4 MiB write buffer).
const N: u64 = 10_000;

/// 100-byte value, matching LevelDB's default benchmark value size.
const VALUE: [u8; 100] = [b'v'; 100];

/// Zero-padded 16-byte decimal key, matching LevelDB's db_bench format.
fn make_key(i: u64) -> [u8; 16] {
  let mut key = [0u8; 16];
  let s = format!("{i:016}");
  key.copy_from_slice(s.as_bytes());
  key
}

/// Fisher-Yates shuffle of [0, n) using a simple xorshift64 for
/// reproducibility without pulling in a rand crate.
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

// ---------------------------------------------------------------------------
// Write benchmarks
// ---------------------------------------------------------------------------

fn write_benchmarks(c: &mut Criterion) {
  let order = shuffled(N);
  let mut group = c.benchmark_group("write");
  group.throughput(Throughput::Elements(N));

  // fillseq: N puts, sequential key order.
  group.bench_function("sequential", |b| {
    b.iter(|| {
      let db = Db::default();
      for i in 0..N {
        db.put(make_key(i), VALUE).unwrap();
      }
      black_box(&db);
    });
  });

  // fillrandom: N puts, random key order.
  group.bench_function("random", |b| {
    b.iter(|| {
      let db = Db::default();
      for &i in &order {
        db.put(make_key(i), VALUE).unwrap();
      }
      black_box(&db);
    });
  });

  // overwrite: N initial puts followed by N overwrites to the same keys.
  // Measures the cost of inserting duplicate entries (the memtable keeps
  // all versions until compaction).
  group.bench_function("overwrite", |b| {
    b.iter(|| {
      let db = Db::default();
      for i in 0..N {
        db.put(make_key(i), VALUE).unwrap();
      }
      for i in 0..N {
        db.put(make_key(i), VALUE).unwrap();
      }
      black_box(&db);
    });
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Read benchmarks
// Pre-populate once outside the timed loop so only reads are measured.
// ---------------------------------------------------------------------------

fn read_benchmarks(c: &mut Criterion) {
  let order = shuffled(N);
  let mut group = c.benchmark_group("read");
  group.throughput(Throughput::Elements(N));

  let db = Db::default();
  for i in 0..N {
    db.put(make_key(i), VALUE).unwrap();
  }

  // readseq: N gets of existing keys, sequential order.
  group.bench_function("sequential", |b| {
    b.iter(|| {
      for i in 0..N {
        black_box(db.get(make_key(i))).unwrap();
      }
    });
  });

  // readrandom: N gets of existing keys, random order.
  group.bench_function("random", |b| {
    b.iter(|| {
      for &i in &order {
        let _ = black_box(db.get(make_key(i)));
      }
    });
  });

  // scan: one full forward iteration over all N entries.
  group.bench_function("scan", |b| {
    use roughdb::ReadOptions;
    b.iter(|| {
      let mut it = db.new_iterator(&ReadOptions::default()).unwrap();
      it.seek_to_first();
      let mut n = 0u64;
      while it.valid() {
        black_box((it.key(), it.value()));
        it.next();
        n += 1;
      }
      assert_eq!(n, N);
    });
  });

  // readmissing: N gets of keys that were never inserted.
  group.bench_function("missing", |b| {
    b.iter(|| {
      for i in N..2 * N {
        black_box(db.get(make_key(i))).unwrap_err().is_not_found();
      }
    });
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Delete benchmarks
// Setup (put N keys) is included in the timed loop; subtract write/sequential
// to isolate pure delete cost if needed.
// ---------------------------------------------------------------------------

fn delete_benchmarks(c: &mut Criterion) {
  let order = shuffled(N);
  let mut group = c.benchmark_group("delete");
  group.throughput(Throughput::Elements(N));

  // deleteseq: delete N existing keys, sequential order.
  group.bench_function("sequential", |b| {
    b.iter(|| {
      let db = Db::default();
      for i in 0..N {
        db.put(make_key(i), VALUE).unwrap();
      }
      for i in 0..N {
        db.delete(make_key(i)).unwrap();
      }
      black_box(&db);
    });
  });

  // deleterandom: delete N existing keys, random order.
  group.bench_function("random", |b| {
    b.iter(|| {
      let db = Db::default();
      for i in 0..N {
        db.put(make_key(i), VALUE).unwrap();
      }
      for &i in &order {
        db.delete(make_key(i)).unwrap();
      }
      black_box(&db);
    });
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Flush / compaction benchmarks
// These need a persistent DB (tempdir; /tmp is tmpfs on most Linux setups, so
// I/O noise stays low).  The default 4 MiB write buffer holds all N entries,
// so flushes and compactions happen only where the benchmark invokes them —
// two L0 files stay below L0_COMPACTION_TRIGGER, keeping the background
// thread out of the measurement.
// ---------------------------------------------------------------------------

fn compaction_benchmarks(c: &mut Criterion) {
  use criterion::BatchSize;
  use roughdb::{FlushOptions, Options};

  let mut group = c.benchmark_group("compaction");
  group.sample_size(20);
  group.throughput(Throughput::Elements(N));

  // flush: N entries memtable → one L0 SSTable.
  group.bench_function("flush", |b| {
    b.iter_batched(
      || {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
          create_if_missing: true,
          ..Options::default()
        };
        let db = Db::open(dir.path(), opts).unwrap();
        for i in 0..N {
          db.put(make_key(i), VALUE).unwrap();
        }
        (dir, db)
      },
      |(dir, db)| {
        db.flush(&FlushOptions { wait: true }).unwrap();
        black_box((dir, db));
      },
      BatchSize::PerIteration,
    );
  });

  // compact_range: merge two fully-overlapping L0 files (2N entries in,
  // N entries out after shadow-key pruning).
  group.bench_function("compact_range", |b| {
    b.iter_batched(
      || {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
          create_if_missing: true,
          ..Options::default()
        };
        let db = Db::open(dir.path(), opts).unwrap();
        for i in 0..N {
          db.put(make_key(i), VALUE).unwrap();
        }
        db.flush(&FlushOptions { wait: true }).unwrap();
        for i in 0..N {
          db.put(make_key(i), VALUE).unwrap();
        }
        db.flush(&FlushOptions { wait: true }).unwrap();
        (dir, db)
      },
      |(dir, db)| {
        db.compact_range(None, None).unwrap();
        black_box((dir, db));
      },
      BatchSize::PerIteration,
    );
  });

  group.finish();
}

criterion_group!(
  benches,
  write_benchmarks,
  read_benchmarks,
  delete_benchmarks,
  compaction_benchmarks
);
criterion_main!(benches);
