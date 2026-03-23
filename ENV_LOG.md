# Env Abstraction — Implementation Log

Pluggable filesystem backend replacing hardcoded `std::fs` / `libc` I/O.

## Steps

- [x] **Step 1: Define traits + `PosixFileSystem`** — Create `src/env.rs` with `SequentialFile`, `RandomAccessFile`,
  `WritableFile`, `FileLock`, `FileSystem` traits and `PosixFileSystem` implementation. Add `Options::file_system:
  Arc<dyn FileSystem>` defaulting to `PosixFileSystem`. No existing code changes beyond the new file and options field.
- [x] **Step 2: `LogWriter` and `LogReader`** — Change `LogWriter` to wrap `Box<dyn WritableFile>` (instead of
  `BufWriter<File>`) and `LogReader` to wrap `Box<dyn SequentialFile>` (instead of `File`). Update all callers
  (WAL creation, MANIFEST creation, WAL replay, repair).
- [x] **Step 3: `Table` reader** — Change `Table::open` to take `Arc<dyn RandomAccessFile>` instead of `File`.
  `read_block` and footer reads go through the trait. `Table` stores `Arc<dyn RandomAccessFile>`.
- [ ] **Step 4: `TableBuilder`** — Change constructor to take `Box<dyn WritableFile>` instead of `File`.
- [ ] **Step 5: `TableCache`** — `get_or_open` calls `fs.open_random_access(path)` instead of `File::open`.
  `TableCache` stores `Arc<dyn FileSystem>`.
- [ ] **Step 6: `VersionSet`** — MANIFEST creation/reading and CURRENT file read/write go through `FileSystem`.
  `VersionSet::create` and `VersionSet::recover` take or store the filesystem handle.
- [ ] **Step 7: `lib.rs`** — Replace all remaining `std::fs` calls (~30 sites) in `Db::open`, `begin_flush`,
  `write_flush`, `finish_flush`, `do_compaction`, `delete_obsolete_files`, `Db::destroy`, `Db::repair`, and
  `acquire_lock`. `Persistence` stores `Arc<dyn FileSystem>`. `libc::flock` moves into `PosixFileSystem::lock_file`.
- [ ] **Step 8: Cleanup** — Remove direct `libc` usage from `lib.rs`. Verify `libc` is only used inside
  `PosixFileSystem`. Run full test suite.

## Call sites (~40 across 7 files)

| File | Current I/O | Target trait method |
|---|---|---|
| `logfile/writer.rs` | `BufWriter<File>`, `write`, `flush`, `sync_all` | `WritableFile` |
| `logfile/reader.rs` | `File`, `Read` trait | `SequentialFile` |
| `table/builder.rs` | `BufWriter<File>`, `write` | `WritableFile` |
| `table/reader.rs` | `File`, `FileExt::read_at` (pread) | `RandomAccessFile` |
| `db/table_cache.rs` | `File::open` -> `Table::open` | `FileSystem::open_random_access` |
| `db/version_set.rs` | `File::create`/`open` for MANIFEST, `fs::write` for CURRENT | `FileSystem` methods |
| `lib.rs` | `create_dir_all`, `File::create`, `File::open`, `read_dir`, `remove_file`, `remove_dir`, `metadata`, `flock` | `FileSystem` methods |
