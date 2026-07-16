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

//! Pluggable filesystem abstraction.
//!
//! All database I/O goes through the [`FileSystem`] trait, which defaults to
//! [`PosixFileSystem`] (local POSIX filesystem via `std::fs` + `libc::flock`).
//!
//! Custom implementations enable in-memory filesystems (for testing), encrypted
//! storage, cloud backends, or async I/O — without touching core database logic.
//!
//! Port of LevelDB's `include/leveldb/env.h` and RocksDB's
//! `include/rocksdb/file_system.h`.

use crate::error::Error;
use std::path::Path;
use std::sync::Arc;

// ── File traits ──────────────────────────────────────────────────────────────

/// A readable file for sequential access (WAL reading, MANIFEST replay).
///
/// Reads proceed from the current position forward.
pub trait SequentialFile: Send {
  /// Read up to `buf.len()` bytes into `buf`.  Returns the number of bytes
  /// read (0 at EOF).
  fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error>;

  /// Skip `n` bytes from the current position.  This may be faster than
  /// reading and discarding for backends that support seeking.
  fn skip(&mut self, n: u64) -> Result<(), Error>;
}

/// A readable file for random access (SSTable block reads via pread).
///
/// Implementations must be safe for concurrent reads from multiple threads.
pub trait RandomAccessFile: Send + Sync {
  /// Read up to `buf.len()` bytes starting at `offset`.  Returns the number
  /// of bytes read (may be less than `buf.len()` near EOF).
  fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize, Error>;
}

/// A writable file (WAL, MANIFEST, SSTable output).
///
/// Writes are buffered internally; call [`flush`](WritableFile::flush) to push
/// to the OS and [`sync`](WritableFile::sync) to `fsync`.
pub trait WritableFile: Send {
  /// Append `data` to the file.
  fn write(&mut self, data: &[u8]) -> Result<(), Error>;

  /// Flush any internal buffer to the OS page cache.
  fn flush(&mut self) -> Result<(), Error>;

  /// Durably persist all previously written data (`fsync`).
  fn sync(&mut self) -> Result<(), Error>;
}

/// An exclusive file lock handle.
///
/// The lock is released when this value is dropped.
pub trait FileLock: Send + Sync {}

// ── FileSystem trait ─────────────────────────────────────────────────────────

/// Pluggable filesystem backend.
///
/// All database I/O (file creation, reading, writing, directory operations,
/// and file locking) goes through this trait.
///
/// See `include/rocksdb/file_system.h: FileSystem`.
pub trait FileSystem: Send + Sync {
  /// Open an existing file for sequential reading.
  fn open_sequential(&self, path: &Path) -> Result<Box<dyn SequentialFile>, Error>;

  /// Open an existing file for random-access reading (pread).
  fn open_random_access(&self, path: &Path) -> Result<Arc<dyn RandomAccessFile>, Error>;

  /// Open an existing file for appending (used to resume a WAL or MANIFEST).
  fn open_appendable(&self, path: &Path) -> Result<Box<dyn WritableFile>, Error>;

  /// Create a new file for writing (truncates if it exists).
  fn create_writable(&self, path: &Path) -> Result<Box<dyn WritableFile>, Error>;

  /// Return the size of the file in bytes.
  fn file_size(&self, path: &Path) -> Result<u64, Error>;

  /// Returns `true` if the path exists.
  fn file_exists(&self, path: &Path) -> bool;

  /// Atomically rename `src` to `dst`.
  fn rename(&self, src: &Path, dst: &Path) -> Result<(), Error>;

  /// Delete a file.
  fn remove_file(&self, path: &Path) -> Result<(), Error>;

  /// Create directory and all parent directories.
  fn create_dir_all(&self, path: &Path) -> Result<(), Error>;

  /// Remove an empty directory.
  fn remove_dir(&self, path: &Path) -> Result<(), Error>;

  /// Durably persist the directory entries of `path` (`fsync` on the directory).
  ///
  /// Required after creating or renaming a file for the new directory entry to
  /// survive a crash — syncing the file itself only persists its contents.
  /// Backends without directory metadata (e.g. in-memory) may no-op.
  fn sync_dir(&self, path: &Path) -> Result<(), Error>;

  /// List the names of files and subdirectories in `path`.
  fn children(&self, path: &Path) -> Result<Vec<String>, Error>;

  /// Acquire an exclusive, non-blocking file lock.
  ///
  /// Returns `Err` immediately if the lock is held by another process.
  fn lock_file(&self, path: &Path) -> Result<Box<dyn FileLock>, Error>;

  /// Write `data` to `path` durably (create or overwrite, then `fsync`).
  ///
  /// Not atomic: a crash mid-write can leave a truncated file.  Callers that
  /// need atomic replacement must write to a temporary file and [`rename`]
  /// (see `write_current_file` in `db/version_set.rs`).
  ///
  /// [`rename`]: FileSystem::rename
  fn write_string_to_file(&self, path: &Path, data: &str) -> Result<(), Error> {
    let mut f = self.create_writable(path)?;
    f.write(data.as_bytes())?;
    f.flush()?;
    f.sync()?;
    Ok(())
  }

  /// Read the entire contents of `path` as a UTF-8 string.
  fn read_string_from_file(&self, path: &Path) -> Result<String, Error> {
    let mut f = self.open_sequential(path)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
      let n = f.read(&mut chunk)?;
      if n == 0 {
        break;
      }
      buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8(buf).map_err(|e| Error::Corruption(format!("non-UTF-8 file content: {e}")))
  }
}

// ── PosixFileSystem ──────────────────────────────────────────────────────────

/// Default [`FileSystem`] backed by the local POSIX filesystem.
///
/// Uses `std::fs` for file operations and `libc::flock` for file locking.
#[derive(Debug, Clone, Copy)]
pub struct PosixFileSystem;

// ── POSIX file implementations ───────────────────────────────────────────────

struct PosixSequentialFile {
  inner: std::fs::File,
}

impl SequentialFile for PosixSequentialFile {
  fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
    use std::io::Read;
    self.inner.read(buf).map_err(Error::IoError)
  }

  fn skip(&mut self, n: u64) -> Result<(), Error> {
    use std::io::{Seek, SeekFrom};
    self
      .inner
      .seek(SeekFrom::Current(n as i64))
      .map_err(Error::IoError)?;
    Ok(())
  }
}

struct PosixRandomAccessFile {
  inner: std::fs::File,
}

impl RandomAccessFile for PosixRandomAccessFile {
  fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize, Error> {
    use std::os::unix::fs::FileExt;
    self.inner.read_at(buf, offset).map_err(Error::IoError)
  }
}

struct PosixWritableFile {
  inner: std::io::BufWriter<std::fs::File>,
}

impl WritableFile for PosixWritableFile {
  fn write(&mut self, data: &[u8]) -> Result<(), Error> {
    use std::io::Write;
    self.inner.write_all(data).map_err(Error::IoError)
  }

  fn flush(&mut self) -> Result<(), Error> {
    use std::io::Write;
    self.inner.flush().map_err(Error::IoError)
  }

  fn sync(&mut self) -> Result<(), Error> {
    // Flush the BufWriter first, then fsync the underlying file.
    use std::io::Write;
    self.inner.flush().map_err(Error::IoError)?;
    self.inner.get_ref().sync_all().map_err(Error::IoError)
  }
}

struct PosixFileLock {
  // Dropping the File releases the flock.
  _file: std::fs::File,
}

impl FileLock for PosixFileLock {}

// ── PosixFileSystem implementation ───────────────────────────────────────────

impl FileSystem for PosixFileSystem {
  fn open_sequential(&self, path: &Path) -> Result<Box<dyn SequentialFile>, Error> {
    let file = std::fs::File::open(path).map_err(Error::IoError)?;
    Ok(Box::new(PosixSequentialFile { inner: file }))
  }

  fn open_random_access(&self, path: &Path) -> Result<Arc<dyn RandomAccessFile>, Error> {
    let file = std::fs::File::open(path).map_err(Error::IoError)?;
    Ok(Arc::new(PosixRandomAccessFile { inner: file }))
  }

  fn open_appendable(&self, path: &Path) -> Result<Box<dyn WritableFile>, Error> {
    let file = std::fs::OpenOptions::new()
      .read(true)
      .append(true)
      .create(true)
      .open(path)
      .map_err(Error::IoError)?;
    Ok(Box::new(PosixWritableFile {
      inner: std::io::BufWriter::new(file),
    }))
  }

  fn create_writable(&self, path: &Path) -> Result<Box<dyn WritableFile>, Error> {
    let file = std::fs::File::create(path).map_err(Error::IoError)?;
    Ok(Box::new(PosixWritableFile {
      inner: std::io::BufWriter::new(file),
    }))
  }

  fn file_size(&self, path: &Path) -> Result<u64, Error> {
    let meta = std::fs::metadata(path).map_err(Error::IoError)?;
    Ok(meta.len())
  }

  fn file_exists(&self, path: &Path) -> bool {
    path.exists()
  }

  fn rename(&self, src: &Path, dst: &Path) -> Result<(), Error> {
    std::fs::rename(src, dst).map_err(Error::IoError)
  }

  fn remove_file(&self, path: &Path) -> Result<(), Error> {
    std::fs::remove_file(path).map_err(Error::IoError)
  }

  fn create_dir_all(&self, path: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(path).map_err(Error::IoError)
  }

  fn remove_dir(&self, path: &Path) -> Result<(), Error> {
    std::fs::remove_dir(path).map_err(Error::IoError)
  }

  fn sync_dir(&self, path: &Path) -> Result<(), Error> {
    // Opening a directory read-only and fsync-ing it persists its entries.
    let dir = std::fs::File::open(path).map_err(Error::IoError)?;
    dir.sync_all().map_err(Error::IoError)
  }

  fn children(&self, path: &Path) -> Result<Vec<String>, Error> {
    let entries = std::fs::read_dir(path).map_err(Error::IoError)?;
    let mut names = Vec::new();
    for entry in entries {
      let entry = entry.map_err(Error::IoError)?;
      if let Some(name) = entry.file_name().to_str() {
        names.push(name.to_owned());
      }
    }
    Ok(names)
  }

  fn lock_file(&self, path: &Path) -> Result<Box<dyn FileLock>, Error> {
    let file = std::fs::OpenOptions::new()
      .create(true)
      .truncate(true)
      .read(true)
      .write(true)
      .open(path)
      .map_err(Error::IoError)?;
    // Non-blocking exclusive flock.
    use std::os::unix::io::AsRawFd;
    // SAFETY: fd is valid for the lifetime of `file`; flock does not alias memory.
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret != 0 {
      return Err(Error::IoError(std::io::Error::last_os_error()));
    }
    Ok(Box::new(PosixFileLock { _file: file }))
  }

  fn read_string_from_file(&self, path: &Path) -> Result<String, Error> {
    std::fs::read_to_string(path).map_err(Error::IoError)
  }
}

// ── Convenience wrappers for tests and interop ──────────────────────────────

/// Create a [`WritableFile`] from an already-open [`std::fs::File`].
///
/// Intended for tests and interop where a raw `File` handle is available.
#[cfg(test)]
pub fn writable_from_file(file: std::fs::File) -> Box<dyn WritableFile> {
  Box::new(PosixWritableFile {
    inner: std::io::BufWriter::new(file),
  })
}

/// Create a [`SequentialFile`] from an already-open [`std::fs::File`].
///
/// Intended for tests and interop where a raw `File` handle is available.
#[cfg(test)]
pub fn sequential_from_file(file: std::fs::File) -> Box<dyn SequentialFile> {
  Box::new(PosixSequentialFile { inner: file })
}

/// Create a [`RandomAccessFile`] from an already-open [`std::fs::File`].
///
/// Intended for tests and interop where a raw `File` handle is available.
#[cfg(test)]
pub fn random_access_from_file(file: std::fs::File) -> Arc<dyn RandomAccessFile> {
  Arc::new(PosixRandomAccessFile { inner: file })
}

#[cfg(test)]
impl WritableFile for Vec<u8> {
  fn write(&mut self, data: &[u8]) -> Result<(), crate::Error> {
    self.extend_from_slice(data);
    Ok(())
  }
  fn flush(&mut self) -> Result<(), crate::Error> {
    Ok(())
  }
  fn sync(&mut self) -> Result<(), crate::Error> {
    Ok(())
  }
}

// ── MemFileSystem (in-memory, for tests) ─────────────────────────────────────

/// In-memory [`FileSystem`] backed by hash maps instead of the local disk.
///
/// Intended for fast, deterministic, dependency-free tests: no tempdirs, no
/// real I/O, no background durability. It is the second [`FileSystem`] adapter
/// (alongside [`PosixFileSystem`]), which is what turns the trait into a real
/// seam rather than a hypothetical one.
///
/// Semantics chosen to match POSIX closely enough for the database's use:
/// - Each file is a shared `Vec<u8>` "inode". `create_writable` installs a
///   fresh (truncated) inode; existing read handles keep the old one, mirroring
///   POSIX name re-creation.
/// - `flush`/`sync` are no-ops: an in-memory file is always "durable" within
///   the process.
/// - `lock_file` enforces exclusivity within a single `MemFileSystem` instance;
///   the lock is released when the returned handle is dropped.
///
/// Not thread-optimised — a single `Mutex` guards all state — but correct for
/// concurrent access, which is all the database requires.
#[cfg(test)]
pub use memfs::MemFileSystem;

#[cfg(test)]
mod memfs {
  use super::*;
  use std::collections::{HashMap, HashSet};
  use std::io::ErrorKind;
  use std::path::PathBuf;
  use std::sync::Mutex;

  /// Shared, mutable file contents.
  type Inode = Arc<Mutex<Vec<u8>>>;

  #[derive(Default)]
  struct State {
    files: HashMap<PathBuf, Inode>,
    dirs: HashSet<PathBuf>,
    locks: HashSet<PathBuf>,
  }

  pub struct MemFileSystem {
    state: Arc<Mutex<State>>,
  }

  impl MemFileSystem {
    pub fn new() -> Self {
      MemFileSystem {
        state: Arc::new(Mutex::new(State::default())),
      }
    }
  }

  impl Default for MemFileSystem {
    fn default() -> Self {
      Self::new()
    }
  }

  /// Record `path` and all of its ancestor directories as existing.
  fn ensure_ancestor_dirs(dirs: &mut HashSet<PathBuf>, path: &Path) {
    let mut cur = path.parent();
    while let Some(dir) = cur {
      if dir.as_os_str().is_empty() {
        break;
      }
      dirs.insert(dir.to_path_buf());
      cur = dir.parent();
    }
  }

  fn not_found(path: &Path) -> Error {
    Error::IoError(std::io::Error::new(
      ErrorKind::NotFound,
      format!("no such file: {}", path.display()),
    ))
  }

  struct MemSequentialFile {
    inode: Inode,
    pos: usize,
  }

  impl SequentialFile for MemSequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
      let data = self.inode.lock().unwrap();
      let remaining = data.len().saturating_sub(self.pos);
      let n = remaining.min(buf.len());
      buf[..n].copy_from_slice(&data[self.pos..self.pos + n]);
      self.pos += n;
      Ok(n)
    }

    fn skip(&mut self, n: u64) -> Result<(), Error> {
      self.pos = self.pos.saturating_add(n as usize);
      Ok(())
    }
  }

  struct MemRandomAccessFile {
    inode: Inode,
  }

  impl RandomAccessFile for MemRandomAccessFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize, Error> {
      let data = self.inode.lock().unwrap();
      let offset = offset as usize;
      if offset >= data.len() {
        return Ok(0);
      }
      let n = (data.len() - offset).min(buf.len());
      buf[..n].copy_from_slice(&data[offset..offset + n]);
      Ok(n)
    }
  }

  struct MemWritableFile {
    inode: Inode,
  }

  impl WritableFile for MemWritableFile {
    fn write(&mut self, data: &[u8]) -> Result<(), Error> {
      self.inode.lock().unwrap().extend_from_slice(data);
      Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
      Ok(()) // in-memory: nothing to push to the OS
    }

    fn sync(&mut self) -> Result<(), Error> {
      Ok(()) // in-memory: always durable within the process
    }
  }

  struct MemFileLock {
    state: Arc<Mutex<State>>,
    path: PathBuf,
  }

  impl FileLock for MemFileLock {}

  impl Drop for MemFileLock {
    fn drop(&mut self) {
      self.state.lock().unwrap().locks.remove(&self.path);
    }
  }

  impl FileSystem for MemFileSystem {
    fn open_sequential(&self, path: &Path) -> Result<Box<dyn SequentialFile>, Error> {
      let st = self.state.lock().unwrap();
      let inode = st.files.get(path).ok_or_else(|| not_found(path))?;
      Ok(Box::new(MemSequentialFile {
        inode: Arc::clone(inode),
        pos: 0,
      }))
    }

    fn open_random_access(&self, path: &Path) -> Result<Arc<dyn RandomAccessFile>, Error> {
      let st = self.state.lock().unwrap();
      let inode = st.files.get(path).ok_or_else(|| not_found(path))?;
      Ok(Arc::new(MemRandomAccessFile {
        inode: Arc::clone(inode),
      }))
    }

    fn open_appendable(&self, path: &Path) -> Result<Box<dyn WritableFile>, Error> {
      let mut st = self.state.lock().unwrap();
      ensure_ancestor_dirs(&mut st.dirs, path);
      let inode = st
        .files
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
      Ok(Box::new(MemWritableFile {
        inode: Arc::clone(inode),
      }))
    }

    fn create_writable(&self, path: &Path) -> Result<Box<dyn WritableFile>, Error> {
      let mut st = self.state.lock().unwrap();
      ensure_ancestor_dirs(&mut st.dirs, path);
      // Truncate by installing a fresh inode.
      let inode = Arc::new(Mutex::new(Vec::new()));
      st.files.insert(path.to_path_buf(), Arc::clone(&inode));
      Ok(Box::new(MemWritableFile { inode }))
    }

    fn file_size(&self, path: &Path) -> Result<u64, Error> {
      let st = self.state.lock().unwrap();
      let len = st
        .files
        .get(path)
        .ok_or_else(|| not_found(path))?
        .lock()
        .unwrap()
        .len();
      Ok(len as u64)
    }

    fn file_exists(&self, path: &Path) -> bool {
      let st = self.state.lock().unwrap();
      st.files.contains_key(path) || st.dirs.contains(path)
    }

    fn rename(&self, src: &Path, dst: &Path) -> Result<(), Error> {
      let mut st = self.state.lock().unwrap();
      let inode = st.files.remove(src).ok_or_else(|| not_found(src))?;
      ensure_ancestor_dirs(&mut st.dirs, dst);
      st.files.insert(dst.to_path_buf(), inode);
      Ok(())
    }

    fn remove_file(&self, path: &Path) -> Result<(), Error> {
      let mut st = self.state.lock().unwrap();
      st.files
        .remove(path)
        .map(|_| ())
        .ok_or_else(|| not_found(path))
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), Error> {
      let mut st = self.state.lock().unwrap();
      st.dirs.insert(path.to_path_buf());
      ensure_ancestor_dirs(&mut st.dirs, path);
      Ok(())
    }

    fn remove_dir(&self, path: &Path) -> Result<(), Error> {
      let mut st = self.state.lock().unwrap();
      let non_empty = st.files.keys().any(|p| p.parent() == Some(path))
        || st.dirs.iter().any(|p| p.parent() == Some(path));
      if non_empty {
        return Err(Error::IoError(std::io::Error::other(format!(
          "directory not empty: {}",
          path.display()
        ))));
      }
      if !st.dirs.remove(path) {
        return Err(not_found(path));
      }
      Ok(())
    }

    fn sync_dir(&self, _path: &Path) -> Result<(), Error> {
      // In-memory: directory entries are always "durable".
      Ok(())
    }

    fn children(&self, path: &Path) -> Result<Vec<String>, Error> {
      let st = self.state.lock().unwrap();
      if !st.dirs.contains(path) {
        return Err(not_found(path));
      }
      let mut names = Vec::new();
      for p in st.files.keys().chain(st.dirs.iter()) {
        if p.parent() == Some(path) {
          if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            names.push(name.to_owned());
          }
        }
      }
      Ok(names)
    }

    fn lock_file(&self, path: &Path) -> Result<Box<dyn FileLock>, Error> {
      let mut st = self.state.lock().unwrap();
      if st.locks.contains(path) {
        return Err(Error::IoError(std::io::Error::new(
          ErrorKind::WouldBlock,
          format!("lock held: {}", path.display()),
        )));
      }
      // POSIX creates the lock file on disk; mirror that.
      ensure_ancestor_dirs(&mut st.dirs, path);
      st.files
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
      st.locks.insert(path.to_path_buf());
      Ok(Box::new(MemFileLock {
        state: Arc::clone(&self.state),
        path: path.to_path_buf(),
      }))
    }
  }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn posix_write_read_sequential() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.txt");
    let fs = PosixFileSystem;

    let mut w = fs.create_writable(&path).unwrap();
    w.write(b"hello world").unwrap();
    w.flush().unwrap();
    w.sync().unwrap();
    drop(w);

    let mut r = fs.open_sequential(&path).unwrap();
    let mut buf = [0u8; 64];
    let n = r.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"hello world");
  }

  #[test]
  fn posix_random_access() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let fs = PosixFileSystem;

    let mut w = fs.create_writable(&path).unwrap();
    w.write(b"abcdefghij").unwrap();
    w.flush().unwrap();
    drop(w);

    let r = fs.open_random_access(&path).unwrap();
    let mut buf = [0u8; 3];
    let n = r.read_at(&mut buf, 4).unwrap();
    assert_eq!(&buf[..n], b"efg");
  }

  #[test]
  fn posix_file_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sized.bin");
    let fs = PosixFileSystem;

    let mut w = fs.create_writable(&path).unwrap();
    w.write(&[0u8; 1234]).unwrap();
    w.flush().unwrap();
    drop(w);

    assert_eq!(fs.file_size(&path).unwrap(), 1234);
  }

  #[test]
  fn posix_children() {
    let dir = tempfile::tempdir().unwrap();
    let fs = PosixFileSystem;
    fs.create_writable(&dir.path().join("a.txt")).unwrap();
    fs.create_writable(&dir.path().join("b.txt")).unwrap();

    let mut names = fs.children(dir.path()).unwrap();
    names.sort();
    assert_eq!(names, vec!["a.txt", "b.txt"]);
  }

  #[test]
  fn posix_lock_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("LOCK");
    let fs = PosixFileSystem;

    let _lock = fs.lock_file(&path).unwrap();
    // Second lock on the same file should fail immediately.
    assert!(fs.lock_file(&path).is_err());
  }

  #[test]
  fn posix_lock_released_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("LOCK");
    let fs = PosixFileSystem;

    let lock = fs.lock_file(&path).unwrap();
    drop(lock);
    // After dropping, a new lock should succeed.
    let _lock2 = fs.lock_file(&path).unwrap();
  }

  #[test]
  fn posix_write_and_read_string() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("text.txt");
    let fs = PosixFileSystem;

    fs.write_string_to_file(&path, "hello\n").unwrap();
    assert_eq!(fs.read_string_from_file(&path).unwrap(), "hello\n");
  }

  #[test]
  fn posix_file_exists() {
    let dir = tempfile::tempdir().unwrap();
    let fs = PosixFileSystem;
    let path = dir.path().join("exists.txt");

    assert!(!fs.file_exists(&path));
    fs.create_writable(&path).unwrap();
    assert!(fs.file_exists(&path));
  }

  #[test]
  fn posix_rename_and_remove() {
    let dir = tempfile::tempdir().unwrap();
    let fs = PosixFileSystem;
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");

    fs.create_writable(&src).unwrap();
    assert!(fs.file_exists(&src));
    fs.rename(&src, &dst).unwrap();
    assert!(!fs.file_exists(&src));
    assert!(fs.file_exists(&dst));
    fs.remove_file(&dst).unwrap();
    assert!(!fs.file_exists(&dst));
  }

  #[test]
  fn posix_sync_dir() {
    let dir = tempfile::tempdir().unwrap();
    let fs = PosixFileSystem;
    fs.create_writable(&dir.path().join("f.txt")).unwrap();
    fs.sync_dir(dir.path()).unwrap();
    // Syncing a non-existent directory must error, not panic.
    assert!(fs.sync_dir(&dir.path().join("no_such_dir")).is_err());
  }

  #[test]
  fn posix_appendable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("append.txt");
    let fs = PosixFileSystem;

    let mut w = fs.create_writable(&path).unwrap();
    w.write(b"hello").unwrap();
    w.flush().unwrap();
    drop(w);

    let mut w = fs.open_appendable(&path).unwrap();
    w.write(b" world").unwrap();
    w.flush().unwrap();
    drop(w);

    assert_eq!(fs.read_string_from_file(&path).unwrap(), "hello world");
  }

  // ── MemFileSystem parity tests ────────────────────────────────────────────

  use std::path::Path;

  #[test]
  fn mem_write_read_sequential() {
    let fs = MemFileSystem::new();
    let path = Path::new("/db/test.txt");
    fs.create_dir_all(Path::new("/db")).unwrap();

    let mut w = fs.create_writable(path).unwrap();
    w.write(b"hello world").unwrap();
    w.sync().unwrap();
    drop(w);

    let mut r = fs.open_sequential(path).unwrap();
    let mut buf = [0u8; 64];
    let n = r.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"hello world");
    // A second read is at EOF.
    assert_eq!(r.read(&mut buf).unwrap(), 0);
  }

  #[test]
  fn mem_random_access() {
    let fs = MemFileSystem::new();
    let path = Path::new("/db/test.bin");

    let mut w = fs.create_writable(path).unwrap();
    w.write(b"abcdefghij").unwrap();
    drop(w);

    let r = fs.open_random_access(path).unwrap();
    let mut buf = [0u8; 3];
    let n = r.read_at(&mut buf, 4).unwrap();
    assert_eq!(&buf[..n], b"efg");
    // Reading at/after EOF yields 0.
    assert_eq!(r.read_at(&mut buf, 100).unwrap(), 0);
  }

  #[test]
  fn mem_file_size_and_exists() {
    let fs = MemFileSystem::new();
    let path = Path::new("/db/sized.bin");
    assert!(!fs.file_exists(path));
    assert!(matches!(fs.file_size(path), Err(Error::IoError(_))));

    let mut w = fs.create_writable(path).unwrap();
    w.write(&[0u8; 1234]).unwrap();
    drop(w);

    assert!(fs.file_exists(path));
    assert_eq!(fs.file_size(path).unwrap(), 1234);
  }

  #[test]
  fn mem_children_lists_directory() {
    let fs = MemFileSystem::new();
    fs.create_dir_all(Path::new("/db")).unwrap();
    fs.create_writable(Path::new("/db/a.txt")).unwrap();
    fs.create_writable(Path::new("/db/b.txt")).unwrap();
    fs.create_dir_all(Path::new("/db/sub")).unwrap();

    let mut names = fs.children(Path::new("/db")).unwrap();
    names.sort();
    assert_eq!(names, vec!["a.txt", "b.txt", "sub"]);
  }

  #[test]
  fn mem_create_writable_truncates() {
    let fs = MemFileSystem::new();
    let path = Path::new("/db/f");
    let mut w = fs.create_writable(path).unwrap();
    w.write(b"old contents").unwrap();
    drop(w);

    let mut w = fs.create_writable(path).unwrap();
    w.write(b"new").unwrap();
    drop(w);

    assert_eq!(fs.file_size(path).unwrap(), 3);
  }

  #[test]
  fn mem_appendable_extends() {
    let fs = MemFileSystem::new();
    let path = Path::new("/db/append.txt");

    let mut w = fs.create_writable(path).unwrap();
    w.write(b"hello").unwrap();
    drop(w);

    let mut w = fs.open_appendable(path).unwrap();
    w.write(b" world").unwrap();
    drop(w);

    assert_eq!(fs.read_string_from_file(path).unwrap(), "hello world");
  }

  #[test]
  fn mem_rename_and_remove() {
    let fs = MemFileSystem::new();
    let src = Path::new("/db/src.txt");
    let dst = Path::new("/db/dst.txt");

    fs.create_writable(src).unwrap();
    assert!(fs.file_exists(src));
    fs.rename(src, dst).unwrap();
    assert!(!fs.file_exists(src));
    assert!(fs.file_exists(dst));
    fs.remove_file(dst).unwrap();
    assert!(!fs.file_exists(dst));
    assert!(matches!(fs.remove_file(dst), Err(Error::IoError(_))));
  }

  #[test]
  fn mem_sync_dir_is_noop() {
    let fs = MemFileSystem::new();
    fs.create_dir_all(Path::new("/db")).unwrap();
    fs.sync_dir(Path::new("/db")).unwrap();
  }

  #[test]
  fn mem_remove_dir_rejects_non_empty() {
    let fs = MemFileSystem::new();
    fs.create_dir_all(Path::new("/db")).unwrap();
    fs.create_writable(Path::new("/db/f")).unwrap();
    assert!(fs.remove_dir(Path::new("/db")).is_err());
    fs.remove_file(Path::new("/db/f")).unwrap();
    fs.remove_dir(Path::new("/db")).unwrap();
    assert!(!fs.file_exists(Path::new("/db")));
  }

  #[test]
  fn mem_lock_is_exclusive_and_released_on_drop() {
    let fs = MemFileSystem::new();
    let path = Path::new("/db/LOCK");

    let lock = fs.lock_file(path).unwrap();
    // A second lock on the same path fails immediately.
    assert!(fs.lock_file(path).is_err());
    drop(lock);
    // After drop, locking succeeds again.
    let _lock2 = fs.lock_file(path).unwrap();
  }
}
