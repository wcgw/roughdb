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

use std::fmt;
use std::sync::Arc;

/// An error from a database operation.
///
/// Mirrors LevelDB's `Status` codes, except `Ok` (which is `Result::Ok`) and
/// `NotFound` (an absent key is `Ok(None)`, not an error).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
  /// Stored data failed a consistency check: a checksum mismatch, or a
  /// malformed block, log record or MANIFEST edit.
  Corruption(String),
  /// The operation cannot accept an argument, or the database's state, as
  /// given: e.g. opening a missing database with `OpenMode::Existing`.
  InvalidArgument(String),
  /// The data asks for something this build does not support, such as an
  /// unknown compression type.
  NotSupported(String),
  /// A file-system operation failed.  Shared, so that cloning the error (a
  /// failed background write is reported to every later caller) keeps the
  /// original rather than a copy of its message.
  IoError(Arc<std::io::Error>),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Error::Corruption(msg) => write!(f, "corruption: {msg}"),
      Error::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
      Error::NotSupported(msg) => write!(f, "not supported: {msg}"),
      Error::IoError(e) => write!(f, "I/O error: {e}"),
    }
  }
}

impl std::error::Error for Error {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      Error::IoError(e) => Some(&**e),
      _ => None,
    }
  }
}

impl From<std::io::Error> for Error {
  fn from(e: std::io::Error) -> Self {
    Error::IoError(Arc::new(e))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn error_is_send_sync_and_static() {
    fn assert_bounds<T: Send + Sync + 'static>() {}
    assert_bounds::<Error>();
  }

  #[test]
  fn a_cloned_io_error_keeps_the_original() {
    let e = Error::from(std::io::Error::other("disk on fire"));
    let clone = e.clone();
    let (Error::IoError(a), Error::IoError(b)) = (&e, &clone) else {
      unreachable!()
    };
    assert!(Arc::ptr_eq(a, b));
    assert_eq!(clone.to_string(), "I/O error: disk on fire");
    assert!(std::error::Error::source(&clone).is_some());
  }
}
