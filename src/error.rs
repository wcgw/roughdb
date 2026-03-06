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

/// Mirrors LevelDB's `Status` error codes (excluding `Ok`, which is `Result::Ok`).
#[derive(Debug)]
pub enum Error {
  NotFound,
  Corruption(String),
  InvalidArgument(String),
  NotSupported(String),
  IoError(std::io::Error),
}

impl Error {
  pub fn is_not_found(&self) -> bool {
    matches!(self, Error::NotFound)
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Error::NotFound => write!(f, "NotFound"),
      Error::Corruption(msg) => write!(f, "Corruption: {msg}"),
      Error::InvalidArgument(msg) => write!(f, "InvalidArgument: {msg}"),
      Error::NotSupported(msg) => write!(f, "NotSupported: {msg}"),
      Error::IoError(e) => write!(f, "IO error: {e}"),
    }
  }
}

impl std::error::Error for Error {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      Error::IoError(e) => Some(e),
      _ => None,
    }
  }
}

impl From<std::io::Error> for Error {
  fn from(e: std::io::Error) -> Self {
    Error::IoError(e)
  }
}
