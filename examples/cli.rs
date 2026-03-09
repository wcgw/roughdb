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

//! Simple command-line interface for a persistent RoughDB database.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example cli -- --dir /tmp/mydb put "hello" "world"
//! cargo run --example cli -- --dir /tmp/mydb get "hello"
//! cargo run --example cli -- --dir /tmp/mydb delete "hello"
//! ```

use clap::{Parser, Subcommand};
use roughdb::Db;

#[derive(Parser)]
#[command(
  name = "roughdb-cli",
  about = "Interact with a RoughDB database on disk"
)]
struct Cli {
  /// Path to the database directory (created if it does not exist).
  #[arg(long, short)]
  dir: std::path::PathBuf,

  #[command(subcommand)]
  command: Command,
}

#[derive(Subcommand)]
enum Command {
  /// Read the value stored at KEY.
  Get {
    /// The key to look up.
    key: String,
  },
  /// Write VALUE at KEY, overwriting any previous value.
  Put {
    /// The key to write.
    key: String,
    /// The value to store.
    value: String,
  },
  /// Remove KEY from the database.
  Delete {
    /// The key to remove.
    key: String,
  },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let cli = Cli::parse();

  let opts = roughdb::Options {
    create_if_missing: true,
    ..Default::default()
  };
  let db = Db::open(&cli.dir, opts)?;

  match cli.command {
    Command::Get { key } => match db.get(key.as_bytes()) {
      Ok(value) => println!("{}", String::from_utf8_lossy(&value)),
      Err(e) if e.is_not_found() => {
        eprintln!("not found: {key}");
        std::process::exit(1);
      }
      Err(e) => return Err(e.into()),
    },
    Command::Put { key, value } => {
      db.put(key.as_bytes(), value.as_bytes())?;
    }
    Command::Delete { key } => {
      db.delete(key.as_bytes())?;
    }
  }

  Ok(())
}
