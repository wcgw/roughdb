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
//

pub enum ValueType {
  Deletion,
  Value,
  ValueWriteTime,
  ValueExplicitExpiry,
}

impl ValueType {
  pub fn from_byte(value: u8) -> ValueType {
    match value {
      0 => ValueType::Deletion,
      1 => ValueType::Value,
      2 => ValueType::ValueWriteTime,
      3 => ValueType::ValueExplicitExpiry,
      _ => panic!("WTF?!"),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::mem::size_of;

  #[test]
  fn as_byte() {
    assert_eq!(0, ValueType::Deletion as u8);
    assert_eq!(1, ValueType::Value as u8);
    assert_eq!(2, ValueType::ValueWriteTime as u8);
    assert_eq!(3, ValueType::ValueExplicitExpiry as u8);
  }

  #[test]
  fn from_byte() {
    assert_eq!(0, ValueType::from_byte(0) as u8);
    assert_eq!(1, ValueType::from_byte(1) as u8);
    assert_eq!(2, ValueType::from_byte(2) as u8);
    assert_eq!(3, ValueType::from_byte(3) as u8);
  }
  #[test]
  fn size() {
    assert_eq!(1, size_of::<ValueType>());
  }
}
