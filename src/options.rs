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

#[derive(Debug, PartialEq)]
pub enum ValueType {
  Deletion,
  Value,
  ValueWriteTime,
  ValueExplicitExpiry,
}

impl TryFrom<u8> for ValueType {
  type Error = ();

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(ValueType::Deletion),
      1 => Ok(ValueType::Value),
      2 => Ok(ValueType::ValueWriteTime),
      3 => Ok(ValueType::ValueExplicitExpiry),
      _ => Err(()),
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
    assert_eq!(Ok(ValueType::Deletion), 0u8.try_into());
    assert_eq!(Ok(ValueType::Value), 1u8.try_into());
    assert_eq!(Ok(ValueType::ValueWriteTime), 2u8.try_into());
    assert_eq!(Ok(ValueType::ValueExplicitExpiry), 3u8.try_into());
    assert!(<u8 as TryInto<ValueType>>::try_into(4u8).is_err());
  }
  #[test]
  fn size() {
    assert_eq!(1, size_of::<ValueType>());
  }
}
