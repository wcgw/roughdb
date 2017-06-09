pub enum ValueType {
  Deletion,
  Value,
  ValueWriteTime,
  ValueExplicitExpiry,
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
  fn size() {
    assert_eq!(1, size_of::<ValueType>());
  }
}
