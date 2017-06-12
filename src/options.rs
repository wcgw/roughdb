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
      _ => panic!("WTF?!")
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
