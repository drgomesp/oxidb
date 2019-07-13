pub(crate) mod column_value;

#[derive(Clone, Debug)]
pub enum DataType {
    String(usize),
    Integer { signed: bool, bytes: u8 },
}

impl DataType {
    pub fn get_fixed_length(&self) -> Option<usize> {
        match self {
            DataType::String(length) => Some(*length),
            DataType::Integer { bytes, .. } => Some(*bytes as usize),
        }
    }
}
