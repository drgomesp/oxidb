/// `DataType` defines the core supported types of the database system.
#[derive(Copy, Clone, Debug)]
pub enum DataType {
    /// A variable-length character string.
    String(usize),

    /// Signed or unsigned [8-64]-bit integer.
    Integer {
        /// Signed or unsigned.
        signed: bool,

        /// [8-64]-bit integer.
        bytes: u8,
    },
}

impl DataType {
    /// Get the optional fixed length of the data type.
    pub fn get_fixed_length(&self) -> Option<usize> {
        match self {
            DataType::String(length) => Some(*length),
            DataType::Integer { bytes, .. } => Some(*bytes as usize),
        }
    }
}
