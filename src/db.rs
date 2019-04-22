use failure::Error;
use std::borrow::Cow;

#[derive(Clone, Debug)]
pub enum DataType {
    String { length: usize },
    Integer { signed: bool, bytes: u8 },
}

impl DataType {
    pub fn get_fixed_length(&self) -> Option<usize> {
        match self {
            DataType::String { length } => Some(*length),
            DataType::Integer { bytes, .. } => Some(*bytes as usize),
        }
    }
}

pub trait ColumnValueOps: Sized {
    type ColumnType;

    fn from_bytes(column_type: &Self::ColumnType, bytes: Cow<[u8]>) -> Result<Self, Error>;
    fn to_bytes(&self, column_type: &Self::ColumnType) -> Result<Box<[u8]>, Error>;
}

pub trait ColumnOps: Sized {
    fn get_offset(&self) -> u32;
    fn get_name(&self) -> &str;
    fn get_data_type(&self) -> &DataType;
}

pub trait TableOps<'a> {
    type ColumnValue: 'a + Clone;

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>;

    fn insert<T>(&mut self, column_data: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>;
}
