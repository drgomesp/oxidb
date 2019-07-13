pub mod types;

use crate::types::DataType;
use failure::Error;
use std::borrow::Cow;
use std::fmt::Debug;

pub trait ColumnValueOps: Sized {
    type ColumnType;

    fn from_bytes(column_type: &Self::ColumnType, bytes: Cow<[u8]>) -> Result<Self, Error>;
    fn to_bytes(&self, column_type: &Self::ColumnType) -> Result<Box<[u8]>, Error>;
}

pub trait ColumnInfo: Debug {
    fn get_name(&self) -> &str;
    fn get_data_type(&self) -> &DataType;
}
