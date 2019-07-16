use crate::StorageOps;
use failure::Error;
use oxidb_core::{types::ColumnValue, ColumnInfo};
use std::borrow::Cow;

#[derive(Debug)]
pub struct Table<'a> {
    name: String,
    pub columns: &'a [Box<dyn ColumnInfo>],
    num_rows: u64,
}

impl<'a> Table<'a> {
    pub fn new(name: String, columns: &'a [Box<dyn ColumnInfo>]) -> Self {
        Self {
            name,
            columns,
            num_rows: 0,
        }
    }
}

impl<'a> StorageOps<'a> for Table<'a> {
    type ColumnValue = ColumnValue;

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [<Self as StorageOps<'a>>::ColumnValue]: std::borrow::ToOwned,
    {
        unimplemented!();
    }

    fn insert_row<T>(&mut self, _: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        unimplemented!()
    }
}
