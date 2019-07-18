use std::borrow::Cow;

use crate::babylon::column::Column;
use crate::babylon::page::Page;
use crate::StorageOps;
use failure::Error;
use oxidb_core::types::ColumnValue;
use oxidb_schema::ColumnInfo;

#[derive(Debug)]
pub struct Table {
    name: String,
    pub columns: Vec<Column>,
    num_rows: u64,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            num_rows: 0,
        }
    }
}

impl<'a> StorageOps<'a> for Table {
    type ColumnValue = ColumnValue;

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [<Self as StorageOps<'a>>::ColumnValue]: std::borrow::ToOwned,
    {
        unimplemented!();
    }

    fn insert_row<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        let mut page = Page::new(&self.columns);
        page.insert_row(row);

        Ok(())
    }
}
