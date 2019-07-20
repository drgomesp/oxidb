use std::borrow::Cow;

use failure::Error;
use oxidb_core::ColumnValue;

use crate::babylon::{column::Column, page::Page};
use crate::{ReadOps, WriteOps};
use oxidb_core::ColumnValueOps;
use oxidb_schema::ColumnInfo;

const MAX_PAGES: usize = 100;

#[derive(Debug)]
pub struct Table {
    name: String,
    pub columns: Vec<Column>,
    num_rows: u64,

    pages: Vec<Page>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            num_rows: 0,

            pages: vec![Page::default(); MAX_PAGES],
        }
    }
}

impl ReadOps for Table {
    type ColumnValue = ColumnValue;

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Cow<'a, [Self::ColumnValue]>> + 'a>
    where
        [Self::ColumnValue]: std::borrow::ToOwned,
    {
        let page = self.pages.first().expect("could not get first page");
        let (mut row_num, mut column_offset) = (0, 0);

        box page.offsets.iter().map(move |(offset, _)| {
            column_offset = *offset as usize;

            let row: Vec<ColumnValue> = self
                .columns
                .iter()
                .map(move |column| {
                    let size = column.get_data_type().get_fixed_length().unwrap();
                    let bytes = &page.data[column_offset..column_offset + size];

                    let v = ColumnValueOps::from_bytes(column.get_data_type(), Cow::from(bytes))
                        .expect("could not create column value ops from bytes");

                    column_offset += size;

                    v
                })
                .collect();

            row_num += 1;

            Cow::from(row)
        })
    }
}

impl<'a> WriteOps<'a> for Table {
    type ColumnValue = ColumnValue;

    fn insert<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        let mut ops = (
            &self.columns,
            self.pages
                .first_mut()
                .expect("could not get first table page"),
        );

        ops.insert(row)
    }
}
