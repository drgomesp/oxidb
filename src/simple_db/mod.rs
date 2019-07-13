use crate::{
    db::{ColumnInfo, TableOps},
    storage::{page::Page, PageOps},
    types::{ColumnValue, DataType},
};
use failure::Error;
use std::borrow::Cow;

pub(crate) const MAX_PAGES: u8 = 100;

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: String, value_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type: value_type,
            nullable,
        }
    }
}

impl ColumnInfo for Column {
    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
pub(crate) struct Table<'a> {
    name: String,
    pub columns: &'a [Box<dyn ColumnInfo>],
    pages: Box<[Page<'a>]>,
    num_rows: u64,
}

impl<'a> Table<'a> {
    pub fn new(name: String, columns: &'a [Box<dyn ColumnInfo>]) -> Self {
        Self {
            name,
            columns,
            pages: vec![Page::new(&columns); MAX_PAGES as usize].into_boxed_slice(),
            num_rows: 0,
        }
    }
}

impl<'a> TableOps<'a> for Table<'a> {
    type ColumnValue = ColumnValue;

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [<Self as TableOps<'a>>::ColumnValue]: std::borrow::ToOwned,
    {
        self.pages[0].iter()
    }

    fn insert<T>(&mut self, column_data: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        let boxed_page = &mut self.pages[0];
        boxed_page.insert(column_data)?;

        self.num_rows += 1;

        Ok(())
    }
}
