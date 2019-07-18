use crate::babylon::column::Column;
use crate::babylon::table::Table;
use crate::{StorageFactory, StorageOps};
use failure::Error;
use oxidb_core::types::{ColumnValue, DataType};
use oxidb_core::ColumnValueOps;
use oxidb_schema::ColumnInfo;
use std::borrow::Cow;

mod column;
mod page;
mod table;

/// `BabylonStorage` is the default implementation of the babylon storage runtime.
#[derive(Debug)]
pub struct BabylonStorage {
    tables: Vec<Table>,
}

impl<'a> StorageFactory<'a> for BabylonStorage {
    type Storage = BabylonStorage;

    fn build() -> Result<Self::Storage, Error> {
        let columns: Vec<Column> = vec![
            Column::new(
                String::from("id"),
                DataType::Integer {
                    bytes: 8,
                    signed: false,
                },
                true,
            ),
            Column::new(String::from("first_name"), DataType::String(8), true),
            Column::new(String::from("last_name"), DataType::String(8), true),
        ];

        let mut table = Table::new(String::from("users"), columns);

        Ok(BabylonStorage {
            tables: vec![table],
        })
    }
}

impl<'a> StorageOps<'a> for BabylonStorage {
    type ColumnValue = ColumnValue;

    fn iter<'b>(&'b self) -> Box<Iterator<Item = Cow<[Self::ColumnValue]>>>
    where
        [Self::ColumnValue]: std::borrow::ToOwned,
    {
        unimplemented!()
    }

    fn insert_row<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        let mut table = self.tables.first_mut().expect("could not get first table");
        table.insert_row(row)
    }
}
