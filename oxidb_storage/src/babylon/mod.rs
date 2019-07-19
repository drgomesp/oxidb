use crate::babylon::column::Column;
use crate::babylon::table::Table;
use crate::{ReadOps, StorageFactory, WriteOps};
use failure::Error;
use oxidb_core::types::{ColumnValue, DataType};
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

        let table = Table::new(String::from("users"), columns);

        Ok(BabylonStorage {
            tables: vec![table],
        })
    }
}

impl<'a> ReadOps<'a> for BabylonStorage {
    type ColumnValue = ColumnValue;

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [Self::ColumnValue]: std::borrow::ToOwned,
    {
        self.tables
            .first()
            .expect("could not get first table")
            .iter()
    }
}

impl<'a> WriteOps<'a> for BabylonStorage {
    type ColumnValue = ColumnValue;

    fn insert<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        self.tables
            .first_mut()
            .expect("could not get first table")
            .insert(row)
    }
}
