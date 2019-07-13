#![feature(box_syntax)]

#[macro_use]
extern crate bitflags;
extern crate failure;
extern crate log;
extern crate oxidb_core;

pub mod babylon;

use failure::Error;
use oxidb_core::ColumnValueOps;
use std::borrow::Cow;

pub trait StorageOps<'a> {
    type ColumnValue: ColumnValueOps;

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [<Self as StorageOps<'a>>::ColumnValue]: std::borrow::ToOwned;

    fn insert_row<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>;
}
