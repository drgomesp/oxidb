#![deny(
    missing_docs,
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_import_braces
)]
#![feature(box_syntax)]

//! # oxidb_core
//!
//! `oxidb_storage` is the storage abstraction layer for an `oxidb` database system implementation.
//!
//! ## Examples
//!

#[macro_use]
extern crate bitflags;
extern crate failure;
extern crate log;
extern crate oxidb_core;
extern crate oxidb_schema;

/// `babylon` is `oxidb`'s default storage engine.
pub mod babylon;

use failure::Error;
use oxidb_core::ColumnValueOps;
use std::borrow::Cow;

/// `StorageOps` define the basic interface of storage engines.
pub trait StorageOps<'a> {
    /// `ColumnValue` implementation of `ColumnValueOps`
    type ColumnValue: ColumnValueOps;

    /// `iter` Returns an iterator over rows as arrays of `Self::ColumnValue` type items.
    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [<Self as StorageOps<'a>>::ColumnValue]: std::borrow::ToOwned;

    /// `insert_row` inserts a new row.
    fn insert_row<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>;
}
