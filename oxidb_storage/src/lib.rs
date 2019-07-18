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

use std::borrow::Cow;

use failure::Error;
use oxidb_core::ColumnValueOps;

/// `babylon` is `oxidb`'s default storage engine.
pub mod babylon;

/// `StorageOps` define the basic interface of storage engines.
pub trait StorageOps<'a> {
    /// `ColumnValue` implementation of `ColumnValueOps`
    type ColumnValue: ColumnValueOps;

    /// `iter` Returns an iterator over rows as arrays of `Self::ColumnValue` type items.
    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [Self::ColumnValue]: std::borrow::ToOwned;

    /// `insert_row` inserts a new row.
    fn insert_row<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>;
}

/// `StorageFactory` defines the interface to build the storage engine.
pub trait StorageFactory<'a> {
    /// The `StorageOps` implementation.
    type Storage: StorageOps<'a>;

    /// `build` a new storage engine instance.
    fn build() -> Result<Self::Storage, Error>;
}
