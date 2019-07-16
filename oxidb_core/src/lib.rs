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
//! `oxidb_core` is the core abstraction layer for an `oxidb` database system implementation.
//!
//! ## Examples
//!

/// `types` groups the basic core supported types and default implementations.
pub mod types;

use self::types::DataType;
use failure::Error;
use std::{borrow::Cow, fmt::Debug};

/// `ColumnValueOps` defines column operations.
pub trait ColumnValueOps: Sized {
    /// ColumnType is the actual column type implemented by the database layer.
    type ColumnType: Copy + Clone;

    /// Deserialize `bytes` into `Self` for a given `Self::ColumnType`.
    fn from_bytes(column_type: &Self::ColumnType, bytes: Cow<[u8]>) -> Result<Self, Error>;

    /// Serialize `Self` into byte slice for a given `Self::ColumnType`.
    fn to_bytes(&self, column_type: &Self::ColumnType) -> Result<Box<[u8]>, Error>;
}

/// `ColumnInfo` exposes column info.
pub trait ColumnInfo: Debug {
    /// Returns the column name.
    fn get_name(&self) -> &str;

    /// Returns the column's `DataType`.
    fn get_data_type(&self) -> &DataType;
}
