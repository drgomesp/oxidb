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

use std::borrow::Cow;

use failure::Error;

use crate::types::DataType;

/// `types` groups the basic core supported types and default implementations.
pub mod types;

/// `ColumnValueOps` defines column operations.
pub trait ColumnValueOps: Sized {
    /// ColumnType is the actual column type implemented by the database layer.
    type ColumnType: Copy + Clone;

    /// Deserialize `bytes` into `Self` for a given `Self::ColumnType`.
    fn from_bytes(column_type: &Self::ColumnType, bytes: Cow<[u8]>) -> Result<Self, Error>;

    /// Serialize `Self` into byte slice for a given `Self::ColumnType`.
    fn to_bytes(&self, column_type: &Self::ColumnType) -> Result<Box<[u8]>, Error>;
}
