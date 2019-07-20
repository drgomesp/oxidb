#![deny(
    rust_2018_idioms,
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

/// `column_value` ...
pub mod column_value;

/// `data_type` ...
pub mod data_type;

use failure::Error;
use std::borrow::Cow;

pub use crate::column_value::*;
pub use crate::data_type::*;

/// `ColumnValueOps` defines column operations.
pub trait ColumnValueOps: Sized + Clone + Eq {
    /// ColumnType is the actual column type implemented by the database layer.
    type ColumnType: Copy + Clone + Sized;

    /// Deserialize `bytes` into `Self` for a given `Self::ColumnType`.
    fn from_bytes(column_type: &Self::ColumnType, bytes: Cow<'_, [u8]>) -> Result<Self, Error>;

    /// Serialize `Self` into byte slice for a given `Self::ColumnType`.
    fn to_bytes(&self, column_type: &Self::ColumnType) -> Result<Box<[u8]>, Error>;
}
