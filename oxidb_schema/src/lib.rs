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

//! # oxidb_schema
//!
//! `oxidb_schema` is the core abstraction layer for an `oxidb` database system schema definition.
//!
//! ## Examples
//!

use oxidb_core::DataType;
use std::fmt::Debug;

/// `ColumnInfo` exposes column info.
pub trait ColumnInfo: Debug + Sized {
    /// Returns the column name.
    fn get_name(&self) -> &str;

    /// Returns the column's `DataType`.
    fn get_data_type(&self) -> &DataType;
}
