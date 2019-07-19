use std::borrow::ToOwned;
use std::{borrow::Cow, fmt};

use byteorder::{ByteOrder, LittleEndian};
use failure::Error;

use crate::{types::DataType, ColumnValueOps};

/// `ColumnValue` is an interpretation the database supported types as meaningful column value
/// enumerations. Each item holds its inner value, each one with their own specific types.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ColumnValue {
    /// `StringLiteral` holds a `String`.
    StringLiteral(String),

    /// `UnsignedInteger(u64)` holds a `u64`
    UnsignedInteger(u64),

    /// `SignedInteger(i64)` holds a `i64`
    SignedInteger(i64),
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            ColumnValue::StringLiteral(ref s) => write!(f, "{}", s),
            ColumnValue::SignedInteger(n) => write!(f, "{}", n),
            ColumnValue::UnsignedInteger(n) => write!(f, "{}", n),
        }
    }
}

impl ColumnValueOps for ColumnValue {
    type ColumnType = DataType;

    fn from_bytes(column_type: &Self::ColumnType, bytes: Cow<[u8]>) -> Result<Self, Error> {
        match column_type {
            DataType::Integer { signed, .. } => Ok({
                if *signed {
                    ColumnValue::SignedInteger(LittleEndian::read_i64(&bytes))
                } else {
                    ColumnValue::UnsignedInteger(LittleEndian::read_u64(&bytes))
                }
            }),
            DataType::String(length) => {
                let len = *length;

                if len > 0 {
                    let s = String::from_utf8_lossy(&bytes[0..len]);
                    Ok(ColumnValue::StringLiteral(s.to_string()))
                } else {
                    unimplemented!()
                }
            }
        }
    }

    fn to_bytes(&self, column_type: &Self::ColumnType) -> Result<Box<[u8]>, Error> {
        match (&self, column_type) {
            (ColumnValue::StringLiteral(ref s), DataType::String(length)) => {
                assert!(*length >= s.len());

                let mut buf = s.to_owned().into_bytes();
                let remaining = vec![0u8; *length - s.len()];

                buf.extend_from_slice(remaining.as_slice());

                Ok(buf.into_boxed_slice())
            }
            (
                ColumnValue::UnsignedInteger(i),
                DataType::Integer {
                    signed: false,
                    bytes: n,
                },
            ) => {
                let mut buf = vec![0u8; *n as usize];
                LittleEndian::write_u64(&mut buf, *i);
                Ok(buf.to_owned().into_boxed_slice())
            }
            (
                ColumnValue::SignedInteger(i),
                DataType::Integer {
                    signed: true,
                    bytes: n,
                },
            ) => {
                let mut buf = vec![0u8; *n as usize];
                LittleEndian::write_i64(&mut buf, *i);
                Ok(buf.to_owned().into_boxed_slice())
            }
            _ => unimplemented!("{:#?} {:#?}", &self, column_type),
        }
    }
}
