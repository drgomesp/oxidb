use crate::{
    db::{ColumnOps, ColumnValueOps, DataType, PageOps, TableOps},
    storage::page::Page,
};
use byteorder::{ByteOrder, LittleEndian};
use failure::Error;
use std::borrow::Cow;
use std::fmt;

pub(crate) const MAX_PAGES: u8 = 100;

#[derive(Clone, Debug)]
pub(crate) enum ColumnValue {
    StringLiteral(String),
    UnsignedInteger(u64),
    SignedInteger(i64),
}

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub name: String,
    pub value_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: String, value_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            value_type,
            nullable,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Table<'a> {
    name: String,
    pub columns: &'a [Column],
    pages: Box<[Page<'a>]>,
    num_rows: u64,
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
            _ => unimplemented!("{:#?}, {:#?}", &self, column_type),
        }
    }
}

impl ColumnOps for Column {
    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_data_type(&self) -> &DataType {
        &self.value_type
    }
}

impl<'a> Table<'a> {
    pub fn new(name: String, columns: &'a [Column]) -> Self {
        Self {
            name,
            columns,
            pages: vec![Page::new(columns); MAX_PAGES as usize].into_boxed_slice(),
            num_rows: 0,
        }
    }
}

impl<'a> TableOps<'a> for Table<'a> {
    type ColumnValue = ColumnValue;

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b>
    where
        [<Self as TableOps<'a>>::ColumnValue]: std::borrow::ToOwned,
    {
        self.pages[0].iter()
    }

    fn insert<T>(&mut self, column_data: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        let boxed_page = &mut self.pages[0];
        boxed_page.insert(column_data)?;

        self.num_rows += 1;

        Ok(())
    }
}

fn build_row_data<I>(values: I) -> Vec<Box<[u8]>>
where
    I: ExactSizeIterator,
    I: Iterator<Item = ColumnValue>,
{
    values
        .map(|ref value| match value {
            ColumnValue::StringLiteral(ref s) => {
                value.to_bytes(&DataType::String(s.len())).unwrap()
            }
            ColumnValue::SignedInteger(..) => value
                .to_bytes(&DataType::Integer {
                    bytes: 8,
                    signed: true,
                })
                .unwrap(),
            ColumnValue::UnsignedInteger(..) => value
                .to_bytes(&DataType::Integer {
                    bytes: 8,
                    signed: false,
                })
                .unwrap(),
        })
        .collect()
}
