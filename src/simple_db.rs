use crate::db::{ColumnOps, ColumnValueOps, DataType, TableOps};
use byteorder::{ByteOrder, LittleEndian};
use failure::Error;
use log::debug;
use std::borrow::Cow;
use std::fmt;
use std::mem;

const MAX_PAGES: u8 = 100;
const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();

#[derive(Clone, Debug)]
pub enum ColumnValue {
    StringLiteral(String),
    UnsignedInteger(u64),
    SignedInteger(i64),
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub value_type: DataType,
    pub nullable: bool,
}
#[derive(Debug)]
pub struct Table<'a> {
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
                let mut buf = s.to_owned().into_bytes();
                let remaining = vec![0u8; *length - s.len()];

                buf.extend_from_slice(remaining.as_slice());

                Ok(buf.into_boxed_slice())
            }
            (ColumnValue::UnsignedInteger(i), DataType::Integer { bytes: n, .. }) => {
                let mut buf = vec![0u8; *n as usize];
                LittleEndian::write_u64(&mut buf, *i);
                Ok(buf.to_owned().into_boxed_slice())
            }
            _ => unimplemented!(),
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

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [Self::ColumnValue]>> + 'b> {
        let page_num = 0;
        self.pages[page_num].iter()
    }

    fn insert<T>(&mut self, column_data: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = Self::ColumnValue>,
    {
        let boxed_page = &mut self.pages[0];
        boxed_page.insert(build_column_data(column_data).iter())?;

        self.num_rows += 1;

        Ok(())
    }
}

fn build_column_data<I>(values: I) -> Vec<Box<[u8]>>
where
    I: ExactSizeIterator,
    I: Iterator<Item = ColumnValue>,
{
    values
        .map(|value| match value {
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

#[derive(Clone, Debug)]
pub enum PageType {
    Data,
}

#[derive(Clone, Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub row_count: u64,
}

#[derive(Clone, Debug)]
pub struct Page<'a> {
    pub header: PageHeader,
    pub columns: &'a [Column],
    data: Vec<u8>,
    offsets: Vec<u16>,
    free_space_offset: u16,
    free_space: u16,
}

impl<'a> Page<'a> {
    fn new(columns: &'a [Column]) -> Self {
        Self {
            header: PageHeader {
                row_count: 0,
                page_type: PageType::Data,
            },
            columns,
            data: vec![],
            offsets: vec![],
            free_space: (PAGE_SIZE - PAGE_HEADER_SIZE) as u16,
            free_space_offset: 0,
        }
    }

    fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [ColumnValue]>> + 'b> {
        let (mut row_num, mut column_offset) = (0, 0);

        Box::new(self.offsets.iter().map(move |offset| {
            column_offset = *offset as usize;

            let row: Vec<ColumnValue> = self
                .columns
                .iter()
                .map(|column| {
                    let size = column.get_data_type().get_fixed_length().unwrap();
                    let bytes = &self.data[column_offset..column_offset + size];

                    let v = ColumnValueOps::from_bytes(column.get_data_type(), Cow::from(bytes))
                        .expect("could not create column value ops from bytes");

                    column_offset += size as usize;

                    v
                })
                .collect();

            row_num += 1;

            Cow::from(row)
        }))
    }

    fn insert<'b, T>(&mut self, row: T) -> Result<(), Error>
    where
        T: Iterator<Item = &'b Box<[u8]>>,
    {
        let mut row_size: usize = 0;

        for (column, data_box) in self.columns.iter().zip(row) {
            let mut data = Vec::new();

            match column.get_data_type().get_fixed_length() {
                Some(l) => row_size += l,
                None => unimplemented!(),
            }

            let column_length = column.get_data_type().get_fixed_length().expect("wtf");
            assert!(data_box.len() <= column_length, "data overflows max length");

            let remaining = vec![0u8; column_length - data_box.len()];

            data.extend_from_slice(&data_box);
            data.extend_from_slice(&remaining);
            self.data.extend_from_slice(&data);
        }

        self.offsets
            .push(self.header.row_count as u16 * row_size as u16);
        self.header.row_count += 1;

        self.free_space -= (row_size + mem::size_of::<u16>()) as u16;
        self.free_space_offset += row_size as u16;

        Ok(())
    }
}
