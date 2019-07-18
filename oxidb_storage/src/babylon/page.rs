use crate::babylon::column::Column;
use failure::Error;
use log::debug;
use oxidb_core::types::DataType;
use oxidb_core::{types::ColumnValue, ColumnValueOps};
use oxidb_schema::ColumnInfo;
use std::{borrow::Cow, mem};

const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();

bitflags!(
  #[derive(Default)]
  pub struct PageFlags: u8 {
    const EMPTY = 0b_1000_0000;
    const FULL  = 0b_0100_0000;
  }
);

type RowPointer = (u16, u16);

#[derive(Clone, Debug)]
pub struct PageHeader {
    pub page_size: usize,
    pub row_count: usize,
    pub flags: PageFlags,
    pub free_space: usize,
}

#[derive(Clone, Debug)]
pub struct Page<'a> {
    pub header: PageHeader,
    columns: &'a Vec<Column>,
    offsets: Vec<RowPointer>,
    data: Vec<u8>,
}

impl<'a> Page<'a> {
    pub fn new(columns: &'a Vec<Column>) -> Self {
        Self {
            header: PageHeader {
                page_size: PAGE_SIZE,
                row_count: 0,
                flags: PageFlags::default(),
                free_space: PAGE_SIZE - PAGE_HEADER_SIZE,
            },
            columns,
            offsets: vec![],
            data: vec![],
        }
    }
}

impl<'a> Page<'a> {
    pub fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [ColumnValue]>> + 'b> {
        let (mut row_num, mut column_offset) = (0, 0);

        Box::new(self.offsets.iter().map(move |(offset, _)| {
            column_offset = *offset as usize;

            let row: Vec<ColumnValue> = self
                .columns
                .iter()
                .map(|column| {
                    let size = column.get_data_type().get_fixed_length().unwrap();
                    let bytes = &self.data[column_offset..column_offset + size];

                    let v = ColumnValueOps::from_bytes(column.get_data_type(), Cow::from(bytes))
                        .expect("could not create column value ops from bytes");

                    column_offset += size;

                    v
                })
                .collect();

            row_num += 1;

            Cow::from(row)
        }))
    }

    pub fn insert_row<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: ExactSizeIterator,
        T: Iterator<Item = ColumnValue>,
    {
        let mut row_size: usize = 0;

        for (column, cv) in self.columns.iter().zip(row) {
            let data_box = cv.to_bytes(column.get_data_type())?;

            match column.get_data_type().get_fixed_length() {
                Some(l) => row_size += l,
                None => unimplemented!(),
            }

            let column_length = column.get_data_type().get_fixed_length().expect("wtf");
            let remaining = vec![0u8; column_length - data_box.len()];

            self.data.extend_from_slice(&data_box);
            self.data.extend_from_slice(&remaining);
        }

        if self.header.free_space < row_size {
            panic!("not enough space in page")
        }

        self.offsets.push((
            self.header.row_count as u16 * row_size as u16,
            row_size as u16,
        ));

        self.header.row_count += 1;
        self.header.free_space -= row_size + mem::size_of::<RowPointer>();

        debug!("page={:#?}\n", self);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use oxidb_core::types::DataType;

    use super::*;

    #[derive(Debug)]
    struct SomeColumn {
        pub name: String,
        pub data_type: DataType,
    }

    impl SomeColumn {
        pub fn new(name: String, value_type: DataType) -> Self {
            Self {
                name,
                data_type: value_type,
            }
        }
    }

    impl ColumnInfo for SomeColumn {
        fn get_name(&self) -> &str {
            self.name.as_str()
        }
        fn get_data_type(&self) -> &DataType {
            &self.data_type
        }
    }

    #[test]
    fn test_page_default() {
        let columns = vec![];
        let page = Page::new(&columns);

        assert_eq!(page.header.page_size, PAGE_SIZE);
        assert_eq!(page.header.free_space, PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_page_insert() {
        let columns: Vec<Column> = vec![
            Column::new(
                "uint".to_string(),
                DataType::Integer {
                    bytes: 8,
                    signed: false,
                },
                false,
            ),
            Column::new(
                "int".to_string(),
                DataType::Integer {
                    bytes: 8,
                    signed: true,
                },
                false,
            ),
            Column::new("string".to_string(), DataType::String(8), false),
        ];

        let mut page = Page::new(&columns);

        let cvs: Vec<_> = "1 -1 string"
            .split(' ')
            .map(|cv| match cv.parse::<u64>() {
                Err(_) => match cv.parse::<i64>() {
                    Err(_) => ColumnValue::StringLiteral(cv.into()),
                    Ok(id) => ColumnValue::SignedInteger(-id),
                },
                Ok(id) => ColumnValue::UnsignedInteger(id),
            })
            .collect();

        let rows: Vec<Vec<ColumnValue>> = vec![cvs.clone(); 3];

        for r in &rows {
            assert!(page.insert_row(r.into_iter().cloned()).is_ok());
        }

        let row_size = 8 + 8 + 8;
        let pointer_size = 4;

        assert_eq!(
            page.header.free_space,
            PAGE_SIZE - PAGE_HEADER_SIZE - (row_size + pointer_size) * rows.len()
        );

        assert_eq!(page.header.row_count, rows.len());
    }
}
