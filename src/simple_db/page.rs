pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();

use crate::{
    db::{ColumnOps, ColumnValueOps, TableOps},
    Column, ColumnValue,
};
use byteorder::{ByteOrder, LittleEndian};
use failure::Error;
use log::debug;
use std::borrow::Cow;
use std::fmt;
use std::mem;

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
    pub fn new(columns: &'a [Column]) -> Self {
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

    pub fn iter<'b>(&'b self) -> Box<dyn Iterator<Item = Cow<'b, [ColumnValue]>> + 'b> {
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

    pub fn insert<'b, T>(&mut self, row: T) -> Result<(), Error>
    where
        T: Iterator<Item = &'b Box<[u8]>>,
    {
        let mut row_size: usize = 0;

        for (column, data_box) in self.columns.iter().zip(row) {
            match column.get_data_type().get_fixed_length() {
                Some(l) => row_size += l,
                None => unimplemented!(),
            }

            let column_length = column.get_data_type().get_fixed_length().expect("wtf");
            assert!(data_box.len() <= column_length, "data overflows max length");

            let remaining = vec![0u8; column_length - data_box.len()];

            self.data.extend_from_slice(&data_box);
            self.data.extend_from_slice(&remaining);
        }

        self.offsets
            .push(self.header.row_count as u16 * row_size as u16);
        self.header.row_count += 1;

        self.free_space -= (row_size + mem::size_of::<u16>()) as u16;
        self.free_space_offset += row_size as u16;

        debug!(
            "data={:?}\noffsets={:?}\nfree_space={:?}\nfree_space_offset={:?}\n",
            self.data, self.offsets, self.free_space, self.free_space_offset
        );

        Ok(())
    }
}
