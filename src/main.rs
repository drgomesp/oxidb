#![feature(box_syntax)]

#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate bitflags;
extern crate failure;
extern crate log;

mod db;

use crate::db::{Column, Table};
use log::LevelFilter;
use oxidb_core::{
    types::{ColumnValue, DataType},
    ColumnInfo, TableOps,
};
use prettytable::{Cell, Row};
use simplelog::{CombinedLogger, Config, TermLogger};
use std::{
    io::{stdin, stdout, Write},
    str::FromStr,
};

#[derive(Debug)]
enum StatementType {
    Insert,
    Select,
}

#[derive(Debug)]
struct Statement {
    pub stmt_type: StatementType,
    pub new_rows: Vec<Vec<ColumnValue>>,
}

impl Statement {
    fn execute(&self, table: &mut Table) -> Result<(), failure::Error> {
        match self.stmt_type {
            StatementType::Insert => {
                for row in &self.new_rows {
                    table.insert(row.into_iter().cloned())?;
                }

                Ok(())
            }
            StatementType::Select => {
                let mut t = prettytable::Table::new();
                t.set_titles(row![cell!("ID"), cell!("FIRST NAME"), cell!("LAST NAME")]);

                for row in table.iter() {
                    let mut tcv = vec![];

                    for (_, cv) in table.columns.iter().zip(row.iter()) {
                        tcv.push(Cell::new(format!("{:?}", cv).as_str()));
                    }

                    t.add_row(Row::new(tcv));
                }

                t.printstd();
                Ok(())
            }
        }
    }
}

impl FromStr for StatementType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "insert" => Ok(StatementType::Insert),
            "select" => Ok(StatementType::Select),
            _ => unimplemented!("unrecognized command"),
        }
    }
}

fn read_stdin() -> String {
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
    input.trim().into()
}

fn prepare_statement<'a>(
    stmt_type: StatementType,
    input: String,
) -> Result<Statement, failure::Error> {
    let column_values: Vec<&str> = input.split(' ').collect();
    let mut row = vec![];

    for cv in column_values {
        match cv.parse::<u64>() {
            Err(_) => row.push(ColumnValue::StringLiteral(cv.into())),
            Ok(id) => row.push(ColumnValue::UnsignedInteger(id)),
        };
    }

    Ok(Statement {
        stmt_type,
        new_rows: vec![row],
    })
}

fn main() {
    CombinedLogger::init(vec![
        TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()
    ])
    .unwrap();

    let columns: Vec<Box<dyn ColumnInfo>> = vec![
        box Column::new(
            String::from("id"),
            DataType::Integer {
                bytes: 8,
                signed: false,
            },
            true,
        ),
        box Column::new(String::from("first_name"), DataType::String(8), true),
        box Column::new(String::from("last_name"), DataType::String(8), true),
    ];

    let mut table = Table::new(String::from("users"), &columns);

    loop {
        print!("oxidb> ");
        stdout().flush().unwrap();

        let stmt = prepare_statement(
            match read_stdin().parse() {
                Ok(t) => t,
                _ => unimplemented!(),
            },
            read_stdin(),
        );

        stmt.unwrap().execute(&mut table).unwrap()
    }
}
