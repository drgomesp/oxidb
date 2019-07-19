#![feature(box_syntax)]

extern crate bitflags;
extern crate failure;
extern crate log;
#[macro_use]
extern crate prettytable;

use std::{
    io::{stdin, stdout, Write},
    str::FromStr,
};

use log::LevelFilter;
use oxidb_core::types::ColumnValue;
use oxidb_storage::babylon::BabylonStorage;
use oxidb_storage::{ReadOps, StorageFactory, WriteOps};
use prettytable::{Cell, Row};
use simplelog::{CombinedLogger, Config, TermLogger};

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
    fn execute(&self, storage: &mut BabylonStorage) -> Result<(), failure::Error> {
        match self.stmt_type {
            StatementType::Insert => {
                for row in &self.new_rows {
                    storage.insert(row.iter().cloned())?;
                }

                Ok(())
            }
            StatementType::Select => {
                let mut t = prettytable::Table::new();
                t.set_titles(row![cell!("ID"), cell!("FIRST NAME"), cell!("LAST NAME")]);

                for row in storage.iter() {
                    let mut tcv = vec![];

                    for cv in row.iter() {
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

fn prepare_statement(stmt_type: StatementType, input: String) -> Result<Statement, failure::Error> {
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

    let mut storage = BabylonStorage::build().expect("could not build storage engine");

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

        stmt.unwrap().execute(&mut storage);
    }
}
