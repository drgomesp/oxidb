#![feature(box_syntax)]

#[macro_use]
extern crate prettytable;
extern crate failure;
extern crate log;

mod db;
mod simple_db;

use db::TableOps;
use log::LevelFilter;
use prettytable::{Cell, Row};
use simple_db::{Column, ColumnValue, Table};
use simplelog::{CombinedLogger, Config, TermLogger};
use std::io::{stdin, stdout, Write};
use std::str::FromStr;

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
                    table.insert(row.clone().into_iter()).unwrap();
                }

                Ok(())
            }
            StatementType::Select => {
                for row in table.iter() {
                    for cv in row.iter() {
                        print!("{}", cv);
                    }

                    println!()
                }

                // Create the table
                let mut t = prettytable::Table::new();
                t.set_titles(row![cell!("ID"), cell!("FIRST NAME"), cell!("LAST NAME")]);

                for row in table.iter() {
                    let mut tcv = vec![];

                    for (c, cv) in table.columns.iter().zip(row.iter()) {
                        tcv.push(Cell::new(format!("{:?}", cv).as_str()));
                    }

                    t.add_row(Row::new(tcv));
                }

                // Print the table to stdout
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
    let mut row = Vec::new();

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

    let columns = vec![
        Column {
            name: String::from("id"),
            value_type: db::DataType::Integer {
                bytes: 8,
                signed: false,
            },
            nullable: true,
        },
        Column {
            name: String::from("first_name"),
            value_type: db::DataType::String(8),
            nullable: true,
        },
        Column {
            name: String::from("last_name"),
            value_type: db::DataType::String(8),
            nullable: true,
        },
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
