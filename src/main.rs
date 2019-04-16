extern crate failure;
extern crate log;

use log::{debug, LevelFilter};
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
    stmt_type: StatementType,
}

impl Statement {
    fn new(stmt_type: StatementType) -> Self {
        Self { stmt_type }
    }

    fn execute(&self) -> Result<(), failure::Error> {
        debug!("execute({:?})", self);
        Ok(())
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
    debug!("prepare_statement({:?})", input);
    Ok(Statement::new(stmt_type))
}

fn main() {
    CombinedLogger::init(vec![
        TermLogger::new(LevelFilter::Debug, Config::default()).unwrap()
    ])
    .unwrap();

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

        stmt.unwrap().execute().unwrap()
    }
}
