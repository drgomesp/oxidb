use oxidb_core::ColumnValue;

#[cfg(test)]
mod tests {
    use super::*;
    use oxidb_core::ColumnValue;
    use oxidb_storage::{BabylonStorage, ReadOps, StorageFactory, WriteOps};

    #[test]
    fn test_factory() {
        assert!(BabylonStorage::build().is_ok());
    }

    #[test]
    fn test_write() {
        let mut storage: BabylonStorage = BabylonStorage::build().unwrap();

        let row: Vec<_> = build_row("1 foo bar".to_string());
        let rows: Vec<Vec<_>> = vec![row.clone(); 3];

        for r in &rows {
            assert!(storage.insert(r.into_iter().cloned()).is_ok());
        }
    }

    #[test]
    fn test_read() {
        let mut storage: BabylonStorage = BabylonStorage::build().unwrap();

        let row: Vec<_> = build_row("1 foo bar".to_string());
        let rows: Vec<Vec<_>> = vec![row.clone(); 3];

        for r in &rows {
            assert!(storage.insert(r.into_iter().cloned()).is_ok());
        }

        let storage = storage;

        for row in storage.iter() {
            assert_eq!(row[0], ColumnValue::UnsignedInteger(1));
            assert_eq!(row[1], ColumnValue::StringLiteral("foo".to_string()));
            assert_eq!(row[2], ColumnValue::StringLiteral("bar".to_string()));
        }
    }
}

fn build_row(row_str: String) -> Vec<ColumnValue> {
    row_str
        .split(' ')
        .map(|cv| match cv.parse::<u64>() {
            Err(_) => match cv.parse::<i64>() {
                Err(_) => ColumnValue::StringLiteral(cv.into()),
                Ok(id) => ColumnValue::SignedInteger(-id),
            },
            Ok(id) => ColumnValue::UnsignedInteger(id),
        })
        .collect()
}
