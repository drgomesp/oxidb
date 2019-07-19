#[cfg(test)]
mod tests {
    use super::*;
    use oxidb_core::types::ColumnValue;
    use oxidb_storage::babylon::BabylonStorage;
    use oxidb_storage::{StorageFactory, WriteOps};

    #[test]
    fn test_factory() {
        assert!(BabylonStorage::build().is_ok());
    }

    #[test]
    fn test_insert_row() {
        let mut storage: BabylonStorage = BabylonStorage::build().unwrap();

        let cvs: Vec<_> = "1 foo bar"
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
            assert!(storage.insert(r.into_iter().cloned()).is_ok());
        }
    }
}
