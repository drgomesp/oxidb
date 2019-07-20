use oxidb_core::DataType;
use oxidb_schema::ColumnInfo;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}

impl ColumnInfo for Column {
    fn get_name(&self) -> &str {
        self.name.as_str()
    }
    fn get_data_type(&self) -> &DataType {
        &self.data_type
    }
}
