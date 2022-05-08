use crate::RecordBatchStream;
use arrow2::datatypes::Schema;

pub trait DataSource {
    fn schema(&self) -> Schema;
    fn scan(&self, projection: Vec<String>) -> RecordBatchStream;
}

pub mod parquet;
