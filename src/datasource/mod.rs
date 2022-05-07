use arrow2::datatypes::Schema;
use crate::RecordBatch;

pub mod parquet;

pub trait DataSource {
    fn schema(&self) -> Schema;
    fn scan(&self, projection: Vec<String>) -> Vec<RecordBatch>;
}