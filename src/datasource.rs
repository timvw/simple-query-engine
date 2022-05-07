use std::sync::Arc;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Schema};

pub trait DataSource {
    fn schema(&self) -> Schema;
    fn scan(&self, projection: Vec<String>) -> Vec<Chunk<Arc<dyn Array>>>;
}

pub struct ParquetDataSource {
    file: String,
}

impl DataSource for ParquetDataSource {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn scan(&self, projection: Vec<String>) -> Vec<Chunk<Arc<dyn Array>>> {
        todo!()
    }
}