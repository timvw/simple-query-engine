use std::fs::File;
use std::sync::Arc;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Schema};
use arrow2::io::parquet::read;
use arrow2::io::parquet::read::FileReader;
use crate::error::Result;
use crate::RecordBatch;

pub trait DataSource {
    fn schema(&self) -> Schema;
    fn scan(&self, projection: Vec<String>) -> Vec<RecordBatch>;
}

pub struct ParquetDataSource {
    file_reader: FileReader<File>,
}

impl ParquetDataSource {
    pub fn new(file_path: String) -> Result<ParquetDataSource> {
        let file = File::open(file_path)?;
        let file_reader = read::FileReader::try_new(file, None, None, None, None)?;
        Ok(ParquetDataSource {
            file_reader
        })
    }
}

impl DataSource for ParquetDataSource {
    fn schema(&self) -> Schema {
        self.file_reader.schema().clone()
    }

    fn scan(&self, _projection: Vec<String>) -> Vec<Chunk<Arc<dyn Array>>> {
        todo!()
    }
}